#!/usr/bin/env python3
"""
dsi_vcs.py — rsync-based file version control system
Captures full Linux file metadata (stat, ACL, xattrs), MD5 hash,
and stores versioned snapshots in SQLite.

Usage:
    dsi-vcs init                 # init repo in current directory
    dsi-vcs add <path>...        # stage paths for the next commit
    dsi-vcs delete <path>...     # stage paths for deletion
    dsi-vcs remove <path>...     # unstage paths
    dsi-vcs commit [message]     # commit a new version
    dsi-vcs log                  # list versions
    dsi-vcs diff <v1> <v2>       # diff two versions
    dsi-vcs restore <version>    # restore a version
    dsi-vcs clone <path>         # clone a repository
    dsi-vcs branch <branch name> # create a new branch
    dsi-vcs list-branch          # list branches
    dsi-vcs merge <branch name>  # merge a branch to current branch

Requirements:
    sudo apt install acl         # for getfacl (ACL support)
"""

import os
import sys
import subprocess
import json
import datetime
import shutil
import tempfile
import pwd
import grp
from typing import Optional

from .vcs_db import DB_NAME, SNAPSHOTS_DIR, open_db
from .vcs_metadata_helper import collect_metadata, owner_name, check_access_permission, set_acl
from .repolog.log_record import OperationType, RecordKind, _utcnow, ns_to_datetime_parts
from .repolog.merkle import HASH_ALGORITHM, build_merkle_tree, commit_hash as merkle_commit_hash, parent_path
from .repolog.repository_log import RepositoryLog
from .repolog.chunking import CHUNK_STORAGE_DIR, store_chunks_for_snapshot, rebuild_file_from_chunks

def snapshot_target(snapshot_path: str, relative_path: str) -> str:
    target = os.path.abspath(os.path.join(snapshot_path, relative_path))
    snapshot_root = os.path.abspath(snapshot_path)
    if os.path.commonpath([snapshot_root, target]) != snapshot_root:
        raise ValueError(f"Snapshot path escapes snapshot root: {relative_path}")
    return target

def copy_path_into_snapshot(root_folder: str, snapshot_path: str, abs_path: str) -> None:
    rel_path = os.path.relpath(abs_path, root_folder)
    if rel_path in ("", "."):
        return

    target = snapshot_target(snapshot_path, rel_path)
    if os.path.isdir(abs_path) and not os.path.islink(abs_path):
        shutil.copytree(abs_path, target, dirs_exist_ok=True, symlinks=True)
    else:
        os.makedirs(os.path.dirname(target), exist_ok=True)
        shutil.copy2(abs_path, target)


def rebuild_tree_from_chunks(conn, commit_hash: str, chunk_root: str, target_tree: str) -> None:
    if not commit_hash:
        os.makedirs(target_tree, exist_ok=True)
        return False

    chunk_dir = os.path.join(chunk_root, CHUNK_STORAGE_DIR)
    rows = conn.execute(
        "SELECT chunk_hash, relative_file_path, chunk_index FROM chunk_store "
        "WHERE commit_hash=? ORDER BY relative_file_path, chunk_index",
        (commit_hash,),
    ).fetchall()

    dir_rows = conn.execute(
        "SELECT merkle_nodes.relative_path FROM merkle_nodes, versions "
        "WHERE merkle_nodes.version_id = versions.id AND versions.commit_hash = ? "
        "AND merkle_nodes.file_type = 'dir' AND merkle_nodes.relative_path <> '.'",
        (commit_hash,),
    ).fetchall()

    grouped: dict[str, list[tuple[str, int]]] = {}
    access_checked = dict[str, str]()
    for row in rows:
        if row["relative_file_path"] not in access_checked:
            metadata = check_access_permission(conn, target_tree, commit_hash, row["relative_file_path"], "read")
            if metadata is not None:
                print(f"---> access granted: {row['relative_file_path']}")
            else:
                print(f"---> access denied: {row['relative_file_path']}")
            access_checked[row["relative_file_path"]] = metadata
        if access_checked[row["relative_file_path"]] is not None:    
            grouped.setdefault(row["relative_file_path"], []).append((row["chunk_hash"], row["chunk_index"]))
        

    try:
        for rel_path, chunks in grouped.items():
            target_path = snapshot_target(target_tree, rel_path)
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            with open(target_path, "wb") as handle:
                for chunk_hash, _chunk_index in sorted(chunks, key=lambda item: item[1]):
                    chunk_path = os.path.join(chunk_dir, chunk_hash)
                    if not os.path.exists(chunk_path):
                        print(f"---> Chunk not found for chunk hash: {chunk_path}")
                        return False
                    with open(chunk_path, "rb") as chunk_handle:
                        shutil.copyfileobj(chunk_handle, handle)
    except Exception as e:
        print(f"---> Error occurred which rebuilding chunks: {e}")
        return False

    '''
    Always create directories: chunk_store only contains file content
    so we would otherwise miss empty directories.
    '''
    for row in dir_rows:
        rel_path = row["relative_path"]
        os.makedirs(snapshot_target(target_tree, rel_path), exist_ok=True)
        if rel_path not in access_checked:
            access_checked[rel_path] = check_access_permission(conn, target_tree, commit_hash, rel_path, "read")

    # update acl text, owner, group, and permissions
    for rel_path in access_checked:
        metadata = access_checked[rel_path]
        if metadata is not None:
            acl_text = metadata.get("acl_text")
            permissions_int = int(metadata.get("permissions_int", 0))
            target_path = snapshot_target(target_tree, rel_path)
            if acl_text is not None and acl_text != "":
                set_acl(target_path, acl_text)

            owner = metadata.get("owner_name")
            group = metadata.get("group_name")
            if owner or group:
                uid = os.getuid()
                gid = os.getgid()
                if owner:
                    try:
                        uid = pwd.getpwnam(owner).pw_uid
                    except KeyError:
                        uid = os.getuid()
                if group:
                    try:
                        gid = grp.getgrnam(group).gr_gid
                    except KeyError:
                        gid = os.getgid()
                # If not running as root, we cannot chown to arbitrary users/groups.
                # Ensure we don't attempt to set owner/group to someone else when
                # the process lacks privilege — fall back to current user/group.
                if os.geteuid() != 0:
                    # cannot set arbitrary owner/group as non-root; assign to current user/group
                    uid = os.getuid()
                    gid = os.getgid()
                try:
                    os.chown(target_path, uid, gid)
                except PermissionError:
                    # as a last resort, try to ensure the file is owned by current user
                    try:
                        os.chown(target_path, os.getuid(), os.getgid())
                    except PermissionError:
                        pass

            if os.geteuid() == 0 or owner_name(os.getuid()) == metadata.get("owner_name"):
                os.chmod(target_path, permissions_int)
    return True


def materialize_commit_to_worktree(conn, commit_hash: str, chunk_root: str, root_folder: str) -> None:
    # TODO: implemente stash, then uncomment the follwoing.
    # for entry in os.listdir(root_folder):
    #     if entry in {DB_NAME, SNAPSHOTS_DIR}:
    #         continue
    #     path = os.path.join(root_folder, entry)
    #     if os.path.isdir(path) and not os.path.islink(path):
    #         shutil.rmtree(path)
    #     elif os.path.lexists(path):
    #         os.unlink(path)

    return rebuild_tree_from_chunks(conn, commit_hash, chunk_root, root_folder)


# ─────────────────────────── COMMANDS ────────────────────────────────────────
class Version:

    def __init__(self, folder: str):
        self.root_folder = os.path.abspath(folder)
        self.skip_names = {DB_NAME, SNAPSHOTS_DIR, CHUNK_STORAGE_DIR}
        if not os.path.isdir(self.root_folder):
            sys.exit(f"Error: '{self.root_folder}' is not a directory.")

        conn = open_db(self.root_folder)
        conn.close()
        self.repo_log = RepositoryLog(
            os.path.join(self.root_folder, SNAPSHOTS_DIR),
            repository_id="repo-123",
        )
        print(f"Initialized dsi-vcs repository in: {self.root_folder}")
        print(f"  Snapshots: {self.root_folder}/{SNAPSHOTS_DIR}/")

    def _load_pending_stage_entries(self) -> dict[str, tuple[object, str]]:
        pending: dict[str, tuple[object, str]] = {}
        for _, record in self.repo_log.iter_records(self.repo_log.latest_commit_location or 0):
            if record.kind == RecordKind.COMMIT:
                pending.clear()
                continue
            if record.kind != RecordKind.DATA or not record.file_path:
                continue

            metadata = record.extra_metadata or {}
            if metadata.get("staging_op") == "remove":
                pending.pop(record.file_path, None)
                continue

            action = metadata.get("staging_action")
            if action == "delete" or record.operation == OperationType.FILE_DELETE:
                pending[record.file_path] = (record, "delete")
            else:
                pending[record.file_path] = (record, "add")
        return pending

    def _print_staged_paths(self) -> None:
        staged = self._load_pending_stage_entries()
        if not staged:
            print("Nothing staged. Use 'add <path>...' or 'delete <path>...' to stage paths.")
            return

        print(f"Staged paths ({len(staged)}):")
        for rel_path, (_record, action) in sorted(
            ((os.path.relpath(path, self.root_folder), entry) for path, entry in staged.items()),
            key=lambda item: item[0],
        ):
            print(f"  {rel_path} [{action}]")

    def _check_path_validity(self, abs_path: str):
        rel_path = os.path.relpath(abs_path, self.root_folder)
        if not os.path.lexists(abs_path):
            print(f"  [skip] {abs_path}: path does not exist.")
            return False
        if os.path.commonpath([os.path.realpath(rel_path), self.root_folder]) != self.root_folder:
            print(f"  [skip] {abs_path}: path is not under root folder.")
            return False
        return True

    def cmd_add(self, paths: list[str]):
        """
        Stage one or more files/directories for the next commit.
        Directories are expanded recursively; each resolved file is inserted into
        the staging table.  Adding an already-staged path is a silent no-op.
        paths should be a list of relative paths to root_folder.
        """
        # print(f"Staging {len(paths)} path(s) for commit…")
        # print(paths)
        db_path = os.path.join(self.root_folder, SNAPSHOTS_DIR, DB_NAME)
        if not os.path.isfile(db_path):
            sys.exit("No dsi-vcs repo found. Run 'init' first.")

        staged = 0
        stage_entries = []
        txn_id = f"staging-{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S%f')}"

        def stage_path(abs_path: str):
            nonlocal staged
            rel_path = os.path.relpath(abs_path, self.root_folder)
            if(self._check_path_validity(abs_path)):    
                stage_entries.append(
                    {
                        "file_path": rel_path,
                        "operation": OperationType.FILE_ADD,
                        "chunk_hash": None,
                        "chunk_ref": None,
                        "extra_metadata": {"staging_action": "add"},
                    }
                )
                staged += 1

        for raw in paths:
            abs_path = os.path.abspath(raw if os.path.isabs(raw) else os.path.join(self.root_folder, raw))

            if os.path.isdir(abs_path):
                # Expand directory recursively
                for dirpath, dirnames, filenames in os.walk(abs_path, followlinks=False):
                    dirnames[:] = [d for d in dirnames if d not in self.skip_names]
                    for fname in filenames:
                        if fname in self.skip_names or dirpath == os.path.join(self.root_folder, SNAPSHOTS_DIR):
                            continue
                        stage_path(os.path.join(dirpath, fname))
            else:
                stage_path(abs_path)

        if stage_entries:
            self.repo_log.append_many(stage_entries, transaction_id=txn_id)

        print(f"  {staged} path(s) added to staging.")
        self._print_staged_paths()

    def cmd_delete(self, paths: list[str]):
        """Stage path(s) for deletion in the next commit."""
        db_path = os.path.join(self.root_folder, SNAPSHOTS_DIR, DB_NAME)
        if not os.path.isfile(db_path):
            sys.exit("No dsi-vcs repo found. Run 'init' first.")

        staged = 0
        stage_entries = []
        txn_id = f"staging-{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S%f')}"

        def stage_path(abs_path: str):
            nonlocal staged
            rel_path = os.path.relpath(abs_path, self.root_folder)
            if(self._check_path_validity(abs_path)):    
                stage_entries.append(
                    {
                        "file_path": rel_path,
                        "operation": OperationType.FILE_DELETE,
                        "chunk_hash": None,
                        "chunk_ref": None,
                        "extra_metadata": {"staging_action": "delete"},
                    }
                )
                staged += 1

        for raw in paths:
            abs_path = os.path.abspath(raw if os.path.isabs(raw) else os.path.join(self.root_folder, raw))

            if os.path.isdir(abs_path):
                # Expand directory recursively
                for dirpath, dirnames, filenames in os.walk(abs_path, followlinks=False):
                    dirnames[:] = [d for d in dirnames if d not in self.skip_names]
                    for fname in filenames:
                        if fname in self.skip_names or dirpath == os.path.join(self.root_folder, SNAPSHOTS_DIR):
                            continue
                        stage_path(os.path.join(dirpath, fname))
            else:
                stage_path(abs_path)

        if stage_entries:
            self.repo_log.append_many(stage_entries, transaction_id=txn_id)

        print(f"  {staged} path(s) staged for deletion.")
        self._print_staged_paths()

    def cmd_remove(self, paths: list[str]):
        """Remove path(s) from the staging area without touching the actual files."""
        db_path = os.path.join(self.root_folder, SNAPSHOTS_DIR, DB_NAME)
        if not os.path.isfile(db_path):
            sys.exit("No dsi-vcs repo found. Run 'init' first.")

        removed = 0
        stage_entries = []
        txn_id = f"staging-{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S%f')}"

        def stage_path(abs_path: str):
            nonlocal removed
            rel_path = os.path.relpath(abs_path, self.root_folder)
            if(self._check_path_validity(abs_path)):    
                stage_entries.append(
                    {
                        "file_path": rel_path,
                        "operation": OperationType.FILE_REMOVE,
                        "chunk_hash": None,
                        "chunk_ref": None,
                        "extra_metadata": {"staging_action": "remove", "staging_op": "remove"},
                    }
                )
                removed += 1

        for raw in paths:
            abs_path = os.path.abspath(raw if os.path.isabs(raw) else os.path.join(self.root_folder, raw))

            if os.path.isdir(abs_path):
                # Expand directory recursively
                for dirpath, dirnames, filenames in os.walk(abs_path, followlinks=False):
                    dirnames[:] = [d for d in dirnames if d not in self.skip_names]
                    for fname in filenames:
                        if fname in self.skip_names or dirpath == os.path.join(self.root_folder, SNAPSHOTS_DIR):
                            continue
                        stage_path(os.path.join(dirpath, fname))
            else:
                stage_path(abs_path)

        if stage_entries:
            self.repo_log.append_many(stage_entries, transaction_id=txn_id)

        print(f"  {removed} path(s) removed from staging.")
        self._print_staged_paths()

    def _get_latest_branch_name(self, conn) -> Optional[str]:
        row = conn.execute(
            "SELECT branch_name FROM branches WHERE root_folder=? AND is_latest=1 LIMIT 1",
            (self.root_folder,),
        ).fetchone()
        return row["branch_name"] if row else None

    def _get_latest_commit_of_branch(self, conn, branch_name: str) -> Optional[str]:
        row = conn.execute(
            "SELECT child_commit_hash FROM branch_links WHERE child_branch_name=? ORDER BY created_at DESC LIMIT 1",
            (branch_name,),
        ).fetchone()
        return row["child_commit_hash"] if row else None

    def _get_tracked_commit_of_branch(self, conn, branch_name: str) -> Optional[str]:
        row = conn.execute(
            "SELECT tracked_commit_hash FROM branches WHERE branch_name=?",
            (branch_name,),
        ).fetchone()
        return row["tracked_commit_hash"] if row else None
    
    def _get_entries_in_commit(self, conn, commit_hash: str) -> list[dict]:
        rows = conn.execute(
            "SELECT relative_path, file_type, node_hash "
            "FROM merkle_nodes, versions "
            "WHERE merkle_nodes.version_id = versions.id AND versions.root_folder = ? AND versions.commit_hash = ? AND merkle_nodes.relative_path <> '.'",
            (self.root_folder, commit_hash),
        ).fetchall()
        return [row['relative_path'] for row in rows]

    def cmd_branch(self, branch_name: str, start_point: Optional[str] = None):
        """Create a branch at the specified commit or latest commit."""
        conn = open_db(self.root_folder)
        cur = conn.cursor()

        if not branch_name or not branch_name.strip():
            conn.close()
            sys.exit("Branch name is required.")

        target_commit = start_point or "latest"
        if target_commit == "latest":
            current_branch_name = self._get_latest_branch_name(conn)
            parent_commit_hash = self._get_latest_commit_of_branch(conn, current_branch_name)
            row = cur.execute(
                "SELECT commit_hash FROM versions WHERE root_folder=? AND commit_hash=?",
                (self.root_folder, parent_commit_hash),
            ).fetchone()
        else:
            row = cur.execute(
                "SELECT commit_hash FROM versions WHERE root_folder=? AND commit_hash LIKE ?",
                (self.root_folder, target_commit + "%"),
            ).fetchone()

        if not row:
            conn.close()
            sys.exit(f"Commit '{target_commit}' not found.")

        commit_hash = row["commit_hash"]
        now = _utcnow()
        '''
        Branch creation only creates (does not activate) the branch.
        Use switch to move to a new branch.
        '''
        cur.execute(
            "INSERT OR IGNORE INTO branches (root_folder, branch_name, head_commit_hash, tracked_commit_hash, is_latest, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (self.root_folder, branch_name, commit_hash, commit_hash, 0, now),
        )
        conn.commit()
        conn.close()
        print(f"Created branch '{branch_name}' at {commit_hash[:12]}")


    def cmd_merge(self, branch_name: str, target_commit: Optional[str] = None):
        """Merge the named branch into the current HEAD by recording a parent-child link."""
        # TODO: implement three way merge.
        conn = open_db(self.root_folder)
        cur = conn.cursor()

        branch_row = cur.execute(
            "SELECT head_commit_hash FROM branches WHERE root_folder=? AND branch_name=?",
            (self.root_folder, branch_name),
        ).fetchone()
        if not branch_row:
            conn.close()
            sys.exit(f"Branch '{branch_name}' not found.")

        if target_commit is None or target_commit == "latest":
            current_branch_name = self._get_latest_branch_name(conn)
            parent_commit_hash = self._get_latest_commit_of_branch(conn, current_branch_name)
            head_row = cur.execute(
                "SELECT commit_hash FROM versions WHERE root_folder=? AND commit_hash=?",
                (self.root_folder, parent_commit_hash),
            ).fetchone()
        else:
            head_row = cur.execute(
                "SELECT commit_hash FROM versions WHERE root_folder=? AND commit_hash LIKE ?",
                (self.root_folder, target_commit + "%"),
            ).fetchone()

        if not head_row:
            conn.close()
            sys.exit(f"Commit '{target_commit or 'latest'}' not found.")

        parent_commit_hash = branch_row["head_commit_hash"]
        child_commit_hash = head_row["commit_hash"]
        now = _utcnow()
        cur.execute(
            "INSERT OR IGNORE INTO branch_links (parent_commit_hash, child_commit_hash, child_branch_name, created_at) "
            "VALUES (?, ?, ?, ?)",
            (parent_commit_hash, child_commit_hash, branch_name, now),
        )
        conn.commit()
        conn.close()
        print(f"Merged branch '{branch_name}' into {child_commit_hash[:12]}")

    def cmd_commit(self, message: str = ""):
        db_path = os.path.join(self.root_folder, SNAPSHOTS_DIR, DB_NAME)
        if not os.path.isfile(db_path):
            sys.exit("No dsi-vcs repo found. Run 'init' first.")

        conn = open_db(self.root_folder)
        cur = conn.cursor()

        # ── Load staged paths from the repository log ─────────────────────────
        pending_entries = self._load_pending_stage_entries()
        if not pending_entries:
            conn.close()
            sys.exit("Nothing staged. Use 'add' or 'delete' before committing.")

        staged_adds = [path for path, (_record, action) in pending_entries.items() if action == "add"]
        staged_deletes = [path for path, (_record, action) in pending_entries.items() if action == "delete"]

        current_branch_name = self._get_latest_branch_name(conn)
        parent_commit_hash = self._get_tracked_commit_of_branch(conn, current_branch_name)

        committed_at = _utcnow()
        running_user = owner_name(os.getuid())
        snapshots_root = os.path.join(self.root_folder, SNAPSHOTS_DIR)
        entries_in_last_commit = set(self._get_entries_in_commit(conn, parent_commit_hash) if parent_commit_hash else [])

        for added_paths in staged_adds:
            entries_in_last_commit.add(os.path.relpath(added_paths, start=self.root_folder))
        for deleted_paths in staged_deletes:
            entries_in_last_commit.discard(os.path.relpath(deleted_paths, start=self.root_folder))

        # ── Collect metadata for the complete committed tree ───────────────────
        entries = []
        for rel_path in entries_in_last_commit:
            e = check_access_permission(conn, self.root_folder, parent_commit_hash, rel_path, "write")
            if e is None:
                print(f"  [skip] {rel_path}: write access denied")
                continue

            temp_meta = collect_metadata(os.path.join(self.root_folder, rel_path), self.root_folder)
            if len(e) == 0 or ("owner_name" in e and running_user == e["owner_name"]): # only owner can update metadata
                e = temp_meta
            else:
                e["absolute_path"] = temp_meta["absolute_path"]
                e["_st_size"] = temp_meta["_st_size"]
                e["file_type"] = temp_meta["file_type"]
                e["relative_path"] = temp_meta["relative_path"]
                e["file_name"] = temp_meta["file_name"]
            if "error" in e:
                print(f"  [skip] {e['relative_path']}: {e['error']}")
            else:
                entries.append(e)

        total_bytes = sum(e.get("_st_size") or 0 for e in entries if e.get("file_type") == "file")
        file_count  = sum(1 for e in entries if e.get("file_type") == "file")
        print(f"  {file_count} file(s), {total_bytes:,} bytes")

        if len(entries) == 0:
            conn.close()
            sys.exit("No files to commit after filtering for access permissions.")

        file_hashes, chunk_hashes, chunk_length = store_chunks_for_snapshot(conn, snapshots_root, entries)
        root_tree_hash, merkle_nodes = build_merkle_tree(entries, file_hashes, chunk_length)
        commit_hash = merkle_commit_hash(
            root_tree_hash=root_tree_hash,
            parent_commit_hash=parent_commit_hash,
            committed_at=committed_at,
            owner_name=running_user,
            message=message,
            file_count=file_count,
            total_bytes=total_bytes,
        )

        # ── update chunk store with commit hash ──────────────────────────────────
        batch_size = 32000
        update_sql = "UPDATE chunk_store SET commit_hash=? WHERE commit_hash = 'UPDATE' AND chunk_hash = ?"
        update_params = [(commit_hash, chunk_hash) for chunk_hash in chunk_hashes]
        for i in range(0, len(update_params), batch_size):
            cur.executemany(update_sql, update_params[i:i + batch_size])
        # ── Insert version row ───────────────────────────────────────────────────
        cur.execute(
            """INSERT INTO versions
            (root_folder, commit_hash, root_tree_hash, hash_algorithm,
                committed_at, owner_name, message, file_count, total_bytes)
            VALUES (?,?,?,?,?,?,?,?,?)""",
            (self.root_folder, commit_hash, root_tree_hash, HASH_ALGORITHM,
            committed_at, running_user, message, file_count, total_bytes)
        )
        version_id = cur.lastrowid

        merkle_cols = [
            "version_id", "root_folder", "relative_path", "file_type",
            "node_hash", "metadata", "content_hash_sha256",
            "subtree_file_count", "subtree_total_bytes", "child_count",
        ]
        merkle_placeholders = ",".join("?" * len(merkle_cols))
        merkle_col_str = ",".join(merkle_cols)

        cur.executemany(
            f"INSERT INTO merkle_nodes ({merkle_col_str}) VALUES ({merkle_placeholders})",
            [
                tuple(
                    version_id if c == "version_id" else
                    self.root_folder if c == "root_folder" else
                    node.get(c)
                    for c in merkle_cols
                )
                for node in merkle_nodes
            ]
        )

        commit_records = [record for record, _action in sorted(
            pending_entries.values(), key=lambda item: item[0].sequence
        )]
        self.repo_log.commit(commit_records)

        branch_name = self._get_latest_branch_name(conn) or "main"
        branch_row = conn.execute(
            "SELECT 1 FROM branches WHERE root_folder=? LIMIT 1",
            (self.root_folder,),
        ).fetchone()
        if not branch_row:
            conn.execute(
                "INSERT INTO branches (root_folder, branch_name, head_commit_hash, tracked_commit_hash, is_latest, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (self.root_folder, branch_name, commit_hash, commit_hash, 1, committed_at),
            )
            conn.execute(
                "INSERT INTO branch_links (parent_commit_hash, child_commit_hash, child_branch_name, created_at) VALUES (?, ?, ?, ?)",
                (None, commit_hash, branch_name, committed_at),
            )
        else:
            conn.execute(
                "UPDATE branches SET is_latest=0 WHERE root_folder=? AND branch_name<>?",
                (self.root_folder, branch_name),
            )
            conn.execute(
                "UPDATE branches SET is_latest=1, tracked_commit_hash=? WHERE root_folder=? AND branch_name=?",
                (commit_hash, self.root_folder, branch_name),
            )
            conn.execute(
                "INSERT INTO branch_links (parent_commit_hash, child_commit_hash, child_branch_name, created_at) VALUES (?, ?, ?, ?)",
                (parent_commit_hash, commit_hash, branch_name, committed_at),
            )
        conn.commit()
        conn.close()

        print(f"\n Committed {commit_hash} at {committed_at}")
        print(f"  Short hash : {commit_hash[:12]}")
        print(f"  Owner      : {running_user}")
        if message:
            print(f"  Message    : {message}")


    def cmd_list_branch(self):
        """List all known branches and their current head commit."""
        conn = open_db(self.root_folder)
        rows = conn.execute(
            "SELECT branch_name, head_commit_hash, created_at FROM branches WHERE root_folder=? ORDER BY branch_name",
            (self.root_folder,),
        ).fetchall()
        conn.close()

        if not rows:
            print("No branches yet.")
            return

        print("Branches:")
        for row in rows:
            short_hash = row["head_commit_hash"][:12] if row["head_commit_hash"] else "(none)"
            print(f"  - {row['branch_name']} @ {short_hash}  ({row['created_at']})")

    def cmd_switch(self, branch_name: str):
        """Switch the working tree to the named branch's latest snapshot."""
        conn = open_db(self.root_folder)
        branch_row = conn.execute(
            "SELECT branch_name, head_commit_hash FROM branches WHERE root_folder=? AND branch_name=?",
            (self.root_folder, branch_name),
        ).fetchone()

        if not branch_row:
            conn.close()
            sys.exit(f"Branch '{branch_name}' not found.")

        old_branch = self._get_latest_branch_name(conn)
        if old_branch == branch_name:
            conn.close()
            print(f"Already on branch '{branch_name}'. No switch needed.")
            return
        
        conn.execute(
            "UPDATE branches SET is_latest=0 WHERE root_folder=? AND branch_name<>?",
            (self.root_folder, branch_name),
        )
        conn.execute(
            "UPDATE branches SET is_latest=1 WHERE root_folder=? AND branch_name=?",
            (self.root_folder, branch_name),
        )
        conn.commit()

        latest_commit_hash = self._get_tracked_commit_of_branch(conn, branch_name)
        if not latest_commit_hash:
            conn.close()
            sys.exit(f"No commit found for branch '{branch_name}'.")

        version_row = conn.execute(
            "SELECT commit_hash FROM versions WHERE root_folder=? AND commit_hash=? ORDER BY id DESC LIMIT 1",
            (self.root_folder, latest_commit_hash),
        ).fetchone()

        if not version_row:
            sys.exit(f"No commit found for branch '{branch_name}'.")

        print(f"Switching to branch '{branch_name}' at {version_row['commit_hash'][:12]}")
        result = materialize_commit_to_worktree(conn, version_row["commit_hash"], os.path.join(self.root_folder, SNAPSHOTS_DIR), self.root_folder)
        conn.close()
        if result is False:
            print(f"Failed to switch branch: {branch_name}.")
        else:
            print(f"Restored branch snapshot into {self.root_folder}")

    def cmd_log(self, branch_name: str = None, v_limit: int = 10):
        root_folder = os.path.abspath(self.root_folder)
        conn = open_db(root_folder)

        if branch_name is None:
            branch_name = self._get_latest_branch_name(conn)

        branch_row = conn.execute(
            "SELECT head_commit_hash FROM branches WHERE root_folder=? AND branch_name=?",
            (root_folder, branch_name),
        ).fetchone()
        if not branch_row:
            conn.close()
            sys.exit(f"Branch '{branch_name}' not found.")
        rows = conn.execute(
            "SELECT commit_hash, committed_at, owner_name, message, file_count, total_bytes "
            "FROM versions, branch_links ON versions.commit_hash = branch_links.child_commit_hash "
            "WHERE branch_links.child_branch_name=? ORDER BY versions.committed_at DESC LIMIT ?",
            (branch_name, v_limit,),
        ).fetchall()

        conn.close()

        if not rows:
            print("No versions yet. Run 'commit' first.")
            return

        if branch_name:
            print(f"Branch log: {branch_name}")
        elif branch_row:
            print(f"Branch log: {branch_row['branch_name']}")
        else:
            print("Branch log: default/latest")

        print("─" * 90)
        print(f"{'COMMIT HASH':<14} {'OWNER':<12} {'DATE/TIME (UTC)':<28} {'FILES':>7} {'BYTES':>15}  MESSAGE")
        print("─" * 90)
        for r in rows:
            msg = (r["message"] or "")[:35]
            ctime = ns_to_datetime_parts(r["committed_at"]).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " UTC"
            print(f"{r['commit_hash'][:12]:<14} {r['owner_name']:<12} {ctime:<28}"
                f"{r['file_count']:>7} {r['total_bytes']:>15,}  {msg}")


    def cmd_diff(self, c1: str, c2: str):
        root_folder = os.path.abspath(self.root_folder)
        conn = open_db(root_folder)

        def get_files_in_root_folder():
            files = {}
            for dirpath, dirnames, filenames in os.walk(root_folder, followlinks=False):
                dirnames[:] = [d for d in dirnames if d not in {DB_NAME, SNAPSHOTS_DIR}]
                if dirpath != root_folder:
                    abs_path = dirpath
                    rel_path = os.path.relpath(abs_path, root_folder)
                    files[rel_path] = collect_metadata(abs_path, self.root_folder)
                    files[rel_path]["absolute_path"] = abs_path
                    files[rel_path]["lstat"] = json.loads(files[rel_path]["lstat"]) if files[rel_path]["lstat"] else {}
                for fname in filenames:
                    if fname not in {DB_NAME, SNAPSHOTS_DIR}:
                        abs_path = os.path.join(dirpath, fname)
                        rel_path = os.path.relpath(abs_path, root_folder)
                        files[rel_path] = collect_metadata(abs_path, self.root_folder)
                        files[rel_path]["absolute_path"] = abs_path
                        files[rel_path]["lstat"] = json.loads(files[rel_path]["lstat"]) if files[rel_path]["lstat"] else {}
            return files

        def get_version(chash):
            if chash == "latest":
                current_branch_name = self._get_latest_branch_name(conn)
                parent_commit_hash = self._get_tracked_commit_of_branch(conn, current_branch_name)
                row = conn.execute(
                    "SELECT id, commit_hash, root_tree_hash FROM versions "
                    "WHERE root_folder=? AND commit_hash=?",
                    (root_folder, parent_commit_hash)
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT id, commit_hash, root_tree_hash FROM versions "
                    "WHERE root_folder = ? AND commit_hash LIKE ?",
                    (root_folder, chash + "%")
                ).fetchone()
            if not row:
                conn.close()
                sys.exit(f"Commit '{chash}' not found.")
            return row

        def get_files(chash):
            vid = get_version(chash)
            return get_files_for_version(vid)

        def get_files_for_version(vid, only_paths: Optional[set[str]] = None):
            result = {}
            
            entries_in_commit = self._get_entries_in_commit(conn, vid["commit_hash"])
            entries = []
            for rel_path in entries_in_commit:
                if rel_path in (only_paths if only_paths is not None else entries_in_commit):
                    entries.append(collect_metadata(os.path.join(self.root_folder, rel_path), self.root_folder))

            result = {}
            for r in entries:
                rec = dict(r)
                rec["absolute_path"] = r["absolute_path"]
                rec["lstat"] = json.loads(r["lstat"]) if r["lstat"] else {}
                result[r["relative_path"]] = rec
            return result

        def get_merkle_nodes(version_id):
            rows = conn.execute(
                "SELECT relative_path, file_type, node_hash, metadata "
                "FROM merkle_nodes WHERE version_id=?",
                (version_id,),
            ).fetchall()
            return {r["relative_path"]: dict(r) for r in rows}

        def children_index(nodes):
            children = {path: [] for path in nodes}
            for path in nodes:
                if path == ".":
                    continue
                children.setdefault(parent_path(path), []).append(path)
            return children

        def add_subtree(changed_paths, path, children):
            if path != ".":
                changed_paths.add(path)
            for child_path in children.get(path, []):
                add_subtree(changed_paths, child_path, children)

        def changed_paths_from_merkle(v1, v2):
            nodes1 = get_merkle_nodes(v1["id"])
            nodes2 = get_merkle_nodes(v2["id"])
            children1 = children_index(nodes1)
            children2 = children_index(nodes2)
            changed_paths = set()

            def walk(path):
                n1 = nodes1.get(path)
                n2 = nodes2.get(path)
                if n1 and n2 and n1["node_hash"] == n2["node_hash"]:
                    return
                if n1 is None:
                    add_subtree(changed_paths, path, children2)
                    return
                if n2 is None:
                    add_subtree(changed_paths, path, children1)
                    return

                if path != ".":
                    if n1["file_type"] != n2["file_type"] or n1["metadata"] != n2["metadata"]:
                        changed_paths.add(path)
                    elif n1["file_type"] != "dir":
                        changed_paths.add(path)

                if n1["file_type"] == "dir" or n2["file_type"] == "dir":
                    child_paths = set(children1.get(path, [])) | set(children2.get(path, []))
                    for child_path in sorted(child_paths):
                        walk(child_path)

            walk(".")
            return changed_paths

        def unchanged_count_from_merkle(v1, v2):
            row = conn.execute(
                "SELECT COUNT(*) AS count "
                "FROM merkle_nodes a "
                "JOIN merkle_nodes b "
                "  ON a.relative_path = b.relative_path "
                " AND a.node_hash = b.node_hash "
                "WHERE a.version_id=? AND b.version_id=? AND a.relative_path <> '.'",
                (v1["id"], v2["id"]),
            ).fetchone()
            return row["count"] if row else 0

        def compare_lists(list_a, list_b):
            equal = []
            different = []
            added_to_a = []
            added_to_b = []
            i = 0
            j = 0

            while i < len(list_a) and j < len(list_b):
                if list_a[i] == list_b[j]:
                    equal.append((i, j))
                    i += 1
                    j += 1
                    continue
                try:
                    next_j = list_b.index(list_a[i], j + 1)
                except ValueError:
                    next_j = None
                try:
                    next_i = list_a.index(list_b[j], i + 1)
                except ValueError:
                    next_i = None

                if next_j is not None and (next_i is None or next_j - j <= next_i - i):
                    added_to_b.extend(range(j, next_j))
                    j = next_j
                elif next_i is not None:
                    added_to_a.extend(range(i, next_i))
                    i = next_i
                else:
                    different.append((i, j))
                    i += 1
                    j += 1
            added_to_a.extend(range(i, len(list_a)))
            added_to_b.extend(range(j, len(list_b)))
            return equal, different, added_to_a, added_to_b

        def compare_file_contents(commit1, file_entry1, commit2, file_entry2):
            tmpdir = tempfile.mkdtemp()
            first_file = file_entry1["absolute_path"]
            second_file = file_entry2["absolute_path"]
            current_branch_name = self._get_latest_branch_name(conn)
            f1_hash = f2_hash = None
            if commit1 == "working tree":
                commit1 = self._get_tracked_commit_of_branch(conn, current_branch_name)
            if commit1 != "latest":
                rows = conn.execute(
                    "SELECT content_hash_sha256 "
                    "FROM merkle_nodes, versions "
                    "WHERE merkle_nodes.version_id = versions.id AND versions.root_folder = ? AND versions.commit_hash LIKE ? "
                    "AND merkle_nodes.relative_path = ?",
                    (self.root_folder, commit1 + "%", file_entry1["relative_path"]),
                ).fetchone()
                f1_hash = rows['content_hash_sha256'] if rows else None
            
            if commit2 == "working tree":
                commit2 = self._get_tracked_commit_of_branch(conn, current_branch_name)
            if commit2 != "latest":
                rows = conn.execute(
                    "SELECT content_hash_sha256 "
                    "FROM merkle_nodes, versions "
                    "WHERE merkle_nodes.version_id = versions.id AND versions.root_folder = ? AND versions.commit_hash LIKE ? "
                    "AND merkle_nodes.relative_path = ?",
                    (self.root_folder, commit2 + "%", file_entry2["relative_path"]),
                ).fetchone()
                f2_hash = rows['content_hash_sha256'] if rows else None
            compare_latest = False
            if commit2 == "latest" and \
                rebuild_file_from_chunks(conn, os.path.join(self.root_folder, SNAPSHOTS_DIR), file_entry1["relative_path"], commit1, f1_hash, tmpdir) is True:
                first_file = os.path.join(tmpdir, file_entry1["relative_path"])
                compare_latest = True
            if commit1 == "latest" and \
                rebuild_file_from_chunks(conn, os.path.join(self.root_folder, SNAPSHOTS_DIR), file_entry2["relative_path"], commit2, f2_hash, tmpdir) is True:
                second_file = os.path.join(tmpdir, file_entry2["relative_path"])
                compare_latest = True
            
            if compare_latest is True:
                result = subprocess.run(['diff', first_file, second_file], capture_output=True, text=True)
                if result.returncode == 0:
                    shutil.rmtree(tmpdir)
                    return True
                print(f"diff result: {result.stdout.strip()}")
                shutil.rmtree(tmpdir)
                return False

            shutil.rmtree(tmpdir)
            if f1_hash == f2_hash:
                return True
            old_rows = conn.execute(
                "SELECT chunk_hash "
                "FROM chunk_store "
                "WHERE commit_hash LIKE ? AND relative_file_path = ? "
                "ORDER BY chunk_index;",
                (commit1 + "%", file_entry1["relative_path"])
            ).fetchall()

            new_rows = conn.execute(
                "SELECT chunk_hash "
                "FROM chunk_store "
                "WHERE commit_hash LIKE ? AND relative_file_path = ? "
                "ORDER BY chunk_index;",
                (commit2 + "%", file_entry2["relative_path"])
            ).fetchall()

            list_old = [row[0] for row in old_rows]
            list_new = [row[0] for row in new_rows]
            _, different, added_a, added_b = compare_lists(list_old, list_new)

            # print(different, added_a, added_b)
            for diff in different:
                first_file = os.path.join(self.root_folder, SNAPSHOTS_DIR, CHUNK_STORAGE_DIR, list_old[diff[0]])
                second_file = os.path.join(self.root_folder, SNAPSHOTS_DIR, CHUNK_STORAGE_DIR, list_new[diff[1]])
                result = subprocess.run(['diff', first_file, second_file], capture_output=True, text=True)
                print(f"diff result: {result.stdout.strip()}")
            if len(added_a) > 0:
                print(f"New content added in {commit1}")
            if len(added_b) > 0:
                print(f"New content added in {commit2}")
            return (len(different) + len(added_a) + len(added_b)) == 0

        files1 = files2 = {}
        unchanged = 0
        if c1 is None and c2 is None:
            files2 = get_files_in_root_folder()
            files1 = get_files("latest")
        elif c1 is None:
            files1 = get_files_in_root_folder()
            files2 = get_files(c2)
        elif c2 is None:
            files1 = get_files(c1)
            files2 = get_files_in_root_folder()
        else:
            version1 = get_version(c1)
            version2 = get_version(c2)
            changed_paths = changed_paths_from_merkle(version1, version2)
            unchanged = unchanged_count_from_merkle(version1, version2)
            files1 = get_files_for_version(version1, changed_paths)
            files2 = get_files_for_version(version2, changed_paths)

        all_paths = sorted(set(files1) | set(files2))
        added = deleted = modified = 0

        c1 = "working tree" if c1 is None else c1
        c2 = "latest" if c2 is None else c2
        print(f"Diff {c1} → {c2}  ({root_folder})\n")
        print(f"{'STATUS':<10} {'PATH'}")
        print("─" * 70)

        for p in all_paths:
            if p not in files1:
                print(f"{'ADDED':<10} {p:<40} in {c2}")
                added += 1
            elif p not in files2:
                print(f"{'DELETED':<10} {p:<40} in {c2}")
                deleted += 1
            else:
                f1, f2 = files1[p], files2[p]
                changes = []
                if f1["file_type"] != f2["file_type"]:
                    changes.append("type")
                if compare_file_contents(c1, f1, c2, f2) is False:
                    changes.append("content")
                if f1["permissions_int"] != f2["permissions_int"]:
                    changes.append("perms")
                if f1["owner_name"] != f2["owner_name"] or f1["group_name"] != f2["group_name"]:
                    changes.append("owner")
                if f1["lstat"].get("st_size") != f2["lstat"].get("st_size"):
                    changes.append("size")
                if changes:
                    print(f"{'MODIFIED':<10} {p}  [{', '.join(changes)}]")
                    modified += 1
                else:
                    unchanged += 1
        
        conn.close()
        print(f"\nSummary: +{added} added  -{deleted} deleted  ~{modified} modified  ={unchanged} unchanged")


    def cmd_restore(self, commit_hash: str):
        root_folder = os.path.abspath(self.root_folder)
        conn = open_db(root_folder)
        row = conn.execute(
            "SELECT commit_hash FROM versions "
            "WHERE root_folder = ? AND commit_hash LIKE ?",
            (root_folder, commit_hash + "%")
        ).fetchone()

        if not row:
            conn.close()
            sys.exit(f"Commit '{commit_hash}' not found.")

        full_hash = row["commit_hash"]
        print(f"Restoring {commit_hash} from chunk store → {root_folder}")
        result = materialize_commit_to_worktree(conn, full_hash, os.path.join(root_folder, SNAPSHOTS_DIR), root_folder)

        if result is False:
            conn.close()
            sys.exit(f"Failed to restore version: {commit_hash}")
        
        branch_name = "latest"
        row = conn.execute(
            "SELECT child_branch_name FROM branch_links "
            "WHERE child_commit_hash = ?",
            (full_hash,)
        ).fetchone()
        if row and row["child_branch_name"] is not None:
            branch_name = row["child_branch_name"]
            print(f"updating restoring {full_hash} to branch {branch_name}")
            conn.execute(
                "UPDATE branches SET is_latest=1, tracked_commit_hash=? WHERE branch_name=?",
                (full_hash, branch_name),
            )
            conn.commit()
        conn.close()
        print(f" Restored to {full_hash} in branch '{branch_name}'")

    def cmd_clone(self, source_repo_path: str, target_repo_path: str = None):
        """Clone a dsi-vcs repository from source to target."""

        if target_repo_path is None:
            target_repo_path = os.getcwd()
        source_repo_path = os.path.abspath(source_repo_path)
        target_repo_path = os.path.abspath(target_repo_path)

        if not os.path.isdir(source_repo_path):
            sys.exit(f"Source repository '{source_repo_path}' does not exist.")

        def safe_copytree(src, dst):
            errors = []
            for root, dirs, files in os.walk(src, onerror=lambda e: errors.append(str(e))):
                rel = os.path.relpath(root, src)
                target_dir = os.path.join(dst, rel) if rel != "." else dst
                try:
                    os.makedirs(target_dir, exist_ok=True)
                except PermissionError as e:
                    errors.append(f"{root}: {e}")
                    dirs[:] = []  # don't descend further into this branch
                    continue

                for f in files:
                    s = os.path.join(root, f)
                    d = os.path.join(target_dir, f)
                    try:
                        shutil.copy2(s, d)
                    except PermissionError as e:
                        errors.append(f"{s}: {e}")

            return errors

        errors = safe_copytree(source_repo_path, target_repo_path)
        if errors:
            print(f"{len(errors)} items skipped:")
            for e in errors:
                print(" ", e)

        # shutil.copytree(source_repo_path, target_repo_path, dirs_exist_ok=True, ignore_errors=True)

        root_folder = os.path.abspath(self.root_folder)
        conn = open_db(root_folder)
        conn.execute(
            "UPDATE versions SET root_folder=?",
            (target_repo_path,),
        )
        conn.execute(
            "UPDATE merkle_nodes SET root_folder=?",
            (target_repo_path,),
        )
        conn.execute(
            "UPDATE branches SET root_folder=?",
            (target_repo_path,),
        )
        conn.commit()
        c_hash = self._get_latest_commit_of_branch(conn, "main")
        conn.close()
        self.cmd_restore(c_hash)
        print(f"Cloned repository from '{source_repo_path}' to '{target_repo_path}'")

    def cmd_status(self):
        """Show current branch name, commit hash, and staged paths."""
        conn = open_db(self.root_folder)
        c_branch = self._get_latest_branch_name(conn)
        print("="*20)
        print(f"Branch: {c_branch}")
        latest_commit_hash = self._get_tracked_commit_of_branch(conn, c_branch)
        print(f"Commit: {latest_commit_hash[:12]}")
        print("="*20)
        self._print_staged_paths()