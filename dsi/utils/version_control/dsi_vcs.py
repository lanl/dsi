#!/usr/bin/env python3
"""
dsi_vcs.py — rsync-based file version control system
Captures full Linux file metadata (stat, ACL, xattrs), MD5 hash,
and stores versioned snapshots in SQLite.

Usage:
    python dsi_vcs.py init                 # init repo in current directory
    python dsi_vcs.py add <path>...        # stage paths for the next commit
    python dsi_vcs.py delete <path>...     # stage paths for deletion
    python dsi_vcs.py remove <path>...     # unstage paths
    python dsi_vcs.py commit [message]     # commit a new version
    python dsi_vcs.py log                  # list versions
    python dsi_vcs.py diff <v1> <v2>       # diff two versions
    python dsi_vcs.py restore <version>    # restore a version

Requirements:
    pip install pyxattr                # for extended attributes
    sudo apt install acl               # for getfacl (ACL support)
    rsync must be installed            # for snapshot copies
"""

import os
import sys
import subprocess
import json
import datetime
import shutil
import tempfile
from typing import Optional

from .vcs_db import DB_NAME, SNAPSHOTS_DIR, open_db
from .vcs_metadata_helper import collect_metadata, collect_tree_metadata, owner_name
from .merkle import HASH_ALGORITHM, build_merkle_tree, commit_hash as merkle_commit_hash, parent_path

# ─────────────────────────── RSYNC SNAPSHOT ──────────────────────────────────

def rsync_snapshot(
    root_folder: str,
    dest_path: str,
    prev_snapshot: Optional[str] = None,
) -> bool:
    """
    Copy the full root_folder tree to dest_path using rsync hard-link deduplication.
    If prev_snapshot is provided, unchanged files are hard-linked (saves disk).
    """
    os.makedirs(dest_path, exist_ok=True)
    cmd = ["rsync", "-aAXH"] # for linux
    if sys.platform == "darwin":
        cmd = ["rsync", "-aEH"] # for macOS

    if prev_snapshot and os.path.isdir(prev_snapshot):
        cmd += ["--link-dest", os.path.abspath(prev_snapshot)]

    cmd += [
        "--delete",
        "--exclude", DB_NAME,
        "--exclude", SNAPSHOTS_DIR,
    ]

    cmd += [
        root_folder.rstrip("/") + "/",
        dest_path.rstrip("/") + "/",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode not in (0, 24):   # 24 = some files vanished (ok)
        print(f"[rsync error] {result.stderr.strip()}", file=sys.stderr)
        return False
    return True


def snapshot_target(snapshot_path: str, relative_path: str) -> str:
    target = os.path.abspath(os.path.join(snapshot_path, relative_path))
    snapshot_root = os.path.abspath(snapshot_path)
    if os.path.commonpath([snapshot_root, target]) != snapshot_root:
        raise ValueError(f"Snapshot path escapes snapshot root: {relative_path}")
    return target


def apply_snapshot_deletes(snapshot_path: str, root_folder: str, staged_deletes: list[str]) -> None:
    for abs_path in staged_deletes:
        rel_path = os.path.relpath(abs_path, root_folder)
        if rel_path in ("", "."):
            raise ValueError("Refusing to stage repository root for deletion.")
        target = snapshot_target(snapshot_path, rel_path)
        if os.path.isdir(target) and not os.path.islink(target):
            shutil.rmtree(target)
        elif os.path.lexists(target):
            os.unlink(target)


# ─────────────────────────── COMMANDS ────────────────────────────────────────
class Version():

    def __init__(self, folder: str):
        self.root_folder = os.path.abspath(folder)
        if not os.path.isdir(self.root_folder):
            sys.exit(f"Error: '{self.root_folder}' is not a directory.")

        conn = open_db(self.root_folder)
        conn.close()
        print(f"Initialized dsi-vcs repository in: {self.root_folder}")
        print(f"  Snapshots: {self.root_folder}/{SNAPSHOTS_DIR}/")


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

        conn = open_db(self.root_folder)
        cur  = conn.cursor()
        added_at   = datetime.datetime.now(datetime.timezone.utc).isoformat()
        staged     = 0
        skip_names = {DB_NAME, SNAPSHOTS_DIR}

        def stage_path(abs_path: str):
            nonlocal staged
            abs_path = os.path.abspath(abs_path)

            if not os.path.lexists(abs_path):
                print(f"  [skip] {abs_path}: path does not exist")
                return

            cur.execute(
                "INSERT OR REPLACE INTO staging (root_folder, absolute_path, action, added_at) "
                "VALUES (?, ?, ?, ?)",
                (self.root_folder, abs_path, "add", added_at)
            )
            if cur.rowcount:
                staged += 1

        for raw in paths:
            abs_path = os.path.abspath(raw if os.path.isabs(raw) else os.path.join(self.root_folder, raw))

            if os.path.isdir(abs_path):
                # Expand directory recursively
                for dirpath, dirnames, filenames in os.walk(abs_path, followlinks=False):
                    dirnames[:] = [d for d in dirnames if d not in skip_names]
                    stage_path(dirpath)
                    for fname in filenames:
                        if fname in skip_names:
                            continue
                        stage_path(os.path.join(dirpath, fname))
            else:
                stage_path(abs_path)

        conn.commit()
        conn.close()
        print(f"  {staged} path(s) added to staging.")
        """Show files currently in the staging area."""
        root_folder = os.path.abspath(self.root_folder)
        conn = open_db(root_folder)
        rows = conn.execute(
            "SELECT absolute_path, action, added_at FROM staging "
            "WHERE root_folder=? ORDER BY absolute_path",
            (root_folder,)
        ).fetchall()
        conn.close()

        if not rows:
            print("Nothing staged. Use 'add <path>...' or 'delete <path>...' to stage paths.")
            return

        print(f"Staged paths ({len(rows)}):")
        for r in rows:
            rel = os.path.relpath(r["absolute_path"], root_folder)
            print(f"  {rel} [{r['action']}]")

    def cmd_delete(self, paths: list[str]):
        """Stage path(s) for deletion in the next commit."""
        db_path = os.path.join(self.root_folder, SNAPSHOTS_DIR, DB_NAME)
        if not os.path.isfile(db_path):
            sys.exit("No dsi-vcs repo found. Run 'init' first.")

        conn = open_db(self.root_folder)
        cur = conn.cursor()
        added_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        staged = 0

        for raw in paths:
            abs_path = os.path.abspath(raw if os.path.isabs(raw) else os.path.join(self.root_folder, raw))
            cur.execute(
                "INSERT OR REPLACE INTO staging (root_folder, absolute_path, action, added_at) "
                "VALUES (?, ?, ?, ?)",
                (self.root_folder, abs_path, "delete", added_at)
            )
            if cur.rowcount:
                staged += 1

        conn.commit()
        conn.close()
        print(f"  {staged} path(s) staged for deletion.")
        """Show files currently in the staging area."""
        root_folder = os.path.abspath(self.root_folder)
        conn = open_db(root_folder)
        rows = conn.execute(
            "SELECT absolute_path, action, added_at FROM staging "
            "WHERE root_folder=? ORDER BY absolute_path",
            (root_folder,)
        ).fetchall()
        conn.close()

        if not rows:
            print("Nothing staged. Use 'add <path>...' or 'delete <path>...' to stage paths.")
            return

        print(f"Staged paths ({len(rows)}):")
        for r in rows:
            rel = os.path.relpath(r["absolute_path"], root_folder)
            print(f"  {rel} [{r['action']}]")

    def cmd_remove(self, paths: list[str]):
        """Remove path(s) from the staging area without touching the actual files."""
        root_folder = os.path.abspath(self.root_folder)
        db_path = os.path.join(self.root_folder, SNAPSHOTS_DIR, DB_NAME)
        if not os.path.isfile(db_path):
            sys.exit("No dsi-vcs repo found. Run 'init' first.")

        conn = open_db(self.root_folder)
        cur  = conn.cursor()
        removed = 0

        for raw in paths:
            abs_path = os.path.abspath(raw if os.path.isabs(raw) else os.path.join(root_folder, raw))
            cur.execute(
                "DELETE FROM staging WHERE root_folder=? AND absolute_path=?",
                (root_folder, abs_path)
            )
            if cur.rowcount:
                rel = os.path.relpath(abs_path, root_folder)
                print(f"  Unstaged: {rel}")
                removed += 1
            else:
                rel = os.path.relpath(abs_path, root_folder)
                print(f"  [not staged] {rel}")

        conn.commit()
        conn.close()
        print(f"  {removed} path(s) removed from staging.")
        """Show files currently in the staging area."""
        root_folder = os.path.abspath(root_folder)
        conn = open_db(root_folder)
        rows = conn.execute(
            "SELECT absolute_path, action, added_at FROM staging "
            "WHERE root_folder=? ORDER BY absolute_path",
            (root_folder,)
        ).fetchall()
        conn.close()

        if not rows:
            print("Nothing staged. Use 'add <path>...' or 'delete <path>...' to stage paths.")
            return

        print(f"Staged paths ({len(rows)}):")
        for r in rows:
            rel = os.path.relpath(r["absolute_path"], root_folder)
            print(f"  {rel} [{r['action']}]")

    def cmd_commit(self, message: str = ""):
        db_path = os.path.join(self.root_folder, SNAPSHOTS_DIR, DB_NAME)
        if not os.path.isfile(db_path):
            sys.exit("No dsi-vcs repo found. Run 'init' first.")

        conn = open_db(self.root_folder)
        cur = conn.cursor()

        # ── Load staged paths ────────────────────────────────────────────────────
        staged_rows = cur.execute(
            "SELECT absolute_path, action FROM staging WHERE root_folder = ? ORDER BY absolute_path",
            (self.root_folder,)
        ).fetchall()

        if not staged_rows:
            conn.close()
            sys.exit("Nothing staged. Use 'add' or 'delete' before committing.")

        staged_adds = [r["absolute_path"] for r in staged_rows if r["action"] == "add"]
        staged_deletes = [r["absolute_path"] for r in staged_rows if r["action"] == "delete"]

        # ── Previous snapshot for hard-link deduplication ────────────────────────
        prev_row = cur.execute(
            "SELECT commit_hash, snapshot_path FROM versions "
            "WHERE root_folder=? ORDER BY id DESC LIMIT 1",
            (self.root_folder,)
        ).fetchone()
        prev_snapshot = prev_row["snapshot_path"] if prev_row else None
        parent_commit_hash = prev_row["commit_hash"] if prev_row else None

        committed_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        running_user = owner_name(os.getuid())
        snapshots_root = os.path.join(self.root_folder, SNAPSHOTS_DIR)

        # ── Validate staged paths before creating the snapshot ─────────────────
        print(f"Validating {len(staged_rows)} staged path(s)…")
        valid_staged = len(staged_deletes)
        for abs_path in staged_adds:
            e = collect_metadata(abs_path, self.root_folder)
            if "error" in e:
                print(f"  [skip] {e['relative_path']}: {e['error']}")
            else:
                valid_staged += 1

        if valid_staged == 0:
            conn.close()
            sys.exit("No readable staged paths — commit aborted.")

        # ── Create a complete snapshot of the current repository tree ───────────
        tmp_snapshot_path = tempfile.mkdtemp(prefix=".tmp-", dir=snapshots_root)
        print(f"  Creating rsync snapshot → {tmp_snapshot_path}")

        ok = rsync_snapshot(self.root_folder, tmp_snapshot_path, prev_snapshot)
        if not ok:
            conn.close()
            shutil.rmtree(tmp_snapshot_path, ignore_errors=True)
            sys.exit("rsync failed — commit aborted.")

        try:
            apply_snapshot_deletes(tmp_snapshot_path, self.root_folder, staged_deletes)
        except ValueError as e:
            conn.close()
            shutil.rmtree(tmp_snapshot_path, ignore_errors=True)
            sys.exit(str(e))

        # ── Collect metadata for the complete committed tree ───────────────────
        entries = collect_tree_metadata(tmp_snapshot_path, self.root_folder)

        total_bytes = sum(e.get("_st_size") or 0 for e in entries if e.get("file_type") == "file")
        file_count  = sum(1 for e in entries if e.get("file_type") == "file")
        print(f"  {file_count} file(s), {total_bytes:,} bytes")

        root_tree_hash, merkle_nodes = build_merkle_tree(entries, tmp_snapshot_path)
        commit_hash = merkle_commit_hash(
            root_tree_hash=root_tree_hash,
            parent_commit_hash=parent_commit_hash,
            committed_at=committed_at,
            owner_name=running_user,
            message=message,
            file_count=file_count,
            total_bytes=total_bytes,
        )
        snapshot_path = os.path.join(snapshots_root, commit_hash[:12])
        if os.path.exists(snapshot_path):
            conn.close()
            shutil.rmtree(tmp_snapshot_path, ignore_errors=True)
            sys.exit(f"Snapshot path already exists for commit prefix: {snapshot_path}")
        os.rename(tmp_snapshot_path, snapshot_path)

        # ── Insert version row ───────────────────────────────────────────────────
        cur.execute(
            """INSERT INTO versions
            (root_folder, commit_hash, root_tree_hash, parent_commit_hash, hash_algorithm,
                committed_at, owner_name, message, snapshot_path, file_count, total_bytes)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (self.root_folder, commit_hash, root_tree_hash, parent_commit_hash, HASH_ALGORITHM,
            committed_at, running_user, message, snapshot_path, file_count, total_bytes)
        )
        version_id = cur.lastrowid

        # ── Bulk-insert file entries ─────────────────────────────────────────────
        cols = [
            "version_id", "root_folder", "relative_path", "absolute_path",
            "file_name", "file_type", "md5_hash",
            "lstat",
            "permissions_int", "owner_name", "group_name",
            "acl_text", "xattrs", "security_context", "symlink_target",
        ]
        placeholders = ",".join("?" * len(cols))
        col_str      = ",".join(cols)

        cur.executemany(
            f"INSERT INTO file_entries ({col_str}) VALUES ({placeholders})",
            [
                tuple(
                    version_id  if c == "version_id"  else
                    self.root_folder if c == "root_folder" else
                    e.get(c)
                    for c in cols
                )
                for e in entries
            ]
        )

        merkle_cols = [
            "version_id", "root_folder", "relative_path", "file_type",
            "node_hash", "metadata_hash", "content_hash_sha256",
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

        # ── Clear staging after a successful commit ──────────────────────────────
        cur.execute("DELETE FROM staging WHERE root_folder=?", (self.root_folder,))

        conn.commit()
        conn.close()

        print(f"\n Committed {commit_hash} at {committed_at}")
        print(f"  Short hash : {commit_hash[:12]}")
        print(f"  Owner      : {running_user}")
        if message:
            print(f"  Message    : {message}")


    def cmd_log(self):
        root_folder = os.path.abspath(self.root_folder)
        conn = open_db(root_folder)
        rows = conn.execute(
            "SELECT commit_hash, committed_at, owner_name, message, file_count, total_bytes "
            "FROM versions WHERE root_folder=? ORDER BY id",
            (root_folder,)
        ).fetchall()
        conn.close()

        if not rows:
            print("No versions yet. Run 'commit' first.")
            return

        print(f"{'COMMIT HASH':<66} {'OWNER':<16} {'DATE/TIME (UTC)':<28} {'FILES':>7} {'BYTES':>15}  MESSAGE")
        print("─" * 132)
        for r in rows:
            msg = (r["message"] or "")[:35]
            print(f"{r['commit_hash']:<66} {r['owner_name']:<16} {r['committed_at']:<28}"
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
                row = conn.execute(
                    "SELECT id, commit_hash, snapshot_path, root_tree_hash FROM versions "
                    "WHERE root_folder=? ORDER BY id DESC LIMIT 1",
                    (root_folder,)
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT id, commit_hash, snapshot_path, root_tree_hash FROM versions "
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

        def get_files_for_version(version, only_paths: Optional[set[str]] = None):
            path_filter = None if only_paths is None else sorted(only_paths)
            if path_filter == []:
                return {}

            rows = []
            base_sql = (
                "SELECT relative_path, absolute_path, file_type, md5_hash, permissions_int, "
                "       owner_name, group_name, lstat "
                "FROM file_entries WHERE version_id=?"
            )
            if path_filter is None:
                rows = conn.execute(base_sql, (version["id"],)).fetchall()
            else:
                for i in range(0, len(path_filter), 500):
                    chunk = path_filter[i:i + 500]
                    placeholders = ",".join("?" * len(chunk))
                    rows.extend(
                        conn.execute(
                            f"{base_sql} AND relative_path IN ({placeholders})",
                            (version["id"], *chunk),
                        ).fetchall()
                    )

            snapshot_path = version["snapshot_path"]
            result = {}
            for r in rows:
                rec = dict(r)
                # Unpack the lstat JSON so callers can access st_size, st_mtime, etc.
                rec["absolute_path"] = snapshot_target(snapshot_path, r["relative_path"])
                rec["lstat"] = json.loads(r["lstat"]) if r["lstat"] else {}
                result[r["relative_path"]] = rec
            return result

        def get_merkle_nodes(version_id):
            rows = conn.execute(
                "SELECT relative_path, file_type, node_hash, metadata_hash "
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
                    if n1["file_type"] != n2["file_type"] or n1["metadata_hash"] != n2["metadata_hash"]:
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

        files1 = files2 = {}
        unchanged = 0
        if c1 is None and c2 is None:
            files1 = get_files_in_root_folder()
            files2 = get_files("latest")
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
        conn.close()

        all_paths = sorted(set(files1) | set(files2))
        added = deleted = modified = 0

        print(f"Diff {c1} → {c2}  ({root_folder})\n")
        print(f"{'STATUS':<10} {'PATH'}")
        print("─" * 70)

        for p in all_paths:
            # if p in files1 and p not in files2:
            #     print(f"{'DELETED':<10} {p:<40} in {c2}")
            #     deleted += 1
            # elif p in files2 and p not in files1:
            #     print(f"{'ADDED':<10} {p:<40} in {c2}")
            #     added += 1
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
                if f1["md5_hash"] != f2["md5_hash"]:
                    if f1["md5_hash"] or f2["md5_hash"]:
                        changes.append("content")
                    if f1["md5_hash"] and f2["md5_hash"]:
                        result = subprocess.run(['diff', f1["absolute_path"], f2["absolute_path"]], capture_output=True, text=True)
                        print(f"diff result: {result.stdout.strip()}")
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

        print(f"\nSummary: +{added} added  -{deleted} deleted  ~{modified} modified  ={unchanged} unchanged")


    def cmd_restore(self, commit_hash: str):
        root_folder = os.path.abspath(self.root_folder)
        conn = open_db(root_folder)
        row = conn.execute(
            "SELECT commit_hash, snapshot_path FROM versions "
            "WHERE root_folder = ? AND commit_hash LIKE ?",
            (root_folder, commit_hash + "%")
        ).fetchone()
        conn.close()

        if not row:
            sys.exit(f"Commit '{commit_hash}' not found.")

        full_hash = row["commit_hash"]
        snapshot = row["snapshot_path"]
        if not os.path.isdir(snapshot):
            sys.exit(f"Snapshot directory missing: {snapshot}")

        print(f"Restoring {commit_hash} from {snapshot} → {root_folder}")
        cmd = [
            "rsync", "-aAXH", "--delete",
            "--exclude", DB_NAME,
            "--exclude", SNAPSHOTS_DIR,
            snapshot.rstrip("/") + "/",
            root_folder.rstrip("/") + "/",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode not in (0, 24):
            sys.exit(f"rsync restore failed:\n{result.stderr}")
        print(f" Restored to {full_hash}")
