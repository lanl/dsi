#!/usr/bin/env python3
"""
dsi_vcs.py — rsync-based file version control system
Captures full Linux file metadata (stat, ACL, xattrs), MD5 hash,
and stores versioned snapshots in SQLite.

Usage:
    python dsi_vcs.py init   <root_folder>              # init repo
    python dsi_vcs.py commit <root_folder> [message]    # commit a new version
    python dsi_vcs.py log    <root_folder>              # list versions
    python dsi_vcs.py diff   <root_folder> <v1> <v2>   # diff two versions
    python dsi_vcs.py restore <root_folder> <version>  # restore a version

Requirements:
    pip install pyxattr                # for extended attributes
    sudo apt install acl               # for getfacl (ACL support)
    rsync must be installed            # for snapshot copies
"""

import os
import sys
import uuid
import subprocess
import json
import datetime
import argparse
from pathlib import Path
from typing import Optional

from vcs_db import *
from vcs_metadata_helper import *

# ─────────────────────────── RSYNC SNAPSHOT ──────────────────────────────────

def rsync_snapshot(
    root_folder: str,
    dest_path: str,
    prev_snapshot: Optional[str] = None,
    rel_paths: Optional[list[str]] = None,
) -> bool:
    """
    Copy files from root_folder → dest_path using rsync hard-link deduplication.
    If rel_paths is given, only those paths are synced (via --files-from).
    If prev_snapshot is provided, unchanged files are hard-linked (saves disk).
    """
    import tempfile

    os.makedirs(dest_path, exist_ok=True)
    cmd = ["rsync", "-aAXH"] # for linux
    if sys.platform == "darwin":
        cmd = ["rsync", "-aEH"] # for macOS

    if prev_snapshot and os.path.isdir(prev_snapshot):
        cmd += ["--link-dest", os.path.abspath(prev_snapshot)]

    # Write a temp file-list when only staging specific paths
    files_from_tmp = None
    if rel_paths is not None:
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False)
        tmp.write("\n".join(rel_paths))
        tmp.close()
        files_from_tmp = tmp.name
        cmd += ["--files-from", files_from_tmp]
    else:
        cmd += [
            "--delete",
            "--exclude", DB_NAME,
            "--exclude", SNAPSHOTS_DIR,
        ]

    cmd += [
        root_folder.rstrip("/") + "/",
        dest_path.rstrip("/") + "/",
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode not in (0, 24):   # 24 = some files vanished (ok)
            print(f"[rsync error] {result.stderr.strip()}", file=sys.stderr)
            return False
        if sys.platform == "darwin":
            for rel_path in rel_paths:
                set_acl(dest_path, get_acl(rel_path) or "")
        return True
    finally:
        if files_from_tmp:
            os.unlink(files_from_tmp)


# ─────────────────────────── COMMANDS ────────────────────────────────────────
class DSIVCS():

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
            # Reject paths outside the root
            try:
                os.path.relpath(abs_path, self.root_folder)
            except ValueError:
                print(f"  [skip] {abs_path}: outside root folder")
                return

            if not os.path.lexists(abs_path):
                print(f"  [skip] {abs_path}: path does not exist")
                return

            cur.execute(
                "INSERT OR IGNORE INTO staging (root_folder, absolute_path, added_at) "
                "VALUES (?, ?, ?)",
                (self.root_folder, abs_path, added_at)
            )
            if cur.rowcount:
                staged += 1

        for raw in paths:
            abs_path = os.path.join(self.root_folder, raw)

            if os.path.isdir(abs_path):
                # Expand directory recursively
                for dirpath, dirnames, filenames in os.walk(abs_path, followlinks=False):
                    dirnames[:] = [d for d in dirnames if d not in skip_names]
                    for fname in filenames:
                        if fname in skip_names:
                            continue
                        stage_path(os.path.join(dirpath, fname))
            else:
                stage_path(abs_path)

        conn.commit()
        conn.close()
        print(f"  {staged} file(s) added to staging.")
        """Show files currently in the staging area."""
        root_folder = os.path.abspath(self.root_folder)
        conn = open_db(root_folder)
        rows = conn.execute(
            "SELECT absolute_path, added_at FROM staging "
            "WHERE root_folder=? ORDER BY absolute_path",
            (root_folder,)
        ).fetchall()
        conn.close()

        if not rows:
            print("Nothing staged. Use 'add <root_folder> <path>...' to stage files.")
            return

        print(f"Staged files ({len(rows)}):")
        for r in rows:
            rel = os.path.relpath(r["absolute_path"], root_folder)
            print(f"  {rel}")
        
    def cmd_remove(self, paths: list[str]):
        """Remove file(s) from the staging area without touching the actual files."""
        root_folder = os.path.abspath(self.root_folder)
        db_path = os.path.join(self.root_folder, SNAPSHOTS_DIR, DB_NAME)
        if not os.path.isfile(db_path):
            sys.exit("No dsi-vcs repo found. Run 'init' first.")

        conn = open_db(self.root_folder)
        cur  = conn.cursor()
        removed = 0

        for raw in paths:
            abs_path = os.path.abspath(raw)
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
        print(f"  {removed} file(s) removed from staging.")
        """Show files currently in the staging area."""
        root_folder = os.path.abspath(root_folder)
        conn = open_db(root_folder)
        rows = conn.execute(
            "SELECT absolute_path, added_at FROM staging "
            "WHERE root_folder=? ORDER BY absolute_path",
            (root_folder,)
        ).fetchall()
        conn.close()

        if not rows:
            print("Nothing staged. Use 'add <root_folder> <path>...' to stage files.")
            return

        print(f"Staged files ({len(rows)}):")
        for r in rows:
            rel = os.path.relpath(r["absolute_path"], root_folder)
            print(f"  {rel}")

    def cmd_commit(self, message: str = ""):
        db_path = os.path.join(self.root_folder, SNAPSHOTS_DIR, DB_NAME)
        if not os.path.isfile(db_path):
            sys.exit("No dsi-vcs repo found. Run 'init' first.")

        conn = open_db(self.root_folder)
        cur = conn.cursor()

        # ── Load staged paths ────────────────────────────────────────────────────
        staged_rows = cur.execute(
            "SELECT absolute_path FROM staging WHERE root_folder = ? ORDER BY absolute_path",
            (self.root_folder,)
        ).fetchall()

        if not staged_rows:
            conn.close()
            sys.exit("Nothing staged. Use 'add' to stage files before committing.")

        staged_paths = [r["absolute_path"] for r in staged_rows]

        # ── Previous snapshot for hard-link deduplication ────────────────────────
        prev_row = cur.execute(
            "SELECT snapshot_path FROM versions "
            "WHERE root_folder=? ORDER BY id DESC LIMIT 1",
            (self.root_folder,)
        ).fetchone()
        prev_snapshot = prev_row["snapshot_path"] if prev_row else None

        committed_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        commit_hash  = uuid.uuid4().hex
        running_user = owner_name(os.getuid())
        snapshot_path = os.path.join(self.root_folder, SNAPSHOTS_DIR, commit_hash[:12])

        # ── Collect metadata for each staged file ───────────────────────────────
        print(f"Collecting metadata for {len(staged_paths)} staged file(s)…")
        entries = []
        for abs_path in staged_paths:
            e = collect_metadata(abs_path, self.root_folder)
            if "error" in e:
                print(f"  [skip] {e['relative_path']}: {e['error']}")
            else:
                entries.append(e)

        if not entries:
            conn.close()
            sys.exit("No readable staged files — commit aborted.")

        total_bytes = sum(e.get("_st_size") or 0 for e in entries if e.get("file_type") == "file")
        file_count  = sum(1 for e in entries if e.get("file_type") == "file")

        # ── rsync only the staged relative paths ────────────────────────────────
        rel_paths = [e["relative_path"] for e in entries]
        print(f"  {file_count} file(s), {total_bytes:,} bytes")
        print(f"  Creating rsync snapshot → {snapshot_path}")

        ok = rsync_snapshot(self.root_folder, snapshot_path, prev_snapshot, rel_paths)
        if not ok:
            conn.close()
            sys.exit("rsync failed — commit aborted.")

        # ── Insert version row ───────────────────────────────────────────────────
        cur.execute(
            """INSERT INTO versions
            (root_folder, commit_hash, committed_at, owner_name, message,
                snapshot_path, file_count, total_bytes)
            VALUES (?,?,?,?,?,?,?,?)""",
            (self.root_folder, commit_hash, committed_at, running_user, message,
            snapshot_path, file_count, total_bytes)
        )
        version_id = cur.lastrowid

        # ── Bulk-insert file entries ─────────────────────────────────────────────
        cols = [
            "version_id", "root_folder", "relative_path", "absolute_path",
            "file_name", "file_type", "md5_hash",
            "lstat",
            "permissions_octal", "permissions_str", "owner_name", "group_name",
            "setuid", "setgid", "sticky",
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

        print(f"{'COMMIT HASH':<34} {'OWNER':<16} {'DATE/TIME (UTC)':<28} {'FILES':>7} {'BYTES':>15}  MESSAGE")
        print("─" * 100)
        for r in rows:
            msg = (r["message"] or "")[:35]
            print(f"{r['commit_hash']:<34} {r['owner_name']:<16} {r['committed_at']:<28}"
                f"{r['file_count']:>7} {r['total_bytes']:>15,}  {msg}")


    def cmd_diff(self, c1: str, c2: str):
        root_folder = os.path.abspath(self.root_folder)
        conn = open_db(root_folder)

        def get_files(chash):
            vid = conn.execute(
                "SELECT id FROM versions WHERE root_folder = ? AND commit_hash LIKE ?",
                (root_folder, chash + "%")
            ).fetchone()
            if not vid:
                conn.close()
                sys.exit(f"Commit '{chash}' not found.")
            rows = conn.execute(
                "SELECT relative_path, file_type, md5_hash, permissions_octal, "
                "       owner_name, group_name, lstat "
                "FROM file_entries WHERE version_id=?", (vid["id"],)
            ).fetchall()
            result = {}
            for r in rows:
                rec = dict(r)
                # Unpack the lstat JSON so callers can access st_size, st_mtime, etc.
                rec["lstat"] = json.loads(r["lstat"]) if r["lstat"] else {}
                result[r["relative_path"]] = rec
            return result

        files1 = get_files(c1)
        files2 = get_files(c2)
        conn.close()

        all_paths = sorted(set(files1) | set(files2))
        added = deleted = modified = unchanged = 0

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
                if f1["md5_hash"] != f2["md5_hash"] and f1["md5_hash"]:
                    changes.append("content")
                if f1["permissions_octal"] != f2["permissions_octal"]:
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


# ─────────────────────────── CLI ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="dsi_vcs — rsync-based file version control with full Linux metadata"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser("init", help="Initialize a new repo")
    # p_init.add_argument("root_folder")

    p_add = sub.add_parser("add", help="add file or directory to staging")
    p_add.add_argument('files', nargs='+', type=str)

    p_remove = sub.add_parser("remove", help="remove file or directory from staging")
    p_remove.add_argument('files', nargs='+', type=str)

    p_commit = sub.add_parser("commit", help="Snapshot and commit staged files")
    p_commit.add_argument("message", nargs="?", default="Committed at " + datetime.datetime.now().isoformat())

    p_log = sub.add_parser("log", help="List all commits")

    p_diff = sub.add_parser("diff", help="Diff two commits")
    p_diff.add_argument("c1")
    p_diff.add_argument("c2")

    p_restore = sub.add_parser("restore", help="Restore a version")
    p_restore.add_argument("root_folder")
    p_restore.add_argument("version")

    args = parser.parse_args(args=None if sys.argv[1:] else ["-h"])

    # parser.print_help()
    # parser.format_help()
    if   args.command == "init":
        vcs = DSIVCS(os.getcwd())
    elif args.command == "add":
        vcs = DSIVCS(os.getcwd())
        vcs.cmd_add(args.files)
    elif args.command == "remove":
        vcs = DSIVCS(os.getcwd())
        vcs.cmd_remove(args.files)
    elif args.command == "commit":
        vcs = DSIVCS(os.getcwd())
        vcs.cmd_commit(args.message)
    elif args.command == "diff":
        vcs = DSIVCS(os.getcwd())
        vcs.cmd_diff(args.c1, args.c2)
    elif args.command == "log":
        vcs = DSIVCS(os.getcwd())
        vcs.cmd_log()
    # elif args.command == "diff":    cmd_diff(args.root_folder, args.v1, args.v2)
    # elif args.command == "restore": cmd_restore(args.root_folder, args.version)

if __name__ == "__main__":
    # print("\n=== dsi-vcs: rsync-based file version control ===\n")
    # print(os.getcwd())
    main()