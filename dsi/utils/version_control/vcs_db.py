import sqlite3
import os

# ─────────────────────────── CONFIG ──────────────────────────────────────────

DB_NAME = ".dsi_vcs.db"           # SQLite DB stored inside the root folder
SNAPSHOTS_DIR = ".dsi_vcs_snapshots"  # rsync snapshot copies live here

# ─────────────────────────── DATABASE ────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS versions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    root_folder     TEXT    NOT NULL,
    commit_hash     TEXT    NOT NULL,           -- UUID4, stored as 32-char hex
    committed_at    TEXT    NOT NULL,           -- ISO-8601 timestamp
    owner_name      TEXT    NOT NULL,           -- Linux user who ran the commit
    message         TEXT,
    snapshot_path   TEXT    NOT NULL,           -- path to rsync copy
    file_count      INTEGER NOT NULL,
    total_bytes     INTEGER NOT NULL,
    UNIQUE(root_folder, commit_hash)
);

CREATE TABLE IF NOT EXISTS file_entries (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id      INTEGER NOT NULL REFERENCES versions(id) ON DELETE CASCADE,
    root_folder     TEXT    NOT NULL,           -- partition key / lookup key
    relative_path   TEXT    NOT NULL,           -- path relative to root
    absolute_path   TEXT    NOT NULL,

    -- ── Identity ──────────────────────────────────────────────────────────
    file_name       TEXT    NOT NULL,
    file_type       TEXT    NOT NULL,           -- file/dir/symlink/block/char/fifo/socket

    -- ── Content hash ──────────────────────────────────────────────────────
    md5_hash        TEXT,                       -- NULL for non-regular files

    -- ── lstat(2) — all raw os.lstat() fields packed as a JSON object ──────
    -- Keys: st_mode, st_ino, st_dev, st_nlink, st_uid, st_gid, st_size,
    --       st_atime, st_mtime, st_ctime, st_blocks, st_blksize
    lstat           TEXT    NOT NULL,

    -- ── Human-readable permission strings ─────────────────────────────────
    permissions_octal   TEXT,                   -- e.g. "0o755"
    permissions_str     TEXT,                   -- e.g. "rwxr-xr-x"
    owner_name      TEXT,
    group_name      TEXT,

    -- ── Special bits ──────────────────────────────────────────────────────
    setuid          INTEGER DEFAULT 0,
    setgid          INTEGER DEFAULT 0,
    sticky          INTEGER DEFAULT 0,

    -- ── ACL (POSIX) ───────────────────────────────────────────────────────
    acl_text        TEXT,                       -- raw getfacl output

    -- ── Extended attributes ───────────────────────────────────────────────
    xattrs          TEXT,                       -- JSON dict of xattr key→value

    -- ── Symlink target ───────────────────────────────────────────────────
    symlink_target  TEXT,

    -- ── SELinux / AppArmor context ────────────────────────────────────────
    security_context TEXT
);

CREATE INDEX IF NOT EXISTS idx_file_entries_root
    ON file_entries(root_folder, version_id);

CREATE INDEX IF NOT EXISTS idx_file_entries_path
    ON file_entries(root_folder, relative_path);

CREATE TABLE IF NOT EXISTS staging (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    root_folder     TEXT    NOT NULL,
    absolute_path   TEXT    NOT NULL UNIQUE,            -- absolute path must be unique in staging
    action          TEXT    NOT NULL DEFAULT 'add',
    added_at        TEXT    NOT NULL,                   -- ISO-8601 timestamp
    UNIQUE(root_folder, absolute_path)
);
"""


def open_db(root_folder: str) -> sqlite3.Connection:
    snaps = os.path.join(root_folder, SNAPSHOTS_DIR)
    os.makedirs(snaps, exist_ok=True)
    db_path = os.path.join(snaps, DB_NAME)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.executescript(SCHEMA)
    conn.commit()
    return conn
