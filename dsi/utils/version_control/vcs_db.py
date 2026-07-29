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
    commit_hash     TEXT    NOT NULL,           -- SHA-256 Merkle commit hash
    root_tree_hash  TEXT    NOT NULL,
    hash_algorithm  TEXT    NOT NULL,
    committed_at    INTEGER NOT NULL,           -- UTC timestamp (seconds since epoch)
    owner_name      TEXT    NOT NULL,           -- Username of the committer
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
    permissions_int     INTEGER,                -- e.g. "755" as an integer
    owner_name      TEXT,
    group_name      TEXT,

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

CREATE TABLE IF NOT EXISTS merkle_nodes (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id          INTEGER NOT NULL REFERENCES versions(id) ON DELETE CASCADE,
    root_folder         TEXT    NOT NULL,
    relative_path       TEXT    NOT NULL,
    file_type           TEXT    NOT NULL,
    node_hash           TEXT    NOT NULL,
    metadata_hash       TEXT    NOT NULL,
    content_hash_sha256 TEXT,
    subtree_file_count  INTEGER NOT NULL,
    subtree_total_bytes INTEGER NOT NULL,
    child_count         INTEGER NOT NULL,
    UNIQUE(version_id, relative_path)
);

CREATE INDEX IF NOT EXISTS idx_merkle_nodes_root_path
    ON merkle_nodes(root_folder, version_id, relative_path);

CREATE INDEX IF NOT EXISTS idx_merkle_nodes_hash
    ON merkle_nodes(root_folder, node_hash);

CREATE TABLE IF NOT EXISTS branches (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    root_folder     TEXT    NOT NULL,
    branch_name     TEXT    NOT NULL,
    head_commit_hash TEXT    NOT NULL,
    is_latest       INTEGER NOT NULL DEFAULT 0,
    created_at      INTEGER NOT NULL,
    UNIQUE(root_folder, branch_name)
);

CREATE TABLE IF NOT EXISTS branch_links (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_commit_hash  TEXT    DEFAULT NULL,
    child_commit_hash   TEXT    NOT NULL,
    child_branch_name   TEXT    NOT NULL,
    created_at          INTEGER NOT NULL,
    UNIQUE(parent_commit_hash, child_commit_hash, child_branch_name)
);

CREATE TABLE IF NOT EXISTS chunk_store (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    chunk_hash           TEXT    NOT NULL UNIQUE,
    chunk_size           INTEGER NOT NULL,
    created_at           INTEGER NOT NULL,
    commit_hash          TEXT NOT NULL,
    relative_file_path   TEXT NOT NULL,
    chunk_index          INTEGER NOT NULL
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
