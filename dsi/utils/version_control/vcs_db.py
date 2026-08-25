import sqlite3
import os

# ─────────────────────────── CONFIG ──────────────────────────────────────────

DB_NAME = "dsi_vcs.db"           # SQLite DB stored inside the root folder
SNAPSHOTS_DIR = "dsi_vcs"  # rsync snapshot copies live here

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
    file_count      INTEGER NOT NULL,
    total_bytes     INTEGER NOT NULL,
    UNIQUE(root_folder, commit_hash)
);

CREATE TABLE IF NOT EXISTS merkle_nodes (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id          INTEGER NOT NULL REFERENCES versions(id) ON DELETE CASCADE,
    root_folder         TEXT    NOT NULL,
    relative_path       TEXT    NOT NULL,
    file_type           TEXT    NOT NULL,
    node_hash           TEXT    NOT NULL,
    metadata            TEXT    DEFAULT NULL,
    content_hash_sha256 TEXT,
    subtree_file_count  INTEGER NOT NULL,
    subtree_total_bytes INTEGER NOT NULL,
    child_count         INTEGER NOT NULL,       -- for directory, number of immediate children, for files total chunk count
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
    tracked_commit_hash TEXT    NOT NULL,   -- latest commit or switched commit hash for this branch
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
    chunk_hash           TEXT    NOT NULL,
    chunk_size           INTEGER NOT NULL,
    created_at           INTEGER NOT NULL,
    commit_hash          TEXT DEFAULT NULL,   -- allow default null as commit hash may not be known at the time of chunk creation
    relative_file_path   TEXT NOT NULL,
    chunk_index          INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_chunk_store_commit_file
    ON chunk_store (commit_hash, relative_file_path, chunk_index);

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
