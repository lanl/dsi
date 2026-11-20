import sqlite3
import os

# ─────────────────────────── CONFIG ──────────────────────────────────────────

DB_NAME = "dsi_vcs.db"           # SQLite DB stored inside the root folder
SNAPSHOTS_DIR = "__dsi_vcs"      # dsi versioning internal files live here

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
    stat_fingerprint    TEXT,
    content_hash_sha256 TEXT,
    manifest_hash       TEXT    DEFAULT NULL,
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

CREATE TABLE IF NOT EXISTS repository_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    commit_location INTEGER
);

"""


def _schema_statements():
    """Split complete SQLite statements without interpreting their contents."""
    statement = ""
    for line in SCHEMA.splitlines(keepends=True):
        statement += line
        if sqlite3.complete_statement(statement):
            yield statement
            statement = ""
    if statement.strip():
        raise ValueError("Incomplete database schema statement")


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _ensure_columns(conn: sqlite3.Connection, expected: sqlite3.Connection) -> None:
    tables = expected.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    # Add parent keys before child columns that may reference them.
    tables = sorted((row[0] for row in tables), key=lambda name: (name != "versions", name))
    for table in tables:
        name = _quote(table)
        current = {row["name"].lower() for row in conn.execute(f"PRAGMA table_info({name})")}
        for column in expected.execute(f"PRAGMA table_info({name})"):
            column_name = column[1]
            if column_name.lower() in current:
                continue
            identifier = _quote(column_name)
            declaration = column[2] or "BLOB"
            default = column[4]
            if table == "versions" and column_name == "hash_algorithm":
                # Versions predating this column were created by the v1 writer.
                declaration += " NOT NULL DEFAULT 'sha256-merkle-v1'"
            elif default is not None:
                if column[3]:
                    declaration += " NOT NULL"
                declaration += f" DEFAULT {default}"
            # SQLite cannot add a primary key, or a NOT NULL column without
            # a default, to a populated table. Unknown historic values stay NULL.
            conn.execute(f"ALTER TABLE {name} ADD COLUMN {identifier} {declaration}")
            if column[5]:
                index = _quote(f"dsi_compat_{table}_{column_name}_unique")
                trigger = _quote(f"dsi_compat_{table}_{column_name}_autofill")
                conn.execute(f"CREATE UNIQUE INDEX {index} ON {name} ({identifier})")
                conn.execute(f"UPDATE {name} SET {identifier}=rowid WHERE {identifier} IS NULL")
                conn.execute(
                    f"CREATE TRIGGER {trigger} AFTER INSERT ON {name} "
                    f"WHEN NEW.{identifier} IS NULL BEGIN "
                    f"UPDATE {name} SET {identifier}=NEW.rowid WHERE rowid=NEW.rowid; END"
                )
            elif table == "branches" and column_name == "tracked_commit_hash" and "head_commit_hash" in current:
                conn.execute(f"UPDATE {name} SET {identifier}=head_commit_hash")
            current.add(column_name.lower())


def open_db(root_folder: str) -> sqlite3.Connection:
    snaps = os.path.join(root_folder, SNAPSHOTS_DIR)
    os.makedirs(snaps, exist_ok=True)
    db_path = os.path.join(snaps, DB_NAME)
    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        statements = list(_schema_statements())
        with sqlite3.connect(":memory:") as expected:
            expected.executescript(SCHEMA)
            conn.execute("BEGIN IMMEDIATE")
            for statement in statements:
                if statement.lstrip().upper().startswith("CREATE TABLE"):
                    conn.execute(statement)
            _ensure_columns(conn, expected)
            for statement in statements:
                if statement.lstrip().upper().startswith("CREATE INDEX"):
                    conn.execute(statement)
            conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    return conn
