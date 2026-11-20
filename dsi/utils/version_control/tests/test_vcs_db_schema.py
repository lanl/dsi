import sqlite3

import pytest

from dsi.utils.version_control.dsi_vcs import Version
from dsi.utils.version_control.vcs_db import DB_NAME, SCHEMA, SNAPSHOTS_DIR, open_db


def test_init_adds_missing_columns_before_indexes_and_preserves_rows(tmp_path):
    snapshots = tmp_path / SNAPSHOTS_DIR
    snapshots.mkdir()
    with sqlite3.connect(snapshots / DB_NAME) as conn:
        conn.executescript("""
            CREATE TABLE versions (id INTEGER PRIMARY KEY, commit_hash TEXT, custom_note TEXT);
            INSERT INTO versions VALUES (7, 'old-commit', 'keep me');
            CREATE TABLE merkle_nodes (id INTEGER PRIMARY KEY, version_id INTEGER, relative_path TEXT);
            INSERT INTO merkle_nodes VALUES (5, 7, 'data.bin');
            CREATE TABLE branches (head_commit_hash TEXT, branch_name TEXT);
            INSERT INTO branches VALUES ('old-commit', 'main');
            CREATE TABLE branch_links (id INTEGER PRIMARY KEY, child_commit_hash TEXT);
            INSERT INTO branch_links VALUES (3, 'old-commit');
            CREATE TABLE repository_state (id INTEGER PRIMARY KEY, commit_location INTEGER);
            INSERT INTO repository_state VALUES (1, NULL);
        """)

    Version(str(tmp_path))
    with sqlite3.connect(":memory:") as expected, open_db(str(tmp_path)) as actual:
        expected.executescript(SCHEMA)
        for (table,) in expected.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ):
            wanted = {row[1] for row in expected.execute(f'PRAGMA table_info("{table}")')}
            found = {row[1] for row in actual.execute(f'PRAGMA table_info("{table}")')}
            assert wanted <= found
        version = actual.execute("SELECT * FROM versions WHERE id=7").fetchone()
        assert (version["commit_hash"], version["custom_note"], version["hash_algorithm"]) == (
            "old-commit", "keep me", "sha256-merkle-v1"
        )
        assert actual.execute("SELECT relative_path FROM merkle_nodes WHERE id=5").fetchone()[0] == "data.bin"
        branch = actual.execute("SELECT id, head_commit_hash, tracked_commit_hash FROM branches").fetchone()
        assert tuple(branch) == (1, "old-commit", "old-commit")
        actual.execute("INSERT INTO branches (head_commit_hash, branch_name) VALUES ('next', 'feature')")
        assert actual.execute("SELECT id FROM branches WHERE branch_name='feature'").fetchone()[0] == 2
        assert actual.execute("SELECT 1 FROM sqlite_master WHERE name='idx_merkle_nodes_hash'").fetchone()

    # Repeated initialization keeps both the schema and data intact.
    Version(str(tmp_path))
    with open_db(str(tmp_path)) as conn:
        assert conn.execute("SELECT custom_note FROM versions WHERE id=7").fetchone()[0] == "keep me"
        assert conn.execute("SELECT count(*) FROM branches").fetchone()[0] == 2


def test_new_database_preserves_declared_constraints(tmp_path):
    with open_db(str(tmp_path)) as conn:
        info = {row["name"]: row for row in conn.execute("PRAGMA table_info(versions)")}
        assert info["id"]["pk"] == 1
        assert info["hash_algorithm"]["notnull"] == 1


def test_failed_schema_upgrade_rolls_back_all_changes(tmp_path):
    snapshots = tmp_path / SNAPSHOTS_DIR
    snapshots.mkdir()
    with sqlite3.connect(snapshots / DB_NAME) as conn:
        conn.executescript("""
            CREATE TABLE branches (branch_name TEXT PRIMARY KEY) WITHOUT ROWID;
            INSERT INTO branches VALUES ('main');
        """)
    with pytest.raises(sqlite3.OperationalError):
        open_db(str(tmp_path))
    with sqlite3.connect(snapshots / DB_NAME) as conn:
        assert [row[1] for row in conn.execute("PRAGMA table_info(branches)")] == ["branch_name"]
        assert conn.execute("SELECT branch_name FROM branches").fetchone()[0] == "main"
        assert conn.execute("SELECT 1 FROM sqlite_master WHERE name='versions'").fetchone() is None
