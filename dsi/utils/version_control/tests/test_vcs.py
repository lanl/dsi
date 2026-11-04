import sqlite3
from shutil import which
import pytest
import stat

from dsi.utils.version_control.dsi_vcs import Version
from dsi.utils.version_control.vcs_db import DB_NAME, SNAPSHOTS_DIR
from dsi.utils.version_control.repolog.chunking import CHUNK_STORAGE_DIR


def connect_repo(repo_path):
    return sqlite3.connect(repo_path / SNAPSHOTS_DIR / DB_NAME)

def commits(repo_path):
    with connect_repo(repo_path) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(
            "SELECT id, commit_hash FROM versions ORDER BY id"
            ).fetchall()

def test_add(tmp_path):
    repo = Version(str(tmp_path))

    empty_dir = tmp_path / "empty"
    nested_dir = tmp_path / "nested" / "child"
    empty_dir.mkdir()
    nested_dir.mkdir(parents=True)
    (tmp_path / "a.txt").write_text("foo")
    (nested_dir / "b.txt").write_text("bar")

    empty_dir.chmod(0o2775)

    repo.cmd_add(["a.txt", "nested", "empty"])
    repo.cmd_commit("alpha")
    alpha_hash = commits(tmp_path)[-1]["commit_hash"]

    (tmp_path / "c.txt").write_text("baz")
    (tmp_path / "a.txt").write_text("alice_and_bob")

    repo.cmd_add(["c.txt", "a.txt"])
    repo.cmd_commit("beta")
    beta_hash = commits(tmp_path)[-1]["commit_hash"]

    repo.cmd_restore(alpha_hash)
    assert (tmp_path / "a.txt").read_text() == "foo"
    assert (tmp_path / "nested" / "child" / "b.txt").read_text() == "bar"
    assert (tmp_path / "empty").exists()
    assert stat.S_IMODE((tmp_path / "empty").stat().st_mode) == 0o2775

    repo.cmd_restore(beta_hash)
    assert (tmp_path / "c.txt").read_text() == "baz"
    assert (tmp_path / "a.txt").read_text() == "alice_and_bob"


def test_commit_records_active_branch_name_in_branch_links(tmp_path):
    repo = Version(str(tmp_path))

    (tmp_path / "a.txt").write_text("first")
    repo.cmd_add(["a.txt"])
    repo.cmd_commit("initial")

    repo.cmd_branch("feature")
    repo.cmd_switch("feature")

    (tmp_path / "b.txt").write_text("second")
    repo.cmd_add(["b.txt"])
    repo.cmd_commit("second")

    with connect_repo(tmp_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT child_commit_hash, child_branch_name FROM branch_links ORDER BY id"
        ).fetchall()

    assert rows[0]["child_branch_name"] == "main"
    assert rows[1]["child_branch_name"] == "feature"
