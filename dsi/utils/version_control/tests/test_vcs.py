import os
import sqlite3
from pathlib import Path
from shutil import which
import pytest
import stat

from dsi.dsi import DSI
from dsi.utils.version_control.dsi_vcs import Version
from dsi.utils.version_control.vcs_db import DB_NAME, SNAPSHOTS_DIR

def require_rsync():
    if which("rsync") is None:
        pytest.skip("rsync is required for dsi_vcs.")



def connect_repo(repo_path):
    return sqlite3.connect(repo_path / SNAPSHOTS_DIR / DB_NAME)

def commits(repo_path):
    with connect_repo(repo_path) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(
            "SELECT id, commit_hash, snapshot_path FROM versions ORDER BY id"
            ).fetchall()

def latest_entries(repo_path):
    with connect_repo(repo_path) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(
            "SELECT relative_path, file_type, permissions_octal, permissions_str, setgid "
            "FROM file_entries WHERE version_id=(SELECT MAX(id) from versions) "
            "ORDER BY relative_path").fetchall()

def test_add(tmp_path):
    require_rsync()
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

    (tmp_path / "c.txt").write_text("baz")

    repo.cmd_add(["c.txt"])
    repo.cmd_commit("beta")

    alpha, beta = commits(tmp_path)
    alpha_path = Path(alpha["snapshot_path"])
    beta_path = Path(beta["snapshot_path"])
    assert (alpha_path / "a.txt").read_text() == "foo"
    assert (alpha_path / "nested" / "child" / "b.txt").read_text() == "bar"
    assert (beta_path / "c.txt").read_text() == "baz"
    assert (alpha_path / "empty").exists()
    assert stat.S_IMODE((alpha_path / "empty").stat().st_mode) == 0o2775

    rows = {row["relative_path"]: row for row in latest_entries(tmp_path)}
    empty_entry = rows["empty"]
    assert empty_entry["file_type"] == "dir"
    assert empty_entry["permissions_str"] == "rwxrwsr-x"
    assert eval(empty_entry["permissions_octal"]) == 0o2775
    assert empty_entry["setgid"] == 1


def test_versioning():
    test = DSI()
    # test.version("init", os.getcwd())
    # wpath = "/Users/ssakin/Public/versioning-test/clover-demo"
    wpath = os.getcwd()
    test.version("init", wpath)
    assert os.path.exists(wpath + "/.dsi_vcs_snapshots/.dsi_vcs.db")

    test.version("add", os.path.join(wpath, "a_dummy_file"))
    print(">Single file added.")
    test.version("add", os.path.join(wpath, "schema.json") + " " + os.path.join(wpath, "schema2.json"))
    print(">Multi file added.")
    print(">Versioning initialized.")

    test.version("remove", os.path.join(wpath, "schema2.json"))
    print(">Single file removed.")

    test.version("commit", "Tester Commits")
    test.version("log")
    test.version("diff", "77345f1115d94c69a1255b9fb0524378 4af9e3d4dc854d699b96b5a84f913ac0")
    assert True
