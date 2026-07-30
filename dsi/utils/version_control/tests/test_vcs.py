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


def test_commit_only_includes_staged_files(tmp_path):
    require_rsync()
    repo = Version(str(tmp_path))

    (tmp_path / "a.txt").write_text("first")
    (tmp_path / "b.txt").write_text("second")
    repo.cmd_add(["a.txt", "b.txt"])
    repo.cmd_commit("initial")

    (tmp_path / "c.txt").write_text("third")
    (tmp_path / "keep.txt").write_text("should-not-commit")
    repo.cmd_add(["c.txt"])
    repo.cmd_commit("second")

    latest_commit = commits(tmp_path)[-1]
    snapshot_path = Path(latest_commit["snapshot_path"])

    assert (snapshot_path / "a.txt").read_text() == "first"
    assert (snapshot_path / "b.txt").read_text() == "second"
    assert (snapshot_path / "c.txt").read_text() == "third"
    assert not (snapshot_path / "keep.txt").exists()


def test_commit_records_active_branch_name_in_branch_links(tmp_path):
    require_rsync()
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


def test_chunking_persists_chunks_to_disk(tmp_path):
    require_rsync()
    repo = Version(str(tmp_path))

    data = "A" * (2 * 1024 * 1024 + 7)
    (tmp_path / "big.bin").write_bytes(data.encode("utf-8"))
    repo.cmd_add(["big.bin"])
    repo.cmd_commit("chunked")

    with connect_repo(tmp_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT chunk_hash, chunk_path FROM chunk_store").fetchall()

    assert rows
    for row in rows:
        assert Path(row["chunk_path"]).exists()
        assert Path(row["chunk_path"]).stat().st_size > 0


def test_switch_restores_branch_snapshot(tmp_path):
    require_rsync()
    repo = Version(str(tmp_path))

    (tmp_path / "tracked.txt").write_text("main")
    repo.cmd_add(["tracked.txt"])
    repo.cmd_commit("main")
    repo.cmd_branch("feature")

    (tmp_path / "tracked.txt").write_text("changed")
    (tmp_path / "new.txt").write_text("uncommitted")

    repo.cmd_switch("feature")

    assert (tmp_path / "tracked.txt").read_text() == "main"
    assert not (tmp_path / "new.txt").exists()
