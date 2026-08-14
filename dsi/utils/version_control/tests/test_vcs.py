import hashlib
import os
import random
import sqlite3
import subprocess
import threading
import time
from pathlib import Path
from shutil import which
import pytest
import stat

from dsi.utils.version_control.dsi_vcs import Version
from dsi.utils.version_control.vcs_db import DB_NAME, SNAPSHOTS_DIR
from dsi.utils.version_control.repolog.chunking import CHUNK_STORAGE_DIR

def require_rsync():
    if which("rsync") is None:
        pytest.skip("rsync is required for dsi_vcs.")

def require_fuse():
    pytest.importorskip("pyfuse3")
    pytest.importorskip("trio")
    if which("fusermount3") is None and which("fusermount") is None:
        pytest.skip("fusermount is required for dsi-vcs mount.")
    if not os.path.exists("/dev/fuse"):
        pytest.skip("/dev/fuse is not available in this environment.")



def connect_repo(repo_path):
    return sqlite3.connect(repo_path / SNAPSHOTS_DIR / DB_NAME)

def commits(repo_path):
    with connect_repo(repo_path) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(
            "SELECT id, commit_hash FROM versions ORDER BY id"
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
    alpha_hash = commits(tmp_path)[-1]["commit_hash"]

    (tmp_path / "c.txt").write_text("baz")

    repo.cmd_add(["c.txt"])
    repo.cmd_commit("beta")
    beta_hash = commits(tmp_path)[-1]["commit_hash"]

    repo.cmd_restore(alpha_hash)
    assert (tmp_path / "a.txt").read_text() == "foo"
    assert (tmp_path / "nested" / "child" / "b.txt").read_text() == "bar"
    assert (tmp_path / "empty").exists()
    assert stat.S_IMODE((tmp_path / "empty").stat().st_mode) == 0o2775
    assert not (tmp_path / "c.txt").exists()

    repo.cmd_restore(beta_hash)
    assert (tmp_path / "c.txt").read_text() == "baz"


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

    latest_hash = commits(tmp_path)[-1]["commit_hash"]
    repo.cmd_restore(latest_hash)

    assert (tmp_path / "a.txt").read_text() == "first"
    assert (tmp_path / "b.txt").read_text() == "second"
    assert (tmp_path / "c.txt").read_text() == "third"
    assert not (tmp_path / "keep.txt").exists()


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
        rows = conn.execute("SELECT chunk_hash FROM chunk_store").fetchall()

    assert rows
    chunk_dir = tmp_path / SNAPSHOTS_DIR / CHUNK_STORAGE_DIR
    for row in rows:
        chunk_path = chunk_dir / row["chunk_hash"]
        assert chunk_path.exists()
        assert chunk_path.stat().st_size > 0


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


def test_mount_serves_chunked_content_read_only(tmp_path):
    require_rsync()
    require_fuse()

    repo = Version(str(tmp_path))

    small_content = "hello from the mount\n"
    (tmp_path / "small.txt").write_text(small_content)

    big_data = random.Random(1234).randbytes(9_000_000)
    (tmp_path / "big.bin").write_bytes(big_data)

    repo.cmd_add(["small.txt", "big.bin"])
    repo.cmd_commit("mount test commit")

    with connect_repo(tmp_path) as conn:
        conn.row_factory = sqlite3.Row
        chunk_sizes = [
            row["chunk_size"]
            for row in conn.execute(
                "SELECT chunk_size FROM chunk_store "
                "WHERE relative_file_path='big.bin' ORDER BY chunk_index"
            )
        ]
    assert len(chunk_sizes) >= 2, "expected big.bin to span multiple chunks"

    mountpoint = tmp_path / "mnt"
    mountpoint.mkdir()

    mount_errors = []

    def run_mount():
        try:
            repo.cmd_mount(None, str(mountpoint))
        except BaseException as exc:
            mount_errors.append(exc)

    mount_thread = threading.Thread(target=run_mount, daemon=True)
    mount_thread.start()
    try:
        deadline = time.monotonic() + 10
        while not os.path.ismount(str(mountpoint)):
            mount_thread.join(timeout=0.1)
            if not mount_thread.is_alive():
                if mount_errors:
                    raise mount_errors[0]
                pytest.fail("FUSE mount exited before becoming active")
            if time.monotonic() > deadline:
                pytest.fail("FUSE mount did not become active in time")

        assert (mountpoint / "small.txt").read_text() == small_content

        with open(mountpoint / "big.bin", "rb") as f:
            mounted = f.read()
        assert mounted == big_data

        # Read a span straddling the first real chunk boundary end-to-end.
        boundary = chunk_sizes[0]
        with open(mountpoint / "big.bin", "rb") as f:
            f.seek(boundary - 10)
            straddled = f.read(20)
        assert straddled == big_data[boundary - 10: boundary + 10]

        with pytest.raises(OSError):
            (mountpoint / "small.txt").write_text("nope")
    finally:
        fusermount = which("fusermount3") or which("fusermount")
        subprocess.run([fusermount, "-u", str(mountpoint)], check=False)
        mount_thread.join(timeout=5)
        assert not mount_thread.is_alive()
