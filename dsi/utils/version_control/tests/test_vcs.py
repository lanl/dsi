import os
import re
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
            "SELECT id, commit_hash, root_tree_hash, parent_commit_hash, "
            "hash_algorithm, snapshot_path FROM versions ORDER BY id"
            ).fetchall()

def merkle_nodes(repo_path, version_id):
    with connect_repo(repo_path) as conn:
        conn.row_factory = sqlite3.Row
        return {
            row["relative_path"]: row
            for row in conn.execute(
                "SELECT relative_path, file_type, node_hash, metadata_hash, "
                "content_hash_sha256, subtree_file_count, subtree_total_bytes, child_count "
                "FROM merkle_nodes WHERE version_id=? ORDER BY relative_path",
                (version_id,),
            ).fetchall()
        }

def latest_entries(repo_path):
    with connect_repo(repo_path) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(
            "SELECT relative_path, file_type, permissions_int "
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
    assert re.fullmatch(r"[0-9a-f]{64}", alpha["commit_hash"])
    assert re.fullmatch(r"[0-9a-f]{64}", beta["commit_hash"])
    assert alpha["hash_algorithm"] == "sha256-merkle-v1"
    assert beta["parent_commit_hash"] == alpha["commit_hash"]
    assert Path(alpha["snapshot_path"]).name == alpha["commit_hash"][:12]
    assert Path(beta["snapshot_path"]).name == beta["commit_hash"][:12]

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
    assert empty_entry["permissions_int"] == 0o2775

    beta_nodes = merkle_nodes(tmp_path, beta["id"])
    assert "." in beta_nodes
    assert beta_nodes["."]["node_hash"] == beta["root_tree_hash"]
    assert beta_nodes["a.txt"]["file_type"] == "file"
    assert re.fullmatch(r"[0-9a-f]{64}", beta_nodes["a.txt"]["content_hash_sha256"])


def test_merkle_tracks_content_metadata_symlink_and_deletes(tmp_path):
    require_rsync()
    repo = Version(str(tmp_path))

    nested = tmp_path / "nested"
    nested.mkdir()
    file_path = nested / "data.txt"
    file_path.write_text("one")
    link_path = tmp_path / "link.txt"
    link_path.symlink_to("nested/data.txt")

    repo.cmd_add(["nested", "link.txt"])
    repo.cmd_commit("initial")
    first = commits(tmp_path)[0]
    first_nodes = merkle_nodes(tmp_path, first["id"])

    file_path.write_text("two more")
    repo.cmd_add(["nested/data.txt"])
    repo.cmd_commit("content")
    second = commits(tmp_path)[1]
    second_nodes = merkle_nodes(tmp_path, second["id"])
    assert second_nodes["nested/data.txt"]["content_hash_sha256"] != first_nodes["nested/data.txt"]["content_hash_sha256"]
    assert second_nodes["nested/data.txt"]["metadata_hash"] == first_nodes["nested/data.txt"]["metadata_hash"]
    assert second["root_tree_hash"] != first["root_tree_hash"]

    file_path.chmod(0o640)
    repo.cmd_add(["nested/data.txt"])
    repo.cmd_commit("metadata")
    third = commits(tmp_path)[2]
    third_nodes = merkle_nodes(tmp_path, third["id"])
    assert third_nodes["nested/data.txt"]["content_hash_sha256"] == second_nodes["nested/data.txt"]["content_hash_sha256"]
    assert third_nodes["nested/data.txt"]["metadata_hash"] != second_nodes["nested/data.txt"]["metadata_hash"]

    link_path.unlink()
    link_path.symlink_to("missing.txt")
    repo.cmd_add(["link.txt"])
    repo.cmd_commit("symlink")
    fourth = commits(tmp_path)[3]
    fourth_nodes = merkle_nodes(tmp_path, fourth["id"])
    assert fourth_nodes["link.txt"]["metadata_hash"] != third_nodes["link.txt"]["metadata_hash"]

    repo.cmd_delete(["nested/data.txt"])
    repo.cmd_commit("delete")
    fifth = commits(tmp_path)[4]
    fifth_nodes = merkle_nodes(tmp_path, fifth["id"])
    assert "nested/data.txt" not in fifth_nodes
    assert fifth["parent_commit_hash"] == fourth["commit_hash"]


def test_diff_short_circuits_equal_root_trees(tmp_path, capsys):
    require_rsync()
    repo = Version(str(tmp_path))

    (tmp_path / "a.txt").write_text("same")
    repo.cmd_add(["a.txt"])
    repo.cmd_commit("first")

    repo.cmd_delete(["does-not-exist.txt"])
    repo.cmd_commit("same tree")

    first, second = commits(tmp_path)
    assert first["root_tree_hash"] == second["root_tree_hash"]
    assert first["commit_hash"] != second["commit_hash"]

    repo.cmd_diff(first["commit_hash"][:12], second["commit_hash"][:12])
    out = capsys.readouterr().out
    assert "ADDED" not in out
    assert "DELETED" not in out
    assert "MODIFIED" not in out
    assert "Summary: +0 added  -0 deleted  ~0 modified" in out
