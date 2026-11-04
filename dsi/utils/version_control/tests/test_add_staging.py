"""Staging behavior and traversal boundaries for cmd_add."""

from pathlib import Path

from dsi.utils.version_control import cli, dsi_vcs
from dsi.utils.version_control.dsi_vcs import Version
from dsi.utils.version_control.repolog.log_record import RecordKind
from dsi.utils.version_control.vcs_db import SNAPSHOTS_DIR


def staged_records(repo):
    return [r for _, r in repo.repo_log.iter_records() if r.kind == RecordKind.DATA]


def test_overlapping_and_repeated_adds_do_not_grow_log(tmp_path, capsys):
    (tmp_path / "data").mkdir()
    (tmp_path / "data/a").write_text("first")
    repo = Version(str(tmp_path))
    capsys.readouterr()
    repo.cmd_add(["data", "data/a", "data", "./data/a"])
    assert capsys.readouterr().out.splitlines()[0] == "  2 path(s) added to staging."
    assert [r.file_path for r in staged_records(repo)] == ["data", "data/a"]
    size = Path(repo.repo_log.log_file).stat().st_size
    # A new instance exercises the same state reconstruction as separate CLI calls.
    repo = Version(str(tmp_path))
    capsys.readouterr()
    (tmp_path / "data/a").write_text("changed")
    repo.cmd_add(["data"])
    assert capsys.readouterr().out.splitlines()[0] == "  0 path(s) added to staging."
    assert Path(repo.repo_log.log_file).stat().st_size == size
    repo.cmd_commit("reads current contents of already staged file")
    conn = dsi_vcs.open_db(str(tmp_path))
    try:
        assert conn.execute("SELECT total_bytes FROM versions").fetchone()[0] == len("changed")
    finally:
        conn.close()


def test_add_overrides_delete_and_readds_removed_path(tmp_path):
    (tmp_path / "a").write_text("a")
    repo = Version(str(tmp_path))
    repo.cmd_add(["a"])
    repo.cmd_delete(["a"])
    repo.cmd_add(["a"])
    assert repo._load_pending_stage_entries()["a"][1] == "add"
    assert len(staged_records(repo)) == 3
    repo.cmd_remove(["a"])
    assert not repo._load_pending_stage_entries()
    repo.cmd_add(["a"])
    assert repo._load_pending_stage_entries()["a"][1] == "add"
    assert len(staged_records(repo)) == 5


def test_invalid_directory_inputs_are_rejected_before_walk(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    repo = Version(str(root))
    def unexpected_walk(*args, **kwargs):
        raise AssertionError("invalid directories must not be traversed")

    monkeypatch.setattr(dsi_vcs.os, "walk", unexpected_walk)
    repo.cmd_add([str(outside), SNAPSHOTS_DIR, "missing"])
    assert not repo._load_pending_stage_entries()


def test_symlink_targets_are_rechecked_on_later_adds(tmp_path):
    root = tmp_path / "repo"
    root.mkdir()
    (root / "inside").mkdir()
    (root / "inside/a").write_text("inside")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "b").write_text("outside")
    link = root / "link"
    link.symlink_to(root / "inside", target_is_directory=True)
    dangling = root / "dangling"
    dangling.symlink_to(root / "missing")
    repo = Version(str(root))
    repo.cmd_add(["link/a", "dangling"])
    link.unlink()
    link.symlink_to(outside, target_is_directory=True)
    repo.cmd_add(["link/b", "link"])
    # Track links themselves, including dangling links, but never outside contents.
    assert set(repo._load_pending_stage_entries()) == {"link/a", "dangling", "link"}
