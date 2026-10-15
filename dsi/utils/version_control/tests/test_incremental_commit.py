"""Regression coverage for incremental snapshots, authorization, and log recovery."""

import hashlib
import json
import os
import sqlite3
from unittest.mock import Mock

import pytest

from dsi.utils.version_control import dsi_vcs, vcs_metadata_helper as metadata
from dsi.utils.version_control.dsi_vcs import Version
from dsi.utils.version_control.repolog import chunking
from dsi.utils.version_control.vcs_db import DB_NAME, SNAPSHOTS_DIR


def db(root):
    conn = sqlite3.connect(root / SNAPSHOTS_DIR / DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn


def latest(root):
    with db(root) as conn:
        return dict(
            conn.execute("SELECT * FROM versions ORDER BY id DESC LIMIT 1").fetchone()
        )


def content(root, commit, path):
    with db(root) as conn:
        rows = conn.execute(
            "SELECT chunk_hash FROM chunk_store WHERE commit_hash=? AND relative_file_path=? ORDER BY chunk_index",
            (commit, path),
        ).fetchall()
    return b"".join(
        (
            root / SNAPSHOTS_DIR / chunking.CHUNK_STORAGE_DIR / r["chunk_hash"]
        ).read_bytes()
        for r in rows
    )


def test_stage_all_reuses_unchanged_content_and_metadata(tmp_path, monkeypatch):
    repo = Version(str(tmp_path))
    for i in range(20):
        (tmp_path / f"f{i}").write_text(str(i))
    repo.cmd_add(["."])
    repo.cmd_commit("initial")
    first = latest(tmp_path)
    collect = Mock(wraps=dsi_vcs.collect_metadata)
    chunks = Mock(wraps=chunking.chunk_file)
    monkeypatch.setattr(dsi_vcs, "collect_metadata", collect)
    monkeypatch.setattr(chunking, "chunk_file", chunks)
    repo.cmd_add(["."])
    repo.cmd_commit("unchanged")
    second = latest(tmp_path)
    assert collect.call_count == chunks.call_count == 0
    assert first["root_tree_hash"] == second["root_tree_hash"]
    assert first["commit_hash"] != second["commit_hash"]
    for i in range(20):
        assert content(tmp_path, second["commit_hash"], f"f{i}") == str(i).encode()


def test_one_change_only_processes_one_file_and_preserves_unstaged_state(
    tmp_path, monkeypatch
):
    repo = Version(str(tmp_path))
    for name in ("a", "b", "c"):
        (tmp_path / name).write_text(name)
    repo.cmd_add(["."])
    repo.cmd_commit("initial")
    (tmp_path / "a").write_text("changed")
    (tmp_path / "b").write_text("unstaged edit")
    (tmp_path / "c").unlink()
    collect = Mock(wraps=dsi_vcs.collect_metadata)
    chunks = Mock(wraps=chunking.chunk_file)
    monkeypatch.setattr(dsi_vcs, "collect_metadata", collect)
    monkeypatch.setattr(chunking, "chunk_file", chunks)
    repo.cmd_add(["a"])
    repo.cmd_commit("one change")
    version = latest(tmp_path)
    assert collect.call_count == chunks.call_count == 1
    assert version["file_count"] == 3
    assert content(tmp_path, version["commit_hash"], "a") == b"changed"
    assert content(tmp_path, version["commit_hash"], "b") == b"b"
    assert content(tmp_path, version["commit_hash"], "c") == b"c"


def test_same_size_edit_with_restored_mtime_is_detected(tmp_path):
    repo = Version(str(tmp_path))
    path = tmp_path / "a"
    path.write_bytes(b"first")
    repo.cmd_add(["a"])
    repo.cmd_commit("initial")
    old = path.stat()
    path.write_bytes(b"other")
    os.utime(path, ns=(old.st_atime_ns, old.st_mtime_ns))
    repo.cmd_add(["."])
    repo.cmd_commit("same size and mtime")
    assert content(tmp_path, latest(tmp_path)["commit_hash"], "a") == b"other"


def test_nested_and_empty_directories_survive_followup_commit(tmp_path):
    repo = Version(str(tmp_path))
    (tmp_path / "data" / "child").mkdir(parents=True)
    (tmp_path / "empty").mkdir()
    (tmp_path / "data" / "child" / "a").write_bytes(b"nested")
    repo.cmd_add(["data/child/a", "empty"])
    repo.cmd_commit("nested initial")
    (tmp_path / "top").write_bytes(b"top")
    repo.cmd_add(["top"])
    repo.cmd_commit("followup")
    version = latest(tmp_path)
    with db(tmp_path) as conn:
        nodes = {
            r["relative_path"]: r
            for r in conn.execute(
                "SELECT * FROM merkle_nodes WHERE version_id=?", (version["id"],)
            )
        }
    assert set(nodes) == {".", "data", "data/child", "data/child/a", "empty", "top"}
    assert nodes["."]["subtree_file_count"] == version["file_count"] == 2
    assert content(tmp_path, version["commit_hash"], "data/child/a") == b"nested"


def test_unstage_directory_and_commit_deleted_tree(tmp_path):
    repo = Version(str(tmp_path))
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "a").write_bytes(b"a")
    repo.cmd_add(["data"])
    repo.cmd_remove(["data"])
    assert not repo._load_pending_stage_entries()
    repo.cmd_add(["data"])
    repo.cmd_commit("initial")
    (tmp_path / "data" / "a").unlink()
    (tmp_path / "data").rmdir()
    repo.cmd_delete(["data"])
    repo.cmd_commit("delete entire tree")
    version = latest(tmp_path)
    assert version["file_count"] == version["total_bytes"] == 0
    with db(tmp_path) as conn:
        assert [
            r[0]
            for r in conn.execute(
                "SELECT relative_path FROM merkle_nodes WHERE version_id=?",
                (version["id"],),
            )
        ] == ["."]
        assert (
            conn.execute(
                "SELECT count(*) FROM chunk_store WHERE commit_hash=?",
                (version["commit_hash"],),
            ).fetchone()[0]
            == 0
        )


def test_denied_staged_write_is_atomic_and_keeps_staging(tmp_path):
    repo = Version(str(tmp_path))
    for name in ("a", "b"):
        (tmp_path / name).write_bytes(b"old")
    (tmp_path / "b").chmod(0o444)
    repo.cmd_add(["."])
    repo.cmd_commit("read-only b")
    before = latest(tmp_path)
    (tmp_path / "a").write_bytes(b"new")
    (tmp_path / "b").chmod(0o644)
    (tmp_path / "b").write_bytes(b"forbidden")
    repo.cmd_add(["a", "b"])
    with pytest.raises(SystemExit, match="Write access denied"):
        repo.cmd_commit("denied")
    assert latest(tmp_path)["commit_hash"] == before["commit_hash"]
    assert set(Version(str(tmp_path))._load_pending_stage_entries()) == {"a", "b"}


def test_metadata_only_change_does_not_change_content(tmp_path):
    repo = Version(str(tmp_path))
    path = tmp_path / "a"
    path.write_bytes(b"content")
    path.chmod(0o644)
    repo.cmd_add(["a"])
    repo.cmd_commit("initial")
    first = latest(tmp_path)
    path.chmod(0o600)
    repo.cmd_add(["a"])
    repo.cmd_commit("mode change")
    second = latest(tmp_path)
    assert first["root_tree_hash"] != second["root_tree_hash"]
    assert content(tmp_path, second["commit_hash"], "a") == b"content"
    with db(tmp_path) as conn:
        node = conn.execute(
            "SELECT metadata FROM merkle_nodes WHERE version_id=? AND relative_path='a'",
            (second["id"],),
        ).fetchone()
    assert json.loads(node[0])["permissions_int"] == 0o600


def test_restart_replays_only_checkpoint_and_pending_records(tmp_path, monkeypatch):
    repo = Version(str(tmp_path))
    path = tmp_path / "a"
    for i in range(4):
        path.write_text(str(i))
        repo.cmd_add(["a"])
        repo.cmd_commit(str(i))
    path.write_text("pending")
    repo.cmd_add(["a"])
    restarted = Version(str(tmp_path))
    decode = Mock(wraps=restarted.repo_log._decode)
    monkeypatch.setattr(restarted.repo_log, "_decode", decode)
    assert set(restarted._load_pending_stage_entries()) == {"a"}
    assert decode.call_count == 2  # checkpoint marker plus one pending record
    restarted.cmd_commit("after restart")
    assert content(tmp_path, latest(tmp_path)["commit_hash"], "a") == b"pending"
    assert restarted.repo_log.verify_chain().ok


def test_marker_without_sql_commit_does_not_lose_pending_changes(tmp_path, monkeypatch):
    repo = Version(str(tmp_path))
    path = tmp_path / "a"
    path.write_bytes(b"old")
    repo.cmd_add(["a"])
    repo.cmd_commit("initial")
    before = latest(tmp_path)
    path.write_bytes(b"new")
    repo.cmd_add(["a"])
    append_commit = repo.repo_log.commit

    def fail_after_marker(*args, **kwargs):
        append_commit(*args, **kwargs)
        raise RuntimeError("interrupted before SQL commit")

    monkeypatch.setattr(repo.repo_log, "commit", fail_after_marker)
    with pytest.raises(RuntimeError, match="interrupted"):
        repo.cmd_commit("interrupted")
    assert latest(tmp_path)["commit_hash"] == before["commit_hash"]
    restarted = Version(str(tmp_path))
    assert set(restarted._load_pending_stage_entries()) == {"a"}
    restarted.cmd_commit("retry")
    assert content(tmp_path, latest(tmp_path)["commit_hash"], "a") == b"new"
    assert restarted.repo_log.verify_chain().ok


def test_legacy_schema_migrates_and_recollects_missing_fingerprints(
    tmp_path, monkeypatch
):
    repo = Version(str(tmp_path))
    (tmp_path / "a").write_text("a")
    repo.cmd_add(["a"])
    repo.cmd_commit("legacy")
    with db(tmp_path) as conn:
        conn.execute("ALTER TABLE merkle_nodes DROP COLUMN stat_fingerprint")
        conn.execute("DROP TABLE repository_state")
    repo = Version(str(tmp_path))
    assert not repo._load_pending_stage_entries()
    collect = Mock(wraps=dsi_vcs.collect_metadata)
    monkeypatch.setattr(dsi_vcs, "collect_metadata", collect)
    repo.cmd_add(["a"])
    repo.cmd_commit("after migration")
    assert collect.call_count == 1
    assert content(tmp_path, latest(tmp_path)["commit_hash"], "a") == b"a"


@pytest.mark.skipif(os.name == "nt", reason="POSIX identity lookup counts")
def test_identity_and_names_are_resolved_once_per_commit(tmp_path, monkeypatch):
    repo = Version(str(tmp_path))
    for i in range(10):
        (tmp_path / str(i)).write_text(str(i))
    lookup = Mock(wraps=metadata.grp.getgrgid)
    monkeypatch.setattr(metadata.grp, "getgrgid", lookup)
    repo.cmd_add(["."])
    repo.cmd_commit("initial")
    group_count = len(set(os.getgroups()) | {os.getegid()})
    assert lookup.call_count <= group_count + 1
    lookup.reset_mock()
    repo.cmd_add(["."])
    repo.cmd_commit("unchanged")
    assert lookup.call_count == group_count


def test_batched_log_write_syncs_once_and_keeps_frame_chain(tmp_path, monkeypatch):
    from dsi.utils.version_control.repolog.repository_log import RepositoryLog
    from dsi.utils.version_control.repolog.log_record import OperationType

    log = RepositoryLog(tmp_path, "test")
    sync = Mock(wraps=os.fsync)
    monkeypatch.setattr(os, "fsync", sync)
    results = log.append_many(
        {"file_path": str(i), "operation": OperationType.FILE_ADD} for i in range(20)
    )
    assert sync.call_count == 1
    assert len(results) == 20
    assert log.verify_chain().ok
    assert [log.read_at(r.location).file_path for r in results] == [
        str(i) for i in range(20)
    ]
    assert log.append_many([]) == []


def test_streamed_fastcdc_preserves_original_boundaries(tmp_path, monkeypatch):
    # Golden boundaries from the pre-streaming implementation, with reduced
    # chunk sizes to exercise many boundaries without a large test fixture.
    for name, value in [
        ("MIN_CHUNK", 1024),
        ("AVG_CHUNK", 4096),
        ("MAX_CHUNK", 8192),
        ("STRICT_MASK", chunking._make_spread_mask(14)),
        ("EAGER_MASK", chunking._make_spread_mask(10)),
    ]:
        monkeypatch.setattr(chunking, name, value)
    data = b"".join(
        hashlib.sha256(i.to_bytes(4, "little")).digest() for i in range(2048)
    )
    path = tmp_path / "data.bin"
    path.write_bytes(data)
    chunks = list(chunking.iter_file_fastcdc(path))
    assert [c["size"] for c in chunks] == [
        4353,
        5855,
        8192,
        5038,
        5185,
        4858,
        2393,
        4191,
        6517,
        4767,
        4822,
        5191,
        1112,
        3062,
    ]
    assert b"".join(c["data"] for c in chunks) == data
    assert all(c["sha256"] == hashlib.sha256(c["data"]).hexdigest() for c in chunks)


def test_unchanged_read_only_file_can_be_reused_when_staging_all(tmp_path):
    repo = Version(str(tmp_path))
    (tmp_path / "readonly").write_bytes(b"keep")
    (tmp_path / "readonly").chmod(0o444)
    (tmp_path / "changed").write_bytes(b"old")
    repo.cmd_add(["."])
    repo.cmd_commit("initial")
    (tmp_path / "changed").write_bytes(b"new")
    repo.cmd_add(["."])
    repo.cmd_commit("reuse readonly")
    assert content(tmp_path, latest(tmp_path)["commit_hash"], "readonly") == b"keep"


def test_source_mutation_during_chunking_rolls_back(tmp_path, monkeypatch):
    repo = Version(str(tmp_path))
    path = tmp_path / "a"
    path.write_bytes(b"old")
    repo.cmd_add(["a"])
    repo.cmd_commit("initial")
    before = latest(tmp_path)
    path.write_bytes(b"new")
    repo.cmd_add(["a"])
    store = dsi_vcs.store_chunks_for_snapshot

    def changing_source(*args, **kwargs):
        result = store(*args, **kwargs)
        path.write_bytes(b"changed during commit")
        return result

    monkeypatch.setattr(dsi_vcs, "store_chunks_for_snapshot", changing_source)
    with pytest.raises(SystemExit, match="File changed while committing"):
        repo.cmd_commit("racy")
    assert latest(tmp_path)["commit_hash"] == before["commit_hash"]
    assert set(repo._load_pending_stage_entries()) == {"a"}


def test_incremental_tree_matches_fresh_tree_and_restores_nested_files(tmp_path):
    from dsi.utils.version_control.repolog.merkle import build_merkle_tree

    repo = Version(str(tmp_path))
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "a").write_bytes(b"a")
    (tmp_path / "data" / "b").write_bytes(b"b")
    repo.cmd_add(["."])
    repo.cmd_commit("initial")
    (tmp_path / "data" / "a").write_bytes(b"changed")
    repo.cmd_add(["data/a"])
    repo.cmd_commit("incremental")
    version = latest(tmp_path)
    with db(tmp_path) as conn:
        nodes = [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM merkle_nodes WHERE version_id=?", (version["id"],)
            )
        ]
    entries = []
    hashes, counts = {}, {}
    for node in nodes:
        if node["relative_path"] == ".":
            continue
        entry = json.loads(node["metadata"])
        entry["_st_size"] = node["subtree_total_bytes"]
        entries.append(entry)
        hashes[node["relative_path"]] = node["content_hash_sha256"]
        counts[node["relative_path"]] = node["child_count"]
    fresh_root, _ = build_merkle_tree(entries, hashes, counts)
    assert fresh_root == version["root_tree_hash"]
    (tmp_path / "data" / "a").unlink()
    (tmp_path / "data" / "b").unlink()
    repo.cmd_restore(version["commit_hash"])
    assert (tmp_path / "data" / "a").read_bytes() == b"changed"
    assert (tmp_path / "data" / "b").read_bytes() == b"b"


def test_branch_commits_reuse_the_selected_parent(tmp_path):
    repo = Version(str(tmp_path))
    (tmp_path / "a").write_bytes(b"a")
    (tmp_path / "b").write_bytes(b"b")
    repo.cmd_add(["."])
    repo.cmd_commit("initial")
    repo.cmd_branch("feature")
    (tmp_path / "a").write_bytes(b"main")
    repo.cmd_add(["a"])
    repo.cmd_commit("main update")
    main = latest(tmp_path)
    repo.cmd_switch("feature")
    (tmp_path / "b").write_bytes(b"feature")
    repo.cmd_add(["b"])
    repo.cmd_commit("feature update")
    feature = latest(tmp_path)
    assert content(tmp_path, feature["commit_hash"], "a") == b"a"
    assert content(tmp_path, feature["commit_hash"], "b") == b"feature"
    repo.cmd_switch("main")
    assert (tmp_path / "a").read_bytes() == b"main"
    assert (tmp_path / "b").read_bytes() == b"b"
    with db(tmp_path) as conn:
        heads = {
            r["branch_name"]: r["head_commit_hash"]
            for r in conn.execute("SELECT * FROM branches")
        }
    assert heads == {"main": main["commit_hash"], "feature": feature["commit_hash"]}


def test_rehash_bypasses_fingerprint_cache(tmp_path, monkeypatch):
    repo = Version(str(tmp_path))
    (tmp_path / "a").write_bytes(b"content")
    repo.cmd_add(["a"])
    repo.cmd_commit("initial")
    first = latest(tmp_path)
    collect = Mock(wraps=dsi_vcs.collect_metadata)
    chunks = Mock(wraps=chunking.chunk_file)
    monkeypatch.setattr(dsi_vcs, "collect_metadata", collect)
    monkeypatch.setattr(chunking, "chunk_file", chunks)
    repo.cmd_add(["."])
    repo.cmd_commit("verify", rehash=True)
    assert collect.call_count == chunks.call_count == 1
    assert latest(tmp_path)["root_tree_hash"] == first["root_tree_hash"]


@pytest.mark.skipif(os.name == "nt", reason="POSIX ACL authorization")
@pytest.mark.parametrize(
    ("acl", "username", "groups", "access", "allowed"),
    [
        ("bob allow read", "bob", set(), "write", False),
        ("bob allow write", "bob", set(), "write", True),
        ("analysts allow read,write", "bob", {"analysts"}, "write", True),
        (None, "owner", set(), "write", True),
        (None, "bob", {"staff"}, "read", True),
        (None, "bob", {"staff"}, "write", False),
        (None, "bob", set(), "read", False),
    ],
)
def test_cached_identity_preserves_acl_and_mode_authorization(
    acl, username, groups, access, allowed
):
    stored = {
        "owner_name": "owner",
        "group_name": "staff",
        "permissions_int": 0o640,
        "acl_text": acl,
    }
    result = metadata.check_metadata_access(
        stored, access, (username, frozenset(groups))
    )
    assert (result is stored) == allowed
    if not allowed:
        assert result is None
