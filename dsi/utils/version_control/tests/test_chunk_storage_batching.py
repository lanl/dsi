"""Packed manifest publication, payload deduplication, and commit rollback."""

import builtins
import hashlib
from pathlib import Path

import pytest

from dsi.utils.version_control import dsi_vcs
from dsi.utils.version_control.repolog import chunking
from dsi.utils.version_control.repolog import manifest
from dsi.utils.version_control.repolog.manifest import iter_manifest_chunks


@pytest.fixture(autouse=True)
def small_chunks(monkeypatch):
    monkeypatch.setattr(chunking, "MIN_CHUNK", 8)
    monkeypatch.setattr(chunking, "MAX_CHUNK", 8)


def entry(path):
    return {"absolute_path": str(path), "relative_path": path.name, "file_type": "file"}


def manifest_content(root, manifest_hash):
    return b"".join(
        (root / chunking.CHUNK_STORAGE_DIR / chunk_hash).read_bytes()
        for chunk_hash, _chunk_size in iter_manifest_chunks(str(root), manifest_hash)
    )


def test_manifests_cover_files_and_preserve_chunk_order(tmp_path):
    paths = [tmp_path / f"file-{i}.bin" for i in range(2)]
    for i, path in enumerate(paths):
        path.write_bytes(builtins.bytes(range(i * 40, (i + 1) * 40)))
    manifest_roots = {}
    file_hashes, hashes, lengths = chunking.store_chunks_for_snapshot(
        None, str(tmp_path), [entry(p) for p in paths], manifest_roots=manifest_roots,
    )
    assert len(hashes) == 10
    for path in paths:
        assert lengths[path.name] == 5
        assert manifest_roots[path.name]
        assert manifest_content(tmp_path, manifest_roots[path.name]) == path.read_bytes()
        hashes_from_manifest = [
            chunk_hash for chunk_hash, _chunk_size in iter_manifest_chunks(
                str(tmp_path), manifest_roots[path.name]
            )
        ]
        assert hashlib.sha256("".join(hashes_from_manifest).encode()).hexdigest() == file_hashes[path.name]


def test_empty_files_get_an_empty_manifest_and_nonfiles_are_skipped(tmp_path):
    path = tmp_path / "empty.bin"
    path.write_bytes(b"")
    directory = {"absolute_path": str(tmp_path), "relative_path": ".", "file_type": "dir"}
    manifest_roots = {}
    hashes, chunks, lengths = chunking.store_chunks_for_snapshot(
        None, str(tmp_path), [directory, entry(path)], manifest_roots=manifest_roots,
    )
    assert hashes == {path.name: hashlib.sha256().hexdigest()}
    assert chunks == set()
    assert lengths == {path.name: 0}
    assert list(iter_manifest_chunks(str(tmp_path), manifest_roots[path.name])) == []


def track_chunk_lookups(monkeypatch, chunk_dir):
    looked_up = []
    real_exists = chunking.os.path.exists

    def exists(path):
        path = Path(path)
        if path.parent == chunk_dir and len(path.name) == 64:
            looked_up.append(path.name)
        return real_exists(path)

    monkeypatch.setattr(chunking.os.path, "exists", exists)
    return looked_up


def test_existence_cache_reuses_existing_and_just_written_chunks_across_files(tmp_path, monkeypatch):
    a, b = b"a" * 8, b"b" * 8
    chunk_dir = tmp_path / chunking.CHUNK_STORAGE_DIR
    chunk_dir.mkdir()
    (chunk_dir / hashlib.sha256(a).hexdigest()).write_bytes(a)
    paths = [tmp_path / f"file-{i}.bin" for i in range(2)]
    for path in paths:
        path.write_bytes(a + b + a + b)
    lookups = track_chunk_lookups(monkeypatch, chunk_dir)
    copies = []

    def copy(view):
        copies.append(len(view))
        return builtins.bytes(view)

    monkeypatch.setattr(chunking, "bytes", copy, raising=False)
    manifest_roots = {}
    chunking.store_chunks_for_snapshot(None, str(tmp_path), [entry(p) for p in paths], manifest_roots=manifest_roots)
    assert lookups == [hashlib.sha256(a).hexdigest(), hashlib.sha256(b).hexdigest()]
    assert copies == [8]
    assert manifest_roots[paths[0].name] == manifest_roots[paths[1].name]


def test_existence_cache_is_bounded_and_updates_recency(tmp_path, monkeypatch):
    monkeypatch.setattr(chunking, "CHUNK_EXISTENCE_CACHE_SIZE", 2)
    chunk_dir = tmp_path / chunking.CHUNK_STORAGE_DIR
    chunk_dir.mkdir()
    payloads = [b"a" * 8, b"b" * 8, b"c" * 8]
    hashes = [hashlib.sha256(p).hexdigest() for p in payloads]
    for digest, payload in zip(hashes, payloads):
        (chunk_dir / digest).write_bytes(payload)
    path = tmp_path / "file.bin"
    path.write_bytes(b"".join(payloads[i] for i in [0, 1, 0, 2, 0, 1]))
    lookups = track_chunk_lookups(monkeypatch, chunk_dir)
    chunking.store_chunks_for_snapshot(None, str(tmp_path), [entry(path)])
    assert lookups == [hashes[i] for i in [0, 1, 2, 1]]


def test_existence_cache_does_not_survive_storage_calls(tmp_path, monkeypatch):
    path = tmp_path / "file.bin"
    path.write_bytes(b"a" * 8)
    chunk_dir = tmp_path / chunking.CHUNK_STORAGE_DIR
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    lookups = track_chunk_lookups(monkeypatch, chunk_dir)
    chunking.store_chunks_for_snapshot(None, str(tmp_path), [entry(path)])
    (chunk_dir / digest).unlink()
    chunking.store_chunks_for_snapshot(None, str(tmp_path), [entry(path)])
    assert lookups == [digest, digest]
    assert (chunk_dir / digest).read_bytes() == path.read_bytes()


def test_commit_rolls_back_manifest_publication_failure_and_preserves_staging(tmp_path, monkeypatch):
    path = tmp_path / "file.bin"
    path.write_bytes(b"original")
    repo = dsi_vcs.Version(str(tmp_path))
    repo.cmd_add([path.name])
    repo.cmd_commit("initial")
    path.write_bytes(builtins.bytes(range(40)))
    repo.cmd_add([path.name])
    with monkeypatch.context() as patch:
        patch.setattr(manifest, "_publish_object", lambda *_args: (_ for _ in ()).throw(OSError("injected manifest failure")))
        with pytest.raises(OSError, match="injected manifest failure"):
            repo.cmd_commit("failed")
    with dsi_vcs.open_db(str(tmp_path)) as db:
        assert db.execute("SELECT count(*) FROM versions").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM merkle_nodes WHERE manifest_hash IS NOT NULL").fetchone()[0] == 1
    restarted = dsi_vcs.Version(str(tmp_path))
    assert path.name in restarted._load_pending_stage_entries()
    restarted.cmd_commit("retry")
    with dsi_vcs.open_db(str(tmp_path)) as db:
        assert db.execute("SELECT count(*) FROM versions").fetchone()[0] == 2
        commit = db.execute("SELECT commit_hash FROM versions ORDER BY id DESC LIMIT 1").fetchone()[0]
    restarted.cmd_restore(commit)
    assert path.read_bytes() == builtins.bytes(range(40))
