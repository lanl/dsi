"""Packed manifest publication, payload deduplication, and commit rollback."""

import builtins
from dsi.utils.version_control.storage_backend import sha256
from pathlib import Path

import pytest

from dsi.utils.version_control import dsi_vcs
from dsi.utils.version_control.repolog import chunking


@pytest.fixture(autouse=True)
def small_chunks(monkeypatch):
    monkeypatch.setattr(chunking, "MIN_CHUNK", 8)
    monkeypatch.setattr(chunking, "MAX_CHUNK", 8)


def entry(path):
    return {"absolute_path": str(path), "relative_path": path.name, "file_type": "file"}


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
    (chunk_dir / sha256(a).hexdigest()).write_bytes(a)
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
    assert lookups == [sha256(a).hexdigest(), sha256(b).hexdigest()]
    assert copies == [8]
    assert manifest_roots[paths[0].name] == manifest_roots[paths[1].name]


def test_existence_cache_is_bounded_and_updates_recency(tmp_path, monkeypatch):
    monkeypatch.setattr(chunking, "CHUNK_EXISTENCE_CACHE_SIZE", 2)
    chunk_dir = tmp_path / chunking.CHUNK_STORAGE_DIR
    chunk_dir.mkdir()
    payloads = [b"a" * 8, b"b" * 8, b"c" * 8]
    hashes = [sha256(p).hexdigest() for p in payloads]
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
    digest = sha256(path.read_bytes()).hexdigest()
    lookups = track_chunk_lookups(monkeypatch, chunk_dir)
    chunking.store_chunks_for_snapshot(None, str(tmp_path), [entry(path)])
    (chunk_dir / digest).unlink()
    chunking.store_chunks_for_snapshot(None, str(tmp_path), [entry(path)])
    assert lookups == [digest, digest]
    assert (chunk_dir / digest).read_bytes() == path.read_bytes()

