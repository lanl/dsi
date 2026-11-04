"""Duplicate mmap chunks need hashes and references, but no payload copies."""

import builtins
import hashlib
from pathlib import Path

import numpy as np
import pytest

from dsi.utils.version_control.repolog import chunking
from dsi.utils.version_control.repolog.content_chunking import mapped_chunking, npy_chunking
from dsi.utils.version_control.repolog.manifest import (
    MANIFEST_STORAGE_DIR,
    iter_manifest_chunks,
)


@pytest.fixture(params=["binary", "npy"])
def source(request, tmp_path, monkeypatch):
    if request.param == "binary":
        monkeypatch.setattr(chunking, "MIN_CHUNK", 128)
        monkeypatch.setattr(chunking, "MAX_CHUNK", 128)
        path = tmp_path / "input.bin"
        path.write_bytes(np.random.default_rng(42).bytes(2048))
        module = chunking
        edit_offset = 256
    else:
        path = tmp_path / "input.npy"
        np.save(path, np.arange(1024, dtype=np.uint16))
        original_iterator = npy_chunking.iter_npy_file

        def small_chunks(path, **kwargs):
            return original_iterator(path, target_chunk_size=128, **kwargs)

        monkeypatch.setattr(chunking, "iter_npy_file", small_chunks)
        # File-level storage currently uses the generic FastCDC stream.  The
        # NPY-specific iterator is covered directly below.
        module = chunking
        edit_offset = npy_chunking._read_npy_metadata(path)["data_offset"] + 256
    return path, module, edit_offset


def manifest_content(root, manifest_hash):
    return b"".join(
        (root / chunking.CHUNK_STORAGE_DIR / chunk_hash).read_bytes()
        for chunk_hash, _chunk_size in iter_manifest_chunks(str(root), manifest_hash)
    )


def test_duplicate_and_edited_chunks_only_copy_missing_payloads(source, tmp_path, monkeypatch):
    path, module, edit_offset = source
    copied_sizes = []

    def copy(view):
        copied_sizes.append(len(view))
        return builtins.bytes(view)

    monkeypatch.setattr(module, "bytes", copy, raising=False)
    entries = [{"absolute_path": str(path), "relative_path": path.name, "file_type": "file"}]
    initial_roots = {}
    initial, hashes, lengths = chunking.store_chunks_for_snapshot(
        None, str(tmp_path), entries, manifest_roots=initial_roots,
    )
    original = path.read_bytes()
    assert sum(copied_sizes) == path.stat().st_size
    stored_files = set((tmp_path / chunking.CHUNK_STORAGE_DIR).iterdir())

    copied_sizes.clear()
    duplicate_roots = {}
    duplicate, duplicate_hashes, duplicate_lengths = chunking.store_chunks_for_snapshot(
        None, str(tmp_path), entries, manifest_roots=duplicate_roots,
    )
    assert copied_sizes == []
    assert (duplicate, duplicate_hashes, duplicate_lengths) == (initial, hashes, lengths)
    assert duplicate_roots[path.name] == initial_roots[path.name]
    assert set((tmp_path / chunking.CHUNK_STORAGE_DIR).iterdir()) == stored_files

    with path.open("r+b") as handle:
        handle.seek(edit_offset)
        old = handle.read(1)
        handle.seek(edit_offset)
        handle.write(builtins.bytes([old[0] ^ 1]))
    edited_roots = {}
    edited, _, _ = chunking.store_chunks_for_snapshot(
        None, str(tmp_path), entries, manifest_roots=edited_roots,
    )
    expected_copy = 128 if path.suffix == ".bin" else path.stat().st_size
    assert copied_sizes == [expected_copy]
    assert edited != initial

    assert manifest_content(tmp_path, duplicate_roots[path.name]) == original
    assert manifest_content(tmp_path, edited_roots[path.name]) == path.read_bytes()


def test_identical_files_in_one_snapshot_only_copy_payload_once(source, tmp_path, monkeypatch):
    path, module, _ = source
    copied_sizes = []

    def copy(view):
        copied_sizes.append(len(view))
        return builtins.bytes(view)

    monkeypatch.setattr(module, "bytes", copy, raising=False)
    entries = [
        {"absolute_path": str(path), "relative_path": name, "file_type": "file"}
        for name in ("first", "second")
    ]
    manifest_roots = {}
    file_hashes, _, lengths = chunking.store_chunks_for_snapshot(
        None, str(tmp_path), entries, manifest_roots=manifest_roots,
    )
    assert sum(copied_sizes) == path.stat().st_size
    assert file_hashes["first"] == file_hashes["second"]
    assert lengths["first"] == lengths["second"]
    assert manifest_roots["first"] == manifest_roots["second"]
    assert len(list((tmp_path / MANIFEST_STORAGE_DIR).iterdir())) == 1


@pytest.mark.parametrize("failure", ["lookup", "write"])
def test_storage_failure_closes_mapping(source, tmp_path, monkeypatch, failure):
    path, module, _ = source
    mappings = []
    real_mmap = chunking.mmap.mmap

    def track(*args, **kwargs):
        mapped = real_mmap(*args, **kwargs)
        mappings.append(mapped)
        return mapped

    def fail(*args, **kwargs):
        raise RuntimeError("injected failure")

    monkeypatch.setattr(chunking.mmap, "mmap", track)
    if failure == "lookup":
        real_exists = chunking.os.path.exists

        def fail_chunk_lookup(path):
            if len(Path(path).name) == 64:
                fail()
            return real_exists(path)

        monkeypatch.setattr(chunking.os.path, "exists", fail_chunk_lookup)
    elif failure == "write":
        monkeypatch.setattr(chunking.os, "fsync", fail)
    with pytest.raises(RuntimeError, match="injected failure"):
        chunking.store_chunks_for_snapshot(None, str(tmp_path), [
            {"absolute_path": str(path), "relative_path": path.name, "file_type": "file"}
        ])
    assert len(mappings) == 1
    assert all(mapped.closed for mapped in mappings)
    assert not list((tmp_path / chunking.CHUNK_STORAGE_DIR).glob("*.tmp"))


@pytest.mark.parametrize("array", [np.arange(30).reshape(5, 6), np.asfortranarray(np.arange(30).reshape(5, 6)), np.empty((0, 5)), np.array(7)])
def test_npy_public_api_and_stream_have_identical_bytes(tmp_path, array):
    path = tmp_path / "array.npy"
    np.save(path, array)
    chunks = npy_chunking.chunk_npy_file(path, target_chunk_size=32)
    assert chunks == list(npy_chunking.iter_npy_file(path, target_chunk_size=32))
    assert b"".join(c["data"] for c in chunks) == path.read_bytes()
    assert all(type(c["data"]) is builtins.bytes for c in chunks)
    assert all(c["sha256"] == hashlib.sha256(c["data"]).hexdigest() for c in chunks)


@pytest.mark.parametrize("payload", [b"\x93NUMPYbroken", None])
def test_unsupported_npy_stream_falls_back_to_fastcdc(tmp_path, payload):
    path = tmp_path / "array.npy"
    if payload is None:
        np.save(path, np.array([{"a": 1}], dtype=object))
    else:
        path.write_bytes(payload)
    assert list(chunking.chunk_file(path, streaming=True)) == chunking.chunk_file_fastcdc(path)


def test_npy_stream_is_lazy_and_closes_early(tmp_path, monkeypatch):
    path = tmp_path / "array.npy"
    np.save(path, np.arange(100))
    made = []
    mappings = []
    real_mmap = mapped_chunking.mmap.mmap

    def copy(view):
        made.append(len(view))
        return builtins.bytes(view)

    def track(*args, **kwargs):
        mapped = real_mmap(*args, **kwargs)
        mappings.append(mapped)
        return mapped

    monkeypatch.setattr(mapped_chunking, "bytes", copy, raising=False)
    monkeypatch.setattr(mapped_chunking.mmap, "mmap", track)
    iterator = npy_chunking.iter_npy_file(path)
    assert made == []
    assert all(mapped.closed for mapped in mappings)
    header = next(iterator)
    assert len(made) == 1
    assert not mappings[-1].closed
    iterator.close()
    assert len(mappings) == 2
    assert all(mapped.closed for mapped in mappings)
    assert header["data"].startswith(npy_chunking.NPY_MAGIC)


@pytest.mark.parametrize("order,payload_sizes", [("C", [96, 96, 48]), ("F", [120, 120])])
def test_npy_ranges_preserve_array_slab_alignment(tmp_path, order, payload_sizes):
    path = tmp_path / "array.npy"
    array = np.array(np.arange(30, dtype=np.int64).reshape(5, 6), order=order)
    np.save(path, array)
    chunks = npy_chunking.chunk_npy_file(path, target_chunk_size=128)
    header_size = npy_chunking._read_npy_metadata(path)["data_offset"]
    assert [c["size"] for c in chunks] == [header_size, *payload_sizes]
    assert b"".join(c["data"] for c in chunks) == path.read_bytes()
