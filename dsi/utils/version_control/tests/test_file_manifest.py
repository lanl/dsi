"""Packed manifest boundaries, immutable history, integrity, and file restore."""

import hashlib
import sqlite3
import struct
from unittest.mock import Mock

import pytest

from dsi.utils.version_control.dsi_vcs import Version
from dsi.utils.version_control.repolog import chunking, manifest
from dsi.utils.version_control.vcs_db import DB_NAME, SNAPSHOTS_DIR


def chunk_references(count):
    return [
        (hashlib.sha256(f"chunk-{index}".encode()).hexdigest(), index + 1)
        for index in range(count)
    ]


def build_manifest(root, references, page_entries=3):
    builder = manifest.ManifestBuilder(str(root), page_entries=page_entries)
    for chunk_hash, size in references:
        builder.add(chunk_hash, size)
    return builder.finish()


def stored_objects(root):
    return {
        path.name: path.read_bytes()
        for path in (root / manifest.MANIFEST_STORAGE_DIR).iterdir()
    }


@pytest.mark.parametrize("count", [0, 1, 2, 3, 4, 6, 9, 10, 28])
def test_round_trip_across_leaf_and_internal_page_boundaries(tmp_path, count):
    references = chunk_references(count)
    result = build_manifest(tmp_path, references)

    assert list(manifest.iter_manifest_chunks(str(tmp_path), result.object_hash)) == references
    assert result.chunk_count == count
    assert result.byte_count == sum(size for _, size in references)
    for object_hash, payload in stored_objects(tmp_path).items():
        assert hashlib.sha256(payload).hexdigest() == object_hash


@pytest.mark.parametrize("count", [4095, 4096, 4097])
def test_default_page_size_boundary(tmp_path, count):
    references = chunk_references(count)
    builder = manifest.ManifestBuilder(str(tmp_path))
    for chunk_hash, size in references:
        builder.add(chunk_hash, size)
    result = builder.finish()

    assert list(manifest.iter_manifest_chunks(str(tmp_path), result.object_hash)) == references
    # One leaf up to the boundary; two leaves and a root after it.
    assert len(stored_objects(tmp_path)) == (1 if count <= 4096 else 3)


@pytest.mark.parametrize("page_entries", [-1, 0, 1])
def test_rejects_page_sizes_that_cannot_form_a_tree(tmp_path, page_entries):
    with pytest.raises(ValueError, match="at least two entries"):
        manifest.ManifestBuilder(str(tmp_path), page_entries=page_entries)


def test_repeated_chunks_keep_their_positions_and_large_sizes(tmp_path):
    digest = hashlib.sha256(b"repeated").hexdigest()
    references = [(digest, 2**32 + 17)] * 7
    result = build_manifest(tmp_path, references)

    assert result.chunk_count == 7
    assert result.byte_count == 7 * (2**32 + 17)
    assert list(manifest.iter_manifest_chunks(str(tmp_path), result.object_hash)) == references


@pytest.mark.parametrize("count", [0, 1, 6, 28])
def test_identical_manifests_reuse_objects_without_rewriting(tmp_path, monkeypatch, count):
    references = chunk_references(count)
    first = build_manifest(tmp_path, references)
    before = stored_objects(tmp_path)

    def unexpected_write(*args, **kwargs):
        pytest.fail("Identical manifest objects should reuse their existing files")

    monkeypatch.setattr(manifest.tempfile, "mkstemp", unexpected_write)
    assert build_manifest(tmp_path, references) == first
    assert stored_objects(tmp_path) == before


def test_single_edit_shares_unchanged_pages_and_preserves_old_version(tmp_path):
    original = chunk_references(6)
    first = build_manifest(tmp_path, original)
    before = stored_objects(tmp_path)
    edited = original.copy()
    edited[4] = (hashlib.sha256(b"replacement").hexdigest(), 99)
    second = build_manifest(tmp_path, edited)
    after = stored_objects(tmp_path)

    assert second.object_hash != first.object_hash
    # With two leaf pages, only the edited leaf and its root are new.
    assert len(after.keys() - before.keys()) == 2
    assert all(after[name] == payload for name, payload in before.items())
    assert list(manifest.iter_manifest_chunks(str(tmp_path), first.object_hash)) == original
    assert list(manifest.iter_manifest_chunks(str(tmp_path), second.object_hash)) == edited


def test_divergent_versions_resolve_their_own_chunk_sequences(tmp_path):
    original = chunk_references(10)
    left = original.copy()
    right = original.copy()
    left[0] = (hashlib.sha256(b"left branch").hexdigest(), 11)
    right[-1] = (hashlib.sha256(b"right branch").hexdigest(), 12)
    versions = [
        (build_manifest(tmp_path, references), references)
        for references in (original, left, right)
    ]

    assert len({root.object_hash for root, _ in versions}) == 3
    for root, expected in reversed(versions):
        assert list(manifest.iter_manifest_chunks(str(tmp_path), root.object_hash)) == expected


def test_iteration_reads_pages_on_demand(tmp_path, monkeypatch):
    references = chunk_references(6)
    root = build_manifest(tmp_path, references)
    read = Mock(wraps=manifest._read_object)
    monkeypatch.setattr(manifest, "_read_object", read)
    iterator = manifest.iter_manifest_chunks(str(tmp_path), root.object_hash)

    assert read.call_count == 0
    assert [next(iterator) for _ in range(3)] == references[:3]
    assert read.call_count == 2  # Root and first leaf only.
    assert next(iterator) == references[3]
    assert read.call_count == 3
    iterator.close()


@pytest.mark.parametrize("target", ["root", "child"])
@pytest.mark.parametrize("damage", ["modified", "missing"])
def test_detects_corrupt_or_missing_objects(tmp_path, target, damage):
    references = chunk_references(6)
    root = build_manifest(tmp_path, references)
    leaf = build_manifest(tmp_path, references[:3])
    object_hash = root.object_hash if target == "root" else leaf.object_hash
    path = tmp_path / manifest.MANIFEST_STORAGE_DIR / object_hash
    if damage == "missing":
        path.unlink()
        expected_error, message = FileNotFoundError, None
    else:
        payload = path.read_bytes()
        path.write_bytes(payload[:-1] + bytes([payload[-1] ^ 1]))
        expected_error, message = ValueError, "checksum mismatch"

    with pytest.raises(expected_error, match=message):
        list(manifest.iter_manifest_chunks(str(tmp_path), root.object_hash))


@pytest.mark.parametrize(
    "payload",
    [
        b"short header",
        struct.pack(">8scIQQ", b"BADMAGIC", b"L", 0, 0, 0),
        struct.pack(">8scIQQ", b"DSIMAN01", b"?", 0, 0, 0),
        struct.pack(">8scIQQ", b"DSIMAN01", b"L", 1, 1, 8),
        struct.pack(">8scIQQ", b"DSIMAN01", b"I", 1, 1, 8),
        struct.pack(">8scIQQ", b"DSIMAN01", b"L", 0, 0, 0) + b"trailing",
    ],
    ids=["short-header", "magic", "kind", "truncated-leaf", "truncated-internal", "trailing"],
)
def test_rejects_malformed_objects_even_with_valid_content_hash(tmp_path, payload):
    directory = tmp_path / manifest.MANIFEST_STORAGE_DIR
    directory.mkdir()
    object_hash = hashlib.sha256(payload).hexdigest()
    (directory / object_hash).write_bytes(payload)

    with pytest.raises(ValueError, match="Invalid manifest object"):
        list(manifest.iter_manifest_chunks(str(tmp_path), object_hash))


@pytest.mark.parametrize("operation", ["fsync", "replace"])
def test_failed_publication_cleans_temporary_files_and_can_be_retried(
    tmp_path, monkeypatch, operation
):
    def fail(*args, **kwargs):
        raise OSError("injected manifest publication failure")

    references = chunk_references(1)
    with monkeypatch.context() as patch:
        patch.setattr(manifest.os, operation, fail)
        with pytest.raises(OSError, match="injected manifest publication failure"):
            build_manifest(tmp_path, references)

    assert stored_objects(tmp_path) == {}
    result = build_manifest(tmp_path, references)
    assert list(manifest.iter_manifest_chunks(str(tmp_path), result.object_hash)) == references


def test_commit_history_restores_edits_insertions_truncation_and_empty_files(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(chunking, "MIN_CHUNK", 8)
    monkeypatch.setattr(chunking, "MAX_CHUNK", 8)
    # Small pages exercise manifest trees using tiny files and real commits.
    monkeypatch.setattr(
        chunking, "ManifestBuilder",
        lambda root: manifest.ManifestBuilder(root, page_entries=2),
    )
    repo = Version(str(tmp_path))
    path = tmp_path / "nested" / "data.bin"
    path.parent.mkdir()
    original = b"A" * 8 + b"B" * 8 + b"C" * 8 + b"D" * 8
    contents = [
        original,
        original,  # Rehashing unchanged content reuses the same manifest.
        b"A" * 8 + b"X" * 8 + b"C" * 8 + b"D" * 8,
        b"Z" * 8 + original,
        original[:8],
        b"",
        original,
    ]
    snapshots = tmp_path / SNAPSHOTS_DIR
    with sqlite3.connect(snapshots / DB_NAME) as conn:
        conn.row_factory = sqlite3.Row
        history = []
        for index, payload in enumerate(contents):
            path.write_bytes(payload)
            repo.cmd_add(["nested/data.bin"])
            repo.cmd_commit(f"manifest version {index}", rehash=True)
            row = conn.execute(
                "SELECT v.commit_hash, mn.manifest_hash, mn.content_hash_sha256, mn.child_count "
                "FROM versions v JOIN merkle_nodes mn ON mn.version_id=v.id "
                "WHERE mn.relative_path='nested/data.bin' ORDER BY v.id DESC LIMIT 1"
            ).fetchone()
            assert row["manifest_hash"]
            assert row["child_count"] == len(payload) // 8
            history.append((dict(row), payload))

        assert history[0][0]["manifest_hash"] == history[1][0]["manifest_hash"]
        assert history[0][0]["manifest_hash"] == history[-1][0]["manifest_hash"]
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='chunk_store'"
        ).fetchone() is None

        # Restore only after all commits exist, to catch accidental history mutation.
        for index, (version, expected) in enumerate(history):
            output = tmp_path / f"restored-{index}"
            assert chunking.rebuild_file_from_chunks(
                conn, str(snapshots), "nested/data.bin", version["commit_hash"][:12],
                version["content_hash_sha256"], str(output),
            )
            assert (output / "nested" / "data.bin").read_bytes() == expected
