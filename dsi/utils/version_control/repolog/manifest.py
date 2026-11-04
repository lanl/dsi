"""Immutable packed manifests for versioned file chunk sequences.

Manifest objects are content-addressed binary pages.  Leaf pages hold ordered
``(chunk SHA-256, byte length)`` pairs; internal pages hold child page hashes
and aggregate counts.  A file-version points at one root object, so chunk
metadata never requires one database row per chunk.
"""

import hashlib
import os
import struct
import tempfile
from dataclasses import dataclass
from typing import Iterable, Iterator


MANIFEST_STORAGE_DIR = "dsi_vcs_manifests"
MANIFEST_PAGE_ENTRIES = 4096
_MAGIC = b"DSIMAN01"
_HEADER = struct.Struct(">8scIQQ")  # magic, kind, entries, chunks below, bytes below
_LEAF_ENTRY = struct.Struct(">32sQ")
_NODE_ENTRY = struct.Struct(">32sQQ")


@dataclass(frozen=True)
class ManifestRef:
    object_hash: str
    chunk_count: int
    byte_count: int


def _object_path(manifest_dir: str, object_hash: str) -> str:
    return os.path.join(manifest_dir, object_hash)


def _publish_object(manifest_dir: str, payload: bytes) -> str:
    object_hash = hashlib.sha256(payload).hexdigest()
    destination = _object_path(manifest_dir, object_hash)
    if os.path.exists(destination):
        return object_hash

    fd, temporary = tempfile.mkstemp(dir=manifest_dir, prefix=f".{object_hash}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as handle:
            fd = None
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        # Competing writers produce identical content under the same object id.
        os.replace(temporary, destination)
    finally:
        if fd is not None:
            os.close(fd)
        if os.path.exists(temporary):
            os.unlink(temporary)
    return object_hash


def _leaf_payload(entries: list[tuple[str, int]]) -> bytes:
    total_bytes = sum(size for _, size in entries)
    payload = bytearray(_HEADER.pack(_MAGIC, b"L", len(entries), len(entries), total_bytes))
    for chunk_hash, size in entries:
        payload.extend(_LEAF_ENTRY.pack(bytes.fromhex(chunk_hash), size))
    return bytes(payload)


def _node_payload(children: list[ManifestRef]) -> bytes:
    chunk_count = sum(child.chunk_count for child in children)
    byte_count = sum(child.byte_count for child in children)
    payload = bytearray(_HEADER.pack(_MAGIC, b"I", len(children), chunk_count, byte_count))
    for child in children:
        payload.extend(
            _NODE_ENTRY.pack(bytes.fromhex(child.object_hash), child.chunk_count, child.byte_count)
        )
    return bytes(payload)


class ManifestBuilder:
    """Build a packed manifest while chunks are streamed from a source file."""

    def __init__(self, chunk_root: str, page_entries: int = MANIFEST_PAGE_ENTRIES):
        if page_entries < 2:
            raise ValueError("Manifest pages must hold at least two entries")
        self.manifest_dir = os.path.join(chunk_root, MANIFEST_STORAGE_DIR)
        os.makedirs(self.manifest_dir, exist_ok=True)
        self.page_entries = page_entries
        self._leaf_entries: list[tuple[str, int]] = []
        self._leaves: list[ManifestRef] = []

    def add(self, chunk_hash: str, chunk_size: int) -> None:
        self._leaf_entries.append((chunk_hash, chunk_size))
        if len(self._leaf_entries) == self.page_entries:
            self._finish_leaf()

    def _finish_leaf(self) -> None:
        if not self._leaf_entries and self._leaves:
            return
        entries = self._leaf_entries
        payload = _leaf_payload(entries)
        self._leaves.append(
            ManifestRef(_publish_object(self.manifest_dir, payload), len(entries), sum(size for _, size in entries))
        )
        self._leaf_entries = []

    def finish(self) -> ManifestRef:
        self._finish_leaf()  # Also materializes a stable empty-file leaf.
        level = self._leaves
        while len(level) > 1:
            next_level = []
            for start in range(0, len(level), self.page_entries):
                children = level[start:start + self.page_entries]
                payload = _node_payload(children)
                next_level.append(
                    ManifestRef(
                        _publish_object(self.manifest_dir, payload),
                        sum(child.chunk_count for child in children),
                        sum(child.byte_count for child in children),
                    )
                )
            level = next_level
        return level[0]


def _read_object(manifest_dir: str, object_hash: str) -> bytes:
    with open(_object_path(manifest_dir, object_hash), "rb") as handle:
        payload = handle.read()
    if hashlib.sha256(payload).hexdigest() != object_hash:
        raise ValueError(f"Manifest object checksum mismatch: {object_hash}")
    return payload


def iter_manifest_chunks(chunk_root: str, manifest_hash: str) -> Iterator[tuple[str, int]]:
    """Yield a manifest's ordered chunk references without loading its tree at once."""
    manifest_dir = os.path.join(chunk_root, MANIFEST_STORAGE_DIR)

    def walk(object_hash: str) -> Iterator[tuple[str, int]]:
        payload = _read_object(manifest_dir, object_hash)
        if len(payload) < _HEADER.size:
            raise ValueError(f"Invalid manifest object: {object_hash}")
        magic, kind, entry_count, _chunks, _bytes = _HEADER.unpack_from(payload)
        if magic != _MAGIC or kind not in (b"L", b"I"):
            raise ValueError(f"Invalid manifest object: {object_hash}")
        entry_size = _LEAF_ENTRY.size if kind == b"L" else _NODE_ENTRY.size
        if len(payload) != _HEADER.size + entry_count * entry_size:
            raise ValueError(f"Invalid manifest object length: {object_hash}")
        offset = _HEADER.size
        if kind == b"L":
            for _ in range(entry_count):
                chunk_hash, size = _LEAF_ENTRY.unpack_from(payload, offset)
                offset += _LEAF_ENTRY.size
                yield chunk_hash.hex(), size
        else:
            for _ in range(entry_count):
                child_hash, _child_chunks, _child_bytes = _NODE_ENTRY.unpack_from(payload, offset)
                offset += _NODE_ENTRY.size
                yield from walk(child_hash.hex())

    yield from walk(manifest_hash)


