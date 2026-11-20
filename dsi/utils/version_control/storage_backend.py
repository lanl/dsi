"""Native snapshot storage and version-specific historical reconstruction."""

import os
import re
from .repolog.chunking import rebuild_file_from_chunks


_ROOT_HASH = re.compile(r"[0-9a-f]{64}\Z")


def _native():
    try:
        from . import _svman_core
    except ImportError as exc:
        raise RuntimeError("The dsi-vcs native storage extension is not installed") from exc
    return _svman_core


def store_snapshot(chunk_root, entries, manifest_roots):
    extension = _native()
    files = []
    base_roots = {}
    for item in entries:
        if item.get("file_type") != "file":
            continue
        entry = extension.FileEntry()
        entry.absolute_path = item["absolute_path"]
        entry.relative_path = item["relative_path"]
        entry.file_type = "file"
        files.append(entry)
        root = manifest_roots.get(entry.relative_path)
        if root:
            base_roots[entry.relative_path] = root
    result = extension.store_snapshot(chunk_root, files, base_roots)
    manifest_roots.update(result.manifest_roots)
    return result.file_hashes, result.chunk_counts


def rebuild_file(conn, chunk_root, relative_path, commit_hash, file_hash, manifest_hash, output_path):
    if manifest_hash is None or not manifest_hash or not _ROOT_HASH.fullmatch(manifest_hash):
        return rebuild_file_from_chunks(conn, chunk_root, relative_path, commit_hash, file_hash, output_path)
    target = os.path.abspath(os.path.join(output_path, relative_path))
    if os.path.commonpath([os.path.abspath(output_path), target]) != os.path.abspath(output_path):
        raise ValueError(f"Snapshot path escapes snapshot root: {relative_path}")
    return _native().rebuild_file(chunk_root, relative_path, manifest_hash, file_hash, output_path)
