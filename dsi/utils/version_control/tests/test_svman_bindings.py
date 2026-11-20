"""Native snapshots and historical v1/v2 reads through the public interface."""

from dsi.utils.version_control.storage_backend import sha256
import json
from pathlib import Path
import subprocess
import sys
from unittest.mock import Mock

import pytest

from dsi.utils.version_control.dsi_vcs import Version
from dsi.utils.version_control import storage_backend
from dsi.utils.version_control.repolog.merkle import _stable_metadata, commit_hash, hash_payload
from dsi.utils.version_control.vcs_db import open_db
from dsi.utils.version_control.vcs_metadata_helper import collect_metadata

try:
    from dsi.utils.version_control import _svman_core as native
except ImportError:
    native = pytest.importorskip("_svman_core")


def legacy_repo(tmp_path):
    """Create the storage and metadata layout produced by a v1 commit."""
    repo = Version(str(tmp_path))
    payload = b"legacy version content" * 2000
    source = tmp_path / "old"
    source.write_bytes(payload)
    chunk_hash = sha256(payload).hexdigest()
    file_hash = sha256(chunk_hash.encode("ascii")).hexdigest()
    metadata = json.dumps(_stable_metadata(collect_metadata(str(source), str(tmp_path)), "old"))
    metadata_hash = hash_payload({"object": "dsi-vcs-metadata-v1", "metadata": metadata})
    file_node = hash_payload({"object": "dsi-vcs-file-v1", "metadata_hash": metadata_hash,
                              "content_hash_sha256": file_hash})
    root_metadata = json.dumps(_stable_metadata({}, "."))
    root_metadata_hash = hash_payload({"object": "dsi-vcs-metadata-v1", "metadata": root_metadata})
    root_hash = hash_payload({"object": "dsi-vcs-tree-v1", "metadata_hash": root_metadata_hash,
                              "children": [{"name": "old", "file_type": "file", "node_hash": file_node}]})
    old = commit_hash(root_hash, None, 123, "owner", "legacy", 1, len(payload))
    chunk_dir = tmp_path / "__dsi_vcs" / "dsi_vcs_chunks"
    chunk_dir.mkdir()
    (chunk_dir / chunk_hash).write_bytes(payload)
    with open_db(str(tmp_path)) as conn:
        conn.executescript("""
            CREATE TABLE chunk_store (chunk_hash TEXT, chunk_size INTEGER, created_at INTEGER,
                                      commit_hash TEXT, relative_file_path TEXT, chunk_index INTEGER);
        """)
        conn.execute("INSERT INTO versions VALUES (1, ?, ?, ?, 'sha256-merkle-v1', 123, 'owner', 'legacy', 1, ?)",
                     (str(tmp_path), old, root_hash, len(payload)))
        conn.execute("INSERT INTO merkle_nodes (version_id,root_folder,relative_path,file_type,node_hash,metadata,"
                     "content_hash_sha256,subtree_file_count,subtree_total_bytes,child_count) "
                     "VALUES (1, ?, 'old', 'file', ?, ?, ?, 1, ?, 1)",
                     (str(tmp_path), file_node, metadata, file_hash, len(payload)))
        conn.execute("INSERT INTO merkle_nodes (version_id,root_folder,relative_path,file_type,node_hash,metadata,"
                     "subtree_file_count,subtree_total_bytes,child_count) VALUES (1, ?, '.', 'dir', ?, ?, 1, ?, 1)",
                     (str(tmp_path), root_hash, root_metadata, len(payload)))
        conn.execute("INSERT INTO chunk_store VALUES (?, ?, 0, ?, 'old', 0)", (chunk_hash, len(payload), old))
        conn.execute("INSERT INTO branches (root_folder,branch_name,head_commit_hash,tracked_commit_hash,is_latest,created_at) "
                     "VALUES (?, 'main', ?, ?, 1, 123)", (str(tmp_path), old, old))
        conn.execute("INSERT INTO branch_links (parent_commit_hash,child_commit_hash,child_branch_name,created_at) "
                     "VALUES (NULL, ?, 'main', 123)", (old,))
    return repo, old, payload, chunk_dir / chunk_hash


def test_snapshot_and_rebuild_preserve_binary_content(tmp_path):
    source = tmp_path / "input"
    content = bytes(range(256)) * 1024
    source.write_bytes(content)
    entry = native.FileEntry()
    entry.absolute_path, entry.relative_path = str(source), "nested/input"
    root = str(tmp_path / "storage")
    result = native.store_snapshot(root, [entry], {})
    assert native.rebuild_file(root, entry.relative_path, result.manifest_roots[entry.relative_path],
                               result.file_hashes[entry.relative_path], str(tmp_path / "output"))
    assert (tmp_path / "output/nested/input").read_bytes() == content


def test_v1_history_remains_unchanged_after_v2_commit(tmp_path):
    project_root = Path(__file__).resolve().parents[4]
    subprocess.run(["git", "clone", "--shared", str(project_root), str(tmp_path)], check=True,
                   capture_output=True, text=True)
    latest_branch = subprocess.run(["git", "branch", "--show-current"], cwd=tmp_path, check=True,
                                  capture_output=True, text=True).stdout.strip()
    subprocess.run(["git", "checkout", "9b422204c1f6f6433a33633fd48dfa63f44c96a"], cwd=tmp_path,
                   check=True, capture_output=True, text=True)

    repo = Version(str(tmp_path))
    first_file = tmp_path / "dsi-vcs-first.txt"
    first_file.write_text("first dsi-vcs snapshot\n")
    repo.cmd_add([first_file.name])
    repo.cmd_commit("first dsi-vcs commit")
    with open_db(str(tmp_path)) as conn:
        first_commit = conn.execute("SELECT commit_hash FROM versions ORDER BY id DESC LIMIT 1").fetchone()[0]

    subprocess.run(["git", "checkout", latest_branch], cwd=tmp_path, check=True, capture_output=True, text=True)
    first_file.write_text("updated after returning to the latest Git branch\n")
    second_file = tmp_path / "dsi-vcs-second.txt"
    second_file.write_text("second dsi-vcs snapshot\n")
    original_file = next(path for path in (tmp_path / "dsi").rglob("*.py") if path.is_file())
    original_file.write_text(original_file.read_text() + "\n# updated for the second snapshot\n")
    repo.cmd_add([first_file.name, second_file.name, str(original_file.relative_to(tmp_path))])
    repo.cmd_commit("second dsi-vcs commit")

    repo.cmd_restore(first_commit)
    assert first_file.read_text() == "first dsi-vcs snapshot\n"
