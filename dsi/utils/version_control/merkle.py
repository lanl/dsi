import hashlib
import json
import os
from typing import Any, Optional


HASH_ALGORITHM = "sha256-merkle-v1"


def _canonical_json(data: dict[str, Any]) -> bytes:
    return json.dumps(data, sort_keys=True, separators=(",", ":")).encode("utf-8")


def hash_payload(data: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(data)).hexdigest()


def sha256_file(path: str, chunk_size: int = 1 << 20) -> Optional[str]:
    h = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            while chunk := f.read(chunk_size):
                h.update(chunk)
        return h.hexdigest()
    except (PermissionError, OSError):
        return None


def parent_path(relative_path: str) -> str:
    parent = os.path.dirname(relative_path)
    return parent if parent else "."


def _stable_metadata(entry: dict[str, Any], relative_path: str) -> dict[str, Any]:
    if relative_path == ".":
        return {
            "relative_path": ".",
            "file_type": "dir",
            "root": True,
        }

    return {
        "relative_path": relative_path,
        "file_name": os.path.basename(relative_path),
        "file_type": entry.get("file_type"),
        "permissions_int": entry.get("permissions_int"),
        "owner_name": entry.get("owner_name"),
        "group_name": entry.get("group_name"),
        "acl_text": entry.get("acl_text"),
        "xattrs": entry.get("xattrs"),
        "security_context": entry.get("security_context"),
        "symlink_target": entry.get("symlink_target"),
    }


def build_merkle_tree(entries: list[dict[str, Any]], snapshot_path: str) -> tuple[str, list[dict[str, Any]]]:
    entries_by_path = {entry["relative_path"]: entry for entry in entries}
    children_by_parent: dict[str, list[tuple[str, str]]] = {".": []}

    for relative_path in entries_by_path:
        parent = parent_path(relative_path)
        children_by_parent.setdefault(parent, []).append((os.path.basename(relative_path), relative_path))
        children_by_parent.setdefault(relative_path, [])

    nodes: dict[str, dict[str, Any]] = {}

    def build_node(relative_path: str) -> dict[str, Any]:
        if relative_path in nodes:
            return nodes[relative_path]

        entry = entries_by_path.get(relative_path, {})
        file_type = "dir" if relative_path == "." else entry.get("file_type")
        metadata_hash = hash_payload(
            {
                "object": "dsi-vcs-metadata-v1",
                "metadata": _stable_metadata(entry, relative_path),
            }
        )
        child_rows = sorted(children_by_parent.get(relative_path, []))
        content_hash = None
        subtree_file_count = 0
        subtree_total_bytes = 0

        if file_type == "dir":
            child_payload = []
            for name, child_path in child_rows:
                child = build_node(child_path)
                subtree_file_count += child["subtree_file_count"]
                subtree_total_bytes += child["subtree_total_bytes"]
                child_payload.append(
                    {
                        "name": name,
                        "file_type": child["file_type"],
                        "node_hash": child["node_hash"],
                    }
                )
            node_hash = hash_payload(
                {
                    "object": "dsi-vcs-tree-v1",
                    "metadata_hash": metadata_hash,
                    "children": child_payload,
                }
            )
        elif file_type == "file":
            content_hash = sha256_file(os.path.join(snapshot_path, relative_path))
            subtree_file_count = 1
            subtree_total_bytes = entry.get("_st_size") or 0
            node_hash = hash_payload(
                {
                    "object": "dsi-vcs-file-v1",
                    "metadata_hash": metadata_hash,
                    "content_hash_sha256": content_hash,
                }
            )
        else:
            node_hash = hash_payload(
                {
                    "object": "dsi-vcs-special-v1",
                    "file_type": file_type,
                    "metadata_hash": metadata_hash,
                }
            )

        node = {
            "relative_path": relative_path,
            "file_type": file_type,
            "node_hash": node_hash,
            "metadata_hash": metadata_hash,
            "content_hash_sha256": content_hash,
            "subtree_file_count": subtree_file_count,
            "subtree_total_bytes": subtree_total_bytes,
            "child_count": len(child_rows),
        }
        nodes[relative_path] = node
        return node

    root = build_node(".")
    return root["node_hash"], [nodes[path] for path in sorted(nodes)]


def commit_hash(
    root_tree_hash: str,
    parent_commit_hash: Optional[str],
    committed_at: str,
    owner_name: str,
    message: str,
    file_count: int,
    total_bytes: int,
) -> str:
    return hash_payload(
        {
            "object": "dsi-vcs-commit-v1",
            "hash_algorithm": HASH_ALGORITHM,
            "root_tree_hash": root_tree_hash,
            "parent_commit_hash": parent_commit_hash,
            "committed_at": committed_at,
            "owner_name": owner_name,
            "message": message or "",
            "file_count": file_count,
            "total_bytes": total_bytes,
        }
    )
