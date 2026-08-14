"""Read-only FUSE view of a single dsi-vcs commit.

Unlike cmd_restore (which reconstructs every tracked file into the
working tree by concatenating its chunks to disk), this mounts a
virtual directory where reads are served directly out of the chunk
store on demand. A file that is O(TB) never gets created as a
contiguous blob~---~only the byte ranges an application reads are
pulled from `.dsi_vcs_chunks/<hash>`.

Requires the optional `pyfuse3` + `trio` dependencies (not needed for any
other dsi-vcs command), and libfuse3 + fusermount3 on the host.
"""

import bisect
import errno
import json
import os
import stat
from typing import Optional

import pyfuse3
import trio


class _Node:
    __slots__ = (
        "relative_path",
        "file_type",
        "mode",
        "size",
        "children",
        "chunk_offsets",
        "chunk_hashes",
    )

    def __init__(self, relative_path: str, file_type: str):
        self.relative_path = relative_path
        self.file_type = file_type
        self.mode = 0o600
        self.size = 0
        self.children: list[str] = []
        self.chunk_offsets: list[int] = []
        self.chunk_hashes: list[str] = []


class CommitMount(pyfuse3.Operations):
    """Serves a single, fixed commit as a read-only tree. One instance per mount."""

    def __init__(self, conn, root_folder: str, commit_hash: str, version_id: int,
                 committed_at_ns: int, chunk_dir: str):
        super().__init__()
        self._chunk_dir = chunk_dir
        self._committed_at_ns = committed_at_ns
        self._nodes: dict[str, _Node] = {}
        self._inode_to_path: dict[int, str] = {pyfuse3.ROOT_INODE: "."}
        self._path_to_inode: dict[str, int] = {".": pyfuse3.ROOT_INODE}
        self._next_inode = pyfuse3.ROOT_INODE + 1

        self._load_tree(conn, commit_hash, version_id)

    # Tree construction (once, at mount time)
    def _load_tree(self, conn, commit_hash: str, version_id: int) -> None:
        root = _Node(".", "dir")
        root.mode = 0o755
        self._nodes["."] = root

        rows = conn.execute(
            "SELECT relative_path, file_type, metadata, subtree_total_bytes "
            "FROM merkle_nodes WHERE version_id=? AND relative_path <> '.'",
            (version_id,),
        ).fetchall()

        for row in rows:
            rel_path = row["relative_path"]
            node = _Node(rel_path, row["file_type"])
            metadata = json.loads(row["metadata"]) if row["metadata"] else {}
            default_mode = 0o700 if row["file_type"] == "dir" else 0o600
            node.mode = int(metadata.get("permissions_int") or default_mode) & 0o7777
            node.size = row["subtree_total_bytes"] or 0
            self._nodes[rel_path] = node

        ''''
        Link every node to its parent. merkle_nodes always contains a row
        for every ancestor directory (build_node() in merkle.py recurses
        top-down from "."), so parents are guaranteed to already exist here.
        '''
        for rel_path, node in self._nodes.items():
            if rel_path == ".":
                continue
            parent = os.path.dirname(rel_path) or "."
            self._nodes[parent].children.append(rel_path)

        '''
        Resolve each file's ordered chunk list once, up front, so read()
        is a pure bisect + seek with no per-call SQL.
        '''
        for rel_path, node in self._nodes.items():
            if node.file_type != "file":
                continue
            chunk_rows = conn.execute(
                "SELECT chunk_hash, chunk_size FROM chunk_store "
                "WHERE commit_hash=? AND relative_file_path=? ORDER BY chunk_index",
                (commit_hash, rel_path),
            ).fetchall()
            offset = 0
            for crow in chunk_rows:
                node.chunk_offsets.append(offset)
                node.chunk_hashes.append(crow["chunk_hash"])
                offset += crow["chunk_size"]
            node.size = offset

    # Inode to path bidirectional mapping
    def _assign_inode(self, relative_path: str) -> int:
        inode = self._path_to_inode.get(relative_path)
        if inode is None:
            inode = self._next_inode
            self._next_inode += 1
            self._path_to_inode[relative_path] = inode
            self._inode_to_path[inode] = relative_path
        return inode

    def _node_for_inode(self, inode: int) -> tuple[str, _Node]:
        rel_path = self._inode_to_path.get(inode)
        node = self._nodes.get(rel_path) if rel_path is not None else None
        if node is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return rel_path, node

    def _attrs(self, inode: int, node: _Node) -> pyfuse3.EntryAttributes:
        entry = pyfuse3.EntryAttributes()
        entry.st_ino = inode
        entry.generation = 0
        entry.entry_timeout = 300
        entry.attr_timeout = 300
        is_dir = node.file_type == "dir"
        entry.st_mode = (stat.S_IFDIR | node.mode) if is_dir else (stat.S_IFREG | node.mode)
        entry.st_nlink = 2 if is_dir else 1
        entry.st_uid = os.getuid()
        entry.st_gid = os.getgid()
        entry.st_size = node.size
        entry.st_atime_ns = self._committed_at_ns
        entry.st_mtime_ns = self._committed_at_ns
        entry.st_ctime_ns = self._committed_at_ns
        entry.st_blksize = 4096
        entry.st_blocks = (node.size + 511) // 512
        return entry

    # pyfuse3.Operations
    async def getattr(self, inode, ctx=None):
        _, node = self._node_for_inode(inode)
        return self._attrs(inode, node)

    async def lookup(self, parent_inode, name, ctx=None):
        parent_path, parent_node = self._node_for_inode(parent_inode)
        if parent_node.file_type != "dir":
            raise pyfuse3.FUSEError(errno.ENOTDIR)
        name_str = os.fsdecode(name)
        rel_path = name_str if parent_path == "." else f"{parent_path}/{name_str}"
        node = self._nodes.get(rel_path)
        if node is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        inode = self._assign_inode(rel_path)
        return self._attrs(inode, node)

    async def opendir(self, inode, ctx):
        _, node = self._node_for_inode(inode)
        if node.file_type != "dir":
            raise pyfuse3.FUSEError(errno.ENOTDIR)
        return inode

    async def readdir(self, fh, start_id, token):
        _, node = self._node_for_inode(fh)
        for idx, child_path in enumerate(sorted(node.children)):
            if idx < start_id:
                continue
            child_node = self._nodes[child_path]
            child_inode = self._assign_inode(child_path)
            name = os.fsencode(os.path.basename(child_path))
            if not pyfuse3.readdir_reply(token, name, self._attrs(child_inode, child_node), idx + 1):
                break

    async def open(self, inode, flags, ctx):
        _, node = self._node_for_inode(inode)
        if node.file_type != "file":
            raise pyfuse3.FUSEError(errno.EISDIR)
        if flags & (os.O_WRONLY | os.O_RDWR):
            raise pyfuse3.FUSEError(errno.EROFS)
        return pyfuse3.FileInfo(fh=inode)

    async def read(self, fh, offset, size):
        _, node = self._node_for_inode(fh)
        if offset >= node.size or size <= 0:
            return b""
        end = min(offset + size, node.size)

        result = bytearray()
        idx = bisect.bisect_right(node.chunk_offsets, offset) - 1
        while idx < len(node.chunk_hashes) and node.chunk_offsets[idx] < end:
            chunk_start = node.chunk_offsets[idx]
            chunk_end = (
                node.chunk_offsets[idx + 1] if idx + 1 < len(node.chunk_offsets) else node.size
            )
            read_start = max(offset, chunk_start) - chunk_start
            read_end = min(end, chunk_end) - chunk_start

            chunk_path = os.path.join(self._chunk_dir, node.chunk_hashes[idx])
            try:
                with open(chunk_path, "rb") as handle:
                    handle.seek(read_start)
                    result += handle.read(read_end - read_start)
            except OSError as exc:
                raise pyfuse3.FUSEError(errno.EIO) from exc

            idx += 1

        return bytes(result)


def mount_commit(conn, root_folder: str, commit_hash: str, version_id: int,
                  committed_at_ns: int, chunk_dir: str, mountpoint: str) -> None:
    '''Blocks until the mount is unmounted.'''
    if not os.path.isdir(mountpoint):
        raise ValueError(f"Mountpoint does not exist or is not a directory: {mountpoint}")

    ops = CommitMount(conn, root_folder, commit_hash, version_id, committed_at_ns, chunk_dir)

    fuse_options = set(pyfuse3.default_options)
    fuse_options.add("ro")
    pyfuse3.init(ops, mountpoint, fuse_options)
    try:
        trio.run(pyfuse3.main)
    except KeyboardInterrupt:
        pass
    finally:
        pyfuse3.close()
