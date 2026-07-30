import hashlib
import os
from typing import Any, Optional
from .log_record import _utcnow

# -----------------------------
# Rolling hash parameters
# -----------------------------
WINDOW_SIZE = 64
BASE = 257
MOD = (1 << 61) - 1   # Large prime

# Chunking parameters
MIN_CHUNK = 4 * 1024      # 4 KB
AVG_CHUNK = 64 * 1024      # 64 KB
MAX_CHUNK = 64 * 1024 * 1024     # 64 MB

MASK = AVG_CHUNK - 1      # AVG_CHUNK must be power of 2
CHUNK_STORAGE_DIR = ".dsi_vcs_chunks"

class RollingHash:
    def __init__(self, window_size):
        self.window_size = window_size
        self.window = []
        self.hash = 0
        self.base_power = pow(BASE, window_size - 1, MOD)

    def slide(self, byte):
        if len(self.window) == self.window_size:
            outgoing = self.window.pop(0)
            self.hash = (
                self.hash
                - outgoing * self.base_power
            ) % MOD

        self.window.append(byte)
        self.hash = (self.hash * BASE + byte) % MOD

        return self.hash


def chunk_file(path):
    rh = RollingHash(WINDOW_SIZE)

    chunks = []
    current = bytearray()

    with open(path, "rb") as f:
        while True:
            b = f.read(1)
            if not b:
                break

            value = b[0]
            current.append(value)
            h = rh.slide(value)

            size = len(current)

            # Wait until minimum chunk size
            if size < MIN_CHUNK:
                continue

            # CDC boundary
            if ((h & MASK) == 0) or size >= MAX_CHUNK:
                digest = hashlib.sha256(current).hexdigest()

                chunks.append({
                    "sha256": digest,
                    "size": size,
                    "data": bytes(current)
                })

                current = bytearray()

    # Final chunk
    if current:
        digest = hashlib.sha256(current).hexdigest()
        chunks.append({
            "sha256": digest,
            "size": len(current),
            "data": bytes(current)
        })

    return chunks

# ─────────────────────────── CHUNK-BASED STORAGE ───────────────────────────
def store_chunks_for_snapshot(conn, chunk_root: str, entries: list[dict[str, Any]]) -> tuple[dict[str, str], set, dict[str, int]]:
    chunk_dir = os.path.join(chunk_root, CHUNK_STORAGE_DIR)
    os.makedirs(chunk_dir, exist_ok=True)
    file_hashes = {}
    chunk_length = {}
    chunk_hashes = set()
    for entry in entries:

        file_path = entry['absolute_path']
        if not os.path.isfile(file_path):
            continue

        h = hashlib.sha256()
        all_chunks = chunk_file(file_path)
        for i, chunk in enumerate(all_chunks):
            chunk_path = os.path.join(chunk_dir, chunk['sha256'])
            h.update(chunk['sha256'].encode('utf-8'))
            if not os.path.exists(chunk_path):
                with open(chunk_path, "wb") as handle:
                    handle.write(chunk['data'])
            conn.execute(
                "INSERT INTO chunk_store "
                "(chunk_hash, chunk_size, created_at, relative_file_path, chunk_index, commit_hash) VALUES (?, ?, ?, ?, ?, ?)",
                (chunk['sha256'], chunk['size'], _utcnow(), entry['relative_path'], i, "UPDATE"),
            )
            chunk_hashes.add(chunk['sha256'])
        file_hashes[entry['relative_path']] = h.hexdigest()
        chunk_length[entry['relative_path']] = len(all_chunks)
    return file_hashes, chunk_hashes, chunk_length

def rebuild_file_from_chunks(conn, chunk_root: str, relative_path: str, commit_hash: str, output_path: str) -> bool:
    chunk_dir = os.path.join(chunk_root, CHUNK_STORAGE_DIR)
    rows = conn.execute(
        "SELECT chunk_hash, chunk_size FROM chunk_store WHERE relative_file_path = ? AND commit_hash LIKE ? ORDER BY chunk_index",
        (relative_path, commit_hash + "%")
    ).fetchall()
    chunk_hashes = [row['chunk_hash'] for row in rows]
    # print(f"Rebuilding {relative_path} from chunks: {chunk_hashes} commit_hash={commit_hash}")
    try:
        os.makedirs(os.path.dirname(os.path.join(output_path, relative_path)), exist_ok=True)
        with open(os.path.join(output_path, relative_path), "wb") as out_file:
            for chunk_hash in chunk_hashes:
                chunk_path = os.path.join(chunk_dir, chunk_hash)
                # print(f"===={chunk_path}====")
                if not os.path.exists(chunk_path):
                    return False
                with open(chunk_path, "rb") as in_file:
                    content = in_file.read()
                    out_file.write(content)
                    # print(content)
        return True
    except Exception:
        return False

if __name__ == "__main__":
    chunks = chunk_file("tests/wildfiredata.csv")

    for i, chunk in enumerate(chunks):
        print(
            f"Chunk {i:3d} "
            f"Size={chunk['size']:6d} "
            f"SHA256={chunk['sha256']}"
        )