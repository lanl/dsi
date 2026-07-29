import hashlib

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


if __name__ == "__main__":
    chunks = chunk_file("tests/wildfiredata.csv")

    for i, chunk in enumerate(chunks):
        print(
            f"Chunk {i:3d} "
            f"Size={chunk['size']:6d} "
            f"SHA256={chunk['sha256']}"
        )