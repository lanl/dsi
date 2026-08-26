import hashlib
import os
import tempfile
from pathlib import Path
from typing import Any
from .log_record import _utcnow
from .content_chunking.npy_chunking import NPY_MAGIC, chunk_npy_file, NpyFormatError
from .content_chunking.db_chunking import SQLITE_MAGIC, chunk_sqlite_file, SQLiteFormatError
from .content_chunking.csv_chunking import CSVFormatError, chunk_csv_file
from .content_chunking.xlsx_chunking import XLSXFormatError, chunk_xlsx_file
from .content_chunking.json_chunking import JSONFormatError, chunk_json_file
from .content_chunking.xml_chunking import XMLFormatError, chunk_xml_file

# -----------------------------
# Rolling hash parameters
# -----------------------------
WINDOW_SIZE = 64
BASE = 257
MOD = (1 << 61) - 1   # Large prime

# Chunking parameters
MIN_CHUNK = 1 * 1024 * 1024      # 1 MB
AVG_CHUNK = 8 * 1024 * 1024      # 8 MB
MAX_CHUNK = 64 * 1024 * 1024     # 64 MB

MASK = AVG_CHUNK - 1      # AVG_CHUNK must be power of 2
CHUNK_STORAGE_DIR = "dsi_vcs_chunks"

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


# Number of bits added/removed from the normal boundary mask.
#
# Before AVG_CHUNK:
#   more bits make a boundary less likely.
#
# After AVG_CHUNK:
#   fewer bits make a boundary more likely.
NORMALIZATION_LEVEL = 2

# Amount read from disk at a time.
READ_SIZE = 1024 * 1024

UINT64_MASK = (1 << 64) - 1


def _build_gear_table() -> tuple[int, ...]:
    """
    Build a deterministic table of 256 pseudo-random 64-bit integers.

    The table must remain unchanged after repositories have been created.
    Changing the table changes all future chunk boundaries.

    SplitMix64 is used only to produce a stable, well-distributed table.
    It is not used as a cryptographic hash.
    """
    table = []
    state = 0xD51FC0DEC0FFEE01

    for _ in range(256):
        state = (
            state + 0x9E3779B97F4A7C15
        ) & UINT64_MASK

        value = state
        value = (
            (value ^ (value >> 30))
            * 0xBF58476D1CE4E5B9
        ) & UINT64_MASK

        value = (
            (value ^ (value >> 27))
            * 0x94D049BB133111EB
        ) & UINT64_MASK

        value ^= value >> 31
        table.append(value & UINT64_MASK)

    return tuple(table)


GEAR_TABLE = _build_gear_table()


def _make_spread_mask(bit_count: int) -> int:
    """
    Create a 64-bit mask whose set bits are spread across the word.

    For a uniformly distributed fingerprint, requiring all selected bits
    to be zero produces a boundary probability of approximately:

        1 / (2 ** bit_count)

    Spreading the bits uses a longer portion of the Gear fingerprint than
    placing all effective bits next to one another.
    """
    if not 1 <= bit_count <= 64:
        raise ValueError("bit_count must be between 1 and 64")

    if bit_count == 1:
        return 1 << 31

    mask = 0

    for index in range(bit_count):
        position = (index * 63) // (bit_count - 1)
        mask |= 1 << position

    return mask


def _validate_parameters() -> None:
    if not 0 < MIN_CHUNK <= AVG_CHUNK <= MAX_CHUNK:
        raise ValueError(
            "Chunk sizes must satisfy "
            "0 < MIN_CHUNK <= AVG_CHUNK <= MAX_CHUNK"
        )

    if AVG_CHUNK & (AVG_CHUNK - 1):
        raise ValueError("AVG_CHUNK must be a power of two")

    average_bits = AVG_CHUNK.bit_length() - 1

    if average_bits - NORMALIZATION_LEVEL < 1:
        raise ValueError(
            "NORMALIZATION_LEVEL is too large for AVG_CHUNK"
        )

    if average_bits + NORMALIZATION_LEVEL > 64:
        raise ValueError(
            "NORMALIZATION_LEVEL produces a mask larger than 64 bits"
        )


_validate_parameters()

AVERAGE_BITS = AVG_CHUNK.bit_length() - 1

# Before the target average, use more effective bits. For AVG_CHUNK=64 KB:
#
#     normal bits = 16
#     strict bits = 18
#
# This makes early chunk boundaries less likely.
STRICT_MASK = _make_spread_mask(
    AVERAGE_BITS + NORMALIZATION_LEVEL
)

# After the target average:
#
#     eager bits = 14
#
# This makes later chunk boundaries more likely.
EAGER_MASK = _make_spread_mask(
    AVERAGE_BITS - NORMALIZATION_LEVEL
)


def _gear_update(fingerprint: int, value: int) -> int:
    """
    Update the 64-bit Gear fingerprint for one byte.
    """
    return (
        (fingerprint << 1) + GEAR_TABLE[value]
    ) & UINT64_MASK


def _find_fastcdc_cut(
    buffer: bytearray,
    start: int,
    available: int,
) -> int:
    """
    Return the length of the next chunk in buffer[start:].

    The returned length is always positive when available is positive.

    FastCDC regions:

        0 ---------------- MIN_CHUNK
          no boundary tests

        MIN_CHUNK -------- AVG_CHUNK
          strict boundary mask

        AVG_CHUNK -------- MAX_CHUNK
          eager boundary mask

        MAX_CHUNK
          forced boundary
    """
    if available <= 0:
        return 0

    scan_length = min(available, MAX_CHUNK)

    # The final chunk may be smaller than MIN_CHUNK.
    if scan_length <= MIN_CHUNK:
        return scan_length

    normal_end = min(AVG_CHUNK, scan_length)

    fingerprint = 0
    position = MIN_CHUNK

    # -----------------------------
    # Region 1: MIN -> AVG
    #
    # More mask bits make boundaries harder to find,
    # reducing undersized chunks.
    # -----------------------------
    while position < normal_end:
        fingerprint = _gear_update(
            fingerprint,
            buffer[start + position],
        )

        if (fingerprint & STRICT_MASK) == 0:
            return position

        position += 1

    # -----------------------------
    # Region 2: AVG -> MAX
    #
    # Fewer mask bits make boundaries easier to find,
    # reducing oversized chunks.
    # -----------------------------
    while position < scan_length:
        fingerprint = _gear_update(
            fingerprint,
            buffer[start + position],
        )

        if (fingerprint & EAGER_MASK) == 0:
            return position

        position += 1

    # No content-defined boundary was found.
    return scan_length


def chunk_file_fastcdc(
    path: str | os.PathLike[str],
) -> list[dict[str, object]]:
    """
    Split a file into FastCDC chunks.

    The return structure is compatible with the previous implementation:

        [
            {
                "sha256": "<hex digest>",
                "size": 65536,
                "data": b"...",
            },
            ...
        ]
    """
    chunks: list[dict[str, object]] = []

    # Bytes before buffer_start have already been emitted.
    buffer = bytearray()
    buffer_start = 0
    end_of_file = False

    with open(path, "rb") as file:
        while True:
            available = len(buffer) - buffer_start

            # Keep enough data available to find any boundary up to
            # MAX_CHUNK. Reading ahead does not alter chunk boundaries.
            while not end_of_file and available < MAX_CHUNK:
                read_length = min(
                    READ_SIZE,
                    MAX_CHUNK - available,
                )

                block = file.read(read_length)

                if not block:
                    end_of_file = True
                    break

                buffer.extend(block)
                available += len(block)

            if available == 0:
                break

            chunk_size = _find_fastcdc_cut(
                buffer=buffer,
                start=buffer_start,
                available=available,
            )

            if chunk_size <= 0:
                raise RuntimeError(
                    "FastCDC produced an invalid chunk size"
                )

            chunk_data = bytes(
                buffer[
                    buffer_start:
                    buffer_start + chunk_size
                ]
            )

            chunks.append(
                {
                    "sha256": hashlib.sha256(
                        chunk_data
                    ).hexdigest(),
                    "size": chunk_size,
                    "data": chunk_data,
                }
            )

            buffer_start += chunk_size

            # Reclaim processed buffer space without shifting the
            # remaining bytes after every small chunk.
            if buffer_start == len(buffer):
                buffer.clear()
                buffer_start = 0

            elif buffer_start >= MAX_CHUNK:
                del buffer[:buffer_start]
                buffer_start = 0

    return chunks


def _has_npy_magic(path: str | os.PathLike[str]) -> bool:
    try:
        with open(path, "rb") as file:
            return file.read(len(NPY_MAGIC)) == NPY_MAGIC
    except OSError:
        return False

def is_sqlite_file(path):
    try:
        with open(path, "rb") as f:
            return f.read(16) == SQLITE_MAGIC
    except OSError:
        return False

def chunk_file(path):
    path = Path(path)
    f_suffix = path.suffix.lower()

    if (f_suffix == ".npy" and _has_npy_magic(path)):
        try:
            return chunk_npy_file(path)
        except NpyFormatError:
            pass

    if (f_suffix in {".db", ".sqlite", ".sqlite3"} and is_sqlite_file(path)):
        try:
            return chunk_sqlite_file(path)
        except SQLiteFormatError:
            pass

    if f_suffix == ".csv":
        try:
            return chunk_csv_file(path)
        except CSVFormatError:
            pass

    if f_suffix == ".xlsx":
        try:
            return chunk_xlsx_file(path)
        except XLSXFormatError:
            pass

    if f_suffix == ".json":
        try:
            return chunk_json_file(path)
        except JSONFormatError:
            pass

    if f_suffix == ".xml":
        try:
            return chunk_xml_file(path)
        except XMLFormatError:
            pass

    return chunk_file_fastcdc(path)


def chunk_file_rolling_hash(path):
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
                fd, temp_path = tempfile.mkstemp(
                    dir=chunk_dir, prefix=f".{chunk['sha256']}.", suffix=".tmp"
                )
                try:
                    os.chmod(temp_path, 0o644)
                    with os.fdopen(fd, "wb") as handle:
                        fd = None
                        handle.write(chunk['data'])
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.replace(temp_path, chunk_path)
                finally:
                    if fd is not None:
                        os.close(fd)
                    if os.path.exists(temp_path):
                        os.unlink(temp_path)
            conn.execute(
                "INSERT INTO chunk_store "
                "(chunk_hash, chunk_size, created_at, relative_file_path, chunk_index, commit_hash) VALUES (?, ?, ?, ?, ?, ?)",
                (chunk['sha256'], chunk['size'], _utcnow(), entry['relative_path'], i, "UPDATE"),
            )
            chunk_hashes.add(chunk['sha256'])
        file_hashes[entry['relative_path']] = h.hexdigest()
        chunk_length[entry['relative_path']] = len(all_chunks)
    return file_hashes, chunk_hashes, chunk_length

def rebuild_file_from_chunks(conn, chunk_root: str, relative_path: str, commit_hash: str, file_hash: str, output_path: str) -> bool:
    chunk_dir = os.path.join(chunk_root, CHUNK_STORAGE_DIR)
    rows = conn.execute(
        "SELECT chunk_hash, chunk_size FROM chunk_store WHERE relative_file_path = ? AND commit_hash LIKE ? ORDER BY chunk_index",
        (relative_path, commit_hash + "%")
    ).fetchall()
    chunk_hashes = [row['chunk_hash'] for row in rows]

    chunk_hash_digest = hashlib.sha256()
    for chunk_hash in chunk_hashes:
        chunk_hash_digest.update(chunk_hash.encode('utf-8'))
    if chunk_hash_digest.hexdigest() != file_hash:
        return False

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
