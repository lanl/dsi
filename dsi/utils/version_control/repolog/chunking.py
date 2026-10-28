import hashlib
import mmap
import os
import tempfile
from pathlib import Path
from typing import Any, Callable, Iterator
from .log_record import _utcnow
from .content_chunking.npy_chunking import (
    NPY_MAGIC, NpyFormatError, chunk_npy_file, iter_npy_file,
)
from .content_chunking.db_chunking import (
    SQLITE_MAGIC, SQLiteFormatError, chunk_sqlite_file, iter_sqlite_file,
)
from .content_chunking.csv_chunking import CSVFormatError, chunk_csv_file, iter_csv_file
from .content_chunking.xlsx_chunking import XLSXFormatError, chunk_xlsx_file, iter_xlsx_file
from .content_chunking.json_chunking import JSONFormatError, chunk_json_file, iter_json_file
from .content_chunking.xml_chunking import XMLFormatError, chunk_xml_file, iter_xml_file

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
# Keep bit 64: the first-byte boundary mask is shifted by one bit too.
# Truncating this table to 64 bits would change tests of the original top bit.
GEAR_TABLE_LS = tuple(value << 1 for value in GEAR_TABLE)


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
    buffer: bytearray | mmap.mmap,
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

    Roll two bytes per iteration with a shifted Gear table and mask for the
    first byte. Test both bytes, preserving the single-byte chunk boundaries.
    """
    if available <= 0:
        return 0

    scan_length = min(available, MAX_CHUNK)

    # The final chunk may be smaller than MIN_CHUNK.
    if scan_length <= MIN_CHUNK:
        return scan_length

    normal_end = min(AVG_CHUNK, scan_length)

    fingerprint = 0
    position = start + MIN_CHUNK
    gear = GEAR_TABLE
    gear_ls = GEAR_TABLE_LS
    uint64_mask = UINT64_MASK

    for end, mask in (
        (start + normal_end, STRICT_MASK),
        (start + scan_length, EAGER_MASK),
    ):
        shifted_mask = mask << 1
        pair_end = end - 1
        while position < pair_end:
            # This is twice the first byte's fingerprint. The shifted mask
            # checks the same bits without a separate one-byte hash update.
            fingerprint = (fingerprint << 2) + gear_ls[buffer[position]]
            if (fingerprint & shifted_mask) == 0:
                return position - start

            # Complete the pair and restore the normal 64-bit fingerprint.
            fingerprint = (fingerprint + gear[buffer[position + 1]]) & uint64_mask
            if (fingerprint & mask) == 0:
                return position + 1 - start
            position += 2

        # Do not let a pair cross the normalization transition or file end.
        if position < end:
            fingerprint = _gear_update(fingerprint, buffer[position])
            if (fingerprint & mask) == 0:
                return position - start
            position += 1

    # No content-defined boundary was found.
    return scan_length


def iter_file_fastcdc(
    path: str | os.PathLike[str],
    *,
    needs_data: Callable[[str], bool] | None = None,
) -> Iterator[dict[str, object]]:
    """
    Stream a read-only memory-mapped file as FastCDC chunks.

    Yield dictionaries with ``sha256``, ``size`` and ``data`` fields.
    Boundary selection is identical to the compatibility list API. If
    needs_data rejects a chunk hash, return data=None without copying its
    payload. No exported mmap views survive a yield.
    """
    with open(path, "rb") as file:
        # mmap cannot map an empty file; preserve the empty chunk sequence.
        if os.fstat(file.fileno()).st_size == 0:
            return

        with mmap.mmap(file.fileno(), length=0, access=mmap.ACCESS_READ) as mapped_file:
            position = 0
            file_size = len(mapped_file)
            while position < file_size:
                chunk_size = _find_fastcdc_cut(
                    buffer=mapped_file,
                    start=position,
                    available=file_size - position,
                )

                if chunk_size <= 0:
                    raise RuntimeError("FastCDC produced an invalid chunk size")

                view = memoryview(mapped_file)[position:position + chunk_size]
                try:
                    digest = hashlib.sha256(view).hexdigest()
                    chunk = {
                        "sha256": digest,
                        "size": chunk_size,
                        "data": (
                            bytes(view) if needs_data is None or needs_data(digest) else None
                        ),
                    }
                finally:
                    # Release before yielding so exhaustion, close(), and errors
                    # can close the mmap without an exported-buffer error.
                    view.release()

                position += chunk_size
                yield chunk


def chunk_file_fastcdc(path):
    """Compatibility list API; snapshot storage uses the streaming iterator."""
    return list(iter_file_fastcdc(path))


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

def chunk_file(path, *, streaming=False, needs_data: Callable[[str], bool] | None = None):
    """Chunk a file; streaming mmap chunkers can omit already-stored payloads.

    needs_data is a hash predicate used only with streaming=True. Supported
    format chunkers validate their layout before yielding any payloads.
    """
    path = Path(path)
    f_suffix = path.suffix.lower()

    if (f_suffix == ".npy" and _has_npy_magic(path)):
        try:
            if streaming:
                return iter_npy_file(path, needs_data=needs_data)
            return chunk_npy_file(path)
        except NpyFormatError:
            pass

    if (f_suffix in {".db", ".sqlite", ".sqlite3"} and is_sqlite_file(path)):
        try:
            if streaming:
                return iter_sqlite_file(path, needs_data=needs_data)
            return chunk_sqlite_file(path)
        except SQLiteFormatError:
            pass

    if f_suffix == ".csv":
        try:
            if streaming:
                return iter_csv_file(path, needs_data=needs_data)
            return chunk_csv_file(path)
        except CSVFormatError:
            pass

    if f_suffix == ".xlsx":
        try:
            if streaming:
                return iter_xlsx_file(path, needs_data=needs_data)
            return chunk_xlsx_file(path)
        except XLSXFormatError:
            pass

    if f_suffix == ".json":
        try:
            if streaming:
                return iter_json_file(path, needs_data=needs_data)
            return chunk_json_file(path)
        except JSONFormatError:
            pass

    if f_suffix == ".xml":
        try:
            if streaming:
                return iter_xml_file(path, needs_data=needs_data)
            return chunk_xml_file(path)
        except XMLFormatError:
            pass

    return iter_file_fastcdc(path, needs_data=needs_data) if streaming else chunk_file_fastcdc(path)


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
def store_chunks_for_snapshot(conn, chunk_root: str, entries: list[dict[str, Any]], *, pending_commit="UPDATE") -> tuple[dict[str, str], set, dict[str, int]]:
    chunk_dir = os.path.join(chunk_root, CHUNK_STORAGE_DIR)
    os.makedirs(chunk_dir, exist_ok=True)
    file_hashes = {}
    chunk_length = {}
    chunk_hashes = set()

    def needs_data(digest):
        return not os.path.exists(os.path.join(chunk_dir, digest))

    for entry in entries:

        file_path = entry['absolute_path']
        if entry.get('file_type') != 'file':
            continue

        h = hashlib.sha256()
        all_chunks = chunk_file(file_path, streaming=True, needs_data=needs_data)
        count = 0
        try:
            for i, chunk in enumerate(all_chunks):
                count += 1
                chunk_path = os.path.join(chunk_dir, chunk['sha256'])
                h.update(chunk['sha256'].encode('utf-8'))
                if chunk['data'] is not None and not os.path.exists(chunk_path):
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
                    (chunk['sha256'], chunk['size'], _utcnow(), entry['relative_path'], i, pending_commit),
                )
                chunk_hashes.add(chunk['sha256'])
        finally:
            # Release the source mapping even if writing or SQL insertion fails.
            close = getattr(all_chunks, "close", None)
            if close is not None:
                close()
        file_hashes[entry['relative_path']] = h.hexdigest()
        chunk_length[entry['relative_path']] = count
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
