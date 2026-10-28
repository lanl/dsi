from __future__ import annotations

import os
from pathlib import Path
from typing import Iterator

from .mapped_chunking import ChunkRange, PayloadPredicate, iter_mapped_chunks


SQLITE_MAGIC = b"SQLite format 3\x00"

# Speed-oriented setting.
SQLITE_TARGET_CHUNK = 8 * 1024 * 1024  # 8 MiB


class SQLiteFormatError(ValueError):
    pass


def get_sqlite_page_size(
    path: str | os.PathLike[str],
) -> int:
    """
    Read the SQLite database page size directly from its header.

    SQLite header:
        offset 0  : 16-byte magic string
        offset 16 : 2-byte big-endian page size

    Special case:
        stored value 1 means 65536-byte pages.
    """
    with open(path, "rb") as f:
        header = f.read(100)

    if len(header) < 100:
        raise SQLiteFormatError(
            "File is too small to be an SQLite database"
        )

    if header[:16] != SQLITE_MAGIC:
        raise SQLiteFormatError(
            "Invalid SQLite database header"
        )

    raw_page_size = int.from_bytes(
        header[16:18],
        byteorder="big",
    )

    if raw_page_size == 1:
        page_size = 65536
    else:
        page_size = raw_page_size

    # SQLite page sizes must be powers of two
    # between 512 and 65536.
    if (
        page_size < 512
        or page_size > 65536
        or page_size & (page_size - 1)
    ):
        raise SQLiteFormatError(
            f"Invalid SQLite page size: {page_size}"
        )

    return page_size


def chunk_sqlite_file(path, target_chunk_size=SQLITE_TARGET_CHUNK):
    """Return page-aligned chunks, with page one stored independently."""
    return list(iter_sqlite_file(path, target_chunk_size))


def iter_sqlite_file(
    path: str | os.PathLike[str],
    target_chunk_size: int = SQLITE_TARGET_CHUNK,
    *,
    needs_data: PayloadPredicate | None = None,
) -> Iterator[dict[str, object]]:
    """Validate the SQLite layout, then stream requested payloads."""
    page_size = get_sqlite_page_size(path)
    file_size = Path(path).stat().st_size
    if file_size % page_size:
        raise SQLiteFormatError(
            f"Database size {file_size} is not a multiple of page size {page_size}"
        )
    aligned_size = max(1, target_chunk_size // page_size) * page_size

    def ranges(mapped):
        yield 0, page_size, {}
        for start in range(page_size, len(mapped), aligned_size):
            yield start, min(start + aligned_size, len(mapped)), {}

    return iter_mapped_chunks(path, ranges, needs_data=needs_data)
