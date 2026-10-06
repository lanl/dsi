from __future__ import annotations

import hashlib
import mmap
import os
from pathlib import Path


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


def _make_chunk(view: memoryview) -> dict[str, object]:
    """
    Convert one memory region to the same chunk representation
    currently used by DSI-VCS.
    """
    digest = hashlib.sha256(view).hexdigest()

    return {
        "sha256": digest,
        "size": len(view),
        "data": bytes(view),
    }


def chunk_sqlite_file(
    path: str | os.PathLike[str],
    target_chunk_size: int = SQLITE_TARGET_CHUNK,
) -> list[dict[str, object]]:
    """
    Fast SQLite-aware chunking.

    Strategy:
        1. Read SQLite page size.
        2. Store page 1 independently.
        3. Group remaining pages into large page-aligned chunks.

    Returns the same structure as the existing DSI-VCS chunk_file():

        [
            {
                "sha256": "...",
                "size": 4096,
                "data": b"...",
            },
            ...
        ]

    Concatenating the chunks recreates the exact input file.
    """
    path = Path(path)

    page_size = get_sqlite_page_size(path)
    file_size = path.stat().st_size

    if file_size == 0:
        return []

    if file_size % page_size != 0:
        raise SQLiteFormatError(
            f"Database size {file_size} is not a multiple "
            f"of page size {page_size}"
        )

    # Ensure every chunk contains complete SQLite pages.
    pages_per_chunk = max(
        1,
        target_chunk_size // page_size,
    )

    aligned_chunk_size = pages_per_chunk * page_size

    chunks: list[dict[str, object]] = []

    with open(path, "rb") as f:
        with mmap.mmap(
            f.fileno(),
            length=0,
            access=mmap.ACCESS_READ,
        ) as mapped:

            # -----------------------------------
            # Chunk 0: SQLite page 1 by itself
            # -----------------------------------

            first_page = memoryview(mapped)[0:page_size]

            try:
                chunks.append(
                    _make_chunk(first_page)
                )
            finally:
                first_page.release()

            # -----------------------------------
            # Remaining pages
            # -----------------------------------

            position = page_size

            while position < file_size:

                end = min(
                    position + aligned_chunk_size,
                    file_size,
                )

                view = memoryview(mapped)[position:end]

                try:
                    chunks.append(
                        _make_chunk(view)
                    )
                finally:
                    view.release()

                position = end

    return chunks