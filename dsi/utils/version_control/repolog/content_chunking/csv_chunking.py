from __future__ import annotations

import hashlib
import mmap
import os
from pathlib import Path
from typing import Iterator


CSV_TARGET_CHUNK = 8 * 1024 * 1024  # 8 MiB


class CSVFormatError(ValueError):
    pass


def _make_chunk(
    view: memoryview,
    **metadata,
) -> dict[str, object]:
    digest = hashlib.sha256(view).hexdigest()

    return {
        "sha256": digest,
        "size": len(view),
        "data": bytes(view),
        **metadata,
    }


def _csv_record_ends(
    mapped: mmap.mmap,
) -> Iterator[int]:
    """
    Yield byte offsets immediately after each complete CSV record.

    Assumptions:
      - standard double quote character: "
      - embedded newlines are allowed inside quoted fields
      - escaped quotes use the standard "" representation

    Counting quote parity works because "" contributes two quote
    characters and therefore does not change quote state.
    """
    size = len(mapped)

    if size == 0:
        return

    position = 0
    in_quotes = False

    while position < size:

        newline = mapped.find(b"\n", position)

        if newline == -1:
            segment_end = size
        else:
            segment_end = newline + 1

        segment = mapped[position:segment_end]

        # Odd number of quotes toggles quoted-field state.
        if segment.count(b'"') % 2:
            in_quotes = not in_quotes

        if not in_quotes:
            yield segment_end

        position = segment_end

        if newline == -1:
            break

    if in_quotes:
        raise CSVFormatError(
            "CSV ended while inside a quoted field"
        )


def chunk_csv_file(
    path: str | os.PathLike[str],
    target_chunk_size: int = CSV_TARGET_CHUNK,
    isolate_header: bool = True,
) -> list[dict[str, object]]:
    """
    Chunk a CSV file only at complete record boundaries.

    Concatenating chunk["data"] recreates the original file exactly.

    By default:
        chunk 0 = header record
        chunk 1+ = groups of complete rows totaling approximately 8 MiB
    """
    path = Path(path)

    if target_chunk_size <= 0:
        raise ValueError(
            "target_chunk_size must be positive"
        )

    file_size = path.stat().st_size

    if file_size == 0:
        return []

    chunks = []

    with open(path, "rb") as file:

        with mmap.mmap(
            file.fileno(),
            length=0,
            access=mmap.ACCESS_READ,
        ) as mapped:

            record_ends = _csv_record_ends(mapped)

            chunk_start = 0
            chunk_first_record = 1
            record_number = 0

            try:
                first_end = next(record_ends)
            except StopIteration:
                return []

            record_number = 1

            # -----------------------------------
            # Header
            # -----------------------------------

            if isolate_header:

                view = memoryview(
                    mapped
                )[0:first_end]

                try:
                    chunks.append(
                        _make_chunk(
                            view,
                            kind="header",
                            start_record=1,
                            record_count=1,
                        )
                    )
                finally:
                    view.release()

                chunk_start = first_end
                chunk_first_record = 2

            else:
                # First record belongs to normal chunk.
                chunk_start = 0
                chunk_first_record = 1

                if (
                    first_end - chunk_start
                    >= target_chunk_size
                ):
                    view = memoryview(
                        mapped
                    )[chunk_start:first_end]

                    try:
                        chunks.append(
                            _make_chunk(
                                view,
                                kind="records",
                                start_record=1,
                                record_count=1,
                            )
                        )
                    finally:
                        view.release()

                    chunk_start = first_end
                    chunk_first_record = 2

            # -----------------------------------
            # Remaining records
            # -----------------------------------

            for record_end in record_ends:

                record_number += 1

                chunk_size = (
                    record_end - chunk_start
                )

                if chunk_size >= target_chunk_size:

                    view = memoryview(
                        mapped
                    )[chunk_start:record_end]

                    try:
                        chunks.append(
                            _make_chunk(
                                view,
                                kind="records",
                                start_record=(
                                    chunk_first_record
                                ),
                                record_count=(
                                    record_number
                                    - chunk_first_record
                                    + 1
                                ),
                            )
                        )
                    finally:
                        view.release()

                    chunk_start = record_end

                    chunk_first_record = (
                        record_number + 1
                    )

            # -----------------------------------
            # Final partial chunk
            # -----------------------------------

            if chunk_start < file_size:

                view = memoryview(
                    mapped
                )[chunk_start:file_size]

                try:
                    chunks.append(
                        _make_chunk(
                            view,
                            kind="records",
                            start_record=(
                                chunk_first_record
                            ),
                            record_count=(
                                record_number
                                - chunk_first_record
                                + 1
                            ),
                        )
                    )
                finally:
                    view.release()

    return chunks