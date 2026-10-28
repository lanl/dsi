from __future__ import annotations

import mmap
import os
from typing import Iterator

from .mapped_chunking import ChunkRange, PayloadPredicate, iter_mapped_chunks


CSV_TARGET_CHUNK = 8 * 1024 * 1024  # 8 MiB


class CSVFormatError(ValueError):
    pass


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

        # Count quote parity in place, including escaped pairs. Slicing the
        # record would allocate its payload even when its chunk already exists.
        quote = mapped.find(b'"', position, segment_end)
        while quote != -1:
            in_quotes = not in_quotes
            quote = mapped.find(b'"', quote + 1, segment_end)

        if not in_quotes:
            yield segment_end

        position = segment_end

        if newline == -1:
            break

    if in_quotes:
        raise CSVFormatError(
            "CSV ended while inside a quoted field"
        )


def chunk_csv_file(path, target_chunk_size=CSV_TARGET_CHUNK, isolate_header=True):
    """Return byte-preserving chunks ending at complete CSV records."""
    return list(iter_csv_file(path, target_chunk_size, isolate_header))


def iter_csv_file(
    path: str | os.PathLike[str],
    target_chunk_size: int = CSV_TARGET_CHUNK,
    isolate_header: bool = True,
    *,
    needs_data: PayloadPredicate | None = None,
) -> Iterator[dict[str, object]]:
    """Validate record boundaries, then copy only requested chunk payloads."""
    if target_chunk_size <= 0:
        raise ValueError("target_chunk_size must be positive")
    return iter_mapped_chunks(
        path, lambda mapped: _csv_ranges(mapped, target_chunk_size, isolate_header),
        needs_data=needs_data,
    )


def _csv_ranges(mapped, target_chunk_size, isolate_header) -> Iterator[ChunkRange]:
    record_ends = _csv_record_ends(mapped)
    chunk_start = 0
    first_record = 1
    record_number = 0
    for record_number, record_end in enumerate(record_ends, 1):
        if isolate_header and record_number == 1:
            yield 0, record_end, {"kind": "header", "start_record": 1, "record_count": 1}
        elif record_end - chunk_start >= target_chunk_size:
            yield chunk_start, record_end, {
                "kind": "records", "start_record": first_record,
                "record_count": record_number - first_record + 1,
            }
        else:
            continue
        chunk_start = record_end
        first_record = record_number + 1
    if chunk_start < len(mapped):
        yield chunk_start, len(mapped), {
            "kind": "records", "start_record": first_record,
            "record_count": record_number - first_record + 1,
        }
