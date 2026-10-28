"""Validate format-specific ranges, then stream payloads without retaining copies."""

from __future__ import annotations

import hashlib
import mmap
import os
from typing import Callable, Iterable, Iterator


ChunkRange = tuple[int, int, dict[str, object]]
PayloadPredicate = Callable[[str], bool]


def iter_mapped_chunks(
    path: str | os.PathLike[str],
    plan: Callable[[mmap.mmap], Iterable[ChunkRange]],
    *,
    needs_data: PayloadPredicate | None = None,
) -> Iterator[dict[str, object]]:
    """Validate eagerly without hashing or copying payloads, then stream chunks.

    Retain only chunk offsets and format metadata. Closing the planning mapping
    before returning also avoids leaking it if the iterator is never consumed.
    Planning errors reach the dispatcher before any chunk references are stored.
    """
    with open(path, "rb") as file:
        if os.fstat(file.fileno()).st_size == 0:
            return iter(())
        with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as mapped:
            ranges = list(plan(mapped))
    return _iter_ranges(path, ranges, needs_data)


def _iter_ranges(path, ranges, needs_data):
    with open(path, "rb") as file:
        with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as mapped:
            for start, end, metadata in ranges:
                view = memoryview(mapped)[start:end]
                try:
                    digest = hashlib.sha256(view).hexdigest()
                    chunk = {
                        "sha256": digest,
                        "size": len(view),
                        "data": (
                            bytes(view) if needs_data is None or needs_data(digest) else None
                        ),
                        **metadata,
                    }
                finally:
                    # No exported buffer survives a yield or an exception.
                    view.release()
                yield chunk
