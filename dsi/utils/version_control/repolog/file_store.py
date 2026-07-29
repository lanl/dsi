"""
Physical, single-file storage for the append-only log.

A repository's entire log lives in one file, which grows without bound
as records are appended. Each frame (see frame_codec.py) is addressable
by its byte offset in that file -- a plain `int` -- which is exactly
what an external SQL-based index is expected to store per log entry.

This module only deals in raw bytes (frames); it knows nothing about
`LogRecord` or hashing. That logic lives in `log.py`.
"""

from __future__ import annotations

import os
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional, Union

from .exceptions import CorruptFrameError, RecordNotFoundError
from .frame_codec import decode_frame_backward, decode_frame_forward, encode_frame

# A record's location in the log is just its byte offset in the single log file.
Location = int


@dataclass(frozen=True)
class TruncationReport:
    """Result of `LogFileStore.truncate_trailing_corruption`."""

    truncated: bool
    bytes_removed: int


class LogFileStore:
    """
    Manages a single append-only file holding every frame in a
    repository's log, in order, forever.
    """

    def __init__(self, log_file_path: Union[str, Path]):
        self.log_file_path = Path(log_file_path)
        self.log_file_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.log_file_path.exists():
            self.log_file_path.touch()
        self._write_lock = threading.Lock()

    # -- writing --------------------------------------------------------------

    def append(self, payload: bytes) -> Location:
        """Append one frame to the log file, fsync, and return its offset."""
        with self._write_lock:
            frame = encode_frame(payload)
            with open(self.log_file_path, "ab") as f:
                offset = f.tell()
                f.write(frame)
                f.flush()
                os.fsync(f.fileno())
            return offset

    # -- reading ----------------------------------------------------------

    def read_at(self, location: Location) -> bytes:
        """Read and return the payload of the frame at `location`."""
        with open(self.log_file_path, "rb") as f:
            f.seek(location)
            result = decode_frame_forward(f)
            if result is None:
                raise RecordNotFoundError(f"No frame at offset {location} (clean EOF).")
            payload, _ = result
            return payload

    def latest_location(self) -> Optional[Location]:
        """
        Return the offset of the very last frame in the log, using O(1)
        backward decoding (no forward scan), or None if the log is empty.

        Raises `CorruptFrameError` if the tail of the file is malformed
        -- callers doing normal appends should run
        `truncate_trailing_corruption()` first (see `RepositoryLog.recover`).
        """
        size = self.log_file_path.stat().st_size
        if size == 0:
            return None
        with open(self.log_file_path, "rb") as f:
            result = decode_frame_backward(f, size)
        if result is None:
            return None
        _, frame_start, _ = result
        return frame_start

    def iter_frames(
        self, start_location: Optional[Location] = None
    ) -> Iterator[tuple[Location, bytes]]:
        """
        Sequentially yield `(Location, payload)` for every frame in the
        log, in order, optionally starting at `start_location`
        (inclusive). This is the primary integration point for building
        or refreshing an external index over the log.
        """
        with open(self.log_file_path, "rb") as f:
            if start_location is not None:
                f.seek(start_location)
            while True:
                pos = f.tell()
                result = decode_frame_forward(f)
                if result is None:
                    break
                payload, _size = result
                yield pos, payload

    # -- crash recovery ---------------------------------------------------

    def truncate_trailing_corruption(self) -> TruncationReport:
        """
        Scan the log file forward from the start, and if a truncated or
        CRC-invalid frame is found, drop everything from that point
        onward (this is exactly what a crash mid-append looks like: a
        well-formed prefix followed by a partial frame).

        This scans the whole file every time it's called, since with a
        single ever-growing file there's no cheaper "just check the
        newest segment" shortcut. It's intended to run once at startup
        after an unclean shutdown, not on every append.
        """
        file_size = self.log_file_path.stat().st_size
        if file_size == 0:
            return TruncationReport(truncated=False, bytes_removed=0)

        last_good_offset = 0
        with open(self.log_file_path, "rb") as f:
            while True:
                pos = f.tell()
                try:
                    result = decode_frame_forward(f)
                except CorruptFrameError:
                    last_good_offset = pos
                    break
                if result is None:
                    last_good_offset = pos
                    break
                _payload, _size = result
                last_good_offset = f.tell()

        if last_good_offset < file_size:
            bytes_removed = file_size - last_good_offset
            with open(self.log_file_path, "r+b") as f:
                f.truncate(last_good_offset)
                f.flush()
                os.fsync(f.fileno())
            return TruncationReport(truncated=True, bytes_removed=bytes_removed)

        return TruncationReport(truncated=False, bytes_removed=0)
