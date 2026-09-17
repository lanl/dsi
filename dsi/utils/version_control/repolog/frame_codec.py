"""
Low-level binary framing for the append-only log.

Every record is stored on disk as one immutable "frame":

    +----------------+-------------------------+--------------+----------------+
    | length (4B BE) | payload (length bytes)  | crc32 (4B BE) | length (4B BE) |
    +----------------+-------------------------+--------------+----------------+

The length is stored both *before and after* the payload. This lets a
reader:

  - scan forward from any offset (read the leading length, read that
    many payload bytes, verify the CRC, verify the trailing length
    matches), and
  - seek directly to the *last* frame from end-of-file with no forward
    scan at all (read the last 4 bytes to get the length, compute the
    frame's start offset, and decode forward from there).

The second property is what gives `RepositoryLog` O(1) access to the
chain tail (needed to resume appending after a restart) without
needing a separate index -- exactly the kind of physical, blockchain-like
"pointer to the previous record" the log provides on its own, with any
richer/random-access indexing left to an external SQL-based index.

A trailing frame that is truncated or fails its CRC is treated as
evidence of a crash mid-write and is handled by
`LogFileStore.truncate_trailing_corruption` (see file_store.py) -- not
silently accepted here.
"""

from __future__ import annotations

import struct
import zlib
from typing import BinaryIO, Optional

from .exceptions import CorruptFrameError

_LEN_STRUCT = struct.Struct(">I")  # 4-byte big-endian unsigned length
_CRC_STRUCT = struct.Struct(">I")  # 4-byte big-endian unsigned CRC32

HEADER_SIZE = _LEN_STRUCT.size
CRC_SIZE = _CRC_STRUCT.size
FOOTER_SIZE = _LEN_STRUCT.size
FRAME_OVERHEAD = HEADER_SIZE + CRC_SIZE + FOOTER_SIZE  # 12 bytes
MAX_PAYLOAD_BYTES = (2**32) - 1  # fits in the 4-byte length field


def encode_frame(payload: bytes) -> bytes:
    """Encode a payload into one on-disk frame, ready to append."""
    if len(payload) > MAX_PAYLOAD_BYTES:
        raise ValueError(
            f"payload of {len(payload)} bytes exceeds max frame payload size "
            f"of {MAX_PAYLOAD_BYTES} bytes"
        )
    length_bytes = _LEN_STRUCT.pack(len(payload))
    crc_bytes = _CRC_STRUCT.pack(zlib.crc32(payload) & 0xFFFFFFFF)
    return length_bytes + payload + crc_bytes + length_bytes


def decode_frame_forward(f: BinaryIO) -> Optional[tuple[bytes, int]]:
    """
    Read one frame starting at the file's current position.

    Returns `(payload, total_frame_size)`, or `None` on a *clean* EOF
    (i.e. nothing at all left to read -- the normal end of a
    well-formed log). Raises `CorruptFrameError` if a frame starts but
    is truncated or fails integrity checks.
    """
    start_pos = f.tell()
    header = f.read(HEADER_SIZE)
    if len(header) == 0:
        return None  # clean EOF, nothing more to read
    if len(header) < HEADER_SIZE:
        raise CorruptFrameError(f"Truncated frame header at offset {start_pos}")

    (length,) = _LEN_STRUCT.unpack(header)

    payload = f.read(length)
    if len(payload) < length:
        raise CorruptFrameError(
            f"Truncated frame payload at offset {start_pos} "
            f"(expected {length} bytes, got {len(payload)})"
        )

    crc_bytes = f.read(CRC_SIZE)
    if len(crc_bytes) < CRC_SIZE:
        raise CorruptFrameError(f"Truncated frame CRC at offset {start_pos}")
    (stored_crc,) = _CRC_STRUCT.unpack(crc_bytes)
    actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
    if actual_crc != stored_crc:
        raise CorruptFrameError(f"CRC mismatch for frame at offset {start_pos}")

    footer_bytes = f.read(FOOTER_SIZE)
    if len(footer_bytes) < FOOTER_SIZE:
        raise CorruptFrameError(f"Truncated frame footer at offset {start_pos}")
    (footer_length,) = _LEN_STRUCT.unpack(footer_bytes)
    if footer_length != length:
        raise CorruptFrameError(
            f"Footer/header length mismatch for frame at offset {start_pos}"
        )

    total_size = HEADER_SIZE + length + CRC_SIZE + FOOTER_SIZE
    return payload, total_size


def decode_frame_backward(f: BinaryIO, end_pos: int) -> Optional[tuple[bytes, int, int]]:
    """
    Read the single frame ending exactly at `end_pos` (typically the
    file's size, i.e. EOF), using the trailing length field to jump
    straight to the frame's start.

    Returns `(payload, frame_start_offset, total_frame_size)`, or
    `None` if `end_pos` is too small to contain any frame at all.
    Raises `CorruptFrameError` if the implied frame is inconsistent.
    """
    if end_pos < FRAME_OVERHEAD:
        return None

    f.seek(end_pos - FOOTER_SIZE)
    footer_bytes = f.read(FOOTER_SIZE)
    if len(footer_bytes) < FOOTER_SIZE:
        raise CorruptFrameError(f"Truncated trailing length at offset {end_pos}")
    (length,) = _LEN_STRUCT.unpack(footer_bytes)

    total_size = HEADER_SIZE + length + CRC_SIZE + FOOTER_SIZE
    frame_start = end_pos - total_size
    if frame_start < 0:
        raise CorruptFrameError(
            f"Trailing length at offset {end_pos} implies a frame start "
            "before the beginning of the file; log file is corrupt."
        )

    f.seek(frame_start)
    result = decode_frame_forward(f)
    if result is None:
        raise CorruptFrameError(
            f"Expected a frame at computed offset {frame_start} but found EOF."
        )
    payload, computed_size = result
    if computed_size != total_size:
        raise CorruptFrameError(
            f"Frame size mismatch decoding backward from {end_pos}: "
            f"forward decode gave {computed_size}, backward pointer gave {total_size}."
        )
    return payload, frame_start, total_size
