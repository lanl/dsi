from __future__ import annotations

import ast
import hashlib
import mmap
import os
import struct
from pathlib import Path
from typing import Any

import numpy as np


NPY_MAGIC = b"\x93NUMPY"

# Speed-oriented setting for 10–20 MiB .npy files.
NPY_TARGET_CHUNK = 8 * 1024 * 1024  # 8 MiB

# Protect against unexpectedly large or malicious headers.
MAX_NPY_HEADER = 1024 * 1024        # 1 MiB


class NpyFormatError(ValueError):
    """Raised when a file is not a supported numeric .npy file."""


def _read_npy_metadata(
    path: str | os.PathLike[str],
) -> dict[str, Any]:
    """
    Parse an NPY header without loading the array payload.

    Returns:
        {
            "version": (major, minor),
            "data_offset": int,
            "shape": tuple[int, ...],
            "fortran_order": bool,
            "dtype": numpy.dtype,
            "payload_size": int,
        }
    """
    with open(path, "rb") as file:
        magic = file.read(6)

        if magic != NPY_MAGIC:
            raise NpyFormatError("File does not contain NPY magic bytes")

        version_bytes = file.read(2)

        if len(version_bytes) != 2:
            raise NpyFormatError("Truncated NPY version field")

        version = (version_bytes[0], version_bytes[1])

        if version == (1, 0):
            length_bytes = file.read(2)

            if len(length_bytes) != 2:
                raise NpyFormatError("Truncated NPY header length")

            header_length = struct.unpack("<H", length_bytes)[0]
            encoding = "latin1"

        elif version in {(2, 0), (3, 0)}:
            length_bytes = file.read(4)

            if len(length_bytes) != 4:
                raise NpyFormatError("Truncated NPY header length")

            header_length = struct.unpack("<I", length_bytes)[0]
            encoding = "utf-8" if version == (3, 0) else "latin1"

        else:
            raise NpyFormatError(
                f"Unsupported NPY format version: {version}"
            )

        if header_length > MAX_NPY_HEADER:
            raise NpyFormatError(
                f"NPY header is too large: {header_length} bytes"
            )

        header_bytes = file.read(header_length)

        if len(header_bytes) != header_length:
            raise NpyFormatError("Truncated NPY header")

        try:
            header = ast.literal_eval(
                header_bytes.decode(encoding).strip()
            )
        except (SyntaxError, ValueError, UnicodeDecodeError) as error:
            raise NpyFormatError("Invalid NPY header") from error

        required_keys = {"descr", "fortran_order", "shape"}

        if not isinstance(header, dict):
            raise NpyFormatError("NPY header is not a dictionary")

        if not required_keys.issubset(header):
            raise NpyFormatError(
                "NPY header is missing required fields"
            )

        dtype = np.dtype(header["descr"])
        shape = tuple(int(dimension) for dimension in header["shape"])
        fortran_order = bool(header["fortran_order"])
        data_offset = file.tell()

    if dtype.hasobject:
        raise NpyFormatError(
            "Object arrays contain pickled objects and should use "
            "the generic chunker"
        )

    element_count = 1

    for dimension in shape:
        if dimension < 0:
            raise NpyFormatError("Negative array dimension")

        element_count *= dimension

    # A scalar array has shape=() but contains one element.
    payload_size = element_count * dtype.itemsize

    actual_size = os.path.getsize(path)
    expected_size = data_offset + payload_size

    if actual_size != expected_size:
        raise NpyFormatError(
            "NPY file size does not match its header: "
            f"expected {expected_size}, found {actual_size}"
        )

    return {
        "version": version,
        "data_offset": data_offset,
        "shape": shape,
        "fortran_order": fortran_order,
        "dtype": dtype,
        "payload_size": payload_size,
    }


def _choose_npy_alignment(
    shape: tuple[int, ...],
    dtype: np.dtype,
    fortran_order: bool,
    target_size: int,
) -> int:
    """
    Select a useful payload alignment.

    For C-order arrays, align chunks to complete slabs along axis 0.

    For Fortran-order arrays, align chunks to complete slabs along the
    last axis.

    If a slab is larger than the target chunk, fall back to dtype
    alignment.
    """
    item_size = dtype.itemsize

    if item_size <= 0:
        raise NpyFormatError("Invalid dtype item size")

    if not shape or any(dimension == 0 for dimension in shape):
        return item_size

    if fortran_order:
        # In Fortran order, earlier axes vary fastest.
        slab_elements = 1

        for dimension in shape[:-1]:
            slab_elements *= dimension
    else:
        # In C order, later axes vary fastest.
        slab_elements = 1

        for dimension in shape[1:]:
            slab_elements *= dimension

    slab_size = slab_elements * item_size

    if 0 < slab_size <= target_size:
        return slab_size

    return item_size


def _aligned_chunk_size(
    target_size: int,
    alignment: int,
) -> int:
    """
    Return the largest alignment multiple not exceeding target_size.
    """
    if target_size <= 0:
        raise ValueError("target_size must be positive")

    if alignment <= 0:
        raise ValueError("alignment must be positive")

    if alignment > target_size:
        return alignment

    return max(
        alignment,
        (target_size // alignment) * alignment,
    )


def _make_chunk(view: memoryview) -> dict[str, object]:
    """
    Produce the dictionary expected by the existing dsi-vcs chunk store.

    The memoryview is hashed directly. One bytes copy is then made for
    the returned chunk data.
    """
    return {
        "sha256": hashlib.sha256(view).hexdigest(),
        "size": len(view),
        "data": bytes(view),
    }


def chunk_npy_file(
    path: str | os.PathLike[str],
    target_chunk_size: int = NPY_TARGET_CHUNK,
) -> list[dict[str, object]]:
    """
    Fast, file-aware chunking for numeric NPY files.

    Output is compatible with the existing dsi-vcs chunk_file():

        {
            "sha256": str,
            "size": int,
            "data": bytes,
        }

    Layout:
        chunk 0: complete NPY header
        chunk 1+: fixed, array-aligned payload chunks
    """
    metadata = _read_npy_metadata(path)

    data_offset = metadata["data_offset"]
    payload_size = metadata["payload_size"]

    alignment = _choose_npy_alignment(
        shape=metadata["shape"],
        dtype=metadata["dtype"],
        fortran_order=metadata["fortran_order"],
        target_size=target_chunk_size,
    )

    payload_chunk_size = _aligned_chunk_size(
        target_size=target_chunk_size,
        alignment=alignment,
    )

    chunks: list[dict[str, object]] = []

    with open(path, "rb") as file:
        with mmap.mmap(
            file.fileno(),
            length=0,
            access=mmap.ACCESS_READ,
        ) as mapped_file:
            # Keep the complete NPY header as an independent chunk.
            header_view = memoryview(mapped_file)[0:data_offset]

            try:
                chunks.append(_make_chunk(header_view))
            finally:
                header_view.release()

            payload_end = data_offset + payload_size
            position = data_offset

            while position < payload_end:
                end = min(
                    position + payload_chunk_size,
                    payload_end,
                )

                payload_view = memoryview(mapped_file)[position:end]

                try:
                    chunks.append(_make_chunk(payload_view))
                finally:
                    payload_view.release()

                position = end

    return chunks
