from __future__ import annotations

import mmap
import os
from pathlib import Path
from typing import Iterator

from .mapped_chunking import ChunkRange, PayloadPredicate, iter_mapped_chunks


JSON_TARGET_CHUNK = 8 * 1024 * 1024  # 8 MiB

JSON_WHITESPACE = {
    ord(" "),
    ord("\t"),
    ord("\r"),
    ord("\n"),
}


class JSONFormatError(ValueError):
    pass


def _skip_whitespace(
    data: mmap.mmap,
    position: int,
    end: int,
) -> int:
    """
    Skip JSON whitespace.
    """

    while (
        position < end
        and data[position] in JSON_WHITESPACE
    ):
        position += 1

    return position


def _find_json_structure(
    data: mmap.mmap,
) -> tuple[str, int, int, list[int]]:
    """
    Inspect the top-level JSON structure.

    Returns:

        (
            root_type,      # "array", "object", or "scalar"
            content_start,
            root_close,
            unit_starts
        )

    unit_starts contains the byte offset of each top-level
    array element or object member.

    Strings, escaped quotes, and nested objects/arrays are handled.
    """

    size = len(data)

    if size == 0:
        raise JSONFormatError("Empty JSON file")

    position = 0

    # Optional UTF-8 BOM.
    if data[:3] == b"\xef\xbb\xbf":
        position = 3

    position = _skip_whitespace(
        data,
        position,
        size,
    )

    if position >= size:
        raise JSONFormatError(
            "JSON contains only whitespace"
        )

    root_byte = data[position]

    # --------------------------------
    # Scalar root
    # --------------------------------

    if root_byte not in {
        ord("["),
        ord("{"),
    }:
        return (
            "scalar",
            position,
            size,
            [position],
        )

    if root_byte == ord("["):
        root_type = "array"
        expected_close = ord("]")
    else:
        root_type = "object"
        expected_close = ord("}")

    root_open = position

    # Stack tracks nested [] / {}.
    stack: list[int] = []

    in_string = False
    escaped = False

    separators: list[int] = []

    root_close: int | None = None

    i = root_open

    while i < size:

        value = data[i]

        # -----------------------------
        # Inside a JSON string
        # -----------------------------

        if in_string:

            if escaped:
                escaped = False

            elif value == ord("\\"):
                escaped = True

            elif value == ord('"'):
                in_string = False

            i += 1
            continue

        # -----------------------------
        # Outside strings
        # -----------------------------

        if value == ord('"'):
            in_string = True

        elif value == ord("["):
            stack.append(ord("]"))

        elif value == ord("{"):
            stack.append(ord("}"))

        elif value in {
            ord("]"),
            ord("}"),
        }:

            if not stack:
                raise JSONFormatError(
                    "Unexpected JSON closing delimiter"
                )

            required = stack.pop()

            if value != required:
                raise JSONFormatError(
                    "Mismatched JSON delimiters"
                )

            if not stack:
                root_close = i
                break

        # A comma at depth 1 separates
        # root-level elements/members.
        elif (
            value == ord(",")
            and len(stack) == 1
        ):
            separators.append(i)

        i += 1

    if in_string:
        raise JSONFormatError(
            "JSON ended inside a string"
        )

    if root_close is None:
        raise JSONFormatError(
            "Could not find JSON root closing delimiter"
        )

    if data[root_close] != expected_close:
        raise JSONFormatError(
            "Unexpected JSON root delimiter"
        )

    # --------------------------------
    # Ensure only whitespace follows
    # the root document.
    # --------------------------------

    trailing = _skip_whitespace(
        data,
        root_close + 1,
        size,
    )

    if trailing != size:
        raise JSONFormatError(
            "Unexpected data after JSON root"
        )

    # --------------------------------
    # Determine first unit
    # --------------------------------

    content_start = _skip_whitespace(
        data,
        root_open + 1,
        root_close,
    )

    # [] or {}
    if content_start >= root_close:
        return (
            root_type,
            content_start,
            root_close,
            [],
        )

    unit_starts = [
        content_start
    ]

    # Each top-level comma starts the
    # next logical unit.
    for separator in separators:

        next_start = _skip_whitespace(
            data,
            separator + 1,
            root_close,
        )

        if next_start >= root_close:
            raise JSONFormatError(
                "Trailing comma in JSON"
            )

        unit_starts.append(
            next_start
        )

    return (
        root_type,
        content_start,
        root_close,
        unit_starts,
    )


def chunk_json_file(path, target_chunk_size=JSON_TARGET_CHUNK):
    """Return chunks preserving JSON bytes and top-level unit boundaries."""
    return list(iter_json_file(path, target_chunk_size))


def iter_json_file(
    path: str | os.PathLike[str],
    target_chunk_size: int = JSON_TARGET_CHUNK,
    *,
    needs_data: PayloadPredicate | None = None,
) -> Iterator[dict[str, object]]:
    """Validate JSON structure before streaming hashes and needed payloads."""
    if target_chunk_size <= 0:
        raise ValueError("target_chunk_size must be positive")
    if Path(path).stat().st_size == 0:
        raise JSONFormatError("Cannot chunk an empty JSON file")
    return iter_mapped_chunks(
        path, lambda mapped: _json_ranges(mapped, target_chunk_size), needs_data=needs_data,
    )


def _json_ranges(mapped, target_chunk_size) -> Iterator[ChunkRange]:
    root_type, content_start, root_close, starts = _find_json_structure(mapped)
    if root_type == "scalar" or not starts:
        kind = "json-scalar" if root_type == "scalar" else f"json-{root_type}"
        yield 0, len(mapped), {"kind": kind}
        return
    if content_start > 0:
        yield 0, content_start, {"kind": "json-prefix"}
    kind = "json-elements" if root_type == "array" else "json-members"
    group_start = starts[0]
    first_unit = 0
    for index, start in enumerate(starts):
        end = starts[index + 1] if index + 1 < len(starts) else root_close
        if index > first_unit and end - group_start > target_chunk_size:
            yield group_start, start, {
                "kind": kind, "start_unit": first_unit, "unit_count": index - first_unit,
            }
            group_start = start
            first_unit = index
        if end - group_start >= target_chunk_size:
            yield group_start, end, {
                "kind": kind, "start_unit": first_unit, "unit_count": index - first_unit + 1,
            }
            group_start = end
            first_unit = index + 1
    if first_unit < len(starts) and group_start < root_close:
        yield group_start, root_close, {
            "kind": kind, "start_unit": first_unit, "unit_count": len(starts) - first_unit,
        }
    yield root_close, len(mapped), {"kind": "json-suffix"}
