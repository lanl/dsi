from __future__ import annotations

import hashlib
import mmap
import os
from pathlib import Path


JSON_TARGET_CHUNK = 8 * 1024 * 1024  # 8 MiB

JSON_WHITESPACE = {
    ord(" "),
    ord("\t"),
    ord("\r"),
    ord("\n"),
}


class JSONFormatError(ValueError):
    pass


def _make_json_chunk(
    view: memoryview,
    *,
    kind: str,
    start_unit: int | None = None,
    unit_count: int | None = None,
) -> dict[str, object]:

    digest = hashlib.sha256(view).hexdigest()

    result = {
        "sha256": digest,
        "size": len(view),
        "data": bytes(view),
        "kind": kind,
    }

    if start_unit is not None:
        result["start_unit"] = start_unit

    if unit_count is not None:
        result["unit_count"] = unit_count

    return result


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


def chunk_json_file(
    path: str | os.PathLike[str],
    target_chunk_size: int = JSON_TARGET_CHUNK,
) -> list[dict[str, object]]:
    """
    Structure-aware JSON chunking.

    Top-level array:
        chunks contain complete array elements.

    Top-level object:
        chunks contain complete object members.

    The original JSON bytes are preserved exactly.

    Concatenating chunk["data"] in returned order recreates
    the original file byte-for-byte.
    """

    path = Path(path)

    if target_chunk_size <= 0:
        raise ValueError(
            "target_chunk_size must be positive"
        )

    file_size = path.stat().st_size

    if file_size == 0:
        raise JSONFormatError(
            "Cannot chunk an empty JSON file"
        )

    chunks: list[dict[str, object]] = []

    with open(path, "rb") as file:

        with mmap.mmap(
            file.fileno(),
            length=0,
            access=mmap.ACCESS_READ,
        ) as mapped:

            (
                root_type,
                content_start,
                root_close,
                unit_starts,
            ) = _find_json_structure(mapped)

            # --------------------------------
            # Scalar JSON
            # --------------------------------

            if root_type == "scalar":

                view = memoryview(mapped)

                try:
                    chunks.append(
                        _make_json_chunk(
                            view,
                            kind="json-scalar",
                        )
                    )
                finally:
                    view.release()

                return chunks

            # --------------------------------
            # Empty [] or {}
            # --------------------------------

            if not unit_starts:

                view = memoryview(mapped)

                try:
                    chunks.append(
                        _make_json_chunk(
                            view,
                            kind=(
                                f"json-{root_type}"
                            ),
                        )
                    )
                finally:
                    view.release()

                return chunks

            # --------------------------------
            # Prefix
            #
            # Includes:
            #   BOM
            #   whitespace
            #   root "[" / "{"
            #   whitespace before first unit
            # --------------------------------

            if content_start > 0:

                prefix = memoryview(
                    mapped
                )[0:content_start]

                try:
                    chunks.append(
                        _make_json_chunk(
                            prefix,
                            kind="json-prefix",
                        )
                    )
                finally:
                    prefix.release()

            # --------------------------------
            # Array elements / object members
            # --------------------------------

            group_start = unit_starts[0]
            group_first_unit = 0

            for unit_index in range(
                len(unit_starts)
            ):

                unit_start = unit_starts[
                    unit_index
                ]

                if (
                    unit_index + 1
                    < len(unit_starts)
                ):
                    unit_end = unit_starts[
                        unit_index + 1
                    ]
                else:
                    unit_end = root_close

                prospective_size = (
                    unit_end - group_start
                )

                # Current unit would push the
                # group over the target.
                if (
                    unit_index > group_first_unit
                    and prospective_size
                    > target_chunk_size
                ):

                    view = memoryview(
                        mapped
                    )[group_start:unit_start]

                    try:
                        chunks.append(
                            _make_json_chunk(
                                view,
                                kind=(
                                    "json-elements"
                                    if root_type == "array"
                                    else "json-members"
                                ),
                                start_unit=(
                                    group_first_unit
                                ),
                                unit_count=(
                                    unit_index
                                    - group_first_unit
                                ),
                            )
                        )
                    finally:
                        view.release()

                    group_start = unit_start
                    group_first_unit = unit_index

                # Emit immediately if the current
                # group reaches the target.
                current_size = (
                    unit_end - group_start
                )

                if (
                    current_size
                    >= target_chunk_size
                ):

                    view = memoryview(
                        mapped
                    )[group_start:unit_end]

                    try:
                        chunks.append(
                            _make_json_chunk(
                                view,
                                kind=(
                                    "json-elements"
                                    if root_type == "array"
                                    else "json-members"
                                ),
                                start_unit=(
                                    group_first_unit
                                ),
                                unit_count=(
                                    unit_index
                                    - group_first_unit
                                    + 1
                                ),
                            )
                        )
                    finally:
                        view.release()

                    if (
                        unit_index + 1
                        < len(unit_starts)
                    ):
                        group_start = (
                            unit_starts[
                                unit_index + 1
                            ]
                        )

                        group_first_unit = (
                            unit_index + 1
                        )

            # --------------------------------
            # Remaining units
            # --------------------------------

            if group_first_unit < len(
                unit_starts
            ):

                if group_start < root_close:

                    view = memoryview(
                        mapped
                    )[group_start:root_close]

                    try:
                        chunks.append(
                            _make_json_chunk(
                                view,
                                kind=(
                                    "json-elements"
                                    if root_type == "array"
                                    else "json-members"
                                ),
                                start_unit=(
                                    group_first_unit
                                ),
                                unit_count=(
                                    len(unit_starts)
                                    - group_first_unit
                                ),
                            )
                        )
                    finally:
                        view.release()

            # --------------------------------
            # Suffix
            #
            # Includes closing "]" / "}"
            # and trailing whitespace.
            # --------------------------------

            suffix = memoryview(
                mapped
            )[root_close:file_size]

            try:
                chunks.append(
                    _make_json_chunk(
                        suffix,
                        kind="json-suffix",
                    )
                )
            finally:
                suffix.release()

    return chunks