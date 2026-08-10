from __future__ import annotations

import hashlib
import mmap
import os
from pathlib import Path
from xml.parsers import expat


XML_TARGET_CHUNK = 8 * 1024 * 1024


class XMLFormatError(ValueError):
    pass

def _make_chunk(
    view: memoryview,
    **metadata,
) -> dict[str, object]:
    """
    Create a DSI-VCS-compatible chunk.
    """
    digest = hashlib.sha256(view).hexdigest()

    return {
        "sha256": digest,
        "size": len(view),
        "data": bytes(view),
        **metadata,
    }

def _xml_element_end(
    data: mmap.mmap,
    byte_index: int,
) -> int:
    """
    Expat reports normal EndElement events at the beginning
    of </tag>.

    For self-closing elements, CurrentByteIndex is already
    positioned after the element.
    """

    if data[
        byte_index:
        byte_index + 2
    ] == b"</":

        closing = data.find(
            b">",
            byte_index,
        )

        if closing == -1:
            raise XMLFormatError(
                "Could not find XML closing tag"
            )

        return closing + 1

    # Self-closing <element/>
    return byte_index


def _find_xml_top_level_elements(
    data: mmap.mmap,
) -> tuple[
    list[tuple[int, int]],
    int | None,
]:
    """
    Find direct children of the document root.

    Returns:
        [
            (start_byte, end_byte),
            ...
        ],
        root_closing_start
    """

    parser = expat.ParserCreate()

    depth = 0

    current_child_start = None

    elements = []

    root_closing_start = None

    def start_element(name, attrs):
        nonlocal depth
        nonlocal current_child_start

        depth += 1

        if depth == 2:
            current_child_start = (
                parser.CurrentByteIndex
            )

    def end_element(name):
        nonlocal depth
        nonlocal current_child_start
        nonlocal root_closing_start

        if depth == 2:

            if current_child_start is None:
                raise XMLFormatError(
                    "Invalid XML element state"
                )

            end = _xml_element_end(
                data,
                parser.CurrentByteIndex,
            )

            elements.append(
                (
                    current_child_start,
                    end,
                )
            )

            current_child_start = None

        elif depth == 1:

            # Start of </root>
            root_closing_start = (
                parser.CurrentByteIndex
            )

        depth -= 1

    def reject_external_entity(*args):
        raise XMLFormatError(
            "External XML entities are not supported"
        )

    parser.StartElementHandler = (
        start_element
    )

    parser.EndElementHandler = (
        end_element
    )

    parser.ExternalEntityRefHandler = (
        reject_external_entity
    )

    try:
        parser.Parse(
            data[:],
            True,
        )

    except expat.ExpatError as error:

        raise XMLFormatError(
            f"Invalid XML: {error}"
        ) from error

    return elements, root_closing_start

def chunk_xml_file(
    path: str | os.PathLike[str],
    target_chunk_size: int = XML_TARGET_CHUNK,
) -> list[dict[str, object]]:
    """
    Chunk XML at root-child element boundaries.

    Result remains byte-for-byte reconstructable simply by
    concatenating chunk data.
    """

    path = Path(path)

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

            elements, root_close = (
                _find_xml_top_level_elements(
                    mapped
                )
            )

            # XML with no root children:
            # simply store whole file.
            if not elements:

                view = memoryview(mapped)

                try:
                    chunks.append(
                        _make_chunk(
                            view,
                            kind="xml-document",
                        )
                    )
                finally:
                    view.release()

                return chunks

            # --------------------------------
            # XML prologue + root opening tag
            # --------------------------------

            first_child_start = (
                elements[0][0]
            )

            if first_child_start > 0:

                view = memoryview(
                    mapped
                )[0:first_child_start]

                try:
                    chunks.append(
                        _make_chunk(
                            view,
                            kind="xml-prefix",
                        )
                    )
                finally:
                    view.release()

            # --------------------------------
            # Root child elements
            # --------------------------------

            chunk_start = first_child_start

            element_start_index = 0

            for index, (
                element_start,
                element_end,
            ) in enumerate(elements):

                # Cut at the beginning of the
                # next element so whitespace
                # remains with the previous unit.

                if index + 1 < len(elements):

                    possible_end = (
                        elements[index + 1][0]
                    )

                else:

                    possible_end = (
                        root_close
                        if root_close is not None
                        else element_end
                    )

                size = (
                    possible_end
                    - chunk_start
                )

                if size >= target_chunk_size:

                    view = memoryview(
                        mapped
                    )[
                        chunk_start:
                        possible_end
                    ]

                    try:
                        chunks.append(
                            _make_chunk(
                                view,
                                kind="xml-elements",
                                first_element=(
                                    element_start_index
                                ),
                                element_count=(
                                    index
                                    - element_start_index
                                    + 1
                                ),
                            )
                        )
                    finally:
                        view.release()

                    chunk_start = (
                        possible_end
                    )

                    element_start_index = (
                        index + 1
                    )

            # Remaining XML elements.
            if (
                root_close is not None
                and chunk_start < root_close
            ):

                view = memoryview(
                    mapped
                )[
                    chunk_start:
                    root_close
                ]

                try:
                    chunks.append(
                        _make_chunk(
                            view,
                            kind="xml-elements",
                            first_element=(
                                element_start_index
                            ),
                            element_count=(
                                len(elements)
                                - element_start_index
                            ),
                        )
                    )
                finally:
                    view.release()

            # --------------------------------
            # </root> + trailing whitespace
            # --------------------------------

            if (
                root_close is not None
                and root_close < file_size
            ):

                view = memoryview(
                    mapped
                )[
                    root_close:
                    file_size
                ]

                try:
                    chunks.append(
                        _make_chunk(
                            view,
                            kind="xml-suffix",
                        )
                    )
                finally:
                    view.release()

    return chunks