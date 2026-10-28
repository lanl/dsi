from __future__ import annotations

import mmap
import os
from typing import Iterator
from xml.parsers import expat

from .mapped_chunking import ChunkRange, PayloadPredicate, iter_mapped_chunks


XML_TARGET_CHUNK = 8 * 1024 * 1024


class XMLFormatError(ValueError):
    pass

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
            data,
            True,
        )

    except expat.ExpatError as error:

        raise XMLFormatError(
            f"Invalid XML: {error}"
        ) from error

    return elements, root_closing_start


def chunk_xml_file(path, target_chunk_size=XML_TARGET_CHUNK):
    """Return chunks preserving XML bytes at root-child boundaries."""
    return list(iter_xml_file(path, target_chunk_size))


def iter_xml_file(
    path: str | os.PathLike[str],
    target_chunk_size: int = XML_TARGET_CHUNK,
    *,
    needs_data: PayloadPredicate | None = None,
) -> Iterator[dict[str, object]]:
    """Validate XML before streaming chunks and copying only needed payloads."""
    return iter_mapped_chunks(
        path, lambda mapped: _xml_ranges(mapped, target_chunk_size), needs_data=needs_data,
    )


def _xml_ranges(mapped, target_chunk_size) -> Iterator[ChunkRange]:
    elements, root_close = _find_xml_top_level_elements(mapped)
    if not elements:
        yield 0, len(mapped), {"kind": "xml-document"}
        return
    chunk_start = elements[0][0]
    if chunk_start > 0:
        yield 0, chunk_start, {"kind": "xml-prefix"}
    first_element = 0
    for index, (_, element_end) in enumerate(elements):
        if index + 1 < len(elements):
            end = elements[index + 1][0]
        else:
            end = root_close if root_close is not None else element_end
        if end - chunk_start >= target_chunk_size:
            yield chunk_start, end, {
                "kind": "xml-elements", "first_element": first_element,
                "element_count": index - first_element + 1,
            }
            chunk_start = end
            first_element = index + 1
    if root_close is not None and chunk_start < root_close:
        yield chunk_start, root_close, {
            "kind": "xml-elements", "first_element": first_element,
            "element_count": len(elements) - first_element,
        }
    if root_close is not None and root_close < len(mapped):
        yield root_close, len(mapped), {"kind": "xml-suffix"}
