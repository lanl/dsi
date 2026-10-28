from __future__ import annotations

import os
import zipfile
from pathlib import Path
from typing import Iterator

from .mapped_chunking import ChunkRange, PayloadPredicate, iter_mapped_chunks


XLSX_TARGET_CHUNK = 8 * 1024 * 1024  # 8 MiB


class XLSXFormatError(ValueError):
    pass

def is_xlsx_file(
    path: str | os.PathLike[str],
) -> bool:
    """
    Verify that the file is a ZIP-based OOXML spreadsheet.
    """

    path = Path(path)

    if not zipfile.is_zipfile(path):
        return False

    try:
        with zipfile.ZipFile(path, "r") as archive:

            names = set(archive.namelist())

            return (
                "[Content_Types].xml" in names
                and "xl/workbook.xml" in names
            )

    except (OSError, zipfile.BadZipFile):
        return False


def chunk_xlsx_file(path, target_chunk_size=XLSX_TARGET_CHUNK):
    """Return chunks at ZIP-member boundaries without recompressing data."""
    return list(iter_xlsx_file(path, target_chunk_size))


def iter_xlsx_file(
    path: str | os.PathLike[str],
    target_chunk_size: int = XLSX_TARGET_CHUNK,
    *,
    needs_data: PayloadPredicate | None = None,
) -> Iterator[dict[str, object]]:
    """Validate the ZIP layout and stream original XLSX bytes on demand."""
    if target_chunk_size <= 0:
        raise ValueError("target_chunk_size must be positive")
    if not is_xlsx_file(path):
        raise XLSXFormatError(f"{path} is not a valid XLSX workbook")
    with zipfile.ZipFile(path, "r") as archive:
        infos = sorted(archive.infolist(), key=lambda info: info.header_offset)
    if not infos:
        raise XLSXFormatError("XLSX archive contains no ZIP members")
    return iter_mapped_chunks(
        path, lambda mapped: _xlsx_ranges(mapped, infos, target_chunk_size),
        needs_data=needs_data,
    )


def _xlsx_ranges(mapped, infos, target_chunk_size) -> Iterator[ChunkRange]:
    if infos[0].header_offset > 0:
        yield 0, infos[0].header_offset, {"kind": "xlsx-prefix", "members": []}
    group_start = None
    members = []
    for index, info in enumerate(infos):
        start = info.header_offset
        end = infos[index + 1].header_offset if index + 1 < len(infos) else len(mapped)
        if group_start is None:
            group_start = start
        if members and end - group_start > target_chunk_size:
            yield group_start, start, {"kind": "xlsx-members", "members": members.copy()}
            group_start = start
            members = []
        members.append(info.filename)
        if end - group_start >= target_chunk_size:
            yield group_start, end, {"kind": "xlsx-members", "members": members.copy()}
            group_start = end
            members = []
    if members and group_start is not None and group_start < len(mapped):
        yield group_start, len(mapped), {"kind": "xlsx-members", "members": members.copy()}
