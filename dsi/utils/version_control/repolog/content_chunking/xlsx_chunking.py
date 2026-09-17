from __future__ import annotations

import hashlib
import mmap
import os
import zipfile
from pathlib import Path


XLSX_TARGET_CHUNK = 8 * 1024 * 1024  # 8 MiB


class XLSXFormatError(ValueError):
    pass


def _make_xlsx_chunk(
    view: memoryview,
    *,
    kind: str,
    members: list[str],
) -> dict[str, object]:

    digest = hashlib.sha256(view).hexdigest()

    return {
        "sha256": digest,
        "size": len(view),
        "data": bytes(view),

        # Optional DSI-VCS metadata
        "kind": kind,
        "members": members,
    }


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


def chunk_xlsx_file(
    path: str | os.PathLike[str],
    target_chunk_size: int = XLSX_TARGET_CHUNK,
) -> list[dict[str, object]]:
    """
    Chunk an XLSX file at ZIP-member boundaries.

    Properties:

    - Never cuts through a ZIP member.
    - Does not decompress/recompress the workbook.
    - Preserves original XLSX bytes exactly.
    - Groups small ZIP members into approximately target_chunk_size
      chunks.
    - Large members remain individual chunks.

    Concatenating chunk["data"] in order recreates the original
    workbook exactly.
    """

    path = Path(path)

    if target_chunk_size <= 0:
        raise ValueError(
            "target_chunk_size must be positive"
        )

    if not is_xlsx_file(path):
        raise XLSXFormatError(
            f"{path} is not a valid XLSX workbook"
        )

    file_size = path.stat().st_size

    if file_size == 0:
        raise XLSXFormatError(
            "Empty file cannot be XLSX"
        )

    # --------------------------------
    # Read ZIP member locations
    # --------------------------------

    with zipfile.ZipFile(path, "r") as archive:

        infos = sorted(
            archive.infolist(),
            key=lambda info: info.header_offset,
        )

    if not infos:
        raise XLSXFormatError(
            "XLSX archive contains no ZIP members"
        )

    chunks: list[dict[str, object]] = []

    with open(path, "rb") as file:

        with mmap.mmap(
            file.fileno(),
            length=0,
            access=mmap.ACCESS_READ,
        ) as mapped:

            first_header = infos[0].header_offset

            # --------------------------------
            # Optional prefix
            #
            # XLSX normally starts immediately
            # with a ZIP local header.
            # --------------------------------

            if first_header > 0:

                prefix_view = memoryview(
                    mapped
                )[0:first_header]

                try:
                    chunks.append(
                        _make_xlsx_chunk(
                            prefix_view,
                            kind="xlsx-prefix",
                            members=[],
                        )
                    )

                finally:
                    prefix_view.release()

            # --------------------------------
            # ZIP members
            # --------------------------------

            group_start = None
            group_members: list[str] = []

            for index, info in enumerate(infos):

                member_start = info.header_offset

                # The next member's local-header offset is
                # the end boundary of this member's physical
                # ZIP representation.
                #
                # For the final member, using EOF also captures
                # the ZIP central directory and end records.
                if index + 1 < len(infos):

                    member_end = (
                        infos[index + 1].header_offset
                    )

                else:
                    member_end = file_size

                if group_start is None:
                    group_start = member_start

                prospective_size = (
                    member_end - group_start
                )

                # --------------------------------
                # Current member would make the
                # group too large.
                #
                # Flush everything before it.
                # --------------------------------

                if (
                    group_members
                    and prospective_size
                    > target_chunk_size
                ):

                    previous_end = member_start

                    view = memoryview(
                        mapped
                    )[group_start:previous_end]

                    try:
                        chunks.append(
                            _make_xlsx_chunk(
                                view,
                                kind="xlsx-members",
                                members=group_members.copy(),
                            )
                        )

                    finally:
                        view.release()

                    group_start = member_start
                    group_members = []

                group_members.append(
                    info.filename
                )

                current_group_size = (
                    member_end - group_start
                )

                # --------------------------------
                # If this group has already reached
                # the target size, emit it now.
                #
                # This also isolates a single large
                # ZIP member automatically.
                # --------------------------------

                if (
                    current_group_size
                    >= target_chunk_size
                ):

                    view = memoryview(
                        mapped
                    )[group_start:member_end]

                    try:
                        chunks.append(
                            _make_xlsx_chunk(
                                view,
                                kind="xlsx-members",
                                members=group_members.copy(),
                            )
                        )

                    finally:
                        view.release()

                    group_start = member_end
                    group_members = []

            # --------------------------------
            # Remaining small members
            # --------------------------------

            if (
                group_members
                and group_start is not None
                and group_start < file_size
            ):

                view = memoryview(
                    mapped
                )[group_start:file_size]

                try:
                    chunks.append(
                        _make_xlsx_chunk(
                            view,
                            kind="xlsx-members",
                            members=group_members.copy(),
                        )
                    )

                finally:
                    view.release()

    return chunks