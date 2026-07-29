"""
Data models for the persistent, append-only repository log.

Two kinds of record share one on-disk record format:

- `RecordKind.DATA`   -- describes a single file/chunk-level change
                          (architecture section 1's per-record metadata).
- `RecordKind.COMMIT` -- an immutable "commit boundary" marker. Because
                          the log is append-only, a commit is never
                          represented by mutating earlier records; it is
                          represented by *appending* a new record that
                          lists the hashes of the data records it
                          finalizes (architecture section 4, step 10:
                          "Establishes a durable commit boundary in the
                          repository log").

Both kinds participate in the same hash chain (`prev_record_hash` /
`record_hash`), so tampering with a commit marker is just as detectable
as tampering with a data record.

Per the architecture doc, a record never contains actual file/chunk
content -- only metadata and hash references.
"""

from __future__ import annotations

import enum
import uuid
import time
from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field, model_validator


class OperationType(str, enum.Enum):
    """Kinds of change a DATA record can describe."""

    FILE_ADD = "file_add"
    FILE_DELETE = "file_delete"
    FILE_REMOVE = "file_remove"
    FILE_METADATA_UPDATE = "file_metadata_update"
    CHUNK_INSERT = "chunk_insert"
    CHUNK_REPLACE = "chunk_replace"


class RecordKind(str, enum.Enum):
    """Whether a log record is a data change or a commit boundary marker."""

    DATA = "data"
    COMMIT = "commit"


def _utcnow() -> int:
    return time.time_ns()


def new_id() -> str:
    """Generate a new opaque identifier (record/transaction/commit ids)."""
    return uuid.uuid4().hex


def ns_to_datetime_parts(timestamp_ns: int) -> dict:
    """
    Convert a Unix timestamp in nanoseconds to UTC date/time components.

    Returns:
        {
            "year": int,
            "month": int,
            "day": int,
            "hour": int,
            "minute": int,
            "second": int,
            "millisecond": int,
            "nanosecond": int,
        }
    """
    seconds, nanosecond = divmod(timestamp_ns, 1_000_000_000)

    dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
    return dt
    # return {
    #     "year": dt.year,
    #     "month": dt.month,
    #     "day": dt.day,
    #     "hour": dt.hour,
    #     "minute": dt.minute,
    #     "second": dt.second,
    #     "millisecond": nanosecond // 1_000_000,
    #     "nanosecond": nanosecond,
    # }


def datetime_parts_to_ns(
    year: int,
    month: int,
    day: int,
    hour: int,
    minute: int,
    second: int,
    millisecond: int,
    nanosecond: int = 0,
) -> int:
    """
    Convert UTC date/time components into a Unix timestamp in nanoseconds.

    Parameters:
        millisecond : 0-999
        nanosecond  : additional nanoseconds within the millisecond (0-999999)
    """
    if not (0 <= millisecond < 1000):
        raise ValueError("millisecond must be between 0 and 999")

    if not (0 <= nanosecond < 1_000_000):
        raise ValueError("nanosecond must be between 0 and 999999")

    dt = datetime(
        year,
        month,
        day,
        hour,
        minute,
        second,
        tzinfo=timezone.utc,
    )

    return (
        int(dt.timestamp()) * 1_000_000_000
        + millisecond * 1_000_000
        + nanosecond
    )

class LogRecord(BaseModel):
    """
    A single, immutable entry in the repository's append-only log.

    Identity / chain fields apply to every record regardless of kind:

      - sequence           -> this repository's monotonic position in the log
      - prev_record_hash    -> hash pointer to the previous record ("blockchain"
                               link)
      - record_hash          -> this record's own derived hash

    DATA-kind fields map onto the metadata list in architecture section 1:

      - file_path      -> File identifier or path
      - chunk_ref       -> Affected chunk or file region
      - operation       -> Operation type
      - chunk_hash      -> Chunk hash for the updated content
      - transaction_id  -> Transaction / staging identifier

    COMMIT-kind fields represent the durable commit boundary:

      - commit_id                  -> identifier for this commit
      - committed_transaction_id    -> which staged transaction this finalizes
      - committed_record_hashes     -> record_hash of every DATA record being
                                       finalized by this commit, in order
    """

    # Identity -----------------------------------------------------------
    record_id: str = Field(default_factory=new_id)
    sequence: int = Field(
        ..., description="Monotonic, per-repository position in the log (0-based)."
    )
    repository_id: str
    kind: RecordKind

    # DATA-kind fields -----------------------------------------------------
    file_path: Optional[str] = None
    chunk_ref: Optional[str] = Field(
        default=None,
        description="Affected chunk or byte-range/region identifier within the file.",
    )
    operation: Optional[OperationType] = None
    chunk_hash: Optional[str] = Field(
        default=None,
        description="Content-hash of the updated chunk. None for e.g. FILE_DELETE.",
    )
    transaction_id: Optional[str] = Field(
        default=None,
        description="Staging/transaction identifier this DATA record belongs to.",
    )
    extra_metadata: dict = Field(
        default_factory=dict,
        description="Free-form additional metadata (e.g. file size, mode bits).",
    )

    # COMMIT-kind fields -----------------------------------------------------
    commit_id: Optional[str] = None
    committed_transaction_id: Optional[str] = None
    committed_record_hashes: Optional[list[str]] = Field(
        default=None,
        description="record_hash values of the DATA records this commit finalizes, "
        "in sequence order.",
    )

    # Chain linkage -------------------------------------------------------
    prev_record_hash: Optional[str] = Field(
        default=None,
        description="Hash of the previous record in this repository's chain, "
        "or None for the first record in the log.",
    )
    record_hash: Optional[str] = Field(
        default=None,
        description="This record's own hash, derived from its metadata and "
        "prev_record_hash. Populated on append.",
    )

    # Bookkeeping ----------------------------------------------------------
    created_at: int = Field(default_factory=_utcnow)

    model_config = {"use_enum_values": False}

    @model_validator(mode="after")
    def _check_kind_specific_fields(self) -> "LogRecord":
        if self.kind == RecordKind.DATA:
            if not self.file_path or not self.file_path.strip():
                raise ValueError("DATA records require a non-empty file_path")
            if self.operation is None:
                raise ValueError("DATA records require an operation")
            if not self.transaction_id:
                raise ValueError("DATA records require a transaction_id")
        elif self.kind == RecordKind.COMMIT:
            if not self.commit_id:
                raise ValueError("COMMIT records require a commit_id")
            if not self.committed_record_hashes:
                raise ValueError(
                    "COMMIT records require a non-empty committed_record_hashes"
                )
        return self

    def hashable_payload(self) -> dict:
        """
        Fields that feed into the record's hash.

        Excludes `record_hash` itself (obviously) and `created_at`
        (wall-clock jitter should not affect chain verification).
        Everything else -- including `prev_record_hash` -- is part of
        the hashed payload, which is what makes this a hash-linked
        ("blockchain-like") chain: each record's hash commits to the
        identity of its predecessor.
        """
        return {
            "record_id": self.record_id,
            "sequence": self.sequence,
            "repository_id": self.repository_id,
            "kind": self.kind.value if isinstance(self.kind, RecordKind) else self.kind,
            "file_path": self.file_path,
            "chunk_ref": self.chunk_ref,
            "operation": (
                self.operation.value
                if isinstance(self.operation, OperationType)
                else self.operation
            ),
            "created_at": self.created_at,
            "chunk_hash": self.chunk_hash,
            "transaction_id": self.transaction_id,
            "extra_metadata": self.extra_metadata,
            "commit_id": self.commit_id,
            "committed_transaction_id": self.committed_transaction_id,
            "committed_record_hashes": self.committed_record_hashes,
            "prev_record_hash": self.prev_record_hash,
        }
