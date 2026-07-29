"""
The persistent, append-only, hash-linked repository log (architecture
sections 1-2, plus the log's own share of section 4's commit process
and section 11's recovery model).

This module does not implement chunk storage, version tables, version
graphs, Merkle trees, or any SQL-based indexing. Those are separate
components; the intended integration point for an external index is
`RepositoryLog.iter_records()`, which sequentially yields every
`(Location, LogRecord)` in the log so an index can be built or
incrementally refreshed without this module knowing anything about SQL.
"""

from __future__ import annotations

from logging import log
import os
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Optional, Sequence, Union

from .exceptions import ChainIntegrityError, InvalidCommitError
from .file_store import Location, LogFileStore, TruncationReport
from .merkle import compute_record_hash
from .log_record import LogRecord, OperationType, RecordKind, new_id


@dataclass(frozen=True)
class AppendResult:
    """Result from appending a record: the record and where it landed."""

    record: LogRecord
    location: Location


@dataclass(frozen=True)
class VerificationResult:
    """Outcome of verifying some span of the hash chain."""

    ok: bool
    checked_count: int
    first_broken_sequence: Optional[int] = None
    reason: Optional[str] = None


@dataclass(frozen=True)
class RecoveryState:
    """Result of running `RepositoryLog.recover()`."""

    truncation: TruncationReport
    tail_location: Optional[Location]
    tail_sequence: Optional[int]
    tail_record_hash: Optional[str]
    verification: Optional[VerificationResult]


class RepositoryLog:
    """
    Append-only, hash-linked log for a single repository, backed by one
    ever-growing file.

    Nothing already written is ever mutated: a "commit" is represented
    by appending a new `RecordKind.COMMIT` record that lists the hashes
    of the DATA records it finalizes, rather than flipping a status
    flag on old records.
    """

    def __init__(self, log_file_dir: Union[str, Path], repository_id: str):
        self.log_file = os.path.join(log_file_dir, f"{repository_id}.log")
        self.latest_commit_location: Optional[Location] = None # this is stored in version sql table
        self.repository_id = repository_id
        self.store = LogFileStore(self.log_file)        
        self._lock = threading.Lock()
        self._tail_cache: Optional[tuple[Location, LogRecord]] = None

    # -- tail tracking (used to resume appending after a restart) --------

    def _decode(self, payload: bytes) -> LogRecord:
        return LogRecord.model_validate_json(payload)

    def _load_tail(self) -> Optional[tuple[Location, LogRecord]]:
        location = self.store.latest_location()
        if location is None:
            return None
        record = self._decode(self.store.read_at(location))
        return location, record

    def _get_tail(self) -> Optional[tuple[Location, LogRecord]]:
        if self._tail_cache is None:
            self._tail_cache = self._load_tail()
        return self._tail_cache

    # -- appending (writing new DATA records) --------------------------------

    def append(
        self,
        *,
        file_path: str,
        operation: OperationType,
        chunk_hash: Optional[str] = None,
        chunk_ref: Optional[str] = None,
        transaction_id: Optional[str] = None,
        extra_metadata: Optional[dict] = None,
    ) -> AppendResult:
        """Append a single new DATA record to the log."""
        with self._lock:
            tail = self._get_tail()
            prev_hash = tail[1].record_hash if tail else None
            next_sequence = tail[1].sequence + 1 if tail else 0

            record = LogRecord(
                sequence=next_sequence,
                repository_id=self.repository_id,
                kind=RecordKind.DATA,
                file_path=file_path,
                chunk_ref=chunk_ref,
                operation=operation,
                chunk_hash=chunk_hash,
                transaction_id=transaction_id or new_id(),
                extra_metadata=extra_metadata or {},
                prev_record_hash=prev_hash,
            )
            record.record_hash = compute_record_hash(
                record.hashable_payload(), prev_hash
            )

            location = self.store.append(record.model_dump_json().encode("utf-8"))
            self._tail_cache = (location, record)
            return AppendResult(record=record, location=location)

    def append_many(
        self, entries: Iterable[dict], *, transaction_id: Optional[str] = None
    ) -> list[AppendResult]:
        """
        Append several DATA records as one logical staged batch (e.g.
        all changes belonging to one working-copy transaction), each
        chained to the one before it.

        Each item in `entries` is a dict of keyword args accepted by
        `append` (minus `transaction_id`, shared across the batch).
        """
        txn_id = transaction_id or new_id()
        with self._lock:
            tail = self._get_tail()
            prev_hash = tail[1].record_hash if tail else None
            next_sequence = tail[1].sequence + 1 if tail else 0

            results: list[AppendResult] = []
            for entry in entries:
                record = LogRecord(
                    sequence=next_sequence,
                    repository_id=self.repository_id,
                    kind=RecordKind.DATA,
                    file_path=entry["file_path"],
                    chunk_ref=entry.get("chunk_ref"),
                    operation=entry["operation"],
                    chunk_hash=entry.get("chunk_hash"),
                    transaction_id=txn_id,
                    extra_metadata=entry.get("extra_metadata") or {},
                    prev_record_hash=prev_hash,
                )
                record.record_hash = compute_record_hash(
                    record.hashable_payload(), prev_hash
                )
                location = self.store.append(record.model_dump_json().encode("utf-8"))
                results.append(AppendResult(record=record, location=location))

                prev_hash = record.record_hash
                next_sequence += 1

            self._tail_cache = (results[-1].location, results[-1].record)
            return results

    # -- commit boundaries (append-only "commit", section 4) -----------------

    def commit(
        self,
        records: Sequence[LogRecord],
        *,
        transaction_id: Optional[str] = None,
    ) -> AppendResult:
        """
        Finalize a set of previously-appended DATA records by appending
        a COMMIT marker record that references their hashes.

        `records` should be the `LogRecord` objects previously returned
        by `append`/`append_many` (e.g. all records from one
        transaction). They must all belong to this repository and, as
        a sanity check, must appear with strictly increasing `sequence`
        values (out of order or duplicated input is rejected).
        """
        if not records:
            raise InvalidCommitError("Cannot commit an empty set of records.")

        ordered = sorted(records, key=lambda r: r.sequence)
        seen_sequences = set()
        for r in ordered:
            if r.repository_id != self.repository_id:
                raise InvalidCommitError(
                    f"Record {r.record_id} belongs to repository "
                    f"{r.repository_id!r}, not {self.repository_id!r}."
                )
            if r.kind != RecordKind.DATA:
                raise InvalidCommitError(
                    f"Only DATA records can be committed; {r.record_id} is "
                    f"kind={r.kind!r}."
                )
            if r.sequence in seen_sequences:
                raise InvalidCommitError(
                    f"Duplicate record for sequence {r.sequence} in commit input."
                )
            seen_sequences.add(r.sequence)

        with self._lock:
            tail = self._get_tail()
            prev_hash = tail[1].record_hash if tail else None
            next_sequence = tail[1].sequence + 1 if tail else 0

            commit_record = LogRecord(
                sequence=next_sequence,
                repository_id=self.repository_id,
                kind=RecordKind.COMMIT,
                transaction_id=None,
                commit_id=new_id(),
                committed_transaction_id=transaction_id,
                committed_record_hashes=[r.record_hash for r in ordered],
                prev_record_hash=prev_hash,
            )
            commit_record.record_hash = compute_record_hash(
                commit_record.hashable_payload(), prev_hash
            )

            location = self.store.append(
                commit_record.model_dump_json().encode("utf-8")
            )
            self._tail_cache = (location, commit_record)

            self.latest_commit_location = location
            return AppendResult(record=commit_record, location=location)

    # -- verification (architecture section 2 / 10) -------------------------

    def verify_chain(
        self,
        *,
        start_location: Optional[Location] = None,
        end_location: Optional[Location] = None,
    ) -> VerificationResult:
        """
        Walk the hash chain, recomputing each record's hash and
        confirming both that it matches the stored hash and that its
        `prev_record_hash` matches the actual previous record's stored
        hash.

        By default this walks the *entire* log from genesis, which is
        the only way to be fully authoritative but can be expensive for
        a very large log. Pass `start_location`/`end_location` (e.g.
        from an external index that already trusts everything up to
        some checkpoint) to verify a bounded range instead -- note that
        in that case the first record's `prev_record_hash` is only
        checked for well-formedness, not against a record outside the
        range.
        """
        expected_prev_hash: Optional[str] = None
        checked = 0
        prev_sequence: Optional[int] = None
        bounded_start = start_location is not None

        for location, payload in self.store.iter_frames(start_location):
            record = self._decode(payload)

            recomputed = compute_record_hash(
                record.hashable_payload(), record.prev_record_hash
            )
            if recomputed != record.record_hash:
                return VerificationResult(
                    ok=False,
                    checked_count=checked,
                    first_broken_sequence=record.sequence,
                    reason=(
                        f"Stored hash for record {record.record_id} "
                        f"(sequence {record.sequence}) does not match "
                        "recomputed hash."
                    ),
                )

            if checked == 0:
                if not bounded_start and record.sequence != 0:
                    return VerificationResult(
                        ok=False,
                        checked_count=checked,
                        first_broken_sequence=record.sequence,
                        reason="Full chain verification did not start at sequence 0.",
                    )
            else:
                if record.prev_record_hash != expected_prev_hash:
                    return VerificationResult(
                        ok=False,
                        checked_count=checked,
                        first_broken_sequence=record.sequence,
                        reason=(
                            f"Record {record.record_id} (sequence "
                            f"{record.sequence}) prev_record_hash does not "
                            "match the previous record's hash."
                        ),
                    )
                if record.sequence != prev_sequence + 1:
                    return VerificationResult(
                        ok=False,
                        checked_count=checked,
                        first_broken_sequence=record.sequence,
                        reason=(
                            f"Non-contiguous sequence at record "
                            f"{record.record_id}: expected {prev_sequence + 1}, "
                            f"got {record.sequence}."
                        ),
                    )

            expected_prev_hash = record.record_hash
            prev_sequence = record.sequence
            checked += 1

            if end_location is not None and location == end_location:
                break

        return VerificationResult(ok=True, checked_count=checked)

    def verify_or_raise(self, **kwargs) -> None:
        """Same as `verify_chain`, but raises `ChainIntegrityError` on failure."""
        result = self.verify_chain(**kwargs)
        if not result.ok:
            raise ChainIntegrityError(result.reason or "Chain verification failed.")

    # -- recovery model (architecture section 11, log-side steps) -----------

    def recover(self, *, verify_full: bool = False) -> RecoveryState:
        """
        Recover the log after a restart or crash:

          1. Repair the tail: drop any partially-written trailing frame
             left by a crash mid-append (this scans the whole file --
             see `LogFileStore.truncate_trailing_corruption` -- so it's
             meant to run once at startup, not on every append).
          2. Re-establish the in-memory tail pointer (sequence,
             record_hash) so appending can resume correctly.
          3. Optionally verify the full hash chain (`verify_full=True`);
             this is the authoritative but most expensive check, so it
             is opt-in for very large logs. When `False`, only the
             physical tail repair happens -- callers with an external
             index can instead verify just the range since their last
             trusted checkpoint via `verify_chain(start_location=...)`.

        Note that identifying "the latest committed boundary" (section
        11, step 3) means finding the most recent COMMIT record, which
        this log can do via a full or partial scan (`iter_records`),
        but efficiently answering "what's currently uncommitted" for a
        huge log is exactly the job of the external SQL-based index.
        """
        truncation = self.store.truncate_trailing_corruption()
        self._tail_cache = None  # force a fresh read after any truncation

        tail = self._get_tail()
        tail_location = tail[0] if tail else None
        tail_sequence = tail[1].sequence if tail else None
        tail_record_hash = tail[1].record_hash if tail else None

        verification = self.verify_chain() if verify_full else None

        return RecoveryState(
            truncation=truncation,
            tail_location=tail_location,
            tail_sequence=tail_sequence,
            tail_record_hash=tail_record_hash,
            verification=verification,
        )

    # -- reading / iteration (primary hook for an external SQL index) -------

    def iter_records(
        self, start_location: Optional[Location] = None
    ) -> Iterator[tuple[Location, LogRecord]]:
        """
        Sequentially yield every `(Location, LogRecord)` in the log, in
        order, optionally resuming from `start_location` (inclusive).

        This is the intended integration point for building or
        incrementally refreshing an external SQL-based index: the index
        owner stores the `Location` (byte offset) of the last record it
        processed and passes it back in as `start_location` next time.
        """
        for location, payload in self.store.iter_frames(start_location):
            yield location, self._decode(payload)

    def read_at(self, location: Location) -> LogRecord:
        """Read a single record directly by its physical `Location` (byte offset)."""
        return self._decode(self.store.read_at(location))

    def get_tail(self) -> Optional[tuple[Location, LogRecord]]:
        """The most recently appended record and its Location, or None if empty."""
        return self._get_tail()

    def get_files_from_latest_commit(self) -> set[str]:
        """Get all files from latest_commit_location to tail (inclusive)."""
        files_list = set()
        for location, record in self.iter_records(self.latest_commit_location or 0):
            if record.kind == RecordKind.DATA and record.file_path:
                files_list.add(record.file_path)
        return files_list
    
    def __len__(self) -> int:
        tail = self._get_tail()
        return (tail[1].sequence + 1) if tail else 0

