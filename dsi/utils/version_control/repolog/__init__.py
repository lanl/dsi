"""
repolog
=======

A persistent, append-only, hash-linked repository log, a blockchain-like
write-ahead log stored as a sequence of immutable, length-framed records
in a single ever-growing file. This package implements the LOG ONLY
(architecture sections 1-2, plus the log's share of sections 4 and 11).
It intentionally does not implement any SQL-based indexing over the log;
that is expected to live in a separate component built on top of
`RepositoryLog.iter_records()`.

Public entry point: `RepositoryLog`.
"""

__all__ = [
    "AppendResult",
    "ChainIntegrityError",
    "CorruptFrameError",
    "InvalidCommitError",
    "Location",
    "LogRecord",
    "OperationType",
    "RecordKind",
    "RecordNotFoundError",
    "RecoveryState",
    "RepoLogError",
    "RepositoryLog",
    "VerificationResult",
]

__version__ = "0.0.1"
