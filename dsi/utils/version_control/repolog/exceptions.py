"""Exception types for the repolog package."""


class RepoLogError(Exception):
    """Base class for all repolog errors."""


class ChainIntegrityError(RepoLogError):
    """Raised when the hash-linked chain fails verification.

    Indicates a record's stored hash does not match a freshly
    recomputed hash, or that a record's `prev_record_hash` does not
    match the actual preceding record's hash -- i.e. tampering,
    corruption, or an out-of-order write.
    """


class RecordNotFoundError(RepoLogError):
    """Raised when a referenced log record or on-disk offset does not exist."""


class InvalidCommitError(RepoLogError):
    """Raised when a commit operation is requested on invalid input.

    Examples: committing an empty set of records, referencing records
    that belong to a different repository, or referencing records that
    don't actually appear (in order) in this log.
    """


class CorruptFrameError(RepoLogError):
    """Raised when a physical on-disk frame is malformed or truncated.

    This is a lower-level, storage-format error (bad length/CRC/footer)
    as opposed to `ChainIntegrityError`, which is a logical hash-chain
    mismatch between otherwise well-formed records. A `CorruptFrameError`
    at the very tail of the log is expected after a crash mid-write and
    is handled by `RepositoryLog.recover()`; one earlier in the file
    indicates real corruption.
    """
