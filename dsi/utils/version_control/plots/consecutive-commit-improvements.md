# Consecutive commit improvements

Measured on 2026-10-15 using conda `cdsi`, Python 3.13.3, macOS 14.8.9 arm64,
on the local filesystem. Baseline checkout:
`9b422204c1f6f6433a33633fd48dfa63f44c96a9`. After measurements use this change.

Each operation runs a fresh CLI process. Identical deterministic datasets are
used in separate baseline and modified repositories. The initial commit stages
all files; subsequent commits change one byte and stage the indicated paths.
Each implementation completes 200 CLI invocations across 90 commits, split
between unprofiled timing and cProfile runs. Times include interpreter startup
and imports. The table uses unprofiled wall times.

## Consecutive commit wall time

Values are medians of commits 2–5, or 2–25 for the history workload.

| Workload | Before | After | Speedup |
|---|---:|---:|---:|
| 500 × 1 KiB files; stage one | 9.191 s | 0.187 s | 49.0× |
| 500 × 1 KiB files; stage all | 9.219 s | 0.194 s | 47.5× |
| 8 × 8 MiB NumPy arrays; stage one | 0.346 s | 0.190 s | 1.8× |
| 4 × 4 MiB binary files; stage one | 2.215 s | 0.686 s | 3.2× |
| 50 × 1 KiB files; 25 commits; stage all | 1.140 s | 0.179 s | 6.4× |

## What the profiles confirm

- For the fifth 500-file stage-all commit, metadata collection drops from
  500 calls to 1; chunking drops from 500 calls to 1; group-name lookups drop
  from 8,000 to 15. Unchanged paths reuse their recorded parent state.
- For the final binary commit, Gear-hash calls drop from 12,582,912 to
  3,145,728: only the changed file is scanned. Streaming preserves FastCDC
  boundary selection. Specialized format chunkers retain their implementations.
- At history commit 25, decoded records drop from 1,275 to 52: the checkpoint
  marker, 50 newly staged paths, and a tail read. Pending staging replay is
  about 0.6 ms instead of 11 ms in the selected profiles.
- New chunk references are finalized with one UPDATE scoped to a unique
  transaction token. References for unchanged files are copied inside SQLite
  without reading their content or transferring each reference through Python.
- Staging batches use one log fsync, retaining individual offsets and the chain.
- Full Merkle-node and chunk-reference manifests remain stored per commit.
  This reduces processing time, but does not eliminate reference storage growth.
  Native FastCDC and an immutable-manifest storage redesign are not introduced.

## Correctness and compatibility

Nested files now have their required ancestor nodes; empty directories are
staged. Following commits retain these paths. This does not reconstruct metadata
omitted by historical commits; staging the current directory again captures its
current nested paths.

Unstaged edits and locally missing tracked files retain their committed parent
state. Changed or deleted staged paths require the parent's write authorization.
A denied change aborts the transaction and preserves staging.

Fingerprints include device, inode, mode, ownership, link count, size, and
nanosecond mtime/ctime, excluding atime. Source changes during metadata/content
collection abort the commit. `commit --rehash` bypasses fingerprint reuse for
staged paths when timestamps are unreliable or a refresh is desired.

Schema migration adds fingerprints and a SQL log checkpoint automatically.
Older staged paths are refreshed on first use. The checkpoint advances in the
snapshot transaction; a log marker whose SQL transaction failed does not clear
pending staging. Branch heads advance after committing, so switching back
selects the newly committed branch state.

## Validation

The 25 passing regression cases cover unchanged/staged reuse, one-file edits,
unstaged/missing files, restored mtimes, mode changes, nested/empty directories,
directory unstaging/deletion, permission denial, restart checkpoints, a failure
after appending a commit marker, legacy schema migration, identity lookup counts,
batched frame integrity, original FastCDC boundaries, source mutation during
chunking, restored nested content, branch parent reuse, rehash, and ACL/mode rules.

Three existing tests in `test_vcs.py` fail on both implementations when the
original checkout is run with a CLI-equivalent working directory:

- `test_add`: setgid-mode expectation on this macOS filesystem.
- `test_commit_only_includes_staged_files`: restore retains extra files.
- `test_switch_restores_branch_snapshot`: switch retains extra files.

The existing restore cleanup behavior is unchanged. Linux root/user/ACL
integration tests cannot execute on this macOS host.

## Reproduce

```bash
conda run --no-capture-output -n cdsi python \
  dsi/utils/version_control/benchmarks/profile_cli.py \
  --output /tmp/dsi-vcs-new-profile

conda run -n cdsi python -m pytest \
  dsi/utils/version_control/tests/test_incremental_commit.py -q
```

The output directory must be new. The profiler writes `timings.csv`,
`results.json`, command logs, text profiles, and `.pstats` files. Original raw
results are in `/private/tmp/dsi-vcs-cli-profile-20261015`; modified results are
in `/private/tmp/dsi-vcs-cli-profile-after-20261015`. These temporary artifacts
are local to this session.

Limitations: synthetic local datasets, warm caches after initial reads, fixed
workload order, and one trajectory per mode. This does not measure Linux identity
services, network/parallel filesystems, cold caches, or concurrent writers.
Profiling adds substantial overhead to Python FastCDC loops; reported speedups
use the separate unprofiled runs.
