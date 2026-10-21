# File-add performance

Measured on 2026-10-21 in conda environment `cdsi`, Python 3.13.3,
macOS 14.8.9 on arm64. Baseline: commit
`fa19fe7cb2022b9e5cb2ea1161400c72a79a3c20`.

## Results

Each workload uses 5,000 ten-byte files distributed across 50 directories.
Times are medians of five runs with separate temporary repositories per run.

| Workload | Before | After | Speedup | Time reduction |
| --- | ---: | ---: | ---: | ---: |
| Fresh directory add | 440.9 ms | 141.4 ms | 3.12× | 67.9% |
| Overlapping directory/file arguments | 891.2 ms | 143.5 ms | 6.21× | 83.9% |
| Re-add an already staged directory | 465.8 ms | 97.5 ms | 4.78× | 79.1% |
| Fifty calls adding 100 files each (total) | 1,188.6 ms | 904.0 ms | 1.31× | 23.9% |

The fresh and overlapping directory workloads stage 5,051 unique paths,
including directories. The overlapping workload supplies the parent directory,
one child directory, one file, and the parent directory again. The re-add
workload stages the directory before starting the timer. The incremental
workload supplies files explicitly and stages 5,000 paths.

Re-adding already staged paths now writes zero log bytes; the baseline appended
2,929,066 bytes. Overlapping inputs now append 2,927,792 bytes instead of
5,916,108 bytes. Fresh and incremental adds write the same number of log bytes
as before. The benchmark checks the final staged paths and actions outside the
timed region. [Raw timing and log-size samples](add-benchmark-results.json)
are retained for review.

## Changes

- Resolve the repository root once per add, and reuse parent realpaths during
  that command. Calculate each staged relative path once.
- Validate inputs before starting directory traversal.
- Skip repeated directory walks and duplicate paths within a command.
- Replay pending staging once when needed, skipping entries whose pending
  action is already `add`. Preserve `delete` to `add` transitions and re-adds
  after removal from staging.
- Print the added count by default. `dsi-vcs add --verbose <path>` retains the
  full staged listing.

Durable log writes still use the existing batched append with one `fsync`.
The measurements compare the old default with the new default and include the
combined effect of these changes. They do not isolate individual optimizations.

## Method and limits

Only `cmd_add` calls are timed, using `time.perf_counter`. File creation,
repository initialization, initial staging for the re-add workload, validation,
and cleanup are excluded. Log writes and their actual `fsync` calls are
included. Output goes to `/dev/null`: Python formatting and printing are timed,
but terminal rendering is excluded. Results are local filesystem measurements,
not predictions for network filesystems or CLI process startup.

Adding does not read file contents; these workloads measure path count and
staging-log costs. Many small calls still replay an increasing pending log to
identify duplicates. A persistent staging index would be a separate change if
that workload needs further improvement.

From the repository root, reproduce the comparison with:

```sh
git show fa19fe7cb2022b9e5cb2ea1161400c72a79a3c20:dsi/utils/version_control/dsi_vcs.py > /tmp/dsi_vcs_add_baseline.py
conda run -n cdsi python -m dsi.utils.version_control.tests.benchmark_add --implementation /tmp/dsi_vcs_add_baseline.py
conda run -n cdsi python -m dsi.utils.version_control.tests.benchmark_add
```

## Validation

The staging and incremental commit suites pass all 31 tests, including new
coverage for duplicate inputs, no-op re-adds across repository instances,
delete/remove transitions, early rejection of invalid directories, symlink
boundaries, and verbose CLI dispatch.

The existing `test_vcs.py` suite passes two tests and fails three tests on both
the original and changed implementations. The failures concern restoring a
directory's setgid bit and removing untracked files during restore/switch;
they are independent of the add changes.
