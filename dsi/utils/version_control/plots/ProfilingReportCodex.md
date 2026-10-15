
# dsi-vcs full CLI profiling — 2026-10-15

Environment: conda `cdsi`, Python 3.13.3, macOS 14.8.9 arm64; checkout `9b422204c1f6f6433a33633fd48dfa63f44c96a9`. Source files were unchanged. See `environment.json` for versions.

Each operation runs a fresh Python process through `python -m dsi.utils.version_control.cli`, using the same parser and main function as the installed `dsi-vcs` console script. Initial commits stage all files. Every later commit changes one byte in the first file; `one` scenarios stage that file and `all` scenarios stage the entire directory. Unprofiled and cProfile runs use separate repositories and deterministic identical starting data. All fixtures are flat directories to avoid the separately reproduced nested-directory correctness bug.

Workloads: `small_one` and `small_all`: 500 × 1 KiB files; `npy_one`: 8 × 8 MiB NumPy payloads plus headers; `binary_one`: 4 × 4 MiB random binary files; `history_all`: 50 × 1 KiB files. Five commits per workload except 25 for history.

## Measured findings

- The two 500-file workloads take 21–22% longer for subsequent commits locally. Staging only one file offers essentially no commit-time savings compared with staging all 500.
- In the second `small_one` profile, 500 metadata collections take 6.907 s and 500 parent permission checks take 1.072 s. There are 1,000 ACL/security subprocesses per commit. Group-name lookups increase from 500 on the first commit to 8,000 on subsequent commits (7,500 additional lookups inside authorization). Resolve the caller identity/groups once rather than per path.
- Binary chunking dominates the binary workload: the final profiled commit spends 3.438 of its 3.525 s inside `cmd_commit` in chunk storage (97.5%). Even after a one-byte edit in one file, all four files are chunked and `_gear_update` runs 12,582,912 times, exactly the same count as the initial commit. Absolute profiled binary timings are inflated by profiler overhead; use the unprofiled timing table for runtime comparisons.
- NumPy uses its specialized chunker: all eight arrays are processed on every commit, but there is no measurable consecutive-commit slowdown in this modest 64 MiB local workload. Initial and later median add+commit times are both about 0.50 s. A dramatic slowdown on larger or remote datasets was not reproduced here.
- At history commit 25, `add`, `commit`, and `status` each decode all 1,275 historical records. Replay costs about 11 ms per command here, confirming history-dependent work but not a dominant cost at this history size. Metadata and authorization dominate the 50-file history workload too.
- Each 500-file commit adds another 500 chunk-reference rows and 501 Merkle-node rows, despite only one byte changing. Chunk-content deduplication does not make processing or metadata storage incremental. The SQL finalization scaling issue remains valid, but SQLite and fsync are secondary to metadata subprocesses at this tested file count.

## Unprofiled end-to-end timings

Wall times include interpreter startup, imports, filesystem work, captured output, and process exit. Subsequent values are medians of commits 2–5, or 2–25 for history. These are single local trajectories, not cold-cache or cross-machine benchmarks.

| Workload    | Commits | Initial add (s) | Initial commit (s) | Later add (s) | Later commit (s) | Initial add+commit (s) | Later add+commit (s) | Commit ratio |
| ----------- | ------: | --------------: | -----------------: | ------------: | ---------------: | ---------------------: | -------------------: | -----------: |
| small_one   |       5 |           0.206 |              7.538 |         0.160 |            9.191 |                  7.744 |                9.351 |       1.22× |
| small_all   |       5 |           0.217 |              7.616 |         0.216 |            9.219 |                  7.833 |                9.436 |       1.21× |
| npy_one     |       5 |           0.151 |              0.354 |         0.159 |            0.346 |                  0.505 |                0.504 |       0.98× |
| binary_one  |       5 |           0.157 |              2.150 |         0.156 |            2.215 |                  2.307 |                2.370 |       1.03× |
| history_all |      25 |           0.169 |              0.911 |         0.164 |            1.140 |                  1.080 |                1.304 |       1.25× |

## Profile attribution

Cumulative times overlap (for example ACL work is included in metadata collection). Profile times are not used in the timing table above. The following selects the first and last commit of each profiled workload. Full statistics are in the corresponding `.pstats` and `.txt` files.

| Workload / commit | Function                                               |    Calls | Cumulative seconds |
| ----------------- | ------------------------------------------------------ | -------: | -----------------: |
| small_one / 1     | `cmd_commit`                                         |        1 |           7.633087 |
| small_one / 1     | `collect_metadata`                                   |      500 |           7.401453 |
| small_one / 1     | `get_acl`                                            |      500 |           3.850400 |
| small_one / 1     | `get_security_context`                               |      500 |           3.324171 |
| small_one / 1     | `store_chunks_for_snapshot`                          |        1 |           0.137375 |
| small_one / 1     | `<built-in method grp.getgrgid>`                     |      500 |           0.063439 |
| small_one / 1     | `chunk_file`                                         |      500 |           0.022949 |
| small_one / 1     | `check_access_permission`                            |      500 |           0.019126 |
| small_one / 1     | `<method 'executemany' of 'sqlite3.Cursor' objects>` |        2 |           0.018606 |
| small_one / 1     | `<built-in method posix.fsync>`                      |      501 |           0.011760 |
| small_one / 1     | `build_merkle_tree`                                  |        1 |           0.010642 |
| small_one / 1     | `_load_pending_stage_entries`                        |        1 |           0.004651 |
| small_one / 1     | `_decode`                                            |      501 |           0.001785 |
| small_one / 1     | `_find_fastcdc_cut`                                  |      500 |           0.000159 |
| small_one / 5     | `cmd_commit`                                         |        1 |           9.730482 |
| small_one / 5     | `collect_metadata`                                   |      500 |           8.451556 |
| small_one / 5     | `get_acl`                                            |      500 |           4.621688 |
| small_one / 5     | `get_security_context`                               |      500 |           3.679842 |
| small_one / 5     | `check_access_permission`                            |      500 |           1.166511 |
| small_one / 5     | `<built-in method grp.getgrgid>`                     |     8000 |           0.818147 |
| small_one / 5     | `store_chunks_for_snapshot`                          |        1 |           0.036705 |
| small_one / 5     | `chunk_file`                                         |      500 |           0.023867 |
| small_one / 5     | `<method 'executemany' of 'sqlite3.Cursor' objects>` |        2 |           0.021378 |
| small_one / 5     | `build_merkle_tree`                                  |        1 |           0.011087 |
| small_one / 5     | `_load_pending_stage_entries`                        |        1 |           0.004752 |
| small_one / 5     | `_decode`                                            |      509 |           0.001816 |
| small_one / 5     | `<built-in method posix.fsync>`                      |        2 |           0.000933 |
| small_one / 5     | `_find_fastcdc_cut`                                  |      500 |           0.000153 |
| small_all / 1     | `cmd_commit`                                         |        1 |           7.718039 |
| small_all / 1     | `collect_metadata`                                   |      500 |           7.482303 |
| small_all / 1     | `get_acl`                                            |      500 |           3.938050 |
| small_all / 1     | `get_security_context`                               |      500 |           3.300136 |
| small_all / 1     | `store_chunks_for_snapshot`                          |        1 |           0.140094 |
| small_all / 1     | `<built-in method grp.getgrgid>`                     |      500 |           0.063188 |
| small_all / 1     | `chunk_file`                                         |      500 |           0.023621 |
| small_all / 1     | `check_access_permission`                            |      500 |           0.019800 |
| small_all / 1     | `<method 'executemany' of 'sqlite3.Cursor' objects>` |        2 |           0.019168 |
| small_all / 1     | `<built-in method posix.fsync>`                      |      501 |           0.012219 |
| small_all / 1     | `build_merkle_tree`                                  |        1 |           0.010948 |
| small_all / 1     | `_load_pending_stage_entries`                        |        1 |           0.004568 |
| small_all / 1     | `_decode`                                            |      501 |           0.001765 |
| small_all / 1     | `_find_fastcdc_cut`                                  |      500 |           0.000161 |
| small_all / 5     | `cmd_commit`                                         |        1 |          10.066245 |
| small_all / 5     | `collect_metadata`                                   |      500 |           8.696474 |
| small_all / 5     | `get_acl`                                            |      500 |           4.730687 |
| small_all / 5     | `get_security_context`                               |      500 |           3.792978 |
| small_all / 5     | `check_access_permission`                            |      500 |           1.231363 |
| small_all / 5     | `<built-in method grp.getgrgid>`                     |     8000 |           0.862483 |
| small_all / 5     | `store_chunks_for_snapshot`                          |        1 |           0.035866 |
| small_all / 5     | `chunk_file`                                         |      500 |           0.023911 |
| small_all / 5     | `_load_pending_stage_entries`                        |        1 |           0.021830 |
| small_all / 5     | `<method 'executemany' of 'sqlite3.Cursor' objects>` |        2 |           0.020519 |
| small_all / 5     | `build_merkle_tree`                                  |        1 |           0.011405 |
| small_all / 5     | `_decode`                                            |     2505 |           0.007709 |
| small_all / 5     | `<built-in method posix.fsync>`                      |        2 |           0.000234 |
| small_all / 5     | `_find_fastcdc_cut`                                  |      500 |           0.000164 |
| npy_one / 1       | `cmd_commit`                                         |        1 |           0.222495 |
| npy_one / 1       | `collect_metadata`                                   |        8 |           0.124021 |
| npy_one / 1       | `store_chunks_for_snapshot`                          |        1 |           0.094174 |
| npy_one / 1       | `chunk_file`                                         |        8 |           0.074415 |
| npy_one / 1       | `get_acl`                                            |        8 |           0.063955 |
| npy_one / 1       | `get_security_context`                               |        8 |           0.057069 |
| npy_one / 1       | `<built-in method posix.fsync>`                      |       10 |           0.010322 |
| npy_one / 1       | `<built-in method grp.getgrgid>`                     |        8 |           0.000883 |
| npy_one / 1       | `build_merkle_tree`                                  |        1 |           0.000354 |
| npy_one / 1       | `check_access_permission`                            |        8 |           0.000209 |
| npy_one / 1       | `_load_pending_stage_entries`                        |        1 |           0.000201 |
| npy_one / 1       | `<method 'executemany' of 'sqlite3.Cursor' objects>` |        2 |           0.000145 |
| npy_one / 1       | `_decode`                                            |        9 |           0.000137 |
| npy_one / 5       | `cmd_commit`                                         |        1 |           0.219092 |
| npy_one / 5       | `collect_metadata`                                   |        8 |           0.143895 |
| npy_one / 5       | `get_acl`                                            |        8 |           0.080037 |
| npy_one / 5       | `get_security_context`                               |        8 |           0.061619 |
| npy_one / 5       | `store_chunks_for_snapshot`                          |        1 |           0.053604 |
| npy_one / 5       | `chunk_file`                                         |        8 |           0.050343 |
| npy_one / 5       | `check_access_permission`                            |        8 |           0.016620 |
| npy_one / 5       | `<built-in method grp.getgrgid>`                     |      128 |           0.011895 |
| npy_one / 5       | `<built-in method posix.fsync>`                      |        2 |           0.001427 |
| npy_one / 5       | `build_merkle_tree`                                  |        1 |           0.000345 |
| npy_one / 5       | `_load_pending_stage_entries`                        |        1 |           0.000290 |
| npy_one / 5       | `<method 'executemany' of 'sqlite3.Cursor' objects>` |        2 |           0.000185 |
| npy_one / 5       | `_decode`                                            |       17 |           0.000173 |
| binary_one / 1    | `cmd_commit`                                         |        1 |           3.369031 |
| binary_one / 1    | `store_chunks_for_snapshot`                          |        1 |           3.299502 |
| binary_one / 1    | `chunk_file`                                         |        4 |           3.290525 |
| binary_one / 1    | `_find_fastcdc_cut`                                  |        4 |           3.275645 |
| binary_one / 1    | `_gear_update`                                       | 12582912 |           1.180725 |
| binary_one / 1    | `collect_metadata`                                   |        4 |           0.065632 |
| binary_one / 1    | `get_acl`                                            |        4 |           0.035333 |
| binary_one / 1    | `get_security_context`                               |        4 |           0.028767 |
| binary_one / 1    | `<built-in method posix.fsync>`                      |        5 |           0.005196 |
| binary_one / 1    | `<built-in method grp.getgrgid>`                     |        4 |           0.000456 |
| binary_one / 1    | `build_merkle_tree`                                  |        1 |           0.000238 |
| binary_one / 1    | `_load_pending_stage_entries`                        |        1 |           0.000213 |
| binary_one / 1    | `_decode`                                            |        5 |           0.000141 |
| binary_one / 1    | `check_access_permission`                            |        4 |           0.000140 |
| binary_one / 1    | `<method 'executemany' of 'sqlite3.Cursor' objects>` |        2 |           0.000105 |
| binary_one / 5    | `cmd_commit`                                         |        1 |           3.524943 |
| binary_one / 5    | `store_chunks_for_snapshot`                          |        1 |           3.437933 |
| binary_one / 5    | `chunk_file`                                         |        4 |           3.435415 |
| binary_one / 5    | `_find_fastcdc_cut`                                  |        4 |           3.421293 |
| binary_one / 5    | `_gear_update`                                       | 12582912 |           1.237159 |
| binary_one / 5    | `collect_metadata`                                   |        4 |           0.074272 |
| binary_one / 5    | `get_acl`                                            |        4 |           0.038541 |
| binary_one / 5    | `get_security_context`                               |        4 |           0.034681 |
| binary_one / 5    | `check_access_permission`                            |        4 |           0.008366 |
| binary_one / 5    | `<built-in method grp.getgrgid>`                     |       64 |           0.005907 |
| binary_one / 5    | `<built-in method posix.fsync>`                      |        2 |           0.001296 |
| binary_one / 5    | `build_merkle_tree`                                  |        1 |           0.000248 |
| binary_one / 5    | `_load_pending_stage_entries`                        |        1 |           0.000246 |
| binary_one / 5    | `_decode`                                            |       13 |           0.000170 |
| binary_one / 5    | `<method 'executemany' of 'sqlite3.Cursor' objects>` |        2 |           0.000128 |
| history_all / 1   | `cmd_commit`                                         |        1 |           0.760023 |
| history_all / 1   | `collect_metadata`                                   |       50 |           0.729732 |
| history_all / 1   | `get_acl`                                            |       50 |           0.387629 |
| history_all / 1   | `get_security_context`                               |       50 |           0.320610 |
| history_all / 1   | `store_chunks_for_snapshot`                          |        1 |           0.020186 |
| history_all / 1   | `<built-in method grp.getgrgid>`                     |       50 |           0.006068 |
| history_all / 1   | `chunk_file`                                         |       50 |           0.003905 |
| history_all / 1   | `<built-in method posix.fsync>`                      |       51 |           0.002223 |
| history_all / 1   | `check_access_permission`                            |       50 |           0.001717 |
| history_all / 1   | `build_merkle_tree`                                  |        1 |           0.001340 |
| history_all / 1   | `_load_pending_stage_entries`                        |        1 |           0.000586 |
| history_all / 1   | `<method 'executemany' of 'sqlite3.Cursor' objects>` |        2 |           0.000454 |
| history_all / 1   | `_decode`                                            |       51 |           0.000283 |
| history_all / 1   | `_find_fastcdc_cut`                                  |       50 |           0.000028 |
| history_all / 25  | `cmd_commit`                                         |        1 |           1.045779 |
| history_all / 25  | `collect_metadata`                                   |       50 |           0.898339 |
| history_all / 25  | `get_acl`                                            |       50 |           0.484573 |
| history_all / 25  | `get_security_context`                               |       50 |           0.398312 |
| history_all / 25  | `check_access_permission`                            |       50 |           0.121258 |
| history_all / 25  | `<built-in method grp.getgrgid>`                     |      800 |           0.084769 |
| history_all / 25  | `_load_pending_stage_entries`                        |        1 |           0.011365 |
| history_all / 25  | `store_chunks_for_snapshot`                          |        1 |           0.005294 |
| history_all / 25  | `_decode`                                            |     1275 |           0.004120 |
| history_all / 25  | `chunk_file`                                         |       50 |           0.002962 |
| history_all / 25  | `build_merkle_tree`                                  |        1 |           0.001543 |
| history_all / 25  | `<method 'executemany' of 'sqlite3.Cursor' objects>` |        2 |           0.000829 |
| history_all / 25  | `<built-in method posix.fsync>`                      |        2 |           0.000208 |
| history_all / 25  | `_find_fastcdc_cut`                                  |       50 |           0.000019 |

## Database growth

| Workload    | Initial → final chunk references | Initial → final Merkle nodes | Initial → final database bytes | Initial → final log bytes |
| ----------- | --------------------------------: | ----------------------------: | ------------------------------: | -------------------------: |
| small_one   |                       500 → 2500 |                   501 → 2505 |               581632 → 2732032 |           317864 → 322548 |
| small_all   |                       500 → 2500 |                   501 → 2505 |               589824 → 2719744 |          317864 → 1591513 |
| npy_one     |                          16 → 80 |                       9 → 45 |                 57344 → 110592 |              5536 → 10211 |
| binary_one  |                           4 → 20 |                       5 → 25 |                  57344 → 69632 |               3004 → 7675 |
| history_all |                        50 → 1250 |                    51 → 1275 |               110592 → 1400832 |            32163 → 807253 |

## History replay

A new CLI process does not restore `RepositoryLog.latest_commit_location`, so staging replay starts at offset zero. The `_decode` call counts below include the replay plus one tail decode for append.

| Workload    | Invocation | Commit number | Decoded records | Replay cumulative seconds |
| ----------- | ---------- | ------------: | --------------: | ------------------------: |
| history_all | add        |             1 |              50 |                  0.000587 |
| history_all | commit     |             1 |              51 |                  0.000586 |
| history_all | add        |             2 |             102 |                  0.000927 |
| history_all | commit     |             2 |             102 |                  0.001003 |
| history_all | add        |            25 |            1275 |                  0.011183 |
| history_all | commit     |            25 |            1275 |                  0.011365 |
| history_all | status     |      after 25 |            1275 |                  0.010937 |

## Fix priorities

1. Persist unchanged-file state and reuse parent manifests/content hashes, processing staged changes and updating ancestor Merkle nodes. If `add .` remains common, persist a filesystem fingerprint (including nanosecond timestamps and identity) to avoid rehashing unchanged files; define validation and metadata invalidation policies explicitly.
2. Resolve current identity/groups once per command, bulk-fetch parent metadata, and cache UID/GID name lookups. Keep authorization behavior intact.
3. Replace per-path ACL/security subprocesses with native or batched access where supported. Avoid querying SELinux context on macOS, where it is unavailable.
4. Persist and restore the last successfully committed log offset, with interrupted-commit recovery, to bound replay to pending records.
5. Finalize all chunk references with one transaction-scoped bulk UPDATE rather than one UPDATE per distinct hash; alternatively add an index on (commit_hash, chunk_hash). The isolated SQLite experiment in the preceding analysis established this scaling issue; full CLI profiles show whether it dominates each tested workload.
6. Stream chunk output; consider native FastCDC for fallback binary chunking. Version chunking behavior if replacing the implementation changes chunk boundaries.
7. Batch log writes and fsync at durable transaction boundaries. Preserve partial-write recovery.

## Reproduction and artifacts

Run `conda run --no-capture-output -n cdsi python profile_cli.py` from a new output directory containing the script (fixture names must not already exist). `results.json` contains measured function counts/times and database snapshots; `timings.csv` contains every CLI wall time. Every invocation has a `.log`; profiled invocations also have `.pstats` and cumulative-time `.txt` reports.

Limitations: synthetic local datasets, fixed workload order, warm filesystem caches after initial reads, one trajectory per mode, no network/parallel-filesystem measurements, and no extrapolated end-to-end speedup claims. Small NumPy and binary file counts do not stress the quadratic finalization query.

## Nested-directory correctness reproduction

A separate five-invocation reproduction used the installed `cdsi/bin/dsi-vcs` console script with `top.bin` and `data/nested.bin`. The initial commit reports 2 files / 8 bytes and stores chunk references for both, but stores Merkle nodes only for `.` and `top.bin`. Staging and committing only a change to `top.bin` produces a second version with 1 file / 4 bytes: the nested file loses its commit membership without a staged deletion. This confirms the earlier isolated Merkle finding through the full CLI. Full commands, outputs, and database rows are in `nested_check.json`; the reproducer is `check_nested.py`. Synthesize required parent directory nodes before using nested data for performance comparisons.

Validation: the primary benchmark completed 200 successful CLI invocations across 90 commits. Expected version counts and Merkle-node row growth were checked for every commit. The additional nested-directory reproduction completed five successful console-script invocations across two commits. No source changes or existing repository data were modified.
