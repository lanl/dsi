# C++ file-version benchmark

The benchmark uses only `manifest_benchmark.cpp`, `chunking.cpp`, `manifest.cpp`,
and `sha256.cpp`. It does not invoke Python or change the production algorithms.

A separate fresh [deduplication benchmark](DEDUPLICATION.md) measures payload
reuse and cumulative storage ratios, including unchanged/revert/rewrite controls.

The recorded run completed 95 snapshot/diff pairs through 4 GB, with successful
initial and final version restoration at every size. Initial 4 GB storage took
5,291.54 ms; median edited snapshots took 4,160–4,367 ms. See the
[result tables](#results) and throughput definitions below.

## Reproduce

From `dsi/utils/version_control/repolog/chunking_tests`:

```sh
make -j4 benchmark
./manifest_benchmark \
  --output benchmark_results/results.tsv \
  --summary benchmark_results/summary.tsv
```

Output files must not already exist. The default matrix uses one synthesized
file at a time, at 256,000,000; 512,000,000; 1,000,000,000; 2,000,000,000; and
4,000,000,000 bytes. MB and GB are decimal; the largest file is 4 GB, not 4 GiB.
The deterministic generator emits a little-endian SplitMix64 stream with seed
`0x20261105D51` (decimal 2,209,241,652,561). All sizes start with the same stream
prefix. Source generation is outside the measurements.

The harness creates its own temporary directory per size and removes only its
generated data when that size completes. It retains results, not the multi-GB
datasets. Ensure sufficient temporary disk space for the working file, retained
version objects, an edit scratch file, and restoration output.

For a smaller smoke test that still exercises edits larger than a chunk:

```sh
./manifest_benchmark --sizes 160000000 --positions middle \
  --output /tmp/dsi-smoke-results.tsv --summary /tmp/dsi-smoke-summary.tsv
```

`--sizes` accepts comma-separated byte counts greater than 128 MiB plus 128 KiB
and no greater than 4 GB. `--positions` accepts a comma-separated subset of
`begin,middle,end`.

## Workload

Each size starts with a fresh payload store and one initial snapshot. It then
performs this consecutive sequence near the beginning, middle, and end:

1. Remove 64 KiB, insert 64 KiB of new bytes, update 64 KiB in place.
2. Remove 128 MiB, insert 128 MiB of new bytes, update 128 MiB in place.

64 KiB is below the configured 1 MiB minimum chunk size. 128 MiB exceeds the
configured 64 MiB maximum chunk size. The target positions are within roughly
64 KiB of the beginning/end, or centered in the available edit range. Insertions
and deletions use the same position within each pair. Deletion precedes insertion
so the working file never exceeds its starting size. Insertions and updates use
new deterministic data; they do not just undo an edit with the original bytes.
The repository and previous manifest roots are reused throughout all 18 edits.

Every initial snapshot and every edited snapshot is followed by a manifest diff:
95 snapshot/diff pairs across five sizes. Initial add has one measurement per
size. Each edit operation has three observations, one at each position; reported
medians summarize positions, not repeated independent trials.

Storage calls `storeChunksForSnapshot(..., threads=1, previousRoots)`. Timings
include file reading, FastCDC, SHA-256, reuse detection, writing new payload
objects, manifest editing/publication, and the existing HLL persistence path.
They exclude source generation, source modification, diff, and verification.
The existing store still re-reads/re-chunks the entire file after every edit.

## Diff definition and throughput

There is no native C++ diff API in this folder. The benchmark's `diff` opens the
two immutable manifests, verifies and reads their ordered references, trims an
unchanged prefix/suffix, and computes exact LCS on the remaining reference
windows. It reports added, removed, and common references. This is chunk-level
version comparison, not a byte-level or textual diff. It reads no payload bytes.
The LCS stage costs `O(n*m)` time and `O(m)` memory for changed-window lengths
`n` and `m`; reference loading is linear in the manifests' chunk counts.

| Metric | Definition |
| --- | --- |
| `store_ms` | Wall time of the C++ snapshot/store call |
| `store_MB_s` | Resulting file bytes / store seconds / 1,000,000 |
| `edit_apply_ms` | Time to modify and fsync the test file, separately measured |
| `edit_plus_store_ms` | Fixture modification plus snapshot time; excludes diff |
| `diff_ms` | Opening, checking, reading, and comparing both manifests |
| `diff_metadata_MB_s` | Exact bytes in both manifest objects / diff seconds / 1,000,000 |
| `diff_logical_MB_s` | Sum of both logical file sizes / diff seconds / 1,000,000 |
| `new_payload_bytes` | Growth in actual content-addressed payload storage |

`diff_logical_MB_s` is only a logical comparison rate, not physical disk or
payload bandwidth. Use `diff_metadata_MB_s` for the measured metadata workload.

Default pages split at 8,192 references. With minimum 1 MiB chunks, a file no
larger than 4 GB cannot reach that threshold; every measured manifest is one
leaf (`manifest_depth=0`). Consequently metadata-byte accounting is exactly the
sum of the two root-object sizes. This benchmark does not measure multi-level
manifest splitting/merging or billion-reference scalability.

## Verification and interpretation

After each snapshot, the harness validates manifest structure, byte/chunk
totals, and ordered-reference digest, and checks that diff detects the edit.
After all edits at each size, it reconstructs both the latest version and the
initial historical version and checks complete byte-level SHA-256 equality.
Verification is outside the reported operation times.

The source is freshly generated or rewritten immediately before snapshots, so
these are warm-cache measurements with normal OS caching. No cache purge,
direct I/O, CPU isolation, or hardware power-loss durability experiment is
performed. File and manifest publication use the existing fsync paths. Run on
an otherwise idle machine for more controlled comparisons.

## Recorded run: 2026-11-05

Environment: Apple M2 Max, 12 CPU cores, 64 GiB RAM, macOS 14.8.9 / Darwin 23.6.0,
Apple clang 16.0.0, `-std=c++17 -O2 -Wall -Wextra`, CommonCrypto SHA-256,
single-threaded storage, default chunk sizes and default 4,096-entry manifest
parameter. The temporary volume had approximately 352 GiB available before the
run.

Raw observations are in [2026-11-05-results.tsv](2026-11-05-results.tsv). The
position medians and min/max store times are in
[2026-11-05-summary.tsv](2026-11-05-summary.tsv).

## Results

All 95 snapshot/diff pairs completed successfully. Both the final edited file and
the initial historical version were restored and verified at all five sizes.

### Initial file storage

One measurement per file size; diff compares the new version with an empty file.

| File size (MB) | Store (ms) | File MB/s | Chunks | Diff (ms) | Diff metadata MB/s |
| --- | ---: | ---: | ---: | ---: | ---: |
| 256 | 339.62 | 753.78 | 27 | 0.078 | 14.99 |
| 512 | 678.23 | 754.91 | 52 | 0.076 | 28.42 |
| 1000 | 1310.28 | 763.20 | 101 | 0.096 | 42.79 |
| 2000 | 2629.46 | 760.61 | 210 | 0.109 | 77.91 |
| 4000 | 5291.54 | 755.92 | 412 | 0.150 | 110.26 |

### Consecutive edits

Each row is the median of the beginning, middle, and end observations. These
are three different edit positions in the same consecutive history, not three
independent repeated trials. File throughput uses the resulting file size;
large removals leave a smaller file to scan. Fixture modification is excluded
from store and diff timings and shown separately.

| File size (MB) | Edit | Store (ms) | File MB/s | Fixture edit (ms) | Diff (ms) | Diff metadata MB/s |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 256 | Remove 64 KiB | 269.07 | 951.19 | 49.72 | 0.078 | 27.66 |
| 256 | Add 64 KiB | 273.88 | 934.72 | 43.39 | 0.082 | 24.80 |
| 256 | Update 64 KiB | 270.23 | 947.34 | 0.16 | 0.084 | 23.79 |
| 256 | Remove 128 MiB | 130.70 | 931.77 | 23.13 | 0.070 | 22.38 |
| 256 | Add 128 MiB | 310.29 | 825.03 | 77.19 | 0.072 | 21.27 |
| 256 | Update 128 MiB | 302.97 | 844.98 | 51.99 | 0.080 | 26.01 |
| 512 | Remove 64 KiB | 540.39 | 947.33 | 85.69 | 0.098 | 42.38 |
| 512 | Add 64 KiB | 559.60 | 914.93 | 218.34 | 0.090 | 46.03 |
| 512 | Update 64 KiB | 546.80 | 936.35 | 0.25 | 0.099 | 42.97 |
| 512 | Remove 128 MiB | 407.87 | 926.23 | 70.40 | 0.084 | 43.68 |
| 512 | Add 128 MiB | 585.14 | 875.00 | 115.15 | 0.088 | 40.06 |
| 512 | Update 128 MiB | 581.34 | 880.72 | 53.27 | 0.093 | 45.65 |
| 1000 | Remove 64 KiB | 1058.84 | 944.37 | 172.97 | 0.106 | 75.06 |
| 1000 | Add 64 KiB | 1055.90 | 947.06 | 233.36 | 0.115 | 70.00 |
| 1000 | Update 64 KiB | 1045.26 | 956.70 | 0.15 | 0.103 | 78.53 |
| 1000 | Remove 128 MiB | 900.12 | 961.85 | 147.57 | 0.102 | 73.96 |
| 1000 | Add 128 MiB | 1083.41 | 923.01 | 268.60 | 0.111 | 67.15 |
| 1000 | Update 128 MiB | 1083.99 | 922.52 | 50.89 | 0.113 | 70.88 |
| 2000 | Remove 64 KiB | 2126.49 | 940.49 | 392.02 | 0.165 | 102.03 |
| 2000 | Add 64 KiB | 2129.62 | 939.13 | 388.59 | 0.173 | 96.80 |
| 2000 | Update 64 KiB | 2129.21 | 939.32 | 0.13 | 0.159 | 105.31 |
| 2000 | Remove 128 MiB | 1979.32 | 942.64 | 369.87 | 0.152 | 104.96 |
| 2000 | Add 128 MiB | 2174.47 | 919.76 | 390.05 | 0.162 | 101.07 |
| 2000 | Update 128 MiB | 2162.87 | 924.70 | 51.46 | 0.162 | 103.58 |
| 4000 | Remove 64 KiB | 4294.99 | 931.30 | 723.62 | 0.264 | 124.68 |
| 4000 | Add 64 KiB | 4307.98 | 928.51 | 712.14 | 0.245 | 134.70 |
| 4000 | Update 64 KiB | 4324.35 | 924.99 | 0.14 | 0.254 | 130.09 |
| 4000 | Remove 128 MiB | 4160.10 | 929.25 | 687.95 | 0.253 | 128.15 |
| 4000 | Add 128 MiB | 4366.50 | 916.06 | 714.94 | 0.252 | 128.25 |
| 4000 | Update 128 MiB | 4342.19 | 921.19 | 52.33 | 0.265 | 124.16 |

### Observations

- Initial storage scales approximately linearly: 339.62 ms for 256 MB and
  5,291.54 ms for 4 GB, at roughly 754–763 MB/s.
- A 64 KiB update to the 4 GB file takes 4324.35 ms despite
  changing only 65,536 bytes. The store still scans and hashes the full file.
  Its 924.99 MB/s is full-file processing throughput;
  effective changed-data throughput is only 0.01516 MB/s.
- That same update writes a median 8.699 MB of new payload,
  rather than another 4 GB. Chunk granularity causes a small byte edit to replace
  an entire chunk. The unchanged payload objects are reused.
- At 4 GB, edited-version diffs take 0.241–0.309 ms across individual observations.
  They compare about 33 KB of manifest metadata, not 8 GB of payload bytes. The
  corresponding metadata throughput measures the metadata workload, with reads
  served through the normal OS cache.
- Initial manifests contain 27, 52, 101, 210, and 412 references respectively;
  all fit one leaf. These measurements cannot establish performance of internal
  splits/merges or billion-chunk trees.
- Single measurements and three-position medians are descriptive. Warm caches,
  OS scheduling, filesystem state, and the sequential workload can affect times;
  the raw TSV preserves all observations and summary min/max values.
