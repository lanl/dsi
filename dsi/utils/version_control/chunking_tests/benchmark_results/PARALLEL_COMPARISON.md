# Parallel C++ benchmark: before and after

This compares the saved serial run with a fresh run of the user-modified
multiprocess implementation. The benchmark harness now accepts `--processes`;
the production chunker, manifest, and SHA-256 source files are preserved.

The fresh run passed all 110 cases. Across 95 main snapshots, storage is
**1.36× faster (26.35% less store time)**. For the 4 GB file,
the averaged stored-size fraction increases from 6.62% to 7.67%,
so the new layout uses more history storage. See [results](#results).

## Comparable dataset and workload

Both runs use one file at a time at 256 MB, 512 MB, 1 GB, 2 GB, and 4 GB
(decimal). The little-endian SplitMix64 stream uses seed `0x20261105D51`.
Positions, lengths, edit-generation seeds, and operation order are identical:
64 KiB remove/add/update, then 128 MiB remove/add/update, at the beginning,
middle, and end. Each file retains 19 main versions and has three additional
calibration snapshots: unchanged, revert to original, and entirely new random
content. Each run contains 110 snapshot/diff pairs, including 95 main pairs.

The baseline is [the saved serial raw data](2026-11-05-dedup-results.tsv), not
a rerun of the old source. Its [summary](2026-11-05-dedup-summary.tsv) contains
one initial measurement and three-position medians for each edit. The fresh
parallel run uses the same generator and fixture-edit functions; deterministic
input fields are checked against the baseline.

After measurements: [parallel raw data](2026-11-05-parallel-results.tsv) and
[parallel summary](2026-11-05-parallel-summary.tsv). Files from the earlier runs
are retained unchanged.

Environment: Apple M2 Max, 12 logical CPU cores, 64 GiB RAM, macOS 14.8.9,
Apple clang 16.0.0, `-std=c++17 -O2 -Wall -Wextra`, CommonCrypto SHA-256.
Both runs use normal OS caching; input files are freshly generated or edited
before storage. They are warm-cache measurements from separate runs, without
CPU isolation or a cold-cache experiment.

## What changes between before and after

Before: the store explicitly selects the serial chunker with `threads=1`.
After: the store selects the multiprocess chunker with 12 requested worker
processes, plus the coordinating parent.
The effective count is capped at the number of 64 MiB file units: initial
256 MB and 512 MB files use 4 and 8 processes; the 1–4 GB files use 12. Smaller
versions following large deletions can use fewer processes.

The new algorithm also changes chunk layout. Workers independently chunk
64 MiB units, include the boundary-match byte, and apply unit-tail merging.
Results are independent of worker count within this layout, but differ from
the serial layout. Thus this is a comparison of two implementations with
different boundaries, not an isolated measurement of parallel scheduling.
The source file bytes and edits are the same; chunk counts, hashes, manifests,
and deduplication can differ.

The current parallel store requests all chunk payloads, transports them through
pipes, and retains them in the returned vector before publishing missing blobs.
These costs are included in store timings. The serial path can omit copying
already-stored payloads. Both paths still read and hash the entire source file.

## Metric definitions

- Store time: the complete `storeChunksForSnapshot` call, including new chunk
  publication, manifest updates, and existing HLL persistence.
- File throughput: resulting file bytes / store seconds / 1,000,000.
- Speedup: saved serial time / parallel time. Below 1 means slower after.
- Diff time: checksum-verified loading/comparison of both chunk manifests;
  payload bytes are not read. The benchmark trims common prefix/suffix and
  computes exact LCS for the changed reference windows.
- Diff metadata throughput: bytes of both manifest objects / diff seconds /
  1,000,000. Logical file-equivalent diff throughput is separate in raw data.

Using the user's stored-size definition, `U` is unique retained chunk payload
bytes and `F` is original file size:

```text
Stored-size ratio per file = U / F
Average over 19 versions = U / (19 * F)      (lower is better)
Actual-version-size fraction = U / sum(actual version sizes)
```

The report also gives conventional history reduction `sum(version sizes)/U`
and per-version payload reuse `100*(1-new_payload_bytes/version_bytes)`, with
each denominator labeled. Main-history calculations exclude calibration cases.
Object bytes exclude filesystem allocation rounding; manifest/HLL bytes are
recorded separately from payloads.

File sizes <=4 GB still fit one default manifest leaf, so this run measures
storage and diff performance, not internal-node balancing or billion-reference
scalability. Each initial/final main version and every calibration case is
restored and verified by byte-level SHA-256 outside the timings.

## Reproduce the after run

From `dsi/utils/version_control/repolog/chunking_tests`:

```sh
make -B -j4 benchmark
./manifest_benchmark --processes 12 --dedup-controls 1 \
  --output benchmark_results/parallel-results.tsv \
  --summary benchmark_results/parallel-summary.tsv
```

Output files must not already exist. Use `--processes 1` for the current
serial path. Generated multi-GB input and object stores are temporary and are
removed after validation; source seeds and raw measurements are retained.

## Results

All 110 after snapshots/diffs completed successfully. Initial and final main
versions and all controls restored with matching byte-level SHA-256. All 110
input descriptions match the saved serial run: size, operation, position, edit
length/offset, source sizes, version number, and logical history bytes. Payload,
manifest, and logical-byte ledgers were independently verified. The six
production source/header SHA-256 fingerprints were unchanged during this run.
An explicit `-O2` rebuild produced byte-identical objects and benchmark binary
to those measured. [Source and binary fingerprints](2026-11-05-parallel-sha256.txt)
are retained alongside the results.

Across the 95 main store calls, total measured store time fell from
157.561 s to 116.049 s: **26.35% less time, 1.36× speedup**.
The same 145.578 GB of logical file processing corresponds to weighted file
throughput rising from 923.95 to
1254.45 MB/s. These totals exclude fixture edits,
generation, diff, verification, and calibration cases; they are not total
benchmark wall-clock duration.

### Initial storage at each size

One observation per size. Worker counts exclude the coordinating parent.

| File (MB) | Workers after | Before ms | After ms | Before MB/s | After MB/s | Speedup |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | 4 | 343.70 | 288.00 | 744.83 | 888.88 | 1.19× |
| 512 | 8 | 673.99 | 538.44 | 759.65 | 950.89 | 1.25× |
| 1000 | 12 | 1314.96 | 1095.47 | 760.48 | 912.85 | 1.20× |
| 2000 | 12 | 2640.89 | 2029.19 | 757.32 | 985.62 | 1.30× |
| 4000 | 12 | 5290.71 | 4134.35 | 756.04 | 967.50 | 1.28× |

### Consecutive edits to the 4 GB file

Medians of beginning/middle/end observations, not three repeated independent
trials. Source fixture modification is excluded; raw files retain it separately.
All six edited 4 GB snapshot medians improve by about 32–34% in this run.

| Edit to 4 GB file | Before ms | After ms | Before MB/s | After MB/s | Speedup | Time reduction |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Remove 64 KiB | 4274.64 | 2911.12 | 935.74 | 1374.02 | 1.47× | 31.90% |
| Add 64 KiB | 4245.74 | 2900.29 | 942.12 | 1379.17 | 1.46× | 31.69% |
| Update 64 KiB | 4237.76 | 2878.70 | 943.90 | 1389.51 | 1.47× | 32.07% |
| Remove 128 MiB | 4130.71 | 2792.74 | 935.86 | 1384.23 | 1.48× | 32.39% |
| Add 128 MiB | 4320.53 | 2890.90 | 925.81 | 1383.65 | 1.49× | 33.09% |
| Update 128 MiB | 4371.14 | 2874.90 | 915.09 | 1391.35 | 1.52× | 34.23% |

### Diff after edits

The comparator itself is unchanged, but it compares the new layout's references.
Changed boundaries can increase the changed LCS window. Across all 95 main
comparisons, measured diff time totals 12.429 ms before and
17.016 ms after. These are metadata comparisons and do not
scan gigabytes of payload. Timing is sub-millisecond and sensitive to cache and
scheduling noise.

| Operation, 4 GB file | Before diff ms | After diff ms | Before metadata MB/s | After metadata MB/s |
| --- | ---: | ---: | ---: | ---: |
| Initial add | 0.155 | 0.196 | 106.94 | 89.73 |
| Remove 64 KiB | 0.238 | 0.396 | 138.21 | 88.55 |
| Add 64 KiB | 0.231 | 0.396 | 142.67 | 88.48 |
| Update 64 KiB | 0.245 | 0.303 | 135.05 | 115.73 |
| Remove 128 MiB | 0.249 | 0.300 | 129.82 | 114.55 |
| Add 128 MiB | 0.247 | 0.298 | 131.20 | 115.81 |
| Update 128 MiB | 0.270 | 0.293 | 121.73 | 119.03 |

### Deduplication using the requested averaged definition

For each 19-version main history, average ratio = unique retained chunk payload
bytes / (19 * original file size). **Lower is better.** Calibration snapshots
are excluded. Ratios below are percentages of one nominal file size per version.

| File (MB) | Payload before (GB) | Payload after (GB) | Payload change | Average ratio before | Average ratio after |
| --- | ---: | ---: | ---: | ---: | ---: |
| 256 | 1.279114 | 1.249191 | -2.34% | 26.30% | 25.68% |
| 512 | 1.551475 | 1.559230 | +0.50% | 15.95% | 16.03% |
| 1000 | 2.023982 | 2.114291 | +4.46% | 10.65% | 11.13% |
| 2000 | 3.049329 | 3.336528 | +9.42% | 8.02% | 8.78% |
| 4000 | 5.033373 | 5.829684 | +15.82% | 6.62% | 7.67% |

For the 4 GB file, retained unique payload increases by
796.311 MB (15.82%). The
averaged stored-size fraction rises from 6.62% to
7.67%, so storage efficiency regresses despite faster
snapshots. The 256 MB history improves slightly; the larger histories consume
more payload after the layout change.

Deletion versions are smaller than the nominal file. For completeness, dividing
by their actual summed logical sizes gives the following fractions and inverse
conventional reduction ratios:

| File (MB) | Stored / actual version bytes, before | Stored / actual version bytes, after | Conventional reduction before | Conventional reduction after |
| --- | ---: | ---: | ---: | ---: |
| 256 | 28.67% | 28.00% | 3.49× | 3.57× |
| 512 | 16.64% | 16.72% | 6.01× | 5.98× |
| 1000 | 10.88% | 11.37% | 9.19× | 8.80× |
| 2000 | 8.11% | 8.87% | 12.33× | 11.27× |
| 4000 | 6.66% | 7.71% | 15.02× | 12.97× |

### Per-operation reuse and the boundary tradeoff

Medians alone hide the first near-beginning deletion's larger storage impact:

| Edit to 4 GB file | Reuse before | Reuse after | New payload before (MB) | New payload after (MB) |
| --- | ---: | ---: | ---: | ---: |
| Remove 64 KiB | 99.784% | 99.744% | 8.633 | 10.243 |
| Add 64 KiB | 99.783% | 99.783% | 8.699 | 8.699 |
| Update 64 KiB | 99.783% | 99.783% | 8.699 | 8.699 |
| Remove 128 MiB | 99.700% | 99.700% | 11.612 | 11.612 |
| Add 128 MiB | 96.419% | 96.419% | 143.238 | 143.238 |
| Update 128 MiB | 96.481% | 96.419% | 140.778 | 143.238 |

For the first 64 KiB deletion near the beginning of the 4 GB file, new unique
payload grows from 8.633 MB before to
879.712 MB after. Reuse falls from
99.784% to 78.007%. An
insertion/deletion shifts content relative to fixed 64 MiB unit boundaries,
changing chunks at many later unit edges. Subsequent edits can reuse shifted
chunks already stored by earlier history, which is why the three-position
median is much less severe than this first deletion.

The new pipeline gains concurrent boundary finding/hashing but includes fork,
pipe transfer, complete payload materialization, and serial publication costs.
These are source-level contributors to the measured end-to-end behavior, not
a separately profiled CPU breakdown. Faster snapshots do not imply identical
chunk layout or improved deduplication.

All calibration checks pass at all five sizes in both runs: unchanged and
reverted-original snapshots add zero unique payload and reuse their roots;
fully new random rewrites store the entire file as new payload. There is no
compression in these payload measurements.

### Complete edit timing/throughput table

| File (MB) | Edit | Before ms | After ms | Before MB/s | After MB/s | Speedup |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 256 | Remove 64 KiB | 269.48 | 228.95 | 949.72 | 1117.86 | 1.18× |
| 256 | Add 64 KiB | 270.23 | 235.63 | 947.34 | 1086.46 | 1.15× |
| 256 | Update 64 KiB | 270.25 | 225.90 | 947.27 | 1133.25 | 1.20× |
| 256 | Remove 128 MiB | 129.75 | 146.61 | 938.59 | 830.67 | 0.89× |
| 256 | Add 128 MiB | 310.40 | 253.99 | 824.75 | 1007.92 | 1.22× |
| 256 | Update 128 MiB | 299.63 | 261.54 | 854.39 | 978.83 | 1.15× |
| 512 | Remove 64 KiB | 535.75 | 474.43 | 955.54 | 1079.06 | 1.13× |
| 512 | Add 64 KiB | 538.57 | 483.78 | 950.66 | 1058.33 | 1.11× |
| 512 | Update 64 KiB | 534.89 | 480.28 | 957.20 | 1066.04 | 1.11× |
| 512 | Remove 128 MiB | 395.95 | 381.77 | 954.11 | 989.56 | 1.04× |
| 512 | Add 128 MiB | 569.45 | 492.18 | 899.12 | 1040.27 | 1.16× |
| 512 | Update 128 MiB | 566.88 | 523.44 | 903.18 | 978.15 | 1.08× |
| 1000 | Remove 64 KiB | 1041.17 | 867.50 | 960.39 | 1152.67 | 1.20× |
| 1000 | Add 64 KiB | 1055.52 | 866.85 | 947.40 | 1153.60 | 1.22× |
| 1000 | Update 64 KiB | 1042.07 | 849.48 | 959.63 | 1177.20 | 1.23× |
| 1000 | Remove 128 MiB | 901.84 | 780.68 | 960.02 | 1109.01 | 1.16× |
| 1000 | Add 128 MiB | 1106.02 | 878.52 | 904.14 | 1138.28 | 1.26× |
| 1000 | Update 128 MiB | 1075.91 | 915.31 | 929.44 | 1092.52 | 1.18× |
| 2000 | Remove 64 KiB | 2097.41 | 1612.21 | 953.53 | 1240.49 | 1.30× |
| 2000 | Add 64 KiB | 2112.27 | 1528.59 | 946.85 | 1308.39 | 1.38× |
| 2000 | Update 64 KiB | 2093.71 | 1556.77 | 955.24 | 1284.71 | 1.34× |
| 2000 | Remove 128 MiB | 1949.46 | 1454.45 | 957.08 | 1282.81 | 1.34× |
| 2000 | Add 128 MiB | 2147.72 | 1570.01 | 931.22 | 1273.87 | 1.37× |
| 2000 | Update 128 MiB | 2125.92 | 1599.51 | 940.77 | 1250.38 | 1.33× |
| 4000 | Remove 64 KiB | 4274.64 | 2911.12 | 935.74 | 1374.02 | 1.47× |
| 4000 | Add 64 KiB | 4245.74 | 2900.29 | 942.12 | 1379.17 | 1.46× |
| 4000 | Update 64 KiB | 4237.76 | 2878.70 | 943.90 | 1389.51 | 1.47× |
| 4000 | Remove 128 MiB | 4130.71 | 2792.74 | 935.86 | 1384.23 | 1.48× |
| 4000 | Add 128 MiB | 4320.53 | 2890.90 | 925.81 | 1383.65 | 1.49× |
| 4000 | Update 128 MiB | 4371.14 | 2874.90 | 915.09 | 1391.35 | 1.52× |

Matched individual observations, including controls and diff metadata throughput,
are in [2026-11-05-comparison.tsv](2026-11-05-comparison.tsv). Group medians
for the 35 main operation/size groups are in
[2026-11-05-comparison-summary.tsv](2026-11-05-comparison-summary.tsv).
