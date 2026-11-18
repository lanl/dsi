# Strong scaling: fixed 50 GB file

All **15 stores and full-file restores passed SHA-256 verification**.
Each worker count (1, 2, 4, 8, 12) has three independent trials.

The lowest observed median is at **4 workers: 22,750.02 ms,
2,197.80 MB/s, 3.244× speedup, 81.10% efficiency** relative to the freshly
measured 50 GB one-worker sequential median of **73,801.48 ms**.
The 4–12-worker medians remain around 2.0–2.2 GB/s, with overlapping trial
ranges. Four workers has the lowest sampled median; three trials do not establish
a precise optimum process count.

## Chart

![Strong scaling: fixed 50 GB file](figures/worker-storage-strong-scaling-50GB.png)

[SVG](figures/worker-storage-strong-scaling-50GB.svg) ·
[PDF](figures/worker-storage-strong-scaling-50GB.pdf)

Only the current 50 GB measurements, their measured one-worker sequential
reference, and ideal scaling are shown. No weak-scaling benchmark or chart was
generated. Existing 4 GB charts and results remain unchanged.

## Results

The file is exactly **50,000,000,000 bytes**; GB and MB are decimal.

| Workers | Median store ms | Min–max ms | MB/s | Speedup vs 1 | Efficiency vs 1 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 73,801.48 | 72,442.87–74,169.86 | 677.49 | 1.000× | 100.00% |
| 2 | 37,018.34 | 36,861.49–37,394.59 | 1,350.68 | 1.994× | 99.68% |
| 4 | 22,750.02 | 22,218.17–24,858.84 | 2,197.80 | 3.244× | 81.10% |
| 8 | 23,223.78 | 20,818.16–24,610.99 | 2,152.97 | 3.178× | 39.72% |
| 12 | 24,878.42 | 21,867.33–26,132.30 | 2,009.77 | 2.966× | 24.72% |

```text
Throughput MB/s = 50,000,000,000 / (median store ms * 1,000)
Speedup(p) = median T(1, 50 GB) / median T(p, 50 GB)
Efficiency(p) = speedup(p) / p
Ideal time(p) = median T(1, 50 GB) / p
Ideal throughput(p) = throughput(1) * p
```

The one-worker reference was measured at 50 GB in this run; the archived 4 GB
time was not extrapolated. The one-worker API uses the sequential chunking
layout (5,169 chunks), while 2–12 workers use the independent-unit parallel
layout (5,460 chunks). These are end-to-end implementation comparisons across
two chunk layouts. All parallel worker counts produce identical ordered-chunk
digests and manifest roots. All sequential trials agree with each other.

Blue points are parallel medians and gray diamonds are the sequential reference.
Time and throughput error bars show the three observed extrema. Speedup and
efficiency ranges vary the parallel time with the sequential median held fixed.
These are observed ranges, not confidence intervals.

## Dataset, timing, and verification

- One deterministic synthesized byte stream with SplitMix64 seed
  `0x20261105D51` (2209241652561), matching the previous benchmark's generator.
- Source SHA-256 for all 15 trials:
  `f49065f24c48581d4a4ad8f38fa57ac212488a4171534cf73d90eb3d786d451a`.
- Each trial has a fresh chunk repository. Exactly 50,000,000,000 payload bytes
  are newly stored per trial; stored payload / logical file size = 1.0.
- The timed operation is the complete `storeChunksForSnapshot` call: reads,
  chunk computation, hashing, worker/cache coordination, chunk publication,
  directory sync, manifest publication, and HLL persistence.
- Source preparation, manifest diff, restoration, and byte verification are
  outside the store timer. All 15 initial manifest diffs also passed.
- Every trial physically reconstructs the full 50 GB file through its manifest,
  validates chunk digests during reconstruction, and checks the restored file's
  complete SHA-256 against the source digest.
- First-sweep 1- and 2-worker trials generate the source before timing. The
  first temporary source was cleaned after its successful restore; the second
  generated copy was retained. The other 13 trials hash/read that same retained
  source before timing. Source bytes are identical across all preparations.
- Filesystem caching is not explicitly flushed or controlled. This is an
  end-to-end file-store benchmark with normal caching and fsync behavior.
  Source, payload repository, and restored output together can occupy about
  150 GB during validation; this is larger than the host's RAM.
- Worker count excludes the coordinating parent for the parallel cases.
- The synthesized input, per-trial repositories, and restored files were
  removed after verification. No 50 GB dataset remains from this run.

## Environment and code

Apple M2 Max, 12 cores (8 performance + 4 efficiency), 64 GB RAM; macOS 14.8.9;
Apple clang 16.0.0, C++17, `-O2 -Wall -Wextra`, CommonCrypto SHA-256.
See [the environment capture](2026-11-05-strong-50GB-environment.txt).

The storage implementation was unchanged. The C++ benchmark harness was
extended to permit 50 GB with `--initial-only 1` and to accept a retained input
file. Input generation and input hashing are outside the measured call.
Production and harness fingerprints are saved in
[the final fingerprint file](2026-11-05-strong-50GB-sha256.txt) and were checked
after measurements. [First-trial fingerprints](2026-11-05-strong-50GB-first-trial-sha256.txt)
preserve the harness revision before the retained-input options were added.
The eight storage/source-header fingerprints are identical between these
revisions. No Python file was changed.

## Raw data and reproduction

[Consolidated measurements](2026-11-05-strong-50GB-scaling.tsv) include all
medians, extrema, derived metrics, source/layout digests, and raw-source paths.
Raw result and one-row summary files follow:
`2026-11-05-strong-50GB-p{workers}-r{trial}-{results|summary}.tsv`.

Pass one ran 1/2/4/8/12 workers; pass two ran 12/8/4/2/1; pass three ran
4/1/12/2/8. Trials were sequential, with no concurrent benchmark jobs.

Build with `make -j4 benchmark`. For a generated-source initial-add trial:

```sh
./manifest_benchmark --initial-only 1 --processes 1 --sizes 50000000000 \
  --output benchmark_results/unique-p1-results.tsv \
  --summary benchmark_results/unique-p1-summary.tsv
```

To retain a generated file for further trials, use `--generate-input` with
an unused path in an existing directory. Subsequent calls use `--input`
instead; the harness rehashes the source outside timing. For example:

```sh
dataset_dir=$(mktemp -d /tmp/dsi-strong50GB-XXXXXX)
./manifest_benchmark --initial-only 1 --processes 2 --sizes 50000000000 \
  --generate-input "$dataset_dir/data.bin" \
  --output benchmark_results/unique-p2-results.tsv \
  --summary benchmark_results/unique-p2-summary.tsv
./manifest_benchmark --initial-only 1 --processes 4 --sizes 50000000000 \
  --input "$dataset_dir/data.bin" \
  --output benchmark_results/unique-p4-results.tsv \
  --summary benchmark_results/unique-p4-summary.tsv
```

Choose unused output names for every trial, and remove the generated input
after finishing. The `--input` and `--generate-input` options require
`--initial-only 1` and one matching size; generated input must not already exist.
The 50 GB allowance applies only to initial-add runs. Existing edit benchmarks
retain their 4 GB limit.
