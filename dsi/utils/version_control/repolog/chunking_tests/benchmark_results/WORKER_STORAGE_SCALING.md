# Scaling relative to the archived one-worker sequential result

The current 12-worker 4 GB store takes **784.69 ms** at **5,097.55 MB/s**.
Compared with the archived one-worker sequential time of **5,290.71 ms**,
this gives **6.742× speedup** and **56.19% efficiency**.

Charts now show the current measurements, the archived one-worker sequential
reference, and ideal scaling. The previous parallel-run series labelled
"Previous: 1 archived sample" has been removed from all chart panels and legends.
No benchmarks were rerun for this calculation.

## Reference and interpretation

The reference is the 4,000,000,000-byte `initial_add` observation in
[2026-11-05-dedup-results.tsv](2026-11-05-dedup-results.tsv):
one worker, 5,290.711291 ms, 756.042010 MB/s, 412 chunks, one sample.
The current parallel chunker emits 438 chunks for the same source bytes.

These are end-to-end comparisons across different chunking implementations and
measurement sessions. Values above 100% efficiency can reflect implementation
differences and measurement variation. They do not isolate the scaling of one
unchanged algorithm.

Strong scaling compares the same 4 GB file directly. Weak scaling uses smaller
files, so its serial comparison is **estimated by normalizing file size**:
the archived 4 GB result implies a serial rate of 756.042010 MB/s. This assumes
serial execution time grows linearly with file bytes, including any overhead.
There is no measured one-worker 333 MB point. The implied time for one worker's
approximately 333.333 MB share is **440.892608 ms**; this is an extrapolation,
not a new observation.

## Strong scaling: fixed 4 GB

Speedup is the archived one-worker time divided by current time. Efficiency is
speedup divided by the current worker count. The first row is the archived
reference; the other rows are current three-trial medians.

| Workers | Time ms | MB/s | Speedup vs archived 1 | Efficiency vs archived 1 | Samples |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 5,290.71 | 756.04 | 1.000× | 100.00% | 1 |
| 2 | 2,527.52 | 1,582.58 | 2.093× | 104.66% | 3 |
| 4 | 1,433.66 | 2,790.06 | 3.690× | 92.26% | 3 |
| 8 | 953.55 | 4,194.84 | 5.548× | 69.36% | 3 |
| 12 | 784.69 | 5,097.55 | 6.742× | 56.19% | 3 |

Current min–max time ranges are 2,504.90–2,535.22 ms (2 workers),
1,431.34–1,446.75 ms (4), 952.85–958.40 ms (8), and 782.87–839.73 ms (12).

![Strong scaling relative to the archived one-worker sequential result](figures/worker-storage-strong-scaling.png)

[SVG](figures/worker-storage-strong-scaling.svg) ·
[PDF](figures/worker-storage-strong-scaling.pdf)

## Weak scaling: approximately 333.333 MB per worker

For varying file sizes, speedup is the current throughput divided by the archived
one-worker throughput. Estimated efficiency is this normalized speedup divided
by worker count. Dividing the raw 4 GB sequential time by the smaller-file
parallel time would compare different amounts of work and is not used.

| Workers | File MB | Median ms | MB/s | Normalized speedup vs archived 1 | Estimated efficiency |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2 | 666.667 | 431.14 | 1,546.30 | 2.045× | 102.26% |
| 4 | 1,333.333 | 508.15 | 2,623.92 | 3.471× | 86.76% |
| 8 | 2,666.667 | 624.02 | 4,273.36 | 5.652× | 70.65% |
| 12 | 4,000.000 | 784.69 | 5,097.55 | 6.742× | 56.19% |

All weak figures depend on the constant serial-throughput assumption. At 12
workers, the file is exactly 4 GB, so the calculation coincides with the directly
measured strong comparison. For the other sizes, these are normalized estimates,
not measurements of sequential execution at the corresponding file size.

![Weak scaling normalized to archived sequential throughput](figures/worker-storage-weak-scaling.png)

[SVG](figures/worker-storage-weak-scaling.svg) ·
[PDF](figures/worker-storage-weak-scaling.pdf)

## Formulas

Let `T_ref = 5290.711291 ms`, `N_ref = 4,000,000,000 bytes`,
`p` be worker count, `N_p` the file bytes, and `T_p` the median time.

```text
Reference throughput = N_ref / (T_ref * 1,000) MB/s
Current throughput = N_p / (T_p * 1,000) MB/s

Strong speedup = T_ref / T_p                 [N_p = N_ref]
Strong efficiency (%) = 100 * speedup / p

Estimated serial time for N_p bytes = T_ref * N_p / N_ref
Weak normalized speedup = (T_ref * N_p / N_ref) / T_p
                       = current throughput / reference throughput
Estimated weak efficiency (%) = 100 * weak normalized speedup / p

Ideal strong time = T_ref / p
Ideal weak time = T_ref * N_p / (N_ref * p)   [about 440.892608 ms]
Ideal throughput = reference throughput * p
Ideal speedup = p
Ideal efficiency = 100%
```

## Measurement scope and validation

- Same deterministic SplitMix64 source as before: seed `0x20261105D51`
  (2209241652561), little-endian bytes.
- Strong: exactly 4,000,000,000 bytes at 2, 4, 8, and 12 workers.
- Weak: 666,666,666 / 1,333,333,333 / 2,666,666,666 / 4,000,000,000 bytes
  at 2 / 4 / 8 / 12 workers.
- Three independent trials per current configuration, run sequentially in
  changed order between passes. There are 21 physical stores; the three 4 GB,
  12-worker observations are shared by both curves.
- Every trial starts with a fresh repository. Stored payload bytes divided by
  logical file bytes is 1.0 in all trials.
- Timed: complete `storeChunksForSnapshot`, including worker launch/read/chunk/
  hash/cache/payload publication, directory sync, manifest publication, and HLL
  persistence. Generation, diff, restore, and verification are outside timing.
- All 21 full restores passed source SHA-256 verification; all manifest diffs
  passed. Saved source hashes, chunk digests, and roots match the earlier
  parallel results where available. Raw timings are unchanged.
- Apple M2 Max, 12 cores (8 performance + 4 efficiency), 64 GB RAM;
  macOS 14.8.9; Apple clang 16.0.0, C++17, `-O2 -Wall -Wextra`,
  CommonCrypto SHA-256. Worker counts exclude the parent.
- Warm source/filesystem caching and the existing fsync behavior.
- Blue points show current medians with min–max ranges. Gray diamonds on the
  strong chart show the archived sequential observation, also at speedup 1
  and efficiency 100%. Dashed ideal lines use that sequential reference.
  Weak ideal time/throughput lines use the file-size normalization above.
  The archived 4 GB time is not plotted as a measured 333 MB weak point.
- Error bars describe three-trial ranges, not confidence intervals. The
  archived sequential reference has only one observation.
- No C++ or Python source changed. Saved fingerprints are in
  [2026-11-05-worker-storage-sha256.txt](2026-11-05-worker-storage-sha256.txt).

## Data and reproduction

[2026-11-05-worker-storage-scaling.tsv](2026-11-05-worker-storage-scaling.tsv)
now includes `speedup_vs_archived_serial`,
`efficiency_vs_archived_serial_pct`, and `serial_comparison_basis`.
Earlier two-worker metrics are retained with explicit
`*_vs_2_workers*` column names. Historical previous-parallel timing and
efficiency columns remain for provenance; those efficiency columns use the
old two-worker baseline and are excluded from the current charts.
The `serial_reference` row is the one archived observation.

Raw files follow
`2026-11-05-worker-storage-{strong|weak}-p{workers}-r{trial}-{results|summary}.tsv`.
Pass one ran strong 2/4/8/12 then weak 2/4/8; pass two weak 8/4/2 then
strong 12/8/4/2; pass three strong 4/2/12/8 then weak 4/8/2.
The weak 12-worker endpoint reuses the matching strong observations.

Build with `make -j4 benchmark`. To reproduce a measurement, choose new output
names and run, for example:

```sh
./manifest_benchmark --initial-only 1 --processes 12 --sizes 4000000000 \
  --output benchmark_results/unique-results.tsv \
  --summary benchmark_results/unique-summary.tsv
```

Generated source files, restored files, and repositories were cleaned up by the
C++ harness after verification. Prior parallel results remain archived in
[2026-11-05-scaling.tsv](2026-11-05-scaling.tsv), but are omitted from the charts.
