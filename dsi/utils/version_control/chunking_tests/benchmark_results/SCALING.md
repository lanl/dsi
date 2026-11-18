# Strong and weak scaling of C++ file storage

Six new measurements were needed; the matching 12-worker endpoint was reused.
New 4 GB store times stay nearly flat at 3.35–3.52 s across 2–8 workers. With
constant bytes per worker, weak-scaling time grows rather than staying flat:
efficiency falls to 27.71% at 8 workers, or 15.42% at the archived 12-worker point.
See [the results](#results) and the 2-worker reference definitions below.

This measures end-to-end **initial file storage**: read, content-defined
chunking, SHA-256, worker IPC/payload collection, publication of unique chunks,
manifest publication, and HLL persistence. Source generation, manifest diff,
restoration, and verification are outside the timed store call.

Only six previously missing configurations are run. The saved 4 GB/12-worker
initial-add observation is reused in both curves. The saved legacy serial
4 GB result is retained separately for context. No existing configurations are
rerun, and no production C++ or Python implementation is changed.

## Experiment design

Strong scaling holds the file at exactly 4,000,000,000 bytes and varies workers
over 2, 4, 8, and 12. All use the same parallel chunk layout and the same
deterministic source bytes.

Weak scaling holds work per worker at approximately 333,333,333.333 bytes:

| Worker processes | File bytes | File MB | Bytes/worker |
| --- | ---: | ---: | ---: |
| 2 | 666666666 | 666.666666 | 333333333.000 |
| 4 | 1333333333 | 1333.333333 | 333333333.250 |
| 8 | 2666666666 | 2666.666666 | 333333333.250 |
| 12 | 4000000000 | 4000.000000 | 333333333.333 |

Sub-byte rounding is immaterial. At these sizes the implementation schedules
5 independent 64 MiB units per worker; the last unit is partial. The process
cap does not reduce the requested worker count. A coordinating parent is
additional to the listed workers.

Both curves use **2 workers as their reference**. The storage API routes
0/1 workers to the legacy serial algorithm, which has different chunk
boundaries. Its timing is not used to calculate same-layout parallel efficiency.
These are therefore relative strong-scaling measurements over 2–12 workers,
not a claim of same-algorithm scaling from one worker.

```text
Strong speedup(p) = T(2, 4 GB) / T(p, 4 GB)
Strong efficiency(p) = strong speedup(p) / (p/2)

Weak efficiency(p) = T(2, 666666666 bytes) / T(p, Np)
Weak throughput gain(p) = throughput(p, Np) / throughput(2, 666666666 bytes)
Ideal weak efficiency = 100%; ideal weak throughput gain = p/2

Throughput MB/s = file bytes / store seconds / 1,000,000
```

MB and GB are decimal. Work per worker is fixed in bytes, not the number of
content-defined chunks. Every store begins with a fresh payload repository;
all source bytes are new, so deduplication does not change the workload.

## Reused observations and comparability

The shared 4 GB/12-worker endpoint is the `initial_add` observation in
[the saved parallel run](2026-11-05-parallel-results.tsv): 4,134.351209 ms,
967.503678 MB/s. The legacy serial context point comes from
[the saved serial run](2026-11-05-dedup-results.tsv): 5,290.711291 ms,
756.042010 MB/s. Both were previously verified by full restoration.

All six production source/header SHA-256 fingerprints match the
[saved parallel fingerprints](2026-11-05-parallel-sha256.txt). Compilation uses
the same Apple clang 16.0.0, C++17, `-O2 -Wall -Wextra`,
and CommonCrypto backend. The native store call and generator are unchanged.
The harness adds `--initial-only 1` to omit edit cycles and duplicate restore
work, and records source, ordered-chunk, and manifest-root digests for new points.

Other old observations do not supply the missing cells: changing file size
while keeping 12 workers does not hold bytes per worker constant. The smaller
old files also cap actual worker counts at their number of units. They are not
silently substituted into either curve.

Hardware remains the Apple M2 Max, 12 logical cores, 64 GiB RAM, macOS 14.8.9.
These are warm-cache measurements with normal filesystem caching/fsync, not
CPU-only chunking timings or a cold-disk experiment. Each point is one
observation; the endpoint is archived rather than interleaved with new trials.
The curves are descriptive, without confidence intervals or a claim of a
statistically established optimum process count.

## Reproduce only the new measurements

From `dsi/utils/version_control/repolog/chunking_tests`, build with
`make -B -j4 benchmark`. Run each command sequentially to avoid resource
contention between benchmark jobs; choose output names that do not exist.

```sh
for p in 2 4 8; do
  ./manifest_benchmark --initial-only 1 --processes "$p" --sizes 4000000000 \
    --output "benchmark_results/scaling-strong-p${p}.tsv" \
    --summary "benchmark_results/scaling-strong-p${p}-summary.tsv"
done
./manifest_benchmark --initial-only 1 --processes 2 --sizes 666666666 \
  --output benchmark_results/scaling-weak-p2.tsv \
  --summary benchmark_results/scaling-weak-p2-summary.tsv
./manifest_benchmark --initial-only 1 --processes 4 --sizes 1333333333 \
  --output benchmark_results/scaling-weak-p4.tsv \
  --summary benchmark_results/scaling-weak-p4-summary.tsv
./manifest_benchmark --initial-only 1 --processes 8 --sizes 2666666666 \
  --output benchmark_results/scaling-weak-p8.tsv \
  --summary benchmark_results/scaling-weak-p8-summary.tsv
```

There is no 12-worker command because that matching observation is reused.
Generated files and object stores are temporary and are removed after the
restored file's byte-level SHA-256 is checked against generation's digest.

## Results

Only **six new stores** were executed, each followed by manifest diff and full
restoration verification. Two saved observations are reused: the parallel
12-worker endpoint (shared by both curves) and the separate legacy serial
reference. All six new restores passed SHA-256 verification. For the three
new 4 GB points, source byte hashes, ordered-chunk digests, and manifest root
hashes are identical across 2/4/8 workers. Chunk counts also agree with the
archived 12-worker result. The six production source fingerprints remained
unchanged after measurements.

### Strong scaling: fixed 4 GB

| Workers | Store ms | MB/s | Speedup vs 2 | Efficiency vs 2 | Observation |
| --- | ---: | ---: | ---: | ---: | --- |
| 2 | 3517.34 | 1137.22 | 1.000× | 100.00% | New |
| 4 | 3350.72 | 1193.77 | 1.050× | 52.49% | New |
| 8 | 3421.56 | 1169.06 | 1.028× | 25.70% | New |
| 12 | 4134.35 | 967.50 | 0.851× | 14.18% | Reused |

Going from 2 to 4 workers reduces time by only
4.74%, despite doubling the workers.
At 8 workers the speedup is only 1.028× rather than the ideal
4× relative to 2 workers. The new points are nearly flat at roughly 1.14–1.19
GB/s. Four workers is the fastest single measured point, but its advantage
over 8 workers is small and is not evidence of a statistically established
optimum. The archived 12-worker observation is slower; differences between
measurement sessions may contribute, so its exact ranking should not be
interpreted as a controlled repeat.

For context, the legacy serial observation was 5290.71 ms and
756.04 MB/s. It is a different chunk layout and is deliberately
excluded from the efficiency calculations above.

### Weak scaling: approximately 333 MB per worker

| Workers | File MB | Store ms | MB/s | Weak efficiency | Throughput gain vs 2 | Observation |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 2 | 666.667 | 637.69 | 1045.44 | 100.00% | 1.000× | New |
| 4 | 1333.333 | 1140.55 | 1169.03 | 55.91% | 1.118× | New |
| 8 | 2666.667 | 2301.02 | 1158.91 | 27.71% | 1.109× | New |
| 12 | 4000.000 | 4134.35 | 967.50 | 15.42% | 0.925× | Reused |

Ideal weak scaling would keep time at 637.69 ms while total
file size and total throughput grow with worker count. Instead, time rises
from 637.69 to 4134.35 ms while
file throughput remains near 1 GB/s. Even using only the new 2/4/8-worker
observations, efficiency falls from 100% to 27.71%; the
archived 12-worker endpoint gives 15.42%.

### Interpretation and scope

End-to-end storage shows little strong scaling and poor weak scaling over this
range. This is consistent with work that remains in the coordinating parent:
collecting full payloads through pipes, materializing the whole chunk vector,
then publishing payload/manifest objects. All workers also share memory and
filesystem bandwidth. Fork/pipe overhead and OS scheduling can contribute.
These are source-level explanations consistent with the measurements, not a
separately profiled allocation of CPU or I/O time.

The results do not establish that the boundary-finding/hash kernel itself
fails to scale: that kernel is not isolated here. Likewise, this does not
measure multi-level manifest balancing (all these manifests fit one leaf).
No algorithm changes or additional worker-count runs were made to tune an
optimum. Storage is fresh in every case and all payload bytes are unique;
deduplication ratios therefore do not bias these initial-store curves.

Consolidated measurements, derived efficiencies, and the provenance of every
point are in [2026-11-05-scaling.tsv](2026-11-05-scaling.tsv). Raw one-row result
and summary TSVs for each new point are alongside that file, named
`2026-11-05-scaling-{strong|weak}-p{2|4|8}-{results|summary}.tsv`.
