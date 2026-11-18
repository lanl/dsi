# C++ deduplication benchmark

This is a fresh run of the C++ implementation, with explicit storage accounting
and calibration cases. The earlier execution-time benchmark's data files remain
unchanged. No Python code is invoked, and the production chunking/manifest
algorithms are unchanged.

The 110-case run completed successfully. For the 4 GB file, 19 versions total
75.597 GB logically and occupy 5.033 GB of unique payload: **15.02× deduplication,
or 93.34% storage savings**. See [the results](#results) for all sizes and controls.

## Definitions

For a version with logical file size `V`, newly created unique payload bytes
`delta_U`, cumulative logical version bytes `L`, and total unique retained
payload bytes `U`:

```text
Per-version payload reuse (%) = 100 * (1 - delta_U / V)
Incremental deduplication ratio = V / delta_U

History payload deduplication ratio = L / U
History payload storage saving (%) = 100 * (1 - U / L)

History total-object ratio = L / (U + manifest bytes + auxiliary bytes)
```

When no new payload is stored, reuse is 100% and the incremental ratio is
infinite (`inf` in TSV). This does not imply an infinite history ratio: original
payloads remain stored. The history ratio is calculated from byte totals, not
by averaging individual version ratios.

Per-version reuse is relative to the entire retained payload store, including
older versions, rather than just the immediately preceding version. Repeated
chunks within a version would also reduce new unique storage, although this
random-data workload is designed without such repetition.

Stored bytes are measured by enumerating actual payload/manifest files after
each snapshot and summing their file sizes. Auxiliary bytes include persisted
HLL state. These are file-content bytes, excluding filesystem block rounding,
directory/inode overhead, temporary source/restore files, and the result files.
The standalone C++ store has no Python/SQL repository metadata. Payloads are
uncompressed, so the measured reduction comes from content-addressed reuse.

## Results

All 110 snapshots and their diffs completed successfully. The initial/final main
versions and every calibration version restored correctly. Stored-byte ledgers
were independently checked against the raw rows, and the 95 main snapshots
produced exactly the same unique-payload totals as the earlier deterministic
execution-time run.

### Cumulative history: 19 main versions per file

The three controls are excluded. Logical history sums actual version sizes,
including smaller snapshots immediately after deletions. Ratios use exact byte
counts, not the rounded GB values displayed here.

| File size (MB) | Logical history (GB) | Unique payload (GB) | Payload ratio | Payload saving | Manifest + HLL (KiB) | Total-object ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | 4.461 | 1.279 | 3.4877× | 71.328% | 22.04 | 3.4876× |
| 512 | 9.325 | 1.551 | 6.0105× | 83.362% | 41.49 | 6.0103× |
| 1000 | 18.597 | 2.024 | 9.1884× | 89.117% | 77.07 | 9.1880× |
| 2000 | 37.597 | 3.049 | 12.3296× | 91.889% | 157.66 | 12.3290× |
| 4000 | 75.597 | 5.033 | 15.0192× | 93.342% | 308.05 | 15.0182× |

At 4 GB, 75.597150208 GB of logical version history occupies 5.033372915 GB of
unique payload: **15.0192× deduplication and 93.3418% payload storage savings**.
Adding 311,340 bytes of manifests and 4,104 bytes of HLL state gives
5.033688359 GB of total object contents and a 15.0182× total-object ratio.

### Payload reuse per edited version

Medians across beginning/middle/end positions. Reuse counts references to any
already-stored payload, including older history. It is a byte-based measure,
not the fraction of chunk hashes that match.

| Edit | 256 MB reuse | 512 MB reuse | 1 GB reuse | 2 GB reuse | 4 GB reuse |
| --- | ---: | ---: | ---: | ---: | ---: |
| Remove 64 KiB | 96.627% | 98.314% | 99.135% | 99.568% | 99.784% |
| Add 64 KiB | 96.602% | 98.301% | 99.128% | 99.565% | 99.783% |
| Update 64 KiB | 96.602% | 98.301% | 99.128% | 99.565% | 99.783% |
| Remove 128 MiB | 92.941% | 97.049% | 99.363% | 99.378% | 99.700% |
| Add 128 MiB | 33.782% | 70.342% | 86.027% | 92.509% | 96.419% |
| Update 128 MiB | 47.417% | 72.778% | 86.250% | 93.146% | 96.481% |

### New payload written for edits to the 4 GB file

| Edit to 4 GB file | New payload (MB) | Payload reuse | Incremental ratio |
| --- | ---: | ---: | ---: |
| Remove 64 KiB | 8.633 | 99.784% | 463.33× |
| Add 64 KiB | 8.699 | 99.783% | 459.85× |
| Update 64 KiB | 8.699 | 99.783% | 459.85× |
| Remove 128 MiB | 11.612 | 99.700% | 332.91× |
| Add 128 MiB | 143.238 | 96.419% | 27.93× |
| Update 128 MiB | 140.778 | 96.481% | 28.41× |

A 64 KiB update stores a median 8.699 MB because the entire containing chunk is
replaced. Larger insertions/updates can also create additional boundary chunks
before content-defined boundaries realign. Deletions can create boundary chunks,
and do not reclaim objects needed by retained older versions.

### Calibration results

All cases passed at all five file sizes:

| Case | New unique payload bytes | Payload reuse | New manifest objects |
| --- | ---: | ---: | ---: |
| Initial random file | Entire file size | 0% | 1 |
| Unchanged snapshot | 0 | 100% | 0 |
| Revert to initial content | 0 | 100% | 0 |
| Entirely new random rewrite | Entire file size | 0% | 1 |

The high reuse of small edits does not remove full-file read/chunk/hash costs:
this benchmark measures storage savings. Larger files have better ratios partly
because the same fixed-size edits modify a smaller fraction of their contents.
These ratios describe this synthesized workload and retention of 19 versions;
they are not a general compression factor for a single current file.

## Workload

The main workload matches the previous benchmark: one synthesized random file
at a time, independently stored at 256 MB, 512 MB, 1 GB, 2 GB, and 4 GB (decimal).
Each size has a fresh repository. The generator, seed, compiler, chunk sizes,
and hardware are described in [the execution-time report](README.md).

Each history contains 19 main versions: the initial file followed by 18 edits.
At the beginning, middle, and end, perform a 64 KiB removal, insertion of new
content, and update, followed by the same sequence with 128 MiB. These sizes are
below the minimum 1 MiB chunk size and above the maximum 64 MiB chunk size,
respectively. Removed history is retained; no garbage collection is performed.

After those 19 versions, run three separate calibration snapshots in the same
store:

1. **Unchanged:** snapshot the latest file again. Expect zero new unique payload
   bytes and no new manifest objects.
2. **Revert to initial:** regenerate the initial file exactly and snapshot it.
   Expect zero new unique payload bytes and reuse of the original manifest.
3. **Full rewrite:** generate the entire file using a different deterministic
   random seed. Expect all payload bytes to be new.

With the default chunk sizes, every file in this matrix fits one manifest leaf.
That leaf has a canonical encoding, so restoring the original content also
restores its original root hash. A larger multi-page tree can have a different
layout after edits even when the content matches. These measurements assess
payload reuse, not multi-level manifest-page sharing or billion-chunk scalability.

The main-history table excludes these three calibration versions. Raw history
columns continue accumulating through all 22 versions, so filter
`scenario_group=main` when reproducing the main-history ratios.

The run covers 110 snapshot/diff pairs: 95 main versions and 15 controls. Each
main version's manifest structure, counts, and digest are validated. Initial
and final main versions, plus all controls, are restored and checked by full
byte-level SHA-256. Dataset generation and validation are outside store timings.

## Reproduce

From `dsi/utils/version_control/repolog/chunking_tests`:

```sh
make -j4 benchmark
./manifest_benchmark --dedup-controls 1 \
  --output benchmark_results/dedup-results.tsv \
  --summary benchmark_results/dedup-summary.tsv
```

Output paths must not already exist. The run needs temporary space for source,
restore output, and retained unique chunks; generated data is removed per size
after validation. Only small result artifacts are retained. A smoke test is:

```sh
./manifest_benchmark --sizes 160000000 --positions middle --dedup-controls 1 \
  --output /tmp/dsi-dedup-smoke.tsv --summary /tmp/dsi-dedup-smoke-summary.tsv
```

Raw observations: [2026-11-05-dedup-results.tsv](2026-11-05-dedup-results.tsv).
Per-operation medians: [2026-11-05-dedup-summary.tsv](2026-11-05-dedup-summary.tsv).

The raw columns include per-version reuse, incremental ratio, cumulative stored
payload/manifest/auxiliary bytes, cumulative logical bytes, version count, and
payload-only and total-object history ratios. Per-operation medians summarize
three edit positions rather than independent repeated trials. Absolute edit
sizes are constant while file sizes vary, so larger files naturally have a
higher unchanged fraction.

The generator deliberately has no compressible structure or repeated blocks
within the initial file. Real datasets and different edit patterns can produce
different ratios; this estimates the specified version-history workload.
