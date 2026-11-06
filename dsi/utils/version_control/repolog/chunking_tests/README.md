# chunking_tests — C++ port of `repolog/chunking.py`

A C++17 implementation of the FastCDC (content-defined) chunker in
`../chunking.py`. The sequential API remains byte-for-byte compatible;
`chunkFileParallel` uses the independent unit algorithm described below.

1. **SHA-256 is a library, never hand-rolled** — Apple CommonCrypto
   (`CC_SHA256`, the default on macOS) or OpenSSL (`-DUSE_OPENSSL`, portable).
2. **File-only storage** — NPY/SQLite magic detection and *all*
   SQL-database logic (`conn`, `INSERT`/`executemany`/`pending_rows`,
   `CHUNK_INSERT_BATCH_SIZE`, the `chunk_store` query) are dropped. Chunk
   payloads persist as files under `<chunk_root>/dsi_vcs_chunks/<sha256>` —
   the exact blob half of the original writer (temp file in the chunk dir,
   chmod 0644, fsync, atomic `os.replace`).

## Build & run

```sh
cd chunking_tests
make            # CommonCrypto (macOS default), builds the three C++ test binaries
make OPENSSL=1  # OpenSSL backend instead (via pkg-config openssl)
make test       # runs the bundled self-test suite
```

CLI modes (also used for cross-checking against the Python original):

```sh
./chunking_test                       # self-test suite (20 MB main file)
./chunking_test --selftest <bytes>    # self-test with a custom main-file size
./chunking_test --gen    <size> <path>
./chunking_test --dump   <fastcdc|rolling|parallel> <path> [processes]
./chunking_test --meta                  # AVERAGE_BITS, masks, gear tables
./chunking_test --store  <chunkRoot> <absPath> <relativePath>
./chunking_test --rebuild <chunkRoot> <relPath> <fileHash> <outDir> <hash,hash,...>
```

## Parallel boundary computation

```cpp
chunking::ParallelOptions options;
options.processes = 4;       // 0 => CPU count; capped at the number of units
options.requestData = true; // false => return only sizes and SHA-256 digests
auto chunks = chunking::chunkFileParallel(path, options);
```

For example, `./chunking_test --dump parallel input.bin 4` uses four workers
and prints the ordered chunk sizes and hashes.

The file is divided into consecutive units of at most `kMaxChunk` (64 MiB).
Separate OS processes read each assigned unit sequentially and compute both
FastCDC boundaries and chunk hashes. Processes reuse their buffers across
units. The parent polls all result pipes and immediately appends each complete
chunk to the producing worker's own list. Partial headers and payloads are read
incrementally, so one slow worker cannot block collection from another.
After all workers exit successfully, the parent merges their lists by file
offset. Workers process interleaved units, so concatenating lists by worker
number would produce the wrong file order.

At a fingerprint match at byte `i`, emit the current prefix **through `i`**.
If the remaining bytes `[i + 1, unitEnd]` number fewer than `kMinChunk`
(1 MiB), cancel this cut and include the remainder in that prefix, finishing
the unit. If the remainder is between `kMinChunk` and `2 * kMinChunk`
(2 MiB), inclusive, emit it separately and finish the unit. Otherwise,
reset the fingerprint and continue from `i + 1`. If no further fingerprint matches,
emit only the unexported remainder; a unit with no matches is emitted whole.
No empty chunks are emitted.

Short tails remain in their own processing unit by canceling only the most
recent cut; earlier cuts remain valid. Chunks within a unit are at most
`kMaxChunk`. A whole final unit smaller than `kMinChunk` has no local cut to
cancel, so it merges backward into the preceding unit's last chunk. This EOF
merge stays below `kMaxChunk + kMinChunk`. Files smaller than `kMinChunk`
remain one chunk.

Workers resolve local short tails before hashing. Only a whole short EOF
unit requires a merged hash recomputed by the parent. With payloads
disabled, only that merged byte range is reread. Worker buffers use
`O(processes * kMaxChunk)` memory, in addition to returned metadata. Requesting
all payloads necessarily retains the complete file in the returned vector.

This uses POSIX `fork`/`pipe`/`waitpid`; call it from a single-threaded process.
Errors close pipes and stop/reap this call's children. Results are independent
of process count but intentionally differ from the sequential Python layout.
The existing `ParallelOptions::threads` field remains an alias for process
count; `processes` takes precedence. For `storeChunksForSnapshot`, its legacy
`threads` argument selects this process path when greater than one; zero or
one still selects the sequential path. Switching layouts may therefore change
the manifest and per-file chunk hash even when file bytes are unchanged.

## Worker-owned chunk storage

`storeChunksForSnapshot(root, files, "UPDATE", 4)` uses a direct-storage path.
The parent sends the filename once to each worker, then sends only a task type,
starting byte offset, and length. Each worker opens the source itself and reads
its assigned range into its own reusable buffer. Chunk payloads never cross the
worker sockets or accumulate in the parent.

For each final chunk, a worker computes SHA-256, checks the shared existence
cache, and writes a missing object to `dsi_vcs_chunks/<sha256>`. Publication
uses a private temporary file, checked writes, `fsync`, and atomic rename.
The worker returns only the hash, byte offset, and length. The parent polls all
worker sockets and appends completed replies to separate per-worker lists.
Each available worker receives its next assigned unit immediately, without
waiting for workers processing earlier units. Once all scan tasks complete,
the parent merges those lists by file offset and resolves the EOF span. EOF
finalization replies are also collected as ready. After the workers exit
successfully, the parent builds the file manifest and per-file hash.

Workers cancel a cut that leaves a short local tail before hashing or
publishing any chunks. Only a whole short final unit and its preceding chunk
are held back for an EOF backward merge. The parent resolves that merge using
range metadata, then sends the final span back to the same worker pool for
reading, hashing, and publication. Thus no provisional tail objects are written
and the chunk layout stays independent of worker count. This rereads the EOF
span; it does not send bytes to the parent.

For example, a unit ending with a 6 MiB chunk followed by a 0.5 MiB tail
cancels the cut between them and publishes one 6.5 MiB chunk. The following
unit's chunks are unaffected.

`SharedExistenceCache` is created before `fork` using `MAP_SHARED` memory and is
reused across files in the snapshot. It contains 8,192 positive entries in
1,024 sets with eight recency-managed slots each, plus 4,096 HyperLogLog
registers. Shared memory contains fixed-size records and lock-free atomics,
without process-private container pointers. All workers can read and update it.

- A brief metadata lock protects lookups, recency changes, and slot replacement.
- A hash-striped publication lock spans lookup, disk verification, publication,
  and cache insertion. Workers finding the same hash cannot publish it twice;
  different stripes perform disk I/O concurrently.
- Positive entries become visible only after successful publication or a disk
  check. Eviction falls back to disk. HLL registers are cardinality hints and
  never prove that a blob exists; missing or invalid persisted hints cannot
  cause worker-side rewrites of existing objects.
- POSIX record locks use an inherited private lock-file descriptor. The kernel
  releases a dead worker's locks. A partially replaced cache slot remains
  invalid until its key and size have been installed.

Workers verify source identity, size, and modification/change timestamps around
reads and publication. The parent checks the open source and pathname again
after joining workers. Keep input files stable during storage; these checks
detect ordinary concurrent changes but do not provide an atomic filesystem
snapshot. Worker failures stop and reap this call's pool, and no manifest roots
are returned. Successfully published objects may remain unreferenced and can be
reused on retry. The chunk directory is synced before the final manifest root
is published; the parent saves the shared HLL sketch once after success.
Invalid older HLL ranks are discarded, and new ranks are bounded by 53.

Payload memory is `O(workers * 64 MiB)` (a final merged span can approach
66 MiB per worker). Descriptor vectors and the returned unique-hash set still
use `O(chunk count)` memory. This change removes the parent payload bottleneck;
it does not yet stream all metadata for billion-chunk files. The standalone
`chunkFileParallel(requestData=true)` API still materializes payloads for callers
that explicitly request them. The cache is shared by this call's descendants,
not unrelated store invocations; stored objects must remain immutable and must
not be deleted by concurrent garbage collection. Call from one thread per
process, including the parent before it forks.

`make test` also runs `shared_cache_test`, which checks concurrent deduplication,
independent publication, eviction, failed writes, worker death, and HLL recovery.
The chunking suite checks direct storage against its bytewise boundary oracle,
including short EOF tails, cross-unit merges, exact restore, and retry after
worker-side publication failure.

## Files

The C++ persistent manifest implementation and its split/merge example are
documented separately in [manifest_docs/README.md](manifest_docs/README.md). Run
`make manifest-test` for its focused tests. `storeChunksForSnapshot` now also
returns per-file manifest roots; legacy chunk-list restore APIs remain available.

| File          | Contents |
|---------------|----------|
| `sha256.hpp/.cpp` | SHA-256 backend: one-shot `sha256Hex` + streaming `Sha256Ctx` (non-destructive `hexdigest`, like `hashlib.sha256()`). |
| `chunking.hpp/.cpp` | Gear table, rolling hash, FastCDC boundaries, bounded-window streaming iterator, multiprocess unit chunker, file-only store/rebuild. |
| `shared_existence_cache.hpp/.cpp` | Process-shared positive cache/HLL, crash-released publication locks, atomic blob writer. |
| `shared_cache_test.cpp` | Forked concurrency, deduplication, eviction, and failure recovery tests. |
| `manifest.hpp/.cpp` | Persistent packed manifest tree with local splits/merges, immutable root publication, bounded clean-page caching, and internal-only recovery. |
| `manifest_test.cpp` | Manifest invariants, history/sharing, randomized edits, integrity checks, and storage integration tests. |
| `worker_results.hpp` / `worker_results_test.cpp` | Incremental result collection, readiness and truncated-message tests, and ordered merging of per-worker lists. |
| `main.cpp`    | Self-test suite + `--gen/--dump/--meta/--store/--rebuild` CLI modes. |
| `pycheck.py`  | Cross-check harness that runs the **unmodified** `../chunking.py` (copied into a scratch package with stubbed `.log_record` / `.content_chunking` imports) so both engines can be diffed. |
| `pystore_check.py` | Storage-half cross-check: runs the original `store_chunks_for_snapshot` / `rebuild_file_from_chunks` (SQL intact, in-memory SQLite) against the C++ file-only store, including blob-dir interop in both directions. |
| `Makefile`    | CommonCrypto default build, `OPENSSL=1` variant, `test/manifest-test/run/clean`. |

## Python → C++ mapping

| Python (chunking.py) | C++ (namespace `chunking`) |
|----------------------|-----------------------------|
| `RollingHash` | `RollingHash` (128-bit math, non-negative reductions) |
| `GEAR_TABLE` / `GEAR_TABLE_LS` | `kGearTable` / 128-bit `kGearTableLs` (keeps Python's bit 64) |
| `_make_spread_mask`, `_validate_parameters`, `AVERAGE_BITS`, `STRICT_MASK`, `EAGER_MASK` | `makeSpreadMask`, `validateParams`, `kStrictMask`, `kEagerMask` (checked at load time, as Python checks at import) |
| `_find_fastcdc_cut` | `findFastCdcCut` (two-region STRICT/EAGER scan, shifted Gear table, pair stride 2) |
| `iter_file_fastcdc` | `FastCdcChunkIterator` / `chunkFileStream` |
| `chunk_file_fastcdc` / `chunk_file` | `chunkFileFastCdc` / `chunkFile` |
| `chunk_file_rolling_hash` | `chunkFileRollingHash` |
| `Chunk` dict `{sha256, size, data}` | `struct Chunk` (`data` is `nullopt` instead of `None`) |
| `needs_data` predicate | `NeedsData` (`std::function<bool(const std::string&)>`) |
| `store_chunks_for_snapshot` | `storeChunksForSnapshot` (SQL removed; `pendingCommit` kept for parity, unused) |
| `rebuild_file_from_chunks` | `rebuildFileFromChunks` (ordered chunk-hash list replaces the DB rows; verifies the order reproduces `fileHash`) |
| existence cache (`OrderdCB`, 8192) | `ExistenceCache` — HyperLogLog register probe (zero ⇒ not exist) → LRU of disk-verified answers (cap `CHUNK_EXISTENCE_CACHE_SIZE`) → stat() on LRU miss |
| `removed` | `_has_npy_magic`, `is_sqlite_file`, magic constants, `CHUNK_INSERT_BATCH_SIZE` |
| *new* | `chunkFileParallel` — independent unit boundaries and hashing in OS processes; `hashRangesParallel` — thread-based hashing of existing in-memory ranges |

Dropped per the task: everything touching the SQL database and the
NPY/SQLite magic sniffing (`CHUNK_STORAGE_DIR = "dsi_vcs_chunks"` and
`CHUNK_EXISTENCE_CACHE_SIZE = 8192` are kept; `NORMALIZATION_LEVEL = 2`).

## Equivalence details

* `_find_fastcdc_cut`'s unmasked intermediate fingerprint in Python is
  `(fingerprint << 2) + GEAR_TABLE_LS[b]` — up to 67 bits before the
  pair-completion masks back to 64. The port carries that intermediate in
  `unsigned __int128` (and the LS table / shifted mask in 128 bits) so
  boundaries are selected the same way on the same bytes.
* Rolling-hash reductions follow Python's `%` (result in `[0, MOD)`).
* `AVG_CHUNK = 8 MB` ⇒ `AVERAGE_BITS = 23`, `STRICT = spread(25)`,
  `EAGER = spread(21)`.
* Per-file hash = SHA-256 over the ordered concatenation of the chunks'
  lowercase-hex digests (identical to the Python comment/behaviour).

## Cross-checking against the Python original

`chunking.py` imports its sibling modules relatively, so `pycheck.py` builds
a scratch package in `$TMPDIR` with stubbed `.log_record` and
`.content_chunking` modules and loads a copy of the original file unleashed:

```sh
size=$(( 20*1024*1024 + 4567 ))
./chunking_test --gen $size /tmp/cdc.bin
python3 pycheck.py fastcdc /tmp/cdc.bin > /tmp/py.txt
./chunking_test --dump fastcdc /tmp/cdc.bin > /tmp/cc.txt
diff /tmp/py.txt /tmp/cc.txt && echo "identical chunking"

python3 pycheck.py meta   > /tmp/pym.txt   # gear tables / masks
./chunking_test --meta    > /tmp/ccm.txt
diff /tmp/pym.txt /tmp/ccm.txt && echo "identical tables"
```

(Any size works; small files exercise the `<= MIN_CHUNK` fast paths,
20 MB+ files cross the strict and eager normalization zones.)

## Verifying the storage half against the Python original

`pystore_check.py` runs the *real* Python store/rebuild (its SQL side intact,
against in-memory SQLite) and diff-checks it against the C++ file-only store:

```sh
make
python3 pystore_check.py /tmp/cdc_main.bin data/bin/main.bin
```

It confirms the per-file hash, chunk counts, unique chunk sets, every SQL
row's `chunk_hash`/`chunk_index`, and — strongest of all — that each
implementation can rebuild the original file from the *other*
implementation's `dsi_vcs_chunks/` blob directory byte-for-byte.
