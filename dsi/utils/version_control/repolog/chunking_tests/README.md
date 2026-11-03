# chunking_tests — C++ port of `repolog/chunking.py`

A byte-for-byte equivalent C++17 implementation of the FastCDC (content-
defined) chunker in `../chunking.py`, with two changes required by the task:

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
make            # CommonCrypto (macOS default), builds ./chunking_test
make OPENSSL=1  # OpenSSL backend instead (via pkg-config openssl)
make test       # runs the bundled self-test suite
```

CLI modes (also used for cross-checking against the Python original):

```sh
./chunking_test                       # self-test suite (20 MB main file)
./chunking_test --selftest <bytes>    # self-test with a custom main-file size
./chunking_test --gen    <size> <path>
./chunking_test --dump   <fastcdc|rolling> <path>
./chunking_test --meta                  # AVERAGE_BITS, masks, gear tables
./chunking_test --store  <chunkRoot> <absPath> <relativePath>
./chunking_test --rebuild <chunkRoot> <relPath> <fileHash> <outDir> <hash,hash,...>
```

## Files

| File          | Contents |
|---------------|----------|
| `sha256.hpp/.cpp` | SHA-256 backend: one-shot `sha256Hex` + streaming `Sha256Ctx` (non-destructive `hexdigest`, like `hashlib.sha256()`). |
| `chunking.hpp/.cpp` | The port: gear table, rolling hash, FastCDC boundaries, bounded-window streaming iterator, parallel hashing pipeline, file-only store/rebuild. |
| `main.cpp`    | Self-test suite + `--gen/--dump/--meta/--store/--rebuild` CLI modes. |
| `pycheck.py`  | Cross-check harness that runs the **unmodified** `../chunking.py` (copied into a scratch package with stubbed `.log_record` / `.content_chunking` imports) so both engines can be diffed. |
| `pystore_check.py` | Storage-half cross-check: runs the original `store_chunks_for_snapshot` / `rebuild_file_from_chunks` (SQL intact, in-memory SQLite) against the C++ file-only store, including blob-dir interop in both directions. |
| `Makefile`    | CommonCrypto default Build, `OPENSSL=1` variant, `test/run/clean`. |

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
| *new* | `chunkFileParallel` / `hashRangesParallel` — same digests computed on a thread pool |

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
