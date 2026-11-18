# `svman-core` — C++ FastCDC, packed storage, and manifests

**svman:** **Secured Version MANager**.

This directory contains the C++17 svman implementation of FastCDC file
chunking, SHA-256 addressing, repository-wide payload deduplication, immutable
packfiles, and persistent per-file manifests. SQL and Python storage/database
behavior are outside this implementation.

The sequential chunker follows the Python FastCDC boundary rules. The parallel
API uses independent `kMaxChunk` work units, so it is deterministic for a given
process count but can produce a different layout than sequential chunking.

## Build and test

```sh
cd svman-core
make            # CommonCrypto on macOS
make OPENSSL=1  # OpenSSL backend
make test       # chunking, manifest, cache, and worker-result tests
```

Useful CLI modes:

```sh
./svman-core --selftest <bytes>
./svman-core --gen <size> <path>
./svman-core --dump <fastcdc|rolling|parallel> <path> [processes]
./svman-core --meta
./svman-core --store-manifest <root> <absolute-path> <relative-path> [base-root]
./svman-core --rebuild-manifest <root> <relative-path> <root-hash> <file-hash> <out-dir>
```

## Storage model

```text
chunk_root/
  dsi_vcs_packs/
    index                         digest -> pack id, byte offset, length
    index.lock                    short-lived publication lock
    packs/<pack-id>.pack          immutable payload packfiles
  dsi_vcs_manifests/<page-hash>   immutable manifest pages

file-version metadata
  -> manifest root -> internal pages -> leaf pages
  -> ordered chunk references
  -> byte ranges in immutable packfiles
```

Payload bytes are appended to temporary packs rather than stored one file per
chunk. A manifest reference records the chunk digest, logical size, pack ID,
byte offset, and stored length. The global digest index is the authority for
payload deduplication; manifests retain file order and reconstruction locations.

### Pack decisions

- Published packs are immutable; there is no configured pack-size limit.
- A one-process store creates at most one pack for new payloads, regardless of
  file size. A 50 GB initial import can create one 50 GB pack.
- A later store never appends to that published pack. If an edit has globally
  new chunks, it creates a new pack containing only those chunks and nearby
  FastCDC-boundary fallout—not another full-file copy.
- A store with no globally new chunks publishes no pack.
- With `P > 1` configured store workers, a call allocates up to `P` independent
  temporary pack slots. New digests are assigned round-robin; only non-empty
  slots that win at least one digest publish.
- Independent snapshot writers never append to the same pack. Their short,
  locked index transactions resolve same-digest races globally.

Pack count and manifest-leaf count are deliberately unrelated. A leaf can
reference many packs, and one pack can be referenced by many leaves, files, and
versions.

## Snapshot publication and crash behavior

`storeChunksForSnapshot`:

1. Chunks input files and computes digests.
2. Resolves known digests from the repository index; stages only unknown
   payloads in writer-owned temporary packs.
3. Seals temporary packs, acquires the short index lock, merges the index,
   publishes only winning packs, and atomically replaces the index.
4. Publishes immutable manifest pages that refer to resolved pack locations and
   returns their root hashes.
5. Inserts positive `ExistenceCache` entries only after the pack/index
   transaction and all referring manifests have published.

Readers ignore temporary pack names. A crash before publication therefore
exposes no partial payload. A crash after the index transaction but before the
application persists returned roots can leave a safe, unreferenced pack/index
entry; later compaction can reclaim it.

The caller owns version metadata. It must atomically associate returned
`StoreResult::manifestRoots` with its new application snapshot; this library
does not update a database or commit graph. Input files must remain stable while
being stored; source checks detect ordinary changes but are not a filesystem
snapshot mechanism.

## Public snapshot API

```cpp
chunking::FileEntry file{inputPath, "data/input.bin", "file"};
auto first = chunking::storeChunksForSnapshot(repositoryRoot, {file});

auto next = chunking::storeChunksForSnapshot(
    repositoryRoot, {file}, "UPDATE", 1, first.manifestRoots);

bool restored = chunking::rebuildFileFromManifest(
    repositoryRoot, file.relativePath,
    next.manifestRoots.at(file.relativePath),
    next.fileHashes.at(file.relativePath), outputDirectory);
```

`StoreResult::fileHashes` is SHA-256 over the ordered lowercase-hex chunk
digests, not over original file bytes. Reconstruction verifies every payload
range's digest and length and then verifies this ordered file hash before
atomically publishing output.

Existing manifests are reconciled using bounded 64-reference lookahead. Nearby
inserts/deletes normally retain matching suffix pages; ambiguous repeated chunk
sequences can reduce page sharing but never change reconstructed bytes.

## Parallel chunking and memory ownership

```cpp
chunking::ParallelOptions options;
options.processes = 4;       // 0: CPU count; capped at available work units
options.requestData = false; // metadata only
auto chunks = chunking::chunkFileParallel(path, options);
```

Files are partitioned into units no larger than `kMaxChunk` (128 KiB). POSIX
workers scan and hash units; the parent collects replies as ready and merges
them by file offset. Only a whole final unit shorter than `kMinChunk` (8 KiB)
merges backward at EOF.

`chunkFileParallel(..., requestData = true)` materializes payloads for callers
that request them. Snapshot storage avoids that complete-file copy: parallel
workers return metadata and the parent rereads only bounded ranges for genuinely
new payloads before packing. Metadata remains `O(chunk count)`.

`storeChunksForSnapshot(..., threads)` uses the same process-count convention:
`0` or `1` is sequential; values above one select independent-unit chunking.
Changing this setting can change boundaries, manifests, and ordered chunk hashes
for identical input bytes.

## Existence cache and global deduplication

`SharedExistenceCache` is a process-shared positive cache plus HyperLogLog
hinting for one store call and its descendants. It improves lookup cost; the
repository index remains the global authority.

- The cache is populated only after pack/index and manifest publication.
- A cache miss falls back to the repository index.
- HyperLogLog is a hint and never proves payload existence.
- Global deduplication wins over pack locality: duplicate payloads are not
  copied merely to keep a worker or manifest leaf pack-local.

## GC and repacking

Compaction is explicit:

```cpp
auto gc = chunking::garbageCollectPacks(repositoryRoot, allLiveManifestRoots);
// Atomically replace corresponding application version roots with
// gc.manifestRoots before considering old packs for deletion.
```

GC walks supplied live roots, copies each distinct reachable payload once into
one fresh immutable pack, replaces the repository index, and returns replacement
manifest roots. Existing packs are intentionally retained. Physical deletion
requires repository-wide reachability, completed metadata migration, and reader
coordination.

`allLiveManifestRoots` must include every retained version/branch intended to
participate in future global deduplication. Supplying only a subset leaves other
roots physically reconstructible but removes their hashes from the compacted
index, allowing future stores to duplicate their payloads. The current GC writes
at most one replacement pack per call and does not delete old packs.

## Manifest details and key files

See [README_manifest.md](README_manifest.md) for manifest-v2 format, page
rules, copy-on-write behavior, and leaf/pack independence.

| File                                                 | Responsibility                                   |
| ---------------------------------------------------- | ------------------------------------------------ |
| `chunking.hpp/.cpp`                                | FastCDC, pack transactions, snapshot/rebuild, GC |
| `manifest.hpp/.cpp`                                | Immutable manifest-v2 pages and tree updates     |
| `shared_existence_cache.hpp/.cpp`                  | Shared positive cache and HLL hints              |
| `sha256.hpp/.cpp`                                  | CommonCrypto/OpenSSL SHA-256 wrapper             |
| `main.cpp`                                         | CLI and integration self-tests                   |
| `manifest_test.cpp`                                | Tree, storage, GC, and restoration tests         |
| `shared_cache_test.cpp`                            | Cache, concurrency, and recovery tests           |
| `worker_results.hpp` / `worker_results_test.cpp` | Worker-result collection/tests                   |
