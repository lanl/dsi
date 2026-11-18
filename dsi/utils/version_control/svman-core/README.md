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

`manifest.hpp` and `manifest.cpp` implement an immutable ordered tree of chunk
references for one file version. Payload bytes live in svman packs;
`chunking.cpp` owns pack publication, snapshot reconciliation, reconstruction,
and repacking.

| File                                                 | Responsibility                                   |
| ---------------------------------------------------- | ------------------------------------------------ |
| `chunking.hpp/.cpp`                                | FastCDC, pack transactions, snapshot/rebuild, GC |
| `manifest.hpp/.cpp`                                | Immutable manifest pages and tree updates        |
| `shared_existence_cache.hpp/.cpp`                  | Shared positive cache and HLL hints              |
| `sha256.hpp/.cpp`                                  | CommonCrypto/OpenSSL SHA-256 wrapper             |
| `main.cpp`                                         | CLI and integration self-tests                   |
| `manifest_test.cpp`                                | Tree, storage, GC, and restoration tests         |
| `shared_cache_test.cpp`                            | Cache, concurrency, and recovery tests           |
| `worker_results.hpp` / `worker_results_test.cpp` | Worker-result collection/tests                   |

### Manifest and pack relationship

```text
version metadata
  -> manifest root page -> internal pages -> leaf pages
  -> ordered chunk references
       (digest, logical size, pack ID, offset, stored length)
  -> immutable packfiles
```

A manifest leaf is a logical page, not a payload container. It can refer to
chunks from several packs, and the same pack can serve leaves in multiple files
and versions. There is no one-leaf/one-pack rule.

For default page target `B = 4,096`, a new sequential import seals an append
leaf every `B` chunk references. More than `B` chunks therefore gives a file
multiple leaves even if one process stored every payload in exactly one pack.
Conversely, a small edit can create a new small pack without increasing leaf
count.

### On-disk objects and manifest format

```text
chunk_root/
  dsi_vcs_packs/index
  dsi_vcs_packs/packs/<pack-id>.pack
  dsi_vcs_manifests/<page-content-sha256>
```

Manifest pages are immutable, content-addressed objects named by SHA-256 of the
complete encoded page. Pages use big-endian format magic `DSIMCP02`:

| Component      | Fields                                                                          | Bytes |
| -------------- | ------------------------------------------------------------------------------- | ----: |
| Header         | magic, `B`, kind, entry count, subtree chunk count, byte count, min/max depth |    41 |
| Leaf entry     | chunk SHA-256, logical length, pack SHA-256 ID, offset, stored length          |    88 |
| Internal entry | child SHA-256, subtree chunk count, byte count, min/max depth                  |    56 |

`stored length` currently equals logical length because payloads are not
compressed. Keeping it explicit permits independent validation of the physical
extent and leaves room for a future encoding.

### Page rules and immutable updates

`ManifestOptions::pageEntries` defaults to 4,096 and permits 4 through 65,536.

| Rule                         | Leaves                           | Internal pages                   |
| ---------------------------- | -------------------------------- | -------------------------------- |
| Minimum non-root occupancy   | `ceil(B / 2)` chunk references | `ceil(B / 2)` child references |
| Generic-edit split threshold | `2B` references                | `2B` children                  |
| Split result                 | two consecutive halves           | two consecutive halves           |

The common initial-import/pure-append path buffers one leaf and seals it at `B`
entries. At snapshot, an underfull trailing leaf is balanced with its
predecessor; only new leaves and a compact upper hierarchy are written.

Known-position `insert`, `update`, and `erase` use copy-on-write. They copy the
affected leaf and ancestor path, retain untouched page hashes, split at the
generic threshold, and merge/redistribute below minimum occupancy. An internal
root with one child collapses. A depth-difference safeguard can rebuild internal
levels while retaining leaves.

`replaceRange(index, eraseCount, replacement)` is the bulk update operation used
by automatic snapshot reconciliation. It reuses complete immutable leaves before
and after the changed range, rebuilds only the edited boundary region, then
rebuilds the compact internal hierarchy. If a rebuilt boundary would be
underfull, it locally incorporates one adjacent leaf and repartitions that small
region to preserve occupancy.

Automatic snapshot reconciliation still rechunks the complete input file, but
it no longer uses bounded lookahead. It builds a digest-to-old-position anchor
index, confirms an anchor with up to three consecutive matching chunks, and
uses `replaceRange` for each unmatched span. Consequently a 128 MiB insertion
or deletion can rejoin a reused suffix beyond the old 64-chunk window without
performing one copy-on-write manifest edit per shifted chunk. If no confirmed
anchor exists, the remaining suffix is replaced as one bulk span.

### Roots and reconstruction

```cpp
chunking::FileManifest manifest(chunkRoot);
manifest.append({chunkHash, logicalBytes, packId, packOffset, storedBytes});
const std::string root = manifest.snapshot();

chunking::FileManifest next(chunkRoot, {}, root);
next.replaceRange(index, eraseCount, replacementChunks);
const std::string nextRoot = next.snapshot();
```

Indexes are zero-based. Insertion permits `index == chunkCount`; lookup,
replacement, and deletion require `index < chunkCount`. Logical sizes must be
positive and hashes must be lowercase 64-hex-character SHA-256 values.

`snapshot()` writes dirty children before parents and returns a root hash. The
application must atomically attach that root to version metadata. Historical
roots remain readable while their pages and referenced packs are retained.

`forEach()` streams entries in file order. `validate()` is an expensive full-tree
occupancy/depth/count diagnostic; normal operations use cached summaries.

`rebuildFileFromManifest` streams each referenced pack range, validates its
length and digest, verifies the ordered chunk-digest file hash, then atomically
publishes the reconstructed output. The file hash is SHA-256 over ordered
lowercase-hex chunk digests, not original-file bytes.

### Manifest GC and repacking

`garbageCollectPacks(chunkRoot, liveManifestRoots)` walks every supplied live
manifest, copies each distinct reachable payload once into a fresh pack, swaps
the digest index, and returns replacement manifest roots. The caller must
atomically replace corresponding version roots with those results.

GC does not delete old packs. Safe deletion needs repository-wide reachability,
completed metadata migration, and no readers using old roots. Provide every
retained root if future global deduplication must recognize its payloads. The
current implementation creates at most one replacement pack per call.

#### Unreferenced manifest-page GC

`FileManifest::garbageCollectPages(chunkRoot, liveRoots, options)` performs
mark-and-sweep collection of manifest objects. It traverses every page reachable
from each supplied root hash, marks those content-addressed page objects, then
deletes only 64-hex-name page files not in the mark set. Shared leaves and
internal pages remain because they are marked once and retained for every root
that reaches them. Temporary/non-content-addressed files are ignored.

```cpp
const chunking::ManifestGcResult gc =
    chunking::FileManifest::garbageCollectPages(chunkRoot, retainedRoots);
// gc.reachablePages and gc.deletedPages report the completed pass.
```

`liveRoots` must contain **every** retained version/branch root, and `options`
must match the pages' `ManifestOptions` (the default is correct for default
pages). This is intentionally an explicit maintenance operation: manifest pages
cannot discover root references held in external version metadata. Run it only
while manifest publishers and version-root metadata updates are quiescent;
otherwise a newly published but not-yet-registered root could be collected.

### Manifest complexity and assumptions

- Appending `C` references creates `O(C / B)` leaves and a small fan-out `B`
  internal hierarchy.
- A known-position edit in height `h` touches `O(h)` pages and has `O(Bh)`
  encoding/hashing work when published. A bulk replacement additionally copies
  its replacement references and a bounded number of boundary leaves, while
  retaining all complete outside leaves by hash.
- Reconstruction visits `O(C)` references and reads reconstructed bytes.
- Snapshotting retains logical chunk metadata; parallel payload buffering stays
  bounded even though metadata remains `O(chunk count)`.
- Input files must not change while chunked. Source checks detect ordinary
  concurrent changes but do not make an atomic filesystem snapshot.

### Manifest tests

`manifest_test.cpp` covers split/merge behavior, root collapse, persistence,
randomized edits, bulk replacement/leaf reuse, manifest-page mark-and-sweep,
malformed pages, packed locations, pack-GC integration, and historical
restoration.

```sh
make manifest-test
```
