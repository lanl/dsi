# C++ persistent file manifest

`../manifest.hpp` and `../manifest.cpp` implement an ordered tree of chunk
references. `../chunking.cpp` stores payloads, returns a manifest root for each
file version, and can reconstruct a file by streaming that root. No SQL table or
commit-ancestor search is required. Python files and their storage format are
unchanged.

## Objects and versions

```text
chunk_root/
  dsi_vcs_chunks/<chunk SHA-256>        actual file bytes
  dsi_vcs_manifests_cpp/<page SHA-256>  packed manifest objects

file-version metadata -> manifest root -> internal pages -> leaf pages
                                                            |
                                                  ordered chunk references
```

A leaf stores `(SHA-256, byte length)` references in file order. An internal
page stores child hashes, subtree chunk counts, subtree byte counts, and minimum
and maximum leaf depths. Positions are derived from subtree counts; inserting a
chunk does not renumber or rewrite subsequent leaves.

Objects are immutable. A working edit copies the affected leaf and ancestor
path. `snapshot()` publishes dirty descendants before their parents and returns
the root hash. Unchanged pages remain shared across versions and branches. The
caller must durably record the returned root in its own version/Merkle metadata;
this standalone C++ implementation does not modify the Python database.

## Page rules

Let `B = MANIFEST_PAGE_ENTRIES`, default 4,096. Options allow 4 through 65,536.

| Rule | Leaves | Internal pages |
| --- | --- | --- |
| Minimum non-root occupancy | `ceil(B/2)` chunk references | `ceil(B/2)` child references |
| Split threshold | `2B` references | `2B` children |
| Split result | Two consecutive halves | Two consecutive halves |

Insert a reference into its existing leaf. Split only when the threshold is
reached, and propagate any resulting parent split toward the root. Root splitting
creates another level.

After deletion, a page below minimum occupancy always combines with its left
sibling if available, otherwise its right sibling. Siblings share a parent. If
the combined page has fewer than `2B` entries, replace the pair with one page.
Otherwise, split the combined sequence into two evenly sized pages. Repair an
underfull parent recursively using the same rules. Promoting an internal root's
only child shortens the entire tree; an empty file uses a canonical empty leaf.

Every successful `insert`, `erase`, and `update` performs a depth check after
local repairs. If `root.maxDepth - root.minDepth` exceeds
`ManifestOptions::maxDepthDifference` (default 2), rebuild internal levels while
retaining existing leaf pages. Correct local edits preserve equal leaf depths,
so this safeguard does not normally trigger. Recovery costs `O(P)` leaf-page
references and auxiliary memory for `P` leaves, rather than repacking all `C`
chunk references. Cached depth metadata identifies leaves without reading their
objects; only internal objects are read and rebuilt. `rebalanceInternal()` exposes this operation
for maintenance. It assumes existing leaves already meet occupancy requirements.

## Example: insertion and consecutive deletion

Use `B = 4` for this example: merge below 2 entries and split at 8.

```text
Version 1:
R1 -> P1[A B C D], P2[E F G H], P3[I J K L]

Insert X after A:
R2 -> P4[A X B C D], P2[E F G H], P3[I J K L]
                       shared          shared
```

Only `P4` and the root are new objects. If the first leaf reaches eight entries,
it becomes two leaves of four; subsequent leaves still retain their hashes.

Starting again from version 1, delete E, F, then G. Each deletion receives its
own repair and balance check. P2 reaches one entry `[H]`, so it combines with its
left sibling despite also having a right sibling:

```text
R3 -> P5[A B C D H], P3[I J K L]
                         shared
```

If the left sibling instead had seven entries, combining it with `[H]` would
reach eight entries and immediately produce two pages of four. The parent loses
a child only when the pair becomes one page. Old roots R1 and R2 remain readable.

## API

```cpp
#include "manifest.hpp"

chunking::FileManifest tree(chunkRoot); // Initially empty.
tree.insert(0, {chunkHashA, sizeA});      // Payload must be stored separately.
tree.insert(1, {chunkHashB, sizeB});
const auto version1 = tree.snapshot();

chunking::FileManifest next(chunkRoot, {}, version1);
next.insert(1, {chunkHashX, sizeX});
next.update(0, {chunkHashY, sizeY});
next.erase(2);
const auto version2 = next.snapshot();

// The previous version still yields A, B.
chunking::FileManifest previous(chunkRoot, {}, version1);
previous.forEach([](const chunking::ManifestChunk& chunk) {
    // Read chunk.hash from the payload store and stream chunk.size bytes.
});
```

Indexes are zero-based. Insertion allows `index == chunkCount`; lookup, deletion,
and replacement require `index < chunkCount`. Chunk sizes must be positive and
hashes must be 64 lowercase hexadecimal characters. Aggregate count overflow is
rejected. Reopening a root requires the same `pageEntries` used to create it.

`forEach` streams the references. `validate()` performs an expensive full-tree
occupancy/depth/count verification intended for tests and diagnostics. Normal
operations use cached subtree summaries rather than a full validation scan.

## Chunk-store integration and CLI

`StoreResult::manifestRoots` maps each relative file path to its new root.
Pass the previous map as the fifth argument to `storeChunksForSnapshot` to edit
existing manifests:

```cpp
auto first = chunking::storeChunksForSnapshot(chunkRoot, entries);
auto next = chunking::storeChunksForSnapshot(
    chunkRoot, entries, "UPDATE", 0, first.manifestRoots);
bool restored = chunking::rebuildFileFromManifest(
    chunkRoot, relativePath, next.manifestRoots.at(relativePath),
    next.fileHashes.at(relativePath), outputDirectory);
```

Snapshotting re-chunks the input and matches old/new references with a bounded
64-reference lookahead. Nearby insertions and deletions reuse matching suffix
pages. Ambiguous repeats or changes beyond that window can select a different
edit sequence, reducing page sharing; reconstructed bytes remain correct.
Known-position `FileManifest` edits provide the local-update guarantee directly.
Automatic discovery of arbitrary edits is not a logarithmic operation.

Dirty pages are coalesced between snapshots. The streaming store checkpoints
after approximately `2B` edits to bound dirty metadata, and uses a bounded LRU
for clean pages. These checkpoints may leave unreferenced intermediate objects;
garbage collection is not implemented. The existing `chunkHashes` result still
retains distinct payload hashes, and the existing parallel chunker materializes
its results; those legacy APIs are not constant-memory at billion-chunk scale.

From `chunking_tests/`:

```sh
make
./chunking_test --store-manifest /tmp/repo /absolute/path/data.bin data.bin
# Prints three lines: manifest root, file hash, chunk count.
# Supply a previous root as the final argument for the next version:
./chunking_test --store-manifest /tmp/repo /absolute/path/data.bin data.bin PREVIOUS_ROOT
./chunking_test --rebuild-manifest /tmp/repo data.bin ROOT FILE_HASH /tmp/restored
make manifest-test
make test
```

The legacy `--store` output and `--rebuild` chunk-list mode remain available.

## Binary format and integrity

Pages use big-endian integers and the version magic `DSIMCP01`:

| Component | Fields | Bytes |
| --- | --- | --- |
| Header | Magic, B, kind, entry count, chunk count, byte count, min depth, max depth | 41 |
| Leaf entry | Raw SHA-256, byte length | 40 |
| Internal entry | Raw child SHA-256, chunk count, byte count, min depth, max depth | 56 |

Each individual object file is named by the SHA-256 of its complete contents.
The format has its own directory and is not interchangeable with Python's
`DSIMAN01` format. These are packed page objects, not a Git-style container of
multiple objects in one packfile.

Publication uses a temporary file, file fsync, atomic rename, and directory
fsync. Readers verify object checksums, lengths, format, capacity, and aggregate
metadata. Restoration checks the ordered-reference file hash and each payload's
size and checksum. Output is staged in a temporary file and published only after
verification, so a missing/corrupt payload does not overwrite an existing file.
Retain all objects reachable from historical roots; removal from a new manifest does not
delete an object.

For a known edit location and tree height `h`, edits touch `O(h)` pages, with
`O(Bh)` encoding/hashing work when published. Restore visits `O(C)` references
and reads the payload bytes. Identical content reached through different edit
histories can have different manifest tree shapes and root hashes; the separate
ordered-chunk file hash still identifies the same chunk sequence.

## Tests

`manifest_test.cpp` exercises boundary splits, cascading internal splits and
merges, root collapse, canonical emptiness, left sibling preference, right
fallback, merge-then-split redistribution, unchanged suffix sharing, divergent
versions, persistence/reopening, odd and even capacities, overflow, malformed
and missing objects, and historical file restoration. It also compares 12,000
deterministic random edits with a vector model and verifies the per-operation
balance-check count.
