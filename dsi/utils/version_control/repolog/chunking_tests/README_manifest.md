# DSI-VCS C++ persistent manifests

`manifest.hpp` and `manifest.cpp` implement an immutable ordered tree of chunk
references for one file version. Payload bytes live in DSI-VCS packs;
`chunking.cpp` owns pack publication, snapshot reconciliation, reconstruction,
and repacking.

## Manifest and pack relationship

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

## On-disk objects and manifest-v2 format

```text
chunk_root/
  dsi_vcs_packs/index
  dsi_vcs_packs/packs/<pack-id>.pack
  dsi_vcs_manifests/<page-content-sha256>
```

Manifest pages are immutable, content-addressed objects named by SHA-256 of the
complete encoded page. New pages use big-endian format magic `DSIMCP02`:

| Component | Fields | Bytes |
| --- | --- | ---: |
| Header | magic, `B`, kind, entry count, subtree chunk count, byte count, min/max depth | 41 |
| Leaf entry | chunk SHA-256, logical length, pack SHA-256 ID, offset, stored length | 88 |
| Internal entry | child SHA-256, subtree chunk count, byte count, min/max depth | 56 |

`stored length` currently equals logical length because payloads are not
compressed. Keeping it explicit permits independent validation of the physical
extent and leaves room for a future encoding.

## Page rules and immutable updates

`ManifestOptions::pageEntries` defaults to 4,096 and permits 4 through 65,536.

| Rule | Leaves | Internal pages |
| --- | --- | --- |
| Minimum non-root occupancy | `ceil(B / 2)` chunk references | `ceil(B / 2)` child references |
| Generic-edit split threshold | `2B` references | `2B` children |
| Split result | two consecutive halves | two consecutive halves |

The common initial-import/pure-append path buffers one leaf and seals it at `B`
entries. At snapshot, an underfull trailing leaf is balanced with its
predecessor; only new leaves and a compact upper hierarchy are written.

Known-position `insert`, `update`, and `erase` use copy-on-write. They copy the
affected leaf and ancestor path, retain untouched page hashes, split at the
generic threshold, and merge/redistribute below minimum occupancy. An internal
root with one child collapses. A depth-difference safeguard can rebuild internal
levels while retaining leaves.

Automatic snapshot reconciliation rechunks the complete file and uses bounded
64-entry lookahead to match old/new reference sequences. Local edits normally
retain a suffix of old pages. Repeated patterns or changes outside that window
can reduce sharing but cannot change reconstructed bytes.

## Roots and reconstruction

```cpp
chunking::FileManifest manifest(chunkRoot);
manifest.append({chunkHash, logicalBytes, packId, packOffset, storedBytes});
const std::string root = manifest.snapshot();

chunking::FileManifest next(chunkRoot, {}, root);
next.insert(index, replacement);
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

## GC/repacking contract

`garbageCollectPacks(chunkRoot, liveManifestRoots)` walks every supplied live
manifest, copies each distinct reachable payload once into a fresh pack, swaps
the digest index, and returns replacement manifest roots. The caller must
atomically replace corresponding version roots with those results.

GC does not delete old packs. Safe deletion needs repository-wide reachability,
completed metadata migration, and no readers using old roots. Provide every
retained root if future global deduplication must recognize its payloads. The
current implementation creates at most one replacement pack per call.

## Complexity and assumptions

- Appending `C` references creates `O(C / B)` leaves and a small fan-out `B`
  internal hierarchy.
- A known-position edit in height `h` touches `O(h)` pages and has `O(Bh)`
  encoding/hashing work when published.
- Reconstruction visits `O(C)` references and reads reconstructed bytes.
- Snapshotting retains logical chunk metadata; parallel payload buffering stays
  bounded even though metadata remains `O(chunk count)`.
- Input files must not change while chunked. Source checks detect ordinary
  concurrent changes but do not make an atomic filesystem snapshot.

## Tests

`manifest_test.cpp` covers split/merge behavior, root collapse, persistence,
randomized edits, malformed pages, packed locations, GC integration, and
historical restoration.

```sh
make manifest-test
```
