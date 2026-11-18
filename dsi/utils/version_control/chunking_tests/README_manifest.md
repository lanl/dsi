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

## Roots and reconstruction

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

## GC/repacking contract

`garbageCollectPacks(chunkRoot, liveManifestRoots)` walks every supplied live
manifest, copies each distinct reachable payload once into a fresh pack, swaps
the digest index, and returns replacement manifest roots. The caller must
atomically replace corresponding version roots with those results.

GC does not delete old packs. Safe deletion needs repository-wide reachability,
completed metadata migration, and no readers using old roots. Provide every
retained root if future global deduplication must recognize its payloads. The
current implementation creates at most one replacement pack per call.

### Unreferenced manifest-page GC

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

## Complexity and assumptions

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

## Tests

`manifest_test.cpp` covers split/merge behavior, root collapse, persistence,
randomized edits, bulk replacement/leaf reuse, manifest-page mark-and-sweep,
malformed pages, packed locations, pack-GC integration, and historical
restoration.

```sh
make manifest-test
```
