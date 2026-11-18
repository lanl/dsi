# svman: parallel chunking and file manifests

**Worker assignment.** The native parallel store divides a nonempty file into consecutive **64 MiB units**, with a possibly shorter final unit. Unit `u` starts at byte `u × 64 MiB`. With `P` workers, worker `w` handles units `w, w+P, w+2P, …`; two workers therefore receive even- and odd-numbered units. Worker count is capped by unit count. Each worker receives the filename once, then offsets and lengths, reading into a private reusable buffer. The parent dispatches its next assigned unit as soon as it replies, without waiting for earlier units.

**Parallel boundary detection.** Workers scan independent units concurrently; each unit's scan is sequential. For each chunk, FastCDC skips the first **1 MiB**, applies a stricter Gear-fingerprint cut test before **8 MiB**, then a more permissive test up to the unit end. A cut includes the matching byte. A leftover tail below 1 MiB cancels that cut; a remaining 1–2 MiB tail becomes one chunk. The current worker collects its unit's boundaries before hashing and storing its chunks. Fixed units make the parallel layout independent of worker count, although it differs from sequential chunking.

**Existence cache and storage.** Workers compute SHA-256 and consult a shared-memory cache of **8,192 verified hash/size entries**. Hits reuse existing blobs; misses check disk before writing. A short metadata lock protects cache entries, and hash-striped locks coordinate lookup through publication so competing workers do not write the same chunk twice. Missing blobs are published through a temporary file, fsync and atomic rename, then cached. Workers return only **hashes, offsets and lengths**; chunk bytes stay outside the parent.

**HyperLogLog.** A shared sketch of **4,096 one-byte registers** estimates distinct digests recorded, using atomic updates. It is loaded at store startup and saved afterward on a best-effort basis. It cannot prove existence: parallel cache misses still check disk. It does not measure stored bytes.

The parent orders worker records and builds the manifest. If a preceding unit exists, a final unit below 1 MiB is merged backward; a worker rereads, hashes and publishes the merged span before its references are finalized.

**File manifests.** The caller records each version's returned immutable manifest root. Leaves contain ordered `(chunk hash, byte length)` references; internal pages contain child hashes and subtree counts. Edits copy affected pages and ancestors, sharing unchanged pages across versions. With the default settings, leaves split at **8,192 entries**; non-root leaves combine with a sibling below **2,048 entries**, redistributing if necessary.

For example, if an edit produces this chunk sequence:

```text
Version 1: root R1 → [A, B, C]
Version 2: root R2 → [A, X, B, C]
```

The existing A, B and C payloads are reused; only X and changed manifest pages need storage. Old roots remain readable. Reconstruction streams references from the chosen root, verifies the chunks and concatenates their bytes.

Snapshots still scan the full input, and worker metadata grows with chunk count. The native parallel store shares one repository; the Xet-matched scaling reports used independent per-worker stores.

Implementation: [chunking.cpp](../chunking.cpp) · [manifest.cpp](../manifest.cpp) · [shared cache](../shared_existence_cache.cpp).

