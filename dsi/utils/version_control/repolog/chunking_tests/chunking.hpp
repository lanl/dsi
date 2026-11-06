// chunking.hpp -- C++ port of repolog/chunking.py
//
// Contents:
//   * RollingHash + gear-table FastCDC boundary detector (the heart of the
//     module), reproduced bit-for-bit (the 128-bit gear math mirrors Python's
//     arbitrary-precision int arithmetic so the boundaries match exactly).
//   * A Python-compatible streaming iterator and a multicore batch path
//     (chunkFileParallel) with independent MAX_CHUNK units read, scanned,
//     and SHA-256 hashed by separate OS processes.
//   * File-only chunk storage (store / rebuild) -- the original Python writes
//     chunk blobs to <chunk_root>/dsi_vcs_chunks/<sha256> AND inserts a row into
//     a SQL `chunk_store` table. Per the task we DROP the SQL side (and the
//     NPY/SQLite magic detection that fed it). Payloads and immutable manifest
//     pages live in content-addressed object stores.
//
// SHA-256 is delegated to a library (CommonCrypto / OpenSSL, see sha256.hpp);
// this file never hand-rolls the digest algorithm.

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

namespace chunking {

// A single content-defined chunk. Mirrors the dict
//   {"sha256": <hex>, "size": <int>, "data": <bytes|None>}
// `data` is std::nullopt when the payload was not copied (mirrors the
// needs_data predicate short-circuiting the copy).
struct Chunk {
    std::string          sha256;   // 64-char lowercase hex digest
    std::size_t          size = 0;  // byte length of the chunk
    std::optional<std::string> data;  // payload, or nullopt when omitted
};

// A byte range [offset, offset+length) within a caller-supplied buffer.
struct SizeRange {
    std::size_t offset = 0;
    std::size_t length = 0;
};

// A file to store -- subset of the Python `entry` dict we actually use.
struct FileEntry {
    std::string absolutePath;     // entry['absolute_path']
    std::string relativePath;     // entry['relative_path']
    std::string fileType = "file"; // only "file" entries are chunked
};

// Result of store(), mirroring
//   (file_hashes, chunk_hashes, chunk_length)
// The SQL `chunk_store` insertion (and its pending_commit/pending_rows)
// is intentionally absent -- this is a file-only store per the task.
struct StoreResult {
    std::unordered_map<std::string, std::string> fileHashes;   // rel_path -> per-file hash
    std::set<std::string>                         chunkHashes;  // dedup'd set of blob hashes
    std::unordered_map<std::string, std::size_t>  chunkLength;  // rel_path -> chunk count
    std::unordered_map<std::string, std::string> manifestRoots; // rel_path -> immutable root
};

// needs_data predicate: given a chunk's hex digest, should we copy its payload?
// (Empty => always copy, matching the Python `None` default.)
using NeedsData = std::function<bool(const std::string&)>;

// -------------------------------------------------------------------------
// FastCDC boundary detection -- the sequential core.
// -------------------------------------------------------------------------

// Return the byte ranges that segment `data[0..size)` into FastCDC chunks.
// Identical boundaries to the streaming iterator below.
std::vector<SizeRange> findChunkBoundaries(const std::uint8_t* data,
                                           std::size_t size);

// Compute SHA-256 (as hex) of every range, in parallel across `threads`
// worker threads (0 or 1 => serial). This is the multicore hot path.
std::vector<std::string>
hashRangesParallel(const std::uint8_t* base,
                   const std::vector<SizeRange>& ranges,
                   std::size_t threads = 0);

// -------------------------------------------------------------------------
// Streaming iterator -- faithful port of iter_file_fastcdc().
//
// The file is consumed sequentially through a bounded sliding window of at
// most MAX_CHUNK bytes: every refill covers exactly the FastCDC lookahead
// range, so cut positions are bit-for-bit identical to a whole-file view,
// while the resident set peaks at one window regardless of file size.
// -------------------------------------------------------------------------
class FastCdcChunkIterator {
public:
    // Streams a file read-only. Iterate it in place (hasMore/next/close); the
    // type is non-copyable / non-movable.
    FastCdcChunkIterator(std::string path, NeedsData needs = {});
    ~FastCdcChunkIterator();
    FastCdcChunkIterator(const FastCdcChunkIterator&) = delete;
    FastCdcChunkIterator& operator=(const FastCdcChunkIterator&) = delete;

    bool        hasMore() const;
    Chunk       next();                 // advances; throws if exhausted
    void        close();                // closes the fd early (like an early break)
    std::vector<Chunk> materialize();   // drain to a vector

private:
    // Slide the window to pos_ and refill it from the fd with as many bytes
    // as remain (at most MAX_CHUNK).
    void fillWindow();

    int          fd_ = -1;
    std::size_t  fileSize_ = 0;
    std::vector<std::uint8_t> buffer_;  // sliding window, <= MAX_CHUNK bytes
    std::size_t  bufStart_ = 0;         // file offset of buffer_[0]
    std::size_t  bufLen_   = 0;         // valid bytes currently in buffer_
    std::size_t  pos_      = 0;         // start (file offset) of the next chunk
    NeedsData    needs_;
    bool         closed_   = false;
};

// -------------------------------------------------------------------------
// High-level entry points -- mirror the Python functions.
// -------------------------------------------------------------------------

// chunk_file_fastcdc(path)  -> list[Chunk]  (snapshots every byte)
std::vector<Chunk> chunkFileFastCdc(const std::string& path);

// chunk_file(path, streaming=..., needs_data=...):
//   streaming == false -> materialise the list (default).
//   streaming == true  -> invoke cb(Chunk) per chunk, lazily, with the open
//                         fd held until finished (mirrors the generator form).
// Both use the needs_data predicate to decide whether to copy payloads.
std::vector<Chunk> chunkFile(const std::string& path, NeedsData needs = {});
void               chunkFileStream(const std::string& path,
                                   NeedsData needs,
                                   const std::function<void(const Chunk&)>& cb);

// chunk_file_rolling_hash(path) -- the legacy (non-FastCDC) rolling-hash path
// kept for parity; reproduces chunk_file_rolling_hash() exactly.
std::vector<Chunk> chunkFileRollingHash(const std::string& path);

// -------------------------------------------------------------------------
// Multiprocess batch API: partition into kMaxChunk units. Workers scan each
// unit sequentially, emitting through matching byte i. If the remaining tail
// is < kMinChunk, cancel that cut and include the tail in its preceding chunk
// within the unit. A tail in [kMinChunk, 2*kMinChunk] is emitted separately;
// otherwise resume chunking. Without another match emit only the unexported
// remainder. Only a whole final unit < kMinChunk merges across a unit boundary,
// backward at EOF. A file < kMinChunk stays whole. Unit-local chunks are at
// most kMaxChunk; the EOF merge stays below kMaxChunk+kMinChunk.
// Results are deterministic across process counts but differ from the Python
// sequential layout. Working memory is O(processes*kMaxChunk), plus returned
// metadata and, if requestData is true, all returned payloads.
// Parent polls all result pipes, incrementally appends complete chunks to
// per-worker lists, and merges those lists by file offset after workers exit.
// POSIX fork/pipe workers: invoke from a single-threaded calling process.
// -------------------------------------------------------------------------
struct ParallelOptions {
    std::size_t threads = 0;          // legacy alias for process count
    bool        requestData = false;  // copy every chunk's payload
    std::size_t processes = 0;        // overrides threads; both 0 => CPU count
};
std::vector<Chunk> chunkFileParallel(const std::string& path,
                                     ParallelOptions opt = {});

// -------------------------------------------------------------------------
// File-only chunk storage (SQL removed).
// -------------------------------------------------------------------------

// storeChunksForSnapshot():
//   for each "file" entry, chunk it, write any not-yet-published chunk blobs to
//   <chunk_root>/dsi_vcs_chunks/<sha256> via atomic temp-file+rename, and
//   compute the per-file hash (sha256 over the ordered chunk hex digests).
// `threads` > 1 selects the multiprocess layout (legacy parameter name).
// Workers open the filename, read bounded ranges into private buffers, hash,
// check a process-shared existence cache, and atomically publish new blobs.
// Parent/worker IPC carries only range descriptors and ordered chunk metadata.
// Replies are collected as ready into separate worker lists; an available
// worker receives its next unit without waiting for earlier units to finish.
// Boundary spans are finalized in workers after tail merges are resolved.
// Requires a single-threaded caller and stable input files during this call;
// observed source changes or worker errors fail the call without returning roots.
// Payload memory is O(threads*kMaxChunk); returned/working metadata is O(chunks).
// 0 or 1 keeps the Python-compatible sequential layout. `pendingCommit` is kept in
// the signature for API parity with the Python original but is unused now that
// the `chunk_store` SQL insert is gone.
// Returns manifestRoots for the caller to persist in its file-version metadata.
// Supply the previous version's roots to preserve page boundaries across edits.
// Automatic sequence matching uses bounded lookahead; known-position edits can
// use FileManifest directly without scanning a complete file (see manifest.hpp).
StoreResult storeChunksForSnapshot(const std::string& chunkRoot,
                                   const std::vector<FileEntry>& entries,
                                   const std::string& pendingCommit = "UPDATE",
                                   std::size_t threads = 0,
                                   const std::unordered_map<std::string, std::string>& baseManifestRoots = {});

// Stream a file version directly from its manifest root; no complete chunk list
// or commit ancestry lookup is required. The legacy vector overload remains.
bool rebuildFileFromManifest(const std::string& chunkRoot,
                             const std::string& relativePath,
                             const std::string& manifestRoot,
                             const std::string& fileHash,
                             const std::string& outputPath);

// rebuildFileFromChunks():
//   verify the ordered chunk digests reproduce `fileHash`, then concatenate the
//   blob payloads (from the on-disk store) into <output_path>/<relative_path>.
// The ordered chunk list stands in for the rows the original read from the now
// removed `chunk_store` SQL table. Returns false on any mismatch / missing blob.
// `commitHash` is kept for signature parity (unused: no DB to filter on).
bool rebuildFileFromChunks(const std::string& chunkRoot,
                           const std::string& relativePath,
                           const std::string& commitHash,
                           const std::string& fileHash,
                           const std::string& outputPath,
                           const std::vector<std::string>& orderedChunkHashes);

// -------------------------------------------------------------------------
// Diagnostics -- exposes the internal parameter tables so the cross-check
// harness (pycheck.py) can diff them against the Python original's
// GEAR_TABLE / GEAR_TABLE_LS / STRICT_MASK / EAGER_MASK / AVERAGE_BITS.
// Not part of the chunking logic itself.
// -------------------------------------------------------------------------
struct ChunkerDiagnostics {
    std::array<std::uint64_t, 256> gearTable{};    // Python GEAR_TABLE
    std::array<std::uint64_t, 256> gearTableLs{};  // low 64 bits of GEAR_TABLE_LS
    std::uint64_t                  strictMask = 0; // Python STRICT_MASK
    std::uint64_t                  eagerMask  = 0; // Python EAGER_MASK
    int                            averageBits = 0; // Python AVERAGE_BITS
    // GEAR_TABLE_LS in full 65-bit precision (carries Python's bit 64).
    std::array<unsigned __int128, 256> gearTableLsWide{};
};
ChunkerDiagnostics chunkerDiagnostics();

}  // namespace chunking
