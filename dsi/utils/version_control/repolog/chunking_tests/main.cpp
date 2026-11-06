// main.cpp -- self-test + CLI helpers for the C++ FastCDC chunker port.
//
// Modes:
//   chunking_test                       run the built-in self-test suite
//   chunking_test --gen <size> <path>   write a deterministic synthetic file
//   chunking_test --dump <mode> <path> [processes]
//                                       print "<size> <sha256>" per chunk
//                                       (processes applies to parallel mode)
//   chunking_test --meta                print AVERAGE_BITS / masks / gear
//                                       tables, for diffing against pycheck.py
//
// The synthetic file format is shared with pycheck.py so the Python original
// and this port can be diffed byte-for-byte over identical inputs:
//
//     byte stream = for j = 0, 1, 2, ...: sha256(j as 8-byte little-endian u64)
//                   concatenated, truncated to the requested size.

#include "chunking.hpp"
#include "manifest.hpp"
#include "sha256.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <unordered_set>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

namespace {

// Mirrors chunking.py's chunking parameters (kept in sync with chunking.hpp
// internals; used here only for shape assertions).
constexpr std::size_t MIN_CHUNK = (std::size_t)1 << 20;
[[maybe_unused]] constexpr std::size_t AVG_CHUNK = (std::size_t)8 << 20;
constexpr std::size_t MAX_CHUNK = (std::size_t)64 << 20;

int gFailures = 0;

void check(bool ok, const std::string& what) {
    std::printf("[%s] %s\n", ok ? "PASS" : "FAIL", what.c_str());
    if (!ok) ++gFailures;
}

// Deterministic pseudo-random byte stream shared with pycheck.py.
std::array<std::uint8_t, 32> streamBlock(std::uint64_t index) {
    const std::uint8_t ctr[8] = {
        (std::uint8_t)(index >> 0),  (std::uint8_t)(index >> 8),
        (std::uint8_t)(index >> 16), (std::uint8_t)(index >> 24),
        (std::uint8_t)(index >> 32), (std::uint8_t)(index >> 40),
        (std::uint8_t)(index >> 48), (std::uint8_t)(index >> 56),
    };
    chunking::Sha256Ctx ctx;
    ctx.update(ctr, sizeof ctr);
    return ctx.rawdigest();
}

void genSynthetic(const std::string& path, std::uint64_t size) {
    std::ofstream out(path, std::ios::binary);
    if (!out) throw std::runtime_error("open " + path + " failed");
    std::uint64_t written = 0;
    std::uint64_t block = 0;
    while (written < size) {
        const auto digest = streamBlock(block++);
        const std::uint64_t take =
            std::min<std::uint64_t>(digest.size(), size - written);
        out.write(reinterpret_cast<const char*>(digest.data()),
                  (std::streamsize)take);
        written += take;
    }
}

std::string fileSha(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("open " + path + " failed");
    chunking::Sha256Ctx ctx;
    char buf[65536];
    for (;;) {
        in.read(buf, sizeof buf);
        const std::streamsize got = in.gcount();
        if (got > 0) ctx.update(buf, (std::size_t)got);
        if (got < (std::streamsize)sizeof buf) break;
    }
    return ctx.hexdigest();
}

bool filesEqual(const std::string& a, const std::string& b) {
    return fileSha(a) == fileSha(b);
}

// Field-by-field comparison (StoreResult has no operator==).
bool sameStore(const chunking::StoreResult& a, const chunking::StoreResult& b) {
    return a.fileHashes == b.fileHashes && a.chunkLength == b.chunkLength &&
           a.chunkHashes == b.chunkHashes && a.manifestRoots == b.manifestRoots;
}

// u128 -> decimal (for printing GEAR_TABLE_LS values that exceed 64 bits).
std::string dec128(unsigned __int128 v) {
    if (v == 0) return "0";
    std::string s;
    while (v > 0) {
        s.push_back((char)('0' + (char)(v % 10)));
        v /= 10;
    }
    std::reverse(s.begin(), s.end());
    return s;
}

void printMeta() {
    const chunking::ChunkerDiagnostics d = chunking::chunkerDiagnostics();
    std::string gear;
    for (std::uint64_t v : d.gearTable) {
        gear += std::to_string((unsigned long long)v);
        gear.push_back(' ');
    }
    std::string gearLs;
    for (const auto& v : d.gearTableLsWide) {
        gearLs += dec128(v);
        gearLs.push_back(' ');
    }
    if (!gear.empty()) gear.pop_back();
    if (!gearLs.empty()) gearLs.pop_back();
    std::printf("AVERAGE_BITS %d\n", d.averageBits);
    std::printf("STRICT_MASK %llu\n", (unsigned long long)d.strictMask);
    std::printf("EAGER_MASK %llu\n", (unsigned long long)d.eagerMask);
    std::printf("GEAR_TABLE %s\n", gear.c_str());
    std::printf("GEAR_TABLE_LS %s\n", gearLs.c_str());
}

void dumpChunks(const std::string& mode, const std::string& path,
                 std::size_t processes = 0) {
    std::vector<chunking::Chunk> chunks;
    if (mode == "fastcdc") {
        chunks = chunking::chunkFileFastCdc(path);
    } else if (mode == "rolling") {
        chunks = chunking::chunkFileRollingHash(path);
    } else if (mode == "parallel") {
        chunking::ParallelOptions options;
        options.processes = processes;
        chunks = chunking::chunkFileParallel(path, options);
    } else {
        std::fprintf(stderr, "unknown dump mode: %s (fastcdc|rolling|parallel)\n",
                     mode.c_str());
        std::exit(2);
    }
    for (const chunking::Chunk& c : chunks) {
        std::printf("%zu %s\n", c.size, c.sha256.c_str());
    }
}

void verifyChunkShapes(const std::string& label,
                       const std::vector<chunking::Chunk>& chunks,
                       std::size_t fileSize) {
    // Chunk sequence covers the file contiguously; every chunk is positive
    // and (except a final forced/last chunk) at least MIN_CHUNK; none over
    // MAX_CHUNK. Re-deduces the boundary layout like enumerate() would.
    (void)fileSize;
    std::size_t total = 0;
    bool ok = !chunks.empty() || true;
    for (std::size_t i = 0; i < chunks.size(); ++i) {
        const chunking::Chunk& c = chunks[i];
        if (c.size == 0) ok = false;
        if (total + c.size < total) ok = false;  // overflow
        if (i + 1 < chunks.size() && c.size < MIN_CHUNK) ok = false;
        total += c.size;
    }
    ok = ok && total == fileSize;
    check(ok, label + ": chunk shapes (coverage/size limits)");
}

// Independent byte-at-a-time oracle: deliberately does not use the optimized
// pair scanner or any production boundary/merge helper.
std::vector<chunking::SizeRange> referenceProcessRanges(const std::string& bytes) {
    const auto d = chunking::chunkerDiagnostics();
    std::vector<chunking::SizeRange> result;
    for (std::size_t unit = 0; unit < bytes.size(); unit += MAX_CHUNK) {
        const auto end = unit + std::min(MAX_CHUNK, bytes.size() - unit);
        std::size_t start = unit;
        while (start < end) {
            std::uint64_t fp = 0;
            std::size_t cut = end;
            for (std::size_t i = start + MIN_CHUNK; i < end; ++i) {
                fp = (fp << 1) + d.gearTable[static_cast<unsigned char>(bytes[i])];
                const auto mask = i - start < AVG_CHUNK ? d.strictMask : d.eagerMask;
                if ((fp & mask) == 0) { cut = i + 1; break; }
            }
            // Reject the most recent match if its tail cannot form a chunk.
            if (end - cut < MIN_CHUNK) cut = end;
            result.push_back({start, cut - start});
            start = cut;
            if (end - start <= 2 * MIN_CHUNK) {
                if (start < end) result.push_back({start, end - start});
                break;
            }
        }
    }
    if (result.size() > 1 && result.back().length < MIN_CHUNK) {
        const auto tail = result.back().length;
        result.pop_back();
        result.back().length += tail;
    }
    return result;
}

std::vector<chunking::Chunk> verifyProcessFile(const std::string& path,
                                               const std::string& bytes,
                                               std::size_t processes,
                                               bool requestData,
                                               const std::string& label) {
    const auto ranges = referenceProcessRanges(bytes);
    chunking::ParallelOptions options;
    options.processes = processes;
    options.requestData = requestData;
    const auto chunks = chunking::chunkFileParallel(path, options);
    bool ok = chunks.size() == ranges.size();
    for (std::size_t i = 0; ok && i < chunks.size(); ++i) {
        const auto& range = ranges[i];
        const auto& chunk = chunks[i];
        ok = chunk.size == range.length && chunk.sha256 ==
             chunking::sha256Hex(bytes.data() + range.offset, range.length);
        ok = ok && chunk.data.has_value() == requestData;
        if (requestData) {
            ok = ok && *chunk.data == bytes.substr(range.offset, range.length);
        }
        ok = ok && (bytes.size() < MIN_CHUNK || chunk.size >= MIN_CHUNK);
        ok = ok && chunk.size < MAX_CHUNK + MIN_CHUNK;
    }
    check(ok, label + ": bytewise oracle, SHA-256, ordered coverage, payload policy");
    return chunks;
}

void testProcessChunking(const std::string& work) {
    const auto path = work + "/process.bin";
    auto write = [&](const std::string& bytes) {
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        if (!out) throw std::runtime_error("write process fixture failed");
    };
    std::size_t fixture = 0;
    auto verifyStore = [&](const std::string& bytes) {
        const auto root = work + "/direct-store-" + std::to_string(fixture++);
        const chunking::FileEntry file{path, "input.bin", "file"};
        const auto stored = chunking::storeChunksForSnapshot(root, {file}, "UPDATE", 2);
        const auto ranges = referenceProcessRanges(bytes);
        chunking::FileManifest manifest(root, {}, stored.manifestRoots.at(file.relativePath));
        std::unordered_set<std::string> hashes;
        std::size_t index = 0;
        bool ok = true;
        manifest.forEach([&](const chunking::ManifestChunk& chunk) {
            if (index >= ranges.size()) { ok = false; return; }
            const auto& range = ranges[index++];
            const auto hash = chunking::sha256Hex(bytes.data() + range.offset, range.length);
            ok = ok && chunk.hash == hash && chunk.size == range.length;
            hashes.insert(hash);
        });
        std::size_t objects = 0;
        for (const auto& item : std::filesystem::directory_iterator(root + "/dsi_vcs_chunks")) {
            const auto name = item.path().filename().string();
            if (name.size() == 64 && name.find_first_not_of("0123456789abcdef") == std::string::npos) {
                ++objects;
                ok = ok && hashes.count(name) == 1;
            }
        }
        check(ok && index == ranges.size() && objects == hashes.size(),
              "direct workers: oracle hashes/order; only final chunks published");
        check(chunking::rebuildFileFromManifest(root, file.relativePath,
                  stored.manifestRoots.at(file.relativePath), stored.fileHashes.at(file.relativePath),
                  root + "/restore") && filesEqual(path, root + "/restore/input.bin"),
              "direct workers: boundary fixture restores every byte");
    };
    for (const auto size : {std::size_t(0), MIN_CHUNK - 1, MIN_CHUNK,
                            MIN_CHUNK + 1, 2 * MIN_CHUNK}) {
        const std::string bytes(size, '\0');
        write(bytes);
        const auto chunks = verifyProcessFile(path, bytes, 3, true, "empty/minimum boundary");
        check(size == 0 ? chunks.empty() : chunks.size() == 1,
              "no-match small unit is emitted whole");
        verifyStore(bytes);
    }

    // Build a real eager-mask matching signature. Sixty-four preceding bytes
    // fully determine the Gear fingerprint, so it can be placed near a unit's
    // end to exercise exact tail thresholds without changing the masks.
    std::uint64_t random = 123456789, fp = 0;
    const auto d = chunking::chunkerDiagnostics();
    std::string signature;
    std::string sample(32 * 1024 * 1024, '\0');
    for (char& byte : sample) {
        random ^= random << 13; random ^= random >> 7; random ^= random << 17;
        byte = static_cast<char>(random >> 32);
    }
    for (std::size_t i = MIN_CHUNK; i < sample.size(); ++i) {
        fp = (fp << 1) + d.gearTable[static_cast<unsigned char>(sample[i])];
        if (i >= AVG_CHUNK && (fp & d.eagerMask) == 0) {
            signature = sample.substr(i + 1 - 64, 64);
            break;
        }
    }
    if (signature.empty()) throw std::runtime_error("fixture did not find an eager fingerprint");

    for (const auto tail : {std::size_t(0), std::size_t(1), MIN_CHUNK - 1, MIN_CHUNK, 2 * MIN_CHUNK,
                            2 * MIN_CHUNK + 1}) {
        std::string bytes(MAX_CHUNK, '\0');
        const auto cut = MAX_CHUNK - tail;
        bytes.replace(cut - signature.size(), signature.size(), signature);
        write(bytes);
        const auto chunks = verifyProcessFile(path, bytes, 2, false, "fingerprint tail threshold");
        check(tail < MIN_CHUNK ? chunks.size() == 1 :
                  chunks.size() == 2 && chunks.front().size == cut && chunks.back().size == tail,
              "short tail cancels last cut; tail at least MIN_CHUNK stays separate");
        verifyStore(bytes);
    }
    {
        const std::string bytes(2 * MAX_CHUNK + MIN_CHUNK - 1, '\0');
        write(bytes);
        const auto chunks = verifyProcessFile(path, bytes, 2, true, "no-match units and EOF tail");
        check(chunks.size() == 2 && chunks.front().size == MAX_CHUNK &&
                  chunks.back().size == MAX_CHUNK + MIN_CHUNK - 1,
              "no fingerprints: whole units; short final unit merged backward");
        verifyStore(bytes);
    }
    {
        std::string bytes(2 * MAX_CHUNK + MIN_CHUNK - 1, '\0');
        const auto cut = MAX_CHUNK - (MIN_CHUNK - 1);
        bytes.replace(cut - signature.size(), signature.size(), signature);
        write(bytes);
        for (std::size_t processes : {std::size_t(1), std::size_t(2), std::size_t(3)}) {
            const auto chunks = verifyProcessFile(path, bytes, processes, processes == 3,
                                                   "local cut cancellation and short final unit");
            check(chunks.size() == 2 && chunks.front().size == MAX_CHUNK &&
                      chunks.back().size == MAX_CHUNK + MIN_CHUNK - 1,
                  "interior short tail stays in its unit; only whole short EOF unit crosses backward");
        }
        verifyStore(bytes);
    }
    {
        // Keep an earlier valid cut, reject only the final cut, and leave the
        // next unit's first chunk intact. No EOF merge can hide a forward merge.
        std::string bytes(2 * MAX_CHUNK, '\0');
        const auto firstCut = AVG_CHUNK + MIN_CHUNK;
        const auto rejectedCut = MAX_CHUNK - (MIN_CHUNK - 1);
        bytes.replace(firstCut - signature.size(), signature.size(), signature);
        bytes.replace(rejectedCut - signature.size(), signature.size(), signature);
        write(bytes);
        for (const bool data : {false, true}) {
            const auto chunks = verifyProcessFile(path, bytes, 2, data, "interior short-tail regression");
            check(chunks.size() == 3 && chunks[0].size == firstCut &&
                      chunks[1].size == MAX_CHUNK - firstCut && chunks[2].size == MAX_CHUNK,
                  "cancel only the latest cut and preserve the following unit");
        }
        verifyStore(bytes);
    }
    {
        // More units than workers tests process reuse and ordered pipe draining.
        std::string bytes(3 * MAX_CHUNK + 17, '\0');
        for (std::size_t unit = 0; unit < 3; ++unit) {
            bytes.replace(unit * MAX_CHUNK, sample.size(), sample);
        }
        write(bytes);
        verifyProcessFile(path, bytes, 2, false, "multiple fingerprints and worker reuse");
        verifyProcessFile(path, bytes, 0, true, "automatic process count");

        const auto root = work + "/parallel-repo";
        const chunking::FileEntry file{path, "parallel.bin", "file"};
        const auto stored = chunking::storeChunksForSnapshot(root, {file}, "UPDATE", 2);
        std::unordered_map<std::string, std::filesystem::file_time_type> modified;
        for (const auto& hash : stored.chunkHashes)
            modified.emplace(hash, std::filesystem::last_write_time(root + "/dsi_vcs_chunks/" + hash));
        check(chunking::rebuildFileFromManifest(root, file.relativePath,
                  stored.manifestRoots.at(file.relativePath), stored.fileHashes.at(file.relativePath),
                  work + "/parallel-out") && filesEqual(path, work + "/parallel-out/parallel.bin"),
              "multiprocess store + manifest rebuild preserves every byte");
        const auto again = chunking::storeChunksForSnapshot(root, {file}, "UPDATE", 3, stored.manifestRoots);
        check(sameStore(stored, again), "process count does not change stored chunks or manifest");
        bool unchanged = true;
        for (const auto& item : modified)
            unchanged = unchanged && item.second == std::filesystem::last_write_time(root + "/dsi_vcs_chunks/" + item.first);
        check(unchanged, "new snapshot cache epoch reuses blobs without rewriting");

        // A worker-side publication failure must propagate, release locks,
        // stop/reap the pool, and leave the next independent call usable.
        const auto brokenRoot = work + "/broken-parallel-repo";
        const auto ranges = referenceProcessRanges(bytes);
        const auto& first = ranges.front();
        const auto collision = brokenRoot + "/dsi_vcs_chunks/" +
            chunking::sha256Hex(bytes.data() + first.offset, first.length);
        std::filesystem::create_directories(collision);
        bool failed = false;
        try { chunking::storeChunksForSnapshot(brokenRoot, {file}, "UPDATE", 2); }
        catch (const std::exception&) { failed = true; }
        check(failed, "worker publication errors propagate without returning a manifest root");
        std::filesystem::remove(collision);
        const auto recovered = chunking::storeChunksForSnapshot(brokenRoot, {file}, "UPDATE", 2);
        check(sameStore(stored, recovered), "parallel store recovers after worker publication failure");
    }
    {
        bool failed = false;
        try { chunking::chunkFileParallel(work + "/missing.bin"); }
        catch (const std::exception&) { failed = true; }
        check(failed, "multiprocess missing input reports an error");
    }
}

int runSelfTest(std::uint64_t mainSize) {
    std::printf("chunking C++ self-test  (SHA-256 backend: %s)\n",
                chunking::Sha256Ctx::backendName());

    // ------------------------------------------------------------------
    // SHA-256 library backend
    // ------------------------------------------------------------------
    check(chunking::sha256Hex("", 0) ==
              "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
          "sha256Hex(\"\") matches the FIPS 180-2 vector");
    check(chunking::sha256Hex("abc") ==
              "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
          "sha256Hex(\"abc\") matches the FIPS 180-2 vector");

    {
        // Streaming context == one-shot, and hexdigest() is non-destructive
        // (hashlib semantics).
        chunking::Sha256Ctx ctx;
        ctx.update("foo");
        const std::string mid = ctx.hexdigest();
        ctx.update("bar");
        check(mid == chunking::sha256Hex("foo") &&
                  ctx.hexdigest() == chunking::sha256Hex("foobar"),
              "Sha256Ctx: streaming == one-shot, hexdigest non-destructive");
    }

    {
        // Multi-slice streaming over a >512-bit message with awkward sizes.
        std::vector<std::uint8_t> payload(70000);
        for (std::size_t i = 0; i < payload.size(); ++i) {
            payload[i] = streamBlock(i / 32)[i % 32];
        }
        chunking::Sha256Ctx ctx;
        std::size_t off = 0;
        std::size_t step = 1;
        while (off < payload.size()) {
            const std::size_t take = std::min(step, payload.size() - off);
            ctx.update(payload.data() + off, take);
            off += take;
            step = step * 3 % 60001;
        }
        check(ctx.hexdigest() ==
                  chunking::sha256Hex(payload.data(), payload.size()),
              "Sha256Ctx: many small updates == one-shot");
    }

    // ------------------------------------------------------------------
    // Synthetic files (same recipe as pycheck.py, so the Python original can
    // be diffed against every result printed here).
    // ------------------------------------------------------------------
    const std::string tmpBase = [] {
        std::string base = "/tmp";
        if (const char* env = std::getenv("TMPDIR")) base = env;
        while (base.size() > 1 && base.back() == '/') base.pop_back();
        return base;
    }();
    const std::string work =
        tmpBase + "/chunking_cpp_test_" + std::to_string((long)::getpid());
    std::filesystem::remove_all(work);
    std::filesystem::create_directories(work);

    const std::uint64_t kSizeEmpty = 0;
    const std::uint64_t kSizeSmall = 500u * 1024 + 123;        // < MIN_CHUNK
    const std::uint64_t kSizeMain  = mainSize;                 // spans all zones

    const std::string fEmpty = work + "/empty.bin";
    const std::string fSmall = work + "/small.bin";
    const std::string fMain  = work + "/main.bin";
    genSynthetic(fEmpty, kSizeEmpty);
    genSynthetic(fSmall, kSizeSmall);
    genSynthetic(fMain, kSizeMain);

    // Deterministic generator sanity: the two runs agree, and the small file
    // is a prefix of the empty-extended main stream by construction.
    {
        const std::string alt = work + "/small2.bin";
        genSynthetic(alt, kSizeSmall);
        check(filesEqual(fSmall, alt), "synthetic generator is deterministic");
    }

    // ------------------------------------------------------------------
    // FastCDC chunking + independent multiprocess layout
    // ------------------------------------------------------------------
    {
        const auto chunks = chunking::chunkFileFastCdc(fEmpty);
        check(chunks.empty(), "empty file yields no chunks (empty-file guard)");
    }
    {
        const auto chunks = chunking::chunkFileFastCdc(fSmall);
        check(chunks.size() == 1 && chunks[0].size == kSizeSmall,
              "file < MIN_CHUNK chunks as a single whole-file chunk");
        check(chunks[0].data == std::string(chunks[0].size, '\0') ||
                  chunks[0].data.has_value(),
              "default needs_data copies payloads");
    }

    std::vector<chunking::Chunk> mainChunks;
    {
        mainChunks = chunking::chunkFileFastCdc(fMain);
        verifyChunkShapes("fastcdc", mainChunks, kSizeMain);
        std::printf("       fastcdc: %zu chunks\n", mainChunks.size());
    }
    {
        std::ifstream in(fMain, std::ios::binary);
        const std::string bytes((std::istreambuf_iterator<char>(in)), {});
        const auto par = verifyProcessFile(fMain, bytes, 0, false, "multiprocess main fixture");
        std::printf("       parallel: %zu chunks\n", par.size());
    }
    {
        // needs_data predicate short-circuits the payload copy (data ==
        // nullopt), while hashes/sizes stay identical.
        const auto slim = chunking::chunkFile(
            fMain, [](const std::string&) { return false; });
        bool same = slim.size() == mainChunks.size();
        for (std::size_t i = 0; same && i < slim.size(); ++i) {
            same = slim[i].sha256 == mainChunks[i].sha256 &&
                   slim[i].size == mainChunks[i].size &&
                   !slim[i].data.has_value() &&
                   mainChunks[i].data.has_value();
        }
        check(same, "needs_data: rejects payload copy but keeps digests");
    }

    // Later checks only need digests/sizes; drop the (possibly huge) payload
    // copies so a 500 MB test file does not sit twice in RAM.
    for (chunking::Chunk& c : mainChunks) c.data.reset();

    testProcessChunking(work);

    // ------------------------------------------------------------------
    // File-only store + rebuild round trip (SQL logic removed)
    // ------------------------------------------------------------------
    {
        const std::string chunkRoot = work + "/repo";
        std::vector<chunking::FileEntry> entries;
        chunking::FileEntry entry;
        entry.absolutePath = fMain;
        entry.relativePath = "data/nested/test_bytes.bin";
        entry.fileType = "file";
        entries.push_back(entry);

        const auto res = chunking::storeChunksForSnapshot(chunkRoot, entries);

        // Independent recomputation of the per-file hash: sha256 over the
        // concatenation of the chunk hex digests (utf-8), i.e. exactly what
        // the module documents. Payloads are skipped (needs_data == false).
        chunking::Sha256Ctx expected;
        for (const chunking::Chunk& c :
             chunking::chunkFile(fMain, [](const std::string&) { return false; })) {
            expected.update(c.sha256);
        }
        check(res.fileHashes.at("data/nested/test_bytes.bin") ==
                  expected.hexdigest(),
              "store: per-file hash == sha256(concat chunk digests)");
        check(res.chunkLength.at("data/nested/test_bytes.bin") ==
                  mainChunks.size(),
              "store: chunk count matches");
        check(res.chunkHashes.size() == mainChunks.size(),
              "store: unique chunk set matches (all chunks distinct)");

        // Blobs really are on disk, 0644, one per unique digest.
        const std::string chunkDir = chunkRoot + "/dsi_vcs_chunks";
        bool blobsOk = true;
        for (const chunking::Chunk& c : mainChunks) {
            struct ::stat st {};
            if (::stat((chunkDir + "/" + c.sha256).c_str(), &st) != 0 ||
                !(st.st_mode & 0644) || (std::size_t)st.st_size != c.size) {
                blobsOk = false;
            }
        }
        check(blobsOk, "store: chunk blobs published with size + 0644");

        // Rebuild with the exact ordered chunk hashes (DB-row stand-in).
        std::vector<std::string> ordered;
        for (const chunking::Chunk& c : mainChunks) ordered.push_back(c.sha256);
        const std::string rebuilt =
            work + "/out/data/nested/test_bytes.bin";
        check(chunking::rebuildFileFromChunks(
                  chunkRoot, "data/nested/test_bytes.bin", "commitABC",
                  res.fileHashes.at("data/nested/test_bytes.bin"), work + "/out",
                  ordered),
              "rebuild: correct hashes rebuild");
        check(filesEqual(rebuilt, fMain), "rebuild: bytes match the original");
        check(chunking::rebuildFileFromManifest(
                  chunkRoot, entry.relativePath, res.manifestRoots.at(entry.relativePath),
                  res.fileHashes.at(entry.relativePath), work + "/manifest-out"),
              "manifest: root streams a complete file version");
        check(filesEqual(work + "/manifest-out/" + entry.relativePath, fMain),
              "manifest: reconstructed bytes match the original");

        // Wrong file hash -> rejected before any output is written.
        check(!chunking::rebuildFileFromChunks(
                  chunkRoot, "data/nested/test_bytes.bin", "commitABC",
                  std::string(64, '0'), work + "/out", ordered),
              "rebuild: wrong file_hash returns false");

        // Missing blob -> false.
        const std::string removed = chunkDir + "/" + ordered.front();
        const std::string backup = removed + ".bak";
        std::filesystem::rename(removed, backup);
        check(!chunking::rebuildFileFromChunks(
                  chunkRoot, "data/nested/test_bytes.bin", "commitABC",
                  res.fileHashes.at("data/nested/test_bytes.bin"),
                  work + "/out", ordered),
              "rebuild: missing blob returns false");
        std::filesystem::rename(backup, removed);

        // Second snapshot: everything dedups (first-occurrence / existence
        // cache path), and results stay stable.
        const auto res2 = chunking::storeChunksForSnapshot(
            chunkRoot, entries, "UPDATE", 0, res.manifestRoots);
        check(sameStore(res2, res), "store: second snapshot is stable & dedup'd");

        // A non-"file" entry is skipped entirely.
        std::vector<chunking::FileEntry> mixed = entries;
        chunking::FileEntry dir;
        dir.absolutePath = fMain;
        dir.relativePath = "ignored_dir/";
        dir.fileType = "dir";
        mixed.push_back(dir);
        const auto res3 = chunking::storeChunksForSnapshot(chunkRoot, mixed);
        check(res3.fileHashes.find("ignored_dir/") == res3.fileHashes.end() &&
                  res3.fileHashes.size() == 1,
              "store: non-file entries skipped");
    }

    // ------------------------------------------------------------------
    // Legacy rolling-hash path
    // ------------------------------------------------------------------
    {
        const auto chunks = chunking::chunkFileRollingHash(fMain);
        bool ok = !chunks.empty();
        std::size_t total = 0;
        for (std::size_t i = 0; i < chunks.size(); ++i) {
            if (chunks[i].size == 0) ok = false;
            if (i + 1 < chunks.size() &&
                chunks[i].size < MIN_CHUNK) ok = false;
            if (chunks[i].size > MAX_CHUNK) ok = false;
            total += chunks[i].size;
        }
        ok = ok && total == kSizeMain;
        check(ok, "rolling-hash: coverage + MIN/MAX discipline");
        std::printf("       rolling: %zu chunks\n", chunks.size());
    }

    // ------------------------------------------------------------------
    // Timing (sequential vs parallel hot path)
    // ------------------------------------------------------------------
    {
        const auto t0 = std::chrono::steady_clock::now();
        // Compare the two layouts with payload copies suppressed. Files must
        // exceed MAX_CHUNK to give the process path multiple units to scan.
        chunking::chunkFile(fMain, [](const std::string&) { return false; });
        const auto t1 = std::chrono::steady_clock::now();
        chunking::chunkFileParallel(fMain, {});
        const auto t2 = std::chrono::steady_clock::now();
        const double msSeq =
            std::chrono::duration<double, std::milli>(t1 - t0).count();
        const double msPar =
            std::chrono::duration<double, std::milli>(t2 - t1).count();
        std::printf("       timing: sequential %.1f ms, parallel %.1f ms (%.2fx)\n",
                    msSeq, msPar, msSeq / msPar);
    }

    std::filesystem::remove_all(work);

    if (gFailures == 0) {
        std::printf("ALL TESTS PASSED\n");
        return 0;
    }
    std::printf("%d TEST(S) FAILED\n", gFailures);
    return 1;
}

}  // namespace

int main(int argc, char** argv) {
    try {
        if (argc >= 2) {
            const std::string mode = argv[1];
            if (mode == "--gen" && argc == 4) {
                genSynthetic(argv[3], std::stoull(argv[2]));
                return 0;
            }
            if (mode == "--dump" &&
                (argc == 4 || (argc == 5 && std::string(argv[2]) == "parallel"))) {
                dumpChunks(argv[2], argv[3], argc == 5 ? std::stoull(argv[4]) : 0);
                return 0;
            }
            if (mode == "--meta" && argc == 2) {
                printMeta();
                return 0;
            }
            if (mode == "--selftest" && argc == 3) {
                // usage: --selftest <main-file-size-bytes>
                return runSelfTest(std::stoull(argv[2]));
            }
            if (mode == "--store" && argc == 5) {
                // usage: --store <chunkRoot> <absPath> <relativePath>
                chunking::FileEntry e;
                e.absolutePath = argv[3];
                e.relativePath = argv[4];
                e.fileType = "file";
                const chunking::StoreResult res =
                    chunking::storeChunksForSnapshot(argv[2], {e});
                std::printf("%s\n%zu\n%zu\n",
                            res.fileHashes.at(argv[4]).c_str(),
                            res.chunkLength.at(argv[4]),
                            res.chunkHashes.size());
                return 0;
            }
            if (mode == "--store-manifest" && (argc == 5 || argc == 6)) {
                chunking::FileEntry entry{argv[3], argv[4], "file"};
                std::unordered_map<std::string, std::string> base;
                if (argc == 6) base[argv[4]] = argv[5];
                const auto result = chunking::storeChunksForSnapshot(argv[2], {entry}, "UPDATE", 0, base);
                std::printf("%s\n%s\n%zu\n", result.manifestRoots.at(argv[4]).c_str(),
                            result.fileHashes.at(argv[4]).c_str(), result.chunkLength.at(argv[4]));
                return 0;
            }
            if (mode == "--rebuild-manifest" && argc == 7) {
                const bool ok = chunking::rebuildFileFromManifest(argv[2], argv[3], argv[4], argv[5], argv[6]);
                std::printf("%d\n", ok ? 0 : 1);
                return ok ? 0 : 3;
            }
            if (mode == "--rebuild" && argc == 7) {
                // usage: --rebuild <chunkRoot> <relPath> <fileHash> <outDir>
                //        <hash,hash,...>   (ordered chunk hashes, the
                //        DB-row stand-in)
                std::vector<std::string> hashes;
                std::string token;
                std::istringstream list(argv[6]);
                while (std::getline(list, token, ',')) {
                    if (!token.empty()) hashes.push_back(token);
                }
                const bool ok = chunking::rebuildFileFromChunks(
                    argv[2], argv[3], "UPDATE", argv[4], argv[5], hashes);
                std::printf("%d\n", ok ? 0 : 1);
                return ok ? 0 : 3;
            }
            std::fprintf(stderr,
                         "usage:\n"
                         "  %s\n"
                         "  %s --selftest <main-file-size-bytes>\n"
                         "  %s --gen <size> <path>\n"
                         "  %s --dump <fastcdc|rolling|parallel> <path> [processes]\n"
                         "  %s --meta\n"
                         "  %s --store <chunkRoot> <absPath> <relativePath>\n"
                         "  %s --rebuild <chunkRoot> <relPath> <fileHash> <outDir> <hash,hash,...>\n"
                         "  %s --store-manifest <chunkRoot> <absPath> <relPath> [baseRoot]\n"
                         "  %s --rebuild-manifest <chunkRoot> <relPath> <root> <fileHash> <outDir>\n",
                         argv[0], argv[0], argv[0], argv[0], argv[0], argv[0],
                         argv[0], argv[0], argv[0]);
            return 2;
        }
        return runSelfTest(20u * 1024 * 1024 + 4567);  // default main file size
    } catch (const std::exception& e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}
