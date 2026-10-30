// chunking.cpp -- faithful C++ port of repolog/chunking.py (file-only storage).
//
// What is ported (and what is deliberately dropped):
//   * RollingHash, _build_gear_table/GEAR_TABLE/GEAR_TABLE_LS, _make_spread_mask,
//     _validate_parameters, _gear_update, _find_fastcdc_cut, iter_file_fastcdc,
//     chunk_file_fastcdc, chunk_file, chunk_file_rolling_hash
//       -> ported bit-for-bit (Python arbitrary-precision int arithmetic is
//          emulated with unsigned __int128 / uint64 exactly where it matters).
//   * store_chunks_for_snapshot / rebuild_file_from_chunks
//       -> ported with ALL SQL-database logic removed (no conn, no INSERT/
//          executemany/pending_rows, no chunk_store SELECT). Chunk payloads
//          persist as files under <chunk_root>/dsi_vcs_chunks/<sha256>, exactly
//          like the Python's blob half of the writer (mkstemp + chmod 0644 +
//          fsync + os.replace). The ordered chunk-hash list argument to
//          rebuild() stands in for the rows the Python read from the DB.
//   * NPY_MAGIC / SQLITE_MAGIC magic detection (_has_npy_magic, is_sqlite_file)
//       -> dropped per the task; chunk_file always routes to FastCDC.
//   * CHUNK_INSERT_BATCH_SIZE -> SQL-only constant, dropped.
//
// SHA-256 is never hand-rolled here: sha256.hpp delegates to CommonCrypto
// (Apple default) or OpenSSL (-DUSE_OPENSSL).

#include "chunking.hpp"

#include "sha256.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <deque>
#include <fstream>
#include <list>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace chunking {

namespace {

// ---------------------------------------------------------------------------
// Parameters (chunking.py module constants)
// ---------------------------------------------------------------------------
constexpr std::uint64_t kUint64Mask = ~(std::uint64_t)0;

constexpr std::size_t kMinChunk   = (std::size_t)1 << 20;   // 1 MB
constexpr std::size_t kAvgChunk   = (std::size_t)8 << 20;   // 8 MB
constexpr std::size_t kMaxChunk   = (std::size_t)64 << 20;  // 64 MB
constexpr std::uint64_t kMask     = kAvgChunk - 1;           // AVG_CHUNK - 1

constexpr int               kNormalizationLevel = 2;
constexpr std::size_t       kWindowSize         = 64;
constexpr std::uint64_t     kRollingBase        = 257;
constexpr std::uint64_t     kRollingMod         = ((std::uint64_t)1 << 61) - 1;

// Storage layout (file-only).
const std::string kChunkStorageDir = "dsi_vcs_chunks";
constexpr std::size_t kExistenceCacheCap = 8192;   // CHUNK_EXISTENCE_CACHE_SIZE

constexpr std::size_t bitLength(std::uint64_t v) {
    return v == 0 ? 0 : 64 - (std::size_t)__builtin_clzll(v);
}
const std::size_t kAverageBits = bitLength(kAvgChunk) - 1;

bool validateParams() {
    if (!(0 < kMinChunk && kMinChunk <= kAvgChunk && kAvgChunk <= kMaxChunk)) {
        throw std::runtime_error(
            "Chunk sizes must satisfy 0 < MIN_CHUNK <= AVG_CHUNK <= MAX_CHUNK");
    }
    if (kAvgChunk & (kAvgChunk - 1)) {
        throw std::runtime_error("AVG_CHUNK must be a power of two");
    }
    if ((int)kAverageBits - kNormalizationLevel < 1) {
        throw std::runtime_error("NORMALIZATION_LEVEL is too large for AVG_CHUNK");
    }
    if ((int)kAverageBits + kNormalizationLevel > 64) {
        throw std::runtime_error(
            "NORMALIZATION_LEVEL produces a mask larger than 64 bits");
    }
    return true;
}

// Raises (at load time) if the constants are inconsistent, like
// chunking.py's module-level _validate_parameters() call.
[[maybe_unused]] const bool kParamsValidated = validateParams();

std::uint64_t makeSpreadMask(int bitCount) {
    if (bitCount < 1 || bitCount > 64) {
        throw std::runtime_error("bit_count must be between 1 and 64");
    }
    if (bitCount == 1) {
        return (std::uint64_t)1 << 31;
    }
    std::uint64_t mask = 0;
    for (int index = 0; index < bitCount; ++index) {
        const int position = (int)(((std::uint64_t)index * 63) /
                                   (std::uint64_t)(bitCount - 1));
        mask |= (std::uint64_t)1 << position;
    }
    return mask;
}

const std::uint64_t kStrictMask = makeSpreadMask((int)kAverageBits + kNormalizationLevel);
const std::uint64_t kEagerMask  = makeSpreadMask((int)kAverageBits - kNormalizationLevel);

// ---------------------------------------------------------------------------
// Gear tables -- deterministic SplitMix64, exactly mirroring
// _build_gear_table() / GEAR_TABLE / GEAR_TABLE_LS.
// ---------------------------------------------------------------------------
std::array<std::uint64_t, 256> buildGearTable() {
    std::array<std::uint64_t, 256> table{};
    std::uint64_t state = 0xD51FC0DEC0FFEE01ULL;
    for (std::uint64_t& slot : table) {
        state += 0x9E3779B97F4A7C15ULL;
        std::uint64_t value = state;
        value = (value ^ (value >> 30)) * 0xBF58476D1CE4E5B9ULL;
        value = (value ^ (value >> 27)) * 0x94D049BB133111EBULL;
        value ^= value >> 31;
        slot = value;
    }
    return table;
}

const std::array<std::uint64_t, 256> kGearTable = buildGearTable();

// GEAR_TABLE_LS[i] = GEAR_TABLE[i] << 1 in Python's arbitrary precision, i.e.
// bit 64 is preserved (the first-byte boundary mask is shifted by one bit and
// must test it).  Stored in 128-bit so nothing is truncated.
const std::array<unsigned __int128, 256> kGearTableLs = [] {
    std::array<unsigned __int128, 256> ls{};
    for (std::size_t i = 0; i < kGearTable.size(); ++i) {
        ls[i] = (unsigned __int128)kGearTable[i] << 1;
    }
    return ls;
}();

// ---------------------------------------------------------------------------
// RollingHash -- classic rolling hash over a 64-byte window, bit-for-bit
// equivalent to chunking.py's RollingHash.  The per-byte hash stays below
// 2^61-1, so 128-bit intermediates reproduce Python's exact results.
// ---------------------------------------------------------------------------
class RollingHash {
public:
    explicit RollingHash(std::size_t windowSize)
        : windowSize_(windowSize),
          basePower_(modPow(kRollingBase, windowSize == 0 ? 0 : windowSize - 1,
                            kRollingMod)) {}

    std::uint64_t slide(std::uint8_t byte) {
        if (window_.size() == windowSize_) {
            const std::uint8_t outgoing = window_.front();
            window_.pop_front();
            // Python: self.hash = (self.hash - outgoing * self.base_power) % MOD
            const std::uint64_t sub =
                (std::uint64_t)(((unsigned __int128)outgoing * basePower_) %
                                kRollingMod);
            hash_ = hash_ >= sub ? hash_ - sub : hash_ + kRollingMod - sub;
        }
        window_.push_back(byte);
        // Python: self.hash = (self.hash * BASE + byte) % MOD
        hash_ = (std::uint64_t)(((unsigned __int128)hash_ * kRollingBase + byte) %
                                kRollingMod);
        return hash_;
    }

private:
    static std::uint64_t modPow(std::uint64_t base, std::uint64_t exp,
                                std::uint64_t mod) {
        unsigned __int128 result = 1;
        unsigned __int128 b = base % mod;
        while (exp > 0) {
            if (exp & 1) result = (result * b) % mod;
            b = (b * b) % mod;
            exp >>= 1;
        }
        return (std::uint64_t)result;
    }

    std::size_t             windowSize_;
    std::deque<std::uint8_t> window_;
    std::uint64_t           hash_ = 0;
    std::uint64_t           basePower_;
};

// ---------------------------------------------------------------------------
// _find_fastcdc_cut -- two-region FastCDC scan with the shifted Gear table.
//
// Python's unmasked intermediate fingerprint is (fingerprint << 2) +
// GEAR_TABLE_LS[byte], which can reach 67 bits before the code masks it back to
// 64 at the pair-completion step.  Holding the intermediate in unsigned
// __int128 (and the LS table / shifted mask in 128 bits) reproduces the
// arbitrary-precision arithmetic exactly.
// ---------------------------------------------------------------------------
std::size_t findFastCdcCut(const std::uint8_t* buffer, std::size_t start,
                           std::size_t available) {
    if (available == 0) {
        return 0;
    }

    const std::size_t scanLength = std::min(available, kMaxChunk);

    // The final chunk may be smaller than MIN_CHUNK.
    if (scanLength <= kMinChunk) {
        return scanLength;
    }

    const std::size_t normalEnd = std::min(kAvgChunk, scanLength);

    std::uint64_t fingerprint = 0;
    std::size_t position = start + kMinChunk;

    const std::pair<std::size_t, std::uint64_t> regions[2] = {
        {start + normalEnd, kStrictMask},
        {start + scanLength, kEagerMask},
    };

    for (const auto& region : regions) {
        const std::size_t end = region.first;
        const std::uint64_t mask = region.second;

        const unsigned __int128 shiftedMask = (unsigned __int128)mask << 1;
        const std::size_t pairEnd = end - 1;

        while (position < pairEnd) {
            // Twice the first byte's fingerprint: the shifted mask checks the
            // same bits without a separate one-byte hash update.
            const unsigned __int128 shifted =
                ((unsigned __int128)fingerprint << 2) +
                kGearTableLs[buffer[position]];
            if ((shifted & shiftedMask) == 0) {
                return position - start;
            }

            // Complete the pair and restore the normal 64-bit fingerprint.
            fingerprint = (std::uint64_t)((shifted +
                                           kGearTable[buffer[position + 1]]) &
                                          kUint64Mask);
            if ((fingerprint & mask) == 0) {
                return position + 1 - start;
            }
            position += 2;
        }

        // Do not let a pair cross the normalization transition or file end.
        if (position < end) {
            // _gear_update: ((fingerprint << 1) + GEAR_TABLE[b]) & UINT64_MASK.
            // uint64 arithmetic wraps, which is exactly that mask.
            fingerprint = (fingerprint << 1) + kGearTable[buffer[position]];
            fingerprint &= kUint64Mask;
            if ((fingerprint & mask) == 0) {
                return position - start;
            }
            position += 1;
        }
    }

    // No content-defined boundary was found.
    return scanLength;
}

// ---------------------------------------------------------------------------
// POSIX helpers
// ---------------------------------------------------------------------------
void mkdirP(const std::string& dir) {
    std::string cur;
    std::size_t pos = 0;
    while (pos < dir.size()) {
        std::size_t next = dir.find('/', pos);
        if (next == std::string::npos) {
            next = dir.size();
        }
        cur.append(dir, pos, next - pos);
        if (cur.size() > 1) {
            if (::mkdir(cur.c_str(), 0777) != 0 && errno != EEXIST) {
                throw std::runtime_error("mkdir " + cur + ": " +
                                         std::strerror(errno));
            }
        }
        cur.push_back('/');
        pos = next + 1;
    }
}

std::string dirNameOf(const std::string& path) {
    const std::size_t slash = path.find_last_of('/');
    if (slash == std::string::npos) {
        return std::string();  // no directory component
    }
    return path.substr(0, slash);
}

bool pathExists(const std::string& path) {
    struct ::stat st {};
    return ::stat(path.c_str(), &st) == 0;
}

// Atomic blob publication: temp file in the chunk dir (mkstemp) + chmod 0644 +
// write + fsync + close + rename, mirroring the Python blob writer order.
// Any failure removes the temp file and throws.
void writeChunkBlob(const std::string& chunkDir, const std::string& digest,
                    const char* data, std::size_t len) {
    const std::string chunkPath = chunkDir + "/" + digest;
    std::string tmpl = chunkDir + "/." + digest + ".tmpXXXXXX";
    std::vector<char> tmplBuf(tmpl.begin(), tmpl.end());
    tmplBuf.push_back('\0');

    int fd = ::mkstemp(tmplBuf.data());
    if (fd < 0) {
        throw std::runtime_error(std::string("mkstemp for chunk failed: ") +
                                 std::strerror(errno));
    }
    const std::string tempPath(tmplBuf.data());

    auto fail = [&](const char* what) {
        if (fd >= 0) {
            ::close(fd);
        }
        ::unlink(tempPath.c_str());
        throw std::runtime_error(std::string("writing chunk blob failed (") +
                                 what + "): " + std::strerror(errno));
    };

    if (::fchmod(fd, 0644) != 0) fail("fchmod");

    std::size_t written = 0;
    while (written < len) {
        const ssize_t n = ::write(fd, data + written, len - written);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            fail("write");
        }
        written += (std::size_t)n;
    }
    if (::fsync(fd) != 0) fail("fsync");
    if (::close(fd) != 0) {
        fd = -1;
        fail("close");
    }
    fd = -1;
    if (::rename(tempPath.c_str(), chunkPath.c_str()) != 0) fail("rename");
}

// LRU cache of digest -> blob-exists, mirroring the existence_cache /
// chunk_exists() / remember_exists() trio in store_chunks_for_snapshot().
class ExistenceCache {
public:
    std::optional<bool> get(const std::string& digest) {
        auto it = map_.find(digest);
        if (it == map_.end()) return std::nullopt;
        order_.splice(order_.begin(), order_, it->second);
        return it->second->second;
    }

    void put(const std::string& digest, bool exists) {
        auto it = map_.find(digest);
        if (it != map_.end()) {
            it->second->second = exists;
            order_.splice(order_.begin(), order_, it->second);
            return;
        }
        order_.emplace_front(digest, exists);
        map_[digest] = order_.begin();
        if (order_.size() > kExistenceCacheCap) {
            map_.erase(order_.back().first);
            order_.pop_back();
        }
    }

private:
    std::list<std::pair<std::string, bool>> order_;
    std::unordered_map<std::string,
                       std::list<std::pair<std::string, bool>>::iterator>
        map_;
};

// Read a whole blob (used only by rebuild).
std::string readWholeFile(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("open " + path + " failed");
    std::string out((std::istreambuf_iterator<char>(in)),
                    std::istreambuf_iterator<char>());
    return out;
}

}  // namespace

std::vector<SizeRange> findChunkBoundaries(const std::uint8_t* data,
                                           std::size_t size) {
    std::vector<SizeRange> ranges;
    std::size_t pos = 0;
    while (pos < size) {
        const std::size_t chunkSize = findFastCdcCut(data, pos, size - pos);
        if (chunkSize == 0) {
            throw std::runtime_error("FastCDC produced an invalid chunk size");
        }
        ranges.push_back({pos, chunkSize});
        pos += chunkSize;
    }
    return ranges;
}

std::vector<std::string> hashRangesParallel(const std::uint8_t* base,
                                            const std::vector<SizeRange>& ranges,
                                            std::size_t threads) {
    const std::size_t n = ranges.size();
    std::vector<std::string> hashes(n);
    if (n == 0) return hashes;

    std::size_t workerCount = threads;
    if (workerCount == 0) {
        workerCount = std::thread::hardware_concurrency();
    }
    if (workerCount <= 1 || n < 2) {
        for (std::size_t i = 0; i < n; ++i) {
            hashes[i] = sha256Hex(base + ranges[i].offset, ranges[i].length);
        }
        return hashes;
    }

    // Dynamic work-stealing over the chunk list; each pthread sha256's whole
    // MB-sized chunks, so contention on the atomic counter is negligible.
    std::atomic<std::size_t> next{0};
    auto worker = [&] {
        for (;;) {
            const std::size_t i = next++;
            if (i >= n) break;
            hashes[i] = sha256Hex(base + ranges[i].offset, ranges[i].length);
        }
    };

    const std::size_t w = std::min(workerCount, n);
    std::vector<std::thread> pool;
    pool.reserve(w);
    for (std::size_t i = 0; i < w; ++i) pool.emplace_back(worker);
    for (auto& t : pool) t.join();
    return hashes;
}

// ===========================================================================
// mmap-backed file view
// ===========================================================================
MappedChunkFile::~MappedChunkFile() { close(); }

MappedChunkFile::MappedChunkFile(MappedChunkFile&& o) noexcept
    : fd_(o.fd_), addr_(o.addr_), len_(o.len_) {
    o.fd_ = -1;
    o.addr_ = MAP_FAILED;
    o.len_ = 0;
}

MappedChunkFile& MappedChunkFile::operator=(MappedChunkFile&& o) noexcept {
    if (this != &o) {
        close();
        fd_ = o.fd_;
        addr_ = o.addr_;
        len_ = o.len_;
        o.fd_ = -1;
        o.addr_ = MAP_FAILED;
        o.len_ = 0;
    }
    return *this;
}

void MappedChunkFile::open(const std::string& path) {
    close();
    fd_ = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd_ < 0) {
        throw std::runtime_error("open " + path + ": " + std::strerror(errno));
    }
    struct ::stat st {};
    if (::fstat(fd_, &st) != 0) {
        const int saved = errno;
        close();
        throw std::runtime_error("fstat " + path + ": " +
                                 std::strerror(saved));
    }
    // mmap cannot map an empty file; keep fd open but map nothing.
    if (st.st_size == 0) return;

    len_ = (std::size_t)st.st_size;
    addr_ = ::mmap(nullptr, len_, PROT_READ, MAP_PRIVATE, fd_, 0);
    if (addr_ == MAP_FAILED) {
        const int saved = errno;
        addr_ = MAP_FAILED;
        len_ = 0;
        close();
        throw std::runtime_error("mmap " + path + ": " + std::strerror(saved));
    }
#if defined(MADV_RANDOM)
    ::madvise(addr_, len_, MADV_RANDOM);
#endif
}

const std::uint8_t* MappedChunkFile::data() const {
    return isMapped() ? static_cast<const std::uint8_t*>(addr_) : nullptr;
}

void MappedChunkFile::close() {
    if (addr_ != MAP_FAILED) {
        ::munmap(addr_, len_);
        addr_ = MAP_FAILED;
        len_ = 0;
    }
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

// ===========================================================================
// Streaming iterator (port of iter_file_fastcdc)
// ===========================================================================
FastCdcChunkIterator::FastCdcChunkIterator(std::string path, NeedsData needs)
    : needs_(std::move(needs)) {
    map_.open(path);
    fileSize_ = map_.size();
}

FastCdcChunkIterator::~FastCdcChunkIterator() { close(); }

bool FastCdcChunkIterator::hasMore() const {
    return !closed_ && pos_ < fileSize_;
}

Chunk FastCdcChunkIterator::next() {
    if (!hasMore()) {
        throw std::runtime_error("FastCdcChunkIterator exhausted");
    }
    const std::size_t chunkSize =
        findFastCdcCut(map_.data(), pos_, fileSize_ - pos_);
    if (chunkSize == 0) {
        throw std::runtime_error("FastCDC produced an invalid chunk size");
    }
    const std::uint8_t* begin = map_.data() + pos_;

    Chunk chunk;
    chunk.sha256 = sha256Hex(begin, chunkSize);
    chunk.size = chunkSize;
    if (!needs_ || needs_(chunk.sha256)) {
        chunk.data = std::string(reinterpret_cast<const char*>(begin), chunkSize);
    }
    pos_ += chunkSize;
    return chunk;
}

void FastCdcChunkIterator::close() {
    closed_ = true;
    map_.close();
}

std::vector<Chunk> FastCdcChunkIterator::materialize() {
    std::vector<Chunk> out;
    while (hasMore()) out.push_back(next());
    close();
    return out;
}

// ===========================================================================
// High-level entry points
// ===========================================================================
std::vector<Chunk> chunkFileFastCdc(const std::string& path) {
    FastCdcChunkIterator it(path);
    return it.materialize();
}

std::vector<Chunk> chunkFile(const std::string& path, NeedsData needs) {
    FastCdcChunkIterator it(path, std::move(needs));
    return it.materialize();
}

void chunkFileStream(const std::string& path, NeedsData needs,
                     const std::function<void(const Chunk&)>& cb) {
    FastCdcChunkIterator it(path, std::move(needs));
    while (it.hasMore()) cb(it.next());
    it.close();
}

std::vector<Chunk> chunkFileRollingHash(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("open " + path + " failed");
    }
    std::string blob((std::istreambuf_iterator<char>(in)),
                     std::istreambuf_iterator<char>());

    RollingHash rh(kWindowSize);
    std::vector<Chunk> chunks;
    std::string current;
    current.reserve(kAvgChunk);

    for (const char c : blob) {
        const std::uint8_t value = static_cast<std::uint8_t>(c);
        current.push_back(c);
        const std::uint64_t h = rh.slide(value);
        const std::size_t size = current.size();

        // Wait until minimum chunk size.
        if (size < kMinChunk) continue;

        // CDC boundary (mask hit) or forced boundary.
        if (((h & kMask) == 0) || size >= kMaxChunk) {
            chunks.push_back(
                {sha256Hex(current.data(), size), size, std::move(current)});
            current.clear();
        }
    }

    // Final chunk.
    if (!current.empty()) {
        chunks.push_back({sha256Hex(current.data(), current.size()),
                          current.size(), std::move(current)});
    }
    return chunks;
}

std::vector<Chunk> chunkFileParallel(const std::string& path,
                                     ParallelOptions opt) {
    MappedChunkFile map;
    map.open(path);
    if (map.size() == 0) return {};

    const std::vector<SizeRange> ranges = findChunkBoundaries(map.data(), map.size());
    const std::vector<std::string> hashes =
        hashRangesParallel(map.data(), ranges, opt.threads);

    std::vector<Chunk> chunks(ranges.size());
    for (std::size_t i = 0; i < ranges.size(); ++i) {
        chunks[i].sha256 = hashes[i];
        chunks[i].size = ranges[i].length;
        if (opt.requestData) {
            chunks[i].data = std::string(
                reinterpret_cast<const char*>(map.data() + ranges[i].offset),
                ranges[i].length);
        }
    }
    return chunks;
}

// ===========================================================================
// File-only chunk storage (SQL database logic removed)
// ===========================================================================
StoreResult storeChunksForSnapshot(const std::string& chunkRoot,
                                   const std::vector<FileEntry>& entries,
                                   const std::string& pendingCommit,
                                   std::size_t threads) {
    (void)pendingCommit;  // SQL-only parameter, kept for signature parity.

    const std::string chunkDir = chunkRoot + "/" + kChunkStorageDir;
    mkdirP(chunkDir);

    StoreResult res;
    ExistenceCache cache;

    auto chunkExists = [&](const std::string& digest) -> bool {
        if (auto cached = cache.get(digest)) {
            return *cached;
        }
        const bool exists = pathExists(chunkDir + "/" + digest);
        cache.put(digest, exists);
        return exists;
    };

    auto remember = [&](const std::string& digest, bool exists) {
        cache.put(digest, exists);
    };

    for (const FileEntry& entry : entries) {
        if (entry.fileType != "file") continue;

        const std::string& fileRel = entry.relativePath;
        Sha256Ctx h;
        std::size_t count = 0;

        auto body = [&](const Chunk& chunk) {
            ++count;
            h.update(chunk.sha256);
            // Blob half of the Python writer: publish only payloads whose
            // digest is not already on disk. (The INSERT side is gone.)
            if (chunk.data && !chunkExists(chunk.sha256)) {
                writeChunkBlob(chunkDir, chunk.sha256, chunk.data->data(),
                               chunk.data->size());
                // Cache presence only after the chunk has been published.
                remember(chunk.sha256, true);
            }
            res.chunkHashes.insert(chunk.sha256);
        };

        if (threads > 1) {
            // Multicore hashing path (same chunks, same boundaries; the per-
            // file hash is over the ordered hex digests either way).
            ParallelOptions po;
            po.threads = threads;
            po.requestData = true;
            const std::vector<Chunk> all = chunkFileParallel(entry.absolutePath, po);
            for (const Chunk& chunk : all) body(chunk);
        } else {
            // Faithful streaming path: skip payloads already in the store.
            chunkFileStream(entry.absolutePath,
                            [&](const std::string& digest) {
                                return !chunkExists(digest);
                            },
                            body);
        }

        res.fileHashes[fileRel] = h.hexdigest();
        res.chunkLength[fileRel] = count;
    }
    return res;
}

bool rebuildFileFromChunks(const std::string& chunkRoot,
                           const std::string& relativePath,
                           const std::string& commitHash,
                           const std::string& fileHash,
                           const std::string& outputPath,
                           const std::vector<std::string>& orderedChunkHashes) {
    (void)commitHash;  // no DB to filter on; kept for signature parity.
    const std::string chunkDir = chunkRoot + "/" + kChunkStorageDir;

    try {
        // Verify the order of chunk digests matches the file hash first.
        Sha256Ctx chunkHashDigest;
        for (const std::string& hash : orderedChunkHashes) {
            chunkHashDigest.update(hash);
        }
        if (chunkHashDigest.hexdigest() != fileHash) {
            return false;
        }

        const std::string target = outputPath + "/" + relativePath;
        const std::string targetDir = dirNameOf(target);
        if (!targetDir.empty()) mkdirP(targetDir);

        std::ofstream out(target, std::ios::binary | std::ios::trunc);
        if (!out) return false;

        for (const std::string& hash : orderedChunkHashes) {
            const std::string chunkPath = chunkDir + "/" + hash;
            if (!pathExists(chunkPath)) return false;
            std::string content = readWholeFile(chunkPath);
            out.write(content.data(), (std::streamsize)content.size());
            if (!out) return false;
        }
        return out.good();
    } catch (const std::exception&) {
        return false;
    }
}

ChunkerDiagnostics chunkerDiagnostics() {
    ChunkerDiagnostics d;
    d.gearTable = kGearTable;
    d.strictMask = kStrictMask;
    d.eagerMask = kEagerMask;
    d.averageBits = (int)kAverageBits;
    d.gearTableLsWide = kGearTableLs;
    for (std::size_t i = 0; i < kGearTable.size(); ++i) {
        d.gearTableLs[i] = (std::uint64_t)kGearTableLs[i];  // low 64 bits
    }
    return d;
}

}  // namespace chunking
