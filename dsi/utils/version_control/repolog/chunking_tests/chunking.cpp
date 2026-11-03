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
#include <cmath>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <deque>
#include <fstream>
#include <list>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
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

// HyperLogLog parameters: the top kHllPrecision bits of the 64-bit hash pick
// the register (4096 one-byte registers, ~1.6% standard error).  The
// existence cache uses the *register value* as its fast path -- a value of
// zero means the digest was never added to that register, which is
// definitive ("hash not exist"); see ExistenceCache below.

// HyperLogLog parameters: the top kHllPrecision bits of the 64-bit hash pick
// the register (4096 one-byte registers, ~1.6% standard error).
constexpr int kHllPrecision = 12;
constexpr std::size_t kHllRegisters = (std::size_t)1 << kHllPrecision;

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

// ---------------------------------------------------------------------------
// Sketch primitives used by the existence cache: a salted 64-bit digest
// hash and a HyperLogLog counter.
// ---------------------------------------------------------------------------
std::uint64_t hashDigest(const std::string& digest, std::uint64_t salt) {
    std::uint64_t h = 14695981039346656037ULL;  // FNV-1a 64 offset basis
    h ^= salt * 0x9E3779B97F4A7C15ULL;
    for (const unsigned char byte : digest) {
        h ^= byte;
        h *= 0x100000001B3ULL;
    }
    // splitmix64 finalizer, to even out FNV's bit distribution.
    h ^= h >> 33; h *= 0xFF51AFD7ED558CCDULL;
    h ^= h >> 33; h *= 0xC4CEB9FE1A85EC53ULL;
    h ^= h >> 33;
    return h;
}

// HyperLogLog over the same digest hashes: estimates how many DISTINCT
// digests the cache has recorded.  The running sum is maintained
// incrementally so estimate() is O(1).  Per-register lookups (registerAt)
// double as the existence cache's "was this hash ever seen" probe: once a
// register is non-zero it can only hold larger ranks, never clear, so a
// zero register is definitive ("this hash was never added here").
class HyperLogLog {
public:
    void add(std::uint64_t h) {
        const std::size_t index = (std::size_t)(h >> (64 - kHllPrecision));
        const std::uint64_t rest = h << kHllPrecision;
        const std::size_t rank = rest == 0
            ? 64 - kHllPrecision + 1
            : (std::size_t)__builtin_clzll(rest) - kHllPrecision + 1;

        const std::size_t current = registers_[index];
        if (rank <= current) return;
        if (current == 0) {
            --zeros_;
            sum_ -= 1.0;
        }
        registers_[index] = (std::uint8_t)rank;
        sum_ += std::ldexp(1.0, -(std::int64_t)rank);
    }

    // The raw register this hash maps to.  Zero means the hash was never
    // added (or no other hash sharing the register was); non-zero means
    // *some* hash in this bucket reached that rank, so it proves nothing
    // beyond "possible member" -- the higher layers verify.
    std::size_t registerAt(std::uint64_t h) const {
        return registers_[h >> (64 - kHllPrecision)];
    }

    double estimate() const {
        if (zeros_ == kHllRegisters) return 0.0;
        const double m = (double)kHllRegisters;
        const double alpha = 0.7213 / (1.0 + 1.079 / m);
        const double raw = alpha * m * m / sum_;
        // Linear counting for the small-range regime (only while there are
        // still empty registers; with all registers non-zero the raw
        // estimate is authoritative and m/log(0) would blow up).
        if (raw <= 2.5 * m && zeros_ > 0) {
            return m * std::log(m / (double)zeros_);
        }
        return raw;
    }

    // Raw register array (for persistence save).
    const std::array<std::uint8_t, kHllRegisters>& registers() const {
        return registers_;
    }

    // Replace the register array wholesale (persistence load) and rebuild
    // the incrementally maintained sum / zero count from it, so the
    // estimate() fast path keeps working after a reload.
    void assignRegisters(const std::array<std::uint8_t, kHllRegisters>& regs) {
        registers_ = regs;
        sum_ = 0.0;
        zeros_ = 0;
        for (const std::uint8_t rank : registers_) {
            sum_ += std::ldexp(1.0, -(std::int64_t)rank);
            if (rank == 0) ++zeros_;
        }
    }

private:
    std::array<std::uint8_t, kHllRegisters> registers_{};
    double sum_ = 0.0;
    std::size_t zeros_ = kHllRegisters;
};

// ---------------------------------------------------------------------------
// HyperLogLog persistence: the "seen" sketch is stored next to the chunk
// blobs and reloaded at the start of each storeChunksForSnapshot() call, so
// the register-probe fast path (and the distinct-count estimate) survive
// across runs.
//
// File format: an 8-byte magic ("RLOGHLL1") followed by the raw
// kHllRegisters one-byte register array (4104 bytes total).  The derived
// running sum / zero count are recomputed on load, so the file stays a flat
// register dump.
//
// Losing the file (or loading a corrupt one) is safe: a fresh sketch marks
// previously-recorded registers as zero, which answers "not exist" and at
// worst re-publishes an identical blob atomically -- it can never skip a
// needed write.
// ---------------------------------------------------------------------------
constexpr char kHllMagic[8] = {'R', 'L', 'O', 'G', 'H', 'L', 'L', '1'};
const std::string kHllFileName = "seen_hyperloglog.bin";

void saveHllState(const std::string& dir, const HyperLogLog& hll) {
    std::string tmpl = dir + "/.seen_hll.tmpXXXXXX";
    std::vector<char> tmplBuf(tmpl.begin(), tmpl.end());
    tmplBuf.push_back('\0');

    int fd = ::mkstemp(tmplBuf.data());
    if (fd < 0) {
        throw std::runtime_error(std::string("mkstemp for HLL state failed: ") +
                                 std::strerror(errno));
    }
    const std::string tempPath(tmplBuf.data());

    auto fail = [&](const char* what) {
        if (fd >= 0) {
            ::close(fd);
        }
        ::unlink(tempPath.c_str());
        throw std::runtime_error(std::string("writing HLL state failed (") +
                                 what + "): " + std::strerror(errno));
    };

    std::vector<std::uint8_t> blob;
    blob.reserve(sizeof(kHllMagic) + kHllRegisters);
    blob.insert(blob.end(), kHllMagic, kHllMagic + sizeof(kHllMagic));
    for (const std::uint8_t rank : hll.registers()) {
        blob.push_back(rank);
    }

    // Same publication order as the blob writer: fchmod 0644 + full write
    // (EINTR-safe) + fsync + close + atomic rename.
    if (::fchmod(fd, 0644) != 0) fail("fchmod");
    std::size_t written = 0;
    while (written < blob.size()) {
        const ssize_t n = ::write(fd, blob.data() + written, blob.size() - written);
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
    if (::rename(tempPath.c_str(), (dir + "/" + kHllFileName).c_str()) != 0) {
        fail("rename");
    }
}

// Load the HLL state saved by a previous call.  Returns false (leaving
// `hll` untouched, i.e. a fresh sketch) when the file is missing, an older
// format, or truncated.
bool loadHllState(const std::string& dir, HyperLogLog& hll) {
    std::ifstream in(dir + "/" + kHllFileName, std::ios::binary);
    if (!in) return false;

    char magic[sizeof(kHllMagic)];
    if (!in.read(magic, sizeof(magic)) ||
        std::memcmp(magic, kHllMagic, sizeof(magic)) != 0) {
        return false;
    }

    std::array<std::uint8_t, kHllRegisters> registers{};
    if (!in.read(reinterpret_cast<char*>(registers.data()),
                 (std::streamsize)registers.size())) {
        return false;
    }
    hll.assignRegisters(registers);
    return true;
}

// Existence + count cache for chunk digests.  It mirrors the same
// existence_cache() / chunk_exists() / remember_exists() trio in
// store_chunks_for_snapshot() as before, with these tiers (fastest first):
//
//   * HyperLogLog "seen_" register probe -- the top kHllPrecision bits of
//     the hashDigest() value pick one of 4096 registers.  HyperLogLog
//     registers never clear once non-zero, so a register of *zero* is
//     definitive: this digest is "not exist" -- no disk stat, no LRU work.
//     A *non-zero* register proves nothing per se (many digests share one
//     register, and ranks overwrite), so it merely falls through to the
//     next tier.
//   * LRU map -- the disk-verified answer for the digest from the last
//     time its blob was actually checked, capped at kExistenceCacheCap
//     entries (CHUNK_EXISTENCE_CACHE_SIZE) exactly like the OrderedDict
//     existence cache in chunking.py.
//   * Disk check -- only on an LRU miss: stat() the blob once, then
//     remember() the verified answer in the LRU (and mark the hash seen).
//
// The cache never answers "exists" without a disk-verified source behind
// it: that is the one direction whose failure mode is a silently skipped
// write.  A wrong "not exist" at worst re-publishes an identical blob
// atomically.
//   * Cost model per lookup: 1 salted hash of the digest (~64 FNV-1a
//     bytes) plus one register read; when the fast path misses, an O(1)
//     LRU probe, and only on a miss of *that*, one stat() + O(1) LRU put.
//     No allocation on the hot path: a fixed ~1.5 MB working set (LRU cap
//     + 4 KB of hyperlog registers) regardless of how many digests pass.
class ExistenceCache {
public:
    // DiskExists: definitive per-digest existence check against the store
    // (the caller owns the path layout); invoked at most once per digest,
    // then the answer is remembered in the LRU.
    using DiskExists = std::function<bool(const std::string&)>;

    explicit ExistenceCache(DiskExists diskExists)
        : diskExists_(std::move(diskExists)) {}

    // Definitive existence answer for `digest`.
    bool get(const std::string& digest) {
        const std::uint64_t h = hashDigest(digest, kSeenHashSalt);
        if (seen_.registerAt(h) == 0) {
            return false;  // hash not exist: the register was never touched
        }
        if (const bool* cached = lru_.get(digest)) {
            return *cached;  // disk-verified answer from the last check
        }
        // Seen but not verified in this generation's LRU: check the file
        // itself, once.
        const bool exists = diskExists_(digest);
        put(digest, exists);
        return exists;
    }

    void put(const std::string& digest, bool exists) {
        lru_.put(digest, exists);
        // A single HyperLogLog serves both jobs: registerAt(h) powers the
        // fast negative path in get(), and estimate() reports how many
        // distinct digests this cache has ever recorded.
        seen_.add(hashDigest(digest, kSeenHashSalt));
    }

    // Estimated number of distinct digests ever recorded by this cache.
    std::uint64_t estimatedDistinctCount() const {
        return (std::uint64_t)(seen_.estimate() + 0.5);
    }

    // Persistence hook: the store loads the sketch state saved by a
    // previous run into this HyperLogLog and saves it back afterwards.
    // Only the sketch is persisted -- the LRU is deliberately rebuilt
    // from one stat() per digest on demand.
    HyperLogLog& seenHll() { return seen_; }

private:
    static constexpr std::uint64_t kSeenHashSalt = 0x5E5E0C4DEBCB6937ULL;

    // LRU-ordered digest -> exists map, mirroring chunking.py's OrderedDict
    // existence_cache: puts and hits both bump recency; the oldest entry is
    // evicted past kExistenceCacheCap.  Every entry holds a disk-verified
    // answer (put() is only fed stat()/publish results), so a hit is
    // definitive.
    class Lru {
    public:
        void put(const std::string& digest, bool exists) {
            auto it = map_.find(digest);
            if (it != map_.end()) {
                it->second.first = exists;
                order_.splice(order_.end(), order_, it->second.second);
                return;
            }
            if (map_.size() >= kExistenceCacheCap) {
                map_.erase(order_.front());
                order_.pop_front();
            }
            order_.push_back(digest);
            map_.emplace(digest, std::make_pair(exists, std::prev(order_.end())));
        }

        const bool* get(const std::string& digest) {
            auto it = map_.find(digest);
            if (it == map_.end()) return nullptr;
            order_.splice(order_.end(), order_, it->second.second);
            return &it->second.first;
        }

    private:
        std::list<std::string> order_;                      // oldest -> newest
        std::unordered_map<std::string,
                           std::pair<bool, std::list<std::string>::iterator>>
            map_;
    };

    DiskExists diskExists_;
    Lru lru_;
    // One HyperLogLog for both roles: a zero register means the digest was
    // never added here (definitive negative), and estimate() reports the
    // cumulative count of distinct digests recorded.
    HyperLogLog seen_;
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
// Streaming iterator (port of iter_file_fastcdc)
//
// The file is read sequentially through a sliding window of at most
// kMaxChunk bytes -- exactly the FastCDC lookahead range -- so a cut is
// searched only within the current window.  On every cut the chunk is
// emitted immediately and the scan resumes right after it, so the fd
// advances front-to-back and the resident set stays bounded at one window
// even for files far larger than RAM (the whole-file mmap this replaces
// would pin the entire >100 GB image into the page cache / RSS).
// ===========================================================================
FastCdcChunkIterator::FastCdcChunkIterator(std::string path, NeedsData needs)
    : needs_(std::move(needs)) {
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
    fileSize_ = (std::size_t)st.st_size;
    buffer_.resize(kMaxChunk);
}

FastCdcChunkIterator::~FastCdcChunkIterator() { close(); }

bool FastCdcChunkIterator::hasMore() const {
    return !closed_ && pos_ < fileSize_;
}

void FastCdcChunkIterator::fillWindow() {
    // Re-anchor the window at pos_: keep the still-unconsumed tail (any
    // bytes past pos_) and slide it to the front of the buffer so the
    // already-read bytes are reused instead of re-read.
    if (pos_ > bufStart_) {
        const std::size_t keep = bufLen_ - (pos_ - bufStart_);
        std::memmove(buffer_.data(), buffer_.data() + (pos_ - bufStart_), keep);
        bufStart_ = pos_;
        bufLen_ = keep;
    }

    // Refill so the window spans [bufStart_, min(bufStart_+kMaxChunk, EOF)).
    const std::size_t want = std::min(kMaxChunk, fileSize_ - bufStart_);
    while (bufLen_ < want) {
        const ssize_t n = ::pread(fd_, buffer_.data() + bufLen_,
                                  want - bufLen_, (off_t)(bufStart_ + bufLen_));
        if (n < 0) {
            if (errno == EINTR) continue;
            close();
            throw std::runtime_error(
                std::string("reading chunk window failed: ") +
                std::strerror(errno));
        }
        if (n == 0) break;  // unexpected EOF; bufLen_ stays authoritative
        bufLen_ += (std::size_t)n;
    }
}

Chunk FastCdcChunkIterator::next() {
    if (!hasMore()) {
        throw std::runtime_error("FastCdcChunkIterator exhausted");
    }
    // The window holds the next chunk's full search space (or the file tail
    // if less than kMaxChunk remains); FastCDC returns the first cut in it.
    fillWindow();
    const std::size_t chunkSize = findFastCdcCut(buffer_.data(), 0, bufLen_);
    if (chunkSize == 0) {
        throw std::runtime_error("FastCDC produced an invalid chunk size");
    }

    const std::uint8_t* begin = buffer_.data();

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
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
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
    std::size_t workerCount = opt.threads;
    if (workerCount == 0) {
        workerCount = std::thread::hardware_concurrency();
    }
    if (workerCount == 0) workerCount = 1;

    FastCdcChunkIterator it(path);

    // Nothing to parallelize with a single worker: drain sequentially.
    if (workerCount == 1) {
        std::vector<Chunk> out = it.materialize();
        it.close();
        return out;
    }

    // Bounded producer/consumer pipeline: the producing thread still streams
    // the file sequentially (one sliding window), handing each freshly cut
    // chunk to a worker pool for hashing -- the whole file is never held in
    // RAM at once.  The in-flight queue is capped so peak memory is
    // ~cap * kMaxChunk, independent of the file size (this is what replaced
    // the old whole-file mmap path in the >100 GB regime).
    struct Item {
        std::size_t index = 0;
        Chunk       chunk;
        bool        stop = false;
    };

    std::mutex              mu;
    std::condition_variable cvFull, cvEmpty;
    std::deque<Item>        work;
    const std::size_t       cap = 2 * workerCount;

    auto push = [&](Item&& item) {
        std::unique_lock<std::mutex> lock(mu);
        cvFull.wait(lock, [&] { return work.size() < cap; });
        work.push_back(std::move(item));
        cvEmpty.notify_one();
    };
    auto pop = [&]() -> Item {
        std::unique_lock<std::mutex> lock(mu);
        cvEmpty.wait(lock, [&] { return !work.empty(); });
        Item item = std::move(work.front());
        work.pop_front();
        cvFull.notify_one();
        return item;
    };

    // Per-worker results, keyed by chunk index and merged in order after
    // the join (each index is written by exactly one thread, so the merge
    // after join() is race-free).
    struct Slot {
        std::size_t index = 0;
        std::string digest;
        std::size_t size = 0;
        std::optional<std::string> data;
    };
    std::vector<std::vector<Slot>> slots(workerCount);

    std::vector<std::thread> pool;
    pool.reserve(workerCount);
    for (std::size_t w = 0; w < workerCount; ++w) {
        pool.emplace_back([&, w] {
            std::vector<Slot>& mine = slots[w];
            for (;;) {
                Item item = pop();
                if (item.stop) return;
                Slot slot;
                slot.index = item.index;
                slot.digest = sha256Hex(item.chunk.data->data(),
                                        item.chunk.size);
                slot.size = item.chunk.size;
                if (opt.requestData) {
                    slot.data = std::move(item.chunk.data);
                }
                mine.push_back(std::move(slot));
            }
        });
    }

    std::size_t produced = 0;
    while (it.hasMore()) {
        Chunk chunk = it.next();  // payload materialised for hashing
        push({produced++, std::move(chunk), false});
    }
    it.close();
    for (std::size_t w = 0; w < workerCount; ++w) push(Item{0, Chunk{}, true});
    for (auto& t : pool) t.join();

    std::vector<Chunk> chunks(produced);
    for (std::vector<Slot>& mine : slots) {
        for (Slot& slot : mine) {
            chunks[slot.index].sha256 = std::move(slot.digest);
            chunks[slot.index].size = slot.size;
            if (opt.requestData) {
                chunks[slot.index].data = std::move(slot.data);
            }
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
    ExistenceCache cache([&](const std::string& digest) {
        return pathExists(chunkDir + "/" + digest);
    });

    // Persistence: reload the HyperLogLog sketch saved by the previous
    // call, so digests recorded by earlier runs still pass the register-
    // probe fast path (zero registers are non-zero there now) and the
    // distinct-count estimate stays cumulative.  A missing or unreadable
    // state file just starts the sketch fresh.
    loadHllState(chunkDir, cache.seenHll());

    auto chunkExists = [&](const std::string& digest) -> bool {
        return cache.get(digest);
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

    // Persist the HyperLogLog sketch for the next call.  Best-effort: a
    // failed save (full disk, read-only root, ...) must not fail a
    // snapshot that has already been stored correctly.
    try {
        saveHllState(chunkDir, cache.seenHll());
    } catch (const std::exception&) {
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
