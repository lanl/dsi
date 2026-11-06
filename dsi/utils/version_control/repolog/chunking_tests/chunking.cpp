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
//          rebuild() remains available for legacy callers. New callers receive
//          an immutable manifest root and stream reconstruction from that root.
//   * NPY_MAGIC / SQLITE_MAGIC magic detection (_has_npy_magic, is_sqlite_file)
//       -> dropped per the task; chunk_file always routes to FastCDC.
//   * CHUNK_INSERT_BATCH_SIZE -> SQL-only constant, dropped.
//
// SHA-256 is never hand-rolled here: sha256.hpp delegates to CommonCrypto
// (Apple default) or OpenSSL (-DUSE_OPENSSL).

#include "chunking.hpp"
#include "manifest.hpp"
#include "shared_existence_cache.hpp"
#include "worker_results.hpp"

#include "sha256.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <deque>
#include <fstream>
#include <filesystem>
#include <list>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/wait.h>
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
                           std::size_t available, bool includeMatchedByte = false) {
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
                return position - start + (includeMatchedByte ? 1 : 0);
            }

            // Complete the pair and restore the normal 64-bit fingerprint.
            fingerprint = (std::uint64_t)((shifted +
                                           kGearTable[buffer[position + 1]]) &
                                          kUint64Mask);
            if ((fingerprint & mask) == 0) {
                return position + 1 - start + (includeMatchedByte ? 1 : 0);
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
                return position - start + (includeMatchedByte ? 1 : 0);
            }
            position += 1;
        }
    }

    // No content-defined boundary was found.
    return scanLength;
}

// The process path includes the matching byte i in the emitted prefix, then
// treats [i + 1, unitEnd] as the tail. The legacy sequential path above keeps
// its Python-compatible convention. All ranges here use exclusive ends.
std::vector<SizeRange> findUnitBoundaries(const std::uint8_t* data,
                                          std::size_t size) {
    std::vector<SizeRange> ranges;
    std::size_t start = 0;
    while (start < size) {
        const std::size_t length = findFastCdcCut(data, start, size - start, true);
        // Cancel this cut if it would leave an undersized tail. Keep those
        // bytes in the preceding chunk within this same processing unit.
        if (size - start - length < kMinChunk) {
            ranges.push_back({start, size - start});
            break;
        }
        ranges.push_back({start, length});
        start += length;
        if (size - start <= 2 * kMinChunk) {
            if (start < size) ranges.push_back({start, size - start});
            break;
        }
    }
    return ranges;
}

// EINTR/short-transfer safe I/O, shared by the process worker protocol and
// independent sequential reads of individual file units.
void readFileRange(int fd, void* data, std::size_t size, std::size_t offset) {
    auto* bytes = static_cast<std::uint8_t*>(data);
    std::size_t done = 0;
    while (done < size) {
        const ssize_t got = ::pread(fd, bytes + done, size - done,
                                     static_cast<off_t>(offset + done));
        if (got < 0 && errno == EINTR) continue;
        if (got < 0) throw std::runtime_error("read file unit: " + std::string(std::strerror(errno)));
        if (got == 0) throw std::runtime_error("unexpected EOF reading file unit");
        done += static_cast<std::size_t>(got);
    }
}

void transferPipe(int fd, void* data, std::size_t size, bool writing) {
    auto* bytes = static_cast<std::uint8_t*>(data);
    std::size_t done = 0;
    while (done < size) {
        const ssize_t got = writing ? ::write(fd, bytes + done, size - done)
                                    : ::read(fd, bytes + done, size - done);
        if (got < 0 && errno == EINTR) continue;
        if (got < 0) throw std::runtime_error("worker pipe: " + std::string(std::strerror(errno)));
        if (got == 0) throw std::runtime_error("FastCDC worker closed its result pipe unexpectedly");
        done += static_cast<std::size_t>(got);
    }
}

// Own only this call's children and descriptors. On errors, stop every child
// before reaping it so a worker blocked on a full pipe cannot hang cleanup.
struct ProcessWorkers {
    struct Worker { int input = -1; int output = -1; pid_t pid = -1; };
    int file = -1;
    std::vector<Worker> workers;

    ~ProcessWorkers() {
        for (auto& worker : workers) {
            if (worker.input >= 0) ::close(worker.input);
            if (worker.output >= 0) ::close(worker.output);
            if (worker.pid > 0) ::kill(worker.pid, SIGKILL);
        }
        for (auto& worker : workers) {
            if (worker.pid > 0) {
                while (::waitpid(worker.pid, nullptr, 0) < 0 && errno == EINTR) {}
            }
        }
        if (file >= 0) ::close(file);
    }
};

struct ChunkReply {
    std::uint64_t offset = 0, length = 0;
    std::array<char, 64> hash{};
};

void runUnitWorker(int fd, int output, std::size_t worker,
                   std::size_t workers, std::size_t units,
                   std::size_t fileSize, bool requestData) {
    std::vector<std::uint8_t> buffer(std::min(kMaxChunk, fileSize));
    for (std::size_t unit = worker; unit < units; unit += workers) {
        const std::size_t offset = unit * kMaxChunk;
        const std::size_t size = std::min(kMaxChunk, fileSize - offset);
        readFileRange(fd, buffer.data(), size, offset);
        const auto ranges = findUnitBoundaries(buffer.data(), size);
        for (const auto& range : ranges) {
            ChunkReply reply{offset + range.offset, range.length, {}};
            const auto hash = sha256Hex(buffer.data() + range.offset, range.length);
            std::copy(hash.begin(), hash.end(), reply.hash.begin());
            transferPipe(output, &reply, sizeof reply, true);
            if (requestData) {
                transferPipe(output, buffer.data() + range.offset, range.length, true);
            }
        }
    }
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
    detail::publishChunkBlob(chunkDir, digest, data, len);
}

// ---------------------------------------------------------------------------
// Sketch primitives used by the existence cache: a salted 64-bit digest
// hash and a HyperLogLog counter.
// ---------------------------------------------------------------------------
std::uint64_t hashDigest(const std::string& digest, std::uint64_t salt) {
    return detail::cacheDigestHash(digest, salt);
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
            : (std::size_t)__builtin_clzll(rest) + 1;

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
    if (std::any_of(registers.begin(), registers.end(), [](auto rank) { return rank > 53; })) return false;
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

// Metadata-only direct-storage worker protocol. The public materializing
// chunkFileParallel API below remains available for callers requesting bytes.
struct StoreTask { std::uint64_t kind = 0, offset = 0, bytes = 0; }; // 0 stop, 1 scan, 2 finalize
struct StoreRecord {
    std::uint64_t offset = 0, bytes = 0, ready = 0;
    std::array<char, 64> hash{};
};
struct StoreReply { std::uint64_t error = 0, count = 0; };

void transferSocket(int fd, void* data, std::size_t size, bool writing) {
    auto* bytes = static_cast<char*>(data);
    int flags = 0;
#ifdef MSG_NOSIGNAL
    flags = MSG_NOSIGNAL;
#endif
    while (size) {
        const auto n = writing ? ::send(fd, bytes, size, flags) : ::recv(fd, bytes, size, 0);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) throw std::runtime_error("storage worker transport failed/closed");
        bytes += n; size -= static_cast<std::size_t>(n);
    }
}

bool sameInput(const struct stat& a, const struct stat& b) {
#ifdef __APPLE__
    const auto am = a.st_mtimespec, bm = b.st_mtimespec;
    const auto ac = a.st_ctimespec, bc = b.st_ctimespec;
#else
    const auto am = a.st_mtim, bm = b.st_mtim;
    const auto ac = a.st_ctim, bc = b.st_ctim;
#endif
    return a.st_dev == b.st_dev && a.st_ino == b.st_ino && a.st_size == b.st_size &&
           am.tv_sec == bm.tv_sec && am.tv_nsec == bm.tv_nsec &&
           ac.tv_sec == bc.tv_sec && ac.tv_nsec == bc.tv_nsec;
}

struct Descriptor {
    int fd = -1;
    ~Descriptor() { if (fd >= 0) ::close(fd); }
};

void directStoreWorker(int socket, const struct stat& expected,
                       detail::SharedExistenceCache& cache) {
    std::uint64_t nameSize = 0;
    transferSocket(socket, &nameSize, sizeof nameSize, false);
    if (!nameSize || nameSize > 1024 * 1024) throw std::runtime_error("invalid worker filename");
    std::string path(static_cast<std::size_t>(nameSize), '\0');
    transferSocket(socket, path.data(), path.size(), false);
    Descriptor source{::open(path.c_str(), O_RDONLY | O_CLOEXEC)};
    if (source.fd < 0) throw std::runtime_error("worker cannot open source file");
    auto verify = [&] {
        struct stat now {};
        if (::fstat(source.fd, &now) != 0 || !sameInput(expected, now))
            throw std::runtime_error("source changed during parallel storage");
    };
    verify();
    const auto fileSize = static_cast<std::uint64_t>(expected.st_size);
    std::vector<std::uint8_t> buffer;
    for (;;) {
        StoreTask task;
        transferSocket(socket, &task, sizeof task, false);
        if (task.kind == 0) return;
        if ((task.kind != 1 && task.kind != 2) || task.offset > fileSize ||
            !task.bytes || task.bytes > fileSize - task.offset ||
            task.bytes > kMaxChunk + kMinChunk || (task.kind == 1 && task.bytes > kMaxChunk))
            throw std::runtime_error("invalid storage worker range");
        verify();
        buffer.resize(static_cast<std::size_t>(task.bytes));
        readFileRange(source.fd, buffer.data(), buffer.size(), static_cast<std::size_t>(task.offset));
        verify();
        std::vector<StoreRecord> records;
        auto publish = [&](StoreRecord& record, std::size_t offset) {
            const auto digest = sha256Hex(buffer.data() + offset, static_cast<std::size_t>(record.bytes));
            cache.store(digest, reinterpret_cast<const char*>(buffer.data() + offset),
                        static_cast<std::size_t>(record.bytes));
            std::copy(digest.begin(), digest.end(), record.hash.begin());
            record.ready = 1;
        };
        if (task.kind == 1) {
            const auto ranges = findUnitBoundaries(buffer.data(), buffer.size());
            const auto finalUnitSize = fileSize % kMaxChunk;
            const bool shortFinalUnit = fileSize > kMaxChunk &&
                finalUnitSize > 0 && finalUnitSize < kMinChunk;
            for (std::size_t i = 0; i < ranges.size(); ++i) {
                const auto& range = ranges[i];
                StoreRecord record{task.offset + range.offset, range.length, 0, {}};
                // Local short tails already canceled their preceding cut.
                // Only a whole undersized final unit and the last chunk of
                // its preceding unit need deferred EOF backward merging.
                const bool boundary = shortFinalUnit && i + 1 == ranges.size() &&
                    fileSize - task.offset - task.bytes < kMinChunk;
                if (!boundary) publish(record, range.offset);
                records.push_back(record);
            }
        } else {
            StoreRecord record{task.offset, task.bytes, 0, {}};
            publish(record, 0);
            records.push_back(record);
        }
        verify();
        StoreReply header{0, records.size()};
        transferSocket(socket, &header, sizeof header, true);
        transferSocket(socket, records.data(), records.size() * sizeof(StoreRecord), true);
    }
}

class DirectStoreWorkers {
    struct Worker { int parent = -1, child = -1; pid_t pid = -1; };
    std::vector<Worker> workers_;
    std::vector<detail::WorkerReplyReader<StoreReply>> readers_;
    void cleanup() noexcept {
        for (auto& w : workers_) {
            if (w.parent >= 0) { ::close(w.parent); w.parent = -1; }
            if (w.child >= 0) { ::close(w.child); w.child = -1; }
            if (w.pid > 0) ::kill(w.pid, SIGKILL);
        }
        for (auto& w : workers_) if (w.pid > 0) {
            while (::waitpid(w.pid, nullptr, 0) < 0 && errno == EINTR) {}
            w.pid = -1;
        }
    }
public:
    DirectStoreWorkers(std::size_t count, const std::string& path,
                        const struct stat& expected, int parentSource,
                        detail::SharedExistenceCache& cache) : workers_(count), readers_(count) {
        try {
            for (auto& w : workers_) {
                int sockets[2];
                if (::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) != 0)
                    throw std::runtime_error("storage socketpair failed");
                w.parent = sockets[0]; w.child = sockets[1];
#ifdef SO_NOSIGPIPE
                const int enabled = 1;
                for (int fd : sockets) if (::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof enabled) != 0)
                    throw std::runtime_error("cannot suppress storage SIGPIPE");
#endif
            }
            for (std::size_t i = 0; i < workers_.size(); ++i) {
                auto pid = ::fork();
                if (pid < 0) throw std::runtime_error("storage worker fork failed");
                if (pid == 0) {
                    for (std::size_t j = 0; j < workers_.size(); ++j) {
                        ::close(workers_[j].parent);
                        if (j != i) ::close(workers_[j].child);
                    }
                    ::close(parentSource); // Worker opens the received filename itself.
                    int status = 0;
                    try { directStoreWorker(workers_[i].child, expected, cache); }
                    catch (const std::exception& error) {
                        status = 1;
                        try {
                            std::string message(error.what());
                            message.resize(std::min<std::size_t>(message.size(), 1024));
                            StoreReply header{1, message.size()};
                            transferSocket(workers_[i].child, &header, sizeof header, true);
                            transferSocket(workers_[i].child, message.data(), message.size(), true);
                        } catch (...) {}
                    } catch (...) { status = 1; }
                    ::close(workers_[i].child);
                    ::_exit(status);
                }
                workers_[i].pid = pid;
            }
            for (auto& w : workers_) {
                ::close(w.child); w.child = -1;
                auto length = static_cast<std::uint64_t>(path.size());
                transferSocket(w.parent, &length, sizeof length, true);
                transferSocket(w.parent, const_cast<char*>(path.data()), path.size(), true);
            }
        } catch (...) { cleanup(); throw; }
    }
    ~DirectStoreWorkers() { cleanup(); }
    void submit(std::size_t worker, StoreTask task) {
        transferSocket(workers_.at(worker).parent, &task, sizeof task, true);
    }
    template<class Consume>
    void receiveReady(const std::vector<bool>& active, Consume consume) {
        std::vector<pollfd> channels(workers_.size());
        for (std::size_t w = 0; w < workers_.size(); ++w)
            channels[w] = {active[w] ? workers_[w].parent : -1, POLLIN, 0};
        detail::waitForWorkerResults(channels);
        for (std::size_t w = 0; w < workers_.size(); ++w) {
            if (!channels[w].revents) continue;
            const bool eof = readers_[w].drain(channels[w].fd, true,
                [](const StoreReply& header) -> std::size_t {
                    if (header.error) {
                        if (header.count > 1024) throw std::runtime_error("invalid storage error reply");
                        return static_cast<std::size_t>(header.count);
                    }
                    if (!header.count || header.count > kMaxChunk / kMinChunk + 1)
                        throw std::runtime_error("invalid storage worker chunk count");
                    return static_cast<std::size_t>(header.count) * sizeof(StoreRecord);
                },
                [&](const StoreReply& header, std::string&& body) {
                    if (header.error) throw std::runtime_error("storage worker: " + body);
                    std::vector<StoreRecord> records(static_cast<std::size_t>(header.count));
                    std::memcpy(records.data(), body.data(), body.size());
                    consume(w, std::move(records));
                });
            if (eof) throw std::runtime_error("storage worker closed its result channel unexpectedly");
        }
    }
    void finish() {
        for (std::size_t i = 0; i < workers_.size(); ++i) submit(i, {});
        for (auto& w : workers_) {
            int status = 0; pid_t waited;
            do { waited = ::waitpid(w.pid, &status, 0); } while (waited < 0 && errno == EINTR);
            if (waited < 0) throw std::runtime_error("storage worker wait failed");
            w.pid = -1;
            if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
                throw std::runtime_error("storage worker failed");
        }
    }
};

void storeFileParallel(const std::string& filename, std::size_t count,
                        detail::SharedExistenceCache& cache,
                        const std::function<void(const Chunk&)>& visitor) {
    const auto path = std::filesystem::absolute(filename).string();
    Descriptor source{::open(path.c_str(), O_RDONLY | O_CLOEXEC)};
    struct stat expected {};
    if (source.fd < 0 || ::fstat(source.fd, &expected) != 0 ||
        !S_ISREG(expected.st_mode) || expected.st_size < 0)
        throw std::runtime_error("parallel store requires a readable regular file");
    const auto fileSize = static_cast<std::size_t>(expected.st_size);
    if (!fileSize) return;
    const auto units = 1 + (fileSize - 1) / kMaxChunk;
    count = std::min(std::max<std::size_t>(1, count), units);
    DirectStoreWorkers pool(count, path, expected, source.fd, cache);
    auto unitTask = [&](std::size_t unit) {
        return StoreTask{1, unit * kMaxChunk, std::min(kMaxChunk, fileSize - unit * kMaxChunk)};
    };
    std::vector<std::vector<StoreRecord>> workerChunks(count);
    std::vector<std::size_t> currentUnit(count);
    std::vector<bool> active(count, true);
    for (std::size_t i = 0; i < count; ++i) {
        currentUnit[i] = i;
        pool.submit(i, unitTask(i));
    }
    auto merge = [](StoreRecord& left, const StoreRecord& right) {
        if (left.ready || right.ready || left.offset + left.bytes != right.offset)
            throw std::logic_error("storage boundary ownership invariant failed");
        left.bytes += right.bytes;
    };
    std::size_t completed = 0;
    while (completed < units) {
        pool.receiveReady(active, [&](std::size_t worker, std::vector<StoreRecord>&& records) {
            if (!active[worker]) throw std::runtime_error("unexpected storage worker reply");
            const auto task = unitTask(currentUnit[worker]);
            std::uint64_t covered = 0;
            for (const auto& record : records) {
                if (!record.bytes || record.bytes > task.bytes - covered ||
                    record.offset != task.offset + covered || record.ready > 1)
                    throw std::runtime_error("invalid stored unit coverage");
                covered += record.bytes;
                workerChunks[worker].push_back(record);
            }
            if (covered != task.bytes) throw std::runtime_error("incomplete stored unit");
            ++completed;
            currentUnit[worker] += count;
            if (currentUnit[worker] < units) pool.submit(worker, unitTask(currentUnit[worker]));
            else active[worker] = false;
        });
    }
    // Every worker has completed its scan tasks. Restore file order before
    // resolving the EOF span; no worker's progress depended on an earlier unit.
    auto final = detail::mergeWorkerLists(workerChunks);
    if (final.size() > 1 && final.back().bytes < kMinChunk) {
        const auto tail = final.back(); final.pop_back(); merge(final.back(), tail);
    }
    std::vector<std::size_t> deferred;
    for (std::size_t i = 0; i < final.size(); ++i) if (!final[i].ready) deferred.push_back(i);
    // Reuse the same processes for final boundary spans. At most one bounded
    // request/reply per worker is outstanding; no payload crosses the socket.
    std::vector<std::size_t> pending(count);
    std::size_t dispatched = 0, finalized = 0;
    auto finalizeNext = [&](std::size_t worker) {
        active[worker] = dispatched < deferred.size();
        if (active[worker]) {
            pending[worker] = deferred[dispatched++];
            const auto& r = final[pending[worker]];
            pool.submit(worker, {2, r.offset, r.bytes});
        }
    };
    for (std::size_t w = 0; w < count; ++w) finalizeNext(w);
    while (finalized < deferred.size()) {
        pool.receiveReady(active, [&](std::size_t worker, std::vector<StoreRecord>&& replies) {
            if (!active[worker]) throw std::runtime_error("unexpected finalized boundary reply");
            auto& r = final[pending[worker]];
            if (replies.size() != 1 || replies[0].offset != r.offset || replies[0].bytes != r.bytes || !replies[0].ready)
                throw std::runtime_error("invalid finalized boundary reply");
            r = replies[0];
            ++finalized;
            finalizeNext(worker);
        });
    }
    pool.finish();
    struct stat now {}, named {};
    if (::fstat(source.fd, &now) != 0 || ::stat(path.c_str(), &named) != 0 ||
        !sameInput(expected, now) || !sameInput(expected, named))
        throw std::runtime_error("source changed during parallel storage");
    std::uint64_t covered = 0;
    for (const auto& r : final) {
        const std::string hash(r.hash.data(), r.hash.size());
        if (!r.ready || r.offset != covered || hash.find_first_not_of("0123456789abcdef") != std::string::npos)
            throw std::runtime_error("invalid finalized chunk metadata");
        covered += r.bytes;
        visitor(Chunk{hash, static_cast<std::size_t>(r.bytes), std::nullopt});
    }
    if (covered != fileSize) throw std::runtime_error("incomplete stored file");
}

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
    ProcessWorkers state;
    state.file = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (state.file < 0) throw std::runtime_error("open " + path + ": " + std::strerror(errno));
    struct ::stat st {};
    if (::fstat(state.file, &st) != 0) {
        throw std::runtime_error("fstat " + path + ": " + std::strerror(errno));
    }
    if (!S_ISREG(st.st_mode) || st.st_size < 0) {
        throw std::runtime_error("parallel FastCDC requires a regular file");
    }
    const auto fileSize = static_cast<std::size_t>(st.st_size);
    if (fileSize == 0) return {};
    const std::size_t units = 1 + (fileSize - 1) / kMaxChunk;
    std::size_t workerCount = opt.processes ? opt.processes : opt.threads;
    if (workerCount == 0) workerCount = std::thread::hardware_concurrency();
    workerCount = std::min(std::max<std::size_t>(1, workerCount), units);

    state.workers.resize(workerCount);
    for (auto& worker : state.workers) {
        int descriptors[2];
        if (::pipe(descriptors) != 0) {
            throw std::runtime_error("create FastCDC worker pipe: " + std::string(std::strerror(errno)));
        }
        worker.input = descriptors[0];
        worker.output = descriptors[1];
    }
    for (std::size_t w = 0; w < workerCount; ++w) {
        const pid_t pid = ::fork();
        if (pid < 0) throw std::runtime_error("fork FastCDC worker: " + std::string(std::strerror(errno)));
        if (pid == 0) {
            // Keep exactly one result writer. Closing every unrelated pipe
            // end is essential for EOF/error detection in the parent.
            for (std::size_t j = 0; j < workerCount; ++j) {
                ::close(state.workers[j].input);
                if (j != w) ::close(state.workers[j].output);
            }
            int status = 0;
            try {
                runUnitWorker(state.file, state.workers[w].output, w,
                              workerCount, units, fileSize, opt.requestData);
            } catch (...) {
                status = 1;  // Parent detects EOF or a nonzero exit status.
            }
            ::close(state.workers[w].output);
            ::close(state.file);
            ::_exit(status);
        }
        state.workers[w].pid = pid;
    }
    for (auto& worker : state.workers) {
        ::close(worker.output);
        worker.output = -1;
        detail::makeResultPipeNonblocking(worker.input);
    }

    struct OffsetChunk { std::size_t offset; Chunk chunk; };
    std::vector<std::vector<OffsetChunk>> workerChunks(workerCount);
    std::vector<detail::WorkerReplyReader<ChunkReply>> readers(workerCount);
    std::vector<std::size_t> currentUnit(workerCount), unitCovered(workerCount, 0);
    std::vector<pollfd> channels(workerCount);
    for (std::size_t w = 0; w < workerCount; ++w) {
        currentUnit[w] = w;
        channels[w] = {state.workers[w].input, POLLIN, 0};
    }
    std::size_t active = workerCount;
    while (active) {
        detail::waitForWorkerResults(channels);
        for (std::size_t w = 0; w < workerCount; ++w) {
            if (!channels[w].revents) continue;
            const bool eof = readers[w].drain(channels[w].fd, false,
                [&](const ChunkReply& reply) -> std::size_t {
                    if (currentUnit[w] >= units) throw std::runtime_error("unexpected extra worker chunk");
                    const auto start = currentUnit[w] * kMaxChunk;
                    const auto size = std::min(kMaxChunk, fileSize - start);
                    const std::string hash(reply.hash.data(), reply.hash.size());
                    if (!reply.length || reply.length > size - unitCovered[w] ||
                        reply.offset != start + unitCovered[w] ||
                        hash.find_first_not_of("0123456789abcdef") != std::string::npos)
                        throw std::runtime_error("invalid FastCDC worker chunk");
                    return opt.requestData ? static_cast<std::size_t>(reply.length) : 0;
                },
                [&](const ChunkReply& reply, std::string&& data) {
                    Chunk chunk;
                    chunk.sha256.assign(reply.hash.data(), reply.hash.size());
                    chunk.size = static_cast<std::size_t>(reply.length);
                    if (opt.requestData) chunk.data = std::move(data);
                    workerChunks[w].push_back({static_cast<std::size_t>(reply.offset), std::move(chunk)});
                    unitCovered[w] += static_cast<std::size_t>(reply.length);
                    const auto start = currentUnit[w] * kMaxChunk;
                    if (unitCovered[w] == std::min(kMaxChunk, fileSize - start)) {
                        currentUnit[w] += workerCount;
                        unitCovered[w] = 0;
                    }
                });
            if (eof) {
                if (currentUnit[w] < units || unitCovered[w])
                    throw std::runtime_error("FastCDC worker closed before finishing its units");
                channels[w].fd = -1;
                --active;
            }
        }
    }
    for (auto& worker : state.workers) {
        int status = 0;
        pid_t waited;
        do { waited = ::waitpid(worker.pid, &status, 0); } while (waited < 0 && errno == EINTR);
        if (waited < 0) throw std::runtime_error("wait for FastCDC worker: " + std::string(std::strerror(errno)));
        worker.pid = -1;
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            throw std::runtime_error("FastCDC worker failed");
        }
    }
    // All children have finished. Merge their interleaved unit lists only now.
    auto ordered = detail::mergeWorkerLists(workerChunks);
    std::vector<Chunk> chunks;
    chunks.reserve(ordered.size());
    std::size_t covered = 0;
    for (auto& record : ordered) {
        if (record.offset != covered) throw std::runtime_error("incomplete FastCDC output coverage");
        covered += record.chunk.size;
        chunks.push_back(std::move(record.chunk));
    }
    if (covered != fileSize) throw std::runtime_error("incomplete FastCDC output");
    // A whole final unit smaller than kMinChunk has no local cut to cancel.
    // Merge it backward into the preceding unit's last chunk at EOF.
    if (chunks.size() > 1 && chunks.back().size < kMinChunk) {
        Chunk tail = std::move(chunks.back());
        chunks.pop_back();
        auto& previous = chunks.back();
        const auto length = previous.size + tail.size;
        if (opt.requestData) {
            previous.data->append(*tail.data);
            previous.sha256 = sha256Hex(*previous.data);
        } else {
            std::vector<std::uint8_t> buffer(length);
            readFileRange(state.file, buffer.data(), length, covered - length);
            previous.sha256 = sha256Hex(buffer.data(), length);
        }
        previous.size = length;
    }
    return chunks;
}

// ===========================================================================
// File-only chunk storage (SQL database logic removed)
// ===========================================================================
StoreResult storeChunksForSnapshot(const std::string& chunkRoot,
                                   const std::vector<FileEntry>& entries,
                                   const std::string& pendingCommit,
                                   std::size_t threads,
                                   const std::unordered_map<std::string, std::string>& baseManifestRoots) {
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
    std::unique_ptr<detail::SharedExistenceCache> sharedCache;
    if (threads > 1) {
        sharedCache = std::make_unique<detail::SharedExistenceCache>(chunkDir, cache.seenHll().registers());
    }

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
        const auto base = baseManifestRoots.find(fileRel);
        FileManifest manifest(chunkRoot, {}, base == baseManifestRoots.end() ? "" : base->second);
        std::deque<ManifestChunk> pending;
        std::uint64_t position = 0;
        constexpr std::size_t lookahead = 64;
        std::uint64_t checkpointOperations = 0;

        // Resolve a bounded window of old/new chunk references. Exact edit
        // callers can use FileManifest directly; snapshotting must discover
        // edits from a freshly chunked stream. Prefer nearby matching anchors
        // to avoid shifting the page grouping of an unchanged suffix.
        auto reconcile = [&] {
            const auto countBefore = manifest.summary().chunkCount;
            const auto next = pending.front();
            if (position < countBefore && manifest.at(position) == next) {
                ++position;
            } else {
                std::size_t oldMatch = lookahead, newMatch = lookahead;
                for (std::size_t i = 1; i < lookahead && i < countBefore - position; ++i) {
                    if (manifest.at(position + i) == next) { oldMatch = i; break; }
                }
                if (position < countBefore) {
                    const auto old = manifest.at(position);
                    for (std::size_t i = 1; i < pending.size(); ++i) {
                        if (pending[i] == old) { newMatch = i; break; }
                    }
                }
                if (oldMatch < lookahead && oldMatch <= newMatch) {
                    for (std::size_t i = 0; i < oldMatch; ++i) manifest.erase(position);
                } else if (newMatch < lookahead || position == countBefore) {
                    manifest.insert(position, next);
                } else {
                    manifest.update(position, next);
                }
                ++position;
            }
            pending.pop_front();
            if (manifest.stats().operations - checkpointOperations >= 2 * MANIFEST_PAGE_ENTRIES) {
                manifest.snapshot();  // Bound retained dirty metadata on huge files.
                checkpointOperations = manifest.stats().operations;
            }
        };

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
            pending.push_back({chunk.sha256, static_cast<std::uint64_t>(chunk.size)});
            if (pending.size() >= lookahead) reconcile();
        };

        if (threads > 1) {
            // Workers read their own source ranges, share cache state, and
            // publish finalized objects. The parent receives metadata only.
            storeFileParallel(entry.absolutePath, threads, *sharedCache, body);
        } else {
            // Faithful streaming path: skip payloads already in the store.
            chunkFileStream(entry.absolutePath,
                            [&](const std::string& digest) {
                                return !chunkExists(digest);
                            },
                            body);
        }

        while (!pending.empty()) reconcile();
        while (manifest.summary().chunkCount > position) {
            manifest.erase(position);
            if (manifest.stats().operations - checkpointOperations >= 2 * MANIFEST_PAGE_ENTRIES) {
                manifest.snapshot();
                checkpointOperations = manifest.stats().operations;
            }
        }

        res.fileHashes[fileRel] = h.hexdigest();
        res.chunkLength[fileRel] = count;
        Descriptor chunkDirectory{::open(chunkDir.c_str(), O_RDONLY | O_DIRECTORY)};
        if (chunkDirectory.fd < 0 || ::fsync(chunkDirectory.fd) != 0)
            throw std::runtime_error("chunk directory sync failed");
        res.manifestRoots[fileRel] = manifest.snapshot();
    }

    // Persist the HyperLogLog sketch for the next call.  Best-effort: a
    // failed save (full disk, read-only root, ...) must not fail a
    // snapshot that has already been stored correctly.
    try {
        if (sharedCache) cache.seenHll().assignRegisters(sharedCache->registers());
        saveHllState(chunkDir, cache.seenHll());
    } catch (const std::exception&) {
    }

    return res;
}

bool rebuildFileFromManifest(const std::string& chunkRoot,
                             const std::string& relativePath,
                             const std::string& manifestRoot,
                             const std::string& fileHash,
                             const std::string& outputPath) {
    struct TemporaryOutput {
        int fd = -1;
        std::string path;
        ~TemporaryOutput() {
            if (fd >= 0) ::close(fd);
            if (!path.empty()) ::unlink(path.c_str());
        }
    } temporary;
    try {
        if (manifestRoot.empty()) return false;
        const std::filesystem::path relative(relativePath);
        if (relative.empty() || relative.is_absolute()) return false;
        for (const auto& part : relative) if (part == "..") return false;
        FileManifest manifest(chunkRoot, {}, manifestRoot);
        Sha256Ctx digest;
        manifest.forEach([&](const ManifestChunk& c) { digest.update(c.hash); });
        if (digest.hexdigest() != fileHash) return false;
        const auto target = std::filesystem::path(outputPath) / relative;
        std::filesystem::create_directories(target.parent_path());
        std::string pattern = (target.parent_path() / ".restore-XXXXXX").string();
        std::vector<char> name(pattern.begin(), pattern.end());
        name.push_back('\0');
        temporary.fd = ::mkstemp(name.data());
        if (temporary.fd < 0) return false;
        temporary.path = name.data();
        if (::fchmod(temporary.fd, 0644) != 0) return false;
        std::array<char, 65536> buffer;
        manifest.forEach([&](const ManifestChunk& chunk) {
            std::ifstream input(chunkRoot + "/" + kChunkStorageDir + "/" + chunk.hash, std::ios::binary);
            if (!input) throw std::runtime_error("missing manifest chunk payload");
            Sha256Ctx payloadHash;
            std::uint64_t size = 0;
            while (input) {
                input.read(buffer.data(), buffer.size());
                const auto got = input.gcount();
                if (static_cast<std::uint64_t>(got) > chunk.size - size)
                    throw std::runtime_error("manifest chunk payload too large");
                size += static_cast<std::uint64_t>(got);
                payloadHash.update(buffer.data(), static_cast<std::size_t>(got));
                std::size_t offset = 0;
                while (offset < static_cast<std::size_t>(got)) {
                    auto written = ::write(temporary.fd, buffer.data() + offset,
                                           static_cast<std::size_t>(got) - offset);
                    if (written < 0 && errno == EINTR) continue;
                    if (written <= 0) throw std::runtime_error("manifest restore write failed");
                    offset += static_cast<std::size_t>(written);
                }
            }
            if (!input.eof() || size != chunk.size || payloadHash.hexdigest() != chunk.hash)
                throw std::runtime_error("manifest chunk payload mismatch");
        });
        if (::fsync(temporary.fd) != 0) return false;
        const int closeResult = ::close(temporary.fd);
        temporary.fd = -1;
        if (closeResult != 0 || ::rename(temporary.path.c_str(), target.c_str()) != 0) return false;
        temporary.path.clear();
        const int directoryFd = ::open(target.parent_path().c_str(), O_RDONLY | O_DIRECTORY);
        if (directoryFd < 0) return false;
        const int synced = ::fsync(directoryFd);
        ::close(directoryFd);
        return synced == 0;
    } catch (const std::exception&) {
        return false;
    }
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
