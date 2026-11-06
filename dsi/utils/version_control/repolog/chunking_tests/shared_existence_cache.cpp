#include "shared_existence_cache.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <new>
#include <stdexcept>
#include <utility>
#include <vector>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace chunking::detail {
namespace {
constexpr std::size_t kSets = 1024, kWays = 8, kStripes = 4096;
static_assert(std::atomic<std::uint64_t>::is_always_lock_free);
static_assert(std::atomic<std::uint8_t>::is_always_lock_free);

class RecordLock {
    int fd_;
    off_t byte_;
public:
    RecordLock(int fd, off_t byte) : fd_(fd), byte_(byte) {
        struct flock lock {};
        lock.l_type = F_WRLCK; lock.l_whence = SEEK_SET;
        lock.l_start = byte_; lock.l_len = 1;
        while (::fcntl(fd_, F_SETLKW, &lock) != 0) {
            if (errno != EINTR) throw std::runtime_error("shared cache lock failed");
        }
        std::atomic_thread_fence(std::memory_order_acquire);
    }
    ~RecordLock() {
        std::atomic_thread_fence(std::memory_order_release);
        struct flock lock {};
        lock.l_type = F_UNLCK; lock.l_whence = SEEK_SET;
        lock.l_start = byte_; lock.l_len = 1;
        while (::fcntl(fd_, F_SETLK, &lock) != 0 && errno == EINTR) {}
    }
    RecordLock(const RecordLock&) = delete;
};
}

std::uint64_t cacheDigestHash(const std::string& digest, std::uint64_t salt) {
    std::uint64_t h = 14695981039346656037ULL;
    h ^= salt * 0x9E3779B97F4A7C15ULL;
    for (const unsigned char c : digest) { h ^= c; h *= 0x100000001B3ULL; }
    h ^= h >> 33; h *= 0xFF51AFD7ED558CCDULL;
    h ^= h >> 33; h *= 0xC4CEB9FE1A85EC53ULL;
    h ^= h >> 33;
    return h;
}

void publishChunkBlob(const std::string& directory, const std::string& digest,
                      const char* data, std::size_t size) {
    std::string pattern = directory + "/." + digest + ".tmpXXXXXX";
    std::vector<char> name(pattern.begin(), pattern.end()); name.push_back('\0');
    int fd = ::mkstemp(name.data());
    if (fd < 0) throw std::runtime_error("mkstemp for chunk failed");
    try {
        if (::fchmod(fd, 0644) != 0) throw std::runtime_error("chunk chmod failed");
        std::size_t offset = 0;
        while (offset < size) {
            const auto n = ::write(fd, data + offset, size - offset);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) throw std::runtime_error("chunk write failed");
            offset += static_cast<std::size_t>(n);
        }
        if (::fsync(fd) != 0) throw std::runtime_error("chunk fsync failed");
        const int result = ::close(fd); fd = -1;
        if (result != 0) throw std::runtime_error("chunk close failed");
        if (::rename(name.data(), (directory + "/" + digest).c_str()) != 0)
            throw std::runtime_error("chunk rename failed");
    } catch (...) {
        if (fd >= 0) ::close(fd);
        ::unlink(name.data());
        throw;
    }
}

struct SharedExistenceCache::Impl {
    struct Entry {
        std::atomic<std::uint8_t> valid{0};
        char digest[64]{};
        std::uint64_t size = 0, age = 0;
    };
    struct Data {
        std::atomic<std::uint64_t> tick{0}, hits{0}, diskChecks{0}, published{0};
        std::array<std::atomic<std::uint8_t>, kSharedHllRegisters> hll;
        Entry entries[kSets][kWays];
    };
    std::string directory;
    Publisher publisher;
    int lockFd = -1;
    Data* data = nullptr;

    Impl(std::string dir, const HllRegisters& initial, Publisher publish)
        : directory(std::move(dir)), publisher(std::move(publish)) {
        std::filesystem::create_directories(directory);
        if (!publisher) publisher = [this](const auto& hash, const char* bytes, std::size_t size) {
            publishChunkBlob(directory, hash, bytes, size);
        };
        auto memory = ::mmap(nullptr, sizeof(Data), PROT_READ | PROT_WRITE,
                             MAP_SHARED | MAP_ANON, -1, 0);
        if (memory == MAP_FAILED) throw std::runtime_error("shared cache mmap failed");
        data = new (memory) Data{};
        // Older sketches with invalid ranks are hints only; dropping them
        // cannot hide an object because cache misses always verify disk.
        const bool valid = std::all_of(initial.begin(), initial.end(), [](auto rank) { return rank <= 53; });
        for (std::size_t i = 0; i < initial.size(); ++i) data->hll[i].store(valid ? initial[i] : 0);
        std::string pattern = directory + "/.existence-lock-XXXXXX";
        std::vector<char> name(pattern.begin(), pattern.end()); name.push_back('\0');
        lockFd = ::mkstemp(name.data());
        if (lockFd < 0) {
            ::munmap(data, sizeof(Data)); data = nullptr;
            throw std::runtime_error("shared cache lock file failed");
        }
        ::unlink(name.data()); // Inherited descriptors retain this private lock inode.
    }
    ~Impl() {
        if (lockFd >= 0) ::close(lockFd);
        if (data) ::munmap(data, sizeof(Data));
    }

    void markSeen(std::uint64_t h) {
        const auto index = h >> 52;
        const auto rest = h << 12;
        const auto rank = static_cast<std::uint8_t>(rest ? __builtin_clzll(rest) + 1 : 53);
        auto previous = data->hll[index].load(std::memory_order_relaxed);
        while (previous < rank && !data->hll[index].compare_exchange_weak(previous, rank)) {}
    }

    bool cached(const std::string& hash, std::uint64_t h, std::size_t size) {
        RecordLock lock(lockFd, 0);
        for (auto& entry : data->entries[h % kSets]) {
            if (entry.valid.load(std::memory_order_acquire) && std::memcmp(entry.digest, hash.data(), 64) == 0) {
                if (entry.size != size) throw std::runtime_error("cached chunk size mismatch");
                entry.age = data->tick.fetch_add(1) + 1;
                data->hits.fetch_add(1);
                return true;
            }
        }
        return false;
    }

    void remember(const std::string& hash, std::uint64_t h, std::size_t size) {
        RecordLock lock(lockFd, 0);
        Entry* slot = &data->entries[h % kSets][0];
        for (auto& entry : data->entries[h % kSets]) {
            if (!entry.valid.load() || entry.age < slot->age) slot = &entry;
            if (!entry.valid.load()) break;
        }
        // If a process dies while modifying a slot, the next owner sees an
        // invalid entry, never a positive entry with a partially written key.
        slot->valid.store(0, std::memory_order_seq_cst);
        std::atomic_thread_fence(std::memory_order_seq_cst);
        std::memcpy(slot->digest, hash.data(), 64);
        slot->size = size; slot->age = data->tick.fetch_add(1) + 1;
        slot->valid.store(1, std::memory_order_release);
        markSeen(h);
    }
};

SharedExistenceCache::SharedExistenceCache(std::string dir, HllRegisters initial, Publisher publisher)
    : impl_(std::make_unique<Impl>(std::move(dir), initial, std::move(publisher))) {}
SharedExistenceCache::~SharedExistenceCache() = default;

bool SharedExistenceCache::store(const std::string& hash, const char* bytes, std::size_t size) {
    if (hash.size() != 64 || hash.find_first_not_of("0123456789abcdef") != std::string::npos || !size || !bytes)
        throw std::invalid_argument("invalid shared cache chunk");
    auto& state = *impl_;
    const auto h = cacheDigestHash(hash);
    // Same-hash writers serialize across lookup + publication. Different
    // stripes perform disk I/O concurrently; the metadata lock is brief.
    RecordLock publication(state.lockFd, 1 + static_cast<off_t>((h >> 10) % kStripes));
    if (state.cached(hash, h, size)) return false;
    state.data->diskChecks.fetch_add(1);
    struct stat info {};
    const auto path = state.directory + "/" + hash;
    if (::stat(path.c_str(), &info) == 0) {
        if (!S_ISREG(info.st_mode) || info.st_size < 0 || static_cast<std::uint64_t>(info.st_size) != size)
            throw std::runtime_error("stored chunk size/type mismatch");
        state.remember(hash, h, size);
        return false;
    }
    if (errno != ENOENT) throw std::runtime_error("cannot stat chunk: " + std::string(std::strerror(errno)));
    state.publisher(hash, bytes, size);
    state.remember(hash, h, size);
    state.data->published.fetch_add(1);
    return true;
}

HllRegisters SharedExistenceCache::registers() const {
    HllRegisters out{};
    for (std::size_t i = 0; i < out.size(); ++i) out[i] = impl_->data->hll[i].load();
    return out;
}
SharedCacheStats SharedExistenceCache::stats() const {
    return {impl_->data->hits.load(), impl_->data->diskChecks.load(), impl_->data->published.load()};
}
}  // namespace chunking::detail
