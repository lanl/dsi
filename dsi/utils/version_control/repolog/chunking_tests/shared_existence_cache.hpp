#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace chunking::detail {

inline constexpr std::size_t kSharedHllRegisters = 4096;
using HllRegisters = std::array<std::uint8_t, kSharedHllRegisters>;

std::uint64_t cacheDigestHash(const std::string& digest,
                             std::uint64_t salt = 0x5E5E0C4DEBCB6937ULL);
void publishChunkBlob(const std::string& directory, const std::string& digest,
                      const char* data, std::size_t size);

struct SharedCacheStats {
    std::uint64_t hits = 0, diskChecks = 0, published = 0;
};

// Created before fork; all descendants share fixed-capacity POD/atomic state.
// No std::string/list/map pointers live in the shared mapping. Call from one
// thread per process. File-record locks are released by the OS if a worker dies.
class SharedExistenceCache {
public:
    using Publisher = std::function<void(const std::string&, const char*, std::size_t)>;
    explicit SharedExistenceCache(std::string directory, HllRegisters initial = {},
                                  Publisher publisher = {});
    ~SharedExistenceCache();
    SharedExistenceCache(const SharedExistenceCache&) = delete;
    SharedExistenceCache& operator=(const SharedExistenceCache&) = delete;

    // Returns true only for a new publication. Positive cache entries are
    // installed after the publisher succeeds; misses always consult disk.
    bool store(const std::string& digest, const char* data, std::size_t size);
    HllRegisters registers() const;
    SharedCacheStats stats() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
}  // namespace chunking::detail
