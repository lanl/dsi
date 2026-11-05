#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace chunking {

inline constexpr std::size_t MANIFEST_PAGE_ENTRIES = 4096;
inline constexpr const char* MANIFEST_STORAGE_DIR = "dsi_vcs_manifests_cpp";

struct ManifestChunk {
    std::string hash;
    std::uint64_t size = 0;
    bool operator==(const ManifestChunk& other) const {
        return hash == other.hash && size == other.size;
    }
    bool operator!=(const ManifestChunk& other) const { return !(*this == other); }
};

struct ManifestOptions {
    std::size_t pageEntries = MANIFEST_PAGE_ENTRIES;
    std::uint32_t maxDepthDifference = 2;
    std::size_t cachedPages = 64;
};

struct ManifestSummary {
    std::uint64_t chunkCount = 0;
    std::uint64_t byteCount = 0;
    std::uint32_t minDepth = 0;
    std::uint32_t maxDepth = 0;
};

struct ManifestStats {
    std::uint64_t operations = 0, balanceChecks = 0;
    std::uint64_t splits = 0, merges = 0, redistributions = 0;
    std::uint64_t pageReads = 0, pageWrites = 0, internalRebuilds = 0;
};

// Immutable on disk; only changed paths are copied in the working tree.
// Call snapshot() to publish dirty pages and persist the returned root in the
// caller's version metadata. Old roots remain readable across edits/branches.
class FileManifest {
public:
    explicit FileManifest(std::string chunkRoot, ManifestOptions options = {},
                          std::string rootHash = {});
    ~FileManifest();
    FileManifest(FileManifest&&) noexcept;
    FileManifest& operator=(FileManifest&&) noexcept;
    FileManifest(const FileManifest&) = delete;
    FileManifest& operator=(const FileManifest&) = delete;

    ManifestSummary summary() const;
    ManifestStats stats() const;
    ManifestChunk at(std::uint64_t index) const;
    void insert(std::uint64_t index, const ManifestChunk& chunk);
    void erase(std::uint64_t index);
    void update(std::uint64_t index, const ManifestChunk& chunk);
    void forEach(const std::function<void(const ManifestChunk&)>& visitor) const;
    std::string snapshot();

    // Recovery/maintenance: reuse existing leaf objects; rebuild internal pages
    // only. Normal split/merge operations preserve equal leaf depths already.
    void rebalanceInternal();
    // Expensive diagnostics, intentionally separate from per-operation checks.
    void validate() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace chunking
