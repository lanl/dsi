#include "manifest.hpp"
#include "sha256.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <list>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace chunking {
namespace {
constexpr const char* kMagicV1 = "DSIMCP01";
constexpr const char* kMagicV2 = "DSIMCP02";
constexpr std::size_t kHeaderSize = 41;
constexpr std::size_t kLeafEntryV1Size = 40;
constexpr std::size_t kLeafEntryV2Size = 88;
constexpr std::size_t kInternalEntrySize = 56;

std::uint64_t checkedAdd(std::uint64_t a, std::uint64_t b) {
    if (b > std::numeric_limits<std::uint64_t>::max() - a)
        throw std::overflow_error("manifest count overflow");
    return a + b;
}

void checkHash(const std::string& hash) {
    if (hash.size() != 64 || hash.find_first_not_of("0123456789abcdef") != std::string::npos)
        throw std::invalid_argument("manifest hash must be lowercase SHA-256 hex");
}

void put(std::string& out, std::uint64_t n, unsigned bytes) {
    for (unsigned i = bytes; i > 0; --i)
        out.push_back(static_cast<char>((n >> ((i - 1) * 8)) & 255));
}

std::uint64_t get(const std::string& in, std::size_t& pos, unsigned bytes) {
    if (pos > in.size() || bytes > in.size() - pos)
        throw std::runtime_error("truncated manifest object");
    std::uint64_t n = 0;
    while (bytes--) n = (n << 8) | static_cast<unsigned char>(in[pos++]);
    return n;
}

void putHash(std::string& out, const std::string& hash) {
    checkHash(hash);
    auto nibble = [](char c) { return c <= '9' ? c - '0' : c - 'a' + 10; };
    for (std::size_t i = 0; i < 64; i += 2)
        out.push_back(static_cast<char>(nibble(hash[i]) * 16 + nibble(hash[i + 1])));
}

std::string getHash(const std::string& in, std::size_t& pos) {
    static constexpr char hex[] = "0123456789abcdef";
    if (pos > in.size() || in.size() - pos < 32)
        throw std::runtime_error("truncated manifest hash");
    std::string hash;
    hash.reserve(64);
    for (unsigned i = 0; i < 32; ++i) {
        auto c = static_cast<unsigned char>(in[pos++]);
        hash.push_back(hex[c >> 4]);
        hash.push_back(hex[c & 15]);
    }
    return hash;
}

bool sameSummary(const ManifestSummary& a, const ManifestSummary& b) {
    return a.chunkCount == b.chunkCount && a.byteCount == b.byteCount &&
           a.minDepth == b.minDepth && a.maxDepth == b.maxDepth;
}
}  // namespace

struct FileManifest::Impl {
    struct Node;
    struct Link {
        std::string hash;
        ManifestSummary summary;
        std::shared_ptr<Node> dirty;
    };
    struct Node {
        bool leaf = true;
        std::vector<ManifestChunk> chunks;
        std::vector<Link> children;
        std::size_t size() const { return leaf ? chunks.size() : children.size(); }
    };
    struct AppendChunk {
        std::array<std::uint8_t, 32> hash{};
        std::uint64_t size = 0;
        std::array<std::uint8_t, 32> packId{};
        std::uint64_t packOffset = 0;
        std::uint64_t storedSize = 0;
    };
    struct Cached {
        std::shared_ptr<Node> node;
        std::list<std::string>::iterator position;
    };

    std::string directory;
    ManifestOptions options;
    Link root;
    mutable ManifestStats counters;
    mutable std::list<std::string> lru;
    mutable std::unordered_map<std::string, Cached> cache;

    // Sequential construction is the common initial-import and pure-append
    // case. Do not turn every streamed chunk into a random tree edit: keep
    // one bounded leaf buffer, retain immutable old leaves, and construct only
    // the small internal hierarchy when publishing a root.
    bool appendMode = false;
    std::vector<Link> appendLeaves;
    std::vector<AppendChunk> appendBuffer;
    ManifestSummary appendSummary;

    Impl(std::string chunkRoot, ManifestOptions opt, const std::string& hash)
        : directory(std::move(chunkRoot) + "/" + MANIFEST_STORAGE_DIR), options(opt) {
        if (options.pageEntries < 4 || options.pageEntries > 65536)
            throw std::invalid_argument("manifest pageEntries must be between 4 and 65536");
        if (options.cachedPages == 0)
            throw std::invalid_argument("manifest cache must hold at least one page");
        appendBuffer.reserve(options.pageEntries);
        std::filesystem::create_directories(directory);
        if (hash.empty()) root = link(std::make_shared<Node>());
        else {
            checkHash(hash);
            auto node = read(hash);
            root = {hash, summarize(*node), {}};
        }
    }

    std::size_t low() const { return (options.pageEntries + 1) / 2; }
    std::size_t splitAt() const { return 2 * options.pageEntries; }

    ManifestSummary summarize(const Node& node) const {
        ManifestSummary s;
        if (node.leaf) {
            s.chunkCount = node.chunks.size();
            for (const auto& c : node.chunks) s.byteCount = checkedAdd(s.byteCount, c.size);
        } else {
            if (node.children.empty()) throw std::runtime_error("empty internal manifest node");
            s.minDepth = std::numeric_limits<std::uint32_t>::max();
            for (const auto& c : node.children) {
                s.chunkCount = checkedAdd(s.chunkCount, c.summary.chunkCount);
                s.byteCount = checkedAdd(s.byteCount, c.summary.byteCount);
                if (c.summary.maxDepth >= 63)
                    throw std::runtime_error("manifest depth exceeds 63");
                s.minDepth = std::min(s.minDepth, c.summary.minDepth + 1);
                s.maxDepth = std::max(s.maxDepth, c.summary.maxDepth + 1);
            }
        }
        return s;
    }

    Link link(std::shared_ptr<Node> node) const {
        return {{}, summarize(*node), std::move(node)};
    }

    std::shared_ptr<Node> read(const std::string& hash) const {
        auto found = cache.find(hash);
        if (found != cache.end()) {
            lru.splice(lru.end(), lru, found->second.position);
            return found->second.node;
        }
        checkHash(hash);
        const auto path = directory + "/" + hash;
        std::ifstream in(path, std::ios::binary | std::ios::ate);
        if (!in) throw std::runtime_error("cannot open manifest object: " + path);
        const auto length = in.tellg();
        const auto maximum = kHeaderSize + splitAt() * kLeafEntryV2Size;
        if (length < static_cast<std::streamoff>(kHeaderSize) ||
            length > static_cast<std::streamoff>(maximum))
            throw std::runtime_error("invalid manifest object length");
        std::string payload(static_cast<std::size_t>(length), '\0');
        in.seekg(0);
        if (!in.read(payload.data(), static_cast<std::streamsize>(payload.size())))
            throw std::runtime_error("cannot read manifest object");
        if (sha256Hex(payload) != hash) throw std::runtime_error("manifest checksum mismatch");
        const bool v1 = payload.compare(0, 8, kMagicV1) == 0;
        const bool v2 = payload.compare(0, 8, kMagicV2) == 0;
        if (!v1 && !v2)
            throw std::runtime_error("unsupported manifest format");
        std::size_t pos = 8;
        if (get(payload, pos, 4) != options.pageEntries)
            throw std::runtime_error("manifest page capacity mismatch");
        const auto kind = get(payload, pos, 1);
        if (kind > 1) throw std::runtime_error("invalid manifest page kind");
        auto count = get(payload, pos, 4);
        ManifestSummary advertised;
        advertised.chunkCount = get(payload, pos, 8);
        advertised.byteCount = get(payload, pos, 8);
        advertised.minDepth = static_cast<std::uint32_t>(get(payload, pos, 4));
        advertised.maxDepth = static_cast<std::uint32_t>(get(payload, pos, 4));
        const auto entrySize = kind == 0 ? (v1 ? kLeafEntryV1Size : kLeafEntryV2Size) : kInternalEntrySize;
        if (count >= splitAt() || payload.size() != kHeaderSize + count * entrySize)
            throw std::runtime_error("invalid manifest entry count");
        auto node = std::make_shared<Node>();
        node->leaf = kind == 0;
        for (std::uint64_t i = 0; i < count; ++i) {
            auto digest = getHash(payload, pos);
            if (node->leaf) {
                auto size = get(payload, pos, 8);
                if (size == 0) throw std::runtime_error("zero-length manifest chunk");
                ManifestChunk chunk{std::move(digest), size};
                if (v2) {
                    chunk.packId = getHash(payload, pos);
                    chunk.packOffset = get(payload, pos, 8);
                    chunk.storedSize = get(payload, pos, 8);
                    if (chunk.packId == std::string(64, '0') && !chunk.packOffset && !chunk.storedSize) {
                        chunk.packId.clear();
                    } else if (chunk.storedSize != chunk.size) {
                        throw std::runtime_error("invalid packed manifest chunk");
                    }
                }
                node->chunks.push_back(std::move(chunk));
            } else {
                ManifestSummary s;
                s.chunkCount = get(payload, pos, 8);
                s.byteCount = get(payload, pos, 8);
                s.minDepth = static_cast<std::uint32_t>(get(payload, pos, 4));
                s.maxDepth = static_cast<std::uint32_t>(get(payload, pos, 4));
                if (s.chunkCount == 0 || s.minDepth > s.maxDepth)
                    throw std::runtime_error("invalid manifest child summary");
                node->children.push_back({std::move(digest), s, {}});
            }
        }
        if (!sameSummary(summarize(*node), advertised))
            throw std::runtime_error("invalid manifest aggregate counts");
        if (cache.size() >= options.cachedPages) {
            cache.erase(lru.front());
            lru.pop_front();
        }
        lru.push_back(hash);
        cache.emplace(hash, Cached{node, std::prev(lru.end())});
        ++counters.pageReads;
        return node;
    }

    std::shared_ptr<Node> load(const Link& value) const {
        if (value.dirty) return value.dirty;
        auto node = read(value.hash);
        if (!sameSummary(summarize(*node), value.summary))
            throw std::runtime_error("manifest child summary mismatch");
        return node;
    }

    void collectLeaves(const Link& value, std::vector<Link>& leaves) const {
        if (value.summary.chunkCount == 0) return;
        if (value.summary.maxDepth == 0) {
            leaves.push_back(value);
            return;
        }
        const auto node = load(value);
        for (const auto& child : node->children) collectLeaves(child, leaves);
    }

    void collectReachable(const Link& value, std::unordered_set<std::string>& pages) const {
        if (value.hash.empty())
            throw std::logic_error("cannot collect reachability from an unpublished manifest page");
        if (!pages.insert(value.hash).second) return;
        const auto node = load(value);
        if (!node->leaf)
            for (const auto& child : node->children) collectReachable(child, pages);
    }

    void beginAppend() {
        if (appendMode) return;
        appendLeaves.clear();
        appendBuffer.clear();
        if (root.summary.chunkCount) collectLeaves(root, appendLeaves);
        appendSummary = root.summary;
        appendMode = true;
        refreshAppendDepth();
    }

    static AppendChunk packAppendChunk(const ManifestChunk& chunk) {
        checkHash(chunk.hash);
        AppendChunk packed;
        packed.size = chunk.size;
        auto nibble = [](char c) -> std::uint8_t {
            return static_cast<std::uint8_t>(c <= '9' ? c - '0' : c - 'a' + 10);
        };
        for (std::size_t i = 0; i < packed.hash.size(); ++i)
            packed.hash[i] = static_cast<std::uint8_t>(nibble(chunk.hash[i * 2]) * 16 +
                                                       nibble(chunk.hash[i * 2 + 1]));
        if (!chunk.packId.empty()) {
            checkHash(chunk.packId);
            if (chunk.storedSize != chunk.size)
                throw std::invalid_argument("packed manifest chunk stored size mismatch");
            for (std::size_t i = 0; i < packed.packId.size(); ++i)
                packed.packId[i] = static_cast<std::uint8_t>(nibble(chunk.packId[i * 2]) * 16 +
                                                             nibble(chunk.packId[i * 2 + 1]));
            packed.packOffset = chunk.packOffset;
            packed.storedSize = chunk.storedSize;
        }
        return packed;
    }

    static ManifestChunk unpackAppendChunk(const AppendChunk& packed) {
        static constexpr char hex[] = "0123456789abcdef";
        std::string hash(64, '0');
        for (std::size_t i = 0; i < packed.hash.size(); ++i) {
            hash[i * 2] = hex[packed.hash[i] >> 4];
            hash[i * 2 + 1] = hex[packed.hash[i] & 15];
        }
        ManifestChunk chunk{std::move(hash), packed.size};
        if (packed.storedSize) {
            std::string packId(64, '0');
            for (std::size_t i = 0; i < packed.packId.size(); ++i) {
                packId[i * 2] = hex[packed.packId[i] >> 4];
                packId[i * 2 + 1] = hex[packed.packId[i] & 15];
            }
            chunk.packId = std::move(packId);
            chunk.packOffset = packed.packOffset;
            chunk.storedSize = packed.storedSize;
        }
        return chunk;
    }

    void refreshAppendDepth() {
        std::size_t levelCount = appendLeaves.size() + (!appendBuffer.empty() ? 1 : 0);
        std::uint32_t depth = 0;
        while (levelCount > 1) {
            levelCount = (levelCount + options.pageEntries - 1) / options.pageEntries;
            ++depth;
        }
        appendSummary.minDepth = depth;
        appendSummary.maxDepth = depth;
    }

    void sealAppendBuffer() {
        if (appendBuffer.empty()) return;
        auto leaf = std::make_shared<Node>();
        leaf->chunks.reserve(appendBuffer.size());
        for (const auto& chunk : appendBuffer)
            leaf->chunks.push_back(unpackAppendChunk(chunk));
        appendBuffer.clear();
        appendBuffer.reserve(options.pageEntries);
        appendLeaves.push_back(link(std::move(leaf)));
        refreshAppendDepth();
    }

    void balanceAppendTail() {
        if (appendLeaves.size() < 2) return;
        const auto tail = load(appendLeaves.back());
        if (tail->chunks.size() >= low()) return;
        const auto previous = load(appendLeaves[appendLeaves.size() - 2]);
        if (!tail->leaf || !previous->leaf)
            throw std::logic_error("append leaf collection contains an internal node");

        std::vector<ManifestChunk> combined;
        combined.reserve(previous->chunks.size() + tail->chunks.size());
        combined.insert(combined.end(), previous->chunks.begin(), previous->chunks.end());
        combined.insert(combined.end(), tail->chunks.begin(), tail->chunks.end());

        auto makeLeaf = [](std::vector<ManifestChunk> chunks) {
            auto leaf = std::make_shared<Node>();
            leaf->chunks = std::move(chunks);
            return leaf;
        };
        appendLeaves.pop_back();
        appendLeaves.pop_back();
        if (combined.size() <= options.pageEntries) {
            appendLeaves.push_back(link(makeLeaf(std::move(combined))));
            refreshAppendDepth();
            return;
        }
        const auto middle = combined.size() / 2;
        std::vector<ManifestChunk> right(combined.begin() + static_cast<std::ptrdiff_t>(middle),
                                         combined.end());
        combined.resize(middle);
        appendLeaves.push_back(link(makeLeaf(std::move(combined))));
        appendLeaves.push_back(link(makeLeaf(std::move(right))));
        refreshAppendDepth();
    }

    Link appendRoot() const {
        if (appendLeaves.empty()) return link(std::make_shared<Node>());
        auto level = appendLeaves;
        while (level.size() > 1) {
            std::vector<Link> next;
            const auto groups = (level.size() + options.pageEntries - 1) / options.pageEntries;
            const auto each = level.size() / groups;
            const auto extra = level.size() % groups;
            next.reserve(groups);
            std::size_t start = 0;
            for (std::size_t group = 0; group < groups; ++group) {
                auto node = std::make_shared<Node>();
                node->leaf = false;
                const auto count = each + (group < extra ? 1 : 0);
                const auto end = start + count;
                node->children.assign(level.begin() + static_cast<std::ptrdiff_t>(start),
                                      level.begin() + static_cast<std::ptrdiff_t>(end));
                next.push_back(link(std::move(node)));
                start = end;
            }
            level = std::move(next);
        }
        return level.front();
    }

    void materializeAppend() {
        if (!appendMode) return;
        sealAppendBuffer();
        balanceAppendTail();
        root = appendRoot();
        appendMode = false;
    }

    void append(const ManifestChunk& chunk) {
        if (chunk.size == 0) throw std::invalid_argument("manifest chunk size must be positive");
        beginAppend();
        appendBuffer.push_back(packAppendChunk(chunk));
        appendSummary.chunkCount = checkedAdd(appendSummary.chunkCount, 1);
        appendSummary.byteCount = checkedAdd(appendSummary.byteCount, chunk.size);
        refreshAppendDepth();
        if (appendBuffer.size() == options.pageEntries) sealAppendBuffer();
        ++counters.operations;
        ++counters.balanceChecks;
    }

    std::string snapshotAppend() {
        sealAppendBuffer();
        balanceAppendTail();
        // Once a leaf is published it is immutable and can be reused by every
        // later append snapshot; only new leaves and the compact upper levels
        // are written again.
        for (auto& leaf : appendLeaves) leaf = persist(leaf);
        root = persist(appendRoot());
        return root.hash;
    }

    ManifestChunk appendAt(std::uint64_t index) const {
        for (const auto& leaf : appendLeaves) {
            if (index < leaf.summary.chunkCount)
                return load(leaf)->chunks[static_cast<std::size_t>(index)];
            index -= leaf.summary.chunkCount;
        }
        if (index < appendBuffer.size()) return unpackAppendChunk(appendBuffer[static_cast<std::size_t>(index)]);
        throw std::out_of_range("manifest chunk index out of range");
    }

    void walkAppend(const std::function<void(const ManifestChunk&)>& visitor) const {
        for (const auto& leaf : appendLeaves) {
            const auto node = load(leaf);
            for (const auto& chunk : node->chunks) visitor(chunk);
        }
        for (const auto& chunk : appendBuffer) visitor(unpackAppendChunk(chunk));
    }

    std::string encode(const Node& node) const {
        const auto s = summarize(node);
        std::string payload(kMagicV2, 8);
        put(payload, options.pageEntries, 4);
        put(payload, node.leaf ? 0 : 1, 1);
        put(payload, node.size(), 4);
        put(payload, s.chunkCount, 8);
        put(payload, s.byteCount, 8);
        put(payload, s.minDepth, 4);
        put(payload, s.maxDepth, 4);
        if (node.leaf) {
            for (const auto& c : node.chunks) {
                putHash(payload, c.hash); put(payload, c.size, 8);
                // New manifests always use the v2 record layout.  Empty
                // locations keep hand-built manifests readable as legacy
                // loose-object references during migration.
                if (c.packId.empty()) {
                    payload.append(32, '\0'); put(payload, 0, 8); put(payload, 0, 8);
                } else {
                    checkHash(c.packId);
                    if (c.storedSize != c.size)
                        throw std::runtime_error("packed manifest chunk stored size mismatch");
                    putHash(payload, c.packId); put(payload, c.packOffset, 8); put(payload, c.storedSize, 8);
                }
            }
        } else {
            for (const auto& c : node.children) {
                putHash(payload, c.hash);
                put(payload, c.summary.chunkCount, 8);
                put(payload, c.summary.byteCount, 8);
                put(payload, c.summary.minDepth, 4);
                put(payload, c.summary.maxDepth, 4);
            }
        }
        return payload;
    }

    std::string publish(const std::string& payload) {
        const auto hash = sha256Hex(payload);
        const auto destination = directory + "/" + hash;
        if (std::filesystem::exists(destination)) {
            read(hash);  // Never silently reuse a corrupted object.
            return hash;
        }
        std::string pattern = directory + "/.manifest-XXXXXX";
        std::vector<char> temporary(pattern.begin(), pattern.end());
        temporary.push_back('\0');
        int fd = ::mkstemp(temporary.data());
        if (fd < 0) throw std::runtime_error("manifest mkstemp failed");
        try {
            if (::fchmod(fd, 0644) != 0) throw std::runtime_error("manifest chmod failed");
            std::size_t written = 0;
            while (written < payload.size()) {
                auto n = ::write(fd, payload.data() + written, payload.size() - written);
                if (n < 0 && errno == EINTR) continue;
                if (n <= 0) throw std::runtime_error("manifest write failed");
                written += static_cast<std::size_t>(n);
            }
            if (::fsync(fd) != 0) throw std::runtime_error("manifest fsync failed");
            const int closeResult = ::close(fd);
            fd = -1;
            if (closeResult != 0) throw std::runtime_error("manifest close failed");
            if (::rename(temporary.data(), destination.c_str()) != 0)
                throw std::runtime_error("manifest rename failed");
            int directoryFd = ::open(directory.c_str(), O_RDONLY | O_DIRECTORY);
            if (directoryFd < 0) throw std::runtime_error("manifest directory open failed");
            const int synced = ::fsync(directoryFd);
            ::close(directoryFd);
            if (synced != 0) throw std::runtime_error("manifest directory fsync failed");
            ++counters.pageWrites;
        } catch (...) {
            if (fd >= 0) ::close(fd);
            ::unlink(temporary.data());
            throw;
        }
        return hash;
    }

    Link persist(Link value) {
        if (!value.dirty) return value;
        auto node = std::make_shared<Node>(*value.dirty);
        for (auto& child : node->children) child = persist(child);
        return {publish(encode(*node)), value.summary, {}};
    }

    std::pair<std::size_t, std::uint64_t> locate(const Node& node, std::uint64_t index) const {
        for (std::size_t i = 0; i < node.children.size(); ++i) {
            auto count = node.children[i].summary.chunkCount;
            if (index < count || i + 1 == node.children.size()) return {i, index};
            index -= count;
        }
        throw std::logic_error("manifest position not found");
    }

    std::vector<Link> split(std::shared_ptr<Node> node) {
        if (node->size() < splitAt()) return {link(std::move(node))};
        const auto middle = node->size() / 2;
        auto right = std::make_shared<Node>();
        right->leaf = node->leaf;
        if (node->leaf) {
            right->chunks.assign(node->chunks.begin() + middle, node->chunks.end());
            node->chunks.resize(middle);
        } else {
            right->children.assign(node->children.begin() + middle, node->children.end());
            node->children.resize(middle);
        }
        ++counters.splits;
        return {link(std::move(node)), link(std::move(right))};
    }

    enum class Edit { Insert, Erase, Update };
    std::vector<Link> edit(const Link& original, std::uint64_t index,
                           Edit kind, const ManifestChunk& chunk) {
        auto node = std::make_shared<Node>(*load(original));
        if (node->leaf) {
            auto position = node->chunks.begin() + static_cast<std::ptrdiff_t>(index);
            if (kind == Edit::Insert) node->chunks.insert(position, chunk);
            else if (kind == Edit::Erase) node->chunks.erase(position);
            else *position = chunk;
        } else {
            const auto [child, offset] = locate(*node, index);
            auto replacement = edit(node->children[child], offset, kind, chunk);
            node->children.erase(node->children.begin() + child);
            node->children.insert(node->children.begin() + child,
                                  replacement.begin(), replacement.end());
            if (kind == Edit::Erase && load(node->children[child])->size() < low())
                repair(*node, child);
        }
        return split(std::move(node));
    }

    void repair(Node& parent, std::size_t child) {
        if (parent.children.size() == 1) return;  // Root will be normalized.
        const auto left = child > 0 ? child - 1 : child;
        auto joined = std::make_shared<Node>(*load(parent.children[left]));
        auto right = load(parent.children[left + 1]);
        if (joined->leaf != right->leaf ||
            parent.children[left].summary.maxDepth != parent.children[left + 1].summary.maxDepth)
            throw std::runtime_error("cannot merge manifest siblings at different levels");
        if (joined->leaf)
            joined->chunks.insert(joined->chunks.end(), right->chunks.begin(), right->chunks.end());
        else
            joined->children.insert(joined->children.end(), right->children.begin(), right->children.end());
        const bool redistribute = joined->size() >= splitAt();
        auto replacements = split(std::move(joined));
        parent.children.erase(parent.children.begin() + left, parent.children.begin() + left + 2);
        parent.children.insert(parent.children.begin() + left, replacements.begin(), replacements.end());
        if (redistribute) ++counters.redistributions;
        else ++counters.merges;
    }

    void normalize() {
        auto node = load(root);
        while (!node->leaf && node->children.size() == 1) {
            root = node->children.front();
            node = load(root);
        }
    }

    void rebuild() {
        // Reuse entire leaves; no chunk-reference repacking during recovery.
        std::vector<Link> leaves;
        std::function<void(const Link&)> collect = [&](const Link& value) {
            if (value.summary.maxDepth == 0) {
                leaves.push_back(value);  // Cached depths identify leaves without reading them.
                return;
            }
            auto node = load(value);
            for (const auto& child : node->children) collect(child);
        };
        collect(root);
        while (leaves.size() > 1) {
            const auto groups = (leaves.size() + options.pageEntries - 1) / options.pageEntries;
            const auto each = leaves.size() / groups;
            const auto extra = leaves.size() % groups;
            std::vector<Link> next;
            std::size_t start = 0;
            for (std::size_t i = 0; i < groups; ++i) {
                auto count = each + (i < extra ? 1 : 0);
                auto node = std::make_shared<Node>();
                node->leaf = false;
                node->children.assign(leaves.begin() + start, leaves.begin() + start + count);
                next.push_back(link(std::move(node)));
                start += count;
            }
            leaves = std::move(next);
        }
        root = leaves.front();
        ++counters.internalRebuilds;
    }

    void rebuildFromLeaves(std::vector<Link> leaves) {
        if (leaves.empty()) {
            root = link(std::make_shared<Node>());
            return;
        }
        while (leaves.size() > 1) {
            const auto groups = (leaves.size() + options.pageEntries - 1) / options.pageEntries;
            const auto each = leaves.size() / groups;
            const auto extra = leaves.size() % groups;
            std::vector<Link> next;
            next.reserve(groups);
            std::size_t start = 0;
            for (std::size_t i = 0; i < groups; ++i) {
                const auto count = each + (i < extra ? 1 : 0);
                auto node = std::make_shared<Node>();
                node->leaf = false;
                node->children.assign(leaves.begin() + static_cast<std::ptrdiff_t>(start),
                                      leaves.begin() + static_cast<std::ptrdiff_t>(start + count));
                next.push_back(link(std::move(node)));
                start += count;
            }
            leaves = std::move(next);
        }
        root = std::move(leaves.front());
    }

    void replaceRange(std::uint64_t index, std::uint64_t eraseCount,
                      const std::vector<ManifestChunk>& replacement) {
        materializeAppend();
        const auto total = root.summary.chunkCount;
        if (index > total || eraseCount > total - index)
            throw std::out_of_range("manifest replacement range out of bounds");
        for (const auto& chunk : replacement) {
            checkHash(chunk.hash);
            if (!chunk.size) throw std::invalid_argument("manifest replacement chunk size must be positive");
        }
        if (!eraseCount && replacement.empty()) return;

        std::vector<Link> leaves;
        collectLeaves(root, leaves);
        if (leaves.empty()) {
            if (index != 0 || eraseCount != 0)
                throw std::logic_error("empty manifest replacement range is inconsistent");
            std::vector<ManifestChunk> middle = replacement;
            if (!middle.empty()) {
                const auto groups = (middle.size() + options.pageEntries - 1) / options.pageEntries;
                const auto each = middle.size() / groups;
                const auto extra = middle.size() % groups;
                std::vector<Link> created;
                created.reserve(groups);
                std::size_t start = 0;
                for (std::size_t i = 0; i < groups; ++i) {
                    const auto count = each + (i < extra ? 1 : 0);
                    auto leaf = std::make_shared<Node>();
                    leaf->chunks.assign(middle.begin() + static_cast<std::ptrdiff_t>(start),
                                        middle.begin() + static_cast<std::ptrdiff_t>(start + count));
                    created.push_back(link(std::move(leaf)));
                    start += count;
                }
                rebuildFromLeaves(std::move(created));
            }
            ++counters.operations;
            ++counters.balanceChecks;
            return;
        }

        std::vector<std::uint64_t> starts;
        starts.reserve(leaves.size());
        std::uint64_t offset = 0;
        for (const auto& leaf : leaves) {
            starts.push_back(offset);
            offset = checkedAdd(offset, leaf.summary.chunkCount);
        }

        auto leafAt = [&](std::uint64_t position) {
            // At EOF, attach an insertion to the previous leaf.  This keeps
            // the untouched right-hand leaves reusable for boundary inserts.
            if (position == total) return leaves.size() - 1;
            const auto it = std::upper_bound(starts.begin(), starts.end(), position);
            return static_cast<std::size_t>(std::distance(starts.begin(), it) - 1);
        };

        const auto first = leafAt(index);
        const auto end = checkedAdd(index, eraseCount);
        const auto last = eraseCount ? leafAt(end - 1) : first;
        const auto firstNode = load(leaves[first]);
        const auto lastNode = load(leaves[last]);
        if (!firstNode->leaf || !lastNode->leaf)
            throw std::logic_error("manifest leaf collection returned an internal node");

        const auto prefixCount = static_cast<std::size_t>(index - starts[first]);
        const auto suffixStart = static_cast<std::size_t>(end - starts[last]);
        std::vector<ManifestChunk> middle;
        middle.reserve(prefixCount + replacement.size() + (lastNode->chunks.size() - suffixStart));
        middle.insert(middle.end(), firstNode->chunks.begin(),
                      firstNode->chunks.begin() + static_cast<std::ptrdiff_t>(prefixCount));
        middle.insert(middle.end(), replacement.begin(), replacement.end());
        middle.insert(middle.end(), lastNode->chunks.begin() + static_cast<std::ptrdiff_t>(suffixStart),
                      lastNode->chunks.end());

        std::size_t before = first;
        std::size_t after = last + 1;
        // The persistent tree requires every non-root leaf to be at least
        // half full.  Borrow one adjacent immutable leaf only when the edited
        // boundary is too small, then repartition that small local region.
        if (middle.size() < low() && before) {
            const auto previous = load(leaves[before - 1]);
            if (!previous->leaf) throw std::logic_error("manifest predecessor is not a leaf");
            middle.insert(middle.begin(), previous->chunks.begin(), previous->chunks.end());
            --before;
        }
        if (middle.size() < low() && after < leaves.size()) {
            const auto following = load(leaves[after]);
            if (!following->leaf) throw std::logic_error("manifest successor is not a leaf");
            middle.insert(middle.end(), following->chunks.begin(), following->chunks.end());
            ++after;
        }

        std::vector<Link> next;
        next.reserve(before + after - last + 2);
        next.insert(next.end(), leaves.begin(), leaves.begin() + static_cast<std::ptrdiff_t>(before));
        if (!middle.empty()) {
            // Spread the boundary data evenly.  Besides satisfying occupancy,
            // this avoids a repeated one-chunk edit ever copying a whole page.
            const auto groups = (middle.size() + options.pageEntries - 1) / options.pageEntries;
            const auto each = middle.size() / groups;
            const auto extra = middle.size() % groups;
            std::size_t start = 0;
            for (std::size_t i = 0; i < groups; ++i) {
                const auto count = each + (i < extra ? 1 : 0);
                auto leaf = std::make_shared<Node>();
                leaf->chunks.assign(middle.begin() + static_cast<std::ptrdiff_t>(start),
                                    middle.begin() + static_cast<std::ptrdiff_t>(start + count));
                next.push_back(link(std::move(leaf)));
                start += count;
            }
        }
        next.insert(next.end(), leaves.begin() + static_cast<std::ptrdiff_t>(after), leaves.end());
        rebuildFromLeaves(std::move(next));
        ++counters.operations;
        ++counters.balanceChecks;
    }

    void apply(std::uint64_t index, Edit kind, const ManifestChunk& chunk = {}) {
        materializeAppend();
        if (index > root.summary.chunkCount ||
            (kind != Edit::Insert && index == root.summary.chunkCount))
            throw std::out_of_range("manifest chunk index out of range");
        if (kind != Edit::Erase) {
            checkHash(chunk.hash);
            if (chunk.size == 0) throw std::invalid_argument("manifest chunk size must be positive");
        }
        // Compute on a new path. Overflow/validation errors leave the root unchanged.
        auto replacement = edit(root, index, kind, chunk);
        Link next;
        if (replacement.size() == 1) next = replacement.front();
        else {
            auto node = std::make_shared<Node>();
            node->leaf = false;
            node->children = std::move(replacement);
            next = link(std::move(node));
        }
        root = std::move(next);
        normalize();
        ++counters.operations;
        ++counters.balanceChecks;
        if (root.summary.maxDepth - root.summary.minDepth > options.maxDepthDifference)
            rebuild();
    }

    void walk(const Link& value, const std::function<void(const ManifestChunk&)>& visitor) const {
        auto node = load(value);
        if (node->leaf) for (const auto& chunk : node->chunks) visitor(chunk);
        else for (const auto& child : node->children) walk(child, visitor);
    }

    void validateNode(const Link& value, bool isRoot) const {
        auto node = load(value);
        if (node->size() >= splitAt() || (!isRoot && node->size() < low()) ||
            (isRoot && !node->leaf && node->size() < 2))
            throw std::runtime_error("manifest occupancy invariant violated");
        if (!sameSummary(value.summary, summarize(*node)))
            throw std::runtime_error("manifest summary invariant violated");
        if (value.summary.minDepth != value.summary.maxDepth)
            throw std::runtime_error("manifest leaf depths differ");
        for (const auto& child : node->children) validateNode(child, false);
    }
};

FileManifest::FileManifest(std::string root, ManifestOptions options, std::string hash)
    : impl_(std::make_unique<Impl>(std::move(root), options, hash)) {}
FileManifest::~FileManifest() = default;
FileManifest::FileManifest(FileManifest&&) noexcept = default;
FileManifest& FileManifest::operator=(FileManifest&&) noexcept = default;
ManifestSummary FileManifest::summary() const {
    return impl_->appendMode ? impl_->appendSummary : impl_->root.summary;
}
ManifestStats FileManifest::stats() const { return impl_->counters; }
ManifestChunk FileManifest::at(std::uint64_t index) const {
    if (index >= summary().chunkCount) throw std::out_of_range("manifest chunk index out of range");
    if (impl_->appendMode) return impl_->appendAt(index);
    auto value = impl_->root;
    auto node = impl_->load(value);
    while (!node->leaf) {
        const auto [child, offset] = impl_->locate(*node, index);
        value = node->children[child];
        index = offset;
        node = impl_->load(value);
    }
    return node->chunks[static_cast<std::size_t>(index)];
}
void FileManifest::append(const ManifestChunk& c) { impl_->append(c); }
void FileManifest::insert(std::uint64_t i, const ManifestChunk& c) { impl_->apply(i, Impl::Edit::Insert, c); }
void FileManifest::erase(std::uint64_t i) { impl_->apply(i, Impl::Edit::Erase); }
void FileManifest::update(std::uint64_t i, const ManifestChunk& c) { impl_->apply(i, Impl::Edit::Update, c); }
void FileManifest::replaceRange(std::uint64_t i, std::uint64_t n,
                                const std::vector<ManifestChunk>& replacement) {
    impl_->replaceRange(i, n, replacement);
}
void FileManifest::forEach(const std::function<void(const ManifestChunk&)>& visitor) const {
    if (impl_->appendMode) {
        impl_->walkAppend(visitor);
        return;
    }
    impl_->walk(impl_->root, visitor);
}
std::string FileManifest::snapshot() {
    if (impl_->appendMode) return impl_->snapshotAppend();
    impl_->root = impl_->persist(impl_->root);
    return impl_->root.hash;
}
void FileManifest::rebalanceInternal() { impl_->materializeAppend(); impl_->rebuild(); }
void FileManifest::validate() const {
    auto* mutableImpl = const_cast<Impl*>(impl_.get());
    mutableImpl->materializeAppend();
    mutableImpl->validateNode(mutableImpl->root, true);
}

ManifestGcResult FileManifest::garbageCollectPages(
    const std::string& chunkRoot, const std::vector<std::string>& liveRoots,
    ManifestOptions options) {
    std::unordered_set<std::string> reachable;
    for (const auto& rootHash : liveRoots) {
        checkHash(rootHash);
        FileManifest manifest(chunkRoot, options, rootHash);
        manifest.impl_->collectReachable(manifest.impl_->root, reachable);
    }

    ManifestGcResult result;
    result.reachablePages = reachable.size();
    const auto directory = std::filesystem::path(chunkRoot) / MANIFEST_STORAGE_DIR;
    std::error_code error;
    if (!std::filesystem::exists(directory, error)) {
        if (error) throw std::runtime_error("checking manifest directory failed: " + error.message());
        return result;
    }
    if (!std::filesystem::is_directory(directory, error))
        throw std::runtime_error("manifest storage path is not a directory: " + directory.string());

    for (const auto& entry : std::filesystem::directory_iterator(directory)) {
        if (!entry.is_regular_file()) continue;
        const auto name = entry.path().filename().string();
        try {
            checkHash(name);
        } catch (const std::invalid_argument&) {
            // Temporary/incomplete files are not content-addressed manifest
            // objects and are deliberately left for their owning publisher.
            continue;
        }
        if (reachable.count(name)) continue;
        if (!std::filesystem::remove(entry.path(), error)) {
            if (error) throw std::runtime_error("removing unreachable manifest page failed: " + error.message());
            continue;
        }
        ++result.deletedPages;
    }
    return result;
}

}  // namespace chunking
