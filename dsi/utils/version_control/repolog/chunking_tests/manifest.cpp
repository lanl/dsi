#include "manifest.hpp"
#include "sha256.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <list>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace chunking {
namespace {
constexpr const char* kMagic = "DSIMCP01";
constexpr std::size_t kHeaderSize = 41;

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

    Impl(std::string chunkRoot, ManifestOptions opt, const std::string& hash)
        : directory(std::move(chunkRoot) + "/" + MANIFEST_STORAGE_DIR), options(opt) {
        if (options.pageEntries < 4 || options.pageEntries > 65536)
            throw std::invalid_argument("manifest pageEntries must be between 4 and 65536");
        if (options.cachedPages == 0)
            throw std::invalid_argument("manifest cache must hold at least one page");
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
        const auto maximum = kHeaderSize + splitAt() * 56;
        if (length < static_cast<std::streamoff>(kHeaderSize) ||
            length > static_cast<std::streamoff>(maximum))
            throw std::runtime_error("invalid manifest object length");
        std::string payload(static_cast<std::size_t>(length), '\0');
        in.seekg(0);
        if (!in.read(payload.data(), static_cast<std::streamsize>(payload.size())))
            throw std::runtime_error("cannot read manifest object");
        if (sha256Hex(payload) != hash) throw std::runtime_error("manifest checksum mismatch");
        if (payload.compare(0, 8, kMagic) != 0)
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
        if (count >= splitAt() || payload.size() != kHeaderSize + count * (kind == 0 ? 40 : 56))
            throw std::runtime_error("invalid manifest entry count");
        auto node = std::make_shared<Node>();
        node->leaf = kind == 0;
        for (std::uint64_t i = 0; i < count; ++i) {
            auto digest = getHash(payload, pos);
            if (node->leaf) {
                auto size = get(payload, pos, 8);
                if (size == 0) throw std::runtime_error("zero-length manifest chunk");
                node->chunks.push_back({std::move(digest), size});
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

    std::string encode(const Node& node) const {
        const auto s = summarize(node);
        std::string payload(kMagic, 8);
        put(payload, options.pageEntries, 4);
        put(payload, node.leaf ? 0 : 1, 1);
        put(payload, node.size(), 4);
        put(payload, s.chunkCount, 8);
        put(payload, s.byteCount, 8);
        put(payload, s.minDepth, 4);
        put(payload, s.maxDepth, 4);
        if (node.leaf) {
            for (const auto& c : node.chunks) { putHash(payload, c.hash); put(payload, c.size, 8); }
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

    void apply(std::uint64_t index, Edit kind, const ManifestChunk& chunk = {}) {
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
ManifestSummary FileManifest::summary() const { return impl_->root.summary; }
ManifestStats FileManifest::stats() const { return impl_->counters; }
ManifestChunk FileManifest::at(std::uint64_t index) const {
    if (index >= summary().chunkCount) throw std::out_of_range("manifest chunk index out of range");
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
void FileManifest::insert(std::uint64_t i, const ManifestChunk& c) { impl_->apply(i, Impl::Edit::Insert, c); }
void FileManifest::erase(std::uint64_t i) { impl_->apply(i, Impl::Edit::Erase); }
void FileManifest::update(std::uint64_t i, const ManifestChunk& c) { impl_->apply(i, Impl::Edit::Update, c); }
void FileManifest::forEach(const std::function<void(const ManifestChunk&)>& visitor) const {
    impl_->walk(impl_->root, visitor);
}
std::string FileManifest::snapshot() {
    impl_->root = impl_->persist(impl_->root);
    return impl_->root.hash;
}
void FileManifest::rebalanceInternal() { impl_->rebuild(); }
void FileManifest::validate() const { impl_->validateNode(impl_->root, true); }

}  // namespace chunking
