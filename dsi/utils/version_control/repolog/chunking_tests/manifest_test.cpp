#include "manifest.hpp"
#include "chunking.hpp"
#include "sha256.hpp"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <random>
#include <set>
#include <stdexcept>
#include <vector>
#include <unistd.h>

using namespace chunking;
namespace fs = std::filesystem;

namespace {
void require(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}
template<class F> void mustThrow(F action, const std::string& message) {
    bool threw = false;
    try { action(); } catch (const std::exception&) { threw = true; }
    require(threw, message);
}
struct Temporary {
    fs::path path;
    Temporary() {
        std::string pattern = (fs::temp_directory_path() / "dsi-manifest-XXXXXX").string();
        std::vector<char> name(pattern.begin(), pattern.end());
        name.push_back('\0');
        if (!::mkdtemp(name.data())) throw std::runtime_error("mkdtemp failed");
        path = name.data();
    }
    ~Temporary() { std::error_code error; fs::remove_all(path, error); }
};
ManifestOptions small(std::size_t entries = 4) { return {entries, 2, 4}; }
ManifestChunk entry(std::uint64_t i) { return {sha256Hex(std::to_string(i)), i + 1}; }
std::vector<ManifestChunk> values(const FileManifest& tree) {
    std::vector<ManifestChunk> out;
    tree.forEach([&](const ManifestChunk& c) { out.push_back(c); });
    return out;
}
void compare(const FileManifest& tree, const std::vector<ManifestChunk>& expected) {
    tree.validate();
    require(values(tree) == expected, "ordered chunks differ");
    require(tree.summary().chunkCount == expected.size(), "chunk total differs");
    std::uint64_t bytes = 0;
    for (const auto& c : expected) bytes += c.size;
    require(tree.summary().byteCount == bytes, "byte total differs");
    require(tree.stats().balanceChecks == tree.stats().operations, "missing per-operation balance check");
    require(tree.summary().maxDepth == tree.summary().minDepth, "leaf depths differ");
}
std::string read(const fs::path& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) throw std::runtime_error("test input missing");
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}
void write(const fs::path& path, const std::string& bytes) {
    std::ofstream output(path, std::ios::binary);
    output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    if (!output) throw std::runtime_error("test write failed");
}
// Inspect the documented disk format independently to check leaf reuse/order.
std::vector<std::string> leafHashes(const fs::path& root, const std::string& hash) {
    auto bytes = read(root / MANIFEST_STORAGE_DIR / hash);
    if (bytes.at(12) == 0) return {hash};
    std::vector<std::string> leaves;
    const char hex[] = "0123456789abcdef";
    for (std::size_t pos = 41; pos < bytes.size(); pos += 56) {
        std::string child;
        for (unsigned i = 0; i < 32; ++i) {
            auto c = static_cast<unsigned char>(bytes[pos + i]);
            child += hex[c >> 4]; child += hex[c & 15];
        }
        auto nested = leafHashes(root, child);
        leaves.insert(leaves.end(), nested.begin(), nested.end());
    }
    return leaves;
}

void boundariesAndHistory() {
    Temporary dir;
    for (auto b : {4u, 5u, 8u}) {
        FileManifest tree(dir.path.string(), small(b));
        std::vector<ManifestChunk> expected;
        const auto empty = tree.snapshot();
        FileManifest oldEmpty(dir.path.string(), small(b), empty);
        compare(oldEmpty, {});
        for (unsigned i = 0; i < 200; ++i) {
            tree.insert(tree.summary().chunkCount, entry(i));
            expected.push_back(entry(i));
            compare(tree, expected);
        }
        require(tree.summary().maxDepth >= 2, "did not exercise internal splits");
        require(tree.stats().pageWrites == 1, "dirty pages were not coalesced");
        auto original = tree.snapshot();
        auto immutable = read(dir.path / MANIFEST_STORAGE_DIR / original);
        FileManifest branch(dir.path.string(), small(b), original);
        branch.insert(1, entry(900));
        auto branchRoot = branch.snapshot();
        require(branchRoot != original, "branch edit did not change root");
        FileManifest previous(dir.path.string(), small(b), original);
        compare(previous, expected);
        require(read(dir.path / MANIFEST_STORAGE_DIR / original) == immutable, "old root mutated");
        while (!expected.empty()) {
            const auto middle = expected.size() / 2;
            tree.erase(middle);
            expected.erase(expected.begin() + middle);
            compare(tree, expected);
        }
        require(tree.snapshot() == empty, "empty root is not canonical");
        require(tree.stats().merges > 0, "did not exercise cascading merges");
        require(tree.stats().internalRebuilds == 0, "normal edits triggered global rebuild");
    }
}

void localSharingAndMerges() {
    Temporary dir;
    FileManifest tree(dir.path.string(), small());
    for (unsigned i = 0; i < 12; ++i) tree.insert(i, entry(i));
    auto before = tree.snapshot();
    auto leaves = leafHashes(dir.path, before);
    require(leaves.size() == 3, "expected three leaves");
    auto writes = tree.stats().pageWrites;
    tree.insert(1, entry(99));
    auto inserted = tree.snapshot();
    auto after = leafHashes(dir.path, inserted);
    require(after[1] == leaves[1] && after[2] == leaves[2], "insertion shifted suffix pages");
    require(tree.stats().pageWrites - writes == 2, "insertion rewrote unaffected pages");
    tree.erase(1);
    require(tree.snapshot() == before, "inverse edit did not restore original page objects");
    // Middle leaf has both siblings: reduce 4 -> 1 and require left preference.
    tree.erase(4); tree.erase(4); tree.erase(4);
    after = leafHashes(dir.path, tree.snapshot());
    require(after.size() == 2 && after.back() == leaves.back(), "merge did not prefer left sibling");
    tree.validate();
    FileManifest rightFallback(dir.path.string(), small(), before);
    rightFallback.erase(0); rightFallback.erase(0); rightFallback.erase(0);
    auto fallback = leafHashes(dir.path, rightFallback.snapshot());
    require(fallback.size() == 2 && fallback.back() == leaves.back(), "leftmost leaf did not merge right");
    rightFallback.validate();

    FileManifest redistribution(dir.path.string(), small());
    for (unsigned i = 0; i < 8; ++i) redistribution.insert(i, entry(i));
    for (unsigned i = 0; i < 3; ++i) redistribution.insert(0, entry(100 + i));
    // Left leaf 7, right leaf 4. Deleting three from the right forces 7+1 -> 4+4.
    redistribution.erase(7); redistribution.erase(7); redistribution.erase(7);
    require(redistribution.stats().redistributions == 1, "oversized merge was not redistributed");
    redistribution.validate();
}

void randomizedEdits() {
    Temporary dir;
    std::mt19937_64 random(20261105);
    for (auto b : {4u, 5u, 16u}) {
        auto tree = std::make_unique<FileManifest>(dir.path.string(), small(b));
        std::vector<ManifestChunk> expected;
        for (unsigned step = 0; step < 4000; ++step) {
            auto choice = random() % 4;
            auto value = entry(random() % 10000);
            if (expected.empty() || choice < 2) {
                auto i = random() % (expected.size() + 1);
                tree->insert(i, value);
                expected.insert(expected.begin() + i, value);
            } else if (choice == 2) {
                auto i = random() % expected.size();
                tree->erase(i); expected.erase(expected.begin() + i);
            } else {
                auto i = random() % expected.size();
                tree->update(i, value); expected[i] = value;
            }
            compare(*tree, expected);
            if (step % 97 == 0) {
                const auto root = tree->snapshot();
                tree = std::make_unique<FileManifest>(dir.path.string(), small(b), root);
                compare(*tree, expected);
            }
        }
        auto root = tree->snapshot();
        auto oldLeaves = leafHashes(dir.path, root);
        const auto readsBefore = tree->stats().pageReads;
        tree->rebalanceInternal();
        require(tree->stats().pageReads - readsBefore < oldLeaves.size(),
                "internal recovery unexpectedly scanned leaf payloads");
        auto rebuiltLeaves = leafHashes(dir.path, tree->snapshot());
        require(oldLeaves == rebuiltLeaves, "internal rebuild rewrote leaves");
        compare(*tree, expected);
    }
}

void depthSafeguard() {
    Temporary dir;
    FileManifest leaf(dir.path.string(), small());
    leaf.insert(0, entry(0)); leaf.insert(1, entry(1));
    const auto leafRoot = leaf.snapshot();
    struct Ref { std::string hash; ManifestSummary summary; };
    Ref base{leafRoot, leaf.summary()}, current = base;
    auto integer = [](std::string& out, std::uint64_t n, unsigned bytes) {
        for (unsigned i = bytes; i > 0; --i) out.push_back(static_cast<char>(n >> ((i - 1) * 8)));
    };
    auto reference = [&](std::string& out, const Ref& ref) {
        for (unsigned i = 0; i < 64; i += 2)
            out.push_back(static_cast<char>(std::stoul(ref.hash.substr(i, 2), nullptr, 16)));
        integer(out, ref.summary.chunkCount, 8); integer(out, ref.summary.byteCount, 8);
        integer(out, ref.summary.minDepth, 4); integer(out, ref.summary.maxDepth, 4);
    };
    // Construct a validly encoded imported tree with leaf depths 1 through 4.
    // Local operations never produce this shape, so exercise the recovery path explicitly.
    for (unsigned level = 0; level < 4; ++level) {
        ManifestSummary summary{current.summary.chunkCount + base.summary.chunkCount,
                                current.summary.byteCount + base.summary.byteCount,
                                1, current.summary.maxDepth + 1};
        std::string bytes = "DSIMCP01";
        integer(bytes, 4, 4); integer(bytes, 1, 1); integer(bytes, 2, 4);
        integer(bytes, summary.chunkCount, 8); integer(bytes, summary.byteCount, 8);
        integer(bytes, summary.minDepth, 4); integer(bytes, summary.maxDepth, 4);
        reference(bytes, current); reference(bytes, base);
        current = {sha256Hex(bytes), summary};
        write(dir.path / MANIFEST_STORAGE_DIR / current.hash, bytes);
    }
    FileManifest imported(dir.path.string(), small(), current.hash);
    auto expected = values(imported);
    imported.update(0, entry(0));
    require(imported.stats().balanceChecks == 1 && imported.stats().internalRebuilds == 1,
            "depth safeguard did not run after an update");
    compare(imported, expected);
    imported.snapshot();
}

void validationAndCorruption() {
    Temporary dir;
    mustThrow([&] { FileManifest bad(dir.path.string(), small(3)); }, "invalid page size accepted");
    FileManifest tree(dir.path.string(), small());
    mustThrow([&] { tree.erase(0); }, "empty erase accepted");
    mustThrow([&] { tree.update(0, entry(0)); }, "empty update accepted");
    mustThrow([&] { tree.insert(1, entry(0)); }, "out of bounds insertion accepted");
    mustThrow([&] { tree.insert(0, {"bad", 1}); }, "invalid hash accepted");
    mustThrow([&] { tree.insert(0, {entry(0).hash, 0}); }, "zero-sized chunk accepted");
    tree.insert(0, {entry(0).hash, std::numeric_limits<std::uint64_t>::max()});
    mustThrow([&] { tree.insert(1, entry(1)); }, "byte overflow accepted");
    require(tree.summary().chunkCount == 1, "failed operation changed root");
    tree.erase(0);
    for (unsigned i = 0; i < 32; ++i) tree.insert(i, entry(i));
    auto root = tree.snapshot();
    mustThrow([&] { FileManifest wrong(dir.path.string(), small(5), root); }, "capacity mismatch accepted");
    auto leaves = leafHashes(dir.path, root);
    auto leafPath = dir.path / MANIFEST_STORAGE_DIR / leaves.front();
    auto leafBytes = read(leafPath);
    write(leafPath, "corrupted");
    mustThrow([&] {
        FileManifest corrupt(dir.path.string(), small(), root);
        values(corrupt);
    }, "corrupted child accepted");
    write(leafPath, leafBytes);
    fs::remove(leafPath);
    mustThrow([&] {
        FileManifest missing(dir.path.string(), small(), root);
        values(missing);
    }, "missing child accepted");
    // A correctly named but malformed object must fail structural validation.
    std::string bad(41, '\0');
    auto badHash = sha256Hex(bad);
    write(dir.path / MANIFEST_STORAGE_DIR / badHash, bad);
    mustThrow([&] { FileManifest malformed(dir.path.string(), small(), badHash); }, "malformed object accepted");
}

void storageRoundTrip() {
    Temporary dir;
    auto source = dir.path / "input.bin";
    auto repo = dir.path / "repo";
    std::string initial(2 * 1024 * 1024 + 17, 'a');
    write(source, initial);
    const std::string relative = "nested/file.bin";
    FileEntry file{source.string(), relative, "file"};
    auto first = storeChunksForSnapshot(repo.string(), {file});
    require(first.manifestRoots.size() == 1, "store did not return root");
    auto same = storeChunksForSnapshot(repo.string(), {file}, "UPDATE", 0, first.manifestRoots);
    require(same.manifestRoots == first.manifestRoots, "unchanged file did not reuse root");
    auto checkRestore = [&](const StoreResult& result, const std::string& expected, const std::string& name) {
        const auto output = dir.path / name;
        require(rebuildFileFromManifest(repo.string(), relative, result.manifestRoots.at(relative),
                                       result.fileHashes.at(relative), output.string()), "manifest restore failed");
        require(read(output / relative) == expected, "restored bytes differ");
    };
    const auto edited = initial.substr(0, 33) + "INSERTED" + initial.substr(33);
    write(source, edited);
    auto second = storeChunksForSnapshot(repo.string(), {file}, "UPDATE", 0, first.manifestRoots);
    write(source, "");
    auto empty = storeChunksForSnapshot(repo.string(), {file}, "UPDATE", 0, second.manifestRoots);
    checkRestore(first, initial, "old");
    checkRestore(second, edited, "new");
    checkRestore(empty, "", "empty");
    auto parallel = storeChunksForSnapshot(repo.string(), {file}, "UPDATE", 2, empty.manifestRoots);
    require(parallel.manifestRoots == empty.manifestRoots, "parallel manifest differs");
    require(!rebuildFileFromManifest(repo.string(), relative, first.manifestRoots.at(relative),
                                    std::string(64, '0'), (dir.path / "bad").string()), "wrong file hash accepted");
    require(!rebuildFileFromManifest(repo.string(), "../escape", first.manifestRoots.at(relative),
                                    first.fileHashes.at(relative), dir.path.string()), "escaping path accepted");
    // Correct manifest and file hash must still reject damaged payload bytes.
    FileManifest stored(repo.string(), {}, first.manifestRoots.at(relative));
    auto payload = repo / "dsi_vcs_chunks" / stored.at(0).hash;
    write(payload, "bad bytes");
    require(!rebuildFileFromManifest(repo.string(), relative, first.manifestRoots.at(relative),
                                    first.fileHashes.at(relative), (dir.path / "old").string()),
            "corrupt payload accepted");
    require(read(dir.path / "old" / relative) == initial, "failed restore overwrote existing file");
    for (const auto& file : fs::directory_iterator(dir.path / "old" / "nested"))
        require(file.path().filename() == "file.bin", "failed restore leaked temporary file");
}

void streamingSequenceMatching() {
    Temporary dir;
    const auto source = dir.path / "sequence.bin";
    const auto repo = dir.path / "repo";
    std::uint64_t state = 123456789;
    std::string original(20 * 1024 * 1024 + 17, '\0');
    for (char& c : original) {
        state ^= state << 13; state ^= state >> 7; state ^= state << 17;
        c = static_cast<char>(state >> 32);
    }
    write(source, original);
    const FileEntry file{source.string(), "sequence.bin", "file"};
    const auto first = storeChunksForSnapshot(repo.string(), {file});
    FileManifest old(repo.string(), {}, first.manifestRoots.at(file.relativePath));
    require(old.summary().chunkCount > 1, "sequence fixture requires multiple chunks");
    const auto prefix = original.substr(0, static_cast<std::size_t>(old.at(0).size));
    write(source, prefix + original);
    const auto added = storeChunksForSnapshot(repo.string(), {file}, "UPDATE", 0, first.manifestRoots);
    FileManifest inserted(repo.string(), {}, added.manifestRoots.at(file.relativePath));
    // An inserted byte sequence may change nearby FastCDC boundaries. Compare
    // with independently chunking the new file, not an assumed chunk insertion.
    std::vector<ManifestChunk> expected;
    chunkFileStream(source.string(), [](const std::string&) { return false; },
                    [&](const Chunk& chunk) { expected.push_back({chunk.sha256, chunk.size}); });
    require(values(inserted) == expected, "automatic insertion matching changed chunk order");
    write(source, original);
    const auto deleted = storeChunksForSnapshot(repo.string(), {file}, "UPDATE", 0, added.manifestRoots);
    require(deleted.manifestRoots == first.manifestRoots,
            "automatic deletion matching failed to recover original manifest");
    const auto parallel = storeChunksForSnapshot(repo.string(), {file}, "UPDATE", 2, first.manifestRoots);
    require(parallel.manifestRoots == first.manifestRoots, "parallel nonempty manifest differs");
    require(rebuildFileFromManifest(repo.string(), file.relativePath,
                                    added.manifestRoots.at(file.relativePath),
                                    added.fileHashes.at(file.relativePath), (dir.path / "added").string()),
            "inserted file restoration failed");
    require(read(dir.path / "added" / file.relativePath) == prefix + original,
            "inserted file bytes differ");
}
}  // namespace

int main() {
    try {
        boundariesAndHistory(); std::cout << "PASS boundaries, splits, cascading deletion, history\n";
        localSharingAndMerges(); std::cout << "PASS local sharing, sibling preference, redistribution\n";
        randomizedEdits(); std::cout << "PASS 12,000 deterministic random edits and internal-only rebuild\n";
        depthSafeguard(); std::cout << "PASS automatic depth recovery after individual update\n";
        validationAndCorruption(); std::cout << "PASS invalid operations, overflow, missing/corrupt objects\n";
        storageRoundTrip(); std::cout << "PASS chunk store integration and historical restoration\n";
        streamingSequenceMatching(); std::cout << "PASS automatic insertion/deletion matching and parallel storage\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "FAIL " << e.what() << '\n';
        return 1;
    }
}
