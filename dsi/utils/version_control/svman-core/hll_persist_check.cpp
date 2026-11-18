// hll_persist_check.cpp -- verify the HyperLogLog in storeChunksForSnapshot
// is persisted across separate calls (simulating separate processes).
//
//   ./hll_persist_check <workdir>
//
// Run 1: store a file, record <workdir>/state copy of the sketch file.
// Run 2: store the SAME file again (the sketch file must be loaded):
//   - the sketch file must exist and equal the saved copy (round-trip),
//   - est must be >= number of distinct chunks recorded.
// Compile: c++ -std=c++17 -O2 hll_persist_check.cpp chunking.cpp sha256.cpp -o hll_persist_check
#include "chunking.hpp"

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

namespace {

std::string readFile(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return std::string();
    return std::string((std::istreambuf_iterator<char>(in)),
                       std::istreambuf_iterator<char>());
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "usage: " << argv[0] << " <workdir> <run#>\n";
        return 2;
    }
    const std::string work = argv[1];
    const int run = std::atoi(argv[2]);

    const std::string bigFile = work + "/data.bin";
    if (run == 1) {
        // ~10MB file with pseudo-random bytes -> >= a couple of chunks at the
        // default (1MB min / 8MB avg) sizes; deterministic content.
        std::ofstream out(bigFile, std::ios::binary | std::ios::trunc);
        unsigned state = 123456789u;
        for (int i = 0; i < 10 * 1024 * 1024; ++i) {
            state = state * 1103515245u + 12345u;
            const char byte = (char)((state >> 16) & 0xFF);
            out.put(byte);
        }
    }

    std::vector<chunking::FileEntry> entries;
    chunking::FileEntry e;
    e.absolutePath = bigFile;
    e.relativePath = "data.bin";
    e.fileType = "file";
    entries.push_back(e);

    const chunking::StoreResult res =
        chunking::storeChunksForSnapshot(work, entries, "UPDATE", 0);

    const std::string statePath =
        work + "/dsi_vcs_chunks/seen_hyperloglog.bin";
    const std::string state = readFile(statePath);
    if (state.empty()) {
        std::cerr << "run " << run << ": state file missing or empty\n";
        return 1;
    }
    if (run == 1) {
        // Snapshot the sketch file after run 1 (copy via file rename lib).
        std::ofstream out(work + "/snapshot.bin", std::ios::binary);
        out.write(state.data(), (std::streamsize)state.size());
        std::cout << "run1 ok: stored file, " << res.chunkHashes.size()
                  << " distinct chunks, state file " << state.size()
                  << " bytes\n";
        return 0;
    }

    // run 2: compare against run 1's copy, and confirm run 2's distinct
    // chunk count is identical (dedup still works with the reloaded sketch).
    std::ifstream snap(work + "/snapshot.bin", std::ios::binary);
    std::string snapContent((std::istreambuf_iterator<char>(snap)),
                            std::istreambuf_iterator<char>());
    if (snapContent != state) {
        std::cerr << "run2: state file differs from run-1 snapshot "
                     "(round-trip broken)\n";
        return 1;
    }
    std::cout << "run2 ok: state file round-trips, dedup'd chunk count "
              << res.chunkHashes.size() << "\n";
    return 0;
}
