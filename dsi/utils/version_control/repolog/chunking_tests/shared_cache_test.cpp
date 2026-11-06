#include "shared_existence_cache.hpp"
#include "sha256.hpp"

#include <algorithm>
#include <cerrno>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>
#include <poll.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {
using chunking::detail::SharedExistenceCache;
namespace fs = std::filesystem;
void require(bool ok, const char* message) {
    if (!ok) throw std::runtime_error(message);
}
struct Scratch {
    std::string path;
    Scratch() {
        char name[] = "/tmp/chunk-cache-test-XXXXXX";
        const auto dir = ::mkdtemp(name);
        if (!dir) throw std::runtime_error("mkdtemp failed");
        path = dir;
    }
    ~Scratch() { std::error_code error; fs::remove_all(path, error); }
};
struct Pipe {
    int fd[2];
    Pipe() { if (::pipe(fd)) throw std::runtime_error("pipe failed"); }
    ~Pipe() { ::close(fd[0]); ::close(fd[1]); }
    void send() const { require(::write(fd[1], "x", 1) == 1, "pipe write failed"); }
    void receive() const {
        struct pollfd ready {fd[0], POLLIN, 0};
        int result;
        do { result = ::poll(&ready, 1, 10000); } while (result < 0 && errno == EINTR);
        char byte;
        require(result == 1 && ::read(fd[0], &byte, 1) == 1, "worker synchronization timed out");
    }
};
struct Children {
    std::vector<pid_t> pids;
    ~Children() {
        for (const auto pid : pids) if (pid > 0) ::kill(pid, SIGKILL);
        for (const auto pid : pids) if (pid > 0)
            while (::waitpid(pid, nullptr, 0) < 0 && errno == EINTR) {}
    }
    template<class F> void spawn(F action) {
        const auto pid = ::fork();
        if (pid < 0) throw std::runtime_error("fork failed");
        if (pid == 0) {
            // Bound even a lock-regression failure; _exit avoids destroying
            // parent-owned scratch/cache state inherited across fork.
            ::alarm(15);
            try { action(); ::_exit(0); }
            catch (...) { ::_exit(1); }
        }
        pids.push_back(pid);
    }
    void join() {
        for (auto& pid : pids) {
            int status = 0;
            pid_t result;
            do { result = ::waitpid(pid, &status, 0); } while (result < 0 && errno == EINTR);
            pid = -1;
            require(result > 0 && WIFEXITED(status) && WEXITSTATUS(status) == 0, "child failed");
        }
    }
};
std::string read(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

void sharedDedup(const std::string& root) {
    const std::string payload(1024 * 1024, 'a');
    const auto hash = chunking::sha256Hex(payload);
    SharedExistenceCache cache(root);
    Pipe gate;
    Children children;
    for (int i = 0; i < 8; ++i) children.spawn([&] {
        gate.receive();
        for (int n = 0; n < 10; ++n) cache.store(hash, payload.data(), payload.size());
    });
    for (int i = 0; i < 8; ++i) gate.send();
    children.join();
    require(cache.stats().published == 1 && cache.stats().diskChecks == 1 && cache.stats().hits == 79,
            "same-hash workers did not share publication/cache hits");
    require(read(root + "/" + hash) == payload, "published bytes differ");
    const auto ranks = cache.registers();
    require(*std::max_element(ranks.begin(), ranks.end()) <= 53 &&
            *std::max_element(ranks.begin(), ranks.end()) > 0, "invalid shared HLL ranks");
    // Losing a persisted hint must never cause an existing blob rewrite.
    SharedExistenceCache fresh(root);
    require(!fresh.store(hash, payload.data(), payload.size()) && fresh.stats().published == 0,
            "fresh cache rewrote an existing blob");
    auto invalid = ranks; invalid[0] = 255;
    SharedExistenceCache recovered(root, invalid);
    require(recovered.registers()[0] == 0 && !recovered.store(hash, payload.data(), payload.size()),
            "invalid legacy HLL recovery failed");
}

void failureRecovery(const std::string& root) {
    const std::string payload = "retry after a failed publication";
    const auto hash = chunking::sha256Hex(payload);
    bool fail = true;
    SharedExistenceCache cache(root, {}, [&](const auto& key, const char* bytes, auto size) {
        if (fail) throw std::runtime_error("injected write failure");
        chunking::detail::publishChunkBlob(root, key, bytes, size);
    });
    bool threw = false;
    try { cache.store(hash, payload.data(), payload.size()); } catch (...) { threw = true; }
    require(threw && cache.stats().published == 0, "failed write cached as present");
    fail = false;
    require(cache.store(hash, payload.data(), payload.size()), "retry after failure did not publish");
    const auto badHash = chunking::sha256Hex("other");
    std::ofstream(root + "/" + badHash) << 'x';
    threw = false;
    try { cache.store(badHash, payload.data(), payload.size()); } catch (...) { threw = true; }
    require(threw, "wrong-size stored object accepted");
}

void deathReleasesLock(const std::string& root) {
    Pipe entered;
    bool block = true;
    const std::string payload = "survives worker failure";
    const auto hash = chunking::sha256Hex(payload);
    SharedExistenceCache cache(root, {}, [&](const auto& key, const char* bytes, auto size) {
        if (block) { entered.send(); for (;;) ::pause(); }
        chunking::detail::publishChunkBlob(root, key, bytes, size);
    });
    Children children;
    children.spawn([&] { cache.store(hash, payload.data(), payload.size()); });
    entered.receive();
    require(::kill(children.pids.front(), SIGKILL) == 0, "kill failed");
    require(::waitpid(children.pids.front(), nullptr, 0) > 0, "reap failed");
    children.pids.front() = -1;
    block = false;
    // Retry in a child with an alarm so deadlocks fail the test instead of hanging.
    Children retry;
    retry.spawn([&] {
        require(cache.store(hash, payload.data(), payload.size()), "publication after death failed");
    });
    retry.join();
    require(cache.stats().hits == 0 && cache.stats().diskChecks == 2 && cache.stats().published == 1,
            "dead publisher installed a false positive");
}

void independentPublishers(const std::string& root) {
    Pipe entered, gate;
    std::string a = "independent-a", b = "independent-b";
    const auto ha = chunking::sha256Hex(a);
    auto hb = chunking::sha256Hex(b);
    auto stripe = [](const auto& h) { return (chunking::detail::cacheDigestHash(h) >> 10) % 4096; };
    while (stripe(ha) == stripe(hb)) { b += 'b'; hb = chunking::sha256Hex(b); }
    SharedExistenceCache cache(root, {}, [&](const auto& hash, const char* bytes, auto size) {
        entered.send(); gate.receive();
        chunking::detail::publishChunkBlob(root, hash, bytes, size);
    });
    Children children;
    children.spawn([&] { cache.store(ha, a.data(), a.size()); });
    children.spawn([&] { cache.store(hb, b.data(), b.size()); });
    // Both must reach publication before either is allowed to finish.
    entered.receive(); entered.receive(); gate.send(); gate.send();
    children.join();
    require(cache.stats().published == 2, "different hashes serialized on metadata lock");
}

void eviction(const std::string& root) {
    SharedExistenceCache cache(root);
    std::vector<std::string> payloads, hashes;
    for (std::size_t n = 0; hashes.size() < 10; ++n) {
        const auto bytes = std::to_string(n);
        const auto hash = chunking::sha256Hex(bytes);
        if (chunking::detail::cacheDigestHash(hash) % 1024 != 0) continue;
        payloads.push_back(bytes); hashes.push_back(hash);
        require(cache.store(hash, bytes.data(), bytes.size()), "initial eviction fixture failed");
    }
    require(!cache.store(hashes[0], payloads[0].data(), payloads[0].size()), "eviction caused rewrite");
    require(cache.stats().published == 10 && cache.stats().diskChecks == 11,
            "evicted cache entry did not fall back to disk");
}
}

int main() {
    try {
        Scratch scratch;
        sharedDedup(scratch.path + "/shared");
        failureRecovery(scratch.path + "/retry");
        deathReleasesLock(scratch.path + "/death");
        independentPublishers(scratch.path + "/concurrent");
        eviction(scratch.path + "/eviction");
        std::cout << "Shared cache tests passed (dedup, concurrency, eviction, failure/death recovery, HLL).\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "Shared cache test failed: " << error.what() << '\n';
        return 1;
    }
}
