#include "worker_results.hpp"

#include <array>
#include <cstdint>
#include <cstdio>
#include <stdexcept>
#include <string>
#include <vector>

namespace {
using chunking::detail::WorkerReplyReader;
struct Header { std::uint64_t offset, bytes; };
struct Record { std::uint64_t offset; std::string data; };
struct Channel {
    int fd[2];
    explicit Channel(bool socket) {
        const int result = socket ? ::socketpair(AF_UNIX, SOCK_STREAM, 0, fd) : ::pipe(fd);
        if (result) throw std::runtime_error("test channel creation failed");
        if (!socket) chunking::detail::makeResultPipeNonblocking(fd[0]);
    }
    ~Channel() { for (int descriptor : fd) if (descriptor >= 0) ::close(descriptor); }
    void closeWriter() { ::close(fd[1]); fd[1] = -1; }
    void send(const void* bytes, std::size_t size) {
        if (::write(fd[1], bytes, size) != static_cast<ssize_t>(size))
            throw std::runtime_error("test channel write failed");
    }
};
void require(bool value, const char* message) {
    if (!value) throw std::runtime_error(message);
}
std::size_t bodySize(const Header& header) {
    if (header.bytes > 1024) throw std::runtime_error("invalid test reply length");
    return static_cast<std::size_t>(header.bytes);
}

void readyOrder(bool socket, bool partialBody) {
    Channel slow(socket), fast(socket);
    std::array<WorkerReplyReader<Header>, 2> readers;
    std::vector<std::vector<Record>> lists(2);
    std::vector<unsigned> arrived;
    const Header first{0, 4}, second{4, 4};
    if (partialBody) {
        slow.send(&first, sizeof first);
        slow.send("ab", 2);
    } else {
        slow.send(&first, 3); // A partial header must not hold up worker 1.
    }
    fast.send(&second, sizeof second);
    fast.send("efgh", 4);
    fast.closeWriter(); // POLLHUP can accompany unread, valid output.
    std::vector<pollfd> ready{{slow.fd[0], POLLIN, 0}, {fast.fd[0], POLLIN, 0}};
    auto collect = [&] {
        chunking::detail::waitForWorkerResults(ready);
        for (unsigned w = 0; w < ready.size(); ++w) {
            if (!ready[w].revents) continue;
            const bool eof = readers[w].drain(ready[w].fd, socket, bodySize,
                [&](const Header& header, std::string&& data) {
                    lists[w].push_back({header.offset, std::move(data)});
                    arrived.push_back(w);
                });
            if (eof) ready[w].fd = -1;
        }
    };
    collect();
    require(arrived == std::vector<unsigned>{1} && lists[0].empty(),
            "partial worker reply blocked a ready worker");
    if (partialBody) slow.send("cd", 2);
    else {
        slow.send(reinterpret_cast<const char*>(&first) + 3, sizeof first - 3);
        slow.send("abcd", 4);
    }
    slow.closeWriter();
    while (ready[0].fd >= 0 || ready[1].fd >= 0) collect();
    require(arrived == std::vector<unsigned>({1, 0}), "did not collect in readiness order");
    auto merged = chunking::detail::mergeWorkerLists(lists);
    require(merged.size() == 2 && merged[0].data == "abcd" && merged[1].data == "efgh",
            "arrival order changed final file order");
}

void truncatedReplies() {
    for (const bool body : {false, true}) {
        Channel channel(false);
        Header header{0, 4};
        channel.send(&header, body ? sizeof header : 3);
        if (body) channel.send("x", 1);
        channel.closeWriter();
        WorkerReplyReader<Header> reader;
        bool failed = false;
        try { reader.drain(channel.fd[0], false, bodySize, [](const Header&, std::string&&) {}); }
        catch (const std::runtime_error&) { failed = true; }
        require(failed, "truncated worker reply was accepted");
    }
}

void interleavedLists() {
    std::vector<std::vector<Record>> lists(4);
    lists[0] = {{0, "a"}, {8, "e"}};
    lists[1] = {{2, "b"}, {10, "f"}};
    lists[3] = {{4, "c"}, {6, "d"}, {12, "g"}};
    const auto merged = chunking::detail::mergeWorkerLists(lists);
    std::string text;
    for (const auto& item : merged) text += item.data;
    require(text == "abcdefg", "interleaved worker lists were concatenated in worker order");
}

void coalescedAndEmptyBodies() {
    Channel channel(false);
    const Header empty{0, 0}, full{1, 1};
    channel.send(&empty, sizeof empty);
    channel.send(&full, sizeof full);
    channel.send("x", 1);
    channel.closeWriter();
    WorkerReplyReader<Header> reader;
    std::vector<std::string> bodies;
    const bool eof = reader.drain(channel.fd[0], false, bodySize,
        [&](const Header&, std::string&& data) { bodies.push_back(std::move(data)); });
    require(eof && bodies == std::vector<std::string>({"", "x"}),
            "coalesced or metadata-only replies were lost at EOF");
}
} // namespace

int main() {
    ::alarm(10); // Bound a regression that accidentally performs blocking I/O.
    try {
        for (const bool socket : {false, true})
            for (const bool body : {false, true}) readyOrder(socket, body);
        truncatedReplies();
        interleavedLists();
        coalescedAndEmptyBodies();
        std::puts("Worker result tests passed (readiness order, partial frames, EOF, ordered merge).");
        return 0;
    } catch (const std::exception& error) {
        std::fprintf(stderr, "worker results test: %s\n", error.what());
        return 1;
    }
}
