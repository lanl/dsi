#pragma once

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <queue>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace chunking::detail {

inline void makeResultPipeNonblocking(int fd) {
    const int flags = ::fcntl(fd, F_GETFL);
    if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
        throw std::runtime_error("cannot make worker result pipe nonblocking");
}

inline void waitForWorkerResults(std::vector<pollfd>& channels) {
    int result;
    do { result = ::poll(channels.data(), channels.size(), -1); }
    while (result < 0 && errno == EINTR);
    if (result < 0) throw std::runtime_error("poll worker results failed");
    for (const auto& channel : channels) {
        if (channel.revents & POLLNVAL) throw std::runtime_error("invalid worker result descriptor");
    }
}

// Assemble a header and optional body incrementally. A worker that sends only
// part of either never blocks collection from the others. Limit each turn to
// 64 KiB so a continuously readable worker cannot monopolize the parent.
template<class Header>
class WorkerReplyReader {
    Header header_{};
    std::size_t headerBytes_ = 0, bodyBytes_ = 0;
    std::string body_;
    bool readingBody_ = false;

public:
    template<class BodySize, class Consume>
    bool drain(int fd, bool socket, BodySize bodySize, Consume consume) {
        std::size_t budget = 64 * 1024;
        while (budget) {
            char* destination;
            std::size_t want;
            if (readingBody_) {
                destination = body_.data() + bodyBytes_;
                want = body_.size() - bodyBytes_;
            } else {
                destination = reinterpret_cast<char*>(&header_) + headerBytes_;
                want = sizeof(Header) - headerBytes_;
            }
            const auto amount = std::min(want, budget);
            const ssize_t got = socket ? ::recv(fd, destination, amount, MSG_DONTWAIT)
                                       : ::read(fd, destination, amount);
            if (got < 0 && errno == EINTR) continue;
            if (got < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return false;
            if (got < 0) throw std::runtime_error("reading worker result failed");
            if (got == 0) {
                if (headerBytes_ || readingBody_)
                    throw std::runtime_error("truncated worker result");
                return true;
            }
            budget -= static_cast<std::size_t>(got);
            if (readingBody_) {
                bodyBytes_ += static_cast<std::size_t>(got);
            } else {
                headerBytes_ += static_cast<std::size_t>(got);
                if (headerBytes_ == sizeof(Header)) {
                    body_.resize(bodySize(header_));
                    bodyBytes_ = 0;
                    readingBody_ = true;
                }
            }
            if (readingBody_ && bodyBytes_ == body_.size()) {
                consume(header_, std::move(body_));
                headerBytes_ = bodyBytes_ = 0;
                readingBody_ = false;
                body_.clear();
            }
        }
        return false;
    }
};

// Each worker's list is already ordered, but round-robin unit assignment
// interleaves the lists. Merge by file offset after collection has completed.
template<class Record>
std::vector<Record> mergeWorkerLists(std::vector<std::vector<Record>>& lists) {
    struct Cursor { std::size_t worker, index; };
    auto later = [&](const Cursor& a, const Cursor& b) {
        return lists[a.worker][a.index].offset > lists[b.worker][b.index].offset;
    };
    std::priority_queue<Cursor, std::vector<Cursor>, decltype(later)> next(later);
    std::size_t total = 0;
    for (std::size_t w = 0; w < lists.size(); ++w) {
        total += lists[w].size();
        if (!lists[w].empty()) next.push({w, 0});
    }
    std::vector<Record> ordered;
    ordered.reserve(total);
    while (!next.empty()) {
        auto cursor = next.top();
        next.pop();
        ordered.push_back(std::move(lists[cursor.worker][cursor.index]));
        if (++cursor.index < lists[cursor.worker].size()) next.push(cursor);
    }
    return ordered;
}

}  // namespace chunking::detail
