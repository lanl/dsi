// sha256.cpp -- implementation of the SHA-256 backend declared in sha256.hpp.
//
// Two interchangeable backends are compiled from one source; see the header
// for the selection rules. Both backends retain incremental digest state.

#include "sha256.hpp"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <stdexcept>

namespace chunking {

// ---------------------------------------------------------------------------
// Hex helpers
// ---------------------------------------------------------------------------
namespace {
char kHex[] = "0123456789abcdef";

std::string toHexBytes(const std::uint8_t* p, std::size_t n) {
    std::string out;
    out.resize(n * 2);
    for (std::size_t i = 0; i < n; ++i) {
        out[2 * i + 0] = kHex[p[i] >> 4];
        out[2 * i + 1] = kHex[p[i] & 0xF];
    }
    return out;
}
}  // namespace

std::string toHexDigest(const std::array<std::uint8_t, 32>& d) {
    return toHexBytes(d.data(), d.size());
}

// ---------------------------------------------------------------------------
// One-shot helpers
// ---------------------------------------------------------------------------
std::string sha256Hex(const void* data, std::size_t len) {
    std::uint8_t out[32];
#if defined(USE_OPENSSL)
    if (EVP_Digest(data, len, out, nullptr, EVP_sha256(), nullptr) != 1) {
        throw std::runtime_error("EVP_Digest(sha256) failed");
    }
#else   // __APPLE__ -> CommonCrypto
    CC_SHA256(data, static_cast<CC_LONG>(len), out);
#endif
    return toHexDigest(*reinterpret_cast<const std::array<std::uint8_t, 32>*>(out));
}

std::string sha256Hex(const std::string& s) { return sha256Hex(s.data(), s.size()); }

// ===========================================================================
// Streaming context
// ===========================================================================
#if defined(USE_OPENSSL)

Sha256Ctx::Sha256Ctx() {
    ctx_ = EVP_MD_CTX_new();
    if (ctx_ == nullptr) throw std::runtime_error("EVP_MD_CTX_new failed");
    if (EVP_DigestInit_ex(ctx_, EVP_sha256(), nullptr) != 1)
        throw std::runtime_error("EVP_DigestInit_ex failed");
}

Sha256Ctx::~Sha256Ctx() {
    if (ctx_ != nullptr) EVP_MD_CTX_free(ctx_);
}

void Sha256Ctx::update(const void* data, std::size_t len) {
    if (EVP_DigestUpdate(ctx_, data, len) != 1)
        throw std::runtime_error("EVP_DigestUpdate failed");
}

void Sha256Ctx::reset() {
    EVP_MD_CTX* c = EVP_MD_CTX_new();
    if (c == nullptr) throw std::runtime_error("EVP_MD_CTX_new failed");
    EVP_MD_CTX_free(ctx_);
    ctx_ = c;
    if (EVP_DigestInit_ex(ctx_, EVP_sha256(), nullptr) != 1)
        throw std::runtime_error("EVP_DigestInit_ex failed");
}

std::array<std::uint8_t, 32> Sha256Ctx::rawdigest() const {
    // Non-destructive: snapshot on a copy so the live context can keep going.
    EVP_MD_CTX* c = EVP_MD_CTX_new();
    if (c == nullptr) throw std::runtime_error("EVP_MD_CTX_new failed");
    std::uint8_t out[32];
    std::size_t  n = 0;
    bool ok = (EVP_MD_CTX_copy_ex(c, ctx_) == 1) &&
              (EVP_DigestFinal_ex(c, out, &n) == 1);
    EVP_MD_CTX_free(c);
    if (!ok) throw std::runtime_error("EVP digest final failed");
    return *reinterpret_cast<const std::array<std::uint8_t, 32>*>(out);
}

std::string Sha256Ctx::hexdigest() const { return toHexDigest(rawdigest()); }

#else  // __APPLE__ -> incremental CommonCrypto

Sha256Ctx::Sha256Ctx() { reset(); }
Sha256Ctx::~Sha256Ctx() {}

void Sha256Ctx::update(const void* data, std::size_t len) {
    auto bytes = static_cast<const unsigned char*>(data);
    while (len > 0) {
        const auto take = static_cast<CC_LONG>(std::min<std::size_t>(len, UINT32_MAX));
        CC_SHA256_Update(&ctx_, bytes, take);
        bytes += take;
        len -= take;
    }
}

void Sha256Ctx::reset() { CC_SHA256_Init(&ctx_); }

std::array<std::uint8_t, 32> Sha256Ctx::rawdigest() const {
    std::array<std::uint8_t, 32> out;
    auto copy = ctx_;
    CC_SHA256_Final(out.data(), &copy);
    return out;
}

std::string Sha256Ctx::hexdigest() const { return toHexDigest(rawdigest()); }

#endif

}  // namespace chunking
