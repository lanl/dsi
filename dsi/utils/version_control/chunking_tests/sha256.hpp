// sha256.hpp -- SHA-256 backend for the FastCDC chunker.
//
// The original chunking.py uses Python's hashlib.sha256. Rather than
// re-implementing the SHA-256 algorithm by hand here, we back it with a
// real, trusted library and pick the best one for the target platform
// (selected at compile time; the call sites are identical):
//
//   * Apple  __APPLE__ (default when building on Apple):
//       CommonCrypto  CC_SHA256  -- the OS-supplied, hardware-capable
//       implementation on Apple Silicon; it links with no extra flags.
//
//   * -DUSE_OPENSSL (portable, also the x86 fallback):
//       OpenSSL  EVP_sha256      -- the de-facto standard crypto library.
//       Build:  make OPENSSL=1   (needs `pkg-config openssl`).
//
//   * -DUSE_INTEL_IPP  (documented, x86 only):
//       Intel IPP SHA-256        -- hardware (SHA-NI) accelerated. Left as
//       a seam; not available on Apple Silicon.
//
// NOTE: the SHA-256 primitive is the "library that we are welcome to use".
// The multicore file path uses OS processes in chunking.cpp to scan units
// and hash their chunks concurrently. On Apple Silicon
// CC_SHA256 already routes to the SoC's crypto engine, so the two combine
// for a genuine multicore + hardware-accelerated hash path.
//
// The streaming context (Sha256Ctx) mirrors Python's hashlib object:
// update() many times, then hexdigest() -- and hexdigest() is
// non-destructive, matching hashlib's behaviour of leaving the object
// usable for further updates.

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>

#if defined(USE_OPENSSL)
  #include <openssl/evp.h>   // EVP_*
  #if OPENSSL_VERSION_NUMBER < 0x10100000L
    #include <openssl/sha.h> // SHA256 one-shot fallback
  #endif
#endif

#if !defined(USE_OPENSSL) && defined(__APPLE__)
  #include <CommonCrypto/CommonDigest.h>   // CC_SHA256
  #define CHASH_BACKEND "CommonCrypto(CC_SHA256)"
#elif defined(USE_OPENSSL)
  #define CHASH_BACKEND "OpenSSL(EVP_sha256)"
#else
  #error "Select a SHA-256 backend: build on Apple (CommonCrypto) " \
         "or with -DUSE_OPENSSL (or provide -DUSE_INTEL_IPP)."
#endif

namespace chunking {

// ---------------------------------------------------------------------------
// Free one-shot helpers (hash a single buffer, return the lowercase hex).
// ---------------------------------------------------------------------------
std::string sha256Hex(const void* data, std::size_t len);
std::string sha256Hex(const std::string& s);

// Canonical lowercase hex of 32 raw digest bytes.
std::string toHexDigest(const std::array<std::uint8_t, 32>& d);

// ---------------------------------------------------------------------------
// Streaming context -- the C++ analogue of hashlib.sha256().
// ---------------------------------------------------------------------------
class Sha256Ctx {
public:
    Sha256Ctx();
    ~Sha256Ctx();
    Sha256Ctx(const Sha256Ctx&) = delete;
    Sha256Ctx& operator=(const Sha256Ctx&) = delete;

    void   update(const void* data, std::size_t len);
    void   update(const std::string& s) { update(s.data(), s.size()); }

    // Non-destructive (like hashlib.sha256().hexdigest()).
    std::string            hexdigest() const;
    std::array<std::uint8_t, 32> rawdigest() const;

    void   reset();

    // Which backend is compiled in (for diagnostics / the self-test banner).
    static const char* backendName() { return CHASH_BACKEND; }

private:
#if defined(USE_OPENSSL)
    EVP_MD_CTX* ctx_ = nullptr;   // OpenSSL streaming context
#else
    // Incremental state keeps manifest/file hashing bounded for huge files.
    CC_SHA256_CTX ctx_{};
#endif
};

}  // namespace chunking
