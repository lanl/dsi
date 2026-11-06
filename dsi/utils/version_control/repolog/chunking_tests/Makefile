# Makefile for chunking_tests -- C++ port of repolog/chunking.py.
#
# Default (Apple) links the OS's CommonCrypto for SHA-256 -- no extra deps.
# Use `make OPENSSL=1` to build against OpenSSL instead (pkg-config openssl).
#
# Targets:
#   all    build chunking, manifest, shared-cache and worker-result test binaries
#   test   runs per-file rules; see below
#   manifest-test   run only manifest tests
#   benchmark       build ./manifest_benchmark (run explicitly)
#   run    alias for test
#   clean  remove objects and the binary

CXX      ?= clang++
CXXFLAGS ?= -std=c++17 -O2 -Wall -Wextra
LIBS      =

OBJS := sha256.o manifest.o shared_existence_cache.o chunking.o main.o
BIN  := chunking_test
MANIFEST_BIN := manifest_test
CACHE_BIN := shared_cache_test
RESULTS_BIN := worker_results_test
BENCHMARK_BIN := manifest_benchmark

ifeq ($(OPENSSL),1)
  CXXFLAGS += -DUSE_OPENSSL $(shell pkg-config --cflags openssl)
  LIBS     += $(shell pkg-config --libs openssl)
endif

all: $(BIN) $(MANIFEST_BIN) $(CACHE_BIN) $(RESULTS_BIN)

$(BIN): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIBS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

sha256.o: sha256.cpp sha256.hpp
manifest.o: manifest.cpp manifest.hpp sha256.hpp
shared_existence_cache.o: shared_existence_cache.cpp shared_existence_cache.hpp
shared_cache_test.o: shared_cache_test.cpp shared_existence_cache.hpp sha256.hpp
chunking.o: chunking.cpp chunking.hpp sha256.hpp manifest.hpp shared_existence_cache.hpp worker_results.hpp
worker_results_test.o: worker_results_test.cpp worker_results.hpp
main.o: main.cpp chunking.hpp sha256.hpp manifest.hpp
manifest_test.o: manifest_test.cpp manifest.hpp chunking.hpp sha256.hpp

$(MANIFEST_BIN): sha256.o manifest.o shared_existence_cache.o chunking.o manifest_test.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIBS)

$(CACHE_BIN): sha256.o shared_existence_cache.o shared_cache_test.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIBS)

$(RESULTS_BIN): worker_results_test.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIBS)

manifest_benchmark.o: manifest_benchmark.cpp manifest.hpp chunking.hpp sha256.hpp

$(BENCHMARK_BIN): sha256.o manifest.o shared_existence_cache.o chunking.o manifest_benchmark.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIBS)

benchmark: $(BENCHMARK_BIN)

test: $(BIN) $(MANIFEST_BIN) $(CACHE_BIN) $(RESULTS_BIN)
	./$(BIN)
	./$(MANIFEST_BIN)
	./$(CACHE_BIN)
	./$(RESULTS_BIN)

manifest-test: $(MANIFEST_BIN)
	./$(MANIFEST_BIN)

run: test

clean:
	rm -f $(OBJS) $(BIN) manifest_test.o $(MANIFEST_BIN) manifest_benchmark.o $(BENCHMARK_BIN) shared_cache_test.o $(CACHE_BIN) worker_results_test.o $(RESULTS_BIN)

.PHONY: all test manifest-test benchmark run clean
