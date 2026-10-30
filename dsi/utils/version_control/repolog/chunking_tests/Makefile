# Makefile for chunking_tests -- C++ port of repolog/chunking.py.
#
# Default (Apple) links the OS's CommonCrypto for SHA-256 -- no extra deps.
# Use `make OPENSSL=1` to build against OpenSSL instead (pkg-config openssl).
#
# Targets:
#   all    build ./chunking_test
#   test   runs per-file rules; see below
#   run    alias for test
#   clean  remove objects and the binary

CXX      ?= clang++
CXXFLAGS ?= -std=c++17 -O2 -Wall -Wextra
LIBS      =

OBJS := sha256.o chunking.o main.o
BIN  := chunking_test

ifeq ($(OPENSSL),1)
  CXXFLAGS += -DUSE_OPENSSL $(shell pkg-config --cflags openssl)
  LIBS     += $(shell pkg-config --libs openssl)
endif

all: $(BIN)

$(BIN): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIBS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

sha256.o: sha256.cpp sha256.hpp
chunking.o: chunking.cpp chunking.hpp sha256.hpp
main.o: main.cpp chunking.hpp sha256.hpp

test: $(BIN)
	./$(BIN)

run: test

clean:
	rm -f $(OBJS) $(BIN)

.PHONY: all test run clean
