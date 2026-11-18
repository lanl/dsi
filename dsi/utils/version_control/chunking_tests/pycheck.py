#!/usr/bin/env python3
"""Cross-check harness: runs the *real* repolog/chunking.py against the C++
port in this directory.

chunking.py uses package-relative imports (``from .log_record import ...``),
so we execute it from a scratch package directory whose sibling modules
(``log_record`` and the whole ``content_chunking`` package) are lightweight
stubs providing exactly the names chunking.py imports at module level. The
chunking logic itself is the untouched original file -- it is only *copied*
into the scratch package, never modified.

Usage:

    python3 pycheck.py gen <size> <path>      # deterministic synthetic file
    python3 pycheck.py fastcdc <path>         # per chunk: "<size> <sha256>"
    python3 pycheck.py rolling <path>         # same, via chunk_file_rolling_hash
    python3 pycheck.py meta                   # AVERAGE_BITS, masks, gear tables

CLI equivalence with the C++ binary:

    ./chunking_test --gen   <size> <path>      -> pycheck.py gen <size> <path>
    ./chunking_test --dump  fastcdc <path>     -> pycheck.py fastcdc <path>
    ./chunking_test --dump  rolling <path>     -> pycheck.py rolling <path>
    ./chunking_test --meta                     -> pycheck.py meta

The synthetic file recipe is the same on both sides:
  block j = sha256(j as 8-byte little-endian u64); file = concat blocks,
  truncated to <size>.
"""

import importlib
import hashlib
import os
import shutil
import struct
import sys
import tempfile


def build_package(chunking_src: str):
    root = tempfile.mkdtemp(prefix="chunking_cpp_pycheck_")
    pkg = os.path.join(root, "repolog")
    cc = os.path.join(pkg, "content_chunking")
    os.makedirs(cc)
    with open(os.path.join(pkg, "__init__.py"), "w") as f:
        f.write("")
    with open(os.path.join(cc, "__init__.py"), "w") as f:
        f.write("")
    with open(os.path.join(pkg, "log_record.py"), "w") as f:
        f.write("def _utcnow():\n    return None\n")

    def stub(name: str, body: str):
        with open(os.path.join(cc, name), "w") as f:
            f.write(body)

    exc = "Err = type('X', (Exception,), {})\n"
    stub("npy_chunking.py",
         "NPY_MAGIC = None\n"
         "NpyFormatError = type('NpyFormatError', (Exception,), {})\n"
         "def chunk_npy_file(*a, **k): raise NotImplementedError\n"
         "def iter_npy_file(*a, **k): raise NotImplementedError\n")
    stub("db_chunking.py",
         "SQLITE_MAGIC = None\n"
         "SQLiteFormatError = type('SQLiteFormatError', (Exception,), {})\n"
         "def chunk_sqlite_file(*a, **k): raise NotImplementedError\n"
         "def iter_sqlite_file(*a, **k): raise NotImplementedError\n")
    stub("csv_chunking.py",
         "CSVFormatError = type('CSVFormatError', (Exception,), {})\n"
         "def chunk_csv_file(*a, **k): raise NotImplementedError\n"
         "def iter_csv_file(*a, **k): raise NotImplementedError\n")
    stub("xlsx_chunking.py",
         "XLSXFormatError = type('XLSXFormatError', (Exception,), {})\n"
         "def chunk_xlsx_file(*a, **k): raise NotImplementedError\n"
         "def iter_xlsx_file(*a, **k): raise NotImplementedError\n")
    stub("json_chunking.py",
         "JSONFormatError = type('JSONFormatError', (Exception,), {})\n"
         "def chunk_json_file(*a, **k): raise NotImplementedError\n"
         "def iter_json_file(*a, **k): raise NotImplementedError\n")
    stub("xml_chunking.py",
         "XMLFormatError = type('XMLFormatError', (Exception,), {})\n"
         "def chunk_xml_file(*a, **k): raise NotImplementedError\n"
         "def iter_xml_file(*a, **k): raise NotImplementedError\n")
    del exc

    shutil.copyfile(chunking_src, os.path.join(pkg, "chunking.py"))
    sys.path.insert(0, root)
    return importlib.import_module("repolog.chunking")


def gen(path: str, size: int) -> None:
    out = bytearray()
    j = 0
    while len(out) < size:
        out += hashlib.sha256(struct.pack("<Q", j)).digest()
        j += 1
    with open(path, "wb") as f:
        f.write(bytes(out[:size]))


def main(argv) -> int:
    if len(argv) < 2:
        print(__doc__)
        return 2
    mode = argv[1]
    here = os.path.dirname(os.path.abspath(__file__))

    if mode == "gen":
        if len(argv) != 4:
            print("usage: pycheck.py gen <size> <path>", file=sys.stderr)
            return 2
        gen(argv[3], int(argv[2]))
        return 0

    mod = build_package(os.path.join(here, "..", "chunking.py"))

    if mode == "meta":
        print("AVERAGE_BITS %d" % mod.AVERAGE_BITS)
        print("STRICT_MASK %d" % mod.STRICT_MASK)
        print("EAGER_MASK %d" % mod.EAGER_MASK)
        print("GEAR_TABLE " + " ".join(str(v) for v in mod.GEAR_TABLE))
        print("GEAR_TABLE_LS " + " ".join(str(v) for v in mod.GEAR_TABLE_LS))
        return 0

    if mode in ("fastcdc", "rolling") and len(argv) == 3:
        path = argv[2]
        chunks = (mod.chunk_file_fastcdc(path) if mode == "fastcdc"
                  else mod.chunk_file_rolling_hash(path))
        for c in chunks:
            print("%d %s" % (c["size"], c["sha256"]))
        return 0

    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
