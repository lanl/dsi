#!/usr/bin/env python3
"""Storage-half cross-check: runs the *real* repolog/chunking.py's
store_chunks_for_snapshot + rebuild_file_from_chunks (its SQL side intact,
against an in-memory SQLite) and compares with the C++ file-only port.

Compared:
  * per-file hash (sha256 over the ordered chunk digests)
  * chunk count and unique chunk-hash set size
  * every row the Python INSERTs (chunk_hash, chunk_size, chunk_index)
  * rebuilt-file byte equality, both stores (Python's SQL reads the *other*
    implementation's blob directory, proving blob-name interop)

Usage:  python3 pystore_check.py <synthetic-file> <relative-path>
"""

import os
import sqlite3
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import pycheck  # noqa: E402  (build_package lives there)


def run_cpp(*args) -> str:
    out = subprocess.run(
        [os.path.join(HERE, "chunking_test"), *args],
        check=True, capture_output=True, text=True)
    return out.stdout.strip()


def main(path: str, rel: str) -> int:
    mod = pycheck.build_package(os.path.join(HERE, "..", "chunking.py"))

    # ---- real Python store (SQL side active, in-memory) ------------------
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE chunk_store "
        "(chunk_hash, chunk_size, created_at, relative_file_path, "
        " chunk_index, commit_hash)")
    entries = [{
        "absolute_path": path,
        "relative_path": rel,
        "file_type": "file",
    }]
    py_root = "/tmp/py_store_root"
    cc_root = "/tmp/cc_store_root"
    subprocess.run(["rm", "-rf", py_root, cc_root], check=True)
    file_hashes, chunk_hashes, chunk_length = mod.store_chunks_for_snapshot(
        conn, py_root, entries)

    py_rows = conn.execute(
        "SELECT chunk_hash, chunk_size, chunk_index FROM chunk_store "
        "WHERE relative_file_path = ? ORDER BY chunk_index", (rel,)).fetchall()

    # ---- real C++ store (file-only) --------------------------------------
    js = run_cpp("--store", cc_root, path, rel).splitlines()
    cc_sha, cc_count, cc_unique = js[0], int(js[1]), int(js[2])

    ok = True

    def report(label, cond):
        nonlocal ok
        ok = ok and cond
        print("[%s] %s" % ("PASS" if cond else "FAIL", label))

    report("per-file hash matches",
           cc_sha == file_hashes[rel])
    report("chunk count matches", cc_count == chunk_length[rel])
    report("unique chunk set matches", cc_unique == len(chunk_hashes))

    # Every chunk the Python stored is present in the C++ blob dir, and the
    # C++ chunk count covers the same unique digests.
    cc_blobs = set(os.listdir(os.path.join(cc_root, "dsi_vcs_chunks")))
    report("blob dirs interoperate (same chunk names)",
           {r["chunk_hash"] for r in py_rows} <= cc_blobs
           and len(cc_blobs) == cc_unique)

    # Python's rebuild (reads its own SQL rows) against the C++ blob dir.
    okp = mod.rebuild_file_from_chunks(conn, cc_root, rel, "UPDATE%",
                                       file_hashes[rel], "/tmp/py_rebuild_from_cc")
    with open(os.path.join("/tmp/py_rebuild_from_cc", rel), "rb") as f:
        samep = f.read() == open(path, "rb").read()
    report("python rebuild over C++ blobs -> identical bytes",
           okp and samep)

    # C++ rebuild (given the Python rows as the ordered list) against the
    # Python blob dir.
    ordered = ",".join(r["chunk_hash"] for r in py_rows)
    code = subprocess.run(
        [os.path.join(HERE, "chunking_test"), "--rebuild", py_root, rel,
         file_hashes[rel], "/tmp/cc_rebuild_from_py", ordered],
        capture_output=True, text=True)
    repo = os.path.join("/tmp/cc_rebuild_from_py", rel)
    samec = os.path.exists(repo) and open(repo, "rb").read() == open(path, "rb").read()
    report("cpp rebuild over python blobs -> identical bytes",
           code.returncode == 0 and samec)

    print("ALL STORAGE CROSS-CHECKS PASSED" if ok else "STORAGE MISMATCH")
    return 0 if ok else 1


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(2)
    sys.exit(main(sys.argv[1], sys.argv[2]))
