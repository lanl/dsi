#!/usr/bin/env python3
"""Storage-half cross-check: compares the *real* repolog/chunking.py's
SQL-backed snapshot metadata with the C++ manifest-backed implementation.

Compared:
  * per-file hash (sha256 over the ordered chunk digests)
  * chunk count
  * C++ manifest reconstruction byte equality

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
        [os.path.join(HERE, "svman-core"), *args],
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
    file_hashes, _, chunk_length = mod.store_chunks_for_snapshot(
        conn, py_root, entries)

    # ---- real C++ store (file-only) --------------------------------------
    manifest = run_cpp("--store-manifest", cc_root, path, rel).splitlines()
    cc_root_hash, cc_sha, cc_count = manifest[0], manifest[1], int(manifest[2])

    ok = True

    def report(label, cond):
        nonlocal ok
        ok = ok and cond
        print("[%s] %s" % ("PASS" if cond else "FAIL", label))

    report("per-file hash matches",
           cc_sha == file_hashes[rel])
    report("chunk count matches", cc_count == chunk_length[rel])

    # C++ reconstruction is driven exclusively by its persisted manifest.
    code = subprocess.run(
        [os.path.join(HERE, "svman-core"), "--rebuild-manifest", cc_root,
         rel, cc_root_hash, cc_sha, "/tmp/cc_rebuild_from_manifest"],
        capture_output=True, text=True)
    repo = os.path.join("/tmp/cc_rebuild_from_manifest", rel)
    samec = os.path.exists(repo) and open(repo, "rb").read() == open(path, "rb").read()
    report("cpp rebuild from manifest -> identical bytes",
           code.returncode == 0 and samec)

    print("ALL STORAGE CROSS-CHECKS PASSED" if ok else "STORAGE MISMATCH")
    return 0 if ok else 1


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(2)
    sys.exit(main(sys.argv[1], sys.argv[2]))
