import os
import sqlite3

from contextlib import closing
#from pandasql import sqldf

def is_valid_sqlite_with_data(path: str) -> tuple[bool, str]:
    """
    Checks if the file at `path` is a valid SQLite3 database file and contains at least one user table with data.

    Arg:
        path: The file path to check.

    Returns:
        A tuple (is_valid, message) where:
        - is_valid: True if the file is a valid SQLite3 database with at least one user table containing data, False otherwise.
        - message: A string describing the reason if not valid, or "valid SQLite file with data" if valid.
    """
    if not os.path.isfile(path):
        return False, "file does not exist"

    try:
        with open(path, "rb") as f:
            header = f.read(16)
        if header != b"SQLite format 3\x00":
            return False, "not a SQLite3 file"
    except OSError as e:
        return False, f"could not read file: {e}"

    try:
        with closing(sqlite3.connect(path)) as conn:
            cur = conn.cursor()

            # Check integrity
            row = cur.execute("PRAGMA integrity_check;").fetchone()
            if not row or row[0].lower() != "ok":
                return False, "SQLite integrity check failed"

            # Find user tables
            tables = cur.execute("""
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                  AND name NOT LIKE 'sqlite_%'
                LIMIT 1
            """).fetchall()

            if not tables:
                return False, "valid SQLite file, but no user tables"

            # Check whether any user table has data
            for (table_name,) in cur.execute("""
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                  AND name NOT LIKE 'sqlite_%'
            """):
                qname = '"' + table_name.replace('"', '""') + '"'
                count = cur.execute(f"SELECT 1 FROM {qname} LIMIT 1").fetchone()
                if count is not None:
                    return True, "valid SQLite file with data"

            return False, "valid SQLite file, but tables are empty"

    except sqlite3.DatabaseError as e:
        return False, f"SQLite open/query failed: {e}"
    except Exception as e:
        return False, f"unexpected error: {e}"
    


def is_valid_duckdb_with_data(path: str) -> tuple[bool, str]:
    """
    Checks if the file at `path` is a valid DuckDB database file and contains at least one user table with data.

    Arg:
        path: The file path to check.

    Returns:
        A tuple (is_valid, message) where:
        - is_valid: True if the file is a valid DuckDB database with at least one user table containing data, False otherwise.
        - message: A string describing the reason if not valid, or "valid DuckDB file with data" if valid.
    """
    if not os.path.isfile(path):
        return False, "file does not exist"

    try:
        import duckdb
    except ImportError:
        return False, "duckdb package is not installed"

    # DuckDB does not have a simple fixed header check as convenient as SQLite.
    # The reliable test is: can DuckDB open it and query its catalog?
    try:
        conn = duckdb.connect(path, read_only=True)
        try:
            # Check that catalog is readable by listing tables
            tables = conn.execute("""
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                  AND table_type = 'BASE TABLE'
            """).fetchall()

            if not tables:
                return False, "valid DuckDB file, but no user tables"

            # Check whether any table has at least one row
            for schema_name, table_name in tables:
                qschema = '"' + schema_name.replace('"', '""') + '"'
                qtable = '"' + table_name.replace('"', '""') + '"'
                row = conn.execute(
                    f"SELECT 1 FROM {qschema}.{qtable} LIMIT 1"
                ).fetchone()
                if row is not None:
                    return True, "valid DuckDB file with data"

            return False, "valid DuckDB file, but tables are empty"
        finally:
            conn.close()

    except Exception as e:
        return False, f"DuckDB open/query failed: {e}"
    


def detect_valid_db_with_data(path: str) -> tuple[str | None, bool, str]:
    """
    Detects whether the file at `path` is a valid SQLite or DuckDB database file containing at least one user table with data.

    Args:
        path: The file path to check.
        
    Returns:
        A tuple (db_type, is_valid, message) where:
    """
    ok, msg = is_valid_sqlite_with_data(path)
    if ok:
        return "sqlite", True

    ok2, msg2 = is_valid_duckdb_with_data(path)
    if ok2:
        return "duckdb", True

    return None, False

