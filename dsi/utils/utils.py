# Standard library imports
import logging
import os
import sqlite3
from contextlib import closing
from datetime import datetime


# Third-party imports
import pandas as pd

# Local application imports
from dsi.dsi import DSI


logger = logging.getLogger(__name__)



def is_valid_sqlite_with_data(path: str) -> tuple[bool, str]:
    """
    Checks if the file at `path` is a valid SQLite3 database file and contains at least one user table with data.

    Args:
        path: The file path to check.

    Returns:
        A tuple (is_valid, message) where:
        - is_valid: True if the file is a valid SQLite3 database with at least one user table containing data, False otherwise.
        - message: A string describing the reason if not valid, or "valid SQLite file with data" if valid.
    
    Raises:
        FileNotFoundError: If the file does not exist.
        OSError: If the file cannot be read.
        sqlite3.DatabaseError: If SQLite operations fail.
        Exception: For any other unexpected errors.
    """
    if not os.path.isfile(path):
        logger.error(f"File does not exist: {path}")
        raise FileNotFoundError(f"File does not exist: {path}")

    try:
        with open(path, "rb") as f:
            header = f.read(16)
        if len(header) < 16 or header[:15] != b"SQLite format 3":
            logger.debug(f"Not a SQLite3 file: {path}")
            return False, "not a SQLite3 file"
    except OSError as e:
        logger.error(f"Could not read file {path}: {e}")
        raise

    try:
        with closing(sqlite3.connect(path, timeout=5.0)) as conn:
            cur = conn.cursor()

            # Check integrity
            row = cur.execute("PRAGMA integrity_check;").fetchone()
            if not row or row[0].lower() != "ok":
                logger.warning(f"SQLite integrity check failed for {path}")
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
                logger.debug(f"Valid SQLite file but no user tables: {path}")
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

            logger.debug(f"Valid SQLite file but tables are empty: {path}")
            return False, "valid SQLite file, but tables are empty"

    except sqlite3.DatabaseError as e:
        logger.error(f"SQLite open/query failed for {path}: {e}")
        raise
    except Exception as e:
        logger.exception(f"Unexpected error while checking SQLite file {path}: {e}")
        raise
    

def is_valid_duckdb_with_data(path: str) -> tuple[bool, str]:
    """
    Checks if the file at `path` is a valid DuckDB database file and contains at least one user table with data.

    Args:
        path: The file path to check.

    Returns:
        A tuple (is_valid, message) where:
        - is_valid: True if the file is a valid DuckDB database with at least one user table containing data, False otherwise.
        - message: A string describing the reason if not valid, or "valid DuckDB file with data" if valid.
    
    Raises:
        FileNotFoundError: If the file does not exist.
        ImportError: If the duckdb package is not installed.
        Exception: If DuckDB operations fail or any other unexpected errors occur.
    """
    if not os.path.isfile(path):
        logger.error(f"File does not exist: {path}")
        raise FileNotFoundError(f"File does not exist: {path}")

    try:
        import duckdb
    except ImportError as e:
        logger.error("duckdb package is not installed")
        raise

    # DuckDB does not have a simple fixed header check as convenient as SQLite.
    # The reliable test is: can DuckDB open it and query its catalog?
    try:
        with closing(duckdb.connect(path, read_only=True)) as conn:
            # Check that catalog is readable by listing tables
            tables = conn.execute("""
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                  AND table_type = 'BASE TABLE'
            """).fetchall()

            if not tables:
                logger.debug(f"Valid DuckDB file but no user tables: {path}")
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

            logger.debug(f"Valid DuckDB file but tables are empty: {path}")
            return False, "valid DuckDB file, but tables are empty"

    except Exception as e:
        logger.error(f"DuckDB open/query failed for {path}: {e}")
        raise
    

def detect_valid_db_with_data(path: str) -> tuple[str | None, bool, str]:
    """
    Detects whether the file at `path` is a valid SQLite or DuckDB database file containing at least one user table with data.

    Args:
        path: The file path to check.
        
    Returns:
        A tuple (db_type, is_valid, message) where:
        - db_type: The detected database type ("sqlite" or "duckdb"), or None if not a valid database.
        - is_valid: True if the file is a valid database with data, False otherwise.
        - message: A string describing the validation result or reason for failure.
    
    Raises:
        FileNotFoundError: If the file does not exist.
        OSError: If the file cannot be read.
    """
    sqlite_msg = "not checked"
    duckdb_msg = "not checked"
    
    # Try SQLite first
    try:
        ok, sqlite_msg = is_valid_sqlite_with_data(path)
        if ok:
            logger.info(f"Detected valid SQLite database with data: {path}")
            return "sqlite", True, sqlite_msg
    except (FileNotFoundError, OSError):
        # Fatal file access errors - re-raise immediately
        raise
    except (sqlite3.DatabaseError, Exception) as e:
        # SQLite validation failed, but file might still be DuckDB
        logger.debug(f"SQLite validation failed for {path}: {e}")
        sqlite_msg = f"SQLite error: {str(e)}"
    
    # Try DuckDB
    try:
        ok, duckdb_msg = is_valid_duckdb_with_data(path)
        if ok:
            logger.info(f"Detected valid DuckDB database with data: {path}")
            return "duckdb", True, duckdb_msg
    except (FileNotFoundError, OSError):
        # Fatal file access errors - re-raise
        raise
    except ImportError:
        # DuckDB package not available
        logger.warning("DuckDB package not installed, skipping DuckDB validation")
        duckdb_msg = "DuckDB not installed"
    except Exception as e:
        # DuckDB validation failed
        logger.debug(f"DuckDB validation failed for {path}: {e}")
        duckdb_msg = f"DuckDB error: {str(e)}"
    
    # Neither database type validated successfully
    logger.warning(f"File is not a valid database with data: {path}")
    return None, False, f"SQLite: {sqlite_msg}; DuckDB: {duckdb_msg}"


