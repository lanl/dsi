# Standard library imports
import csv
import hashlib
import json
import logging
import os
import shutil
import sqlite3
from contextlib import closing
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Tuple
from urllib.parse import urlparse

# Third-party imports
import pandas as pd

# Local application imports
from dsi.dsi import DSI


logger = logging.getLogger(__name__)


def confirm_large_download_prompt(filesize: int, download_limit: int = 10485760) -> bool:
    """Prompts the user to confirm the download of a file if its size exceeds a specified limit. The function displays the file size in a human-readable format and asks the user for confirmation before proceeding with the download.
    
    Args:        
        filesize (int): The size of the file in bytes.
        download_limit (int): The download limit in bytes. If the file size exceeds this limit (default is 10MB), the user will be prompted for confirmation.

    Returns:
        bool: True if the user confirms the download, False otherwise.
    """

    if filesize <= download_limit:
        return True

    try:
        print(
            f"File size {human_readable_size(filesize)} exceeds the "
            f"download limit of {human_readable_size(download_limit)}."
        )
        choice = input(" -- Please confirm that you want to download this file (y/n): ").strip().lower()
        return choice == "y"
    except Exception:
        return False
        


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


def parse_timestamp(ts: str) -> datetime:
    """Parses a timestamp string in the format "YYYY-MM-DD--HH:MM:SS" and returns a datetime object. 
    The function also handles an optional trailing 's' character.

    Args:
        ts (str): The timestamp string to parse.
    """
    
    # normalize your slightly inconsistent format
    ts = ts.rstrip("s")  # remove trailing 's' if present
    return datetime.strptime(ts, "%Y-%m-%d--%H:%M:%S")


def deduplicate_keep_latest(records: list[dict]) -> list[dict]:
    """Deduplicates a list of records by keeping only the latest record for each unique combination of location_type, location, and path. 
    The function uses the timestamp field to determine which record is the latest.
    
    Arg:
        records: A list of dictionaries, where each dictionary represents a record with at least the following keys: "location_type", "location", "path", and "timestamp".

    Returns:
        A deduplicated list of records, where only the latest record for each unique combination of location_type, location, and path is kept.
    """
    best = {}

    for record in records:
        key = (
            record.get("location_type"),
            record.get("location"),
            record.get("path"),
        )

        current_ts = parse_timestamp(record.get("timestamp", ""))

        if key not in best:
            best[key] = record
        else:
            existing_ts = parse_timestamp(best[key].get("timestamp", ""))

            if current_ts > existing_ts:
                best[key] = record

    return list(best.values())


def create_hashed_folder_from_path(s: str, base_dir: str) -> tuple[str, str]:
    """Generates a folder name from a given path or URL by taking the last part of the path and hashing it to create a unique identifier.
    
    Arg:
        s (str): The input string, which can be a file path or a URL.
        base_dir (str): The base directory where the folder will be created.


    Returns:
        str: A unique folder name derived from the last part of the path or URL.
        str: The full path to the created folder.
    """
    name = PurePosixPath(urlparse(s).path).name
    folder_name = hashlib.sha256(name.encode("utf-8")).hexdigest()[:16]

    out_dir = Path(base_dir) / folder_name
    out_dir.mkdir(parents=True, exist_ok=True)


    return folder_name, str(out_dir)


def combine_csv(folder_path: str, output_csv: str) -> list:
    """ Combine all CSV files in a folder into a single CSV and return as records.
    
    Args:
        folder_path: Path to folder containing CSV files to combine.
        output_csv: Path where the combined CSV file will be saved.
    
    Returns:
        list: List of dictionaries, where each dictionary represents a row
              from the combined data.
    
    Raises:
        FileNotFoundError: If the folder does not exist.
        NotADirectoryError: If the path is not a directory.
        ValueError: If no CSV files are found in the folder.
    """
    # Check if folder exists and is a directory
    folder = Path(folder_path)
    if not folder.exists():
        raise FileNotFoundError(f"Folder does not exist: {folder_path}")
    
    if not folder.is_dir():
        raise NotADirectoryError(f"Path is not a directory: {folder_path}")
    
    # Check if output directory exists
    output_path = Path(output_csv)
    if not output_path.parent.exists():
        raise FileNotFoundError(f"Output directory does not exist: {output_path.parent}")
    
    # Get CSV files
    csv_files = list(folder.glob("*.csv"))
    
    if not csv_files:
        raise ValueError(f"No CSV files found in folder: {folder_path}")
    
    # Combine CSV files
    dfs = []
    for file in csv_files:
        df = pd.read_csv(file)
        df['source_file'] = file.name  # Add column with source filename
        dfs.append(df)
    
    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df.to_csv(output_csv, index=False)

    return combined_df.to_dict('records')


def get_last_part(s: str) -> str:
    """Get the last part of a path or URL, which is often the filename.
    
    Arg:
        s (str): The input string, which can be a file path or a URL.
    
        Returns:
            str: The last part of the path or URL, typically the filename.
    """
    try:
        # Convert to string first (handles float, int, None, etc.)
        s = str(s).strip()
        
        # Now parse it
        path = urlparse(s).path
        return PurePosixPath(path).name
        
    except Exception as e:
        print(f"Cannot parse '{s}': {e}")
        return ""


def human_readable_size(num_bytes: int) -> str:
    """Converts a file size in bytes to a human-readable string with appropriate units (e.g., KB, MB, GB).  
    
    Arg:
        num_bytes (int): The file size in bytes.

    Returns:
        str: A human-readable string representing the file size with appropriate units. 
    """
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = float(num_bytes)

    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.2f} {unit}"
        size /= 1024


def compute_md5(file_path:str, chunk_size:int = 8192) -> str:
    """Computes the MD5 checksum of a file.
    
    Args:
        file_path (str): The path to the file to compute the checksum for.
        chunk_size (int): The size of the chunks to read from the file. Default is 8192 bytes.

    Returns:
        str: The computed MD5 checksum as a hexadecimal string.
    """
    md5 = hashlib.md5()
    
    with open(file_path, "rb") as f:
        while chunk := f.read(chunk_size):
            md5.update(chunk)
    
    return md5.hexdigest()


def is_file(path_provided: str)-> int:
    """Checks if the provided path is a file, a directory, or does not exist.
    
    Args:
        path_provided (str): The path to check.

    Returns:
        int: 1 if it is a file, 2 if it is a directory, 0 if it does not exist or is something else.
    """
    p = Path(path_provided)

    if p.is_file():
        return 1
    elif p.is_dir():
        return 2
    else:
        return 0


def csv_to_list_of_dicts(path: str) -> list[dict[str, str]]:
    """
    Reads a CSV file and returns a list of dictionaries, where each dictionary represents a row in the CSV file with column headers as keys.

    Args:
        path (str): The file path to the CSV file.

    Returns:
        list[dict[str, str]]: A list of dictionaries representing the rows in the CSV file.
    """
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def create_directory(
    dir_name: str,
    base_path: str | Path | None = None,
    delete_if_exists: bool = False,
    exist_ok: bool = True,
    verbose: bool = False
) -> Path:
    """
    Creates a directory, with options to use current directory as base or handle existing folders.
    
    Args:
        dir_name: Name or path of the directory to create.
                 Can be absolute path or relative to base_path.
        base_path: Base directory to create dir_name in. 
                  If None, uses current working directory.
                  Ignored if dir_name is an absolute path.
                  Default: None (uses cwd)
        delete_if_exists: If True, deletes the folder and all its contents if it exists
                         before creating it. If False, behavior depends on exist_ok.
                         Default: False (safer - won't delete existing data)
        exist_ok: If True and folder exists, no error is raised (unless delete_if_exists=True).
                 If False and folder exists, raises FileExistsError.
                 Default: True
        verbose: If True, prints the created directory path.
                Default: False
    
    Returns:
        Path: The Path object of the created directory.
    
    Warning:
        When delete_if_exists=True, permanently deletes the folder and all its contents.
    
    Raises:
        FileExistsError: If the directory exists and exist_ok=False.
    
    Examples:
        >>> create_directory("temp")  # Creates ./temp
        >>> create_directory("data", base_path="/home/user")  # Creates /home/user/data
        >>> create_directory("/absolute/path")  # Creates at absolute path
        >>> create_directory("old_data", delete_if_exists=True)  # Deletes and recreates
    """
    # Determine the full path
    dir_path = Path(dir_name)
    
    # If path is relative and base_path is provided, use base_path
    if not dir_path.is_absolute() and base_path is not None:
        dir_path = Path(base_path) / dir_name
    # If path is relative and no base_path, use current working directory
    elif not dir_path.is_absolute():
        dir_path = Path.cwd() / dir_name
    
    # Delete if exists and delete_if_exists is True
    if delete_if_exists and dir_path.exists():
        shutil.rmtree(dir_path)
        if verbose:
            print(f"Deleted existing: {dir_path}")
    
    # Create the directory
    dir_path.mkdir(parents=True, exist_ok=exist_ok)
    
    if verbose:
        print(f"Created: {dir_path}")
    
    return dir_path


def upsert_records(file_path: str, new_records: list[dict], key: str) -> None:
    """Upserts records into a JSON file by using a specified key to determine uniqueness. If a record with the same key already exists, it will be updated; otherwise, the new record will be inserted.

    Args:
        file_path (str): The path to the JSON file where the records are stored.
        new_records (list[dict]): A list of dictionaries representing the new records to upsert.
        key (str): The key in the dictionaries that should be used to determine uniqueness for upserting.

    """
    # Load existing data
    _file_path = Path(file_path)
    if _file_path.exists():
        with open(_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = []

    # Index existing records by key
    indexed = {item[key]: item for item in data}

    # Insert or update
    for record in new_records:
        indexed[record[key]] = record

    # Convert back to list
    updated_data = list(indexed.values())

    # Save back to file
    with open(_file_path, "w", encoding="utf-8") as f:
        json.dump(updated_data, f, indent=2)
        
        
def split_path(path: str) -> Tuple[str, str]:
    """Split a path into folder path and filename.
    
    Args:
        path (str): The file path to split.
    
    Returns:
        Tuple[str, str]: A tuple of (folder_path, filename).
                        If path is just a filename, folder_path will be empty string.
    
    Examples:
        >>> split_path("/home/user/data/file.csv")
        ('/home/user/data', 'file.csv')
        
        >>> split_path("relative/path/file.txt")
        ('relative/path', 'file.txt')
        
        >>> split_path("file.txt")
        ('', 'file.txt')
    """
    p = Path(path)
    folder_path = str(p.parent) if p.parent != Path('.') else ''
    filename = p.name
    return folder_path, filename