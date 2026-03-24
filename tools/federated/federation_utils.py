import csv
import hashlib
import shlex
import subprocess

from dsi.dsi import DSI
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse



def create_folder_from_path(s: str, base_dir: str) -> str:
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


    return folder_name, out_dir



def get_last_part(s: str) -> str:
    """Get the last part of a path or URL, which is often the filename. Works for both URLs and plain paths.
    
    Arg:
        s (str): The input string, which can be a file path or a URL.

    Returns:
        str: The last part of the path or URL, typically the filename.
    """
    path = urlparse(s).path  # works for both URLs and plain paths
    return PurePosixPath(path).name



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



def remote_md5(remote: str, remote_path: str, timeout: int | None = None) -> str:
    """Computes the MD5 checksum of a remote file by running md5sum over SSH.
    
    Args:
        remote (str): The remote host, optionally with username (e.g. "user@host")
        remote_path (str): The path to the file on the remote host (e.g. "/data/file.bin")
        timeout (int | None): Optional timeout in seconds for the entire operation. Default is None (no timeout).

    Returns:
        str: The computed MD5 checksum as a hexadecimal string.
    """
    cmd = [
        "ssh",
        remote,
        f"md5sum {shlex.quote(remote_path)}"
    ]

    result = subprocess.run(
        cmd,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=True,
    )

    return result.stdout.split()[0]



def should_download(remote:str, remote_path:str, stored_md5:str) -> bool:
    """Determines whether a remote file should be downloaded by comparing its MD5 checksum to a stored value.
    
    Args:
        remote (str): The remote host, optionally with username (e.g. "user@host")
        remote_path (str): The path to the file on the remote host (e.g. "/data/file.bin")
        stored_md5 (str): The stored MD5 checksum to compare against.

    Returns:
        bool: True if the file should be downloaded, False otherwise.
    """
    try:
        remote_hash = remote_md5(remote, remote_path)
    except Exception as e:
        print(f"Failed to get remote hash for {remote}:{remote_path}: {e}")
        return False

    return remote_hash != stored_md5



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



def create_directory(dir_name: str = "temp", exist_ok=True):
    """
    Creates a directory with the given name in the current working directory.
    If the directory already exists and exist_ok is True, it will not raise an error.

    Args:
        dir_name (str): The name of the directory to create. Default is "temp".
        exist_ok (bool): If True, do not raise an error if the directory already exists. Default is True.   
    """

    workspace = Path.cwd()  # current directory (your local workspace)
    dir_path = workspace / dir_name
    dir_path.mkdir(parents=True, exist_ok=exist_ok)
    print("Created:", dir_path)



def run_shell_cmd(cmd: str, cwd: str | None = None) -> None:
    """
    Runs a shell command and raises an error if it fails.

    Args:
        cmd (str): The command to run.  
        cwd (str | None): The working directory to run the command in. If None, it will run in the current directory. Default is None.  
    """

    p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, shell=True)
    if p.returncode != 0:
        raise RuntimeError(f"Command failed: {cmd}\nstdout:\n{p.stdout}\nstderr:\n{p.stderr}")
    


def create_index_db(index_path:str, catalogue_name: str = "metadata_catalogue.db"):
    """
    Creates an index database from a CSV file containing metadata about databases.

    Args:
        index_path (str): The path to the CSV file containing the metadata.  
        catalogue_name (str): The name of the catalogue database to create. Default is "metadata_catalogue.db". 
    """

    print(index_path)
    dsi_catalogue = DSI(catalogue_name)
    dsi_catalogue.read(index_path, reader_name="CSV", table_name="catalogue")
    dsi_catalogue.close()
    print(f"Database is accessible at: {catalogue_name}")
