from pathlib import Path
import subprocess
import csv
from dsi.dsi import DSI

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


def run(cmd: str, cwd: str | None = None) -> None:
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
