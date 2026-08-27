import sys
import yaml
import subprocess
import os
import json
import getpass
import shutil
import logging
import asyncio
import asyncssh

import pandas as pd
from pathlib import Path
from typing import Tuple, List, Dict, Any, Optional


from dsi.utils.acquisition.utils import (
    create_directory, 
    create_hashed_folder_from_path, 
    csv_to_list_of_dicts, 
    deduplicate_keep_latest, 
    get_last_part, 
    upsert_records,
    combine_csv,
    split_path,
    confirm_large_download_prompt
)

from dsi.utils.acquisition.git_utils import download_github_file, get_github_remote_file_size
from dsi.utils.acquisition.web_utils import download_web_file, get_url_file_size
from dsi.utils.acquisition.s3_utils import download_s3_file, get_s3_remote_file_size, resolve_s3_bucket_and_key, should_download_s3, get_s3_client

logger = logging.getLogger(__name__)




async def get_file_size_and_download(
    hostname, 
    username, 
    password=None,
    private_key_path=None,
    remote_path=None,
    local_folder=None
):
    """Get file size and download using a single SSH connection.
    
    Args:
        hostname: SSH server hostname
        username: SSH username
        password: SSH password (optional, for password auth or TOTP)
        private_key_path: Path to private key file (optional, for key auth)
        remote_path: Path to file on remote server
        local_folder: Folder to save to (default: current directory)
    
    Returns:
        int: filesize in bytes, success boolean

    Raises:
        asyncssh.PermissionDenied: If authentication fails after password prompt
        asyncssh.Error: For other SSH-related errors
        Exception: For any other errors during download
    """
    print(f"hostname {hostname}, username: {username}, len(password): {len(password) if password else 0}, remote_path: {remote_path}")
    
    # Get the filename from remote path
    filename = os.path.basename(remote_path)
    
    # Set default folder to current directory
    if local_folder is None:
        local_folder = "."
    
    # Create the full local path
    local_path = os.path.join(local_folder, filename)
    
    # Create folder if it doesn't exist
    os.makedirs(local_folder, exist_ok=True)
    
    # Prepare connection options
    connect_options = {
        'host': hostname,
        'username': username,
        'known_hosts': None,
        'connect_timeout': 30,
        'pkcs11_provider': None  # Disable PKCS#11 to avoid "PKCS#11 support not available" error
    }
    
    # Add authentication method
    if password:
        connect_options['password'] = password
    elif private_key_path:
        connect_options['client_keys'] = [private_key_path]
    # If neither is provided, it will try SSH agent or default keys
    
    try:
        # Single async connection for both operations
        async with asyncssh.connect(**connect_options) as conn:
            
            # Open SFTP session
            async with conn.start_sftp_client() as sftp:
                
                # Get file size
                file_stat = await sftp.stat(remote_path)
                file_size = file_stat.size
                print(f"File size: {file_size} bytes")
                
                # Download the file using the same connection
                print(f"Downloading to {local_path}...")
                await sftp.get(remote_path, local_path)
                print(f"Success!!!")
                
                return file_size
        
    except asyncssh.PermissionDenied as e:
        # If no password was provided and authentication failed, prompt for password
        if not password:
            print(f"Authentication failed: {e}")
            print(f"Please enter password for {username}@{hostname}")
            password = getpass.getpass("Password: ")
            
            # Retry with password
            connect_options['password'] = password
            try:
                async with asyncssh.connect(**connect_options) as conn:
                    async with conn.start_sftp_client() as sftp:
                        file_stat = await sftp.stat(remote_path)
                        file_size = file_stat.size
                        print(f"File size: {file_size} bytes")
                        print(f"Downloading to {local_path}...")
                        await sftp.get(remote_path, local_path)
                        print(f"Success!!!")
                        return file_size
            except asyncssh.PermissionDenied as e2:
                print(f"Authentication failed again: {e2}")
                raise
        else:
            # Password was provided but still failed
            print(f"Authentication failed: {e}")
            raise
    except asyncssh.Error as e:
        print(f"SSH Error: {e}")
        raise
    except Exception as e:
        print(f"Error: {e}")
        raise



#
# Endpoints
#

def pull_remote_db(hpc_name: str, remote_dsi: dict, temp_db_storage: str) -> list:
    """ Pull database files from remote HPC endpoints.
    
    Args:
        hpc_name: Name of the HPC system to connect to.
        remote_dsi: Dictionary mapping endpoint names to their database paths.
        temp_db_storage: Local path where downloaded databases will be stored.
    
    Returns:
        list: Database information objects for each successfully pulled endpoint. """
    
    db_infos = []
    for key, value in remote_dsi.items():
        endpoint_name = key
        endpoint_db_path = value
        print(f"Retreiving data for {endpoint_name} at {endpoint_db_path}")
    
        username = input("Username: ")
    
        try:
            db_info = pull_data(location_type="hpc",
                          remote_location=hpc_name,
                          remote_path=endpoint_db_path,
                          download_location=temp_db_storage,
                          username=username)
            db_infos.append(db_info)
        except asyncssh.PermissionDenied as e:
            print(f"Authentication failed for {endpoint_name}: {e}")
            print(f"   Skipping {endpoint_name} and continuing with remaining endpoints...\n")
            continue
        except Exception as e:
            print(f"Error accessing {endpoint_name}: {e}")
            print(f"   Skipping {endpoint_name} and continuing with remaining endpoints...\n")
            continue
    return db_infos


def read_data_sources(csv_data: list, workspace_folder: str) -> Tuple[List[Dict[str, Any]], int]:
    """ Read and pull data sources from CSV records, prompting for credentials when needed.
    
    Args:
        csv_data: List of dictionaries containing source information with keys:
                 'location_type', 'location', 'path', 'submitter_name'.
        workspace_folder: Path to workspace folder for storing pulled data and metadata.
    
    Returns:
        tuple: (database_info, success_counter) where:
            - database_info: List of database information dictionaries for successfully pulled sources.
            - success_counter: Number of successfully pulled data sources. """
    
    database_info = []
    federation_dbs = []
    success_counter = 0
    for row in csv_data:
        username = ""
        password = ""
        if row['location_type'].strip().lower() == "hpc":
            print(f"\n{'='*60}")
            print(f"Enter credentials for data at {row['location']} : {row['path']}")
            username = input("Enter username: ")
            password = getpass.getpass("Enter password: ")  # Hidden input!

        try:
            folder_hash = create_hashed_folder_from_path(row['path'], workspace_folder)[0]
            print(f"folder_hash: {folder_hash}")
            print(f"workspace_folder: {workspace_folder}")

            downloaded_file_path = pull_data(location_type=row['location_type'],
                      remote_location=row['location'],
                      remote_path=row['path'],
                      download_location=(workspace_folder + '/' + folder_hash),
                      username=username,
                      password=password)
            
            if downloaded_file_path:
                # Extract folder and filename from the downloaded path
                _local_folder, _local_filename = split_path(downloaded_file_path)
                
                
                db_info = {
                    "original_location_type": row['location_type'],
                    "original_path": row['path'],
                    "folder_hash": folder_hash,
                    "local_path": _local_folder,
                    "name": _local_filename,
                }
                
                database_info.append(db_info)
                combined = {k: row[k] for k in ["location_type", "location", "submitter_name"]} | {k: db_info[k] for k in ["local_path", "name", "folder_hash"]}
                combined["workspace_folder"] = workspace_folder
                federation_dbs.append(combined)
                success_counter += 1
        except Exception as e:
            print(f"Warning: Skipping database at {row['location']}:{row['path']} due to error: {e}")
            print(f"   Continuing with remaining databases...\n")
            continue

    # Save databases information to a JSON file
    upsert_records(f"{workspace_folder}/dsi_database_list.json", database_info, key="original_path")

    return database_info, success_counter




def get_remote_endpoints_ssh(hostname: str, 
                             username: str,
                             hpc_type: str = "hpc",
                             script_path: str = '/users/pascalgrosset/dsi_test/load_dsi_endpoints.sh',
                             prefixes: List[str] = ['DSI_ENDPOINT_', 'DIANA_ENDPOINT_'],
                             verbose: bool = False) -> dict:
    """ Source bash script on remote server and retrieve environment variables matching specified prefixes.
    
    Args:
        hostname: Remote server hostname or IP address.
        username: SSH username for authentication.
        script_path: Path to bash script on remote server that sets endpoint variables.
                    Default: '/users/pascalgrosset/dsi_test/load_dsi_endpoints.sh'
        prefixes: List of environment variable prefixes to match (e.g., 'DSI_ENDPOINT_').
                 Default: ['DSI_ENDPOINT_', 'DIANA_ENDPOINT_']
    
    Returns:
        dict: Dictionary mapping endpoint variable names to their values.
              Returns empty dict if connection fails or no endpoints found.
    
    Note:
        Uses system SSH with Kerberos authentication. Ensure you have a valid Kerberos ticket
        (run 'klist' to check, 'reticket' to obtain).
    """
    # Convert prefixes list to a format safe for bash
    prefixes_str = ','.join(f'"{p}"' for p in prefixes)
    
    # Use heredoc to avoid quote escaping issues
    command = f"""
source {script_path} && python3 << 'PYTHON_EOF'
import os
import json

# The prefixes we're looking for
prefixes = [{prefixes_str}]
prefix_tuple = tuple(prefixes)

# Get matching environment variables
endpoints = {{
    key: value 
    for key, value in os.environ.items() 
    if key.startswith(prefix_tuple)
}}

# Output as JSON so we can parse it easily
print(json.dumps(endpoints))
PYTHON_EOF
"""
    
    print(f"Connecting to {hostname} as {username}...")
    
    try:
        result = subprocess.run(
            ['ssh', f'{username}@{hostname}', command],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            if verbose:
                print(f"Command failed: {result.stderr.strip()}")
            return {}
        
        endpoints = json.loads(result.stdout.strip())
        
        if verbose:
            print(f"Found {len(endpoints)} endpoints on {hostname}")
        
        return endpoints
        
    except subprocess.TimeoutExpired:
        if verbose:
            print(f"Connection to {hostname} timed out")
        return {}
    except json.JSONDecodeError:
        if verbose:
            print(f"Failed to parse endpoint data")
        return {}
    except FileNotFoundError:
        if verbose:
            print(f"SSH client not found")
        return {}
    except Exception as e:
        if verbose:
            print(f"Error: {e}")
        return {}


def pull_data_endpoints(endpoints_location: dict, hpc_name: str, workspace_folder: str) -> Tuple[List[Dict[str, Any]], int]:
    """ Pull data from multiple remote endpoints by downloading metadata CSVs and fetching the actual data.
    
    Args:
        endpoints_location: Dictionary mapping endpoint names to their remote CSV paths.
        hpc_name: Name of the HPC system to connect to.
        workspace_folder: Path to workspace folder for storing pulled data and metadata.
    
    Returns:
        tuple: (database_info, success_counter) from read_data_sources containing:
            - database_info: List of database information dictionaries.
            - success_counter: Number of successfully pulled data sources.
    
    Note:
        Creates temporary folder '.test_00' for intermediate CSV files and
        generates 'output_csv.csv' with combined metadata. """
    
    # create a temporaty folder to store the csv files to be downloaded
    temp_db_storage = ".test_00"
    create_directory(dir_name=temp_db_storage, delete_if_exists=True, verbose=True)

    # download them
    db_infos = pull_remote_db(hpc_name, endpoints_location, temp_db_storage)
    
    # Check if any endpoints were successfully downloaded
    if not db_infos:
        print("\nWarning: No endpoint metadata files were successfully downloaded.")
        print("   No databases will be federated.")
        return [], 0

    # combine them to output_csv and the dictionary csv_data_sources
    output_csv = str(Path(temp_db_storage) / "output_csv.csv")
    try:
        csv_data_sources = combine_csv(temp_db_storage, output_csv)
    except ValueError as e:
        print(f"\nWarning: {e}")
        print("   No databases will be federated.")
        return [], 0

    # Pull the data from the CSV file, return a dictionary, and output the dictionaty to workspace_folder
    database_info = read_data_sources(csv_data_sources, workspace_folder)

    return database_info




#
# Get data
#

def pull_data(location_type: str, 
              remote_location: str, 
              remote_path: str, 
              download_location: str, 
              username: str,
              password: str = "",
              download_limit: int = 0) -> str:
    """Pulls data from a specified location based on the location type (e.g., "github", "HPC", "HPC-Kerberos", "URL", "local"). 
    The function checks for existing files, compares them with remote versions using MD5 checksums, and downloads or skips files accordingly. 
    It also handles user interactions for confirming downloads of large files and manages host usernames for HPC access.

    Args:
        location_type (str): The type of the original location (e.g., "github", "HPC", "HPC-kerberos", "URL", "local").
        remote_location (str): The location of the database (e.g., hostname for HPC, URL for web).
        remote_path (str): The path to the data or database at the remote location.
        download_location (str): The absolute path to the workspace folder where the data or database will be stored.
        username (str): username for hpc systems
        password (str): optional
        download_limit (int): The maximum size of a file that can be downloaded without confirmation, if 0 no limit
    Returns:
        str: filepath"""

    # Do some cleanup
    cleaned_location_type = location_type.strip().lower()
    remote_path = remote_path.strip()

    # Extract filepath
    filename = get_last_part(remote_path)
    if not filename:
        raise ValueError(f"Could not extract filename from {remote_path}")  # Fix: raise instead of return
    else:
        print(f"Filename: {filename}")
    file_path = Path(download_location) / filename

    
    print(f"\n\n - Downloading file at {location_type}:{remote_location}:{remote_path} to {download_location} ...")

    # Stat downloading
    if cleaned_location_type == "github":
        
        # Check if the file exists and get its size
        filesize = 0
        try:
            filesize = get_github_remote_file_size(remote_path)
        except Exception as e:
            print(f" -- Could not access the file at {remote_path}. Skipping this database.")
            raise

        # Confirm for sizes above a limit
        if download_limit != 0:
            if not confirm_large_download_prompt(filesize, download_limit):
                print(" -- Skipping this database.")
                raise PermissionError("Download cancelled by user.")

        # Download the file
        try:
            download_github_file(url=remote_path, out_path=download_location)

            local_file_size = file_path.stat().st_size
            return str(file_path)

        except Exception as e:
            print(f" -- Error downloading file from GitHub: {e}. Skipping this database.")
            raise


    elif cleaned_location_type == "url":
    
            filesize = 0
            try:
                filesize = get_url_file_size(remote_path)
            except Exception:
                print(f" -- Could not access the file at {remote_path}. Skipping this database.")
                raise
    
            # Confirm for sizes above a limit
            if not confirm_large_download_prompt(filesize, download_limit):
                print(" -- Skipping this database.")
                raise PermissionError("Download cancelled by user.")
    
            # Download the file
            try:
                download_web_file(url=remote_path, output_dir=download_location)
                local_file_size = file_path.stat().st_size
                return str(file_path)
    
            except Exception as e:
                print(f" -- Error {e} downloading file at {remote_path}. Skipping this database.")
                raise


    #elif cleaned_location_type == "hpc-kerberos":
        

    elif cleaned_location_type == "hpc":

        # Ask for username if we don't have it for this host yet
        if username == "":
            try:
                username = input(f" -- Enter the username for {remote_location}: ")
            except KeyboardInterrupt:
                print(f"\n -- Interrupted while entering username for {remote_location}. Skipping this database.")
                raise

        # Note: MD5 verification not implemented for HPC downloads in pull_data
        # File will be downloaded/re-downloaded

        # Get file size and download in one connection (TOTP passwords can only be used once!)
        try:
            # Run the async function synchronously
            filesize = asyncio.run(get_file_size_and_download(
                hostname=remote_location,
                username=username,
                password=password,
                remote_path=remote_path,
                local_folder=download_location
            ))
            
            if filesize is None:
                print(f" -- Could not access or download the file at {remote_location}:{remote_path}. Skipping this database.")
                raise RuntimeError(f"Failed to download from {remote_location}:{remote_path}")
            
            # Note: We get the size but don't check download_limit until after download
            # because TOTP can't be reused. If you want to check first, you'll need
            # to prompt for a new TOTP code.
            if download_limit > 0 and filesize > download_limit:
                print(f" -- Downloaded file is {filesize} bytes (above {download_limit} byte limit)")
                print(" -- Note: File was already downloaded due to one-time password limitation")
            
            return str(file_path)
                
        except KeyboardInterrupt:
            print(f" -- Interrupted while accessing {remote_location}:{remote_path}. Skipping this database.")
            raise
        except Exception as e:
            print(f" -- Could not access the file at {remote_location}:{remote_path}; error: {e}. Skipping this database.")
            raise
    

    elif cleaned_location_type == "s3":
        try:
            bucket, key = resolve_s3_bucket_and_key(location=remote_location, path=remote_path)
        except ValueError as e:
            print(f" -- Invalid S3 remote_location/path: {e}. Skipping this database.")
            raise

        aws_region = "us-gov-west-1"
        aws_profile = None

        try:
            s3_client = get_s3_client(
                region_name=aws_region,
                profile_name=aws_profile,
                allow_anonymous=False,
                interactive=True,
            )
        except Exception as e:
            print(f" -- Could not initialize S3 client: {e}. Skipping this database.")
            raise

        try:
            filesize = get_s3_remote_file_size(bucket=bucket, key=key, s3_client=s3_client)
        except PermissionError as e:
            print(f" -- Permission error: {e}. Skipping this database.")
            raise
        except FileNotFoundError as e:
            print(f" -- Could not access S3 object s3://{bucket}/{key}; error: {e}. Skipping this database.")
            raise
        except Exception as e:
            print(f" -- Could not access S3 object s3://{bucket}/{key}; error: {e}. Skipping this database.")
            raise

        if not confirm_large_download_prompt(filesize, download_limit):
            print(" -- Skipping this database.")
            raise PermissionError("Download cancelled by user.")

        try:
            downloaded_path = download_s3_file(
                bucket=bucket,
                key=key,
                output_dir=download_location,
                s3_client=s3_client,
            )
            local_file_size = Path(downloaded_path).stat().st_size
            return str(downloaded_path)
        except Exception as e:
            print(f" -- Error downloading file from S3: {e}. Skipping this database.")
            raise 


    elif cleaned_location_type == "local":
        # Check if the file exists
        if not Path(remote_path).exists():
            print(f" -- Local file {remote_path} does not exist. Skipping this database.")
            raise FileNotFoundError(f"Local file {remote_path} does not exist")

        # Check if it's a file
        if not Path(remote_path).is_file():
            print(f" -- Local path {remote_path} is not a file. Skipping this database.")
            raise ValueError(f"Local path {remote_path} is not a file")


        _abs_path = str(Path(remote_path).resolve())

        try:
            shutil.copy2(_abs_path, file_path)
            print(f"Copied to {file_path}")
            local_file_size = file_path.stat().st_size
            return str(file_path)
        except Exception as e:
            print(f" -- Error copying this file from local: {e}. Skipping this database.")
            raise

    else:
        print(f"Location type {location_type} for database {remote_path} is unsupported. Skipping.")
        raise ValueError(f"Unsupported location type: {location_type}")


def accquire_data(location_type: str, 
              remote_location: str, 
              remote_path: str, 
              abs_path_workspace_folder: str, 
              username: str,
              password: str,
              download_limit: int = 10485760,
              internal_use = False,
              parent_hash: str = None) -> dict:
    """Pulls data from a specified location based on the location type (e.g., "github", "HPC", "HPC-Kerberos", "URL", "local"). 
    The function checks for existing files, compares them with remote versions using MD5 checksums, and downloads or skips files accordingly. 
    It also handles user interactions for confirming downloads of large files and manages host usernames for HPC access.

    Args:
        location_type (str): The type of the original location (e.g., "github", "HPC", "HPC-kerberos", "URL", "local").
        location (str): The location of the database (e.g., hostname for HPC, URL for web).
        path (str): The path to the data or database at the original location.
        abs_path_workspace_folder (str): The absolute path to the workspace folder where the data or database will be stored.
        username (str): username for hpc systems
        pass
        download_limit (int): The maximum size of a file that can be downloaded without confirmation.
    Returns:
        dict : a dictionary entry for the data"""

    # Create folder for data
    tmp_path = Path(abs_path_workspace_folder).resolve()
    if not tmp_path:
        print(f"{abs_path_workspace_folder} is invalid!!!")
        return None

    
    abs_path_workspace_folder = str(tmp_path)
    if parent_hash:
        folder_hash, abs_path_db_folder = create_hashed_folder_from_path(parent_hash, abs_path_workspace_folder)
    else:
        folder_hash, abs_path_db_folder = create_hashed_folder_from_path(remote_path, abs_path_workspace_folder)

    print(f"folder_hash: {folder_hash}, abs_path_db_folder: {abs_path_db_folder}")
    try:
        downloaded_file_path = pull_data(location_type, 
                                        remote_location, 
                                        remote_path, 
                                        folder_hash, 
                                        username,
                                        password,
                                        download_limit)

        _local_folder, _local_filename = split_path(downloaded_file_path)
        print(f"Successfully acquired the data at {remote_location}:{remote_path} to {_local_folder}") 
        
        return {
                "original_location_type": location_type,
                "original_path": remote_path,
                "folder_hash": folder_hash,
                "local_path": _local_folder,
                "name": _local_filename,
            }

    
    except Exception as e:
        print(f"Could not acquire data at {remote_location}:{remote_path}")
        return None
