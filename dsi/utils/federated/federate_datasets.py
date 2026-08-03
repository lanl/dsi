import sys
import uuid
import yaml
import paramiko
import argparse
import subprocess
import os
import json
import getpass
import shutil

import pandas as pd
from pathlib import Path
from typing import Tuple, List, Dict, Any, Optional


from dsi.utils.federation_utils import (
    compute_md5, 
    create_directory, 
    create_hashed_folder_from_path, 
    csv_to_list_of_dicts, 
    deduplicate_keep_latest, 
    get_last_part, 
    human_readable_size, 
    should_download, 
    upsert_records,
    combine_csv,
    create_folder
)

from dsi.utils.git_utils import download_github_file, get_github_remote_file_size
from dsi.utils.rsync_utils import rsync_download_interactive, ssh_remote_size_bytes_interactive
from dsi.utils.web_utils import download_web_file, get_url_file_size
from dsi.utils.s3_utils import download_s3_file, get_s3_remote_file_size, resolve_s3_bucket_and_key, should_download_s3, get_s3_client
from dsi.utils.hpc_kerberos import ssh_k_remote_size_bytes, scp_k_copy_from



def confirm_large_download(filesize: int, download_limit: int) -> bool:
    """Prompts the user to confirm the download of a file if its size exceeds a specified limit. The function displays the file size in a human-readable format and asks the user for confirmation before proceeding with the download.
    
    Args:        
        filesize (int): The size of the file in bytes.
        download_limit (int): The download limit in bytes. If the file size exceeds this limit, the user will be prompted for confirmation.

    Returns:
        bool: True if the user confirms the download, False otherwise.
    """

    if filesize <= download_limit:
        return True

    print(
        f"File size {human_readable_size(filesize)} exceeds the "
        f"download limit of {human_readable_size(download_limit)}."
    )
    choice = input(" -- Please confirm that you want to download this file (y/n): ").strip().lower()
    return choice == "y"



def make_db_info(location_type:str, path:str, folder_hash:str, local_path:str, db_name:str="") -> dict:
    """Creates a dictionary containing information about a database, including its original location type, original path, local path, and name.
    
    Args:
     location_type (str): The type of the original location (e.g., "github", "HPC", "URL", "local").
        path (str): The original path to the database.
        folder_hash (str): The unique hash identifier for the folder that stores this database.
        local_path (str): The local path where the database is stored after downloading or copying.
        db_name (str): The name of the database.

    Returns:
        dict: A dictionary containing the database information with keys "original_location_type", "original_path", "local_path", and "name".
    
    """
    return {
        "original_location_type": location_type,
        "original_path": path,
        "folder_hash": folder_hash,
        "local_path": str(local_path),
        "name": db_name,
    }


def get_file_size_and_download(hostname, username, password, remote_path, local_folder=None):
    """Get file size and download using a single SSH connection.
    
    Args:
        hostname: SSH server hostname
        username: SSH username
        password: SSH password (TOTP from authenticator)
        remote_path: Path to file on remote server
        local_folder: Folder to save to (default: current directory)
    
    Returns:
        tuple: (filesize in bytes, success boolean), or (None, False) on error
    """
    print(f"hostname {hostname}, username: {username}, len(password): {len(password)}, remote_path: {remote_path}")
    
    # Get the filename from remote path
    filename = os.path.basename(remote_path)
    
    # Set default folder to current directory
    if local_folder is None:
        local_folder = "."
    
    # Create the full local path
    local_path = os.path.join(local_folder, filename)
    
    # Create folder if it doesn't exist
    os.makedirs(local_folder, exist_ok=True)
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    sftp = None
    
    try:
        # Single connection for both operations
        ssh.connect(hostname, username=username, password=password, timeout=30)
        sftp = ssh.open_sftp()
        
        # Get file size
        file_stat = sftp.stat(remote_path)
        file_size = file_stat.st_size
        print(f"File size: {file_size} bytes")
        
        # Download the file using the same connection
        print(f"Downloading to {local_path}...")
        sftp.get(remote_path, local_path)
        print(f"Success!!!")
        
        return file_size, True
        
    except Exception as e:
        print(f"!!!! !!!! !!! Error: {e}")
        return None, False
        
    finally:
        # Always close connections
        if sftp:
            try:
                sftp.close()
            except:
                pass
        try:
            ssh.close()
        except:
            pass






# Enhanced version with progress indicator for large files
def get_file_size_and_download_with_progress(hostname: str, 
                                             username: str, 
                                             remote_path: str, 
                                             local_folder: Optional[str] = None) -> Tuple[Optional[int], bool]:
    """Get file size and download using SSH/SCP with progress display.
    
    Same as get_file_size_and_download but shows progress during download.
    """
    print(f"hostname: {hostname}, username: {username}, remote_path: {remote_path}")
    
    filename = os.path.basename(remote_path)
    if local_folder is None:
        local_folder = "."
    local_path = os.path.join(local_folder, filename)
    os.makedirs(local_folder, exist_ok=True)
    
    try:
        # Get file size
        print(f"Getting file size...")
        stat_command = f"stat -c %s {remote_path} 2>/dev/null || stat -f %z {remote_path}"
        
        result = subprocess.run(
            ['ssh', f'{username}@{hostname}', stat_command],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            print(f"✗ Failed to get file size: {result.stderr.strip()}")
            return None, False
        
        file_size = int(result.stdout.strip())
        print(f"File size: {file_size} bytes ({file_size / (1024*1024):.2f} MB)")
        
        # Download with progress (uses stderr for progress updates)
        print(f"Downloading to {local_path}...")
        
        # Run SCP without capture_output to show progress in terminal
        scp_result = subprocess.run(
            ['scp', '-v', f'{username}@{hostname}:{remote_path}', local_path],
            timeout=300
        )
        
        if scp_result.returncode != 0:
            print(f"✗ Download failed")
            return file_size, False
        
        # Verify
        if os.path.exists(local_path):
            downloaded_size = os.path.getsize(local_path)
            if downloaded_size == file_size:
                print(f"Success!!! Downloaded {downloaded_size} bytes")
                return file_size, True
            else:
                print(f"⚠ Warning: Size mismatch. Expected {file_size}, got {downloaded_size}")
                return file_size, False
        else:
            print(f"✗ File not found after download")
            return file_size, False
        
    except subprocess.TimeoutExpired:
        print(f"✗ Operation timed out")
        return None, False
    except Exception as e:
        print(f"!!!! !!!! !!! Error: {e}")
        return None, False

        
        
# def just_pull_data(location_type: str, 
#               location: str, 
#               path: str, 
#               abs_path_workspace_folder: str, 
#               username: str,
#               password: str,
#               download_limit: int = 10485760) -> bool:
#     """Pulls data from a specified location based on the location type (e.g., "github", "HPC", "HPC-Kerberos", "URL", "local"). 
#     The function checks for existing files, compares them with remote versions using MD5 checksums, and downloads or skips files accordingly. 
#     It also handles user interactions for confirming downloads of large files and manages host usernames for HPC access.

#     Args:
#         location_type (str): The type of the original location (e.g., "github", "HPC", "HPC-kerberos", "URL", "local").
#         location (str): The location of the database (e.g., hostname for HPC, URL for web).
#         path (str): The path to the data or database at the original location.
#         abs_path_workspace_folder (str): The absolute path to the workspace folder where the data or database will be stored.
#         username (str): username for hpc systems
#         pass
#         download_limit (int): The maximum size of a file that can be downloaded without confirmation.
#         internal_use (bool): Determines if returned object is a dict or a tuple of (dict, username)
#     Returns:
#         dict | tuple[dict, str]: A dict of data/db info or a tuple of (data/db information, username). Second case if internal_use = True
#     """

#     cleaned_location_type = location_type.strip().lower()

#     if cleaned_location_type == "hpc":

#         # Ask for username if we don't have it for this host yet
#         if username == "":
#             try:
#                 username = input(f" -- Enter the username for {location}: ")
#             except KeyboardInterrupt:
#                 print(f"\n -- Interrupted while entering username for {location}. Skipping this database.")
#                 return False

#         # Get file size and download in one connection (TOTP passwords can only be used once!)
#         try:
#             filesize, success = get_file_size_and_download(
#                 hostname=location,
#                 username=username,
#                 password=password,
#                 remote_path=path,
#                 local_folder=abs_path_workspace_folder
#             )
            
#             if not success or filesize is None:
#                 print(f" -- Could not access or download the file at {location}:{path}. Skipping this database.")
#                 return False

#             # Note: We get the size but don't check download_limit until after download
#             # because TOTP can't be reused. If you want to check first, you'll need
#             # to prompt for a new TOTP code.
#             if filesize > download_limit:
#                 print(f" -- Downloaded file is {filesize} bytes (above {download_limit} byte limit)")
#                 print(" -- Note: File was already downloaded due to one-time password limitation")
#                 return False
             
#             return True
           
#         except KeyboardInterrupt:
#             print(f" -- Interrupted while accessing {location}:{path}. Skipping this database.")
#             return False
#         except Exception as e:
#             print(f" -- Could not access the file at {location}:{path}; error: {e}. Skipping this database.")
#             return False

#     else:
#         return False
    

def just_pull_data(
    location_type: str,
    location: str,
    path: str,
    abs_path_workspace_folder: str,
    username: str = "",
    #password: str = "",
    download_limit: int = 10_485_760,
) -> bool:
    """Download a file from an HPC system.

    The remote file size and file contents are retrieved using one connection
    because a one-time password may not be reusable.

    Args:
        location_type: Source type. Currently only ``"hpc"`` is supported.
        location: HPC hostname.
        path: Absolute path to the remote file.
        abs_path_workspace_folder: Local destination directory.
        username: HPC username. If empty, the user is prompted.
        password: HPC password or one-time authentication code.
        download_limit: Size threshold used to report large downloads, in bytes.

    Returns:
        True if the file was downloaded successfully; otherwise False.
    """
    cleaned_location_type = location_type.strip().lower()

    if cleaned_location_type != "hpc":
        print(f" -- Unsupported location type: {location_type}")
        return False

    if not username:
        try:
            username = input(f" -- Enter the username for {location}: ").strip()
        except (KeyboardInterrupt, EOFError):
            print(
                f"\n -- Interrupted while entering username for "
                f"{location}. Skipping this database."
            )
            return False

        if not username:
            print(f" -- No username provided for {location}.")

    # if not password:
    #     print(f" -- No password or authentication code provided for {location}.")

    try:
        # filesize, success = get_file_size_and_download(
        #     hostname=location,
        #     username=username,
        #     password=password,
        #     remote_path=path,
        #     local_folder=abs_path_workspace_folder,
        # )
        filesize, success = get_file_size_and_download_with_progress(
                    hostname=location,
                    username=username,
                    remote_path=path,
                    local_folder=abs_path_workspace_folder,
                )

        if not success or filesize is None:
            print(
                f" -- Could not access or download the file at "
                f"{location}:{path}. Skipping this database."
            )
            return False

        if filesize > download_limit:
            print(
                f" -- Downloaded file is {filesize} bytes "
                f"(above the {download_limit}-byte threshold)."
            )
            print(
                " -- The file was already downloaded because the one-time "
                "password could not be reused."
            )

        return True

    except KeyboardInterrupt:
        print(
            f" -- Interrupted while accessing {location}:{path}. "
            "Skipping this database."
        )
        return False
    except Exception as exc:
        print(
            f" -- Could not access the file at {location}:{path}; "
            f"error: {exc}. Skipping this database."
        )
        return False
    
    

def pull_data(location_type: str, 
              location: str, 
              path: str, 
              abs_path_workspace_folder: str, 
              username: str,
              password: str,
              download_limit: int = 10485760,
              internal_use = False,
              parent_hash: str = None) -> dict | tuple[dict | None, str]:
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
        internal_use (bool): Determines if returned object is a dict or a tuple of (dict, username)
    Returns:
        dict | tuple[dict, str]: A dict of data/db info or a tuple of (data/db information, username). Second case if internal_use = True
    """

    cleaned_location_type = location_type.strip().lower()
    filename = get_last_part(path)

    # Create folder for data
    abs_path_workspace_folder = str(Path(abs_path_workspace_folder).resolve())
    if parent_hash:
        folder_hash, abs_path_db_folder = create_hashed_folder_from_path(parent_hash, abs_path_workspace_folder)
    else:
        folder_hash, abs_path_db_folder = create_hashed_folder_from_path(path, abs_path_workspace_folder)

    # Get the absolute path to the file to be downloaded
    file_path = Path(abs_path_db_folder) / filename
    
    # remove extra spaces
    path = path.strip()


    # Compute the MD5 hash of the existing file if it exists
    md5_file_hash = ""
    if file_path.exists():
        md5_file_hash = compute_md5(str(file_path))

    print(f"\n\n - Processing database at {location_type}:{location}:{path}")


    if cleaned_location_type == "github":
        
        # Check if the file exists and get its size
        filesize = 0
        try:
            filesize = get_github_remote_file_size(path)
        except Exception:
            print(f" -- Could not access the file at {path}. Skipping this database.")
            return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None

        # Confirm for sizes above a limit
        if not confirm_large_download(filesize, download_limit):
            print(" -- Skipping this database.")
            return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None

        # Download the file
        try:
            download_github_file(url=path, out_path=abs_path_db_folder)

            db_info = make_db_info(location, path, folder_hash, file_path, filename)
            return (db_info | {"new_db_folder": abs_path_db_folder}, username) if internal_use else db_info
            
        except Exception as e:
            print(f" -- Error downloading file from GitHub: {e}. Skipping this database.")
            return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None
        

    elif cleaned_location_type == "hpc-kerberos":
        if username == "":
            try:
                username = input(f" -- Enter the username for {location}: ")
            except KeyboardInterrupt:
                print(f"\n -- Interrupted while entering username for {location}. Skipping this database.")
                return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None
        
        # can now download data from HPC-kerberos
        if path.endswith("/"):
            try:
                abs_path_workspace_folder = abs_path_workspace_folder if abs_path_workspace_folder.endswith("/") else abs_path_workspace_folder + "/"
                subprocess.run(["rsync", "-av", f"{username}@{location}:{path}", abs_path_workspace_folder], check=True)
                print(f"\n - Downloaded all data to {abs_path_workspace_folder}")
                return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None
            except KeyboardInterrupt:
                print(f" -- Interrupted while downloading data from {location}:{path}")
                return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None
            except Exception as e:
                print(f" -- Error {e} downloading data from HPC.")
                return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None
        
        # Check if the file exists and get its size
        filesize = 0
        try:
            filesize = ssh_k_remote_size_bytes(
                user=username,
                host=location,
                remote_path=path
            )
        except KeyboardInterrupt:
            print(f" -- Interrupted while checking {location}:{path}. Skipping this database.")
            return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None
        except FileNotFoundError as e:
            print(f" -- Could not access the file at {location}:{path}; error: {e}. Skipping this database.")
            return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None
        except Exception as e:
            print(f" -- Could not access the file at {location}:{path}; error: {e}. Skipping this database.")
            return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None


        # Skip for now
        # # Check if the file already exists and has the same hash as the remote file
        # if md5_file_hash != "":

        #     need_redownload = True
        #     try:
        #         need_redownload = should_download(
        #             remote=f"{username}@{location}",
        #             remote_path=path,
        #             stored_md5=md5_file_hash
        #         )
        #     except KeyboardInterrupt:
        #         print(f" -- Interrupted while checking {location}:{path}. Skipping this database.")
        #         return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None
        #     except Exception as e:
        #         print(f" -- Failed to get remote hash for {location}:{path}: {e}")
        #         print(" -- Will proceed to download the file to ensure we have the correct version.")
        #     if not need_redownload:
        #         print(" -- Local file is up to date with the remote file. Skipping download.")
        #         db_info = make_db_info(location, path, folder_hash, file_path, filename)
        #         return (db_info | {"new_db_folder": abs_path_db_folder}, username) if internal_use else db_info

        # Confirm for sizes above a limit
        if not confirm_large_download(filesize, download_limit):
            print(" -- Skipping this database.")
            return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None

        # Download the file
        try:
            scp_k_copy_from(
                user=username,
                host=location,
                remote_path=path,
                local_path=abs_path_db_folder
            )

            db_info = make_db_info(location, path, folder_hash, file_path, filename)
            return (db_info | {"new_db_folder": abs_path_db_folder}, username) if internal_use else db_info

        except KeyboardInterrupt:
            print(f" -- Interrupted while checking {location}:{path}. Skipping this database.")
            return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None
        except Exception as e:
            print(f" -- Error {e} downloading file from HPC. Skipping this database.")
            return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None


    elif cleaned_location_type == "hpc":

        # Ask for username if we don't have it for this host yet
        if username == "":
            try:
                username = input(f" -- Enter the username for {location}: ")
            except KeyboardInterrupt:
                print(f"\n -- Interrupted while entering username for {location}. Skipping this database.")
                return None

        # Check if the file already exists and has the same hash
        if md5_file_hash != "":
            print(" -- Local file exists. Skipping MD5 verification (not implemented with new functions).")
            print(" -- File will be re-downloaded to ensure it's up to date.")

        # Get file size and download in one connection (TOTP passwords can only be used once!)
        try:
            filesize, success = get_file_size_and_download(
                hostname=location,
                username=username,
                password=password,
                remote_path=path,
                local_folder=abs_path_db_folder
            )
            
            if not success or filesize is None:
                print(f" -- Could not access or download the file at {location}:{path}. Skipping this database.")
                return None
            
            # Note: We get the size but don't check download_limit until after download
            # because TOTP can't be reused. If you want to check first, you'll need
            # to prompt for a new TOTP code.
            if filesize > download_limit:
                print(f" -- Downloaded file is {filesize} bytes (above {download_limit} byte limit)")
                print(" -- Note: File was already downloaded due to one-time password limitation")
                
        except KeyboardInterrupt:
            print(f" -- Interrupted while accessing {location}:{path}. Skipping this database.")
            return None
        except Exception as e:
            print(f" -- Could not access the file at {location}:{path}; error: {e}. Skipping this database.")
            return None

        db_info = make_db_info(location, path, folder_hash, file_path, filename)
        return db_info


    elif cleaned_location_type == "url":

        filesize = 0
        try:
            filesize = get_url_file_size(path)
        except Exception:
            print(f" -- Could not access the file at {path}. Skipping this database.")
            return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None

        # Confirm for sizes above a limit
        if not confirm_large_download(filesize, download_limit):
            print(" -- Skipping this database.")
            return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None

        # Download the file
        try:
            download_web_file(url=path, output_dir=abs_path_db_folder)

            db_info = make_db_info(location, path, folder_hash, file_path, filename)
            return (db_info | {"new_db_folder": abs_path_db_folder}, username) if internal_use else db_info

        except Exception as e:
            print(f" -- Error {e} downloading file at {path}. Skipping this database.")
            return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None
    

    elif cleaned_location_type == "s3":
        try:
            bucket, key = resolve_s3_bucket_and_key(location=location, path=path)
        except ValueError as e:
            print(f" -- Invalid S3 location/path: {e}. Skipping this database.")
            return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None

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
            return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None

        try:
            filesize = get_s3_remote_file_size(bucket=bucket, key=key, s3_client=s3_client)
        except PermissionError as e:
            print(f" -- Permission error: {e}. Skipping this database.")
            return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None
        except FileNotFoundError as e:
            print(f" -- Could not access S3 object s3://{bucket}/{key}; error: {e}. Skipping this database.")
            return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None
        except Exception as e:
            print(f" -- Could not access S3 object s3://{bucket}/{key}; error: {e}. Skipping this database.")
            return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None

        if md5_file_hash != "":
            try:
                need_redownload = should_download_s3(
                    bucket=bucket,
                    key=key,
                    stored_md5=md5_file_hash,
                    s3_client=s3_client,
                )
                if not need_redownload:
                    print(" -- Local file is up to date with the S3 object. Skipping download.")
                    return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None
            except Exception as e:
                print(f" -- Failed to compare local file with S3 object s3://{bucket}/{key}: {e}")
                print(" -- Will proceed to download the file to ensure we have the correct version.")

        if not confirm_large_download(filesize, download_limit):
            print(" -- Skipping this database.")
            return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None

        try:
            downloaded_path = download_s3_file(
                bucket=bucket,
                key=key,
                output_dir=abs_path_db_folder,
                s3_client=s3_client,
            )
            db_info = make_db_info(
                f"s3://{bucket}",
                f"s3://{bucket}/{key}",
                folder_hash,
                downloaded_path,
                Path(downloaded_path).name,
            )
            return (db_info | {"new_db_folder": abs_path_db_folder}, username) if internal_use else db_info
        except Exception as e:
            print(f" -- Error downloading file from S3: {e}. Skipping this database.")
            return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None


    elif cleaned_location_type == "local":
        # Check if the file exists
        if not Path(path).exists():
            print(f" -- Local file {path} does not exist. Skipping this database.")
            return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None

        # Check if it's a file
        if not Path(path).is_file():
            print(f" -- Local path {path} is not a file. Skipping this database.")
            return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None


        _abs_path = str(Path(path).resolve())

        if md5_file_hash != "":

            need_redownload = True
            try:
                need_redownload = should_download(remote="", remote_path=_abs_path, stored_md5=md5_file_hash)
            except KeyboardInterrupt:
                print(f" -- Interrupted while checking {_abs_path}. Skipping this database.")
                return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None
            except Exception as e:
                print(f" -- Failed to get hash for {_abs_path}: {e}")
                print(" -- Will proceed to download the file to ensure we have the correct version.")
            if not need_redownload:
                print(f" -- File in workspace is up to date with the file in {_abs_path}. Skipping download.")
                db_info = make_db_info(location, _abs_path, folder_hash, file_path, filename)
                return (db_info | {"new_db_folder": abs_path_db_folder}, username) if internal_use else db_info
        
        try:
            shutil.copy2(_abs_path, file_path)
            print(f"Downloaded to {file_path}")
        except Exception as e:
            print(f" -- Error copying this file from local: {e}. Skipping this database.")
            return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None

        db_info = make_db_info(location, _abs_path, folder_hash, file_path, filename)
        return (db_info | {"new_db_folder": abs_path_db_folder}, username) if internal_use else db_info


    else:
        print(f"Location type {location_type} for database {path} is unsupported. Skipping.")

    return ({"new_db_folder": abs_path_db_folder}, "") if internal_use else None

    
def get_data_endpoints(default_endpoints_prefix=['DSI_ENDPOINT_', 'DIANA_ENDPOINT_']):
    endpoints = {
        key: value 
        for key, value in os.environ.items() 
        if key.startswith(default_endpoints_prefix)
    }

    print(endpoints)

    return endpoints      



def federate_datasets(workspace_folder: str, config_data: dict, base_path: str) -> list[dict[str, str]]:
    """Federates datasets from various sources (local, GitHub, HPC, URL) based on the provided configuration.
      It checks for existing files, compares them with remote versions using MD5 checksums, and downloads or skips files accordingly.
      The function also handles user interactions for confirming downloads of large files and manages host usernames for HPC access.

    Args:
        workspace_folder (str): The local folder where the datasets will be stored.
        config_data (dict): A dictionary containing configuration data, including repository paths and download limits.
        base_path (str): The path used for resolving relative paths to the data.
    Returns:
        list[dict[str, str]]: A list of dictionaries of downloaded databases and associated metadata, or an empty list if no databases downloaded.
    """

    # Create the workspace folder if it doesn't exist
    abs_path_workspace_folder = str(Path(workspace_folder).resolve()) 
    create_directory(abs_path_workspace_folder)  
    print(f"Databases will be synchronized to: {abs_path_workspace_folder}")


    # Get the list of repos
    db_catalogue_list = []

    for repo in config_data.get("repo_paths", []):
        if Path(repo).is_absolute():
            repo_path = Path(repo)
        else:
            repo_path = Path(base_path) / repo

        clean_repo_path = str(repo_path.resolve())

        if clean_repo_path.endswith(".csv"):
            try:
                _temp_catalogues = csv_to_list_of_dicts(clean_repo_path)
                db_catalogue_list.extend(_temp_catalogues)
            except Exception as e:
                print(f"Error reading local repository {clean_repo_path}: {e}")
        else:
            print(f"Unsupported repository type for {clean_repo_path}. Only CSV files are supported for local repositories. Skipping this repo.")

    
    # Remove duplicates while keeping the latest entry for each unique path
        # TODO: Allow the user to choose which one to keep instead of just keeping the 
        # latest one or specify a resolution mode in the yaml file or allow user to keep both and rename them or ...
    cleaned_db_catalogue_list = deduplicate_keep_latest(db_catalogue_list)
    print("Number of repos found: ", len(cleaned_db_catalogue_list))


    # Create/open the list of hostnames and usernames
    try:
        with open(f"{abs_path_workspace_folder}/host_usernames.json", "r", encoding="utf-8") as f:
            host_username = yaml.safe_load(f)
    except Exception:
        host_username = {}


    # information about each database
    database_info = []
    federation_dbs = []

    
    # Gather the databases and create the index database
    success_counter = 0
    for db in cleaned_db_catalogue_list:

        db_info, actual_username = pull_data(
            location_type=db['location_type'],
            location=db['location'],
            path=db['path'],
            abs_path_workspace_folder=abs_path_workspace_folder,
            username=(host_username or {}).get(db['location'], ""),
            download_limit=config_data["download_limit"],
            internal_use=True
        )

        new_folder = Path(db_info.pop("new_db_folder"))
        if new_folder.is_dir() and not any(new_folder.iterdir()):
            new_folder.rmdir()

        if db_info:
            database_info.append(db_info)
            combined = {k: db[k] for k in ["location_type", "location", "submitter_name"]} | {k: db_info[k] for k in ["local_path", "name", "folder_hash"]}
            combined["workspace_folder"] = abs_path_workspace_folder
            federation_dbs.append(combined)
            if actual_username != "":
                host_username[db['location']] = actual_username
            success_counter += 1

    # Save host_usernames to a file for future runs
    with open(f"{abs_path_workspace_folder}/host_usernames.json", "w", encoding="utf-8") as f:
            yaml.safe_dump(host_username, f)

    # Save databases information to a JSON file
    upsert_records(f"{abs_path_workspace_folder}/dsi_database_list.json", database_info, key="original_path")


    print(f"\nFinished gathering databases. Successfully downloaded {success_counter} databases to {abs_path_workspace_folder}.")

    return federation_dbs


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
        password = getpass.getpass("Password: ")  # Hidden input!
    
        db_info = just_pull_data(location_type="hpc",
                      location=hpc_name,
                      path=endpoint_db_path,
                      abs_path_workspace_folder=temp_db_storage,
                      username=username)
                      #password=password)
        db_infos.append(db_info)
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
            username = input("Username: ")
            password = getpass.getpass("Password: ")  # Hidden input!
    
        db_info = pull_data(location_type=row['location_type'],
                  location=row['location'],
                  path=row['path'],
                  abs_path_workspace_folder=workspace_folder,
                  username=username,
                  password=password,
                  internal_use=False)
        
        if db_info:
            database_info.append(db_info)
            combined = {k: row[k] for k in ["location_type", "location", "submitter_name"]} | {k: db_info[k] for k in ["local_path", "name", "folder_hash"]}
            combined["workspace_folder"] = workspace_folder
            federation_dbs.append(combined)
            success_counter += 1

    # Save databases information to a JSON file
    upsert_records(f"{workspace_folder}/dsi_database_list.json", database_info, key="original_path")

    return database_info, success_counter




def get_remote_endpoints_ssh(hostname: str, 
                             username: str,
                            script_path: str = '/users/pascalgrosset/dsi_test/load_dsi_endpoints.sh',
                            prefixes: List[str] = ['DSI_ENDPOINT_', 'DIANA_ENDPOINT_']) -> dict:
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
        # Use system SSH with Kerberos authentication
        result = subprocess.run(
            ['ssh', f'{username}@{hostname}', command],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        stdout_text = result.stdout.strip()
        stderr_text = result.stderr.strip()
        exit_code = result.returncode
        
        if exit_code != 0:
            print(f"✗ Command failed with exit code {exit_code}")
            if stderr_text:
                print(f"Error: {stderr_text}")
            return {}
        
        print(f"✓ Sourced {script_path} and reading endpoints...")
        
        # Parse the JSON output
        endpoints = json.loads(stdout_text)
        
        print(f"✓ Found {len(endpoints)} endpoints:")
        for key, value in endpoints.items():
            print(f"  {key} = {value}")
        
        return endpoints
        
    except subprocess.TimeoutExpired:
        print(f"✗ Connection timed out after 30 seconds")
        return {}
    except json.JSONDecodeError as e:
        print(f"✗ Failed to parse output: {e}")
        print(f"Raw output: {stdout_text}")
        if stderr_text:
            print(f"Stderr: {stderr_text}")
        return {}
    except FileNotFoundError:
        print(f"✗ SSH command not found. Ensure SSH is installed.")
        return {}
    except Exception as e:
        print(f"!!!! !!!! !!! Error: {e}")
        return {}




def get_remote_endpoints(hostname: str, username: str, password: str, 
                         script_path: str = '/users/pascalgrosset/dsi_test/load_dsi_endpoints.sh',
                         prefixes: List[str] = ['DSI_ENDPOINT_', 'DIANA_ENDPOINT_']) -> dict:
    """ Source bash script on remote server and retrieve environment variables matching specified prefixes.
    
    Args:
        hostname: Remote server hostname or IP address.
        username: SSH username for authentication.
        password: SSH password for authentication.
        script_path: Path to bash script on remote server that sets endpoint variables.
                    Default: '/users/pascalgrosset/dsi_test/load_dsi_endpoints.sh'
        prefixes: List of environment variable prefixes to match (e.g., 'DSI_ENDPOINT_').
                 Default: ['DSI_ENDPOINT_', 'DIANA_ENDPOINT_']
    
    Returns:
        dict: Dictionary mapping endpoint variable names to their values.
              Returns empty dict if connection fails or no endpoints found."""
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
    
    print(f"Connecting to {hostname}...")
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(hostname, username=username, password=password, timeout=30)
        
        print(f"Sourcing {script_path} and reading endpoints...")
        stdin, stdout, stderr = ssh.exec_command(command)
        
        stdout_text = stdout.read().decode('utf-8').strip()
        stderr_text = stderr.read().decode('utf-8').strip()
        exit_code = stdout.channel.recv_exit_status()
        
        if exit_code != 0:
            print(f"✗ Command failed with exit code {exit_code}")
            if stderr_text:
                print(f"Error: {stderr_text}")
            return {}
        
        # Parse the JSON output
        endpoints = json.loads(stdout_text)
        
        print(f"✓ Found {len(endpoints)} endpoints:")
        for key, value in endpoints.items():
            print(f"  {key} = {value}")
        
        return endpoints
        
    except json.JSONDecodeError as e:
        print(f"✗ Failed to parse output: {e}")
        print(f"Raw output: {stdout_text}")
        return {}
    except Exception as e:
        print(f"!!!! !!!! !!! Error: {e}")
        return {}
    finally:
        try:
            ssh.close()
        except:
            pass



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
    create_folder(temp_db_storage, delete_if_exists=True)

    # download them
    pull_remote_db(hpc_name, endpoints_location, temp_db_storage)

    # combine them to output_csv and the dictionary csv_data_sources
    output_csv = "output_csv.csv"
    csv_data_sources = combine_csv(temp_db_storage, output_csv)

    # Pull the data from the CSV file, return a dictionary, and output the dictionaty to workspace_folder
    database_info = read_data_sources(csv_data_sources, workspace_folder)

    return database_info


def main():
    parser = argparse.ArgumentParser(description="Federate datasets from a YAML config or a CSV repo file.")
    input_group = parser.add_mutually_exclusive_group(required=True)

    input_group.add_argument(
        "--yaml",
        type=Path,
        help="YAML configuration file",
    )

    input_group.add_argument(
        "--csv",
        type=Path,
        help="CSV repository file to federate",
    )

    parser.add_argument(
        "dsi_datasets_folder",
        nargs="?",
        help="Optional workspace folder override",
    )

    args = parser.parse_args()

    
    # YAML input mode
    if args.yaml:
        yaml_path = args.yaml

        try:
            config_data = yaml.safe_load( yaml_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            print(f"Error: Could not find YAML file {yaml_path}")
            sys.exit(1)

        config_folder = yaml_path.parent


    # CSV input mode
    else:
        csv_path = args.csv

        if not csv_path.exists():
            print(f"Error: Could not find CSV file {csv_path}")
            sys.exit(1)

        config_data = {
            "repo_paths": [csv_path.name],
            "download_limit": 10485760,  # 10 MB
            "conflict_resolution": "keep_latest",
        }

        config_folder = csv_path.parent


    # Workspace folder
    if args.dsi_datasets_folder:
        workspace_folder = args.dsi_datasets_folder
    else:
        workspace_folder = (
            config_data.get("workspace_folder", "")
            or f"_dsi_datasets_folder_{uuid.uuid4().hex[:8]}"
        )


    print(f"workspace_folder: {workspace_folder}, config_data: {config_data}, config_folder: {config_folder}")
    federate_datasets(workspace_folder, config_data, str(config_folder))

if __name__=="__main__":
    main()


# Run as:
# python dsi/utils/federated/federate_datasets.py --yaml input.yaml
# python dsi/utils/federated/federate_datasets.py --csv repo_paths.csv
# python dsi/utils/federated/federate_datasets.py --csv repo_paths.csv dsi_databases_test_merge_01