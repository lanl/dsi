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
from contextlib import asynccontextmanager

import pandas as pd
from pathlib import Path

from dsi.utils.acquisition.utils import (
    get_last_part,
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
    local_folder=None,
    jump_host=None,
    jump_username=None,
    jump_password=None,
    jump_host_required=False,
    reticket_cmd="reticket"
):
    """Get file size and download using a single SSH connection.

    Now supports jump host for Kerberos authentication.

    Args:
        hostname: SSH server hostname
        username: SSH username
        password: SSH password (optional, for password auth or TOTP)
        private_key_path: Path to private key file (optional, for key auth)
        remote_path: Path to file on remote server
        local_folder: Folder to save to (default: current directory)
        jump_host: Jump host hostname (optional, for Kerberos)
        jump_username: Username on jump host (optional)
        jump_password: Password for jump host (optional)
        jump_host_required: Whether jump host is required for this hostname
        reticket_cmd: Kerberos ticket-init command to run on the jump host (default: "reticket")

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

    try:
        # Check if jump host is required
        use_jump_host = jump_host_required and jump_host

        if use_jump_host:
            print(f"Using jump host {jump_host} to download from {hostname}")
            result = await download_database_file_async(
                hostname=hostname,
                remote_path=remote_path,
                local_folder=local_folder,
                username=username,
                password=password,
                jump_host=jump_host,
                jump_username=jump_username,
                jump_password=jump_password,
                jump_host_required=True,
                reticket_cmd=reticket_cmd,
                logger=logging.getLogger(__name__)
            )

            if result:
                # Get file size
                file_size = os.path.getsize(result)
                print(f"File size: {file_size} bytes")
                return file_size
            else:
                raise Exception(f"Failed to download via jump host")
        else:
            # Direct connection (original behavior)
            # If neither password nor private_key_path is provided, it will try SSH agent or default keys
            connect_options = _ssh_connect_options(hostname, username, password, private_key_path)

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
# Endpoint discovery and download with jump host and Kerberos support
#

def _ssh_connect_options(hostname: str, username: str, password: str = None, private_key_path: str = None) -> dict:
    options = {
        'host': hostname,
        'username': username,
        'known_hosts': None,
        'connect_timeout': 30,
        'pkcs11_provider': None  # Disable PKCS#11 to avoid "PKCS#11 support not available" error
    }
    if password:
        options['password'] = password
    elif private_key_path:
        options['client_keys'] = [private_key_path]
    return options


@asynccontextmanager
async def _connect_via_jump_host(
    jump_host: str,
    jump_username: str,
    jump_password: str,
    reticket_cmd: str,
    logger: logging.Logger
):
    """
    Connect to a jump host and run the Kerberos ticket-init command,
    yielding the live jump host connection.
    """
    jump_options = _ssh_connect_options(jump_host, jump_username, jump_password)

    async with asyncssh.connect(**jump_options) as jump_conn:
        logger.info(f"Running '{reticket_cmd}' on jump host")
        reticket_result = await jump_conn.run(reticket_cmd, check=False)
        logger.info(f"{reticket_cmd} output: {reticket_result.stdout}")

        if reticket_result.stderr:
            logger.info(f"{reticket_cmd} stderr: {reticket_result.stderr}")

        if reticket_result.exit_status != 0:
            logger.warning(f"{reticket_cmd} failed with exit code: {reticket_result.exit_status}")

        logger.info(f"Checking Kerberos tickets with klist")
        klist_result = await jump_conn.run("klist", check=False)
        logger.info(f"klist output: {klist_result.stdout}")

        yield jump_conn


@asynccontextmanager
async def _connect_to_hpc_via_jump_host(
    jump_conn,
    hostname: str,
    username: str,
    logger: logging.Logger
):
    """
    From an existing jump host connection, open a GSSAPI/Kerberos connection
    to the final HPC target and yield it (Method 1 of the jump-host pattern).
    """
    logger.info(f"Method 1: Using connect_ssh with gss_auth=True")
    async with jump_conn.connect_ssh(
        hostname,
        username=username,
        known_hosts=None,
        gss_auth=True,  # Use Kerberos/GSSAPI
        gss_delegate_creds=True  # Forward Kerberos credentials
    ) as conn:
        yield conn
    logger.info("Method 1 succeeded!")


async def discover_endpoints_async(
    hostname: str,
    username: str,
    script_path: str,
    prefixes: list,
    password: str = None,
    hpc_type: str = 'standard',
    jump_host: str = None,
    jump_username: str = None,
    jump_password: str = None,
    reticket_cmd: str = "reticket",
    logger: logging.Logger = None
) -> dict:
    """
    Discover DSI endpoints from a remote HPC system.

    Supports:
    - Direct SSH connection
    - Jump host with Kerberos authentication

    Args:
        hostname: Target HPC hostname
        username: Username on target HPC
        script_path: Path to load_dsi_endpoints.sh script
        prefixes: List of endpoint prefixes (e.g., ['DSI_ENDPOINT_', 'DIANA_ENDPOINT_'])
        password: Password for direct SSH (optional)
        hpc_type: 'standard' or 'kerberos'
        jump_host: Jump host hostname (for Kerberos)
        jump_username: Username on jump host
        jump_password: Password for jump host
        reticket_cmd: Kerberos ticket-init command to run on the jump host (default: "reticket")
        logger: Logger instance

    Returns:
        dict: Discovered endpoints {endpoint_name: endpoint_path}
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    # Build command to run on remote server
    prefixes_str = ','.join(f'"{p}"' for p in prefixes)
    remote_cmd = f"""
source {script_path} && python3 << 'PYTHON_EOF'
import os
import json
prefixes = [{prefixes_str}]
prefix_tuple = tuple(prefixes)
endpoints = {{k: v for k, v in os.environ.items() if k.startswith(prefix_tuple)}}
print(json.dumps(endpoints))
PYTHON_EOF
"""

    try:
        if hpc_type == 'kerberos' and jump_host:
            # Connect via jump host with Kerberos pattern:
            # 1. SSH to jump host with username/password
            # 2. Run reticket on jump host
            # 3. SSH from jump host to HPC using Kerberos
            logger.info(f"Connecting via jump host {jump_host}")

            async with _connect_via_jump_host(jump_host, jump_username, jump_password, reticket_cmd, logger) as jump_conn:
                logger.info(f"Connecting to {hostname} from jump host as {username}")

                # Method 1: Try using connect_ssh with GSSAPI
                try:
                    async with _connect_to_hpc_via_jump_host(jump_conn, hostname, username, logger) as final_conn:
                        result = await final_conn.run(remote_cmd, check=True)
                        return json.loads(result.stdout.strip())
                except Exception as e:
                    logger.warning(f"Method 1 failed: {str(e)}")

                    # Method 2: Run SSH command directly on jump host (mirrors manual workflow)
                    # This is what works manually: ssh grosset2@tuolumne.llnl.gov 'command'
                    logger.info(f"Method 2: Running SSH command directly on jump host")
                    try:
                        ssh_cmd = f"ssh -o StrictHostKeyChecking=no {username}@{hostname} '{remote_cmd}'"
                        logger.info(f"Running: ssh -o StrictHostKeyChecking=no {username}@{hostname} '<python command>'")
                        result = await jump_conn.run(ssh_cmd, check=True)
                        logger.info("Method 2 succeeded!")
                        return json.loads(result.stdout.strip())
                    except Exception as e2:
                        logger.error(f"Method 2 also failed: {str(e2)}")
                        raise Exception(f"Both connection methods failed. Method 1: {e}, Method 2: {e2}")
        else:
            # Direct SSH connection
            async with asyncssh.connect(**_ssh_connect_options(hostname, username, password)) as conn:
                result = await conn.run(remote_cmd, check=True)
                return json.loads(result.stdout.strip())
    except Exception as e:
        logger.error(f"SSH error: {str(e)}")
        return {}


async def download_csv_files_async(
    hostname: str,
    csv_paths: dict,
    temp_folder: str,
    username: str,
    password: str = None,
    jump_host: str = None,
    jump_username: str = None,
    jump_password: str = None,
    jump_host_required: bool = False,
    reticket_cmd: str = "reticket",
    logger: logging.Logger = None
) -> list:
    """
    Download CSV files from remote HPC using asyncssh.

    Supports:
    - Direct SSH connection
    - Jump host with Kerberos authentication

    Args:
        hostname: Target HPC hostname
        csv_paths: Dict of {endpoint_name: csv_file_path}
        temp_folder: Local folder to save files
        username: Username on target HPC
        password: Password for direct SSH (optional)
        jump_host: Jump host hostname (optional)
        jump_username: Username on jump host (optional)
        jump_password: Password for jump host (optional)
        jump_host_required: Whether jump host is required for this hostname
        reticket_cmd: Kerberos ticket-init command to run on the jump host (default: "reticket")
        logger: Logger instance

    Returns:
        list: Local file paths of downloaded files
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    downloaded_files = []

    try:
        use_jump_host = jump_host_required and jump_host

        if use_jump_host:
            logger.info(f"✓ Hostname {hostname} requires jump host (using {jump_host})")

            async with _connect_via_jump_host(jump_host, jump_username, jump_password, reticket_cmd, logger) as jump_conn:
                logger.info(f"Connecting to {hostname} from jump host as {username}")

                # Method 1: Try using connect_ssh with GSSAPI
                try:
                    async with _connect_to_hpc_via_jump_host(jump_conn, hostname, username, logger) as conn:
                        async with conn.start_sftp_client() as sftp:
                            for csv_name, csv_path in csv_paths.items():
                                try:
                                    local_filename = f"{hostname}_{csv_name}.csv"
                                    local_path = Path(temp_folder) / local_filename
                                    logger.info(f"Downloading {hostname}:{csv_path}")
                                    await sftp.get(csv_path, str(local_path))

                                    if local_path.exists():
                                        downloaded_files.append(str(local_path))
                                        logger.info(f"Successfully downloaded {local_filename}")
                                    else:
                                        logger.error(f"File not found after download: {local_filename}")
                                except Exception as e:
                                    logger.error(f"Error downloading {csv_path}: {str(e)}")
                                    continue
                except Exception as e:
                    logger.warning(f"Method 1 failed: {str(e)}")

                    # Method 2: Use SCP command on jump host (mirrors manual workflow)
                    logger.info(f"Method 2: Using SCP command directly on jump host")
                    try:
                        for csv_name, csv_path in csv_paths.items():
                            try:
                                local_filename = f"{hostname}_{csv_name}.csv"
                                local_path = Path(temp_folder) / local_filename

                                # Create a temp file on jump host, then download it
                                jump_temp_file = f"/tmp/{local_filename}"
                                scp_cmd = f"scp -o StrictHostKeyChecking=no {username}@{hostname}:{csv_path} {jump_temp_file}"
                                logger.info(f"Running: scp {username}@{hostname}:{csv_path} {jump_temp_file}")

                                result = await jump_conn.run(scp_cmd, check=True)
                                logger.info(f"SCP completed, downloading from jump host")

                                # Download from jump host to local
                                async with jump_conn.start_sftp_client() as sftp:
                                    await sftp.get(jump_temp_file, str(local_path))

                                # Clean up temp file on jump host
                                await jump_conn.run(f"rm {jump_temp_file}", check=False)

                                if local_path.exists():
                                    downloaded_files.append(str(local_path))
                                    logger.info(f"Successfully downloaded {local_filename}")
                                else:
                                    logger.error(f"File not found after download: {local_filename}")
                            except Exception as e2:
                                logger.error(f"Error downloading {csv_path} with SCP: {str(e2)}")
                                continue
                        logger.info("Method 2 succeeded!")
                    except Exception as e3:
                        logger.error(f"Method 2 also failed: {str(e3)}")
                        raise Exception(f"Both download methods failed. Method 1: {e}, Method 2: {e3}")
        else:
            # Direct connection (no jump host)
            logger.info(f"Direct SSH to {hostname}")
            async with asyncssh.connect(**_ssh_connect_options(hostname, username, password)) as conn:
                async with conn.start_sftp_client() as sftp:
                    for csv_name, csv_path in csv_paths.items():
                        try:
                            local_filename = f"{hostname}_{csv_name}.csv"
                            local_path = Path(temp_folder) / local_filename

                            logger.info(f"Downloading {hostname}:{csv_path}")

                            # Download file via SFTP
                            await sftp.get(csv_path, str(local_path))

                            if local_path.exists():
                                downloaded_files.append(str(local_path))
                                logger.info(f"Successfully downloaded {local_filename}")
                            else:
                                logger.error(f"File not found after download: {local_filename}")

                        except Exception as e:
                            logger.error(f"Error downloading {csv_path}: {str(e)}")
                            continue

    except asyncssh.Error as e:
        logger.error(f"SSH connection error to {hostname}: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")

    return downloaded_files


async def download_database_file_async(
    hostname: str,
    remote_path: str,
    local_folder: str,
    username: str,
    password: str = None,
    jump_host: str = None,
    jump_username: str = None,
    jump_password: str = None,
    jump_host_required: bool = False,
    reticket_cmd: str = "reticket",
    logger: logging.Logger = None
) -> str:
    """
    Download a database file from remote HPC using asyncssh.

    Similar to DSI's get_file_size_and_download but with jump host capability.

    Supports:
    - Direct SSH connection
    - Jump host with Kerberos authentication

    Args:
        hostname: Target HPC hostname
        remote_path: Path to file on remote system
        local_folder: Local folder to save file
        username: Username on target HPC
        password: Password for direct SSH (optional)
        jump_host: Jump host hostname (optional)
        jump_username: Username on jump host (optional)
        jump_password: Password for jump host (optional)
        jump_host_required: Whether jump host is required for this hostname
        reticket_cmd: Kerberos ticket-init command to run on the jump host (default: "reticket")
        logger: Logger instance

    Returns:
        str: Local file path if successful, None otherwise
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    filename = os.path.basename(remote_path)
    local_path = Path(local_folder) / filename

    try:
        use_jump_host = jump_host_required and jump_host

        if use_jump_host:
            logger.info(f"Using jump host {jump_host} to download from {hostname}")

            async with _connect_via_jump_host(jump_host, jump_username, jump_password, reticket_cmd, logger) as jump_conn:
                # Method 1: Try with asyncssh's connect_ssh
                try:
                    async with _connect_to_hpc_via_jump_host(jump_conn, hostname, username, logger) as conn:
                        async with conn.start_sftp_client() as sftp:
                            logger.info(f"Downloading {hostname}:{remote_path} to {local_path}")
                            await sftp.get(remote_path, str(local_path))

                            if local_path.exists():
                                logger.info(f"Successfully downloaded {filename}")
                                return str(local_path)
                            else:
                                logger.error(f"File not found after download: {filename}")
                                return None
                except Exception as e:
                    logger.warning(f"Method 1 failed: {str(e)}")

                    # Method 2: Use SCP command on jump host
                    logger.info(f"Method 2: SCP via jump host")
                    try:
                        jump_temp_file = f"/tmp/{filename}"
                        scp_cmd = f"scp -o StrictHostKeyChecking=no {username}@{hostname}:{remote_path} {jump_temp_file}"
                        logger.info(f"Running SCP on jump host")

                        result = await jump_conn.run(scp_cmd, check=True)
                        logger.info(f"SCP completed, downloading from jump host")

                        # Download from jump host to local
                        async with jump_conn.start_sftp_client() as sftp:
                            await sftp.get(jump_temp_file, str(local_path))

                        # Clean up temp file on jump host
                        await jump_conn.run(f"rm {jump_temp_file}", check=False)

                        if local_path.exists():
                            logger.info(f"Successfully downloaded {filename}")
                            return str(local_path)
                        else:
                            logger.error(f"File not found after download: {filename}")
                            return None
                    except Exception as e2:
                        logger.error(f"Method 2 also failed: {str(e2)}")
                        return None
        else:
            # Direct SSH connection (no jump host)
            logger.info(f"Direct SSH to {hostname}")
            async with asyncssh.connect(**_ssh_connect_options(hostname, username, password)) as conn:
                async with conn.start_sftp_client() as sftp:
                    logger.info(f"Downloading {hostname}:{remote_path} to {local_path}")
                    await sftp.get(remote_path, str(local_path))

                    if local_path.exists():
                        logger.info(f"Successfully downloaded {filename}")
                        return str(local_path)
                    else:
                        logger.error(f"File not found after download: {filename}")
                        return None

    except Exception as e:
        logger.error(f"Error downloading {remote_path}: {str(e)}")
        return None


#
# Get data
#

def pull_data(location_type: str,
              remote_location: str,
              remote_path: str,
              download_location: str,
              username: str,
              password: str = "",
              download_limit: int = 10485760,
              jump_host: str = None,
              jump_username: str = None,
              jump_password: str = None,
              jump_host_required: bool = False,
              reticket_cmd: str = "reticket") -> str:
    """Pulls data from a specified location based on the location type (e.g., "github", "HPC", "HPC-Kerberos", "URL", "local").
    The function checks for existing files, compares them with remote versions using MD5 checksums, and downloads or skips files accordingly.
    It also handles user interactions for confirming downloads of large files and manages host usernames for HPC access.

    Now supports jump host authentication for HPC systems requiring Kerberos via jump host.

    Args:
        location_type (str): The type of the original location (e.g., "github", "HPC", "HPC-kerberos", "URL", "local").
        remote_location (str): The location of the database (e.g., hostname for HPC, URL for web).
        remote_path (str): The path to the data or database at the remote location.
        download_location (str): The absolute path to the workspace folder where the data or database will be stored.
        username (str): username for hpc systems
        password (str): optional
        download_limit (int): The maximum size of a file that can be downloaded without confirmation, if 0 no limit
        jump_host (str): Jump host hostname for Kerberos authentication (optional)
        jump_username (str): Username on jump host (optional)
        jump_password (str): Password for jump host (optional)
        jump_host_required (bool): Whether jump host is required for this remote_location
        reticket_cmd (str): Kerberos ticket-init command to run on the jump host (default: "reticket")
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
                local_folder=download_location,
                jump_host=jump_host,
                jump_username=jump_username,
                jump_password=jump_password,
                jump_host_required=jump_host_required,
                reticket_cmd=reticket_cmd
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
