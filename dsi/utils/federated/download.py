"""
File download utilities with jump host and Kerberos support
"""

import os
import asyncio
import asyncssh
import logging
from pathlib import Path


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

            # Connect to jump host
            jump_options = {
                'host': jump_host,
                'username': jump_username,
                'known_hosts': None,
                'connect_timeout': 30,
            }
            if jump_password:
                jump_options['password'] = jump_password

            async with asyncssh.connect(**jump_options) as jump_conn:
                # Run the Kerberos ticket-init command on jump host to get Kerberos tickets
                logger.info(f"Running '{reticket_cmd}' on jump host")
                reticket_result = await jump_conn.run(reticket_cmd, check=False)
                logger.info(f"{reticket_cmd} output: {reticket_result.stdout}")

                if reticket_result.stderr:
                    logger.info(f"{reticket_cmd} stderr: {reticket_result.stderr}")

                if reticket_result.exit_status != 0:
                    logger.warning(f"{reticket_cmd} failed with exit code: {reticket_result.exit_status}")

                # Check Kerberos tickets
                logger.info(f"Checking Kerberos tickets with klist")
                klist_result = await jump_conn.run("klist", check=False)
                logger.info(f"klist output: {klist_result.stdout}")

                # Connect to final host from jump host using Kerberos
                logger.info(f"Connecting to {hostname} from jump host as {username}")

                # Method 1: Try using connect_ssh with GSSAPI
                download_success = False
                try:
                    logger.info(f"Method 1: Using connect_ssh with gss_auth=True")
                    async with jump_conn.connect_ssh(
                        hostname,
                        username=username,
                        known_hosts=None,
                        gss_auth=True,
                        gss_delegate_creds=True  # Forward Kerberos credentials
                    ) as conn:
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
                    download_success = True
                    logger.info("Method 1 succeeded!")
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
                        download_success = True
                        logger.info("Method 2 succeeded!")
                    except Exception as e3:
                        logger.error(f"Method 2 also failed: {str(e3)}")
                        raise Exception(f"Both download methods failed. Method 1: {e}, Method 2: {e3}")
        else:
            # Direct connection (no jump host)
            logger.info(f"Direct SSH to {hostname}")
            connect_options = {
                'host': hostname,
                'username': username,
                'known_hosts': None,
                'connect_timeout': 30,
            }
            if password:
                connect_options['password'] = password

            async with asyncssh.connect(**connect_options) as conn:
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

            # Connect to jump host
            jump_options = {
                'host': jump_host,
                'username': jump_username,
                'known_hosts': None,
                'connect_timeout': 30,
            }
            if jump_password:
                jump_options['password'] = jump_password

            async with asyncssh.connect(**jump_options) as jump_conn:
                # Run the Kerberos ticket-init command
                logger.info(f"Running '{reticket_cmd}' on jump host")
                await jump_conn.run(reticket_cmd, check=False)

                # Method 1: Try with asyncssh's connect_ssh
                try:
                    logger.info(f"Method 1: Direct SFTP via jump host")
                    async with jump_conn.connect_ssh(
                        hostname,
                        username=username,
                        known_hosts=None,
                        gss_auth=True,
                        gss_delegate_creds=True
                    ) as conn:
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
            connect_options = {
                'host': hostname,
                'username': username,
                'known_hosts': None,
                'connect_timeout': 30,
            }
            if password:
                connect_options['password'] = password

            async with asyncssh.connect(**connect_options) as conn:
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
