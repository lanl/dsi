"""
Endpoint discovery with jump host and Kerberos support
"""

import asyncio
import asyncssh
import json
import logging


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

            # Connect to jump host first
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

                # Try to connect to final HPC from jump host using Kerberos
                logger.info(f"Connecting to {hostname} from jump host as {username}")

                # Method 1: Try using connect_ssh with GSSAPI
                try:
                    logger.info(f"Method 1: Using connect_ssh with gss_auth=True")
                    async with jump_conn.connect_ssh(
                        hostname,
                        username=username,
                        known_hosts=None,
                        gss_auth=True,  # Use Kerberos/GSSAPI
                        gss_delegate_creds=True  # Forward Kerberos credentials
                    ) as final_conn:
                        result = await final_conn.run(remote_cmd, check=True)
                        logger.info("Method 1 succeeded!")
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
            connect_options = {
                'host': hostname,
                'username': username,
                'known_hosts': None,
                'connect_timeout': 30,
            }
            if password:
                connect_options['password'] = password

            async with asyncssh.connect(**connect_options) as conn:
                result = await conn.run(remote_cmd, check=True)
                return json.loads(result.stdout.strip())
    except Exception as e:
        logger.error(f"SSH error: {str(e)}")
        return {}
