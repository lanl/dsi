"""
Flask web UI for DSI Data Federation Tool - Interactive Credential Version
Provides a user-friendly interface with UI-based credential collection
"""

import sys
import logging
from pathlib import Path
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_from_directory
import json
import os
import shutil
import asyncio
import asyncssh

# Add parent directory to path to import dsi modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dsi.utils.data_acquisition import (
    get_remote_endpoints_ssh,
    pull_remote_db,
    pull_data,
)
from dsi.utils.acquisition.utils import (
    create_directory,
    combine_csv,
    create_hashed_folder_from_path,
    split_path,
    upsert_records,
)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'dev-secret-key-change-in-production'

# Configure logging
LOG_DIR = Path(__file__).parent / 'logs'
LOG_DIR.mkdir(exist_ok=True)

# Session storage for credentials (in-memory)
session_credentials = {}

def setup_logging(session_id):
    """Setup logging for a specific session"""
    log_filename = LOG_DIR / f"federate_{session_id}.log"

    # Create a logger for this session
    session_logger = logging.getLogger(f"session_{session_id}")
    session_logger.setLevel(logging.DEBUG)

    # Clear existing handlers
    session_logger.handlers = []

    # File handler
    fh = logging.FileHandler(log_filename)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    session_logger.addHandler(fh)

    return session_logger, log_filename


@app.route('/')
def index():
    """Main page"""
    return render_template('index_inter.html')


@app.route('/api/discover-endpoints', methods=['POST'])
def discover_endpoints():
    """
    Discover endpoints from one or more remote HPCs
    """
    try:
        data = request.json
        session_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        logger, log_file = setup_logging(session_id)

        hpc_type = data.get('hpc_type')
        hpc_names = data.get('hpc_names', [])  # Now accepts a list
        username = data.get('username')
        password = data.get('password')
        script_path = data.get('script_path')
        prefixes = data.get('prefixes', ['DSI_ENDPOINT_', 'DIANA_ENDPOINT_'])

        # Jump host configuration (for Kerberos)
        jump_host = data.get('jump_host')
        jump_username = data.get('jump_username')
        jump_password = data.get('jump_password')

        # Store credentials for this session
        session_credentials[session_id] = {
            'username': username,
            'password': password,
            'hpc_type': hpc_type,
            'jump_host': jump_host,
            'jump_username': jump_username,
            'jump_password': jump_password,
            'jump_host_required': {}  # Maps hostname -> True if jump host needed
        }

        logger.info(f"Starting endpoint discovery for {len(hpc_names)} cluster(s)")
        logger.info(f"Clusters: {hpc_names}")
        logger.info(f"Script path: {script_path}")
        logger.info(f"Prefixes: {prefixes}")
        logger.info(f"Using password auth: {bool(password)}")
        logger.info(f"HPC Type: {hpc_type}")
        if hpc_type == 'kerberos':
            logger.info(f"Jump host: {jump_host}, Jump username: {jump_username}")

        # Aggregate endpoints from all clusters
        all_endpoints = {}
        cluster_results = {}
        failed_clusters = []

        for hpc_name in hpc_names:
            try:
                logger.info(f"Discovering endpoints from {hpc_name}")

                # Use asyncssh for discovery
                async def discover_with_asyncssh():
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
                                # Run reticket on jump host to get Kerberos tickets
                                logger.info(f"Running reticket on jump host")
                                reticket_cmd = "reticket"
                                reticket_result = await jump_conn.run(reticket_cmd, check=False)
                                logger.info(f"reticket output: {reticket_result.stdout}")

                                if reticket_result.stderr:
                                    logger.info(f"reticket stderr: {reticket_result.stderr}")

                                if reticket_result.exit_status != 0:
                                    logger.warning(f"reticket failed with exit code: {reticket_result.exit_status}")

                                # Check Kerberos tickets
                                logger.info(f"Checking Kerberos tickets with klist")
                                klist_result = await jump_conn.run("klist", check=False)
                                logger.info(f"klist output: {klist_result.stdout}")

                                # Try to connect to final HPC from jump host using Kerberos
                                logger.info(f"Connecting to {hpc_name} from jump host as {username}")

                                # Method 1: Try using connect_ssh with GSSAPI
                                try:
                                    logger.info(f"Method 1: Using connect_ssh with gss_auth=True")
                                    async with jump_conn.connect_ssh(
                                        hpc_name,
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
                                        ssh_cmd = f"ssh -o StrictHostKeyChecking=no {username}@{hpc_name} '{remote_cmd}'"
                                        logger.info(f"Running: ssh -o StrictHostKeyChecking=no {username}@{hpc_name} '<python command>'")
                                        result = await jump_conn.run(ssh_cmd, check=True)
                                        logger.info("Method 2 succeeded!")
                                        return json.loads(result.stdout.strip())
                                    except Exception as e2:
                                        logger.error(f"Method 2 also failed: {str(e2)}")
                                        raise Exception(f"Both connection methods failed. Method 1: {e}, Method 2: {e2}")
                        else:
                            # Direct SSH connection
                            connect_options = {
                                'host': hpc_name,
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

                endpoints_location = asyncio.run(discover_with_asyncssh())

                if endpoints_location:
                    # Mark this hostname as requiring jump host if we used one
                    if hpc_type == 'kerberos' and jump_host:
                        session_credentials[session_id]['jump_host_required'][hpc_name] = True
                        logger.info(f"Marked {hpc_name} as requiring jump host access")

                    # Prefix endpoint names with cluster name to avoid conflicts
                    for endpoint_name, endpoint_path in endpoints_location.items():
                        prefixed_name = f"{hpc_name}::{endpoint_name}"
                        all_endpoints[prefixed_name] = endpoint_path

                    cluster_results[hpc_name] = {
                        'success': True,
                        'count': len(endpoints_location),
                        'endpoints': endpoints_location
                    }
                    logger.info(f"Found {len(endpoints_location)} endpoint(s) from {hpc_name}")
                else:
                    cluster_results[hpc_name] = {
                        'success': False,
                        'message': f'No endpoints found at {script_path}'
                    }
                    logger.warning(f"No endpoints found on {hpc_name}")
                    failed_clusters.append(hpc_name)

            except Exception as e:
                logger.error(f"Error discovering endpoints from {hpc_name}: {str(e)}", exc_info=True)
                cluster_results[hpc_name] = {
                    'success': False,
                    'message': str(e)
                }
                failed_clusters.append(hpc_name)

        logger.info(f"Total endpoints discovered: {len(all_endpoints)}")

        if not all_endpoints:
            return jsonify({
                'success': False,
                'message': f'No endpoints found from any cluster. Failed clusters: {", ".join(failed_clusters)}',
                'cluster_results': cluster_results,
                'log_file': str(log_file)
            })

        return jsonify({
            'success': True,
            'endpoints': all_endpoints,
            'cluster_results': cluster_results,
            'total_clusters': len(hpc_names),
            'successful_clusters': len([r for r in cluster_results.values() if r['success']]),
            'failed_clusters': failed_clusters,
            'session_id': session_id,
            'log_file': str(log_file),
            'jump_host_map': session_credentials[session_id].get('jump_host_required', {}),
            'jump_host': jump_host,
            'jump_username': jump_username
        })

    except Exception as e:
        if 'logger' in locals():
            logger.error(f"Error discovering endpoints: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'message': f'Error: {str(e)}',
            'log_file': str(log_file) if 'log_file' in locals() else None
        }), 500


async def download_csv_files_asyncssh(hostname, csv_paths, temp_folder, username, password, logger, session_creds=None):
    """
    Download CSV files from remote HPC using asyncssh
    Returns list of local file paths
    Supports jump host if session_creds contains jump host info
    """
    downloaded_files = []

    try:
        # Check if we need to use jump host for THIS specific hostname
        jump_host_required_map = session_creds.get('jump_host_required', {}) if session_creds else {}

        # Debug logging
        logger.info(f"Download check for hostname: {hostname}")
        logger.info(f"Jump host required map: {jump_host_required_map}")
        logger.info(f"Jump host available: {session_creds.get('jump_host') if session_creds else None}")

        use_jump_host = (session_creds and
                        session_creds.get('jump_host') and
                        jump_host_required_map.get(hostname, False))

        if use_jump_host:
            logger.info(f"✓ Hostname {hostname} requires jump host (marked during discovery)")
            jump_host = session_creds['jump_host']
            jump_username = session_creds['jump_username']
            jump_password = session_creds.get('jump_password')

            logger.info(f"Using jump host {jump_host} to reach {hostname}")

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
                # Run reticket on jump host to get Kerberos tickets
                logger.info(f"Running reticket on jump host")
                reticket_cmd = "reticket"
                reticket_result = await jump_conn.run(reticket_cmd, check=False)
                logger.info(f"reticket output: {reticket_result.stdout}")

                if reticket_result.stderr:
                    logger.info(f"reticket stderr: {reticket_result.stderr}")

                if reticket_result.exit_status != 0:
                    logger.warning(f"reticket failed with exit code: {reticket_result.exit_status}")

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
            # Direct connection
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


def download_csv_files_ssh(hostname, csv_paths, temp_folder, username, password, logger, session_creds=None):
    """
    Synchronous wrapper for download_csv_files_asyncssh
    """
    return asyncio.run(download_csv_files_asyncssh(hostname, csv_paths, temp_folder, username, password, logger, session_creds))


async def download_database_file_asyncssh(hostname, remote_path, local_folder, username, password, logger, session_creds=None):
    """
    Download a database file from remote HPC using asyncssh with jump host support
    Similar to DSI's get_file_size_and_download but with jump host capability

    Returns: local file path if successful, None otherwise
    """
    filename = os.path.basename(remote_path)
    local_path = Path(local_folder) / filename

    try:
        # Check if this hostname needs jump host
        jump_host_required_map = session_creds.get('jump_host_required', {}) if session_creds else {}
        use_jump_host = (session_creds and
                        session_creds.get('jump_host') and
                        jump_host_required_map.get(hostname, False))

        if use_jump_host:
            jump_host = session_creds['jump_host']
            jump_username = session_creds['jump_username']
            jump_password = session_creds.get('jump_password')

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
                # Run reticket
                logger.info(f"Running reticket on jump host")
                await jump_conn.run("reticket", check=False)

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


def download_database_file_ssh(hostname, remote_path, local_folder, username, password, logger, session_creds=None):
    """
    Synchronous wrapper for download_database_file_asyncssh
    """
    return asyncio.run(download_database_file_asyncssh(hostname, remote_path, local_folder, username, password, logger, session_creds))


@app.route('/api/analyze-endpoints', methods=['POST'])
def analyze_endpoints():
    """
    Step 2: pull_remote_db - Download CSV files with per-endpoint credentials
    """
    try:
        data = request.json
        session_id = data.get('session_id', datetime.now().strftime('%Y%m%d_%H%M%S'))
        logger, log_file = setup_logging(f"{session_id}_pull")

        endpoints_location = data.get('endpoints_location')
        endpoint_credentials = data.get('endpoint_credentials', {})

        # Get jump host credentials if provided (they expire, so user re-enters them)
        jump_host = data.get('jump_host')
        jump_username = data.get('jump_username')
        jump_password = data.get('jump_password')

        # Get jump host mapping from frontend (merged from all discovery sessions)
        jump_host_required_map = data.get('jump_host_required_map', {})

        logger.info(f"=== STEP 2: pull_remote_db ===")
        logger.info(f"Pulling {len(endpoints_location)} endpoint(s)")
        logger.info(f"Credentials provided for {len(endpoint_credentials)} endpoint(s)")
        if jump_host:
            logger.info(f"Jump host provided: {jump_host} (user: {jump_username})")
        if jump_host_required_map:
            logger.info(f"Jump host required for: {list(jump_host_required_map.keys())}")

        # Group endpoints by cluster
        cluster_endpoints = {}
        for endpoint_name, endpoint_path in endpoints_location.items():
            if '::' in endpoint_name:
                cluster_name, original_endpoint = endpoint_name.split('::', 1)
                if cluster_name not in cluster_endpoints:
                    cluster_endpoints[cluster_name] = {}
                cluster_endpoints[cluster_name][original_endpoint] = endpoint_path
            else:
                if 'default' not in cluster_endpoints:
                    cluster_endpoints['default'] = {}
                cluster_endpoints['default'][endpoint_name] = endpoint_path

        # Download and parse CSV files to find what databases exist
        databases_by_cluster = {}
        temp_db_storage = f".analyze_{session_id}"

        try:
            create_directory(dir_name=temp_db_storage, delete_if_exists=True, verbose=True)

            for cluster_name, cluster_eps in cluster_endpoints.items():
                logger.info(f"pull_remote_db for cluster: {cluster_name}")

                try:
                    # Download CSV files with per-endpoint credentials
                    logger.info(f"Downloading CSV files from {cluster_name}")

                    # For each endpoint in this cluster, use its specific credentials
                    downloaded_files = []
                    for endpoint_name, csv_path in cluster_eps.items():
                        full_name = f"{cluster_name}::{endpoint_name}"

                        # Get credentials for this specific endpoint
                        ep_creds = endpoint_credentials.get(full_name, {})
                        username = ep_creds.get('username', '')
                        password = ep_creds.get('password', '')

                        if not username:
                            logger.warning(f"No credentials for {full_name}, skipping")
                            print(f"Skipping {full_name} - no credentials provided")
                            continue

                        # Download this single CSV file
                        single_ep = {endpoint_name: csv_path}

                        # Get session credentials for jump host support
                        # Make sure we get a reference to the actual session dict, not a copy
                        if session_id not in session_credentials:
                            session_credentials[session_id] = {}
                        session_data = session_credentials[session_id]

                        # Update session with jump host mapping from frontend (merged from all discoveries)
                        if jump_host_required_map:
                            session_data['jump_host_required'] = jump_host_required_map
                            logger.info(f"Updated session with jump host mapping: {jump_host_required_map}")

                        # Update session with new jump host credentials if provided
                        if jump_host:
                            session_data['jump_host'] = jump_host
                            session_data['jump_username'] = jump_username
                            session_data['jump_password'] = jump_password
                            session_credentials[session_id] = session_data  # Save back
                            logger.info(f"Updated session with fresh jump host credentials")

                        # Log jump host status for this cluster
                        jump_required = session_data.get('jump_host_required', {}).get(cluster_name, False)
                        if jump_required:
                            logger.info(f"✓ Using jump host for {cluster_name} (marked during discovery)")
                        else:
                            logger.info(f"Direct SSH to {cluster_name} (no jump host needed)")

                        files = download_csv_files_ssh(cluster_name, single_ep, temp_db_storage,
                                                       username, password, logger, session_data)
                        downloaded_files.extend(files)

                    if not downloaded_files:
                        logger.warning(f"No CSV files downloaded from {cluster_name}")
                        continue

                    logger.info(f"Downloaded {len(downloaded_files)} CSV file(s) from {cluster_name}")

                    # Combine CSVs (this is what pull_data_endpoints does)
                    output_csv = str(Path(temp_db_storage) / f"output_{cluster_name}.csv")
                    csv_data_sources = combine_csv(temp_db_storage, output_csv)

                    logger.info(f"Combined CSV contains {len(csv_data_sources)} database entries")

                    # Parse the CSV to show what read_data_sources will process
                    # This mimics what read_data_sources will do, but without downloading
                    hpc_databases = []
                    url_databases = []
                    s3_databases = []

                    for row in csv_data_sources:
                        location_type = row['location_type'].strip().lower()
                        db_entry = {
                            'location': row['location'],
                            'path': row['path'],
                            'type': row.get('type', 'unknown'),
                            'submitter_name': row.get('submitter_name', 'unknown'),
                            'location_type': location_type
                        }

                        if location_type == 'hpc':
                            hpc_databases.append(db_entry)
                        elif location_type == 'url':
                            url_databases.append(db_entry)
                        elif location_type == 's3':
                            s3_databases.append(db_entry)

                    # Group HPC databases by unique location (hostname)
                    # This is what will need credentials in Phase 2
                    hpc_by_location = {}
                    for db in hpc_databases:
                        loc = db['location']
                        if loc not in hpc_by_location:
                            hpc_by_location[loc] = []
                        hpc_by_location[loc].append(db)

                    databases_by_cluster[cluster_name] = {
                        'hpc_by_location': hpc_by_location,
                        'url_databases': url_databases,
                        's3_databases': s3_databases,
                        'total_hpc': len(hpc_databases),
                        'total_url': len(url_databases),
                        'total_s3': len(s3_databases),
                        'csv_file': output_csv  # Store for Phase 2
                    }

                    logger.info(f"Analysis complete for {cluster_name}:")
                    logger.info(f"  - HPC databases: {len(hpc_databases)} (grouped by {len(hpc_by_location)} location(s))")
                    logger.info(f"  - URL databases: {len(url_databases)}")
                    logger.info(f"  - S3 databases: {len(s3_databases)}")

                    # Keep CSV files for Phase 2 (federation)
                    # Don't delete them here!

                except Exception as e:
                    logger.error(f"Error analyzing {cluster_name}: {str(e)}", exc_info=True)
                    continue

            # Clean up temp directory
            shutil.rmtree(temp_db_storage, ignore_errors=True)

            # Calculate totals
            total_hpc = sum(c['total_hpc'] for c in databases_by_cluster.values())
            total_url = sum(c['total_url'] for c in databases_by_cluster.values())
            total_s3 = sum(c['total_s3'] for c in databases_by_cluster.values())

            # Store for step 3
            if session_id not in session_credentials:
                session_credentials[session_id] = {}
            session_credentials[session_id]['temp_directory'] = temp_db_storage
            session_credentials[session_id]['databases_by_cluster'] = databases_by_cluster

            logger.info(f"Stored session data for {session_id}: {len(databases_by_cluster)} cluster(s)")

            return jsonify({
                'success': True,
                'databases_by_cluster': databases_by_cluster,
                'totals': {
                    'hpc': total_hpc,
                    'url': total_url,
                    's3': total_s3,
                    'total': total_hpc + total_url + total_s3
                },
                'session_id': session_id,
                'log_file': str(log_file),
                'jump_host_map': session_credentials.get(session_id, {}).get('jump_host_required', {}),
                'jump_host': session_credentials.get(session_id, {}).get('jump_host', ''),
                'jump_username': session_credentials.get(session_id, {}).get('jump_username', '')
            })

        finally:
            # DON'T cleanup - keep files for Phase 2
            pass

    except Exception as e:
        if 'logger' in locals():
            logger.error(f"Error analyzing endpoints: {str(e)}", exc_info=True)
        # Cleanup on error
        if 'temp_db_storage' in locals() and Path(temp_db_storage).exists():
            shutil.rmtree(temp_db_storage, ignore_errors=True)
        return jsonify({
            'success': False,
            'message': f'Error: {str(e)}',
            'log_file': str(log_file) if 'log_file' in locals() else None
        }), 500


@app.route('/api/federate', methods=['POST'])
def federate_data():
    """
    Step 3: read_data_sources - Download a single selected database with credentials
    """
    try:
        data = request.json
        session_id = data.get('session_id', datetime.now().strftime('%Y%m%d_%H%M%S'))
        logger, log_file = setup_logging(f"{session_id}_read")

        workspace_folder = data.get('workspace_folder')
        credentials = data.get('credentials', {})

        # Get jump host credentials if provided (they expire, so user re-enters them)
        jump_host = data.get('jump_host')
        jump_username = data.get('jump_username')
        jump_password = data.get('jump_password')

        # New: Accept single database specification
        selected_database = data.get('selected_database')  # {location, path, type, cluster}

        # Get stored data from Step 2
        session_data = session_credentials.get(session_id, {})
        temp_db_storage = session_data.get('temp_directory')
        databases_by_cluster = session_data.get('databases_by_cluster', {})

        # Update session with new jump host credentials if provided
        if jump_host:
            session_data['jump_host'] = jump_host
            session_data['jump_username'] = jump_username
            session_data['jump_password'] = jump_password
            logger.info(f"Updated session with fresh jump host credentials for Step 3")

        # Resolve workspace folder path
        workspace_path = str(Path(workspace_folder).resolve())

        logger.info(f"=== STEP 3: read_data_sources (Single Database) ===")
        logger.info(f"Starting download to {workspace_path}")

        # Use direct database specification if provided (new single-db mode)
        if selected_database:
            location = selected_database['location']
            path = selected_database['path']
            location_type = selected_database['type']
            cluster_name = selected_database['cluster']

            logger.info(f"Downloading single database: {location}:{path}")

            # Check if this location needs jump host
            jump_required = session_data.get('jump_host_required', {}).get(location, False)
            if jump_required and jump_host:
                logger.info(f"✓ Location {location} requires jump host - using {jump_host}")
            elif jump_required and not jump_host:
                logger.warning(f"⚠️ Location {location} requires jump host but none provided")

            # Get credentials
            username = credentials.get(location, {}).get('username', '')
            password = credentials.get(location, {}).get('password', '')

            if location_type == 'hpc' and not username:
                logger.error(f"No credentials for {location}")
                return jsonify({
                    'success': False,
                    'message': f'Missing username for {location}',
                    'log_file': str(log_file)
                }), 400

            try:
                folder_hash = create_hashed_folder_from_path(path, workspace_path)[0]
                logger.info(f"Downloading {location}:{path}")
                print(f"\nDownloading from {location}: {path}")

                # Check if this location needs jump host
                if location_type == 'hpc' and jump_required:
                    # Use our custom download function with jump host support
                    logger.info(f"Using custom download function with jump host support")
                    downloaded_file_path = download_database_file_ssh(
                        hostname=location,
                        remote_path=path,
                        local_folder=(workspace_path + '/' + folder_hash),
                        username=username,
                        password=password,
                        logger=logger,
                        session_creds=session_data
                    )
                else:
                    # Use standard DSI pull_data function
                    downloaded_file_path = pull_data(
                        location_type=location_type,
                        remote_location=location,
                        remote_path=path,
                        download_location=(workspace_path + '/' + folder_hash),
                        username=username,
                        password=password
                    )

                if downloaded_file_path:
                    _local_folder, _local_filename = split_path(downloaded_file_path)

                    db_info = {
                        "original_location_type": location_type,
                        "original_location": location,
                        "original_path": path,
                        "folder_hash": folder_hash,
                        "local_path": _local_folder,
                        "name": _local_filename,
                        "source_cluster": cluster_name,
                    }

                    # Save to database list
                    upsert_records(f"{workspace_path}/dsi_database_list.json", [db_info], key="original_path")

                    logger.info(f"Successfully downloaded {_local_filename}")
                    print(f"✓ Success: {_local_filename}")

                    return jsonify({
                        'success': True,
                        'success_count': 1,
                        'database_info': [db_info],
                        'clusters_processed': 1,
                        'workspace_folder': workspace_path,
                        'log_file': str(log_file)
                    })
                else:
                    logger.error("Download failed - no file path returned")
                    return jsonify({
                        'success': False,
                        'message': 'Download failed',
                        'log_file': str(log_file)
                    })

            except Exception as e:
                import traceback
                error_msg = f"Error downloading {location}:{path}: {str(e)}"
                error_details = traceback.format_exc()
                logger.error(error_msg, exc_info=True)
                print(f"✗ Error: {error_msg}")
                print(f"Details: {error_details}")
                return jsonify({
                    'success': False,
                    'message': error_msg,
                    'details': error_details,
                    'log_file': str(log_file)
                }), 500

        # Legacy multi-database mode (if no selected_database provided)
        else:
            logger.warning("No selected_database provided - legacy mode not fully supported")
            return jsonify({
                'success': False,
                'message': 'Please select a specific database to download',
                'log_file': str(log_file)
            }), 400

    except Exception as e:
        if 'logger' in locals():
            logger.error(f"Error federating data: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'message': f'Error: {str(e)}',
            'log_file': str(log_file) if 'log_file' in locals() else None
        }), 500


@app.route('/api/logs/<session_id>')
def get_log(session_id):
    """
    Retrieve log file for a specific session
    """
    try:
        # Look for any log file matching the session_id
        log_files = list(LOG_DIR.glob(f"*{session_id}*.log"))
        if log_files:
            with open(log_files[0], 'r') as f:
                content = f.read()
            return jsonify({
                'success': True,
                'content': content
            })
        else:
            return jsonify({
                'success': False,
                'message': 'Log file not found'
            }), 404
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500


@app.route('/api/logs')
def list_logs():
    """
    List all available log files
    """
    try:
        log_files = sorted(LOG_DIR.glob('federate_*.log'), reverse=True)
        logs = []
        for log_file in log_files:
            stat = log_file.stat()
            logs.append({
                'filename': log_file.name,
                'session_id': log_file.stem.replace('federate_', ''),
                'size': stat.st_size,
                'modified': datetime.fromtimestamp(stat.st_mtime).isoformat()
            })
        return jsonify({
            'success': True,
            'logs': logs
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500


if __name__ == '__main__':
    print("Starting DSI Data Federation UI (Interactive Credentials)")
    print("Access the application at: http://localhost:5001")
    print(f"Logs will be saved to: {LOG_DIR}")
    app.run(debug=True, host='0.0.0.0', port=5001)
