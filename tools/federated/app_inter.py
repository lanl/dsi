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

        # Store credentials for this session
        session_credentials[session_id] = {
            'username': username,
            'password': password
        }

        logger.info(f"Starting endpoint discovery for {len(hpc_names)} cluster(s)")
        logger.info(f"Clusters: {hpc_names}")
        logger.info(f"Script path: {script_path}")
        logger.info(f"Prefixes: {prefixes}")
        logger.info(f"Using password auth: {bool(password)}")

        # Aggregate endpoints from all clusters
        all_endpoints = {}
        cluster_results = {}
        failed_clusters = []

        for hpc_name in hpc_names:
            try:
                logger.info(f"Discovering endpoints from {hpc_name}")

                # Use asyncssh for discovery
                async def discover_with_asyncssh():
                    # Prepare connection options
                    connect_options = {
                        'host': hpc_name,
                        'username': username,
                        'known_hosts': None,
                        'connect_timeout': 30,
                    }

                    if password:
                        connect_options['password'] = password

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
                        async with asyncssh.connect(**connect_options) as conn:
                            result = await conn.run(remote_cmd, check=True)
                            return json.loads(result.stdout.strip())
                    except Exception as e:
                        logger.error(f"SSH error: {str(e)}")
                        return {}

                endpoints_location = asyncio.run(discover_with_asyncssh())

                if endpoints_location:
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
            'log_file': str(log_file)
        })

    except Exception as e:
        if 'logger' in locals():
            logger.error(f"Error discovering endpoints: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'message': f'Error: {str(e)}',
            'log_file': str(log_file) if 'log_file' in locals() else None
        }), 500


async def download_csv_files_asyncssh(hostname, csv_paths, temp_folder, username, password, logger):
    """
    Download CSV files from remote HPC using asyncssh
    Returns list of local file paths
    """
    downloaded_files = []

    # Prepare connection options
    connect_options = {
        'host': hostname,
        'username': username,
        'known_hosts': None,
        'connect_timeout': 30,
    }

    if password:
        connect_options['password'] = password
    # If no password, will try SSH agent or default keys

    try:
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


def download_csv_files_ssh(hostname, csv_paths, temp_folder, username, password, logger):
    """
    Synchronous wrapper for download_csv_files_asyncssh
    """
    return asyncio.run(download_csv_files_asyncssh(hostname, csv_paths, temp_folder, username, password, logger))


@app.route('/api/analyze-endpoints', methods=['POST'])
def analyze_endpoints():
    """
    Phase 1: Download CSV files and analyze what databases need credentials
    """
    try:
        data = request.json
        session_id = data.get('session_id', datetime.now().strftime('%Y%m%d_%H%M%S'))
        logger, log_file = setup_logging(f"{session_id}_analyze")

        endpoints_location = data.get('endpoints_location')

        # Get stored credentials
        creds = session_credentials.get(session_id, {})
        username = creds.get('username')
        password = creds.get('password')

        logger.info(f"Analyzing {len(endpoints_location)} endpoint(s)")
        logger.info(f"Using stored credentials: username={username}, password={'***' if password else 'none'}")

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
                logger.info(f"Analyzing cluster: {cluster_name}")

                try:
                    # Download CSV files using SSH
                    downloaded_files = download_csv_files_ssh(cluster_name, cluster_eps, temp_db_storage,
                                                              username, password, logger)

                    if not downloaded_files:
                        logger.warning(f"No CSV files downloaded from {cluster_name}")
                        continue

                    # Combine CSVs
                    output_csv = str(Path(temp_db_storage) / f"output_{cluster_name}.csv")
                    csv_data_sources = combine_csv(temp_db_storage, output_csv)

                    # Analyze what databases need credentials
                    hpc_databases = []
                    url_databases = []
                    s3_databases = []

                    for row in csv_data_sources:
                        location_type = row['location_type'].strip().lower()
                        db_entry = {
                            'location': row['location'],
                            'path': row['path'],
                            'type': row.get('type', 'unknown'),
                            'submitter_name': row.get('submitter_name', 'unknown')
                        }

                        if location_type == 'hpc':
                            hpc_databases.append(db_entry)
                        elif location_type == 'url':
                            url_databases.append(db_entry)
                        elif location_type == 's3':
                            s3_databases.append(db_entry)

                    # Group HPC databases by unique location (hostname)
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
                        'total_s3': len(s3_databases)
                    }

                    logger.info(f"Cluster {cluster_name}: {len(hpc_databases)} HPC, {len(url_databases)} URL, {len(s3_databases)} S3 databases")

                    # Clean up CSV files for this cluster
                    for file in Path(temp_db_storage).glob('*.csv'):
                        file.unlink()

                except Exception as e:
                    logger.error(f"Error analyzing {cluster_name}: {str(e)}", exc_info=True)
                    continue

            # Clean up temp directory
            shutil.rmtree(temp_db_storage, ignore_errors=True)

            # Calculate totals
            total_hpc = sum(c['total_hpc'] for c in databases_by_cluster.values())
            total_url = sum(c['total_url'] for c in databases_by_cluster.values())
            total_s3 = sum(c['total_s3'] for c in databases_by_cluster.values())

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
                'log_file': str(log_file)
            })

        finally:
            # Ensure cleanup
            if Path(temp_db_storage).exists():
                shutil.rmtree(temp_db_storage, ignore_errors=True)

    except Exception as e:
        if 'logger' in locals():
            logger.error(f"Error analyzing endpoints: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'message': f'Error: {str(e)}',
            'log_file': str(log_file) if 'log_file' in locals() else None
        }), 500


@app.route('/api/federate', methods=['POST'])
def federate_data():
    """
    Phase 2: Execute federation with provided credentials
    """
    try:
        data = request.json
        session_id = data.get('session_id', datetime.now().strftime('%Y%m%d_%H%M%S'))
        logger, log_file = setup_logging(f"{session_id}_federate")

        endpoints_location = data.get('endpoints_location')
        workspace_folder = data.get('workspace_folder')
        credentials = data.get('credentials', {})

        # Get stored SSH credentials
        ssh_creds = session_credentials.get(session_id, {})
        ssh_username = ssh_creds.get('username')
        ssh_password = ssh_creds.get('password')

        # Resolve workspace folder path
        workspace_path = str(Path(workspace_folder).resolve())

        logger.info(f"Starting data federation to {workspace_path}")
        logger.info(f"Total endpoints to federate: {len(endpoints_location)}")
        logger.info(f"Credentials provided for {len(credentials)} location(s)")
        logger.info(f"Using stored SSH credentials: {ssh_username}")

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

        logger.info(f"Federating from {len(cluster_endpoints)} cluster(s)")

        # Pull data from all endpoints with credentials
        all_database_info = []
        total_success_count = 0
        temp_db_storage = f".federate_{session_id}"

        try:
            create_directory(dir_name=temp_db_storage, delete_if_exists=True, verbose=True)

            for cluster_name, cluster_eps in cluster_endpoints.items():
                logger.info(f"Processing cluster: {cluster_name} with {len(cluster_eps)} endpoint(s)")

                try:
                    # Download CSV files using SSH
                    downloaded_files = download_csv_files_ssh(cluster_name, cluster_eps, temp_db_storage,
                                                              ssh_username, ssh_password, logger)

                    if not downloaded_files:
                        logger.warning(f"No endpoint metadata files downloaded from {cluster_name}")
                        continue

                    # Combine CSVs
                    output_csv = str(Path(temp_db_storage) / f"output_{cluster_name}.csv")
                    csv_data_sources = combine_csv(temp_db_storage, output_csv)

                    # Process each database with credentials
                    for row in csv_data_sources:
                        location_type = row['location_type'].strip().lower()
                        location = row['location']

                        # Get credentials for this location
                        username = credentials.get(location, {}).get('username', '')
                        password = credentials.get(location, {}).get('password', '')

                        if location_type == 'hpc' and not username:
                            logger.warning(f"No credentials for {location}, skipping {row['path']}")
                            print(f"Skipping {location}:{row['path']} - no credentials provided")
                            continue

                        try:
                            folder_hash = create_hashed_folder_from_path(row['path'], workspace_path)[0]
                            logger.info(f"Downloading {location}:{row['path']}")
                            print(f"\nDownloading from {location}: {row['path']}")

                            downloaded_file_path = pull_data(
                                location_type=location_type,
                                remote_location=location,
                                remote_path=row['path'],
                                download_location=(workspace_path + '/' + folder_hash),
                                username=username,
                                password=password
                            )

                            if downloaded_file_path:
                                _local_folder, _local_filename = split_path(downloaded_file_path)

                                db_info = {
                                    "original_location_type": row['location_type'],
                                    "original_location": location,
                                    "original_path": row['path'],
                                    "folder_hash": folder_hash,
                                    "local_path": _local_folder,
                                    "name": _local_filename,
                                    "source_cluster": cluster_name,
                                }

                                all_database_info.append(db_info)
                                total_success_count += 1
                                logger.info(f"Successfully downloaded {_local_filename}")
                                print(f"✓ Success: {_local_filename}")

                        except Exception as e:
                            error_msg = f"Error downloading {location}:{row['path']}: {str(e)}"
                            logger.error(error_msg)
                            print(f"✗ Failed: {row['path']} - {str(e)}")
                            continue

                    # Clean up CSV files
                    for file in Path(temp_db_storage).glob('*.csv'):
                        file.unlink()

                    logger.info(f"Cluster {cluster_name}: {total_success_count} successful so far")

                except Exception as e:
                    logger.error(f"Error federating from {cluster_name}: {str(e)}", exc_info=True)
                    continue

            # Clean up temp directory
            shutil.rmtree(temp_db_storage, ignore_errors=True)

            # Save databases information to a JSON file
            if all_database_info:
                upsert_records(f"{workspace_path}/dsi_database_list.json", all_database_info, key="original_path")

            logger.info(f"Completed pulling data. Total success count: {total_success_count}")

            if total_success_count == 0:
                return jsonify({
                    'success': False,
                    'message': 'No databases were successfully federated. Check credentials and network connection.',
                    'log_file': str(log_file)
                })

            return jsonify({
                'success': True,
                'success_count': total_success_count,
                'database_info': all_database_info,
                'clusters_processed': len(cluster_endpoints),
                'workspace_folder': workspace_path,
                'log_file': str(log_file)
            })

        finally:
            # Ensure cleanup
            if Path(temp_db_storage).exists():
                shutil.rmtree(temp_db_storage, ignore_errors=True)

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
