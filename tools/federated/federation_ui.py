"""
Flask web UI for DSI Data Federation Tool - Sidebar Version
Provides a user-friendly interface with UI-based credential collection:
left sidebar to pick a federation destination and drill into its endpoints,
right sidebar for contextual details + a running activity log.
"""

import sys
import logging
from pathlib import Path
from datetime import datetime
from flask import Flask, render_template, request, jsonify
import json
import shutil
import asyncio

# Add parent directory to path to import dsi modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dsi.utils.data_acquisition import (
    pull_data,
)
from dsi.utils.acquisition.utils import (
    create_directory,
    combine_csv,
    create_hashed_folder_from_path,
    split_path,
    upsert_records,
)

# Import federated utilities (core reusable functions)
from dsi.utils.federated import (
    discover_endpoints_async,
    download_csv_files_async,
    download_database_file_async,
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
    """Main page - sidebar layout"""
    return render_template('federation_ui.html')


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
        reticket_cmd = data.get('reticket_cmd') or 'reticket'

        # Store credentials for this session
        session_credentials[session_id] = {
            'username': username,
            'password': password,
            'hpc_type': hpc_type,
            'jump_host': jump_host,
            'jump_username': jump_username,
            'jump_password': jump_password,
            'reticket_cmd': reticket_cmd,
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

                # Use core discover_endpoints_async function
                endpoints_location = asyncio.run(discover_endpoints_async(
                    hostname=hpc_name,
                    username=username,
                    script_path=script_path,
                    prefixes=prefixes,
                    password=password,
                    hpc_type=hpc_type,
                    jump_host=jump_host,
                    jump_username=jump_username,
                    jump_password=jump_password,
                    reticket_cmd=reticket_cmd,
                    logger=logger
                ))

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
            'jump_username': jump_username,
            'reticket_cmd': reticket_cmd
        })

    except Exception as e:
        if 'logger' in locals():
            logger.error(f"Error discovering endpoints: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'message': f'Error: {str(e)}',
            'log_file': str(log_file) if 'log_file' in locals() else None
        }), 500


def download_csv_files_ssh(hostname, csv_paths, temp_folder, username, password, logger, session_creds=None):
    """
    Wrapper around core download_csv_files_async function.
    Extracts jump host info from session_creds and calls the core function.
    """
    # Extract jump host info from session credentials
    jump_host_required_map = session_creds.get('jump_host_required', {}) if session_creds else {}
    jump_host_required = jump_host_required_map.get(hostname, False)

    jump_host = session_creds.get('jump_host') if session_creds else None
    jump_username = session_creds.get('jump_username') if session_creds else None
    jump_password = session_creds.get('jump_password') if session_creds else None
    reticket_cmd = session_creds.get('reticket_cmd', 'reticket') if session_creds else 'reticket'

    # Debug logging
    logger.info(f"Download check for hostname: {hostname}")
    logger.info(f"Jump host required map: {jump_host_required_map}")
    logger.info(f"Jump host available: {jump_host}")

    # Call the core download function
    return asyncio.run(download_csv_files_async(
        hostname=hostname,
        csv_paths=csv_paths,
        temp_folder=temp_folder,
        username=username,
        password=password,
        jump_host=jump_host,
        jump_username=jump_username,
        jump_password=jump_password,
        jump_host_required=jump_host_required,
        reticket_cmd=reticket_cmd,
        logger=logger
    ))


def download_database_file_ssh(hostname, remote_path, local_folder, username, password, logger, session_creds=None):
    """
    Wrapper around core download_database_file_async function.
    Extracts jump host info from session_creds and calls the core function.
    """
    # Extract jump host info from session credentials
    jump_host_required_map = session_creds.get('jump_host_required', {}) if session_creds else {}
    jump_host_required = jump_host_required_map.get(hostname, False)

    jump_host = session_creds.get('jump_host') if session_creds else None
    jump_username = session_creds.get('jump_username') if session_creds else None
    jump_password = session_creds.get('jump_password') if session_creds else None
    reticket_cmd = session_creds.get('reticket_cmd', 'reticket') if session_creds else 'reticket'

    # Call the core download function
    return asyncio.run(download_database_file_async(
        hostname=hostname,
        remote_path=remote_path,
        local_folder=local_folder,
        username=username,
        password=password,
        jump_host=jump_host,
        jump_username=jump_username,
        jump_password=jump_password,
        jump_host_required=jump_host_required,
        reticket_cmd=reticket_cmd,
        logger=logger
    ))


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
        reticket_cmd = data.get('reticket_cmd')

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

                        # Update session with reticket_cmd if provided, otherwise fall back
                        # to whatever was stored during discovery, defaulting to "reticket"
                        if reticket_cmd:
                            session_data['reticket_cmd'] = reticket_cmd
                        reticket_cmd_for_call = session_data.get('reticket_cmd', 'reticket')

                        # Log jump host status for this cluster
                        jump_required = session_data.get('jump_host_required', {}).get(cluster_name, False)
                        if jump_required:
                            logger.info(f"✓ Using jump host for {cluster_name} (marked during discovery)")
                        else:
                            logger.info(f"Direct SSH to {cluster_name} (no jump host needed)")

                        # Use pull_data() for each CSV file (unified with CLI path!)
                        jump_required = session_data.get('jump_host_required', {}).get(cluster_name, False)

                        for endpoint_name, csv_path in single_ep.items():
                            logger.info(f"Downloading CSV: {endpoint_name} from {csv_path}")
                            try:
                                downloaded_file = pull_data(
                                    location_type='hpc',
                                    remote_location=cluster_name,
                                    remote_path=csv_path,
                                    download_location=temp_db_storage,
                                    username=username,
                                    password=password,
                                    jump_host=session_data.get('jump_host'),
                                    jump_username=session_data.get('jump_username'),
                                    jump_password=session_data.get('jump_password'),
                                    jump_host_required=jump_required,
                                    reticket_cmd=reticket_cmd_for_call
                                )

                                if downloaded_file:
                                    # Rename to match expected format: hostname_endpointname.csv
                                    folder, filename = split_path(downloaded_file)
                                    new_filename = f"{cluster_name}_{endpoint_name}.csv"
                                    new_path = str(Path(folder) / new_filename)

                                    shutil.move(downloaded_file, new_path)
                                    downloaded_files.append(new_path)
                                    logger.info(f"✓ Downloaded and renamed to: {new_filename}")
                            except Exception as e:
                                logger.error(f"Failed to download {endpoint_name}: {e}")
                                continue

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
                'jump_username': session_credentials.get(session_id, {}).get('jump_username', ''),
                'reticket_cmd': session_credentials.get(session_id, {}).get('reticket_cmd', 'reticket')
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
        reticket_cmd = data.get('reticket_cmd')

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

        # Update session with reticket_cmd if provided, otherwise fall back
        # to whatever was stored earlier, defaulting to "reticket"
        if reticket_cmd:
            session_data['reticket_cmd'] = reticket_cmd
        reticket_cmd_for_call = session_data.get('reticket_cmd', 'reticket')

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

                # Use enhanced DSI pull_data function (now supports jump host!)
                logger.info(f"Using DSI pull_data() with jump_host_required={jump_required}")

                downloaded_file_path = pull_data(
                    location_type=location_type,
                    remote_location=location,
                    remote_path=path,
                    download_location=(workspace_path + '/' + folder_hash),
                    username=username,
                    password=password,
                    jump_host=session_data.get('jump_host'),
                    jump_username=session_data.get('jump_username'),
                    jump_password=session_data.get('jump_password'),
                    jump_host_required=jump_required,
                    reticket_cmd=reticket_cmd_for_call
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


@app.route('/api/explore-databases', methods=['POST'])
def explore_databases():
    """
    List all databases in a workspace folder by reading the dsi_database_list.json file
    """
    try:
        data = request.json
        workspace_folder = data.get('workspace_folder')

        if not workspace_folder:
            return jsonify({
                'success': False,
                'message': 'Workspace folder path required'
            }), 400

        workspace_path = Path(workspace_folder).resolve()
        db_list_file = workspace_path / 'dsi_database_list.json'

        if not db_list_file.exists():
            return jsonify({
                'success': False,
                'message': f'No database list found at {db_list_file}',
                'databases': []
            })

        # Read the database list
        with open(db_list_file, 'r') as f:
            databases = json.load(f)

        return jsonify({
            'success': True,
            'databases': databases,
            'workspace': str(workspace_path),
            'count': len(databases)
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error: {str(e)}',
            'databases': []
        }), 500


@app.route('/api/database-info', methods=['POST'])
def get_database_info():
    """
    Get detailed information about a database file (size, tables, row counts) using DSI interface
    """
    try:
        from dsi.backends.sqlite import Sqlite

        data = request.json
        database_path = data.get('database_path')

        if not database_path:
            return jsonify({
                'success': False,
                'message': 'Database path required'
            }), 400

        db_path = Path(database_path)

        if not db_path.exists():
            return jsonify({
                'success': False,
                'message': f'Database file not found: {database_path}'
            }), 404

        # Get file size
        file_size = db_path.stat().st_size

        result = {
            'success': True,
            'file_size': file_size,
            'tables': []
        }

        # Try to get table information using DSI interface
        try:
            # Open database in read-only mode using DSI
            dsi_db = Sqlite(f'file:{db_path}?mode=ro', uri=True)

            # Get all table names using DSI's query_artifacts
            tables_df = dsi_db.query_artifacts(
                "SELECT name FROM sqlite_master WHERE type='table';",
                isVerbose=False
            )

            if tables_df is not None and not tables_df.empty:
                for table_name in tables_df['name']:
                    try:
                        # Get row count for each table using DSI
                        count_query = f"SELECT COUNT(*) as count FROM `{table_name}`;"
                        count_df = dsi_db.query_artifacts(count_query, isVerbose=False)

                        if count_df is not None and not count_df.empty:
                            row_count = int(count_df['count'].iloc[0])
                            result['tables'].append({
                                'name': table_name,
                                'row_count': row_count
                            })
                        else:
                            result['tables'].append({
                                'name': table_name,
                                'row_count': 0
                            })
                    except Exception as e:
                        result['tables'].append({
                            'name': table_name,
                            'error': str(e)
                        })

            # Close DSI connection
            dsi_db.con.close()

        except Exception as e:
            result['error'] = f'Could not read database structure: {str(e)}'

        return jsonify(result)

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error: {str(e)}'
        }), 500


@app.route('/api/execute-dsi-query', methods=['POST'])
def execute_dsi_query():
    """
    Execute a DSI query using one of the four methods: query(), get_table(), find(), search()
    """
    try:
        import time
        from dsi.dsi import DSI

        data = request.json
        databases = data.get('databases', [])
        method = data.get('method', 'query')
        query_param = data.get('query_param', '').strip()

        if not query_param:
            return jsonify({
                'success': False,
                'message': 'Query parameter is required'
            }), 400

        if not databases:
            return jsonify({
                'success': False,
                'message': 'At least one database must be selected'
            }), 400

        # Validate method
        valid_methods = ['query', 'get_table', 'find', 'search']
        if method not in valid_methods:
            return jsonify({
                'success': False,
                'message': f'Invalid method. Must be one of: {", ".join(valid_methods)}'
            }), 400

        # For query method, validate it's SELECT or PRAGMA only
        if method == 'query':
            query_upper = query_param.upper().strip()
            command = query_upper.split()[0] if query_upper else ''
            if command not in ['SELECT', 'PRAGMA']:
                return jsonify({
                    'success': False,
                    'message': f'Only SELECT and PRAGMA queries are allowed. Attempted command: {command}'
                }), 400

        results = []

        for db in databases:
            db_result = {
                'database_name': db.get('name'),
                'database_path': f"{db.get('local_path')}/{db.get('name')}",
                'success': False
            }

            db_path = Path(db.get('local_path')) / db.get('name')

            if not db_path.exists():
                db_result['error'] = f'Database file not found: {db_path}'
                results.append(db_result)
                continue

            try:
                start_time = time.time()

                # Initialize DSI with this database (read-only mode)
                dsi_instance = DSI(
                    filename=str(db_path),
                    backend_name='Sqlite',
                    silence_messages=True
                )

                # Execute the appropriate DSI method
                result_df = None

                if method == 'query':
                    # Execute SQL query
                    result_df = dsi_instance.query(query_param, collection=True)

                elif method == 'get_table':
                    # Get entire table
                    result_df = dsi_instance.get_table(query_param, collection=True)

                elif method == 'find':
                    # Find rows matching condition
                    result_df = dsi_instance.find(query_param, collection=True)

                elif method == 'search':
                    # Search across all tables
                    # search() returns a list of DataFrames
                    search_results = dsi_instance.search(query_param, collection=True)

                    if search_results:
                        # Combine all search results into one DataFrame
                        import pandas as pd
                        result_df = pd.concat(search_results, ignore_index=True)
                    else:
                        result_df = None

                execution_time = round(time.time() - start_time, 3)

                # Close DSI instance
                dsi_instance.close()

                if result_df is not None and not result_df.empty:
                    # Limit to 1000 rows for safety
                    if len(result_df) > 1000:
                        result_df = result_df.head(1000)

                    # Convert DataFrame to list of lists for JSON serialization
                    rows = result_df.values.tolist()
                    columns = result_df.columns.tolist()

                    db_result['success'] = True
                    db_result['rows'] = rows
                    db_result['columns'] = columns
                    db_result['row_count'] = len(rows)
                    db_result['execution_time'] = execution_time
                else:
                    # Empty result
                    db_result['success'] = True
                    db_result['rows'] = []
                    db_result['columns'] = []
                    db_result['row_count'] = 0
                    db_result['execution_time'] = execution_time

            except Exception as e:
                db_result['error'] = f'Error: {str(e)}'

            results.append(db_result)

        return jsonify({
            'success': True,
            'results': results,
            'method': method,
            'query_param': query_param
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error: {str(e)}'
        }), 500


@app.route('/api/execute-query', methods=['POST'])
def execute_query():
    """
    Execute a SQL query against one or more databases using DSI interface
    """
    try:
        import time
        from dsi.backends.sqlite import Sqlite

        data = request.json
        databases = data.get('databases', [])
        query = data.get('query', '').strip()

        if not query:
            return jsonify({
                'success': False,
                'message': 'Query is required'
            }), 400

        if not databases:
            return jsonify({
                'success': False,
                'message': 'At least one database must be selected'
            }), 400

        # Security check - only allow SELECT and PRAGMA queries
        query_upper = query.upper().strip()
        command = query_upper.split()[0] if query_upper else ''

        if command not in ['SELECT', 'PRAGMA']:
            return jsonify({
                'success': False,
                'message': f'Only SELECT and PRAGMA queries are allowed through the web interface. Attempted command: {command}'
            }), 400

        results = []

        for db in databases:
            db_result = {
                'database_name': db.get('name'),
                'database_path': f"{db.get('local_path')}/{db.get('name')}",
                'success': False
            }

            db_path = Path(db.get('local_path')) / db.get('name')

            if not db_path.exists():
                db_result['error'] = f'Database file not found: {db_path}'
                results.append(db_result)
                continue

            try:
                start_time = time.time()

                # Use DSI Sqlite interface with read-only connection
                # Pass uri=True to allow read-only mode via file: URI scheme
                dsi_db = Sqlite(f'file:{db_path}?mode=ro', uri=True)

                # Execute query using DSI's query_artifacts method
                # Returns a pandas DataFrame
                result_df = dsi_db.query_artifacts(query, isVerbose=False)

                execution_time = round(time.time() - start_time, 3)

                if result_df is not None and not result_df.empty:
                    # Limit to 1000 rows for safety
                    if len(result_df) > 1000:
                        result_df = result_df.head(1000)

                    # Convert DataFrame to list of lists for JSON serialization
                    rows = result_df.values.tolist()
                    columns = result_df.columns.tolist()

                    db_result['success'] = True
                    db_result['rows'] = rows
                    db_result['columns'] = columns
                    db_result['row_count'] = len(rows)
                    db_result['execution_time'] = execution_time
                else:
                    # Empty result
                    db_result['success'] = True
                    db_result['rows'] = []
                    db_result['columns'] = []
                    db_result['row_count'] = 0
                    db_result['execution_time'] = execution_time

                # Close DSI connection
                dsi_db.con.close()

            except Exception as e:
                db_result['error'] = f'Error: {str(e)}'

            results.append(db_result)

        return jsonify({
            'success': True,
            'results': results,
            'query': query
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error: {str(e)}'
        }), 500


@app.route('/api/database-summary', methods=['POST'])
def get_database_summary():
    """
    Get summary information and table list for selected databases using DSI
    """
    try:
        import time
        from dsi.dsi import DSI

        data = request.json
        databases = data.get('databases', [])

        if not databases:
            return jsonify({
                'success': False,
                'message': 'At least one database must be selected'
            }), 400

        summaries = []

        for db in databases:
            summary_result = {
                'database_name': db.get('name'),
                'database_path': f"{db.get('local_path')}/{db.get('name')}",
                'success': False
            }

            db_path = Path(db.get('local_path')) / db.get('name')

            if not db_path.exists():
                summary_result['error'] = f'Database file not found: {db_path}'
                summaries.append(summary_result)
                continue

            try:
                # Initialize DSI with this database
                dsi_instance = DSI(
                    filename=str(db_path),
                    backend_name='Sqlite',
                    silence_messages=True
                )

                # Get table list
                table_names = dsi_instance.list(collection=True)

                if not table_names:
                    summary_result['success'] = True
                    summary_result['tables'] = []
                    summary_result['total_rows'] = 0
                    dsi_instance.close()
                    summaries.append(summary_result)
                    continue

                # Get detailed info for each table
                tables_info = []
                total_rows = 0

                for table_name in table_names:
                    try:
                        # Get table data to count rows and columns
                        table_df = dsi_instance.get_table(table_name, collection=True)

                        if table_df is not None and not table_df.empty:
                            row_count = len(table_df)
                            columns = []

                            # Get column names and types
                            for col_name in table_df.columns:
                                col_type = str(table_df[col_name].dtype)
                                columns.append({
                                    'name': col_name,
                                    'type': col_type
                                })

                            tables_info.append({
                                'name': table_name,
                                'row_count': row_count,
                                'columns': columns
                            })

                            total_rows += row_count
                        else:
                            # Empty table
                            tables_info.append({
                                'name': table_name,
                                'row_count': 0,
                                'columns': []
                            })

                    except Exception as table_error:
                        # If we can't read the table, still include it with error
                        tables_info.append({
                            'name': table_name,
                            'row_count': None,
                            'columns': [],
                            'error': str(table_error)
                        })

                summary_result['success'] = True
                summary_result['tables'] = tables_info
                summary_result['total_rows'] = total_rows

                # Close DSI instance
                dsi_instance.close()

            except Exception as e:
                summary_result['error'] = f'Error: {str(e)}'

            summaries.append(summary_result)

        return jsonify({
            'success': True,
            'summaries': summaries
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error: {str(e)}'
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


@app.route('/api/help')
def get_help():
    """
    Render help.md to HTML so the UI's Help tab can load it without a rebuild.
    """
    try:
        import markdown
        help_path = Path(__file__).parent / 'help.md'
        md_text = help_path.read_text()
        html = markdown.markdown(md_text, extensions=['fenced_code', 'tables'])
        return jsonify({'success': True, 'html': html})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


if __name__ == '__main__':
    print("Starting DSI Data Federation UI (Sidebar Version)")
    print("Access the application at: http://localhost:5002")
    print(f"Logs will be saved to: {LOG_DIR}")
    app.run(debug=True, host='0.0.0.0', port=5002)
