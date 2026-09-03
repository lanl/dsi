"""
Flask web UI for DSI Data Federation Tool
Provides a user-friendly interface for federating data endpoints from remote HPCs
"""

import sys
import logging
from pathlib import Path
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_from_directory
import json
import os

# Add parent directory to path to import dsi modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dsi.utils.data_acquisition import (
    get_remote_endpoints_ssh,
    pull_data_endpoints,
)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'dev-secret-key-change-in-production'

# Configure logging
LOG_DIR = Path(__file__).parent / 'logs'
LOG_DIR.mkdir(exist_ok=True)

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
    return render_template('index.html')


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
        script_path = data.get('script_path')
        prefixes = data.get('prefixes', ['DSI_ENDPOINT_', 'DIANA_ENDPOINT_'])

        logger.info(f"Starting endpoint discovery for {len(hpc_names)} cluster(s)")
        logger.info(f"Clusters: {hpc_names}")
        logger.info(f"Script path: {script_path}")
        logger.info(f"Prefixes: {prefixes}")

        # Aggregate endpoints from all clusters
        all_endpoints = {}
        cluster_results = {}
        failed_clusters = []

        for hpc_name in hpc_names:
            try:
                logger.info(f"Discovering endpoints from {hpc_name}")

                # Get remote endpoints for this cluster
                endpoints_location = get_remote_endpoints_ssh(
                    hostname=hpc_name,
                    username=username,
                    hpc_type=hpc_type,
                    script_path=script_path,
                    prefixes=prefixes,
                    verbose=True
                )

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


@app.route('/api/federate', methods=['POST'])
def federate_data():
    """
    Federate data from discovered endpoints (from multiple clusters)
    """
    try:
        data = request.json
        session_id = data.get('session_id', datetime.now().strftime('%Y%m%d_%H%M%S'))
        logger, log_file = setup_logging(session_id)

        endpoints_location = data.get('endpoints_location')
        workspace_folder = data.get('workspace_folder')

        # Resolve workspace folder path
        workspace_path = str(Path(workspace_folder).resolve())

        logger.info(f"Starting data federation to {workspace_path}")
        logger.info(f"Total endpoints to federate: {len(endpoints_location)}")
        logger.info(f"Endpoints: {endpoints_location}")

        # Group endpoints by cluster
        cluster_endpoints = {}
        for endpoint_name, endpoint_path in endpoints_location.items():
            # Extract cluster name from prefixed endpoint name (format: "cluster::endpoint")
            if '::' in endpoint_name:
                cluster_name, original_endpoint = endpoint_name.split('::', 1)
                if cluster_name not in cluster_endpoints:
                    cluster_endpoints[cluster_name] = {}
                cluster_endpoints[cluster_name][original_endpoint] = endpoint_path
            else:
                # Fallback for non-prefixed endpoints
                if 'default' not in cluster_endpoints:
                    cluster_endpoints['default'] = {}
                cluster_endpoints['default'][endpoint_name] = endpoint_path

        logger.info(f"Federating from {len(cluster_endpoints)} cluster(s)")

        # Pull data from all endpoints, organized by cluster
        all_database_info = []
        total_success_count = 0

        for cluster_name, cluster_eps in cluster_endpoints.items():
            logger.info(f"Processing cluster: {cluster_name} with {len(cluster_eps)} endpoint(s)")

            try:
                # Use the original pull_data_endpoints function
                # It will prompt for credentials as needed in the terminal
                database_info, success_count = pull_data_endpoints(
                    cluster_eps,
                    cluster_name,
                    workspace_path
                )

                # Add cluster info to each database entry
                for db in database_info:
                    db['source_cluster'] = cluster_name

                all_database_info.extend(database_info)
                total_success_count += success_count

                logger.info(f"Cluster {cluster_name}: {success_count} successful")

            except Exception as e:
                logger.error(f"Error federating from {cluster_name}: {str(e)}", exc_info=True)
                continue

        logger.info(f"Completed pulling data. Total success count: {total_success_count}")

        if total_success_count == 0:
            return jsonify({
                'success': False,
                'message': 'No databases were successfully federated from any cluster. Please check your credentials and network connection.',
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
        log_file = LOG_DIR / f"federate_{session_id}.log"
        if log_file.exists():
            with open(log_file, 'r') as f:
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
    print("Starting DSI Data Federation UI")
    print("Access the application at: http://localhost:5000")
    print(f"Logs will be saved to: {LOG_DIR}")
    app.run(debug=True, host='0.0.0.0', port=5000)
