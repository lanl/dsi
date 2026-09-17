"""
DSI Data Explorer - MotherDuck-inspired interface for exploring federated databases
A standalone tool focused on data exploration from workspace folders
"""

import sys
from pathlib import Path
from flask import Flask, render_template, request, jsonify
import json
import logging

# Add parent directory to path to import dsi modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dsi.dsi import DSI

app = Flask(__name__)
app.config['SECRET_KEY'] = 'dsi-explorer-dev-key'

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@app.route('/')
def index():
    """Main explorer interface"""
    return render_template('explorer.html')


@app.route('/api/load-workspace', methods=['POST'])
def load_workspace():
    """
    Load workspace and discover all databases
    """
    try:
        data = request.json
        workspace_path = data.get('workspace_path')

        if not workspace_path:
            return jsonify({
                'success': False,
                'message': 'Workspace path required'
            }), 400

        workspace = Path(workspace_path).resolve()
        db_list_file = workspace / 'dsi_database_list.json'

        if not db_list_file.exists():
            return jsonify({
                'success': False,
                'message': f'No database list found at {db_list_file}'
            })

        # Read the database list
        with open(db_list_file, 'r') as f:
            databases = json.load(f)

        # Organize databases by cluster
        databases_by_cluster = {}
        for db in databases:
            cluster = db.get('source_cluster', 'unknown')
            if cluster not in databases_by_cluster:
                databases_by_cluster[cluster] = []
            databases_by_cluster[cluster].append(db)

        return jsonify({
            'success': True,
            'workspace': str(workspace),
            'databases': databases,
            'databases_by_cluster': databases_by_cluster,
            'count': len(databases)
        })

    except Exception as e:
        logger.error(f"Error loading workspace: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'message': f'Error: {str(e)}'
        }), 500


@app.route('/api/get-database-structure', methods=['POST'])
def get_database_structure():
    """
    Get structure of a specific database (tables and columns)
    """
    try:
        data = request.json
        db_path = data.get('database_path')

        if not db_path:
            return jsonify({
                'success': False,
                'message': 'Database path required'
            }), 400

        db_file = Path(db_path)
        if not db_file.exists():
            return jsonify({
                'success': False,
                'message': f'Database not found: {db_path}'
            }), 404

        # Initialize DSI
        dsi_instance = DSI(
            filename=str(db_file),
            backend_name='Sqlite',
            silence_messages=True
        )

        # Get table names
        table_names = dsi_instance.list(collection=True)

        if not table_names:
            dsi_instance.close()
            return jsonify({
                'success': True,
                'tables': [],
                'database_path': str(db_file)
            })

        # Get structure for each table
        tables = []
        for table_name in table_names:
            try:
                # Get table structure using PRAGMA
                pragma_query = f"PRAGMA table_info({table_name});"
                table_info_df = dsi_instance.query(pragma_query, collection=True)

                columns = []
                if table_info_df is not None and not table_info_df.empty:
                    for _, row in table_info_df.iterrows():
                        columns.append({
                            'name': row['name'],
                            'type': row['type'],
                            'notnull': bool(row['notnull']),
                            'pk': bool(row['pk'])
                        })

                # Get row count
                count_query = f"SELECT COUNT(*) as count FROM `{table_name}`;"
                count_df = dsi_instance.query(count_query, collection=True)
                row_count = int(count_df['count'].iloc[0]) if count_df is not None and not count_df.empty else 0

                tables.append({
                    'name': table_name,
                    'columns': columns,
                    'row_count': row_count
                })

            except Exception as table_error:
                logger.error(f"Error getting table info for {table_name}: {table_error}")
                tables.append({
                    'name': table_name,
                    'columns': [],
                    'row_count': None,
                    'error': str(table_error)
                })

        dsi_instance.close()

        return jsonify({
            'success': True,
            'tables': tables,
            'database_path': str(db_file)
        })

    except Exception as e:
        logger.error(f"Error getting database structure: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'message': f'Error: {str(e)}'
        }), 500


@app.route('/api/execute-operation', methods=['POST'])
def execute_operation():
    """
    Execute a DSI operation (query, get_table, find, search)
    """
    try:
        import time

        data = request.json
        db_path = data.get('database_path')
        operation = data.get('operation', 'query')
        query_param = data.get('query_param', '').strip()

        if not db_path or not query_param:
            return jsonify({
                'success': False,
                'message': 'Database path and query parameter required'
            }), 400

        db_file = Path(db_path)
        if not db_file.exists():
            return jsonify({
                'success': False,
                'message': f'Database not found: {db_path}'
            }), 404

        # Initialize DSI
        start_time = time.time()
        dsi_instance = DSI(
            filename=str(db_file),
            backend_name='Sqlite',
            silence_messages=True
        )

        result_df = None

        # Execute operation
        if operation == 'query':
            # Validate SELECT/PRAGMA only
            query_upper = query_param.upper().strip()
            command = query_upper.split()[0] if query_upper else ''
            if command not in ['SELECT', 'PRAGMA']:
                dsi_instance.close()
                return jsonify({
                    'success': False,
                    'message': f'Only SELECT and PRAGMA queries allowed. Got: {command}'
                }), 400
            result_df = dsi_instance.query(query_param, collection=True)

        elif operation == 'get_table':
            result_df = dsi_instance.get_table(query_param, collection=True)

        elif operation == 'find':
            result_df = dsi_instance.find(query_param, collection=True)

        elif operation == 'search':
            import pandas as pd
            search_results = dsi_instance.search(query_param, collection=True)
            if search_results:
                result_df = pd.concat(search_results, ignore_index=True)

        execution_time = round(time.time() - start_time, 3)
        dsi_instance.close()

        # Process results
        if result_df is not None and not result_df.empty:
            # Limit to 1000 rows
            if len(result_df) > 1000:
                result_df = result_df.head(1000)
                truncated = True
            else:
                truncated = False

            rows = result_df.values.tolist()
            columns = result_df.columns.tolist()

            return jsonify({
                'success': True,
                'rows': rows,
                'columns': columns,
                'row_count': len(rows),
                'execution_time': execution_time,
                'truncated': truncated,
                'operation': operation
            })
        else:
            return jsonify({
                'success': True,
                'rows': [],
                'columns': [],
                'row_count': 0,
                'execution_time': execution_time,
                'truncated': False,
                'operation': operation
            })

    except Exception as e:
        logger.error(f"Error executing operation: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'message': f'Error: {str(e)}'
        }), 500


@app.route('/api/preview-table', methods=['POST'])
def preview_table():
    """
    Quick preview of a table (first 100 rows)
    """
    try:
        data = request.json
        db_path = data.get('database_path')
        table_name = data.get('table_name')

        if not db_path or not table_name:
            return jsonify({
                'success': False,
                'message': 'Database path and table name required'
            }), 400

        db_file = Path(db_path)
        if not db_file.exists():
            return jsonify({
                'success': False,
                'message': f'Database not found: {db_path}'
            }), 404

        # Initialize DSI and query first 100 rows
        dsi_instance = DSI(
            filename=str(db_file),
            backend_name='Sqlite',
            silence_messages=True
        )

        query = f"SELECT * FROM `{table_name}` LIMIT 100;"
        result_df = dsi_instance.query(query, collection=True)

        dsi_instance.close()

        if result_df is not None and not result_df.empty:
            rows = result_df.values.tolist()
            columns = result_df.columns.tolist()

            return jsonify({
                'success': True,
                'rows': rows,
                'columns': columns,
                'row_count': len(rows),
                'table_name': table_name
            })
        else:
            return jsonify({
                'success': True,
                'rows': [],
                'columns': [],
                'row_count': 0,
                'table_name': table_name
            })

    except Exception as e:
        logger.error(f"Error previewing table: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'message': f'Error: {str(e)}'
        }), 500


if __name__ == '__main__':
    print("=" * 60)
    print("DSI Data Explorer")
    print("=" * 60)
    print("Starting server...")
    print("Access the explorer at: http://localhost:5002")
    print("Press Ctrl+C to stop")
    print("=" * 60)
    app.run(debug=True, host='0.0.0.0', port=5002)
