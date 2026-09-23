"""
Flask web UI for DSI Data Federation Tool - Sidebar Version
Reuses all API routes from app_inter.py, only the UI/template is different:
left sidebar to pick a federation destination and drill into its endpoints,
right sidebar for contextual details + a running activity log.
"""

import sys
from pathlib import Path
from flask import Flask, render_template

# Add parent directory to path to import dsi modules (same as app_inter.py)
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Reuse the existing Flask app's routes/backend logic instead of duplicating it.
# Importing app_inter does NOT start its server since its __main__ guard only
# runs when app_inter.py itself is executed directly.
import app_inter as backend

app = Flask(__name__)
app.config['SECRET_KEY'] = backend.app.config['SECRET_KEY']

# Re-register every backend API route (discover/analyze/federate/explore/query/logs)
# onto this app, skipping the index page and Flask's built-in static route so we
# can serve our own sidebar UI at "/".
for rule in backend.app.url_map.iter_rules():
    if rule.endpoint in ('index', 'static'):
        continue
    app.add_url_rule(
        rule.rule,
        endpoint=rule.endpoint,
        view_func=backend.app.view_functions[rule.endpoint],
        methods=sorted(rule.methods - {'HEAD', 'OPTIONS'}) or None,
    )


@app.route('/')
def index():
    """Main page - sidebar layout"""
    return render_template('federation_ui.html')


if __name__ == '__main__':
    print("Starting DSI Data Federation UI (Sidebar Version)")
    print("Access the application at: http://localhost:5002")
    print(f"Logs will be saved to: {backend.LOG_DIR}")
    app.run(debug=True, host='0.0.0.0', port=5002)
