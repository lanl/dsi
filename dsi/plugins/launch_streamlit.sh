#!/usr/bin/env bash
# Usage on REMOTE:
#   ./streamlit_remote.sh <app.py>
# Example:
#   ./streamlit_remote.sh app.py

set -euo pipefail

if [ $# -lt 2 ]; then
  echo "Usage: $0 <port> <app.py>"
  exit 1
fi

PORT=8501
APP="$2"
shift 2

if [ ! -f "$APP" ]; then
  echo "ERROR: file not found: $APP"
  exit 2
fi

REMOTE_HOST="$(hostname -f 2>/dev/null || hostname)"
REMOTE_USER="$(whoami)"

cat <<EOF

On your LAPTOP, run:
  ssh -L ${PORT}:localhost:${PORT} ${REMOTE_USER}@${REMOTE_HOST}

Then open:
  http://localhost:${PORT}

(Leave that SSH command running while you use the app.)

EOF

echo "Starting Streamlit on remote: $APP (port $PORT)"
echo "Press Ctrl+C here to stop Streamlit."

# Best practice: bind to localhost on remote because you'll access via SSH tunnel
exec streamlit run "$APP" \
  --server.port="$PORT" \
  --server.address=127.0.0.1 \
  --browser.gatherUsageStats=false \
  -- "$@"