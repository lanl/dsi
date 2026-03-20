#!/usr/bin/env bash
# Usage on REMOTE:
#   ./streamlit_remote.sh <app.py>
# Example:
#   ./streamlit_remote.sh app.py

set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: $0 <app.py>"
  exit 1
fi

PORT=8501
APP="$1"
shift 1

REMOTE_HOST="$(hostname -f 2>/dev/null || hostname)"
REMOTE_USER="$(whoami)"

if [[ -n "${SSH_CONNECTION-}" || -n "${SSH_CLIENT-}" || -n "${SSH_TTY-}" ]]; then
  echo "remote"
  echo "On your laptop, run: ssh -L ${PORT}:localhost:${PORT} ${REMOTE_USER}@${REMOTE_HOST}"
  echo "Open: http://localhost:${PORT}"
  echo "Leave that SSH command running while you use the app."

  exec streamlit run "$APP" \
    --server.port="$PORT" \
    --server.address=127.0.0.1 \
    --server.headless=true \
    --browser.gatherUsageStats=false \
    -- "$@"

else
  echo "local"
  exec streamlit run "$APP" \
    --server.headless=true \
    --browser.gatherUsageStats=false \
    -- "$@"
fi