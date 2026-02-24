#!/usr/bin/env bash
# Example call within cli.py: ./launch_streamlit.sh app.py {extra args}

set -euo pipefail

PORT=8501
APP="$1"
shift 1

REMOTE_HOST="$(hostname -f 2>/dev/null || hostname)"
REMOTE_USER="$(whoami)"

if [[ -n "${SSH_CONNECTION-}" || -n "${SSH_CLIENT-}" || -n "${SSH_TTY-}" ]]; then
  echo "remote"
  echo "In a separate terminal on your local machine, run:"
  echo " ssh -L ${PORT}:localhost:${PORT} ${REMOTE_USER}@${REMOTE_HOST}"
  echo "http://localhost:${PORT}"
else
  echo "local"  
fi

exec streamlit run "$APP" \
  --server.port="$PORT" \
  --server.headless=true \
  --browser.gatherUsageStats=false \
  -- "$@"
#  --server.address=127.0.0.1 \