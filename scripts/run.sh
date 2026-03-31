#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY="$ROOT/.venv/bin/python"

if [ ! -x "$PY" ]; then
  echo ".venv missing. Run: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt" >&2
  exit 1
fi

if ! "$PY" -c "import sys; sys.exit(0)" 2>/dev/null; then
  echo "ERROR: $PY does not run (broken venv)." >&2
  exit 1
fi

if ! "$PY" -c "import uvicorn, httpx" 2>/dev/null; then
  echo "Dependencies missing. Run: .venv/bin/pip install -r requirements.txt" >&2
  exit 1
fi

# Stop any existing instance on port 8008.
EXISTING=$(lsof -ti tcp:8008 2>/dev/null || true)
if [ -n "$EXISTING" ]; then
  echo "Stopping existing processes on port 8008 (PIDs: $EXISTING)…"
  echo "$EXISTING" | xargs kill -9 2>/dev/null || true
  for i in 1 2 3 4 5; do
    sleep 1
    if ! lsof -ti tcp:8008 &>/dev/null; then
      echo "Port 8008 freed."
      break
    fi
    if [ "$i" = "5" ]; then
      echo "ERROR: Port 8008 still in use after 5 seconds." >&2
      exit 1
    fi
  done
fi

# Start uvicorn via python -c to raise FD limit before launching.
exec "$PY" -c "
import resource, os
os.chdir('$ROOT')
try:
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (min(4096, hard), hard))
except Exception:
    pass
import uvicorn
uvicorn.run('app:app', host='0.0.0.0', port=8008)
"
