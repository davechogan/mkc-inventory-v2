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
  echo "ERROR: $PY does not run (broken venv — often after copying the repo from another path)." >&2
  echo "Recreate: cd \"$ROOT\" && rm -rf .venv && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt" >&2
  exit 1
fi

if ! "$PY" -c "import uvicorn, httpx" 2>/dev/null; then
  echo "Dependencies missing in .venv. Run: .venv/bin/pip install -r requirements.txt" >&2
  exit 1
fi

# Stop any existing instance on port 8008.
EXISTING=$(lsof -ti tcp:8008 2>/dev/null || true)
if [ -n "$EXISTING" ]; then
  echo "Stopping existing processes on port 8008 (PIDs: $EXISTING)…"
  echo "$EXISTING" | xargs kill -9 2>/dev/null || true
  # Wait up to 5 seconds for the port to actually free.
  for i in 1 2 3 4 5; do
    sleep 1
    if ! lsof -ti tcp:8008 &>/dev/null; then
      echo "Port 8008 freed."
      break
    fi
    if [ "$i" = "5" ]; then
      echo "ERROR: Port 8008 still in use after 5 seconds. Aborting." >&2
      lsof -ti tcp:8008 | xargs ps -p 2>/dev/null || true
      exit 1
    fi
  done
fi

# Use python -m uvicorn so we do not rely on .venv/bin/uvicorn's shebang.
# No --reload in production — avoids parent/child process complexity.
exec "$PY" -m uvicorn app:app --host 0.0.0.0 --port 8008
