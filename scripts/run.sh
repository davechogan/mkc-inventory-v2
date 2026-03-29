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
  echo "ERROR: $PY does not run (broken venv — often after copying the repo from another path, e.g. /Volumes/... vs /Users/...)." >&2
  echo "Virtualenv paths are baked into scripts; recreate on this machine:" >&2
  echo "  cd \"$ROOT\" && rm -rf .venv && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt" >&2
  exit 1
fi

if ! "$PY" -c "import uvicorn, httpx" 2>/dev/null; then
  echo "Dependencies missing in .venv. Run: .venv/bin/pip install -r requirements.txt" >&2
  exit 1
fi

# Stop any existing instance on port 8008 before starting.
EXISTING=$(lsof -ti tcp:8008 2>/dev/null || true)
if [ -n "$EXISTING" ]; then
  echo "Stopping existing process on port 8008 (PID $EXISTING)…"
  kill "$EXISTING" 2>/dev/null || true
  # Wait up to 3 seconds for the port to free.
  for i in 1 2 3; do
    sleep 1
    lsof -ti tcp:8008 &>/dev/null || break
  done
fi

# Use python -m uvicorn so we do not rely on .venv/bin/uvicorn's shebang (stale if .venv was moved/copied).
exec "$PY" -m uvicorn app:app --reload --host 0.0.0.0 --port 8008
