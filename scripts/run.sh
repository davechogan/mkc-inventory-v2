#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
if [ ! -d .venv ]; then
  echo ".venv not found. Run scripts/setup.sh first." >&2
  exit 1
fi
source .venv/bin/activate
exec uvicorn app:app --reload --host 0.0.0.0 --port 8008
