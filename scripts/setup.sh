#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
printf '\nSetup complete. Run API: ./scripts/run.sh\n(or: .venv/bin/uvicorn app:app --reload --host 0.0.0.0 --port 8008)\n'
