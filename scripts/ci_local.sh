#!/usr/bin/env bash
# Local checks aligned with .github/workflows/ci.yml job "checks".
# Usage: from repo root, run: ./scripts/ci_local.sh
#
# Optional: VERIFY_IMPORT_APP=1 also runs "import app" against a fresh SQLite file
# in $TMPDIR (via MKC_INVENTORY_DB). That executes full init_db() and can take
# several minutes on first run; use when you need import-time validation.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PY="${ROOT}/.venv/bin/python"
PIP="${ROOT}/.venv/bin/pip"

if [[ ! -x "$PY" ]]; then
  echo "No .venv found. Create it with: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt" >&2
  exit 1
fi

"$PIP" install -q -r requirements.txt
if [[ -f "${ROOT}/requirements-dev.txt" ]]; then
  "$PIP" install -q -r requirements-dev.txt
fi
"$PY" -m py_compile app.py
"$PY" -m py_compile reporting/domain.py
"$PY" -m py_compile reporting/regex_contract.py
"$PY" -m py_compile reporting/routes.py
"$PY" -m py_compile routes/v2_routes.py
"$PY" -m py_compile routes/normalized_routes.py
"$PY" -m py_compile routes/legacy_catalog_routes.py
"$PY" -m py_compile routes/ai_routes.py
"$PY" -m py_compile sqlite_schema.py
"$PY" -m py_compile tools/reporting_eval_harness.py
"$PY" -m pytest -q

if [[ "${VERIFY_IMPORT_APP:-}" == "1" ]]; then
  SMOKE_DB="${TMPDIR:-/tmp}/mkc_ci_smoke_$$.db"
  export MKC_INVENTORY_DB="$SMOKE_DB"
  rm -f "$SMOKE_DB"
  "$PY" -c "import app; print('import app: OK')"
  rm -f "$SMOKE_DB"
  unset MKC_INVENTORY_DB
fi

echo "ci_local.sh: all checks passed."
