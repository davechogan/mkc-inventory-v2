#!/usr/bin/env bash
# Timestamped copy of the inventory SQLite file used by the app.
# Respects MKC_INVENTORY_DB when set; otherwise uses repo data/mkc_inventory.db.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB="${MKC_INVENTORY_DB:-$ROOT/data/mkc_inventory.db}"
DB="$(cd "$(dirname "$DB")" && pwd)/$(basename "$DB")"

if [[ ! -f "$DB" ]]; then
  echo "backup_mkc_db: no file at $DB — skipping (nothing to back up)." >&2
  exit 0
fi

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
DEST="$(dirname "$DB")/mkc_inventory.db.backup.${STAMP}"
cp -a "$DB" "$DEST"
echo "backup_mkc_db: $DB -> $DEST ($(wc -c < "$DEST" | tr -d ' ') bytes)"
