"""Complete v2 migration — fill gaps and validate.

This script is a one-shot, idempotent tool that:
1. Asserts the v2 schema is present.
2. Fills any legacy inventory_items that have no v2 counterpart (INSERT OR IGNORE).
   Does NOT re-run the full migration or delete direct-to-v2 inventory additions.
3. Validates referential integrity and record counts.
4. Writes a provenance snapshot to the Artifacts repo.
5. Prints a pass/fail summary.

Safe to run multiple times — uses INSERT OR IGNORE on legacy_inventory_id UNIQUE constraint.

Usage:
    python -m tools.complete_migrate_v2
    python -m tools.complete_migrate_v2 --dry-run
    python -m tools.complete_migrate_v2 --artifacts-dir /path/to/Artifacts/projects/mkc-inventory-v2
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import shutil
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

_REPO_ROOT = Path(__file__).parent.parent
_DB_PATH = _REPO_ROOT / "data" / "mkc_inventory.db"
_ARTIFACTS_DIR = Path(__file__).parent.parent.parent / "Artifacts" / "projects" / "mkc-inventory-v2"

REQUIRED_V2_TABLES = [
    "knife_models_v2",
    "inventory_items_v2",
    "knife_families",
    "knife_types",
    "knife_forms",
    "knife_series",
    "collaborators",
]


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ).fetchone()
    )


def assert_v2_schema(conn: sqlite3.Connection) -> None:
    missing = [t for t in REQUIRED_V2_TABLES if not _table_exists(conn, t)]
    if missing:
        print(f"FAIL: v2 schema incomplete — missing tables: {missing}", file=sys.stderr)
        sys.exit(1)
    print("OK  v2 schema present")


def fill_inventory_gaps(conn: sqlite3.Connection, dry_run: bool = False) -> list[dict]:
    """Insert legacy inventory items that have no v2 counterpart.

    Uses INSERT OR IGNORE so it is safe to run multiple times.
    Does NOT touch items added directly to v2 (legacy_inventory_id IS NULL).
    """
    gaps = conn.execute("""
        SELECT ii.*, km2.id AS knife_model_v2_id
        FROM inventory_items ii
        LEFT JOIN knife_models_v2 km2 ON km2.legacy_master_id = ii.master_knife_id
        WHERE NOT EXISTS (
            SELECT 1 FROM inventory_items_v2 WHERE legacy_inventory_id = ii.id
        )
    """).fetchall()

    if not gaps:
        print("OK  no inventory gaps to fill (all legacy items already migrated)")
        return []

    def _get(row, key, default=None):
        try:
            return row[key]
        except (IndexError, KeyError):
            return default

    inserted = []
    for row in gaps:
        row = dict(row)
        knife_model_id = row.get("knife_model_v2_id")
        if knife_model_id is None:
            print(
                f"WARN  legacy inventory id={row['id']} (master_knife_id={row['master_knife_id']}) "
                f"has no v2 model counterpart — skipping"
            )
            continue
        print(
            f"{'DRY-RUN' if dry_run else 'INSERT'} legacy inventory id={row['id']} "
            f"→ knife_models_v2.id={knife_model_id}"
        )
        if not dry_run:
            conn.execute(
                """
                INSERT OR IGNORE INTO inventory_items_v2 (
                    legacy_inventory_id, legacy_master_id, knife_model_id,
                    nickname, quantity, acquired_date,
                    purchase_price, estimated_value, condition,
                    steel, blade_finish, blade_color, handle_color,
                    collaboration_name, serial_number, location,
                    purchase_source, last_sharpened, notes,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["id"],
                    row["master_knife_id"],
                    knife_model_id,
                    row.get("nickname"),
                    row.get("quantity") or 1,
                    row.get("acquired_date"),
                    row.get("purchase_price"),
                    row.get("estimated_value"),
                    row.get("condition"),
                    row.get("blade_steel"),  # legacy column name differs
                    row.get("blade_finish"),
                    row.get("blade_color"),
                    row.get("handle_color"),
                    row.get("collaboration_name"),
                    row.get("serial_number"),
                    row.get("location"),
                    row.get("purchase_source"),
                    row.get("last_sharpened"),
                    row.get("notes"),
                    row.get("created_at"),
                    row.get("updated_at"),
                ),
            )
        inserted.append({"legacy_id": row["id"], "knife_model_v2_id": knife_model_id})
    return inserted


def validate_counts(conn: sqlite3.Connection) -> bool:
    """Validate that v2 tables are consistent and reporting views work."""
    ok = True

    legacy_inv = conn.execute("SELECT COUNT(*) FROM inventory_items").fetchone()[0]
    v2_inv = conn.execute("SELECT COUNT(*) FROM inventory_items_v2").fetchone()[0]
    view_inv = conn.execute("SELECT COUNT(*) FROM reporting_inventory").fetchone()[0]

    legacy_mod = conn.execute("SELECT COUNT(*) FROM master_knives").fetchone()[0]
    v2_mod = conn.execute("SELECT COUNT(*) FROM knife_models_v2").fetchone()[0]
    view_mod = conn.execute("SELECT COUNT(*) FROM reporting_models").fetchone()[0]

    orphan = conn.execute(
        "SELECT COUNT(*) FROM inventory_items_v2 WHERE knife_model_id IS NULL"
    ).fetchone()[0]

    unmigrated = conn.execute("""
        SELECT COUNT(*) FROM inventory_items ii
        WHERE NOT EXISTS (
            SELECT 1 FROM inventory_items_v2 WHERE legacy_inventory_id = ii.id
        )
    """).fetchone()[0]

    print(f"\n--- Count summary ---")
    print(f"master_knives (legacy):       {legacy_mod:4d}")
    print(f"knife_models_v2:              {v2_mod:4d}  (reporting_models view: {view_mod})")
    print(f"inventory_items (legacy):     {legacy_inv:4d}")
    print(f"inventory_items_v2:           {v2_inv:4d}  (reporting_inventory view: {view_inv})")
    print(f"v2 items with null model_id:  {orphan:4d}")
    print(f"legacy items without v2 row:  {unmigrated:4d}")

    if orphan > 0:
        print(f"FAIL: {orphan} inventory_items_v2 rows have NULL knife_model_id")
        ok = False
    else:
        print("OK  no orphaned v2 inventory rows")

    if unmigrated > 0:
        print(f"WARN: {unmigrated} legacy inventory items still have no v2 counterpart")
        print("      (check if their master_knife_id has a knife_models_v2 entry)")
    else:
        print("OK  all legacy inventory items are covered in v2")

    if view_inv != v2_inv:
        print(f"FAIL: reporting_inventory view ({view_inv}) does not match inventory_items_v2 ({v2_inv})")
        ok = False
    else:
        print("OK  reporting_inventory view count matches")

    if view_mod != v2_mod:
        print(f"FAIL: reporting_models view ({view_mod}) does not match knife_models_v2 ({v2_mod})")
        ok = False
    else:
        print("OK  reporting_models view count matches")

    return ok


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def write_provenance_snapshot(
    artifacts_dir: Path,
    counts: dict,
    dry_run: bool,
) -> None:
    if dry_run:
        print("\nDRY-RUN  skipping provenance snapshot write")
        return

    import subprocess

    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=_REPO_ROOT,
            text=True,
        ).strip()
    except Exception:
        commit = "unknown"

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    date_dir = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    snapshot_dir = artifacts_dir / "db_snapshots" / date_dir
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    # Compress the DB
    db_gz = snapshot_dir / "mkc_inventory.db.gz"
    with open(_DB_PATH, "rb") as fin, gzip.open(db_gz, "wb") as fout:
        shutil.copyfileobj(fin, fout)

    db_sha = sha256_file(db_gz)
    checksums_path = snapshot_dir / "SHA256SUMS.txt"
    with open(checksums_path, "w") as f:
        f.write(f"{db_sha}  mkc_inventory.db.gz\n")

    provenance = {
        "project_slug": "mkc-inventory-v2",
        "source_repo_url": "https://github.com/davechogan/mkc-inventory-v2",
        "source_commit": commit,
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "created_by": "tools/complete_migrate_v2.py",
        "artifact_set": "phase-b-v2-migration-complete",
        "counts": counts,
        "artifacts": [
            {"path": "db_snapshots/" + date_dir + "/mkc_inventory.db.gz", "sha256": db_sha},
        ],
    }
    prov_path = snapshot_dir / "provenance.json"
    with open(prov_path, "w") as f:
        json.dump(provenance, f, indent=2)

    print(f"\nOK  snapshot written to {snapshot_dir}")
    print(f"    {db_gz.name}: {db_sha[:16]}...")


def main() -> None:
    parser = argparse.ArgumentParser(description="Complete v2 migration and validate.")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without writing")
    parser.add_argument(
        "--artifacts-dir",
        default=str(_ARTIFACTS_DIR),
        help="Path to Artifacts/projects/mkc-inventory-v2",
    )
    args = parser.parse_args()

    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row

    print(f"DB: {_DB_PATH}")
    print(f"Dry-run: {args.dry_run}\n")

    print("--- Step 1: Assert v2 schema ---")
    assert_v2_schema(conn)

    print("\n--- Step 2: Fill inventory gaps ---")
    inserted = fill_inventory_gaps(conn, dry_run=args.dry_run)
    if inserted and not args.dry_run:
        conn.commit()
        print(f"    Committed {len(inserted)} new inventory_items_v2 rows")

    print("\n--- Step 3: Validate counts ---")
    ok = validate_counts(conn)

    counts = {
        "master_knives": conn.execute("SELECT COUNT(*) FROM master_knives").fetchone()[0],
        "inventory_items": conn.execute("SELECT COUNT(*) FROM inventory_items").fetchone()[0],
        "knife_models_v2": conn.execute("SELECT COUNT(*) FROM knife_models_v2").fetchone()[0],
        "inventory_items_v2": conn.execute("SELECT COUNT(*) FROM inventory_items_v2").fetchone()[0],
    }

    print("\n--- Step 4: Write provenance snapshot ---")
    write_provenance_snapshot(Path(args.artifacts_dir), counts, dry_run=args.dry_run)

    print("\n" + ("=" * 40))
    if ok:
        print("PASS  v2 migration complete and validated")
    else:
        print("FAIL  validation errors above must be resolved")
        sys.exit(1)


if __name__ == "__main__":
    main()
