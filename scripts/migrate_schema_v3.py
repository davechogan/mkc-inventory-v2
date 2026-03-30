"""
DF-000 Schema normalization: rebuild knife_models_v2 and inventory_items_v2
with clean FK-only schema. Drops all legacy text columns.

Usage:
    .venv/bin/python3 scripts/migrate_schema_v3.py [--dry-run]

ALWAYS backup first: ./scripts/backup_mkc_db.sh
"""

import argparse
import os
import sqlite3
import sys

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "mkc_inventory.db")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def migrate(dry_run=False):
    conn = get_conn()

    # ── Step 1: Create new lookup tables ──────────────────────────────────
    print("\n=== Step 1: Create lookup tables ===")

    if not dry_run:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS locations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS handle_types (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            );
        """)

        # Seed handle_types from existing data
        for r in conn.execute(
            "SELECT DISTINCT handle_type FROM knife_models_v2 WHERE handle_type IS NOT NULL AND handle_type != ''"
        ):
            conn.execute("INSERT OR IGNORE INTO handle_types (name) VALUES (?)", (r["handle_type"],))

        print(f"  handle_types: {conn.execute('SELECT COUNT(*) as c FROM handle_types').fetchone()['c']} rows")
        print(f"  locations: {conn.execute('SELECT COUNT(*) as c FROM locations').fetchone()['c']} rows")
    else:
        print("  [DRY RUN] Would create locations and handle_types tables")

    # ── Step 2: Add handle_type_id to knife_models_v2 and backfill ────────
    print("\n=== Step 2: Backfill handle_type_id ===")

    # Check if column exists
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(knife_models_v2)")]
    if "handle_type_id" not in cols:
        if not dry_run:
            conn.execute("ALTER TABLE knife_models_v2 ADD COLUMN handle_type_id INTEGER REFERENCES handle_types(id)")
            conn.execute("""
                UPDATE knife_models_v2 SET handle_type_id = (
                    SELECT ht.id FROM handle_types ht WHERE ht.name = knife_models_v2.handle_type
                ) WHERE handle_type IS NOT NULL AND handle_type != ''
            """)
            print(f"  Backfilled handle_type_id")
        else:
            print("  [DRY RUN] Would add and backfill handle_type_id")
    else:
        print("  handle_type_id already exists")

    # ── Step 3: Map inventory items to colorway_id ────────────────────────
    print("\n=== Step 3: Map inventory → colorway_id ===")

    inv_cols = [r["name"] for r in conn.execute("PRAGMA table_info(inventory_items_v2)")]
    if "colorway_id" not in inv_cols:
        if not dry_run:
            conn.execute("ALTER TABLE inventory_items_v2 ADD COLUMN colorway_id INTEGER REFERENCES model_colorways(id)")
        else:
            print("  [DRY RUN] Would add colorway_id column")

    # Map each inventory item to its colorway
    items = conn.execute("""
        SELECT i.id, i.knife_model_id, i.handle_color, i.blade_color
        FROM inventory_items_v2 i
        WHERE i.handle_color IS NOT NULL AND i.handle_color != ''
    """).fetchall()

    mapped = 0
    unmapped = []
    for item in items:
        cw = None
        hc = item["handle_color"]
        bc = item["blade_color"]
        mid = item["knife_model_id"]

        # Try exact match first (handle + blade color)
        if bc:
            cw = conn.execute("""
                SELECT mc.id FROM model_colorways mc
                JOIN handle_colors hc ON hc.id = mc.handle_color_id
                LEFT JOIN blade_colors blc ON blc.id = mc.blade_color_id
                WHERE mc.knife_model_id = ?
                  AND LOWER(hc.name) = LOWER(?)
                  AND LOWER(blc.name) = LOWER(?)
                LIMIT 1
            """, (mid, hc, bc)).fetchone()

        # Fallback: match handle color only (ignore blade color — most colorways don't store it)
        if not cw:
            cw = conn.execute("""
                SELECT mc.id FROM model_colorways mc
                JOIN handle_colors hc ON hc.id = mc.handle_color_id
                WHERE mc.knife_model_id = ?
                  AND LOWER(hc.name) = LOWER(?)
                LIMIT 1
            """, (mid, hc)).fetchone()

        if cw:
            if not dry_run:
                conn.execute("UPDATE inventory_items_v2 SET colorway_id = ? WHERE id = ?", (cw["id"], item["id"]))
            mapped += 1
        else:
            unmapped.append(f"inv={item['id']} handle={item['handle_color']} blade={item['blade_color']}")

    print(f"  Mapped: {mapped}/{len(items)}")
    if unmapped:
        print(f"  Unmapped: {len(unmapped)}")
        for u in unmapped:
            print(f"    WARNING: {u}")

    # ── Step 4: Add location_id to inventory ──────────────────────────────
    print("\n=== Step 4: Add location_id ===")
    if "location_id" not in inv_cols:
        if not dry_run:
            conn.execute("ALTER TABLE inventory_items_v2 ADD COLUMN location_id INTEGER REFERENCES locations(id)")
            # Backfill from existing location text (if any)
            locs = conn.execute(
                "SELECT DISTINCT location FROM inventory_items_v2 WHERE location IS NOT NULL AND location != ''"
            ).fetchall()
            for loc in locs:
                conn.execute("INSERT OR IGNORE INTO locations (name) VALUES (?)", (loc["location"],))
                conn.execute("""
                    UPDATE inventory_items_v2 SET location_id = (
                        SELECT l.id FROM locations l WHERE l.name = inventory_items_v2.location
                    ) WHERE location = ?
                """, (loc["location"],))
            print(f"  Added location_id, backfilled {len(locs)} distinct locations")
        else:
            print("  [DRY RUN] Would add location_id")
    else:
        print("  location_id already exists")

    # ── Step 5: Rebuild knife_models_v2 ───────────────────────────────────
    print("\n=== Step 5: Rebuild knife_models_v2 ===")

    if not dry_run:
        conn.execute("PRAGMA foreign_keys = OFF")

        # Drop views that reference the old tables
        conn.execute("DROP VIEW IF EXISTS reporting_inventory")
        conn.execute("DROP VIEW IF EXISTS reporting_models")

        conn.execute("""
            CREATE TABLE knife_models_v3 (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                official_name   TEXT NOT NULL,
                sortable_name   TEXT,
                slug            TEXT UNIQUE,
                type_id         INTEGER REFERENCES knife_types(id),
                family_id       INTEGER REFERENCES knife_families(id),
                form_id         INTEGER REFERENCES knife_forms(id),
                series_id       INTEGER REFERENCES knife_series(id),
                collaborator_id INTEGER REFERENCES collaborators(id),
                parent_model_id INTEGER REFERENCES knife_models_v2(id),
                steel_id        INTEGER REFERENCES blade_steels(id),
                blade_finish_id INTEGER REFERENCES blade_finishes(id),
                handle_type_id  INTEGER REFERENCES handle_types(id),
                blade_length    REAL,
                msrp            REAL,
                official_product_url TEXT,
                model_notes     TEXT,
                created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.execute("""
            INSERT INTO knife_models_v3 (
                id, official_name, sortable_name, slug,
                type_id, family_id, form_id, series_id, collaborator_id, parent_model_id,
                steel_id, blade_finish_id, handle_type_id,
                blade_length, msrp, official_product_url, model_notes,
                created_at, updated_at
            )
            SELECT
                id, official_name, sortable_name, slug,
                type_id, family_id, form_id, series_id, collaborator_id, parent_model_id,
                steel_id, blade_finish_id, handle_type_id,
                blade_length, msrp, official_product_url, notes,
                created_at, updated_at
            FROM knife_models_v2
        """)

        old_count = conn.execute("SELECT COUNT(*) as c FROM knife_models_v2").fetchone()["c"]
        new_count = conn.execute("SELECT COUNT(*) as c FROM knife_models_v3").fetchone()["c"]
        assert old_count == new_count, f"Row count mismatch: {old_count} vs {new_count}"

        conn.execute("DROP TABLE knife_models_v2")
        conn.execute("ALTER TABLE knife_models_v3 RENAME TO knife_models_v2")

        conn.execute("PRAGMA foreign_keys = ON")
        print(f"  Rebuilt knife_models_v2: {new_count} rows, 19 columns")
    else:
        print("  [DRY RUN] Would rebuild knife_models_v2 with 19 columns")

    # ── Step 6: Rebuild inventory_items_v2 ────────────────────────────────
    print("\n=== Step 6: Rebuild inventory_items_v2 ===")

    if not dry_run:
        conn.execute("PRAGMA foreign_keys = OFF")

        conn.execute("""
            CREATE TABLE inventory_items_v3 (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                knife_model_id  INTEGER NOT NULL REFERENCES knife_models_v2(id),
                colorway_id     INTEGER REFERENCES model_colorways(id),
                quantity        INTEGER NOT NULL DEFAULT 1,
                purchase_price  REAL,
                acquired_date   TEXT,
                mkc_order_number TEXT,
                location_id     INTEGER REFERENCES locations(id),
                notes           TEXT,
                created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.execute("""
            INSERT INTO inventory_items_v3 (
                id, knife_model_id, colorway_id, quantity, purchase_price,
                acquired_date, mkc_order_number, location_id, notes,
                created_at, updated_at
            )
            SELECT
                id, knife_model_id, colorway_id, quantity, purchase_price,
                acquired_date, mkc_order_number, location_id, notes,
                COALESCE(created_at, CURRENT_TIMESTAMP),
                COALESCE(updated_at, CURRENT_TIMESTAMP)
            FROM inventory_items_v2
        """)

        old_count = conn.execute("SELECT COUNT(*) as c FROM inventory_items_v2").fetchone()["c"]
        new_count = conn.execute("SELECT COUNT(*) as c FROM inventory_items_v3").fetchone()["c"]
        assert old_count == new_count, f"Row count mismatch: {old_count} vs {new_count}"

        conn.execute("DROP TABLE inventory_items_v2")
        conn.execute("ALTER TABLE inventory_items_v3 RENAME TO inventory_items_v2")

        conn.execute("PRAGMA foreign_keys = ON")
        print(f"  Rebuilt inventory_items_v2: {new_count} rows, 11 columns")
    else:
        print("  [DRY RUN] Would rebuild inventory_items_v2 with 11 columns")

    # ── Step 7: Summary ───────────────────────────────────────────────────
    print("\n=== Summary ===")
    if not dry_run:
        conn.commit()

        # Verify column counts
        km_cols = [r["name"] for r in conn.execute("PRAGMA table_info(knife_models_v2)")]
        inv_cols = [r["name"] for r in conn.execute("PRAGMA table_info(inventory_items_v2)")]
        print(f"  knife_models_v2: {len(km_cols)} columns: {', '.join(km_cols)}")
        print(f"  inventory_items_v2: {len(inv_cols)} columns: {', '.join(inv_cols)}")

        # Verify no null colorway_ids where there should be one
        null_cw = conn.execute(
            "SELECT COUNT(*) as c FROM inventory_items_v2 WHERE colorway_id IS NULL"
        ).fetchone()["c"]
        print(f"  Inventory items with null colorway_id: {null_cw}")

        print("\n  Migration committed.")
    else:
        print("  [DRY RUN] No changes made.")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DF-000: Rebuild tables with clean FK schema")
    parser.add_argument("--dry-run", action="store_true", help="Preview without making changes")
    args = parser.parse_args()
    migrate(dry_run=args.dry_run)
