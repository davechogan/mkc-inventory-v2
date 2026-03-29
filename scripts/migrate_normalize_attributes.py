#!/usr/bin/env python3
"""
Normalize free-text attribute columns on knife_models_v2 and inventory_items_v2.

Creates proper lookup tables (handle_colors, blade_colors, blade_steels,
blade_finishes, conditions), seeds them from v2_option_values (plus any
values present in the data but missing from options), adds *_id FK columns
to the main tables, and backfills them.

Safe to re-run: uses IF NOT EXISTS / INSERT OR IGNORE / UPDATE … WHERE id IS NULL.
"""
import sqlite3
import sys
from pathlib import Path

DB = Path(__file__).parent.parent / "data" / "mkc_inventory.db"

LOOKUP_TABLES = [
    # (new_table,       option_type,      text_col_on_models,     text_col_on_inventory)
    ("handle_colors",  "handle-colors",  "handle_color",         "handle_color"),
    ("blade_colors",   "blade-colors",   "blade_color",          "blade_color"),
    ("blade_steels",   "blade-steels",   "steel",                "steel"),
    ("blade_finishes", "blade-finishes", "blade_finish",         "blade_finish"),
    ("conditions",     "conditions",     None,                   "condition"),
]

def run():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = OFF")   # off during migration
    conn.execute("PRAGMA journal_mode = WAL")

    # ── 1. Create lookup tables ───────────────────────────────────────────────
    for table, _, _, _ in LOOKUP_TABLES:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id   INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT    NOT NULL UNIQUE COLLATE NOCASE
            )
        """)
    print("Lookup tables created (or already exist).")

    # ── 2. Seed from v2_option_values ────────────────────────────────────────
    for table, option_type, _, _ in LOOKUP_TABLES:
        rows = conn.execute(
            "SELECT name FROM v2_option_values WHERE option_type = ? ORDER BY name COLLATE NOCASE",
            (option_type,)
        ).fetchall()
        inserted = 0
        for r in rows:
            cur = conn.execute(
                f"INSERT OR IGNORE INTO {table} (name) VALUES (?)", (r["name"],)
            )
            inserted += cur.rowcount
        print(f"  {table}: seeded {inserted} new row(s) from v2_option_values.")

    # ── 3. Seed any values present in the data but not in options ────────────
    extra_seeds = [
        # (table, values_to_ensure)
        ("blade_colors", ["Coyote"]),   # valid tactical blade color, confirmed by user
    ]
    for table, values in extra_seeds:
        for v in values:
            cur = conn.execute(f"INSERT OR IGNORE INTO {table} (name) VALUES (?)", (v,))
            if cur.rowcount:
                print(f"  {table}: added extra value {v!r}")

    # Also sweep the data tables for any values not yet in lookup tables
    for table, _, models_col, inv_col in LOOKUP_TABLES:
        for src_table, col in [("knife_models_v2", models_col), ("inventory_items_v2", inv_col)]:
            if not col:
                continue
            rows = conn.execute(
                f"SELECT DISTINCT {col} FROM {src_table} WHERE {col} IS NOT NULL AND {col} != ''"
            ).fetchall()
            for r in rows:
                cur = conn.execute(f"INSERT OR IGNORE INTO {table} (name) VALUES (?)", (r[0],))
                if cur.rowcount:
                    print(f"  {table}: added data-only value {r[0]!r} from {src_table}.{col}")

    # ── 4. Add *_id FK columns to main tables (idempotent) ───────────────────
    existing_cols = {
        "knife_models_v2":    {r["name"] for r in conn.execute("PRAGMA table_info(knife_models_v2)")},
        "inventory_items_v2": {r["name"] for r in conn.execute("PRAGMA table_info(inventory_items_v2)")},
    }

    fk_columns = [
        # (main_table,           new_col,           lookup_table)
        ("knife_models_v2",    "handle_color_id",  "handle_colors"),
        ("knife_models_v2",    "blade_color_id",   "blade_colors"),
        ("knife_models_v2",    "steel_id",         "blade_steels"),
        ("knife_models_v2",    "blade_finish_id",  "blade_finishes"),
        ("inventory_items_v2", "handle_color_id",  "handle_colors"),
        ("inventory_items_v2", "blade_color_id",   "blade_colors"),
        ("inventory_items_v2", "steel_id",         "blade_steels"),
        ("inventory_items_v2", "blade_finish_id",  "blade_finishes"),
        ("inventory_items_v2", "condition_id",     "conditions"),
    ]

    for main_table, new_col, lookup_table in fk_columns:
        if new_col not in existing_cols[main_table]:
            conn.execute(
                f"ALTER TABLE {main_table} ADD COLUMN {new_col} INTEGER REFERENCES {lookup_table}(id)"
            )
            print(f"  Added column {main_table}.{new_col}")
        else:
            print(f"  Column {main_table}.{new_col} already exists, skipping.")

    # ── 5. Backfill *_id columns from text values ─────────────────────────────
    backfills = [
        ("knife_models_v2",    "handle_color_id",  "handle_colors",  "handle_color"),
        ("knife_models_v2",    "blade_color_id",   "blade_colors",   "blade_color"),
        ("knife_models_v2",    "steel_id",         "blade_steels",   "steel"),
        ("knife_models_v2",    "blade_finish_id",  "blade_finishes", "blade_finish"),
        ("inventory_items_v2", "handle_color_id",  "handle_colors",  "handle_color"),
        ("inventory_items_v2", "blade_color_id",   "blade_colors",   "blade_color"),
        ("inventory_items_v2", "steel_id",         "blade_steels",   "steel"),
        ("inventory_items_v2", "blade_finish_id",  "blade_finishes", "blade_finish"),
        ("inventory_items_v2", "condition_id",     "conditions",     "condition"),
    ]

    for main_table, id_col, lookup_table, text_col in backfills:
        cur = conn.execute(f"""
            UPDATE {main_table}
            SET {id_col} = (
                SELECT id FROM {lookup_table}
                WHERE name = {main_table}.{text_col} COLLATE NOCASE
            )
            WHERE {text_col} IS NOT NULL AND {text_col} != ''
        """)
        print(f"  Backfilled {cur.rowcount} rows in {main_table}.{id_col}")

    # ── 6. Verification ───────────────────────────────────────────────────────
    print("\nVerification:")
    for main_table, id_col, lookup_table, text_col in backfills:
        # Rows with a text value but no resolved ID
        unmatched = conn.execute(f"""
            SELECT COUNT(*) FROM {main_table}
            WHERE {text_col} IS NOT NULL AND {text_col} != ''
              AND {id_col} IS NULL
        """).fetchone()[0]
        if unmatched:
            print(f"  WARNING: {main_table}.{id_col} — {unmatched} rows have text but no ID match")
            for r in conn.execute(f"""
                SELECT DISTINCT {text_col} FROM {main_table}
                WHERE {text_col} IS NOT NULL AND {text_col} != '' AND {id_col} IS NULL
            """).fetchall():
                print(f"    unmatched: {r[0]!r}")
        else:
            print(f"  OK  {main_table}.{id_col}")

    conn.commit()
    conn.execute("PRAGMA foreign_keys = ON")
    print("\nMigration complete.")

if __name__ == "__main__":
    run()
