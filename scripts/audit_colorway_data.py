#!/usr/bin/env python3
"""Audit colorway data for blade/handle color misassignment.

MKC naming convention: "Red/Black" means Red blade, Black handle.
"COY/OD" means Coyote blade, OD Green handle.
Check if these were split correctly during migration.

Usage:
    PYTHONPATH=. .venv/bin/python scripts/audit_colorway_data.py
"""
import sqlite3

DB_PATH = "data/mkc_inventory.db"

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

# 1. All handle_colors with a slash
print("=== Handle colors with '/' (may be blade/handle combos) ===")
rows = conn.execute("""
    SELECT hc.id, hc.name, COUNT(mc.id) as cw_count
    FROM handle_colors hc
    LEFT JOIN model_colorways mc ON mc.handle_color_id = hc.id
    GROUP BY hc.id
    ORDER BY hc.name
""").fetchall()
for r in rows:
    marker = " <-- SLASH" if "/" in r["name"] else ""
    print(f"  {r['id']:3d} {r['name']:25s} ({r['cw_count']} colorways){marker}")

print()

# 2. All blade_colors
print("=== Blade colors ===")
rows = conn.execute("""
    SELECT bc.id, bc.name, COUNT(mc.id) as cw_count
    FROM blade_colors bc
    LEFT JOIN model_colorways mc ON mc.blade_color_id = bc.id
    GROUP BY bc.id
    ORDER BY bc.name
""").fetchall()
for r in rows:
    print(f"  {r['id']:3d} {r['name']:25s} ({r['cw_count']} colorways)")

print()

# 3. Show all colorways with slash handle_color — are they misassigned?
print("=== Colorways with slash handle_color (potential misassignments) ===")
rows = conn.execute("""
    SELECT mc.id, km.official_name, hc.name as handle_color, bc.name as blade_color
    FROM model_colorways mc
    JOIN knife_models_v2 km ON km.id = mc.knife_model_id
    LEFT JOIN handle_colors hc ON hc.id = mc.handle_color_id
    LEFT JOIN blade_colors bc ON bc.id = mc.blade_color_id
    WHERE hc.name LIKE '%/%'
    ORDER BY hc.name, km.official_name
""").fetchall()
prev_hc = None
for r in rows:
    hc = r["handle_color"]
    if hc != prev_hc:
        print(f"\n  --- {hc} ---")
        # Parse: first part = blade, second = handle
        parts = hc.split("/")
        if len(parts) == 2:
            print(f"  If MKC convention: blade={parts[0]}, handle={parts[1]}")
        prev_hc = hc
    bc = r["blade_color"] or "(none)"
    print(f"    cw={r['id']:4d}  {r['official_name']:45s}  blade_color_in_db={bc}")

print()

# 4. Summary: how many colorways have NULL blade_color?
row = conn.execute("""
    SELECT COUNT(*) as total,
           SUM(CASE WHEN blade_color_id IS NULL THEN 1 ELSE 0 END) as null_blade,
           SUM(CASE WHEN blade_color_id IS NOT NULL THEN 1 ELSE 0 END) as has_blade
    FROM model_colorways
""").fetchone()
print(f"=== Blade color coverage ===")
print(f"  Total colorways: {row['total']}")
print(f"  With blade_color: {row['has_blade']}")
print(f"  NULL blade_color: {row['null_blade']}")

print()

# 5. Blood Brothers specifically
print("=== Blood Brothers colorways ===")
rows = conn.execute("""
    SELECT mc.id, km.official_name, hc.name as handle_color, bc.name as blade_color,
           ht.name as model_handle_type
    FROM model_colorways mc
    JOIN knife_models_v2 km ON km.id = mc.knife_model_id
    LEFT JOIN handle_colors hc ON hc.id = mc.handle_color_id
    LEFT JOIN blade_colors bc ON bc.id = mc.blade_color_id
    LEFT JOIN handle_types ht ON ht.id = km.handle_type_id
    WHERE km.official_name LIKE '%Blood Brothers%'
    ORDER BY km.official_name
""").fetchall()
for r in rows:
    bc = r["blade_color"] or "(none)"
    print(f"  {r['official_name']:45s} handle_color={r['handle_color']:15s} blade_color={bc:10s} handle_type={r['model_handle_type']}")
