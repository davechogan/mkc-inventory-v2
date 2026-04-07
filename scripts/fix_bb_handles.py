#!/usr/bin/env python3
"""Fix BB handle colors - set the two with red/black handles correctly.

BB Blackfoot 2.0 (cw=237) - Black Burl CF - handle is BLACK
BB Wargoat (cw=6) - Black Burl CF - handle is BLACK
BB Mini Speedgoat 2.0 (cw=238) - Paracord - handle is RED/BLACK
BB Speedgoat Ultra (cw=472) - Marbled CF - handle is RED/BLACK

Usage:
    PYTHONPATH=. .venv/bin/python scripts/fix_bb_handles.py [--apply]
"""
import sqlite3
import sys

DB_PATH = "data/mkc_inventory.db"
DRY_RUN = "--apply" not in sys.argv

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

# Check available handle colors
rows = conn.execute("SELECT id, name FROM handle_colors ORDER BY name").fetchall()
print("Available handle colors:")
for r in rows:
    print(f"  {r['id']:3d} {r['name']}")

# Find the IDs we need
black_id = None
red_black_id = None
for r in rows:
    if r["name"] == "Black":
        black_id = r["id"]
    if r["name"] == "Black/Red":
        red_black_id = r["id"]

print(f"\nBlack = {black_id}")
print(f"Black/Red or Red/Black = {red_black_id}")

if not red_black_id:
    # Need to create it or use Red
    red_id = None
    for r in rows:
        if r["name"] == "Red":
            red_id = r["id"]
    print(f"Red = {red_id}")
    print("\nNo Black/Red handle color found. Options:")
    print("  1. Use 'Red' for the red/black handles")
    print("  2. Create a new 'Black/Red' handle color")

    if not DRY_RUN:
        # The Black/Red entry exists (id=2 from earlier audit) but might have been removed
        # Let's check by ID
        row = conn.execute("SELECT id, name FROM handle_colors WHERE id = 2").fetchone()
        if row:
            print(f"\n  ID 2 = '{row['name']}' — using this")
            red_black_id = 2
        else:
            print("\n  Creating 'Black/Red' handle color...")
            conn.execute("INSERT INTO handle_colors (name) VALUES ('Black/Red')")
            conn.commit()
            red_black_id = conn.execute(
                "SELECT id FROM handle_colors WHERE name = 'Black/Red'"
            ).fetchone()["id"]
            print(f"  Created Black/Red with id={red_black_id}")

# Current state
print("\nCurrent BB colorways:")
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
    print(f"  cw={r['id']:4d}  {r['official_name']:45s}  handle={r['handle_color']:15s}  "
          f"blade={r['blade_color'] or '(none)':10s}  type={r['model_handle_type']}")

if DRY_RUN:
    print("\n[DRY RUN] Would update:")
    print(f"  cw=237 BB Blackfoot 2.0:      handle → Black (id={black_id})")
    print(f"  cw=6   BB Wargoat:            handle → Black (id={black_id})")
    print(f"  cw=238 BB Mini Speedgoat 2.0: handle → Black/Red (id={red_black_id})")
    print(f"  cw=472 BB Speedgoat Ultra:    handle → Black/Red (id={red_black_id})")
    print("\nRun with --apply to execute.")
elif red_black_id:
    # BB Blackfoot and BB Wargoat = Black handles
    conn.execute("UPDATE model_colorways SET handle_color_id = ? WHERE id IN (237, 6)", (black_id,))
    # BB Mini Speedgoat and BB Speedgoat Ultra = Red/Black handles
    conn.execute("UPDATE model_colorways SET handle_color_id = ? WHERE id IN (238, 472)", (red_black_id,))
    conn.commit()

    print("\nUpdated. Verifying:")
    rows = conn.execute("""
        SELECT mc.id, km.official_name, hc.name as handle_color, bc.name as blade_color
        FROM model_colorways mc
        JOIN knife_models_v2 km ON km.id = mc.knife_model_id
        LEFT JOIN handle_colors hc ON hc.id = mc.handle_color_id
        LEFT JOIN blade_colors bc ON bc.id = mc.blade_color_id
        WHERE km.official_name LIKE '%Blood Brothers%'
        ORDER BY km.official_name
    """).fetchall()
    for r in rows:
        print(f"  cw={r['id']:4d}  {r['official_name']:45s}  handle={r['handle_color']:15s}  blade={r['blade_color']}")
