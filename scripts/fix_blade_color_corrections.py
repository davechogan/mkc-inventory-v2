#!/usr/bin/env python3
"""Fix blade color and finish corrections from user review.

1. Flathead Fillet: blade_color Steel → Black (satin finish but dark blade)
2. Stoned Goat 2.0: finish Stonewashed → PVD, blade_color Steel → Black
3. Stoned Goat 2.0 VIP: finish Stonewashed → PVD, blade_color Steel → Black
4. Archery Country Speedgoat: blade_color Distressed Gray → Steel

Usage:
    PYTHONPATH=. .venv/bin/python scripts/fix_blade_color_corrections.py [--apply]
"""
import sqlite3
import sys

DB_PATH = "data/mkc_inventory.db"
DRY_RUN = "--apply" not in sys.argv

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

# Get IDs
black_bc = conn.execute("SELECT id FROM blade_colors WHERE name = 'Black'").fetchone()["id"]
steel_bc = conn.execute("SELECT id FROM blade_colors WHERE name = 'Steel'").fetchone()["id"]
pvd_bf = conn.execute("SELECT id FROM blade_finishes WHERE name = 'PVD'").fetchone()["id"]

print(f"Black blade_color_id = {black_bc}")
print(f"Steel blade_color_id = {steel_bc}")
print(f"PVD blade_finish_id = {pvd_bf}")

corrections = []

# 1. Flathead Fillet: blade_color → Black
row = conn.execute("SELECT id FROM knife_models_v2 WHERE official_name = 'Flathead Fillet'").fetchone()
if row:
    corrections.append((row["id"], "Flathead Fillet", {"blade_color_id": black_bc}, "blade_color Steel→Black"))

# 2. Stoned Goat 2.0: finish → PVD, blade_color → Black
row = conn.execute("SELECT id FROM knife_models_v2 WHERE official_name = 'Stoned Goat 2.0'").fetchone()
if row:
    corrections.append((row["id"], "Stoned Goat 2.0", {"blade_color_id": black_bc, "blade_finish_id": pvd_bf}, "finish→PVD, blade_color→Black"))

# 3. Stoned Goat 2.0 VIP: finish → PVD, blade_color → Black
row = conn.execute("SELECT id FROM knife_models_v2 WHERE official_name = 'Stoned Goat 2.0 VIP'").fetchone()
if row:
    corrections.append((row["id"], "Stoned Goat 2.0 VIP", {"blade_color_id": black_bc, "blade_finish_id": pvd_bf}, "finish→PVD, blade_color→Black"))

# 4. Archery Country: blade_color Distressed Gray → Steel
row = conn.execute("SELECT id FROM knife_models_v2 WHERE official_name LIKE '%Archery Country%'").fetchone()
if row:
    corrections.append((row["id"], "Archery Country Speedgoat", {"blade_color_id": steel_bc}, "blade_color Distressed Gray→Steel"))

print(f"\nCorrections to apply: {len(corrections)}")
for mid, name, updates, desc in corrections:
    print(f"  {name:45s} → {desc}")

if DRY_RUN:
    print("\n[DRY RUN] Run with --apply to execute.")
else:
    for mid, name, updates, desc in corrections:
        set_clauses = ", ".join(f"{k} = ?" for k in updates)
        params = list(updates.values()) + [mid]
        conn.execute(f"UPDATE knife_models_v2 SET {set_clauses} WHERE id = ?", params)
    conn.commit()
    print("\nApplied. Verifying:")
    for mid, name, _, _ in corrections:
        r = conn.execute("""
            SELECT km.official_name, bf.name as finish, bc.name as blade_color
            FROM knife_models_v2 km
            LEFT JOIN blade_finishes bf ON bf.id = km.blade_finish_id
            LEFT JOIN blade_colors bc ON bc.id = km.blade_color_id
            WHERE km.id = ?
        """, (mid,)).fetchone()
        print(f"  {r['official_name']:45s} finish={r['finish']:20s} blade_color={r['blade_color']}")
