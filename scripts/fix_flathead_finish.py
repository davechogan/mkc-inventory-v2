#!/usr/bin/env python3
"""Fix Flathead Fillet knives: both are Cerakote with Black blades."""
import sqlite3
conn = sqlite3.connect("data/mkc_inventory.db")
conn.row_factory = sqlite3.Row

cerakote_id = conn.execute("SELECT id FROM blade_finishes WHERE name = 'Cerakote'").fetchone()["id"]
black_id = conn.execute("SELECT id FROM blade_colors WHERE name = 'Black'").fetchone()["id"]

rows = conn.execute("SELECT id, official_name FROM knife_models_v2 WHERE official_name LIKE '%Flathead%'").fetchall()
for r in rows:
    conn.execute("UPDATE knife_models_v2 SET blade_finish_id = ?, blade_color_id = ? WHERE id = ?",
                 (cerakote_id, black_id, r["id"]))
    print(f"  {r['official_name']}: finish→Cerakote, blade_color→Black")
conn.commit()
print("Done.")
