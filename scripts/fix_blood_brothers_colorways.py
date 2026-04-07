#!/usr/bin/env python3
"""Fix Blood Brothers colorway data.

Problem: All 4 BB colorways have handle_color=Black/Red, blade_color=NULL.
Correct: handle_color=Black, blade_color=Red.

BB knives all have red cerakote blades. The handle_color "Black/Red" was
misinterpreted — it's not a two-tone handle, it's blade/handle from MKC's
internal naming. The actual handle colors are black (carbon fiber patterns).

Usage:
    PYTHONPATH=. .venv/bin/python scripts/fix_blood_brothers_colorways.py [--apply]
"""
import sqlite3
import sys

DB_PATH = "data/mkc_inventory.db"
DRY_RUN = "--apply" not in sys.argv


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Find the color IDs we need
    black_hc = conn.execute(
        "SELECT id FROM handle_colors WHERE name = 'Black'"
    ).fetchone()
    red_bc = conn.execute(
        "SELECT id FROM blade_colors WHERE name = 'Red'"
    ).fetchone()

    if not black_hc or not red_bc:
        print("ERROR: Could not find Black handle color or Red blade color")
        return

    black_hc_id = black_hc["id"]
    red_bc_id = red_bc["id"]
    print(f"Black handle_color_id = {black_hc_id}")
    print(f"Red blade_color_id = {red_bc_id}")

    # Find BB colorways
    rows = conn.execute("""
        SELECT mc.id, km.official_name, hc.name as handle_color, bc.name as blade_color
        FROM model_colorways mc
        JOIN knife_models_v2 km ON km.id = mc.knife_model_id
        LEFT JOIN handle_colors hc ON hc.id = mc.handle_color_id
        LEFT JOIN blade_colors bc ON bc.id = mc.blade_color_id
        WHERE km.official_name LIKE '%Blood Brothers%'
        ORDER BY km.official_name
    """).fetchall()

    print(f"\nFound {len(rows)} Blood Brothers colorways:")
    for r in rows:
        bc = r["blade_color"] or "(none)"
        print(f"  cw={r['id']:4d}  {r['official_name']:45s}  handle={r['handle_color']:15s}  blade={bc}")

    if DRY_RUN:
        print("\n[DRY RUN] Would update all BB colorways:")
        print(f"  handle_color_id: current → {black_hc_id} (Black)")
        print(f"  blade_color_id: NULL → {red_bc_id} (Red)")
        print("\nRun with --apply to execute.")
    else:
        for r in rows:
            conn.execute(
                "UPDATE model_colorways SET handle_color_id = ?, blade_color_id = ? WHERE id = ?",
                (black_hc_id, red_bc_id, r["id"]),
            )
            print(f"  Updated cw={r['id']}: handle→Black, blade→Red")
        conn.commit()
        print("\nDone. Verifying:")

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


if __name__ == "__main__":
    main()
