#!/usr/bin/env python3
"""Backfill blade_color_id on knife_models_v2 for models where blade color is a constant.

Rules:
- Tactical models with multiple blade color options: SKIP (blade_color stays on colorways)
- Models that already have blade_color_id set: SKIP
- PVD / Black Parkerized / Cerakote (non-tactical, non-BB) → Black
- Stonewashed / Satin / Polished / Working Grind → Steel
- Blood Brothers → Red (already on colorways, also set on model for clarity)

Usage:
    PYTHONPATH=. .venv/bin/python migrations/backfill_model_blade_color.py [--apply]
"""
import sqlite3
import sys

DB_PATH = "data/mkc_inventory.db"
DRY_RUN = "--apply" not in sys.argv


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Get blade color IDs
    colors = {}
    for row in conn.execute("SELECT id, name FROM blade_colors"):
        colors[row["name"]] = row["id"]

    print("Blade color IDs:", {k: v for k, v in colors.items()})
    black_id = colors["Black"]
    steel_id = colors["Steel"]
    red_id = colors["Red"]

    damascus_id = colors["Damascus Wood Grain"]
    distressed_id = colors["Distressed Gray"]

    # Finish → blade color mapping
    finish_to_color = {
        "PVD": black_id,
        "Black Parkerized": black_id,
        "Cerakote": black_id,  # Default for non-tactical, non-BB cerakote
        "Stonewashed": steel_id,
        "Satin": steel_id,
        "Polished": steel_id,
        "Working Grind": steel_id,
        "Etched": damascus_id,
    }

    # Models that have MULTIPLE blade colors on colorways (tactical) — skip these
    tactical_multi = conn.execute("""
        SELECT km.id FROM knife_models_v2 km
        JOIN model_colorways mc ON mc.knife_model_id = km.id
        JOIN blade_colors bc ON bc.id = mc.blade_color_id
        GROUP BY km.id HAVING COUNT(DISTINCT bc.id) > 1
    """).fetchall()
    skip_ids = {r["id"] for r in tactical_multi}
    print(f"\nSkipping {len(skip_ids)} tactical models with variable blade colors")

    # Blood Brothers — always Red
    bb_rows = conn.execute("""
        SELECT id, official_name FROM knife_models_v2
        WHERE official_name LIKE '%Blood Brothers%'
    """).fetchall()

    # Models with a single blade_color already on colorways — use that
    single_color_models = {}
    for row in conn.execute("""
        SELECT km.id, bc.id as bc_id, bc.name as bc_name
        FROM knife_models_v2 km
        JOIN model_colorways mc ON mc.knife_model_id = km.id
        JOIN blade_colors bc ON bc.id = mc.blade_color_id
        GROUP BY km.id HAVING COUNT(DISTINCT bc.id) = 1
    """).fetchall():
        single_color_models[row["id"]] = (row["bc_id"], row["bc_name"])

    # All other models
    rows = conn.execute("""
        SELECT km.id, km.official_name, km.blade_color_id, bf.name as finish
        FROM knife_models_v2 km
        LEFT JOIN blade_finishes bf ON bf.id = km.blade_finish_id
        ORDER BY km.official_name
    """).fetchall()

    updates = []
    skipped = 0
    already_set = 0

    for r in rows:
        model_id = r["id"]
        name = r["official_name"]
        current = r["blade_color_id"]
        finish = r["finish"]

        if model_id in skip_ids:
            skipped += 1
            continue

        if current is not None:
            already_set += 1
            continue

        # Blood Brothers → Red
        if "Blood Brothers" in name:
            updates.append((model_id, name, red_id, "Red", "Blood Brothers"))
            continue

        # If colorway has a single blade color, use that
        if model_id in single_color_models:
            bc_id, bc_name = single_color_models[model_id]
            updates.append((model_id, name, bc_id, bc_name, f"colorway ({bc_name})"))
            continue

        # Map by finish
        color_id = finish_to_color.get(finish)
        if color_id:
            color_names = {black_id: "Black", steel_id: "Steel",
                           damascus_id: "Damascus Wood Grain", distressed_id: "Distressed Gray"}
            color_name = color_names.get(color_id, "?")
            updates.append((model_id, name, color_id, color_name, finish))
        else:
            print(f"  WARNING: No mapping for finish '{finish}' on {name}")

    print(f"\nAlready set: {already_set}")
    print(f"Skipped (tactical): {skipped}")
    print(f"To update: {len(updates)}")
    print()

    for model_id, name, color_id, color_name, reason in updates:
        print(f"  {name:45s} → {color_name:8s} (from {reason})")

    if DRY_RUN:
        print(f"\n[DRY RUN] Run with --apply to execute {len(updates)} updates.")
    else:
        for model_id, name, color_id, color_name, reason in updates:
            conn.execute(
                "UPDATE knife_models_v2 SET blade_color_id = ? WHERE id = ?",
                (color_id, model_id),
            )
        conn.commit()
        print(f"\nApplied {len(updates)} updates.")

        # Verify
        row = conn.execute("""
            SELECT COUNT(*) as c FROM knife_models_v2 WHERE blade_color_id IS NULL
        """).fetchone()
        print(f"Models still without blade_color_id: {row['c']}")


if __name__ == "__main__":
    main()
