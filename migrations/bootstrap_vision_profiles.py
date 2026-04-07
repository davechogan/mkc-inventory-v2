#!/usr/bin/env python3
"""Phase 2: Bootstrap knife_model_vision_profiles for all models.

Pre-populates catalog_alignment from existing FK relationships and sets
observable features that can be inferred from catalog data (not vision).

Safe to run multiple times — uses INSERT OR IGNORE.
"""

import json
import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "mkc_inventory.db"


def bootstrap(conn: sqlite3.Connection):
    """Create a vision profile row for every model, populated from catalog truth."""

    rows = conn.execute("""
        SELECT km.id, km.official_name,
               km.family_id, km.type_id, km.form_id, km.series_id,
               km.handle_type_id, km.blade_finish_id, km.blade_length,
               kt.name AS knife_type,
               frm.name AS form_name,
               ht.name AS handle_type,
               bf.name AS blade_finish,
               fam.name AS family_name
        FROM knife_models_v2 km
        LEFT JOIN knife_types kt ON kt.id = km.type_id
        LEFT JOIN knife_forms frm ON frm.id = km.form_id
        LEFT JOIN handle_types ht ON ht.id = km.handle_type_id
        LEFT JOIN blade_finishes bf ON bf.id = km.blade_finish_id
        LEFT JOIN knife_families fam ON fam.id = km.family_id
        ORDER BY km.official_name
    """).fetchall()

    inserted = 0
    for r in rows:
        # Catalog alignment — direct from FK values
        catalog_alignment = {
            "family_id": r["family_id"],
            "type_id": r["type_id"],
            "form_id": r["form_id"],
            "series_id": r["series_id"],
            "handle_type_id": r["handle_type_id"],
            "blade_finish_id": r["blade_finish_id"],
        }

        # Observable features we can infer from catalog data (not vision)
        observable = {}

        # Handle type: map catalog handle_type to visual category
        ht = r["handle_type"] or ""
        if ht == "Paracord":
            observable["handle_type_visual"] = "paracord"
        elif ht in ("G-10", "Burled Carbon Fiber", "Marbled Carbon Fiber"):
            observable["handle_type_visual"] = "scales"
        elif ht in ("Desert Ironwood", "Desert Ironwood Burl"):
            observable["handle_type_visual"] = "wood"
        else:
            observable["handle_type_visual"] = "unknown"

        # Hatchet: from blade form
        observable["is_hatchet"] = r["form_name"] == "Hatchet"

        # Kitchen vs field: from knife type
        kt = r["knife_type"] or ""
        if kt == "Culinary":
            observable["knife_type_visual"] = "kitchen"
        elif kt in ("Hunting", "Hunting / Fishing", "Tactical", "Bushcraft & Camp", "Everyday Carry"):
            observable["knife_type_visual"] = "field"
        else:
            observable["knife_type_visual"] = "unknown"

        # Blade wider than handle: only for cleaver
        observable["blade_wider_than_handle"] = r["form_name"] == "Cleaver"

        # Blade-to-handle ratio: estimate from blade length
        bl = r["blade_length"]
        if bl is not None:
            if bl >= 7.0:
                observable["blade_to_handle_ratio_bucket"] = "much_longer"
            elif bl >= 5.5:
                observable["blade_to_handle_ratio_bucket"] = "longer"
            elif bl >= 3.5:
                observable["blade_to_handle_ratio_bucket"] = "similar"
            elif bl >= 2.5:
                observable["blade_to_handle_ratio_bucket"] = "shorter"
            else:
                observable["blade_to_handle_ratio_bucket"] = "much_shorter"
        else:
            observable["blade_to_handle_ratio_bucket"] = "unknown"

        # Blade finish visual: map catalog finish to visual category
        bf = r["blade_finish"] or ""
        if bf in ("Satin", "Polished", "Stonewashed", "Working Grind", "Etched"):
            observable["blade_finish_visual"] = "silver_satin"
        elif bf in ("PVD", "Black Parkerized"):
            observable["blade_finish_visual"] = "black_dark"
        elif bf == "Cerakote":
            # Cerakote can be black or coyote — mark as unknown, vision will resolve
            observable["blade_finish_visual"] = "unknown"
        else:
            observable["blade_finish_visual"] = "unknown"

        # Semantic soft label from catalog form
        semantic = {}
        form = r["form_name"] or ""
        form_map = {
            "Drop Point": "drop_point", "Trailing Point": "trailing_point",
            "Clip Point": "clip_point", "Sheepsfoot": "sheepsfoot",
            "Spear Point": "spear_point", "Dagger": "dagger",
            "Hawkbill": "hawkbill", "Skinner": "skinner",
            "Fillet": "fillet", "Chef": "chef",
            "Santoku": "santoku", "Petty": "petty",
            "Paring": "paring", "Cleaver": "cleaver",
            "Hatchet": "hatchet",
        }
        semantic["blade_style_soft_label"] = form_map.get(form, "unknown")

        # All remaining observable features start as unknown — Phase 3 (vision) will fill them
        for key in [
            "lanyard_hole_presence", "lanyard_hole_shape", "handle_fastener_count_visible",
            "choil_presence", "choil_depth", "guard_prominence",
            "jimping_presence", "jimping_location",
            "handle_profile_primary", "handle_butt_shape", "handle_texture_visual",
            "spine_profile_primary", "edge_profile_primary",
            "tip_acuteness", "tip_drop_relative_to_spine",
            "handle_color_primary",
        ]:
            observable.setdefault(key, "unknown")

        try:
            conn.execute("""
                INSERT OR IGNORE INTO knife_model_vision_profiles
                (knife_model_id, observable_profile_json, derived_profile_json,
                 semantic_profile_json, catalog_alignment_json,
                 annotation_status, source_summary)
                VALUES (?, ?, '{}', ?, ?, 'draft', 'Bootstrapped from catalog data')
            """, (
                r["id"],
                json.dumps(observable),
                json.dumps(semantic),
                json.dumps(catalog_alignment),
            ))
            inserted += 1
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    print(f"  Bootstrapped {inserted} profiles ({len(rows)} models total, {len(rows) - inserted} already existed).")


def verify(conn: sqlite3.Connection):
    """Show summary of bootstrapped profiles."""
    total = conn.execute("SELECT COUNT(*) FROM knife_model_vision_profiles").fetchone()[0]
    print(f"  Total profiles: {total}")

    # Show a sample
    sample = conn.execute("""
        SELECT km.official_name, p.observable_profile_json
        FROM knife_model_vision_profiles p
        JOIN knife_models_v2 km ON km.id = p.knife_model_id
        ORDER BY km.official_name
        LIMIT 3
    """).fetchall()
    for s in sample:
        obs = json.loads(s["observable_profile_json"])
        print(f"  Sample: {s['official_name']}")
        print(f"    handle_type_visual={obs.get('handle_type_visual')}, "
              f"is_hatchet={obs.get('is_hatchet')}, "
              f"knife_type_visual={obs.get('knife_type_visual')}, "
              f"blade_finish_visual={obs.get('blade_finish_visual')}, "
              f"blade_to_handle_ratio={obs.get('blade_to_handle_ratio_bucket')}")


def main():
    db = sys.argv[1] if len(sys.argv) > 1 else str(DB_PATH)
    print(f"Database: {db}")

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")

    print("\n1. Bootstrapping vision profiles...")
    bootstrap(conn)

    print("\n2. Verifying...")
    verify(conn)

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
