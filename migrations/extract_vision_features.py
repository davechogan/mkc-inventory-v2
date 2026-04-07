#!/usr/bin/env python3
"""Phase 3: Extract observable features from catalog images via gemma3:27B.

Runs each model's reference image through the vision model with structured
questions and updates the vision profile with the answers.

Only processes models that still have 'unknown' values for vision-dependent features.
Safe to run multiple times — skips already-extracted profiles.
"""

import base64
import json
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from blade_ai import ollama_chat, try_parse_json_response

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "mkc_inventory.db"

VISION_MODEL = "gemma3:27B"

EXTRACTION_PROMPT = """Look at this knife photo carefully and answer each question based on what you see.

HANDLE:
1. handle_color_primary: What is the PRIMARY color of the handle? Answer one: "black", "orange", "green", "tan", "brown", "red", "grey", "coyote", "wood_grain", "camo", "other"
2. handle_texture_visual: What is the handle surface texture? Answer one: "smooth", "textured", "woven", "scaled", "wood_grain", "carbon_fiber_pattern", "unknown"
3. handle_fastener_count_visible: How many visible screws/rivets/fasteners on the handle? Answer a number (0-5).
4. lanyard_hole_presence: Is there a hole at the end of the handle (lanyard hole)? Answer: true or false
5. lanyard_hole_shape: If lanyard hole exists, what shape? Answer one: "round", "oval", "slot", "irregular", "none"
6. handle_profile_primary: What is the overall handle shape? Answer one: "mostly_straight", "tapered", "center_swell", "palm_swell", "curved", "mixed"
7. handle_butt_shape: What shape is the butt end of the handle? Answer one: "rounded", "squared", "tapered", "flared"
8. finger_ring_presence: Is there a large finger ring or finger hole at the bottom/butt end of the handle? Answer: true or false

BLADE-HANDLE JUNCTION:
9. choil_presence: Is there a choil (notch/cutout) where the blade meets the handle on the edge side? Answer: true or false
10. choil_depth: If choil exists, how deep? Answer one: "none", "shallow", "medium", "deep"
11. guard_prominence: How prominent is the guard/bolster between handle and blade? Answer one: "none", "low", "medium", "high"
12. jimping_presence: Is there jimping (textured notches) on the spine near the handle? Answer: true or false
13. jimping_location: If jimping exists, where? Answer one: "spine_near_handle", "spine_mid", "underside", "multiple", "none"

BLADE:
14. blade_color_primary: What is the PRIMARY color of the blade itself? Answer one: "silver", "black", "red", "coyote_tan", "grey", "two_tone", "other"
15. spine_profile_primary: Looking at the top edge (spine) of the blade, does it: Answer one: "mostly_straight", "gentle_drop", "strong_drop", "hump_then_drop", "rising", "mixed"
16. edge_profile_primary: Looking at the cutting edge, is it: Answer one: "mostly_straight", "gentle_belly", "pronounced_belly", "recurve", "mixed"
17. tip_acuteness: How pointed/sharp is the tip? Answer one: "fine", "medium", "stout"
18. tip_drop_relative_to_spine: Is the blade tip BELOW the spine line, NEAR the spine line, or ABOVE it? Answer one: "below", "near", "above"

Return VALID JSON ONLY (no markdown):
{"handle_color_primary": "...", "handle_texture_visual": "...", "handle_fastener_count_visible": 0, "lanyard_hole_presence": true, "lanyard_hole_shape": "...", "handle_profile_primary": "...", "handle_butt_shape": "...", "finger_ring_presence": false, "choil_presence": true, "choil_depth": "...", "guard_prominence": "...", "jimping_presence": true, "jimping_location": "...", "blade_color_primary": "...", "spine_profile_primary": "...", "edge_profile_primary": "...", "tip_acuteness": "...", "tip_drop_relative_to_spine": "..."}"""


def needs_extraction(profile_json: str) -> bool:
    """Check if this profile still has unknown vision-dependent features."""
    obs = json.loads(profile_json)
    vision_keys = [
        "lanyard_hole_presence", "choil_presence", "jimping_presence",
        "handle_profile_primary", "spine_profile_primary", "edge_profile_primary",
        "handle_color_primary", "blade_color_primary", "finger_ring_presence",
    ]
    return any(obs.get(k) in ("unknown", None) or k not in obs for k in vision_keys)


def extract_features(image_b64: str) -> dict | None:
    """Ask gemma3:27B the feature questions about a single image."""
    raw = ollama_chat(
        VISION_MODEL,
        "You are analyzing a knife photo. Answer each question precisely based on what you see.",
        EXTRACTION_PROMPT,
        images_b64=[image_b64],
        timeout=120.0,
    )
    return try_parse_json_response(raw)


def main():
    db = sys.argv[1] if len(sys.argv) > 1 else str(DB_PATH)
    force = "--force" in sys.argv
    print(f"Database: {db}")
    print(f"Vision model: {VISION_MODEL}")
    if force:
        print("Force mode: re-extracting all profiles")

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row

    # Load models with profiles and images
    rows = conn.execute("""
        SELECT km.id, km.official_name, p.id AS profile_id,
               p.observable_profile_json,
               kmi.image_blob
        FROM knife_models_v2 km
        JOIN knife_model_vision_profiles p ON p.knife_model_id = km.id
        LEFT JOIN knife_model_images kmi ON kmi.knife_model_id = km.id
        WHERE kmi.image_blob IS NOT NULL AND length(kmi.image_blob) > 0
        ORDER BY km.official_name
    """).fetchall()

    print(f"\nFound {len(rows)} models with images")

    # Filter to those needing extraction
    if not force:
        to_process = [(r, json.loads(r["observable_profile_json"])) for r in rows if needs_extraction(r["observable_profile_json"])]
    else:
        to_process = [(r, json.loads(r["observable_profile_json"])) for r in rows]

    print(f"Models needing extraction: {len(to_process)}")

    if not to_process:
        print("Nothing to do.")
        conn.close()
        return

    print(f"\nEstimated time: {len(to_process) * 15 // 60} - {len(to_process) * 30 // 60} minutes")
    print("Starting extraction...\n")

    success = 0
    failed = 0
    start = time.time()

    for i, (row, existing_obs) in enumerate(to_process):
        name = row["official_name"]
        elapsed = time.time() - start
        rate = (i / elapsed) if elapsed > 0 and i > 0 else 0
        eta = ((len(to_process) - i) / rate / 60) if rate > 0 else 0
        print(f"  [{i+1}/{len(to_process)}] {name:40s} (ETA: {eta:.1f}min)", end="", flush=True)

        image_b64 = base64.b64encode(row["image_blob"]).decode("ascii")
        answers = extract_features(image_b64)

        if answers and isinstance(answers, dict):
            # Merge vision answers into existing profile (don't overwrite catalog-derived values)
            for key, val in answers.items():
                if key in existing_obs:
                    # Only overwrite if currently unknown
                    if existing_obs[key] == "unknown" or force:
                        existing_obs[key] = val
                else:
                    existing_obs[key] = val

            conn.execute("""
                UPDATE knife_model_vision_profiles
                SET observable_profile_json = ?,
                    annotation_status = 'draft',
                    source_summary = 'Catalog bootstrap + gemma3:27B vision extraction'
                WHERE id = ?
            """, (json.dumps(existing_obs), row["profile_id"]))
            conn.commit()
            success += 1
            print(f"  ✓")
        else:
            failed += 1
            print(f"  ✗ (parse failed)")

    total_time = time.time() - start
    print(f"\nComplete: {success} extracted, {failed} failed, {total_time:.0f}s total ({total_time/len(to_process):.1f}s avg)")

    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
