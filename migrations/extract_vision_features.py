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

EXTRACTION_PROMPT = """Examine this knife photo carefully. For each question below, LOOK at the specific area described before answering. Do NOT guess — answer based on what you actually see.

STEP 1 — BLADE (look at the blade first):
- blade_color_primary: What color is the BLADE METAL (not the handle)? Look at the flat of the blade. Answer: "silver", "black", "red", "coyote_tan", "grey", "two_tone", "other"
- spine_profile_primary: Look at the TOP edge (spine) of the blade from handle to tip. Answer: "mostly_straight", "gentle_drop", "strong_drop", "hump_then_drop", "rising", "mixed"
- edge_profile_primary: Look at the BOTTOM edge (cutting edge). Answer: "mostly_straight", "gentle_belly", "pronounced_belly", "recurve", "mixed"
- tip_acuteness: How pointed is the tip? Answer: "fine", "medium", "stout"
- tip_drop_relative_to_spine: Is the tip BELOW the spine line, NEAR it, or ABOVE it? Answer: "below", "near", "above"

STEP 2 — HANDLE (now look at the handle):
- handle_color_primary: What is the PRIMARY color of the handle? Answer: "black", "orange", "green", "tan", "brown", "red", "grey", "coyote", "wood_grain", "camo", "other"
- handle_texture_visual: What is the surface texture? Answer: "smooth", "textured", "woven", "scaled", "wood_grain", "carbon_fiber_pattern", "unknown"
- handle_fastener_count_visible: Count the visible screws/rivets/pins. Answer: a number 0-5
- handle_profile_primary: Overall handle shape? Answer: "mostly_straight", "tapered", "center_swell", "palm_swell", "curved", "mixed"
- handle_butt_shape: Shape of the butt end? Answer: "rounded", "squared", "tapered", "flared"
- finger_ring_presence: Is there a LARGE ring, loop, or circular hole at the butt end of the handle that a finger could go through? This is NOT a small lanyard hole — it is a prominent circular opening. Answer: true or false
- lanyard_hole_presence: Is there a small hole near the end of the handle (lanyard hole)? Answer: true or false
- lanyard_hole_shape: If lanyard hole exists, shape? Answer: "round", "oval", "slot", "irregular", "none"

STEP 3 — BLADE-HANDLE JUNCTION (look where blade meets handle):
- choil_presence: Is there a choil (notch/cutout) on the edge side where blade meets handle? Answer: true or false
- choil_depth: If choil exists, how deep? Answer: "none", "shallow", "medium", "deep"
- guard_prominence: How prominent is the guard/bolster? Answer: "none", "low", "medium", "high"
- jimping_presence: Is there jimping (textured notches) on the spine near the handle? Answer: true or false
- jimping_location: If jimping exists, where? Answer: "spine_near_handle", "spine_mid", "underside", "multiple", "none"

Return VALID JSON ONLY (no markdown):
{"blade_color_primary": "...", "spine_profile_primary": "...", "edge_profile_primary": "...", "tip_acuteness": "...", "tip_drop_relative_to_spine": "...", "handle_color_primary": "...", "handle_texture_visual": "...", "handle_fastener_count_visible": 0, "handle_profile_primary": "...", "handle_butt_shape": "...", "finger_ring_presence": false, "lanyard_hole_presence": true, "lanyard_hole_shape": "...", "choil_presence": true, "choil_depth": "...", "guard_prominence": "...", "jimping_presence": true, "jimping_location": "..."}"""


def needs_extraction(profile_json: str) -> bool:
    """Check if this profile still has unknown vision-dependent features."""
    obs = json.loads(profile_json)
    vision_keys = [
        "lanyard_hole_presence", "choil_presence", "jimping_presence",
        "handle_profile_primary", "spine_profile_primary", "edge_profile_primary",
        "handle_color_primary", "blade_color_primary", "finger_ring_presence",
    ]
    return any(obs.get(k) in ("unknown", None) or k not in obs for k in vision_keys)


def _ask_focused(image_b64: str, question: str) -> dict | None:
    """Ask a single focused question. Returns parsed JSON or None."""
    raw = ollama_chat(
        VISION_MODEL,
        "Answer with ONLY valid JSON. No markdown.",
        question,
        images_b64=[image_b64],
        timeout=30.0,
    )
    return try_parse_json_response(raw)


# High-value features that must be asked individually (multi-question prompts give wrong answers)
FOCUSED_QUESTIONS = [
    ('What color is the BLADE (not the handle) of this knife? Answer JSON: {"blade_color_primary": "silver" or "black" or "red" or "coyote_tan" or "grey" or "two_tone" or "other"}',),
    # finger_ring_presence is manually annotated — the model confuses lanyard holes with finger rings
]

# Features that must NEVER be overwritten by extraction (manually annotated)
MANUAL_ONLY_FEATURES = {"finger_ring_presence"}


def extract_features(image_b64: str) -> dict | None:
    """Extract features using focused individual calls for high-value features + batch for the rest."""
    # Step 1: Focused calls for features that fail in multi-question prompts
    result = {}
    for (question,) in FOCUSED_QUESTIONS:
        parsed = _ask_focused(image_b64, question)
        if parsed and isinstance(parsed, dict):
            result.update(parsed)

    # Step 2: Batch call for remaining features
    raw = ollama_chat(
        VISION_MODEL,
        "You are analyzing a knife photo. Answer each question precisely based on what you see.",
        EXTRACTION_PROMPT,
        images_b64=[image_b64],
        timeout=120.0,
    )
    batch = try_parse_json_response(raw)
    if batch and isinstance(batch, dict):
        # Merge batch results but don't overwrite focused results or manual-only features
        for k, v in batch.items():
            if k not in result and k not in MANUAL_ONLY_FEATURES:
                result[k] = v

    return result if result else None


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    db = args[0] if args else str(DB_PATH)
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
            # Merge vision answers into existing profile
            # Don't overwrite: catalog-derived values (unless force), manual-only features (never)
            for key, val in answers.items():
                if key in MANUAL_ONLY_FEATURES:
                    continue  # Never overwrite manually annotated features
                if key in existing_obs:
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
