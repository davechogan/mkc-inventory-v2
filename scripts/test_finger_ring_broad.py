#!/usr/bin/env python3
"""Broad finger ring detection test across many models.

The Wargoat family (4 models) should have finger rings.
All other models should NOT have finger rings.
Previous batch extraction found 50/87 false positives.
This tests focused single-question calls.

Usage:
    PYTHONPATH=. .venv/bin/python scripts/test_finger_ring_broad.py
"""

import base64
import sqlite3
import time

DB_PATH = "data/mkc_inventory.db"
MODEL = "gemma3:27B"

FINGER_RING_PROMPT = (
    "Look at this knife where the blade meets the handle. "
    "Is there a finger ring or circular loop guard at the front of the handle "
    "(a ring you could put your finger through)? "
    'Answer ONLY "yes" or "no".'
)

# Models known to have finger rings (Wargoat family)
RING_MODELS = {304, 305, 306, 354}  # Wargoat, Mini Wargoat, Battle Goat, Blood Brothers Wargoat


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    from blade_ai import ollama_chat

    # Load ALL models with their first colorway image
    rows = conn.execute("""
        SELECT km.id, km.official_name, kf.name as family
        FROM knife_models_v2 km
        LEFT JOIN knife_families kf ON km.family_id = kf.id
        ORDER BY km.official_name
    """).fetchall()

    print(f"Testing finger ring detection on {len(rows)} models")
    print(f"Expected: {len(RING_MODELS)} have rings (Wargoat family)")
    print(f"Model: {MODEL}")
    print()
    print(f"{'Name':45s} | {'Family':15s} | {'Expect':6s} | {'Got':6s} | {'OK':3s} | Raw")
    print("-" * 110)

    total = 0
    correct = 0
    false_positives = []
    false_negatives = []
    t0 = time.time()

    for r in rows:
        model_id = r["id"]
        name = r["official_name"]
        family = r["family"] or "?"

        # Get first colorway image
        img_row = conn.execute(
            "SELECT image_blob FROM model_colorways "
            "WHERE knife_model_id = ? AND image_blob IS NOT NULL "
            "ORDER BY id LIMIT 1",
            (model_id,),
        ).fetchone()

        if not img_row:
            continue

        img_b64 = base64.b64encode(img_row["image_blob"]).decode("ascii")
        raw = ollama_chat(MODEL, "", FINGER_RING_PROMPT, images_b64=[img_b64])
        got = "yes" if "yes" in raw.lower()[:15] else "no"
        expected = "yes" if model_id in RING_MODELS else "no"
        ok = got == expected
        total += 1
        if ok:
            correct += 1
        else:
            if got == "yes":
                false_positives.append(f"{name} ({family})")
            else:
                false_negatives.append(f"{name} ({family})")

        mark = "Y" if ok else "N"
        raw_short = raw.strip().replace("\n", " ")[:30]
        print(f"{name:45s} | {family:15s} | {expected:6s} | {got:6s} | {mark:3s} | {raw_short}")

    elapsed = time.time() - t0
    print()
    print(f"=== RESULTS: {correct}/{total} correct ({100*correct/total:.0f}%) in {elapsed:.0f}s ===")

    if false_positives:
        print(f"\nFalse positives ({len(false_positives)} — said ring when there isn't one):")
        for fp in false_positives:
            print(f"  - {fp}")

    if false_negatives:
        print(f"\nFalse negatives ({len(false_negatives)} — missed actual ring):")
        for fn in false_negatives:
            print(f"  - {fn}")


if __name__ == "__main__":
    main()
