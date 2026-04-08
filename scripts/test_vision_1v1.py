#!/usr/bin/env python3
"""Test 1:1 vision comparison — ask "is this the same knife?" for each candidate individually.

For each pair, the user photo is the target's reference image.
We ask two questions:
  1. Is user photo the same as target reference? (should be YES)
  2. Is user photo the same as distractor reference? (should be NO)

Usage:
    PYTHONPATH=. .venv/bin/python scripts/test_vision_1v1.py
"""
import sqlite3
import base64
import json
import time

DB_PATH = "data/mkc_inventory.db"
MODEL = "gemma3:27B"

# All 10 pairs from previous tests — easy and hard
TEST_PAIRS = [
    (285, "Hellgate Hatchet", 302, "MKC Chopper"),
    (319, "Flathead Fillet", 281, "Blackfoot 2.0"),
    (277, "Speedgoat 2.0", 314, "Jackstone"),
    (307, "V24", 316, "Rocker"),
    (291, "Cattlemen Cleaver 2.0", 293, "Bighorn Chef"),
    (304, "Wargoat", 282, "Super Cub"),
    (284, "Marshall Bushcraft Knife", 301, "Flattail"),
    (315, "Stockyard", 321, "Mule Deer"),
    (332, "Triumph SL", 288, "Freezout"),
    (312, "MKC Elk Knife", 283, "Beartooth"),
]


def get_image(conn, model_id):
    row = conn.execute("""
        SELECT mc.image_blob FROM model_colorways mc
        LEFT JOIN handle_colors hc ON hc.id = mc.handle_color_id
        WHERE mc.knife_model_id = ? AND mc.image_blob IS NOT NULL
        ORDER BY CASE WHEN LOWER(hc.name) = 'orange/black' THEN 0 ELSE 1 END
        LIMIT 1
    """, (model_id,)).fetchone()
    return base64.b64encode(row["image_blob"]).decode("ascii") if row else None


def ask_same(ollama_chat, img_a, img_b, candidate_name):
    """Ask: is Image 1 the same knife as Image 2? Returns True/False."""
    prompt = (
        f"Image 1 is the user's knife/tool. Image 2 is a reference photo of '{candidate_name}'.\n\n"
        f"Is the user's knife/tool (Image 1) the same model as '{candidate_name}' (Image 2)? "
        f"Focus on overall shape and proportions, not color or finish.\n\n"
        f'Answer ONLY "yes" or "no".'
    )
    raw = ollama_chat(MODEL, "", prompt, images_b64=[img_a, img_b])
    return "yes" in raw.lower()[:15], raw.strip()[:40]


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    from blade_ai import ollama_chat

    print(f"1:1 Vision Test — {len(TEST_PAIRS)} pairs × 2 comparisons each = {len(TEST_PAIRS) * 2} calls")
    print(f"Model: {MODEL}")
    print()
    print(f"{'Target':30s} {'Candidate':30s} | {'Expected':8s} | {'Got':8s} | {'OK':3s} | Raw")
    print("-" * 120)

    total = 0
    correct = 0
    true_pos = 0   # correctly said yes to match
    true_neg = 0   # correctly said no to non-match
    false_pos = 0  # wrongly said yes to non-match
    false_neg = 0  # wrongly said no to match
    t0 = time.time()

    # Also track per-pair: did we get the RIGHT identification?
    pair_results = []

    for target_id, target_name, dist_id, dist_name in TEST_PAIRS:
        target_img = get_image(conn, target_id)
        dist_img = get_image(conn, dist_id)
        if not target_img or not dist_img:
            continue

        # Compare user photo (= target) against target reference (should be YES)
        is_match, raw1 = ask_same(ollama_chat, target_img, target_img, target_name)
        expected = True
        ok = is_match == expected
        total += 1
        if ok:
            correct += 1
            true_pos += 1
        else:
            false_neg += 1
        mark = "Y" if ok else "N"
        print(f"{target_name:30s} {target_name:30s} | {'yes':8s} | {'yes' if is_match else 'no':8s} | {mark:3s} | {raw1}")

        # Compare user photo (= target) against distractor (should be NO)
        is_match2, raw2 = ask_same(ollama_chat, target_img, dist_img, dist_name)
        expected2 = False
        ok2 = is_match2 == expected2
        total += 1
        if ok2:
            correct += 1
            true_neg += 1
        else:
            false_pos += 1
        mark2 = "Y" if ok2 else "N"
        print(f"{target_name:30s} {dist_name:30s} | {'no':8s} | {'yes' if is_match2 else 'no':8s} | {mark2:3s} | {raw2}")

        # Did we identify correctly? (said yes to target AND no to distractor)
        identified = ok and ok2
        pair_results.append((target_name, dist_name, identified, ok, ok2))
        print()

    elapsed = time.time() - t0
    print(f"=== RESULTS: {correct}/{total} correct ({100*correct/total:.0f}%) in {elapsed:.0f}s ===")
    print()
    print(f"True positives (correctly matched):     {true_pos}/10")
    print(f"True negatives (correctly rejected):    {true_neg}/10")
    print(f"False positives (wrongly matched):      {false_pos}/10")
    print(f"False negatives (wrongly rejected):     {false_neg}/10")
    print()
    print(f"Correct identifications (yes to target AND no to distractor):")
    for target, dist, identified, tp, tn in pair_results:
        status = "IDENTIFIED" if identified else f"FAILED ({'missed match' if not tp else 'false match on ' + dist})"
        print(f"  {target:30s} vs {dist:25s} — {status}")


if __name__ == "__main__":
    main()
