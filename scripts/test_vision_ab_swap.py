#!/usr/bin/env python3
"""A/B vision test with swap confirmation.

For each pair, run twice with candidates swapped. Only declare a match
when both runs agree on the same knife.

Usage:
    PYTHONPATH=. .venv/bin/python scripts/test_vision_ab_swap.py
"""
import sqlite3
import base64
import json
import time

DB_PATH = "data/mkc_inventory.db"
MODEL = "gemma3:27B"

SYSTEM = """You are matching a user's knife/tool photo against candidate reference photos.

Image 1 is the user's knife/tool. The remaining images are labeled reference photos.

Look at Image 1 carefully. Then look at each candidate reference photo. Which candidate's reference photo shows the SAME knife/tool as Image 1? Focus on overall shape and proportions, not color.

Return VALID JSON ONLY (no markdown):
{"best_match": "<exact candidate name>", "reason": "<one sentence explaining why>"}"""

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


def ask_model(ollama_chat, user_img, name_a, img_a, name_b, img_b):
    """Ask model which candidate matches. Returns the name it picked."""
    user_text = (
        f"Image 1 is the user's knife/tool.\n\n"
        f"'{name_a}' is Image 2\n"
        f"'{name_b}' is Image 3\n\n"
        f"Which candidate is the same knife/tool as Image 1?"
    )
    raw = ollama_chat(MODEL, SYSTEM, user_text, images_b64=[user_img, img_a, img_b])
    try:
        text = raw.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()
        if text.startswith("json"):
            text = text[4:].strip()
        parsed = json.loads(text)
        return parsed.get("best_match", "")
    except Exception:
        return raw.strip()[:40]


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    from blade_ai import ollama_chat

    print(f"A/B Swap Test — {len(TEST_PAIRS)} pairs, 2 runs each (swapped order)")
    print(f"Model: {MODEL}")
    print(f"Rule: only declare a match when both runs agree")
    print()
    print(f"{'Target':30s} {'Distractor':25s} | {'Run1 picked':25s} | {'Run2 picked':25s} | {'Agree':5s} | {'Correct':7s}")
    print("-" * 130)

    total = 0
    agreed = 0
    agreed_correct = 0
    disagreed = 0
    t0 = time.time()

    for target_id, target_name, dist_id, dist_name in TEST_PAIRS:
        target_img = get_image(conn, target_id)
        dist_img = get_image(conn, dist_id)
        if not target_img or not dist_img:
            continue

        total += 1

        # Run 1: target as Image 2
        pick1 = ask_model(ollama_chat, target_img, target_name, target_img, dist_name, dist_img)

        # Run 2: target as Image 3 (swapped)
        pick2 = ask_model(ollama_chat, target_img, dist_name, dist_img, target_name, target_img)

        # Do they agree?
        pick1_is_target = target_name.lower() in pick1.lower()
        pick2_is_target = target_name.lower() in pick2.lower()

        if pick1_is_target and pick2_is_target:
            agree = "YES"
            result = "CORRECT"
            agreed += 1
            agreed_correct += 1
        elif not pick1_is_target and not pick2_is_target:
            agree = "YES"
            result = "WRONG"
            agreed += 1
        else:
            agree = "NO"
            result = "UNSURE"
            disagreed += 1

        pick1_short = pick1[:23]
        pick2_short = pick2[:23]
        print(f"{target_name:30s} {dist_name:25s} | {pick1_short:25s} | {pick2_short:25s} | {agree:5s} | {result:7s}")

    elapsed = time.time() - t0
    print()
    print(f"=== RESULTS ({elapsed:.0f}s) ===")
    print(f"Total pairs: {total}")
    print(f"Both runs agreed: {agreed}/{total} ({100*agreed/total:.0f}%)")
    print(f"  Agreed AND correct: {agreed_correct}/{total}")
    print(f"  Agreed BUT wrong: {agreed - agreed_correct}/{total}")
    print(f"Runs disagreed (no AI pick): {disagreed}/{total} ({100*disagreed/total:.0f}%)")
    print()
    if agreed > 0:
        print(f"When AI makes a pick, accuracy: {agreed_correct}/{agreed} ({100*agreed_correct/agreed:.0f}%)")


if __name__ == "__main__":
    main()
