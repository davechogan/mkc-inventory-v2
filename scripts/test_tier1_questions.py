#!/usr/bin/env python3
"""Test tier-1 decision tree questions asked one-at-a-time to gemma3:27B.

Tests whether the vision model can reliably answer simple binary/categorical
questions about knife images when asked individually (not batched).

Usage:
    PYTHONPATH=. .venv/bin/python scripts/test_tier1_questions.py
"""

import base64
import sqlite3
import sys
import time

DB_PATH = "data/mkc_inventory.db"
MODEL = "gemma3:27B"


def parse_yes_no(raw):
    return "yes" if "yes" in raw.lower()[:15] else "no"


def parse_kitchen(raw):
    return "yes" if "kitchen" in raw.lower()[:30] else "no"


def parse_blade_length(raw):
    r = raw.lower().strip()[:30]
    if "longer" in r or "yes" in r:
        return "longer"
    elif "shorter" in r:
        return "shorter"
    return "similar"


def parse_blade_color(raw):
    r = raw.lower().strip()[:40]
    if "red" in r:
        return "red"
    if "black" in r or "dark" in r or "pvd" in r:
        return "black"
    if "coyote" in r or "tan" in r:
        return "coyote"
    return "silver"


QUESTIONS = {
    "hatchet": {
        "prompt": (
            "Look at this knife or tool. Is it a hatchet or axe shape — "
            "meaning it has a wide chopping head mounted on a handle below? "
            'Answer ONLY "yes" or "no".'
        ),
        "parse": parse_yes_no,
    },
    "blade_wider": {
        "prompt": (
            "Look at this knife. Is the blade clearly wider (taller) than the handle? "
            "A cleaver has a blade much taller than its handle. Most hunting knives do not. "
            'Answer ONLY "yes" or "no".'
        ),
        "parse": parse_yes_no,
    },
    "paracord": {
        "prompt": (
            "Look at the handle of this knife. Is the handle wrapped in paracord or cord? "
            "Paracord handles have a visible woven/wrapped texture. Solid handles (G-10, "
            'wood, micarta) are smooth or textured but not wrapped. Answer ONLY "yes" or "no".'
        ),
        "parse": parse_yes_no,
    },
    "kitchen": {
        "prompt": (
            "Is this a kitchen/culinary knife (chef knife, paring knife, butcher knife, "
            "santoku, cleaver — designed for food preparation) or a field/hunting/tactical "
            'knife? Answer ONLY "kitchen" or "field".'
        ),
        "parse": parse_kitchen,
    },
    "blade_longer": {
        "prompt": (
            "Compare the blade length to the handle length on this knife. "
            "Is the blade clearly LONGER than the handle, about the SAME length, "
            'or clearly SHORTER? Answer ONLY "longer", "similar", or "shorter".'
        ),
        "parse": parse_blade_length,
    },
    "blade_color": {
        "prompt": (
            "What is the primary color of the BLADE (not the handle) on this knife? "
            'Answer ONLY one of: "silver", "black", "red", "coyote".'
        ),
        "parse": parse_blade_color,
    },
}


# Test set: (model_id, name, expected_answers)
# Expected answers based on actual product knowledge
TEST_KNIVES = [
    (285, "Hellgate Hatchet", {
        "hatchet": "yes", "blade_wider": "yes", "paracord": "no",
        "kitchen": "no", "blade_longer": "similar", "blade_color": "silver",
    }),
    (291, "Cattlemen Cleaver 2.0", {
        "hatchet": "no", "blade_wider": "yes", "paracord": "no",
        "kitchen": "yes", "blade_longer": "similar", "blade_color": "silver",
    }),
    (288, "Freezout", {
        "hatchet": "no", "blade_wider": "no", "paracord": "yes",
        "kitchen": "no", "blade_longer": "shorter", "blade_color": "silver",
    }),
    (293, "Bighorn Chef", {
        "hatchet": "no", "blade_wider": "no", "paracord": "no",
        "kitchen": "yes", "blade_longer": "longer", "blade_color": "silver",
    }),
    (277, "Speedgoat 2.0", {
        "hatchet": "no", "blade_wider": "no", "paracord": "no",
        "kitchen": "no", "blade_longer": "similar", "blade_color": "silver",
    }),
    (319, "Flathead Fillet", {
        "hatchet": "no", "blade_wider": "no", "paracord": "no",
        "kitchen": "no", "blade_longer": "longer", "blade_color": "silver",
    }),
    (304, "Wargoat", {
        "hatchet": "no", "blade_wider": "no", "paracord": "yes",
        "kitchen": "no", "blade_longer": "similar", "blade_color": "silver",
    }),
    (372, "Blood Brothers Speedgoat Ultra", {
        "hatchet": "no", "blade_wider": "no", "paracord": "no",
        "kitchen": "no", "blade_longer": "similar", "blade_color": "red",
    }),
    (307, "V24", {
        "hatchet": "no", "blade_wider": "no", "paracord": "no",
        "kitchen": "no", "blade_longer": "similar", "blade_color": "black",
    }),
    (315, "Stockyard", {
        "hatchet": "no", "blade_wider": "no", "paracord": "no",
        "kitchen": "no", "blade_longer": "shorter", "blade_color": "silver",
    }),
]


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    from blade_ai import ollama_chat

    # Load images (first colorway per model)
    images = {}
    for model_id, name, _ in TEST_KNIVES:
        row = conn.execute(
            "SELECT mc.image_blob FROM model_colorways mc "
            "WHERE mc.knife_model_id = ? AND mc.image_blob IS NOT NULL "
            "ORDER BY mc.id LIMIT 1",
            (model_id,),
        ).fetchone()
        if row:
            images[model_id] = base64.b64encode(row["image_blob"]).decode("ascii")
        else:
            print(f"MISSING IMAGE: {name}")

    n_questions = len(QUESTIONS)
    n_knives = len(TEST_KNIVES)
    print(f"Testing {n_knives} knives x {n_questions} questions = {n_knives * n_questions} vision calls")
    print(f"Model: {MODEL}")
    print()
    print(f"{'Knife':40s} | {'Question':14s} | {'Expect':8s} | {'Got':8s} | {'OK':3s} | Raw")
    print("-" * 115)

    total = 0
    correct = 0
    errors_by_question = {q: {"total": 0, "correct": 0} for q in QUESTIONS}
    t0 = time.time()

    for model_id, name, expected in TEST_KNIVES:
        img = images.get(model_id)
        if not img:
            print(f"{name:40s} | SKIPPED")
            continue

        for qkey, qinfo in QUESTIONS.items():
            raw = ollama_chat(MODEL, "", qinfo["prompt"], images_b64=[img])
            answer = qinfo["parse"](raw)
            exp = expected[qkey]
            ok = answer == exp
            total += 1
            errors_by_question[qkey]["total"] += 1
            if ok:
                correct += 1
                errors_by_question[qkey]["correct"] += 1
            mark = "Y" if ok else "N"
            raw_short = raw.strip().replace("\n", " ")[:40]
            print(f"{name:40s} | {qkey:14s} | {exp:8s} | {answer:8s} | {mark:3s} | {raw_short}")

    elapsed = time.time() - t0
    print()
    print(f"=== RESULTS: {correct}/{total} correct ({100*correct/total:.0f}%) in {elapsed:.0f}s ===")
    print()
    print("Per-question accuracy:")
    for qkey, stats in errors_by_question.items():
        pct = 100 * stats["correct"] / stats["total"] if stats["total"] else 0
        print(f"  {qkey:14s}: {stats['correct']}/{stats['total']} ({pct:.0f}%)")


if __name__ == "__main__":
    main()
