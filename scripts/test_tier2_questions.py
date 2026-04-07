#!/usr/bin/env python3
"""Test tier-2 structural questions — features that differentiate within groups.

These are the questions that would help AFTER the tier-1 questions have
narrowed the field. They focus on structural details the model should
be able to see.

Usage:
    PYTHONPATH=. .venv/bin/python scripts/test_tier2_questions.py
"""

import base64
import sqlite3
import sys
import time

DB_PATH = "data/mkc_inventory.db"
MODEL = "gemma3:27B"


def parse_yes_no(raw):
    return "yes" if "yes" in raw.lower()[:15] else "no"


QUESTIONS = {
    "finger_ring": {
        "prompt": (
            "Look at this knife where the blade meets the handle. "
            "Is there a finger ring or circular loop guard at the front of the handle "
            "(a ring you could put your finger through)? "
            'Answer ONLY "yes" or "no".'
        ),
        "parse": parse_yes_no,
    },
    "lanyard_hole": {
        "prompt": (
            "Look at the end (butt/pommel) of this knife's handle. "
            "Is there a lanyard hole — a small hole or opening at the end of the handle "
            "for attaching a cord or lanyard? "
            'Answer ONLY "yes" or "no".'
        ),
        "parse": parse_yes_no,
    },
    "jimping": {
        "prompt": (
            "Look at the spine (top/back) of this knife near where the blade meets the handle. "
            "Is there jimping — a series of small notches or grooves cut into the spine "
            "to provide thumb grip? "
            'Answer ONLY "yes" or "no".'
        ),
        "parse": parse_yes_no,
    },
    "choil": {
        "prompt": (
            "Look at where the blade meets the handle on this knife. "
            "Is there a finger choil — a curved notch or cutout in the blade edge "
            "right in front of the handle where you could hook your index finger? "
            'Answer ONLY "yes" or "no".'
        ),
        "parse": parse_yes_no,
    },
    "swedge": {
        "prompt": (
            "Look at the spine (top/back) of the blade near the tip. "
            "Does the blade have a swedge or false edge — a sharpened or beveled "
            "section on the top of the blade near the tip (making it look like a "
            "double-edged dagger near the point)? "
            'Answer ONLY "yes" or "no".'
        ),
        "parse": parse_yes_no,
    },
    "guard": {
        "prompt": (
            "Look at where the blade meets the handle. "
            "Is there a prominent cross-guard or bolster that sticks out beyond "
            "the width of the handle (a piece of metal between blade and handle "
            "that would stop your hand from sliding onto the blade)? "
            'Answer ONLY "yes" or "no".'
        ),
        "parse": parse_yes_no,
    },
}


# Test set with known correct structural features
# Based on MKC product knowledge
TEST_KNIVES = [
    (304, "Wargoat", {
        "finger_ring": "yes", "lanyard_hole": "no", "jimping": "yes",
        "choil": "yes", "swedge": "no", "guard": "no",
    }),
    (307, "V24", {
        "finger_ring": "no", "lanyard_hole": "no", "jimping": "yes",
        "choil": "no", "swedge": "yes", "guard": "yes",
    }),
    (277, "Speedgoat 2.0", {
        "finger_ring": "no", "lanyard_hole": "yes", "jimping": "yes",
        "choil": "yes", "swedge": "no", "guard": "no",
    }),
    (281, "Blackfoot 2.0", {
        "finger_ring": "no", "lanyard_hole": "yes", "jimping": "yes",
        "choil": "yes", "swedge": "no", "guard": "no",
    }),
    (319, "Flathead Fillet", {
        "finger_ring": "no", "lanyard_hole": "yes", "jimping": "yes",
        "choil": "yes", "swedge": "no", "guard": "no",
    }),
    (293, "Bighorn Chef", {
        "finger_ring": "no", "lanyard_hole": "no", "jimping": "no",
        "choil": "no", "swedge": "no", "guard": "no",
    }),
    (285, "Hellgate Hatchet", {
        "finger_ring": "no", "lanyard_hole": "yes", "jimping": "no",
        "choil": "no", "swedge": "no", "guard": "no",
    }),
    (316, "Rocker", {
        "finger_ring": "no", "lanyard_hole": "yes", "jimping": "yes",
        "choil": "yes", "swedge": "no", "guard": "no",
    }),
    (318, "TF24", {
        "finger_ring": "no", "lanyard_hole": "no", "jimping": "yes",
        "choil": "no", "swedge": "yes", "guard": "yes",
    }),
    (315, "Stockyard", {
        "finger_ring": "no", "lanyard_hole": "yes", "jimping": "yes",
        "choil": "yes", "swedge": "no", "guard": "no",
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
