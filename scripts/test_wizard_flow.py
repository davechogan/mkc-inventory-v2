#!/usr/bin/env python3
"""Test the wizard engine locally — no vision model, just decision tree logic.

Usage:
    PYTHONPATH=. .venv/bin/python scripts/test_wizard_flow.py
"""

import sqlite3
import json

DB_PATH = "data/mkc_inventory.db"


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    from reporting.identify_wizard import start_session, answer_question, go_back

    print("=== Test 1: Start session without image (no vision) ===")
    result = start_session(conn, image_b64=None)
    sid = result["session_id"]
    print(f"Session: {sid[:8]}...")
    print(f"Models: {result['remaining_models']}/{result['total_models']}")
    print(f"Families: {result['remaining_families']}")
    print(f"Auto-gates: {result['auto_gates']}")
    print(f"Done: {result['done']}")
    if result.get("next_question"):
        q = result["next_question"]
        print(f"Next question: {q['key']} — {q['display_text']}")
    print()

    # Simulate answering: not paracord
    print("=== Answer: paracord_handle = False ===")
    r = answer_question(conn, sid, "paracord_handle", False)
    print(f"Remaining: {r['remaining_models']} models, {r['remaining_families']} families")
    print(f"Eliminated: {r['eliminated_this_step']}")
    print(f"Done: {r['done']}")
    if r.get("next_question"):
        print(f"Next: {r['next_question']['key']} — {r['next_question']['display_text']}")
    print()

    # Answer: field knife
    print("=== Answer: kitchen_or_field = False (field) ===")
    r = answer_question(conn, sid, "kitchen_or_field", False)
    print(f"Remaining: {r['remaining_models']} models, {r['remaining_families']} families")
    print(f"Eliminated: {r['eliminated_this_step']}")
    if r.get("next_question"):
        print(f"Next: {r['next_question']['key']} — {r['next_question']['display_text']}")
    print()

    # Answer: no finger ring
    print("=== Answer: finger_ring = False ===")
    r = answer_question(conn, sid, "finger_ring", False)
    print(f"Remaining: {r['remaining_models']} models, {r['remaining_families']} families")
    print(f"Eliminated: {r['eliminated_this_step']}")
    if r.get("next_question"):
        print(f"Next: {r['next_question']['key']} — {r['next_question']['display_text']}")
    print()

    # Answer: blade color = Silver
    print("=== Answer: blade_color = Silver ===")
    r = answer_question(conn, sid, "blade_color", "Silver")
    print(f"Remaining: {r['remaining_models']} models, {r['remaining_families']} families")
    print(f"Eliminated: {r['eliminated_this_step']}")
    if r.get("next_question"):
        print(f"Next: {r['next_question']['key']} — {r['next_question']['display_text']}")
    print()

    # Answer: handle material = G-10
    print("=== Answer: handle_material = G-10 ===")
    r = answer_question(conn, sid, "handle_material", "G-10")
    print(f"Remaining: {r['remaining_models']} models, {r['remaining_families']} families")
    print(f"Eliminated: {r['eliminated_this_step']}")
    if r.get("next_question"):
        print(f"Next: {r['next_question']['key']} — {r['next_question']['display_text']}")
    print()

    # Answer: blade length bin 2 (3-4.5")
    print("=== Answer: blade_length_bin = 2 (3-4.5\") ===")
    r = answer_question(conn, sid, "blade_length_bin", 2)
    print(f"Remaining: {r['remaining_models']} models, {r['remaining_families']} families")
    print(f"Eliminated: {r['eliminated_this_step']}")
    if r.get("next_question"):
        print(f"Next: {r['next_question']['key']} — {r['next_question']['display_text']}")
    print()

    # Check if done, if not answer one more
    if not r.get("done"):
        print("=== Answer: blade_form = ['Drop Point'] ===")
        r = answer_question(conn, sid, "blade_form", ["Drop Point"])
        print(f"Remaining: {r['remaining_models']} models, {r['remaining_families']} families")
        print(f"Eliminated: {r['eliminated_this_step']}")
        print()

    if r.get("done") and r.get("candidates"):
        print("=== FINAL CANDIDATES ===")
        for c in r["candidates"]:
            print(f"  {c['name']:40s} | {c.get('family',''):15s} | {c.get('form',''):15s} | {c.get('blade_length',''):>5} | {c.get('handle_type','')}")
    print()

    # Test back
    print("=== Test go_back ===")
    r = go_back(conn, sid)
    print(f"After back: {r['remaining_models']} models, {r['remaining_families']} families")
    if r.get("next_question"):
        print(f"Re-ask: {r['next_question']['key']}")
    print()

    # === Test 2: Hatchet path ===
    print("=" * 60)
    print("=== Test 2: Hatchet path (should get 2 models) ===")
    result = start_session(conn, image_b64=None)
    sid2 = result["session_id"]
    print(f"Models: {result['remaining_models']}")
    # Answer hatchet=True (normally auto-gated, but without image we answer manually)
    r = answer_question(conn, sid2, "is_hatchet", True)
    print(f"After hatchet=True: {r['remaining_models']} models")
    if r.get("done") and r.get("candidates"):
        print("Candidates:")
        for c in r["candidates"]:
            print(f"  {c['name']}")


if __name__ == "__main__":
    main()
