#!/usr/bin/env python3
"""Check splitting power for remaining questions with 2 BB candidates."""
import sqlite3
from reporting.identify_wizard import _compute_splitting_power, QUESTIONS, _load_models

conn = sqlite3.connect("data/mkc_inventory.db")
conn.row_factory = sqlite3.Row
models = _load_models(conn)
candidate_ids = {347, 372}

answered = {"is_hatchet", "blade_wider_than_handle", "paracord_handle",
            "blade_color", "finger_ring", "blade_length_bin", "blade_form", "handle_color"}

print("Remaining questions and their splitting power:")
for q in QUESTIONS:
    if q.key in answered:
        continue
    power = _compute_splitting_power(q, candidate_ids, models)
    print(f"  {q.key:20s} power={power:.4f}")

print("\nCandidate details:")
for m in models:
    if m["id"] in candidate_ids:
        mid = m["id"]
        name = m["official_name"]
        ht = m.get("handle_type")
        form = m.get("form_name")
        print(f"  {mid}: {name:40s} handle={ht}, form={form}")
