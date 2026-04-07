#!/usr/bin/env python3
"""Test Blood Brothers Speedgoat Ultra wizard path."""
import sqlite3
from reporting.identify_wizard import start_session, answer_question

conn = sqlite3.connect("data/mkc_inventory.db")
conn.row_factory = sqlite3.Row

r = start_session(conn, image_b64=None)
sid = r["session_id"]
print(f"Start: {r['remaining_models']} models")

r = answer_question(conn, sid, "blade_color", "Red")
print(f"After Red blade: {r['remaining_models']} models, {r['remaining_families']} families")
for c in r.get("candidates", []):
    print(f"  {c['name']:45s} | {c.get('handle_type','')}")

if not r.get("done"):
    r = answer_question(conn, sid, "handle_material", "Carbon Fiber")
    print(f"\nAfter Carbon Fiber: {r['remaining_models']} models")
    for c in r.get("candidates", []):
        print(f"  {c['name']:45s} | {c.get('handle_type','')}")
