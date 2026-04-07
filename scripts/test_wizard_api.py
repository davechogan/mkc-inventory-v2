#!/usr/bin/env python3
"""Test wizard API endpoints with a real image.

Usage:
    PYTHONPATH=. .venv/bin/python scripts/test_wizard_api.py
"""

import json
import sqlite3
import urllib.request
from io import BytesIO

DB_PATH = "data/mkc_inventory.db"
BASE_URL = "http://localhost:8008"


def multipart_upload(url, image_bytes, filename="test.jpg"):
    boundary = "----TestBoundary12345"
    parts = []
    parts.append(f"--{boundary}")
    parts.append(f'Content-Disposition: form-data; name="image"; filename="{filename}"')
    parts.append("Content-Type: image/jpeg")
    parts.append("")

    header = "\r\n".join(parts).encode() + b"\r\n"
    footer = f"\r\n--{boundary}--\r\n".encode()
    body = header + image_bytes + footer

    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    resp = urllib.request.urlopen(req, timeout=120)
    return json.loads(resp.read())


def post_json(url, data):
    body = json.dumps(data).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
    )
    resp = urllib.request.urlopen(req, timeout=30)
    return json.loads(resp.read())


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Test 1: Hatchet (should auto-gate to 2 models)
    print("=== Test 1: Hellgate Hatchet image ===")
    row = conn.execute(
        "SELECT image_blob FROM model_colorways "
        "WHERE knife_model_id = 285 AND image_blob IS NOT NULL ORDER BY id LIMIT 1"
    ).fetchone()
    img = row["image_blob"]
    print(f"Image: {len(img):,} bytes")

    result = multipart_upload(f"{BASE_URL}/api/v2/identify/wizard/start", img)
    print(f"Remaining: {result['remaining_models']}/{result['total_models']}")
    print(f"Auto-gates: {json.dumps(result['auto_gates'], indent=2)}")
    print(f"Done: {result['done']}")
    if result.get("candidates"):
        print("Candidates:")
        for c in result["candidates"]:
            print(f"  {c['name']}")
    elif result.get("next_question"):
        nq = result["next_question"]
        print(f"Next: {nq['key']} (suggestion={nq.get('vision_suggestion')})")
    print()

    # Test 2: Speedgoat (should NOT be hatchet, should proceed to questions)
    print("=== Test 2: Speedgoat 2.0 image ===")
    row = conn.execute(
        "SELECT image_blob FROM model_colorways "
        "WHERE knife_model_id = 277 AND image_blob IS NOT NULL ORDER BY id LIMIT 1"
    ).fetchone()
    img = row["image_blob"]

    result = multipart_upload(f"{BASE_URL}/api/v2/identify/wizard/start", img)
    sid = result["session_id"]
    print(f"Remaining: {result['remaining_models']}/{result['total_models']}")
    print(f"Auto-gates: {[g['question'] + '=' + str(g['vision_answer']) for g in result.get('auto_gates', [])]}")
    if result.get("next_question"):
        nq = result["next_question"]
        print(f"Next: {nq['key']} — {nq['display_text']}")
        if nq.get("vision_suggestion") is not None:
            print(f"  Vision suggests: {nq['vision_suggestion']} (reliability={nq['vision_reliability']})")

    # Walk through a few answers
    print("\n  Answering: blade_length_bin=2, kitchen=False, blade_form=[Trailing Point]")
    r = post_json(f"{BASE_URL}/api/v2/identify/wizard/answer", {
        "session_id": sid, "question_key": "blade_length_bin", "answer": 2
    })
    print(f"  After length: {r['remaining_models']} models, {r['remaining_families']} families")

    if not r["done"]:
        nq = r.get("next_question", {})
        print(f"  Next: {nq.get('key')} (suggestion={nq.get('vision_suggestion')})")

        r = post_json(f"{BASE_URL}/api/v2/identify/wizard/answer", {
            "session_id": sid, "question_key": nq["key"], "answer": False
        })
        print(f"  After {nq['key']}: {r['remaining_models']} models")

    if not r.get("done"):
        r = post_json(f"{BASE_URL}/api/v2/identify/wizard/answer", {
            "session_id": sid, "question_key": "blade_form", "answer": ["Trailing Point"]
        })
        print(f"  After form: {r['remaining_models']} models, {r['remaining_families']} families")

    if r.get("done") and r.get("candidates"):
        print("\n  FINAL CANDIDATES:")
        for c in r["candidates"]:
            print(f"    {c['name']:40s} | {c.get('family',''):15s} | {c.get('form','')}")


if __name__ == "__main__":
    main()
