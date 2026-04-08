#!/usr/bin/env python3
"""Test: send Hellgate Hatchet reference image as user photo,
with both hatchet candidates. The model MUST pick Hellgate Hatchet."""
import sqlite3
import base64
import json

DB_PATH = "data/mkc_inventory.db"

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

from blade_ai import ollama_chat

MODEL = "gemma3:27B"

# Get Hellgate Hatchet Orange/Black colorway image (the reference image)
hh_row = conn.execute("""
    SELECT mc.image_blob, hc.name as color FROM model_colorways mc
    LEFT JOIN handle_colors hc ON hc.id = mc.handle_color_id
    WHERE mc.knife_model_id = 285
    ORDER BY CASE WHEN LOWER(hc.name) = 'orange/black' THEN 0 ELSE 1 END
    LIMIT 1
""").fetchone()
hh_image = base64.b64encode(hh_row["image_blob"]).decode("ascii")
print(f"User photo: Hellgate Hatchet ({hh_row['color']}) — {len(hh_row['image_blob']):,} bytes")

# Get MKC Chopper Orange/Black colorway image
ch_row = conn.execute("""
    SELECT mc.image_blob, hc.name as color FROM model_colorways mc
    LEFT JOIN handle_colors hc ON hc.id = mc.handle_color_id
    WHERE mc.knife_model_id = 302
    ORDER BY CASE WHEN LOWER(hc.name) = 'orange/black' THEN 0 ELSE 1 END
    LIMIT 1
""").fetchone()
ch_image = base64.b64encode(ch_row["image_blob"]).decode("ascii")
print(f"Candidate 1: MKC Chopper ({ch_row['color']}) — {len(ch_row['image_blob']):,} bytes")
print(f"Candidate 2: Hellgate Hatchet ({hh_row['color']}) — same image as user photo")

# Test 1: Direct match prompt (what the wizard uses)
SYSTEM = """You are matching a user's knife photo against candidate reference photos.

Image 1 is the user's knife. The remaining images are reference photos of candidate models, one per candidate.

For each candidate, compare the user's photo directly against the candidate's reference photo. Focus on:
1. Overall shape and proportions — does the knife/tool look the same?
2. Blade shape — similar profile, similar size relative to handle?
3. Handle shape and style — similar proportions and features?

Do NOT focus on color or finish — the user may have a different colorway of the same model.

Rate each:
  STRONG — clearly the same model (shape and proportions match)
  POSSIBLE — similar but not certain
  UNLIKELY — clearly different shape or proportions

Return VALID JSON ONLY (no markdown):
{"comparisons": [{"model": "<exact model name>", "match": "STRONG|POSSIBLE|UNLIKELY", "reason": "<one sentence>"}]}"""

user_text = """Image 1 is the user's knife/tool.

Candidates:
Candidate 'MKC Chopper': Image 2
Candidate 'Hellgate Hatchet': Image 3"""

print("\n=== Test 1: Wizard prompt (Chopper=Image2, Hellgate=Image3) ===")
images = [hh_image, ch_image, hh_image]
print(f"Sending {len(images)} images")
raw = ollama_chat(MODEL, SYSTEM, user_text, images_b64=images)
print(f"Response: {raw}")

# Test 2: Swap the order
print("\n=== Test 2: Swapped order (Hellgate=Image2, Chopper=Image3) ===")
user_text2 = """Image 1 is the user's knife/tool.

Candidates:
Candidate 'Hellgate Hatchet': Image 2
Candidate 'MKC Chopper': Image 3"""

images2 = [hh_image, hh_image, ch_image]
raw2 = ollama_chat(MODEL, SYSTEM, user_text2, images_b64=images2)
print(f"Response: {raw2}")

# Test 3: Ultra simple — are these the same?
print("\n=== Test 3: Simple question — is Image 1 the same as Image 2? ===")
raw3 = ollama_chat(MODEL, "",
    "Are Image 1 and Image 2 photos of the same knife/tool? Answer yes or no, then explain.",
    images_b64=[hh_image, hh_image])
print(f"Response: {raw3}")

print("\n=== Test 4: Simple — is Image 1 more similar to Image 2 or Image 3? ===")
raw4 = ollama_chat(MODEL, "",
    "Image 1 is a user's tool. Image 2 is 'MKC Chopper'. Image 3 is 'Hellgate Hatchet'. Which one (Image 2 or Image 3) is the same tool as Image 1? Just give the name.",
    images_b64=[hh_image, ch_image, hh_image])
print(f"Response: {raw4}")
