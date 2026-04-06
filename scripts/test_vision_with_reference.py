#!/usr/bin/env python3
"""Test the vision model's ability to identify blade shapes using a reference image.

Sends the trailing point reference SVG + 5 candidate knife images to the vision model
and checks if it correctly identifies which are trailing point blades.
"""

import base64
import json
import sqlite3
import sys
from io import BytesIO
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from PIL import Image, ImageDraw, ImageFont
from blade_ai import ollama_chat, try_parse_json_response

PROJECT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT / "data" / "mkc_inventory.db"


def render_trailing_point_reference() -> str:
    """Render the trailing point blade outline from the SVG polygon data and return base64 PNG."""
    # Polygon points from the TrailingPoint.svg the user provided
    # These are the raw SVG coordinates
    raw_points = [
        (40.93, 308.37), (32.66, 10.64), (89.64, 2.61), (89.64, 2.61),
        (98.23, 8.28), (103.55, 10.05), (114.18, 11.82), (108.27, 21.28),
        (108.86, 40.77), (104.14, 143.56), (99.41, 171.32), (89.64, 212.08),
        (77.55, 243.98), (65.75, 271.15), (49.79, 297.14), (40.93, 308.37),
    ]

    # The SVG viewBox is 131.5 x 321.61 — rotate 90° CCW so blade points right
    # After rotation: new_x = y, new_y = max_x - x
    max_x = 131.5
    rotated = [(y, max_x - x) for x, y in raw_points]

    # Scale to fit in a 600x300 image with padding
    W, H = 600, 300
    PAD = 30
    xs = [p[0] for p in rotated]
    ys = [p[1] for p in rotated]
    min_x, max_x_r = min(xs), max(xs)
    min_y, max_y_r = min(ys), max(ys)
    range_x = max_x_r - min_x or 1
    range_y = max_y_r - min_y or 1
    scale = min((W - 2 * PAD) / range_x, (H - 2 * PAD) / range_y)
    off_x = PAD + ((W - 2 * PAD) - range_x * scale) / 2
    off_y = PAD + ((H - 2 * PAD) - range_y * scale) / 2
    scaled = [(int((x - min_x) * scale + off_x), int((y - min_y) * scale + off_y)) for x, y in rotated]

    img = Image.new("RGB", (W, H), (255, 255, 255))
    draw = ImageDraw.Draw(img)
    draw.polygon(scaled, fill=(30, 30, 30), outline=(30, 30, 30))

    # Label
    try:
        font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 22)
    except (OSError, IOError):
        font = ImageFont.load_default()
    draw.text((W // 2 - 80, H - 35), "Trailing Point", fill=(100, 100, 100), font=font)

    buf = BytesIO()
    img.save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode("ascii")


def load_candidates(db_path: Path) -> list[dict]:
    """Load a mix of trailing point and non-trailing point knife images from DB."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Pick specific models: 2 trailing point + 3 other shapes
    targets = [
        # Trailing point knives
        ("Speedgoat Ultra", True),
        ("Stoned Goat Ultra", True),
        # Non-trailing point
        ("Blackfoot Ultra", False),       # Drop Point
        ("Bighorn Chef VIP", False),       # Chef
        ("Cattlemen Cleaver", False),      # Cleaver
    ]

    candidates = []
    for name_fragment, is_trailing in targets:
        row = conn.execute(
            """
            SELECT km.id, km.official_name, frm.name AS form_name,
                   kmi.image_blob
            FROM knife_models_v2 km
            LEFT JOIN knife_forms frm ON frm.id = km.form_id
            LEFT JOIN knife_model_images kmi ON kmi.knife_model_id = km.id
            WHERE km.official_name LIKE ? AND kmi.image_blob IS NOT NULL
            LIMIT 1
            """,
            (f"%{name_fragment}%",),
        ).fetchone()
        if row and row["image_blob"]:
            candidates.append({
                "name": row["official_name"],
                "form": row["form_name"],
                "is_trailing_point": is_trailing,
                "image_b64": base64.b64encode(row["image_blob"]).decode("ascii"),
            })
            print(f"  Loaded: {row['official_name']} (form: {row['form_name']}, trailing: {is_trailing})")
        else:
            print(f"  MISSING: {name_fragment}")

    conn.close()
    return candidates


def run_test():
    print("Rendering trailing point reference silhouette...")
    ref_b64 = render_trailing_point_reference()
    print(f"  Reference image: {len(ref_b64)} chars base64")

    print("\nLoading candidate knife images from DB...")
    candidates = load_candidates(DB_PATH)
    if len(candidates) < 3:
        print("ERROR: Not enough candidates found")
        return

    print(f"\nLoaded {len(candidates)} candidates")

    # Build the vision prompt
    system = """You are a knife blade shape identification expert.

Image 1 is a REFERENCE SILHOUETTE of a "Trailing Point" blade shape. Study it carefully.
The remaining images are photos of actual knives.

For each knife photo, determine whether its blade shape matches the Trailing Point reference (Image 1).
Focus on the overall blade PROFILE and CURVATURE — does the spine curve upward toward the tip like the reference?

Return VALID JSON ONLY (no markdown):
{"results": [{"name": "<exact name>", "is_trailing_point": true/false, "confidence": "HIGH|MEDIUM|LOW", "reason": "<one sentence>"}]}"""

    images = [ref_b64]
    candidate_descriptions = []
    for i, c in enumerate(candidates):
        images.append(c["image_b64"])
        candidate_descriptions.append(f"Image {i + 2}: {c['name']} (cataloged as: {c['form']})")

    user_text = (
        "Image 1 is the Trailing Point reference silhouette.\n"
        "For each of the following knife photos, tell me if the blade is a Trailing Point:\n"
        + "\n".join(candidate_descriptions)
    )

    print(f"\nSending {len(images)} images to vision model (qwen3-vl:latest)...")
    print("  This may take 15-30 seconds...\n")

    raw = ollama_chat("qwen3-vl:latest", system, user_text, images_b64=images)
    print("=== RAW RESPONSE ===")
    print(raw)
    print("====================\n")

    parsed = try_parse_json_response(raw)
    if isinstance(parsed, dict) and "results" in parsed:
        print("=== PARSED RESULTS ===")
        for r in parsed["results"]:
            actual = next((c for c in candidates if c["name"] == r.get("name")), None)
            actual_trailing = actual["is_trailing_point"] if actual else "?"
            match = "CORRECT" if actual and actual["is_trailing_point"] == r.get("is_trailing_point") else "WRONG"
            print(f"  {r.get('name')}:")
            print(f"    Model says: trailing={r.get('is_trailing_point')} ({r.get('confidence')})")
            print(f"    Actual:     trailing={actual_trailing}")
            print(f"    Result:     {match}")
            print(f"    Reason:     {r.get('reason')}")
            print()
    else:
        print("Could not parse response as JSON")


if __name__ == "__main__":
    run_test()
