#!/usr/bin/env python3
"""Generate blade form silhouette PNGs from actual catalog reference photos.

For each of the 15 blade forms, picks a representative model from the DB,
extracts its reference image (already background-removed), converts it to
a black silhouette on white background, and saves as a reference outline.

Also generates a labeled composite reference sheet for the vision model.
"""

import base64
import math
import sqlite3
import sys
from io import BytesIO
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from PIL import Image, ImageDraw, ImageFont

PROJECT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT / "data" / "mkc_inventory.db"
OUT_DIR = PROJECT / "static" / "blade-outlines"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Representative model for each blade form (one good example per form)
# Format: (form_name, model_name_fragment) — we pick the first match with an image
FORM_REPRESENTATIVES = [
    ("Drop Point", "Blackfoot Ultra"),
    ("Trailing Point", "Speedgoat Ultra"),
    ("Clip Point", "Badrock"),
    ("Sheepsfoot", "Stockyard"),
    ("Spear Point", "Marshall"),
    ("Dagger", "V24"),
    ("Hawkbill", "Great Falls"),
    ("Skinner", "Beartooth"),
    ("Fillet", "Flathead Fillet"),
    ("Chef", "Bighorn Chef"),
    ("Santoku", "Smith River Santoku"),
    ("Petty", "Little Bighorn Petty"),
    ("Paring", "Cutbank Paring"),
    ("Cleaver", "Cattlemen Cleaver"),
    ("Hatchet", "Hellgate Hatchet"),
    # Tanto: no models currently classified as Tanto in DB — skip for now
]

CELL_W, CELL_H = 400, 300
SILHOUETTE_COLOR = (20, 20, 20)
BG_COLOR = (255, 255, 255)
LABEL_COLOR = (80, 80, 80)
THRESHOLD = 128  # alpha threshold for silhouette


def _get_font(size: int):
    candidates = [
        "/System/Library/Fonts/Helvetica.ttc",
        "/System/Library/Fonts/SFNSText.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size)
        except (OSError, IOError):
            continue
    return ImageFont.load_default()


def image_to_silhouette(img_bytes: bytes, target_w: int, target_h: int, label: str) -> Image.Image:
    """Convert a background-removed image to a black silhouette on white, with label.

    Handles both:
    - RGBA images (transparent background): alpha < threshold = background
    - RGB images (white background): pixels close to white = background
    """
    src = Image.open(BytesIO(img_bytes))

    has_alpha = src.mode == "RGBA"
    src_rgba = src.convert("RGBA")
    pixels = src_rgba.load()

    # Detect background color from corner pixels
    corners = [
        pixels[0, 0], pixels[src_rgba.width - 1, 0],
        pixels[0, src_rgba.height - 1], pixels[src_rgba.width - 1, src_rgba.height - 1],
    ]
    avg_r = sum(c[0] for c in corners) // 4
    avg_g = sum(c[1] for c in corners) // 4
    avg_b = sum(c[2] for c in corners) // 4

    if has_alpha and corners[0][3] < 50:
        bg_type = "transparent"
    elif avg_r < 50 and avg_g < 50 and avg_b < 50:
        bg_type = "black"
    else:
        bg_type = "white"

    COLOR_DIST = 60  # distance threshold for background detection

    silhouette = Image.new("RGB", src_rgba.size, BG_COLOR)
    sil_pixels = silhouette.load()

    for y in range(src_rgba.height):
        for x in range(src_rgba.width):
            r, g, b, a = pixels[x, y]
            is_bg = False

            if bg_type == "transparent":
                is_bg = a < THRESHOLD
            elif bg_type == "black":
                dist = (r ** 2 + g ** 2 + b ** 2) ** 0.5
                is_bg = dist < COLOR_DIST
            else:  # white
                dist = ((255 - r) ** 2 + (255 - g) ** 2 + (255 - b) ** 2) ** 0.5
                is_bg = dist < COLOR_DIST

            if not is_bg:
                sil_pixels[x, y] = SILHOUETTE_COLOR
            else:
                sil_pixels[x, y] = BG_COLOR

    # Find bounding box of non-white pixels
    sil_rgb = silhouette
    bbox = None
    for y in range(sil_rgb.height):
        for x in range(sil_rgb.width):
            px = sil_rgb.getpixel((x, y))
            if px != BG_COLOR:
                if bbox is None:
                    bbox = [x, y, x, y]
                else:
                    bbox[0] = min(bbox[0], x)
                    bbox[1] = min(bbox[1], y)
                    bbox[2] = max(bbox[2], x)
                    bbox[3] = max(bbox[3], y)

    if bbox is None:
        # No content found, return blank
        result = Image.new("RGB", (target_w, target_h), BG_COLOR)
        return result

    # Add small padding to bbox
    pad = 10
    bbox[0] = max(0, bbox[0] - pad)
    bbox[1] = max(0, bbox[1] - pad)
    bbox[2] = min(sil_rgb.width, bbox[2] + pad)
    bbox[3] = min(sil_rgb.height, bbox[3] + pad)

    cropped = sil_rgb.crop(tuple(bbox))

    # Fit into target cell with label area
    label_h = 40
    avail_w = target_w - 20
    avail_h = target_h - label_h - 20

    # Scale to fit
    scale = min(avail_w / cropped.width, avail_h / cropped.height)
    new_w = int(cropped.width * scale)
    new_h = int(cropped.height * scale)
    resized = cropped.resize((new_w, new_h), Image.LANCZOS)

    # Center in cell
    result = Image.new("RGB", (target_w, target_h), BG_COLOR)
    x_off = (target_w - new_w) // 2
    y_off = (target_h - label_h - new_h) // 2
    result.paste(resized, (x_off, y_off))

    # Label
    draw = ImageDraw.Draw(result)
    font = _get_font(20)
    text_bbox = draw.textbbox((0, 0), label, font=font)
    tw = text_bbox[2] - text_bbox[0]
    draw.text(((target_w - tw) // 2, target_h - label_h + 6), label, fill=LABEL_COLOR, font=font)

    return result


def main():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    shapes = []  # (slug, display_name, Image)

    for form_name, model_fragment in FORM_REPRESENTATIVES:
        slug = form_name.lower().replace(" ", "_").replace("/", "_")

        row = conn.execute(
            """
            SELECT km.official_name, kmi.image_blob
            FROM knife_models_v2 km
            LEFT JOIN knife_forms frm ON frm.id = km.form_id
            LEFT JOIN knife_model_images kmi ON kmi.knife_model_id = km.id
            WHERE frm.name = ? AND km.official_name LIKE ?
                  AND kmi.image_blob IS NOT NULL AND length(kmi.image_blob) > 0
            LIMIT 1
            """,
            (form_name, f"%{model_fragment}%"),
        ).fetchone()

        if not row:
            # Try any model with this form
            row = conn.execute(
                """
                SELECT km.official_name, kmi.image_blob
                FROM knife_models_v2 km
                LEFT JOIN knife_forms frm ON frm.id = km.form_id
                LEFT JOIN knife_model_images kmi ON kmi.knife_model_id = km.id
                WHERE frm.name = ?
                      AND kmi.image_blob IS NOT NULL AND length(kmi.image_blob) > 0
                LIMIT 1
                """,
                (form_name,),
            ).fetchone()

        if row and row["image_blob"]:
            print(f"  {form_name}: using {row['official_name']}")
            sil = image_to_silhouette(row["image_blob"], CELL_W, CELL_H, form_name)
            sil.save(OUT_DIR / f"{slug}.png")
            shapes.append((slug, form_name, sil))
        else:
            print(f"  {form_name}: NO IMAGE FOUND — skipping")

    conn.close()

    if not shapes:
        print("No shapes generated!")
        return

    # Generate composite reference sheet
    cols = 4
    rows = math.ceil(len(shapes) / cols)
    sheet_w = cols * CELL_W
    sheet_h = rows * CELL_H + 60

    sheet = Image.new("RGB", (sheet_w, sheet_h), BG_COLOR)
    draw = ImageDraw.Draw(sheet)

    title_font = _get_font(28)
    title = "Blade Form Reference — Use These Names"
    bbox = draw.textbbox((0, 0), title, font=title_font)
    tw = bbox[2] - bbox[0]
    draw.text(((sheet_w - tw) // 2, 16), title, fill=(0, 0, 0), font=title_font)

    for i, (slug, name, img) in enumerate(shapes):
        col = i % cols
        row = i // cols
        x = col * CELL_W
        y = 60 + row * CELL_H
        sheet.paste(img, (x, y))
        draw.rectangle([x, y, x + CELL_W - 1, y + CELL_H - 1], outline=(200, 200, 200))

    sheet_path = OUT_DIR / "reference_sheet.png"
    sheet.save(sheet_path)
    print(f"\n  reference_sheet.png ({sheet.size[0]}x{sheet.size[1]})")
    print(f"\nGenerated {len(shapes)} silhouettes + reference sheet in {OUT_DIR}")


if __name__ == "__main__":
    main()
