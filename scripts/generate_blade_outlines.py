#!/usr/bin/env python3
"""Generate blade form silhouette PNGs and a composite reference sheet.

Reads SEED_POLYGONS from blade_ai.py and renders:
  - Individual silhouettes in static/blade-outlines/<slug>.png
  - A labeled composite grid in static/blade-outlines/reference_sheet.png
    (used by the vision model to identify blade forms by name)
"""

import math
import sys
from pathlib import Path

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from PIL import Image, ImageDraw, ImageFont
from blade_ai import SEED_POLYGONS

OUT_DIR = Path(__file__).resolve().parent.parent / "static" / "blade-outlines"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Individual silhouette dimensions
CELL_W, CELL_H = 400, 280
PAD = 30
BLADE_COLOR = (20, 20, 20)
BG_COLOR = (255, 255, 255)
LABEL_COLOR = (60, 60, 60)


def _scale_polygon(pts: list[list[int]], w: int, h: int, pad: int) -> list[tuple[int, int]]:
    """Scale 0-100 normalized polygon to fit within (w, h) with padding."""
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    min_x, max_x = min(xs), max(xs)
    min_y, max_y = min(ys), max(ys)
    range_x = max_x - min_x or 1
    range_y = max_y - min_y or 1

    draw_w = w - 2 * pad
    draw_h = h - 2 * pad
    scale = min(draw_w / range_x, draw_h / range_y)

    # Center the polygon
    offset_x = pad + (draw_w - range_x * scale) / 2
    offset_y = pad + (draw_h - range_y * scale) / 2

    return [
        (int((p[0] - min_x) * scale + offset_x),
         int((p[1] - min_y) * scale + offset_y))
        for p in pts
    ]


def _get_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    """Try common system fonts, fall back to default."""
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


def render_single(slug: str, display_name: str, pts: list[list[int]]) -> Image.Image:
    """Render one blade silhouette with label."""
    img = Image.new("RGB", (CELL_W, CELL_H), BG_COLOR)
    draw = ImageDraw.Draw(img)

    # Draw filled polygon
    label_h = 36
    poly = _scale_polygon(pts, CELL_W, CELL_H - label_h, PAD)
    draw.polygon(poly, fill=BLADE_COLOR, outline=BLADE_COLOR)

    # Label
    font = _get_font(20)
    bbox = draw.textbbox((0, 0), display_name, font=font)
    tw = bbox[2] - bbox[0]
    draw.text(((CELL_W - tw) // 2, CELL_H - label_h + 4), display_name, fill=LABEL_COLOR, font=font)

    return img


def render_reference_sheet(shapes: list[tuple[str, str, list[list[int]]]]) -> Image.Image:
    """Render a labeled grid of all blade forms for the vision model."""
    n = len(shapes)
    cols = 4
    rows = math.ceil(n / cols)

    sheet_w = cols * CELL_W
    sheet_h = rows * CELL_H + 60  # title bar

    img = Image.new("RGB", (sheet_w, sheet_h), BG_COLOR)
    draw = ImageDraw.Draw(img)

    # Title
    title_font = _get_font(28)
    title = "Blade Form Reference — Use These Names"
    bbox = draw.textbbox((0, 0), title, font=title_font)
    tw = bbox[2] - bbox[0]
    draw.text(((sheet_w - tw) // 2, 16), title, fill=(0, 0, 0), font=title_font)

    # Grid of silhouettes
    for i, (slug, display_name, pts) in enumerate(shapes):
        cell = render_single(slug, display_name, pts)
        col = i % cols
        row = i // cols
        img.paste(cell, (col * CELL_W, 60 + row * CELL_H))

        # Grid lines
        x = col * CELL_W
        y = 60 + row * CELL_H
        draw.rectangle([x, y, x + CELL_W - 1, y + CELL_H - 1], outline=(200, 200, 200))

    return img


def main():
    # Collect shapes in a stable order matching the DB knife_forms names
    db_form_order = [
        "drop_point", "trailing_point", "clip_point", "sheepsfoot",
        "spear", "dagger", "hawkbill", "skinner_belly",
        "fillet", "chef_rocker", "santoku", "petty",
        "paring", "cleaver", "hatchet", "tanto",
    ]

    shapes = []
    for slug in db_form_order:
        if slug in SEED_POLYGONS:
            display_name, _desc, pts = SEED_POLYGONS[slug]
            shapes.append((slug, display_name, pts))

    # Also pick up any shapes not in the explicit order
    for slug, (display_name, _desc, pts) in SEED_POLYGONS.items():
        if slug not in db_form_order:
            shapes.append((slug, display_name, pts))

    # Generate individual PNGs
    for slug, display_name, pts in shapes:
        single = render_single(slug, display_name, pts)
        out_path = OUT_DIR / f"{slug}.png"
        single.save(out_path)
        print(f"  {out_path.name}")

    # Generate composite reference sheet
    sheet = render_reference_sheet(shapes)
    sheet_path = OUT_DIR / "reference_sheet.png"
    sheet.save(sheet_path)
    print(f"  {sheet_path.name} ({sheet.size[0]}x{sheet.size[1]})")

    print(f"\nGenerated {len(shapes)} silhouettes + reference sheet in {OUT_DIR}")


if __name__ == "__main__":
    main()
