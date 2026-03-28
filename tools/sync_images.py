#!/usr/bin/env python3
"""Scan Images/MKC_Colors/ and populate knife_model_image_files metadata table.

Usage:
    python tools/sync_images.py [--db PATH] [--dry-run]

Idempotent: clears and repopulates the table on every run. Matches image
filenames to knife_models_v2 slugs using compact-key comparison (all
non-alphanumeric chars stripped and lowercased), which handles variations
like "2.0" vs "20" in filenames. The remaining tokens after the model
prefix become the canonical color name.

Primary image selection per model:
  1. File whose color matches knife_models_v2.handle_color (default color)
  2. "Orange Black" if available and no explicit default set
  3. First file alphabetically

Unmatched files are logged to stdout for manual review.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import re
import sqlite3
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
COLORS_DIR = ROOT / "Images" / "MKC_Colors"
URL_PREFIX = "/images/colors"


def compact(s: str) -> str:
    """Strip all non-alphanumeric chars and lowercase — used for fuzzy matching."""
    return re.sub(r"[^a-z0-9]+", "", s.lower())


def normalize_color(tokens: list[str]) -> str:
    """Convert filename color tokens to a canonical color name."""
    return " ".join(t.replace("_", " ") for t in tokens).title().strip()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def build_model_index(
    conn: sqlite3.Connection,
) -> dict[str, dict]:
    """Return {compact_key: model_row} for all knife_models_v2 rows.

    Sorted longest-key-first so the matching loop prefers the most specific
    (longest) model name when prefixes overlap.
    """
    rows = conn.execute(
        "SELECT id, slug, official_name, handle_color FROM knife_models_v2"
    ).fetchall()
    index: dict[str, dict] = {}
    for r in rows:
        key = compact(r["official_name"])
        if key:
            index[key] = dict(r)
    return index


def match_file(
    stem: str,
    model_index: dict[str, dict],
) -> Optional[tuple[dict, str]]:
    """Return (model_row, color_name) for a filename stem, or None if no match.

    Tries each split point from right-to-left (longest prefix first) so that
    a model like "Flattail PVD" is preferred over "Flattail" when the filename
    starts with "Flattail_PVD".
    """
    tokens = stem.split("_")
    # Try from longest prefix down to shortest (need at least 1 color token)
    for i in range(len(tokens) - 1, 0, -1):
        prefix_compact = compact("".join(tokens[:i]))
        if prefix_compact in model_index:
            color = normalize_color(tokens[i:])
            return model_index[prefix_compact], color
    return None


def run(db_path: Path, dry_run: bool = False) -> None:
    if not COLORS_DIR.exists():
        print(f"ERROR: Images directory not found at {COLORS_DIR}")
        return

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    model_index = build_model_index(conn)

    # Collect all candidate files (skip OS files and known duplicates)
    files = sorted(
        f for f in COLORS_DIR.iterdir()
        if f.is_file()
        and f.suffix.lower() in (".jpg", ".jpeg", ".png", ".webp")
        and " (" not in f.stem   # skip "ModelName_Color (1).jpg" duplicates
    )

    # Build records: list of (model_slug, color_name, filename, url_path, sha256)
    records: list[dict] = []
    unmatched: list[str] = []

    for filepath in files:
        result = match_file(filepath.stem, model_index)
        if result is None:
            unmatched.append(filepath.name)
            continue
        model, color = result
        records.append({
            "model_slug": model["slug"],
            "color_name": color,
            "filename": filepath.name,
            "url_path": f"{URL_PREFIX}/{filepath.name}",
            "handle_color": model["handle_color"],
            "sha256": sha256_file(filepath),
        })

    # Determine is_primary per model
    # Group by slug
    by_slug: dict[str, list[dict]] = {}
    for rec in records:
        by_slug.setdefault(rec["model_slug"], []).append(rec)

    final_records: list[dict] = []
    for slug, slug_records in by_slug.items():
        # Sort alphabetically for stable primary selection
        slug_records.sort(key=lambda r: r["filename"])
        primary_set = False

        # Pass 1: exact handle_color match
        for rec in slug_records:
            model_color = (rec["handle_color"] or "").strip()
            if model_color and compact(model_color) == compact(rec["color_name"]):
                rec["is_primary"] = 1
                primary_set = True
                break

        # Pass 2: fallback to Orange Black
        if not primary_set:
            for rec in slug_records:
                if compact(rec["color_name"]) == compact("Orange Black"):
                    rec["is_primary"] = 1
                    primary_set = True
                    break

        # Pass 3: fallback to first alphabetically
        if not primary_set and slug_records:
            slug_records[0]["is_primary"] = 1

        for rec in slug_records:
            rec.setdefault("is_primary", 0)
            final_records.append(rec)

    if dry_run:
        print(f"DRY RUN — would upsert {len(final_records)} records")
        for rec in final_records[:10]:
            print(f"  {rec['model_slug']} | {rec['color_name']} | {rec['filename']} | primary={rec['is_primary']}")
        if len(final_records) > 10:
            print(f"  ... and {len(final_records) - 10} more")
    else:
        conn.execute("DELETE FROM knife_model_image_files")
        conn.executemany(
            """
            INSERT INTO knife_model_image_files
                (model_slug, color_name, filename, url_path, is_primary, sha256)
            VALUES
                (:model_slug, :color_name, :filename, :url_path, :is_primary, :sha256)
            """,
            final_records,
        )
        conn.commit()
        print(f"Synced {len(final_records)} image records across {len(by_slug)} models.")

    if unmatched:
        print(f"\nUnmatched files ({len(unmatched)}) — no model slug found:")
        for name in sorted(unmatched):
            print(f"  {name}")

    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync MKC_Colors images to DB metadata table.")
    parser.add_argument("--db", default=str(ROOT / "data" / "mkc_inventory.db"), help="DB path")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be synced without writing")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"ERROR: DB not found at {db_path}")
        return

    run(db_path, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
