#!/usr/bin/env python3
"""Scan Images/ tree and populate `knife_model_image_files` metadata table.

Usage: python tools/sync_images.py

Creates a small metadata table mapping model slug and color to a filesystem path.
This keeps large binaries out of the DB while preserving provenance and lookup.
"""
from __future__ import annotations

import os
from pathlib import Path
import sqlite3
import re

ROOT = Path(__file__).resolve().parent.parent
IMAGES_DIR = ROOT / "Images"

def normalize_color(name: str) -> str:
    s = re.sub(r"[^A-Za-z0-9 _-]+", "", name)
    s = s.replace("_", " ").strip()
    return s.title()

def main() -> None:
    db_path = ROOT / "data" / "mkc_inventory.db"
    if not db_path.exists():
        print("DB not found at", db_path)
        return
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS knife_model_image_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            model_slug TEXT NOT NULL,
            color_name TEXT,
            file_path TEXT NOT NULL,
            is_primary INTEGER NOT NULL DEFAULT 0,
            sha256 TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    if not IMAGES_DIR.exists():
        print("Images directory not found at", IMAGES_DIR)
        return

    for model_dir in IMAGES_DIR.iterdir():
        if not model_dir.is_dir():
            continue
        slug = model_dir.name
        files = sorted(model_dir.iterdir())
        for i, f in enumerate(files):
            if not f.is_file():
                continue
            name = f.stem
            color = normalize_color(name)
            is_primary = 1 if i == 0 else 0
            conn.execute(
                "INSERT INTO knife_model_image_files (model_slug, color_name, file_path, is_primary) VALUES (?, ?, ?, ?)",
                (slug, color, str(f.relative_to(ROOT)), is_primary),
            )
    conn.commit()
    conn.close()
    print("Image metadata sync complete.")

if __name__ == '__main__':
    main()
