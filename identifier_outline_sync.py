"""
One-time seed: read ``montanaknife_identifier_outline.md``, set ``identifier_product_url`` on each
mapped master row, download the storefront hero image **once**, store **bytes + MIME + Hu silhouette
JSON** in SQLite. Runtime identification compares user photos against those stored Hu vectors — no
image HTTP on normal app startup.

Environment (only affects the seed run in ``init_db``):
    IDENTIFIER_SYNC_REFERENCE_IMAGES — ``0`` / ``false`` / ``no`` skips downloading images and
        computing silhouettes (URLs from the markdown are still applied).
    IDENTIFIER_REFRESH_REFERENCE_IMAGES — ``1`` / ``true`` re-downloads and recomputes even when
        a row already has stored bytes (for manual re-seed after deleting ``app_meta``).
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
from pathlib import Path
from typing import Any, Optional

import httpx

import blade_ai

URL_LINE_RE = re.compile(r"https://www\.montanaknifecompany\.com/products/[\w\-]+")
OG_IMAGE_RES = (
    re.compile(
        r'<meta\s+property=["\']og:image["\']\s+content=["\']([^"\']+)["\']',
        re.I,
    ),
    re.compile(
        r'<meta\s+content=["\']([^"\']+)["\']\s+property=["\']og:image["\']',
        re.I,
    ),
    re.compile(
        r'<meta\s+name=["\']twitter:image["\']\s+content=["\']([^"\']+)["\']',
        re.I,
    ),
)

OUTLINE_NAME_FALLBACKS: dict[str, tuple[str, ...]] = {
    "Whitetail Knife": ("Whitetail",),
}

TRADITIONS_SLUGS: dict[str, str] = {
    "traditions-speedgoat": "Traditions Speedgoat",
    "traditions-blackfoot-2-0": "Traditions Blackfoot 2.0",
    "traditions-jackstone": "Traditions Jackstone",
    "traditions-mkc-whitetail": "Traditions MKC Whitetail",
    "traditions-knives-full-set-of-5": "Traditions Knives Full Set of 5",
}

SECTION_HEADING_TO_MASTER: list[tuple[str, str]] = [
    ("mkc whitetail", "Whitetail Knife"),
    ("stoned goat", "Stoned Goat 2.0"),
    ("meat church", "Meat Church Chef Knife"),
    ("jackstone", "Jackstone"),
    ("magnacut blackfoot", "Magnacut Blackfoot 2.0"),
    ("blackfoot 2.0", "Blackfoot 2.0"),
    ("stockyard", "The Stockyard"),
    ("wargoat", "Wargoat"),
    ("battle goat", "Battle Goat"),
    ("tf24", "TF24"),
]

OUTLINE_BOOTSTRAP: list[dict[str, Any]] = [
    {
        "name": "Meat Church Chef Knife",
        "category": "Culinary",
        "blade_profile": "chef",
        "default_steel": "MagnaCut",
        "default_blade_finish": "PVD",
        "default_blade_color": "Steel",
        "record_type": "Collaboration model",
        "catalog_status": "Current",
        "notes": "Meat Church collaboration chef knife (storefront identifier outline).",
        "is_kitchen": 1,
        "is_collab": 1,
        "collaboration_name": "Meat Church",
        "identifier_keywords": "meat church, chef, culinary, orange, red, black",
    },
    {
        "name": "Magnacut Blackfoot 2.0",
        "category": "Hunting / all-purpose",
        "blade_profile": "drop point",
        "default_steel": "MagnaCut",
        "default_blade_finish": "Satin",
        "default_blade_color": "Steel",
        "record_type": "Major revision",
        "catalog_status": "Current",
        "notes": "MagnaCut steel Blackfoot 2.0 line per storefront outline.",
        "identifier_keywords": "magnacut, blackfoot, hunting, fixed blade",
    },
    {
        "name": "Traditions Speedgoat",
        "catalog_line": "Traditions",
        "category": "EDC / ultralight hunting",
        "blade_profile": "drop point",
        "default_steel": "MagnaCut",
        "default_blade_finish": "Satin",
        "default_blade_color": "Steel",
        "record_type": "Limited series",
        "catalog_status": "Upcoming / limited drop",
        "notes": "MKC Traditions series Speedgoat (outline Mar 2026).",
        "identifier_keywords": "traditions, speedgoat, limited, traditional",
    },
    {
        "name": "Traditions Blackfoot 2.0",
        "catalog_line": "Traditions",
        "category": "Hunting / all-purpose",
        "blade_profile": "drop point",
        "default_steel": "MagnaCut",
        "default_blade_finish": "Satin",
        "default_blade_color": "Steel",
        "record_type": "Limited series",
        "catalog_status": "Upcoming / limited drop",
        "notes": "MKC Traditions series Blackfoot 2.0.",
        "identifier_keywords": "traditions, blackfoot, limited",
    },
    {
        "name": "Traditions Jackstone",
        "catalog_line": "Traditions",
        "category": "Hunting / belt knife",
        "blade_profile": "drop point",
        "default_steel": "MagnaCut",
        "default_blade_finish": "PVD",
        "default_blade_color": "Steel",
        "record_type": "Limited series",
        "catalog_status": "Upcoming / limited drop",
        "notes": "MKC Traditions series Jackstone.",
        "identifier_keywords": "traditions, jackstone, belt knife, limited",
    },
    {
        "name": "Traditions MKC Whitetail",
        "catalog_line": "Traditions",
        "category": "Hunting",
        "blade_profile": "drop point",
        "default_steel": "MagnaCut",
        "default_blade_finish": "PVD",
        "default_blade_color": "Steel",
        "record_type": "Limited series",
        "catalog_status": "Upcoming / limited drop",
        "notes": "MKC Traditions series Whitetail.",
        "identifier_keywords": "traditions, whitetail, limited",
    },
    {
        "name": "Traditions Knives Full Set of 5",
        "catalog_line": "Traditions",
        "category": "Heritage / traditional",
        "blade_profile": "mixed",
        "default_steel": "MagnaCut",
        "default_blade_finish": "Satin",
        "default_blade_color": "Steel",
        "record_type": "Limited set",
        "catalog_status": "Upcoming / limited drop",
        "notes": "Traditions full set bundle (outline).",
        "identifier_keywords": "traditions, set, bundle, full set",
    },
]


def parse_outline_product_urls(md_text: str) -> list[tuple[str, str]]:
    assignments: list[tuple[str, str]] = []
    seen_master: set[str] = set()
    current_heading = ""

    for line in md_text.splitlines():
        st = line.strip()
        if st.startswith("###"):
            current_heading = st.lstrip("#").strip().lower()
            continue
        if "montanaknifecompany.com/products/" not in line:
            continue
        m = URL_LINE_RE.search(line)
        if not m:
            continue
        url = m.group(0).rstrip(")")
        slug = url.rsplit("/", 1)[-1]

        if "tradition" in current_heading:
            tname = TRADITIONS_SLUGS.get(slug)
            if tname:
                assignments.append((tname, url))
            continue

        for needle, master in SECTION_HEADING_TO_MASTER:
            if needle in current_heading:
                if master in seen_master:
                    break
                seen_master.add(master)
                assignments.append((master, url))
                break

    return assignments


def fetch_og_image_url(product_url: str, client: httpx.Client) -> Optional[str]:
    try:
        r = client.get(
            product_url,
            headers={"User-Agent": "MKC-Inventory/1.0 (catalog seed; local)"},
            follow_redirects=True,
        )
        r.raise_for_status()
        for cre in OG_IMAGE_RES:
            m = cre.search(r.text)
            if m:
                return m.group(1).strip()
        return None
    except Exception:
        return None


def download_image_bytes(image_url: str, client: httpx.Client) -> tuple[Optional[bytes], Optional[str]]:
    try:
        r = client.get(
            image_url,
            headers={"User-Agent": "MKC-Inventory/1.0 (catalog seed; local)"},
            follow_redirects=True,
        )
        r.raise_for_status()
        raw_ct = (r.headers.get("content-type") or "").split(";")[0].strip().lower()
        mime = raw_ct if raw_ct.startswith("image/") else "image/jpeg"
        if mime == "image/jpg":
            mime = "image/jpeg"
        return r.content, mime
    except Exception:
        return None, None


def bootstrap_outline_models(conn: sqlite3.Connection) -> int:
    added = 0
    for spec in OUTLINE_BOOTSTRAP:
        name = spec["name"]
        if conn.execute("SELECT 1 FROM master_knives WHERE name = ?", (name,)).fetchone():
            continue
        conn.execute(
            """
            INSERT INTO master_knives
            (name, category, catalog_line, blade_profile, default_steel, default_blade_finish, default_blade_color,
             record_type, catalog_status, notes, is_kitchen, is_collab, collaboration_name,
             is_tactical, has_ring, is_filleting_knife, is_hatchet, status, identifier_keywords,
             updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, 'active', ?, CURRENT_TIMESTAMP)
            """,
            (
                name,
                spec.get("category"),
                spec.get("catalog_line"),
                spec.get("blade_profile"),
                spec.get("default_steel"),
                spec.get("default_blade_finish"),
                spec.get("default_blade_color"),
                spec.get("record_type"),
                spec.get("catalog_status"),
                spec.get("notes"),
                int(spec.get("is_kitchen") or 0),
                int(spec.get("is_collab") or 0),
                spec.get("collaboration_name"),
                spec.get("identifier_keywords"),
            ),
        )
        added += 1
    return added


def sync_identifier_outline(
    conn: sqlite3.Connection,
    md_path: Path,
    *,
    download_reference_images: Optional[bool] = None,
) -> dict[str, Any]:
    if not md_path.is_file():
        return {"skipped": True, "reason": "outline file missing"}

    if download_reference_images is None:
        download_reference_images = os.environ.get("IDENTIFIER_SYNC_REFERENCE_IMAGES", "1").lower() not in (
            "0",
            "false",
            "no",
        )

    refresh = os.environ.get("IDENTIFIER_REFRESH_REFERENCE_IMAGES", "").lower() in ("1", "true", "yes")

    md_text = md_path.read_text(encoding="utf-8")
    boot_added = bootstrap_outline_models(conn)
    pairs = parse_outline_product_urls(md_text)

    updated_urls = 0
    stored_images = 0
    stored_silhouettes = 0
    missing_masters: list[str] = []

    client: Optional[httpx.Client] = None
    if download_reference_images:
        client = httpx.Client(timeout=35.0, follow_redirects=True)

    try:
        for name, product_url in pairs:
            db_name = name
            row = conn.execute(
                """
                SELECT id, identifier_image_blob, identifier_silhouette_hu_json
                FROM master_knives WHERE name = ?
                """,
                (db_name,),
            ).fetchone()
            if not row:
                for alt in OUTLINE_NAME_FALLBACKS.get(name, ()):
                    row = conn.execute(
                        """
                        SELECT id, identifier_image_blob, identifier_silhouette_hu_json
                        FROM master_knives WHERE name = ?
                        """,
                        (alt,),
                    ).fetchone()
                    if row:
                        db_name = alt
                        break
            if not row:
                missing_masters.append(name)
                continue

            updated_urls += 1
            blob = row.get("identifier_image_blob")
            has_blob = blob is not None and len(blob) > 0
            has_hu = bool((row.get("identifier_silhouette_hu_json") or "").strip())

            img_url: Optional[str] = None
            img_bytes: Optional[bytes] = None
            mime: Optional[str] = None
            hu_json: Optional[str] = None

            need_media = client is not None and (
                refresh or not has_blob or not has_hu
            )
            if need_media:
                img_url = fetch_og_image_url(product_url, client)
                if img_url:
                    img_bytes, mime = download_image_bytes(img_url, client)
                if img_bytes:
                    hu_list, _err = blade_ai.extract_blade_hu_from_image_bytes(img_bytes)
                    if hu_list:
                        hu_json = json.dumps(hu_list)
                        stored_silhouettes += 1
                    stored_images += 1

            conn.execute(
                """
                UPDATE master_knives
                SET identifier_product_url = ?,
                    identifier_image_blob = COALESCE(?, identifier_image_blob),
                    identifier_image_mime = COALESCE(?, identifier_image_mime),
                    identifier_silhouette_hu_json = COALESCE(?, identifier_silhouette_hu_json),
                    updated_at = CURRENT_TIMESTAMP
                WHERE name = ?
                """,
                (product_url, img_bytes, mime, hu_json, db_name),
            )
    finally:
        if client:
            client.close()

    return {
        "skipped": False,
        "bootstrap_inserted": boot_added,
        "models_linked": updated_urls,
        "reference_images_stored_this_run": stored_images,
        "silhouettes_stored_this_run": stored_silhouettes,
        "missing_master_names": missing_masters,
        "download_reference_images": download_reference_images,
    }
