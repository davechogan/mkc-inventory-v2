"""
MKC inventory API: SQLite-backed master catalog, per-piece inventory, descriptor options,
CSV import/export for the master list, knife identification scoring, and inventory duplication.

On first run, seed files (if present) populate the master catalog; reference images are stored as BLOBs
in SQLite with Hu-moment silhouette vectors for offline identification. Use the Master page or
``/api/master-knives/import.csv`` to add new models.

Web UI: ``/`` collection dashboard, ``/master`` catalog and descriptor management.

Optional: set ``OLLAMA_HOST`` (default ``http://192.168.50.196:11434``) for AI + vision identification via Ollama;
blade silhouette templates live in ``blade_shape_templates`` (Hu moments vs OpenCV hints).

Optional: set ``MKC_INVENTORY_DB`` to an absolute path to override the default SQLite file under ``data/``
(useful for CI or smoke tests on read-only or flaky volumes).
"""
from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
import csv
import io
import json
import os
import re
import sqlite3
import subprocess
import sys
import uuid
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
import time
from typing import Any, Optional
from urllib.parse import urlencode

import httpx

import blade_ai
import identifier_outline_sync
import normalized_model
from mkc_csv_columns import INVENTORY_CSV_COLUMNS, MASTER_CSV_COLUMNS
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator
from sqlite_schema import column_exists

from reporting import (
    _reporting_plan_to_sql,
    _reporting_validate_sql,
    ensure_reporting_schema,
)
from reporting.routes import create_reporting_router
from routes.admin_routes import (
    DistinguishingFeaturesRecomputeBody,
    create_admin_router,
    recompute_silhouettes_for_masters_without_hu,
)
from routes.ai_routes import create_ai_router
from routes.normalized_routes import create_normalized_router
from routes.static_pages_routes import create_static_pages_router
from routes.v2_routes import create_v2_router

BASE_DIR = Path(__file__).resolve().parent
_db_override = (os.environ.get("MKC_INVENTORY_DB") or "").strip()
DB_PATH = (
    Path(_db_override).expanduser().resolve()
    if _db_override
    else (BASE_DIR / "data" / "mkc_inventory.db")
)
STATIC_DIR = BASE_DIR / "static"
LOG_PATH = BASE_DIR / "data" / "mkc_app.log"

def _parse_log_level(value: Optional[str], default: int = logging.INFO) -> int:
    raw = (value or "").strip().upper()
    if not raw:
        return default
    return getattr(logging, raw, default)


def _configure_logging() -> logging.Logger:
    """
    Configure app + server logging with separate console/file levels.

    Env vars:
    - APP_LOG_LEVEL (default: INFO)
    - APP_LOG_CONSOLE_LEVEL (default: APP_LOG_LEVEL)
    - APP_LOG_FILE_LEVEL (default: APP_LOG_LEVEL)
    - APP_LOG_FILE_MAX_BYTES (default: 10485760 = 10MB)
    - APP_LOG_FILE_BACKUPS (default: 5)
    """
    BASE_DIR.joinpath("data").mkdir(parents=True, exist_ok=True)

    app_level = _parse_log_level(os.environ.get("APP_LOG_LEVEL"), logging.INFO)
    console_level = _parse_log_level(os.environ.get("APP_LOG_CONSOLE_LEVEL"), app_level)
    file_level = _parse_log_level(os.environ.get("APP_LOG_FILE_LEVEL"), app_level)

    try:
        file_max_bytes = max(1024, int(os.environ.get("APP_LOG_FILE_MAX_BYTES", "10485760")))
    except ValueError:
        file_max_bytes = 10485760
    try:
        file_backups = max(1, int(os.environ.get("APP_LOG_FILE_BACKUPS", "5")))
    except ValueError:
        file_backups = 5

    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    file_handler = RotatingFileHandler(LOG_PATH, maxBytes=file_max_bytes, backupCount=file_backups, encoding="utf-8")
    file_handler.setLevel(file_level)
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(formatter)

    # Root logger: ensures third-party and framework logs can also be persisted to disk.
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(min(app_level, console_level, file_level))
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    app_logger = logging.getLogger("mkc_app")
    app_logger.handlers.clear()
    app_logger.setLevel(app_level)
    app_logger.propagate = True

    # Mirror uvicorn/fastapi logs into the same handlers.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "fastapi"):
        lg = logging.getLogger(name)
        lg.handlers.clear()
        lg.setLevel(app_level)
        lg.propagate = True

    app_logger.info(
        "Logging configured: level=%s console=%s file=%s path=%s",
        logging.getLevelName(app_level),
        logging.getLevelName(console_level),
        logging.getLevelName(file_level),
        LOG_PATH,
    )
    return app_logger


_app_logger = _configure_logging()
OLLAMA_VISION_MODEL = (os.environ.get("OLLAMA_VISION_MODEL") or "qwen3-vl:latest").strip() or "qwen3-vl:latest"

# Authoritative model list + research fields (record type, URLs, evidence, etc.)
KNIFE_MASTER_CSV = BASE_DIR / "Knife Master.csv"  # Optional; moved to cleanup/ after catalog built
# After first run, CSV + outline file are not re-applied automatically (see ``init_db``).
AUTO_KNIFE_FILE_SEED_META_KEY = "auto_knife_file_seed_v1"

# List/API responses exclude ``identifier_image_blob`` (large); use ``has_identifier_image`` + image route.
# v2 Phase 1 fields included when present (canonical_slug, version, parent_model_id, lifecycle, msrp, traits, URLs)
MASTER_KNIVES_PUBLIC_COLUMNS = (
    "id, name, family, default_blade_length, default_steel, default_blade_finish, default_blade_color, "
    "(SELECT COUNT(*) FROM inventory_items WHERE master_knife_id = master_knives.id) AS in_inventory_count, "
    "default_handle_color, record_type, catalog_status, confidence, evidence_summary, collector_notes, "
    "identifier_product_url, identifier_image_mime, identifier_silhouette_hu_json, "
    "(CASE WHEN identifier_image_blob IS NOT NULL AND length(identifier_image_blob) > 0 "
    "THEN 1 ELSE 0 END) AS has_identifier_image, "
    "(CASE WHEN identifier_silhouette_hu_json IS NOT NULL AND trim(identifier_silhouette_hu_json) != '' "
    "THEN 1 ELSE 0 END) AS has_silhouette_hint, "
    "is_collab, collaboration_name, status, notes, created_at, updated_at, "
    "category, catalog_line, blade_profile, "
    "has_ring, is_filleting_knife, is_hatchet, is_kitchen, is_tactical, identifier_keywords, identifier_distinguishing_features, "
    "canonical_slug, version, parent_model_id, first_release_date, last_seen_date, "
    "is_discontinued, is_current_catalog, msrp, blade_shape, tip_style, grind_style, size_class, "
    "primary_use_case, spine_profile, is_fillet, default_product_url, primary_image_url"
)


IDENTIFIER_COLUMNS = {
    "category": "TEXT",
    "blade_profile": "TEXT",
    "blade_shape": "TEXT",
    "has_ring": "INTEGER NOT NULL DEFAULT 0",
    "is_filleting_knife": "INTEGER NOT NULL DEFAULT 0",
    "is_hatchet": "INTEGER NOT NULL DEFAULT 0",
    "is_kitchen": "INTEGER NOT NULL DEFAULT 0",
    "is_tactical": "INTEGER NOT NULL DEFAULT 0",
    "identifier_keywords": "TEXT",
}


def dict_factory(cursor: sqlite3.Cursor, row: tuple[Any, ...]) -> dict[str, Any]:
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


@contextmanager
def get_conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = dict_factory
    conn.execute("PRAGMA foreign_keys = ON;")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()
from migrations.migrate_v2 import (
    ensure_v2_exclusive_schema,
    migrate_legacy_media_to_v2,
    ensure_phase1_schema,
    ensure_version_parent_model_columns,
    ensure_identifier_columns,
    ensure_master_extra_columns,
    ensure_master_catalog_columns,
    backfill_v2_model_identity,
    normalize_v2_additional_fields,
    table_exists,
)

def infer_identifier_flags(
    name: str, category: Optional[str], record_type: Optional[str]
) -> tuple[int, int, int, int, int, int]:
    """Best-effort defaults when inserting a row only from Knife Master.csv (no blade specs)."""
    n = (name or "").lower()
    c = (category or "").lower()
    r = (record_type or "").lower()
    is_collab = 1 if ("collaboration" in r or "collab" in r) else 0
    is_tactical = 1 if "tactical" in c else 0
    is_kitchen = 1 if any(x in c for x in ("culinary", "butchery", "butcher", "steak", "paring", "santoku", "chef", "cleaver")) else 0
    is_hatchet = 1 if ("axe" in c or "hatchet" in c) else 0
    is_filleting = 1 if ("fillet" in n or "fillet" in c or c == "fishing") else 0
    has_ring = 1 if "wargoat" in n else 0
    return has_ring, is_filleting, is_hatchet, is_kitchen, is_tactical, is_collab


CANONICAL_CATEGORY_NAMES = (
    "Hunting",
    "Culinary",
    "Tactical",
    "Everyday Carry",
    "Bushcraft & Camp",
)


def normalize_category_value(value: Optional[str]) -> Optional[str]:
    """Normalize free-form category text to canonical category names."""
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    lower = raw.lower()

    if lower in {
        "hunting",
        "culinary",
        "tactical",
        "everyday carry",
        "bushcraft & camp",
    }:
        return {
            "hunting": "Hunting",
            "culinary": "Culinary",
            "tactical": "Tactical",
            "everyday carry": "Everyday Carry",
            "bushcraft & camp": "Bushcraft & Camp",
        }[lower]

    culinary_tokens = ("culinary", "kitchen", "chef", "butcher", "steak", "paring", "santoku", "cleaver", "fillet")
    tactical_tokens = ("tactical",)
    camp_tokens = ("bushcraft", "camp", "hatchet", "axe", "chopper")
    edc_tokens = ("edc", "everyday carry", "utility", "work", "ranch")
    hunting_tokens = ("hunting", "archery", "waterfowl", "small-game", "processing", "skinner", "belt knife", "all-purpose", "traditions", "heritage")

    if any(tok in lower for tok in culinary_tokens):
        return "Culinary"
    if any(tok in lower for tok in tactical_tokens):
        return "Tactical"
    if any(tok in lower for tok in camp_tokens):
        return "Bushcraft & Camp"
    if any(tok in lower for tok in edc_tokens):
        return "Everyday Carry"
    if any(tok in lower for tok in hunting_tokens):
        return "Hunting"
    return "Hunting"


def normalize_master_category_data(conn: sqlite3.Connection) -> int:
    """
    Normalize existing master category values against canonical categories.
    Returns number of changed rows.
    """
    rows = conn.execute("SELECT id, category FROM master_knives").fetchall()
    changed = 0
    for row in rows:
        before = row.get("category")
        after = normalize_category_value(before)
        if (before or None) != after:
            conn.execute(
                "UPDATE master_knives SET category = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (after, row["id"]),
            )
            changed += 1
    for category in CANONICAL_CATEGORY_NAMES:
        conn.execute(
            "INSERT OR IGNORE INTO option_categories (name) VALUES (?)",
            (category,),
        )
    return changed






def sync_knife_master_csv_file(conn: sqlite3.Connection, path: Path) -> tuple[int, int]:
    """
    Upsert rows from ``Knife Master.csv`` by model name.
    Updates catalog/research fields; preserves blade defaults and identifier flags on existing rows.
    """
    if not path.is_file():
        return 0, 0
    inserted = 0
    updated = 0
    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return 0, 0
        fields = {(h or "").strip().lstrip("\ufeff") for h in reader.fieldnames if h}
        required = {
            "Model / family",
            "Record type",
            "Category",
            "Status",
            "Confidence",
            "Evidence summary",
            "Collector notes",
            "Primary source URL",
            "Secondary source URL",
        }
        if not required.issubset(fields):
            return 0, 0

        fnames_l = {(h or "").strip().lower() for h in reader.fieldnames if h}
        km_has_catalog_line_col = any(
            x in fnames_l for x in ("catalog line", "catalog_line")
        )

        def nz(val: Optional[str]) -> Optional[str]:
            s = (val or "").strip()
            return s if s else None

        for raw in reader:
            row = {(k or "").strip(): (v if v is None else str(v).strip()) for k, v in raw.items()}
            name = (row.get("Model / family") or "").strip()
            if not name:
                continue

            record_type = nz(row.get("Record type"))
            category = normalize_category_value(nz(row.get("Category")))
            catalog_status = nz(row.get("Status"))
            confidence = nz(row.get("Confidence"))
            evidence_summary = nz(row.get("Evidence summary"))
            collector_notes = nz(row.get("Collector notes"))
            cl_val: Optional[str] = None
            if km_has_catalog_line_col:
                cl_raw = (row.get("Catalog line") or row.get("catalog_line") or "").strip()
                cl_val = normalize_master_catalog_line_input(cl_raw, strict=False) if cl_raw else None
            existing = conn.execute("SELECT id FROM master_knives WHERE name = ?", (name,)).fetchone()
            if existing:
                if km_has_catalog_line_col:
                    conn.execute(
                        """
                        UPDATE master_knives
                        SET record_type = ?, category = ?, catalog_line = ?, catalog_status = ?, confidence = ?,
                            evidence_summary = ?, collector_notes = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (
                            record_type,
                            category,
                            cl_val,
                            catalog_status,
                            confidence,
                            evidence_summary,
                            collector_notes,
                            existing["id"],
                        ),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE master_knives
                        SET record_type = ?, category = ?, catalog_status = ?, confidence = ?,
                            evidence_summary = ?, collector_notes = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (
                            record_type,
                            category,
                            catalog_status,
                            confidence,
                            evidence_summary,
                            collector_notes,
                            existing["id"],
                        ),
                    )
                updated += 1
            else:
                hr, ff, hx, kit, tac, col = infer_identifier_flags(name, category, record_type)
                conn.execute(
                    """
                    INSERT INTO master_knives
                    (name, record_type, category, catalog_line, catalog_status, confidence, evidence_summary,
                     collector_notes, has_ring, is_filleting_knife, is_hatchet, is_kitchen, is_tactical, is_collab,
                     status, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', CURRENT_TIMESTAMP)
                    """,
                    (
                        name,
                        record_type,
                        category,
                        cl_val,
                        catalog_status,
                        confidence,
                        evidence_summary,
                        collector_notes,
                        hr,
                        ff,
                        hx,
                        kit,
                        tac,
                        col,
                    ),
                )
                inserted += 1
    return inserted, updated


INVENTORY_EXTRA_COLUMNS = {
    "blade_length": "REAL",
    "purchase_source": "TEXT",
    "last_sharpened": "TEXT",
}


def ensure_inventory_extra_columns(conn: sqlite3.Connection) -> None:
    for column, sql_type in INVENTORY_EXTRA_COLUMNS.items():
        if not column_exists(conn, "inventory_items", column):
            conn.execute(f"ALTER TABLE inventory_items ADD COLUMN {column} {sql_type}")


def ensure_blade_shape_templates(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS blade_shape_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            description TEXT,
            hu_json TEXT NOT NULL,
            outline_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    count = conn.execute("SELECT COUNT(*) AS c FROM blade_shape_templates").fetchone()["c"]
    if count == 0:
        for row in blade_ai.seed_blade_shape_rows():
            conn.execute(
                """
                INSERT OR IGNORE INTO blade_shape_templates
                (slug, name, description, hu_json, outline_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                row,
            )


def ensure_master_identifier_media_columns(conn: sqlite3.Connection) -> None:
    if not column_exists(conn, "master_knives", "identifier_product_url"):
        conn.execute("ALTER TABLE master_knives ADD COLUMN identifier_product_url TEXT")
    if not column_exists(conn, "master_knives", "identifier_image_blob"):
        conn.execute("ALTER TABLE master_knives ADD COLUMN identifier_image_blob BLOB")
    if not column_exists(conn, "master_knives", "identifier_image_mime"):
        conn.execute("ALTER TABLE master_knives ADD COLUMN identifier_image_mime TEXT")
    if not column_exists(conn, "master_knives", "identifier_silhouette_hu_json"):
        conn.execute("ALTER TABLE master_knives ADD COLUMN identifier_silhouette_hu_json TEXT")
    if not column_exists(conn, "master_knives", "identifier_distinguishing_features"):
        conn.execute(
            "ALTER TABLE master_knives ADD COLUMN identifier_distinguishing_features TEXT"
        )


def ensure_master_catalog_line_column(conn: sqlite3.Connection) -> None:
    if not column_exists(conn, "master_knives", "catalog_line"):
        conn.execute("ALTER TABLE master_knives ADD COLUMN catalog_line TEXT")


BLADE_TYPE_ALIASES = {
    "drop point": "Drop point",
    "clip point": "Clip point",
    "trailing point": "Trailing point",
    "trailing": "Trailing point",
    "skinner": "Skinner",
    "fillet": "Fillet",
    "sheepsfoot": "Sheepsfoot",
    "tanto": "Tanto",
    "spear": "Spear",
    "chef": "Chef",
    "chef/butcher": "Chef",
    "cleaver": "Cleaver",
    "santoku": "Santoku",
    "hatchet": "Hatchet",
    "tactical": "Tactical",
    "mixed": "Mixed",
}


def ensure_tier_option_tables(conn: sqlite3.Connection) -> None:
    """Create option_categories, option_blade_families, option_primary_use_cases if missing."""
    for table in ("option_categories", "option_blade_families", "option_primary_use_cases"):
        if conn.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'"
        ).fetchone():
            continue
        conn.execute(
            f"""
            CREATE TABLE {table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            )
            """
        )


def _backfill_family_from_name_once(conn: sqlite3.Connection) -> None:
    """One-time: set family from derived name where family is empty."""
    if conn.execute(
        "SELECT 1 FROM app_meta WHERE key = 'family_backfilled_from_name'"
    ).fetchone():
        return
    rows = conn.execute(
        "SELECT id, name, family FROM master_knives WHERE name IS NOT NULL"
    ).fetchall()
    updated = 0
    for row in rows:
        if not (row.get("family") or "").strip():
            derived = derive_blade_family_from_name(row.get("name"))
            if derived:
                conn.execute(
                    "UPDATE master_knives SET family = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (derived, row["id"]),
                )
                updated += 1
    conn.execute(
        "INSERT OR REPLACE INTO app_meta (key, value) VALUES ('family_backfilled_from_name', '1')"
    )


def _seed_tier_options_once(conn: sqlite3.Connection) -> None:
    """One-time seed of categories and primary use cases."""
    if conn.execute(
        "SELECT 1 FROM app_meta WHERE key = 'tier_options_seeded'"
    ).fetchone():
        return
    categories = [
        "Hunting",
        "Culinary",
        "Tactical",
        "Everyday Carry",
        "Bushcraft & Camp",
    ]
    use_cases = [
        "Skinning",
        "Hunting",
        "Cooking",
        "Fishing",
        "EDC",
        "Tactical",
        "Camp/Bushcraft",
        "Fillet",
        "Utility",
    ]
    for name in categories:
        try:
            conn.execute(
                "INSERT INTO option_categories (name) VALUES (?)",
                (name,),
            )
        except sqlite3.IntegrityError:
            pass
    for name in use_cases:
        try:
            conn.execute(
                "INSERT INTO option_primary_use_cases (name) VALUES (?)",
                (name,),
            )
        except sqlite3.IntegrityError:
            pass
    conn.execute(
        "INSERT OR REPLACE INTO app_meta (key, value) VALUES ('tier_options_seeded', '1')"
    )


def derive_blade_family_from_name(name: Optional[str]) -> str:
    """
    Derive blade family from model name. Used for dropdown population and auto-suggest.
    - "Speedgoat Tactical" -> "Speedgoat"
    - "Stoned Goat 2.0" -> "Stoned Goat"
    - "TF24" -> "TF24" (standalone)
    """
    if not name or not str(name).strip():
        return ""
    s = str(name).strip()
    s = re.sub(r"\s+Tactical\s*$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+2\.0\s*$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+2\s*$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+3\.0\s*$", "", s, flags=re.IGNORECASE)
    s = s.strip()
    return s if s else name.strip()


def ensure_blade_types_option_table(conn: sqlite3.Connection) -> None:
    """Create option_blade_types if missing (e.g. existing DBs)."""
    if conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='option_blade_types'"
    ).fetchone():
        return
    conn.execute(
        """
        CREATE TABLE option_blade_types (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        )
        """
    )


def _harmonize_blade_types_once(conn: sqlite3.Connection) -> None:
    """One-time migration: normalize blade_shape/blade_profile to canonical option names."""
    if conn.execute(
        "SELECT 1 FROM app_meta WHERE key = 'blade_types_harmonized'"
    ).fetchone():
        return
    for raw, canonical in BLADE_TYPE_ALIASES.items():
        conn.execute(
            "UPDATE master_knives SET blade_shape = ?, blade_profile = ? WHERE LOWER(TRIM(COALESCE(blade_shape, ''))) = ? OR LOWER(TRIM(COALESCE(blade_profile, ''))) = ?",
            (canonical, canonical, raw, raw),
        )
    conn.execute(
        "INSERT OR REPLACE INTO app_meta (key, value) VALUES ('blade_types_harmonized', '1')"
    )


# Base models from mkc_missing_items.md — ensure they exist for identifier product URL mapping.
MKC_MISSING_ITEMS_BOOTSTRAP: list[dict[str, Any]] = [
    {
        "name": "Whitetail Knife",
        "category": "Hunting",
        "blade_profile": "drop point",
        "default_blade_length": 3.5,
        "default_steel": "MagnaCut",
        "default_blade_finish": "PVD",
        "default_blade_color": "Steel",
        "record_type": "Standalone model",
        "catalog_status": "Current",
        "notes": "MKC Whitetail PVD series (mkc_missing_items).",
        "identifier_keywords": "whitetail, pvd, hunting, deer, buck skin, orange, olive, grey, tan, green",
    },
    {
        "name": "Stoned Goat 2.0",
        "category": "Hunting / skinner",
        "blade_profile": "drop point",
        "default_blade_length": 4.25,
        "default_steel": "MagnaCut",
        "default_blade_finish": "Stonewashed",
        "default_blade_color": "Steel",
        "record_type": "Major revision",
        "catalog_status": "Current",
        "notes": "Stoned Goat 2.0 series; blaze, black, forest camo, grey, desert camo, olive (mkc_missing_items).",
        "identifier_keywords": "stoned goat, 2.0, skinner, blaze, camo, olive",
    },
    {
        "name": "Meat Church Chef Knife",
        "category": "Culinary",
        "blade_profile": "chef",
        "default_steel": "MagnaCut",
        "default_blade_finish": "PVD",
        "default_blade_color": "Steel",
        "record_type": "Collaboration model",
        "catalog_status": "Current",
        "notes": "Meat Church collaboration chef knife (mkc_missing_items).",
        "is_kitchen": 1,
        "is_collab": 1,
        "collaboration_name": "Meat Church",
        "identifier_keywords": "meat church, chef, culinary, orange, red, black",
    },
    {
        "name": "Jackstone",
        "category": "Hunting / belt knife",
        "blade_profile": "drop point",
        "default_blade_length": 3.625,
        "default_steel": "MagnaCut",
        "default_blade_finish": "PVD",
        "default_blade_color": "Steel",
        "record_type": "Standalone model",
        "catalog_status": "Current",
        "notes": "Canadian-style belt knife; PVD Snyder Edition, orange, black, tan, grey, green, olive (mkc_missing_items).",
        "identifier_keywords": "jackstone, pvd, snyder, belt knife, olive, tan",
    },
    {
        "name": "Blackfoot 2.0",
        "category": "Hunting / all-purpose",
        "blade_profile": "drop point",
        "default_blade_length": 4.0,
        "default_steel": "MagnaCut",
        "default_blade_finish": "Satin",
        "default_blade_color": "Steel",
        "record_type": "Major revision",
        "catalog_status": "Current",
        "notes": "The Blackfoot Fixed Blade 2.0; orange, black, green, grey, tan, olive (mkc_missing_items).",
        "identifier_keywords": "blackfoot, 2.0, hunting, fixed blade",
    },
    {
        "name": "Magnacut Blackfoot 2.0",
        "category": "Hunting / all-purpose",
        "blade_profile": "drop point",
        "default_blade_length": 4.0,
        "default_steel": "MagnaCut",
        "default_blade_finish": "Satin",
        "default_blade_color": "Steel",
        "record_type": "Major revision",
        "catalog_status": "Current",
        "notes": "Premium Magnacut Blackfoot 2.0 (mkc_missing_items).",
        "identifier_keywords": "magnacut, blackfoot, premium",
    },
    {
        "name": "The Stockyard",
        "category": "Ranch / utility",
        "blade_profile": "sheepsfoot",
        "default_blade_length": 4.75,
        "default_steel": "MagnaCut",
        "default_blade_finish": "Satin",
        "default_blade_color": "Steel",
        "record_type": "Standalone model",
        "catalog_status": "Current",
        "notes": "Stockyard series; orange, black, green, grey, tan, olive (mkc_missing_items).",
        "identifier_keywords": "stockyard, ranch, sheepsfoot, utility",
    },
    {
        "name": "Wargoat",
        "category": "Tactical",
        "blade_profile": "drop point",
        "default_blade_length": 3.75,
        "default_steel": "MagnaCut",
        "default_blade_finish": "PVD",
        "default_blade_color": "Black",
        "record_type": "Standalone model",
        "catalog_status": "Current",
        "notes": "Tactical ring knife; BLK/BLK, black coyote, black OD-green, coyote black, coyote OD-green (mkc_missing_items).",
        "has_ring": 1,
        "is_tactical": 1,
        "identifier_keywords": "wargoat, ring, tactical, coyote, od green",
    },
    {
        "name": "Battle Goat",
        "category": "Tactical / EDC",
        "blade_profile": "clip point",
        "default_blade_length": 4.75,
        "default_steel": "MagnaCut",
        "default_blade_finish": "PVD",
        "default_blade_color": "Black",
        "record_type": "Standalone model",
        "catalog_status": "Current",
        "notes": "Tactical fixed blade; BLK/BLK, blk coyote, blk OD-green, coyote blk, coyote OD-green (mkc_missing_items).",
        "is_tactical": 1,
        "identifier_keywords": "battle goat, tactical, coyote, od",
    },
    {
        "name": "TF24",
        "category": "Tactical",
        "blade_profile": "clip point",
        "default_blade_length": 4.125,
        "default_steel": "MagnaCut",
        "default_blade_finish": "Cerakote",
        "default_blade_color": "Black",
        "record_type": "Standalone model",
        "catalog_status": "Current",
        "notes": "Premium tactical; Cerakote finish; BLK/OD, BLK/BLK, BLK/COYOTE, COYOTE/BLK, COYOTE/OD (mkc_missing_items).",
        "is_tactical": 1,
        "identifier_keywords": "tf24, tactical, cerakote, coyote, od",
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
        "notes": "MKC Traditions series Speedgoat (mkc_missing_items).",
        "identifier_keywords": "traditions, speedgoat, limited",
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
        "notes": "MKC Traditions series Blackfoot 2.0 (mkc_missing_items).",
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
        "notes": "MKC Traditions series Jackstone (mkc_missing_items).",
        "identifier_keywords": "traditions, jackstone, limited",
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
        "notes": "MKC Traditions series Whitetail (mkc_missing_items).",
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
        "notes": "Traditions full set bundle (mkc_missing_items).",
        "identifier_keywords": "traditions, set, bundle, full set",
    },
]


def ensure_mkc_missing_items_models(conn: sqlite3.Connection) -> int:
    """Insert any mkc_missing_items base models that do not exist. Returns count added."""
    added = 0
    for spec in MKC_MISSING_ITEMS_BOOTSTRAP:
        name = spec["name"]
        if conn.execute("SELECT 1 FROM master_knives WHERE name = ?", (name,)).fetchone():
            continue
        conn.execute(
            """
            INSERT INTO master_knives
            (name, family, category, catalog_line, blade_profile, default_blade_length, default_steel,
             default_blade_finish, default_blade_color, record_type, catalog_status, notes,
             is_kitchen, is_collab, collaboration_name, has_ring, is_tactical, identifier_keywords, status, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', CURRENT_TIMESTAMP)
            """,
            (
                name,
                spec.get("family"),
                spec.get("category"),
                spec.get("catalog_line"),
                spec.get("blade_profile"),
                spec.get("default_blade_length"),
                spec.get("default_steel"),
                spec.get("default_blade_finish"),
                spec.get("default_blade_color"),
                spec.get("record_type"),
                spec.get("catalog_status"),
                spec.get("notes"),
                int(spec.get("is_kitchen") or 0),
                int(spec.get("is_collab") or 0),
                spec.get("collaboration_name"),
                int(spec.get("has_ring") or 0),
                int(spec.get("is_tactical") or 0),
                spec.get("identifier_keywords"),
            ),
        )
        added += 1
    return added


def normalize_master_catalog_line_input(value: Any, *, strict: bool = True) -> Optional[str]:
    """
    Canonical DB values: ``None`` (standard core catalog), ``VIP``, or ``Traditions``.
    When ``strict`` is False (CSV import), unknown tokens become ``None``.
    """
    if value is None or (isinstance(value, str) and not str(value).strip()):
        return None
    s = str(value).strip().lower()
    if s in ("standard", "core", "regular", "none", ""):
        return None
    if s == "vip":
        return "VIP"
    if s == "traditions":
        return "Traditions"
    if strict:
        raise ValueError("catalog_line must be 'VIP', 'Traditions', or empty (standard).")
    return None


def normalize_identifier_catalog_line_filter(value: Any) -> Optional[str]:
    """
    ``None`` = no filter. Otherwise ``standard`` (core only), ``VIP``, or ``Traditions``
    (canonical strings matching :func:`master_row_catalog_line_bucket`).
    """
    if value is None or (isinstance(value, str) and not str(value).strip()):
        return None
    s = str(value).strip().lower()
    if s in ("any", "all"):
        return None
    if s in ("standard", "core", "regular", "main"):
        return "standard"
    if s == "vip":
        return "VIP"
    if s == "traditions":
        return "Traditions"
    raise ValueError("catalog_line filter must be standard, VIP, Traditions, or empty (any line).")


def master_row_catalog_line_bucket(catalog_line: Optional[Any]) -> str:
    raw = (str(catalog_line).strip() if catalog_line is not None else "").lower()
    if raw == "vip":
        return "VIP"
    if raw == "traditions":
        return "Traditions"
    return "standard"


def build_master_catalog_llm_block(conn: sqlite3.Connection) -> str:
    rows = conn.execute(
        """
        SELECT name, category, catalog_line, blade_profile, default_blade_length, is_collab, collaboration_name,
               identifier_keywords, record_type, catalog_status,
               identifier_product_url,
               substr(COALESCE(evidence_summary, collector_notes, notes, ''), 1, 140) AS blurb
        FROM master_knives
        ORDER BY name COLLATE NOCASE
        """
    ).fetchall()
    lines: list[str] = []
    for r in rows:
        collab = (r.get("collaboration_name") or "") if r.get("is_collab") else ""
        ln = r.get("default_blade_length")
        le = f'{float(ln):.2f}"' if ln is not None else "?"
        prod = (r.get("identifier_product_url") or "").strip()
        media = f" storefront={prod}" if prod else ""
        line = (r.get("catalog_line") or "").strip()
        line_bit = f" line={line}" if line else ""
        lines.append(
            f"- {r['name']}; cat={r.get('category') or ''};{line_bit}; profile={r.get('blade_profile') or ''}; "
            f"len~{le}; collab={collab}; kw={r.get('identifier_keywords') or ''}; "
            f"type={r.get('record_type') or ''};{media} {r.get('blurb') or ''}"
        )
    return "\n".join(lines)


def build_shape_llm_block(conn: sqlite3.Connection) -> str:
    rows = conn.execute(
        "SELECT slug, name, description FROM blade_shape_templates ORDER BY name COLLATE NOCASE"
    ).fetchall()
    return "\n".join(
        f"- {r['slug']}: {r['name']} — {r.get('description') or ''}" for r in rows
    )


def init_db() -> None:
    with get_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS master_knives (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                family TEXT,
                default_blade_length REAL,
                default_steel TEXT,
                default_blade_finish TEXT,
                default_blade_color TEXT,
                default_handle_color TEXT,
                record_type TEXT,
                catalog_status TEXT,
                confidence TEXT,
                evidence_summary TEXT,
                collector_notes TEXT,
                identifier_product_url TEXT,
                identifier_image_blob BLOB,
                identifier_image_mime TEXT,
                identifier_silhouette_hu_json TEXT,
                is_collab INTEGER NOT NULL DEFAULT 0,
                collaboration_name TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                notes TEXT,
                catalog_line TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS option_handle_colors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS option_blade_steels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS option_blade_finishes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS option_blade_colors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS option_blade_types (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS inventory_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                master_knife_id INTEGER NOT NULL,
                nickname TEXT,
                quantity INTEGER NOT NULL DEFAULT 1,
                acquired_date TEXT,
                purchase_price REAL,
                estimated_value REAL,
                condition TEXT NOT NULL DEFAULT 'Like New',
                handle_color TEXT,
                blade_steel TEXT,
                blade_finish TEXT,
                blade_color TEXT,
                blade_length REAL,
                is_collab INTEGER NOT NULL DEFAULT 0,
                collaboration_name TEXT,
                serial_number TEXT,
                location TEXT,
                purchase_source TEXT,
                last_sharpened TEXT,
                notes TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(master_knife_id) REFERENCES master_knives(id) ON DELETE RESTRICT
            );

            CREATE TABLE IF NOT EXISTS app_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )

        ensure_identifier_columns(conn)
        ensure_master_extra_columns(conn)
        ensure_master_catalog_columns(conn)
        ensure_inventory_extra_columns(conn)
        ensure_blade_shape_templates(conn)
        ensure_master_identifier_media_columns(conn)
        ensure_master_catalog_line_column(conn)
        ensure_blade_types_option_table(conn)
        ensure_tier_option_tables(conn)
        _seed_tier_options_once(conn)
        _backfill_family_from_name_once(conn)
        _harmonize_blade_types_once(conn)

        # v2 Phase 1: version, parent_model_id, canonical identity, lifecycle, MSRP, traits, mapping tables, slug backfill
        ensure_phase1_schema(conn)

        option_seeds = {
            "option_handle_colors": [
                "Black", "Orange", "Orange/Black", "Green", "OD Green",
                "Coyote", "Tan", "Gray", "Blue", "Red", "Natural", "Micarta Brown"
            ],
            "option_blade_steels": [
                "MagnaCut", "52100", "AEB-L", "D2", "Unknown"
            ],
            "option_blade_finishes": [
                "Satin", "Stonewashed", "PVD", "Cerakote", "Distressed", "Blackened", "Raw"
            ],
            "option_blade_colors": [
                "Steel", "Black", "Gray", "Bronze", "Distressed Gray"
            ],
            "option_blade_types": [
                "Drop point", "Clip point", "Trailing point", "Skinner", "Fillet",
                "Sheepsfoot", "Tanto", "Spear", "Chef", "Cleaver", "Santoku",
                "Hatchet", "Tactical", "Mixed"
            ],
        }

        for table, values in option_seeds.items():
            for value in values:
                conn.execute(f"INSERT OR IGNORE INTO {table} (name) VALUES (?)", (value,))

        seeded = conn.execute(
            "SELECT value FROM app_meta WHERE key = 'seed_version'"
        ).fetchone()

        if not seeded:
            if not KNIFE_MASTER_CSV.is_file():
                master_seed = [
                ("Speedgoat", "Hunting / EDC", 3.75, "MagnaCut", "Satin", "Steel", None, 0, None, "active", "Flagship lightweight fixed blade", "edc", "drop point", "standard", "plain", "medium", 0, 0, 0, 0, 0, "lightweight,field,edc"),
                ("Mini Speedgoat", "Hunting / EDC", 3.0, "MagnaCut", "Satin", "Steel", None, 0, None, "active", "Compact Speedgoat variant", "edc", "drop point", "standard", "plain", "small", 0, 0, 0, 0, 0, "compact,edc,field"),
                ("Stoned Goat", "Hunting / EDC", 4.25, "MagnaCut", "Stonewashed", "Steel", None, 0, None, "active", "Heavy-duty goat family", "field", "drop point", "standard", "plain", "medium", 0, 0, 0, 0, 0, "heavy duty,field,belly"),
                ("Blackfoot 2.0", "Hunting", 4.0, "MagnaCut", "Satin", "Steel", None, 0, None, "active", "Popular all-around hunting knife", "hunting", "drop point", "standard", "plain", "medium", 0, 0, 0, 0, 0, "all around,hunting,field"),
                ("Stonewall Skinner", "Hunting", 4.0, "MagnaCut", "Satin", "Steel", None, 0, None, "active", "Skinning-focused model", "skinner", "skinner", "standard", "belly", "medium", 0, 0, 0, 0, 0, "skinner,belly,game"),
                ("Packout Skinner", "Hunting", 3.75, "MagnaCut", "Satin", "Steel", None, 0, None, "active", "Pack-friendly skinner", "skinner", "skinner", "standard", "belly", "medium", 0, 0, 0, 0, 0, "skinner,pack,belly"),
                ("Great Falls Skinner", "Hunting", 3.875, "MagnaCut", "Satin", "Steel", None, 0, None, "active", "Modern skinner", "skinner", "skinner", "standard", "belly", "medium", 0, 0, 0, 0, 0, "skinner,modern,belly"),
                ("Elkhorn", "Hunting", 3.25, "MagnaCut", "Satin", "Steel", None, 0, None, "active", "Caping / utility field knife", "hunting", "drop point", "fine", "plain", "small", 0, 0, 0, 0, 0, "caping,field,compact"),
                ("Whitetail", "Hunting", 3.5, "MagnaCut", "Satin", "Steel", None, 0, None, "active", "Smaller game knife", "hunting", "drop point", "standard", "plain", "small", 0, 0, 0, 0, 0, "small game,field"),
                ("Stubhorn", "Hunting / EDC", 3.6, "MagnaCut", "Satin", "Steel", None, 0, None, "active", "Compact fixed blade", "edc", "drop point", "standard", "plain", "small", 0, 0, 0, 0, 0, "compact,edc"),
                ("Super Cub", "Hunting / EDC", 3.75, "MagnaCut", "Satin", "Steel", None, 0, None, "active", "Broad utility profile", "utility", "drop point", "standard", "plain", "medium", 0, 0, 0, 0, 0, "utility,broad blade"),
                ("Jackstone", "Hunting / Camp", 4.5, "MagnaCut", "Satin", "Steel", None, 0, None, "active", "Mid-size all-around field knife", "camp", "drop point", "standard", "plain", "medium", 0, 0, 0, 0, 0, "camp,field,utility"),
                ("The Stockyard", "Camp / Utility", 4.75, "MagnaCut", "Satin", "Steel", None, 0, None, "active", "Hard-use ranch style knife", "camp", "drop point", "standard", "plain", "large", 0, 0, 0, 0, 0, "ranch,utility,hard use"),
                ("The Rocker", "Camp / Utility", 4.5, "MagnaCut", "Satin", "Steel", None, 0, None, "active", "General purpose outdoor blade", "camp", "drop point", "standard", "plain", "medium", 0, 0, 0, 0, 0, "outdoor,utility"),
                ("Marshall", "Bushcraft", 5.0, "MagnaCut", "Stonewashed", "Steel", None, 0, None, "active", "Bushcraft-focused model", "bushcraft", "drop point", "standard", "plain", "large", 0, 0, 0, 0, 0, "bushcraft,camp"),
                ("Fieldcraft Survival", "Bushcraft", 5.5, "MagnaCut", "Stonewashed", "Steel", None, 1, "Fieldcraft Survival", "active", "Collab bushcraft model", "bushcraft", "drop point", "standard", "plain", "large", 0, 0, 0, 0, 0, "bushcraft,survival,collab"),
                ("Wargoat", "Tactical", 3.75, "MagnaCut", "PVD", "Black", None, 0, None, "active", "Tactical ring knife", "tactical", "drop point", "standard", "plain", "medium", 1, 0, 0, 0, 1, "ring,tactical,black blade"),
                ("Battle Goat", "Tactical", 4.75, "MagnaCut", "PVD", "Black", None, 0, None, "active", "Larger tactical blade", "tactical", "clip point", "aggressive", "plain", "large", 0, 0, 0, 0, 1, "tactical,large"),
                ("Tactical Speedgoat", "Tactical", 3.75, "MagnaCut", "PVD", "Black", None, 0, None, "active", "Tactical take on Speedgoat", "tactical", "drop point", "standard", "plain", "medium", 0, 0, 0, 0, 1, "tactical,speedgoat,black blade"),
                ("TF24", "Tactical", 4.5, "MagnaCut", "PVD", "Black", None, 1, "Tactical collab", "active", "Tactical collaboration model", "tactical", "clip point", "aggressive", "plain", "large", 0, 0, 0, 0, 1, "tactical,collab"),
                ("Flathead Fillet", "Fishing / Culinary", 7.0, "AEB-L", "Satin", "Steel", None, 0, None, "active", "Fillet knife", "fillet", "fillet", "fine", "plain", "large", 0, 1, 0, 0, 0, "fillet,fishing,flexible"),
                ("Westslope", "Fishing / Utility", 4.0, "MagnaCut", "Satin", "Steel", None, 0, None, "active", "Fishing-focused field knife", "fishing", "drop point", "standard", "plain", "medium", 0, 0, 0, 0, 0, "fishing,utility"),
                ("Freezout", "Waterfowl", 3.75, "MagnaCut", "Satin", "Steel", None, 0, None, "active", "Waterfowl and bird knife", "bird and trout", "drop point", "fine", "plain", "medium", 0, 0, 0, 0, 0, "waterfowl,bird"),
                ("Bighorn Chef", "Culinary", 8.0, "AEB-L", "Satin", "Steel", None, 0, None, "active", "Chef knife", "culinary", "chef", "fine", "plain", "large", 0, 0, 0, 1, 0, "chef,kitchen"),
                ("Smith River Santoku", "Culinary", 7.0, "AEB-L", "Satin", "Steel", None, 0, None, "active", "Santoku knife", "culinary", "santoku", "fine", "plain", "large", 0, 0, 0, 1, 0, "santoku,kitchen"),
                ("Hellgate Hatchet", "Camp / Axe", None, "Unknown", "Raw", "Steel", None, 0, None, "active", "Hatchet / camp tool", "hatchet", "hatchet", "heavy", "plain", "large", 0, 0, 1, 0, 0, "hatchet,axe,camp"),
                ("Triumph Pro", "Hunting / Utility", 4.0, "MagnaCut", "Satin", "Steel", None, 1, "Nock On", "active", "Collab utility model", "utility", "drop point", "standard", "plain", "medium", 0, 0, 0, 0, 0, "collab,utility,hunting"),
                ("Mule Deer", "Hunting", 3.25, "MagnaCut", "Satin", "Steel", None, 0, None, "active", "Compact hunting knife", "hunting", "drop point", "fine", "plain", "small", 0, 0, 0, 0, 0, "compact,hunting"),
                ("Castle Rock", "Hunting / EDC", 3.5, "MagnaCut", "Satin", "Steel", None, 0, None, "active", "Modern EDC / field crossover", "edc", "drop point", "standard", "plain", "small", 0, 0, 0, 0, 0, "modern,edc,field"),
                ]
                conn.executemany(
                    """
                    INSERT OR IGNORE INTO master_knives
                    (name, family, default_blade_length, default_steel, default_blade_finish, default_blade_color,
                     default_handle_color, is_collab, collaboration_name, status, notes, category, blade_profile,
                     tip_style, edge_style, overall_size, has_ring, is_filleting_knife, is_hatchet, is_kitchen,
                     is_tactical, identifier_keywords)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    master_seed,
                )
                conn.execute(
                    "INSERT OR REPLACE INTO app_meta (key, value) VALUES ('seed_version', '2')"
                )
            else:
                conn.execute(
                    "INSERT OR REPLACE INTO app_meta (key, value) VALUES ('seed_version', '3')"
                )
        else:
            # Backfill identifier fields for older databases that were seeded before v2.
            existing = conn.execute("SELECT COUNT(*) AS c FROM master_knives WHERE category IS NOT NULL").fetchone()["c"]
            if existing == 0:
                updates = [
                    ("edc", "drop point", "standard", "plain", "medium", 0, 0, 0, 0, 0, "lightweight,field,edc", "Speedgoat"),
                    ("edc", "drop point", "standard", "plain", "small", 0, 0, 0, 0, 0, "compact,edc,field", "Mini Speedgoat"),
                    ("field", "drop point", "standard", "plain", "medium", 0, 0, 0, 0, 0, "heavy duty,field,belly", "Stoned Goat"),
                    ("hunting", "drop point", "standard", "plain", "medium", 0, 0, 0, 0, 0, "all around,hunting,field", "Blackfoot 2.0"),
                    ("skinner", "skinner", "standard", "belly", "medium", 0, 0, 0, 0, 0, "skinner,belly,game", "Stonewall Skinner"),
                    ("skinner", "skinner", "standard", "belly", "medium", 0, 0, 0, 0, 0, "skinner,pack,belly", "Packout Skinner"),
                    ("skinner", "skinner", "standard", "belly", "medium", 0, 0, 0, 0, 0, "skinner,modern,belly", "Great Falls Skinner"),
                    ("hunting", "drop point", "fine", "plain", "small", 0, 0, 0, 0, 0, "caping,field,compact", "Elkhorn"),
                    ("hunting", "drop point", "standard", "plain", "small", 0, 0, 0, 0, 0, "small game,field", "Whitetail"),
                    ("edc", "drop point", "standard", "plain", "small", 0, 0, 0, 0, 0, "compact,edc", "Stubhorn"),
                    ("utility", "drop point", "standard", "plain", "medium", 0, 0, 0, 0, 0, "utility,broad blade", "Super Cub"),
                    ("camp", "drop point", "standard", "plain", "medium", 0, 0, 0, 0, 0, "camp,field,utility", "Jackstone"),
                    ("camp", "drop point", "standard", "plain", "large", 0, 0, 0, 0, 0, "ranch,utility,hard use", "The Stockyard"),
                    ("camp", "drop point", "standard", "plain", "medium", 0, 0, 0, 0, 0, "outdoor,utility", "The Rocker"),
                    ("bushcraft", "drop point", "standard", "plain", "large", 0, 0, 0, 0, 0, "bushcraft,camp", "Marshall"),
                    ("bushcraft", "drop point", "standard", "plain", "large", 0, 0, 0, 0, 0, "bushcraft,survival,collab", "Fieldcraft Survival"),
                    ("tactical", "drop point", "standard", "plain", "medium", 1, 0, 0, 0, 1, "ring,tactical,black blade", "Wargoat"),
                    ("tactical", "clip point", "aggressive", "plain", "large", 0, 0, 0, 0, 1, "tactical,large", "Battle Goat"),
                    ("tactical", "drop point", "standard", "plain", "medium", 0, 0, 0, 0, 1, "tactical,speedgoat,black blade", "Tactical Speedgoat"),
                    ("tactical", "clip point", "aggressive", "plain", "large", 0, 0, 0, 0, 1, "tactical,collab", "TF24"),
                    ("fillet", "fillet", "fine", "plain", "large", 0, 1, 0, 0, 0, "fillet,fishing,flexible", "Flathead Fillet"),
                    ("fishing", "drop point", "standard", "plain", "medium", 0, 0, 0, 0, 0, "fishing,utility", "Westslope"),
                    ("bird and trout", "drop point", "fine", "plain", "medium", 0, 0, 0, 0, 0, "waterfowl,bird", "Freezout"),
                    ("culinary", "chef", "fine", "plain", "large", 0, 0, 0, 1, 0, "chef,kitchen", "Bighorn Chef"),
                    ("culinary", "santoku", "fine", "plain", "large", 0, 0, 0, 1, 0, "santoku,kitchen", "Smith River Santoku"),
                    ("hatchet", "hatchet", "heavy", "plain", "large", 0, 0, 1, 0, 0, "hatchet,axe,camp", "Hellgate Hatchet"),
                    ("utility", "drop point", "standard", "plain", "medium", 0, 0, 0, 0, 0, "collab,utility,hunting", "Triumph Pro"),
                    ("hunting", "drop point", "fine", "plain", "small", 0, 0, 0, 0, 0, "compact,hunting", "Mule Deer"),
                    ("edc", "drop point", "standard", "plain", "small", 0, 0, 0, 0, 0, "modern,edc,field", "Castle Rock"),
                ]
                conn.executemany(
                    """
                    UPDATE master_knives
                    SET category = ?, blade_profile = ?, tip_style = ?, edge_style = ?, overall_size = ?,
                        has_ring = ?, is_filleting_knife = ?, is_hatchet = ?, is_kitchen = ?, is_tactical = ?,
                        identifier_keywords = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE name = ?
                    """,
                    updates,
                )

        if not conn.execute(
            "SELECT 1 FROM app_meta WHERE key = ? AND value = '1'",
            (AUTO_KNIFE_FILE_SEED_META_KEY,),
        ).fetchone():
            if KNIFE_MASTER_CSV.is_file():
                sync_knife_master_csv_file(conn, KNIFE_MASTER_CSV)
            outline_md = BASE_DIR / "montanaknife_identifier_outline.md"
            if outline_md.is_file():
                identifier_outline_sync.sync_identifier_outline(conn, outline_md)
            conn.execute(
                "INSERT OR REPLACE INTO app_meta (key, value) VALUES (?, '1')",
                (AUTO_KNIFE_FILE_SEED_META_KEY,),
            )
        ensure_mkc_missing_items_models(conn)
        normalize_master_category_data(conn)

        # Normalized v2 schema + v2-only support tables
        normalized_model.ensure_normalized_schema(conn)
        ensure_v2_exclusive_schema(conn)
        ensure_reporting_schema(conn)
        migrate_legacy_media_to_v2(conn)
        backfill_v2_model_identity(conn)
        normalize_v2_additional_fields(conn)
        recompute_silhouettes_for_masters_without_hu(conn)


class MasterKnifeIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    family: Optional[str] = None
    default_blade_length: Optional[float] = None
    default_steel: Optional[str] = None
    default_blade_finish: Optional[str] = None
    default_blade_color: Optional[str] = None
    default_handle_color: Optional[str] = None
    record_type: Optional[str] = None
    catalog_status: Optional[str] = None
    confidence: Optional[str] = None
    evidence_summary: Optional[str] = None
    collector_notes: Optional[str] = None
    is_collab: bool = False
    collaboration_name: Optional[str] = None
    status: str = "active"
    notes: Optional[str] = None
    category: Optional[str] = None
    catalog_line: Optional[str] = Field(
        default=None,
        description="VIP, Traditions, or empty for standard core catalog.",
    )
    blade_profile: Optional[str] = None
    has_ring: bool = False
    is_filleting_knife: bool = False
    is_hatchet: bool = False
    is_kitchen: bool = False
    is_tactical: bool = False
    identifier_keywords: Optional[str] = None
    identifier_distinguishing_features: Optional[str] = None
    identifier_product_url: Optional[str] = None
    identifier_image_mime: Optional[str] = None
    identifier_silhouette_hu_json: Optional[str] = None
    # v2 Phase 1 fields
    canonical_slug: Optional[str] = None
    version: Optional[str] = None
    parent_model_id: Optional[int] = None  # references master_knives.id; no FK enforced
    first_release_date: Optional[str] = None
    last_seen_date: Optional[str] = None
    is_discontinued: Optional[bool] = None
    is_current_catalog: Optional[bool] = None
    msrp: Optional[float] = None
    blade_shape: Optional[str] = None
    tip_style: Optional[str] = None
    grind_style: Optional[str] = None
    size_class: Optional[str] = None
    primary_use_case: Optional[str] = None
    spine_profile: Optional[str] = None
    is_fillet: Optional[bool] = None
    default_product_url: Optional[str] = None
    primary_image_url: Optional[str] = None

    @field_validator("catalog_line", mode="before")
    @classmethod
    def normalize_catalog_line(cls, v: Any) -> Optional[str]:
        return normalize_master_catalog_line_input(v, strict=True)

    @field_validator("category", mode="before")
    @classmethod
    def normalize_category(cls, v: Any) -> Optional[str]:
        return normalize_category_value(v)


class InventoryItemIn(BaseModel):
    master_knife_id: int
    nickname: Optional[str] = None
    quantity: int = 1
    acquired_date: Optional[str] = None
    purchase_price: Optional[float] = None
    estimated_value: Optional[float] = None
    condition: str = "Like New"
    handle_color: Optional[str] = None
    blade_steel: Optional[str] = None
    blade_finish: Optional[str] = None
    blade_color: Optional[str] = None
    blade_length: Optional[float] = None
    is_collab: bool = False
    collaboration_name: Optional[str] = None
    serial_number: Optional[str] = None
    location: Optional[str] = None
    purchase_source: Optional[str] = None
    last_sharpened: Optional[str] = None
    notes: Optional[str] = None


class OptionIn(BaseModel):
    name: str = Field(min_length=1, max_length=100)


class IdentifierQuery(BaseModel):
    """Clues matched only against the master catalog (``master_knives``), never inventory."""

    q: Optional[str] = None
    family: Optional[str] = None
    blade_shape: Optional[str] = None
    size_class: Optional[str] = None
    use_case: Optional[str] = None
    steel: Optional[str] = None
    finish: Optional[str] = None
    blade_color: Optional[str] = None
    is_collab: Optional[bool] = None
    has_ring: Optional[bool] = None
    is_filleting_knife: Optional[bool] = None
    is_fillet: Optional[bool] = None
    is_hatchet: Optional[bool] = None
    is_kitchen: Optional[bool] = None
    is_tactical: Optional[bool] = None
    blade_length: Optional[float] = None
    catalog_line: Optional[str] = Field(
        default=None,
        description="Restrict to standard core, VIP, or Traditions (e.g. standard, VIP, Traditions).",
    )
    include_archived: bool = False

    @field_validator("catalog_line", mode="before")
    @classmethod
    def normalize_identifier_catalog_line(cls, v: Any) -> Optional[str]:
        return normalize_identifier_catalog_line_filter(v)


app = FastAPI(title="MKC Inventory Manager")
init_db()
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# ---------------------------------------------------------------------------
# Version endpoint — reads git info once at startup
# ---------------------------------------------------------------------------

def _read_git_version() -> dict[str, str]:
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=BASE_DIR, text=True
        ).strip()
        dt_raw = subprocess.check_output(
            ["git", "log", "-1", "--format=%ci"], cwd=BASE_DIR, text=True
        ).strip()
        # dt_raw is "YYYY-MM-DD HH:MM:SS ±HHMM" — trim timezone for display
        dt = dt_raw[:16] if len(dt_raw) >= 16 else dt_raw
        return {"commit": commit, "committed_at": dt}
    except Exception:
        return {"commit": "unknown", "committed_at": ""}

_GIT_VERSION = _read_git_version()

@app.get("/api/version", tags=["meta"])
def get_version() -> dict[str, str]:
    return _GIT_VERSION
app.include_router(create_static_pages_router(static_dir=STATIC_DIR))
app.include_router(
    create_admin_router(
        get_conn=get_conn,
        ollama_vision_model=OLLAMA_VISION_MODEL,
        app_logger=_app_logger,
    )
)


v2_router, run_v2_identify = create_v2_router(
    get_conn=get_conn,
    ollama_vision_model=OLLAMA_VISION_MODEL,
    inventory_csv_columns=INVENTORY_CSV_COLUMNS,
    migrate_legacy_media_to_v2=migrate_legacy_media_to_v2,
    backfill_v2_model_identity=backfill_v2_model_identity,
    normalize_v2_additional_fields=normalize_v2_additional_fields,
    normalize_category_value=normalize_category_value,
    option_in_model=OptionIn,
    identifier_query_model=IdentifierQuery,
    distinguishing_recompute_body=DistinguishingFeaturesRecomputeBody,
)
app.include_router(v2_router)

app.include_router(
    create_normalized_router(
        get_conn=get_conn,
        static_dir=STATIC_DIR,
        ensure_v2_exclusive_schema=ensure_v2_exclusive_schema,
        ensure_reporting_schema=ensure_reporting_schema,
        migrate_legacy_media_to_v2=migrate_legacy_media_to_v2,
        backfill_v2_model_identity=backfill_v2_model_identity,
        normalize_v2_additional_fields=normalize_v2_additional_fields,
    )
)

ai_router, ollama_check = create_ai_router(
    run_identify=run_v2_identify,
    identifier_query_model=IdentifierQuery,
)
app.include_router(ai_router)

app.include_router(
    create_reporting_router(
        get_conn=get_conn,
        static_dir=STATIC_DIR,
        ollama_check=ollama_check,
    )
)

