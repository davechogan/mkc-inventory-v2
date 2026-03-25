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

import base64
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
import gap_analysis_core
import identifier_outline_sync
import normalized_model
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

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


def column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(row["name"] == column for row in rows)


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone() is not None


def ensure_v2_exclusive_schema(conn: sqlite3.Connection) -> None:
    """
    Ensure v2-only support tables exist.

    Legacy tables can remain for migration/fallback, but v2 media, descriptors,
    and controlled option values must be persisted on v2-linked tables.
    """
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS knife_model_images (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            knife_model_id INTEGER NOT NULL UNIQUE,
            image_blob BLOB,
            image_mime TEXT,
            silhouette_hu_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(knife_model_id) REFERENCES knife_models_v2(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS knife_model_descriptors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            knife_model_id INTEGER NOT NULL UNIQUE,
            distinguishing_features TEXT,
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(knife_model_id) REFERENCES knife_models_v2(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS v2_option_values (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            option_type TEXT NOT NULL,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(option_type, name)
        );
        """
    )

    if not column_exists(conn, "inventory_items_v2", "blade_length"):
        conn.execute("ALTER TABLE inventory_items_v2 ADD COLUMN blade_length REAL")
    if not column_exists(conn, "inventory_items_v2", "mkc_order_number"):
        conn.execute("ALTER TABLE inventory_items_v2 ADD COLUMN mkc_order_number TEXT")
    if not column_exists(conn, "knife_models_v2", "handle_type"):
        conn.execute("ALTER TABLE knife_models_v2 ADD COLUMN handle_type TEXT")

    # Seed controlled v2 option values from existing v2 data.
    seed_sql = {
        "blade-steels": "SELECT DISTINCT steel AS v FROM knife_models_v2 WHERE steel IS NOT NULL AND trim(steel) != ''",
        "blade-finishes": "SELECT DISTINCT blade_finish AS v FROM knife_models_v2 WHERE blade_finish IS NOT NULL AND trim(blade_finish) != ''",
        "blade-colors": "SELECT DISTINCT blade_color AS v FROM knife_models_v2 WHERE blade_color IS NOT NULL AND trim(blade_color) != ''",
        "handle-colors": (
            "SELECT DISTINCT v FROM ("
            "  SELECT trim(handle_color) AS v FROM knife_models_v2 "
            "  WHERE handle_color IS NOT NULL AND trim(handle_color) != '' "
            "  UNION "
            "  SELECT trim(handle_color) AS v FROM inventory_items_v2 "
            "  WHERE handle_color IS NOT NULL AND trim(handle_color) != ''"
            ")"
        ),
        "conditions": "SELECT DISTINCT condition AS v FROM inventory_items_v2 WHERE condition IS NOT NULL AND trim(condition) != ''",
        "handle-types": "SELECT DISTINCT handle_type AS v FROM knife_models_v2 WHERE handle_type IS NOT NULL AND trim(handle_type) != ''",
        "blade-types": "SELECT DISTINCT name AS v FROM option_blade_types WHERE name IS NOT NULL AND trim(name) != ''",
        "categories": "SELECT DISTINCT name AS v FROM option_categories WHERE name IS NOT NULL AND trim(name) != ''",
        "blade-families": "SELECT DISTINCT name AS v FROM option_blade_families WHERE name IS NOT NULL AND trim(name) != ''",
        "primary-use-cases": "SELECT DISTINCT name AS v FROM option_primary_use_cases WHERE name IS NOT NULL AND trim(name) != ''",
        "collaborators": "SELECT DISTINCT name AS v FROM collaborators WHERE name IS NOT NULL AND trim(name) != ''",
        "generations": "SELECT DISTINCT generation_label AS v FROM knife_models_v2 WHERE generation_label IS NOT NULL AND trim(generation_label) != ''",
        "size-modifiers": "SELECT DISTINCT size_modifier AS v FROM knife_models_v2 WHERE size_modifier IS NOT NULL AND trim(size_modifier) != ''",
        "platform-variants": "SELECT DISTINCT platform_variant AS v FROM knife_models_v2 WHERE platform_variant IS NOT NULL AND trim(platform_variant) != ''",
    }
    for option_type, sql in seed_sql.items():
        rows = conn.execute(sql).fetchall()
        for row in rows:
            v = (row.get("v") or "").strip()
            if not v:
                continue
            conn.execute(
                "INSERT OR IGNORE INTO v2_option_values (option_type, name) VALUES (?, ?)",
                (option_type, v),
            )


REPORTING_DEFAULT_MODEL = "qwen2.5:7b-instruct"
REPORTING_PLANNER_MODEL = (os.environ.get("REPORTING_PLANNER_MODEL") or "qwen2.5:32b-instruct").strip() or "qwen2.5:32b-instruct"
REPORTING_RESPONDER_MODEL = (os.environ.get("REPORTING_RESPONDER_MODEL") or "qwen2.5:7b-instruct").strip() or "qwen2.5:7b-instruct"
REPORTING_PLANNER_RETRY_MODEL = (os.environ.get("REPORTING_PLANNER_RETRY_MODEL") or "").strip() or None
REPORTING_MAX_ROWS_DEFAULT = 200
REPORTING_MAX_ROWS_HARD = 1000
REPORTING_ALLOWED_SOURCES = {"reporting_inventory", "reporting_models"}
REPORTING_FORBIDDEN_SQL = (
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "create",
    "replace",
    "truncate",
    "attach",
    "detach",
    "pragma",
    "vacuum",
    "reindex",
)
REPORTING_INTENTS = {"missing_models", "list_inventory", "aggregate", "completion_cost"}
REPORTING_GROUPABLE_DIMENSIONS = {
    "series": "series_name",
    "series_name": "series_name",
    "family": "family_name",
    "family_name": "family_name",
    "type": "knife_type",
    "knife_type": "knife_type",
    "form": "form_name",
    "form_name": "form_name",
    "collaborator": "collaborator_name",
    "collaborator_name": "collaborator_name",
    "steel": "steel",
    "condition": "condition",
    "location": "location",
}
REPORTING_SERIES_ALIASES = {
    "traditions": "Traditions",
    "vip": "VIP",
    "ultra": "Ultra",
    "blood brothers": "Blood Brothers",
}
REPORTING_METRICS = {"count", "total_spend", "total_estimated_value"}
REPORTING_HINT_MIN_CONFIDENCE = 0.55


def ensure_reporting_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS reporting_sessions (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL DEFAULT 'New chat',
            model_default TEXT,
            memory_summary TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS reporting_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            sql_executed TEXT,
            result_json TEXT,
            chart_spec_json TEXT,
            meta_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(session_id) REFERENCES reporting_sessions(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS reporting_saved_queries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            question TEXT NOT NULL,
            config_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS reporting_query_telemetry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT,
            question TEXT NOT NULL,
            planner_model TEXT,
            responder_model TEXT,
            generation_mode TEXT,
            semantic_intent TEXT,
            sql_excerpt TEXT,
            row_count INTEGER,
            execution_ms REAL,
            total_ms REAL,
            status TEXT NOT NULL,
            error_detail TEXT,
            meta_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS reporting_semantic_hints (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scope_type TEXT NOT NULL DEFAULT 'session',
            scope_id TEXT,
            entity_norm TEXT NOT NULL,
            cue_word TEXT NOT NULL,
            target_dimension TEXT NOT NULL,
            target_value TEXT NOT NULL,
            confidence REAL NOT NULL DEFAULT 0.55,
            evidence_count INTEGER NOT NULL DEFAULT 1,
            success_count INTEGER NOT NULL DEFAULT 0,
            failure_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_used_at TEXT
        );

        CREATE INDEX IF NOT EXISTS ix_reporting_messages_session_created
            ON reporting_messages(session_id, created_at);
        CREATE INDEX IF NOT EXISTS ix_reporting_query_telemetry_created
            ON reporting_query_telemetry(created_at DESC);
        CREATE UNIQUE INDEX IF NOT EXISTS ux_reporting_semantic_hints_key
            ON reporting_semantic_hints(scope_type, scope_id, entity_norm, cue_word, target_dimension, target_value);
        CREATE INDEX IF NOT EXISTS ix_reporting_semantic_hints_lookup
            ON reporting_semantic_hints(scope_type, scope_id, entity_norm, cue_word, confidence DESC, evidence_count DESC);

        DROP VIEW IF EXISTS reporting_inventory;
        CREATE VIEW reporting_inventory AS
        SELECT
            i.id AS inventory_id,
            i.knife_model_id,
            km.official_name AS knife_name,
            kt.name AS knife_type,
            fam.name AS family_name,
            frm.name AS form_name,
            ks.name AS series_name,
            c.name AS collaborator_name,
            i.quantity,
            i.acquired_date,
            i.purchase_price,
            i.estimated_value,
            i.condition,
            COALESCE(i.steel, km.steel) AS steel,
            COALESCE(i.blade_finish, km.blade_finish) AS blade_finish,
            COALESCE(i.blade_color, km.blade_color) AS blade_color,
            COALESCE(i.handle_color, km.handle_color) AS handle_color,
            COALESCE(i.blade_length, km.blade_length) AS blade_length,
            i.location,
            i.purchase_source,
            i.notes,
            km.msrp
        FROM inventory_items_v2 i
        LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
        LEFT JOIN knife_types kt ON kt.id = km.type_id
        LEFT JOIN knife_forms frm ON frm.id = km.form_id
        LEFT JOIN knife_families fam ON fam.id = km.family_id
        LEFT JOIN knife_series ks ON ks.id = km.series_id
        LEFT JOIN collaborators c ON c.id = km.collaborator_id;

        DROP VIEW IF EXISTS reporting_models;
        CREATE VIEW reporting_models AS
        SELECT
            km.id AS model_id,
            km.official_name,
            kt.name AS knife_type,
            fam.name AS family_name,
            frm.name AS form_name,
            ks.name AS series_name,
            c.name AS collaborator_name,
            km.generation_label,
            km.size_modifier,
            km.steel,
            km.blade_finish,
            km.blade_color,
            km.handle_color,
            km.handle_type,
            km.blade_length,
            km.msrp,
            km.record_status
        FROM knife_models_v2 km
        LEFT JOIN knife_types kt ON kt.id = km.type_id
        LEFT JOIN knife_forms frm ON frm.id = km.form_id
        LEFT JOIN knife_families fam ON fam.id = km.family_id
        LEFT JOIN knife_series ks ON ks.id = km.series_id
        LEFT JOIN collaborators c ON c.id = km.collaborator_id;
        """
    )
    if not column_exists(conn, "reporting_sessions", "last_query_state_json"):
        conn.execute("ALTER TABLE reporting_sessions ADD COLUMN last_query_state_json TEXT")


def ensure_gap_reconciliation_schema(conn: sqlite3.Connection) -> None:
    """Persist user notes on order vs inventory gaps and manual order line → inventory links."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS order_inv_gap_bucket_overrides (
            bucket_key TEXT NOT NULL,
            match_mode TEXT NOT NULL,
            cleared INTEGER NOT NULL DEFAULT 0,
            resolution_code TEXT,
            note TEXT,
            linked_inventory_item_ids TEXT,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (bucket_key, match_mode)
        );

        CREATE TABLE IF NOT EXISTS order_inv_gap_line_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_number TEXT NOT NULL,
            order_date TEXT,
            line_title TEXT NOT NULL,
            matched_catalog_name TEXT,
            inventory_item_id INTEGER NOT NULL,
            note TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(inventory_item_id) REFERENCES inventory_items_v2(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS ix_order_inv_gap_line_links_order
            ON order_inv_gap_line_links(order_number);
        """
    )


def _reporting_iso_now() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _reporting_detect_date_bounds(question: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
    q = (question or "").lower()
    today = date.today()
    start: Optional[date] = None
    end: Optional[date] = None
    label: Optional[str] = None

    m = re.search(r"\blast\s+(\d+)\s+days?\b", q)
    if m:
        n = max(1, int(m.group(1)))
        start = today - timedelta(days=n)
        end = today
        label = f"last {n} days"
    m = m or re.search(r"\blast\s+(\d+)\s+months?\b", q)
    if m and label is None:
        n = max(1, int(m.group(1)))
        start = today - timedelta(days=(30 * n))
        end = today
        label = f"last {n} months"
    m = m or re.search(r"\blast\s+(\d+)\s+years?\b", q)
    if m and label is None:
        n = max(1, int(m.group(1)))
        start = date(today.year - n, today.month, min(today.day, 28))
        end = today
        label = f"last {n} years"
    if "this year" in q and label is None:
        start = date(today.year, 1, 1)
        end = today
        label = "this year"
    if "last year" in q and label is None:
        start = date(today.year - 1, 1, 1)
        end = date(today.year - 1, 12, 31)
        label = "last year"
    m_since = re.search(r"\bsince\s+(\d{4}-\d{2}-\d{2})\b", q)
    if m_since and label is None:
        try:
            start = datetime.strptime(m_since.group(1), "%Y-%m-%d").date()
            end = today
            label = f"since {m_since.group(1)}"
        except ValueError:
            pass
    return (
        start.isoformat() if start else None,
        end.isoformat() if end else None,
        label,
    )


def _reporting_detect_year_comparison(question: str) -> Optional[tuple[str, str]]:
    """
    Detect patterns like "2024 vs 2025" / "2024 versus 2025".

    Returns (year_a, year_b) as strings when exactly 2 years are detected.
    """
    q = " ".join((question or "").strip().lower().split())
    m = re.search(r"\b((?:19|20)\d{2})\b\s*(?:vs|versus)\s*\b((?:19|20)\d{2})\b", q, flags=re.I)
    if not m:
        return None
    a, b = m.group(1), m.group(2)
    if not a or not b:
        return None
    return (a, b)


def _reporting_detect_unsafe_request(question: str) -> Optional[str]:
    """
    Detect obvious prompt-injection / SQL-command attempts in user text.

    This guardrail runs before planning/SQL generation and rejects requests
    that try to force SQL execution or schema exfiltration instructions.
    """
    q = (question or "").strip()
    if not q:
        return None
    ql = q.lower()
    compact = " ".join(ql.split())
    patterns: list[tuple[str, str]] = [
        (r"(?is)```(?:sql)?\s*(select|with|insert|update|delete|drop|alter|create)\b", "sql_code_block"),
        (r"\b(drop\s+table|delete\s+from|insert\s+into|update\s+\w+\s+set|alter\s+table|create\s+table)\b", "mutating_sql_phrase"),
        (r"\b(pragma|sqlite_master|information_schema)\b", "schema_exfiltration_phrase"),
        (r"\bunion\s+select\b", "union_select_phrase"),
        (r";\s*(select|with|insert|update|delete|drop|alter|create)\b", "multi_statement_hint"),
        (r"(?is)\b(ignore|bypass|override)\b.{0,40}\b(instruction|guardrail|safety|policy)\b", "guardrail_bypass_phrase"),
    ]
    for pat, reason in patterns:
        if re.search(pat, compact):
            return reason
    # Direct SQL command starters are not supported as user input.
    if re.match(r"^\s*(select|with|insert|update|delete|drop|alter|create|pragma)\b", compact):
        return "direct_sql_prefix"
    return None


def _reporting_detect_scope(question: str) -> Optional[str]:
    """
    Infer reporting scope (inventory vs full catalog) from user language.

    Prefer high-precision phrases and word-boundary tokens so we do not treat
    substrings like "own" inside "location" as ownership cues.
    """
    q = " ".join((question or "").strip().lower().split())
    if not q:
        return None
    inventory_markers = (
        "in my inventory",
        "my inventory",
        "my collection",
        "inventory counts",
        "inventory count",
        "do i have",
        "i have",
        "did i buy",
        "i buy",
        "list my",
        "show my",
        "each location",
        "by location",
        "storage location",
        "purchase source",
    )
    catalog_markers = (
        "mkc has made",
        "mkc made",
        "full catalog",
        "catalog",
        "all models",
        "offered by mkc",
        "ever made",
    )
    inv = any(m in q for m in inventory_markers)
    if not inv and (re.search(r"\bowned\b", q) or re.search(r"\bown\b", q)):
        inv = True
    # Personal collection value / ranking (inventory pieces), not catalog MSRP rollups.
    if not inv and "estimated value" in q and re.search(r"\b(which|what)\s+knives\b", q):
        inv = True
    cat = any(m in q for m in catalog_markers)
    if inv and not cat:
        return "inventory"
    if cat and not inv:
        return "catalog"
    return None


def _reporting_is_completion_cost_question(q: str) -> bool:
    """Cost to obtain missing catalog models (complete / finish collection wording)."""
    if "collection" not in q:
        return False
    if not ("complete" in q or "finish" in q):
        return False
    return bool(
        re.search(
            r"\b(how much|cost|price|msrp|estimate|estimated)\b",
            q,
            flags=re.I,
        )
    )


def _reporting_needs_scope_clarification(question: str) -> bool:
    """
    Detect naturally ambiguous prompts where "inventory vs full catalog" is unclear.
    """
    q = " ".join((question or "").strip().lower().split())
    if not q:
        return False
    if _reporting_detect_scope(question) is not None:
        return False
    # These intents already imply scope semantics in our planner/compiler.
    if (
        "missing" in q
        or "not in inventory" in q
        or "do i not have" in q
        or "still not have" in q
        or ("complete" in q and "collection" in q)
        or ("finish" in q and "collection" in q)
    ):
        return False
    if any(x in q for x in ("compare ", "vs ", "versus")):
        return False
    # Ambiguous counting/listing phrasing with entity classifier terms.
    asks_count_or_list = any(k in q for k in ("how many", "count", "list", "which"))
    has_entity_cue = any(k in q for k in (" family", " families", " series", " type", " line", " knives"))
    return asks_count_or_list and has_entity_cue


def _reporting_is_scope_status_question(question: str) -> bool:
    q = " ".join((question or "").strip().lower().split())
    if not q:
        return False
    asks_scope = any(
        p in q for p in (
            "are you looking at my inventory",
            "are you looking at inventory",
            "inventory or the catalog",
            "inventory or catalog",
            "what scope are you using",
            "are you using inventory",
            "are you using catalog",
        )
    )
    return asks_scope


def _reporting_validate_sql(sql: str) -> str:
    if not sql or not str(sql).strip():
        raise HTTPException(status_code=400, detail="No SQL generated for this question.")
    s = " ".join(str(sql).strip().split())
    # Allow trailing semicolons from LLM output, but reject true multi-statement SQL.
    while s.endswith(";"):
        s = s[:-1].rstrip()
    lower = s.lower()
    if ";" in s:
        segments = [seg.strip() for seg in s.split(";")]
        non_empty = [seg for seg in segments if seg]
        # More than one non-empty segment indicates multiple statements.
        if len(non_empty) > 1:
            raise HTTPException(status_code=400, detail="Multi-statement SQL is not allowed.")
        # If we got here, keep the only segment.
        s = non_empty[0] if non_empty else ""
        lower = s.lower()
    if not (lower.startswith("select ") or lower.startswith("with ")):
        raise HTTPException(status_code=400, detail="Only SELECT queries are allowed.")
    # Disallow quoted relation references to avoid bypassing allowlist parsing.
    if re.search(r'\b(?:from|join)\s+["`\\[]', lower):
        raise HTTPException(status_code=400, detail="Quoted relation references are not allowed in reporting SQL.")
    for token in REPORTING_FORBIDDEN_SQL:
        if re.search(rf"\b{token}\b", lower):
            raise HTTPException(status_code=400, detail=f"Forbidden SQL token: {token}")
    refs = re.findall(r"\b(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*)", lower)
    if not refs:
        raise HTTPException(status_code=400, detail="Query must read from approved reporting sources.")
    for ref in refs:
        if ref not in REPORTING_ALLOWED_SOURCES:
            raise HTTPException(status_code=400, detail=f"Source not allowed: {ref}")
    return s


def _reporting_exec_sql(
    conn: sqlite3.Connection,
    sql: str,
    max_rows: int,
) -> tuple[list[str], list[dict[str, Any]], float]:
    started = time.perf_counter()
    safe_sql = _reporting_validate_sql(sql)
    try:
        # Parse check first so malformed SQL returns a user-facing 400 instead of an ASGI trace.
        conn.execute(f"EXPLAIN QUERY PLAN {safe_sql}").fetchall()
        rows = conn.execute(f"SELECT * FROM ({safe_sql}) LIMIT ?", (max_rows,)).fetchall()
    except sqlite3.OperationalError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Could not derive a safe SQL query. Generated SQL was invalid: {str(exc)[:200]}",
        ) from exc
    elapsed_ms = round((time.perf_counter() - started) * 1000.0, 2)
    if not rows:
        return [], [], elapsed_ms
    cols = list(rows[0].keys())
    return cols, rows, elapsed_ms


def _reporting_build_drill_link(row: dict[str, Any]) -> Optional[str]:
    mapping = [
        ("knife_name", "search"),
        ("knife_type", "type"),
        ("family_name", "family"),
        ("form_name", "form"),
        ("series_name", "series"),
        ("steel", "steel"),
        ("blade_finish", "finish"),
        ("handle_color", "handle_color"),
        ("condition", "condition"),
        ("location", "location"),
    ]
    params: dict[str, str] = {}
    for src, target in mapping:
        val = row.get(src)
        if val is None:
            continue
        s = str(val).strip()
        if s:
            params[target] = s
    return f"/?{urlencode(params)}" if params else None


def _reporting_infer_chart(
    question: str,
    columns: list[str],
    rows: list[dict[str, Any]],
    preference: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    if not columns or not rows:
        return None
    numeric_cols = []
    for c in columns:
        if all((r.get(c) is None) or isinstance(r.get(c), (int, float)) for r in rows):
            numeric_cols.append(c)
    if not numeric_cols:
        return None
    y = numeric_cols[0]
    x_candidates = [c for c in columns if c != y]
    if not x_candidates:
        return None
    x = x_candidates[0]
    q = (question or "").lower()
    chart_type = "bar"
    if preference in {"bar", "line", "pie"}:
        chart_type = preference
    elif "trend" in q or "over time" in q or "by month" in q or "monthly" in q:
        chart_type = "line"
    elif "share" in q or "distribution" in q or "breakdown" in q:
        chart_type = "pie"
    points = [{x: r.get(x), y: r.get(y)} for r in rows]
    return {"type": chart_type, "x": x, "y": y, "data": points}


def _reporting_default_followups(question: str, columns: list[str], rows: list[dict[str, Any]]) -> list[str]:
    q = (question or "").lower()
    base = []
    if "spend" in q:
        base.append("Show the same spend analysis split by family.")
        base.append("Compare this period to the previous period.")
    if "value" in q:
        base.append("Show top 10 knives by estimated value.")
    if any(c in columns for c in ("family_name", "knife_type", "series_name")):
        base.append("Show this as a percentage distribution.")
    base.append("Drill into the matching inventory rows.")
    dedup = []
    for b in base:
        if b not in dedup:
            dedup.append(b)
    return dedup[:4]


def _reporting_build_prompt_schema(conn: sqlite3.Connection) -> str:
    chunks = []
    for view in sorted(REPORTING_ALLOWED_SOURCES):
        cols = conn.execute(f"PRAGMA table_info({view})").fetchall()
        names = ", ".join(c["name"] for c in cols)
        chunks.append(f"- {view}: {names}")
    return "\n".join(chunks)


def _reporting_get_last_query_state(conn: sqlite3.Connection, session_id: str) -> Optional[dict[str, Any]]:
    row = conn.execute(
        "SELECT last_query_state_json FROM reporting_sessions WHERE id = ?",
        (session_id,),
    ).fetchone()
    if not row:
        return None
    raw = row.get("last_query_state_json")
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def _reporting_set_last_query_state(conn: sqlite3.Connection, session_id: str, state: dict[str, Any]) -> None:
    conn.execute(
        "UPDATE reporting_sessions SET last_query_state_json = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (json.dumps(state or {}), session_id),
    )


def _reporting_log_query_event(
    conn: sqlite3.Connection,
    *,
    session_id: Optional[str],
    question: str,
    planner_model: Optional[str],
    responder_model: Optional[str],
    generation_mode: Optional[str],
    semantic_intent: Optional[str],
    sql_excerpt: Optional[str],
    row_count: Optional[int],
    execution_ms: Optional[float],
    total_ms: Optional[float],
    status: str,
    error_detail: Optional[str] = None,
    meta: Optional[dict[str, Any]] = None,
) -> None:
    conn.execute(
        """
        INSERT INTO reporting_query_telemetry
        (session_id, question, planner_model, responder_model, generation_mode, semantic_intent,
         sql_excerpt, row_count, execution_ms, total_ms, status, error_detail, meta_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (
            session_id,
            (question or "")[:2000],
            planner_model,
            responder_model,
            generation_mode,
            semantic_intent,
            ((sql_excerpt or "")[:1000] if sql_excerpt else None),
            row_count,
            execution_ms,
            total_ms,
            status,
            (error_detail[:800] if error_detail else None),
            json.dumps(meta or {}),
        ),
    )


def _reporting_is_short_followup(question: str) -> bool:
    q = " ".join((question or "").strip().lower().split())
    if not q:
        return False
    if len(q.split()) <= 5:
        return True
    prefixes = ("look at ", "only ", "just ", "now ", "same ", "what about ")
    return any(q.startswith(p) for p in prefixes)


def _reporting_is_contextual_followup(question: str) -> bool:
    q = " ".join((question or "").strip().lower().split())
    if not q:
        return False
    cues = (
        "verify your answer",
        "verify that",
        "verify this",
        "list them",
        "show them",
        "which ones",
        "what are the names",
        "show names",
        "list names",
        "those names",
    )
    return any(c in q for c in cues)


def _reporting_is_followup(question: str) -> bool:
    return _reporting_is_short_followup(question) or _reporting_is_contextual_followup(question)


def _reporting_extract_dimension_from_text(q: str) -> Optional[str]:
    ql = (q or "").lower()
    for k, col in REPORTING_GROUPABLE_DIMENSIONS.items():
        if f"by {k}" in ql or f"look at {k}" in ql or f"use {k}" in ql:
            return col
    return None


def _reporting_filter_explicit_in_question(question: str, key: str, value: str) -> bool:
    base_key = key[:-5] if str(key).endswith("__not") else key
    q = " ".join((question or "").strip().lower().split())
    v = " ".join((value or "").strip().lower().split())
    if not q or not v:
        return False
    if base_key == "series_name":
        if f"{v} series" in q or f"series {v}" in q or f"from the {v} series" in q:
            return True
        for alias, canonical in REPORTING_SERIES_ALIASES.items():
            if canonical.lower() == v and alias in q:
                return True
        return False
    if base_key == "knife_type":
        # Only accept if type is explicitly requested in language.
        return (("knife type" in q) or ("type" in q and "by type" in q)) and (v in q)
    if base_key == "text_search":
        return v in q
    if base_key == "family_name":
        if ("family" in q or "families" in q) and v in q:
            return True
        if re.search(rf"\b{re.escape(v)}\s+knives?\b", q):
            return True
        return False
    if base_key == "form_name":
        return ("form" in q) and (v in q)
    if base_key == "collaborator_name":
        return (("collaborator" in q) or ("collab" in q) or ("collaboration" in q)) and (v in q)
    if base_key == "steel":
        return ("steel" in q) and (v in q)
    if base_key == "condition":
        return ("condition" in q) and (v in q)
    if base_key == "location":
        return (("location" in q) or ("where" in q)) and (v in q)
    # Common semantic aliases for series-oriented prompts.
    if base_key in {"knife_type", "family_name", "form_name", "collaborator_name", "steel", "condition", "location"}:
        token = base_key.replace("_name", "").replace("_", " ")
        if token in q and v in q:
            return True
    return False


def _reporting_norm_entity(value: str) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _reporting_extract_hint_entities(question: str) -> list[tuple[str, str]]:
    """
    Extract (entity, cue_word) pairs from colloquial phrasing.

    Example:
      "Blood Brothers family" -> ("blood brothers", "family")
      "family blood brothers" -> ("blood brothers", "family")
    """
    q = " ".join((question or "").strip().lower().split())
    if not q:
        return []
    cues = ("family", "type", "series", "line", "kind", "style")
    out: list[tuple[str, str]] = []
    cue_re = "|".join(cues)
    pats = [
        rf"\b([a-z0-9][a-z0-9 '&/-]{{1,60}}?)\s+({cue_re})\b",
        rf"\b({cue_re})\s+([a-z0-9][a-z0-9 '&/-]{{1,60}}?)\b",
    ]
    stop_prefix = re.compile(
        r"^(how many|what is|what are|which|show me|show|list|count|are there|there are|there is|in|the)\s+",
        re.I,
    )
    stop_suffix = re.compile(r"\s+(in|the|my|our|collection|inventory|there|are|is|by)$", re.I)
    for pat in pats:
        for m in re.finditer(pat, q):
            if len(m.groups()) != 2:
                continue
            g1 = _reporting_norm_entity(m.group(1))
            g2 = _reporting_norm_entity(m.group(2))
            cue = g1 if g1 in cues else g2
            ent = g2 if cue == g1 else g1
            ent = _reporting_normalize_filter_value("text_search", ent)
            # Trim conversational scaffolding so we keep just the entity phrase.
            while True:
                new_ent = stop_prefix.sub("", ent).strip()
                if new_ent == ent:
                    break
                ent = new_ent
            while True:
                new_ent = stop_suffix.sub("", ent).strip()
                if new_ent == ent:
                    break
                ent = new_ent
            if ent and cue:
                pair = (ent, cue)
                if pair not in out:
                    out.append(pair)
    return out


def _reporting_get_semantic_hints(
    conn: sqlite3.Connection,
    session_id: str,
    question: str,
    min_confidence: float = REPORTING_HINT_MIN_CONFIDENCE,
) -> dict[str, Any]:
    entities = _reporting_extract_hint_entities(question)
    if not entities:
        return {"filters": {}, "hint_ids": [], "hints": []}
    filters: dict[str, str] = {}
    hint_ids: list[int] = []
    hints: list[dict[str, Any]] = []
    for ent, cue in entities:
        rows = conn.execute(
            """
            SELECT id, target_dimension, target_value, confidence, scope_type, scope_id
            FROM reporting_semantic_hints
            WHERE entity_norm = ? AND cue_word = ?
              AND confidence >= ?
              AND (
                    (scope_type = 'session' AND scope_id = ?)
                 OR (scope_type = 'global' AND scope_id IS NULL)
              )
            ORDER BY
              CASE WHEN scope_type = 'session' THEN 0 ELSE 1 END,
              confidence DESC,
              evidence_count DESC,
              id DESC
            LIMIT 3
            """,
            (ent, cue, float(min_confidence), session_id),
        ).fetchall()
        for r in rows:
            dim = str(r.get("target_dimension") or "").strip()
            val = str(r.get("target_value") or "").strip()
            if not dim or not val:
                continue
            if dim not in {"series_name", "family_name", "knife_type", "form_name", "collaborator_name", "steel", "condition", "location", "text_search"}:
                continue
            # Do not overwrite stronger hints in same pass.
            if dim not in filters:
                filters[dim] = val
                hid = int(r.get("id"))
                hint_ids.append(hid)
                hints.append(
                    {
                        "id": hid,
                        "dimension": dim,
                        "value": val,
                        "confidence": r.get("confidence"),
                        "scope_type": r.get("scope_type"),
                    }
                )
    return {"filters": filters, "hint_ids": hint_ids, "hints": hints}


def _reporting_feedback_semantic_hints(conn: sqlite3.Connection, hint_ids: list[int], success: bool) -> None:
    if not hint_ids:
        return
    adj = 0.06 if success else -0.08
    succ_inc = 1 if success else 0
    fail_inc = 0 if success else 1
    for hid in hint_ids:
        conn.execute(
            """
            UPDATE reporting_semantic_hints
            SET confidence = MIN(0.95, MAX(0.2, confidence + ?)),
                success_count = success_count + ?,
                failure_count = failure_count + ?,
                last_used_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (adj, succ_inc, fail_inc, int(hid)),
        )


def _reporting_learn_semantic_hints(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    question: str,
    plan: Optional[dict[str, Any]],
    row_count: int,
) -> None:
    # Learn only from successful, non-empty answers with a semantic plan.
    if not plan or row_count <= 0:
        return
    candidates = _reporting_extract_hint_entities(question)
    if not candidates:
        return
    plan_filters = dict(plan.get("filters") or {})
    if not plan_filters:
        return

    # Prefer identity dimensions over free text for hint targets.
    priority = ["series_name", "family_name", "knife_type", "form_name", "collaborator_name", "steel", "condition", "location", "text_search"]
    for ent, cue in candidates:
        chosen_dim = None
        chosen_val = None
        for dim in priority:
            val = plan_filters.get(dim)
            if not val:
                continue
            nval = _reporting_norm_entity(str(val))
            if ent in nval or nval in ent or dim == "text_search":
                chosen_dim = dim
                chosen_val = str(val).strip()
                break
        if not chosen_dim or not chosen_val:
            continue
        conn.execute(
            """
            INSERT INTO reporting_semantic_hints
            (scope_type, scope_id, entity_norm, cue_word, target_dimension, target_value, confidence, evidence_count, success_count, failure_count, created_at, updated_at, last_used_at)
            VALUES ('session', ?, ?, ?, ?, ?, 0.55, 1, 1, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(scope_type, scope_id, entity_norm, cue_word, target_dimension, target_value)
            DO UPDATE SET
                confidence = MIN(0.95, reporting_semantic_hints.confidence + 0.05),
                evidence_count = reporting_semantic_hints.evidence_count + 1,
                success_count = reporting_semantic_hints.success_count + 1,
                updated_at = CURRENT_TIMESTAMP,
                last_used_at = CURRENT_TIMESTAMP
            """,
            (session_id, ent, cue, chosen_dim, chosen_val),
        )


def _reporting_normalize_filter_value(key: str, value: str) -> str:
    base_key = key[:-5] if str(key).endswith("__not") else key
    v = " ".join(str(value or "").strip().lower().split())
    if not v:
        return ""
    # Remove lightweight quoting wrappers.
    v = v.strip("\"'`")
    # Remove common filler words that LLM may include in entity values.
    v = re.sub(r"\b(any|all|the|a|an)\b", " ", v)
    v = re.sub(r"\b(knife|knives|model|models)\b", " ", v)
    # Remove common tail phrases that do not belong to entity values.
    v = re.sub(r"\b(do i have|that i have|in my inventory|from inventory|from my inventory)\b", " ", v)
    v = re.sub(r"\b(please|show|list|count|how many)\b", " ", v)
    v = " ".join(v.split()).strip(" ,.;:-")
    if base_key == "series_name":
        for alias, canonical in REPORTING_SERIES_ALIASES.items():
            if alias in v or v == canonical.lower():
                return canonical
    return v


def _reporting_is_series_term(value: str) -> bool:
    v = " ".join(str(value or "").strip().lower().split())
    if not v:
        return False
    if v in (k.lower() for k in REPORTING_SERIES_ALIASES.keys()):
        return True
    if v in (v2.lower() for v2 in REPORTING_SERIES_ALIASES.values()):
        return True
    return False


def _reporting_explicit_constraints(question: str) -> dict[str, Any]:
    q = " ".join((question or "").strip().lower().split())
    out: dict[str, Any] = {"filters": {}}
    if not q:
        return out
    scope = _reporting_detect_scope(q)
    if scope:
        out["scope"] = scope

    if _reporting_is_completion_cost_question(q):
        out["intent"] = "completion_cost"
    elif "missing" in q or "not in inventory" in q or "do i not have" in q:
        out["intent"] = "missing_models"
    elif "spend" in q or "spent" in q:
        out["intent"] = "aggregate"
        out["metric"] = "total_spend"
    elif "value" in q and ("total" in q or "by" in q):
        out["intent"] = "aggregate"
        out["metric"] = "total_estimated_value"
    elif "how many" in q or "count" in q or "breakdown" in q or "distribution" in q:
        out["intent"] = "aggregate"
        out["metric"] = "count"
    elif ("verify" in q or "name" in q) and ("list" in q or "show" in q or "what are" in q or "verify" in q):
        out["intent"] = "list_inventory"

    group_by = _reporting_extract_dimension_from_text(q)
    if group_by:
        out["group_by"] = group_by
    if "look at series" in q:
        out["group_by"] = "series_name"

    for needle, canonical in REPORTING_SERIES_ALIASES.items():
        if needle in q:
            out["filters"]["series_name"] = canonical
            break
    if "tactical" in q:
        out["filters"]["knife_type"] = "Tactical"
    if "hunting" in q:
        out["filters"]["knife_type"] = "Hunting"
    ds, de, dl = _reporting_detect_date_bounds(question)
    if ds:
        out["date_start"] = ds
    if de:
        out["date_end"] = de
    if dl:
        out["date_label"] = dl
    yc = _reporting_detect_year_comparison(question)
    if yc:
        out["year_compare"] = [yc[0], yc[1]]
        if "intent" not in out:
            out["intent"] = "aggregate"
        if "metric" not in out and ("spend" in q or "spent" in q):
            out["metric"] = "total_spend"

    # Scope extraction for prompts like:
    # - how many "goat" knives do i have?
    # - how many goat knives do i have?
    # - list goat knives
    scope_patterns = [
        r"['\"]([a-z0-9][a-z0-9 -]+?)['\"]\s+knives?\b",
        r"\bhow many\s+([a-z0-9][a-z0-9 -]+?)\s+knives?\b",
        r"\b(?:list|show)\s+([a-z0-9][a-z0-9 -]+?)\s+knives?\b",
    ]
    for pat in scope_patterns:
        m = re.search(pat, q)
        if not m:
            continue
        raw = m.group(1).strip()
        if raw in {"tactical", "hunting"}:
            break
        # "list my hunting knives" captures "my hunting"; knife_type is already set from keywords.
        my_tail = re.match(r"^my\s+(.+)$", raw, flags=re.I)
        if my_tail and my_tail.group(1).strip().lower() in {"hunting", "tactical"}:
            break
        if _reporting_is_series_term(raw):
            # Treat known series aliases as series filters.
            norm_series = _reporting_normalize_filter_value("series_name", raw)
            if norm_series:
                out["filters"]["series_name"] = norm_series
            break
        term = _reporting_normalize_filter_value("text_search", raw)
        if term:
            out["filters"]["text_search"] = term
            break

    # Negation / exclusion phrases (soft, generalized):
    # - "except Speedgoat"
    # - "without traditions"
    # - "except tactical knives"
    neg_patterns = [
        r"\bexcept\s+([a-z0-9][a-z0-9 -]+?)(?:\s+knives?|\s+models?)?\b",
        r"\bwithout\s+([a-z0-9][a-z0-9 -]+?)(?:\s+knives?|\s+models?)?\b",
    ]
    for pat in neg_patterns:
        m = re.search(pat, q)
        if not m:
            continue
        raw = m.group(1).strip()
        norm = _reporting_normalize_filter_value("text_search", raw)
        if not norm:
            continue
        # Prefer explicit controlled dimensions when obvious.
        series_norm = _reporting_normalize_filter_value("series_name", norm)
        if series_norm and series_norm in REPORTING_SERIES_ALIASES.values():
            out["filters"]["series_name__not"] = series_norm
            break
        if norm in {"tactical", "hunting"}:
            out["filters"]["knife_type__not"] = norm.title()
            break
        out["filters"]["text_search__not"] = norm
        break

    # Family extraction only for explicit missing-family phrasing; avoid broad false captures.
    family_patterns = [
        r"\bmissing any ([a-z0-9][a-z0-9 -]+?)\s+knives?\b",
        r"\bmissing any ([a-z0-9][a-z0-9 -]+?)\s+models?\b",
        r"\bwhich ([a-z0-9][a-z0-9 -]+?)\s+models?\s+do i still not have\b",
    ]
    for pat in family_patterns:
        m = re.search(pat, q)
        if not m:
            continue
        fam_raw = m.group(1).strip()
        if _reporting_is_series_term(fam_raw):
            break
        fam = _reporting_normalize_filter_value("family_name", fam_raw)
        if fam:
            out["filters"]["family_name"] = fam.title()
            break

    return out


def _reporting_prune_conflicting_filters(question: str, filters: dict[str, Any]) -> dict[str, Any]:
    """
    Resolve ambiguous identity filters that can over-constrain results.

    Example: prompts with "Traditions knives" should not accidentally enforce both
    series_name=Traditions and family_name=Traditions unless family is explicitly requested.
    """
    q = " ".join((question or "").strip().lower().split())
    out = dict(filters or {})
    if not out:
        return out

    series_val = out.get("series_name")
    if series_val:
        norm_series = _reporting_normalize_filter_value("series_name", series_val)
        fam_val = out.get("family_name")
        if fam_val:
            norm_fam = _reporting_normalize_filter_value("family_name", fam_val)
            explicit_family_dim = (
                "by family" in q
                or "grouped by family" in q
                or "family breakdown" in q
                or "per family" in q
                or "each family" in q
            )
            if norm_fam and norm_series and norm_fam.lower() == norm_series.lower() and (not explicit_family_dim):
                out.pop("family_name", None)
        type_val = out.get("knife_type")
        if type_val:
            norm_type = _reporting_normalize_filter_value("knife_type", type_val)
            explicit_type_dim = (
                "by type" in q
                or "grouped by type" in q
                or "type breakdown" in q
                or "knife type" in q
            )
            if norm_type and norm_series and norm_type.lower() == norm_series.lower() and (not explicit_type_dim):
                out.pop("knife_type", None)
    return out


def _reporting_has_substantive_rows(intent: Optional[str], rows: list[dict[str, Any]]) -> bool:
    if not rows:
        return False
    if intent in {"aggregate", "completion_cost"}:
        numeric_keys = (
            "rows_count",
            "total_spend",
            "total_estimated_value",
            "missing_models_count",
            "estimated_completion_cost_msrp",
        )
        for r in rows:
            for k in numeric_keys:
                try:
                    if float(r.get(k) or 0) > 0:
                        return True
                except Exception:
                    continue
        return False
    return True


def _reporting_relax_ambiguous_plan(plan: dict[str, Any], question: str) -> Optional[dict[str, Any]]:
    """
    If an initial plan yields non-substantive results, relax ambiguous filters by
    dropping likely duplicate constraints across dimensions.
    """
    p = dict(plan or {})
    f = dict(p.get("filters") or {})
    if not f:
        return None
    changed = False
    q = " ".join((question or "").strip().lower().split())
    s = _reporting_normalize_filter_value("series_name", f.get("series_name") or "")
    fam = _reporting_normalize_filter_value("family_name", f.get("family_name") or "")
    typ = _reporting_normalize_filter_value("knife_type", f.get("knife_type") or "")
    if s and fam and s == fam:
        # If user did not explicitly demand family segmentation, prefer series scope.
        if "by family" not in q and "family breakdown" not in q:
            f.pop("family_name", None)
            changed = True
    if s and typ and s == typ:
        if "by type" not in q and "knife type" not in q:
            f.pop("knife_type", None)
            changed = True
    if changed:
        p["filters"] = f
        return p
    return None


def _reporting_heuristic_plan(question: str, last_state: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    q = " ".join((question or "").strip().lower().split())
    plan = dict(last_state or {})
    if not plan or not _reporting_is_followup(q):
        plan = {
            "intent": "list_inventory",
            "filters": {},
            "group_by": None,
            "metric": "count",
            "limit": 200,
            "scope": _reporting_detect_scope(q) or "inventory",
        }
    else:
        # Preserve prior scope across contextual follow-ups unless user changes it.
        plan["scope"] = _reporting_detect_scope(q) or str(plan.get("scope") or "inventory")
    filters = dict(plan.get("filters") or {})

    if _reporting_is_completion_cost_question(q):
        plan["intent"] = "completion_cost"
    elif "missing" in q or "not in inventory" in q or "do i not have" in q:
        plan["intent"] = "missing_models"
    elif "how many" in q or "count" in q or "breakdown" in q or "distribution" in q:
        plan["intent"] = "aggregate"
        plan["metric"] = "count"
    elif "spend" in q or "spent" in q:
        plan["intent"] = "aggregate"
        plan["metric"] = "total_spend"
    elif "value" in q and ("total" in q or "by" in q):
        plan["intent"] = "aggregate"
        plan["metric"] = "total_estimated_value"
    elif any(w in q for w in ("list", "show", "which")) and "knife" in q:
        plan["intent"] = "list_inventory"
    elif _reporting_is_short_followup(q) and q in {"list them", "show them", "which ones", "show those", "list those"}:
        # Keep prior scope; just switch output shape to row listing.
        plan["intent"] = "list_inventory"
        plan["group_by"] = None

    group_by = _reporting_extract_dimension_from_text(q)
    if group_by:
        plan["group_by"] = group_by
        if plan.get("intent") == "list_inventory":
            plan["intent"] = "aggregate"
            plan["metric"] = plan.get("metric") or "count"
    if "look at series" in q:
        plan["group_by"] = "series_name"
        if plan.get("intent") == "list_inventory":
            plan["intent"] = "aggregate"
            plan["metric"] = "count"

    # Preserve date intent across short/contextual follow-ups when user did not
    # restate a new date window explicitly.
    if _reporting_is_followup(q) and isinstance(last_state, dict):
        for k in ("date_start", "date_end", "date_label", "year_compare"):
            if k in last_state and k not in plan:
                plan[k] = last_state[k]

    # Fresh question date intent overrides inherited state.
    ds, de, dl = _reporting_detect_date_bounds(question)
    if ds:
        plan["date_start"] = ds
    if de:
        plan["date_end"] = de
    if dl:
        plan["date_label"] = dl
    yc = _reporting_detect_year_comparison(question)
    if yc:
        plan["year_compare"] = [yc[0], yc[1]]

    for needle, canonical in REPORTING_SERIES_ALIASES.items():
        if needle in q:
            filters["series_name"] = canonical
            break
    if "tactical" in q:
        filters["knife_type"] = "Tactical"
    if "hunting" in q:
        filters["knife_type"] = "Hunting"

    plan["filters"] = filters
    plan["limit"] = min(REPORTING_MAX_ROWS_HARD, max(1, int(plan.get("limit") or REPORTING_MAX_ROWS_DEFAULT)))
    if plan.get("intent") not in REPORTING_INTENTS:
        plan["intent"] = "list_inventory"
    if plan.get("metric") not in REPORTING_METRICS:
        plan["metric"] = "count"
    if plan.get("group_by") and plan["group_by"] not in REPORTING_GROUPABLE_DIMENSIONS.values():
        plan["group_by"] = None
    return plan


def _reporting_llm_plan(
    model: str,
    question: str,
    context_block: str,
    schema_context: str,
) -> Optional[dict[str, Any]]:
    system = (
        "You convert collection questions into semantic JSON plans. "
        "Return JSON only with keys: intent, filters, group_by, metric, limit, date_start, date_end, year_compare. "
        "intent must be one of: missing_models, list_inventory, aggregate, completion_cost. "
        "filters is an object using only: series_name, family_name, knife_type, form_name, collaborator_name, steel, condition, location. "
        "group_by must be null or one of: series_name, family_name, knife_type, form_name, collaborator_name, steel, condition, location. "
        "metric must be one of: count, total_spend, total_estimated_value. "
        "date_start/date_end must be YYYY-MM-DD or null. "
        "year_compare must be null or [YYYY, YYYY] when user asks year-vs-year."
    )
    user = (
        f"Schema:\n{schema_context}\n\n"
        f"Context:\n{context_block or '(none)'}\n\n"
        f"Question:\n{question}\n"
    )
    try:
        raw = blade_ai.ollama_chat(model, system, user, timeout=60.0)
        parsed = None
        if raw.strip().startswith("{"):
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = None
        if parsed is None:
            m = re.search(r"\{.*\}", raw, re.S)
            if m:
                try:
                    parsed = json.loads(m.group(0))
                except Exception:
                    parsed = None
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def _reporting_semantic_plan(
    conn: sqlite3.Connection,
    planner_model: str,
    question: str,
    session_id: str,
    context_block: str,
    retry_model: Optional[str] = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    last_state = _reporting_get_last_query_state(conn, session_id)
    is_followup = _reporting_is_followup(question)
    explicit = _reporting_explicit_constraints(question)
    heuristic = _reporting_heuristic_plan(question, last_state=last_state)
    learned_hints = _reporting_get_semantic_hints(conn, session_id, question)
    schema_context = _reporting_build_prompt_schema(conn)
    llm_plan = _reporting_llm_plan(planner_model, question, context_block, schema_context)
    planner_attempts = 1
    if not isinstance(llm_plan, dict) and retry_model and retry_model != planner_model:
        retry = _reporting_llm_plan(retry_model, question, context_block, schema_context)
        planner_attempts = 2
        if isinstance(retry, dict):
            llm_plan = retry
    plan = dict(heuristic)
    mode = "semantic_heuristic"
    if isinstance(llm_plan, dict):
        # LLM can refine intent/grouping/metric while keeping validated filters.
        if llm_plan.get("intent") in REPORTING_INTENTS:
            plan["intent"] = llm_plan["intent"]
        if llm_plan.get("metric") in REPORTING_METRICS:
            plan["metric"] = llm_plan["metric"]
        if llm_plan.get("group_by") in REPORTING_GROUPABLE_DIMENSIONS.values():
            plan["group_by"] = llm_plan["group_by"]
        if isinstance(llm_plan.get("filters"), dict):
            f = {}
            for k, v in llm_plan["filters"].items():
                if k in {
                    "series_name", "family_name", "knife_type", "form_name", "collaborator_name", "steel", "condition", "location",
                    "series_name__not", "family_name__not", "knife_type__not", "form_name__not", "collaborator_name__not", "steel__not", "condition__not", "location__not",
                    "text_search", "text_search__not",
                }:
                    sv = _reporting_normalize_filter_value(k, str(v or ""))
                    if sv and (is_followup or _reporting_filter_explicit_in_question(question, k, sv)):
                        f[k] = sv
            if f:
                plan["filters"] = {**(plan.get("filters") or {}), **f}
        llm_ds = str(llm_plan.get("date_start") or "").strip()
        llm_de = str(llm_plan.get("date_end") or "").strip()
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", llm_ds):
            plan["date_start"] = llm_ds
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", llm_de):
            plan["date_end"] = llm_de
        yc = llm_plan.get("year_compare")
        if isinstance(yc, (list, tuple)) and len(yc) == 2:
            ya = str(yc[0]).strip()
            yb = str(yc[1]).strip()
            if re.fullmatch(r"(?:19|20)\d{2}", ya) and re.fullmatch(r"(?:19|20)\d{2}", yb):
                plan["year_compare"] = [ya, yb]
        mode = "semantic_llm_plus_heuristic"

    # Explicit user constraints always win over LLM refinements.
    if explicit.get("intent") in REPORTING_INTENTS:
        plan["intent"] = explicit["intent"]
    if explicit.get("metric") in REPORTING_METRICS:
        plan["metric"] = explicit["metric"]
    if explicit.get("scope") in {"inventory", "catalog"}:
        plan["scope"] = explicit["scope"]
    if explicit.get("group_by") in REPORTING_GROUPABLE_DIMENSIONS.values():
        plan["group_by"] = explicit["group_by"]
    if isinstance(explicit.get("filters"), dict) and explicit["filters"]:
        plan["filters"] = {**(plan.get("filters") or {}), **explicit["filters"]}
    if explicit.get("date_start"):
        plan["date_start"] = explicit.get("date_start")
    if explicit.get("date_end"):
        plan["date_end"] = explicit.get("date_end")
    if explicit.get("date_label"):
        plan["date_label"] = explicit.get("date_label")
    if isinstance(explicit.get("year_compare"), list) and len(explicit["year_compare"]) == 2:
        plan["year_compare"] = [str(explicit["year_compare"][0]), str(explicit["year_compare"][1])]

    # Final pass: remove ambiguous cross-dimension filters unless the dimension
    # is explicitly requested in user language.
    merged_filters = dict(plan.get("filters") or {})
    # Learned hints are soft priors: only add if the dimension is currently unset.
    for k, v in dict(learned_hints.get("filters") or {}).items():
        if k not in merged_filters and v:
            merged_filters[k] = v
    plan["filters"] = _reporting_prune_conflicting_filters(question, merged_filters)

    plan["scope"] = str(plan.get("scope") or "inventory")
    if plan["scope"] not in {"inventory", "catalog"}:
        plan["scope"] = "inventory"

    return plan, {
        "mode": mode,
        "planner_attempts": planner_attempts,
        "hint_ids": learned_hints.get("hint_ids") or [],
        "hints": learned_hints.get("hints") or [],
    }


def _reporting_plan_to_sql(
    plan: dict[str, Any],
    date_start: Optional[str],
    date_end: Optional[str],
    max_rows: int,
) -> tuple[Optional[str], dict[str, Any]]:
    intent = str(plan.get("intent") or "").strip()
    filters = dict(plan.get("filters") or {})
    group_by = plan.get("group_by")
    metric = str(plan.get("metric") or "count")
    limit = min(max_rows, int(plan.get("limit") or max_rows))
    scope = str(plan.get("scope") or "inventory").strip().lower()
    use_catalog = scope == "catalog"
    plan_date_start = str(plan.get("date_start") or "").strip() or None
    plan_date_end = str(plan.get("date_end") or "").strip() or None
    yc = plan.get("year_compare")
    year_compare: Optional[tuple[str, str]] = None
    if isinstance(yc, (list, tuple)) and len(yc) == 2:
        ya = str(yc[0]).strip()
        yb = str(yc[1]).strip()
        if re.fullmatch(r"(?:19|20)\d{2}", ya) and re.fullmatch(r"(?:19|20)\d{2}", yb):
            year_compare = (ya, yb)

    def esc(v: Any) -> str:
        return str(v or "").replace("'", "''").strip()

    def cond(k: str, v: str, *, exact: bool) -> str:
        ev = esc(v)
        if exact:
            return f"lower(COALESCE({k}, '')) = lower('{ev}')"
        return f"lower(COALESCE({k}, '')) LIKE lower('%{ev}%')"

    model_filter_cols = {
        "series_name", "family_name", "knife_type", "form_name", "collaborator_name", "steel", "text_search",
        "series_name__not", "family_name__not", "knife_type__not", "form_name__not", "collaborator_name__not", "steel__not", "text_search__not",
    }
    inv_filter_cols = {
        "series_name", "family_name", "knife_type", "form_name", "collaborator_name", "steel", "condition", "location", "text_search",
        "series_name__not", "family_name__not", "knife_type__not", "form_name__not", "collaborator_name__not", "steel__not", "condition__not", "location__not", "text_search__not",
    }

    if intent == "completion_cost":
        where = ["COALESCE(inv.total_qty, 0) = 0"]
        for k, v in filters.items():
            if k not in model_filter_cols:
                continue
            negate = k.endswith("__not")
            base_k = k[:-5] if negate else k
            if base_k == "text_search":
                ev = esc(v)
                expr = (
                    "("
                    "lower(COALESCE(m.official_name, '')) LIKE lower('%" + ev + "%') OR "
                    "lower(COALESCE(m.family_name, '')) LIKE lower('%" + ev + "%') OR "
                    "lower(COALESCE(m.form_name, '')) LIKE lower('%" + ev + "%') OR "
                    "lower(COALESCE(m.series_name, '')) LIKE lower('%" + ev + "%')"
                    ")"
                )
                where.append(f"NOT {expr}" if negate else expr)
                continue
            expr = cond(f"m.{base_k}", v, exact=(base_k in {"series_name", "knife_type"}))
            where.append(f"NOT ({expr})" if negate else expr)
        sql = (
            "SELECT "
            "COUNT(*) AS missing_models_count, "
            "ROUND(SUM(COALESCE(m.msrp, 0)), 2) AS estimated_completion_cost_msrp, "
            "ROUND(AVG(COALESCE(m.msrp, 0)), 2) AS avg_missing_model_msrp "
            "FROM reporting_models m "
            "LEFT JOIN (SELECT knife_model_id, SUM(COALESCE(quantity, 1)) AS total_qty FROM reporting_inventory GROUP BY knife_model_id) inv "
            "ON inv.knife_model_id = m.model_id "
            f"WHERE {' AND '.join(where)}"
        )
        return sql, {"mode": "semantic_compiled_completion_cost"}

    if intent == "missing_models":
        where = ["COALESCE(inv.total_qty, 0) = 0"]
        for k, v in filters.items():
            if k not in model_filter_cols:
                continue
            negate = k.endswith("__not")
            base_k = k[:-5] if negate else k
            if base_k == "text_search":
                ev = esc(v)
                expr = (
                    "("
                    "lower(COALESCE(m.official_name, '')) LIKE lower('%" + ev + "%') OR "
                    "lower(COALESCE(m.family_name, '')) LIKE lower('%" + ev + "%') OR "
                    "lower(COALESCE(m.form_name, '')) LIKE lower('%" + ev + "%') OR "
                    "lower(COALESCE(m.series_name, '')) LIKE lower('%" + ev + "%')"
                    ")"
                )
                where.append(f"NOT {expr}" if negate else expr)
                continue
            expr = cond(f"m.{base_k}", v, exact=(base_k in {"series_name", "knife_type"}))
            where.append(f"NOT ({expr})" if negate else expr)
        sql = (
            "SELECT m.model_id, m.official_name, m.knife_type, m.family_name, m.form_name, "
            "m.series_name, m.collaborator_name, m.record_status, COALESCE(inv.total_qty, 0) AS inventory_quantity "
            "FROM reporting_models m "
            "LEFT JOIN (SELECT knife_model_id, SUM(COALESCE(quantity, 1)) AS total_qty FROM reporting_inventory GROUP BY knife_model_id) inv "
            "ON inv.knife_model_id = m.model_id "
            f"WHERE {' AND '.join(where)} "
            "ORDER BY m.official_name "
            f"LIMIT {limit}"
        )
        return sql, {"mode": "semantic_compiled_missing_models"}

    where = []
    for k, v in filters.items():
        if k not in inv_filter_cols:
            continue
        negate = k.endswith("__not")
        base_k = k[:-5] if negate else k
        if use_catalog and base_k in {"condition", "location"}:
            # Catalog scope does not include inventory-only dimensions.
            continue
        if base_k == "text_search":
            ev = esc(v)
            if use_catalog:
                expr = (
                    "("
                    "lower(COALESCE(official_name, '')) LIKE lower('%" + ev + "%') OR "
                    "lower(COALESCE(family_name, '')) LIKE lower('%" + ev + "%') OR "
                    "lower(COALESCE(form_name, '')) LIKE lower('%" + ev + "%') OR "
                    "lower(COALESCE(series_name, '')) LIKE lower('%" + ev + "%') OR "
                    "lower(COALESCE(collaborator_name, '')) LIKE lower('%" + ev + "%')"
                    ")"
                )
            else:
                expr = (
                    "("
                    "lower(COALESCE(knife_name, '')) LIKE lower('%" + ev + "%') OR "
                    "lower(COALESCE(family_name, '')) LIKE lower('%" + ev + "%') OR "
                    "lower(COALESCE(form_name, '')) LIKE lower('%" + ev + "%') OR "
                    "lower(COALESCE(series_name, '')) LIKE lower('%" + ev + "%') OR "
                    "lower(COALESCE(collaborator_name, '')) LIKE lower('%" + ev + "%')"
                    ")"
                )
            where.append(f"NOT {expr}" if negate else expr)
            continue
        expr = cond(base_k, v, exact=(base_k in {"series_name", "knife_type", "condition"}))
        where.append(f"NOT ({expr})" if negate else expr)
    effective_date_start = plan_date_start or date_start
    effective_date_end = plan_date_end or date_end
    if effective_date_start:
        where.append(f"acquired_date >= '{esc(effective_date_start)}'")
    if effective_date_end:
        where.append(f"acquired_date <= '{esc(effective_date_end)}'")
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    if intent == "aggregate":
        source_view = "reporting_models" if use_catalog else "reporting_inventory"
        supported_catalog_group = {"series_name", "family_name", "knife_type", "form_name", "collaborator_name", "steel"}
        if use_catalog and group_by and group_by not in supported_catalog_group:
            # Fallback to inventory when grouping on inventory-only dimensions.
            source_view = "reporting_inventory"
        if metric == "total_spend":
            expr = "ROUND(SUM(COALESCE(purchase_price, 0) * COALESCE(quantity, 1)), 2) AS total_spend"
            sort_col = "total_spend"
        elif metric == "total_estimated_value":
            expr = "ROUND(SUM(COALESCE(estimated_value, 0) * COALESCE(quantity, 1)), 2) AS total_estimated_value"
            sort_col = "total_estimated_value"
        else:
            expr = "COUNT(*) AS rows_count"
            sort_col = "rows_count"
            if source_view == "reporting_models":
                expr = "COUNT(*) AS rows_count"
        if year_compare and source_view == "reporting_inventory" and not group_by:
            ya, yb = year_compare
            sql = (
                "SELECT substr(acquired_date, 1, 4) AS bucket, "
                f"{expr} "
                "FROM reporting_inventory "
                f"WHERE acquired_date IS NOT NULL AND substr(acquired_date, 1, 4) IN ('{esc(ya)}', '{esc(yb)}') "
                "GROUP BY bucket "
                "ORDER BY bucket"
            )
            return sql, {"mode": "semantic_compiled_year_compare"}
        if group_by in REPORTING_GROUPABLE_DIMENSIONS.values():
            sql = (
                f"SELECT COALESCE({group_by}, 'Unknown') AS bucket, {expr} "
                f"FROM {source_view} "
                f"{where_sql} "
                "GROUP BY bucket "
                f"ORDER BY {sort_col} DESC "
                f"LIMIT {limit}"
            )
        else:
            sql = f"SELECT {expr} FROM {source_view} {where_sql}"
        return sql, {"mode": "semantic_compiled_aggregate"}

    if use_catalog:
        sql = (
            "SELECT model_id, official_name AS knife_name, knife_type, family_name, form_name, series_name, collaborator_name, "
            "steel, blade_finish, handle_color, handle_type, blade_length, msrp, record_status "
            "FROM reporting_models "
            f"{where_sql} "
            "ORDER BY knife_name "
            f"LIMIT {limit}"
        )
    else:
        sql = (
            "SELECT inventory_id, knife_name, knife_type, family_name, form_name, series_name, collaborator_name, "
            "steel, blade_finish, handle_color, condition, quantity, location "
            "FROM reporting_inventory "
            f"{where_sql} "
            "ORDER BY knife_name "
            f"LIMIT {limit}"
        )
    return sql, {"mode": "semantic_compiled_list_inventory"}


def _reporting_template_sql(
    question: str,
    start_date: Optional[str],
    end_date: Optional[str],
    compare_dimension: Optional[str] = None,
    compare_a: Optional[str] = None,
    compare_b: Optional[str] = None,
) -> tuple[Optional[str], dict[str, Any]]:
    q = (question or "").lower()
    where = []
    if start_date:
        where.append(f"acquired_date >= '{start_date}'")
    if end_date:
        where.append(f"acquired_date <= '{end_date}'")
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    dim_map = {
        "family": "family_name",
        "type": "knife_type",
        "series": "series_name",
        "steel": "steel",
        "condition": "condition",
        "location": "location",
    }
    if compare_dimension and compare_a and compare_b:
        dim_col = dim_map.get(compare_dimension.lower())
        if dim_col:
            safe_a = compare_a.replace("'", "''")
            safe_b = compare_b.replace("'", "''")
            base_where = [f"{dim_col} IN ('{safe_a}', '{safe_b}')"]
            if start_date:
                base_where.append(f"acquired_date >= '{start_date}'")
            if end_date:
                base_where.append(f"acquired_date <= '{end_date}'")
            return (
                "SELECT "
                f"{dim_col} AS bucket, "
                "ROUND(SUM(COALESCE(purchase_price, 0) * COALESCE(quantity, 1)), 2) AS total_spend, "
                "ROUND(SUM(COALESCE(estimated_value, 0) * COALESCE(quantity, 1)), 2) AS total_estimated_value, "
                "COUNT(*) AS rows_count "
                "FROM reporting_inventory "
                f"WHERE {' AND '.join(base_where)} "
                "GROUP BY bucket ORDER BY total_estimated_value DESC",
                {"mode": "template_compare", "chart_type": "bar"},
            )

    # Missing-models intents are handled by semantic planner/compiler in primary flow.

    if _reporting_is_completion_cost_question(q):
        return (
            "SELECT "
            "COUNT(*) AS missing_models_count, "
            "ROUND(SUM(COALESCE(m.msrp, 0)), 2) AS estimated_completion_cost_msrp, "
            "ROUND(AVG(COALESCE(m.msrp, 0)), 2) AS avg_missing_model_msrp "
            "FROM reporting_models m "
            "LEFT JOIN ("
            "  SELECT knife_model_id, SUM(COALESCE(quantity, 1)) AS total_qty "
            "  FROM reporting_inventory "
            "  GROUP BY knife_model_id"
            ") inv ON inv.knife_model_id = m.model_id "
            "WHERE COALESCE(inv.total_qty, 0) = 0",
            {"mode": "template_completion_cost", "chart_type": "bar"},
        )

    if (
        ("tactical" in q and ("list" in q or "show" in q or "what" in q))
        or ("my tactical knives" in q)
    ):
        return (
            "SELECT "
            "inventory_id, knife_name, knife_type, family_name, form_name, series_name, "
            "collaborator_name, steel, blade_finish, handle_color, condition, quantity, location "
            "FROM reporting_inventory "
            "WHERE ("
            "  LOWER(COALESCE(knife_type, '')) LIKE '%tactical%' "
            "  OR LOWER(COALESCE(family_name, '')) LIKE '%tactical%' "
            "  OR LOWER(COALESCE(form_name, '')) LIKE '%tactical%' "
            "  OR LOWER(COALESCE(series_name, '')) LIKE '%tactical%' "
            "  OR LOWER(COALESCE(notes, '')) LIKE '%tactical%'"
            ") "
            "ORDER BY knife_name",
            {"mode": "template_tactical_list", "chart_type": "bar"},
        )

    if "spend by month" in q or "spent by month" in q or "monthly spend" in q:
        month_where = []
        if start_date:
            month_where.append(f"acquired_date >= '{start_date}'")
        if end_date:
            month_where.append(f"acquired_date <= '{end_date}'")
        month_where.append("acquired_date IS NOT NULL")
        month_where_sql = "WHERE " + " AND ".join(month_where)
        return (
            "SELECT substr(acquired_date, 1, 7) AS month, "
            "ROUND(SUM(COALESCE(purchase_price, 0) * COALESCE(quantity, 1)), 2) AS total_spend "
            "FROM reporting_inventory "
            f"{month_where_sql} "
            "GROUP BY month ORDER BY month",
            {"mode": "template_spend_month", "chart_type": "line"},
        )
    if "most valuable" in q or "top value" in q or "highest value" in q:
        return (
            "SELECT knife_name, family_name, series_name, estimated_value, quantity "
            "FROM reporting_inventory "
            f"{where_sql} "
            "ORDER BY COALESCE(estimated_value, 0) DESC LIMIT 25",
            {"mode": "template_top_value", "chart_type": "bar"},
        )
    if "by family" in q:
        return (
            "SELECT COALESCE(family_name, 'Uncategorized') AS family_name, "
            "COUNT(*) AS rows_count, "
            "SUM(COALESCE(quantity, 1)) AS total_quantity, "
            "ROUND(SUM(COALESCE(estimated_value, 0) * COALESCE(quantity, 1)), 2) AS total_estimated_value "
            "FROM reporting_inventory "
            f"{where_sql} "
            "GROUP BY family_name ORDER BY total_estimated_value DESC",
            {"mode": "template_family_breakdown", "chart_type": "bar"},
        )
    if "by steel" in q:
        return (
            "SELECT COALESCE(steel, 'Unknown') AS steel, COUNT(*) AS rows_count "
            "FROM reporting_inventory "
            f"{where_sql} "
            "GROUP BY steel ORDER BY rows_count DESC",
            {"mode": "template_steel_breakdown", "chart_type": "pie"},
        )
    if "condition" in q and ("breakdown" in q or "distribution" in q or "by condition" in q):
        return (
            "SELECT COALESCE(condition, 'Like New') AS condition, COUNT(*) AS rows_count "
            "FROM reporting_inventory "
            f"{where_sql} "
            "GROUP BY condition ORDER BY rows_count DESC",
            {"mode": "template_condition_breakdown", "chart_type": "pie"},
        )
    if "purchase" in q and "source" in q:
        return (
            "SELECT COALESCE(purchase_source, 'Unknown') AS purchase_source, COUNT(*) AS rows_count "
            "FROM reporting_inventory "
            f"{where_sql} "
            "GROUP BY purchase_source ORDER BY rows_count DESC",
            {"mode": "template_source_breakdown", "chart_type": "bar"},
        )
    return None, {}


def _reporting_call_llm_for_sql(
    conn: sqlite3.Connection,
    model: str,
    question: str,
    context_summary: str,
    start_date: Optional[str],
    end_date: Optional[str],
) -> tuple[Optional[str], dict[str, Any]]:
    schema = _reporting_build_prompt_schema(conn)
    date_rule = ""
    if start_date or end_date:
        date_rule = f"Date window for acquired_date: start={start_date or 'none'}, end={end_date or 'none'}."
    system = (
        "You generate read-only SQLite SELECT queries for collection reporting. "
        "Return JSON only: {\"sql\":..., \"chart_type\":..., \"confidence\":..., \"limitations\":..., \"follow_ups\":[...]}. "
        "Use only allowed sources and columns exactly as provided. "
        "For knife names, prefer reporting_inventory.knife_name or reporting_models.official_name; never use nickname aliases. "
        "When terms like Traditions, VIP, Ultra, or Blood Brothers appear, treat them as series_name unless user explicitly asks for type/family. "
        "For short follow-up prompts (e.g., 'look at series'), preserve prior user intent from context and correct the dimension rather than starting a new unrelated query. "
        "Never emit non-SELECT SQL. Do not include semicolons."
    )
    user = (
        f"Allowed views:\n{schema}\n\n"
        f"Context summary:\n{context_summary or '(none)'}\n\n"
        f"{date_rule}\n"
        f"Question: {question}\n"
        "Generate one query."
    )
    try:
        raw = blade_ai.ollama_chat(model, system, user, timeout=90.0)
        def _clean_extracted_sql(candidate: str) -> str:
            s = (candidate or "").strip()
            # Remove markdown fences/labels if present.
            s = re.sub(r"(?is)^```(?:sql)?\s*", "", s).strip()
            s = re.sub(r"(?is)\s*```$", "", s).strip()
            # Cut non-SQL trailing sections from common malformed outputs.
            cut_markers = [
                "\n```",
                "\nAnswer:",
                "\nRationale:",
                "\nNotes:",
                "\n{",
                '"chart_type"',
                '"confidence"',
                '"limitations"',
                '"follow_ups"',
            ]
            cut_at = -1
            for marker in cut_markers:
                idx = s.find(marker)
                if idx > 0 and (cut_at == -1 or idx < cut_at):
                    cut_at = idx
            if cut_at > 0:
                s = s[:cut_at].strip()
            # Normalize escaped newlines from JSON-like string dumps.
            s = s.replace("\\n", "\n").strip()
            return s

        parsed = None
        parse_error: Optional[str] = None
        if raw.strip().startswith("{"):
            try:
                parsed = json.loads(raw)
            except (json.JSONDecodeError, TypeError, ValueError) as exc:
                parse_error = str(exc)
        if parsed is None:
            m = re.search(r"\{.*\}", raw, re.S)
            if m:
                try:
                    parsed = json.loads(m.group(0))
                except (json.JSONDecodeError, TypeError, ValueError) as exc:
                    parse_error = str(exc)
        sql = None
        meta: dict[str, Any]
        if isinstance(parsed, dict):
            sql = _clean_extracted_sql(str(parsed.get("sql") or ""))
            meta = {
                "mode": "llm_sql",
                "chart_type": parsed.get("chart_type"),
                "confidence": parsed.get("confidence"),
                "limitations": parsed.get("limitations"),
                "follow_ups": parsed.get("follow_ups") if isinstance(parsed.get("follow_ups"), list) else [],
                "raw": raw[:1000],
            }
        else:
            # Fallback when model returns prose with embedded SQL instead of strict JSON.
            m_fenced = re.search(r"(?is)```(?:sql)?\s*((?:select|with)\b.*?)```", raw)
            if m_fenced:
                sql = _clean_extracted_sql(m_fenced.group(1))
            else:
                m_sql = re.search(r"(?is)\b(select|with)\b.+", raw)
                if m_sql:
                    sql = _clean_extracted_sql(m_sql.group(0))
            meta = {
                "mode": "llm_sql_fallback_extract",
                "chart_type": None,
                "confidence": None,
                "limitations": "LLM did not return strict JSON; extracted SQL from response text.",
                "follow_ups": [],
                "raw": raw[:1000],
            }
            if parse_error:
                meta["parse_error"] = parse_error[:300]
        meta = {
            **meta,
        }
        return sql, meta
    except Exception as exc:
        return None, {"mode": "llm_failed", "error": str(exc)}

def migrate_legacy_media_to_v2(conn: sqlite3.Connection) -> dict[str, int]:
    """
    Copy legacy master image/silhouette/descriptor data into v2 media tables.
    Safe to run repeatedly; only fills missing v2 values.
    """
    models = conn.execute(
        """
        SELECT id, legacy_master_id
        FROM knife_models_v2
        WHERE legacy_master_id IS NOT NULL
        """
    ).fetchall()
    images_copied = 0
    descriptors_copied = 0
    for m in models:
        master = conn.execute(
            """
            SELECT identifier_image_blob, identifier_image_mime, identifier_silhouette_hu_json,
                   identifier_distinguishing_features
            FROM master_knives
            WHERE id = ?
            """,
            (m["legacy_master_id"],),
        ).fetchone()
        if not master:
            continue

        has_blob = bool(master.get("identifier_image_blob"))
        if has_blob:
            existing_img = conn.execute(
                "SELECT image_blob, image_mime, silhouette_hu_json FROM knife_model_images WHERE knife_model_id = ?",
                (m["id"],),
            ).fetchone()
            should_write_img = (
                not existing_img
                or not existing_img.get("image_blob")
                or not existing_img.get("image_mime")
                or (
                    (master.get("identifier_silhouette_hu_json") or "").strip()
                    and not (existing_img.get("silhouette_hu_json") or "").strip()
                )
            )
            if should_write_img:
                conn.execute(
                    """
                    INSERT INTO knife_model_images
                    (knife_model_id, image_blob, image_mime, silhouette_hu_json, updated_at)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(knife_model_id) DO UPDATE SET
                        image_blob = COALESCE(knife_model_images.image_blob, excluded.image_blob),
                        image_mime = COALESCE(knife_model_images.image_mime, excluded.image_mime),
                        silhouette_hu_json = COALESCE(knife_model_images.silhouette_hu_json, excluded.silhouette_hu_json),
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        m["id"],
                        master.get("identifier_image_blob"),
                        master.get("identifier_image_mime") or "image/jpeg",
                        master.get("identifier_silhouette_hu_json"),
                    ),
                )
                images_copied += 1

        legacy_features = (master.get("identifier_distinguishing_features") or "").strip()
        if legacy_features:
            existing_desc = conn.execute(
                "SELECT distinguishing_features FROM knife_model_descriptors WHERE knife_model_id = ?",
                (m["id"],),
            ).fetchone()
            if not existing_desc or not (existing_desc.get("distinguishing_features") or "").strip():
                conn.execute(
                    """
                    INSERT INTO knife_model_descriptors
                    (knife_model_id, distinguishing_features, updated_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(knife_model_id) DO UPDATE SET
                        distinguishing_features = COALESCE(knife_model_descriptors.distinguishing_features, excluded.distinguishing_features),
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (m["id"], legacy_features),
                )
                descriptors_copied += 1

    return {"images_copied": images_copied, "descriptors_copied": descriptors_copied}


# v2 Phase 1: columns and mapping tables (was migrate_v2_phase1.py)
PHASE1_MASTER_COLUMNS = [
    ("canonical_slug", "TEXT"),
    ("version", "TEXT"),
    ("parent_model_id", "INTEGER"),
    ("first_release_date", "TEXT"),
    ("last_seen_date", "TEXT"),
    ("is_discontinued", "INTEGER DEFAULT 0"),
    ("is_current_catalog", "INTEGER DEFAULT 1"),
    ("msrp", "REAL"),
    ("blade_shape", "TEXT"),
    ("tip_style", "TEXT"),
    ("grind_style", "TEXT"),
    ("size_class", "TEXT"),
    ("primary_use_case", "TEXT"),
    ("spine_profile", "TEXT"),
    ("has_ring", "INTEGER DEFAULT 0"),
    ("is_fillet", "INTEGER DEFAULT 0"),
    ("is_hatchet", "INTEGER DEFAULT 0"),
    ("default_product_url", "TEXT"),
    ("primary_image_url", "TEXT"),
]
PHASE1_MAPPING_TABLES = [
    ("master_knife_allowed_handle_colors", "handle_color_id", "option_handle_colors"),
    ("master_knife_allowed_blade_steels", "blade_steel_id", "option_blade_steels"),
    ("master_knife_allowed_blade_finishes", "blade_finish_id", "option_blade_finishes"),
    ("master_knife_allowed_blade_colors", "blade_color_id", "option_blade_colors"),
]


def _slugify_for_canonical(name: str) -> str:
    """Lowercase, replace spaces with hyphens, strip punctuation."""
    if not name or not name.strip():
        return ""
    s = name.lower().strip()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"-+", "-", s)
    return s.strip("-") or ""


def ensure_phase1_schema(conn: sqlite3.Connection) -> None:
    """v2 Phase 1 migration inlined from migrate_v2_phase1. Idempotent."""
    for col, sql_type in PHASE1_MASTER_COLUMNS:
        if not column_exists(conn, "master_knives", col):
            conn.execute(f"ALTER TABLE master_knives ADD COLUMN {col} {sql_type}")

    for table, id_col, option_table in PHASE1_MAPPING_TABLES:
        if table_exists(conn, table):
            continue
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                master_knife_id INTEGER NOT NULL,
                {id_col} INTEGER NOT NULL,
                FOREIGN KEY (master_knife_id) REFERENCES master_knives(id) ON DELETE CASCADE,
                FOREIGN KEY ({id_col}) REFERENCES {option_table}(id) ON DELETE CASCADE,
                UNIQUE(master_knife_id, {id_col})
            )
            """
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS ix_{table}_master ON {table}(master_knife_id)"
        )

    rows = conn.execute(
        "SELECT id, name, canonical_slug FROM master_knives"
    ).fetchall()
    used_slugs: set[str] = set()
    for row in rows:
        rid = row["id"]
        name = row["name"]
        existing_slug = row.get("canonical_slug")
        if existing_slug and str(existing_slug).strip():
            used_slugs.add(str(existing_slug).strip().lower())
            continue
        base = _slugify_for_canonical(name or "")
        if not base:
            continue
        slug = base
        n = 2
        while slug.lower() in used_slugs:
            slug = f"{base}-{n}"
            n += 1
        used_slugs.add(slug.lower())
        conn.execute(
            "UPDATE master_knives SET canonical_slug = ? WHERE id = ?",
            (slug, rid),
        )

    try:
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_master_knives_canonical_slug "
            "ON master_knives(canonical_slug) WHERE canonical_slug IS NOT NULL AND canonical_slug != ''"
        )
    except sqlite3.OperationalError:
        pass

    conn.execute(
        """
        UPDATE master_knives
        SET is_discontinued = 0, is_current_catalog = 1
        WHERE is_discontinued IS NULL OR is_current_catalog IS NULL
        """
    )


def ensure_version_parent_model_columns(conn: sqlite3.Connection) -> None:
    """
    Safely add version and parent_model_id to master_knives if missing.
    - version: TEXT, nullable
    - parent_model_id: INTEGER, nullable, self-references master_knives.id (conceptual only; no FK enforced).
    No data loss; idempotent.
    """
    for col, sql_type, desc in [
        ("version", "TEXT", "version (TEXT, nullable)"),
        ("parent_model_id", "INTEGER", "parent_model_id (INTEGER, nullable, references master_knives.id)"),
    ]:
        if column_exists(conn, "master_knives", col):
            _app_logger.debug("[migration] master_knives.%s: already existed", col)
        else:
            conn.execute(f"ALTER TABLE master_knives ADD COLUMN {col} {sql_type}")
            _app_logger.info("[migration] master_knives.%s: added (%s)", col, desc)


def ensure_identifier_columns(conn: sqlite3.Connection) -> None:
    for column, sql_type in IDENTIFIER_COLUMNS.items():
        if not column_exists(conn, "master_knives", column):
            conn.execute(f"ALTER TABLE master_knives ADD COLUMN {column} {sql_type}")


def ensure_master_extra_columns(conn: sqlite3.Connection) -> None:
    if not column_exists(conn, "master_knives", "default_handle_color"):
        conn.execute("ALTER TABLE master_knives ADD COLUMN default_handle_color TEXT")


# Columns sourced from ``Knife Master.csv`` (distinct from ``status`` = active/archived in the app).
MASTER_CATALOG_TEXT_COLUMNS = {
    "record_type": "TEXT",
    "catalog_status": "TEXT",
    "confidence": "TEXT",
    "evidence_summary": "TEXT",
    "collector_notes": "TEXT",
}


def ensure_master_catalog_columns(conn: sqlite3.Connection) -> None:
    for column, sql_type in MASTER_CATALOG_TEXT_COLUMNS.items():
        if not column_exists(conn, "master_knives", column):
            conn.execute(f"ALTER TABLE master_knives ADD COLUMN {column} {sql_type}")


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


def backfill_v2_model_identity(conn: sqlite3.Connection) -> dict[str, int]:
    """
    Normalize and backfill v2 identity dimensions using normalized-model heuristics.

    This is idempotent. It fills missing values and also fixes malformed legacy-like
    values that leaked into v2 dimensions (for example: "hunting/edc", "tactical",
    lowercase family labels, etc.).
    """
    rows = conn.execute(
        """
        SELECT km.id, km.official_name, km.normalized_name, km.generation_label, km.size_modifier, km.platform_variant,
               km.type_id, km.form_id, km.family_id, km.series_id, km.collaborator_id,
               kt.name AS type_name, frm.name AS form_name, fam.name AS family_name,
               ks.name AS series_name, c.name AS collaborator_name
        FROM knife_models_v2 km
        LEFT JOIN knife_types kt ON kt.id = km.type_id
        LEFT JOIN knife_forms frm ON frm.id = km.form_id
        LEFT JOIN knife_families fam ON fam.id = km.family_id
        LEFT JOIN knife_series ks ON ks.id = km.series_id
        LEFT JOIN collaborators c ON c.id = km.collaborator_id
        ORDER BY km.id
        """
    ).fetchall()
    updated = 0
    inferred = 0

    canonical_series_names = {
        "traditions": "Traditions",
        "vip": "VIP",
        "ultra": "Ultra",
        "blood brothers": "Blood Brothers",
    }
    collab_like_series = {
        "archery country": "Archery Country",
        "archery": "Archery Country",
        "bearded butchers": "Bearded Butchers",
        "meat church": "Meat Church",
        "nock on": "Nock On",
    }
    generic_family_labels = {
        "hunting", "culinary", "tactical", "everyday carry", "edc", "bushcraft",
        "bushcraft & camp", "camp", "utility", "work", "kitchen", "fillet", "skinner",
    }

    def _dim_id_local(table: str, name: Optional[str]) -> Optional[int]:
        if not name or not str(name).strip():
            return None
        n = str(name).strip()
        row = conn.execute(f"SELECT id FROM {table} WHERE name = ?", (n,)).fetchone()
        if row:
            return row["id"]
        if table == "knife_types":
            cur = conn.execute(
                "INSERT INTO knife_types (name, slug, sort_order) VALUES (?, ?, ?)",
                (n, normalized_model.slugify(n), 999),
            )
            return cur.lastrowid
        if table == "knife_forms":
            cur = conn.execute(
                "INSERT INTO knife_forms (name, slug) VALUES (?, ?)",
                (n, normalized_model.slugify(n)),
            )
            return cur.lastrowid
        if table == "knife_families":
            cur = conn.execute(
                "INSERT INTO knife_families (name, normalized_name, slug) VALUES (?, ?, ?)",
                (n, n, normalized_model.slugify(n)),
            )
            return cur.lastrowid
        if table == "knife_series":
            cur = conn.execute(
                "INSERT INTO knife_series (name, slug) VALUES (?, ?)",
                (n, normalized_model.slugify(n)),
            )
            return cur.lastrowid
        if table == "collaborators":
            cur = conn.execute(
                "INSERT INTO collaborators (name, slug) VALUES (?, ?)",
                (n, normalized_model.slugify(n)),
            )
            return cur.lastrowid
        return None

    def _is_generic_or_legacy_label(v: Optional[str]) -> bool:
        if not v or not str(v).strip():
            return True
        s = str(v).strip()
        ls = s.lower()
        if "/" in s:
            return True
        if ls in generic_family_labels:
            return True
        return False

    def _is_lowercase_word(v: Optional[str]) -> bool:
        if not v or not str(v).strip():
            return False
        s = str(v).strip()
        has_alpha = any(ch.isalpha() for ch in s)
        return has_alpha and s == s.lower()

    for row in rows:
        official = (row.get("official_name") or "").strip()
        normalized = (row.get("normalized_name") or official).strip()

        if not official:
            continue

        current_type = (row.get("type_name") or "").strip()
        current_form = (row.get("form_name") or "").strip()
        current_family = (row.get("family_name") or "").strip()
        current_series = (row.get("series_name") or "").strip()
        current_collaborator = (row.get("collaborator_name") or "").strip()

        normalized_type = normalize_category_value(current_type) if current_type else None
        if not normalized_type:
            normalized_type = normalized_model.detect_type(None, current_family, 0, 0, 0, normalized)

        series_guess_from_name = normalized_model.detect_series(official, current_series or None)
        series_guess = (series_guess_from_name or current_series or "").strip() or None
        family_guess = normalized_model.detect_family(normalized)
        form_guess = normalized_model.detect_form(
            normalized,
            current_form or None,
            None,
            None,
            normalized_type or "Hunting",
        )
        collab_guess = normalized_model.detect_collaborator(
            1 if (series_guess and series_guess.lower() in collab_like_series) else 0,
            current_collaborator or None,
            series_guess,
        )
        if collab_guess and collab_guess.strip().lower() in {"nock on", "nock on archery", "knock on archery"}:
            collab_guess = "Cam Hanes"
        if series_guess:
            sk = series_guess.lower()
            if sk in canonical_series_names:
                series_guess = canonical_series_names[sk]
            elif sk in collab_like_series:
                if not collab_guess:
                    collab_guess = collab_like_series[sk]
                series_guess = None

        needs_type = row.get("type_id") is None or (normalized_type and normalized_type != current_type)
        needs_form = row.get("form_id") is None or _is_generic_or_legacy_label(current_form)
        needs_family = (
            row.get("family_id") is None
            or _is_generic_or_legacy_label(current_family)
            or _is_lowercase_word(current_family)
        )
        needs_series = row.get("series_id") is None or bool(current_series and current_series.lower() in collab_like_series)
        needs_collab = row.get("collaborator_id") is None and bool(collab_guess)
        needs_gen = not (row.get("generation_label") or "").strip()
        needs_size = not (row.get("size_modifier") or "").strip()
        needs_platform = not (row.get("platform_variant") or "").strip()

        if not any((needs_type, needs_form, needs_family, needs_series, needs_collab, needs_gen, needs_size, needs_platform)):
            continue

        _, _, _, size_or_generation, platform_guess = normalized_model.normalize_model_name(official, series_guess)
        if platform_guess and str(platform_guess).strip().lower() == "ultra":
            series_guess = "Ultra"
            needs_series = True
            platform_guess = None

        type_id = row.get("type_id")
        form_id = row.get("form_id")
        family_id = row.get("family_id")
        series_id = row.get("series_id")
        collaborator_id = row.get("collaborator_id")
        generation_label = row.get("generation_label")
        size_modifier = row.get("size_modifier")
        platform_variant = row.get("platform_variant")

        if needs_type and normalized_type:
            type_id = _dim_id_local("knife_types", normalized_type)
            inferred += 1
        if needs_form and form_guess:
            form_id = _dim_id_local("knife_forms", form_guess)
            inferred += 1
        if needs_family and family_guess:
            family_id = _dim_id_local("knife_families", family_guess)
            inferred += 1
        if needs_series and series_guess:
            series_id = _dim_id_local("knife_series", series_guess)
            inferred += 1
        if needs_collab and collab_guess:
            collaborator_id = _dim_id_local("collaborators", collab_guess)
            inferred += 1

        if needs_gen and size_or_generation and str(size_or_generation).replace(".", "", 1).isdigit():
            generation_label = str(size_or_generation)
            inferred += 1
        if needs_size and size_or_generation and not str(size_or_generation).replace(".", "", 1).isdigit():
            size_modifier = str(size_or_generation)
            inferred += 1
        if needs_platform and platform_guess:
            platform_variant = platform_guess
            inferred += 1

        conn.execute(
            """
            UPDATE knife_models_v2
            SET type_id = COALESCE(?, type_id),
                form_id = COALESCE(?, form_id),
                family_id = COALESCE(?, family_id),
                series_id = COALESCE(?, series_id),
                collaborator_id = COALESCE(?, collaborator_id),
                generation_label = COALESCE(?, generation_label),
                size_modifier = COALESCE(?, size_modifier),
                platform_variant = COALESCE(?, platform_variant),
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                type_id,
                form_id,
                family_id,
                series_id,
                collaborator_id,
                generation_label,
                size_modifier,
                platform_variant,
                row["id"],
            ),
        )
        updated += 1
    return {"rows_updated": updated, "values_inferred": inferred}


def normalize_v2_additional_fields(conn: sqlite3.Connection) -> dict[str, int]:
    """
    Normalize additional non-category fields:
    - collaborator / series alias cleanup (dimension level)
    - model + inventory attribute text normalization (steel, finish, colors, condition)
    """
    changes = {
        "series_alias_merged": 0,
        "collaborator_alias_merged": 0,
        "series_collab_reclassified": 0,
        "ultra_platform_to_series": 0,
        "ultra_platform_cleared": 0,
        "model_attr_rows_updated": 0,
        "inventory_attr_rows_updated": 0,
    }

    def _collapse(v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        s = " ".join(str(v).strip().split())
        return s or None

    def _norm_map(v: Optional[str], mapping: dict[str, str]) -> Optional[str]:
        s = _collapse(v)
        if s is None:
            return None
        return mapping.get(s.lower(), s)

    def _ensure_dim_local(table: str, name: Optional[str]) -> Optional[int]:
        if not name or not str(name).strip():
            return None
        n = str(name).strip()
        row = conn.execute(f"SELECT id FROM {table} WHERE lower(name) = lower(?) LIMIT 1", (n,)).fetchone()
        if row:
            return row["id"]
        if table == "knife_series":
            cur = conn.execute(
                "INSERT INTO knife_series (name, slug) VALUES (?, ?)",
                (n, normalized_model.slugify(n)),
            )
            return cur.lastrowid
        if table == "collaborators":
            cur = conn.execute(
                "INSERT INTO collaborators (name, slug) VALUES (?, ?)",
                (n, normalized_model.slugify(n)),
            )
            return cur.lastrowid
        return None

    def _merge_dim_aliases(table: str, fk_col: str, aliases: dict[str, str]) -> int:
        merged = 0
        for alias, canonical in aliases.items():
            a = conn.execute(
                f"SELECT id, name, slug FROM {table} WHERE lower(name) = lower(?) LIMIT 1",
                (alias,),
            ).fetchone()
            if not a:
                continue
            c = conn.execute(
                f"SELECT id, name, slug FROM {table} WHERE lower(name) = lower(?) LIMIT 1",
                (canonical,),
            ).fetchone()
            canonical_slug = normalized_model.slugify(canonical)
            if not c:
                c = conn.execute(
                    f"SELECT id, name, slug FROM {table} WHERE slug = ? LIMIT 1",
                    (canonical_slug,),
                ).fetchone()
            if c and c["id"] != a["id"]:
                conn.execute(
                    f"UPDATE knife_models_v2 SET {fk_col} = ? WHERE {fk_col} = ?",
                    (c["id"], a["id"]),
                )
                conn.execute(f"DELETE FROM {table} WHERE id = ?", (a["id"],))
                merged += 1
            elif not c:
                try:
                    conn.execute(
                        f"UPDATE {table} SET name = ?, slug = ? WHERE id = ?",
                        (canonical, canonical_slug, a["id"]),
                    )
                    merged += 1
                except sqlite3.IntegrityError:
                    # If a conflicting row exists (by slug/name), merge references into it.
                    conflict = conn.execute(
                        f"SELECT id FROM {table} WHERE (lower(name) = lower(?) OR slug = ?) AND id != ? LIMIT 1",
                        (canonical, canonical_slug, a["id"]),
                    ).fetchone()
                    if conflict:
                        conn.execute(
                            f"UPDATE knife_models_v2 SET {fk_col} = ? WHERE {fk_col} = ?",
                            (conflict["id"], a["id"]),
                        )
                        conn.execute(f"DELETE FROM {table} WHERE id = ?", (a["id"],))
                        merged += 1
                    else:
                        # Last-resort: keep canonical name, assign unique slug.
                        i = 2
                        slug = canonical_slug
                        while conn.execute(
                            f"SELECT 1 FROM {table} WHERE slug = ? AND id != ?",
                            (slug, a["id"]),
                        ).fetchone():
                            slug = f"{canonical_slug}-{i}"
                            i += 1
                        conn.execute(
                            f"UPDATE {table} SET name = ?, slug = ? WHERE id = ?",
                            (canonical, slug, a["id"]),
                        )
                        merged += 1
        return merged

    series_aliases = {
        "nock on archery": "Nock On",
        "knock on archery": "Nock On",
    }
    collaborator_aliases = {
        "bearded butcher": "Bearded Butchers",
        "nock on archery": "Cam Hanes",
        "knock on archery": "Cam Hanes",
        "nock on": "Cam Hanes",
    }
    changes["series_alias_merged"] = _merge_dim_aliases("knife_series", "series_id", series_aliases)
    changes["collaborator_alias_merged"] = _merge_dim_aliases("collaborators", "collaborator_id", collaborator_aliases)

    # Enforce canonical interpretation:
    # - Ultra is a series (not platform variant)
    # - Meat Church / Archery Country are collaborators (not series)
    ultra_series_id = _ensure_dim_local("knife_series", "Ultra")
    if ultra_series_id is not None:
        cur = conn.execute(
            """
            UPDATE knife_models_v2
            SET series_id = ?, updated_at = CURRENT_TIMESTAMP
            WHERE lower(trim(COALESCE(platform_variant, ''))) = 'ultra'
              AND (series_id IS NULL OR series_id != ?)
            """,
            (ultra_series_id, ultra_series_id),
        )
        changes["ultra_platform_to_series"] = cur.rowcount or 0
        cur = conn.execute(
            """
            UPDATE knife_models_v2
            SET platform_variant = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE lower(trim(COALESCE(platform_variant, ''))) = 'ultra'
            """
        )
        changes["ultra_platform_cleared"] = cur.rowcount or 0

    for bad_series, collab_name in (("Meat Church", "Meat Church"), ("Archery Country", "Archery Country"), ("Archery", "Archery Country")):
        bad = conn.execute(
            "SELECT id FROM knife_series WHERE lower(name) = lower(?) LIMIT 1",
            (bad_series,),
        ).fetchone()
        if not bad:
            continue
        collab_id = _ensure_dim_local("collaborators", collab_name)
        cur = conn.execute(
            """
            UPDATE knife_models_v2
            SET collaborator_id = COALESCE(collaborator_id, ?),
                series_id = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE series_id = ?
            """,
            (collab_id, bad["id"]),
        )
        changes["series_collab_reclassified"] += cur.rowcount or 0

    steel_map = {
        "magnacut": "MagnaCut",
        "magna cut": "MagnaCut",
        "aebl": "AEB-L",
        "aeb-l": "AEB-L",
        "440c": "440C",
    }
    finish_map = {
        "stonewashed": "Stonewashed",
        "pvd": "PVD",
        "cerakote": "Cerakote",
        "polished": "Polished",
        "satin": "Satin",
        "black parkerized": "Black Parkerized",
        "working grind": "Working Grind",
        "etched": "Etched",
    }
    blade_color_map = {
        "black": "Black",
        "steel": "Steel",
        "distressed gray": "Distressed Gray",
        "red": "Red",
        "damascus wood grain": "Damascus Wood Grain",
    }
    handle_color_map = {
        "black": "Black",
        "red": "Red",
        "carbon fiber": "Carbon Fiber",
        "desert ironwood": "Desert Ironwood",
    }
    condition_map = {
        "new": "New",
        "like new": "Like New",
        "very good": "Very Good",
        "good": "Good",
        "user": "User",
    }

    model_rows = conn.execute(
        "SELECT id, steel, blade_finish, blade_color, handle_color FROM knife_models_v2"
    ).fetchall()
    for row in model_rows:
        steel = _norm_map(row.get("steel"), steel_map)
        finish = _norm_map(row.get("blade_finish"), finish_map)
        blade_color = _norm_map(row.get("blade_color"), blade_color_map)
        handle_color = _norm_map(row.get("handle_color"), handle_color_map)
        if (steel, finish, blade_color, handle_color) != (
            _collapse(row.get("steel")),
            _collapse(row.get("blade_finish")),
            _collapse(row.get("blade_color")),
            _collapse(row.get("handle_color")),
        ):
            conn.execute(
                """
                UPDATE knife_models_v2
                SET steel = ?, blade_finish = ?, blade_color = ?, handle_color = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (steel, finish, blade_color, handle_color, row["id"]),
            )
            changes["model_attr_rows_updated"] += 1

    inv_rows = conn.execute(
        "SELECT id, steel, blade_finish, blade_color, handle_color, condition FROM inventory_items_v2"
    ).fetchall()
    for row in inv_rows:
        steel = _norm_map(row.get("steel"), steel_map)
        finish = _norm_map(row.get("blade_finish"), finish_map)
        blade_color = _norm_map(row.get("blade_color"), blade_color_map)
        handle_color = _norm_map(row.get("handle_color"), handle_color_map)
        condition = _norm_map(row.get("condition"), condition_map)
        if (steel, finish, blade_color, handle_color, condition) != (
            _collapse(row.get("steel")),
            _collapse(row.get("blade_finish")),
            _collapse(row.get("blade_color")),
            _collapse(row.get("handle_color")),
            _collapse(row.get("condition")),
        ):
            conn.execute(
                """
                UPDATE inventory_items_v2
                SET steel = ?, blade_finish = ?, blade_color = ?, handle_color = ?, condition = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (steel, finish, blade_color, handle_color, condition, row["id"]),
            )
            changes["inventory_attr_rows_updated"] += 1

    return changes


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


def recompute_silhouettes_for_masters_without_hu(conn: sqlite3.Connection) -> int:
    """
    Process masters that have identifier_image_blob but:
    - no identifier_silhouette_hu_json, or
    - degenerate Hu (e.g. [0.77, 12, 12, 12, -12, 12, -12]) that cannot discriminate blades.
    Ensures stored reference images yield usable shape data; clears degenerate Hu.
    """
    rows = conn.execute(
        """
        SELECT id, identifier_image_blob, identifier_silhouette_hu_json
        FROM master_knives
        WHERE identifier_image_blob IS NOT NULL
          AND length(identifier_image_blob) > 0
        """
    ).fetchall()
    updated = 0
    for row in rows:
        blob = row["identifier_image_blob"]
        if not blob:
            continue
        hu_json = (row.get("identifier_silhouette_hu_json") or "").strip()
        needs_recompute = not hu_json
        if hu_json:
            try:
                hu_list = json.loads(hu_json)
                needs_recompute = blade_ai.is_hu_vector_degenerate(hu_list)
            except (json.JSONDecodeError, TypeError):
                needs_recompute = True
        if not needs_recompute:
            continue
        hu_list, _ = blade_ai.extract_blade_hu_from_image_bytes(blob)
        if hu_list:
            conn.execute(
                """
                UPDATE master_knives
                SET identifier_silhouette_hu_json = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (json.dumps(hu_list), row["id"]),
            )
            updated += 1
        else:
            conn.execute(
                """
                UPDATE master_knives
                SET identifier_silhouette_hu_json = NULL, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (row["id"],),
            )
            updated += 1
    return updated


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
        recompute_silhouettes_for_masters_without_hu(conn)
        normalize_master_category_data(conn)

        # Normalized v2 schema + v2-only support tables
        normalized_model.ensure_normalized_schema(conn)
        ensure_v2_exclusive_schema(conn)
        ensure_reporting_schema(conn)
        ensure_gap_reconciliation_schema(conn)
        migrate_legacy_media_to_v2(conn)
        backfill_v2_model_identity(conn)
        normalize_v2_additional_fields(conn)


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


@app.get("/")
def root():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/identify")
def identify_page():
    return FileResponse(STATIC_DIR / "identify.html")


@app.get("/master")
def master_page():
    return FileResponse(STATIC_DIR / "master.html")


@app.get("/order-inventory-gaps")
def order_inventory_gaps_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "gap_reconciliation.html")


class OrderInvGapOverrideIn(BaseModel):
    bucket_key: str = Field(min_length=1, max_length=500)
    match_mode: str = Field(default="full")
    cleared: bool = False
    resolution_code: Optional[str] = Field(default=None, max_length=80)
    note: Optional[str] = Field(default=None, max_length=4000)
    linked_inventory_item_ids: Optional[str] = Field(
        default=None, max_length=500, description="Comma-separated inventory_items_v2.id"
    )

    @field_validator("match_mode")
    @classmethod
    def _gap_override_match_mode(cls, v: Any) -> str:
        x = (v or "full").strip()
        if x not in ("full", "name_handle"):
            raise ValueError("match_mode must be 'full' or 'name_handle'")
        return x


class OrderInvGapLineLinkIn(BaseModel):
    order_number: str = Field(min_length=1, max_length=40)
    order_date: Optional[str] = Field(default=None, max_length=32)
    line_title: str = Field(min_length=1, max_length=500)
    matched_catalog_name: Optional[str] = Field(default=None, max_length=300)
    inventory_item_id: int = Field(ge=1)
    note: Optional[str] = Field(default=None, max_length=2000)


def _gap_iso_now() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _order_inventory_gaps_payload(mode: str) -> dict[str, Any]:
    raw_mode = (mode or "full").strip().lower()
    name_handle = raw_mode in ("name_handle", "name-handle", "nh", "handle")
    orders_path = BASE_DIR / "data/mkc_email_orders_knives.csv"
    if not orders_path.is_file():
        raise HTTPException(
            status_code=503,
            detail="Missing data/mkc_email_orders_knives.csv — run the email filter / enrich pipeline first.",
        )
    result = gap_analysis_core.compute_gap_analysis(
        orders_path, DB_PATH, name_handle=name_handle, strict=False
    )
    try:
        mtime = orders_path.stat().st_mtime
        orders_csv_mtime_iso = datetime.utcfromtimestamp(mtime).replace(microsecond=0).isoformat() + "Z"
    except OSError:
        orders_csv_mtime_iso = ""
    mm = "name_handle" if name_handle else "full"
    with get_conn() as conn:
        ensure_gap_reconciliation_schema(conn)
        ovr_rows = conn.execute(
            "SELECT bucket_key, match_mode, cleared, resolution_code, note, linked_inventory_item_ids, updated_at "
            "FROM order_inv_gap_bucket_overrides WHERE match_mode = ?",
            (mm,),
        ).fetchall()
        overrides_by_key = {str(r["bucket_key"]): dict(r) for r in ovr_rows}
        links = conn.execute(
            """
            SELECT l.id, l.order_number, l.order_date, l.line_title, l.matched_catalog_name,
                   l.inventory_item_id, l.note, l.created_at,
                   km.official_name AS knife_name,
                   COALESCE(i.handle_color, km.handle_color) AS inv_handle,
                   i.quantity AS inv_qty
            FROM order_inv_gap_line_links l
            JOIN inventory_items_v2 i ON i.id = l.inventory_item_id
            LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
            ORDER BY l.id DESC
            """
        ).fetchall()
    merged_rows: list[dict[str, Any]] = []
    open_count = 0
    for r in result["rows"]:
        g = int(r["gap_ordered_minus_inventory"])
        o = overrides_by_key.get(r["bucket_key"])
        cleared = bool(o and int(o.get("cleared") or 0))
        needs = g != 0 and not cleared
        if needs:
            open_count += 1
        merged_rows.append({**r, "override": o, "needs_attention": needs})
    return {
        "match_mode": mm,
        "stats": result["stats"],
        "model_gaps": result["model_gaps"],
        "skipped_bundle": result["skipped_bundle"],
        "skipped_unresolved": result["skipped_unresolved"],
        "vip_inventory_excluded": result["vip_inventory_excluded"],
        "orders_path": result["orders_path"],
        "db_path": result["db_path"],
        "orders_csv_mtime_iso": orders_csv_mtime_iso,
        "rows": merged_rows,
        "line_links": links,
        "open_discrepancy_count": open_count,
    }


@app.get("/api/order-inventory-gaps")
def api_order_inventory_gaps(mode: str = "full") -> dict[str, Any]:
    """Compare email knife orders CSV to inventory; merge saved overrides and manual links."""
    return _order_inventory_gaps_payload(mode)


@app.post("/api/order-inventory-gaps/rebuild-order-pipeline")
def api_order_inventory_gaps_rebuild_order_pipeline() -> dict[str, Any]:
    """
    Run knife-order pipeline on the server: filter → enrich colors → normalize to v2 → write gap CSVs.
    Refreshes data/mkc_email_orders_knives.csv; UI should call GET /api/order-inventory-gaps after.
    """
    root = BASE_DIR
    py = sys.executable
    steps: list[list[str]] = [
        [py, str(root / "tools" / "filter_email_orders_to_catalog_knives.py")],
        [py, str(root / "tools" / "enrich_order_line_colors.py")],
        [py, str(root / "tools" / "normalize_order_colors_to_v2.py")],
        [py, str(root / "tools" / "gap_analysis_orders_vs_inventory.py")],
        [py, str(root / "tools" / "gap_analysis_orders_vs_inventory.py"), "--name-handle"],
    ]
    log_parts: list[str] = []
    for cmd in steps:
        try:
            proc = subprocess.run(
                cmd,
                cwd=str(root),
                capture_output=True,
                text=True,
                timeout=600,
            )
        except subprocess.TimeoutExpired:
            raise HTTPException(
                status_code=504,
                detail=f"Timed out running: {' '.join(cmd)}",
            )
        out = ((proc.stdout or "") + (proc.stderr or "")).strip()
        log_parts.append(f"$ {' '.join(cmd)}\n{out if out else '(no output)'}")
        if proc.returncode != 0:
            raise HTTPException(
                status_code=500,
                detail="Pipeline failed on: "
                + " ".join(cmd)
                + "\n\n"
                + "\n\n".join(log_parts)[-12000:],
            )
    return {"ok": True, "log": "\n\n".join(log_parts)}


@app.post("/api/order-inventory-gaps/override")
def api_order_inventory_gaps_override(body: OrderInvGapOverrideIn) -> dict[str, Any]:
    now = _gap_iso_now()
    with get_conn() as conn:
        ensure_gap_reconciliation_schema(conn)
        conn.execute(
            """
            INSERT INTO order_inv_gap_bucket_overrides
                (bucket_key, match_mode, cleared, resolution_code, note, linked_inventory_item_ids, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(bucket_key, match_mode) DO UPDATE SET
                cleared = excluded.cleared,
                resolution_code = excluded.resolution_code,
                note = excluded.note,
                linked_inventory_item_ids = excluded.linked_inventory_item_ids,
                updated_at = excluded.updated_at
            """,
            (
                body.bucket_key.strip(),
                body.match_mode,
                1 if body.cleared else 0,
                (body.resolution_code or "").strip() or None,
                (body.note or "").strip() or None,
                (body.linked_inventory_item_ids or "").strip() or None,
                now,
            ),
        )
    return {"ok": True, "updated_at": now}


@app.post("/api/order-inventory-gaps/link")
def api_order_inventory_gaps_link(body: OrderInvGapLineLinkIn) -> dict[str, Any]:
    with get_conn() as conn:
        ensure_gap_reconciliation_schema(conn)
        exists = conn.execute(
            "SELECT 1 FROM inventory_items_v2 WHERE id = ?",
            (body.inventory_item_id,),
        ).fetchone()
        if not exists:
            raise HTTPException(status_code=400, detail="inventory_item_id not found")
        cur = conn.execute(
            """
            INSERT INTO order_inv_gap_line_links
                (order_number, order_date, line_title, matched_catalog_name, inventory_item_id, note)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                body.order_number.strip(),
                (body.order_date or "").strip() or None,
                body.line_title.strip(),
                (body.matched_catalog_name or "").strip() or None,
                body.inventory_item_id,
                (body.note or "").strip() or None,
            ),
        )
        link_id = int(cur.lastrowid)
    return {"ok": True, "id": link_id}


@app.delete("/api/order-inventory-gaps/link/{link_id}")
def api_order_inventory_gaps_link_delete(link_id: int) -> dict[str, Any]:
    with get_conn() as conn:
        ensure_gap_reconciliation_schema(conn)
        cur = conn.execute("DELETE FROM order_inv_gap_line_links WHERE id = ?", (link_id,))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Link not found")
    return {"ok": True}


@app.get("/api/admin/silhouettes/status")
def admin_silhouettes_status():
    """
    Report Hu status for all masters. Use to verify what is actually in the DB (e.g. Speedgoat Ultra)
    and which masters have images but missing or degenerate Hu.
    """
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, name,
                   (identifier_image_blob IS NOT NULL AND length(identifier_image_blob) > 0) AS has_image,
                   identifier_silhouette_hu_json
            FROM master_knives
            ORDER BY name COLLATE NOCASE
            """
        ).fetchall()
    result: list[dict[str, Any]] = []
    missing_hu: list[dict[str, Any]] = []
    for r in rows:
        hu_json = (r.get("identifier_silhouette_hu_json") or "").strip()
        has_hu = bool(hu_json)
        degenerate = False
        if has_hu:
            try:
                hu_list = json.loads(hu_json)
                degenerate = blade_ai.is_hu_vector_degenerate(hu_list)
            except (json.JSONDecodeError, TypeError):
                degenerate = True
        entry = {
            "id": r["id"],
            "name": r["name"],
            "has_image": bool(r["has_image"]),
            "has_hu": has_hu and not degenerate,
            "hu_json": hu_json if hu_json else None,
        }
        if degenerate and hu_json:
            entry["hu_degenerate"] = True
        result.append(entry)
        if r["has_image"] and (not has_hu or degenerate):
            missing_hu.append({"id": r["id"], "name": r["name"], "reason": "degenerate" if degenerate and has_hu else "missing"})
    return {
        "total": len(result),
        "with_image": sum(1 for e in result if e["has_image"]),
        "with_valid_hu": sum(1 for e in result if e["has_hu"]),
        "missing_hu": missing_hu,
        "masters": result,
    }


@app.post("/api/admin/silhouettes/recompute")
def admin_silhouettes_recompute():
    """Re-run Hu extraction for masters that have an image but missing or degenerate Hu."""
    with get_conn() as conn:
        updated = recompute_silhouettes_for_masters_without_hu(conn)
    return {"updated": updated, "message": f"Processed {updated} master(s)."}


def recompute_distinguishing_features(
    knife_ids: Optional[list[int]] = None,
    missing_only: bool = False,
    model: str = OLLAMA_VISION_MODEL,
) -> dict[str, Any]:
    """
    Run vision LLM to extract distinguishing features for masters with images.
    Fetches rows first, releases DB, runs LLM calls (no DB held), then writes back.
    Returns {"updated": int, "failed": list[dict], "skipped": int}.
    """
    model_ok, err = blade_ai.check_ollama_model(model)
    if not model_ok:
        return {"updated": 0, "failed": [{"reason": err}], "skipped": 0}
    if knife_ids is not None and not knife_ids:
        return {"updated": 0, "failed": [], "skipped": 0}

    with get_conn() as conn:
        if knife_ids is not None:
            placeholders = ",".join("?" * len(knife_ids))
            rows = conn.execute(
                """
                SELECT id, name, identifier_image_blob
                FROM master_knives
                WHERE id IN (""" + placeholders + """)
                  AND identifier_image_blob IS NOT NULL
                  AND length(identifier_image_blob) > 0
                """,
                knife_ids,
            ).fetchall()
        else:
            cond = "AND (identifier_distinguishing_features IS NULL OR trim(identifier_distinguishing_features) = '')" if missing_only else ""
            rows = conn.execute(
                f"""
                SELECT id, name, identifier_image_blob
                FROM master_knives
                WHERE identifier_image_blob IS NOT NULL
                  AND length(identifier_image_blob) > 0
                {cond}
                ORDER BY name COLLATE NOCASE
                """,
            ).fetchall()
    # Connection released; run LLM calls without holding DB
    to_update: list[tuple[str, int]] = []
    failed: list[dict[str, Any]] = []
    total = len(rows)
    for i, r in enumerate(rows, 1):
        blob = r["identifier_image_blob"]
        if not blob:
            continue
        _app_logger.info("[dist-features] Processing %s/%s: %s...", i, total, r["name"])
        img_b64 = base64.standard_b64encode(blob).decode("ascii")
        features, feat_err = blade_ai.extract_distinguishing_features_from_image(model, img_b64)
        if feat_err:
            _app_logger.warning("[dist-features] FAILED %s: %s", r["name"], feat_err)
            failed.append({"id": r["id"], "name": r["name"], "reason": feat_err})
            continue
        if features:
            preview = (features[:60] + "…") if len(features) > 60 else features
            _app_logger.info("[dist-features] OK %s: %s", r["name"], preview)
            to_update.append((features, r["id"]))

    # Write updates in short transactions
    with get_conn() as conn:
        for features, kid in to_update:
            conn.execute(
                """
                UPDATE master_knives
                SET identifier_distinguishing_features = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (features, kid),
            )

    return {
        "updated": len(to_update),
        "failed": failed,
        "skipped": len(rows) - len(to_update) - len(failed),
    }


@app.get("/api/admin/distinguishing-features/status")
def admin_distinguishing_features_status():
    """Report which masters have images and distinguishing features."""
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, name,
                   (identifier_image_blob IS NOT NULL AND length(identifier_image_blob) > 0) AS has_image,
                   identifier_distinguishing_features
            FROM master_knives
            ORDER BY name COLLATE NOCASE
            """
        ).fetchall()
    result: list[dict[str, Any]] = []
    missing: list[dict[str, Any]] = []
    for r in rows:
        dist = (r.get("identifier_distinguishing_features") or "").strip()
        has_dist = bool(dist)
        entry = {"id": r["id"], "name": r["name"], "has_image": bool(r["has_image"]), "has_features": has_dist}
        if dist:
            entry["features"] = dist[:80] + ("..." if len(dist) > 80 else "")
        result.append(entry)
        if r["has_image"] and not has_dist:
            missing.append({"id": r["id"], "name": r["name"]})
    return {
        "total": len(result),
        "with_image": sum(1 for e in result if e["has_image"]),
        "with_features": sum(1 for e in result if e["has_features"]),
        "missing": missing,
        "masters": result,
    }


class DistinguishingFeaturesRecomputeBody(BaseModel):
    knife_id: Optional[int] = None
    knife_ids: Optional[list[int]] = None
    missing_only: bool = False
    model: Optional[str] = None


@app.post("/api/admin/distinguishing-features/recompute")
def admin_distinguishing_features_recompute(body: DistinguishingFeaturesRecomputeBody):
    """
    Re-run vision LLM to extract distinguishing features.
    - knife_id: single master
    - knife_ids: selected masters
    - missing_only: only those with image but no features (ignored if knife_id/knife_ids given)
    """
    model = (body.model or "").strip() or OLLAMA_VISION_MODEL
    if body.knife_id is not None:
        ids = [body.knife_id]
    elif body.knife_ids:
        ids = body.knife_ids
    else:
        ids = None
    result = recompute_distinguishing_features(
        knife_ids=ids, missing_only=body.missing_only, model=model
    )
    return {
        "updated": result["updated"],
        "failed": result["failed"],
        "skipped": result["skipped"],
        "message": f"Updated {result['updated']} master(s). {len(result['failed'])} failed." if result["failed"] else f"Updated {result['updated']} master(s).",
    }


MASTER_CSV_COLUMNS = [
    "id",
    "name",
    "canonical_slug",
    "family",
    "record_type",
    "category",
    "catalog_line",
    "catalog_status",
    "confidence",
    "evidence_summary",
    "collector_notes",
    "default_blade_length",
    "default_steel",
    "default_blade_finish",
    "default_blade_color",
    "default_handle_color",
    "is_collab",
    "collaboration_name",
    "status",
    "notes",
    "blade_profile",
    "has_ring",
    "is_filleting_knife",
    "is_hatchet",
    "is_kitchen",
    "is_tactical",
    "identifier_keywords",
    "identifier_distinguishing_features",
    "identifier_product_url",
    "identifier_image_mime",
    "identifier_silhouette_hu_json",
    "msrp",
    "first_release_date",
    "last_seen_date",
    "is_discontinued",
    "is_current_catalog",
    "blade_shape",
    "tip_style",
    "grind_style",
    "size_class",
    "primary_use_case",
    "spine_profile",
    "is_fillet",
    "default_product_url",
    "primary_image_url",
]


def _parse_csv_bool(value: Optional[str]) -> int:
    if value is None or str(value).strip() == "":
        return 0
    return 1 if str(value).strip().lower() in ("1", "true", "yes", "y") else 0


def _parse_csv_optional_float(value: Optional[str]) -> Optional[float]:
    if value is None or str(value).strip() == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _parse_csv_pipe_option_ids(conn: sqlite3.Connection, value: Optional[str], table: str, name_col: str = "name") -> list[int]:
    """Parse pipe-delimited option names into IDs. e.g. 'Orange|Black' -> [1, 2]."""
    if not value or not str(value).strip():
        return []
    names = [n.strip() for n in str(value).split("|") if n.strip()]
    if not names:
        return []
    placeholders = ",".join("?" * len(names))
    rows = conn.execute(
        f"SELECT id FROM {table} WHERE {name_col} IN ({placeholders})", names
    ).fetchall()
    return [r["id"] for r in rows]


@app.get("/api/summary")
def get_summary():
    with get_conn() as conn:
        summary = conn.execute(
            """
            SELECT
                COUNT(*) AS inventory_rows,
                COALESCE(SUM(quantity), 0) AS total_quantity,
                COALESCE(SUM(COALESCE(purchase_price, 0) * quantity), 0) AS total_spend,
                COALESCE(SUM(COALESCE(estimated_value, 0) * quantity), 0) AS total_estimated_value
            FROM inventory_items
            """
        ).fetchone()
        summary["master_count"] = conn.execute(
            "SELECT COUNT(*) AS c FROM master_knives WHERE status != 'archived'"
        ).fetchone()["c"]
        summary["by_family"] = conn.execute(
            """
            SELECT
                COALESCE(NULLIF(TRIM(m.family), ''), 'Uncategorized') AS family,
                COUNT(*) AS inventory_rows,
                COALESCE(SUM(i.quantity), 0) AS total_quantity
            FROM inventory_items i
            JOIN master_knives m ON m.id = i.master_knife_id
            GROUP BY family
            ORDER BY total_quantity DESC, family COLLATE NOCASE
            """
        ).fetchall()
        return summary


@app.get("/api/master-knives")
def list_master_knives(
    active_only: bool = False,
    family: Optional[str] = None,
    use_case: Optional[str] = None,
    blade_shape: Optional[str] = None,
    is_current_catalog: Optional[bool] = None,
    is_discontinued: Optional[bool] = None,
    is_collab: Optional[bool] = None,
    record_type: Optional[str] = None,
):
    """List master knives with optional filters for catalog, lifecycle, and collab."""
    with get_conn() as conn:
        conditions = []
        params: list[Any] = []

        if active_only:
            conditions.append("status != 'archived'")

        if is_current_catalog is True:
            conditions.append("(is_current_catalog = 1 OR is_current_catalog IS NULL)")
        elif is_current_catalog is False:
            conditions.append("is_current_catalog = 0")

        if is_discontinued is True:
            conditions.append("is_discontinued = 1")
        elif is_discontinued is False:
            conditions.append("(is_discontinued = 0 OR is_discontinued IS NULL)")

        if is_collab is True:
            conditions.append("is_collab = 1")
        elif is_collab is False:
            conditions.append("is_collab = 0")

        if record_type and record_type.strip():
            conditions.append("(record_type LIKE ? OR record_type = ?)")
            q = f"%{record_type.strip()}%"
            params.extend([q, record_type.strip()])

        if family and family.strip():
            conditions.append("(family LIKE ? OR category LIKE ?)")
            q = f"%{family.strip()}%"
            params.extend([q, q])

        if use_case and use_case.strip():
            conditions.append("(primary_use_case LIKE ? OR category LIKE ?)")
            q = f"%{use_case.strip()}%"
            params.extend([q, q])

        if blade_shape and blade_shape.strip():
            conditions.append("(blade_shape LIKE ? OR blade_profile LIKE ?)")
            q = f"%{blade_shape.strip()}%"
            params.extend([q, q])

        where_sql = " AND ".join(conditions) if conditions else "1=1"

        rows = conn.execute(
            f"""
            SELECT {MASTER_KNIVES_PUBLIC_COLUMNS}
            FROM master_knives
            WHERE {where_sql}
            ORDER BY name COLLATE NOCASE
            """,
            params,
        ).fetchall()
        return rows


@app.get("/api/master-knives/export.csv")
def export_master_csv():
    with get_conn() as conn:
        rows = conn.execute(
            f"""
            SELECT {MASTER_KNIVES_PUBLIC_COLUMNS}
            FROM master_knives
            ORDER BY name COLLATE NOCASE
            """
        ).fetchall()
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=MASTER_CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            flat: dict[str, Any] = {}
            for key in MASTER_CSV_COLUMNS:
                val = row.get(key)
                if key.startswith("is_") or key in ("has_ring",):
                    flat[key] = "1" if val else "0"
                elif val is None:
                    flat[key] = ""
                else:
                    flat[key] = val
            writer.writerow(flat)
    data = buffer.getvalue()
    return Response(
        content=data.encode("utf-8"),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="mkc_master_knives.csv"'},
    )


def _csv_cell(row: dict[str, str], *keys: str) -> str:
    for key in keys:
        for rk, rv in row.items():
            if (rk or "").strip().lower() == key.lower():
                return (rv or "").strip()
    return ""


def import_master_csv_from_path(path: Path) -> dict[str, Any]:
    """
    Import master knives from a CSV file. Upserts by name.
    Returns {"inserted": N, "updated": N, "message": "..."}.
    """
    try:
        text = path.read_text(encoding="utf-8-sig")
    except UnicodeDecodeError as e:
        raise ValueError("File must be UTF-8 encoded CSV.") from e
    return _run_master_csv_import(text)


def _run_master_csv_import(csv_text: str) -> dict[str, Any]:
    """Core import logic; expects UTF-8 CSV text."""
    reader = csv.DictReader(io.StringIO(csv_text))
    fnames = {(f or "").strip().lstrip("\ufeff") for f in (reader.fieldnames or [])}
    is_knife_master = "Model / family" in fnames
    if not reader.fieldnames or (not is_knife_master and "name" not in fnames):
        raise HTTPException(
            status_code=400,
            detail="CSV must include a name column, or Knife Master columns (e.g. Model / family).",
        )
    csv_has_catalog_line = any(
        (f or "").strip().lower() in ("catalog_line", "catalog line") for f in fnames
    )
    inserted = 0
    updated = 0
    with get_conn() as conn:
        for raw in reader:
            row = {(k or "").strip(): (v if v is None else str(v).strip()) for k, v in raw.items()}
            if is_knife_master:
                name = _csv_cell(row, "Model / family")
            else:
                name = _csv_cell(row, "name")
            if not name:
                continue

            catalog_line_val = None
            if is_knife_master:
                record_type = _csv_cell(row, "Record type") or None
                category = normalize_category_value(_csv_cell(row, "Category") or None)
                catalog_status = _csv_cell(row, "Status") or None
                confidence = _csv_cell(row, "Confidence") or None
                evidence_summary = _csv_cell(row, "Evidence summary") or None
                collector_notes = _csv_cell(row, "Collector notes") or None
                if not record_type:
                    record_type = None
                if not category:
                    category = None
                # optional app-export columns may be present on a merged sheet
                family = _csv_cell(row, "family") or None
                default_blade_length = _parse_csv_optional_float(_csv_cell(row, "default_blade_length"))
                default_steel = _csv_cell(row, "default_steel") or None
                default_blade_finish = _csv_cell(row, "default_blade_finish") or None
                default_blade_color = _csv_cell(row, "default_blade_color") or None
                default_handle_color = _csv_cell(row, "default_handle_color") or None
                _cr = _csv_cell(row, "is_collab")
                is_collab = _parse_csv_bool(_cr) if _cr else None
                collaboration_name = _csv_cell(row, "collaboration_name") or None
                # Operational active/archived — only when a lowercase ``status`` column exists (app export).
                # Knife Master ``Status`` maps to ``catalog_status``, not this field.
                op_status = "active"
                if "status" in row:
                    op_status = (row.get("status") or "active").strip() or "active"
                notes = _csv_cell(row, "notes") or None
                blade_profile = _csv_cell(row, "blade_profile") or None
                _hr = _csv_cell(row, "has_ring")
                has_ring = _parse_csv_bool(_hr) if _hr else None
                _ff = _csv_cell(row, "is_filleting_knife")
                is_filleting = _parse_csv_bool(_ff) if _ff else None
                _hx = _csv_cell(row, "is_hatchet")
                is_hatchet = _parse_csv_bool(_hx) if _hx else None
                _kit = _csv_cell(row, "is_kitchen")
                is_kitchen = _parse_csv_bool(_kit) if _kit else None
                _tac = _csv_cell(row, "is_tactical")
                is_tactical = _parse_csv_bool(_tac) if _tac else None
                identifier_keywords = _csv_cell(row, "identifier_keywords") or None
                identifier_distinguishing_features = _csv_cell(row, "identifier_distinguishing_features") or None
                identifier_product_url = _csv_cell(row, "identifier_product_url") or None
                identifier_image_mime = _csv_cell(row, "identifier_image_mime") or None
                identifier_silhouette_hu_json = _csv_cell(row, "identifier_silhouette_hu_json") or None
                if csv_has_catalog_line:
                    cl_raw = _csv_cell(row, "catalog_line") or _csv_cell(row, "Catalog line")
                    catalog_line_val = (
                        normalize_master_catalog_line_input(cl_raw, strict=False) if cl_raw else None
                    )
            else:
                record_type = _csv_cell(row, "record_type") or None
                catalog_status = _csv_cell(row, "catalog_status") or None
                confidence = _csv_cell(row, "confidence") or None
                evidence_summary = _csv_cell(row, "evidence_summary") or None
                collector_notes = _csv_cell(row, "collector_notes") or None
                family = _csv_cell(row, "family") or None
                category = normalize_category_value(_csv_cell(row, "category") or None)
                default_blade_length = _parse_csv_optional_float(_csv_cell(row, "default_blade_length"))
                default_steel = _csv_cell(row, "default_steel") or None
                default_blade_finish = _csv_cell(row, "default_blade_finish") or None
                default_blade_color = _csv_cell(row, "default_blade_color") or None
                default_handle_color = _csv_cell(row, "default_handle_color") or None
                is_collab = _parse_csv_bool(_csv_cell(row, "is_collab"))
                collaboration_name = _csv_cell(row, "collaboration_name") or None
                op_status = (_csv_cell(row, "status") or "active").strip() or "active"
                notes = _csv_cell(row, "notes") or None
                blade_profile = _csv_cell(row, "blade_profile") or None
                has_ring = _parse_csv_bool(_csv_cell(row, "has_ring"))
                is_filleting = _parse_csv_bool(_csv_cell(row, "is_filleting_knife"))
                is_hatchet = _parse_csv_bool(_csv_cell(row, "is_hatchet"))
                is_kitchen = _parse_csv_bool(_csv_cell(row, "is_kitchen"))
                is_tactical = _parse_csv_bool(_csv_cell(row, "is_tactical"))
                identifier_keywords = _csv_cell(row, "identifier_keywords") or None
                identifier_distinguishing_features = _csv_cell(row, "identifier_distinguishing_features") or None
                identifier_product_url = _csv_cell(row, "identifier_product_url") or None
                identifier_image_mime = _csv_cell(row, "identifier_image_mime") or None
                identifier_silhouette_hu_json = _csv_cell(row, "identifier_silhouette_hu_json") or None
                if csv_has_catalog_line:
                    cl_raw = _csv_cell(row, "catalog_line")
                    catalog_line_val = (
                        normalize_master_catalog_line_input(cl_raw, strict=False) if cl_raw else None
                    )
                canonical_slug = _csv_cell(row, "canonical_slug") or None
                msrp = _parse_csv_optional_float(_csv_cell(row, "msrp"))
                first_release_date = _csv_cell(row, "first_release_date") or None
                last_seen_date = _csv_cell(row, "last_seen_date") or None
                _disc = _csv_cell(row, "is_discontinued")
                is_discontinued = _parse_csv_bool(_disc) if _disc else None
                _curr = _csv_cell(row, "is_current_catalog")
                is_current_catalog = _parse_csv_bool(_curr) if _curr else None
                blade_shape = _csv_cell(row, "blade_shape") or blade_profile
                tip_style = _csv_cell(row, "tip_style") or None
                grind_style = _csv_cell(row, "grind_style") or None
                size_class = _csv_cell(row, "size_class") or None
                primary_use_case = _csv_cell(row, "primary_use_case") or category
                spine_profile = _csv_cell(row, "spine_profile") or None
                _fil = _csv_cell(row, "is_fillet")
                is_fillet = _parse_csv_bool(_fil) if _fil else is_filleting
                default_product_url = _csv_cell(row, "default_product_url") or None
                primary_image_url = _csv_cell(row, "primary_image_url") or None
            existing = conn.execute(
                "SELECT id FROM master_knives WHERE name = ?", (name,)
            ).fetchone()

            if is_knife_master and existing:
                if csv_has_catalog_line:
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
                            catalog_line_val,
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
                continue

            if is_knife_master and not existing:
                hr, ff, hx, kit, tac, col = infer_identifier_flags(name, category, record_type)
                if is_collab is not None:
                    col = is_collab
                if has_ring is not None:
                    hr = has_ring
                if is_filleting is not None:
                    ff = is_filleting
                if is_hatchet is not None:
                    hx = is_hatchet
                if is_kitchen is not None:
                    kit = is_kitchen
                if is_tactical is not None:
                    tac = is_tactical
                conn.execute(
                    """
                    INSERT INTO master_knives
                    (name, family, record_type, category, catalog_line, catalog_status, confidence, evidence_summary,
                     collector_notes, default_blade_length, default_steel, default_blade_finish, default_blade_color,
                     default_handle_color, is_collab, collaboration_name, status, notes, blade_profile,
                     has_ring, is_filleting_knife, is_hatchet, is_kitchen, is_tactical, identifier_keywords,
                     identifier_distinguishing_features, identifier_product_url, identifier_image_mime, identifier_silhouette_hu_json, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (
                        name,
                        family,
                        record_type,
                        category,
                        catalog_line_val,
                        catalog_status,
                        confidence,
                        evidence_summary,
                        collector_notes,
                        default_blade_length,
                        default_steel,
                        default_blade_finish,
                        default_blade_color,
                        default_handle_color,
                        int(col),
                        collaboration_name,
                        op_status,
                        notes,
                        blade_profile,
                        int(hr),
                        int(ff),
                        int(hx),
                        int(kit),
                        int(tac),
                        identifier_keywords,
                        identifier_distinguishing_features,
                        identifier_product_url,
                        identifier_image_mime,
                        identifier_silhouette_hu_json,
                    ),
                )
                inserted += 1
                continue

            v2_extras = (
                canonical_slug,
                msrp,
                first_release_date,
                last_seen_date,
                1 if is_discontinued else 0,
                0 if is_current_catalog is False else 1,
                blade_shape,
                tip_style,
                grind_style,
                size_class,
                primary_use_case,
                spine_profile,
                1 if is_fillet else 0,
                default_product_url,
                primary_image_url,
            )
            values = (
                name,
                family,
                default_blade_length,
                default_steel,
                default_blade_finish,
                default_blade_color,
                default_handle_color,
                int(is_collab),
                collaboration_name,
                op_status,
                notes,
                record_type,
                catalog_status,
                confidence,
                evidence_summary,
                collector_notes,
                category,
                catalog_line_val,
                blade_profile,
                int(has_ring),
                int(is_filleting),
                int(is_hatchet),
                int(is_kitchen),
                int(is_tactical),
                identifier_keywords,
                identifier_distinguishing_features,
                identifier_product_url,
                identifier_image_mime,
                identifier_silhouette_hu_json,
            ) + v2_extras
            if existing:
                conn.execute(
                    """
                    UPDATE master_knives
                    SET family = ?, default_blade_length = ?, default_steel = ?, default_blade_finish = ?,
                        default_blade_color = ?, default_handle_color = ?, is_collab = ?, collaboration_name = ?,
                        status = ?, notes = ?, record_type = ?, catalog_status = ?, confidence = ?,
                        evidence_summary = ?, collector_notes = ?, category = ?, catalog_line = ?, blade_profile = ?,
                        has_ring = ?, is_filleting_knife = ?, is_hatchet = ?, is_kitchen = ?,
                        is_tactical = ?, identifier_keywords = ?, identifier_distinguishing_features = ?,
                        identifier_product_url = ?, identifier_image_mime = ?, identifier_silhouette_hu_json = ?,
                        canonical_slug = ?, msrp = ?, first_release_date = ?, last_seen_date = ?,
                        is_discontinued = ?, is_current_catalog = ?, blade_shape = ?, tip_style = ?, grind_style = ?,
                        size_class = ?, primary_use_case = ?, spine_profile = ?, is_fillet = ?,
                        default_product_url = ?, primary_image_url = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    values[1:] + (existing["id"],),
                )
                updated += 1
            else:
                cur = conn.execute(
                    """
                    INSERT INTO master_knives
                    (name, family, default_blade_length, default_steel, default_blade_finish, default_blade_color,
                     default_handle_color, is_collab, collaboration_name, status, notes, record_type,
                     catalog_status, confidence, evidence_summary, collector_notes, category, catalog_line, blade_profile,
                     has_ring, is_filleting_knife, is_hatchet, is_kitchen, is_tactical, identifier_keywords,
                     identifier_distinguishing_features, identifier_product_url, identifier_image_mime, identifier_silhouette_hu_json,
                     canonical_slug, msrp, first_release_date, last_seen_date, is_discontinued, is_current_catalog,
                     blade_shape, tip_style, grind_style, size_class, primary_use_case, spine_profile, is_fillet,
                     default_product_url, primary_image_url, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    values,
                )
                new_id = cur.lastrowid
                inserted += 1
    return {"inserted": inserted, "updated": updated, "message": "Import complete."}


@app.post("/api/master-knives/import.csv")
async def import_master_csv(file: UploadFile = File(...)):
    raw = await file.read()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="File must be UTF-8 encoded CSV.")
    return _run_master_csv_import(text)


@app.get("/api/master-knives/{knife_id}/identifier-image")
def get_master_identifier_image(knife_id: int):
    """Serves stored reference bytes from the one-time seed (no hotlinking at runtime)."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT identifier_image_blob, identifier_image_mime FROM master_knives WHERE id = ?",
            (knife_id,),
        ).fetchone()
        if not row or not row.get("identifier_image_blob"):
            raise HTTPException(status_code=404, detail="No stored reference image for this model.")
        mime = (row.get("identifier_image_mime") or "image/jpeg").strip() or "image/jpeg"
        return Response(content=row["identifier_image_blob"], media_type=mime)


@app.post("/api/master-knives/{knife_id}/identifier-image")
async def upload_master_identifier_image(
    knife_id: int,
    file: UploadFile = File(...),
    model: Optional[str] = Form(None),
):
    """
    Store an uploaded reference photo, compute the Hu silhouette vector, and extract
    distinguishing features via vision LLM (lanyard hole, handle type, ring guard, etc.).
    """
    raw = await file.read()
    if len(raw) > 15 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="Image larger than 15MB")
    mime = ((file.content_type or "").split(";")[0].strip().lower() or "image/jpeg")
    if not mime.startswith("image/"):
        mime = "image/jpeg"
    if mime == "image/jpg":
        mime = "image/jpeg"
    hu_list, hu_err = blade_ai.extract_blade_hu_from_image_bytes(raw)
    hu_json = json.dumps(hu_list) if hu_list else None

    dist_features: Optional[str] = None
    dist_error: Optional[str] = None
    vision_model = (model or "").strip() or OLLAMA_VISION_MODEL
    if vision_model:
        model_ok, _ = blade_ai.check_ollama_model(vision_model)
        if model_ok:
            img_b64 = base64.standard_b64encode(raw).decode("ascii")
            dist_features, dist_error = blade_ai.extract_distinguishing_features_from_image(vision_model, img_b64)

    with get_conn() as conn:
        exists = conn.execute("SELECT id FROM master_knives WHERE id = ?", (knife_id,)).fetchone()
        if not exists:
            raise HTTPException(status_code=404, detail="Master knife not found.")
        conn.execute(
            """
            UPDATE master_knives
            SET identifier_image_blob = ?, identifier_image_mime = ?, identifier_silhouette_hu_json = ?,
                identifier_distinguishing_features = COALESCE(?, identifier_distinguishing_features),
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (raw, mime, hu_json, dist_features, knife_id),
        )
    return {
        "message": "Reference image stored.",
        "has_silhouette": hu_list is not None,
        "silhouette_error": hu_err,
        "distinguishing_features": dist_features,
        "distinguishing_features_error": dist_error,
    }


@app.delete("/api/master-knives/{knife_id}/identifier-image")
def delete_master_identifier_image(knife_id: int):
    """Remove stored reference bytes and silhouette data (URLs in other fields are unchanged)."""
    with get_conn() as conn:
        cur = conn.execute(
            """
            UPDATE master_knives
            SET identifier_image_blob = NULL, identifier_image_mime = NULL,
                identifier_silhouette_hu_json = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (knife_id,),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Master knife not found.")
    return {"message": "Reference image cleared."}


@app.get("/api/master-knives/{knife_id}")
def get_master_knife(knife_id: int):
    with get_conn() as conn:
        row = conn.execute(
            f"""
            SELECT {MASTER_KNIVES_PUBLIC_COLUMNS}
            FROM master_knives
            WHERE id = ?
            """,
            (knife_id,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Master knife not found.")
        return dict(row)


@app.post("/api/master-knives")
def create_master_knife(payload: MasterKnifeIn):
    with get_conn() as conn:
        try:
            cur = conn.execute(
                """
                INSERT INTO master_knives
                (name, family, record_type, category, catalog_line, catalog_status, confidence, evidence_summary,
                 collector_notes, default_blade_length, default_steel, default_blade_finish, default_blade_color,
                 default_handle_color, is_collab, collaboration_name, status, notes, blade_profile,
                 has_ring, is_filleting_knife, is_hatchet, is_kitchen, is_tactical, identifier_keywords,
                 identifier_product_url, identifier_image_mime, identifier_silhouette_hu_json,
                 canonical_slug, version, parent_model_id, first_release_date, last_seen_date,
                 is_discontinued, is_current_catalog, msrp, blade_shape, tip_style, grind_style, size_class,
                 primary_use_case, spine_profile, is_fillet, default_product_url, primary_image_url,
                 updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    payload.name.strip(),
                    payload.family,
                    payload.record_type,
                    payload.category,
                    payload.catalog_line,
                    payload.catalog_status,
                    payload.confidence,
                    payload.evidence_summary,
                    payload.collector_notes,
                    payload.default_blade_length,
                    payload.default_steel,
                    payload.default_blade_finish,
                    payload.default_blade_color,
                    payload.default_handle_color,
                    int(payload.is_collab),
                    payload.collaboration_name,
                    payload.status,
                    payload.notes,
                    payload.blade_profile,
                    int(payload.has_ring),
                    int(payload.is_filleting_knife),
                    int(payload.is_hatchet),
                    int(payload.is_kitchen),
                    int(payload.is_tactical),
                    payload.identifier_keywords,
                    payload.identifier_product_url,
                    payload.identifier_image_mime,
                    payload.identifier_silhouette_hu_json,
                    payload.canonical_slug,
                    payload.version,
                    payload.parent_model_id,
                    payload.first_release_date,
                    payload.last_seen_date,
                    1 if payload.is_discontinued else 0,
                    0 if payload.is_current_catalog is False else 1,
                    payload.msrp,
                    payload.blade_shape,
                    payload.tip_style,
                    payload.grind_style,
                    payload.size_class,
                    payload.primary_use_case,
                    payload.spine_profile,
                    1 if (payload.is_fillet if payload.is_fillet is not None else payload.is_filleting_knife) else 0,
                    payload.default_product_url,
                    payload.primary_image_url,
                ),
            )
            new_id = cur.lastrowid
        except sqlite3.IntegrityError:
            raise HTTPException(status_code=400, detail="Master knife already exists.")
        return {"id": new_id, "message": "Created"}


def _sync_inventory_from_master(
    conn: sqlite3.Connection,
    master_id: int,
    old: dict[str, Any],
    new_handle: Optional[str],
    new_steel: Optional[str],
    new_finish: Optional[str],
    new_blade_color: Optional[str],
    new_blade_len: Optional[float],
    new_is_collab: bool,
    new_collab_name: Optional[str],
) -> None:
    """Update inventory items that match old master defaults to new master defaults."""
    fields = [
        ("handle_color", old.get("default_handle_color"), new_handle),
        ("blade_steel", old.get("default_steel"), new_steel),
        ("blade_finish", old.get("default_blade_finish"), new_finish),
        ("blade_color", old.get("default_blade_color"), new_blade_color),
        ("blade_length", old.get("default_blade_length"), new_blade_len),
        ("is_collab", old.get("is_collab"), new_is_collab),
        ("collaboration_name", old.get("collaboration_name"), new_collab_name),
    ]
    for col, old_val, new_val in fields:
        if col == "blade_length":
            old_v = float(old_val) if old_val is not None else None
            conn.execute(
                """
                UPDATE inventory_items
                SET blade_length = ?, updated_at = CURRENT_TIMESTAMP
                WHERE master_knife_id = ?
                  AND ((blade_length IS NULL AND ? IS NULL) OR (blade_length = ?))
                """,
                (new_val, master_id, old_v, old_v),
            )
        elif col == "is_collab":
            old_int = 1 if old_val else 0
            conn.execute(
                """
                UPDATE inventory_items
                SET is_collab = ?, updated_at = CURRENT_TIMESTAMP
                WHERE master_knife_id = ?
                  AND is_collab = ?
                """,
                (int(new_is_collab), master_id, old_int),
            )
        else:
            conn.execute(
                f"""
                UPDATE inventory_items
                SET {col} = ?, updated_at = CURRENT_TIMESTAMP
                WHERE master_knife_id = ?
                  AND (({col} IS NULL AND ? IS NULL) OR ({col} = ?))
                """,
                (new_val, master_id, old_val, old_val),
            )


@app.put("/api/master-knives/{knife_id}")
def update_master_knife(knife_id: int, payload: MasterKnifeIn):
    with get_conn() as conn:
        old = conn.execute(
            "SELECT default_handle_color, default_steel, default_blade_finish, default_blade_color, "
            "default_blade_length, is_collab, collaboration_name FROM master_knives WHERE id = ?",
            (knife_id,),
        ).fetchone()
        if not old:
            raise HTTPException(status_code=404, detail="Master knife not found.")
        old = dict(old)

        cur = conn.execute(
            """
            UPDATE master_knives
            SET name = ?, family = ?, record_type = ?, category = ?, catalog_line = ?, catalog_status = ?, confidence = ?,
                evidence_summary = ?, collector_notes = ?,
                default_blade_length = ?, default_steel = ?, default_blade_finish = ?, default_blade_color = ?,
                default_handle_color = ?, is_collab = ?, collaboration_name = ?, status = ?, notes = ?,
                blade_profile = ?, has_ring = ?, is_filleting_knife = ?, is_hatchet = ?, is_kitchen = ?, is_tactical = ?,
                identifier_keywords = ?, identifier_distinguishing_features = ?, identifier_product_url = ?,
                identifier_image_mime = ?, identifier_silhouette_hu_json = ?,
                canonical_slug = ?, version = ?, parent_model_id = ?, first_release_date = ?, last_seen_date = ?,
                is_discontinued = ?, is_current_catalog = ?, msrp = ?, blade_shape = ?, tip_style = ?, grind_style = ?,
                size_class = ?, primary_use_case = ?, spine_profile = ?, is_fillet = ?,
                default_product_url = ?, primary_image_url = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                payload.name.strip(),
                payload.family,
                payload.record_type,
                payload.category,
                payload.catalog_line,
                payload.catalog_status,
                payload.confidence,
                payload.evidence_summary,
                payload.collector_notes,
                payload.default_blade_length,
                payload.default_steel,
                payload.default_blade_finish,
                payload.default_blade_color,
                payload.default_handle_color,
                int(payload.is_collab),
                payload.collaboration_name,
                payload.status,
                payload.notes,
                payload.blade_profile,
                int(payload.has_ring),
                int(payload.is_filleting_knife),
                int(payload.is_hatchet),
                int(payload.is_kitchen),
                int(payload.is_tactical),
                payload.identifier_keywords,
                payload.identifier_distinguishing_features,
                payload.identifier_product_url,
                payload.identifier_image_mime,
                payload.identifier_silhouette_hu_json,
                payload.canonical_slug,
                payload.version,
                payload.parent_model_id,
                payload.first_release_date,
                payload.last_seen_date,
                1 if payload.is_discontinued else 0,
                0 if payload.is_current_catalog is False else 1,
                payload.msrp,
                payload.blade_shape,
                payload.tip_style,
                payload.grind_style,
                payload.size_class,
                payload.primary_use_case,
                payload.spine_profile,
                1 if (payload.is_fillet if payload.is_fillet is not None else payload.is_filleting_knife) else 0,
                payload.default_product_url,
                payload.primary_image_url,
                knife_id,
            ),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Master knife not found.")

        _sync_inventory_from_master(
            conn,
            knife_id,
            old,
            payload.default_handle_color,
            payload.default_steel,
            payload.default_blade_finish,
            payload.default_blade_color,
            payload.default_blade_length,
            payload.is_collab,
            payload.collaboration_name,
        )
        return {"message": "Updated"}


def _next_duplicate_name(conn: sqlite3.Connection, base_name: str) -> str:
    """Find first available name of the form '{base_name} (n)' for n >= 2."""
    n = 2
    while True:
        candidate = f"{base_name} ({n})"
        if not conn.execute("SELECT 1 FROM master_knives WHERE name = ?", (candidate,)).fetchone():
            return candidate
        n += 1


@app.post("/api/master-knives/{knife_id}/duplicate")
def duplicate_master_knife(knife_id: int):
    """Copy a master record; new name defaults to '{original} (2)' (or (3), etc. if taken)."""
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM master_knives WHERE id = ?", (knife_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Master knife not found.")
        base = (row.get("name") or "").strip()
        if not base:
            raise HTTPException(status_code=400, detail="Source record has no name.")
        new_name = _next_duplicate_name(conn, base)

        cols = [c["name"] for c in conn.execute("PRAGMA table_info(master_knives)").fetchall()]
        copy_cols = [c for c in cols if c not in ("id", "name", "created_at", "updated_at")]
        col_list = ", ".join(["name"] + copy_cols + ["updated_at"])
        placeholders = ", ".join(["?"] * (len(copy_cols) + 1) + ["CURRENT_TIMESTAMP"])

        override: dict[str, Any] = {"canonical_slug": None}
        vals = [new_name] + [override.get(c, row.get(c)) for c in copy_cols]

        cur = conn.execute(
            f"INSERT INTO master_knives ({col_list}) VALUES ({placeholders})",
            vals,
        )
        new_id = cur.lastrowid
        return {"id": new_id, "name": new_name, "message": "Duplicated"}


@app.delete("/api/master-knives/{knife_id}")
def delete_master_knife(knife_id: int):
    with get_conn() as conn:
        count = conn.execute(
            "SELECT COUNT(*) AS c FROM inventory_items WHERE master_knife_id = ?",
            (knife_id,),
        ).fetchone()["c"]
        if count > 0:
            raise HTTPException(status_code=400, detail="Cannot delete: knife is used in inventory.")
        cur = conn.execute("DELETE FROM master_knives WHERE id = ?", (knife_id,))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Master knife not found.")
        return {"message": "Deleted"}


@app.get("/api/inventory")
def list_inventory():
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT
                i.*,
                m.name AS knife_name,
                m.family AS knife_family,
                m.catalog_line AS catalog_line,
                (CASE WHEN m.identifier_image_blob IS NOT NULL AND length(m.identifier_image_blob) > 0
                  THEN 1 ELSE 0 END) AS has_identifier_image
            FROM inventory_items i
            JOIN master_knives m ON m.id = i.master_knife_id
            ORDER BY m.name COLLATE NOCASE, i.id DESC
            """
        ).fetchall()
        return rows


@app.post("/api/inventory")
def create_inventory_item(payload: InventoryItemIn):
    with get_conn() as conn:
        exists = conn.execute(
            "SELECT id FROM master_knives WHERE id = ?",
            (payload.master_knife_id,),
        ).fetchone()
        if not exists:
            raise HTTPException(status_code=400, detail="Invalid master knife id.")
        cur = conn.execute(
            """
            INSERT INTO inventory_items
            (master_knife_id, nickname, quantity, acquired_date, purchase_price, estimated_value, condition,
             handle_color, blade_steel, blade_finish, blade_color, blade_length, is_collab, collaboration_name,
             serial_number, location, purchase_source, last_sharpened, notes, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                payload.master_knife_id,
                payload.nickname,
                payload.quantity,
                payload.acquired_date,
                payload.purchase_price,
                payload.estimated_value,
                payload.condition,
                payload.handle_color,
                payload.blade_steel,
                payload.blade_finish,
                payload.blade_color,
                payload.blade_length,
                int(payload.is_collab),
                payload.collaboration_name,
                payload.serial_number,
                payload.location,
                payload.purchase_source,
                payload.last_sharpened,
                payload.notes,
            ),
        )
        return {"id": cur.lastrowid, "message": "Created"}


@app.put("/api/inventory/{item_id}")
def update_inventory_item(item_id: int, payload: InventoryItemIn):
    with get_conn() as conn:
        cur = conn.execute(
            """
            UPDATE inventory_items
            SET master_knife_id = ?, nickname = ?, quantity = ?, acquired_date = ?, purchase_price = ?,
                estimated_value = ?, condition = ?, handle_color = ?, blade_steel = ?, blade_finish = ?,
                blade_color = ?, blade_length = ?, is_collab = ?, collaboration_name = ?, serial_number = ?,
                location = ?, purchase_source = ?, last_sharpened = ?, notes = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                payload.master_knife_id,
                payload.nickname,
                payload.quantity,
                payload.acquired_date,
                payload.purchase_price,
                payload.estimated_value,
                payload.condition,
                payload.handle_color,
                payload.blade_steel,
                payload.blade_finish,
                payload.blade_color,
                payload.blade_length,
                int(payload.is_collab),
                payload.collaboration_name,
                payload.serial_number,
                payload.location,
                payload.purchase_source,
                payload.last_sharpened,
                payload.notes,
                item_id,
            ),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Inventory item not found.")
        return {"message": "Updated"}


@app.delete("/api/inventory/{item_id}")
def delete_inventory_item(item_id: int):
    with get_conn() as conn:
        cur = conn.execute("DELETE FROM inventory_items WHERE id = ?", (item_id,))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Inventory item not found.")
        return {"message": "Deleted"}


INVENTORY_CSV_COLUMNS = [
    "id",
    "knife_name",
    "knife_family",
    "master_knife_id",
    "nickname",
    "quantity",
    "acquired_date",
    "mkc_order_number",
    "purchase_price",
    "estimated_value",
    "condition",
    "handle_color",
    "blade_steel",
    "blade_finish",
    "blade_color",
    "blade_length",
    "is_collab",
    "collaboration_name",
    "serial_number",
    "location",
    "purchase_source",
    "last_sharpened",
    "notes",
    "created_at",
    "updated_at",
]


@app.get("/api/inventory/export.csv")
def export_inventory_csv():
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT
                i.id,
                m.name AS knife_name,
                m.family AS knife_family,
                i.master_knife_id,
                i.nickname,
                i.quantity,
                i.acquired_date,
                i.purchase_price,
                i.estimated_value,
                i.condition,
                i.handle_color,
                i.blade_steel,
                i.blade_finish,
                i.blade_color,
                i.blade_length,
                i.is_collab,
                i.collaboration_name,
                i.serial_number,
                i.location,
                i.purchase_source,
                i.last_sharpened,
                i.notes,
                i.created_at,
                i.updated_at
            FROM inventory_items i
            JOIN master_knives m ON m.id = i.master_knife_id
            ORDER BY m.name COLLATE NOCASE, i.id DESC
            """
        ).fetchall()
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=INVENTORY_CSV_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        flat: dict[str, Any] = {}
        for key in INVENTORY_CSV_COLUMNS:
            val = row.get(key)
            if key == "is_collab":
                flat[key] = "1" if val else "0"
            elif val is None:
                flat[key] = ""
            else:
                flat[key] = val
        writer.writerow(flat)
    data = buffer.getvalue()
    return Response(
        content=data.encode("utf-8"),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="mkc_inventory.csv"'},
    )


@app.post("/api/inventory/{item_id}/duplicate")
def duplicate_inventory_item(item_id: int):
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM inventory_items WHERE id = ?", (item_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Inventory item not found.")
        nick = (row.get("nickname") or "").strip()
        new_nick = f"{nick} (copy)" if nick else "Copy"
        cur = conn.execute(
            """
            INSERT INTO inventory_items
            (master_knife_id, nickname, quantity, acquired_date, purchase_price, estimated_value, condition,
             handle_color, blade_steel, blade_finish, blade_color, blade_length, is_collab, collaboration_name,
             serial_number, location, purchase_source, last_sharpened, notes, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                row["master_knife_id"],
                new_nick,
                row["quantity"],
                row["acquired_date"],
                row["purchase_price"],
                row["estimated_value"],
                row["condition"],
                row["handle_color"],
                row["blade_steel"],
                row["blade_finish"],
                row["blade_color"],
                row["blade_length"],
                int(row["is_collab"]),
                row["collaboration_name"],
                None,
                row["location"],
                row["purchase_source"],
                row["last_sharpened"],
                row["notes"],
            ),
        )
        return {"id": cur.lastrowid, "message": "Duplicated"}


OPTION_TABLES = {
    "handle-colors": "option_handle_colors",
    "blade-steels": "option_blade_steels",
    "blade-finishes": "option_blade_finishes",
    "blade-colors": "option_blade_colors",
    "blade-types": "option_blade_types",
    "categories": "option_categories",
    "blade-families": "option_blade_families",
    "primary-use-cases": "option_primary_use_cases",
}

TIER_OPTION_TYPES = ("categories", "blade-families", "primary-use-cases")


@app.get("/api/derive-blade-family")
def derive_blade_family(name: Optional[str] = None):
    """Return derived blade family for a model name. Used for auto-suggest when editing name."""
    return {"family": derive_blade_family_from_name(name)}


@app.get("/api/options")
def get_options():
    with get_conn() as conn:
        result: dict[str, list[dict[str, Any]]] = {}
        for key, table in OPTION_TABLES.items():
            if key == "blade-families":
                tbl_rows = conn.execute(
                    "SELECT * FROM option_blade_families ORDER BY name COLLATE NOCASE"
                ).fetchall()
                masters = conn.execute(
                    "SELECT DISTINCT name FROM master_knives WHERE name IS NOT NULL"
                ).fetchall()
                derived = {derive_blade_family_from_name(r["name"]) for r in masters if r["name"]}
                derived = {f for f in derived if f}
                from_table = {r["name"] for r in tbl_rows}
                combined = sorted(from_table | derived, key=lambda x: x.lower())
                result[key] = [{"id": n, "name": n} for n in combined]
            else:
                result[key] = conn.execute(
                    f"SELECT * FROM {table} ORDER BY name COLLATE NOCASE"
                ).fetchall()
        return result


@app.get("/api/inventory/options")
def get_inventory_options(master_knife_id: Optional[int] = None):
    """Return option lists for inventory form (all options)."""
    with get_conn() as conn:
        result: dict[str, list[dict[str, Any]]] = {}
        for key, table in OPTION_TABLES.items():
            result[key] = conn.execute(
                f"SELECT * FROM {table} ORDER BY name COLLATE NOCASE"
            ).fetchall()
        result["_filtered"] = False  # All options shown (allowed variants removed)
        return result


@app.post("/api/options/{option_type}")
def add_option(option_type: str, payload: OptionIn):
    table = OPTION_TABLES.get(option_type)
    if not table:
        raise HTTPException(status_code=404, detail="Unknown option type.")
    with get_conn() as conn:
        try:
            cur = conn.execute(
                f"INSERT INTO {table} (name) VALUES (?)",
                (payload.name.strip(),),
            )
        except sqlite3.IntegrityError:
            raise HTTPException(status_code=400, detail="Option already exists.")
        return {"id": cur.lastrowid, "message": "Created"}


@app.delete("/api/options/{option_type}/{option_id}")
def delete_option(option_type: str, option_id: int):
    table = OPTION_TABLES.get(option_type)
    if not table:
        raise HTTPException(status_code=404, detail="Unknown option type.")
    with get_conn() as conn:
        cur = conn.execute(f"DELETE FROM {table} WHERE id = ?", (option_id,))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Option not found.")
        return {"message": "Deleted"}


def _identify_catalog_blurb(row: dict[str, Any]) -> Optional[str]:
    """Short line from Knife Master / catalog text for result cards."""
    for key in ("evidence_summary", "collector_notes", "catalog_status"):
        raw = row.get(key)
        if raw and str(raw).strip():
            s = " ".join(str(raw).split())
            return s if len(s) <= 180 else s[:179] + "…"
    return None


@app.post("/api/identify")
def identify_knives(payload: IdentifierQuery):
    """Backward-compatible route now powered by canonical v2 catalog."""
    return v2_identify_knives(payload)


@app.get("/api/ai/ollama/config")
def api_ollama_config():
    return {"ollama_host": blade_ai.OLLAMA_HOST}


@app.get("/api/ai/ollama/check")
def api_ollama_check(model: Optional[str] = None):
    """
    Verify Ollama is reachable and optionally that the given model is loaded.
    Returns reachable status, model list, and validation error if model specified and missing.
    """
    try:
        data = blade_ai.fetch_ollama_models()
        models = data.get("models") or []
        model_names = [m.get("name") or m.get("model", "") for m in models if isinstance(m, dict)]
        ok, err = True, None
        if model and (model or "").strip():
            ok, err = blade_ai.check_ollama_model(model)
        return {
            "reachable": True,
            "ollama_host": blade_ai.OLLAMA_HOST,
            "models": models,
            "model_names": model_names,
            "model_ok": ok if model else None,
            "model_error": err,
        }
    except httpx.ConnectError:
        return {
            "reachable": False,
            "ollama_host": blade_ai.OLLAMA_HOST,
            "error": f"Ollama not reachable at {blade_ai.OLLAMA_HOST}. Is it running?",
            "models": [],
            "model_names": [],
        }
    except httpx.HTTPError as exc:
        return {
            "reachable": False,
            "ollama_host": blade_ai.OLLAMA_HOST,
            "error": str(exc),
            "models": [],
            "model_names": [],
        }
    except Exception as exc:
        return {
            "reachable": False,
            "ollama_host": blade_ai.OLLAMA_HOST,
            "error": str(exc),
            "models": [],
            "model_names": [],
        }


@app.get("/api/ai/ollama/models")
def api_ollama_list_models():
    try:
        return blade_ai.fetch_ollama_models()
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Cannot reach Ollama: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


class ReportingQueryIn(BaseModel):
    question: str = Field(min_length=2, max_length=2000)
    session_id: Optional[str] = None
    model: Optional[str] = None
    max_rows: int = Field(default=REPORTING_MAX_ROWS_DEFAULT, ge=1, le=REPORTING_MAX_ROWS_HARD)
    chart_preference: Optional[str] = None
    compare_dimension: Optional[str] = None
    compare_value_a: Optional[str] = None
    compare_value_b: Optional[str] = None


class ReportingSaveQueryIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    question: str = Field(min_length=2, max_length=2000)
    config: Optional[dict[str, Any]] = None


class ReportingFeedbackIn(BaseModel):
    session_id: str = Field(min_length=8, max_length=120)
    message_id: int = Field(ge=1)
    helpful: bool


def _reporting_create_session(conn: sqlite3.Connection, model_default: Optional[str] = None) -> dict[str, Any]:
    sid = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO reporting_sessions (id, title, model_default, memory_summary, created_at, updated_at)
        VALUES (?, 'New chat', ?, '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        (sid, model_default or REPORTING_DEFAULT_MODEL),
    )
    return {
        "id": sid,
        "title": "New chat",
        "model_default": model_default or REPORTING_DEFAULT_MODEL,
        "memory_summary": "",
    }


def _reporting_get_or_create_session(conn: sqlite3.Connection, session_id: Optional[str], model_default: Optional[str]) -> dict[str, Any]:
    if session_id:
        row = conn.execute(
            "SELECT * FROM reporting_sessions WHERE id = ?",
            (session_id,),
        ).fetchone()
        if row:
            return row
    return _reporting_create_session(conn, model_default)


def _reporting_store_message(
    conn: sqlite3.Connection,
    session_id: str,
    role: str,
    content: str,
    sql_executed: Optional[str] = None,
    result: Optional[dict[str, Any]] = None,
    chart_spec: Optional[dict[str, Any]] = None,
    meta: Optional[dict[str, Any]] = None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO reporting_messages (session_id, role, content, sql_executed, result_json, chart_spec_json, meta_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (
            session_id,
            role,
            content,
            sql_executed,
            json.dumps(result) if result is not None else None,
            json.dumps(chart_spec) if chart_spec is not None else None,
            json.dumps(meta) if meta is not None else None,
        ),
    )
    conn.execute(
        "UPDATE reporting_sessions SET updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (session_id,),
    )
    return int(cur.lastrowid)


def _reporting_context_block(conn: sqlite3.Connection, session_id: str, limit: int = 12) -> str:
    session = conn.execute(
        "SELECT memory_summary, last_query_state_json FROM reporting_sessions WHERE id = ?",
        (session_id,),
    ).fetchone()
    summary = (session.get("memory_summary") or "").strip() if session else ""
    last_state = ""
    if session and session.get("last_query_state_json"):
        try:
            parsed = json.loads(session.get("last_query_state_json"))
            if isinstance(parsed, dict):
                last_state = json.dumps(parsed, ensure_ascii=False)
        except Exception:
            last_state = ""
    rows = conn.execute(
        """
        SELECT role, content
        FROM reporting_messages
        WHERE session_id = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (session_id, limit),
    ).fetchall()
    rows = list(reversed(rows))
    lines = []
    if summary:
        lines.append(f"Summary: {summary}")
    if last_state:
        lines.append(f"LastQueryState: {last_state[:700]}")
    for r in rows:
        role = "User" if r.get("role") == "user" else "Assistant"
        content = " ".join(str(r.get("content") or "").split())
        if content:
            lines.append(f"{role}: {content[:500]}")
    block = "\n".join(lines).strip()
    return block[-4000:] if len(block) > 4000 else block


def _reporting_update_summary(conn: sqlite3.Connection, session_id: str) -> None:
    rows = conn.execute(
        """
        SELECT role, content
        FROM reporting_messages
        WHERE session_id = ?
        ORDER BY id DESC
        LIMIT 20
        """,
        (session_id,),
    ).fetchall()
    if len(rows) < 10:
        return
    rows = list(reversed(rows))
    bullets = []
    for r in rows:
        if r.get("role") != "user":
            continue
        q = " ".join(str(r.get("content") or "").split())
        if q:
            bullets.append(f"- {q[:120]}")
    if not bullets:
        return
    summary = "\n".join(bullets[-8:])
    conn.execute(
        "UPDATE reporting_sessions SET memory_summary = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (summary, session_id),
    )


def _reporting_generate_answer(
    model: str,
    question: str,
    columns: list[str],
    rows: list[dict[str, Any]],
    sql_executed: str,
    context_block: str,
    semantic_intent: Optional[str] = None,
) -> tuple[str, list[str], Optional[str], Optional[float]]:
    if not rows:
        return (
            "No matching rows found for that question. Try broadening filters or date range.",
            ["Remove a filter and rerun.", "Try 'Show all knives by family.'"],
            "No rows matched the generated query.",
            0.6,
        )
    if semantic_intent == "missing_models":
        names = [str(r.get("official_name") or "").strip() for r in rows if str(r.get("official_name") or "").strip()]
        if names:
            max_list = 30
            listed = ", ".join(names[:max_list])
            extra = f" (+{len(names)-max_list} more)" if len(names) > max_list else ""
            return (
                f"You are missing {len(names)} models matching that scope: {listed}{extra}.",
                ["Show this grouped by family.", "Show only missing Traditions models.", "Estimate completion cost for these."],
                "Deterministic missing-model answer.",
                0.9,
            )
    preview = rows[:40]
    try:
        system = (
            "You are a concise collection reporting assistant. "
            "Summarize SQL query results faithfully. "
            "Return JSON only: {\"answer_text\":..., \"follow_ups\":[...], \"limitations\":..., \"confidence\":...}."
        )
        user = (
            f"Question: {question}\n"
            f"SQL: {sql_executed}\n"
            f"Columns: {columns}\n"
            f"Rows sample: {json.dumps(preview, ensure_ascii=False)}\n"
            f"Context: {context_block or '(none)'}"
        )
        raw = blade_ai.ollama_chat(model, system, user, timeout=90.0)
        parsed = json.loads(raw) if raw.strip().startswith("{") else None
        if parsed is None:
            m = re.search(r"\{.*\}", raw, re.S)
            if m:
                parsed = json.loads(m.group(0))
        if isinstance(parsed, dict):
            answer = str(parsed.get("answer_text") or "").strip() or f"Returned {len(rows)} rows."
            followups = parsed.get("follow_ups") if isinstance(parsed.get("follow_ups"), list) else []
            limitations = parsed.get("limitations")
            conf_raw = parsed.get("confidence")
            try:
                confidence = float(conf_raw) if conf_raw is not None else None
            except (TypeError, ValueError):
                confidence = None
            if not followups:
                followups = _reporting_default_followups(question, columns, rows)
            return answer, followups[:5], limitations, confidence
    except Exception:
        pass

    top = rows[0]
    top_bits = ", ".join(f"{k}={top.get(k)}" for k in columns[:4])
    return (
        f"Found {len(rows)} rows. First row: {top_bits}.",
        _reporting_default_followups(question, columns, rows),
        "Summary generated with deterministic fallback.",
        0.55,
    )


def _reporting_pick_available(preferred: str, available: list[str], fallback: str) -> str:
    if preferred in available:
        return preferred
    if fallback in available:
        return fallback
    return available[0] if available else fallback


def _reporting_model_route(
    requested_model: Optional[str],
    check_payload: Optional[dict[str, Any]] = None,
) -> dict[str, Optional[str]]:
    names = (check_payload or {}).get("model_names") or []
    if requested_model and requested_model.strip():
        forced = requested_model.strip()
        return {"planner_model": forced, "responder_model": forced, "retry_model": None}
    planner = _reporting_pick_available(REPORTING_PLANNER_MODEL, names, REPORTING_DEFAULT_MODEL)
    responder = _reporting_pick_available(REPORTING_RESPONDER_MODEL, names, REPORTING_DEFAULT_MODEL)
    retry_model = None
    if REPORTING_PLANNER_RETRY_MODEL:
        retry_model = _reporting_pick_available(REPORTING_PLANNER_RETRY_MODEL, names, planner)
    return {"planner_model": planner, "responder_model": responder, "retry_model": retry_model}


@app.get("/reporting")
def reporting_page():
    return FileResponse(STATIC_DIR / "reporting.html")


@app.get("/api/reporting/schema")
def reporting_schema():
    with get_conn() as conn:
        ensure_reporting_schema(conn)
        views = {}
        for view in sorted(REPORTING_ALLOWED_SOURCES):
            cols = conn.execute(f"PRAGMA table_info({view})").fetchall()
            views[view] = [{"name": c["name"], "type": c.get("type")} for c in cols]
        return {
            "views": views,
            "allowed_sources": sorted(REPORTING_ALLOWED_SOURCES),
            "default_model": REPORTING_RESPONDER_MODEL,
            "planner_model": REPORTING_PLANNER_MODEL,
            "responder_model": REPORTING_RESPONDER_MODEL,
        }


@app.get("/api/reporting/suggested-questions")
def reporting_suggested_questions():
    return {
        "questions": [
            "What is my total collection value by family?",
            "Show monthly spend for the last 12 months.",
            "Which knives have the highest estimated value?",
            "How many knives do I have by steel?",
            "Show condition distribution across my inventory.",
            "Compare spend between Traditions and VIP this year.",
            "What are my top 10 most expensive purchases?",
            "How many knives are in each location?",
        ]
    }


@app.get("/api/reporting/sessions")
def reporting_sessions():
    with get_conn() as conn:
        ensure_reporting_schema(conn)
        rows = conn.execute(
            """
            SELECT s.id, s.title, s.model_default, s.created_at, s.updated_at,
                   (SELECT COUNT(*) FROM reporting_messages m WHERE m.session_id = s.id) AS message_count
            FROM reporting_sessions s
            ORDER BY s.updated_at DESC, s.created_at DESC
            LIMIT 100
            """
        ).fetchall()
        return {"sessions": rows}


@app.get("/api/reporting/telemetry")
def reporting_telemetry(limit: int = 100):
    with get_conn() as conn:
        ensure_reporting_schema(conn)
        safe_limit = min(1000, max(1, int(limit)))
        rows = conn.execute(
            """
            SELECT id, session_id, question, planner_model, responder_model, generation_mode, semantic_intent,
                   sql_excerpt, row_count, execution_ms, total_ms, status, error_detail, meta_json, created_at
            FROM reporting_query_telemetry
            ORDER BY id DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()
        for r in rows:
            r["meta"] = json.loads(r["meta_json"]) if r.get("meta_json") else {}
        return {"events": rows}


@app.get("/api/reporting/hints")
def reporting_hints(limit: int = 100, session_id: Optional[str] = None):
    with get_conn() as conn:
        ensure_reporting_schema(conn)
        safe_limit = min(1000, max(1, int(limit)))
        if session_id and session_id.strip():
            rows = conn.execute(
                """
                SELECT id, scope_type, scope_id, entity_norm, cue_word, target_dimension, target_value,
                       confidence, evidence_count, success_count, failure_count, created_at, updated_at, last_used_at
                FROM reporting_semantic_hints
                WHERE (scope_type = 'session' AND scope_id = ?) OR (scope_type = 'global' AND scope_id IS NULL)
                ORDER BY confidence DESC, evidence_count DESC, id DESC
                LIMIT ?
                """,
                (session_id.strip(), safe_limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, scope_type, scope_id, entity_norm, cue_word, target_dimension, target_value,
                       confidence, evidence_count, success_count, failure_count, created_at, updated_at, last_used_at
                FROM reporting_semantic_hints
                ORDER BY confidence DESC, evidence_count DESC, id DESC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()
        return {"hints": rows}


@app.post("/api/reporting/feedback")
def reporting_feedback(payload: ReportingFeedbackIn):
    with get_conn() as conn:
        ensure_reporting_schema(conn)
        sid = payload.session_id.strip()
        msg = conn.execute(
            """
            SELECT id, session_id, role, meta_json
            FROM reporting_messages
            WHERE id = ? AND session_id = ?
            """,
            (int(payload.message_id), sid),
        ).fetchone()
        if not msg:
            raise HTTPException(status_code=404, detail="Message not found for this session.")
        if str(msg.get("role") or "") != "assistant":
            raise HTTPException(status_code=400, detail="Feedback is only supported on assistant messages.")

        meta = json.loads(msg.get("meta_json") or "{}") if msg.get("meta_json") else {}
        if not isinstance(meta, dict):
            meta = {}
        prior = meta.get("feedback_helpful")
        if isinstance(prior, bool):
            # Idempotent behavior: do not re-apply confidence changes.
            if prior == bool(payload.helpful):
                return {"ok": True, "message": "Feedback already recorded.", "changed": False}
            raise HTTPException(status_code=409, detail="Feedback already recorded for this message.")

        hint_ids: list[int] = []
        for h in (meta.get("semantic_hints") or []):
            if not isinstance(h, dict):
                continue
            hid = h.get("id")
            if isinstance(hid, int):
                hint_ids.append(hid)
            elif isinstance(hid, str) and hid.isdigit():
                hint_ids.append(int(hid))

        if hint_ids:
            _reporting_feedback_semantic_hints(conn, hint_ids, success=bool(payload.helpful))

        meta["feedback_helpful"] = bool(payload.helpful)
        meta["feedback_at"] = _reporting_iso_now()
        conn.execute(
            "UPDATE reporting_messages SET meta_json = ? WHERE id = ?",
            (json.dumps(meta), int(payload.message_id)),
        )
        conn.execute(
            "UPDATE reporting_sessions SET updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (sid,),
        )
        return {"ok": True, "changed": True, "hint_ids_updated": hint_ids}


@app.post("/api/reporting/sessions")
def reporting_session_create(model: Optional[str] = None):
    with get_conn() as conn:
        ensure_reporting_schema(conn)
        s = _reporting_create_session(conn, model)
        return {"session": s}


@app.get("/api/reporting/sessions/{session_id}")
def reporting_session_detail(session_id: str):
    with get_conn() as conn:
        ensure_reporting_schema(conn)
        session = conn.execute("SELECT * FROM reporting_sessions WHERE id = ?", (session_id,)).fetchone()
        if not session:
            raise HTTPException(status_code=404, detail="Session not found.")
        msgs = conn.execute(
            """
            SELECT id, role, content, sql_executed, result_json, chart_spec_json, meta_json, created_at
            FROM reporting_messages
            WHERE session_id = ?
            ORDER BY id
            """,
            (session_id,),
        ).fetchall()
        parsed_msgs = []
        for m in msgs:
            parsed_msgs.append(
                {
                    "id": m["id"],
                    "role": m["role"],
                    "content": m["content"],
                    "sql_executed": m.get("sql_executed"),
                    "result": json.loads(m["result_json"]) if m.get("result_json") else None,
                    "chart_spec": json.loads(m["chart_spec_json"]) if m.get("chart_spec_json") else None,
                    "meta": json.loads(m["meta_json"]) if m.get("meta_json") else None,
                    "created_at": m["created_at"],
                }
            )
        return {"session": session, "messages": parsed_msgs}


@app.get("/api/reporting/saved-queries")
def reporting_saved_queries():
    with get_conn() as conn:
        ensure_reporting_schema(conn)
        rows = conn.execute(
            """
            SELECT id, name, question, config_json, created_at, updated_at
            FROM reporting_saved_queries
            ORDER BY updated_at DESC, created_at DESC
            """
        ).fetchall()
        for row in rows:
            row["config"] = json.loads(row["config_json"]) if row.get("config_json") else {}
        return {"saved_queries": rows}


@app.post("/api/reporting/saved-queries")
def reporting_save_query(payload: ReportingSaveQueryIn):
    with get_conn() as conn:
        ensure_reporting_schema(conn)
        cur = conn.execute(
            """
            INSERT INTO reporting_saved_queries (name, question, config_json, created_at, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (payload.name.strip(), payload.question.strip(), json.dumps(payload.config or {})),
        )
        return {"id": cur.lastrowid, "message": "Saved"}


@app.delete("/api/reporting/saved-queries/{saved_id}")
def reporting_delete_query(saved_id: int):
    with get_conn() as conn:
        ensure_reporting_schema(conn)
        cur = conn.execute("DELETE FROM reporting_saved_queries WHERE id = ?", (saved_id,))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Saved query not found.")
        return {"message": "Deleted"}


@app.post("/api/reporting/query")
def reporting_query(payload: ReportingQueryIn):
    started = time.perf_counter()
    question = payload.question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="Question is required.")
    date_start, date_end, date_label = _reporting_detect_date_bounds(question)

    try:
        check_payload = api_ollama_check()
    except Exception:
        check_payload = None
    route_models = _reporting_model_route(payload.model, check_payload)
    planner_model = route_models["planner_model"] or REPORTING_DEFAULT_MODEL
    responder_model = route_models["responder_model"] or REPORTING_DEFAULT_MODEL
    retry_model = route_models["retry_model"]

    with get_conn() as conn:
        ensure_reporting_schema(conn)
        session = _reporting_get_or_create_session(conn, payload.session_id, responder_model)
        session_id = session["id"]

        if (session.get("title") or "").strip().lower() == "new chat":
            conn.execute(
                "UPDATE reporting_sessions SET title = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (question[:80], session_id),
            )

        _reporting_store_message(conn, session_id, "user", question)
        context_block = _reporting_context_block(conn, session_id)
        def _log_error(status: str, detail: str, mode: Optional[str] = None, semantic_intent: Optional[str] = None) -> None:
            total_ms = round((time.perf_counter() - started) * 1000.0, 2)
            _reporting_log_query_event(
                conn,
                session_id=session_id,
                question=question,
                planner_model=planner_model,
                responder_model=responder_model,
                generation_mode=mode,
                semantic_intent=semantic_intent,
                sql_excerpt=None,
                row_count=None,
                execution_ms=None,
                total_ms=total_ms,
                status=status,
                error_detail=detail,
                meta={},
            )

        if _reporting_is_scope_status_question(question):
            state = _reporting_get_last_query_state(conn, session_id) or {}
            scope = str(state.get("scope") or "inventory").strip().lower()
            if scope == "catalog":
                msg = (
                    "I am currently scoped to the full MKC catalog "
                    "(all models made), not just your inventory."
                )
            else:
                msg = (
                    "I am currently scoped to your inventory "
                    "(knives you own), not the full MKC catalog."
                )
            assistant_message_id = _reporting_store_message(
                conn,
                session_id,
                "assistant",
                msg,
                meta={"scope_status": scope},
            )
            total_ms = round((time.perf_counter() - started) * 1000.0, 2)
            _reporting_log_query_event(
                conn,
                session_id=session_id,
                question=question,
                planner_model=planner_model,
                responder_model=responder_model,
                generation_mode="scope_status",
                semantic_intent=None,
                sql_excerpt=None,
                row_count=0,
                execution_ms=None,
                total_ms=total_ms,
                status="ok",
                error_detail=None,
                meta={"scope": scope},
            )
            return {
                "session_id": session_id,
                "model": responder_model,
                "planner_model": planner_model,
                "answer_text": msg,
                "columns": [],
                "rows": [],
                "chart_spec": None,
                "sql_executed": None,
                "follow_ups": [],
                "confidence": 0.9,
                "limitations": None,
                "generation_mode": "scope_status",
                "execution_ms": None,
                "date_window": {"start": date_start, "end": date_end, "label": date_label},
                "assistant_message_id": assistant_message_id,
            }

        if _reporting_needs_scope_clarification(question):
            clarify = (
                "Quick clarification: do you want this based on knives you currently own "
                "(your inventory), or based on all models MKC has made (full catalog)?"
            )
            follow_ups = [
                f"{question.rstrip('?')} in my inventory (knives I own)?",
                f"{question.rstrip('?')} in the full MKC catalog (all models made)?",
            ]
            assistant_message_id = _reporting_store_message(
                conn,
                session_id,
                "assistant",
                clarify,
                meta={"clarification_needed": "scope", "follow_ups": follow_ups},
            )
            total_ms = round((time.perf_counter() - started) * 1000.0, 2)
            _reporting_log_query_event(
                conn,
                session_id=session_id,
                question=question,
                planner_model=planner_model,
                responder_model=responder_model,
                generation_mode="clarification_scope",
                semantic_intent=None,
                sql_excerpt=None,
                row_count=0,
                execution_ms=None,
                total_ms=total_ms,
                status="clarification_needed",
                error_detail="scope_ambiguous",
                meta={"follow_ups": follow_ups},
            )
            return {
                "session_id": session_id,
                "model": responder_model,
                "planner_model": planner_model,
                "answer_text": clarify,
                "columns": [],
                "rows": [],
                "chart_spec": None,
                "sql_executed": None,
                "follow_ups": follow_ups,
                "confidence": None,
                "limitations": "Scope was ambiguous (inventory vs full catalog).",
                "generation_mode": "clarification_scope",
                "execution_ms": None,
                "date_window": {"start": date_start, "end": date_end, "label": date_label},
                "assistant_message_id": assistant_message_id,
            }

        unsafe_reason = _reporting_detect_unsafe_request(question)
        if unsafe_reason:
            safe_msg = (
                "I can only help with safe, read-only collection questions. "
                "Please ask in plain language without SQL commands or schema instructions."
            )
            _reporting_store_message(
                conn,
                session_id,
                "assistant",
                safe_msg,
                meta={"guardrail": "unsafe_request", "reason": unsafe_reason},
            )
            _log_error("guardrail_reject", unsafe_reason, mode="guardrail", semantic_intent=None)
            raise HTTPException(status_code=400, detail=safe_msg)

        semantic_plan: Optional[dict[str, Any]] = None
        sql: Optional[str] = None
        sql_meta: dict[str, Any] = {}
        hint_ids_used: list[int] = []

        # Explicit compare mode remains template-backed for predictable behavior.
        if payload.compare_dimension and payload.compare_value_a and payload.compare_value_b:
            sql, sql_meta = _reporting_template_sql(
                question,
                date_start,
                date_end,
                compare_dimension=payload.compare_dimension,
                compare_a=payload.compare_value_a,
                compare_b=payload.compare_value_b,
            )
        else:
            semantic_plan, semantic_meta = _reporting_semantic_plan(
                conn,
                planner_model,
                question,
                session_id,
                context_block,
                retry_model=retry_model,
            )
            sql, compile_meta = _reporting_plan_to_sql(
                semantic_plan,
                date_start,
                date_end,
                payload.max_rows,
            )
            sql_meta = {**semantic_meta, **compile_meta}
            hint_ids_used = [int(x) for x in (semantic_meta.get("hint_ids") or []) if isinstance(x, int) or str(x).isdigit()]

        # Fallback path for robustness with old behavior.
        if not sql:
            sql, sql_meta = _reporting_template_sql(
                question,
                date_start,
                date_end,
                compare_dimension=payload.compare_dimension,
                compare_a=payload.compare_value_a,
                compare_b=payload.compare_value_b,
            )
        if not sql:
            sql, llm_meta = _reporting_call_llm_for_sql(
                conn,
                planner_model,
                question,
                context_block,
                date_start,
                date_end,
            )
            sql_meta = {**sql_meta, **llm_meta}
        if not sql:
            _log_error("no_sql", f"Could not derive SQL. {sql_meta.get('error') or ''}".strip(), mode=sql_meta.get("mode"), semantic_intent=(semantic_plan or {}).get("intent"))
            raise HTTPException(
                status_code=400,
                detail=f"Could not derive a safe SQL query. {sql_meta.get('error') or ''}".strip(),
            )

        try:
            columns, rows, execution_ms = _reporting_exec_sql(conn, sql, payload.max_rows)
        except HTTPException as exc:
            _log_error("sql_error", str(exc.detail), mode=sql_meta.get("mode"), semantic_intent=(semantic_plan or {}).get("intent"))
            raise
        rows_out = []
        for r in rows:
            row = dict(r)
            drill = _reporting_build_drill_link(row)
            if drill:
                row["_drill_link"] = drill
            rows_out.append(row)

        primary_intent = (semantic_plan or {}).get("intent")
        substantive = _reporting_has_substantive_rows(primary_intent, rows_out)
        # If semantic plan looks over-constrained, attempt one generic ambiguity relaxation pass.
        if not substantive and semantic_plan:
            relaxed = _reporting_relax_ambiguous_plan(semantic_plan, question)
            if relaxed:
                relaxed_sql, relaxed_meta = _reporting_plan_to_sql(
                    relaxed,
                    date_start,
                    date_end,
                    payload.max_rows,
                )
                if relaxed_sql:
                    try:
                        cols2, rows2, exec2 = _reporting_exec_sql(conn, relaxed_sql, payload.max_rows)
                        rows_out2 = []
                        for r2 in rows2:
                            row2 = dict(r2)
                            drill2 = _reporting_build_drill_link(row2)
                            if drill2:
                                row2["_drill_link"] = drill2
                            rows_out2.append(row2)
                        if _reporting_has_substantive_rows((relaxed or {}).get("intent"), rows_out2):
                            columns = cols2
                            rows_out = rows_out2
                            execution_ms = exec2
                            semantic_plan = relaxed
                            sql = relaxed_sql
                            sql_meta = {**sql_meta, **relaxed_meta, "mode": f"{sql_meta.get('mode')}_relaxed"}
                            substantive = True
                    except HTTPException:
                        pass

        # Learn and feedback hint confidence from final outcome.
        if hint_ids_used:
            _reporting_feedback_semantic_hints(conn, hint_ids_used, success=substantive)
        if semantic_plan:
            _reporting_learn_semantic_hints(
                conn,
                session_id=session_id,
                question=question,
                plan=semantic_plan,
                row_count=(1 if substantive else 0),
            )

        chart_spec = _reporting_infer_chart(
            question,
            columns,
            rows_out,
            preference=(payload.chart_preference or "").strip().lower() or None,
        )
        answer_text, follow_ups, limitations, confidence = _reporting_generate_answer(
            responder_model,
            question,
            columns,
            rows_out,
            sql,
            context_block,
            semantic_intent=(semantic_plan or {}).get("intent"),
        )
        if isinstance(sql_meta.get("follow_ups"), list) and sql_meta["follow_ups"]:
            follow_ups = sql_meta["follow_ups"][:5]
        if sql_meta.get("limitations") and not limitations:
            limitations = str(sql_meta["limitations"])
        if sql_meta.get("confidence") is not None and confidence is None:
            try:
                confidence = float(sql_meta["confidence"])
            except (TypeError, ValueError):
                confidence = confidence
        effective_date_start = (semantic_plan or {}).get("date_start") or date_start
        effective_date_end = (semantic_plan or {}).get("date_end") or date_end
        effective_date_label = (semantic_plan or {}).get("date_label") or date_label
        yc = (semantic_plan or {}).get("year_compare")
        if not effective_date_label and isinstance(yc, (list, tuple)) and len(yc) == 2:
            effective_date_label = f"{yc[0]} vs {yc[1]}"

        result_payload = {
            "columns": columns,
            "rows": rows_out,
            "row_count": len(rows_out),
            "date_window": {"start": effective_date_start, "end": effective_date_end, "label": effective_date_label},
        }
        meta = {
            "planner_model": planner_model,
            "responder_model": responder_model,
            "retry_model": retry_model,
            "generation_mode": sql_meta.get("mode"),
            "confidence": confidence,
            "limitations": limitations,
            "follow_ups": follow_ups,
            "execution_ms": execution_ms,
            "semantic_plan": semantic_plan,
            "timestamp": _reporting_iso_now(),
            "semantic_hints": sql_meta.get("hints") or [],
        }
        assistant_message_id = _reporting_store_message(
            conn,
            session_id,
            "assistant",
            answer_text,
            sql_executed=sql,
            result=result_payload,
            chart_spec=chart_spec,
            meta=meta,
        )
        if semantic_plan:
            _reporting_set_last_query_state(conn, session_id, semantic_plan)
        _reporting_update_summary(conn, session_id)

        total_ms = round((time.perf_counter() - started) * 1000.0, 2)
        _reporting_log_query_event(
            conn,
            session_id=session_id,
            question=question,
            planner_model=planner_model,
            responder_model=responder_model,
            generation_mode=sql_meta.get("mode"),
            semantic_intent=(semantic_plan or {}).get("intent"),
            sql_excerpt=sql,
            row_count=len(rows_out),
            execution_ms=execution_ms,
            total_ms=total_ms,
            status="ok",
            meta={
                "date_window": {"start": effective_date_start, "end": effective_date_end, "label": effective_date_label},
                "planner_attempts": sql_meta.get("planner_attempts"),
                "has_compare_mode": bool(payload.compare_dimension and payload.compare_value_a and payload.compare_value_b),
                "semantic_plan": semantic_plan,
                "semantic_hints": sql_meta.get("hints") or [],
            },
        )

        return {
            "session_id": session_id,
            "model": responder_model,
            "planner_model": planner_model,
            "answer_text": answer_text,
            "columns": columns,
            "rows": rows_out,
            "chart_spec": chart_spec,
            "sql_executed": sql,
            "follow_ups": follow_ups,
            "confidence": confidence,
            "limitations": limitations,
            "generation_mode": sql_meta.get("mode"),
            "execution_ms": execution_ms,
            "date_window": {"start": effective_date_start, "end": effective_date_end, "label": effective_date_label},
            "assistant_message_id": assistant_message_id,
        }


@app.get("/api/blade-shapes")
def api_blade_shapes():
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, slug, name, description, outline_json
            FROM blade_shape_templates
            ORDER BY name COLLATE NOCASE
            """
        ).fetchall()
    return rows


def _keyword_results_to_parsed(keyword_results: list[dict[str, Any]], max_score: float) -> dict[str, Any]:
    """Convert keyword search results to parsed.ranked_models format for frontend."""
    ranked = []
    for r in keyword_results[:5]:
        score = r.get("score") or 0
        conf = min(1.0, (score / max(max_score, 1)) * 0.95) if max_score > 0 else 0.5
        ranked.append({
            "name": r.get("name", "?"),
            "confidence": round(conf, 2),
            "rationale": "; ".join(r.get("reasons") or [])[:200],
        })
    return {"ranked_models": ranked, "caveats": "", "shape_read": None}


@app.post("/api/ai/identify")
async def api_ai_identify(
    model: str = Form(...),
    description: str = Form(""),
    include_shape_hint: str = Form("false"),
    image: Optional[UploadFile] = File(None),
):
    """
    Vision-assisted identification: Hu silhouette → optional early exit → vision describes blade →
    keyword search with that description → LLM rerank if ambiguous.
    Uses deterministic keyword scoring (which excels at traits like carbon fiber / Ultra) plus
    vision for perception and LLM for disambiguation.
    """
    image_bytes: Optional[bytes] = None
    if image and getattr(image, "filename", None):
        image_bytes = await image.read()
        if len(image_bytes) > 15 * 1024 * 1024:
            raise HTTPException(status_code=413, detail="Image larger than 15MB")

    if not image_bytes and not (description or "").strip():
        raise HTTPException(
            status_code=400,
            detail="Provide a written description and/or a photo.",
        )

    model_ok, model_err = blade_ai.check_ollama_model(model)
    if not model_ok:
        raise HTTPException(
            status_code=400,
            detail=model_err or "Selected model is not available in Ollama.",
        )

    with get_conn() as conn:
        master_hu_rows = conn.execute(
            """
            SELECT id, name, identifier_silhouette_hu_json
            FROM master_knives
            WHERE status != 'archived'
              AND identifier_silhouette_hu_json IS NOT NULL
              AND trim(identifier_silhouette_hu_json) != ''
            ORDER BY name COLLATE NOCASE
            """
        ).fetchall()
        tpl_rows = conn.execute(
            "SELECT slug, name, hu_json FROM blade_shape_templates"
        ).fetchall()

    catalog_templates = []
    for r in master_hu_rows:
        try:
            hu_list = json.loads(r["identifier_silhouette_hu_json"])
            if blade_ai.is_hu_vector_degenerate(hu_list):
                continue
        except (json.JSONDecodeError, TypeError):
            continue
        catalog_templates.append({
            "slug": f"catalog-{r['id']}",
            "name": r["name"],
            "hu_json": r["identifier_silhouette_hu_json"],
        })
    generic_templates = [dict(r) for r in tpl_rows]
    combined_templates = catalog_templates + generic_templates

    shape_hints: list[dict[str, Any]] = []
    shape_err: Optional[str] = None
    if image_bytes:
        shape_hints, shape_err = blade_ai.silhouette_hints_from_image(
            image_bytes, combined_templates
        )

    # Step 1–2: Strong Hu match → early exit
    if shape_hints:
        top = shape_hints[0]
        top_slug = top.get("slug") or ""
        top_name = top.get("name") or ""
        top_dist = top.get("distance", 999)
        is_catalog_match = isinstance(top_slug, str) and top_slug.startswith("catalog-")
        if is_catalog_match and top_name and top_dist < 1.0:
            payload = IdentifierQuery(q=top_name, include_archived=False)
            kw_resp = identify_knives(payload)
            results = kw_resp.get("results") or []
            if results and results[0].get("name") == top_name:
                max_score = results[0].get("score") or 1
                parsed = _keyword_results_to_parsed(results, max_score)
                parsed["caveats"] = f"Strong silhouette match (Hu distance {top_dist})."
                return {
                    "ollama_host": blade_ai.OLLAMA_HOST,
                    "model": model,
                    "raw_response": f"Early exit: Hu match to {top_name}",
                    "parsed": parsed,
                    "shape_hints": shape_hints,
                    "shape_hint_error": shape_err,
                    "pipeline": "hu_early_exit",
                }

    # Step 3: Vision describes blade for search
    vision_desc: dict[str, Any] = {}
    if image_bytes:
        try:
            img_b64 = base64.standard_b64encode(image_bytes).decode("ascii")
            vision_desc = blade_ai.vision_describe_knife(model, img_b64, description)
        except Exception as e:
            vision_desc = {"keywords": str(e)[:100]}

    # Step 4: Keyword search with vision output + user description
    q_parts = []
    if vision_desc.get("keywords"):
        q_parts.append(str(vision_desc["keywords"]).strip())
    if vision_desc.get("handle_material"):
        q_parts.append(str(vision_desc["handle_material"]).strip())
    if vision_desc.get("distinctive"):
        q_parts.append(str(vision_desc["distinctive"]).strip())
    if (description or "").strip():
        q_parts.append(description.strip())
    q_str = " ".join(q_parts) if q_parts else (description or "").strip() or "knife"

    bl_len = None
    if vision_desc.get("blade_length_inches") is not None:
        try:
            bl_len = float(vision_desc["blade_length_inches"])
        except (TypeError, ValueError):
            pass

    payload = IdentifierQuery(
        q=q_str,
        blade_shape=vision_desc.get("blade_shape") or None,
        blade_length=bl_len,
        finish=vision_desc.get("blade_finish") or None,
        include_archived=False,
    )
    kw_resp = identify_knives(payload)
    results = kw_resp.get("results") or []
    max_score = results[0].get("score", 0) if results else 1
    second_score = results[1].get("score", 0) if len(results) > 1 else 0

    # Step 5: Clear winner (top >> second) → return keyword results
    score_gap = max_score - second_score
    if not results or (len(results) == 1) or (score_gap >= 15 and max_score >= 20):
        parsed = _keyword_results_to_parsed(results, max_score)
        if vision_desc:
            parsed["shape_read"] = vision_desc.get("keywords", "")
        return {
            "ollama_host": blade_ai.OLLAMA_HOST,
            "model": model,
            "raw_response": f"Keyword search (q={q_str!r})",
            "parsed": parsed,
            "shape_hints": shape_hints,
            "shape_hint_error": shape_err,
            "pipeline": "keyword",
            "vision_description": vision_desc,
        }

    # Step 6: Ambiguous → LLM rerank with top candidates + full catalog data
    with get_conn() as conn:
        full_cols = (
            "id, name, category, catalog_line, blade_profile, blade_shape, default_blade_length, "
            "default_handle_color, default_blade_color, "
            "collector_notes, evidence_summary, identifier_keywords, identifier_distinguishing_features"
        )
        top_names = [r["name"] for r in results[:6]]
        placeholders = ",".join("?" * len(top_names))
        cand_rows = conn.execute(
            f"SELECT {full_cols} FROM master_knives WHERE name IN ({placeholders})",
            top_names,
        ).fetchall()
    cand_full = [dict(r) for r in cand_rows]
    vision_text = json.dumps(vision_desc) if vision_desc else ""
    if (description or "").strip():
        vision_text = f"User: {description}\nVision: {vision_text}"
    rerank_prompt = blade_ai.build_rerank_prompt(cand_full, vision_text)

    try:
        raw = blade_ai.ollama_chat(model, blade_ai.RERANK_SYSTEM, rerank_prompt)
    except (httpx.HTTPStatusError, httpx.HTTPError) as exc:
        err = str(exc)
        try:
            if hasattr(exc, "response") and exc.response.json():
                err = exc.response.json().get("error", err)
        except Exception:
            pass
        parsed = _keyword_results_to_parsed(results, max_score)
        parsed["caveats"] = f"Rerank failed ({err}); showing keyword results."
        return {
            "ollama_host": blade_ai.OLLAMA_HOST,
            "model": model,
            "raw_response": "",
            "parsed": parsed,
            "shape_hints": shape_hints,
            "shape_hint_error": shape_err,
            "pipeline": "keyword_fallback",
        }

    parsed = blade_ai.try_parse_json_response(raw)
    if not parsed or not parsed.get("ranked_models"):
        parsed = _keyword_results_to_parsed(results, max_score)
        parsed["caveats"] = (parsed.get("caveats") or "") + " LLM rerank produced no models."
    return {
        "ollama_host": blade_ai.OLLAMA_HOST,
        "model": model,
        "raw_response": raw,
        "parsed": parsed,
        "shape_hints": shape_hints,
        "shape_hint_error": shape_err,
        "pipeline": "rerank",
        "vision_description": vision_desc,
    }


# -----------------------------------------------------------------------------
# API v2 — Flattened reads from normalized tables (mkc_ui_rewire_spec.md)
# -----------------------------------------------------------------------------


def _v2_inventory_base_sql() -> str:
    """Base SQL for flattened inventory from v2 tables with model dimension joins."""
    return """
        SELECT
            i.id,
            i.knife_model_id,
            i.nickname,
            i.quantity,
            i.acquired_date,
            i.mkc_order_number,
            i.purchase_price,
            i.estimated_value,
            i.condition,
            i.location,
            i.serial_number,
            i.purchase_source,
            i.last_sharpened,
            i.notes,
            COALESCE(NULLIF(i.collaboration_name, ''), c.name) AS collaboration_name,
            c.name AS collaborator_name,
            COALESCE(i.steel, km.steel) AS blade_steel,
            COALESCE(i.blade_finish, km.blade_finish) AS blade_finish,
            COALESCE(i.blade_color, km.blade_color) AS blade_color,
            COALESCE(i.handle_color, km.handle_color) AS handle_color,
            COALESCE(i.blade_length, km.blade_length) AS blade_length,
            km.official_name AS knife_name,
            fam.name AS knife_family,
            COALESCE(ks.name, NULLIF(i.collaboration_name, ''), c.name) AS catalog_line,
            kt.name AS knife_type,
            frm.name AS form_name,
            ks.name AS series_name,
            (COALESCE(NULLIF(i.collaboration_name, ''), c.name) IS NOT NULL) AS is_collab,
            (CASE WHEN kmi.image_blob IS NOT NULL AND length(kmi.image_blob) > 0
              THEN 1 ELSE 0 END) AS has_identifier_image
        FROM inventory_items_v2 i
        LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
        LEFT JOIN knife_types kt ON kt.id = km.type_id
        LEFT JOIN knife_forms frm ON frm.id = km.form_id
        LEFT JOIN knife_families fam ON fam.id = km.family_id
        LEFT JOIN knife_series ks ON ks.id = km.series_id
        LEFT JOIN collaborators c ON c.id = km.collaborator_id
        LEFT JOIN knife_model_images kmi ON kmi.knife_model_id = i.knife_model_id
    """


@app.get("/api/v2/inventory")
def v2_list_inventory(
    search: Optional[str] = None,
    type: Optional[str] = None,
    family: Optional[str] = None,
    form: Optional[str] = None,
    series: Optional[str] = None,
    steel: Optional[str] = None,
    finish: Optional[str] = None,
    handle_color: Optional[str] = None,
    condition: Optional[str] = None,
    location: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Return flattened inventory rows from v2 tables. Supports server-side filters."""
    with get_conn() as conn:
        base = _v2_inventory_base_sql()
        conditions = []
        params: list[Any] = []

        if search and search.strip():
            q = f"%{search.strip()}%"
            conditions.append(
                "(km.official_name LIKE ? OR km.normalized_name LIKE ? OR fam.name LIKE ? "
                "OR i.nickname LIKE ? OR i.serial_number LIKE ? OR i.notes LIKE ?)"
            )
            params.extend([q, q, q, q, q, q])

        if type and type.strip():
            conditions.append("kt.name = ?")
            params.append(type.strip())

        if family and family.strip():
            conditions.append("fam.name = ?")
            params.append(family.strip())

        if form and form.strip():
            conditions.append("frm.name = ?")
            params.append(form.strip())

        if series and series.strip():
            conditions.append("ks.name = ?")
            params.append(series.strip())

        if steel and steel.strip():
            conditions.append("(COALESCE(i.steel, km.steel) = ? OR i.steel = ? OR km.steel = ?)")
            params.extend([steel.strip(), steel.strip(), steel.strip()])

        if finish and finish.strip():
            conditions.append("(COALESCE(i.blade_finish, km.blade_finish) = ?)")
            params.append(finish.strip())

        if handle_color and handle_color.strip():
            conditions.append("(COALESCE(i.handle_color, km.handle_color) = ?)")
            params.append(handle_color.strip())

        if condition and condition.strip():
            conditions.append("(i.condition = ? OR (i.condition IS NULL AND ? = 'Like New'))")
            params.extend([condition.strip(), condition.strip()])

        if location and location.strip():
            conditions.append("i.location LIKE ?")
            params.append(f"%{location.strip()}%")

        where_sql = " AND ".join(conditions) if conditions else "1=1"
        sql = f"{base} WHERE {where_sql} ORDER BY km.sortable_name COLLATE NOCASE, i.id DESC"
        rows = conn.execute(sql, params).fetchall()
        return rows


@app.get("/api/v2/inventory/summary")
def v2_inventory_summary() -> dict[str, Any]:
    """Return inventory summary: rows, total quantity, spend, value, master count, by_family."""
    with get_conn() as conn:
        summary = conn.execute(
            """
            SELECT
                COUNT(*) AS inventory_rows,
                COALESCE(SUM(i.quantity), 0) AS total_quantity,
                COALESCE(SUM(COALESCE(i.purchase_price, 0) * i.quantity), 0) AS total_spend,
                COALESCE(SUM(COALESCE(i.estimated_value, 0) * i.quantity), 0) AS estimated_value
            FROM inventory_items_v2 i
            """
        ).fetchone()
        master_models = conn.execute(
            "SELECT COUNT(DISTINCT knife_model_id) AS c FROM inventory_items_v2 WHERE knife_model_id IS NOT NULL"
        ).fetchone()["c"]
        by_family = conn.execute(
            """
            SELECT
                COALESCE(NULLIF(TRIM(fam.name), ''), 'Uncategorized') AS family,
                COUNT(*) AS inventory_rows,
                COALESCE(SUM(i.quantity), 0) AS total_quantity
            FROM inventory_items_v2 i
            LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
            LEFT JOIN knife_families fam ON fam.id = km.family_id
            GROUP BY family
            ORDER BY total_quantity DESC, family COLLATE NOCASE
            """
        ).fetchall()
        return {
            "inventory_rows": summary["inventory_rows"],
            "total_quantity": summary["total_quantity"],
            "total_spend": summary["total_spend"],
            "estimated_value": summary["estimated_value"],
            "master_models": master_models,
            "master_count": master_models,
            "by_family": by_family,
        }


@app.get("/api/v2/inventory/filters")
def v2_inventory_filters() -> dict[str, list[str]]:
    """Return distinct filter values for inventory dropdowns from v2 data."""
    with get_conn() as conn:
        type_vals = conn.execute(
            """SELECT DISTINCT kt.name FROM inventory_items_v2 i
               LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
               LEFT JOIN knife_types kt ON kt.id = km.type_id
               WHERE kt.name IS NOT NULL AND kt.name != '' ORDER BY kt.name"""
        ).fetchall()
        family_vals = conn.execute(
            """SELECT DISTINCT fam.name FROM inventory_items_v2 i
               LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
               LEFT JOIN knife_families fam ON fam.id = km.family_id
               WHERE fam.name IS NOT NULL AND fam.name != '' ORDER BY fam.name"""
        ).fetchall()
        form_vals = conn.execute(
            """SELECT DISTINCT frm.name FROM inventory_items_v2 i
               LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
               LEFT JOIN knife_forms frm ON frm.id = km.form_id
               WHERE frm.name IS NOT NULL AND frm.name != '' ORDER BY frm.name"""
        ).fetchall()
        series_vals = conn.execute(
            """SELECT DISTINCT ks.name FROM inventory_items_v2 i
               LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
               LEFT JOIN knife_series ks ON ks.id = km.series_id
               WHERE ks.name IS NOT NULL AND ks.name != '' ORDER BY ks.name"""
        ).fetchall()
        steel_vals = conn.execute(
            """SELECT DISTINCT COALESCE(i.steel, km.steel) AS v FROM inventory_items_v2 i
               LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
               WHERE COALESCE(i.steel, km.steel) IS NOT NULL AND COALESCE(i.steel, km.steel) != ''
               ORDER BY v"""
        ).fetchall()
        finish_vals = conn.execute(
            """SELECT DISTINCT COALESCE(i.blade_finish, km.blade_finish) AS v FROM inventory_items_v2 i
               LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
               WHERE COALESCE(i.blade_finish, km.blade_finish) IS NOT NULL
               ORDER BY v"""
        ).fetchall()
        handle_vals = conn.execute(
            """SELECT DISTINCT COALESCE(i.handle_color, km.handle_color) AS v FROM inventory_items_v2 i
               LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
               WHERE COALESCE(i.handle_color, km.handle_color) IS NOT NULL
               ORDER BY v"""
        ).fetchall()
        blade_color_vals = conn.execute(
            """SELECT DISTINCT COALESCE(i.blade_color, km.blade_color) AS v FROM inventory_items_v2 i
               LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
               WHERE COALESCE(i.blade_color, km.blade_color) IS NOT NULL
               ORDER BY v"""
        ).fetchall()
        cond_vals = conn.execute(
            "SELECT DISTINCT COALESCE(condition, 'Like New') AS v FROM inventory_items_v2 ORDER BY v"
        ).fetchall()
        loc_vals = conn.execute(
            "SELECT DISTINCT location FROM inventory_items_v2 WHERE location IS NOT NULL AND location != '' ORDER BY location"
        ).fetchall()

        def pluck(rows: list, key: str = "name") -> list[str]:
            out = []
            for r in rows:
                v = r.get(key) or r.get("v")
                if v and str(v).strip() and str(v).strip() not in out:
                    out.append(str(v).strip())
            return out

        return {
            "type": pluck(type_vals, "name"),
            "family": pluck(family_vals, "name"),
            "form": pluck(form_vals, "name"),
            "series": pluck(series_vals, "name"),
            "steel": pluck(steel_vals, "v"),
            "finish": pluck(finish_vals, "v"),
            "handle_color": pluck(handle_vals, "v"),
            "blade_color": pluck(blade_color_vals, "v"),
            "condition": pluck(cond_vals, "v"),
            "location": pluck(loc_vals, "location"),
        }


@app.get("/api/v2/catalog")
def v2_list_catalog(
    search: Optional[str] = None,
    type: Optional[str] = None,
    family: Optional[str] = None,
    form: Optional[str] = None,
    series: Optional[str] = None,
    collaboration: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Return flattened model data from knife_models_v2. Supports filters."""
    with get_conn() as conn:
        conditions = []
        params: list[Any] = []

        if search and search.strip():
            q = f"%{search.strip()}%"
            conditions.append(
                "(km.official_name LIKE ? OR km.normalized_name LIKE ? OR fam.name LIKE ? OR km.slug LIKE ?)"
            )
            params.extend([q, q, q, q])

        if type and type.strip():
            conditions.append("kt.name = ?")
            params.append(type.strip())

        if family and family.strip():
            conditions.append("fam.name = ?")
            params.append(family.strip())

        if form and form.strip():
            conditions.append("frm.name = ?")
            params.append(form.strip())

        if series and series.strip():
            conditions.append("ks.name = ?")
            params.append(series.strip())

        if collaboration and collaboration.strip():
            conditions.append("c.name = ?")
            params.append(collaboration.strip())

        where_sql = " AND ".join(conditions) if conditions else "1=1"
        rows = conn.execute(
            f"""
            SELECT km.id, km.parent_model_id, km.official_name, km.normalized_name, km.sortable_name, km.slug,
                   kt.name AS knife_type, fam.name AS family_name, frm.name AS form_name,
                   ks.name AS series_name, c.name AS collaborator_name,
                   km.generation_label, km.size_modifier, km.platform_variant,
                   km.steel, km.blade_finish, km.blade_color, km.handle_color, km.handle_type, km.blade_length,
                   km.record_status, km.is_current_catalog, km.is_discontinued, km.msrp,
                   km.official_product_url, km.official_image_url,
                   (SELECT COUNT(*) FROM inventory_items_v2 WHERE knife_model_id = km.id) AS in_inventory_count,
                   (CASE WHEN kmi.image_blob IS NOT NULL AND length(kmi.image_blob) > 0 THEN 1 ELSE 0 END) AS has_identifier_image
            FROM knife_models_v2 km
            LEFT JOIN knife_model_images kmi ON kmi.knife_model_id = km.id
            LEFT JOIN knife_types kt ON kt.id = km.type_id
            LEFT JOIN knife_forms frm ON frm.id = km.form_id
            LEFT JOIN knife_families fam ON fam.id = km.family_id
            LEFT JOIN knife_series ks ON ks.id = km.series_id
            LEFT JOIN collaborators c ON c.id = km.collaborator_id
            WHERE {where_sql}
            ORDER BY km.sortable_name COLLATE NOCASE, km.official_name
            """,
            params,
        ).fetchall()
        return rows


@app.get("/api/v2/catalog/filters")
def v2_catalog_filters() -> dict[str, list[str]]:
    """Return distinct filter values for catalog dropdowns."""
    with get_conn() as conn:
        type_vals = conn.execute(
            "SELECT DISTINCT kt.name FROM knife_models_v2 km "
            "LEFT JOIN knife_types kt ON kt.id = km.type_id WHERE kt.name IS NOT NULL ORDER BY kt.name"
        ).fetchall()
        family_vals = conn.execute(
            "SELECT DISTINCT fam.name FROM knife_models_v2 km "
            "LEFT JOIN knife_families fam ON fam.id = km.family_id WHERE fam.name IS NOT NULL ORDER BY fam.name"
        ).fetchall()
        form_vals = conn.execute(
            "SELECT DISTINCT frm.name FROM knife_models_v2 km "
            "LEFT JOIN knife_forms frm ON frm.id = km.form_id WHERE frm.name IS NOT NULL ORDER BY frm.name"
        ).fetchall()
        series_vals = conn.execute(
            "SELECT DISTINCT ks.name FROM knife_models_v2 km "
            "LEFT JOIN knife_series ks ON ks.id = km.series_id WHERE ks.name IS NOT NULL ORDER BY ks.name"
        ).fetchall()
        collab_vals = conn.execute(
            "SELECT DISTINCT c.name FROM knife_models_v2 km "
            "LEFT JOIN collaborators c ON c.id = km.collaborator_id WHERE c.name IS NOT NULL ORDER BY c.name"
        ).fetchall()

        def pluck(rows: list, key: str = "name") -> list[str]:
            return [str(r[key]).strip() for r in rows if r.get(key) and str(r.get(key)).strip()]

        return {
            "type": pluck(type_vals),
            "family": pluck(family_vals),
            "form": pluck(form_vals),
            "series": pluck(series_vals),
            "collaboration": pluck(collab_vals),
        }


@app.get("/api/v2/models/by-legacy-master/{legacy_id}")
def v2_model_by_legacy_master(legacy_id: int) -> dict[str, Any]:
    """Resolve legacy master_knives.id to v2 model for ?add= flow from Identify page."""
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT km.id, km.official_name, km.normalized_name, km.slug,
                   kt.name AS knife_type, fam.name AS family_name, frm.name AS form_name,
                   ks.name AS series_name, c.name AS collaborator_name,
                   km.steel, km.blade_finish, km.blade_color, km.handle_color, km.handle_type, km.blade_length, km.msrp
            FROM knife_models_v2 km
            LEFT JOIN knife_types kt ON kt.id = km.type_id
            LEFT JOIN knife_forms frm ON frm.id = km.form_id
            LEFT JOIN knife_families fam ON fam.id = km.family_id
            LEFT JOIN knife_series ks ON ks.id = km.series_id
            LEFT JOIN collaborators c ON c.id = km.collaborator_id
            WHERE km.legacy_master_id = ?
            """,
            (legacy_id,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="No v2 model for legacy master id.")
        return row


@app.get("/api/v2/models/search")
def v2_models_search(q: Optional[str] = None, limit: int = 50) -> list[dict[str, Any]]:
    """Search knife_models_v2 for model picker. Returns flattened rows."""
    with get_conn() as conn:
        params: list[Any] = []
        where = "1=1"
        if q and q.strip():
            search_term = f"%{q.strip()}%"
            where = "(km.official_name LIKE ? OR km.normalized_name LIKE ? OR fam.name LIKE ? OR km.slug LIKE ?)"
            params = [search_term, search_term, search_term, search_term]
        params.append(limit)
        rows = conn.execute(
            f"""
            SELECT km.id, km.official_name, km.normalized_name, km.slug,
                   kt.name AS knife_type, fam.name AS family_name, frm.name AS form_name,
                   ks.name AS series_name, c.name AS collaborator_name,
                   km.steel, km.blade_finish, km.blade_color, km.handle_color, km.handle_type, km.blade_length
            FROM knife_models_v2 km
            LEFT JOIN knife_types kt ON kt.id = km.type_id
            LEFT JOIN knife_forms frm ON frm.id = km.form_id
            LEFT JOIN knife_families fam ON fam.id = km.family_id
            LEFT JOIN knife_series ks ON ks.id = km.series_id
            LEFT JOIN collaborators c ON c.id = km.collaborator_id
            WHERE {where}
            ORDER BY km.sortable_name COLLATE NOCASE
            LIMIT ?
            """,
            params,
        ).fetchall()
        return rows


class V2ModelIn(BaseModel):
    model_config = {"extra": "ignore"}

    official_name: str = Field(min_length=1, max_length=200)
    canonical_slug: Optional[str] = None
    normalized_name: Optional[str] = None
    knife_type: Optional[str] = None
    form_name: Optional[str] = None
    family_name: Optional[str] = None
    series_name: Optional[str] = None
    collaborator_name: Optional[str] = None
    generation_label: Optional[str] = None
    size_modifier: Optional[str] = None
    platform_variant: Optional[str] = None
    steel: Optional[str] = None
    blade_finish: Optional[str] = None
    blade_color: Optional[str] = None
    handle_color: Optional[str] = None
    handle_type: Optional[str] = None
    blade_length: Optional[float] = None
    record_status: Optional[str] = "active"
    is_current_catalog: Optional[bool] = True
    is_discontinued: Optional[bool] = False
    msrp: Optional[float] = None
    official_product_url: Optional[str] = None
    official_image_url: Optional[str] = None
    notes: Optional[str] = None
    parent_model_id: Optional[int] = None
    distinguishing_features: Optional[str] = None

    @field_validator("knife_type", mode="before")
    @classmethod
    def normalize_knife_type(cls, v: Any) -> Optional[str]:
        return normalize_category_value(v)


def _require_v2_identity(payload: V2ModelIn) -> None:
    missing = []
    if not (payload.official_name or "").strip():
        missing.append("official_name")
    if not (payload.knife_type or "").strip():
        missing.append("knife_type")
    if not (payload.form_name or "").strip():
        missing.append("form_name")
    if not (payload.family_name or "").strip():
        missing.append("family_name")
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required identity fields: {', '.join(missing)}",
        )


def _v2_option_exists(conn: sqlite3.Connection, option_type: str, value: Optional[str]) -> bool:
    if not value or not str(value).strip():
        return True
    row = conn.execute(
        "SELECT 1 FROM v2_option_values WHERE option_type = ? AND lower(name) = lower(?) LIMIT 1",
        (option_type, str(value).strip()),
    ).fetchone()
    return row is not None


def _validate_v2_controlled_identity(payload: V2ModelIn, conn: sqlite3.Connection) -> None:
    controlled = [
        ("collaborator_name", "collaborators", payload.collaborator_name),
        ("generation_label", "generations", payload.generation_label),
        ("size_modifier", "size-modifiers", payload.size_modifier),
        ("platform_variant", "platform-variants", payload.platform_variant),
        ("handle_type", "handle-types", payload.handle_type),
    ]
    invalid = [field for field, option_type, value in controlled if not _v2_option_exists(conn, option_type, value)]
    if invalid:
        raise HTTPException(
            status_code=400,
            detail=(
                "Controlled fields contain values not in allowed options: "
                + ", ".join(invalid)
                + ". Add option first, then save."
            ),
        )


def _v2_dim_id(conn: sqlite3.Connection, table: str, name: Optional[str]) -> Optional[int]:
    if not name or not str(name).strip():
        return None
    n = str(name).strip()
    row = conn.execute(f"SELECT id FROM {table} WHERE name = ?", (n,)).fetchone()
    if row:
        return row["id"]
    if table == "knife_types":
        cur = conn.execute(
            "INSERT INTO knife_types (name, slug, sort_order) VALUES (?, ?, ?)",
            (n, normalized_model.slugify(n), 999),
        )
        return cur.lastrowid
    if table == "knife_forms":
        cur = conn.execute(
            "INSERT INTO knife_forms (name, slug) VALUES (?, ?)",
            (n, normalized_model.slugify(n)),
        )
        return cur.lastrowid
    if table == "knife_families":
        cur = conn.execute(
            "INSERT INTO knife_families (name, normalized_name, slug) VALUES (?, ?, ?)",
            (n, n, normalized_model.slugify(n)),
        )
        return cur.lastrowid
    if table == "knife_series":
        cur = conn.execute(
            "INSERT INTO knife_series (name, slug) VALUES (?, ?)",
            (n, normalized_model.slugify(n)),
        )
        return cur.lastrowid
    if table == "collaborators":
        cur = conn.execute(
            "INSERT INTO collaborators (name, slug) VALUES (?, ?)",
            (n, normalized_model.slugify(n)),
        )
        return cur.lastrowid
    return None


def _v2_model_slug(conn: sqlite3.Connection, base_name: str, existing_id: Optional[int] = None) -> str:
    base = normalized_model.slugify(base_name) or "model"
    slug = base
    i = 2
    while True:
        if existing_id is None:
            row = conn.execute("SELECT id FROM knife_models_v2 WHERE slug = ?", (slug,)).fetchone()
        else:
            row = conn.execute("SELECT id FROM knife_models_v2 WHERE slug = ? AND id != ?", (slug, existing_id)).fetchone()
        if not row:
            return slug
        slug = f"{base}-{i}"
        i += 1


@app.get("/api/v2/models/{model_id}")
def v2_get_model(model_id: int) -> dict[str, Any]:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT km.id, km.parent_model_id, km.official_name, km.normalized_name, km.sortable_name, km.slug,
                   kt.name AS knife_type, fam.name AS family_name, frm.name AS form_name,
                   ks.name AS series_name, c.name AS collaborator_name,
                   km.generation_label, km.size_modifier, km.platform_variant, km.steel, km.blade_finish,
                   km.blade_color, km.handle_color, km.handle_type, km.blade_length, km.record_status, km.is_current_catalog,
                   km.is_discontinued, km.msrp, km.official_product_url, km.official_image_url, km.notes,
                   d.distinguishing_features,
                   (CASE WHEN kmi.image_blob IS NOT NULL AND length(kmi.image_blob) > 0 THEN 1 ELSE 0 END) AS has_identifier_image
            FROM knife_models_v2 km
            LEFT JOIN knife_types kt ON kt.id = km.type_id
            LEFT JOIN knife_forms frm ON frm.id = km.form_id
            LEFT JOIN knife_families fam ON fam.id = km.family_id
            LEFT JOIN knife_series ks ON ks.id = km.series_id
            LEFT JOIN collaborators c ON c.id = km.collaborator_id
            LEFT JOIN knife_model_images kmi ON kmi.knife_model_id = km.id
            LEFT JOIN knife_model_descriptors d ON d.knife_model_id = km.id
            WHERE km.id = ?
            """,
            (model_id,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Model not found.")
        return row


@app.post("/api/v2/models")
def v2_create_model(payload: V2ModelIn):
    _require_v2_identity(payload)
    with get_conn() as conn:
        _validate_v2_controlled_identity(payload, conn)
        type_id = _v2_dim_id(conn, "knife_types", payload.knife_type)
        form_id = _v2_dim_id(conn, "knife_forms", payload.form_name)
        family_id = _v2_dim_id(conn, "knife_families", payload.family_name)
        series_id = _v2_dim_id(conn, "knife_series", payload.series_name)
        collaborator_id = _v2_dim_id(conn, "collaborators", payload.collaborator_name)
        normalized_name = (payload.normalized_name or payload.official_name).strip()
        sortable_name = normalized_name
        slug = _v2_model_slug(conn, payload.canonical_slug or normalized_name)
        cur = conn.execute(
            """
            INSERT INTO knife_models_v2 (
                official_name, normalized_name, sortable_name, slug, type_id, form_id, family_id, series_id, collaborator_id,
                parent_model_id, generation_label, size_modifier, platform_variant, steel, blade_finish, blade_color, handle_color, handle_type,
                blade_length, record_status, is_current_catalog, is_discontinued, msrp, official_product_url, official_image_url, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload.official_name.strip(), normalized_name, sortable_name, slug, type_id, form_id, family_id, series_id, collaborator_id,
                payload.parent_model_id, payload.generation_label, payload.size_modifier, payload.platform_variant,
                payload.steel, payload.blade_finish, payload.blade_color, payload.handle_color, payload.handle_type, payload.blade_length,
                payload.record_status or "active", 0 if payload.is_current_catalog is False else 1,
                1 if payload.is_discontinued else 0, payload.msrp, payload.official_product_url, payload.official_image_url, payload.notes,
            ),
        )
        if payload.distinguishing_features is not None:
            conn.execute(
                """
                INSERT INTO knife_model_descriptors (knife_model_id, distinguishing_features, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(knife_model_id) DO UPDATE SET
                    distinguishing_features=excluded.distinguishing_features, updated_at=CURRENT_TIMESTAMP
                """,
                (cur.lastrowid, payload.distinguishing_features),
            )
        return {"id": cur.lastrowid, "message": "Created"}


@app.put("/api/v2/models/{model_id}")
def v2_update_model(model_id: int, payload: V2ModelIn):
    _require_v2_identity(payload)
    with get_conn() as conn:
        _validate_v2_controlled_identity(payload, conn)
        exists = conn.execute("SELECT id FROM knife_models_v2 WHERE id = ?", (model_id,)).fetchone()
        if not exists:
            raise HTTPException(status_code=404, detail="Model not found.")
        type_id = _v2_dim_id(conn, "knife_types", payload.knife_type)
        form_id = _v2_dim_id(conn, "knife_forms", payload.form_name)
        family_id = _v2_dim_id(conn, "knife_families", payload.family_name)
        series_id = _v2_dim_id(conn, "knife_series", payload.series_name)
        collaborator_id = _v2_dim_id(conn, "collaborators", payload.collaborator_name)
        normalized_name = (payload.normalized_name or payload.official_name).strip()
        slug = _v2_model_slug(conn, payload.canonical_slug or normalized_name, existing_id=model_id)
        conn.execute(
            """
            UPDATE knife_models_v2
            SET official_name = ?, normalized_name = ?, sortable_name = ?, slug = ?,
                type_id = ?, form_id = ?, family_id = ?, series_id = ?, collaborator_id = ?, parent_model_id = ?,
                generation_label = ?, size_modifier = ?, platform_variant = ?, steel = ?, blade_finish = ?,
                blade_color = ?, handle_color = ?, handle_type = ?, blade_length = ?, record_status = ?, is_current_catalog = ?,
                is_discontinued = ?, msrp = ?, official_product_url = ?, official_image_url = ?, notes = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                payload.official_name.strip(), normalized_name, normalized_name, slug,
                type_id, form_id, family_id, series_id, collaborator_id, payload.parent_model_id,
                payload.generation_label, payload.size_modifier, payload.platform_variant, payload.steel, payload.blade_finish,
                payload.blade_color, payload.handle_color, payload.handle_type, payload.blade_length, payload.record_status or "active",
                0 if payload.is_current_catalog is False else 1, 1 if payload.is_discontinued else 0,
                payload.msrp, payload.official_product_url, payload.official_image_url, payload.notes, model_id,
            ),
        )
        if payload.distinguishing_features is not None:
            conn.execute(
                """
                INSERT INTO knife_model_descriptors (knife_model_id, distinguishing_features, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(knife_model_id) DO UPDATE SET
                    distinguishing_features=excluded.distinguishing_features, updated_at=CURRENT_TIMESTAMP
                """,
                (model_id, payload.distinguishing_features),
            )
        return {"message": "Updated"}


@app.delete("/api/v2/models/{model_id}")
def v2_delete_model(model_id: int):
    with get_conn() as conn:
        used = conn.execute("SELECT COUNT(*) AS c FROM inventory_items_v2 WHERE knife_model_id = ?", (model_id,)).fetchone()["c"]
        if used > 0:
            raise HTTPException(status_code=400, detail="Cannot delete model used by inventory.")
        cur = conn.execute("DELETE FROM knife_models_v2 WHERE id = ?", (model_id,))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Model not found.")
        return {"message": "Deleted"}


@app.post("/api/v2/models/{model_id}/duplicate")
def v2_duplicate_model(model_id: int):
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM knife_models_v2 WHERE id = ?", (model_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Model not found.")
        name = (row.get("official_name") or "").strip()
        new_name = f"{name} (copy)" if name else "Copy"
        slug = _v2_model_slug(conn, new_name)
        cur = conn.execute(
            """
            INSERT INTO knife_models_v2 (
                legacy_master_id, official_name, normalized_name, sortable_name, slug, type_id, form_id, family_id, series_id,
                collaborator_id, parent_model_id, generation_label, size_modifier, platform_variant, steel, blade_finish,
                blade_color, handle_color, handle_type, blade_length, record_status, is_current_catalog, is_discontinued, msrp,
                official_product_url, official_image_url, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                # v2 schema marks `legacy_master_id` as UNIQUE; duplicates should not point at the same legacy master.
                None, new_name, row.get("normalized_name") or new_name, row.get("sortable_name") or new_name, slug,
                row.get("type_id"), row.get("form_id"), row.get("family_id"), row.get("series_id"), row.get("collaborator_id"),
                row.get("parent_model_id"), row.get("generation_label"), row.get("size_modifier"), row.get("platform_variant"),
                row.get("steel"), row.get("blade_finish"), row.get("blade_color"), row.get("handle_color"), row.get("handle_type"), row.get("blade_length"),
                row.get("record_status"), row.get("is_current_catalog"), row.get("is_discontinued"), row.get("msrp"),
                row.get("official_product_url"), row.get("official_image_url"), row.get("notes"),
            ),
        )
        return {"id": cur.lastrowid, "message": "Duplicated"}


@app.get("/api/v2/models/{model_id}/image")
def v2_get_model_image(model_id: int):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT image_blob, image_mime FROM knife_model_images WHERE knife_model_id = ?",
            (model_id,),
        ).fetchone()
        if not row or not row.get("image_blob"):
            raise HTTPException(status_code=404, detail="No stored reference image for this model.")
        return Response(content=row["image_blob"], media_type=(row.get("image_mime") or "image/jpeg"))


@app.post("/api/v2/models/{model_id}/image")
async def v2_upload_model_image(
    model_id: int,
    file: UploadFile = File(...),
    model: Optional[str] = Form(None),
):
    raw = await file.read()
    if len(raw) > 15 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="Image larger than 15MB")
    mime = ((file.content_type or "").split(";")[0].strip().lower() or "image/jpeg")
    if not mime.startswith("image/"):
        mime = "image/jpeg"
    hu_list, hu_err = blade_ai.extract_blade_hu_from_image_bytes(raw)
    hu_json = json.dumps(hu_list) if hu_list else None
    dist_features: Optional[str] = None
    dist_error: Optional[str] = None
    vision_model = (model or "").strip() or OLLAMA_VISION_MODEL
    if vision_model:
        model_ok, _ = blade_ai.check_ollama_model(vision_model)
        if model_ok:
            img_b64 = base64.standard_b64encode(raw).decode("ascii")
            dist_features, dist_error = blade_ai.extract_distinguishing_features_from_image(vision_model, img_b64)
    with get_conn() as conn:
        exists = conn.execute("SELECT id FROM knife_models_v2 WHERE id = ?", (model_id,)).fetchone()
        if not exists:
            raise HTTPException(status_code=404, detail="Model not found.")
        conn.execute(
            """
            INSERT INTO knife_model_images (knife_model_id, image_blob, image_mime, silhouette_hu_json, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(knife_model_id) DO UPDATE SET
                image_blob=excluded.image_blob, image_mime=excluded.image_mime,
                silhouette_hu_json=excluded.silhouette_hu_json, updated_at=CURRENT_TIMESTAMP
            """,
            (model_id, raw, mime, hu_json),
        )
        if dist_features is not None:
            conn.execute(
                """
                INSERT INTO knife_model_descriptors (knife_model_id, distinguishing_features, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(knife_model_id) DO UPDATE SET
                    distinguishing_features=excluded.distinguishing_features, updated_at=CURRENT_TIMESTAMP
                """,
                (model_id, dist_features),
            )
    return {
        "message": "Reference image stored.",
        "has_silhouette": hu_list is not None,
        "silhouette_error": hu_err,
        "distinguishing_features": dist_features,
        "distinguishing_features_error": dist_error,
    }


@app.delete("/api/v2/models/{model_id}/image")
def v2_delete_model_image(model_id: int):
    with get_conn() as conn:
        cur = conn.execute("DELETE FROM knife_model_images WHERE knife_model_id = ?", (model_id,))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Image not found.")
        return {"message": "Reference image cleared."}


@app.post("/api/v2/models/{model_id}/recompute-descriptors")
def v2_recompute_model_descriptors(model_id: int, model: Optional[str] = None):
    vision_model = (model or "").strip() or OLLAMA_VISION_MODEL
    with get_conn() as conn:
        row = conn.execute(
            "SELECT image_blob FROM knife_model_images WHERE knife_model_id = ?",
            (model_id,),
        ).fetchone()
        if not row or not row.get("image_blob"):
            raise HTTPException(status_code=404, detail="No reference image stored for model.")
        raw = row["image_blob"]
        model_ok, err = blade_ai.check_ollama_model(vision_model)
        if not model_ok:
            raise HTTPException(status_code=400, detail=err or "Vision model unavailable.")
        img_b64 = base64.standard_b64encode(raw).decode("ascii")
        dist_features, dist_error = blade_ai.extract_distinguishing_features_from_image(vision_model, img_b64)
        if dist_error:
            raise HTTPException(status_code=500, detail=dist_error)
        conn.execute(
            """
            INSERT INTO knife_model_descriptors (knife_model_id, distinguishing_features, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(knife_model_id) DO UPDATE SET
                distinguishing_features=excluded.distinguishing_features, updated_at=CURRENT_TIMESTAMP
            """,
            (model_id, dist_features),
        )
        return {"updated": 1, "distinguishing_features": dist_features}


@app.get("/api/v2/admin/silhouettes/status")
def v2_admin_silhouette_status():
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT km.id, km.official_name AS name, kmi.image_blob, kmi.silhouette_hu_json
            FROM knife_models_v2 km
            LEFT JOIN knife_model_images kmi ON kmi.knife_model_id = km.id
            ORDER BY km.sortable_name COLLATE NOCASE
            """
        ).fetchall()
    result = []
    missing = []
    for row in rows:
        has_image = bool(row.get("image_blob"))
        hu_json = row.get("silhouette_hu_json")
        has_hu = False
        hu_degenerate = False
        if hu_json and str(hu_json).strip():
            try:
                arr = json.loads(hu_json)
                if isinstance(arr, list) and len(arr) == 7:
                    has_hu = True
                    hu_degenerate = all(float(x) == 0.0 for x in arr)
                else:
                    hu_degenerate = True
            except Exception:
                hu_degenerate = True
        if has_image and (not has_hu or hu_degenerate):
            missing.append({"id": row["id"], "name": row["name"]})
        result.append(
            {
                "id": row["id"],
                "name": row["name"],
                "has_image": has_image,
                "has_hu": has_hu,
                "hu_degenerate": hu_degenerate,
                "hu_json": hu_json,
            }
        )
    return {
        "total": len(result),
        "with_image": sum(1 for e in result if e["has_image"]),
        "with_valid_hu": sum(1 for e in result if e["has_image"] and e["has_hu"] and not e["hu_degenerate"]),
        "missing_hu": missing,
        "masters": result,
    }


@app.post("/api/v2/admin/silhouettes/recompute")
def v2_admin_silhouette_recompute():
    updated = 0
    failed: list[dict[str, Any]] = []
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT km.id, km.official_name AS name, kmi.image_blob, kmi.silhouette_hu_json
            FROM knife_models_v2 km
            JOIN knife_model_images kmi ON kmi.knife_model_id = km.id
            ORDER BY km.sortable_name COLLATE NOCASE
            """
        ).fetchall()
        for row in rows:
            hu_json = row.get("silhouette_hu_json")
            needs = True
            if hu_json:
                try:
                    arr = json.loads(hu_json)
                    if isinstance(arr, list) and len(arr) == 7 and not all(float(x) == 0.0 for x in arr):
                        needs = False
                except Exception:
                    needs = True
            if not needs:
                continue
            hu_list, hu_err = blade_ai.extract_blade_hu_from_image_bytes(row["image_blob"])
            if not hu_list:
                failed.append({"id": row["id"], "name": row["name"], "reason": hu_err or "Unable to compute Hu"})
                continue
            conn.execute(
                """
                UPDATE knife_model_images
                SET silhouette_hu_json = ?, updated_at = CURRENT_TIMESTAMP
                WHERE knife_model_id = ?
                """,
                (json.dumps(hu_list), row["id"]),
            )
            updated += 1
    return {"updated": updated, "failed": failed, "message": f"Updated {updated} model(s)."}


@app.get("/api/v2/admin/distinguishing-features/status")
def v2_admin_distinguishing_status():
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT km.id, km.official_name AS name, kmi.image_blob, d.distinguishing_features
            FROM knife_models_v2 km
            LEFT JOIN knife_model_images kmi ON kmi.knife_model_id = km.id
            LEFT JOIN knife_model_descriptors d ON d.knife_model_id = km.id
            ORDER BY km.sortable_name COLLATE NOCASE
            """
        ).fetchall()
    result = []
    missing = []
    for row in rows:
        has_image = bool(row.get("image_blob"))
        features = (row.get("distinguishing_features") or "").strip()
        has_features = bool(features)
        if has_image and not has_features:
            missing.append({"id": row["id"], "name": row["name"]})
        result.append(
            {
                "id": row["id"],
                "name": row["name"],
                "has_image": has_image,
                "has_features": has_features,
                "features": features,
            }
        )
    return {
        "total": len(result),
        "with_image": sum(1 for e in result if e["has_image"]),
        "with_features": sum(1 for e in result if e["has_features"]),
        "missing": missing,
        "masters": result,
    }


@app.post("/api/v2/admin/distinguishing-features/recompute")
def v2_admin_distinguishing_recompute(body: DistinguishingFeaturesRecomputeBody):
    model_name = (body.model or "").strip() or OLLAMA_VISION_MODEL
    model_ok, err = blade_ai.check_ollama_model(model_name)
    if not model_ok:
        raise HTTPException(status_code=400, detail=err or "Vision model unavailable.")
    if body.knife_id is not None:
        ids = [body.knife_id]
    elif body.knife_ids:
        ids = body.knife_ids
    else:
        ids = None
    updated = 0
    failed: list[dict[str, Any]] = []
    skipped = 0
    with get_conn() as conn:
        params: list[Any] = []
        where = "1=1"
        if ids:
            placeholders = ",".join("?" for _ in ids)
            where = f"km.id IN ({placeholders})"
            params.extend(ids)
        rows = conn.execute(
            f"""
            SELECT km.id, km.official_name AS name, kmi.image_blob, d.distinguishing_features
            FROM knife_models_v2 km
            LEFT JOIN knife_model_images kmi ON kmi.knife_model_id = km.id
            LEFT JOIN knife_model_descriptors d ON d.knife_model_id = km.id
            WHERE {where}
            ORDER BY km.sortable_name COLLATE NOCASE
            """,
            params,
        ).fetchall()
        for row in rows:
            if not row.get("image_blob"):
                skipped += 1
                continue
            if body.missing_only and row.get("distinguishing_features"):
                skipped += 1
                continue
            img_b64 = base64.standard_b64encode(row["image_blob"]).decode("ascii")
            features, dist_err = blade_ai.extract_distinguishing_features_from_image(model_name, img_b64)
            if dist_err or not features:
                failed.append({"id": row["id"], "name": row["name"], "reason": dist_err or "No features returned"})
                continue
            conn.execute(
                """
                INSERT INTO knife_model_descriptors (knife_model_id, distinguishing_features, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(knife_model_id) DO UPDATE SET
                    distinguishing_features=excluded.distinguishing_features, updated_at=CURRENT_TIMESTAMP
                """,
                (row["id"], features),
            )
            updated += 1
    return {
        "updated": updated,
        "failed": failed,
        "skipped": skipped,
        "message": f"Updated {updated} model(s). {len(failed)} failed." if failed else f"Updated {updated} model(s).",
    }


@app.get("/api/v2/options")
def v2_get_options():
    option_types = (
        "blade-steels",
        "blade-finishes",
        "blade-colors",
        "handle-colors",
        "conditions",
        "blade-types",
        "categories",
        "blade-families",
        "primary-use-cases",
        "handle-types",
        "collaborators",
        "generations",
        "size-modifiers",
        "platform-variants",
    )
    with get_conn() as conn:
        result: dict[str, list[dict[str, Any]]] = {}
        for key in option_types:
            rows = conn.execute(
                "SELECT id, name FROM v2_option_values WHERE option_type = ? ORDER BY name COLLATE NOCASE",
                (key,),
            ).fetchall()
            result[key] = rows
        return result


@app.post("/api/v2/options/{option_type}")
def v2_add_option(option_type: str, payload: OptionIn):
    allowed = {
        "blade-steels",
        "blade-finishes",
        "blade-colors",
        "handle-colors",
        "conditions",
        "blade-types",
        "categories",
        "blade-families",
        "primary-use-cases",
        "handle-types",
        "collaborators",
        "generations",
        "size-modifiers",
        "platform-variants",
    }
    if option_type not in allowed:
        raise HTTPException(status_code=404, detail="Unknown option type.")
    with get_conn() as conn:
        clean_name = payload.name.strip()
        if not clean_name:
            raise HTTPException(status_code=400, detail="Option name is required.")
        try:
            cur = conn.execute(
                "INSERT INTO v2_option_values (option_type, name) VALUES (?, ?)",
                (option_type, clean_name),
            )
        except sqlite3.IntegrityError:
            raise HTTPException(status_code=400, detail="Option already exists.")
        if option_type == "collaborators":
            _v2_dim_id(conn, "collaborators", clean_name)
        return {"id": cur.lastrowid, "message": "Created"}


@app.delete("/api/v2/options/{option_type}/{option_id}")
def v2_delete_option(option_type: str, option_id: int):
    allowed = {
        "blade-steels",
        "blade-finishes",
        "blade-colors",
        "handle-colors",
        "conditions",
        "blade-types",
        "categories",
        "blade-families",
        "primary-use-cases",
        "handle-types",
        "collaborators",
        "generations",
        "size-modifiers",
        "platform-variants",
    }
    if option_type not in allowed:
        raise HTTPException(status_code=404, detail="Unknown option type.")
    with get_conn() as conn:
        row = conn.execute(
            "SELECT name FROM v2_option_values WHERE id = ? AND option_type = ?",
            (option_id, option_type),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Option not found.")
        option_name = (row.get("name") or "").strip()
        usage_sql = {
            "collaborators": (
                "SELECT COUNT(*) AS c FROM knife_models_v2 km "
                "LEFT JOIN collaborators c ON c.id = km.collaborator_id "
                "WHERE lower(c.name) = lower(?)"
            ),
            "generations": "SELECT COUNT(*) AS c FROM knife_models_v2 WHERE lower(generation_label) = lower(?)",
            "size-modifiers": "SELECT COUNT(*) AS c FROM knife_models_v2 WHERE lower(size_modifier) = lower(?)",
            "platform-variants": "SELECT COUNT(*) AS c FROM knife_models_v2 WHERE lower(platform_variant) = lower(?)",
            "handle-types": "SELECT COUNT(*) AS c FROM knife_models_v2 WHERE lower(handle_type) = lower(?)",
        }
        sql = usage_sql.get(option_type)
        if sql and option_name:
            in_use = conn.execute(sql, (option_name,)).fetchone()["c"]
            if in_use:
                raise HTTPException(
                    status_code=400,
                    detail=f"Cannot delete option in use by {in_use} model(s).",
                )
        cur = conn.execute(
            "DELETE FROM v2_option_values WHERE id = ? AND option_type = ?",
            (option_id, option_type),
        )
        return {"message": "Deleted"}


class InventoryItemV2In(BaseModel):
    knife_model_id: int
    nickname: Optional[str] = None
    quantity: int = 1
    acquired_date: Optional[str] = None
    mkc_order_number: Optional[str] = None
    purchase_price: Optional[float] = None
    estimated_value: Optional[float] = None
    condition: str = "Like New"
    handle_color: Optional[str] = None
    steel: Optional[str] = None
    blade_finish: Optional[str] = None
    blade_color: Optional[str] = None
    blade_length: Optional[float] = None
    collaboration_name: Optional[str] = None
    serial_number: Optional[str] = None
    location: Optional[str] = None
    purchase_source: Optional[str] = None
    last_sharpened: Optional[str] = None
    notes: Optional[str] = None


@app.post("/api/v2/inventory")
def v2_create_inventory_item(payload: InventoryItemV2In):
    """Create inventory item in v2 only (canonical write path)."""
    with get_conn() as conn:
        model_exists = conn.execute(
            "SELECT id, legacy_master_id FROM knife_models_v2 WHERE id = ?",
            (payload.knife_model_id,),
        ).fetchone()
        if not model_exists:
            raise HTTPException(status_code=400, detail="Invalid knife model id.")
        cur = conn.execute(
            """
            INSERT INTO inventory_items_v2
            (legacy_master_id, knife_model_id, nickname, quantity, acquired_date, mkc_order_number, purchase_price, estimated_value,
             condition, steel, blade_finish, blade_color, handle_color, blade_length, collaboration_name, serial_number,
             location, purchase_source, last_sharpened, notes, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (
                model_exists.get("legacy_master_id"),
                payload.knife_model_id,
                payload.nickname,
                payload.quantity,
                payload.acquired_date,
                payload.mkc_order_number,
                payload.purchase_price,
                payload.estimated_value,
                payload.condition,
                payload.steel,
                payload.blade_finish,
                payload.blade_color,
                payload.handle_color,
                payload.blade_length,
                payload.collaboration_name,
                payload.serial_number,
                payload.location,
                payload.purchase_source,
                payload.last_sharpened,
                payload.notes,
            ),
        )
        return {"id": cur.lastrowid, "message": "Created"}


@app.put("/api/v2/inventory/{item_id}")
def v2_update_inventory_item(item_id: int, payload: InventoryItemV2In):
    """Update inventory item in v2 only (canonical write path)."""
    with get_conn() as conn:
        model_exists = conn.execute(
            "SELECT id, legacy_master_id FROM knife_models_v2 WHERE id = ?",
            (payload.knife_model_id,),
        ).fetchone()
        if not model_exists:
            raise HTTPException(status_code=400, detail="Invalid knife model id.")
        cur = conn.execute(
            """
            UPDATE inventory_items_v2
            SET knife_model_id = ?, legacy_master_id = ?, nickname = ?, quantity = ?, acquired_date = ?,
                mkc_order_number = ?, purchase_price = ?, estimated_value = ?, condition = ?, steel = ?, blade_finish = ?, blade_color = ?,
                handle_color = ?, blade_length = ?, collaboration_name = ?, serial_number = ?, location = ?,
                purchase_source = ?, last_sharpened = ?, notes = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                payload.knife_model_id,
                model_exists.get("legacy_master_id"),
                payload.nickname,
                payload.quantity,
                payload.acquired_date,
                payload.mkc_order_number,
                payload.purchase_price,
                payload.estimated_value,
                payload.condition,
                payload.steel,
                payload.blade_finish,
                payload.blade_color,
                payload.handle_color,
                payload.blade_length,
                payload.collaboration_name,
                payload.serial_number,
                payload.location,
                payload.purchase_source,
                payload.last_sharpened,
                payload.notes,
                item_id,
            ),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Inventory item not found.")
        return {"message": "Updated"}


@app.delete("/api/v2/inventory/{item_id}")
def v2_delete_inventory_item(item_id: int):
    with get_conn() as conn:
        cur = conn.execute("DELETE FROM inventory_items_v2 WHERE id = ?", (item_id,))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Inventory item not found.")
        return {"message": "Deleted"}


@app.post("/api/v2/inventory/{item_id}/duplicate")
def v2_duplicate_inventory_item(item_id: int):
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM inventory_items_v2 WHERE id = ?", (item_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Inventory item not found.")
        nick = (row.get("nickname") or "").strip()
        new_nick = f"{nick} (copy)" if nick else "Copy"
        cur = conn.execute(
            """
            INSERT INTO inventory_items_v2
            (legacy_master_id, knife_model_id, nickname, quantity, acquired_date, mkc_order_number, purchase_price, estimated_value,
             condition, steel, blade_finish, blade_color, handle_color, blade_length, collaboration_name, serial_number,
             location, purchase_source, last_sharpened, notes, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (
                row.get("legacy_master_id"), row["knife_model_id"], new_nick, row["quantity"], row["acquired_date"],
                row.get("mkc_order_number"),
                row["purchase_price"], row["estimated_value"], row.get("condition") or "Like New", row["steel"],
                row["blade_finish"], row["blade_color"], row["handle_color"], row.get("blade_length"),
                row["collaboration_name"], None, row["location"], row["purchase_source"], row["last_sharpened"], row["notes"],
            ),
        )
        return {"id": cur.lastrowid, "message": "Duplicated"}


@app.get("/api/v2/export/inventory.csv")
def v2_export_inventory_csv() -> Response:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT
                i.id,
                km.official_name AS knife_name,
                fam.name AS knife_family,
                i.knife_model_id,
                i.nickname,
                i.quantity,
                i.acquired_date,
                i.mkc_order_number,
                i.purchase_price,
                i.estimated_value,
                i.condition,
                i.handle_color,
                i.steel AS blade_steel,
                i.blade_finish,
                i.blade_color,
                i.blade_length,
                (CASE WHEN i.collaboration_name IS NOT NULL AND i.collaboration_name != '' THEN 1 ELSE 0 END) AS is_collab,
                i.collaboration_name,
                i.serial_number,
                i.location,
                i.purchase_source,
                i.last_sharpened,
                i.notes,
                i.created_at,
                i.updated_at
            FROM inventory_items_v2 i
            LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
            LEFT JOIN knife_families fam ON fam.id = km.family_id
            ORDER BY km.sortable_name COLLATE NOCASE, i.id DESC
            """
        ).fetchall()
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=INVENTORY_CSV_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        flat: dict[str, Any] = {}
        for key in INVENTORY_CSV_COLUMNS:
            val = row.get(key)
            if key == "is_collab":
                flat[key] = "1" if val else "0"
            elif val is None:
                flat[key] = ""
            else:
                flat[key] = val
        writer.writerow(flat)
    return Response(
        content=buffer.getvalue().encode("utf-8"),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="mkc_inventory_v2.csv"'},
    )


@app.get("/api/v2/export/catalog.csv")
def v2_export_catalog_csv() -> Response:
    with get_conn() as conn:
        csv_data = normalized_model.export_models_csv(conn)
    return Response(
        content=csv_data.encode("utf-8"),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="knife_models_v2.csv"'},
    )


@app.post("/api/v2/import/models.csv")
async def v2_import_models_csv(file: UploadFile = File(...)) -> dict[str, Any]:
    raw = await file.read()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="File must be UTF-8 encoded CSV.")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise HTTPException(status_code=400, detail="CSV must include headers.")
    inserted = 0
    updated = 0
    with get_conn() as conn:
        for src in reader:
            row = {(k or "").strip(): ("" if v is None else str(v).strip()) for k, v in src.items()}
            official_name = row.get("official_name") or row.get("name") or ""
            if not official_name:
                continue
            payload = V2ModelIn(
                official_name=official_name,
                normalized_name=row.get("normalized_name") or official_name,
                knife_type=row.get("knife_type") or None,
                family_name=row.get("family_name") or row.get("family") or None,
                form_name=row.get("form_name") or None,
                series_name=row.get("series_name") or row.get("catalog_line") or None,
                collaborator_name=row.get("collaborator_name") or row.get("collaboration_name") or None,
                generation_label=row.get("generation_label") or row.get("version") or None,
                steel=row.get("steel") or row.get("default_steel") or None,
                blade_finish=row.get("blade_finish") or row.get("default_blade_finish") or None,
                blade_color=row.get("blade_color") or row.get("default_blade_color") or None,
                handle_color=row.get("handle_color") or row.get("default_handle_color") or None,
                blade_length=(float(row["blade_length"]) if row.get("blade_length") else None),
                record_status=row.get("record_status") or row.get("status") or "active",
                is_current_catalog=(row.get("is_current_catalog", "1").lower() in {"1", "true", "yes", "y"}),
                is_discontinued=(row.get("is_discontinued", "0").lower() in {"1", "true", "yes", "y"}),
                msrp=(float(row["msrp"]) if row.get("msrp") else None),
                official_product_url=row.get("official_product_url") or row.get("default_product_url") or None,
                official_image_url=row.get("official_image_url") or row.get("primary_image_url") or None,
                notes=row.get("notes") or None,
                distinguishing_features=row.get("distinguishing_features") or row.get("identifier_distinguishing_features") or None,
            )
            existing = conn.execute(
                "SELECT id FROM knife_models_v2 WHERE official_name = ?",
                (payload.official_name,),
            ).fetchone()
            type_id = _v2_dim_id(conn, "knife_types", payload.knife_type)
            form_id = _v2_dim_id(conn, "knife_forms", payload.form_name)
            family_id = _v2_dim_id(conn, "knife_families", payload.family_name)
            series_id = _v2_dim_id(conn, "knife_series", payload.series_name)
            collaborator_id = _v2_dim_id(conn, "collaborators", payload.collaborator_name)
            normalized_name = (payload.normalized_name or payload.official_name).strip()
            if existing:
                slug = _v2_model_slug(conn, normalized_name, existing_id=existing["id"])
                conn.execute(
                    """
                    UPDATE knife_models_v2
                    SET official_name = ?, normalized_name = ?, sortable_name = ?, slug = ?,
                        type_id = ?, form_id = ?, family_id = ?, series_id = ?, collaborator_id = ?,
                        generation_label = ?, steel = ?, blade_finish = ?, blade_color = ?, handle_color = ?,
                        blade_length = ?, record_status = ?, is_current_catalog = ?, is_discontinued = ?,
                        msrp = ?, official_product_url = ?, official_image_url = ?, notes = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (
                        payload.official_name.strip(), normalized_name, normalized_name, slug,
                        type_id, form_id, family_id, series_id, collaborator_id,
                        payload.generation_label, payload.steel, payload.blade_finish, payload.blade_color, payload.handle_color,
                        payload.blade_length, payload.record_status or "active",
                        0 if payload.is_current_catalog is False else 1, 1 if payload.is_discontinued else 0,
                        payload.msrp, payload.official_product_url, payload.official_image_url, payload.notes, existing["id"],
                    ),
                )
                if payload.distinguishing_features is not None:
                    conn.execute(
                        """
                        INSERT INTO knife_model_descriptors (knife_model_id, distinguishing_features, updated_at)
                        VALUES (?, ?, CURRENT_TIMESTAMP)
                        ON CONFLICT(knife_model_id) DO UPDATE SET
                            distinguishing_features=excluded.distinguishing_features, updated_at=CURRENT_TIMESTAMP
                        """,
                        (existing["id"], payload.distinguishing_features),
                    )
                updated += 1
            else:
                slug = _v2_model_slug(conn, normalized_name)
                conn.execute(
                    """
                    INSERT INTO knife_models_v2 (
                        official_name, normalized_name, sortable_name, slug, type_id, form_id, family_id, series_id, collaborator_id,
                        generation_label, steel, blade_finish, blade_color, handle_color, blade_length, record_status,
                        is_current_catalog, is_discontinued, msrp, official_product_url, official_image_url, notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        payload.official_name.strip(), normalized_name, normalized_name, slug,
                        type_id, form_id, family_id, series_id, collaborator_id,
                        payload.generation_label, payload.steel, payload.blade_finish, payload.blade_color, payload.handle_color,
                        payload.blade_length, payload.record_status or "active",
                        0 if payload.is_current_catalog is False else 1, 1 if payload.is_discontinued else 0,
                        payload.msrp, payload.official_product_url, payload.official_image_url, payload.notes,
                    ),
                )
                new_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
                if payload.distinguishing_features is not None:
                    conn.execute(
                        """
                        INSERT INTO knife_model_descriptors (knife_model_id, distinguishing_features, updated_at)
                        VALUES (?, ?, CURRENT_TIMESTAMP)
                        ON CONFLICT(knife_model_id) DO UPDATE SET
                            distinguishing_features=excluded.distinguishing_features, updated_at=CURRENT_TIMESTAMP
                        """,
                        (new_id, payload.distinguishing_features),
                    )
                inserted += 1
    return {"inserted": inserted, "updated": updated, "message": "Import complete."}


@app.post("/api/v2/models/backfill-identity")
def v2_backfill_identity() -> dict[str, Any]:
    with get_conn() as conn:
        media = migrate_legacy_media_to_v2(conn)
        summary = backfill_v2_model_identity(conn)
        extra = normalize_v2_additional_fields(conn)
        return {"ok": True, "media_migration": media, **summary, "field_normalization": extra}


@app.post("/api/v2/models/migrate-legacy-media")
def v2_migrate_legacy_media() -> dict[str, Any]:
    with get_conn() as conn:
        summary = migrate_legacy_media_to_v2(conn)
        return {"ok": True, **summary}


@app.get("/api/v2/models/qa/identity")
def v2_identity_qa() -> dict[str, Any]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT km.id, km.official_name, km.generation_label, km.size_modifier, km.platform_variant,
                   kt.name AS knife_type, frm.name AS form_name, fam.name AS family_name,
                   ks.name AS series_name, c.name AS collaborator_name
            FROM knife_models_v2 km
            LEFT JOIN knife_types kt ON kt.id = km.type_id
            LEFT JOIN knife_forms frm ON frm.id = km.form_id
            LEFT JOIN knife_families fam ON fam.id = km.family_id
            LEFT JOIN knife_series ks ON ks.id = km.series_id
            LEFT JOIN collaborators c ON c.id = km.collaborator_id
            ORDER BY km.sortable_name COLLATE NOCASE, km.id
            """
        ).fetchall()
    collab_like_series = {"archery country", "bearded butchers", "meat church", "nock on"}
    generic_family_labels = {
        "hunting", "culinary", "tactical", "everyday carry", "edc", "bushcraft",
        "bushcraft & camp", "camp", "utility", "work", "kitchen", "fillet", "skinner",
    }
    incomplete = []
    warnings = []
    family_warnings = []
    for row in rows:
        missing = []
        for field in ("official_name", "knife_type", "form_name", "family_name"):
            if not (row.get(field) or "").strip():
                missing.append(field)
        if missing:
            incomplete.append(
                {
                    "id": row["id"],
                    "official_name": row.get("official_name"),
                    "missing_fields": missing,
                }
            )
        series_name = (row.get("series_name") or "").strip()
        collaborator_name = (row.get("collaborator_name") or "").strip()
        if series_name.lower() in collab_like_series and not collaborator_name:
            warnings.append(
                {
                    "id": row["id"],
                    "official_name": row.get("official_name"),
                    "warning": "Series suggests collaboration but collaborator_name is empty",
                }
            )
        family_name = (row.get("family_name") or "").strip()
        if family_name and ("/" in family_name or family_name.lower() in generic_family_labels or family_name == family_name.lower()):
            family_warnings.append(
                {
                    "id": row["id"],
                    "official_name": row.get("official_name"),
                    "family_name": family_name,
                    "warning": "Family label appears generic or malformed",
                }
            )
    total = len(rows)
    complete = total - len(incomplete)
    return {
        "total_models": total,
        "complete_identity_models": complete,
        "incomplete_identity_models": len(incomplete),
        "completeness_pct": round((complete / total) * 100, 2) if total else 100.0,
        "incomplete_rows": incomplete,
        "warnings": warnings,
        "family_warnings": family_warnings,
    }


@app.post("/api/v2/identify")
def v2_identify_knives(payload: IdentifierQuery):
    """Rank v2 catalog models only and return canonical v2 model IDs."""
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT km.id, km.official_name, km.normalized_name, km.record_status, km.blade_length, km.steel,
                   km.blade_finish, km.blade_color, km.generation_label, km.notes,
                   kt.name AS knife_type, fam.name AS family_name, frm.name AS form_name, ks.name AS series_name,
                   c.name AS collaborator_name,
                   d.distinguishing_features,
                   (CASE WHEN kmi.image_blob IS NOT NULL AND length(kmi.image_blob) > 0 THEN 1 ELSE 0 END) AS has_identifier_image
            FROM knife_models_v2 km
            LEFT JOIN knife_types kt ON kt.id = km.type_id
            LEFT JOIN knife_forms frm ON frm.id = km.form_id
            LEFT JOIN knife_families fam ON fam.id = km.family_id
            LEFT JOIN knife_series ks ON ks.id = km.series_id
            LEFT JOIN collaborators c ON c.id = km.collaborator_id
            LEFT JOIN knife_model_descriptors d ON d.knife_model_id = km.id
            LEFT JOIN knife_model_images kmi ON kmi.knife_model_id = km.id
            ORDER BY km.sortable_name COLLATE NOCASE
            """
        ).fetchall()
    tokens = [token.strip().lower() for token in (payload.q or "").replace("/", " ").replace(",", " ").split() if token.strip()]
    results: list[dict[str, Any]] = []
    for row in rows:
        score = 0.0
        reasons: list[str] = []
        if payload.family:
            fam_hay = " ".join(
                str(row.get(k) or "")
                for k in ("family_name", "knife_type", "official_name", "normalized_name", "form_name")
            ).lower()
            if payload.family.lower() in fam_hay:
                score += 22
                reasons.append(f"use / category matches {payload.family}")
        if payload.steel and row.get("steel") and payload.steel.lower() == str(row["steel"]).lower():
            score += 12
            reasons.append(f"steel matches {row['steel']}")
        if payload.finish and row.get("blade_finish") and payload.finish.lower() == str(row["blade_finish"]).lower():
            score += 12
            reasons.append(f"finish matches {row['blade_finish']}")
        if payload.blade_color and row.get("blade_color") and payload.blade_color.lower() == str(row["blade_color"]).lower():
            score += 10
            reasons.append(f"blade color matches {row['blade_color']}")
        if payload.blade_length is not None and row.get("blade_length") is not None:
            diff = abs(float(row["blade_length"]) - payload.blade_length)
            if diff <= 0.2:
                score += 18
            elif diff <= 0.5:
                score += 10
            elif diff <= 1.0:
                score += 3
        haystack = " ".join(str(v) for v in row.values() if v is not None and not isinstance(v, bytes)).lower()
        token_hits = [token for token in tokens if token in haystack]
        if token_hits:
            score += len(token_hits) * 8
            reasons.append("keyword matches: " + ", ".join(token_hits[:4]))
        if score > 0:
            results.append(
                {
                    "id": row["id"],
                    "name": row["official_name"],
                    "family": row.get("family_name"),
                    "category": row.get("knife_type"),
                    "catalog_line": row.get("series_name"),
                    "record_type": row.get("generation_label"),
                    "catalog_status": row.get("record_status"),
                    "identifier_product_url": None,
                    "has_identifier_image": bool(row.get("has_identifier_image")),
                    "has_silhouette_hint": False,
                    "catalog_blurb": row.get("notes"),
                    "default_blade_length": row.get("blade_length"),
                    "default_steel": row.get("steel"),
                    "default_blade_finish": row.get("blade_finish"),
                    "default_blade_color": row.get("blade_color"),
                    "is_collab": bool(row.get("collaborator_name")),
                    "collaboration_name": row.get("collaborator_name"),
                    "list_status": row.get("record_status") or "active",
                    "score": round(score, 1),
                    "reasons": reasons[:5],
                }
            )
    results.sort(key=lambda item: (-item["score"], item["name"].lower()))
    return {"source": "v2_catalog", "results": results[:10]}


# -----------------------------------------------------------------------------


@app.get("/normalized")
def normalized_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "normalized.html")


@app.get("/api/normalized/summary")
def normalized_summary() -> dict[str, Any]:
    with get_conn() as conn:
        return {
            "types": conn.execute("SELECT COUNT(*) AS c FROM knife_types").fetchone()["c"],
            "forms": conn.execute("SELECT COUNT(*) AS c FROM knife_forms").fetchone()["c"],
            "families": conn.execute("SELECT COUNT(*) AS c FROM knife_families").fetchone()["c"],
            "series": conn.execute("SELECT COUNT(*) AS c FROM knife_series").fetchone()["c"],
            "collaborators": conn.execute("SELECT COUNT(*) AS c FROM collaborators").fetchone()["c"],
            "models": conn.execute("SELECT COUNT(*) AS c FROM knife_models_v2").fetchone()["c"],
            "inventory_items": conn.execute("SELECT COUNT(*) AS c FROM inventory_items_v2").fetchone()["c"],
            "last_migration": conn.execute("SELECT * FROM migration_runs_v2 ORDER BY id DESC LIMIT 1").fetchone(),
        }


@app.get("/api/normalized/models")
def normalized_models() -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT km.id, km.legacy_master_id, km.official_name, km.normalized_name, km.sortable_name, km.slug,
                      kt.name AS knife_type, frm.name AS form_name, fam.name AS family_name, ks.name AS series_name,
                      c.name AS collaborator_name, km.generation_label, km.size_modifier, km.platform_variant,
                      km.steel, km.blade_finish, km.blade_color, km.handle_color, km.blade_length, km.msrp,
                      km.record_status, km.is_current_catalog, km.is_discontinued, km.official_product_url
               FROM knife_models_v2 km
               LEFT JOIN knife_types kt ON kt.id = km.type_id
               LEFT JOIN knife_forms frm ON frm.id = km.form_id
               LEFT JOIN knife_families fam ON fam.id = km.family_id
               LEFT JOIN knife_series ks ON ks.id = km.series_id
               LEFT JOIN collaborators c ON c.id = km.collaborator_id
               ORDER BY km.sortable_name, km.official_name"""
        ).fetchall()
        return rows


@app.get("/api/normalized/inventory")
def normalized_inventory() -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT i.id, i.legacy_inventory_id, i.quantity, i.condition, i.purchase_price, i.estimated_value,
                      i.acquired_date, i.mkc_order_number,
                      i.steel, i.blade_finish, i.blade_color, i.handle_color, i.location, i.serial_number, i.notes,
                      km.official_name, km.normalized_name, kt.name AS knife_type, fam.name AS family_name, frm.name AS form_name, ks.name AS series_name
               FROM inventory_items_v2 i
               LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
               LEFT JOIN knife_types kt ON kt.id = km.type_id
               LEFT JOIN knife_families fam ON fam.id = km.family_id
               LEFT JOIN knife_forms frm ON frm.id = km.form_id
               LEFT JOIN knife_series ks ON ks.id = km.series_id
               ORDER BY km.sortable_name, i.id"""
        ).fetchall()
        return rows


@app.post("/api/normalized/rebuild")
def normalized_rebuild() -> dict[str, Any]:
    with get_conn() as conn:
        summary = normalized_model.migrate_legacy_to_v2(conn, force=True)
        ensure_v2_exclusive_schema(conn)
        ensure_reporting_schema(conn)
        ensure_gap_reconciliation_schema(conn)
        media_summary = migrate_legacy_media_to_v2(conn)
        identity_summary = backfill_v2_model_identity(conn)
        extra_summary = normalize_v2_additional_fields(conn)
        return {
            "ok": True,
            **summary,
            "media_migration": media_summary,
            "identity_backfill": identity_summary,
            "field_normalization": extra_summary,
        }


@app.get("/api/normalized/export/models.csv")
def normalized_export_models_csv() -> Response:
    with get_conn() as conn:
        csv_data = normalized_model.export_models_csv(conn)
    return Response(
        content=csv_data,
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="knife_models_v2.csv"'},
    )
