"""Natural-language reporting: schema, semantic planning, SQL compilation, sessions.

Env:
  ``REPORTING_SCOPE_PREPROCESSING`` — when ``1``/``true``/``yes``/``on``, enable scope-status
  short-circuit and inventory-vs-catalog clarification prompts. Default **unset** = off (legacy behavior).
  Other reporting model env vars: ``REPORTING_PLANNER_MODEL``, ``REPORTING_RESPONDER_MODEL``, …
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import time
import uuid
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import date, datetime, timedelta
from typing import Any, Optional
from urllib.parse import urlencode

import blade_ai
from fastapi import HTTPException
from pydantic import BaseModel, Field

from sqlite_schema import column_exists

from reporting.regex_contract import (
    RE_COMPLETION_COST_LEX,
    RE_DATE_ISO,
    RE_DIRECT_SQL_USER_PREFIX,
    RE_HINT_ENTITY_STOP_PREFIX,
    RE_HINT_ENTITY_STOP_SUFFIX,
    RE_LAST_N_DAYS,
    RE_LAST_N_MONTHS,
    RE_LAST_N_YEARS,
    RE_NORM_STRIP_ANY_ALL,
    RE_NORM_STRIP_INVENTORY_PHRASES,
    RE_NORM_STRIP_KNIFE_WORDS,
    RE_NORM_STRIP_POLITE_VERBS,
    RE_SCOPE_OWN,
    RE_SCOPE_OWNED,
    RE_SINCE_ISO_DATE,
    RE_SQL_FENCED_BLOCK,
    RE_SQL_FROM_JOIN_IDENT,
    RE_SQL_LOOSE_FROM_SELECT,
    RE_SQL_QUOTED_RELATION_REF,
    RE_WHICH_WHAT_KNIVES,
    RE_YEAR_4,
    RE_YEAR_VS_YEAR,
    UNSAFE_REQUEST_PATTERN_REASONS,
    clean_llm_sql_fences,
    extract_first_json_object,
)
from reporting.plan_models import CanonicalReportingPlan
from reporting.plan_validator import validate_canonical_structure, validate_canonical_semantics

_ConnectionCtx = AbstractContextManager[sqlite3.Connection]
GetConn = Callable[[], _ConnectionCtx]

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

# Debug A/B toggle: when enabled, bypass semantic planning + SQL compiler and
# ask the LLM to generate SQL directly (while still running SQL through the
# existing safety validator + executor).
REPORTING_DIRECT_LLM_SQL_META_KEY = "reporting_direct_llm_sql"


def _reporting_direct_llm_sql_enabled(conn: sqlite3.Connection) -> bool:
    """
    Read the current debug toggle from `app_meta`.

    Stored as a string value; truthy values match `reporting_scope_preprocessing_enabled()`.
    """
    row = conn.execute(
        "SELECT value FROM app_meta WHERE key = ?",
        (REPORTING_DIRECT_LLM_SQL_META_KEY,),
    ).fetchone()
    raw = (row.get("value") if row else None) if isinstance(row, dict) else None
    return reporting_scope_preprocessing_enabled(raw)

def reporting_scope_preprocessing_enabled(raw: Optional[str]) -> bool:
    """Parse ``REPORTING_SCOPE_PREPROCESSING``. Unset/empty -> False (matches legacy ``if False and …``)."""
    s = (raw or "").strip().lower()
    if not s:
        return False
    return s in {"1", "true", "yes", "on"}


REPORTING_SCOPE_PREPROCESSING = reporting_scope_preprocessing_enabled(
    os.environ.get("REPORTING_SCOPE_PREPROCESSING")
)


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

def _reporting_iso_now() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _reporting_detect_date_bounds(question: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
    q = (question or "").lower()
    today = date.today()
    start: Optional[date] = None
    end: Optional[date] = None
    label: Optional[str] = None

    m = RE_LAST_N_DAYS.search(q)
    if m:
        n = max(1, int(m.group(1)))
        start = today - timedelta(days=n)
        end = today
        label = f"last {n} days"
    m = m or RE_LAST_N_MONTHS.search(q)
    if m and label is None:
        n = max(1, int(m.group(1)))
        start = today - timedelta(days=(30 * n))
        end = today
        label = f"last {n} months"
    m = m or RE_LAST_N_YEARS.search(q)
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
    m_since = RE_SINCE_ISO_DATE.search(q)
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
    m = RE_YEAR_VS_YEAR.search(q)
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
    for pat, reason in UNSAFE_REQUEST_PATTERN_REASONS:
        if re.search(pat, compact):
            return reason
    # Direct SQL command starters are not supported as user input.
    # Do not treat plain English "create a …" / "make a …" as CREATE TABLE.
    if RE_DIRECT_SQL_USER_PREFIX.match(compact):
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
    if not inv and (RE_SCOPE_OWNED.search(q) or RE_SCOPE_OWN.search(q)):
        inv = True
    # Personal collection value / ranking (inventory pieces), not catalog MSRP rollups.
    if not inv and "estimated value" in q and RE_WHICH_WHAT_KNIVES.search(q):
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
    return bool(RE_COMPLETION_COST_LEX.search(q))


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
    if RE_SQL_QUOTED_RELATION_REF.search(lower):
        raise HTTPException(status_code=400, detail="Quoted relation references are not allowed in reporting SQL.")
    for token in REPORTING_FORBIDDEN_SQL:
        if re.search(rf"\b{token}\b", lower):
            raise HTTPException(status_code=400, detail=f"Forbidden SQL token: {token}")
    refs = RE_SQL_FROM_JOIN_IDENT.findall(lower)
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
    stop_prefix = RE_HINT_ENTITY_STOP_PREFIX
    stop_suffix = RE_HINT_ENTITY_STOP_SUFFIX
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
    # Require entity text to overlap the plan filter value — never bind text_search (or any
    # dimension) when the extracted entity is unrelated to that filter value.
    priority = ["series_name", "family_name", "knife_type", "form_name", "collaborator_name", "steel", "condition", "location", "text_search"]
    for ent, cue in candidates:
        chosen_dim = None
        chosen_val = None
        for dim in priority:
            val = plan_filters.get(dim)
            if not val:
                continue
            nval = _reporting_norm_entity(str(val))
            if ent in nval or nval in ent:
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
    v = RE_NORM_STRIP_ANY_ALL.sub(" ", v)
    v = RE_NORM_STRIP_KNIFE_WORDS.sub(" ", v)
    # Remove common tail phrases that do not belong to entity values.
    v = RE_NORM_STRIP_INVENTORY_PHRASES.sub(" ", v)
    v = RE_NORM_STRIP_POLITE_VERBS.sub(" ", v)
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
            braced = extract_first_json_object(raw)
            if braced:
                try:
                    parsed = json.loads(braced)
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
        if RE_DATE_ISO.fullmatch(llm_ds):
            plan["date_start"] = llm_ds
        if RE_DATE_ISO.fullmatch(llm_de):
            plan["date_end"] = llm_de
        yc = llm_plan.get("year_compare")
        if isinstance(yc, (list, tuple)) and len(yc) == 2:
            ya = str(yc[0]).strip()
            yb = str(yc[1]).strip()
            if RE_YEAR_4.fullmatch(ya) and RE_YEAR_4.fullmatch(yb):
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

    canonical_candidate = CanonicalReportingPlan.from_legacy_semantic_plan(plan)
    structural = validate_canonical_structure(canonical_candidate.model_dump())
    if not structural.valid:
        err = "; ".join(structural.errors[:3]) or "Invalid canonical plan."
        raise HTTPException(status_code=400, detail=f"Planner produced invalid plan structure: {err}")
    canonical_valid = structural.canonical_plan
    if canonical_valid is None:
        raise HTTPException(status_code=400, detail="Planner produced invalid canonical plan structure.")
    # Semantic validation is performed in run_reporting_query before compilation.
    normalized_plan = canonical_valid.to_legacy_semantic_plan()

    return normalized_plan, {
        "mode": mode,
        "planner_attempts": planner_attempts,
        "hint_ids": learned_hints.get("hint_ids") or [],
        "hints": learned_hints.get("hints") or [],
        "plan_validation": {
            "structural": structural.classification,
            "semantic": "pending",
        },
    }


def _reporting_plan_to_sql_legacy(
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
        if RE_YEAR_4.fullmatch(ya) and RE_YEAR_4.fullmatch(yb):
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

    def inv_filter_where(filters_map: dict[str, Any], *, catalog_style: bool) -> list[str]:
        """WHERE fragments using reporting_inventory / reporting_models column names."""
        w: list[str] = []
        for k, v in filters_map.items():
            if k not in inv_filter_cols:
                continue
            negate = k.endswith("__not")
            base_k = k[:-5] if negate else k
            if catalog_style and base_k in {"condition", "location"}:
                continue
            if base_k == "text_search":
                ev = esc(v)
                if catalog_style:
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
                w.append(f"NOT {expr}" if negate else expr)
                continue
            expr = cond(base_k, v, exact=(base_k in {"series_name", "knife_type", "condition"}))
            w.append(f"NOT ({expr})" if negate else expr)
        return w

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

    where = inv_filter_where(filters, catalog_style=use_catalog)
    effective_date_start = plan_date_start or date_start
    effective_date_end = plan_date_end or date_end
    # Filters only (year-compare path adds its own acquired_date / year constraints).
    where_filters_only = list(where)
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
        if year_compare and not group_by:
            ya, yb = year_compare
            yc_parts = (
                inv_filter_where(filters, catalog_style=False)
                if use_catalog
                else list(where_filters_only)
            )
            yc_parts.append("acquired_date IS NOT NULL")
            yc_parts.append(f"substr(acquired_date, 1, 4) IN ('{esc(ya)}', '{esc(yb)}')")
            yc_where_sql = f"WHERE {' AND '.join(yc_parts)}"
            meta: dict[str, Any] = {"mode": "semantic_compiled_year_compare"}
            if use_catalog:
                meta["year_compare_inventory_note"] = (
                    "Catalog filters preserved; results are bucketed by inventory acquired_date "
                    "(year-over-year needs collection dates, not the catalog view alone)."
                )
            sql = (
                "SELECT substr(acquired_date, 1, 4) AS bucket, "
                f"{expr} "
                "FROM reporting_inventory "
                f"{yc_where_sql} "
                "GROUP BY bucket "
                "ORDER BY bucket"
            )
            return sql, meta
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


def _reporting_plan_to_sql(
    plan: CanonicalReportingPlan,
    date_start: Optional[str],
    date_end: Optional[str],
    max_rows: int,
) -> tuple[Optional[str], dict[str, Any]]:
    """Compile SQL only from validated canonical plans.

    The legacy dict compiler remains as an internal adapter during migration,
    but external callers must pass ``CanonicalReportingPlan``.
    """
    if not isinstance(plan, CanonicalReportingPlan):
        raise TypeError("Compiler requires CanonicalReportingPlan input.")
    return _reporting_plan_to_sql_legacy(
        plan.to_legacy_semantic_plan(),
        date_start,
        date_end,
        max_rows,
    )


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
            s = clean_llm_sql_fences(candidate)
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
            braced = extract_first_json_object(raw)
            if braced:
                try:
                    parsed = json.loads(braced)
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
            m_fenced = RE_SQL_FENCED_BLOCK.search(raw)
            if m_fenced:
                sql = _clean_extracted_sql(m_fenced.group(1))
            else:
                m_sql = RE_SQL_LOOSE_FROM_SELECT.search(raw)
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
            braced = extract_first_json_object(raw)
            if braced:
                parsed = json.loads(braced)
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


def run_reporting_query(
    payload: ReportingQueryIn,
    *,
    get_conn: GetConn,
    ollama_check_payload: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    question = payload.question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="Question is required.")
    date_start, date_end, date_label = _reporting_detect_date_bounds(question)

    route_models = _reporting_model_route(payload.model, ollama_check_payload)
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

        # Optional scope preprocessing: short-circuit on scope_status or clarification_scope.
        # Controlled by REPORTING_SCOPE_PREPROCESSING (default off = legacy production).
        if REPORTING_SCOPE_PREPROCESSING and _reporting_is_scope_status_question(question):
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

        if REPORTING_SCOPE_PREPROCESSING and _reporting_needs_scope_clarification(question):
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
        semantic_plan_validated: Optional[CanonicalReportingPlan] = None
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
            semantic_plan_validated = CanonicalReportingPlan.from_legacy_semantic_plan(semantic_plan)
            semantic_check = validate_canonical_semantics(semantic_plan_validated)
            if not semantic_check.valid:
                reason = "; ".join(semantic_check.errors[:2]) or "Plan validation failed."
                if semantic_check.classification == "clarification_needed":
                    assistant_message_id = _reporting_store_message(
                        conn,
                        session_id,
                        "assistant",
                        reason,
                        meta={"clarification_needed": True, "validation_stage": "semantic"},
                    )
                    total_ms = round((time.perf_counter() - started) * 1000.0, 2)
                    _reporting_log_query_event(
                        conn,
                        session_id=session_id,
                        question=question,
                        planner_model=planner_model,
                        responder_model=responder_model,
                        generation_mode="clarification_semantic",
                        semantic_intent=(semantic_plan or {}).get("intent"),
                        sql_excerpt=None,
                        row_count=0,
                        execution_ms=None,
                        total_ms=total_ms,
                        status="clarification_needed",
                        error_detail=reason,
                        meta={"validation_stage": "semantic"},
                    )
                    return {
                        "session_id": session_id,
                        "model": responder_model,
                        "planner_model": planner_model,
                        "answer_text": reason,
                        "columns": [],
                        "rows": [],
                        "chart_spec": None,
                        "sql_executed": None,
                        "follow_ups": [],
                        "confidence": None,
                        "limitations": "Semantic plan requested clarification.",
                        "generation_mode": "clarification_semantic",
                        "execution_ms": None,
                        "date_window": {"start": date_start, "end": date_end, "label": date_label},
                        "assistant_message_id": assistant_message_id,
                    }
                _log_error(
                    "invalid_plan",
                    reason,
                    mode="semantic_invalid",
                    semantic_intent=(semantic_plan or {}).get("intent"),
                )
                raise HTTPException(status_code=400, detail=f"Invalid semantic plan: {reason}")
            sql, compile_meta = _reporting_plan_to_sql(
                semantic_plan_validated,
                date_start,
                date_end,
                payload.max_rows,
            )
            sql_meta = {**semantic_meta, **compile_meta}
            sql_meta["plan_validation"] = {
                "structural": (semantic_meta.get("plan_validation") or {}).get("structural", "ok"),
                "semantic": semantic_check.classification,
            }
            hint_ids_used = [int(x) for x in (semantic_meta.get("hint_ids") or []) if isinstance(x, int) or str(x).isdigit()]
        if not sql:
            _log_error("no_sql", f"Could not derive SQL. {sql_meta.get('error') or ''}".strip(), mode=sql_meta.get("mode"), semantic_intent=(semantic_plan or {}).get("intent"))
            raise HTTPException(
                status_code=400,
                detail=f"Could not derive SQL from validated semantic plan. {sql_meta.get('error') or ''}".strip(),
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
                relaxed_validated = CanonicalReportingPlan.from_legacy_semantic_plan(relaxed)
                relaxed_check = validate_canonical_semantics(relaxed_validated)
                if not relaxed_check.valid:
                    relaxed_sql = None
                    relaxed_meta = {}
                else:
                    relaxed_sql, relaxed_meta = _reporting_plan_to_sql(
                        relaxed_validated,
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
        yc_inv_note = sql_meta.get("year_compare_inventory_note")
        if yc_inv_note:
            extra = str(yc_inv_note).strip()
            if extra:
                if limitations:
                    limitations = f"{str(limitations).strip()} {extra}".strip()
                else:
                    limitations = extra
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
            # Top-level for clients and reporting_eval_harness brittle/plan-equiv checks (also in stored meta).
            "semantic_plan": semantic_plan,
        }
