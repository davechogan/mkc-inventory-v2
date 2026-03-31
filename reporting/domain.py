"""Natural-language reporting orchestrator: session management, hint tracking, and query execution.

Pipeline (per query):
  Retrieval → LLM planner → Structural validation → Semantic validation
  → Deterministic SQL compilation → SQL validation → Execution → LLM responder → Response

Constants live in ``reporting.constants``.
SQL compilation lives in ``reporting.compiler``.
Semantic planning lives in ``reporting.planner``.

Env:
  ``REPORTING_DEBUG_PIPELINE`` — when ``1``/``true``/``yes``/``on``, responses include ``pipeline_debug``
  (retrieval query text, planner/responder prompts and raw model output). Do not enable in untrusted environments.
  Other reporting model env vars: ``REPORTING_PLANNER_MODEL``, ``REPORTING_RESPONDER_MODEL``, …
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import time
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Optional
from urllib.parse import urlencode

import blade_ai
from fastapi import HTTPException
from pydantic import BaseModel, Field

from sqlite_schema import column_exists

from reporting.regex_contract import (
    RE_HINT_ENTITY_STOP_PREFIX,
    RE_HINT_ENTITY_STOP_SUFFIX,
)
from reporting.compiler import (
    compile_plan as _reporting_plan_to_sql,
    exec_sql as _reporting_exec_sql,
    validate_sql as _reporting_validate_sql,
)
from reporting.constants import (
    REPORTING_DEFAULT_MODEL,
    REPORTING_HINT_MIN_CONFIDENCE,
    REPORTING_HINT_PROMOTION_ENABLED,
    REPORTING_HINT_PROMOTION_MAX_PER_RUN,
    REPORTING_HINT_PROMOTION_MIN_CONFIDENCE,
    REPORTING_HINT_PROMOTION_MIN_EVIDENCE,
    REPORTING_MAX_ROWS_DEFAULT,
    REPORTING_MAX_ROWS_HARD,
    REPORTING_PLANNER_MODEL,
    REPORTING_PLANNER_RETRY_MODEL,
    REPORTING_RESPONDER_MODEL,
    REPORTING_REWRITER_MODEL,
    GetConn,
)
from reporting.plan_models import (
    CanonicalReportingPlan,
    FilterClause,
    FilterOp,
    PlanDimension,
    PlanField,
    PlanIntent,
    PlanMetric,
    PlanScope,
    SortSpec,
    SortDirection,
    TimeRange,
)
from reporting.plan_validator import validate_canonical_semantics
from reporting.planner import (
    _reporting_build_prompt_schema,
    _reporting_has_substantive_rows,
    _reporting_llm_plan,
    _reporting_rewrite_query_for_retrieval,
    _reporting_summarize_state_for_hints,
)
from reporting.retrieval import format_retrieval_context, retrieve_artifacts_with_meta



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
            bs.name AS steel,
            bf.name AS blade_finish,
            blc.name AS blade_color,
            hc.name AS handle_color,
            ht.name AS handle_type,
            km.blade_length,
            loc.name AS location,
            i.notes,
            km.msrp
        FROM inventory_items_v2 i
        LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
        LEFT JOIN knife_types kt ON kt.id = km.type_id
        LEFT JOIN knife_forms frm ON frm.id = km.form_id
        LEFT JOIN knife_families fam ON fam.id = km.family_id
        LEFT JOIN knife_series ks ON ks.id = km.series_id
        LEFT JOIN collaborators c ON c.id = km.collaborator_id
        LEFT JOIN blade_steels bs ON bs.id = km.steel_id
        LEFT JOIN blade_finishes bf ON bf.id = km.blade_finish_id
        LEFT JOIN handle_types ht ON ht.id = km.handle_type_id
        LEFT JOIN model_colorways mc ON mc.id = i.colorway_id
        LEFT JOIN handle_colors hc ON hc.id = mc.handle_color_id
        LEFT JOIN blade_colors blc ON blc.id = mc.blade_color_id
        LEFT JOIN locations loc ON loc.id = i.location_id;

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
            bs.name AS steel,
            bf.name AS blade_finish,
            ht.name AS handle_type,
            km.blade_length,
            km.msrp
        FROM knife_models_v2 km
        LEFT JOIN knife_types kt ON kt.id = km.type_id
        LEFT JOIN knife_forms frm ON frm.id = km.form_id
        LEFT JOIN knife_families fam ON fam.id = km.family_id
        LEFT JOIN knife_series ks ON ks.id = km.series_id
        LEFT JOIN collaborators c ON c.id = km.collaborator_id
        LEFT JOIN blade_steels bs ON bs.id = km.steel_id
        LEFT JOIN blade_finishes bf ON bf.id = km.blade_finish_id
        LEFT JOIN handle_types ht ON ht.id = km.handle_type_id;
        """
    )
    if not column_exists(conn, "reporting_sessions", "last_query_state_json"):
        conn.execute("ALTER TABLE reporting_sessions ADD COLUMN last_query_state_json TEXT")

def _reporting_iso_now() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


_REPORTING_META_ANSWER = (
    "Here is how I calculate cost and value fields:\n\n"
    "• **purchase_price** — what you actually paid when you acquired the knife "
    "(from your inventory record). This is used for 'total spend' or 'how much did I spend' questions.\n"
    "• **estimated_value** — your current estimated resale or market value "
    "(from your inventory record). This is used for 'estimated value' or 'what is my collection worth' questions.\n"
    "• **msrp** — the manufacturer's suggested retail price from the catalog. "
    "This is catalog data and is used for 'msrp' or 'retail price' questions.\n\n"
    "For any spend question I use **purchase_price**. "
    "For value questions I use **estimated_value**. "
    "You can ask me things like: 'how much did I spend by series?', "
    "'what is the estimated value of my Blackfoot knives?', or "
    "'which knives have an msrp above $300?'."
)



def _reporting_build_drill_link(row: dict[str, Any], intent: Optional[str] = None) -> Optional[str]:
    """Build a drill-through URL for a result row.

    For ``missing_models`` intent the row describes a model that is NOT in the
    user's inventory.  Linking to the inventory view produces an empty list,
    which is confusing (RPT-005).  Instead, link to the master catalog page with
    the model name pre-filled in the search field so the user can inspect or add
    it from there.
    """
    if intent == "missing_models":
        name = str(row.get("official_name") or row.get("knife_name") or "").strip()
        if name:
            return f"/master.html?{urlencode({'search': name})}"
        return None

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


def _reporting_get_last_query_state(conn: sqlite3.Connection, session_id: str) -> Optional[dict[str, Any]]:
    row = conn.execute(
        "SELECT last_query_state_json FROM reporting_sessions WHERE id = ?",
        (session_id,),
    ).fetchone()
    if not row:
        return None
    raw = row["last_query_state_json"]
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
            dim = str(r["target_dimension"] or "").strip()
            val = str(r["target_value"] or "").strip()
            if not dim or not val:
                continue
            if dim not in {"series_name", "family_name", "knife_type", "form_name", "collaborator_name", "steel", "condition", "location", "text_search"}:
                continue
            # Do not overwrite stronger hints in same pass.
            if dim not in filters:
                filters[dim] = val
                hid = int(r["id"])
                hint_ids.append(hid)
                hints.append(
                    {
                        "id": hid,
                        "dimension": dim,
                        "value": val,
                        "confidence": r["confidence"],
                        "scope_type": r["scope_type"],
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
    plan: Optional["CanonicalReportingPlan"],
    row_count: int,
) -> None:
    # Learn only from successful, non-empty answers with a semantic plan.
    if not plan or row_count <= 0:
        return
    candidates = _reporting_extract_hint_entities(question)
    if not candidates:
        return
    plan_filters: dict[str, str] = {
        c.field.value: str(c.value)
        for c in plan.filters
        if c.op.value == "=" and isinstance(c.value, (str, int, float))
    }
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


def _reporting_promote_semantic_hints(
    conn: sqlite3.Connection,
    *,
    session_id: Optional[str] = None,
    dry_run: bool = True,
    min_confidence: Optional[float] = None,
    min_evidence: Optional[int] = None,
    max_promotions: Optional[int] = None,
) -> dict[str, Any]:
    """Promote strong session hints into global soft priors.

    Guardrails:
    - only promotes session-scoped hints
    - requires confidence/evidence thresholds and positive success signal
    - skips candidates that conflict with an existing global hint for same cue+dimension
    """
    if not REPORTING_HINT_PROMOTION_ENABLED:
        return {"enabled": False, "considered": 0, "promoted": 0, "skipped": 0, "reasons": ["disabled_by_env"], "candidates": []}

    min_conf = float(min_confidence if min_confidence is not None else REPORTING_HINT_PROMOTION_MIN_CONFIDENCE)
    min_ev = int(min_evidence if min_evidence is not None else REPORTING_HINT_PROMOTION_MIN_EVIDENCE)
    max_prom = max(1, int(max_promotions if max_promotions is not None else REPORTING_HINT_PROMOTION_MAX_PER_RUN))
    filters: list[Any] = [float(min_conf), int(min_ev), max_prom]
    sql_where = """
        WHERE scope_type = 'session'
          AND confidence >= ?
          AND evidence_count >= ?
          AND success_count > failure_count
    """
    if session_id and session_id.strip():
        sql_where += " AND scope_id = ?"
        filters = [float(min_conf), int(min_ev), session_id.strip(), max_prom]
    candidates = conn.execute(
        f"""
        SELECT id, scope_id, entity_norm, cue_word, target_dimension, target_value,
               confidence, evidence_count, success_count, failure_count
        FROM reporting_semantic_hints
        {sql_where}
        ORDER BY confidence DESC, evidence_count DESC, success_count DESC, id DESC
        LIMIT ?
        """
        ,
        tuple(filters),
    ).fetchall()
    promoted: list[dict[str, Any]] = []
    skipped = 0
    reasons: dict[str, int] = {}
    for row in candidates:
        ent = str(row["entity_norm"] or "").strip()
        cue = str(row["cue_word"] or "").strip()
        dim = str(row["target_dimension"] or "").strip()
        val = str(row["target_value"] or "").strip()
        if not ent or not cue or not dim or not val:
            skipped += 1
            reasons["invalid_candidate"] = reasons.get("invalid_candidate", 0) + 1
            continue
        conflict = conn.execute(
            """
            SELECT id
            FROM reporting_semantic_hints
            WHERE scope_type = 'global'
              AND scope_id IS NULL
              AND entity_norm = ?
              AND cue_word = ?
              AND target_dimension = ?
              AND lower(target_value) <> lower(?)
            LIMIT 1
            """,
            (ent, cue, dim, val),
        ).fetchone()
        if conflict:
            skipped += 1
            reasons["conflict_global_value"] = reasons.get("conflict_global_value", 0) + 1
            continue
        promoted_conf = min(0.90, max(0.60, float(row["confidence"] or min_conf) - 0.03))
        item = {
            "source_hint_id": int(row["id"]),
            "source_session_id": row["scope_id"],
            "entity_norm": ent,
            "cue_word": cue,
            "target_dimension": dim,
            "target_value": val,
            "promoted_confidence": promoted_conf,
            "evidence_count": int(row["evidence_count"] or 0),
        }
        promoted.append(item)
        if dry_run:
            continue
        existing = conn.execute(
            """
            SELECT id, confidence, evidence_count
            FROM reporting_semantic_hints
            WHERE scope_type = 'global'
              AND scope_id IS NULL
              AND entity_norm = ?
              AND cue_word = ?
              AND target_dimension = ?
              AND lower(target_value) = lower(?)
            ORDER BY id DESC
            LIMIT 1
            """,
            (ent, cue, dim, val),
        ).fetchone()
        if existing:
            conn.execute(
                """
                UPDATE reporting_semantic_hints
                SET confidence = ?,
                    evidence_count = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    min(0.95, max(float(existing["confidence"] or 0.0), promoted_conf)),
                    max(int(existing["evidence_count"] or 0), int(row["evidence_count"] or 1)),
                    int(existing["id"]),
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO reporting_semantic_hints
                (scope_type, scope_id, entity_norm, cue_word, target_dimension, target_value,
                 confidence, evidence_count, success_count, failure_count, created_at, updated_at, last_used_at)
                VALUES ('global', NULL, ?, ?, ?, ?, ?, ?, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL)
                """,
                (
                    ent,
                    cue,
                    dim,
                    val,
                    promoted_conf,
                    int(row["evidence_count"] or 1),
                ),
            )
    return {
        "enabled": True,
        "dry_run": bool(dry_run),
        "considered": len(candidates),
        "promoted": len(promoted),
        "skipped": skipped,
        "reasons": reasons,
        "candidates": promoted,
    }


def _reporting_semantic_plan(
    conn: sqlite3.Connection,
    planner_model: str,
    question: str,
    session_id: str,
    context_block: str,
    retrieval_context: str,
    schema_context: str,
    retry_model: Optional[str] = None,
    *,
    debug: bool = False,
) -> tuple[CanonicalReportingPlan, dict[str, Any]]:
    """Call LLM planner and return a CanonicalReportingPlan.

    Retrieval and schema context are prepared by the caller (run_reporting_query)
    so they appear as first-class visible steps in the pipeline.
    Returns (canonical_plan, meta). plan may have needs_clarification=True if
    the LLM could not produce a valid plan.
    """
    learned_hints = _reporting_get_semantic_hints(conn, session_id, question)

    canonical_plan, planner_dbg = _reporting_llm_plan(
        planner_model,
        question,
        context_block,
        schema_context,
        retrieval_context,
        learned_hints=learned_hints,
        debug=debug,
    )
    planner_attempts = 1
    planner_llm: dict[str, Any] = {}
    if debug:
        planner_llm["primary"] = planner_dbg

    if canonical_plan is None and retry_model and retry_model != planner_model:
        canonical_plan, planner_dbg_retry = _reporting_llm_plan(
            retry_model,
            question,
            context_block,
            schema_context,
            retrieval_context,
            learned_hints=learned_hints,
            debug=debug,
        )
        planner_attempts = 2
        if debug:
            planner_llm["retry"] = planner_dbg_retry

    if canonical_plan is None:
        canonical_plan = CanonicalReportingPlan(
            intent=PlanIntent.LIST,
            scope=PlanScope.INVENTORY,
            metric=PlanMetric.COUNT,
            needs_clarification=True,
            clarification_reason=(
                "The planner could not produce a valid structured plan for this question. "
                "Please rephrase with a concrete scope, time window, or sort intent."
            ),
        )

    meta_out: dict[str, Any] = {
        "mode": "semantic_llm_plan" if not canonical_plan.needs_clarification else "semantic_llm_unparsed",
        "planner_attempts": planner_attempts,
        "hint_ids": learned_hints.get("hint_ids") or [],
        "hints": learned_hints.get("hints") or [],
    }
    if debug and planner_llm:
        meta_out["planner_llm"] = planner_llm
    return canonical_plan, meta_out

class ReportingQueryIn(BaseModel):
    question: str = Field(min_length=2, max_length=2000)
    session_id: Optional[str] = None
    model: Optional[str] = None
    max_rows: int = Field(default=REPORTING_MAX_ROWS_DEFAULT, ge=1, le=REPORTING_MAX_ROWS_HARD)
    chart_preference: Optional[str] = None
    compare_dimension: Optional[str] = None
    compare_value_a: Optional[str] = None
    compare_value_b: Optional[str] = None
    debug: bool = False


def _reporting_pipeline_debug_enabled(payload: ReportingQueryIn) -> bool:
    if payload.debug:
        return True
    v = (os.environ.get("REPORTING_DEBUG_PIPELINE") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


class ReportingSaveQueryIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    question: str = Field(min_length=2, max_length=2000)
    config: Optional[dict[str, Any]] = None


class ReportingFeedbackIn(BaseModel):
    session_id: str = Field(min_length=8, max_length=120)
    message_id: int = Field(ge=1)
    helpful: bool


_SESSION_TTL_DAYS = 30

def _reporting_cleanup_old_sessions(conn: sqlite3.Connection) -> int:
    """Delete sessions and their messages older than TTL. Returns count deleted."""
    cutoff = f"datetime('now', '-{_SESSION_TTL_DAYS} days')"
    old = conn.execute(f"SELECT id FROM reporting_sessions WHERE updated_at < {cutoff}").fetchall()
    if not old:
        return 0
    old_ids = [r["id"] for r in old]
    for sid in old_ids:
        conn.execute("DELETE FROM reporting_messages WHERE session_id = ?", (sid,))
    conn.execute(f"DELETE FROM reporting_sessions WHERE updated_at < {cutoff}")
    return len(old_ids)


def _reporting_create_session(conn: sqlite3.Connection, model_default: Optional[str] = None) -> dict[str, Any]:
    # Opportunistic cleanup of old sessions
    try:
        cleaned = _reporting_cleanup_old_sessions(conn)
        if cleaned:
            logging.getLogger("mkc_app").info("Cleaned up %d old reporting sessions", cleaned)
    except Exception:
        pass

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
            return dict(row)
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
    summary = (session["memory_summary"] or "").strip() if session else ""
    last_state = ""
    if session and session["last_query_state_json"]:
        try:
            parsed = json.loads(session["last_query_state_json"])
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
        role = "User" if r["role"] == "user" else "Assistant"
        content = " ".join(str(r["content"] or "").split())
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
        if r["role"] != "user":
            continue
        q = " ".join(str(r["content"] or "").split())
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
    canonical_plan: Optional["CanonicalReportingPlan"] = None,
    *,
    debug: bool = False,
) -> tuple[str, list[str], Optional[str], Optional[float], dict[str, Any]]:
    if not rows:
        dbg: dict[str, Any] = {"path": "deterministic_no_rows"} if debug else {}
        return (
            "No matching rows found for that question. Try broadening filters or date range.",
            ["Remove a filter and rerun.", "Try 'Show all knives by family.'"],
            "No rows matched the generated query.",
            0.6,
            dbg,
        )
    if (
        canonical_plan is not None
        and canonical_plan.intent.value == "list"
        and canonical_plan.sort is not None
        and canonical_plan.sort.field == "purchase_price"
        and canonical_plan.sort.direction.value == "desc"
    ):
        lines: list[str] = []
        for i, r in enumerate(rows[:40], start=1):
            rd = dict(r) if not isinstance(r, dict) else r
            name = str(rd.get("knife_name") or "").strip() or "(unnamed)"
            lt_raw = rd.get("line_purchase_total")
            try:
                lt_f = float(lt_raw) if lt_raw is not None else None
            except (TypeError, ValueError):
                lt_f = None
            if lt_f is None:
                try:
                    pp = rd.get("purchase_price")
                    qty = float(rd.get("quantity") or 1) or 1.0
                    lt_f = float(pp) * qty if pp is not None else None
                except (TypeError, ValueError):
                    lt_f = None
            if lt_f is not None:
                price_str = f"${lt_f:,.2f}"
            else:
                price_str = "—"
            qty_v = rd.get("quantity")
            try:
                qn = int(qty_v) if qty_v is not None else 1
            except (TypeError, ValueError):
                qn = 1
            qsuffix = f" (qty {qn})" if qn != 1 else ""
            lines.append(f"{i}. {name}{qsuffix}: {price_str} line total")
        body = "\n".join(lines)
        dbg2: dict[str, Any] = (
            {"path": "deterministic_ranked_purchase_lines", "model": model} if debug else {}
        )
        return (
            f"Most expensive purchase lines (by line total: price × quantity), showing {len(lines)}:\n{body}",
            _reporting_default_followups(question, columns, rows),
            "Deterministic ranked purchase list.",
            0.88,
            dbg2,
        )
    if canonical_plan is not None and canonical_plan.intent.value == "missing_models":
        names = [str((dict(r) if not isinstance(r, dict) else r).get("official_name") or "").strip() for r in rows if str((dict(r) if not isinstance(r, dict) else r).get("official_name") or "").strip()]
        if names:
            max_list = 30
            listed = ", ".join(names[:max_list])
            extra = f" (+{len(names)-max_list} more)" if len(names) > max_list else ""
            dbg3: dict[str, Any] = {"path": "deterministic_missing_models", "model": model} if debug else {}
            return (
                f"You are missing {len(names)} models matching that scope: {listed}{extra}.",
                ["Show this grouped by family.", "Show only missing Traditions models.", "Estimate completion cost for these."],
                "Deterministic missing-model answer.",
                0.9,
                dbg3,
            )
    preview = rows[:40]
    responder_exc: Optional[BaseException] = None
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
            r_dbg: dict[str, Any] = {}
            if debug:
                r_dbg = {
                    "path": "ollama_json_responder",
                    "model": model,
                    "system": system,
                    "user": user,
                    "raw_response": raw,
                    "parsed_json": parsed,
                }
            return answer, followups[:5], limitations, confidence, r_dbg
    except Exception as exc:
        responder_exc = exc

    top = rows[0]
    top_bits = ", ".join(f"{k}={top.get(k)}" for k in columns[:4])
    fb_dbg: dict[str, Any] = {"path": "deterministic_fallback_first_row"} if debug else {}
    if debug and responder_exc is not None:
        fb_dbg["responder_exception"] = repr(responder_exc)
    return (
        f"Found {len(rows)} rows. First row: {top_bits}.",
        _reporting_default_followups(question, columns, rows),
        "Summary generated with deterministic fallback.",
        0.55,
        fb_dbg,
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
    debug_pipeline = _reporting_pipeline_debug_enabled(payload)

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

        def _log_error(
            status: str,
            detail: str,
            mode: Optional[str] = None,
            semantic_intent: Optional[str] = None,
            classification: Optional[str] = None,
        ) -> None:
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
                meta={"classification": classification} if classification else {},
            )

        # ── Step 1a: Query rewriting ───────────────────────────────────────
        # For follow-up questions the raw question often contains pronouns
        # ("those", "it", "them") that embed poorly. Rewrite into a standalone
        # question using last_query_state so Chroma retrieves field-relevant
        # artifacts rather than generic scope/intent documents.
        last_query_state = _reporting_get_last_query_state(conn, session_id)
        retrieval_query, rewriter_debug = _reporting_rewrite_query_for_retrieval(
            REPORTING_REWRITER_MODEL,
            question,
            last_query_state or {},
            debug=debug_pipeline,
        )

        # ── Step 1b: Retrieval ─────────────────────────────────────────────
        retrieval_artifacts, retrieval_meta = retrieve_artifacts_with_meta(
            retrieval_query, top_k=6, conn=conn, debug=debug_pipeline
        )
        retrieval_context = format_retrieval_context(retrieval_artifacts)

        # ── Step 2: Schema context ─────────────────────────────────────────
        schema_context = _reporting_build_prompt_schema(conn)

        # ── Step 3: Plan generation ────────────────────────────────────────
        # Explicit compare mode: build canonical plan directly from UI params.
        # No LLM call needed; still goes through compilation and execution.
        canonical_plan: Optional[CanonicalReportingPlan] = None
        plan_meta: dict[str, Any] = {}

        if payload.compare_dimension and payload.compare_value_a and payload.compare_value_b:
            dim_map = {
                "family": "family_name",
                "type": "knife_type",
                "series": "series_name",
                "steel": "steel",
                "condition": "condition",
                "location": "location",
            }
            dim_col = dim_map.get(str(payload.compare_dimension).lower())
            if dim_col:
                try:
                    canonical_plan = CanonicalReportingPlan(
                        intent=PlanIntent.LIST,
                        scope=PlanScope.INVENTORY,
                        metric=PlanMetric.TOTAL_SPEND,
                        group_by=[PlanDimension(dim_col)],
                        filters=[FilterClause(
                            field=PlanField(dim_col),
                            op=FilterOp.IN,
                            value=[payload.compare_value_a, payload.compare_value_b],
                        )],
                    )
                    plan_meta = {"mode": "compare_explicit"}
                except Exception:
                    canonical_plan = None

        if canonical_plan is None:
            canonical_plan, plan_meta = _reporting_semantic_plan(
                conn,
                planner_model,
                question,
                session_id,
                context_block,
                retrieval_context,
                schema_context,
                retry_model=retry_model,
                debug=debug_pipeline,
            )

        # ── Step 4: Clarification check ────────────────────────────────────
        if canonical_plan.needs_clarification:
            reason = canonical_plan.clarification_reason or "Please clarify your question."
            assistant_message_id = _reporting_store_message(
                conn, session_id, "assistant", reason,
                meta={"clarification_needed": True, "generation_mode": plan_meta.get("mode")},
            )
            total_ms = round((time.perf_counter() - started) * 1000.0, 2)
            _reporting_log_query_event(
                conn, session_id=session_id, question=question,
                planner_model=planner_model, responder_model=responder_model,
                generation_mode="clarification", semantic_intent=None,
                sql_excerpt=None, row_count=0, execution_ms=None, total_ms=total_ms,
                status="clarification_needed", error_detail=reason,
                meta={"planner_attempts": plan_meta.get("planner_attempts")},
            )
            return {
                "session_id": session_id,
                "model": responder_model,
                "planner_model": planner_model,
                "answer_text": reason,
                "columns": [], "rows": [], "chart_spec": None, "sql_executed": None,
                "follow_ups": [], "confidence": None,
                "limitations": "Clarification required before query can proceed.",
                "generation_mode": "clarification",
                "execution_ms": None,
                "date_window": {"start": None, "end": None, "label": None},
                "assistant_message_id": assistant_message_id,
                "semantic_plan": None, "retrieval": retrieval_meta,
            }

        # ── Step 5: Semantic validation ────────────────────────────────────
        semantic_check = validate_canonical_semantics(canonical_plan)
        if not semantic_check.valid:
            reason = "; ".join(semantic_check.errors[:2]) or "Plan validation failed."
            _log_error(
                "invalid_plan", reason,
                mode="semantic_invalid",
                semantic_intent=canonical_plan.intent.value,
                classification="invalid_plan",
            )
            raise HTTPException(status_code=400, detail=f"Invalid semantic plan: {reason}")

        # ── Step 6: SQL compilation ────────────────────────────────────────
        date_start = canonical_plan.time_range.start if canonical_plan.time_range else None
        date_end = canonical_plan.time_range.end if canonical_plan.time_range else None
        sql, compile_meta = _reporting_plan_to_sql(canonical_plan, date_start, date_end, payload.max_rows)

        if not sql:
            _log_error(
                "no_sql",
                f"Could not derive SQL. {compile_meta.get('error') or ''}".strip(),
                mode=compile_meta.get("mode"),
                semantic_intent=canonical_plan.intent.value,
                classification="invalid_plan",
            )
            raise HTTPException(
                status_code=400,
                detail=f"Could not derive SQL from validated plan. {compile_meta.get('error') or ''}".strip(),
            )

        # ── Step 7: SQL execution ──────────────────────────────────────────
        try:
            columns, rows, execution_ms = _reporting_exec_sql(conn, sql, payload.max_rows)
        except HTTPException as exc:
            _log_error(
                "sql_error", str(exc.detail),
                mode=compile_meta.get("mode"),
                semantic_intent=canonical_plan.intent.value,
                classification="internal_failure",
            )
            raise

        # ── Step 8: Post-execution processing ─────────────────────────────
        primary_intent = canonical_plan.intent.value
        rows_out = []
        for r in rows:
            row = dict(r)
            drill = _reporting_build_drill_link(row, intent=primary_intent)
            if drill:
                row["_drill_link"] = drill
            rows_out.append(row)
        substantive = _reporting_has_substantive_rows(primary_intent, rows_out)

        hint_ids_used = [int(x) for x in (plan_meta.get("hint_ids") or []) if isinstance(x, int) or str(x).isdigit()]
        if hint_ids_used:
            _reporting_feedback_semantic_hints(conn, hint_ids_used, success=substantive)

        _reporting_learn_semantic_hints(
            conn, session_id=session_id, question=question,
            plan=canonical_plan, row_count=(1 if substantive else 0),
        )

        # ── Step 9: Response generation ────────────────────────────────────
        chart_spec = _reporting_infer_chart(
            question, columns, rows_out,
            preference=(payload.chart_preference or "").strip().lower() or None,
        )
        answer_text, follow_ups, limitations, confidence, responder_debug = _reporting_generate_answer(
            responder_model, question, columns, rows_out, sql, context_block,
            canonical_plan=canonical_plan,
            debug=debug_pipeline,
        )
        if compile_meta.get("limitations") and not limitations:
            limitations = str(compile_meta["limitations"])

        date_label = canonical_plan.time_range.label if canonical_plan.time_range else None
        if not date_label and canonical_plan.year_compare and len(canonical_plan.year_compare) == 2:
            date_label = f"{canonical_plan.year_compare[0]} vs {canonical_plan.year_compare[1]}"

        result_payload = {
            "columns": columns, "rows": rows_out, "row_count": len(rows_out),
            "date_window": {"start": date_start, "end": date_end, "label": date_label},
            "retrieval": retrieval_meta,
        }
        meta = {
            "planner_model": planner_model,
            "responder_model": responder_model,
            "retry_model": retry_model,
            "generation_mode": plan_meta.get("mode") or compile_meta.get("mode"),
            "confidence": confidence,
            "limitations": limitations,
            "follow_ups": follow_ups,
            "execution_ms": execution_ms,
            "semantic_plan": canonical_plan.model_dump(mode="json"),
            "timestamp": _reporting_iso_now(),
            "semantic_hints": plan_meta.get("hints") or [],
            "retrieval": retrieval_meta,
        }
        if debug_pipeline:
            meta["pipeline_debug"] = {
                "rewriter_llm": rewriter_debug,
                "retrieval_query": retrieval_query,
                "retrieval": retrieval_meta,
                "planner_llm": plan_meta.get("planner_llm") or {},
                "responder_llm": responder_debug,
            }

        # ── Step 10: Persist and log ───────────────────────────────────────
        assistant_message_id = _reporting_store_message(
            conn, session_id, "assistant", answer_text,
            sql_executed=sql, result=result_payload, chart_spec=chart_spec, meta=meta,
        )
        _reporting_set_last_query_state(
            conn, session_id, {**canonical_plan.to_planner_context_dict(), "_result_row_count": len(rows_out)}
        )
        _reporting_update_summary(conn, session_id)

        total_ms = round((time.perf_counter() - started) * 1000.0, 2)
        _reporting_log_query_event(
            conn, session_id=session_id, question=question,
            planner_model=planner_model, responder_model=responder_model,
            generation_mode=meta["generation_mode"],
            semantic_intent=primary_intent,
            sql_excerpt=sql, row_count=len(rows_out),
            execution_ms=execution_ms, total_ms=total_ms, status="ok",
            meta={
                "date_window": {"start": date_start, "end": date_end, "label": date_label},
                "planner_attempts": plan_meta.get("planner_attempts"),
                "semantic_plan": canonical_plan.model_dump(mode="json"),
                "semantic_hints": plan_meta.get("hints") or [],
                "retrieval": retrieval_meta,
            },
        )

        out: dict[str, Any] = {
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
            "generation_mode": meta["generation_mode"],
            "execution_ms": execution_ms,
            "date_window": {"start": date_start, "end": date_end, "label": date_label},
            "assistant_message_id": assistant_message_id,
            "semantic_plan": canonical_plan.model_dump(mode="json"),
            "retrieval": retrieval_meta,
        }
        if debug_pipeline:
            out["pipeline_debug"] = meta.get("pipeline_debug") or {}
        return out
