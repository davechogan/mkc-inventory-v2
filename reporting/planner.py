"""Reporting planner: LLM-based canonical plan generation.

Responsibilities:
  - Build schema context for LLM prompts
  - Call the planner LLM and parse output into CanonicalReportingPlan
  - Post-execution helpers (substantive row check, state summarization for hints)

All regex-based pre-processing, heuristic planning, explicit constraint
extraction, and template SQL have been removed. Scope ambiguity, date
extraction, and follow-up carryover are handled by the LLM using the
conversation context block and retrieved grounding.
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any, Optional

import blade_ai

from reporting.constants import REPORTING_ALLOWED_SOURCES
from reporting.plan_models import (
    CanonicalReportingPlan,
    PlanIntent,
    PlanMetric,
    PlanScope,
)
from reporting.plan_validator import parse_planner_raw_text, validate_canonical_structure
from reporting.regex_contract import extract_first_json_object
from reporting.retrieval import format_retrieval_context, retrieve_artifacts_with_meta


# ---------------------------------------------------------------------------
# Schema context builder
# ---------------------------------------------------------------------------

def _reporting_build_prompt_schema(conn: sqlite3.Connection) -> str:
    chunks = []
    for view in sorted(REPORTING_ALLOWED_SOURCES):
        cols = conn.execute(f"PRAGMA table_info({view})").fetchall()
        names = ", ".join(c["name"] for c in cols)
        chunks.append(f"- {view}: {names}")
    return "\n".join(chunks)


# ---------------------------------------------------------------------------
# LLM planner
# ---------------------------------------------------------------------------

_PLANNER_SYSTEM = (
    "You convert natural language collection questions into a strict canonical JSON plan. "
    "Return JSON only — no prose, no markdown fences, no explanation. Do not generate SQL.\n\n"
    "Required JSON structure:\n"
    "{\n"
    '  "intent": "aggregate | list | missing_models | completion_cost",\n'
    '  "scope": "inventory | catalog",\n'
    '  "metric": "count | total_spend | estimated_value | msrp",\n'
    '  "group_by": [],\n'
    '  "filters": [],\n'
    '  "exclusions": [],\n'
    '  "time_range": null,\n'
    '  "year_compare": [],\n'
    '  "sort": null,\n'
    '  "limit": null,\n'
    '  "needs_clarification": false,\n'
    '  "clarification_reason": null\n'
    "}\n\n"
    "intent:\n"
    "  aggregate       — summaries, totals, counts, breakdowns by group\n"
    "  list            — individual rows from inventory\n"
    "  missing_models  — catalog models not present in my inventory\n"
    "  completion_cost — MSRP cost to acquire all missing catalog models\n\n"
    "scope: inventory (knives you own) or catalog (all MKC models ever made). "
    "Default to inventory unless the question is explicitly about the full catalog. "
    "If genuinely ambiguous and cannot be inferred from context, set needs_clarification=true.\n\n"
    "metric: count, total_spend, estimated_value, msrp\n\n"
    "group_by: array of zero or more dimension names:\n"
    "  series_name, family_name, knife_type, form_name, collaborator_name, steel, condition, location\n\n"
    "filters: array of {\"field\": \"...\", \"op\": \"...\", \"value\": ...} for required matches.\n"
    "exclusions: array of {\"field\": \"...\", \"op\": \"...\", \"value\": ...} for not/except/exclude conditions.\n"
    "  Allowed fields: series_name, family_name, knife_type, form_name, collaborator_name,\n"
    "    steel, condition, location, knife_name, official_name, record_status, acquired_date,\n"
    "    purchase_price, estimated_value, msrp, text_search\n"
    "  Allowed ops: =, !=, contains, not_contains, in, not_in, >, >=, <, <=, between\n"
    "  For 'in' and 'not_in', value must be a JSON array. For all others, value is a scalar.\n\n"
    "time_range: null or {\"start\": \"YYYY-MM-DD\", \"end\": \"YYYY-MM-DD\", \"label\": \"...\"}\n"
    "  Extract from the question when a date, year, or time period is mentioned.\n"
    "  Set label to a human-readable description (e.g. '2024', 'last 12 months').\n\n"
    "year_compare: [] or [YYYY, YYYY] — only for explicit year-vs-year comparison questions.\n\n"
    "sort: null or {\"field\": \"purchase_price\", \"direction\": \"asc\" | \"desc\"}\n"
    "  Use for ranked questions such as 'top 10 most expensive'.\n\n"
    "limit: null or positive integer — for 'top N' or 'show me N' requests.\n\n"
    "needs_clarification: true only when the question cannot be safely interpreted. "
    "Always include a specific clarification_reason when true.\n\n"
    "Use the conversation context to resolve follow-up questions. "
    "Carry forward scope, group_by, and filters from the prior turn when the user refers to the same subject."
)


def _reporting_llm_plan(
    model: str,
    question: str,
    context_block: str,
    schema_context: str,
    retrieval_context: str,
    *,
    learned_hints: Optional[dict[str, Any]] = None,
    debug: bool = False,
) -> tuple[Optional[CanonicalReportingPlan], dict[str, Any]]:
    """Call planner LLM and parse output directly into a CanonicalReportingPlan.

    Returns (plan, debug_dict). plan is None if the LLM output could not be
    parsed or validated; the caller should retry or surface a clarification.
    """
    hints_block = ""
    if learned_hints and (learned_hints.get("hints") or []):
        try:
            hints_block = "\n\nLearned semantic hints (advisory — use only if relevant):\n" + json.dumps(
                learned_hints.get("hints") or [], ensure_ascii=False, default=str
            )
        except (TypeError, ValueError):
            hints_block = ""

    user = (
        f"Schema context:\n{schema_context}\n\n"
        f"Retrieved grounding:\n{retrieval_context or '(none)'}\n\n"
        f"Conversation context:\n{context_block or '(none)'}\n\n"
        f"Question: {question}"
        f"{hints_block}"
    )

    planner_debug: dict[str, Any] = {}
    if debug:
        planner_debug = {"model": model, "system": _PLANNER_SYSTEM, "user": user}

    try:
        raw = blade_ai.ollama_chat(model, _PLANNER_SYSTEM, user, timeout=60.0)
        if debug:
            planner_debug["raw_response"] = raw

        # Try direct parse first, then fallback to first-JSON-object extraction.
        plan_dict: Optional[dict[str, Any]] = None
        txt = (raw or "").strip()
        if txt.startswith("{"):
            try:
                parsed = json.loads(txt)
                if isinstance(parsed, dict):
                    plan_dict = parsed
            except Exception:
                pass
        if plan_dict is None:
            braced = extract_first_json_object(raw)
            if braced:
                try:
                    parsed = json.loads(braced)
                    if isinstance(parsed, dict):
                        plan_dict = parsed
                except Exception:
                    pass

        if plan_dict is None:
            if debug:
                planner_debug["parse_error"] = "No JSON object found in LLM output."
            return None, planner_debug

        result = validate_canonical_structure(plan_dict)
        if debug:
            planner_debug["parsed_plan"] = plan_dict
            planner_debug["validation_errors"] = result.errors if not result.valid else []
        if not result.valid:
            return None, planner_debug

        return result.canonical_plan, planner_debug

    except Exception as exc:
        if debug:
            planner_debug["exception"] = repr(exc)
        return None, planner_debug


# ---------------------------------------------------------------------------
# Post-execution helpers
# ---------------------------------------------------------------------------

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


def _reporting_summarize_state_for_hints(last_state: Optional[dict[str, Any]]) -> dict[str, Any]:
    if not isinstance(last_state, dict) or not last_state:
        return {}
    return {
        "intent": last_state.get("intent"),
        "scope": last_state.get("scope"),
        "date_start": last_state.get("date_start"),
        "date_end": last_state.get("date_end"),
        "year_compare": last_state.get("year_compare"),
        "filter_keys": list((last_state.get("filters") or {}).keys()),
    }
