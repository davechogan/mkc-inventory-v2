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
    '  "intent": "list | missing_models",\n'
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
    "  list            — all data-returning questions: individual rows, counts, totals, or breakdowns.\n"
    "                    Use group_by to break down by a dimension. Use metric to control what is measured.\n"
    "  missing_models  — catalog models not present in my inventory.\n"
    "                    Use metric=msrp to get the total MSRP cost to acquire all missing models.\n\n"
    "scope: inventory (knives you own) or catalog (all MKC models ever made).\n"
    "  inventory signals: 'I have', 'I own', 'do I own', 'do I have', 'own one', 'my collection',\n"
    "    'my knives', 'I bought', 'I paid', 'in my collection'\n"
    "  catalog signals: 'MKC has', 'MKC makes', 'MKC offers', 'MKC produces', 'MKC carries',\n"
    "    'does MKC make', 'are available', 'has MKC released', 'MKC catalog', 'all MKC models'\n"
    "  Default to inventory when the subject is clearly the user's possessions.\n"
    "  Use catalog when the subject is MKC's product line, regardless of what the user owns.\n"
    "  IMPORTANT: scope can change between turns. Even if prior turns used catalog scope,\n"
    "    switch to inventory when the current question is about what the user personally owns.\n"
    "  If genuinely ambiguous and cannot be inferred from context, set needs_clarification=true.\n\n"
    "metric: count, total_spend, estimated_value, msrp\n\n"
    "group_by: array of zero or more dimension names:\n"
    "  series_name, family_name, knife_type, form_name, collaborator_name, steel,\n"
    "  blade_finish, handle_color, condition, location\n\n"
    "filters: ARRAY of {\"field\": \"...\", \"op\": \"...\", \"value\": ...} for required matches. Always an array, never a dict.\n"
    "exclusions: ARRAY of {\"field\": \"...\", \"op\": \"...\", \"value\": ...} for NOT/except/exclude conditions. Always an array, never a dict.\n"
    "  Trigger words: 'exclude', 'except', 'without', 'if you take out', 'minus', 'not counting',\n"
    "    'not including', 'ignore', 'leave out'.\n"
    "  Field mapping for exclusions:\n"
    "    - Named series (e.g. 'Traditions', 'Blood Brothers'): field=series_name, op==\n"
    "    - Text pattern in knife name (e.g. 'Damascus', 'Sprint'): field=text_search, op==\n"
    "    - Blade finish values (e.g. 'Stonewash'): field=blade_finish, op==\n"
    "  Example: 'how much on Blackfoot, excluding Damascus and Traditions versions'\n"
    "    filters: [{\"field\": \"family_name\", \"op\": \"=\", \"value\": \"Blackfoot\"}]\n"
    "    exclusions: [{\"field\": \"text_search\", \"op\": \"=\", \"value\": \"Damascus\"},\n"
    "                 {\"field\": \"series_name\",  \"op\": \"=\", \"value\": \"Traditions\"}]\n"
    "  Allowed fields: series_name, family_name, knife_type, form_name, collaborator_name,\n"
    "    steel, blade_finish, blade_color, handle_color, handle_type, blade_length,\n"
    "    condition, location, knife_name, official_name, record_status, acquired_date,\n"
    "    purchase_price, estimated_value, msrp, quantity, purchase_source,\n"
    "    generation_label, size_modifier, text_search\n"
    "  Field glossary — use the correct field; do NOT mix up knife_type and knife_name:\n"
    "    knife_type     — knife CATEGORY, e.g. 'Hunting', 'Tactical', 'Everyday Carry', 'Culinary'.\n"
    "                     Never put a knife model name (e.g. 'Blackfoot 2.0') in knife_type.\n"
    "    knife_name     — specific knife model name, e.g. 'Blood Brothers Blackfoot 2.0'.\n"
    "    family_name    — product family, e.g. 'Blackfoot', 'Speedgoat', 'Wargoat'.\n"
    "    series_name    — named series or collaboration, e.g. 'Blood Brothers', 'Traditions'.\n"
    "    blade_finish   — surface treatment, e.g. 'Stonewash', 'Satin', 'PVD'.\n"
    "    blade_color    — blade color, e.g. 'Black', 'Bronze', 'Sniper Grey'.\n"
    "    handle_color   — handle color, e.g. 'OD Green', 'FDE', 'Black'.\n"
    "    handle_type    — handle material or type, e.g. 'G10', 'Micarta', 'Carbon Fiber'.\n"
    "  Allowed ops: =, !=, contains, not_contains, in, not_in, >, >=, <, <=, between\n"
    "  Use a single = for equality — never ==.\n"
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
    "Carry forward scope, group_by, filters, AND exclusions from the prior turn when the user refers to "
    "the same subject. Do not drop exclusions just because the follow-up question does not re-state them."
)


# ---------------------------------------------------------------------------
# Query rewriter — context-aware retrieval query expansion
# ---------------------------------------------------------------------------

_REWRITER_SYSTEM = (
    "You rewrite follow-up questions into fully standalone questions for a knife collection database.\n\n"
    "Rules:\n"
    "1. Use first person (\"I\", \"my\") throughout.\n"
    "2. Replace ALL pronouns (\"those\", \"them\", \"it\", \"ones\", \"which\") with the exact entity names from the context filters.\n"
    "3. Preserve domain values EXACTLY — never substitute synonyms. \"like new\" stays \"like new\". "
    "\"MagnaCut\" stays \"MagnaCut\". \"Blood Brothers\" stays \"Blood Brothers\".\n"
    "4. If context has year_compare, include both years explicitly (e.g. \"in 2023 vs 2024\").\n"
    "5. Output ONLY the rewritten question — one sentence, no quotes, no explanation.\n\n"
    "Examples:\n"
    "Context: {\"scope\":\"catalog\",\"filters\":{\"series_name\":\"Blood Brothers\"}}\n"
    "Follow-up: do I own one of those?\n"
    "Output: do I own any Blood Brothers series knives in my inventory?\n\n"
    "Context: {\"scope\":\"inventory\",\"metric\":\"total_spend\",\"year_compare\":[2023,2024],\"filters\":{}}\n"
    "Follow-up: which year was higher?\n"
    "Output: which year had higher total spend in my inventory, 2023 or 2024?"
)


def _reporting_rewrite_query_for_retrieval(
    model: str,
    question: str,
    last_state: dict[str, Any],
    *,
    debug: bool = False,
) -> tuple[str, dict[str, Any]]:
    """Rewrite a follow-up question into a standalone question for Chroma retrieval.

    Uses the last query state to resolve pronouns and inject entity names so the
    embedding search surfaces field-relevant artifacts instead of generic
    scope/intent documents.

    Returns (retrieval_query, debug_dict). Falls back to the original question
    on any failure so retrieval always proceeds.
    """
    dbg: dict[str, Any] = {}
    if not last_state:
        return question, dbg

    # Compact context: only entity-bearing fields matter for pronoun resolution.
    # Exclude sort/limit — they are query implementation details, not entity
    # references, and including them causes the rewriter to inject noise like
    # "highest msrp" or "listed in the catalog" which degrades retrieval quality.
    ctx = {
        k: v for k, v in last_state.items()
        if k in ("scope", "filters", "group_by", "year_compare")
        and v not in (None, [], {})
    }
    user_msg = f"Context: {json.dumps(ctx, ensure_ascii=False)}\nFollow-up: {question}"

    if debug:
        dbg = {"model": model, "system": _REWRITER_SYSTEM, "user": user_msg}

    try:
        raw = blade_ai.ollama_chat(model, _REWRITER_SYSTEM, user_msg, timeout=15.0)
        if debug:
            dbg["raw_response"] = raw
        rewritten = (raw or "").strip()
        if rewritten.startswith('"') and rewritten.endswith('"'):
            rewritten = rewritten[1:-1]
        # guard against multi-line output from verbose models
        rewritten = rewritten.split("\n")[0].strip()
        if rewritten:
            if debug:
                dbg["rewritten_query"] = rewritten
            return rewritten, dbg
    except Exception:
        pass

    return question, dbg


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
    # For aggregate-style results (GROUP BY, scalar sums, completion cost), verify
    # at least one numeric value is non-zero. Detected by known aggregate column names.
    numeric_keys = (
        "rows_count",
        "total_spend",
        "total_estimated_value",
        "missing_models_count",
        "estimated_completion_cost_msrp",
    )
    row_cols = set(rows[0].keys())
    if row_cols & set(numeric_keys):
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
