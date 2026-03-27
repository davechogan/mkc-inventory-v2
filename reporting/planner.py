"""Planning functions for the reporting pipeline.

Responsibilities:
  - Question predicates (is_meta, is_followup, detect_scope, etc.)
  - Heuristic and LLM-based semantic plan generation
  - Explicit constraint extraction and carryover
  - Filter normalization and conflict pruning
  - Template SQL for explicit compare mode (legacy path, kept separate from
    the canonical compiler)

Public entry points consumed by domain.py:
  _reporting_semantic_plan  — orchestrates all planning steps (stays in
                              domain.py until session.py / hints.py are
                              extracted, because it depends on
                              _reporting_get_last_query_state and
                              _reporting_get_semantic_hints)
  Everything else below is callable from domain.py after the split.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
from datetime import date, datetime, timedelta
from typing import Any, Optional

import blade_ai
from fastapi import HTTPException

from reporting.constants import (
    REPORTING_ALLOWED_SOURCES,
    REPORTING_DIRECT_LLM_SQL_META_KEY,
    REPORTING_GROUPABLE_DIMENSIONS,
    REPORTING_INTENTS,
    REPORTING_MAX_ROWS_DEFAULT,
    REPORTING_MAX_ROWS_HARD,
    REPORTING_METRICS,
    REPORTING_SERIES_ALIASES,
)
from reporting.plan_models import CanonicalReportingPlan
from reporting.plan_validator import validate_canonical_structure
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
    RE_WHICH_WHAT_KNIVES,
    RE_YEAR_4,
    RE_YEAR_VS_YEAR,
    UNSAFE_REQUEST_PATTERN_REASONS,
    extract_first_json_object,
)
from reporting.retrieval import format_retrieval_context, retrieve_artifacts_with_meta

# ---------------------------------------------------------------------------
# LLM plan filter key allowlist
# ---------------------------------------------------------------------------
_REPORTING_LLM_FILTER_KEYS = {
    "series_name",
    "family_name",
    "knife_type",
    "form_name",
    "collaborator_name",
    "steel",
    "condition",
    "location",
    "series_name__not",
    "family_name__not",
    "knife_type__not",
    "form_name__not",
    "collaborator_name__not",
    "steel__not",
    "condition__not",
    "location__not",
    "text_search",
    "text_search__not",
}

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


def _reporting_is_meta_question(question: str) -> bool:
    """Return True for questions about the system's data model or capabilities.

    These questions ask *about* the data fields or how the system works, not
    *about* the knife collection data itself.  Routing them to the SQL planner
    produces a confusing inventory listing (RPT-003).
    """
    q = " ".join((question or "").strip().lower().split())
    if not q:
        return False
    patterns = (
        "what field",
        "which field",
        "what fields",
        "which fields",
        "what column",
        "which column",
        "what data",
        "what information do you",
        "what are you using for",
        "what do you use for",
        "how do you calculate",
        "how do you determine",
        "how is cost",
        "how is spend",
        "how is value",
        "what is your cost",
        "what is the cost field",
        "what does cost mean",
        "what does spend mean",
        "what does value mean",
        "what metrics",
        "what types of questions",
        "what can you answer",
        "what can you tell",
        "what kinds of questions",
    )
    return any(p in q for p in patterns)


def _reporting_build_prompt_schema(conn: sqlite3.Connection) -> str:
    chunks = []
    for view in sorted(REPORTING_ALLOWED_SOURCES):
        cols = conn.execute(f"PRAGMA table_info({view})").fetchall()
        names = ", ".join(c["name"] for c in cols)
        chunks.append(f"- {view}: {names}")
    return "\n".join(chunks)


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
        "that made up that number",
        "made up that number",
        "cost of each knife",
        "each knife",
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
    elif (
        "list the knives that made up that number" in q
        or "made up that number" in q
        or "cost of each knife" in q
        or "list the knives that made up" in q
    ):
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

    # Negation / exclusion phrases:
    # - "except Speedgoat"
    # - "without traditions"
    # - "excluding the Damascus and Traditions versions"
    m_ex = re.search(r"\b(?:except|without|exclude|excluding)\s+(.+)$", q)
    if m_ex:
        raw_tail = m_ex.group(1).strip()
        raw_tail = re.sub(r"[?.!,;:]+$", "", raw_tail).strip()
        raw_tail = re.sub(r"\b(versions?|models?|knives?|ones?)\b", " ", raw_tail).strip()
        parts = [p.strip(" -") for p in re.split(r"\s+(?:and|or)\s+|,", raw_tail) if p.strip(" -")]
        series_ex: list[str] = []
        type_ex: list[str] = []
        text_ex: list[str] = []
        for part in parts:
            norm = _reporting_normalize_filter_value("text_search", part)
            if not norm:
                continue
            series_norm = _reporting_normalize_filter_value("series_name", norm)
            if series_norm and series_norm in REPORTING_SERIES_ALIASES.values():
                if series_norm not in series_ex:
                    series_ex.append(series_norm)
                continue
            if norm in {"tactical", "hunting"}:
                val = norm.title()
                if val not in type_ex:
                    type_ex.append(val)
                continue
            if norm not in text_ex:
                text_ex.append(norm)
        if series_ex:
            out["filters"]["series_name__not"] = series_ex if len(series_ex) > 1 else series_ex[0]
        if type_ex:
            out["filters"]["knife_type__not"] = type_ex if len(type_ex) > 1 else type_ex[0]
        if text_ex:
            out["filters"]["text_search__not"] = text_ex if len(text_ex) > 1 else text_ex[0]

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
    Resolve ambiguous or contradictory filters that would over-constrain results to zero rows.

    Two cases are handled:

    1. Same-field contradiction (RPT-001): a positive filter and its negation for the same
       field (e.g. series_name="Blood Brothers" and series_name__not="Blood Brothers") produce
       a SQL WHERE clause that can never be satisfied.  Drop the exclusion when the positive
       and negative values are equal under normalization.

    2. Cross-dimension ambiguity: series_name, family_name, and knife_type carrying the same
       normalized value simultaneously (e.g. from a heuristic that stamped the same entity
       into multiple dimensions).  Drop the redundant dimensions unless the user explicitly
       requested a breakdown by that dimension.
    """
    q = " ".join((question or "").strip().lower().split())
    out = dict(filters or {})
    if not out:
        return out

    # --- Case 1: same-field positive+negative contradiction ---
    for key in list(out.keys()):
        if key.endswith("__not"):
            base = key[:-5]
            pos_val = out.get(base)
            neg_val = out[key]
            if pos_val is not None:
                norm_pos = _reporting_normalize_filter_value(base, pos_val)
                norm_neg = _reporting_normalize_filter_value(base, neg_val)
                if norm_pos and norm_neg and norm_pos.lower() == norm_neg.lower():
                    # Drop the exclusion; the positive filter is the user's intent.
                    out.pop(key)

    # --- Case 2: cross-dimension ambiguity ---
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

    top_m = re.search(r"\btop\s+(\d{1,4})\b", q)
    if top_m:
        plan["limit"] = min(REPORTING_MAX_ROWS_HARD, max(1, int(top_m.group(1))))

    inv_scope = str(plan.get("scope") or "inventory").strip().lower() == "inventory"
    if (
        inv_scope
        and plan.get("intent") == "list_inventory"
        and not plan.get("group_by")
        and any(w in q for w in ("expensive", "most expensive", "highest price", "priciest", "costliest"))
        and any(w in q for w in ("purchase", "purchases", "paid", "buy", "bought"))
    ):
        plan["sort"] = {"field": "purchase_price", "direction": "desc"}
        if not top_m:
            plan["limit"] = min(10, REPORTING_MAX_ROWS_HARD, max(1, int(plan.get("limit") or 10)))

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


def _reporting_planner_hints_payload(
    question: str,
    explicit: dict[str, Any],
    heuristic: dict[str, Any],
    learned_hints: dict[str, Any],
    last_state: Optional[dict[str, Any]],
) -> dict[str, Any]:
    ds, de, dl = _reporting_detect_date_bounds(question)
    yc = _reporting_detect_year_comparison(question)
    q = " ".join((question or "").strip().lower().split())
    compact_h: dict[str, Any] = {}
    for k in ("intent", "metric", "group_by", "scope", "limit"):
        if heuristic.get(k) is not None:
            compact_h[k] = heuristic.get(k)
    hf = heuristic.get("filters")
    if isinstance(hf, dict) and hf:
        compact_h["filters"] = dict(list(hf.items())[:16])
    out: dict[str, Any] = {
        "advisory_explicit_constraints": {k: v for k, v in explicit.items() if v not in (None, "", [], {})},
        "advisory_heuristic_guess": compact_h,
        "retrieval_learned_hints": learned_hints.get("hints") or [],
        "detected_date_window": {"start": ds, "end": de, "label": dl},
        "detected_year_compare": [yc[0], yc[1]] if yc else None,
    }
    if _reporting_is_followup(q):
        out["prior_turn_plan_summary"] = _reporting_summarize_state_for_hints(last_state)
    return out


def _reporting_repair_completion_cost_vs_ranked_purchases(question: str, plan: dict[str, Any]) -> None:
    """If the model chose completion_cost but the question is about ranked owned purchases, normalize intent."""
    q = " ".join((question or "").strip().lower().split())
    if plan.get("intent") != "completion_cost":
        return
    ranked_owned = ("purchase" in q or "purchases" in q or "bought" in q or "paid" in q) and (
        "expensive" in q or "top" in q or "most" in q or "highest" in q or "price" in q
    )
    if not ranked_owned:
        return
    plan["intent"] = "list_inventory"
    plan["metric"] = "count"
    plan["sort"] = {"field": "purchase_price", "direction": "desc"}
    m = re.search(r"\btop\s+(\d+)\b", q)
    if m and m.group(1).isdigit():
        plan["limit"] = min(REPORTING_MAX_ROWS_HARD, max(1, int(m.group(1))))
    elif re.search(r"\b(\d+)\s+most\s+expensive\b", q):
        m2 = re.search(r"\b(\d+)\s+most\s+expensive\b", q)
        if m2 and m2.group(1).isdigit():
            plan["limit"] = min(REPORTING_MAX_ROWS_HARD, max(1, int(m2.group(1))))


def _reporting_legacy_plan_from_llm_dict(llm_plan: dict[str, Any], question: str) -> dict[str, Any]:
    plan: dict[str, Any] = {
        "intent": "list_inventory",
        "scope": "inventory",
        "metric": "count",
        "filters": {},
        "group_by": None,
        "limit": REPORTING_MAX_ROWS_DEFAULT,
    }
    ri = str(llm_plan.get("intent") or "").strip()
    if ri in REPORTING_INTENTS:
        plan["intent"] = ri
    rm = str(llm_plan.get("metric") or "").strip()
    if rm in REPORTING_METRICS:
        plan["metric"] = rm
    sc = str(llm_plan.get("scope") or "").strip().lower()
    if sc in {"inventory", "catalog"}:
        plan["scope"] = sc
    gb = llm_plan.get("group_by")
    if isinstance(gb, str) and gb.strip() and gb.strip() in REPORTING_GROUPABLE_DIMENSIONS.values():
        plan["group_by"] = gb.strip()
    try:
        lim_raw = llm_plan.get("limit")
        if lim_raw is not None:
            plan["limit"] = min(REPORTING_MAX_ROWS_HARD, max(1, int(lim_raw)))
    except (TypeError, ValueError):
        pass
    if isinstance(llm_plan.get("filters"), dict):
        merged_f: dict[str, Any] = {}
        for k, v in llm_plan["filters"].items():
            if k in _REPORTING_LLM_FILTER_KEYS:
                sv = _reporting_normalize_filter_value(k, str(v or ""))
                if sv:
                    merged_f[k] = sv
        plan["filters"] = merged_f
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
    raw_sort = llm_plan.get("sort")
    if isinstance(raw_sort, dict):
        sf = str(raw_sort.get("field") or "").strip()
        sd = str(raw_sort.get("direction") or "desc").strip().lower()
        if sf and sd in ("asc", "desc"):
            plan["sort"] = {"field": sf, "direction": sd}
    _reporting_repair_completion_cost_vs_ranked_purchases(question, plan)
    if plan.get("intent") == "list_inventory" and plan.get("metric") not in {"count", "total_estimated_value"}:
        plan["metric"] = "count"
    plan["filters"] = _reporting_prune_conflicting_filters(question, plan.get("filters") or {})
    plan["scope"] = str(plan.get("scope") or "inventory")
    if plan["scope"] not in {"inventory", "catalog"}:
        plan["scope"] = "inventory"
    return plan


def _reporting_clarification_plan_planner_failed() -> dict[str, Any]:
    return {
        "intent": "list_inventory",
        "scope": "inventory",
        "metric": "count",
        "filters": {},
        "group_by": None,
        "limit": REPORTING_MAX_ROWS_DEFAULT,
        "needs_clarification": True,
        "clarification_reason": (
            "The planner could not produce structured JSON for this question. "
            "Rephrase with a concrete scope (series, date window, inventory vs catalog, or sort intent)."
        ),
    }


def _reporting_apply_followup_carryover(
    plan: dict[str, Any],
    last_state: Optional[dict[str, Any]],
    question: str,
) -> None:
    """For contextual follow-ups, carry forward prior filters and reshape list-after-aggregate prompts."""
    if not last_state or not _reporting_is_followup(question):
        return
    # Do not carry forward filters from a prior turn that returned zero rows.
    # Those filters are likely contradictory or over-constrained; inheriting them
    # locks the conversation into an unrecoverable empty state (RPT-002).
    prior_row_count = last_state.get("_result_row_count")
    if prior_row_count is not None and prior_row_count == 0:
        return
    q = " ".join(question.strip().lower().split())
    prev_f = dict(last_state.get("filters") or {})
    if prev_f:
        plan["filters"] = {**prev_f, **dict(plan.get("filters") or {})}
    if last_state.get("intent") == "aggregate" and any(
        phrase in q for phrase in ("list", "show", "which knives", "which ones", "break down", "made up")
    ):
        plan["intent"] = "list_inventory"
        plan["metric"] = "count"
        plan["group_by"] = None


def _reporting_merge_explicit_constraints_into_plan(plan: dict[str, Any], explicit: dict[str, Any]) -> dict[str, Any]:
    """Apply regex/phrase extractions from the raw question (explicit constraints) on top of the LLM plan."""
    if explicit.get("intent") in REPORTING_INTENTS:
        plan["intent"] = explicit["intent"]
    if explicit.get("metric") in REPORTING_METRICS:
        plan["metric"] = explicit["metric"]
    if plan.get("intent") == "list_inventory" and plan.get("metric") not in {"count", "total_estimated_value"}:
        plan["metric"] = "count"
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
    return plan


def _reporting_llm_plan(
    conn: sqlite3.Connection,
    model: str,
    question: str,
    context_block: str,
    schema_context: str,
    *,
    planner_hints: Optional[dict[str, Any]] = None,
    debug: bool = False,
) -> tuple[Optional[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    retrieval_artifacts, retrieval_meta = retrieve_artifacts_with_meta(
        question, top_k=6, conn=conn, debug=debug
    )
    retrieval_ctx = format_retrieval_context(retrieval_artifacts)
    system = (
        "You convert collection questions into semantic JSON plans. Do not generate SQL. "
        "Return JSON only with keys: intent, scope, filters, group_by, metric, limit, sort, date_start, date_end, year_compare. "
        "intent must be one of: missing_models, list_inventory, aggregate, completion_cost. "
        "scope must be inventory or catalog. "
        "filters is an object using only: series_name, family_name, knife_type, form_name, collaborator_name, steel, condition, location "
        "(and parallel __not keys or text_search as in schema notes). "
        "group_by must be null or one of: series_name, family_name, knife_type, form_name, collaborator_name, steel, condition, location. "
        "metric must be one of: count, total_spend, total_estimated_value. "
        "limit is a positive integer row cap when the user asks for top N or a short list. "
        "sort is null or {\"field\": \"purchase_price\", \"direction\": \"asc\"|\"desc\"} for ranked inventory rows "
        "(e.g. most expensive purchases). "
        "Use completion_cost only for cost-to-complete-the-collection / missing-model MSRP style questions—not for ranking knives you already bought by purchase price. "
        "date_start/date_end must be YYYY-MM-DD or null. "
        "year_compare must be null or [YYYY, YYYY] when user asks year-vs-year. "
        "Align the JSON with the user question; advisory hints (if present) are suggestions only."
    )
    hints_block = ""
    if planner_hints:
        try:
            hints_block = "\n\nPlanner hints (advisory JSON, not orders):\n" + json.dumps(
                planner_hints, ensure_ascii=False, default=str
            )
        except (TypeError, ValueError):
            hints_block = ""
    user = (
        f"Schema:\n{schema_context}\n\n"
        f"Retrieved grounding:\n{retrieval_ctx or '(none)'}\n\n"
        f"Context:\n{context_block or '(none)'}\n\n"
        f"Question:\n{question}\n"
        f"{hints_block}"
    )
    planner_debug: dict[str, Any] = {}
    if debug:
        planner_debug = {
            "model": model,
            "system": system,
            "user": user,
            "retrieval_context_block": retrieval_ctx,
        }
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
        if debug:
            planner_debug["raw_response"] = raw
            planner_debug["parsed_plan"] = parsed if isinstance(parsed, dict) else None
        return (parsed if isinstance(parsed, dict) else None), retrieval_meta, planner_debug
    except Exception as exc:
        if debug:
            planner_debug["exception"] = repr(exc)
        return None, retrieval_meta, planner_debug


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


# Computed after reporting_scope_preprocessing_enabled is defined above.
REPORTING_SCOPE_PREPROCESSING = reporting_scope_preprocessing_enabled(
    os.environ.get("REPORTING_SCOPE_PREPROCESSING")
)
