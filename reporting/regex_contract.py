"""
Compiled regex and LLM-output helpers for reporting.

Read this before adding new ``re...`` calls in :mod:`reporting.domain`.

Layers (keep them distinct)
---------------------------
**A — Pre-planner heuristics** — Raw user text: date phrases, scope cues, unsafe-request guardrails,
entity hints, explicit constraint extraction. Prefer extending the **semantic plan** (new keys carried
through ``_reporting_semantic_plan`` and validated in ``_reporting_plan_to_sql``) instead of new
English phrase regex when the planner can emit structured fields.

**B — Planner contract** — Shapes from ``_reporting_llm_plan``; validate dates/years with
``RE_DATE_ISO`` / ``RE_YEAR_4`` ``fullmatch``, not loose text scans of the question.

**C — LLM response extraction** — Recover JSON or SQL from non-compliant model output.
``RE_FIRST_JSON_OBJECT`` is **greedy** (first `{` through last `}`); prefer prompts that return
strict JSON. Used for planner JSON, responder JSON, and similar.

**D — SQL gate** — ``_reporting_validate_sql``: allowlisted sources, forbidden tokens, ``FROM``/``JOIN``
identifier capture — not natural-language understanding.

Do **not** add new one-off question regex here without checking whether a **plan field** or **filter key**
already covers the intent.
"""

from __future__ import annotations

import re
from typing import Optional

# --- Layer B: structured plan / validation ---
RE_DATE_ISO = re.compile(r"\d{4}-\d{2}-\d{2}")
RE_YEAR_4 = re.compile(r"(?:19|20)\d{2}")

# --- Layer A: question text heuristics (dates, scope, guardrails) ---
RE_LAST_N_DAYS = re.compile(r"\blast\s+(\d+)\s+days?\b")
RE_LAST_N_MONTHS = re.compile(r"\blast\s+(\d+)\s+months?\b")
RE_LAST_N_YEARS = re.compile(r"\blast\s+(\d+)\s+years?\b")
RE_SINCE_ISO_DATE = re.compile(r"\bsince\s+(\d{4}-\d{2}-\d{2})\b")
RE_YEAR_VS_YEAR = re.compile(
    r"\b((?:19|20)\d{2})\b\s*(?:vs|versus)\s*\b((?:19|20)\d{2})\b",
    re.I,
)

RE_SCOPE_OWNED = re.compile(r"\bowned\b")
RE_SCOPE_OWN = re.compile(r"\bown\b")
RE_WHICH_WHAT_KNIVES = re.compile(r"\b(which|what)\s+knives\b", re.I)

RE_COMPLETION_COST_LEX = re.compile(
    r"\b(how much|cost|price|msrp|estimate|estimated)\b",
    re.I,
)

UNSAFE_REQUEST_PATTERN_REASONS: tuple[tuple[str, str], ...] = (
    (
        r"(?is)```(?:sql)?\s*(select|with|insert|update|delete|drop|alter|create)\b",
        "sql_code_block",
    ),
    (
        r"\b(drop\s+table|delete\s+from|insert\s+into|update\s+\w+\s+set|alter\s+table|create\s+table)\b",
        "mutating_sql_phrase",
    ),
    (r"\b(pragma|sqlite_master|information_schema)\b", "schema_exfiltration_phrase"),
    (r"\bunion\s+select\b", "union_select_phrase"),
    (r";\s*(select|with|insert|update|delete|drop|alter|create)\b", "multi_statement_hint"),
    (
        r"(?is)\b(ignore|bypass|override)\b.{0,40}\b(instruction|guardrail|safety|policy)\b",
        "guardrail_bypass_phrase",
    ),
)

RE_DIRECT_SQL_USER_PREFIX = re.compile(
    r"^\s*(select|with|insert|update|delete|drop|alter|pragma)\b"
    r"|^\s*create\s+(table|view|index|database|trigger|virtual)\b",
)

# --- Layer D: validated reporting SQL ---
RE_SQL_QUOTED_RELATION_REF = re.compile(r'\b(?:from|join)\s+["`\\[]')
RE_SQL_FROM_JOIN_IDENT = re.compile(r"\b(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*)")

# --- Layer C: LLM / markdown cleanup ---
RE_FIRST_JSON_OBJECT = re.compile(r"\{.*\}", re.S)
RE_SQL_FENCED_BLOCK = re.compile(r"(?is)```(?:sql)?\s*((?:select|with)\b.*?)```")
RE_SQL_LOOSE_FROM_SELECT = re.compile(r"(?is)\b(select|with)\b.+")
RE_MARKDOWN_LEADING_FENCE = re.compile(r"(?is)^```(?:sql)?\s*")
RE_MARKDOWN_TRAILING_FENCE = re.compile(r"(?is)\s*```$")

# Filter / entity normalization (Layer A adjunct)
RE_NORM_STRIP_ANY_ALL = re.compile(r"\b(any|all|the|a|an)\b")
RE_NORM_STRIP_KNIFE_WORDS = re.compile(r"\b(knife|knives|model|models)\b")
RE_NORM_STRIP_INVENTORY_PHRASES = re.compile(
    r"\b(do i have|that i have|in my inventory|from inventory|from my inventory)\b"
)
RE_NORM_STRIP_POLITE_VERBS = re.compile(r"\b(please|show|list|count|how many)\b")

RE_HINT_ENTITY_STOP_PREFIX = re.compile(
    r"^(how many|what is|what are|which|show me|show|list|count|are there|there are|there is|in|the)\s+",
    re.I,
)
RE_HINT_ENTITY_STOP_SUFFIX = re.compile(
    r"\s+(in|the|my|our|collection|inventory|there|are|is|by)$",
    re.I,
)


def extract_first_json_object(raw: str) -> Optional[str]:
    """Return the greedy ``{...}`` substring for ``json.loads``, or None."""
    m = RE_FIRST_JSON_OBJECT.search(raw or "")
    return m.group(0) if m else None


def clean_llm_sql_fences(candidate: str) -> str:
    """Strip common markdown SQL fences from a model-emitted SQL fragment."""
    s = (candidate or "").strip()
    s = RE_MARKDOWN_LEADING_FENCE.sub("", s).strip()
    s = RE_MARKDOWN_TRAILING_FENCE.sub("", s).strip()
    return s
