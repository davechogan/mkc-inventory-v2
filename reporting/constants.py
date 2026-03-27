"""Shared constants for the reporting pipeline.

All modules in the reporting package import from here to avoid circular
imports and to keep the single source of truth in one place.
"""
from __future__ import annotations

import os
import sqlite3
from collections.abc import Callable
from contextlib import AbstractContextManager

# ---------------------------------------------------------------------------
# Connection type aliases
# ---------------------------------------------------------------------------
_ConnectionCtx = AbstractContextManager[sqlite3.Connection]
GetConn = Callable[[], _ConnectionCtx]

# ---------------------------------------------------------------------------
# Model routing
# ---------------------------------------------------------------------------
REPORTING_DEFAULT_MODEL = "qwen2.5:7b-instruct"
REPORTING_PLANNER_MODEL = (
    os.environ.get("REPORTING_PLANNER_MODEL") or "qwen2.5:32b-instruct"
).strip() or "qwen2.5:32b-instruct"
REPORTING_RESPONDER_MODEL = (
    os.environ.get("REPORTING_RESPONDER_MODEL") or "qwen2.5:7b-instruct"
).strip() or "qwen2.5:7b-instruct"
REPORTING_PLANNER_RETRY_MODEL = (
    (os.environ.get("REPORTING_PLANNER_RETRY_MODEL") or "").strip() or None
)

# ---------------------------------------------------------------------------
# Row limits
# ---------------------------------------------------------------------------
REPORTING_MAX_ROWS_DEFAULT = 200
REPORTING_MAX_ROWS_HARD = 1000

# ---------------------------------------------------------------------------
# SQL safety
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# Plan vocabulary
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# Hint subsystem
# ---------------------------------------------------------------------------
REPORTING_HINT_MIN_CONFIDENCE = 0.55
REPORTING_HINT_PROMOTION_ENABLED = (
    os.environ.get("REPORTING_HINT_PROMOTION_ENABLED", "1").strip().lower()
    in {"1", "true", "yes", "on"}
)
REPORTING_HINT_PROMOTION_MIN_CONFIDENCE = float(
    os.environ.get("REPORTING_HINT_PROMOTION_MIN_CONFIDENCE") or 0.80
)
REPORTING_HINT_PROMOTION_MIN_EVIDENCE = int(
    os.environ.get("REPORTING_HINT_PROMOTION_MIN_EVIDENCE") or 3
)
REPORTING_HINT_PROMOTION_MAX_PER_RUN = int(
    os.environ.get("REPORTING_HINT_PROMOTION_MAX_PER_RUN") or 50
)

# ---------------------------------------------------------------------------
# Debug / feature flags
# ---------------------------------------------------------------------------

# When set, bypass semantic planning and ask the LLM to generate SQL directly.
# SQL still runs through the safety validator + executor.
REPORTING_DIRECT_LLM_SQL_META_KEY = "reporting_direct_llm_sql"
