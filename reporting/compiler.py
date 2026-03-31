"""SQL compiler for the reporting pipeline.

Contract (per spec §8):
  - compile_plan() accepts ONLY a validated CanonicalReportingPlan.
  - The compiler translates; it does not interpret, infer missing fields,
    silently repair bad plans, or add hidden filters.
  - SQL produced here is always run through validate_sql() before execution.

Public API:
  compile_plan(plan, date_start, date_end, max_rows) -> (sql, meta)
  validate_sql(sql) -> str                 (raises HTTPException on violation)
  exec_sql(conn, sql, max_rows) -> (cols, rows, elapsed_ms)
"""
from __future__ import annotations

import re
import sqlite3
import time
from typing import Any, Optional

from fastapi import HTTPException

from reporting.constants import (
    REPORTING_ALLOWED_SOURCES,
    REPORTING_FORBIDDEN_SQL,
    REPORTING_GROUPABLE_DIMENSIONS,
    REPORTING_MAX_ROWS_DEFAULT,
    REPORTING_MAX_ROWS_HARD,
)
from reporting.plan_models import (
    CanonicalReportingPlan,
    FilterClause,
    FilterOp,
    PlanIntent,
    PlanMetric,
    PlanScope,
)
from reporting.regex_contract import (
    RE_SQL_FROM_JOIN_IDENT,
    RE_SQL_QUOTED_RELATION_REF,
    RE_YEAR_4,
)


# ---------------------------------------------------------------------------
# SQL safety layer
# ---------------------------------------------------------------------------

def validate_sql(sql: str) -> str:
    """Validate and sanitize a SQL string.

    Raises HTTPException(400) on any violation.
    Returns the cleaned SQL string on success.
    """
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
        if len(non_empty) > 1:
            raise HTTPException(status_code=400, detail="Multi-statement SQL is not allowed.")
        s = non_empty[0] if non_empty else ""
        lower = s.lower()
    if not (lower.startswith("select ") or lower.startswith("with ")):
        raise HTTPException(status_code=400, detail="Only SELECT queries are allowed.")
    if RE_SQL_QUOTED_RELATION_REF.search(lower):
        raise HTTPException(
            status_code=400,
            detail="Quoted relation references are not allowed in reporting SQL.",
        )
    for token in REPORTING_FORBIDDEN_SQL:
        if re.search(rf"\b{token}\b", lower):
            raise HTTPException(status_code=400, detail=f"Forbidden SQL token: {token}")
    refs = RE_SQL_FROM_JOIN_IDENT.findall(lower)
    if not refs:
        raise HTTPException(
            status_code=400, detail="Query must read from approved reporting sources."
        )
    for ref in refs:
        if ref not in REPORTING_ALLOWED_SOURCES:
            raise HTTPException(status_code=400, detail=f"Source not allowed: {ref}")
    return s


def exec_sql(
    conn: sqlite3.Connection,
    sql: str,
    max_rows: int,
) -> tuple[list[str], list[dict[str, Any]], float]:
    """Validate then execute a SQL string. Returns (columns, rows, elapsed_ms)."""
    started = time.perf_counter()
    safe_sql = validate_sql(sql)
    try:
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


# ---------------------------------------------------------------------------
# Canonical compiler entry point
# ---------------------------------------------------------------------------

def compile_plan(
    plan: CanonicalReportingPlan,
    date_start: Optional[str],
    date_end: Optional[str],
    max_rows: int,
) -> tuple[Optional[str], dict[str, Any]]:
    """Compile a validated canonical plan to SQL.

    Raises TypeError if anything other than a CanonicalReportingPlan is passed.
    This is the enforcement point for the compiler contract.
    """
    if not isinstance(plan, CanonicalReportingPlan):
        raise TypeError("Compiler requires CanonicalReportingPlan input.")
    return _compile_canonical(plan, date_start, date_end, max_rows)


# ---------------------------------------------------------------------------
# Filter expression helpers
# ---------------------------------------------------------------------------

# Columns searched by text_search per source view.
_CATALOG_TEXT_COLS = (
    "official_name", "family_name", "form_name", "series_name", "collaborator_name"
)
_INVENTORY_TEXT_COLS = (
    "knife_name", "family_name", "form_name", "series_name", "collaborator_name"
)


def _esc(v: Any) -> str:
    """Escape a scalar value for safe embedding in a SQL string literal."""
    return str(v or "").replace("'", "''").strip()


def _clause_expr(
    clause: FilterClause,
    *,
    catalog_style: bool,
    prefix: str = "",
) -> Optional[str]:
    """Translate a FilterClause to a positive SQL expression fragment.

    For exclusions the caller wraps the result in NOT (...).
    prefix is a table alias with trailing dot (e.g. "m." for missing_models JOIN).

    Returns None when the clause is irrelevant for the source view (e.g. a
    catalog-only field in an inventory query) so callers can silently skip it.
    """
    field = clause.field.value
    op = clause.op
    val = clause.value

    # text_search: multi-column LIKE across name-bearing columns.
    if field == "text_search":
        cols = _CATALOG_TEXT_COLS if catalog_style else _INVENTORY_TEXT_COLS
        vals: list[str] = [str(val)] if not isinstance(val, list) else [str(v) for v in val]
        parts: list[str] = []
        for raw_val in vals:
            ev = _esc(raw_val)
            col_exprs = [
                f"lower(COALESCE({prefix}{c}, '')) LIKE lower('%{ev}%')" for c in cols
            ]
            parts.append(f"({' OR '.join(col_exprs)})")
        return f"({' AND '.join(parts)})" if parts else None

    # Skip fields that don't exist in the source view.
    _inv_only = {
        "location", "knife_name", "acquired_date",
        "purchase_price", "quantity",
    }
    _cat_only = {
        "msrp", "official_name", "handle_type",
    }
    if catalog_style and field in _inv_only:
        return None
    if not catalog_style and field in _cat_only:
        return None

    col = f"{prefix}{field}"

    if op == FilterOp.EQ:
        ev = _esc(str(val))
        return f"lower(COALESCE({col}, '')) = lower('{ev}')"
    if op == FilterOp.NEQ:
        ev = _esc(str(val))
        return f"lower(COALESCE({col}, '')) != lower('{ev}')"
    if op == FilterOp.CONTAINS:
        ev = _esc(str(val))
        return f"lower(COALESCE({col}, '')) LIKE lower('%{ev}%')"
    if op == FilterOp.NOT_CONTAINS:
        ev = _esc(str(val))
        return f"lower(COALESCE({col}, '')) NOT LIKE lower('%{ev}%')"
    if op == FilterOp.IN:
        items = val if isinstance(val, list) else [val]
        quoted = ", ".join(f"lower('{_esc(str(v))}')" for v in items)
        return f"lower(COALESCE({col}, '')) IN ({quoted})"
    if op == FilterOp.NOT_IN:
        items = val if isinstance(val, list) else [val]
        quoted = ", ".join(f"lower('{_esc(str(v))}')" for v in items)
        return f"lower(COALESCE({col}, '')) NOT IN ({quoted})"
    if op == FilterOp.GT:
        return f"CAST({col} AS REAL) > {float(val)}"  # type: ignore[arg-type]
    if op == FilterOp.GTE:
        return f"CAST({col} AS REAL) >= {float(val)}"  # type: ignore[arg-type]
    if op == FilterOp.LT:
        return f"CAST({col} AS REAL) < {float(val)}"  # type: ignore[arg-type]
    if op == FilterOp.LTE:
        return f"CAST({col} AS REAL) <= {float(val)}"  # type: ignore[arg-type]
    if op == FilterOp.BETWEEN and isinstance(val, list) and len(val) == 2:
        return f"CAST({col} AS REAL) BETWEEN {float(val[0])} AND {float(val[1])}"  # type: ignore[arg-type]
    return None


def _build_where_fragments(
    filters: list[FilterClause],
    exclusions: list[FilterClause],
    *,
    catalog_style: bool,
    prefix: str = "",
) -> list[str]:
    """Build WHERE clause fragments from canonical filter/exclusion lists.

    Filters produce positive conditions; exclusions are wrapped in NOT (...).
    Clauses irrelevant to the source view are silently skipped.
    """
    frags: list[str] = []
    for clause in filters:
        expr = _clause_expr(clause, catalog_style=catalog_style, prefix=prefix)
        if expr:
            frags.append(expr)
    for clause in exclusions:
        expr = _clause_expr(clause, catalog_style=catalog_style, prefix=prefix)
        if expr:
            frags.append(f"NOT ({expr})")
    return frags


# ---------------------------------------------------------------------------
# Internal SQL builder
# ---------------------------------------------------------------------------

def _compile_canonical(
    plan: CanonicalReportingPlan,
    date_start: Optional[str],
    date_end: Optional[str],
    max_rows: int,
) -> tuple[Optional[str], dict[str, Any]]:
    """Build SQL directly from a validated CanonicalReportingPlan.

    Called only via compile_plan() after the plan has passed both structural
    and semantic validation. The compiler translates; it does not repair.
    """
    use_catalog = plan.scope == PlanScope.CATALOG
    limit = min(max_rows, plan.limit or max_rows)
    group_by = plan.group_by[0].value if plan.group_by else None

    # Time range: explicit plan dates take priority over caller-supplied range.
    effective_date_start = (plan.time_range.start if plan.time_range else None) or date_start
    effective_date_end = (plan.time_range.end if plan.time_range else None) or date_end

    # Year-compare: validated at plan layer; extract as string pair.
    year_compare: Optional[tuple[str, str]] = None
    if len(plan.year_compare) == 2:
        ya, yb = str(plan.year_compare[0]), str(plan.year_compare[1])
        if RE_YEAR_4.fullmatch(ya) and RE_YEAR_4.fullmatch(yb):
            year_compare = (ya, yb)

    # ── missing_models branch ───────────────────────────────────────────────
    if plan.intent == PlanIntent.MISSING_MODELS:
        inv_join = (
            "LEFT JOIN (SELECT knife_model_id, SUM(COALESCE(quantity, 1)) AS total_qty "
            "FROM reporting_inventory GROUP BY knife_model_id) inv "
            "ON inv.knife_model_id = m.model_id"
        )
        where = ["COALESCE(inv.total_qty, 0) = 0"]
        where += _build_where_fragments(
            plan.filters, plan.exclusions, catalog_style=True, prefix="m."
        )
        where_sql = f"WHERE {' AND '.join(where)}"

        if plan.metric == PlanMetric.MSRP:
            sql = (
                "SELECT "
                "COUNT(*) AS missing_models_count, "
                "ROUND(SUM(COALESCE(m.msrp, 0)), 2) AS estimated_completion_cost_msrp, "
                "ROUND(AVG(COALESCE(m.msrp, 0)), 2) AS avg_missing_model_msrp "
                f"FROM reporting_models m {inv_join} {where_sql}"
            )
            return sql, {"mode": "semantic_compiled_completion_cost"}

        sql = (
            "SELECT m.model_id, m.official_name, m.knife_type, m.family_name, m.form_name, "
            "m.series_name, m.collaborator_name, "
            "COALESCE(inv.total_qty, 0) AS inventory_quantity "
            f"FROM reporting_models m {inv_join} {where_sql} "
            "ORDER BY m.official_name "
            f"LIMIT {limit}"
        )
        return sql, {"mode": "semantic_compiled_missing_models"}

    # ── list branch (all other queries) ────────────────────────────────────
    where = _build_where_fragments(
        plan.filters, plan.exclusions, catalog_style=use_catalog
    )
    # Date filters only apply to inventory (reporting_models has no acquired_date).
    where_filters_only = list(where)
    if not use_catalog:
        if effective_date_start:
            where.append(f"acquired_date >= '{_esc(effective_date_start)}'")
        if effective_date_end:
            where.append(f"acquired_date <= '{_esc(effective_date_end)}'")
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    # Source view: catalog unless an inventory-only group_by forces a switch.
    source_view = "reporting_models" if use_catalog else "reporting_inventory"
    supported_catalog_group = {
        "series_name", "family_name", "knife_type", "form_name", "collaborator_name", "steel",
        "blade_finish", "handle_color", "blade_color", "blade_length",
        "handle_type", "generation_label", "size_modifier",
    }
    if use_catalog and group_by and group_by not in supported_catalog_group:
        source_view = "reporting_inventory"

    # Aggregate expressions — guard against inventory-only columns in catalog view.
    _inv_only_metric = (
        plan.metric in (PlanMetric.TOTAL_SPEND,)
        and source_view == "reporting_models"
    )
    if plan.metric == PlanMetric.TOTAL_SPEND and not _inv_only_metric:
        agg_expr = "ROUND(SUM(COALESCE(purchase_price, 0)), 2) AS total_spend"
        agg_sort_col = "total_spend"
    else:
        agg_expr = "COUNT(*) AS rows_count"
        agg_sort_col = "rows_count"

    # Year-over-year comparison.
    if year_compare and not group_by:
        ya, yb = year_compare
        yc_parts = (
            _build_where_fragments(plan.filters, plan.exclusions, catalog_style=False)
            if use_catalog
            else list(where_filters_only)
        )
        yc_parts.append("acquired_date IS NOT NULL")
        yc_parts.append(f"substr(acquired_date, 1, 4) IN ('{_esc(ya)}', '{_esc(yb)}')")
        yc_where_sql = f"WHERE {' AND '.join(yc_parts)}"
        meta: dict[str, Any] = {"mode": "semantic_compiled_year_compare"}
        if use_catalog:
            meta["year_compare_inventory_note"] = (
                "Catalog filters preserved; results are bucketed by inventory acquired_date "
                "(year-over-year needs collection dates, not the catalog view alone)."
            )
        sql = (
            "SELECT substr(acquired_date, 1, 4) AS bucket, "
            f"{agg_expr} "
            "FROM reporting_inventory "
            f"{yc_where_sql} "
            "GROUP BY bucket "
            "ORDER BY bucket"
        )
        return sql, meta

    # Grouped aggregate.
    if group_by in REPORTING_GROUPABLE_DIMENSIONS.values() and not _inv_only_metric:
        sql = (
            f"SELECT COALESCE({group_by}, 'Unknown') AS bucket, {agg_expr} "
            f"FROM {source_view} "
            f"{where_sql} "
            "GROUP BY bucket "
            f"ORDER BY {agg_sort_col} DESC "
            f"LIMIT {limit}"
        )
        return sql, {"mode": "semantic_compiled_aggregate"}

    # Scalar aggregate (no group_by, no sort, metric is a sum over the whole set).
    # If there's a sort field, the user wants a list, not an aggregate total.
    if plan.metric in (PlanMetric.TOTAL_SPEND,) and not _inv_only_metric and not plan.sort:
        sql = f"SELECT {agg_expr} FROM {source_view} {where_sql}"
        return sql, {"mode": "semantic_compiled_aggregate"}

    # ── List path ───────────────────────────────────────────────────────────
    sort_field = plan.sort.field if plan.sort else ""
    sort_dir = plan.sort.direction.value if plan.sort else "asc"
    ord_kw = "DESC" if sort_dir == "desc" else "ASC"
    line_total_sql = "COALESCE(purchase_price, 0)"

    if use_catalog:
        catalog_sort_map = {
            "msrp": ("msrp", "msrp"),
            "knife_name": ("knife_name", None),
            "official_name": ("knife_name", None),
        }
        sort_entry = catalog_sort_map.get(sort_field)
        if sort_entry:
            sort_col, extra_col = sort_entry
            extra_select = f", {extra_col}" if extra_col else ""
            order_by = f"{sort_col} {ord_kw}, knife_name"
        else:
            extra_select = ""
            order_by = "knife_name"
        sql = (
            "SELECT model_id, official_name AS knife_name, knife_type, family_name, form_name, "
            "series_name, collaborator_name, "
            f"steel, blade_finish, handle_type, blade_length, msrp{extra_select} "
            "FROM reporting_models "
            f"{where_sql} "
            f"ORDER BY {order_by} "
            f"LIMIT {limit}"
        )
    else:
        base_select = (
            "SELECT inventory_id, knife_name, knife_type, family_name, form_name, series_name, "
            "collaborator_name, steel, blade_finish, handle_color, handle_type, quantity, location"
        )
        inv_sort_map = {
            "purchase_price": (
                f"{line_total_sql} {ord_kw}, knife_name",
                f", purchase_price, {line_total_sql} AS line_purchase_total",
            ),
            "acquired_date": (f"acquired_date {ord_kw}, knife_name", ", acquired_date"),
            "knife_name": ("knife_name", ""),
        }
        sort_entry_inv = inv_sort_map.get(sort_field)
        if sort_entry_inv:
            order_by, extra = sort_entry_inv
        else:
            order_by = "knife_name"
            extra = ""
        sql = (
            f"{base_select}{extra} "
            "FROM reporting_inventory "
            f"{where_sql} "
            f"ORDER BY {order_by} "
            f"LIMIT {limit}"
        )
    return sql, {"mode": "semantic_compiled_list_inventory"}
