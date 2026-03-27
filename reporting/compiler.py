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
from reporting.plan_models import CanonicalReportingPlan
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
    return _compile_legacy_dict(plan.to_legacy_semantic_plan(), date_start, date_end, max_rows)


# ---------------------------------------------------------------------------
# Internal SQL builder
# ---------------------------------------------------------------------------

def _compile_legacy_dict(
    plan: dict[str, Any],
    date_start: Optional[str],
    date_end: Optional[str],
    max_rows: int,
) -> tuple[Optional[str], dict[str, Any]]:
    """Build SQL from a validated plan dict.

    Called only via compile_plan() after the plan has passed both structural
    and semantic validation.  The compiler translates; it does not repair.
    """
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

    def values_of(v: Any) -> list[str]:
        if isinstance(v, list):
            return [str(x) for x in v if str(x).strip()]
        return [str(v)] if str(v or "").strip() else []

    model_filter_cols = {
        "series_name", "family_name", "knife_type", "form_name", "collaborator_name", "steel", "text_search",
        "series_name__not", "family_name__not", "knife_type__not", "form_name__not", "collaborator_name__not", "steel__not", "text_search__not",
    }
    inv_filter_cols = {
        "series_name", "family_name", "knife_type", "form_name", "collaborator_name", "steel", "condition", "location", "text_search",
        "series_name__not", "family_name__not", "knife_type__not", "form_name__not", "collaborator_name__not", "steel__not", "condition__not", "location__not", "text_search__not",
    }

    def inv_filter_where(filters_map: dict[str, Any], *, catalog_style: bool) -> list[str]:
        """Build WHERE fragments using reporting_inventory / reporting_models column names."""
        w: list[str] = []
        for k, v in filters_map.items():
            if k not in inv_filter_cols:
                continue
            negate = k.endswith("__not")
            base_k = k[:-5] if negate else k
            if catalog_style and base_k in {"condition", "location"}:
                continue
            if base_k == "text_search":
                vals = values_of(v)
                for raw_val in vals:
                    ev = esc(raw_val)
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
            for raw_val in values_of(v):
                expr = cond(base_k, raw_val, exact=(base_k in {"series_name", "knife_type", "condition"}))
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
                for raw_val in values_of(v):
                    ev = esc(raw_val)
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
            for raw_val in values_of(v):
                expr = cond(f"m.{base_k}", raw_val, exact=(base_k in {"series_name", "knife_type"}))
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
                for raw_val in values_of(v):
                    ev = esc(raw_val)
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
            for raw_val in values_of(v):
                expr = cond(f"m.{base_k}", raw_val, exact=(base_k in {"series_name", "knife_type"}))
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

    raw_sort = plan.get("sort")
    sort_field = ""
    sort_dir = "asc"
    if isinstance(raw_sort, dict):
        sort_field = str(raw_sort.get("field") or "").strip()
        sort_dir = str(raw_sort.get("direction") or "asc").strip().lower()
    line_total_sql = "(COALESCE(purchase_price, 0) * COALESCE(quantity, 1))"

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
        base_select = (
            "SELECT inventory_id, knife_name, knife_type, family_name, form_name, series_name, collaborator_name, "
            "steel, blade_finish, handle_color, condition, quantity, location"
        )
        if sort_field == "purchase_price" and sort_dir in ("asc", "desc"):
            ord_kw = "DESC" if sort_dir == "desc" else "ASC"
            order_by = f"{line_total_sql} {ord_kw}, knife_name"
            extra = f", purchase_price, {line_total_sql} AS line_purchase_total"
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
