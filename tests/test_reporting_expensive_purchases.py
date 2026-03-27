"""Compiler contract: top-N expensive purchase lines."""

from __future__ import annotations

from reporting.compiler import _compile_legacy_dict as _reporting_plan_to_sql_legacy


def test_sql_list_inventory_orders_by_line_total_and_limits() -> None:
    plan = {
        "intent": "list_inventory",
        "filters": {},
        "metric": "count",
        "limit": 10,
        "scope": "inventory",
        "sort": {"field": "purchase_price", "direction": "desc"},
    }
    sql, meta = _reporting_plan_to_sql_legacy(plan, None, None, 100)
    assert meta.get("mode") == "semantic_compiled_list_inventory"
    assert "line_purchase_total" in sql
    assert "ORDER BY (COALESCE(purchase_price, 0) * COALESCE(quantity, 1)) DESC" in sql
    assert "LIMIT 10" in sql


def test_sql_list_inventory_route_default_name_order_without_sort() -> None:
    plan = {
        "intent": "list_inventory",
        "filters": {},
        "metric": "count",
        "limit": 50,
        "scope": "inventory",
    }
    sql, _ = _reporting_plan_to_sql_legacy(plan, None, None, 100)
    assert "ORDER BY knife_name" in sql
    assert "line_purchase_total" not in sql
