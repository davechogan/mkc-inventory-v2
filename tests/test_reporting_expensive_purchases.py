"""Regression: top-N expensive purchase lines (heuristic + SQL compiler)."""

from __future__ import annotations

from reporting.compiler import _compile_legacy_dict as _reporting_plan_to_sql_legacy
from reporting.domain import _reporting_heuristic_plan


def test_heuristic_top_10_most_expensive_purchases() -> None:
    p = _reporting_heuristic_plan("What are my top 10 most expensive purchases?")
    assert p["intent"] == "list_inventory"
    assert p["limit"] == 10
    assert p.get("sort") == {"field": "purchase_price", "direction": "desc"}


def test_heuristic_most_expensive_purchases_default_limit_10() -> None:
    p = _reporting_heuristic_plan("What are my most expensive purchases?")
    assert p["intent"] == "list_inventory"
    assert p["limit"] == 10
    assert p.get("sort") == {"field": "purchase_price", "direction": "desc"}


def test_heuristic_paraphrase_expensive_purchase_lines_top_5() -> None:
    p = _reporting_heuristic_plan("Show the top 5 priciest things I bought")
    assert p["intent"] == "list_inventory"
    assert p["limit"] == 5
    assert p.get("sort") == {"field": "purchase_price", "direction": "desc"}


def test_heuristic_negative_not_triggered_for_spend_aggregate() -> None:
    p = _reporting_heuristic_plan("How much did I spend last year?")
    assert p["intent"] == "aggregate"
    assert p.get("sort") is None


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
