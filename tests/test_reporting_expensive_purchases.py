"""Compiler contract: top-N expensive purchase lines."""

from __future__ import annotations

from reporting.compiler import compile_plan
from reporting.plan_models import (
    CanonicalReportingPlan,
    PlanIntent,
    PlanMetric,
    PlanScope,
    SortSpec,
    SortDirection,
)


def test_sql_list_inventory_orders_by_purchase_price_and_limits() -> None:
    plan = CanonicalReportingPlan(
        intent=PlanIntent.LIST,
        scope=PlanScope.INVENTORY,
        metric=PlanMetric.COUNT,
        limit=10,
        sort=SortSpec(field="purchase_price", direction=SortDirection.DESC),
    )
    sql, meta = compile_plan(plan, None, None, 100)
    assert meta.get("mode") == "semantic_compiled_list_inventory"
    assert "line_purchase_total" in sql
    # Each row is one knife — no quantity multiplication
    assert "ORDER BY COALESCE(purchase_price, 0) DESC" in sql
    assert "LIMIT 10" in sql


def test_sql_list_inventory_route_default_name_order_without_sort() -> None:
    plan = CanonicalReportingPlan(
        intent=PlanIntent.LIST,
        scope=PlanScope.INVENTORY,
        metric=PlanMetric.COUNT,
        limit=50,
    )
    sql, _ = compile_plan(plan, None, None, 100)
    assert "ORDER BY knife_name" in sql
    assert "line_purchase_total" not in sql
