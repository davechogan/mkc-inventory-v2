import pytest
from fastapi import HTTPException

from reporting.plan_models import (
    CanonicalReportingPlan,
    PlanDimension,
    PlanIntent,
    PlanMetric,
    PlanScope,
    SortSpec,
    SortDirection,
)


def test_validate_sql_allows_whitelisted_source(invapp):
    sql = "SELECT * FROM reporting_inventory"
    validated = invapp._reporting_validate_sql(sql)
    assert "FROM reporting_inventory" in validated


def test_validate_sql_rejects_unapproved_source(invapp):
    with pytest.raises(HTTPException) as excinfo:
        invapp._reporting_validate_sql("SELECT * FROM inventory_items_v2")
    assert excinfo.value.status_code == 400


def test_plan_to_sql_grouped_uses_expected_source_view(invapp):
    # Catalog scope with group_by should compile to `reporting_models` with GROUP BY.
    plan_catalog = CanonicalReportingPlan(
        intent=PlanIntent.LIST,
        scope=PlanScope.CATALOG,
        metric=PlanMetric.COUNT,
        group_by=[PlanDimension.SERIES_NAME],
    )
    sql, meta = invapp._reporting_plan_to_sql(plan_catalog, date_start=None, date_end=None, max_rows=50)
    assert "FROM reporting_models" in sql
    assert "GROUP BY bucket" in sql
    assert meta.get("mode") == "semantic_compiled_aggregate"

    # Inventory scope with group_by should compile to `reporting_inventory` with GROUP BY.
    plan_inventory = CanonicalReportingPlan(
        intent=PlanIntent.LIST,
        scope=PlanScope.INVENTORY,
        metric=PlanMetric.COUNT,
        group_by=[PlanDimension.FAMILY_NAME],
    )
    sql2, meta2 = invapp._reporting_plan_to_sql(plan_inventory, date_start=None, date_end=None, max_rows=50)
    assert "FROM reporting_inventory" in sql2
    assert "GROUP BY bucket" in sql2
    assert meta2.get("mode") == "semantic_compiled_aggregate"


def test_plan_to_sql_catalog_falls_back_for_inventory_only_group_by(invapp):
    # `condition` is an inventory-only dimension; even with `scope=catalog` we should fall back
    # to the inventory view to satisfy the GROUP BY.
    plan = CanonicalReportingPlan(
        intent=PlanIntent.LIST,
        scope=PlanScope.CATALOG,
        metric=PlanMetric.COUNT,
        group_by=[PlanDimension.CONDITION],
    )
    sql, meta = invapp._reporting_plan_to_sql(plan, date_start=None, date_end=None, max_rows=50)
    assert "FROM reporting_inventory" in sql
    assert meta.get("mode") == "semantic_compiled_aggregate"


def test_plan_to_sql_rejects_unvalidated_dict(invapp):
    with pytest.raises(TypeError):
        invapp._reporting_plan_to_sql({"intent": "list"}, date_start=None, date_end=None, max_rows=50)


def test_plan_to_sql_list_inventory_orders_by_purchase_price_when_sort_set(invapp):
    plan = CanonicalReportingPlan(
        intent=PlanIntent.LIST,
        scope=PlanScope.INVENTORY,
        metric=PlanMetric.COUNT,
        limit=10,
        sort=SortSpec(field="purchase_price", direction=SortDirection.DESC),
    )
    sql, meta = invapp._reporting_plan_to_sql(plan, date_start=None, date_end=None, max_rows=50)
    assert "COALESCE(purchase_price, 0)" in sql
    assert "DESC" in sql
    assert meta.get("mode") == "semantic_compiled_list_inventory"

