import pytest
from fastapi import HTTPException

from reporting.plan_models import CanonicalReportingPlan


def test_validate_sql_allows_whitelisted_source(invapp):
    sql = "SELECT * FROM reporting_inventory"
    validated = invapp._reporting_validate_sql(sql)
    assert "FROM reporting_inventory" in validated


def test_validate_sql_rejects_unapproved_source(invapp):
    with pytest.raises(HTTPException) as excinfo:
        invapp._reporting_validate_sql("SELECT * FROM inventory_items_v2")
    assert excinfo.value.status_code == 400


def test_plan_to_sql_aggregate_uses_expected_source_view(invapp):
    # Catalog scope should compile to `reporting_models`.
    plan_catalog = CanonicalReportingPlan.from_legacy_semantic_plan(
        {
        "intent": "aggregate",
        "metric": "count",
        "scope": "catalog",
        "group_by": None,
        "filters": {},
        }
    )
    sql, meta = invapp._reporting_plan_to_sql(plan_catalog, date_start=None, date_end=None, max_rows=50)
    assert "FROM reporting_models" in sql
    assert meta.get("mode") == "semantic_compiled_aggregate"

    # Inventory scope should compile to `reporting_inventory`.
    plan_inventory = CanonicalReportingPlan.from_legacy_semantic_plan(
        {
        "intent": "aggregate",
        "metric": "count",
        "scope": "inventory",
        "group_by": "family_name",
        "filters": {},
        }
    )
    sql2, meta2 = invapp._reporting_plan_to_sql(plan_inventory, date_start=None, date_end=None, max_rows=50)
    assert "FROM reporting_inventory" in sql2
    assert "GROUP BY bucket" in sql2
    assert meta2.get("mode") == "semantic_compiled_aggregate"


def test_plan_to_sql_catalog_falls_back_for_inventory_only_group_by(invapp):
    # `condition` is an inventory-only dimension; even with `scope=catalog` we should fall back
    # to the inventory view to satisfy the GROUP BY.
    plan = CanonicalReportingPlan.from_legacy_semantic_plan(
        {
        "intent": "aggregate",
        "metric": "count",
        "scope": "catalog",
        "group_by": "condition",
        "filters": {},
        }
    )
    sql, meta = invapp._reporting_plan_to_sql(plan, date_start=None, date_end=None, max_rows=50)
    assert "FROM reporting_inventory" in sql
    assert meta.get("mode") == "semantic_compiled_aggregate"


def test_plan_to_sql_rejects_unvalidated_dict(invapp):
    with pytest.raises(TypeError):
        invapp._reporting_plan_to_sql({"intent": "aggregate"}, date_start=None, date_end=None, max_rows=50)


def test_plan_to_sql_list_inventory_orders_by_purchase_price_when_sort_set(invapp):
    plan = CanonicalReportingPlan.from_legacy_semantic_plan(
        {
            "intent": "list_inventory",
            "metric": "count",
            "scope": "inventory",
            "group_by": None,
            "filters": {},
            "limit": 10,
            "sort": {"field": "purchase_price", "direction": "desc"},
        }
    )
    sql, meta = invapp._reporting_plan_to_sql(plan, date_start=None, date_end=None, max_rows=50)
    assert "COALESCE(purchase_price, 0)" in sql
    assert "DESC" in sql
    assert meta.get("mode") == "semantic_compiled_list_inventory"


def test_legacy_plan_repair_fixes_completion_cost_for_ranked_purchases(invapp):
    from reporting.domain import _reporting_legacy_plan_from_llm_dict

    q = "What are my top 10 most expensive purchases?"
    raw = {"intent": "completion_cost", "metric": "count", "filters": {}, "scope": "inventory"}
    fixed = _reporting_legacy_plan_from_llm_dict(raw, q)
    assert fixed["intent"] == "list_inventory"
    assert fixed.get("sort") == {"field": "purchase_price", "direction": "desc"}
    assert fixed.get("limit") == 10

