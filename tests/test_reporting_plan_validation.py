from __future__ import annotations

from reporting.plan_models import CanonicalReportingPlan, PlanIntent, PlanMetric, PlanScope
from reporting.plan_validator import parse_planner_raw_text, validate_canonical_structure, validate_canonical_semantics


def _valid_plan_payload() -> dict:
    return {
        "intent": "aggregate",
        "scope": "inventory",
        "metric": "count",
        "group_by": ["family_name"],
        "filters": [],
        "exclusions": [],
        "time_range": None,
        "year_compare": [],
        "sort": None,
        "limit": 100,
        "needs_clarification": False,
        "clarification_reason": None,
    }


def test_parse_planner_raw_text_rejects_non_json() -> None:
    payload, err = parse_planner_raw_text("not-json")
    assert payload is None
    assert err and "not valid JSON" in err


def test_structural_validation_rejects_unknown_enum() -> None:
    plan = _valid_plan_payload()
    plan["intent"] = "unknown"
    result = validate_canonical_structure(plan)
    assert result.valid is False
    assert result.classification == "invalid_plan"


def test_structural_validation_accepts_valid_plan() -> None:
    result = validate_canonical_structure(_valid_plan_payload())
    assert result.valid is True
    assert isinstance(result.canonical_plan, CanonicalReportingPlan)


def test_semantic_validation_rejects_catalog_inventory_only_field() -> None:
    plan = CanonicalReportingPlan(
        intent=PlanIntent.AGGREGATE,
        scope=PlanScope.CATALOG,
        metric=PlanMetric.COUNT,
        group_by=[],
        filters=[{"field": "condition", "op": "=", "value": "Like New"}],
        exclusions=[],
        time_range=None,
        year_compare=[],
        sort=None,
        limit=50,
        needs_clarification=False,
        clarification_reason=None,
    )
    result = validate_canonical_semantics(plan)
    assert result.valid is False
    assert any("Catalog scope does not support field 'condition'" in e for e in result.errors)


def test_run_reporting_query_blocks_semantically_invalid_plan(invapp, monkeypatch) -> None:
    import reporting.domain as reporting_domain
    from reporting.domain import ReportingQueryIn, run_reporting_query

    def fake_semantic_plan(*args, **kwargs):
        return (
            {
                "intent": "aggregate",
                "scope": "catalog",
                "metric": "count",
                "group_by": None,
                "filters": {"condition": "Like New"},
                "limit": 50,
            },
            {"mode": "semantic_test"},
        )

    monkeypatch.setattr(reporting_domain, "_reporting_semantic_plan", fake_semantic_plan)

    payload = ReportingQueryIn(question="count catalog by condition")
    try:
        run_reporting_query(payload, get_conn=invapp.get_conn)
        assert False, "Expected semantic plan validation to block execution"
    except Exception as exc:
        detail = getattr(exc, "detail", str(exc))
        assert "Invalid semantic plan" in str(detail)

