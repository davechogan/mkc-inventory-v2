from __future__ import annotations

import reporting.domain as reporting_domain
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
    from reporting.plan_models import FilterClause, FilterOp, PlanField

    def fake_semantic_plan(*args, **kwargs):
        plan = CanonicalReportingPlan(
            intent=PlanIntent.AGGREGATE,
            scope=PlanScope.CATALOG,
            metric=PlanMetric.COUNT,
            filters=[FilterClause(field=PlanField.CONDITION, op=FilterOp.EQ, value="Like New")],
        )
        return plan, {"mode": "semantic_test"}

    monkeypatch.setattr(reporting_domain, "_reporting_semantic_plan", fake_semantic_plan)
    monkeypatch.setattr(
        reporting_domain,
        "_reporting_plan_to_sql",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("Compiler should not run for invalid plans")),
    )

    payload = ReportingQueryIn(question="count catalog by condition")
    try:
        run_reporting_query(payload, get_conn=invapp.get_conn)
        assert False, "Expected semantic plan validation to block execution"
    except Exception as exc:
        detail = getattr(exc, "detail", str(exc))
        assert "Invalid semantic plan" in str(detail)


def test_paraphrase_year_compare_normalizes_equivalently(invapp, monkeypatch) -> None:
    import reporting.domain as reporting_domain
    from reporting.domain import ReportingQueryIn, run_reporting_query

    # Keep planner output deterministic so equivalence is tested at normalized plan boundary.
    def fake_ollama_chat(model, system, user_text, images_b64=None, timeout=180.0):
        if "canonical JSON plan" in system:
            import json
            return json.dumps({
                "intent": "aggregate",
                "scope": "inventory",
                "metric": "total_spend",
                "group_by": ["family_name"],
                "filters": [],
                "exclusions": [],
                "time_range": None,
                "year_compare": [2024, 2025],
                "sort": None,
                "limit": 50,
                "needs_clarification": False,
                "clarification_reason": None,
            })
        if "concise collection reporting assistant" in system:
            raise RuntimeError("force deterministic fallback")
        return "{}"

    monkeypatch.setattr(reporting_domain.blade_ai, "ollama_chat", fake_ollama_chat)

    q1 = ReportingQueryIn(question="show me how much i spent in 2024 vs 2025", max_rows=50)
    q2 = ReportingQueryIn(question="compare my spend in 2024 and 2025", max_rows=50)

    r1 = run_reporting_query(q1, get_conn=invapp.get_conn)
    r2 = run_reporting_query(q2, get_conn=invapp.get_conn)

    p1 = r1.get("semantic_plan") or {}
    p2 = r2.get("semantic_plan") or {}
    assert p1.get("intent") == p2.get("intent") == "aggregate"
    assert p1.get("metric") == p2.get("metric") == "total_spend"
    assert p1.get("year_compare") == p2.get("year_compare") == [2024, 2025]

