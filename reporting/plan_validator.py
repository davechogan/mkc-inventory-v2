"""Structural and semantic validation for canonical reporting plans."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Optional

from pydantic import ValidationError

from reporting.plan_models import (
    CanonicalReportingPlan,
    FilterOp,
    PlanDimension,
    PlanField,
    PlanIntent,
    PlanMetric,
    PlanScope,
)


INVENTORY_ONLY_FIELDS = {
    PlanField.CONDITION,
    PlanField.LOCATION,
    PlanField.ACQUIRED_DATE,
    PlanField.PURCHASE_PRICE,
    PlanField.ESTIMATED_VALUE,
    PlanField.KNIFE_NAME,
}

CATALOG_ONLY_FIELDS = {
    PlanField.MSRP,
    PlanField.RECORD_STATUS,
    PlanField.OFFICIAL_NAME,
}


@dataclass
class PlanValidationResult:
    """Result envelope for structural/semantic canonical plan validation."""

    valid: bool
    classification: str
    errors: list[str]
    canonical_plan: Optional[CanonicalReportingPlan] = None
    raw_plan: Optional[dict[str, Any]] = None


def parse_planner_raw_text(raw: str) -> tuple[Optional[dict[str, Any]], Optional[str]]:
    """Parse planner raw text into JSON object safely."""
    txt = (raw or "").strip()
    if not txt:
        return None, "Planner returned empty output."
    try:
        parsed = json.loads(txt)
    except json.JSONDecodeError as exc:
        return None, f"Planner output is not valid JSON: {exc.msg}"
    if not isinstance(parsed, dict):
        return None, "Planner output must be a JSON object."
    return parsed, None


def validate_canonical_structure(plan_payload: dict[str, Any]) -> PlanValidationResult:
    """Validate structural schema using the typed canonical model."""
    try:
        plan = CanonicalReportingPlan.model_validate(plan_payload)
    except ValidationError as exc:
        return PlanValidationResult(
            valid=False,
            classification="invalid_plan",
            errors=[e.get("msg", "Invalid plan field.") for e in exc.errors()],
            canonical_plan=None,
            raw_plan=plan_payload,
        )
    return PlanValidationResult(
        valid=True,
        classification="ok",
        errors=[],
        canonical_plan=plan,
        raw_plan=plan_payload,
    )


def validate_canonical_semantics(plan: CanonicalReportingPlan) -> PlanValidationResult:
    """Apply semantic/business validation not captured by pure shape checks."""
    errors: list[str] = []
    clarification = bool(plan.needs_clarification)

    if clarification:
        return PlanValidationResult(
            valid=False,
            classification="clarification_needed",
            errors=[plan.clarification_reason or "Plan requires clarification before execution."],
            canonical_plan=plan,
            raw_plan=plan.model_dump(),
        )

    if plan.intent == PlanIntent.AGGREGATE and plan.metric not in {
        PlanMetric.COUNT,
        PlanMetric.TOTAL_SPEND,
        PlanMetric.ESTIMATED_VALUE,
        PlanMetric.MSRP,
    }:
        errors.append("Aggregate intent requires a supported aggregate metric.")

    if plan.intent == PlanIntent.LIST and plan.metric not in {PlanMetric.COUNT, PlanMetric.ESTIMATED_VALUE}:
        errors.append("List intent uses an incompatible metric.")

    if plan.intent in {PlanIntent.COMPARE, PlanIntent.AGGREGATE} and not plan.group_by and not plan.year_compare:
        # We allow scalar aggregates; compare should include grouping or year_compare.
        if plan.intent == PlanIntent.COMPARE:
            errors.append("Compare intent must include group_by or year_compare.")

    if plan.year_compare and plan.time_range and (plan.time_range.start or plan.time_range.end):
        errors.append("year_compare conflicts with explicit time_range; use only one.")

    if plan.scope == PlanScope.CATALOG:
        for clause in [*plan.filters, *plan.exclusions]:
            if clause.field in INVENTORY_ONLY_FIELDS:
                errors.append(f"Catalog scope does not support field '{clause.field.value}'.")
        for dim in plan.group_by:
            if dim in {PlanDimension.CONDITION, PlanDimension.LOCATION}:
                errors.append(f"Catalog scope does not support group_by '{dim.value}'.")

    if plan.scope == PlanScope.INVENTORY:
        for clause in [*plan.filters, *plan.exclusions]:
            if clause.field in CATALOG_ONLY_FIELDS:
                errors.append(f"Inventory scope does not support field '{clause.field.value}'.")

    for clause in [*plan.filters, *plan.exclusions]:
        if clause.op in {FilterOp.IN, FilterOp.NOT_IN, FilterOp.BETWEEN} and not isinstance(clause.value, list):
            errors.append(f"Operator '{clause.op.value}' requires list value.")
        if clause.op in {
            FilterOp.EQ,
            FilterOp.NEQ,
            FilterOp.CONTAINS,
            FilterOp.NOT_CONTAINS,
            FilterOp.GT,
            FilterOp.GTE,
            FilterOp.LT,
            FilterOp.LTE,
        } and isinstance(clause.value, list):
            errors.append(f"Operator '{clause.op.value}' requires scalar value.")

    if plan.metric == PlanMetric.MSRP and plan.scope != PlanScope.CATALOG:
        errors.append("msrp metric requires catalog scope.")

    if errors:
        return PlanValidationResult(
            valid=False,
            classification="invalid_plan",
            errors=errors,
            canonical_plan=plan,
            raw_plan=plan.model_dump(),
        )

    return PlanValidationResult(
        valid=True,
        classification="ok",
        errors=[],
        canonical_plan=plan,
        raw_plan=plan.model_dump(),
    )

