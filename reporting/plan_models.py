"""Canonical reporting plan models and enums.

This module is the typed source of truth for reporting semantic plans.
It mirrors ``reporting_plan.schema.json`` and is designed for strict boundary checks.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class PlanIntent(str, Enum):
    AGGREGATE = "aggregate"
    LIST = "list"
    MISSING_MODELS = "missing_models"
    COMPARE = "compare"
    COMPLETION_COST = "completion_cost"


class PlanScope(str, Enum):
    INVENTORY = "inventory"
    CATALOG = "catalog"


class PlanMetric(str, Enum):
    COUNT = "count"
    TOTAL_SPEND = "total_spend"
    ESTIMATED_VALUE = "estimated_value"
    MSRP = "msrp"


class PlanDimension(str, Enum):
    SERIES_NAME = "series_name"
    FAMILY_NAME = "family_name"
    KNIFE_TYPE = "knife_type"
    FORM_NAME = "form_name"
    COLLABORATOR_NAME = "collaborator_name"
    STEEL = "steel"
    CONDITION = "condition"
    LOCATION = "location"


class PlanField(str, Enum):
    SERIES_NAME = "series_name"
    FAMILY_NAME = "family_name"
    KNIFE_TYPE = "knife_type"
    FORM_NAME = "form_name"
    COLLABORATOR_NAME = "collaborator_name"
    STEEL = "steel"
    CONDITION = "condition"
    LOCATION = "location"
    KNIFE_NAME = "knife_name"
    OFFICIAL_NAME = "official_name"
    RECORD_STATUS = "record_status"
    ACQUIRED_DATE = "acquired_date"
    PURCHASE_PRICE = "purchase_price"
    ESTIMATED_VALUE = "estimated_value"
    MSRP = "msrp"
    TEXT_SEARCH = "text_search"


class FilterOp(str, Enum):
    EQ = "="
    NEQ = "!="
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    IN = "in"
    NOT_IN = "not_in"
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="
    BETWEEN = "between"


class SortDirection(str, Enum):
    ASC = "asc"
    DESC = "desc"


ScalarValue = Union[str, int, float]
ClauseValue = Union[ScalarValue, list[ScalarValue]]


class FilterClause(BaseModel):
    """Single canonical filter/exclusion clause."""

    model_config = ConfigDict(extra="forbid")

    field: PlanField
    op: FilterOp
    value: ClauseValue

    @field_validator("value")
    @classmethod
    def _non_empty_value(cls, value: ClauseValue) -> ClauseValue:
        if isinstance(value, list) and len(value) == 0:
            raise ValueError("Clause value list cannot be empty.")
        return value


class TimeRange(BaseModel):
    """Optional time window normalized at the plan layer."""

    model_config = ConfigDict(extra="forbid")

    start: Optional[str] = None
    end: Optional[str] = None
    label: Optional[str] = None


class SortSpec(BaseModel):
    """Deterministic sorting configuration from canonical plan."""

    model_config = ConfigDict(extra="forbid")

    field: str
    direction: SortDirection


class CanonicalReportingPlan(BaseModel):
    """Validated canonical plan used as the only compiler input."""

    model_config = ConfigDict(extra="forbid")

    intent: PlanIntent
    scope: PlanScope
    metric: PlanMetric
    group_by: list[PlanDimension] = Field(default_factory=list)
    filters: list[FilterClause] = Field(default_factory=list)
    exclusions: list[FilterClause] = Field(default_factory=list)
    time_range: Optional[TimeRange] = None
    year_compare: list[int] = Field(default_factory=list)
    sort: Optional[SortSpec] = None
    limit: Optional[int] = None
    needs_clarification: bool = False
    clarification_reason: Optional[str] = None

    @field_validator("year_compare")
    @classmethod
    def _validate_year_compare(cls, years: list[int]) -> list[int]:
        if years and len(years) != 2:
            raise ValueError("year_compare must contain exactly two years when present.")
        for year in years:
            if year < 1900 or year > 2200:
                raise ValueError("year_compare years must be in [1900, 2200].")
        return years

    @field_validator("limit")
    @classmethod
    def _validate_limit(cls, limit: Optional[int]) -> Optional[int]:
        if limit is None:
            return None
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be in [1, 1000].")
        return limit

    @model_validator(mode="after")
    def _clarification_contract(self) -> "CanonicalReportingPlan":
        if self.needs_clarification and not (self.clarification_reason or "").strip():
            raise ValueError("clarification_reason is required when needs_clarification=true.")
        return self

    def to_legacy_semantic_plan(self) -> dict[str, Any]:
        """Convert canonical plan to the current compiler's legacy dict shape.

        This adapter is temporary while we migrate the compiler internals to consume
        canonical objects directly. It preserves deterministic translation.
        """
        filters_map: dict[str, Any] = {}
        for clause in self.filters:
            if clause.op != FilterOp.EQ:
                continue
            filters_map[str(clause.field.value)] = clause.value
        for clause in self.exclusions:
            if clause.op != FilterOp.EQ:
                continue
            filters_map[f"{clause.field.value}__not"] = clause.value

        group_by = self.group_by[0].value if self.group_by else None
        metric = self.metric.value
        if metric == PlanMetric.ESTIMATED_VALUE.value:
            metric = "total_estimated_value"
        return {
            "intent": "list_inventory" if self.intent == PlanIntent.LIST else self.intent.value,
            "scope": self.scope.value,
            "metric": metric,
            "group_by": group_by,
            "filters": filters_map,
            "limit": self.limit,
            "year_compare": self.year_compare or None,
            "date_start": (self.time_range.start if self.time_range else None),
            "date_end": (self.time_range.end if self.time_range else None),
            "date_label": (self.time_range.label if self.time_range else None),
            "needs_clarification": self.needs_clarification,
            "clarification_reason": self.clarification_reason,
        }

    @classmethod
    def from_legacy_semantic_plan(cls, plan: dict[str, Any]) -> "CanonicalReportingPlan":
        """Adapt the current legacy semantic-plan dict to canonical typed shape."""
        raw_intent = str(plan.get("intent") or "list_inventory").strip().lower()
        intent_map = {
            "list_inventory": PlanIntent.LIST,
            "aggregate": PlanIntent.AGGREGATE,
            "missing_models": PlanIntent.MISSING_MODELS,
            "compare": PlanIntent.COMPARE,
            "completion_cost": PlanIntent.COMPLETION_COST,
            "list": PlanIntent.LIST,
        }
        intent = intent_map.get(raw_intent, PlanIntent.LIST)

        raw_metric = str(plan.get("metric") or "count").strip().lower()
        metric_map = {
            "count": PlanMetric.COUNT,
            "total_spend": PlanMetric.TOTAL_SPEND,
            "total_estimated_value": PlanMetric.ESTIMATED_VALUE,
            "estimated_value": PlanMetric.ESTIMATED_VALUE,
            "msrp": PlanMetric.MSRP,
        }
        metric = metric_map.get(raw_metric, PlanMetric.COUNT)

        group_by_values: list[PlanDimension] = []
        group_raw = plan.get("group_by")
        if isinstance(group_raw, str) and group_raw.strip():
            try:
                group_by_values = [PlanDimension(group_raw.strip())]
            except ValueError:
                group_by_values = []
        elif isinstance(group_raw, list):
            for item in group_raw:
                try:
                    group_by_values.append(PlanDimension(str(item).strip()))
                except ValueError:
                    continue

        filters: list[FilterClause] = []
        exclusions: list[FilterClause] = []
        for key, value in dict(plan.get("filters") or {}).items():
            if value is None:
                continue
            base_key = str(key).strip()
            negate = base_key.endswith("__not")
            field_name = base_key[:-5] if negate else base_key
            try:
                field = PlanField(field_name)
            except ValueError:
                continue
            clause = FilterClause(
                field=field,
                op=FilterOp.EQ,
                value=value,
            )
            if negate:
                exclusions.append(clause)
            else:
                filters.append(clause)

        time_range = None
        if plan.get("date_start") or plan.get("date_end") or plan.get("date_label"):
            time_range = TimeRange(
                start=(str(plan.get("date_start")) if plan.get("date_start") else None),
                end=(str(plan.get("date_end")) if plan.get("date_end") else None),
                label=(str(plan.get("date_label")) if plan.get("date_label") else None),
            )

        years: list[int] = []
        for y in list(plan.get("year_compare") or []):
            try:
                years.append(int(str(y)))
            except (TypeError, ValueError):
                continue

        scope_raw = str(plan.get("scope") or "inventory").strip().lower()
        scope = PlanScope.CATALOG if scope_raw == "catalog" else PlanScope.INVENTORY

        return cls(
            intent=intent,
            scope=scope,
            metric=metric,
            group_by=group_by_values,
            filters=filters,
            exclusions=exclusions,
            time_range=time_range,
            year_compare=years,
            sort=None,
            limit=plan.get("limit"),
            needs_clarification=bool(plan.get("needs_clarification")),
            clarification_reason=(
                str(plan.get("clarification_reason")).strip() if plan.get("clarification_reason") else None
            ),
        )
