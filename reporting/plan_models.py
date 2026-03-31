"""Canonical reporting plan models and enums.

This module is the typed source of truth for reporting semantic plans.
It mirrors ``reporting_plan.schema.json`` and is designed for strict boundary checks.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class PlanIntent(str, Enum):
    LIST = "list"
    MISSING_MODELS = "missing_models"


class PlanScope(str, Enum):
    INVENTORY = "inventory"
    CATALOG = "catalog"


class PlanMetric(str, Enum):
    COUNT = "count"
    TOTAL_SPEND = "total_spend"
    MSRP = "msrp"


class PlanDimension(str, Enum):
    SERIES_NAME = "series_name"
    FAMILY_NAME = "family_name"
    KNIFE_TYPE = "knife_type"
    FORM_NAME = "form_name"
    COLLABORATOR_NAME = "collaborator_name"
    STEEL = "steel"
    BLADE_FINISH = "blade_finish"
    HANDLE_COLOR = "handle_color"
    LOCATION = "location"
    KNIFE_NAME = "knife_name"


class PlanField(str, Enum):
    SERIES_NAME = "series_name"
    FAMILY_NAME = "family_name"
    KNIFE_TYPE = "knife_type"
    FORM_NAME = "form_name"
    COLLABORATOR_NAME = "collaborator_name"
    STEEL = "steel"
    BLADE_FINISH = "blade_finish"
    BLADE_COLOR = "blade_color"
    HANDLE_COLOR = "handle_color"
    HANDLE_TYPE = "handle_type"
    BLADE_LENGTH = "blade_length"
    LOCATION = "location"
    KNIFE_NAME = "knife_name"
    OFFICIAL_NAME = "official_name"
    ACQUIRED_DATE = "acquired_date"
    PURCHASE_PRICE = "purchase_price"
    MSRP = "msrp"
    QUANTITY = "quantity"
    PURCHASE_SOURCE = "purchase_source"
    GENERATION_LABEL = "generation_label"
    SIZE_MODIFIER = "size_modifier"
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

    @field_validator("op", mode="before")
    @classmethod
    def _normalize_op(cls, v: object) -> object:
        # LLM occasionally outputs "==" instead of "="
        if v == "==":
            return "="
        return v

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

    @field_validator("intent", mode="before")
    @classmethod
    def _coerce_intent(cls, v: object) -> object:
        if v is None:
            return PlanIntent.LIST
        # Collapse retired/alias intents to canonical 2-intent design.
        if v in ("aggregate", "compare", "list_inventory", "inventory_list"):
            return "list"
        if v in ("completion_cost", "missing_catalog", "catalog_gap"):
            return "missing_models"
        # Any unrecognised string from the LLM defaults to list rather than failing validation.
        if isinstance(v, str) and v not in ("list", "missing_models"):
            return "list"
        return v

    @field_validator("scope", mode="before")
    @classmethod
    def _coerce_scope(cls, v: object) -> object:
        return v if v is not None else PlanScope.INVENTORY

    @field_validator("group_by", "filters", "exclusions", mode="before")
    @classmethod
    def _coerce_list_fields(cls, v: object) -> object:
        if v is None:
            return []
        # LLM occasionally outputs filters/exclusions as a flat dict
        # e.g. {"series_name": "Blood Brothers"} instead of
        # [{"field": "series_name", "op": "=", "value": "Blood Brothers"}]
        if isinstance(v, dict):
            return [{"field": k, "op": "=", "value": val} for k, val in v.items()]
        return v

    @field_validator("needs_clarification", mode="before")
    @classmethod
    def _coerce_needs_clarification(cls, v: object) -> object:
        return v if v is not None else False

    @field_validator("year_compare", mode="before")
    @classmethod
    def _validate_year_compare(cls, years: object) -> list[int]:
        if years is None:
            return []
        if not isinstance(years, list):
            raise ValueError("year_compare must be a list or null.")
        if years and len(years) != 2:
            raise ValueError("year_compare must contain exactly two years when present.")
        for year in years:
            if year < 1900 or year > 2200:
                raise ValueError("year_compare years must be in [1900, 2200].")
        return years

    @field_validator("metric", mode="before")
    @classmethod
    def _coerce_metric(cls, v: object) -> object:
        # LLM sometimes returns null for metric on list intents where metric is irrelevant.
        if v is None:
            return PlanMetric.COUNT
        # Coerce common LLM metric mistakes to valid values
        s = str(v).strip().lower()
        _METRIC_ALIASES = {
            "purchase_price": "total_spend",
            "price": "total_spend",
            "spend": "total_spend",
            "cost": "total_spend",
            "value": "total_spend",
            "worth": "total_spend",
            "estimated_value": "total_spend",
            "total": "total_spend",
            "quantity": "count",
            "num": "count",
            "number": "count",
        }
        if s in _METRIC_ALIASES:
            return _METRIC_ALIASES[s]
        # If it's not a valid enum value, default to count
        valid = {m.value for m in PlanMetric}
        if s not in valid:
            return PlanMetric.COUNT
        return v

    @field_validator("limit")
    @classmethod
    def _validate_limit(cls, limit: Optional[int]) -> Optional[int]:
        if limit is None:
            return None
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be in [1, 1000].")
        return limit

    @model_validator(mode="after")
    def _coerce_scope_from_fields(self) -> "CanonicalReportingPlan":
        """Auto-upgrade scope to inventory when inventory-only fields appear in filters/exclusions.

        The LLM sometimes sets scope=catalog while using fields that only exist in
        reporting_inventory (e.g. knife_name, condition, acquired_date). Silently
        correct rather than rejecting the whole plan.
        """
        _INVENTORY_ONLY = {
            "location", "acquired_date",
            "purchase_price", "knife_name",
            "quantity",
        }
        if self.scope == PlanScope.CATALOG:
            all_field_vals = {c.field.value for c in [*self.filters, *self.exclusions]}
            if all_field_vals & _INVENTORY_ONLY:
                self.scope = PlanScope.INVENTORY
        return self

    @model_validator(mode="after")
    def _clarification_contract(self) -> "CanonicalReportingPlan":
        if self.needs_clarification and not (self.clarification_reason or "").strip():
            raise ValueError("clarification_reason is required when needs_clarification=true.")
        return self

    def to_planner_context_dict(self) -> dict[str, Any]:
        """Produce a compact dict for storage in last_query_state_json.

        This format is optimised for LLM readability: filters as a flat
        {field: value} dict, only EQ filters included (non-EQ ops are rare in
        session carry-forward), and a minimal set of keys so the LLM context
        block stays focused.
        """
        filters_map: dict[str, Any] = {}
        for clause in self.filters:
            if clause.op == FilterOp.EQ:
                filters_map[str(clause.field.value)] = clause.value
        for clause in self.exclusions:
            if clause.op == FilterOp.EQ:
                filters_map[f"{clause.field.value}__not"] = clause.value

        group_by = self.group_by[0].value if self.group_by else None
        out: dict[str, Any] = {
            "intent": self.intent.value,
            "scope": self.scope.value,
            "metric": self.metric.value,
            "group_by": group_by,
            "filters": filters_map,
            "year_compare": self.year_compare or None,
            "date_start": (self.time_range.start if self.time_range else None),
            "date_end": (self.time_range.end if self.time_range else None),
        }
        if self.sort is not None:
            out["sort"] = {"field": self.sort.field, "direction": self.sort.direction.value}
        if self.limit is not None:
            out["limit"] = self.limit
        return out

