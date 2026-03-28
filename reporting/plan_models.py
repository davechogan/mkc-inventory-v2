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
    KNIFE_NAME = "knife_name"


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
        # Default to count rather than failing validation.
        if v is None:
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
            "condition", "location", "acquired_date",
            "purchase_price", "estimated_value", "knife_name",
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
        out: dict[str, Any] = {
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
        if self.sort is not None:
            out["sort"] = {"field": self.sort.field, "direction": self.sort.direction.value}
        return out

    @classmethod
    def from_legacy_semantic_plan(cls, plan: dict[str, Any]) -> "CanonicalReportingPlan":
        """Adapt the current legacy semantic-plan dict to canonical typed shape."""
        raw_intent = str(plan.get("intent") or "list_inventory").strip().lower()
        intent_map = {
            "list_inventory": PlanIntent.LIST,
            "aggregate": PlanIntent.LIST,
            "compare": PlanIntent.LIST,
            "missing_models": PlanIntent.MISSING_MODELS,
            "completion_cost": PlanIntent.MISSING_MODELS,
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

        sort_spec: Optional[SortSpec] = None
        raw_sort = plan.get("sort")
        if isinstance(raw_sort, dict):
            sf = str(raw_sort.get("field") or "").strip()
            sd = str(raw_sort.get("direction") or "asc").strip().lower()
            if sf and sd in ("asc", "desc"):
                try:
                    PlanField(sf)  # validate allowed field name
                    sort_spec = SortSpec(field=sf, direction=SortDirection(sd))
                except ValueError:
                    sort_spec = None

        lim = plan.get("limit")
        try:
            lim_i = int(lim) if lim is not None else None
        except (TypeError, ValueError):
            lim_i = None

        return cls(
            intent=intent,
            scope=scope,
            metric=metric,
            group_by=group_by_values,
            filters=filters,
            exclusions=exclusions,
            time_range=time_range,
            year_compare=years,
            sort=sort_spec,
            limit=lim_i,
            needs_clarification=bool(plan.get("needs_clarification")),
            clarification_reason=(
                str(plan.get("clarification_reason")).strip() if plan.get("clarification_reason") else None
            ),
        )
