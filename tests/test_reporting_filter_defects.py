"""Regression tests for RPT-001 and RPT-002.

RPT-001: Same-field positive+negative contradiction → zero rows.
RPT-002: Empty-result follow-up lock — broken filters from prior turn propagate.

Each fix is covered by:
  - the exact failing case
  - at least 2 sibling variants
  - one negative case (fix must not fire)
  - one structural assertion (the function must exist and be importable)
"""

import pytest
from reporting.domain import (
    _reporting_apply_followup_carryover,
    _reporting_prune_conflicting_filters,
)


# ---------------------------------------------------------------------------
# Structural assertions
# ---------------------------------------------------------------------------

def test_prune_conflicting_filters_is_callable():
    assert callable(_reporting_prune_conflicting_filters)


def test_apply_followup_carryover_is_callable():
    assert callable(_reporting_apply_followup_carryover)


# ---------------------------------------------------------------------------
# RPT-001: same-field contradiction
# ---------------------------------------------------------------------------

def test_rpt001_same_field_contradiction_removed():
    """series_name positive + series_name__not with same value → exclusion dropped."""
    filters = {"series_name": "Blood Brothers", "series_name__not": "Blood Brothers"}
    result = _reporting_prune_conflicting_filters("show blood brothers knives", filters)
    assert "series_name__not" not in result
    assert result["series_name"] == "Blood Brothers"


def test_rpt001_family_name_contradiction_removed():
    """family_name positive + family_name__not with same value → exclusion dropped."""
    filters = {"family_name": "Blackfoot", "family_name__not": "Blackfoot"}
    result = _reporting_prune_conflicting_filters("show blackfoot knives", filters)
    assert "family_name__not" not in result
    assert result["family_name"] == "Blackfoot"


def test_rpt001_knife_type_contradiction_removed():
    """knife_type positive + knife_type__not with same value → exclusion dropped."""
    filters = {"knife_type": "Hunter", "knife_type__not": "Hunter"}
    result = _reporting_prune_conflicting_filters("list hunter knives", filters)
    assert "knife_type__not" not in result
    assert result["knife_type"] == "Hunter"


def test_rpt001_different_values_exclusion_preserved():
    """Negative case: exclusion of a DIFFERENT value must not be removed."""
    filters = {"series_name": "Blood Brothers", "series_name__not": "Traditions"}
    result = _reporting_prune_conflicting_filters("blood brothers but not traditions", filters)
    assert "series_name__not" in result
    assert result["series_name__not"] == "Traditions"


# ---------------------------------------------------------------------------
# RPT-002: empty-result follow-up lock
# ---------------------------------------------------------------------------

def test_rpt002_empty_prior_result_skips_carryover():
    """When prior result had 0 rows, filters must NOT be carried forward."""
    last_state = {
        "intent": "aggregate",
        "filters": {"series_name": "Blood Brothers", "series_name__not": "Blood Brothers"},
        "_result_row_count": 0,
    }
    plan: dict = {"intent": "aggregate", "filters": {}}
    _reporting_apply_followup_carryover(plan, last_state, "double check the cost")
    assert plan["filters"] == {}


def test_rpt002_empty_prior_result_list_followup_skips_carryover():
    """Zero-row guard applies even for list-reshaping follow-up phrases."""
    last_state = {
        "intent": "aggregate",
        "filters": {"family_name": "Blackfoot", "family_name__not": "Blackfoot"},
        "_result_row_count": 0,
    }
    plan: dict = {"intent": "aggregate", "filters": {}}
    _reporting_apply_followup_carryover(plan, last_state, "list the knives that made up that number")
    assert plan["filters"] == {}
    # intent must also not be mutated to list_inventory when prior was empty
    assert plan["intent"] == "aggregate"


def test_rpt002_zero_row_count_key_missing_allows_carryover():
    """Backward compatibility: if _result_row_count is absent, carry forward as before."""
    last_state = {
        "intent": "aggregate",
        "filters": {"series_name": "Blackfoot"},
        # No _result_row_count key — old session state
    }
    plan: dict = {"intent": "list_inventory", "filters": {}}
    _reporting_apply_followup_carryover(plan, last_state, "show me those knives")
    assert plan["filters"].get("series_name") == "Blackfoot"


def test_rpt002_nonzero_prior_result_carries_filters():
    """Negative case: prior result with rows > 0 must still carry filters."""
    last_state = {
        "intent": "aggregate",
        "filters": {"series_name": "Blackfoot"},
        "_result_row_count": 5,
    }
    plan: dict = {"intent": "list_inventory", "filters": {}}
    _reporting_apply_followup_carryover(plan, last_state, "show me those knives")
    assert plan["filters"].get("series_name") == "Blackfoot"
