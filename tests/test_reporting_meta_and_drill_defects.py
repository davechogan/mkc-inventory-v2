"""Regression tests for RPT-003 and RPT-005.

RPT-003: Meta/schema questions must not route to the SQL planner.
RPT-005: missing_models drill-through must target the catalog, not inventory.

Each fix is covered by:
  - the exact failing case
  - at least 2 sibling variants
  - one negative case (fix must not fire)
  - one structural assertion (function must be importable)
"""

import pytest
from reporting.domain import (
    _reporting_build_drill_link,
    _reporting_is_meta_question,
)


# ---------------------------------------------------------------------------
# Structural assertions
# ---------------------------------------------------------------------------

def test_is_meta_question_is_callable():
    assert callable(_reporting_is_meta_question)


def test_build_drill_link_is_callable():
    assert callable(_reporting_build_drill_link)


# ---------------------------------------------------------------------------
# RPT-003: meta/schema question detection
# ---------------------------------------------------------------------------

def test_rpt003_cost_field_question_is_meta():
    """'what field are you using for the cost' must be detected as a meta question."""
    assert _reporting_is_meta_question("what field are you using for the cost of the knives?")


def test_rpt003_how_do_you_calculate_is_meta():
    """'how do you calculate spend' must be detected as a meta question."""
    assert _reporting_is_meta_question("how do you calculate spend?")


def test_rpt003_what_fields_do_you_track_is_meta():
    """'what fields do you track' must be detected as a meta question."""
    assert _reporting_is_meta_question("what fields do you track for each knife?")


def test_rpt003_what_data_is_meta():
    """'what data do you have' must be detected as a meta question."""
    assert _reporting_is_meta_question("what data do you have on my knives?")


def test_rpt003_normal_data_question_is_not_meta():
    """Negative case: a real inventory question must NOT be flagged as meta."""
    assert not _reporting_is_meta_question("how much have I spent on Blackfoot knives?")


def test_rpt003_series_breakdown_is_not_meta():
    """Negative case: a grouping question must NOT be flagged as meta."""
    assert not _reporting_is_meta_question("show me my knives broken down by series")


# ---------------------------------------------------------------------------
# RPT-005: missing_models drill-through routes to catalog
# ---------------------------------------------------------------------------

def test_rpt005_missing_models_links_to_master():
    """missing_models drill link must point to /master.html, not /?..."""
    row = {"official_name": "Speedgoat 2.0", "family_name": "Speedgoat", "series_name": "Core"}
    link = _reporting_build_drill_link(row, intent="missing_models")
    assert link is not None
    assert link.startswith("/master.html")
    assert "/?family" not in link


def test_rpt005_missing_models_uses_official_name_as_search():
    """missing_models drill link must embed the official_name as ?search= param."""
    row = {"official_name": "Blackfoot 2.0", "family_name": "Blackfoot"}
    link = _reporting_build_drill_link(row, intent="missing_models")
    assert link is not None
    assert "search=Blackfoot+2.0" in link or "search=Blackfoot%202.0" in link


def test_rpt005_missing_models_falls_back_to_knife_name():
    """missing_models drill uses knife_name when official_name is absent."""
    row = {"knife_name": "Mini Speedgoat", "family_name": "Speedgoat"}
    link = _reporting_build_drill_link(row, intent="missing_models")
    assert link is not None
    assert link.startswith("/master.html")
    assert "Mini+Speedgoat" in link or "Mini%20Speedgoat" in link


def test_rpt005_missing_models_no_name_returns_none():
    """missing_models with no name fields returns None (no broken link)."""
    row = {"family_name": "Blackfoot"}
    link = _reporting_build_drill_link(row, intent="missing_models")
    assert link is None


def test_rpt005_non_missing_models_links_to_inventory():
    """Negative case: aggregate/list intent must still produce inventory links."""
    row = {"family_name": "Blackfoot", "series_name": "Core"}
    link = _reporting_build_drill_link(row, intent="aggregate")
    assert link is not None
    assert link.startswith("/?")
    assert "master.html" not in link


def test_rpt005_no_intent_links_to_inventory():
    """Negative case: no intent passed → falls through to inventory URL."""
    row = {"family_name": "Speedgoat"}
    link = _reporting_build_drill_link(row)
    assert link is not None
    assert link.startswith("/?")
