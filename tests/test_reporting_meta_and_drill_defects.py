"""Regression tests for RPT-005.

RPT-005: missing_models drill-through must target the catalog, not inventory.
"""

from reporting.domain import _reporting_build_drill_link


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
    """Negative case: list intent must still produce inventory links."""
    row = {"family_name": "Blackfoot", "series_name": "Core"}
    link = _reporting_build_drill_link(row, intent="list")
    assert link is not None
    assert link.startswith("/?")
    assert "master.html" not in link


def test_rpt005_no_intent_links_to_inventory():
    """Negative case: no intent passed → falls through to inventory URL."""
    row = {"family_name": "Speedgoat"}
    link = _reporting_build_drill_link(row)
    assert link is not None
    assert link.startswith("/?")
