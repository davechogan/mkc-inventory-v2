from __future__ import annotations

import json
from typing import Optional

import pytest

import reporting.domain as reporting_domain
from reporting.domain import ReportingQueryIn, run_reporting_query


def test_blackfoot_followup_chain_preserves_exclusions_and_lists_items(invapp, monkeypatch: pytest.MonkeyPatch) -> None:
    """Regression for Blackfoot sequence:
    aggregate with exclusions -> list contributing knives should stay itemized and preserve exclusions.
    """
    call_count = [0]

    def fake_ollama_chat(
        model: str,
        system: str,
        user_text: str,
        images_b64: Optional[list[str]] = None,
        timeout: float = 180.0,
    ) -> str:
        if "canonical JSON plan" in system:
            call_count[0] += 1
            # Turn 1: aggregate with family filter + series/text exclusions.
            # Turn 2: LLM carries forward context → list intent, same filters.
            if call_count[0] == 1:
                return json.dumps({
                    "intent": "list",
                    "scope": "inventory",
                    "metric": "total_spend",
                    "group_by": [],
                    "filters": [{"field": "family_name", "op": "=", "value": "Blackfoot"}],
                    "exclusions": [
                        {"field": "series_name", "op": "=", "value": "Traditions"},
                        {"field": "text_search", "op": "=", "value": "Damascus"},
                    ],
                    "time_range": None,
                    "year_compare": [],
                    "sort": None,
                    "limit": 50,
                    "needs_clarification": False,
                    "clarification_reason": None,
                })
            else:
                # Follow-up: list the contributing items, preserve exclusions.
                return json.dumps({
                    "intent": "list",
                    "scope": "inventory",
                    "metric": "count",
                    "group_by": [],
                    "filters": [{"field": "family_name", "op": "=", "value": "Blackfoot"}],
                    "exclusions": [
                        {"field": "series_name", "op": "=", "value": "Traditions"},
                        {"field": "text_search", "op": "=", "value": "Damascus"},
                    ],
                    "time_range": None,
                    "year_compare": [],
                    "sort": None,
                    "limit": 50,
                    "needs_clarification": False,
                    "clarification_reason": None,
                })
        if "concise collection reporting assistant" in system:
            raise RuntimeError("force deterministic fallback")
        return "{}"

    monkeypatch.setattr(reporting_domain.blade_ai, "ollama_chat", fake_ollama_chat)

    # Seed DB already has real Blackfoot data (including Damascus and Traditions rows).
    # This test only checks SQL structure — no synthetic rows needed.

    # Turn 1: spend with exclusions.
    q1 = ReportingQueryIn(
        question="how much have i spent on the Blackfoot knives, if you exclude the Damascus and Traditions versions?",
        max_rows=50,
    )
    r1 = run_reporting_query(q1, get_conn=invapp.get_conn)
    sql1 = str(r1.get("sql_executed") or "").lower()
    assert "from reporting_inventory" in sql1
    assert "family_name" in sql1 and "blackfoot" in sql1
    assert "not" in sql1
    assert "traditions" in sql1
    assert "damascus" in sql1

    # Turn 2: list items making up that number (same session).
    q2 = ReportingQueryIn(
        question="list the knives that made up that number",
        session_id=r1.get("session_id"),
        max_rows=50,
    )
    r2 = run_reporting_query(q2, get_conn=invapp.get_conn)
    sql2 = str(r2.get("sql_executed") or "").lower()
    assert "from reporting_inventory" in sql2
    assert "knife_name" in sql2
    assert "sum(coalesce(purchase_price" not in sql2
    assert "blackfoot" in sql2
    assert "traditions" in sql2
    assert "damascus" in sql2

