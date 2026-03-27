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
    inserted_model_ids: list[int] = []
    inserted_inventory_ids: list[int] = []

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
                    "intent": "aggregate",
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

    try:
        with invapp.get_conn() as conn:
            # Blackfoot family rows with one Traditions series row to exclude.
            rows = [
                ("Blackfoot 2.0", "blackfoot-2-0", "Blackfoot", None, 100.0),
                ("Damascus Blackfoot 2.0", "damascus-blackfoot-2-0", "Blackfoot", None, 120.0),
                ("Traditions Blackfoot 2.0", "traditions-blackfoot-2-0", "Blackfoot", "Traditions", 130.0),
                ("Blood Brothers Blackfoot 2.0", "blood-brothers-blackfoot-2-0", "Blackfoot", "Blood Brothers", 140.0),
            ]
            family_ids: dict[str, int] = {}
            series_ids: dict[str, int] = {}
            for official_name, slug, family_name, series_name, price in rows:
                cur = conn.execute(
                    """
                    INSERT INTO knife_models_v2 (official_name, normalized_name, sortable_name, slug, record_status)
                    VALUES (?, ?, ?, ?, 'active')
                    """,
                    (official_name, official_name.lower(), official_name.lower(), slug),
                )
                model_id = int(cur.lastrowid)
                inserted_model_ids.append(model_id)
                fam_id = family_ids.get(family_name)
                if fam_id is None:
                    fam_slug = family_name.lower().replace(" ", "-")
                    fam_id = int(
                        conn.execute(
                            "INSERT INTO knife_families (name, normalized_name, slug) VALUES (?, ?, ?)",
                            (family_name, family_name.lower(), fam_slug),
                        ).lastrowid
                    )
                    family_ids[family_name] = fam_id
                conn.execute("UPDATE knife_models_v2 SET family_id = ? WHERE id = ?", (int(fam_id), model_id))
                if series_name:
                    series_id = series_ids.get(series_name)
                    if series_id is None:
                        series_slug = series_name.lower().replace(" ", "-")
                        series_id = int(
                            conn.execute(
                                "INSERT INTO knife_series (name, slug) VALUES (?, ?)",
                                (series_name, series_slug),
                            ).lastrowid
                        )
                        series_ids[series_name] = series_id
                    conn.execute("UPDATE knife_models_v2 SET series_id = ? WHERE id = ?", (int(series_id), model_id))
                inv_cur = conn.execute(
                    """
                    INSERT INTO inventory_items_v2
                        (knife_model_id, quantity, purchase_price, condition, created_at, updated_at)
                    VALUES (?, 1, ?, 'Like New', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """,
                    (model_id, float(price)),
                )
                inserted_inventory_ids.append(int(inv_cur.lastrowid))

        # Turn 1: aggregate with exclusions.
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

        # Turn 2: list items making up the number (same session).
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
    finally:
        with invapp.get_conn() as conn:
            for iid in inserted_inventory_ids:
                conn.execute("DELETE FROM inventory_items_v2 WHERE id = ?", (iid,))
            for mid in inserted_model_ids:
                conn.execute("DELETE FROM knife_models_v2 WHERE id = ?", (mid,))

