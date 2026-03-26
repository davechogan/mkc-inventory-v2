from __future__ import annotations

import json
from typing import Any, Optional

import pytest
from fastapi.testclient import TestClient

import reporting.domain as reporting_domain
from reporting.domain import ReportingQueryIn, run_reporting_query


def test_direct_llm_sql_toggle_post_json_body(invapp) -> None:
    """Regression: nested Pydantic body models made FastAPI expect query param ``payload``."""
    client = TestClient(invapp.app)
    try:
        res = client.post("/api/reporting/debug/direct-llm-sql", json={"enabled": True})
        assert res.status_code == 200, res.text
        assert res.json() == {"enabled": True}
        assert client.get("/api/reporting/debug/direct-llm-sql").json()["enabled"] is True
    finally:
        client.post("/api/reporting/debug/direct-llm-sql", json={"enabled": False})


def test_reporting_direct_llm_sql_toggle_does_not_bypass_planner(invapp, monkeypatch: pytest.MonkeyPatch) -> None:
    # Toggle remains persisted for UI, but canonical planner/compiler path remains authoritative.
    with invapp.get_conn() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO app_meta (key, value) VALUES (?, ?)",
            ("reporting_direct_llm_sql", "1"),
        )

    inserted_model_ids: list[int] = []
    inserted_inventory_ids: list[int] = []

    # Track which prompt "families" we called.
    systems_seen: list[str] = []

    # Fake LLM:
    # - Planner prompt returns a canonical semantic plan.
    # - SQL-generation prompt must not run (direct LLM SQL path disabled for normal execution).
    # - Answer prompt fails to force deterministic fallback text.
    def fake_ollama_chat(
        model: str,
        system: str,
        user_text: str,
        images_b64: Optional[list[str]] = None,
        timeout: float = 180.0,
    ) -> str:
        systems_seen.append(system or "")
        if "convert collection questions into semantic JSON plans" in system:
            return json.dumps(
                {
                    "intent": "list_inventory",
                    "filters": {},
                    "group_by": None,
                    "metric": "count",
                    "limit": 10,
                    "date_start": None,
                    "date_end": None,
                    "year_compare": None,
                }
            )
        if "generate read-only SQLite SELECT queries for collection reporting" in system:
            raise AssertionError("Direct LLM SQL generation path should not run.")
        if "concise collection reporting assistant" in system:
            raise RuntimeError("Responder forced failure to use deterministic fallback.")
        raise AssertionError("Unexpected system prompt in fake_ollama_chat.")

    monkeypatch.setattr(reporting_domain.blade_ai, "ollama_chat", fake_ollama_chat)

    # Insert minimal v2 model + inventory rows with known acquired_date ordering.
    new_date = "2026-01-15"
    old_date = "2025-01-15"
    knife_new = "Pytest New Knife"
    knife_old = "Pytest Old Knife"
    slug_new = "pytest-new-knife"
    slug_old = "pytest-old-knife"

    try:
        with invapp.get_conn() as conn:
            cur_new = conn.execute(
                """
                INSERT INTO knife_models_v2
                    (official_name, normalized_name, sortable_name, slug, record_status)
                VALUES (?, ?, ?, ?, 'active')
                """,
                (knife_new, knife_new.lower(), knife_new.lower(), slug_new),
            )
            model_new_id = int(cur_new.lastrowid)
            inserted_model_ids.append(model_new_id)

            cur_old = conn.execute(
                """
                INSERT INTO knife_models_v2
                    (official_name, normalized_name, sortable_name, slug, record_status)
                VALUES (?, ?, ?, ?, 'active')
                """,
                (knife_old, knife_old.lower(), knife_old.lower(), slug_old),
            )
            model_old_id = int(cur_old.lastrowid)
            inserted_model_ids.append(model_old_id)

            cur_inv_old = conn.execute(
                """
                INSERT INTO inventory_items_v2
                    (knife_model_id, nickname, quantity, acquired_date, condition, notes, created_at, updated_at)
                VALUES (?, ?, 1, ?, 'Like New', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                (model_old_id, "old", old_date, "old-notes"),
            )
            inserted_inventory_ids.append(int(cur_inv_old.lastrowid))

            cur_inv_new = conn.execute(
                """
                INSERT INTO inventory_items_v2
                    (knife_model_id, nickname, quantity, acquired_date, condition, notes, created_at, updated_at)
                VALUES (?, ?, 1, ?, 'Like New', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                (model_new_id, "new", new_date, "new-notes"),
            )
            inserted_inventory_ids.append(int(cur_inv_new.lastrowid))

        payload = ReportingQueryIn(
            question="what was the last knife i bought and when was it?",
            max_rows=10,
        )

        result = run_reporting_query(payload, get_conn=invapp.get_conn)
        assert "FROM reporting_inventory" in result["sql_executed"]
        assert "Found 2 rows. First row" in result["answer_text"]
        assert f"knife_name={knife_new}" in result["answer_text"]
        assert isinstance(result.get("retrieval"), dict)

        # Sanity: planner prompt called, direct SQL generation prompt was not.
        assert any("convert collection questions into semantic JSON plans" in s for s in systems_seen)
        assert not any("generate read-only SQLite SELECT queries for collection reporting" in s for s in systems_seen)
        assert any("concise collection reporting assistant" in s for s in systems_seen)
    finally:
        # Cleanup inserted rows to keep the test DB stable.
        with invapp.get_conn() as conn:
            for iid in inserted_inventory_ids:
                conn.execute("DELETE FROM inventory_items_v2 WHERE id = ?", (iid,))
            for mid in inserted_model_ids:
                conn.execute("DELETE FROM knife_models_v2 WHERE id = ?", (mid,))
            conn.execute(
                "INSERT OR REPLACE INTO app_meta (key, value) VALUES (?, ?)",
                ("reporting_direct_llm_sql", "0"),
            )

