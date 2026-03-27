from __future__ import annotations

import json
from typing import Optional

import pytest

import reporting.domain as reporting_domain
from reporting.domain import ReportingQueryIn, run_reporting_query


def test_reporting_retrieval_meta_is_persisted_in_assistant_message(invapp, monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure retrieval diagnostics survive session reload through reporting_messages.meta_json."""

    def fake_ollama_chat(
        model: str,
        system: str,
        user_text: str,
        images_b64: Optional[list[str]] = None,
        timeout: float = 180.0,
    ) -> str:
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
        if "concise collection reporting assistant" in system:
            raise RuntimeError("force deterministic fallback")
        return "{}"

    monkeypatch.setattr(reporting_domain.blade_ai, "ollama_chat", fake_ollama_chat)

    result = run_reporting_query(
        ReportingQueryIn(question="list my knives", max_rows=10),
        get_conn=invapp.get_conn,
    )
    sid = str(result.get("session_id") or "")
    assert sid
    retrieval = result.get("retrieval")
    assert isinstance(retrieval, dict)
    assert retrieval.get("effective_backend") in {"lexical", "embedding"}
    assert isinstance(retrieval.get("artifact_ids"), list)
    assert len(retrieval.get("artifact_ids") or []) >= 1
    assert isinstance(retrieval.get("semantic_candidates"), list)
    assert retrieval.get("corpus_fingerprint")

    with invapp.get_conn() as conn:
        row = conn.execute(
            """
            SELECT meta_json
            FROM reporting_messages
            WHERE session_id = ? AND role = 'assistant'
            ORDER BY id DESC
            LIMIT 1
            """,
            (sid,),
        ).fetchone()
        assert row is not None
        meta = json.loads(row["meta_json"]) if row.get("meta_json") else {}
        assert isinstance(meta, dict)
        persisted = meta.get("retrieval")
        assert isinstance(persisted, dict)
        assert persisted.get("effective_backend") == retrieval.get("effective_backend")
        assert isinstance(persisted.get("artifact_ids"), list)
        assert isinstance(persisted.get("semantic_candidates"), list)

