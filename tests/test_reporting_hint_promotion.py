from __future__ import annotations

from fastapi.testclient import TestClient


def _insert_session_hint(invapp, *, session_id: str, target_value: str, confidence: float = 0.9, evidence: int = 4) -> int:
    with invapp.get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO reporting_semantic_hints
            (scope_type, scope_id, entity_norm, cue_word, target_dimension, target_value,
             confidence, evidence_count, success_count, failure_count, created_at, updated_at, last_used_at)
            VALUES ('session', ?, 'blood brothers', 'family', 'collaborator_name', ?, ?, ?, 5, 0,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (session_id, target_value, confidence, evidence),
        )
        return int(cur.lastrowid)


def test_reporting_hint_promotion_dry_run_does_not_write_global(invapp) -> None:
    client = TestClient(invapp.app)
    sid = "test-session-promotion-dryrun"
    _insert_session_hint(invapp, session_id=sid, target_value="Blood Brothers")

    res = client.post("/api/reporting/hints/promote", json={"session_id": sid, "dry_run": True})
    assert res.status_code == 200, res.text
    payload = res.json()
    assert payload["enabled"] is True
    assert payload["dry_run"] is True
    assert payload["promoted"] >= 1

    with invapp.get_conn() as conn:
        global_rows = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM reporting_semantic_hints
            WHERE scope_type = 'global'
              AND scope_id IS NULL
              AND entity_norm = 'blood brothers'
              AND cue_word = 'family'
              AND target_dimension = 'collaborator_name'
            """
        ).fetchone()
    assert int(global_rows["c"]) == 0


def test_reporting_hint_promotion_apply_writes_global_and_skips_conflicts(invapp) -> None:
    client = TestClient(invapp.app)
    sid = "test-session-promotion-apply"
    _insert_session_hint(invapp, session_id=sid, target_value="Blood Brothers")
    _insert_session_hint(invapp, session_id=sid, target_value="Blackfoot")

    with invapp.get_conn() as conn:
        # Existing global for same cue+dimension with different value forces conflict skip for "Blackfoot".
        conn.execute(
            """
            INSERT INTO reporting_semantic_hints
            (scope_type, scope_id, entity_norm, cue_word, target_dimension, target_value,
             confidence, evidence_count, success_count, failure_count, created_at, updated_at, last_used_at)
            VALUES ('global', NULL, 'blood brothers', 'family', 'collaborator_name', 'Blood Brothers',
                    0.82, 4, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL)
            """
        )

    res = client.post("/api/reporting/hints/promote", json={"session_id": sid, "dry_run": False})
    assert res.status_code == 200, res.text
    payload = res.json()
    assert payload["enabled"] is True
    assert payload["dry_run"] is False
    assert payload["considered"] >= 2
    assert payload["skipped"] >= 1
    assert payload["reasons"].get("conflict_global_value", 0) >= 1

