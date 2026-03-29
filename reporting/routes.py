"""FastAPI routes for natural-language reporting (mounted from ``app``)."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel

from reporting.constants import REPORTING_ALLOWED_SOURCES
from reporting.domain import (
    REPORTING_PLANNER_MODEL,
    REPORTING_RESPONDER_MODEL,
    GetConn,
    ReportingFeedbackIn,
    ReportingQueryIn,
    ReportingSaveQueryIn,
    _reporting_create_session,
    _reporting_feedback_semantic_hints,
    _reporting_iso_now,
    _reporting_promote_semantic_hints,
    ensure_reporting_schema,
    run_reporting_query,
)
from reporting.retrieval import (
    DEFAULT_RETRIEVAL_BACKEND,
    RETRIEVAL_BACKEND_META_KEY,
    VALID_RETRIEVAL_BACKENDS,
    get_retrieval_status,
    reload_retrieval_artifacts,
    resolve_retrieval_backend,
)


class DirectLlmSqlToggleIn(BaseModel):
    """POST body for the direct-LLM-SQL toggle; module-scoped for correct FastAPI body binding."""

    enabled: bool


class RetrievalBackendIn(BaseModel):
    """POST body for persisted retrieval backend (``app_meta``)."""

    backend: str


class PromoteHintsIn(BaseModel):
    """POST body for guarded session->global semantic hint promotion."""

    session_id: Optional[str] = None
    dry_run: bool = True
    min_confidence: Optional[float] = None
    min_evidence: Optional[int] = None
    max_promotions: Optional[int] = None


def create_reporting_router(
    *,
    get_conn: GetConn,
    static_dir: Path,
    ollama_check: Callable[..., Any],
) -> APIRouter:
    router = APIRouter(tags=["reporting"])

    @router.get("/reporting")
    def reporting_page() -> FileResponse:
        # Serve the React SPA build; falls back to legacy HTML if build is missing
        react_build = static_dir / "dist" / "index.html"
        return FileResponse(react_build if react_build.exists() else static_dir / "reporting.html")

    @router.get("/api/reporting/schema")
    def reporting_schema() -> dict[str, Any]:
        with get_conn() as conn:
            ensure_reporting_schema(conn)
            views = {}
            for view in sorted(REPORTING_ALLOWED_SOURCES):
                cols = conn.execute(f"PRAGMA table_info({view})").fetchall()
                views[view] = [{"name": c["name"], "type": c.get("type")} for c in cols]
            return {
                "views": views,
                "allowed_sources": sorted(REPORTING_ALLOWED_SOURCES),
                "default_model": REPORTING_RESPONDER_MODEL,
                "planner_model": REPORTING_PLANNER_MODEL,
                "responder_model": REPORTING_RESPONDER_MODEL,
            }

    @router.get("/api/reporting/suggested-questions")
    def reporting_suggested_questions() -> dict[str, list[str]]:
        return {
            "questions": [
                "What is my total collection value by family?",
                "Show monthly spend for the last 12 months.",
                "Which knives have the highest estimated value?",
                "How many knives do I have by steel?",
                "Show condition distribution across my inventory.",
                "Compare spend between Traditions and VIP this year.",
                "What are my top 10 most expensive purchases?",
                "How many knives are in each location?",
            ]
        }

    @router.get("/api/reporting/sessions")
    def reporting_sessions() -> dict[str, Any]:
        with get_conn() as conn:
            ensure_reporting_schema(conn)
            rows = conn.execute(
                """
                SELECT s.id, s.title, s.model_default, s.created_at, s.updated_at,
                       (SELECT COUNT(*) FROM reporting_messages m WHERE m.session_id = s.id) AS message_count
                FROM reporting_sessions s
                ORDER BY s.updated_at DESC, s.created_at DESC
                LIMIT 100
                """
            ).fetchall()
            return {"sessions": rows}

    @router.get("/api/reporting/telemetry")
    def reporting_telemetry(limit: int = 100) -> dict[str, Any]:
        with get_conn() as conn:
            ensure_reporting_schema(conn)
            safe_limit = min(1000, max(1, int(limit)))
            rows = conn.execute(
                """
                SELECT id, session_id, question, planner_model, responder_model, generation_mode, semantic_intent,
                       sql_excerpt, row_count, execution_ms, total_ms, status, error_detail, meta_json, created_at
                FROM reporting_query_telemetry
                ORDER BY id DESC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()
            for r in rows:
                r["meta"] = json.loads(r["meta_json"]) if r.get("meta_json") else {}
            return {"events": rows}

    @router.get("/api/reporting/debug/direct-llm-sql")
    def reporting_direct_llm_sql_status() -> dict[str, Any]:
        with get_conn() as conn:
            row = conn.execute(
                "SELECT value FROM app_meta WHERE key = ?",
                ("reporting_direct_llm_sql",),
            ).fetchone()
            raw = row.get("value") if isinstance(row, dict) else None
            enabled = str(raw or "").strip().lower() in {"1", "true", "yes", "on"}
            return {"enabled": enabled}

    @router.post("/api/reporting/debug/direct-llm-sql")
    def reporting_set_direct_llm_sql(payload: DirectLlmSqlToggleIn) -> dict[str, Any]:
        with get_conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO app_meta (key, value) VALUES (?, ?)",
                ("reporting_direct_llm_sql", "1" if payload.enabled else "0"),
            )
            return {"enabled": bool(payload.enabled)}

    @router.get("/api/reporting/hints")
    def reporting_hints(limit: int = 100, session_id: Optional[str] = None) -> dict[str, Any]:
        with get_conn() as conn:
            ensure_reporting_schema(conn)
            safe_limit = min(1000, max(1, int(limit)))
            if session_id and session_id.strip():
                rows = conn.execute(
                    """
                    SELECT id, scope_type, scope_id, entity_norm, cue_word, target_dimension, target_value,
                           confidence, evidence_count, success_count, failure_count, created_at, updated_at, last_used_at
                    FROM reporting_semantic_hints
                    WHERE (scope_type = 'session' AND scope_id = ?) OR (scope_type = 'global' AND scope_id IS NULL)
                    ORDER BY confidence DESC, evidence_count DESC, id DESC
                    LIMIT ?
                    """,
                    (session_id.strip(), safe_limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT id, scope_type, scope_id, entity_norm, cue_word, target_dimension, target_value,
                           confidence, evidence_count, success_count, failure_count, created_at, updated_at, last_used_at
                    FROM reporting_semantic_hints
                    ORDER BY confidence DESC, evidence_count DESC, id DESC
                    LIMIT ?
                    """,
                    (safe_limit,),
                ).fetchall()
            return {"hints": rows}

    @router.get("/api/reporting/retrieval/status")
    def reporting_retrieval_status() -> dict[str, Any]:
        with get_conn() as conn:
            return {"retrieval": get_retrieval_status(conn)}

    @router.get("/api/reporting/retrieval/backend")
    def reporting_retrieval_backend_get() -> dict[str, Any]:
        """Return effective backend, stored value, and whether env overrides UI."""
        with get_conn() as conn:
            st = get_retrieval_status(conn)
            return {
                "backend": resolve_retrieval_backend(conn),
                "stored_backend": st.get("stored_backend"),
                "default_backend": DEFAULT_RETRIEVAL_BACKEND,
                "valid_backends": sorted(VALID_RETRIEVAL_BACKENDS),
                "env_override_active": st.get("env_override_active"),
            }

    @router.post("/api/reporting/retrieval/backend")
    def reporting_retrieval_backend_set(payload: RetrievalBackendIn) -> dict[str, Any]:
        b = str(payload.backend or "").strip().lower()
        if b not in VALID_RETRIEVAL_BACKENDS:
            raise HTTPException(
                status_code=400,
                detail=f"backend must be one of: {', '.join(sorted(VALID_RETRIEVAL_BACKENDS))}",
            )
        with get_conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO app_meta (key, value) VALUES (?, ?)",
                (RETRIEVAL_BACKEND_META_KEY, b),
            )
            st = get_retrieval_status(conn)
            return {
                "backend": resolve_retrieval_backend(conn),
                "stored_backend": b,
                "default_backend": DEFAULT_RETRIEVAL_BACKEND,
                "valid_backends": sorted(VALID_RETRIEVAL_BACKENDS),
                "env_override_active": st.get("env_override_active"),
            }

    @router.post("/api/reporting/retrieval/reload")
    def reporting_retrieval_reload() -> dict[str, Any]:
        with get_conn() as conn:
            return {"retrieval": reload_retrieval_artifacts(conn)}

    @router.post("/api/reporting/feedback")
    def reporting_feedback(payload: ReportingFeedbackIn) -> dict[str, Any]:
        with get_conn() as conn:
            ensure_reporting_schema(conn)
            sid = payload.session_id.strip()
            msg = conn.execute(
                """
                SELECT id, session_id, role, meta_json
                FROM reporting_messages
                WHERE id = ? AND session_id = ?
                """,
                (int(payload.message_id), sid),
            ).fetchone()
            if not msg:
                raise HTTPException(status_code=404, detail="Message not found for this session.")
            if str(msg.get("role") or "") != "assistant":
                raise HTTPException(status_code=400, detail="Feedback is only supported on assistant messages.")

            meta = json.loads(msg.get("meta_json") or "{}") if msg.get("meta_json") else {}
            if not isinstance(meta, dict):
                meta = {}
            prior = meta.get("feedback_helpful")
            if isinstance(prior, bool):
                if prior == bool(payload.helpful):
                    return {"ok": True, "message": "Feedback already recorded.", "changed": False}
                raise HTTPException(status_code=409, detail="Feedback already recorded for this message.")

            hint_ids: list[int] = []
            for h in (meta.get("semantic_hints") or []):
                if not isinstance(h, dict):
                    continue
                hid = h.get("id")
                if isinstance(hid, int):
                    hint_ids.append(hid)
                elif isinstance(hid, str) and hid.isdigit():
                    hint_ids.append(int(hid))

            if hint_ids:
                _reporting_feedback_semantic_hints(conn, hint_ids, success=bool(payload.helpful))

            meta["feedback_helpful"] = bool(payload.helpful)
            meta["feedback_at"] = _reporting_iso_now()
            conn.execute(
                "UPDATE reporting_messages SET meta_json = ? WHERE id = ?",
                (json.dumps(meta), int(payload.message_id)),
            )
            conn.execute(
                "UPDATE reporting_sessions SET updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (sid,),
            )
            return {"ok": True, "changed": True, "hint_ids_updated": hint_ids}

    @router.post("/api/reporting/hints/promote")
    def reporting_promote_hints(payload: PromoteHintsIn) -> dict[str, Any]:
        with get_conn() as conn:
            ensure_reporting_schema(conn)
            return _reporting_promote_semantic_hints(
                conn,
                session_id=(payload.session_id.strip() if payload.session_id else None),
                dry_run=bool(payload.dry_run),
                min_confidence=payload.min_confidence,
                min_evidence=payload.min_evidence,
                max_promotions=payload.max_promotions,
            )

    @router.post("/api/reporting/sessions")
    def reporting_session_create(model: Optional[str] = None) -> dict[str, Any]:
        with get_conn() as conn:
            ensure_reporting_schema(conn)
            s = _reporting_create_session(conn, model)
            return {"session": s}

    @router.get("/api/reporting/sessions/{session_id}")
    def reporting_session_detail(session_id: str) -> dict[str, Any]:
        with get_conn() as conn:
            ensure_reporting_schema(conn)
            session = conn.execute("SELECT * FROM reporting_sessions WHERE id = ?", (session_id,)).fetchone()
            if not session:
                raise HTTPException(status_code=404, detail="Session not found.")
            msgs = conn.execute(
                """
                SELECT id, role, content, sql_executed, result_json, chart_spec_json, meta_json, created_at
                FROM reporting_messages
                WHERE session_id = ?
                ORDER BY id
                """,
                (session_id,),
            ).fetchall()
            parsed_msgs = []
            for m in msgs:
                parsed_msgs.append(
                    {
                        "id": m["id"],
                        "role": m["role"],
                        "content": m["content"],
                        "sql_executed": m.get("sql_executed"),
                        "result": json.loads(m["result_json"]) if m.get("result_json") else None,
                        "chart_spec": json.loads(m["chart_spec_json"]) if m.get("chart_spec_json") else None,
                        "meta": json.loads(m["meta_json"]) if m.get("meta_json") else None,
                        "created_at": m["created_at"],
                    }
                )
            return {"session": session, "messages": parsed_msgs}

    @router.get("/api/reporting/saved-queries")
    def reporting_saved_queries() -> dict[str, Any]:
        with get_conn() as conn:
            ensure_reporting_schema(conn)
            rows = conn.execute(
                """
                SELECT id, name, question, config_json, created_at, updated_at
                FROM reporting_saved_queries
                ORDER BY updated_at DESC, created_at DESC
                """
            ).fetchall()
            for row in rows:
                row["config"] = json.loads(row["config_json"]) if row.get("config_json") else {}
            return {"saved_queries": rows}

    @router.post("/api/reporting/saved-queries")
    def reporting_save_query(payload: ReportingSaveQueryIn) -> dict[str, Any]:
        with get_conn() as conn:
            ensure_reporting_schema(conn)
            cur = conn.execute(
                """
                INSERT INTO reporting_saved_queries (name, question, config_json, created_at, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                (payload.name.strip(), payload.question.strip(), json.dumps(payload.config or {})),
            )
            return {"id": cur.lastrowid, "message": "Saved"}

    @router.delete("/api/reporting/saved-queries/{saved_id}")
    def reporting_delete_query(saved_id: int) -> dict[str, str]:
        with get_conn() as conn:
            ensure_reporting_schema(conn)
            cur = conn.execute("DELETE FROM reporting_saved_queries WHERE id = ?", (saved_id,))
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Saved query not found.")
            return {"message": "Deleted"}

    @router.post("/api/reporting/query")
    def reporting_query(payload: ReportingQueryIn) -> dict[str, Any]:
        try:
            check_payload = ollama_check()
        except Exception:
            check_payload = None
        return run_reporting_query(
            payload,
            get_conn=get_conn,
            ollama_check_payload=check_payload,
        )

    return router
