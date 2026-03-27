"""Ollama / vision identification API routes (mounted from app)."""
from __future__ import annotations

import base64
import json
from collections.abc import Callable
from typing import Any, Optional, Type

import blade_ai
import httpx
from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from pydantic import BaseModel

from reporting.domain import GetConn


def create_ai_router(
    *,
    get_conn: GetConn,
    run_identify: Callable[[Any], dict[str, Any]],
    identifier_query_model: Type[BaseModel],
) -> tuple[APIRouter, Callable[..., dict[str, Any]]]:
    """
    Mount /api/identify, /api/ai/*, /api/blade-shapes, /api/ai/identify.

    Returns (router, ollama_check) so reporting can embed Ollama reachability in its UI.
    """
    IdentifierQuery = identifier_query_model
    router = APIRouter(tags=["ai"])

    def ollama_check(model: Optional[str] = None) -> dict[str, Any]:
        """
        Verify Ollama is reachable and optionally that the given model is loaded.
        Returns reachable status, model list, and validation error if model specified and missing.
        """
        try:
            data = blade_ai.fetch_ollama_models()
            models = data.get("models") or []
            model_names = [m.get("name") or m.get("model", "") for m in models if isinstance(m, dict)]
            ok, err = True, None
            if model and (model or "").strip():
                ok, err = blade_ai.check_ollama_model(model)
            return {
                "reachable": True,
                "ollama_host": blade_ai.OLLAMA_HOST,
                "models": models,
                "model_names": model_names,
                "model_ok": ok if model else None,
                "model_error": err,
            }
        except httpx.ConnectError:
            return {
                "reachable": False,
                "ollama_host": blade_ai.OLLAMA_HOST,
                "error": f"Ollama not reachable at {blade_ai.OLLAMA_HOST}. Is it running?",
                "models": [],
                "model_names": [],
            }
        except httpx.HTTPError as exc:
            return {
                "reachable": False,
                "ollama_host": blade_ai.OLLAMA_HOST,
                "error": str(exc),
                "models": [],
                "model_names": [],
            }
        except Exception as exc:
            return {
                "reachable": False,
                "ollama_host": blade_ai.OLLAMA_HOST,
                "error": str(exc),
                "models": [],
                "model_names": [],
            }

    def _keyword_results_to_parsed(keyword_results: list[dict[str, Any]], max_score: float) -> dict[str, Any]:
        """Convert keyword search results to parsed.ranked_models format for frontend."""
        ranked = []
        for r in keyword_results[:5]:
            score = r.get("score") or 0
            conf = min(1.0, (score / max(max_score, 1)) * 0.95) if max_score > 0 else 0.5
            ranked.append({
                "name": r.get("name", "?"),
                "confidence": round(conf, 2),
                "rationale": "; ".join(r.get("reasons") or [])[:200],
            })
        return {"ranked_models": ranked, "caveats": "", "shape_read": None}

    @router.post("/api/identify")
    def identify_knives(payload: IdentifierQuery):
        """Backward-compatible route now powered by canonical v2 catalog."""
        return run_identify(payload)

    @router.get("/api/ai/ollama/config")
    def api_ollama_config():
        return {"ollama_host": blade_ai.OLLAMA_HOST}

    @router.get("/api/ai/ollama/check")
    def api_ollama_check(model: Optional[str] = None):
        return ollama_check(model)

    @router.get("/api/ai/ollama/models")
    def api_ollama_list_models():
        try:
            return blade_ai.fetch_ollama_models()
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=502, detail=f"Cannot reach Ollama: {exc}") from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @router.get("/api/blade-shapes")
    def api_blade_shapes():
        with get_conn() as conn:
            rows = conn.execute(
                """
                SELECT id, slug, name, description, outline_json
                FROM blade_shape_templates
                ORDER BY name COLLATE NOCASE
                """
            ).fetchall()
        return rows

    @router.post("/api/ai/identify")
    async def api_ai_identify(
        model: str = Form(...),
        description: str = Form(""),
        include_shape_hint: str = Form("false"),
        image: Optional[UploadFile] = File(None),
    ):
        """
        Vision-assisted identification: Hu silhouette → optional early exit → vision describes blade →
        keyword search with that description → LLM rerank if ambiguous.
        Uses deterministic keyword scoring (which excels at traits like carbon fiber / Ultra) plus
        vision for perception and LLM for disambiguation.
        """
        image_bytes: Optional[bytes] = None
        if image and getattr(image, "filename", None):
            image_bytes = await image.read()
            if len(image_bytes) > 15 * 1024 * 1024:
                raise HTTPException(status_code=413, detail="Image larger than 15MB")

        if not image_bytes and not (description or "").strip():
            raise HTTPException(
                status_code=400,
                detail="Provide a written description and/or a photo.",
            )

        model_ok, model_err = blade_ai.check_ollama_model(model)
        if not model_ok:
            raise HTTPException(
                status_code=400,
                detail=model_err or "Selected model is not available in Ollama.",
            )

        with get_conn() as conn:
            master_hu_rows = conn.execute(
                """
                SELECT km.id, km.official_name AS name, kmi.silhouette_hu_json
                FROM knife_models_v2 km
                JOIN knife_model_images kmi ON kmi.knife_model_id = km.id
                WHERE km.record_status != 'archived'
                  AND kmi.silhouette_hu_json IS NOT NULL
                  AND trim(kmi.silhouette_hu_json) != ''
                ORDER BY km.official_name COLLATE NOCASE
                """
            ).fetchall()
            tpl_rows = conn.execute(
                "SELECT slug, name, hu_json FROM blade_shape_templates"
            ).fetchall()

        catalog_templates = []
        for r in master_hu_rows:
            try:
                hu_list = json.loads(r["silhouette_hu_json"])
                if blade_ai.is_hu_vector_degenerate(hu_list):
                    continue
            except (json.JSONDecodeError, TypeError):
                continue
            catalog_templates.append({
                "slug": f"catalog-{r['id']}",
                "name": r["name"],
                "hu_json": r["silhouette_hu_json"],
            })
        generic_templates = [dict(r) for r in tpl_rows]
        combined_templates = catalog_templates + generic_templates

        shape_hints: list[dict[str, Any]] = []
        shape_err: Optional[str] = None
        if image_bytes:
            shape_hints, shape_err = blade_ai.silhouette_hints_from_image(
                image_bytes, combined_templates
            )

        # Step 1–2: Strong Hu match → early exit
        if shape_hints:
            top = shape_hints[0]
            top_slug = top.get("slug") or ""
            top_name = top.get("name") or ""
            top_dist = top.get("distance", 999)
            is_catalog_match = isinstance(top_slug, str) and top_slug.startswith("catalog-")
            if is_catalog_match and top_name and top_dist < 1.0:
                payload = IdentifierQuery(q=top_name, include_archived=False)
                kw_resp = run_identify(payload)
                results = kw_resp.get("results") or []
                if results and results[0].get("name") == top_name:
                    max_score = results[0].get("score") or 1
                    parsed = _keyword_results_to_parsed(results, max_score)
                    parsed["caveats"] = f"Strong silhouette match (Hu distance {top_dist})."
                    return {
                        "ollama_host": blade_ai.OLLAMA_HOST,
                        "model": model,
                        "raw_response": f"Early exit: Hu match to {top_name}",
                        "parsed": parsed,
                        "shape_hints": shape_hints,
                        "shape_hint_error": shape_err,
                        "pipeline": "hu_early_exit",
                    }

        # Step 3: Vision describes blade for search
        vision_desc: dict[str, Any] = {}
        if image_bytes:
            try:
                img_b64 = base64.standard_b64encode(image_bytes).decode("ascii")
                vision_desc = blade_ai.vision_describe_knife(model, img_b64, description)
            except Exception as e:
                vision_desc = {"keywords": str(e)[:100]}

        # Step 4: Keyword search with vision output + user description
        q_parts = []
        if vision_desc.get("keywords"):
            q_parts.append(str(vision_desc["keywords"]).strip())
        if vision_desc.get("handle_material"):
            q_parts.append(str(vision_desc["handle_material"]).strip())
        if vision_desc.get("distinctive"):
            q_parts.append(str(vision_desc["distinctive"]).strip())
        if (description or "").strip():
            q_parts.append(description.strip())
        q_str = " ".join(q_parts) if q_parts else (description or "").strip() or "knife"

        bl_len = None
        if vision_desc.get("blade_length_inches") is not None:
            try:
                bl_len = float(vision_desc["blade_length_inches"])
            except (TypeError, ValueError):
                pass

        payload = IdentifierQuery(
            q=q_str,
            blade_shape=vision_desc.get("blade_shape") or None,
            blade_length=bl_len,
            finish=vision_desc.get("blade_finish") or None,
            include_archived=False,
        )
        kw_resp = run_identify(payload)
        results = kw_resp.get("results") or []
        max_score = results[0].get("score", 0) if results else 1
        second_score = results[1].get("score", 0) if len(results) > 1 else 0

        # Step 5: Clear winner (top >> second) → return keyword results
        score_gap = max_score - second_score
        if not results or (len(results) == 1) or (score_gap >= 15 and max_score >= 20):
            parsed = _keyword_results_to_parsed(results, max_score)
            if vision_desc:
                parsed["shape_read"] = vision_desc.get("keywords", "")
            return {
                "ollama_host": blade_ai.OLLAMA_HOST,
                "model": model,
                "raw_response": f"Keyword search (q={q_str!r})",
                "parsed": parsed,
                "shape_hints": shape_hints,
                "shape_hint_error": shape_err,
                "pipeline": "keyword",
                "vision_description": vision_desc,
            }

        # Step 6: Ambiguous → LLM rerank with top candidates + full catalog data
        with get_conn() as conn:
            full_cols = (
                "id, name, category, catalog_line, blade_profile, blade_shape, default_blade_length, "
                "default_handle_color, default_blade_color, "
                "collector_notes, evidence_summary, identifier_keywords, identifier_distinguishing_features"
            )
            top_names = [r["name"] for r in results[:6]]
            placeholders = ",".join("?" * len(top_names))
            cand_rows = conn.execute(
                f"SELECT {full_cols} FROM master_knives WHERE name IN ({placeholders})",
                top_names,
            ).fetchall()
        cand_full = [dict(r) for r in cand_rows]
        vision_text = json.dumps(vision_desc) if vision_desc else ""
        if (description or "").strip():
            vision_text = f"User: {description}\nVision: {vision_text}"
        rerank_prompt = blade_ai.build_rerank_prompt(cand_full, vision_text)

        try:
            raw = blade_ai.ollama_chat(model, blade_ai.RERANK_SYSTEM, rerank_prompt)
        except (httpx.HTTPStatusError, httpx.HTTPError) as exc:
            err = str(exc)
            try:
                if hasattr(exc, "response") and exc.response.json():
                    err = exc.response.json().get("error", err)
            except Exception:
                pass
            parsed = _keyword_results_to_parsed(results, max_score)
            parsed["caveats"] = f"Rerank failed ({err}); showing keyword results."
            return {
                "ollama_host": blade_ai.OLLAMA_HOST,
                "model": model,
                "raw_response": "",
                "parsed": parsed,
                "shape_hints": shape_hints,
                "shape_hint_error": shape_err,
                "pipeline": "keyword_fallback",
            }

        parsed = blade_ai.try_parse_json_response(raw)
        if not parsed or not parsed.get("ranked_models"):
            parsed = _keyword_results_to_parsed(results, max_score)
            parsed["caveats"] = (parsed.get("caveats") or "") + " LLM rerank produced no models."
        return {
            "ollama_host": blade_ai.OLLAMA_HOST,
            "model": model,
            "raw_response": raw,
            "parsed": parsed,
            "shape_hints": shape_hints,
            "shape_hint_error": shape_err,
            "pipeline": "rerank",
            "vision_description": vision_desc,
        }

    return router, ollama_check
