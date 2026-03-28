"""Ollama API routes (mounted from app)."""
from __future__ import annotations

from collections.abc import Callable
from typing import Any, Optional, Type

import blade_ai
import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel


def create_ai_router(
    *,
    run_identify: Callable[[Any], dict[str, Any]],
    identifier_query_model: Type[BaseModel],
) -> tuple[APIRouter, Callable[..., dict[str, Any]]]:
    """
    Mount /api/identify, /api/ai/*.

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

    @router.post("/api/identify")
    def identify_knives(payload: IdentifierQuery):
        """Backward-compatible route now powered by canonical v2 catalog."""
        return run_identify(payload)

    # Same annotation fix as v2_routes: `from __future__ import annotations` makes
    # the locally-scoped `IdentifierQuery` a string FastAPI cannot resolve.
    identify_knives.__annotations__["payload"] = IdentifierQuery

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

    return router, ollama_check
