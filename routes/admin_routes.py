"""Legacy admin API: silhouette Hu and distinguishing-features maintenance."""
from __future__ import annotations

import base64
import json
import logging
import sqlite3
from typing import Any, Optional

import blade_ai
from fastapi import APIRouter
from pydantic import BaseModel

from reporting.domain import GetConn


def recompute_silhouettes_for_masters_without_hu(conn: sqlite3.Connection) -> int:
    """
    Process masters that have identifier_image_blob but:
    - no identifier_silhouette_hu_json, or
    - degenerate Hu (e.g. [0.77, 12, 12, 12, -12, 12, -12]) that cannot discriminate blades.
    Ensures stored reference images yield usable shape data; clears degenerate Hu.
    """
    rows = conn.execute(
        """
        SELECT km.id, kmi.image_blob, kmi.silhouette_hu_json
        FROM knife_models_v2 km
        JOIN knife_model_images kmi ON kmi.knife_model_id = km.id
        WHERE kmi.image_blob IS NOT NULL
          AND length(kmi.image_blob) > 0
        """
    ).fetchall()
    updated = 0
    for row in rows:
        blob = row["image_blob"]
        if not blob:
            continue
        hu_json = (row.get("silhouette_hu_json") or "").strip()
        needs_recompute = not hu_json
        if hu_json:
            try:
                hu_list = json.loads(hu_json)
                needs_recompute = blade_ai.is_hu_vector_degenerate(hu_list)
            except (json.JSONDecodeError, TypeError):
                needs_recompute = True
        if not needs_recompute:
            continue
        hu_list, _ = blade_ai.extract_blade_hu_from_image_bytes(blob)
        if hu_list:
            conn.execute(
                """
                UPDATE knife_model_images
                SET silhouette_hu_json = ?, updated_at = CURRENT_TIMESTAMP
                WHERE knife_model_id = ?
                """,
                (json.dumps(hu_list), row["id"]),
            )
            updated += 1
        else:
            conn.execute(
                """
                UPDATE knife_model_images
                SET silhouette_hu_json = NULL, updated_at = CURRENT_TIMESTAMP
                WHERE knife_model_id = ?
                """,
                (row["id"],),
            )
            updated += 1
    return updated


def recompute_distinguishing_features(
    *,
    get_conn: GetConn,
    app_logger: logging.Logger,
    ollama_vision_model: str,
    knife_ids: Optional[list[int]] = None,
    missing_only: bool = False,
    model: Optional[str] = None,
) -> dict[str, Any]:
    """
    Run vision LLM to extract distinguishing features for masters with images.
    Fetches rows first, releases DB, runs LLM calls (no DB held), then writes back.
    Returns {"updated": int, "failed": list[dict], "skipped": int}.
    """
    model_name = (model or "").strip() or ollama_vision_model
    model_ok, err = blade_ai.check_ollama_model(model_name)
    if not model_ok:
        return {"updated": 0, "failed": [{"reason": err}], "skipped": 0}
    if knife_ids is not None and not knife_ids:
        return {"updated": 0, "failed": [], "skipped": 0}

    with get_conn() as conn:
        if knife_ids is not None:
            placeholders = ",".join("?" * len(knife_ids))
            rows = conn.execute(
                """
                SELECT id, name, identifier_image_blob
                FROM master_knives
                WHERE id IN (""" + placeholders + """)
                  AND identifier_image_blob IS NOT NULL
                  AND length(identifier_image_blob) > 0
                """,
                knife_ids,
            ).fetchall()
        else:
            cond = "AND (identifier_distinguishing_features IS NULL OR trim(identifier_distinguishing_features) = '')" if missing_only else ""
            rows = conn.execute(
                f"""
                SELECT id, name, identifier_image_blob
                FROM master_knives
                WHERE identifier_image_blob IS NOT NULL
                  AND length(identifier_image_blob) > 0
                {cond}
                ORDER BY name COLLATE NOCASE
                """,
            ).fetchall()
    # Connection released; run LLM calls without holding DB
    to_update: list[tuple[str, int]] = []
    failed: list[dict[str, Any]] = []
    total = len(rows)
    for i, r in enumerate(rows, 1):
        blob = r["identifier_image_blob"]
        if not blob:
            continue
        app_logger.info("[dist-features] Processing %s/%s: %s...", i, total, r["name"])
        img_b64 = base64.standard_b64encode(blob).decode("ascii")
        features, feat_err = blade_ai.extract_distinguishing_features_from_image(model_name, img_b64)
        if feat_err:
            app_logger.warning("[dist-features] FAILED %s: %s", r["name"], feat_err)
            failed.append({"id": r["id"], "name": r["name"], "reason": feat_err})
            continue
        if features:
            preview = (features[:60] + "…") if len(features) > 60 else features
            app_logger.info("[dist-features] OK %s: %s", r["name"], preview)
            to_update.append((features, r["id"]))

    # Write updates in short transactions
    with get_conn() as conn:
        for features, kid in to_update:
            conn.execute(
                """
                UPDATE master_knives
                SET identifier_distinguishing_features = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (features, kid),
            )

    return {
        "updated": len(to_update),
        "failed": failed,
        "skipped": len(rows) - len(to_update) - len(failed),
    }


class DistinguishingFeaturesRecomputeBody(BaseModel):
    knife_id: Optional[int] = None
    knife_ids: Optional[list[int]] = None
    missing_only: bool = False
    model: Optional[str] = None


def create_admin_router(
    *,
    get_conn: GetConn,
    ollama_vision_model: str,
    app_logger: logging.Logger,
) -> APIRouter:
    router = APIRouter(tags=["admin"])

    @router.get("/api/admin/silhouettes/status")
    def admin_silhouettes_status():
        """
        Report Hu status for all masters. Use to verify what is actually in the DB (e.g. Speedgoat Ultra)
        and which masters have images but missing or degenerate Hu.
        """
        with get_conn() as conn:
            rows = conn.execute(
                """
                SELECT km.id, km.official_name AS name,
                       (kmi.image_blob IS NOT NULL AND length(kmi.image_blob) > 0) AS has_image,
                       kmi.silhouette_hu_json
                FROM knife_models_v2 km
                LEFT JOIN knife_model_images kmi ON kmi.knife_model_id = km.id
                ORDER BY km.official_name COLLATE NOCASE
                """
            ).fetchall()
        result: list[dict[str, Any]] = []
        missing_hu: list[dict[str, Any]] = []
        for r in rows:
            hu_json = (r.get("silhouette_hu_json") or "").strip()
            has_hu = bool(hu_json)
            degenerate = False
            if has_hu:
                try:
                    hu_list = json.loads(hu_json)
                    degenerate = blade_ai.is_hu_vector_degenerate(hu_list)
                except (json.JSONDecodeError, TypeError):
                    degenerate = True
            entry = {
                "id": r["id"],
                "name": r["name"],
                "has_image": bool(r["has_image"]),
                "has_hu": has_hu and not degenerate,
                "hu_json": hu_json if hu_json else None,
            }
            if degenerate and hu_json:
                entry["hu_degenerate"] = True
            result.append(entry)
            if r["has_image"] and (not has_hu or degenerate):
                missing_hu.append({"id": r["id"], "name": r["name"], "reason": "degenerate" if degenerate and has_hu else "missing"})
        return {
            "total": len(result),
            "with_image": sum(1 for e in result if e["has_image"]),
            "with_valid_hu": sum(1 for e in result if e["has_hu"]),
            "missing_hu": missing_hu,
            "masters": result,
        }

    @router.post("/api/admin/silhouettes/recompute")
    def admin_silhouettes_recompute():
        """Re-run Hu extraction for masters that have an image but missing or degenerate Hu."""
        with get_conn() as conn:
            updated = recompute_silhouettes_for_masters_without_hu(conn)
        return {"updated": updated, "message": f"Processed {updated} master(s)."}

    @router.get("/api/admin/distinguishing-features/status")
    def admin_distinguishing_features_status():
        """Report which masters have images and distinguishing features."""
        with get_conn() as conn:
            rows = conn.execute(
                """
                SELECT id, name,
                       (identifier_image_blob IS NOT NULL AND length(identifier_image_blob) > 0) AS has_image,
                       identifier_distinguishing_features
                FROM master_knives
                ORDER BY name COLLATE NOCASE
                """
            ).fetchall()
        result: list[dict[str, Any]] = []
        missing: list[dict[str, Any]] = []
        for r in rows:
            dist = (r.get("identifier_distinguishing_features") or "").strip()
            has_dist = bool(dist)
            entry = {"id": r["id"], "name": r["name"], "has_image": bool(r["has_image"]), "has_features": has_dist}
            if dist:
                entry["features"] = dist[:80] + ("..." if len(dist) > 80 else "")
            result.append(entry)
            if r["has_image"] and not has_dist:
                missing.append({"id": r["id"], "name": r["name"]})
        return {
            "total": len(result),
            "with_image": sum(1 for e in result if e["has_image"]),
            "with_features": sum(1 for e in result if e["has_features"]),
            "missing": missing,
            "masters": result,
        }

    @router.post("/api/admin/distinguishing-features/recompute")
    def admin_distinguishing_features_recompute(body: DistinguishingFeaturesRecomputeBody):
        """
        Re-run vision LLM to extract distinguishing features.
        - knife_id: single master
        - knife_ids: selected masters
        - missing_only: only those with image but no features (ignored if knife_id/knife_ids given)
        """
        model = (body.model or "").strip() or ollama_vision_model
        if body.knife_id is not None:
            ids = [body.knife_id]
        elif body.knife_ids:
            ids = body.knife_ids
        else:
            ids = None
        result = recompute_distinguishing_features(
            get_conn=get_conn,
            app_logger=app_logger,
            ollama_vision_model=ollama_vision_model,
            knife_ids=ids,
            missing_only=body.missing_only,
            model=model,
        )
        return {
            "updated": result["updated"],
            "failed": result["failed"],
            "skipped": result["skipped"],
            "message": f"Updated {result['updated']} master(s). {len(result['failed'])} failed." if result["failed"] else f"Updated {result['updated']} master(s).",
        }

    return router
