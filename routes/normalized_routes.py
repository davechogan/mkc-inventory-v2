"""Normalized schema admin: HTML page + read APIs + rebuild + CSV export."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import Any

import normalized_model
from fastapi import APIRouter
from fastapi.responses import FileResponse, Response

from reporting.domain import GetConn


def create_normalized_router(
    *,
    get_conn: GetConn,
    static_dir: Path,
    ensure_v2_exclusive_schema: Callable[[sqlite3.Connection], None],
    ensure_reporting_schema: Callable[[sqlite3.Connection], None],
    migrate_legacy_media_to_v2: Callable[[sqlite3.Connection], dict[str, int]],
    backfill_v2_model_identity: Callable[[sqlite3.Connection], dict[str, int]],
    normalize_v2_additional_fields: Callable[[sqlite3.Connection], dict[str, int]],
) -> APIRouter:
    router = APIRouter(tags=["normalized"])

    @router.get("/normalized")
    def normalized_page() -> FileResponse:
        return FileResponse(static_dir / "normalized.html")

    @router.get("/api/normalized/summary")
    def normalized_summary() -> dict[str, Any]:
        with get_conn() as conn:
            return {
                "types": conn.execute("SELECT COUNT(*) AS c FROM knife_types").fetchone()["c"],
                "forms": conn.execute("SELECT COUNT(*) AS c FROM knife_forms").fetchone()["c"],
                "families": conn.execute("SELECT COUNT(*) AS c FROM knife_families").fetchone()["c"],
                "series": conn.execute("SELECT COUNT(*) AS c FROM knife_series").fetchone()["c"],
                "collaborators": conn.execute("SELECT COUNT(*) AS c FROM collaborators").fetchone()["c"],
                "models": conn.execute("SELECT COUNT(*) AS c FROM knife_models_v2").fetchone()["c"],
                "inventory_items": conn.execute("SELECT COUNT(*) AS c FROM inventory_items_v2").fetchone()["c"],
                "last_migration": conn.execute("SELECT * FROM migration_runs_v2 ORDER BY id DESC LIMIT 1").fetchone(),
            }

    @router.get("/api/normalized/models")
    def normalized_models() -> list[dict[str, Any]]:
        with get_conn() as conn:
            rows = conn.execute(
                """SELECT km.id, km.legacy_master_id, km.official_name, km.normalized_name, km.sortable_name, km.slug,
                          kt.name AS knife_type, frm.name AS form_name, fam.name AS family_name, ks.name AS series_name,
                          c.name AS collaborator_name, km.generation_label, km.size_modifier, km.platform_variant,
                          km.steel, km.blade_finish, km.blade_color, km.handle_color, km.blade_length, km.msrp,
                          km.record_status, km.is_current_catalog, km.is_discontinued, km.official_product_url
                   FROM knife_models_v2 km
                   LEFT JOIN knife_types kt ON kt.id = km.type_id
                   LEFT JOIN knife_forms frm ON frm.id = km.form_id
                   LEFT JOIN knife_families fam ON fam.id = km.family_id
                   LEFT JOIN knife_series ks ON ks.id = km.series_id
                   LEFT JOIN collaborators c ON c.id = km.collaborator_id
                   ORDER BY km.sortable_name, km.official_name"""
            ).fetchall()
            return rows

    @router.get("/api/normalized/inventory")
    def normalized_inventory() -> list[dict[str, Any]]:
        with get_conn() as conn:
            rows = conn.execute(
                """SELECT i.id, i.legacy_inventory_id, i.quantity, i.condition, i.purchase_price, i.estimated_value,
                          i.acquired_date, i.mkc_order_number,
                          i.steel, i.blade_finish, i.blade_color, i.handle_color, i.location, i.serial_number, i.notes,
                          km.official_name, km.normalized_name, kt.name AS knife_type, fam.name AS family_name, frm.name AS form_name, ks.name AS series_name
                   FROM inventory_items_v2 i
                   LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
                   LEFT JOIN knife_types kt ON kt.id = km.type_id
                   LEFT JOIN knife_families fam ON fam.id = km.family_id
                   LEFT JOIN knife_forms frm ON frm.id = km.form_id
                   LEFT JOIN knife_series ks ON ks.id = km.series_id
                   ORDER BY km.sortable_name, i.id"""
            ).fetchall()
            return rows

    @router.post("/api/normalized/rebuild")
    def normalized_rebuild() -> dict[str, Any]:
        with get_conn() as conn:
            summary = normalized_model.migrate_legacy_to_v2(conn, force=True)
            ensure_v2_exclusive_schema(conn)
            ensure_reporting_schema(conn)
            media_summary = migrate_legacy_media_to_v2(conn)
            identity_summary = backfill_v2_model_identity(conn)
            extra_summary = normalize_v2_additional_fields(conn)
            return {
                "ok": True,
                **summary,
                "media_migration": media_summary,
                "identity_backfill": identity_summary,
                "field_normalization": extra_summary,
            }

    @router.get("/api/normalized/export/models.csv")
    def normalized_export_models_csv() -> Response:
        with get_conn() as conn:
            csv_data = normalized_model.export_models_csv(conn)
        return Response(
            content=csv_data.encode("utf-8"),
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="knife_models_v2.csv"'},
        )

    return router
