"""V2 catalog/inventory API routes (mounted from app)."""
from __future__ import annotations

import base64
import csv
import io
import json
import re
import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import Any, Optional, Type

import blade_ai
import normalized_model
from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import Response
from pydantic import BaseModel, Field, field_validator

from reporting.domain import GetConn


class InventoryItemV2In(BaseModel):
    """Request body for POST/PUT /api/v2/inventory (module-level so FastAPI/Pydantic resolve the model reliably)."""

    knife_model_id: int
    nickname: Optional[str] = None
    quantity: int = 1
    acquired_date: Optional[str] = None
    mkc_order_number: Optional[str] = None
    purchase_price: Optional[float] = None
    estimated_value: Optional[float] = None
    condition: str = "Like New"
    handle_color: Optional[str] = None
    steel: Optional[str] = None
    blade_finish: Optional[str] = None
    blade_color: Optional[str] = None
    blade_length: Optional[float] = None
    collaboration_name: Optional[str] = None
    serial_number: Optional[str] = None
    location: Optional[str] = None
    purchase_source: Optional[str] = None
    last_sharpened: Optional[str] = None
    notes: Optional[str] = None


def _color_to_filename_part(color: str) -> str:
    """Normalize a color string for use in a filename. 'Orange/Black' → 'Orange_Black'."""
    return re.sub(r'[\s/]+', '_', color.strip()).title()


def _slug_to_title(slug: str) -> str:
    """Convert a model slug to title-cased filename prefix. 'cutbank-paring-knife' → 'Cutbank_Paring_Knife'."""
    return "_".join(word.capitalize() for word in slug.split("-"))


def create_v2_router(
    *,
    get_conn: GetConn,
    ollama_vision_model: str,
    inventory_csv_columns: list[str],
    images_colors_dir: Path,
    migrate_legacy_media_to_v2,
    backfill_v2_model_identity,
    normalize_v2_additional_fields,
    normalize_category_value,
    option_in_model: Type[BaseModel],
    identifier_query_model: Type[BaseModel],
    distinguishing_recompute_body: Type[BaseModel],
) -> tuple[APIRouter, Callable[[Any], dict[str, Any]]]:
    router = APIRouter(tags=["v2"])
    def _v2_inventory_base_sql() -> str:
        """Base SQL for flattened inventory from v2 tables with model dimension joins."""
        return """
            SELECT
                i.id,
                i.knife_model_id,
                i.nickname,
                i.quantity,
                i.acquired_date,
                i.mkc_order_number,
                i.purchase_price,
                i.estimated_value,
                i.condition,
                i.location,
                i.serial_number,
                i.purchase_source,
                i.last_sharpened,
                i.notes,
                COALESCE(NULLIF(i.collaboration_name, ''), c.name) AS collaboration_name,
                c.name AS collaborator_name,
                COALESCE(i.steel, km.steel) AS blade_steel,
                COALESCE(i.blade_finish, km.blade_finish) AS blade_finish,
                COALESCE(i.blade_color, km.blade_color) AS blade_color,
                COALESCE(i.handle_color, km.handle_color) AS handle_color,
                COALESCE(i.blade_length, km.blade_length) AS blade_length,
                km.official_name AS knife_name,
                fam.name AS knife_family,
                COALESCE(ks.name, NULLIF(i.collaboration_name, ''), c.name) AS catalog_line,
                kt.name AS knife_type,
                frm.name AS form_name,
                ks.name AS series_name,
                (COALESCE(NULLIF(i.collaboration_name, ''), c.name) IS NOT NULL) AS is_collab,
                (CASE WHEN kmi.image_blob IS NOT NULL AND length(kmi.image_blob) > 0
                  THEN 1 ELSE 0 END) AS has_identifier_image,
                COALESCE(
                  -- Level 1: exact handle color match (normalized, slash-tolerant)
                  (SELECT url_path FROM knife_model_image_files
                   WHERE model_slug = km.slug
                     AND REPLACE(REPLACE(LOWER(color_name), ' ', ''), '/', '')
                       = REPLACE(REPLACE(LOWER(COALESCE(NULLIF(i.handle_color,''), km.handle_color, '')), ' ', ''), '/', '')
                   LIMIT 1),
                  -- Level 2: blade_color + handle_color combined (e.g. "Steel Desert Ironwood")
                  (SELECT url_path FROM knife_model_image_files
                   WHERE model_slug = km.slug
                     AND REPLACE(REPLACE(LOWER(color_name), ' ', ''), '/', '')
                       = REPLACE(REPLACE(LOWER(
                           TRIM(COALESCE(NULLIF(i.blade_color,''), km.blade_color, '')
                             || ' ' || COALESCE(NULLIF(i.handle_color,''), km.handle_color, ''))
                         ), ' ', ''), '/', '')
                   LIMIT 1),
                  -- Level 3: primary image for this model
                  (SELECT url_path FROM knife_model_image_files
                   WHERE model_slug = km.slug AND is_primary = 1 LIMIT 1),
                  -- Level 4: any image for this model (before falling back to BLOB)
                  (SELECT url_path FROM knife_model_image_files
                   WHERE model_slug = km.slug LIMIT 1)
                ) AS colorway_image_url
            FROM inventory_items_v2 i
            LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
            LEFT JOIN knife_types kt ON kt.id = km.type_id
            LEFT JOIN knife_forms frm ON frm.id = km.form_id
            LEFT JOIN knife_families fam ON fam.id = km.family_id
            LEFT JOIN knife_series ks ON ks.id = km.series_id
            LEFT JOIN collaborators c ON c.id = km.collaborator_id
            LEFT JOIN knife_model_images kmi ON kmi.knife_model_id = i.knife_model_id
        """


    @router.get("/api/v2/inventory")
    def v2_list_inventory(
        search: Optional[str] = None,
        type: Optional[str] = None,
        family: Optional[str] = None,
        form: Optional[str] = None,
        series: Optional[str] = None,
        steel: Optional[str] = None,
        finish: Optional[str] = None,
        handle_color: Optional[str] = None,
        condition: Optional[str] = None,
        location: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Return flattened inventory rows from v2 tables. Supports server-side filters."""
        with get_conn() as conn:
            base = _v2_inventory_base_sql()
            conditions = []
            params: list[Any] = []

            if search and search.strip():
                q = f"%{search.strip()}%"
                conditions.append(
                    "(km.official_name LIKE ? OR km.normalized_name LIKE ? OR fam.name LIKE ? "
                    "OR i.nickname LIKE ? OR i.serial_number LIKE ? OR i.notes LIKE ?)"
                )
                params.extend([q, q, q, q, q, q])

            if type and type.strip():
                conditions.append("kt.name = ?")
                params.append(type.strip())

            if family and family.strip():
                conditions.append("fam.name = ?")
                params.append(family.strip())

            if form and form.strip():
                conditions.append("frm.name = ?")
                params.append(form.strip())

            if series and series.strip():
                conditions.append("ks.name = ?")
                params.append(series.strip())

            if steel and steel.strip():
                conditions.append("(COALESCE(i.steel, km.steel) = ? OR i.steel = ? OR km.steel = ?)")
                params.extend([steel.strip(), steel.strip(), steel.strip()])

            if finish and finish.strip():
                conditions.append("(COALESCE(i.blade_finish, km.blade_finish) = ?)")
                params.append(finish.strip())

            if handle_color and handle_color.strip():
                conditions.append("(COALESCE(i.handle_color, km.handle_color) = ?)")
                params.append(handle_color.strip())

            if condition and condition.strip():
                conditions.append("(i.condition = ? OR (i.condition IS NULL AND ? = 'Like New'))")
                params.extend([condition.strip(), condition.strip()])

            if location and location.strip():
                conditions.append("i.location LIKE ?")
                params.append(f"%{location.strip()}%")

            where_sql = " AND ".join(conditions) if conditions else "1=1"
            sql = f"{base} WHERE {where_sql} ORDER BY km.sortable_name COLLATE NOCASE, i.id DESC"
            rows = conn.execute(sql, params).fetchall()
            return rows


    @router.get("/api/v2/inventory/summary")
    def v2_inventory_summary() -> dict[str, Any]:
        """Return inventory summary: rows, total quantity, spend, value, master count, by_family."""
        with get_conn() as conn:
            summary = conn.execute(
                """
                SELECT
                    COUNT(*) AS inventory_rows,
                    COALESCE(SUM(i.quantity), 0) AS total_quantity,
                    COALESCE(SUM(COALESCE(i.purchase_price, 0) * i.quantity), 0) AS total_spend,
                    COALESCE(SUM(COALESCE(i.estimated_value, 0) * i.quantity), 0) AS estimated_value
                FROM inventory_items_v2 i
                """
            ).fetchone()
            master_models = conn.execute(
                "SELECT COUNT(DISTINCT knife_model_id) AS c FROM inventory_items_v2 WHERE knife_model_id IS NOT NULL"
            ).fetchone()["c"]
            catalog_total = conn.execute(
                "SELECT COUNT(*) AS c FROM knife_models_v2"
            ).fetchone()["c"]
            by_family = conn.execute(
                """
                SELECT
                    COALESCE(NULLIF(TRIM(fam.name), ''), 'Uncategorized') AS family,
                    COUNT(*) AS inventory_rows,
                    COALESCE(SUM(i.quantity), 0) AS total_quantity
                FROM inventory_items_v2 i
                LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
                LEFT JOIN knife_families fam ON fam.id = km.family_id
                GROUP BY family
                ORDER BY total_quantity DESC, family COLLATE NOCASE
                """
            ).fetchall()
            return {
                "inventory_rows": summary["inventory_rows"],
                "total_quantity": summary["total_quantity"],
                "total_spend": summary["total_spend"],
                "estimated_value": summary["estimated_value"],
                "master_models": master_models,
                "master_count": master_models,
                "catalog_total": catalog_total,
                "by_family": by_family,
            }


    @router.get("/api/v2/inventory/filters")
    def v2_inventory_filters() -> dict[str, list[str]]:
        """Return distinct filter values for inventory dropdowns from v2 data."""
        with get_conn() as conn:
            type_vals = conn.execute(
                """SELECT DISTINCT kt.name FROM inventory_items_v2 i
                   LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
                   LEFT JOIN knife_types kt ON kt.id = km.type_id
                   WHERE kt.name IS NOT NULL AND kt.name != '' ORDER BY kt.name"""
            ).fetchall()
            family_vals = conn.execute(
                """SELECT DISTINCT fam.name FROM inventory_items_v2 i
                   LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
                   LEFT JOIN knife_families fam ON fam.id = km.family_id
                   WHERE fam.name IS NOT NULL AND fam.name != '' ORDER BY fam.name"""
            ).fetchall()
            form_vals = conn.execute(
                """SELECT DISTINCT frm.name FROM inventory_items_v2 i
                   LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
                   LEFT JOIN knife_forms frm ON frm.id = km.form_id
                   WHERE frm.name IS NOT NULL AND frm.name != '' ORDER BY frm.name"""
            ).fetchall()
            series_vals = conn.execute(
                """SELECT DISTINCT ks.name FROM inventory_items_v2 i
                   LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
                   LEFT JOIN knife_series ks ON ks.id = km.series_id
                   WHERE ks.name IS NOT NULL AND ks.name != '' ORDER BY ks.name"""
            ).fetchall()
            steel_vals = conn.execute(
                """SELECT DISTINCT COALESCE(i.steel, km.steel) AS v FROM inventory_items_v2 i
                   LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
                   WHERE COALESCE(i.steel, km.steel) IS NOT NULL AND COALESCE(i.steel, km.steel) != ''
                   ORDER BY v"""
            ).fetchall()
            finish_vals = conn.execute(
                """SELECT DISTINCT COALESCE(i.blade_finish, km.blade_finish) AS v FROM inventory_items_v2 i
                   LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
                   WHERE COALESCE(i.blade_finish, km.blade_finish) IS NOT NULL
                   ORDER BY v"""
            ).fetchall()
            handle_vals = conn.execute(
                """SELECT DISTINCT COALESCE(i.handle_color, km.handle_color) AS v FROM inventory_items_v2 i
                   LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
                   WHERE COALESCE(i.handle_color, km.handle_color) IS NOT NULL
                   ORDER BY v"""
            ).fetchall()
            blade_color_vals = conn.execute(
                """SELECT DISTINCT COALESCE(i.blade_color, km.blade_color) AS v FROM inventory_items_v2 i
                   LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
                   WHERE COALESCE(i.blade_color, km.blade_color) IS NOT NULL
                   ORDER BY v"""
            ).fetchall()
            cond_vals = conn.execute(
                "SELECT DISTINCT COALESCE(condition, 'Like New') AS v FROM inventory_items_v2 ORDER BY v"
            ).fetchall()
            loc_vals = conn.execute(
                "SELECT DISTINCT location FROM inventory_items_v2 WHERE location IS NOT NULL AND location != '' ORDER BY location"
            ).fetchall()

            def pluck(rows: list, key: str = "name") -> list[str]:
                out = []
                for r in rows:
                    v = r.get(key) or r.get("v")
                    if v and str(v).strip() and str(v).strip() not in out:
                        out.append(str(v).strip())
                return out

            return {
                "type": pluck(type_vals, "name"),
                "family": pluck(family_vals, "name"),
                "form": pluck(form_vals, "name"),
                "series": pluck(series_vals, "name"),
                "steel": pluck(steel_vals, "v"),
                "finish": pluck(finish_vals, "v"),
                "handle_color": pluck(handle_vals, "v"),
                "blade_color": pluck(blade_color_vals, "v"),
                "condition": pluck(cond_vals, "v"),
                "location": pluck(loc_vals, "location"),
            }


    @router.get("/api/v2/catalog")
    def v2_list_catalog(
        search: Optional[str] = None,
        type: Optional[str] = None,
        family: Optional[str] = None,
        form: Optional[str] = None,
        series: Optional[str] = None,
        collaboration: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Return flattened model data from knife_models_v2. Supports filters."""
        with get_conn() as conn:
            conditions = []
            params: list[Any] = []

            if search and search.strip():
                q = f"%{search.strip()}%"
                conditions.append(
                    "(km.official_name LIKE ? OR km.normalized_name LIKE ? OR fam.name LIKE ? OR km.slug LIKE ?)"
                )
                params.extend([q, q, q, q])

            if type and type.strip():
                conditions.append("kt.name = ?")
                params.append(type.strip())

            if family and family.strip():
                conditions.append("fam.name = ?")
                params.append(family.strip())

            if form and form.strip():
                conditions.append("frm.name = ?")
                params.append(form.strip())

            if series and series.strip():
                conditions.append("ks.name = ?")
                params.append(series.strip())

            if collaboration and collaboration.strip():
                conditions.append("c.name = ?")
                params.append(collaboration.strip())

            where_sql = " AND ".join(conditions) if conditions else "1=1"
            rows = conn.execute(
                f"""
                SELECT km.id, km.parent_model_id, km.official_name, km.normalized_name, km.sortable_name, km.slug,
                       kt.name AS knife_type, fam.name AS family_name, frm.name AS form_name,
                       ks.name AS series_name, c.name AS collaborator_name,
                       km.generation_label, km.size_modifier, km.platform_variant,
                       km.steel, km.blade_finish, km.blade_color, km.handle_color, km.handle_type, km.blade_length,
                       km.record_status, km.is_current_catalog, km.is_discontinued, km.msrp,
                       km.official_product_url, km.official_image_url,
                       (SELECT COUNT(*) FROM inventory_items_v2 WHERE knife_model_id = km.id) AS in_inventory_count,
                       (CASE WHEN kmi.image_blob IS NOT NULL AND length(kmi.image_blob) > 0 THEN 1 ELSE 0 END) AS has_identifier_image,
                       (SELECT url_path FROM knife_model_image_files WHERE model_slug = km.slug LIMIT 1) AS colorway_image_url
                FROM knife_models_v2 km
                LEFT JOIN knife_model_images kmi ON kmi.knife_model_id = km.id
                LEFT JOIN knife_types kt ON kt.id = km.type_id
                LEFT JOIN knife_forms frm ON frm.id = km.form_id
                LEFT JOIN knife_families fam ON fam.id = km.family_id
                LEFT JOIN knife_series ks ON ks.id = km.series_id
                LEFT JOIN collaborators c ON c.id = km.collaborator_id
                WHERE {where_sql}
                ORDER BY km.sortable_name COLLATE NOCASE, km.official_name
                """,
                params,
            ).fetchall()
            return rows


    @router.get("/api/v2/catalog/filters")
    def v2_catalog_filters() -> dict[str, list[str]]:
        """Return distinct filter values for catalog dropdowns."""
        with get_conn() as conn:
            type_vals = conn.execute(
                "SELECT DISTINCT kt.name FROM knife_models_v2 km "
                "LEFT JOIN knife_types kt ON kt.id = km.type_id WHERE kt.name IS NOT NULL ORDER BY kt.name"
            ).fetchall()
            family_vals = conn.execute(
                "SELECT DISTINCT fam.name FROM knife_models_v2 km "
                "LEFT JOIN knife_families fam ON fam.id = km.family_id WHERE fam.name IS NOT NULL ORDER BY fam.name"
            ).fetchall()
            form_vals = conn.execute(
                "SELECT DISTINCT frm.name FROM knife_models_v2 km "
                "LEFT JOIN knife_forms frm ON frm.id = km.form_id WHERE frm.name IS NOT NULL ORDER BY frm.name"
            ).fetchall()
            series_vals = conn.execute(
                "SELECT DISTINCT ks.name FROM knife_models_v2 km "
                "LEFT JOIN knife_series ks ON ks.id = km.series_id WHERE ks.name IS NOT NULL ORDER BY ks.name"
            ).fetchall()
            collab_vals = conn.execute(
                "SELECT DISTINCT c.name FROM knife_models_v2 km "
                "LEFT JOIN collaborators c ON c.id = km.collaborator_id WHERE c.name IS NOT NULL ORDER BY c.name"
            ).fetchall()

            def pluck(rows: list, key: str = "name") -> list[str]:
                return [str(r[key]).strip() for r in rows if r.get(key) and str(r.get(key)).strip()]

            return {
                "type": pluck(type_vals),
                "family": pluck(family_vals),
                "form": pluck(form_vals),
                "series": pluck(series_vals),
                "collaboration": pluck(collab_vals),
            }


    @router.get("/api/v2/colors")
    def v2_colors() -> dict[str, list[str]]:
        """Return distinct handle and blade colors for use in dropdowns."""
        with get_conn() as conn:
            handle = conn.execute(
                """SELECT DISTINCT handle_color FROM knife_models_v2
                   WHERE handle_color IS NOT NULL AND handle_color != ''
                   UNION
                   SELECT DISTINCT handle_color FROM inventory_items_v2
                   WHERE handle_color IS NOT NULL AND handle_color != ''
                   ORDER BY handle_color COLLATE NOCASE"""
            ).fetchall()
            blade = conn.execute(
                """SELECT DISTINCT blade_color FROM knife_models_v2
                   WHERE blade_color IS NOT NULL AND blade_color != ''
                   UNION
                   SELECT DISTINCT blade_color FROM inventory_items_v2
                   WHERE blade_color IS NOT NULL AND blade_color != ''
                   ORDER BY blade_color COLLATE NOCASE"""
            ).fetchall()
            return {
                "handle_colors": [r[0] for r in handle],
                "blade_colors": [r[0] for r in blade],
            }

    @router.get("/api/v2/models/by-legacy-master/{legacy_id}")
    def v2_model_by_legacy_master(legacy_id: int) -> dict[str, Any]:
        """Resolve legacy master_knives.id to v2 model for ?add= flow from Identify page."""
        with get_conn() as conn:
            row = conn.execute(
                """
                SELECT km.id, km.official_name, km.normalized_name, km.slug,
                       kt.name AS knife_type, fam.name AS family_name, frm.name AS form_name,
                       ks.name AS series_name, c.name AS collaborator_name,
                       km.steel, km.blade_finish, km.blade_color, km.handle_color, km.handle_type, km.blade_length, km.msrp
                FROM knife_models_v2 km
                LEFT JOIN knife_types kt ON kt.id = km.type_id
                LEFT JOIN knife_forms frm ON frm.id = km.form_id
                LEFT JOIN knife_families fam ON fam.id = km.family_id
                LEFT JOIN knife_series ks ON ks.id = km.series_id
                LEFT JOIN collaborators c ON c.id = km.collaborator_id
                WHERE km.legacy_master_id = ?
                """,
                (legacy_id,),
            ).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="No v2 model for legacy master id.")
            return row


    @router.get("/api/v2/models/search")
    def v2_models_search(q: Optional[str] = None, limit: int = 50) -> list[dict[str, Any]]:
        """Search knife_models_v2 for model picker. Returns flattened rows."""
        with get_conn() as conn:
            params: list[Any] = []
            where = "1=1"
            if q and q.strip():
                search_term = f"%{q.strip()}%"
                where = "(km.official_name LIKE ? OR km.normalized_name LIKE ? OR fam.name LIKE ? OR km.slug LIKE ?)"
                params = [search_term, search_term, search_term, search_term]
            params.append(limit)
            rows = conn.execute(
                f"""
                SELECT km.id, km.official_name, km.normalized_name, km.slug,
                       kt.name AS knife_type, fam.name AS family_name, frm.name AS form_name,
                       ks.name AS series_name, c.name AS collaborator_name,
                       km.steel, km.blade_finish, km.blade_color, km.handle_color, km.handle_type, km.blade_length
                FROM knife_models_v2 km
                LEFT JOIN knife_types kt ON kt.id = km.type_id
                LEFT JOIN knife_forms frm ON frm.id = km.form_id
                LEFT JOIN knife_families fam ON fam.id = km.family_id
                LEFT JOIN knife_series ks ON ks.id = km.series_id
                LEFT JOIN collaborators c ON c.id = km.collaborator_id
                WHERE {where}
                ORDER BY km.sortable_name COLLATE NOCASE
                LIMIT ?
                """,
                params,
            ).fetchall()
            return rows


    class V2ModelIn(BaseModel):
        model_config = {"extra": "ignore"}

        official_name: str = Field(min_length=1, max_length=200)
        canonical_slug: Optional[str] = None
        normalized_name: Optional[str] = None
        knife_type: Optional[str] = None
        form_name: Optional[str] = None
        family_name: Optional[str] = None
        series_name: Optional[str] = None
        collaborator_name: Optional[str] = None
        generation_label: Optional[str] = None
        size_modifier: Optional[str] = None
        platform_variant: Optional[str] = None
        steel: Optional[str] = None
        blade_finish: Optional[str] = None
        blade_color: Optional[str] = None
        handle_color: Optional[str] = None
        handle_type: Optional[str] = None
        blade_length: Optional[float] = None
        record_status: Optional[str] = "active"
        is_current_catalog: Optional[bool] = True
        is_discontinued: Optional[bool] = False
        msrp: Optional[float] = None
        official_product_url: Optional[str] = None
        official_image_url: Optional[str] = None
        notes: Optional[str] = None
        parent_model_id: Optional[int] = None
        distinguishing_features: Optional[str] = None

        @field_validator("knife_type", mode="before")
        @classmethod
        def normalize_knife_type(cls, v: Any) -> Optional[str]:
            return normalize_category_value(v)


    def _require_v2_identity(payload: V2ModelIn) -> None:
        missing = []
        if not (payload.official_name or "").strip():
            missing.append("official_name")
        if not (payload.knife_type or "").strip():
            missing.append("knife_type")
        if not (payload.form_name or "").strip():
            missing.append("form_name")
        if not (payload.family_name or "").strip():
            missing.append("family_name")
        if missing:
            raise HTTPException(
                status_code=400,
                detail=f"Missing required identity fields: {', '.join(missing)}",
            )


    def _v2_option_exists(conn: sqlite3.Connection, option_type: str, value: Optional[str]) -> bool:
        if not value or not str(value).strip():
            return True
        row = conn.execute(
            "SELECT 1 FROM v2_option_values WHERE option_type = ? AND lower(name) = lower(?) LIMIT 1",
            (option_type, str(value).strip()),
        ).fetchone()
        return row is not None


    def _validate_v2_controlled_identity(payload: V2ModelIn, conn: sqlite3.Connection) -> None:
        controlled = [
            ("collaborator_name", "collaborators", payload.collaborator_name),
            ("generation_label", "generations", payload.generation_label),
            ("size_modifier", "size-modifiers", payload.size_modifier),
            ("platform_variant", "platform-variants", payload.platform_variant),
            ("handle_type", "handle-types", payload.handle_type),
        ]
        invalid = [field for field, option_type, value in controlled if not _v2_option_exists(conn, option_type, value)]
        if invalid:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Controlled fields contain values not in allowed options: "
                    + ", ".join(invalid)
                    + ". Add option first, then save."
                ),
            )


    def _v2_dim_id(conn: sqlite3.Connection, table: str, name: Optional[str]) -> Optional[int]:
        if not name or not str(name).strip():
            return None
        n = str(name).strip()
        row = conn.execute(f"SELECT id FROM {table} WHERE name = ?", (n,)).fetchone()
        if row:
            return row["id"]
        if table == "knife_types":
            cur = conn.execute(
                "INSERT INTO knife_types (name, slug, sort_order) VALUES (?, ?, ?)",
                (n, normalized_model.slugify(n), 999),
            )
            return cur.lastrowid
        if table == "knife_forms":
            cur = conn.execute(
                "INSERT INTO knife_forms (name, slug) VALUES (?, ?)",
                (n, normalized_model.slugify(n)),
            )
            return cur.lastrowid
        if table == "knife_families":
            cur = conn.execute(
                "INSERT INTO knife_families (name, normalized_name, slug) VALUES (?, ?, ?)",
                (n, n, normalized_model.slugify(n)),
            )
            return cur.lastrowid
        if table == "knife_series":
            cur = conn.execute(
                "INSERT INTO knife_series (name, slug) VALUES (?, ?)",
                (n, normalized_model.slugify(n)),
            )
            return cur.lastrowid
        if table == "collaborators":
            cur = conn.execute(
                "INSERT INTO collaborators (name, slug) VALUES (?, ?)",
                (n, normalized_model.slugify(n)),
            )
            return cur.lastrowid
        return None


    def _v2_model_slug(conn: sqlite3.Connection, base_name: str, existing_id: Optional[int] = None) -> str:
        base = normalized_model.slugify(base_name) or "model"
        slug = base
        i = 2
        while True:
            if existing_id is None:
                row = conn.execute("SELECT id FROM knife_models_v2 WHERE slug = ?", (slug,)).fetchone()
            else:
                row = conn.execute("SELECT id FROM knife_models_v2 WHERE slug = ? AND id != ?", (slug, existing_id)).fetchone()
            if not row:
                return slug
            slug = f"{base}-{i}"
            i += 1


    @router.get("/api/v2/models/{model_id}")
    def v2_get_model(model_id: int) -> dict[str, Any]:
        with get_conn() as conn:
            row = conn.execute(
                """
                SELECT km.id, km.parent_model_id, km.official_name, km.normalized_name, km.sortable_name, km.slug,
                       kt.name AS knife_type, fam.name AS family_name, frm.name AS form_name,
                       ks.name AS series_name, c.name AS collaborator_name,
                       km.generation_label, km.size_modifier, km.platform_variant, km.steel, km.blade_finish,
                       km.blade_color, km.handle_color, km.handle_type, km.blade_length, km.record_status, km.is_current_catalog,
                       km.is_discontinued, km.msrp, km.official_product_url, km.official_image_url, km.notes,
                       d.distinguishing_features,
                       (CASE WHEN kmi.image_blob IS NOT NULL AND length(kmi.image_blob) > 0 THEN 1 ELSE 0 END) AS has_identifier_image
                FROM knife_models_v2 km
                LEFT JOIN knife_types kt ON kt.id = km.type_id
                LEFT JOIN knife_forms frm ON frm.id = km.form_id
                LEFT JOIN knife_families fam ON fam.id = km.family_id
                LEFT JOIN knife_series ks ON ks.id = km.series_id
                LEFT JOIN collaborators c ON c.id = km.collaborator_id
                LEFT JOIN knife_model_images kmi ON kmi.knife_model_id = km.id
                LEFT JOIN knife_model_descriptors d ON d.knife_model_id = km.id
                WHERE km.id = ?
                """,
                (model_id,),
            ).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Model not found.")
            return row


    @router.post("/api/v2/models")
    def v2_create_model(payload: V2ModelIn):
        _require_v2_identity(payload)
        with get_conn() as conn:
            _validate_v2_controlled_identity(payload, conn)
            type_id = _v2_dim_id(conn, "knife_types", payload.knife_type)
            form_id = _v2_dim_id(conn, "knife_forms", payload.form_name)
            family_id = _v2_dim_id(conn, "knife_families", payload.family_name)
            series_id = _v2_dim_id(conn, "knife_series", payload.series_name)
            collaborator_id = _v2_dim_id(conn, "collaborators", payload.collaborator_name)
            normalized_name = (payload.normalized_name or payload.official_name).strip()
            sortable_name = normalized_name
            slug = _v2_model_slug(conn, payload.canonical_slug or normalized_name)
            cur = conn.execute(
                """
                INSERT INTO knife_models_v2 (
                    official_name, normalized_name, sortable_name, slug, type_id, form_id, family_id, series_id, collaborator_id,
                    parent_model_id, generation_label, size_modifier, platform_variant, steel, blade_finish, blade_color, handle_color, handle_type,
                    blade_length, record_status, is_current_catalog, is_discontinued, msrp, official_product_url, official_image_url, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload.official_name.strip(), normalized_name, sortable_name, slug, type_id, form_id, family_id, series_id, collaborator_id,
                    payload.parent_model_id, payload.generation_label, payload.size_modifier, payload.platform_variant,
                    payload.steel, payload.blade_finish, payload.blade_color, payload.handle_color, payload.handle_type, payload.blade_length,
                    payload.record_status or "active", 0 if payload.is_current_catalog is False else 1,
                    1 if payload.is_discontinued else 0, payload.msrp, payload.official_product_url, payload.official_image_url, payload.notes,
                ),
            )
            if payload.distinguishing_features is not None:
                conn.execute(
                    """
                    INSERT INTO knife_model_descriptors (knife_model_id, distinguishing_features, updated_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(knife_model_id) DO UPDATE SET
                        distinguishing_features=excluded.distinguishing_features, updated_at=CURRENT_TIMESTAMP
                    """,
                    (cur.lastrowid, payload.distinguishing_features),
                )
            return {"id": cur.lastrowid, "message": "Created"}


    @router.put("/api/v2/models/{model_id}")
    def v2_update_model(model_id: int, payload: V2ModelIn):
        _require_v2_identity(payload)
        with get_conn() as conn:
            _validate_v2_controlled_identity(payload, conn)
            exists = conn.execute("SELECT id FROM knife_models_v2 WHERE id = ?", (model_id,)).fetchone()
            if not exists:
                raise HTTPException(status_code=404, detail="Model not found.")
            type_id = _v2_dim_id(conn, "knife_types", payload.knife_type)
            form_id = _v2_dim_id(conn, "knife_forms", payload.form_name)
            family_id = _v2_dim_id(conn, "knife_families", payload.family_name)
            series_id = _v2_dim_id(conn, "knife_series", payload.series_name)
            collaborator_id = _v2_dim_id(conn, "collaborators", payload.collaborator_name)
            normalized_name = (payload.normalized_name or payload.official_name).strip()
            slug = _v2_model_slug(conn, payload.canonical_slug or normalized_name, existing_id=model_id)
            conn.execute(
                """
                UPDATE knife_models_v2
                SET official_name = ?, normalized_name = ?, sortable_name = ?, slug = ?,
                    type_id = ?, form_id = ?, family_id = ?, series_id = ?, collaborator_id = ?, parent_model_id = ?,
                    generation_label = ?, size_modifier = ?, platform_variant = ?, steel = ?, blade_finish = ?,
                    blade_color = ?, handle_color = ?, handle_type = ?, blade_length = ?, record_status = ?, is_current_catalog = ?,
                    is_discontinued = ?, msrp = ?, official_product_url = ?, official_image_url = ?, notes = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    payload.official_name.strip(), normalized_name, normalized_name, slug,
                    type_id, form_id, family_id, series_id, collaborator_id, payload.parent_model_id,
                    payload.generation_label, payload.size_modifier, payload.platform_variant, payload.steel, payload.blade_finish,
                    payload.blade_color, payload.handle_color, payload.handle_type, payload.blade_length, payload.record_status or "active",
                    0 if payload.is_current_catalog is False else 1, 1 if payload.is_discontinued else 0,
                    payload.msrp, payload.official_product_url, payload.official_image_url, payload.notes, model_id,
                ),
            )
            if payload.distinguishing_features is not None:
                conn.execute(
                    """
                    INSERT INTO knife_model_descriptors (knife_model_id, distinguishing_features, updated_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(knife_model_id) DO UPDATE SET
                        distinguishing_features=excluded.distinguishing_features, updated_at=CURRENT_TIMESTAMP
                    """,
                    (model_id, payload.distinguishing_features),
                )
            return {"message": "Updated"}


    @router.delete("/api/v2/models/{model_id}")
    def v2_delete_model(model_id: int):
        with get_conn() as conn:
            used = conn.execute("SELECT COUNT(*) AS c FROM inventory_items_v2 WHERE knife_model_id = ?", (model_id,)).fetchone()["c"]
            if used > 0:
                raise HTTPException(status_code=400, detail="Cannot delete model used by inventory.")
            cur = conn.execute("DELETE FROM knife_models_v2 WHERE id = ?", (model_id,))
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Model not found.")
            return {"message": "Deleted"}


    @router.post("/api/v2/models/{model_id}/duplicate")
    def v2_duplicate_model(model_id: int):
        with get_conn() as conn:
            row = conn.execute("SELECT * FROM knife_models_v2 WHERE id = ?", (model_id,)).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Model not found.")
            name = (row.get("official_name") or "").strip()
            new_name = f"{name} (copy)" if name else "Copy"
            slug = _v2_model_slug(conn, new_name)
            cur = conn.execute(
                """
                INSERT INTO knife_models_v2 (
                    legacy_master_id, official_name, normalized_name, sortable_name, slug, type_id, form_id, family_id, series_id,
                    collaborator_id, parent_model_id, generation_label, size_modifier, platform_variant, steel, blade_finish,
                    blade_color, handle_color, handle_type, blade_length, record_status, is_current_catalog, is_discontinued, msrp,
                    official_product_url, official_image_url, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    # v2 schema marks `legacy_master_id` as UNIQUE; duplicates should not point at the same legacy master.
                    None, new_name, row.get("normalized_name") or new_name, row.get("sortable_name") or new_name, slug,
                    row.get("type_id"), row.get("form_id"), row.get("family_id"), row.get("series_id"), row.get("collaborator_id"),
                    row.get("parent_model_id"), row.get("generation_label"), row.get("size_modifier"), row.get("platform_variant"),
                    row.get("steel"), row.get("blade_finish"), row.get("blade_color"), row.get("handle_color"), row.get("handle_type"), row.get("blade_length"),
                    row.get("record_status"), row.get("is_current_catalog"), row.get("is_discontinued"), row.get("msrp"),
                    row.get("official_product_url"), row.get("official_image_url"), row.get("notes"),
                ),
            )
            return {"id": cur.lastrowid, "message": "Duplicated"}


    @router.get("/api/v2/models/{model_id}/image")
    def v2_get_model_image(model_id: int):
        with get_conn() as conn:
            row = conn.execute(
                "SELECT image_blob, image_mime FROM knife_model_images WHERE knife_model_id = ?",
                (model_id,),
            ).fetchone()
            if not row or not row.get("image_blob"):
                raise HTTPException(status_code=404, detail="No stored reference image for this model.")
            return Response(content=row["image_blob"], media_type=(row.get("image_mime") or "image/jpeg"))


    @router.post("/api/v2/models/{model_id}/image")
    async def v2_upload_model_image(
        model_id: int,
        file: UploadFile = File(...),
        model: Optional[str] = Form(None),
    ):
        raw = await file.read()
        if len(raw) > 15 * 1024 * 1024:
            raise HTTPException(status_code=413, detail="Image larger than 15MB")
        mime = ((file.content_type or "").split(";")[0].strip().lower() or "image/jpeg")
        if not mime.startswith("image/"):
            mime = "image/jpeg"
        hu_list, hu_err = blade_ai.extract_blade_hu_from_image_bytes(raw)
        hu_json = json.dumps(hu_list) if hu_list else None
        dist_features: Optional[str] = None
        dist_error: Optional[str] = None
        vision_model = (model or "").strip() or ollama_vision_model
        if vision_model:
            model_ok, _ = blade_ai.check_ollama_model(vision_model)
            if model_ok:
                img_b64 = base64.standard_b64encode(raw).decode("ascii")
                dist_features, dist_error = blade_ai.extract_distinguishing_features_from_image(vision_model, img_b64)
        with get_conn() as conn:
            exists = conn.execute("SELECT id FROM knife_models_v2 WHERE id = ?", (model_id,)).fetchone()
            if not exists:
                raise HTTPException(status_code=404, detail="Model not found.")
            conn.execute(
                """
                INSERT INTO knife_model_images (knife_model_id, image_blob, image_mime, silhouette_hu_json, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(knife_model_id) DO UPDATE SET
                    image_blob=excluded.image_blob, image_mime=excluded.image_mime,
                    silhouette_hu_json=excluded.silhouette_hu_json, updated_at=CURRENT_TIMESTAMP
                """,
                (model_id, raw, mime, hu_json),
            )
            if dist_features is not None:
                conn.execute(
                    """
                    INSERT INTO knife_model_descriptors (knife_model_id, distinguishing_features, updated_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(knife_model_id) DO UPDATE SET
                        distinguishing_features=excluded.distinguishing_features, updated_at=CURRENT_TIMESTAMP
                    """,
                    (model_id, dist_features),
                )
        return {
            "message": "Reference image stored.",
            "has_silhouette": hu_list is not None,
            "silhouette_error": hu_err,
            "distinguishing_features": dist_features,
            "distinguishing_features_error": dist_error,
        }


    @router.delete("/api/v2/models/{model_id}/image")
    def v2_delete_model_image(model_id: int):
        with get_conn() as conn:
            cur = conn.execute("DELETE FROM knife_model_images WHERE knife_model_id = ?", (model_id,))
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Image not found.")
            return {"message": "Reference image cleared."}


    @router.post("/api/v2/models/{model_id}/colorway-image")
    async def v2_upload_colorway_image(
        model_id: int,
        file: UploadFile = File(...),
        handle_color: str = Form(...),
        blade_color: str = Form(""),
    ):
        """Upload a transparent PNG for a specific model colorway.

        Saves to Images/MKC_Colors/ with a slug-derived filename and registers
        it in knife_model_image_files.
        """
        if not (file.content_type or "").startswith("image/png") and not (file.filename or "").lower().endswith(".png"):
            raise HTTPException(status_code=400, detail="File must be a PNG.")

        with get_conn() as conn:
            model = conn.execute(
                "SELECT slug FROM knife_models_v2 WHERE id = ?", (model_id,)
            ).fetchone()
            if not model or not model["slug"]:
                raise HTTPException(status_code=404, detail="Model not found or has no slug.")

            slug = model["slug"]
            slug_part = _slug_to_title(slug)
            handle_part = _color_to_filename_part(handle_color)
            blade_part = _color_to_filename_part(blade_color) if blade_color.strip() else ""

            if blade_part:
                filename = f"{slug_part}_{blade_part}_{handle_part}.png"
                color_name = f"{blade_color.strip()} {handle_color.strip()}"
            else:
                filename = f"{slug_part}_{handle_part}.png"
                color_name = handle_color.strip()

            dest = images_colors_dir / filename
            content = await file.read()
            dest.write_bytes(content)

            url_path = f"/images/colors/{filename}"
            conn.execute(
                """INSERT INTO knife_model_image_files (model_slug, color_name, filename, url_path)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(filename) DO UPDATE SET
                     model_slug=excluded.model_slug,
                     color_name=excluded.color_name,
                     url_path=excluded.url_path""",
                (slug, color_name, filename, url_path),
            )

        return {"filename": filename, "url_path": url_path, "color_name": color_name}

    @router.post("/api/v2/models/{model_id}/recompute-descriptors")
    def v2_recompute_model_descriptors(model_id: int, model: Optional[str] = None):
        vision_model = (model or "").strip() or ollama_vision_model
        with get_conn() as conn:
            row = conn.execute(
                "SELECT image_blob FROM knife_model_images WHERE knife_model_id = ?",
                (model_id,),
            ).fetchone()
            if not row or not row.get("image_blob"):
                raise HTTPException(status_code=404, detail="No reference image stored for model.")
            raw = row["image_blob"]
            model_ok, err = blade_ai.check_ollama_model(vision_model)
            if not model_ok:
                raise HTTPException(status_code=400, detail=err or "Vision model unavailable.")
            img_b64 = base64.standard_b64encode(raw).decode("ascii")
            dist_features, dist_error = blade_ai.extract_distinguishing_features_from_image(vision_model, img_b64)
            if dist_error:
                raise HTTPException(status_code=500, detail=dist_error)
            conn.execute(
                """
                INSERT INTO knife_model_descriptors (knife_model_id, distinguishing_features, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(knife_model_id) DO UPDATE SET
                    distinguishing_features=excluded.distinguishing_features, updated_at=CURRENT_TIMESTAMP
                """,
                (model_id, dist_features),
            )
            return {"updated": 1, "distinguishing_features": dist_features}


    @router.get("/api/v2/admin/silhouettes/status")
    def v2_admin_silhouette_status():
        with get_conn() as conn:
            rows = conn.execute(
                """
                SELECT km.id, km.official_name AS name, kmi.image_blob, kmi.silhouette_hu_json
                FROM knife_models_v2 km
                LEFT JOIN knife_model_images kmi ON kmi.knife_model_id = km.id
                ORDER BY km.sortable_name COLLATE NOCASE
                """
            ).fetchall()
        result = []
        missing = []
        for row in rows:
            has_image = bool(row.get("image_blob"))
            hu_json = row.get("silhouette_hu_json")
            has_hu = False
            hu_degenerate = False
            if hu_json and str(hu_json).strip():
                try:
                    arr = json.loads(hu_json)
                    if isinstance(arr, list) and len(arr) == 7:
                        has_hu = True
                        hu_degenerate = all(float(x) == 0.0 for x in arr)
                    else:
                        hu_degenerate = True
                except Exception:
                    hu_degenerate = True
            if has_image and (not has_hu or hu_degenerate):
                missing.append({"id": row["id"], "name": row["name"]})
            result.append(
                {
                    "id": row["id"],
                    "name": row["name"],
                    "has_image": has_image,
                    "has_hu": has_hu,
                    "hu_degenerate": hu_degenerate,
                    "hu_json": hu_json,
                }
            )
        return {
            "total": len(result),
            "with_image": sum(1 for e in result if e["has_image"]),
            "with_valid_hu": sum(1 for e in result if e["has_image"] and e["has_hu"] and not e["hu_degenerate"]),
            "missing_hu": missing,
            "masters": result,
        }


    @router.post("/api/v2/admin/silhouettes/recompute")
    def v2_admin_silhouette_recompute():
        updated = 0
        failed: list[dict[str, Any]] = []
        with get_conn() as conn:
            rows = conn.execute(
                """
                SELECT km.id, km.official_name AS name, kmi.image_blob, kmi.silhouette_hu_json
                FROM knife_models_v2 km
                JOIN knife_model_images kmi ON kmi.knife_model_id = km.id
                ORDER BY km.sortable_name COLLATE NOCASE
                """
            ).fetchall()
            for row in rows:
                hu_json = row.get("silhouette_hu_json")
                needs = True
                if hu_json:
                    try:
                        arr = json.loads(hu_json)
                        if isinstance(arr, list) and len(arr) == 7 and not all(float(x) == 0.0 for x in arr):
                            needs = False
                    except Exception:
                        needs = True
                if not needs:
                    continue
                hu_list, hu_err = blade_ai.extract_blade_hu_from_image_bytes(row["image_blob"])
                if not hu_list:
                    failed.append({"id": row["id"], "name": row["name"], "reason": hu_err or "Unable to compute Hu"})
                    continue
                conn.execute(
                    """
                    UPDATE knife_model_images
                    SET silhouette_hu_json = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE knife_model_id = ?
                    """,
                    (json.dumps(hu_list), row["id"]),
                )
                updated += 1
        return {"updated": updated, "failed": failed, "message": f"Updated {updated} model(s)."}


    @router.get("/api/v2/admin/distinguishing-features/status")
    def v2_admin_distinguishing_status():
        with get_conn() as conn:
            rows = conn.execute(
                """
                SELECT km.id, km.official_name AS name, kmi.image_blob, d.distinguishing_features
                FROM knife_models_v2 km
                LEFT JOIN knife_model_images kmi ON kmi.knife_model_id = km.id
                LEFT JOIN knife_model_descriptors d ON d.knife_model_id = km.id
                ORDER BY km.sortable_name COLLATE NOCASE
                """
            ).fetchall()
        result = []
        missing = []
        for row in rows:
            has_image = bool(row.get("image_blob"))
            features = (row.get("distinguishing_features") or "").strip()
            has_features = bool(features)
            if has_image and not has_features:
                missing.append({"id": row["id"], "name": row["name"]})
            result.append(
                {
                    "id": row["id"],
                    "name": row["name"],
                    "has_image": has_image,
                    "has_features": has_features,
                    "features": features,
                }
            )
        return {
            "total": len(result),
            "with_image": sum(1 for e in result if e["has_image"]),
            "with_features": sum(1 for e in result if e["has_features"]),
            "missing": missing,
            "masters": result,
        }


    @router.post("/api/v2/admin/distinguishing-features/recompute")
    def v2_admin_distinguishing_recompute(body: distinguishing_recompute_body):
        model_name = (body.model or "").strip() or ollama_vision_model
        model_ok, err = blade_ai.check_ollama_model(model_name)
        if not model_ok:
            raise HTTPException(status_code=400, detail=err or "Vision model unavailable.")
        if body.knife_id is not None:
            ids = [body.knife_id]
        elif body.knife_ids:
            ids = body.knife_ids
        else:
            ids = None
        updated = 0
        failed: list[dict[str, Any]] = []
        skipped = 0
        with get_conn() as conn:
            params: list[Any] = []
            where = "1=1"
            if ids:
                placeholders = ",".join("?" for _ in ids)
                where = f"km.id IN ({placeholders})"
                params.extend(ids)
            rows = conn.execute(
                f"""
                SELECT km.id, km.official_name AS name, kmi.image_blob, d.distinguishing_features
                FROM knife_models_v2 km
                LEFT JOIN knife_model_images kmi ON kmi.knife_model_id = km.id
                LEFT JOIN knife_model_descriptors d ON d.knife_model_id = km.id
                WHERE {where}
                ORDER BY km.sortable_name COLLATE NOCASE
                """,
                params,
            ).fetchall()
            for row in rows:
                if not row.get("image_blob"):
                    skipped += 1
                    continue
                if body.missing_only and row.get("distinguishing_features"):
                    skipped += 1
                    continue
                img_b64 = base64.standard_b64encode(row["image_blob"]).decode("ascii")
                features, dist_err = blade_ai.extract_distinguishing_features_from_image(model_name, img_b64)
                if dist_err or not features:
                    failed.append({"id": row["id"], "name": row["name"], "reason": dist_err or "No features returned"})
                    continue
                conn.execute(
                    """
                    INSERT INTO knife_model_descriptors (knife_model_id, distinguishing_features, updated_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(knife_model_id) DO UPDATE SET
                        distinguishing_features=excluded.distinguishing_features, updated_at=CURRENT_TIMESTAMP
                    """,
                    (row["id"], features),
                )
                updated += 1
        return {
            "updated": updated,
            "failed": failed,
            "skipped": skipped,
            "message": f"Updated {updated} model(s). {len(failed)} failed." if failed else f"Updated {updated} model(s).",
        }


    @router.get("/api/v2/options")
    def v2_get_options():
        option_types = (
            "blade-steels",
            "blade-finishes",
            "blade-colors",
            "handle-colors",
            "conditions",
            "blade-types",
            "categories",
            "blade-families",
            "primary-use-cases",
            "handle-types",
            "collaborators",
            "generations",
            "size-modifiers",
            "platform-variants",
        )
        with get_conn() as conn:
            result: dict[str, list[dict[str, Any]]] = {}
            for key in option_types:
                rows = conn.execute(
                    "SELECT id, name FROM v2_option_values WHERE option_type = ? ORDER BY name COLLATE NOCASE",
                    (key,),
                ).fetchall()
                result[key] = rows
            return result


    @router.post("/api/v2/options/{option_type}")
    def v2_add_option(option_type: str, payload: option_in_model):
        allowed = {
            "blade-steels",
            "blade-finishes",
            "blade-colors",
            "handle-colors",
            "conditions",
            "blade-types",
            "categories",
            "blade-families",
            "primary-use-cases",
            "handle-types",
            "collaborators",
            "generations",
            "size-modifiers",
            "platform-variants",
        }
        if option_type not in allowed:
            raise HTTPException(status_code=404, detail="Unknown option type.")
        with get_conn() as conn:
            clean_name = payload.name.strip()
            if not clean_name:
                raise HTTPException(status_code=400, detail="Option name is required.")
            try:
                cur = conn.execute(
                    "INSERT INTO v2_option_values (option_type, name) VALUES (?, ?)",
                    (option_type, clean_name),
                )
            except sqlite3.IntegrityError:
                raise HTTPException(status_code=400, detail="Option already exists.")
            if option_type == "collaborators":
                _v2_dim_id(conn, "collaborators", clean_name)
            return {"id": cur.lastrowid, "message": "Created"}


    @router.delete("/api/v2/options/{option_type}/{option_id}")
    def v2_delete_option(option_type: str, option_id: int):
        allowed = {
            "blade-steels",
            "blade-finishes",
            "blade-colors",
            "handle-colors",
            "conditions",
            "blade-types",
            "categories",
            "blade-families",
            "primary-use-cases",
            "handle-types",
            "collaborators",
            "generations",
            "size-modifiers",
            "platform-variants",
        }
        if option_type not in allowed:
            raise HTTPException(status_code=404, detail="Unknown option type.")
        with get_conn() as conn:
            row = conn.execute(
                "SELECT name FROM v2_option_values WHERE id = ? AND option_type = ?",
                (option_id, option_type),
            ).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Option not found.")
            option_name = (row.get("name") or "").strip()
            usage_sql = {
                "collaborators": (
                    "SELECT COUNT(*) AS c FROM knife_models_v2 km "
                    "LEFT JOIN collaborators c ON c.id = km.collaborator_id "
                    "WHERE lower(c.name) = lower(?)"
                ),
                "generations": "SELECT COUNT(*) AS c FROM knife_models_v2 WHERE lower(generation_label) = lower(?)",
                "size-modifiers": "SELECT COUNT(*) AS c FROM knife_models_v2 WHERE lower(size_modifier) = lower(?)",
                "platform-variants": "SELECT COUNT(*) AS c FROM knife_models_v2 WHERE lower(platform_variant) = lower(?)",
                "handle-types": "SELECT COUNT(*) AS c FROM knife_models_v2 WHERE lower(handle_type) = lower(?)",
            }
            sql = usage_sql.get(option_type)
            if sql and option_name:
                in_use = conn.execute(sql, (option_name,)).fetchone()["c"]
                if in_use:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Cannot delete option in use by {in_use} model(s).",
                    )
            cur = conn.execute(
                "DELETE FROM v2_option_values WHERE id = ? AND option_type = ?",
                (option_id, option_type),
            )
            return {"message": "Deleted"}


    @router.get("/api/inventory/options")
    def inventory_options(master_knife_id: Optional[int] = None):  # noqa: ARG001
        """Return option lists for the inventory form. master_knife_id accepted but unused (all options returned)."""
        option_types = (
            "blade-steels", "blade-finishes", "blade-colors",
            "handle-colors", "blade-types", "categories", "primary-use-cases",
        )
        with get_conn() as conn:
            result: dict[str, list[dict[str, Any]]] = {}
            for key in option_types:
                result[key] = conn.execute(
                    "SELECT id, name FROM v2_option_values WHERE option_type = ? ORDER BY name COLLATE NOCASE",
                    (key,),
                ).fetchall()
            result["_filtered"] = False
            return result


    @router.post("/api/v2/inventory")
    def v2_create_inventory_item(payload: InventoryItemV2In):
        """Create inventory item in v2 only (canonical write path)."""
        with get_conn() as conn:
            model_exists = conn.execute(
                "SELECT id, legacy_master_id FROM knife_models_v2 WHERE id = ?",
                (payload.knife_model_id,),
            ).fetchone()
            if not model_exists:
                raise HTTPException(status_code=400, detail="Invalid knife model id.")
            cur = conn.execute(
                """
                INSERT INTO inventory_items_v2
                (legacy_master_id, knife_model_id, nickname, quantity, acquired_date, mkc_order_number, purchase_price, estimated_value,
                 condition, steel, blade_finish, blade_color, handle_color, blade_length, collaboration_name, serial_number,
                 location, purchase_source, last_sharpened, notes, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                (
                    model_exists.get("legacy_master_id"),
                    payload.knife_model_id,
                    payload.nickname,
                    payload.quantity,
                    payload.acquired_date,
                    payload.mkc_order_number,
                    payload.purchase_price,
                    payload.estimated_value,
                    payload.condition,
                    payload.steel,
                    payload.blade_finish,
                    payload.blade_color,
                    payload.handle_color,
                    payload.blade_length,
                    payload.collaboration_name,
                    payload.serial_number,
                    payload.location,
                    payload.purchase_source,
                    payload.last_sharpened,
                    payload.notes,
                ),
            )
            return {"id": cur.lastrowid, "message": "Created"}


    @router.put("/api/v2/inventory/{item_id}")
    def v2_update_inventory_item(item_id: int, payload: InventoryItemV2In):
        """Update inventory item in v2 only (canonical write path)."""
        with get_conn() as conn:
            model_exists = conn.execute(
                "SELECT id, legacy_master_id FROM knife_models_v2 WHERE id = ?",
                (payload.knife_model_id,),
            ).fetchone()
            if not model_exists:
                raise HTTPException(status_code=400, detail="Invalid knife model id.")
            cur = conn.execute(
                """
                UPDATE inventory_items_v2
                SET knife_model_id = ?, legacy_master_id = ?, nickname = ?, quantity = ?, acquired_date = ?,
                    mkc_order_number = ?, purchase_price = ?, estimated_value = ?, condition = ?, steel = ?, blade_finish = ?, blade_color = ?,
                    handle_color = ?, blade_length = ?, collaboration_name = ?, serial_number = ?, location = ?,
                    purchase_source = ?, last_sharpened = ?, notes = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    payload.knife_model_id,
                    model_exists.get("legacy_master_id"),
                    payload.nickname,
                    payload.quantity,
                    payload.acquired_date,
                    payload.mkc_order_number,
                    payload.purchase_price,
                    payload.estimated_value,
                    payload.condition,
                    payload.steel,
                    payload.blade_finish,
                    payload.blade_color,
                    payload.handle_color,
                    payload.blade_length,
                    payload.collaboration_name,
                    payload.serial_number,
                    payload.location,
                    payload.purchase_source,
                    payload.last_sharpened,
                    payload.notes,
                    item_id,
                ),
            )
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Inventory item not found.")
            return {"message": "Updated"}


    @router.patch("/api/v2/inventory/{item_id}/quantity")
    def v2_patch_inventory_quantity(item_id: int, delta: int = 1):
        """Increment (or decrement) quantity by delta. Quantity cannot go below 1."""
        with get_conn() as conn:
            row = conn.execute(
                "SELECT quantity FROM inventory_items_v2 WHERE id = ?", (item_id,)
            ).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Inventory item not found.")
            new_qty = max(1, row["quantity"] + delta)
            conn.execute(
                "UPDATE inventory_items_v2 SET quantity = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (new_qty, item_id),
            )
            return {"id": item_id, "quantity": new_qty}

    @router.delete("/api/v2/inventory/{item_id}")
    def v2_delete_inventory_item(item_id: int):
        with get_conn() as conn:
            cur = conn.execute("DELETE FROM inventory_items_v2 WHERE id = ?", (item_id,))
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Inventory item not found.")
            return {"message": "Deleted"}


    @router.post("/api/v2/inventory/{item_id}/duplicate")
    def v2_duplicate_inventory_item(item_id: int):
        with get_conn() as conn:
            row = conn.execute("SELECT * FROM inventory_items_v2 WHERE id = ?", (item_id,)).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Inventory item not found.")
            nick = (row.get("nickname") or "").strip()
            new_nick = f"{nick} (copy)" if nick else "Copy"
            cur = conn.execute(
                """
                INSERT INTO inventory_items_v2
                (legacy_master_id, knife_model_id, nickname, quantity, acquired_date, mkc_order_number, purchase_price, estimated_value,
                 condition, steel, blade_finish, blade_color, handle_color, blade_length, collaboration_name, serial_number,
                 location, purchase_source, last_sharpened, notes, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                (
                    row.get("legacy_master_id"), row["knife_model_id"], new_nick, row["quantity"], row["acquired_date"],
                    row.get("mkc_order_number"),
                    row["purchase_price"], row["estimated_value"], row.get("condition") or "Like New", row["steel"],
                    row["blade_finish"], row["blade_color"], row["handle_color"], row.get("blade_length"),
                    row["collaboration_name"], None, row["location"], row["purchase_source"], row["last_sharpened"], row["notes"],
                ),
            )
            return {"id": cur.lastrowid, "message": "Duplicated"}


    @router.get("/api/v2/export/inventory.csv")
    def v2_export_inventory_csv() -> Response:
        with get_conn() as conn:
            rows = conn.execute(
                """
                SELECT
                    i.id,
                    km.official_name AS knife_name,
                    fam.name AS knife_family,
                    i.knife_model_id,
                    i.nickname,
                    i.quantity,
                    i.acquired_date,
                    i.mkc_order_number,
                    i.purchase_price,
                    i.estimated_value,
                    i.condition,
                    i.handle_color,
                    i.steel AS blade_steel,
                    i.blade_finish,
                    i.blade_color,
                    i.blade_length,
                    (CASE WHEN i.collaboration_name IS NOT NULL AND i.collaboration_name != '' THEN 1 ELSE 0 END) AS is_collab,
                    i.collaboration_name,
                    i.serial_number,
                    i.location,
                    i.purchase_source,
                    i.last_sharpened,
                    i.notes,
                    i.created_at,
                    i.updated_at
                FROM inventory_items_v2 i
                LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
                LEFT JOIN knife_families fam ON fam.id = km.family_id
                ORDER BY km.sortable_name COLLATE NOCASE, i.id DESC
                """
            ).fetchall()
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=inventory_csv_columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            flat: dict[str, Any] = {}
            for key in inventory_csv_columns:
                val = row.get(key)
                if key == "is_collab":
                    flat[key] = "1" if val else "0"
                elif val is None:
                    flat[key] = ""
                else:
                    flat[key] = val
            writer.writerow(flat)
        return Response(
            content=buffer.getvalue().encode("utf-8"),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": 'attachment; filename="mkc_inventory_v2.csv"'},
        )


    @router.get("/api/v2/export/catalog.csv")
    def v2_export_catalog_csv() -> Response:
        with get_conn() as conn:
            csv_data = normalized_model.export_models_csv(conn)
        return Response(
            content=csv_data.encode("utf-8"),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": 'attachment; filename="knife_models_v2.csv"'},
        )


    @router.post("/api/v2/import/models.csv")
    async def v2_import_models_csv(file: UploadFile = File(...)) -> dict[str, Any]:
        raw = await file.read()
        try:
            text = raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            raise HTTPException(status_code=400, detail="File must be UTF-8 encoded CSV.")
        reader = csv.DictReader(io.StringIO(text))
        if not reader.fieldnames:
            raise HTTPException(status_code=400, detail="CSV must include headers.")
        inserted = 0
        updated = 0
        with get_conn() as conn:
            for src in reader:
                row = {(k or "").strip(): ("" if v is None else str(v).strip()) for k, v in src.items()}
                official_name = row.get("official_name") or row.get("name") or ""
                if not official_name:
                    continue
                payload = V2ModelIn(
                    official_name=official_name,
                    normalized_name=row.get("normalized_name") or official_name,
                    knife_type=row.get("knife_type") or None,
                    family_name=row.get("family_name") or row.get("family") or None,
                    form_name=row.get("form_name") or None,
                    series_name=row.get("series_name") or row.get("catalog_line") or None,
                    collaborator_name=row.get("collaborator_name") or row.get("collaboration_name") or None,
                    generation_label=row.get("generation_label") or row.get("version") or None,
                    steel=row.get("steel") or row.get("default_steel") or None,
                    blade_finish=row.get("blade_finish") or row.get("default_blade_finish") or None,
                    blade_color=row.get("blade_color") or row.get("default_blade_color") or None,
                    handle_color=row.get("handle_color") or row.get("default_handle_color") or None,
                    blade_length=(float(row["blade_length"]) if row.get("blade_length") else None),
                    record_status=row.get("record_status") or row.get("status") or "active",
                    is_current_catalog=(row.get("is_current_catalog", "1").lower() in {"1", "true", "yes", "y"}),
                    is_discontinued=(row.get("is_discontinued", "0").lower() in {"1", "true", "yes", "y"}),
                    msrp=(float(row["msrp"]) if row.get("msrp") else None),
                    official_product_url=row.get("official_product_url") or row.get("default_product_url") or None,
                    official_image_url=row.get("official_image_url") or row.get("primary_image_url") or None,
                    notes=row.get("notes") or None,
                    distinguishing_features=row.get("distinguishing_features") or row.get("identifier_distinguishing_features") or None,
                )
                existing = conn.execute(
                    "SELECT id FROM knife_models_v2 WHERE official_name = ?",
                    (payload.official_name,),
                ).fetchone()
                type_id = _v2_dim_id(conn, "knife_types", payload.knife_type)
                form_id = _v2_dim_id(conn, "knife_forms", payload.form_name)
                family_id = _v2_dim_id(conn, "knife_families", payload.family_name)
                series_id = _v2_dim_id(conn, "knife_series", payload.series_name)
                collaborator_id = _v2_dim_id(conn, "collaborators", payload.collaborator_name)
                normalized_name = (payload.normalized_name or payload.official_name).strip()
                if existing:
                    slug = _v2_model_slug(conn, normalized_name, existing_id=existing["id"])
                    conn.execute(
                        """
                        UPDATE knife_models_v2
                        SET official_name = ?, normalized_name = ?, sortable_name = ?, slug = ?,
                            type_id = ?, form_id = ?, family_id = ?, series_id = ?, collaborator_id = ?,
                            generation_label = ?, steel = ?, blade_finish = ?, blade_color = ?, handle_color = ?,
                            blade_length = ?, record_status = ?, is_current_catalog = ?, is_discontinued = ?,
                            msrp = ?, official_product_url = ?, official_image_url = ?, notes = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (
                            payload.official_name.strip(), normalized_name, normalized_name, slug,
                            type_id, form_id, family_id, series_id, collaborator_id,
                            payload.generation_label, payload.steel, payload.blade_finish, payload.blade_color, payload.handle_color,
                            payload.blade_length, payload.record_status or "active",
                            0 if payload.is_current_catalog is False else 1, 1 if payload.is_discontinued else 0,
                            payload.msrp, payload.official_product_url, payload.official_image_url, payload.notes, existing["id"],
                        ),
                    )
                    if payload.distinguishing_features is not None:
                        conn.execute(
                            """
                            INSERT INTO knife_model_descriptors (knife_model_id, distinguishing_features, updated_at)
                            VALUES (?, ?, CURRENT_TIMESTAMP)
                            ON CONFLICT(knife_model_id) DO UPDATE SET
                                distinguishing_features=excluded.distinguishing_features, updated_at=CURRENT_TIMESTAMP
                            """,
                            (existing["id"], payload.distinguishing_features),
                        )
                    updated += 1
                else:
                    slug = _v2_model_slug(conn, normalized_name)
                    conn.execute(
                        """
                        INSERT INTO knife_models_v2 (
                            official_name, normalized_name, sortable_name, slug, type_id, form_id, family_id, series_id, collaborator_id,
                            generation_label, steel, blade_finish, blade_color, handle_color, blade_length, record_status,
                            is_current_catalog, is_discontinued, msrp, official_product_url, official_image_url, notes
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            payload.official_name.strip(), normalized_name, normalized_name, slug,
                            type_id, form_id, family_id, series_id, collaborator_id,
                            payload.generation_label, payload.steel, payload.blade_finish, payload.blade_color, payload.handle_color,
                            payload.blade_length, payload.record_status or "active",
                            0 if payload.is_current_catalog is False else 1, 1 if payload.is_discontinued else 0,
                            payload.msrp, payload.official_product_url, payload.official_image_url, payload.notes,
                        ),
                    )
                    new_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
                    if payload.distinguishing_features is not None:
                        conn.execute(
                            """
                            INSERT INTO knife_model_descriptors (knife_model_id, distinguishing_features, updated_at)
                            VALUES (?, ?, CURRENT_TIMESTAMP)
                            ON CONFLICT(knife_model_id) DO UPDATE SET
                                distinguishing_features=excluded.distinguishing_features, updated_at=CURRENT_TIMESTAMP
                            """,
                            (new_id, payload.distinguishing_features),
                        )
                    inserted += 1
        return {"inserted": inserted, "updated": updated, "message": "Import complete."}


    @router.post("/api/v2/models/backfill-identity")
    def v2_backfill_identity() -> dict[str, Any]:
        with get_conn() as conn:
            media = migrate_legacy_media_to_v2(conn)
            summary = backfill_v2_model_identity(conn)
            extra = normalize_v2_additional_fields(conn)
            return {"ok": True, "media_migration": media, **summary, "field_normalization": extra}


    @router.post("/api/v2/models/migrate-legacy-media")
    def v2_migrate_legacy_media() -> dict[str, Any]:
        with get_conn() as conn:
            summary = migrate_legacy_media_to_v2(conn)
            return {"ok": True, **summary}


    @router.get("/api/v2/models/qa/identity")
    def v2_identity_qa() -> dict[str, Any]:
        with get_conn() as conn:
            rows = conn.execute(
                """
                SELECT km.id, km.official_name, km.generation_label, km.size_modifier, km.platform_variant,
                       kt.name AS knife_type, frm.name AS form_name, fam.name AS family_name,
                       ks.name AS series_name, c.name AS collaborator_name
                FROM knife_models_v2 km
                LEFT JOIN knife_types kt ON kt.id = km.type_id
                LEFT JOIN knife_forms frm ON frm.id = km.form_id
                LEFT JOIN knife_families fam ON fam.id = km.family_id
                LEFT JOIN knife_series ks ON ks.id = km.series_id
                LEFT JOIN collaborators c ON c.id = km.collaborator_id
                ORDER BY km.sortable_name COLLATE NOCASE, km.id
                """
            ).fetchall()
        collab_like_series = {"archery country", "bearded butchers", "meat church", "nock on"}
        generic_family_labels = {
            "hunting", "culinary", "tactical", "everyday carry", "edc", "bushcraft",
            "bushcraft & camp", "camp", "utility", "work", "kitchen", "fillet", "skinner",
        }
        incomplete = []
        warnings = []
        family_warnings = []
        for row in rows:
            missing = []
            for field in ("official_name", "knife_type", "form_name", "family_name"):
                if not (row.get(field) or "").strip():
                    missing.append(field)
            if missing:
                incomplete.append(
                    {
                        "id": row["id"],
                        "official_name": row.get("official_name"),
                        "missing_fields": missing,
                    }
                )
            series_name = (row.get("series_name") or "").strip()
            collaborator_name = (row.get("collaborator_name") or "").strip()
            if series_name.lower() in collab_like_series and not collaborator_name:
                warnings.append(
                    {
                        "id": row["id"],
                        "official_name": row.get("official_name"),
                        "warning": "Series suggests collaboration but collaborator_name is empty",
                    }
                )
            family_name = (row.get("family_name") or "").strip()
            if family_name and ("/" in family_name or family_name.lower() in generic_family_labels or family_name == family_name.lower()):
                family_warnings.append(
                    {
                        "id": row["id"],
                        "official_name": row.get("official_name"),
                        "family_name": family_name,
                        "warning": "Family label appears generic or malformed",
                    }
                )
        total = len(rows)
        complete = total - len(incomplete)
        return {
            "total_models": total,
            "complete_identity_models": complete,
            "incomplete_identity_models": len(incomplete),
            "completeness_pct": round((complete / total) * 100, 2) if total else 100.0,
            "incomplete_rows": incomplete,
            "warnings": warnings,
            "family_warnings": family_warnings,
        }


    @router.post("/api/v2/identify")
    def v2_identify_knives(payload: identifier_query_model):
        """Rank v2 catalog models only and return canonical v2 model IDs."""
        with get_conn() as conn:
            rows = conn.execute(
                """
                SELECT km.id, km.official_name, km.normalized_name, km.record_status, km.blade_length, km.steel,
                       km.blade_finish, km.blade_color, km.generation_label, km.notes,
                       kt.name AS knife_type, fam.name AS family_name, frm.name AS form_name, ks.name AS series_name,
                       c.name AS collaborator_name,
                       d.distinguishing_features,
                       (CASE WHEN kmi.image_blob IS NOT NULL AND length(kmi.image_blob) > 0 THEN 1 ELSE 0 END) AS has_identifier_image
                FROM knife_models_v2 km
                LEFT JOIN knife_types kt ON kt.id = km.type_id
                LEFT JOIN knife_forms frm ON frm.id = km.form_id
                LEFT JOIN knife_families fam ON fam.id = km.family_id
                LEFT JOIN knife_series ks ON ks.id = km.series_id
                LEFT JOIN collaborators c ON c.id = km.collaborator_id
                LEFT JOIN knife_model_descriptors d ON d.knife_model_id = km.id
                LEFT JOIN knife_model_images kmi ON kmi.knife_model_id = km.id
                ORDER BY km.sortable_name COLLATE NOCASE
                """
            ).fetchall()
        tokens = [token.strip().lower() for token in (payload.q or "").replace("/", " ").replace(",", " ").split() if token.strip()]
        results: list[dict[str, Any]] = []
        for row in rows:
            score = 0.0
            reasons: list[str] = []
            if payload.family:
                fam_hay = " ".join(
                    str(row.get(k) or "")
                    for k in ("family_name", "knife_type", "official_name", "normalized_name", "form_name")
                ).lower()
                if payload.family.lower() in fam_hay:
                    score += 22
                    reasons.append(f"use / category matches {payload.family}")
            if payload.steel and row.get("steel") and payload.steel.lower() == str(row["steel"]).lower():
                score += 12
                reasons.append(f"steel matches {row['steel']}")
            if payload.finish and row.get("blade_finish") and payload.finish.lower() == str(row["blade_finish"]).lower():
                score += 12
                reasons.append(f"finish matches {row['blade_finish']}")
            if payload.blade_color and row.get("blade_color") and payload.blade_color.lower() == str(row["blade_color"]).lower():
                score += 10
                reasons.append(f"blade color matches {row['blade_color']}")
            if payload.blade_length is not None and row.get("blade_length") is not None:
                diff = abs(float(row["blade_length"]) - payload.blade_length)
                if diff <= 0.2:
                    score += 18
                elif diff <= 0.5:
                    score += 10
                elif diff <= 1.0:
                    score += 3
            haystack = " ".join(str(v) for v in row.values() if v is not None and not isinstance(v, bytes)).lower()
            token_hits = [token for token in tokens if token in haystack]
            if token_hits:
                score += len(token_hits) * 8
                reasons.append("keyword matches: " + ", ".join(token_hits[:4]))
            if score > 0:
                results.append(
                    {
                        "id": row["id"],
                        "name": row["official_name"],
                        "family": row.get("family_name"),
                        "category": row.get("knife_type"),
                        "catalog_line": row.get("series_name"),
                        "record_type": row.get("generation_label"),
                        "catalog_status": row.get("record_status"),
                        "identifier_product_url": None,
                        "has_identifier_image": bool(row.get("has_identifier_image")),
                        "has_silhouette_hint": False,
                        "catalog_blurb": row.get("notes"),
                        "default_blade_length": row.get("blade_length"),
                        "default_steel": row.get("steel"),
                        "default_blade_finish": row.get("blade_finish"),
                        "default_blade_color": row.get("blade_color"),
                        "is_collab": bool(row.get("collaborator_name")),
                        "collaboration_name": row.get("collaborator_name"),
                        "list_status": row.get("record_status") or "active",
                        "score": round(score, 1),
                        "reasons": reasons[:5],
                    }
                )
        results.sort(key=lambda item: (-item["score"], item["name"].lower()))
        return {"source": "v2_catalog", "results": results[:10]}

    # Fix annotation resolution: `from __future__ import annotations` converts the
    # closure-scoped `identifier_query_model` to a string at runtime, so FastAPI
    # cannot resolve it as a Pydantic body model.  Overwrite with the real class.
    v2_identify_knives.__annotations__["payload"] = identifier_query_model

    return router, v2_identify_knives
