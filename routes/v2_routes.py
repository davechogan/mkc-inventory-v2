"""V2 catalog/inventory API routes (mounted from app)."""
from __future__ import annotations

import base64
import csv
import io
import json
import sqlite3
from collections.abc import Callable
from typing import Any, Optional, Type

import blade_ai
import normalized_model
from fastapi import APIRouter, Body, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import Response
from pydantic import BaseModel, Field, field_validator

from reporting.domain import GetConn


class InventoryItemV2In(BaseModel):
    """Request body for POST/PUT /api/v2/inventory (module-level so FastAPI/Pydantic resolve the model reliably)."""

    knife_model_id: int
    colorway_id: Optional[int] = None
    quantity: int = 1
    purchase_price: Optional[float] = None
    acquired_date: Optional[str] = None
    mkc_order_number: Optional[str] = None
    location_id: Optional[int] = None
    notes: Optional[str] = None


def create_v2_router(
    *,
    get_conn: GetConn,
    ollama_vision_model: str,
    inventory_csv_columns: list[str],
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
                i.colorway_id,
                i.quantity,
                i.acquired_date,
                i.mkc_order_number,
                i.purchase_price,
                i.notes,
                c.name AS collaborator_name,
                bs.name AS blade_steel,
                bf.name AS blade_finish,
                ht.name AS handle_type,
                hc.name AS handle_color,
                bc.name AS blade_color,
                km.blade_length,
                loc.name AS location,
                km.official_name AS knife_name,
                fam.name AS knife_family,
                COALESCE(ks.name, c.name) AS catalog_line,
                kt.name AS knife_type,
                frm.name AS form_name,
                ks.name AS series_name,
                (c.name IS NOT NULL) AS is_collab,
                (CASE WHEN kmi.image_blob IS NOT NULL AND length(kmi.image_blob) > 0
                  THEN 1 ELSE 0 END) AS has_identifier_image,
                CASE WHEN mc.id IS NOT NULL AND mc.image_blob IS NOT NULL
                  THEN '/api/v2/colorway-images/' || mc.id
                  ELSE (SELECT '/api/v2/colorway-images/' || mc2.id FROM model_colorways mc2
                        WHERE mc2.knife_model_id = km.id AND mc2.image_blob IS NOT NULL
                        LIMIT 1)
                END AS colorway_image_url
            FROM inventory_items_v2 i
            LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
            LEFT JOIN knife_types kt ON kt.id = km.type_id
            LEFT JOIN knife_forms frm ON frm.id = km.form_id
            LEFT JOIN knife_families fam ON fam.id = km.family_id
            LEFT JOIN knife_series ks ON ks.id = km.series_id
            LEFT JOIN collaborators c ON c.id = km.collaborator_id
            LEFT JOIN knife_model_images kmi ON kmi.knife_model_id = i.knife_model_id
            LEFT JOIN blade_steels bs ON bs.id = km.steel_id
            LEFT JOIN blade_finishes bf ON bf.id = km.blade_finish_id
            LEFT JOIN handle_types ht ON ht.id = km.handle_type_id
            LEFT JOIN model_colorways mc ON mc.id = i.colorway_id
            LEFT JOIN handle_colors hc ON hc.id = mc.handle_color_id
            LEFT JOIN blade_colors bc ON bc.id = mc.blade_color_id
            LEFT JOIN locations loc ON loc.id = i.location_id
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
                    "(km.official_name LIKE ? OR fam.name LIKE ? OR i.notes LIKE ?)"
                )
                params.extend([q, q, q])

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
                conditions.append("bs.name = ?")
                params.append(steel.strip())

            if finish and finish.strip():
                conditions.append("bf.name = ?")
                params.append(finish.strip())

            if handle_color and handle_color.strip():
                conditions.append("hc.name = ?")
                params.append(handle_color.strip())

            if location and location.strip():
                conditions.append("loc.name LIKE ?")
                params.append(f"%{location.strip()}%")

            where_sql = " AND ".join(conditions) if conditions else "1=1"
            sql = f"{base} WHERE {where_sql} ORDER BY fam.name COLLATE NOCASE, km.sortable_name COLLATE NOCASE, i.id DESC"
            rows = conn.execute(sql, params).fetchall()
            return rows


    @router.get("/api/v2/inventory/summary")
    def v2_inventory_summary() -> dict[str, Any]:
        """Return inventory summary: rows, total quantity, spend, master count, by_family."""
        with get_conn() as conn:
            summary = conn.execute(
                """
                SELECT
                    COUNT(*) AS inventory_rows,
                    COALESCE(SUM(i.quantity), 0) AS total_quantity,
                    COALESCE(SUM(COALESCE(i.purchase_price, 0) * i.quantity), 0) AS total_spend
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
                """SELECT DISTINCT bs.name AS v FROM inventory_items_v2 i
                   LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
                   LEFT JOIN blade_steels bs ON bs.id = km.steel_id
                   WHERE bs.name IS NOT NULL AND bs.name != ''
                   ORDER BY v"""
            ).fetchall()
            finish_vals = conn.execute(
                """SELECT DISTINCT bf.name AS v FROM inventory_items_v2 i
                   LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
                   LEFT JOIN blade_finishes bf ON bf.id = km.blade_finish_id
                   WHERE bf.name IS NOT NULL
                   ORDER BY v"""
            ).fetchall()
            handle_vals = conn.execute(
                """SELECT DISTINCT hc.name AS v FROM inventory_items_v2 i
                   LEFT JOIN model_colorways mc ON mc.id = i.colorway_id
                   LEFT JOIN handle_colors hc ON hc.id = mc.handle_color_id
                   WHERE hc.name IS NOT NULL
                   ORDER BY v"""
            ).fetchall()
            blade_color_vals = conn.execute(
                """SELECT DISTINCT bc.name AS v FROM inventory_items_v2 i
                   LEFT JOIN model_colorways mc ON mc.id = i.colorway_id
                   LEFT JOIN blade_colors bc ON bc.id = mc.blade_color_id
                   WHERE bc.name IS NOT NULL
                   ORDER BY v"""
            ).fetchall()
            loc_vals = conn.execute(
                """SELECT DISTINCT loc.name AS v FROM inventory_items_v2 i
                   LEFT JOIN locations loc ON loc.id = i.location_id
                   WHERE loc.name IS NOT NULL AND loc.name != ''
                   ORDER BY v"""
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
                "location": pluck(loc_vals, "v"),
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
                    "(km.official_name LIKE ? OR fam.name LIKE ? OR km.slug LIKE ?)"
                )
                params.extend([q, q, q])

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
                SELECT km.id, km.parent_model_id, km.official_name, km.sortable_name, km.slug,
                       kt.name AS knife_type, fam.name AS family_name, frm.name AS form_name,
                       ks.name AS series_name, c.name AS collaborator_name,
                       bs.name AS blade_steel, bf.name AS blade_finish, ht.name AS handle_type,
                       km.blade_length, km.msrp,
                       km.official_product_url, km.model_notes,
                       (SELECT COUNT(*) FROM inventory_items_v2 WHERE knife_model_id = km.id) AS in_inventory_count,
                       (CASE WHEN kmi.image_blob IS NOT NULL AND length(kmi.image_blob) > 0 THEN 1 ELSE 0 END) AS has_identifier_image,
                       (SELECT '/api/v2/colorway-images/' || mc.id FROM model_colorways mc
                        JOIN handle_colors hc ON hc.id = mc.handle_color_id
                        WHERE mc.knife_model_id = km.id AND mc.image_blob IS NOT NULL
                        ORDER BY CASE WHEN LOWER(hc.name) = 'orange/black' THEN 0 ELSE 1 END
                        LIMIT 1) AS colorway_image_url
                FROM knife_models_v2 km
                LEFT JOIN knife_model_images kmi ON kmi.knife_model_id = km.id
                LEFT JOIN knife_types kt ON kt.id = km.type_id
                LEFT JOIN knife_forms frm ON frm.id = km.form_id
                LEFT JOIN knife_families fam ON fam.id = km.family_id
                LEFT JOIN knife_series ks ON ks.id = km.series_id
                LEFT JOIN collaborators c ON c.id = km.collaborator_id
                LEFT JOIN blade_steels bs ON bs.id = km.steel_id
                LEFT JOIN blade_finishes bf ON bf.id = km.blade_finish_id
                LEFT JOIN handle_types ht ON ht.id = km.handle_type_id
                WHERE {where_sql}
                ORDER BY fam.name COLLATE NOCASE, km.sortable_name COLLATE NOCASE
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


    @router.get("/api/v2/me")
    def v2_me(request: Request):
        """Return the current authenticated user's profile, or null if unauthenticated."""
        from auth import get_current_user
        user = get_current_user(request)
        if not user:
            return {"authenticated": False, "user": None}
        return {
            "authenticated": True,
            "user": {
                "id": user.id,
                "email": user.email,
                "name": user.name,
                "tenant_id": user.tenant_id,
                "role": user.role,
            },
        }

    @router.get("/api/v2/users")
    def v2_list_users():
        """Return all users with access history. Admin-only in future RBAC."""
        with get_conn() as conn:
            rows = conn.execute(
                "SELECT id, email, name, tenant_id, role, first_seen, last_seen FROM users ORDER BY last_seen DESC"
            ).fetchall()
            return [dict(r) for r in rows]

    @router.get("/api/v2/colors")
    def v2_colors():
        """Return handle and blade colors from the normalized lookup tables."""
        with get_conn() as conn:
            handle = conn.execute(
                "SELECT id, name FROM handle_colors ORDER BY name COLLATE NOCASE"
            ).fetchall()
            blade = conn.execute(
                "SELECT id, name FROM blade_colors ORDER BY name COLLATE NOCASE"
            ).fetchall()
            return {
                "handle_colors": [{"id": r["id"], "name": r["name"]} for r in handle],
                "blade_colors": [{"id": r["id"], "name": r["name"]} for r in blade],
            }


    @router.get("/api/v2/models/search")
    def v2_models_search(q: Optional[str] = None, limit: int = 50) -> list[dict[str, Any]]:
        """Search knife_models_v2 for model picker. Returns flattened rows."""
        with get_conn() as conn:
            params: list[Any] = []
            where = "1=1"
            if q and q.strip():
                search_term = f"%{q.strip()}%"
                where = "(km.official_name LIKE ? OR fam.name LIKE ? OR km.slug LIKE ?)"
                params = [search_term, search_term, search_term]
            params.append(limit)
            rows = conn.execute(
                f"""
                SELECT km.id, km.official_name, km.slug,
                       kt.name AS knife_type, fam.name AS family_name, frm.name AS form_name,
                       ks.name AS series_name, c.name AS collaborator_name,
                       bs.name AS blade_steel, bf.name AS blade_finish, ht.name AS handle_type,
                       km.blade_length
                FROM knife_models_v2 km
                LEFT JOIN knife_types kt ON kt.id = km.type_id
                LEFT JOIN knife_forms frm ON frm.id = km.form_id
                LEFT JOIN knife_families fam ON fam.id = km.family_id
                LEFT JOIN knife_series ks ON ks.id = km.series_id
                LEFT JOIN collaborators c ON c.id = km.collaborator_id
                LEFT JOIN blade_steels bs ON bs.id = km.steel_id
                LEFT JOIN blade_finishes bf ON bf.id = km.blade_finish_id
                LEFT JOIN handle_types ht ON ht.id = km.handle_type_id
                WHERE {where}
                ORDER BY fam.name COLLATE NOCASE, km.sortable_name COLLATE NOCASE
                LIMIT ?
                """,
                params,
            ).fetchall()
            return rows


    class V2ModelIn(BaseModel):
        model_config = {"extra": "ignore"}

        official_name: str = Field(min_length=1, max_length=200)
        knife_type: Optional[str] = None
        form_name: Optional[str] = None
        family_name: Optional[str] = None
        series_name: Optional[str] = None
        collaborator_name: Optional[str] = None
        steel: Optional[str] = None
        blade_finish: Optional[str] = None
        handle_type: Optional[str] = None
        blade_length: Optional[float] = None
        msrp: Optional[float] = None
        official_product_url: Optional[str] = None
        model_notes: Optional[str] = None
        parent_model_id: Optional[int] = None

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


    def _v2_attr_id(conn: sqlite3.Connection, table: str, name: Optional[str]) -> Optional[int]:
        """Resolve a text attribute name to its FK id in a lookup table, auto-inserting if needed."""
        if not name or not str(name).strip():
            return None
        n = str(name).strip()
        row = conn.execute(f"SELECT id FROM {table} WHERE name = ?", (n,)).fetchone()
        if row:
            return row["id"]
        cur = conn.execute(f"INSERT INTO {table} (name) VALUES (?)", (n,))
        return cur.lastrowid


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
                SELECT km.id, km.parent_model_id, km.official_name, km.sortable_name, km.slug,
                       kt.name AS knife_type, fam.name AS family_name, frm.name AS form_name,
                       ks.name AS series_name, c.name AS collaborator_name,
                       bs.name AS blade_steel, bf.name AS blade_finish, ht.name AS handle_type,
                       km.blade_length, km.msrp, km.official_product_url, km.model_notes,
                       d.distinguishing_features,
                       (CASE WHEN kmi.image_blob IS NOT NULL AND length(kmi.image_blob) > 0 THEN 1 ELSE 0 END) AS has_identifier_image
                FROM knife_models_v2 km
                LEFT JOIN knife_types kt ON kt.id = km.type_id
                LEFT JOIN knife_forms frm ON frm.id = km.form_id
                LEFT JOIN knife_families fam ON fam.id = km.family_id
                LEFT JOIN knife_series ks ON ks.id = km.series_id
                LEFT JOIN collaborators c ON c.id = km.collaborator_id
                LEFT JOIN blade_steels bs ON bs.id = km.steel_id
                LEFT JOIN blade_finishes bf ON bf.id = km.blade_finish_id
                LEFT JOIN handle_types ht ON ht.id = km.handle_type_id
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
    def v2_create_model(payload: dict = Body(...)):
        payload = V2ModelIn(**payload)
        _require_v2_identity(payload)
        with get_conn() as conn:
            _validate_v2_controlled_identity(payload, conn)
            type_id = _v2_dim_id(conn, "knife_types", payload.knife_type)
            form_id = _v2_dim_id(conn, "knife_forms", payload.form_name)
            family_id = _v2_dim_id(conn, "knife_families", payload.family_name)
            series_id = _v2_dim_id(conn, "knife_series", payload.series_name)
            collaborator_id = _v2_dim_id(conn, "collaborators", payload.collaborator_name)
            steel_id = _v2_attr_id(conn, "blade_steels", payload.steel)
            blade_finish_id = _v2_attr_id(conn, "blade_finishes", payload.blade_finish)
            handle_type_id = _v2_attr_id(conn, "handle_types", payload.handle_type)
            sortable_name = payload.official_name.strip()
            slug = _v2_model_slug(conn, payload.official_name.strip())
            cur = conn.execute(
                """
                INSERT INTO knife_models_v2 (
                    official_name, sortable_name, slug, type_id, form_id, family_id, series_id, collaborator_id,
                    parent_model_id, steel_id, blade_finish_id, handle_type_id,
                    blade_length, msrp, official_product_url, model_notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload.official_name.strip(), sortable_name, slug, type_id, form_id, family_id, series_id, collaborator_id,
                    payload.parent_model_id, steel_id, blade_finish_id, handle_type_id,
                    payload.blade_length, payload.msrp, payload.official_product_url, payload.model_notes,
                ),
            )
            return {"id": cur.lastrowid, "message": "Created"}


    @router.put("/api/v2/models/{model_id}")
    def v2_update_model(model_id: int, payload: dict = Body(...)):
        payload = V2ModelIn(**payload)
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
            steel_id = _v2_attr_id(conn, "blade_steels", payload.steel)
            blade_finish_id = _v2_attr_id(conn, "blade_finishes", payload.blade_finish)
            handle_type_id = _v2_attr_id(conn, "handle_types", payload.handle_type)
            slug = _v2_model_slug(conn, payload.official_name.strip(), existing_id=model_id)
            conn.execute(
                """
                UPDATE knife_models_v2
                SET official_name = ?, sortable_name = ?, slug = ?,
                    type_id = ?, form_id = ?, family_id = ?, series_id = ?, collaborator_id = ?, parent_model_id = ?,
                    steel_id = ?, blade_finish_id = ?, handle_type_id = ?,
                    blade_length = ?, msrp = ?, official_product_url = ?, model_notes = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    payload.official_name.strip(), payload.official_name.strip(), slug,
                    type_id, form_id, family_id, series_id, collaborator_id, payload.parent_model_id,
                    steel_id, blade_finish_id, handle_type_id,
                    payload.blade_length, payload.msrp, payload.official_product_url, payload.model_notes, model_id,
                ),
            )
            return {"message": "Updated"}


    @router.delete("/api/v2/models/{model_id}")
    def v2_delete_model(model_id: int):
        with get_conn() as conn:
            used = conn.execute("SELECT COUNT(*) AS c FROM inventory_items_v2 WHERE knife_model_id = ?", (model_id,)).fetchone()["c"]
            if used > 0:
                raise HTTPException(status_code=400, detail="Cannot delete model used by inventory.")
            # Clean up dependent rows first
            conn.execute("DELETE FROM model_colorways WHERE knife_model_id = ?", (model_id,))
            conn.execute("DELETE FROM knife_model_images WHERE knife_model_id = ?", (model_id,))
            conn.execute("DELETE FROM knife_model_descriptors WHERE knife_model_id = ?", (model_id,))
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
                    official_name, sortable_name, slug, type_id, form_id, family_id, series_id,
                    collaborator_id, parent_model_id, steel_id, blade_finish_id, handle_type_id,
                    blade_length, msrp, official_product_url, model_notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    new_name, row.get("sortable_name") or new_name, slug,
                    row.get("type_id"), row.get("form_id"), row.get("family_id"), row.get("series_id"),
                    row.get("collaborator_id"), row.get("parent_model_id"),
                    row.get("steel_id"), row.get("blade_finish_id"), row.get("handle_type_id"),
                    row.get("blade_length"), row.get("msrp"),
                    row.get("official_product_url"), row.get("model_notes"),
                ),
            )
            return {"id": cur.lastrowid, "message": "Duplicated"}


    @router.get("/api/v2/models/{model_id}/image")
    def v2_get_model_image(model_id: int):
        with get_conn() as conn:
            row = conn.execute(
                "SELECT image_blob, image_mime, updated_at FROM knife_model_images WHERE knife_model_id = ?",
                (model_id,),
            ).fetchone()
            if not row or not row["image_blob"]:
                raise HTTPException(status_code=404, detail="No stored reference image for this model.")
            import hashlib
            etag = hashlib.md5(f"model:{model_id}:{row['updated_at']}".encode()).hexdigest()
            return Response(
                content=row["image_blob"],
                media_type=(row["image_mime"] or "image/jpeg"),
                headers={
                    "Cache-Control": "public, max-age=604800",
                    "ETag": f'"{etag}"',
                },
            )


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


    # ── model_colorways endpoints ──────────────────────────────────────

    @router.get("/api/v2/colorway-images/{colorway_id}")
    def v2_get_colorway_image(colorway_id: int, request: Request):
        """Serve the PNG blob for a colorway with cache headers + ETag."""
        with get_conn() as conn:
            row = conn.execute(
                "SELECT image_blob, updated_at FROM model_colorways WHERE id = ?",
                (colorway_id,),
            ).fetchone()
            if not row or not row["image_blob"]:
                raise HTTPException(status_code=404, detail="No image for this colorway.")
            import hashlib
            etag = hashlib.md5(f"{colorway_id}:{row['updated_at']}".encode()).hexdigest()
            # Return 304 if browser has the same version cached
            if request.headers.get("if-none-match") == f'"{etag}"':
                return Response(status_code=304, headers={"ETag": f'"{etag}"', "Cache-Control": "public, max-age=604800"})
            return Response(
                content=row["image_blob"],
                media_type="image/png",
                headers={
                    "Cache-Control": "public, max-age=604800",
                    "ETag": f'"{etag}"',
                },
            )

    @router.get("/api/v2/models/{model_id}/colorways")
    def v2_list_colorways(model_id: int):
        """List all colorways for a model with color names and image status."""
        with get_conn() as conn:
            if not conn.execute(
                "SELECT 1 FROM knife_models_v2 WHERE id = ?", (model_id,)
            ).fetchone():
                raise HTTPException(status_code=404, detail="Model not found.")
            rows = conn.execute(
                """SELECT mc.id, mc.handle_color_id, hc.name AS handle_color,
                          mc.blade_color_id, bc.name AS blade_color,
                          mc.image_blob IS NOT NULL AS has_image,
                          mc.is_transparent
                   FROM model_colorways mc
                   JOIN handle_colors hc ON mc.handle_color_id = hc.id
                   LEFT JOIN blade_colors bc ON mc.blade_color_id = bc.id
                   WHERE mc.knife_model_id = ?
                   ORDER BY hc.name COLLATE NOCASE, bc.name COLLATE NOCASE""",
                (model_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    @router.post("/api/v2/models/{model_id}/colorways")
    def v2_add_colorway(model_id: int, payload: dict = Body(...)):
        """Add a colorway row (no image yet). Expects handle_color_id, optional blade_color_id."""
        handle_color_id = payload.get("handle_color_id")
        blade_color_id = payload.get("blade_color_id") or None
        if not handle_color_id:
            raise HTTPException(status_code=400, detail="handle_color_id is required.")
        with get_conn() as conn:
            if not conn.execute(
                "SELECT 1 FROM knife_models_v2 WHERE id = ?", (model_id,)
            ).fetchone():
                raise HTTPException(status_code=404, detail="Model not found.")
            if not conn.execute(
                "SELECT 1 FROM handle_colors WHERE id = ?", (handle_color_id,)
            ).fetchone():
                raise HTTPException(status_code=400, detail="Invalid handle_color_id.")
            if blade_color_id and not conn.execute(
                "SELECT 1 FROM blade_colors WHERE id = ?", (blade_color_id,)
            ).fetchone():
                raise HTTPException(status_code=400, detail="Invalid blade_color_id.")
            try:
                cur = conn.execute(
                    """INSERT INTO model_colorways (knife_model_id, handle_color_id, blade_color_id)
                       VALUES (?, ?, ?)""",
                    (model_id, handle_color_id, blade_color_id),
                )
            except sqlite3.IntegrityError:
                raise HTTPException(status_code=409, detail="This colorway already exists for the model.")
            return {"id": cur.lastrowid, "message": "Colorway added."}

    @router.put("/api/v2/models/{model_id}/colorways/{colorway_id}/image")
    async def v2_upload_colorway_image(
        model_id: int,
        colorway_id: int,
        file: UploadFile = File(...),
    ):
        """Upload a PNG blob for an existing colorway."""
        if not (file.content_type or "").startswith("image/png") and not (file.filename or "").lower().endswith(".png"):
            raise HTTPException(status_code=400, detail="File must be a PNG.")
        content = await file.read()
        if len(content) > 10 * 1024 * 1024:
            raise HTTPException(status_code=413, detail="Image larger than 10 MB.")
        with get_conn() as conn:
            row = conn.execute(
                "SELECT id FROM model_colorways WHERE id = ? AND knife_model_id = ?",
                (colorway_id, model_id),
            ).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Colorway not found for this model.")
            conn.execute(
                """UPDATE model_colorways
                   SET image_blob = ?, is_transparent = 1, updated_at = CURRENT_TIMESTAMP
                   WHERE id = ?""",
                (content, colorway_id),
            )
        return {"message": "Image uploaded."}

    @router.delete("/api/v2/models/{model_id}/colorways/{colorway_id}")
    def v2_delete_colorway(model_id: int, colorway_id: int):
        """Remove a colorway and its image."""
        with get_conn() as conn:
            cur = conn.execute(
                "DELETE FROM model_colorways WHERE id = ? AND knife_model_id = ?",
                (colorway_id, model_id),
            )
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Colorway not found for this model.")
        return {"message": "Colorway deleted."}

    @router.get("/api/v2/colorway-audit")
    def v2_colorway_audit():
        """Per-model colorway image completeness for the admin audit view."""
        with get_conn() as conn:
            rows = conn.execute(
                """SELECT km.id, km.official_name,
                          COUNT(mc.id) AS total_colorways,
                          SUM(CASE WHEN mc.image_blob IS NOT NULL THEN 1 ELSE 0 END) AS with_image
                   FROM knife_models_v2 km
                   LEFT JOIN model_colorways mc ON mc.knife_model_id = km.id
                   GROUP BY km.id
                   ORDER BY with_image ASC, total_colorways DESC, km.official_name COLLATE NOCASE"""
            ).fetchall()
            return [dict(r) for r in rows]

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


    # Option types backed by normalized lookup tables (single source of truth)
    _NORMALIZED_OPTION_TABLES: dict[str, str] = {
        "handle-colors":  "handle_colors",
        "blade-colors":   "blade_colors",
        "blade-steels":   "blade_steels",
        "blade-finishes": "blade_finishes",
        "handle-types":   "handle_types",
        "locations":      "locations",
    }
    # Option types still backed by v2_option_values
    _LEGACY_OPTION_TYPES = {
        "blade-types", "categories", "blade-families", "primary-use-cases",
        "collaborators", "generations", "size-modifiers", "platform-variants",
    }

    @router.get("/api/v2/options")
    def v2_get_options():
        with get_conn() as conn:
            result: dict[str, list[dict[str, Any]]] = {}
            for key, table in _NORMALIZED_OPTION_TABLES.items():
                rows = conn.execute(
                    f"SELECT id, name FROM {table} ORDER BY name COLLATE NOCASE"
                ).fetchall()
                result[key] = rows
            for key in _LEGACY_OPTION_TYPES:
                rows = conn.execute(
                    "SELECT id, name FROM v2_option_values WHERE option_type = ? ORDER BY name COLLATE NOCASE",
                    (key,),
                ).fetchall()
                result[key] = rows
            return result


    @router.post("/api/v2/options/{option_type}")
    def v2_add_option(option_type: str, payload: dict = Body(...)):
        if option_type not in _NORMALIZED_OPTION_TABLES and option_type not in _LEGACY_OPTION_TYPES:
            raise HTTPException(status_code=404, detail="Unknown option type.")
        with get_conn() as conn:
            clean_name = (payload.get("name") or "").strip()
            if not clean_name:
                raise HTTPException(status_code=400, detail="Option name is required.")
            try:
                if option_type in _NORMALIZED_OPTION_TABLES:
                    table = _NORMALIZED_OPTION_TABLES[option_type]
                    cur = conn.execute(
                        f"INSERT INTO {table} (name) VALUES (?)", (clean_name,)
                    )
                else:
                    cur = conn.execute(
                        "INSERT INTO v2_option_values (option_type, name) VALUES (?, ?)",
                        (option_type, clean_name),
                    )
                    if option_type == "collaborators":
                        _v2_dim_id(conn, "collaborators", clean_name)
            except sqlite3.IntegrityError:
                raise HTTPException(status_code=400, detail="Option already exists.")
            return {"id": cur.lastrowid, "message": "Created"}


    @router.delete("/api/v2/options/{option_type}/{option_id}")
    def v2_delete_option(option_type: str, option_id: int):
        if option_type not in _NORMALIZED_OPTION_TABLES and option_type not in _LEGACY_OPTION_TYPES:
            raise HTTPException(status_code=404, detail="Unknown option type.")
        with get_conn() as conn:
            if option_type in _NORMALIZED_OPTION_TABLES:
                table = _NORMALIZED_OPTION_TABLES[option_type]
                row = conn.execute(
                    f"SELECT name FROM {table} WHERE id = ?", (option_id,)
                ).fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="Option not found.")
                # Check FK usage before deleting
                id_col_map = {
                    "handle-colors":  "handle_color_id",
                    "blade-colors":   "blade_color_id",
                    "blade-steels":   "steel_id",
                    "blade-finishes": "blade_finish_id",
                    "handle-types":   "handle_type_id",
                    "locations":      "location_id",
                }
                id_col = id_col_map.get(option_type)
                if id_col:
                    usage = 0
                    # Check model_colorways for color FKs
                    if option_type in ("handle-colors", "blade-colors"):
                        colorway_col = "handle_color_id" if option_type == "handle-colors" else "blade_color_id"
                        usage += conn.execute(
                            f"SELECT COUNT(*) AS c FROM model_colorways WHERE {colorway_col} = ?", (option_id,)
                        ).fetchone()["c"]
                    # Check knife_models_v2 for steel/finish/handle_type FKs
                    if option_type in ("blade-steels", "blade-finishes", "handle-types"):
                        if id_col in {r["name"] for r in conn.execute("PRAGMA table_info(knife_models_v2)")}:
                            usage += conn.execute(
                                f"SELECT COUNT(*) AS c FROM knife_models_v2 WHERE {id_col} = ?", (option_id,)
                            ).fetchone()["c"]
                    # Check inventory_items_v2 for location FK
                    if option_type == "locations":
                        usage += conn.execute(
                            f"SELECT COUNT(*) AS c FROM inventory_items_v2 WHERE location_id = ?", (option_id,)
                        ).fetchone()["c"]
                    if usage:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Cannot delete — in use by {usage} record(s).",
                        )
                conn.execute(f"DELETE FROM {table} WHERE id = ?", (option_id,))
            else:
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
                }
                sql = usage_sql.get(option_type)
                if sql and option_name:
                    in_use = conn.execute(sql, (option_name,)).fetchone()["c"]
                    if in_use:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Cannot delete option in use by {in_use} model(s).",
                        )
                conn.execute(
                    "DELETE FROM v2_option_values WHERE id = ? AND option_type = ?",
                    (option_id, option_type),
                )
            return {"message": "Deleted"}


    @router.get("/api/inventory/options")
    def inventory_options(master_knife_id: Optional[int] = None):  # noqa: ARG001
        """Return option lists for the inventory form. master_knife_id accepted but unused (all options returned)."""
        with get_conn() as conn:
            result: dict[str, list[dict[str, Any]]] = {}
            # Normalized types — read from lookup tables
            for key, table in _NORMALIZED_OPTION_TABLES.items():
                result[key] = conn.execute(
                    f"SELECT id, name FROM {table} ORDER BY name COLLATE NOCASE"
                ).fetchall()
            # Legacy types still in v2_option_values
            for key in ("blade-types", "categories", "primary-use-cases"):
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
                "SELECT id FROM knife_models_v2 WHERE id = ?",
                (payload.knife_model_id,),
            ).fetchone()
            if not model_exists:
                raise HTTPException(status_code=400, detail="Invalid knife model id.")
            cur = conn.execute(
                """
                INSERT INTO inventory_items_v2
                (knife_model_id, colorway_id, quantity, purchase_price, acquired_date,
                 mkc_order_number, location_id, notes, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                (
                    payload.knife_model_id,
                    payload.colorway_id,
                    payload.quantity,
                    payload.purchase_price,
                    payload.acquired_date,
                    payload.mkc_order_number,
                    payload.location_id,
                    payload.notes,
                ),
            )
            return {"id": cur.lastrowid, "message": "Created"}


    @router.put("/api/v2/inventory/{item_id}")
    def v2_update_inventory_item(item_id: int, payload: InventoryItemV2In):
        """Update inventory item in v2 only (canonical write path)."""
        with get_conn() as conn:
            model_exists = conn.execute(
                "SELECT id FROM knife_models_v2 WHERE id = ?",
                (payload.knife_model_id,),
            ).fetchone()
            if not model_exists:
                raise HTTPException(status_code=400, detail="Invalid knife model id.")
            cur = conn.execute(
                """
                UPDATE inventory_items_v2
                SET knife_model_id = ?, colorway_id = ?, quantity = ?, purchase_price = ?,
                    acquired_date = ?, mkc_order_number = ?, location_id = ?, notes = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    payload.knife_model_id,
                    payload.colorway_id,
                    payload.quantity,
                    payload.purchase_price,
                    payload.acquired_date,
                    payload.mkc_order_number,
                    payload.location_id,
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
            cur = conn.execute(
                """
                INSERT INTO inventory_items_v2
                (knife_model_id, colorway_id, quantity, purchase_price, acquired_date,
                 mkc_order_number, location_id, notes, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                (
                    row["knife_model_id"], row.get("colorway_id"), row["quantity"],
                    row["purchase_price"], row["acquired_date"],
                    row.get("mkc_order_number"), row.get("location_id"), row["notes"],
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
                    i.quantity,
                    i.acquired_date,
                    i.mkc_order_number,
                    i.purchase_price,
                    hc.name AS handle_color,
                    bs.name AS blade_steel,
                    bf.name AS blade_finish,
                    bc.name AS blade_color,
                    ht.name AS handle_type,
                    km.blade_length,
                    (CASE WHEN c.name IS NOT NULL THEN 1 ELSE 0 END) AS is_collab,
                    c.name AS collaboration_name,
                    loc.name AS location,
                    i.notes,
                    i.created_at,
                    i.updated_at
                FROM inventory_items_v2 i
                LEFT JOIN knife_models_v2 km ON km.id = i.knife_model_id
                LEFT JOIN knife_families fam ON fam.id = km.family_id
                LEFT JOIN blade_steels bs ON bs.id = km.steel_id
                LEFT JOIN blade_finishes bf ON bf.id = km.blade_finish_id
                LEFT JOIN handle_types ht ON ht.id = km.handle_type_id
                LEFT JOIN collaborators c ON c.id = km.collaborator_id
                LEFT JOIN model_colorways mc ON mc.id = i.colorway_id
                LEFT JOIN handle_colors hc ON hc.id = mc.handle_color_id
                LEFT JOIN blade_colors bc ON bc.id = mc.blade_color_id
                LEFT JOIN locations loc ON loc.id = i.location_id
                ORDER BY fam.name COLLATE NOCASE, km.sortable_name COLLATE NOCASE, i.id DESC
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
                    knife_type=row.get("knife_type") or None,
                    family_name=row.get("family_name") or row.get("family") or None,
                    form_name=row.get("form_name") or None,
                    series_name=row.get("series_name") or row.get("catalog_line") or None,
                    collaborator_name=row.get("collaborator_name") or row.get("collaboration_name") or None,
                    steel=row.get("steel") or row.get("default_steel") or None,
                    blade_finish=row.get("blade_finish") or row.get("default_blade_finish") or None,
                    handle_type=row.get("handle_type") or None,
                    blade_length=(float(row["blade_length"]) if row.get("blade_length") else None),
                    msrp=(float(row["msrp"]) if row.get("msrp") else None),
                    official_product_url=row.get("official_product_url") or row.get("default_product_url") or None,
                    model_notes=row.get("model_notes") or row.get("notes") or None,
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
                steel_id = _v2_attr_id(conn, "blade_steels", payload.steel)
                blade_finish_id = _v2_attr_id(conn, "blade_finishes", payload.blade_finish)
                handle_type_id = _v2_attr_id(conn, "handle_types", payload.handle_type)
                sortable_name = payload.official_name.strip()
                if existing:
                    slug = _v2_model_slug(conn, sortable_name, existing_id=existing["id"])
                    conn.execute(
                        """
                        UPDATE knife_models_v2
                        SET official_name = ?, sortable_name = ?, slug = ?,
                            type_id = ?, form_id = ?, family_id = ?, series_id = ?, collaborator_id = ?,
                            steel_id = ?, blade_finish_id = ?, handle_type_id = ?,
                            blade_length = ?, msrp = ?, official_product_url = ?, model_notes = ?,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (
                            payload.official_name.strip(), sortable_name, slug,
                            type_id, form_id, family_id, series_id, collaborator_id,
                            steel_id, blade_finish_id, handle_type_id,
                            payload.blade_length, payload.msrp, payload.official_product_url, payload.model_notes,
                            existing["id"],
                        ),
                    )
                    updated += 1
                else:
                    slug = _v2_model_slug(conn, sortable_name)
                    conn.execute(
                        """
                        INSERT INTO knife_models_v2 (
                            official_name, sortable_name, slug, type_id, form_id, family_id, series_id, collaborator_id,
                            steel_id, blade_finish_id, handle_type_id,
                            blade_length, msrp, official_product_url, model_notes
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            payload.official_name.strip(), sortable_name, slug,
                            type_id, form_id, family_id, series_id, collaborator_id,
                            steel_id, blade_finish_id, handle_type_id,
                            payload.blade_length, payload.msrp, payload.official_product_url, payload.model_notes,
                        ),
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
                SELECT km.id, km.official_name,
                       kt.name AS knife_type, frm.name AS form_name, fam.name AS family_name,
                       ks.name AS series_name, c.name AS collaborator_name
                FROM knife_models_v2 km
                LEFT JOIN knife_types kt ON kt.id = km.type_id
                LEFT JOIN knife_forms frm ON frm.id = km.form_id
                LEFT JOIN knife_families fam ON fam.id = km.family_id
                LEFT JOIN knife_series ks ON ks.id = km.series_id
                LEFT JOIN collaborators c ON c.id = km.collaborator_id
                ORDER BY fam.name COLLATE NOCASE, km.sortable_name COLLATE NOCASE, km.id
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
                SELECT km.id, km.official_name, km.blade_length, km.model_notes,
                       bs.name AS blade_steel, bf.name AS blade_finish, ht.name AS handle_type,
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
                LEFT JOIN blade_steels bs ON bs.id = km.steel_id
                LEFT JOIN blade_finishes bf ON bf.id = km.blade_finish_id
                LEFT JOIN handle_types ht ON ht.id = km.handle_type_id
                LEFT JOIN knife_model_descriptors d ON d.knife_model_id = km.id
                LEFT JOIN knife_model_images kmi ON kmi.knife_model_id = km.id
                ORDER BY fam.name COLLATE NOCASE, km.sortable_name COLLATE NOCASE
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
                    for k in ("family_name", "knife_type", "official_name", "form_name")
                ).lower()
                if payload.family.lower() in fam_hay:
                    score += 22
                    reasons.append(f"use / category matches {payload.family}")
            if payload.steel and row.get("blade_steel") and payload.steel.lower() == str(row["blade_steel"]).lower():
                score += 12
                reasons.append(f"steel matches {row['blade_steel']}")
            if payload.finish and row.get("blade_finish") and payload.finish.lower() == str(row["blade_finish"]).lower():
                score += 12
                reasons.append(f"finish matches {row['blade_finish']}")
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
                        "has_identifier_image": bool(row.get("has_identifier_image")),
                        "has_silhouette_hint": False,
                        "catalog_blurb": row.get("model_notes"),
                        "default_blade_length": row.get("blade_length"),
                        "default_steel": row.get("blade_steel"),
                        "default_blade_finish": row.get("blade_finish"),
                        "is_collab": bool(row.get("collaborator_name")),
                        "collaboration_name": row.get("collaborator_name"),
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
