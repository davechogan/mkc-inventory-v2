"""Legacy master_knives + inventory + options HTTP API (pre-v2 UI paths)."""
from __future__ import annotations

import csv
import io
import re
import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import Any, Optional, Type

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import Response
from pydantic import BaseModel

from reporting.domain import GetConn

OPTION_TABLES = {
    "handle-colors": "option_handle_colors",
    "blade-steels": "option_blade_steels",
    "blade-finishes": "option_blade_finishes",
    "blade-colors": "option_blade_colors",
    "blade-types": "option_blade_types",
    "categories": "option_categories",
    "blade-families": "option_blade_families",
    "primary-use-cases": "option_primary_use_cases",
}


def create_legacy_catalog_router(
    *,
    get_conn: GetConn,
    master_knives_public_columns: tuple[str, ...],
    master_csv_columns: list[str],
    inventory_csv_columns: list[str],
    derive_blade_family_from_name: Callable[[Optional[str]], str],
    normalize_category_value: Callable[[Optional[str]], Optional[str]],
    master_knife_in_model: Type[BaseModel],
    inventory_item_in_model: Type[BaseModel],
    option_in_model: Type[BaseModel],
) -> APIRouter:
    MasterKnifeIn = master_knife_in_model
    InventoryItemIn = inventory_item_in_model
    OptionIn = option_in_model
    router = APIRouter(tags=["legacy-catalog"])

    def _parse_csv_bool(value: Optional[str]) -> int:
        if value is None or str(value).strip() == "":
            return 0
        return 1 if str(value).strip().lower() in ("1", "true", "yes", "y") else 0

    def _parse_csv_optional_float(value: Optional[str]) -> Optional[float]:
        if value is None or str(value).strip() == "":
            return None
        try:
            return float(value)
        except ValueError:
            return None


    def _parse_csv_pipe_option_ids(conn: sqlite3.Connection, value: Optional[str], table: str, name_col: str = "name") -> list[int]:
        """Parse pipe-delimited option names into IDs. e.g. 'Orange|Black' -> [1, 2]."""
        if not value or not str(value).strip():
            return []
        names = [n.strip() for n in str(value).split("|") if n.strip()]
        if not names:
            return []
        placeholders = ",".join("?" * len(names))
        rows = conn.execute(
            f"SELECT id FROM {table} WHERE {name_col} IN ({placeholders})", names
        ).fetchall()
        return [r["id"] for r in rows]


    @router.get("/api/summary")
    def get_summary():
        with get_conn() as conn:
            summary = conn.execute(
                """
                SELECT
                    COUNT(*) AS inventory_rows,
                    COALESCE(SUM(quantity), 0) AS total_quantity,
                    COALESCE(SUM(COALESCE(purchase_price, 0) * quantity), 0) AS total_spend,
                    COALESCE(SUM(COALESCE(estimated_value, 0) * quantity), 0) AS total_estimated_value
                FROM inventory_items
                """
            ).fetchone()
            summary["master_count"] = conn.execute(
                "SELECT COUNT(*) AS c FROM master_knives WHERE status != 'archived'"
            ).fetchone()["c"]
            summary["by_family"] = conn.execute(
                """
                SELECT
                    COALESCE(NULLIF(TRIM(m.family), ''), 'Uncategorized') AS family,
                    COUNT(*) AS inventory_rows,
                    COALESCE(SUM(i.quantity), 0) AS total_quantity
                FROM inventory_items i
                JOIN master_knives m ON m.id = i.master_knife_id
                GROUP BY family
                ORDER BY total_quantity DESC, family COLLATE NOCASE
                """
            ).fetchall()
            return summary


    @router.get("/api/master-knives")
    def list_master_knives(
        active_only: bool = False,
        family: Optional[str] = None,
        use_case: Optional[str] = None,
        blade_shape: Optional[str] = None,
        is_current_catalog: Optional[bool] = None,
        is_discontinued: Optional[bool] = None,
        is_collab: Optional[bool] = None,
        record_type: Optional[str] = None,
    ):
        """List master knives with optional filters for catalog, lifecycle, and collab."""
        with get_conn() as conn:
            conditions = []
            params: list[Any] = []

            if active_only:
                conditions.append("status != 'archived'")

            if is_current_catalog is True:
                conditions.append("(is_current_catalog = 1 OR is_current_catalog IS NULL)")
            elif is_current_catalog is False:
                conditions.append("is_current_catalog = 0")

            if is_discontinued is True:
                conditions.append("is_discontinued = 1")
            elif is_discontinued is False:
                conditions.append("(is_discontinued = 0 OR is_discontinued IS NULL)")

            if is_collab is True:
                conditions.append("is_collab = 1")
            elif is_collab is False:
                conditions.append("is_collab = 0")

            if record_type and record_type.strip():
                conditions.append("(record_type LIKE ? OR record_type = ?)")
                q = f"%{record_type.strip()}%"
                params.extend([q, record_type.strip()])

            if family and family.strip():
                conditions.append("(family LIKE ? OR category LIKE ?)")
                q = f"%{family.strip()}%"
                params.extend([q, q])

            if use_case and use_case.strip():
                conditions.append("(primary_use_case LIKE ? OR category LIKE ?)")
                q = f"%{use_case.strip()}%"
                params.extend([q, q])

            if blade_shape and blade_shape.strip():
                conditions.append("(blade_shape LIKE ? OR blade_profile LIKE ?)")
                q = f"%{blade_shape.strip()}%"
                params.extend([q, q])

            where_sql = " AND ".join(conditions) if conditions else "1=1"

            rows = conn.execute(
                f"""
                SELECT {master_knives_public_columns}
                FROM master_knives
                WHERE {where_sql}
                ORDER BY name COLLATE NOCASE
                """,
                params,
            ).fetchall()
            return rows


    @router.get("/api/master-knives/export.csv")
    def export_master_csv():
        with get_conn() as conn:
            rows = conn.execute(
                f"""
                SELECT {master_knives_public_columns}
                FROM master_knives
                ORDER BY name COLLATE NOCASE
                """
            ).fetchall()
            buffer = io.StringIO()
            writer = csv.DictWriter(buffer, fieldnames=master_csv_columns, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                flat: dict[str, Any] = {}
                for key in master_csv_columns:
                    val = row.get(key)
                    if key.startswith("is_") or key in ("has_ring",):
                        flat[key] = "1" if val else "0"
                    elif val is None:
                        flat[key] = ""
                    else:
                        flat[key] = val
                writer.writerow(flat)
        data = buffer.getvalue()
        return Response(
            content=data.encode("utf-8"),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": 'attachment; filename="mkc_master_knives.csv"'},
        )


    def _csv_cell(row: dict[str, str], *keys: str) -> str:
        for key in keys:
            for rk, rv in row.items():
                if (rk or "").strip().lower() == key.lower():
                    return (rv or "").strip()
        return ""


    def import_master_csv_from_path(path: Path) -> dict[str, Any]:
        """
        Import master knives from a CSV file. Upserts by name.
        Returns {"inserted": N, "updated": N, "message": "..."}.
        """
        try:
            text = path.read_text(encoding="utf-8-sig")
        except UnicodeDecodeError as e:
            raise ValueError("File must be UTF-8 encoded CSV.") from e
        return _run_master_csv_import(text)


    def _run_master_csv_import(csv_text: str) -> dict[str, Any]:
        """Core import logic; expects UTF-8 CSV text."""
        reader = csv.DictReader(io.StringIO(csv_text))
        fnames = {(f or "").strip().lstrip("\ufeff") for f in (reader.fieldnames or [])}
        is_knife_master = "Model / family" in fnames
        if not reader.fieldnames or (not is_knife_master and "name" not in fnames):
            raise HTTPException(
                status_code=400,
                detail="CSV must include a name column, or Knife Master columns (e.g. Model / family).",
            )
        csv_has_catalog_line = any(
            (f or "").strip().lower() in ("catalog_line", "catalog line") for f in fnames
        )
        inserted = 0
        updated = 0
        with get_conn() as conn:
            for raw in reader:
                row = {(k or "").strip(): (v if v is None else str(v).strip()) for k, v in raw.items()}
                if is_knife_master:
                    name = _csv_cell(row, "Model / family")
                else:
                    name = _csv_cell(row, "name")
                if not name:
                    continue

                catalog_line_val = None
                if is_knife_master:
                    record_type = _csv_cell(row, "Record type") or None
                    category = normalize_category_value(_csv_cell(row, "Category") or None)
                    catalog_status = _csv_cell(row, "Status") or None
                    confidence = _csv_cell(row, "Confidence") or None
                    evidence_summary = _csv_cell(row, "Evidence summary") or None
                    collector_notes = _csv_cell(row, "Collector notes") or None
                    if not record_type:
                        record_type = None
                    if not category:
                        category = None
                    # optional app-export columns may be present on a merged sheet
                    family = _csv_cell(row, "family") or None
                    default_blade_length = _parse_csv_optional_float(_csv_cell(row, "default_blade_length"))
                    default_steel = _csv_cell(row, "default_steel") or None
                    default_blade_finish = _csv_cell(row, "default_blade_finish") or None
                    default_blade_color = _csv_cell(row, "default_blade_color") or None
                    default_handle_color = _csv_cell(row, "default_handle_color") or None
                    _cr = _csv_cell(row, "is_collab")
                    is_collab = _parse_csv_bool(_cr) if _cr else None
                    collaboration_name = _csv_cell(row, "collaboration_name") or None
                    # Operational active/archived — only when a lowercase ``status`` column exists (app export).
                    # Knife Master ``Status`` maps to ``catalog_status``, not this field.
                    op_status = "active"
                    if "status" in row:
                        op_status = (row.get("status") or "active").strip() or "active"
                    notes = _csv_cell(row, "notes") or None
                    blade_profile = _csv_cell(row, "blade_profile") or None
                    _hr = _csv_cell(row, "has_ring")
                    has_ring = _parse_csv_bool(_hr) if _hr else None
                    _ff = _csv_cell(row, "is_filleting_knife")
                    is_filleting = _parse_csv_bool(_ff) if _ff else None
                    _hx = _csv_cell(row, "is_hatchet")
                    is_hatchet = _parse_csv_bool(_hx) if _hx else None
                    _kit = _csv_cell(row, "is_kitchen")
                    is_kitchen = _parse_csv_bool(_kit) if _kit else None
                    _tac = _csv_cell(row, "is_tactical")
                    is_tactical = _parse_csv_bool(_tac) if _tac else None
                    identifier_keywords = _csv_cell(row, "identifier_keywords") or None
                    identifier_distinguishing_features = _csv_cell(row, "identifier_distinguishing_features") or None
                    identifier_product_url = _csv_cell(row, "identifier_product_url") or None
                    identifier_image_mime = _csv_cell(row, "identifier_image_mime") or None
                    identifier_silhouette_hu_json = _csv_cell(row, "identifier_silhouette_hu_json") or None
                    if csv_has_catalog_line:
                        cl_raw = _csv_cell(row, "catalog_line") or _csv_cell(row, "Catalog line")
                        catalog_line_val = (
                            normalize_master_catalog_line_input(cl_raw, strict=False) if cl_raw else None
                        )
                else:
                    record_type = _csv_cell(row, "record_type") or None
                    catalog_status = _csv_cell(row, "catalog_status") or None
                    confidence = _csv_cell(row, "confidence") or None
                    evidence_summary = _csv_cell(row, "evidence_summary") or None
                    collector_notes = _csv_cell(row, "collector_notes") or None
                    family = _csv_cell(row, "family") or None
                    category = normalize_category_value(_csv_cell(row, "category") or None)
                    default_blade_length = _parse_csv_optional_float(_csv_cell(row, "default_blade_length"))
                    default_steel = _csv_cell(row, "default_steel") or None
                    default_blade_finish = _csv_cell(row, "default_blade_finish") or None
                    default_blade_color = _csv_cell(row, "default_blade_color") or None
                    default_handle_color = _csv_cell(row, "default_handle_color") or None
                    is_collab = _parse_csv_bool(_csv_cell(row, "is_collab"))
                    collaboration_name = _csv_cell(row, "collaboration_name") or None
                    op_status = (_csv_cell(row, "status") or "active").strip() or "active"
                    notes = _csv_cell(row, "notes") or None
                    blade_profile = _csv_cell(row, "blade_profile") or None
                    has_ring = _parse_csv_bool(_csv_cell(row, "has_ring"))
                    is_filleting = _parse_csv_bool(_csv_cell(row, "is_filleting_knife"))
                    is_hatchet = _parse_csv_bool(_csv_cell(row, "is_hatchet"))
                    is_kitchen = _parse_csv_bool(_csv_cell(row, "is_kitchen"))
                    is_tactical = _parse_csv_bool(_csv_cell(row, "is_tactical"))
                    identifier_keywords = _csv_cell(row, "identifier_keywords") or None
                    identifier_distinguishing_features = _csv_cell(row, "identifier_distinguishing_features") or None
                    identifier_product_url = _csv_cell(row, "identifier_product_url") or None
                    identifier_image_mime = _csv_cell(row, "identifier_image_mime") or None
                    identifier_silhouette_hu_json = _csv_cell(row, "identifier_silhouette_hu_json") or None
                    if csv_has_catalog_line:
                        cl_raw = _csv_cell(row, "catalog_line")
                        catalog_line_val = (
                            normalize_master_catalog_line_input(cl_raw, strict=False) if cl_raw else None
                        )
                    canonical_slug = _csv_cell(row, "canonical_slug") or None
                    msrp = _parse_csv_optional_float(_csv_cell(row, "msrp"))
                    first_release_date = _csv_cell(row, "first_release_date") or None
                    last_seen_date = _csv_cell(row, "last_seen_date") or None
                    _disc = _csv_cell(row, "is_discontinued")
                    is_discontinued = _parse_csv_bool(_disc) if _disc else None
                    _curr = _csv_cell(row, "is_current_catalog")
                    is_current_catalog = _parse_csv_bool(_curr) if _curr else None
                    blade_shape = _csv_cell(row, "blade_shape") or blade_profile
                    tip_style = _csv_cell(row, "tip_style") or None
                    grind_style = _csv_cell(row, "grind_style") or None
                    size_class = _csv_cell(row, "size_class") or None
                    primary_use_case = _csv_cell(row, "primary_use_case") or category
                    spine_profile = _csv_cell(row, "spine_profile") or None
                    _fil = _csv_cell(row, "is_fillet")
                    is_fillet = _parse_csv_bool(_fil) if _fil else is_filleting
                    default_product_url = _csv_cell(row, "default_product_url") or None
                    primary_image_url = _csv_cell(row, "primary_image_url") or None
                existing = conn.execute(
                    "SELECT id FROM master_knives WHERE name = ?", (name,)
                ).fetchone()

                if is_knife_master and existing:
                    if csv_has_catalog_line:
                        conn.execute(
                            """
                            UPDATE master_knives
                            SET record_type = ?, category = ?, catalog_line = ?, catalog_status = ?, confidence = ?,
                                evidence_summary = ?, collector_notes = ?, updated_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                            """,
                            (
                                record_type,
                                category,
                                catalog_line_val,
                                catalog_status,
                                confidence,
                                evidence_summary,
                                collector_notes,
                                existing["id"],
                            ),
                        )
                    else:
                        conn.execute(
                            """
                            UPDATE master_knives
                            SET record_type = ?, category = ?, catalog_status = ?, confidence = ?,
                                evidence_summary = ?, collector_notes = ?, updated_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                            """,
                            (
                                record_type,
                                category,
                                catalog_status,
                                confidence,
                                evidence_summary,
                                collector_notes,
                                existing["id"],
                            ),
                        )
                    updated += 1
                    continue

                if is_knife_master and not existing:
                    hr, ff, hx, kit, tac, col = infer_identifier_flags(name, category, record_type)
                    if is_collab is not None:
                        col = is_collab
                    if has_ring is not None:
                        hr = has_ring
                    if is_filleting is not None:
                        ff = is_filleting
                    if is_hatchet is not None:
                        hx = is_hatchet
                    if is_kitchen is not None:
                        kit = is_kitchen
                    if is_tactical is not None:
                        tac = is_tactical
                    conn.execute(
                        """
                        INSERT INTO master_knives
                        (name, family, record_type, category, catalog_line, catalog_status, confidence, evidence_summary,
                         collector_notes, default_blade_length, default_steel, default_blade_finish, default_blade_color,
                         default_handle_color, is_collab, collaboration_name, status, notes, blade_profile,
                         has_ring, is_filleting_knife, is_hatchet, is_kitchen, is_tactical, identifier_keywords,
                         identifier_distinguishing_features, identifier_product_url, identifier_image_mime, identifier_silhouette_hu_json, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        """,
                        (
                            name,
                            family,
                            record_type,
                            category,
                            catalog_line_val,
                            catalog_status,
                            confidence,
                            evidence_summary,
                            collector_notes,
                            default_blade_length,
                            default_steel,
                            default_blade_finish,
                            default_blade_color,
                            default_handle_color,
                            int(col),
                            collaboration_name,
                            op_status,
                            notes,
                            blade_profile,
                            int(hr),
                            int(ff),
                            int(hx),
                            int(kit),
                            int(tac),
                            identifier_keywords,
                            identifier_distinguishing_features,
                            identifier_product_url,
                            identifier_image_mime,
                            identifier_silhouette_hu_json,
                        ),
                    )
                    inserted += 1
                    continue

                v2_extras = (
                    canonical_slug,
                    msrp,
                    first_release_date,
                    last_seen_date,
                    1 if is_discontinued else 0,
                    0 if is_current_catalog is False else 1,
                    blade_shape,
                    tip_style,
                    grind_style,
                    size_class,
                    primary_use_case,
                    spine_profile,
                    1 if is_fillet else 0,
                    default_product_url,
                    primary_image_url,
                )
                values = (
                    name,
                    family,
                    default_blade_length,
                    default_steel,
                    default_blade_finish,
                    default_blade_color,
                    default_handle_color,
                    int(is_collab),
                    collaboration_name,
                    op_status,
                    notes,
                    record_type,
                    catalog_status,
                    confidence,
                    evidence_summary,
                    collector_notes,
                    category,
                    catalog_line_val,
                    blade_profile,
                    int(has_ring),
                    int(is_filleting),
                    int(is_hatchet),
                    int(is_kitchen),
                    int(is_tactical),
                    identifier_keywords,
                    identifier_distinguishing_features,
                    identifier_product_url,
                    identifier_image_mime,
                    identifier_silhouette_hu_json,
                ) + v2_extras
                if existing:
                    conn.execute(
                        """
                        UPDATE master_knives
                        SET family = ?, default_blade_length = ?, default_steel = ?, default_blade_finish = ?,
                            default_blade_color = ?, default_handle_color = ?, is_collab = ?, collaboration_name = ?,
                            status = ?, notes = ?, record_type = ?, catalog_status = ?, confidence = ?,
                            evidence_summary = ?, collector_notes = ?, category = ?, catalog_line = ?, blade_profile = ?,
                            has_ring = ?, is_filleting_knife = ?, is_hatchet = ?, is_kitchen = ?,
                            is_tactical = ?, identifier_keywords = ?, identifier_distinguishing_features = ?,
                            identifier_product_url = ?, identifier_image_mime = ?, identifier_silhouette_hu_json = ?,
                            canonical_slug = ?, msrp = ?, first_release_date = ?, last_seen_date = ?,
                            is_discontinued = ?, is_current_catalog = ?, blade_shape = ?, tip_style = ?, grind_style = ?,
                            size_class = ?, primary_use_case = ?, spine_profile = ?, is_fillet = ?,
                            default_product_url = ?, primary_image_url = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        values[1:] + (existing["id"],),
                    )
                    updated += 1
                else:
                    cur = conn.execute(
                        """
                        INSERT INTO master_knives
                        (name, family, default_blade_length, default_steel, default_blade_finish, default_blade_color,
                         default_handle_color, is_collab, collaboration_name, status, notes, record_type,
                         catalog_status, confidence, evidence_summary, collector_notes, category, catalog_line, blade_profile,
                         has_ring, is_filleting_knife, is_hatchet, is_kitchen, is_tactical, identifier_keywords,
                         identifier_distinguishing_features, identifier_product_url, identifier_image_mime, identifier_silhouette_hu_json,
                         canonical_slug, msrp, first_release_date, last_seen_date, is_discontinued, is_current_catalog,
                         blade_shape, tip_style, grind_style, size_class, primary_use_case, spine_profile, is_fillet,
                         default_product_url, primary_image_url, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        """,
                        values,
                    )
                    new_id = cur.lastrowid
                    inserted += 1
        return {"inserted": inserted, "updated": updated, "message": "Import complete."}


    @router.post("/api/master-knives/import.csv")
    async def import_master_csv(file: UploadFile = File(...)):
        raw = await file.read()
        try:
            text = raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            raise HTTPException(status_code=400, detail="File must be UTF-8 encoded CSV.")
        return _run_master_csv_import(text)


    @router.get("/api/master-knives/{knife_id}/identifier-image")
    def get_master_identifier_image(knife_id: int):
        """Serves stored reference bytes from the one-time seed (no hotlinking at runtime)."""
        with get_conn() as conn:
            row = conn.execute(
                "SELECT identifier_image_blob, identifier_image_mime FROM master_knives WHERE id = ?",
                (knife_id,),
            ).fetchone()
            if not row or not row.get("identifier_image_blob"):
                raise HTTPException(status_code=404, detail="No stored reference image for this model.")
            mime = (row.get("identifier_image_mime") or "image/jpeg").strip() or "image/jpeg"
            return Response(content=row["identifier_image_blob"], media_type=mime)


    @router.post("/api/master-knives/{knife_id}/identifier-image")
    async def upload_master_identifier_image(
        knife_id: int,
        file: UploadFile = File(...),
        model: Optional[str] = Form(None),
    ):
        """
        Store an uploaded reference photo, compute the Hu silhouette vector, and extract
        distinguishing features via vision LLM (lanyard hole, handle type, ring guard, etc.).
        """
        raw = await file.read()
        if len(raw) > 15 * 1024 * 1024:
            raise HTTPException(status_code=413, detail="Image larger than 15MB")
        mime = ((file.content_type or "").split(";")[0].strip().lower() or "image/jpeg")
        if not mime.startswith("image/"):
            mime = "image/jpeg"
        if mime == "image/jpg":
            mime = "image/jpeg"
        hu_list, hu_err = blade_ai.extract_blade_hu_from_image_bytes(raw)
        hu_json = json.dumps(hu_list) if hu_list else None

        dist_features: Optional[str] = None
        dist_error: Optional[str] = None
        vision_model = (model or "").strip() or OLLAMA_VISION_MODEL
        if vision_model:
            model_ok, _ = blade_ai.check_ollama_model(vision_model)
            if model_ok:
                img_b64 = base64.standard_b64encode(raw).decode("ascii")
                dist_features, dist_error = blade_ai.extract_distinguishing_features_from_image(vision_model, img_b64)

        with get_conn() as conn:
            exists = conn.execute("SELECT id FROM master_knives WHERE id = ?", (knife_id,)).fetchone()
            if not exists:
                raise HTTPException(status_code=404, detail="Master knife not found.")
            conn.execute(
                """
                UPDATE master_knives
                SET identifier_image_blob = ?, identifier_image_mime = ?, identifier_silhouette_hu_json = ?,
                    identifier_distinguishing_features = COALESCE(?, identifier_distinguishing_features),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (raw, mime, hu_json, dist_features, knife_id),
            )
        return {
            "message": "Reference image stored.",
            "has_silhouette": hu_list is not None,
            "silhouette_error": hu_err,
            "distinguishing_features": dist_features,
            "distinguishing_features_error": dist_error,
        }


    @router.delete("/api/master-knives/{knife_id}/identifier-image")
    def delete_master_identifier_image(knife_id: int):
        """Remove stored reference bytes and silhouette data (URLs in other fields are unchanged)."""
        with get_conn() as conn:
            cur = conn.execute(
                """
                UPDATE master_knives
                SET identifier_image_blob = NULL, identifier_image_mime = NULL,
                    identifier_silhouette_hu_json = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (knife_id,),
            )
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Master knife not found.")
        return {"message": "Reference image cleared."}


    @router.get("/api/master-knives/{knife_id}")
    def get_master_knife(knife_id: int):
        with get_conn() as conn:
            row = conn.execute(
                f"""
                SELECT {master_knives_public_columns}
                FROM master_knives
                WHERE id = ?
                """,
                (knife_id,),
            ).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Master knife not found.")
            return dict(row)


    @router.post("/api/master-knives")
    def create_master_knife(payload: MasterKnifeIn):
        with get_conn() as conn:
            try:
                cur = conn.execute(
                    """
                    INSERT INTO master_knives
                    (name, family, record_type, category, catalog_line, catalog_status, confidence, evidence_summary,
                     collector_notes, default_blade_length, default_steel, default_blade_finish, default_blade_color,
                     default_handle_color, is_collab, collaboration_name, status, notes, blade_profile,
                     has_ring, is_filleting_knife, is_hatchet, is_kitchen, is_tactical, identifier_keywords,
                     identifier_product_url, identifier_image_mime, identifier_silhouette_hu_json,
                     canonical_slug, version, parent_model_id, first_release_date, last_seen_date,
                     is_discontinued, is_current_catalog, msrp, blade_shape, tip_style, grind_style, size_class,
                     primary_use_case, spine_profile, is_fillet, default_product_url, primary_image_url,
                     updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                     ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (
                        payload.name.strip(),
                        payload.family,
                        payload.record_type,
                        payload.category,
                        payload.catalog_line,
                        payload.catalog_status,
                        payload.confidence,
                        payload.evidence_summary,
                        payload.collector_notes,
                        payload.default_blade_length,
                        payload.default_steel,
                        payload.default_blade_finish,
                        payload.default_blade_color,
                        payload.default_handle_color,
                        int(payload.is_collab),
                        payload.collaboration_name,
                        payload.status,
                        payload.notes,
                        payload.blade_profile,
                        int(payload.has_ring),
                        int(payload.is_filleting_knife),
                        int(payload.is_hatchet),
                        int(payload.is_kitchen),
                        int(payload.is_tactical),
                        payload.identifier_keywords,
                        payload.identifier_product_url,
                        payload.identifier_image_mime,
                        payload.identifier_silhouette_hu_json,
                        payload.canonical_slug,
                        payload.version,
                        payload.parent_model_id,
                        payload.first_release_date,
                        payload.last_seen_date,
                        1 if payload.is_discontinued else 0,
                        0 if payload.is_current_catalog is False else 1,
                        payload.msrp,
                        payload.blade_shape,
                        payload.tip_style,
                        payload.grind_style,
                        payload.size_class,
                        payload.primary_use_case,
                        payload.spine_profile,
                        1 if (payload.is_fillet if payload.is_fillet is not None else payload.is_filleting_knife) else 0,
                        payload.default_product_url,
                        payload.primary_image_url,
                    ),
                )
                new_id = cur.lastrowid
            except sqlite3.IntegrityError:
                raise HTTPException(status_code=400, detail="Master knife already exists.")
            return {"id": new_id, "message": "Created"}


    def _sync_inventory_from_master(
        conn: sqlite3.Connection,
        master_id: int,
        old: dict[str, Any],
        new_handle: Optional[str],
        new_steel: Optional[str],
        new_finish: Optional[str],
        new_blade_color: Optional[str],
        new_blade_len: Optional[float],
        new_is_collab: bool,
        new_collab_name: Optional[str],
    ) -> None:
        """Update inventory items that match old master defaults to new master defaults."""
        fields = [
            ("handle_color", old.get("default_handle_color"), new_handle),
            ("blade_steel", old.get("default_steel"), new_steel),
            ("blade_finish", old.get("default_blade_finish"), new_finish),
            ("blade_color", old.get("default_blade_color"), new_blade_color),
            ("blade_length", old.get("default_blade_length"), new_blade_len),
            ("is_collab", old.get("is_collab"), new_is_collab),
            ("collaboration_name", old.get("collaboration_name"), new_collab_name),
        ]
        for col, old_val, new_val in fields:
            if col == "blade_length":
                old_v = float(old_val) if old_val is not None else None
                conn.execute(
                    """
                    UPDATE inventory_items
                    SET blade_length = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE master_knife_id = ?
                      AND ((blade_length IS NULL AND ? IS NULL) OR (blade_length = ?))
                    """,
                    (new_val, master_id, old_v, old_v),
                )
            elif col == "is_collab":
                old_int = 1 if old_val else 0
                conn.execute(
                    """
                    UPDATE inventory_items
                    SET is_collab = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE master_knife_id = ?
                      AND is_collab = ?
                    """,
                    (int(new_is_collab), master_id, old_int),
                )
            else:
                conn.execute(
                    f"""
                    UPDATE inventory_items
                    SET {col} = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE master_knife_id = ?
                      AND (({col} IS NULL AND ? IS NULL) OR ({col} = ?))
                    """,
                    (new_val, master_id, old_val, old_val),
                )


    @router.put("/api/master-knives/{knife_id}")
    def update_master_knife(knife_id: int, payload: MasterKnifeIn):
        with get_conn() as conn:
            old = conn.execute(
                "SELECT default_handle_color, default_steel, default_blade_finish, default_blade_color, "
                "default_blade_length, is_collab, collaboration_name FROM master_knives WHERE id = ?",
                (knife_id,),
            ).fetchone()
            if not old:
                raise HTTPException(status_code=404, detail="Master knife not found.")
            old = dict(old)

            cur = conn.execute(
                """
                UPDATE master_knives
                SET name = ?, family = ?, record_type = ?, category = ?, catalog_line = ?, catalog_status = ?, confidence = ?,
                    evidence_summary = ?, collector_notes = ?,
                    default_blade_length = ?, default_steel = ?, default_blade_finish = ?, default_blade_color = ?,
                    default_handle_color = ?, is_collab = ?, collaboration_name = ?, status = ?, notes = ?,
                    blade_profile = ?, has_ring = ?, is_filleting_knife = ?, is_hatchet = ?, is_kitchen = ?, is_tactical = ?,
                    identifier_keywords = ?, identifier_distinguishing_features = ?, identifier_product_url = ?,
                    identifier_image_mime = ?, identifier_silhouette_hu_json = ?,
                    canonical_slug = ?, version = ?, parent_model_id = ?, first_release_date = ?, last_seen_date = ?,
                    is_discontinued = ?, is_current_catalog = ?, msrp = ?, blade_shape = ?, tip_style = ?, grind_style = ?,
                    size_class = ?, primary_use_case = ?, spine_profile = ?, is_fillet = ?,
                    default_product_url = ?, primary_image_url = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    payload.name.strip(),
                    payload.family,
                    payload.record_type,
                    payload.category,
                    payload.catalog_line,
                    payload.catalog_status,
                    payload.confidence,
                    payload.evidence_summary,
                    payload.collector_notes,
                    payload.default_blade_length,
                    payload.default_steel,
                    payload.default_blade_finish,
                    payload.default_blade_color,
                    payload.default_handle_color,
                    int(payload.is_collab),
                    payload.collaboration_name,
                    payload.status,
                    payload.notes,
                    payload.blade_profile,
                    int(payload.has_ring),
                    int(payload.is_filleting_knife),
                    int(payload.is_hatchet),
                    int(payload.is_kitchen),
                    int(payload.is_tactical),
                    payload.identifier_keywords,
                    payload.identifier_distinguishing_features,
                    payload.identifier_product_url,
                    payload.identifier_image_mime,
                    payload.identifier_silhouette_hu_json,
                    payload.canonical_slug,
                    payload.version,
                    payload.parent_model_id,
                    payload.first_release_date,
                    payload.last_seen_date,
                    1 if payload.is_discontinued else 0,
                    0 if payload.is_current_catalog is False else 1,
                    payload.msrp,
                    payload.blade_shape,
                    payload.tip_style,
                    payload.grind_style,
                    payload.size_class,
                    payload.primary_use_case,
                    payload.spine_profile,
                    1 if (payload.is_fillet if payload.is_fillet is not None else payload.is_filleting_knife) else 0,
                    payload.default_product_url,
                    payload.primary_image_url,
                    knife_id,
                ),
            )
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Master knife not found.")

            _sync_inventory_from_master(
                conn,
                knife_id,
                old,
                payload.default_handle_color,
                payload.default_steel,
                payload.default_blade_finish,
                payload.default_blade_color,
                payload.default_blade_length,
                payload.is_collab,
                payload.collaboration_name,
            )
            return {"message": "Updated"}


    def _next_duplicate_name(conn: sqlite3.Connection, base_name: str) -> str:
        """Find first available name of the form '{base_name} (n)' for n >= 2."""
        n = 2
        while True:
            candidate = f"{base_name} ({n})"
            if not conn.execute("SELECT 1 FROM master_knives WHERE name = ?", (candidate,)).fetchone():
                return candidate
            n += 1


    @router.post("/api/master-knives/{knife_id}/duplicate")
    def duplicate_master_knife(knife_id: int):
        """Copy a master record; new name defaults to '{original} (2)' (or (3), etc. if taken)."""
        with get_conn() as conn:
            row = conn.execute("SELECT * FROM master_knives WHERE id = ?", (knife_id,)).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Master knife not found.")
            base = (row.get("name") or "").strip()
            if not base:
                raise HTTPException(status_code=400, detail="Source record has no name.")
            new_name = _next_duplicate_name(conn, base)

            cols = [c["name"] for c in conn.execute("PRAGMA table_info(master_knives)").fetchall()]
            copy_cols = [c for c in cols if c not in ("id", "name", "created_at", "updated_at")]
            col_list = ", ".join(["name"] + copy_cols + ["updated_at"])
            placeholders = ", ".join(["?"] * (len(copy_cols) + 1) + ["CURRENT_TIMESTAMP"])

            override: dict[str, Any] = {"canonical_slug": None}
            vals = [new_name] + [override.get(c, row.get(c)) for c in copy_cols]

            cur = conn.execute(
                f"INSERT INTO master_knives ({col_list}) VALUES ({placeholders})",
                vals,
            )
            new_id = cur.lastrowid
            return {"id": new_id, "name": new_name, "message": "Duplicated"}


    @router.delete("/api/master-knives/{knife_id}")
    def delete_master_knife(knife_id: int):
        with get_conn() as conn:
            count = conn.execute(
                "SELECT COUNT(*) AS c FROM inventory_items WHERE master_knife_id = ?",
                (knife_id,),
            ).fetchone()["c"]
            if count > 0:
                raise HTTPException(status_code=400, detail="Cannot delete: knife is used in inventory.")
            cur = conn.execute("DELETE FROM master_knives WHERE id = ?", (knife_id,))
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Master knife not found.")
            return {"message": "Deleted"}


    @router.get("/api/inventory")
    def list_inventory():
        with get_conn() as conn:
            rows = conn.execute(
                """
                SELECT
                    i.*,
                    m.name AS knife_name,
                    m.family AS knife_family,
                    m.catalog_line AS catalog_line,
                    (CASE WHEN m.identifier_image_blob IS NOT NULL AND length(m.identifier_image_blob) > 0
                      THEN 1 ELSE 0 END) AS has_identifier_image
                FROM inventory_items i
                JOIN master_knives m ON m.id = i.master_knife_id
                ORDER BY m.name COLLATE NOCASE, i.id DESC
                """
            ).fetchall()
            return rows


    @router.post("/api/inventory")
    def create_inventory_item(payload: inventory_item_in_model):
        with get_conn() as conn:
            exists = conn.execute(
                "SELECT id FROM master_knives WHERE id = ?",
                (payload.master_knife_id,),
            ).fetchone()
            if not exists:
                raise HTTPException(status_code=400, detail="Invalid master knife id.")
            cur = conn.execute(
                """
                INSERT INTO inventory_items
                (master_knife_id, nickname, quantity, acquired_date, purchase_price, estimated_value, condition,
                 handle_color, blade_steel, blade_finish, blade_color, blade_length, is_collab, collaboration_name,
                 serial_number, location, purchase_source, last_sharpened, notes, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    payload.master_knife_id,
                    payload.nickname,
                    payload.quantity,
                    payload.acquired_date,
                    payload.purchase_price,
                    payload.estimated_value,
                    payload.condition,
                    payload.handle_color,
                    payload.blade_steel,
                    payload.blade_finish,
                    payload.blade_color,
                    payload.blade_length,
                    int(payload.is_collab),
                    payload.collaboration_name,
                    payload.serial_number,
                    payload.location,
                    payload.purchase_source,
                    payload.last_sharpened,
                    payload.notes,
                ),
            )
            return {"id": cur.lastrowid, "message": "Created"}


    @router.put("/api/inventory/{item_id}")
    def update_inventory_item(item_id: int, payload: InventoryItemIn):
        with get_conn() as conn:
            cur = conn.execute(
                """
                UPDATE inventory_items
                SET master_knife_id = ?, nickname = ?, quantity = ?, acquired_date = ?, purchase_price = ?,
                    estimated_value = ?, condition = ?, handle_color = ?, blade_steel = ?, blade_finish = ?,
                    blade_color = ?, blade_length = ?, is_collab = ?, collaboration_name = ?, serial_number = ?,
                    location = ?, purchase_source = ?, last_sharpened = ?, notes = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    payload.master_knife_id,
                    payload.nickname,
                    payload.quantity,
                    payload.acquired_date,
                    payload.purchase_price,
                    payload.estimated_value,
                    payload.condition,
                    payload.handle_color,
                    payload.blade_steel,
                    payload.blade_finish,
                    payload.blade_color,
                    payload.blade_length,
                    int(payload.is_collab),
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


    @router.delete("/api/inventory/{item_id}")
    def delete_inventory_item(item_id: int):
        with get_conn() as conn:
            cur = conn.execute("DELETE FROM inventory_items WHERE id = ?", (item_id,))
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Inventory item not found.")
            return {"message": "Deleted"}


    @router.get("/api/inventory/export.csv")
    def export_inventory_csv():
        with get_conn() as conn:
            rows = conn.execute(
                """
                SELECT
                    i.id,
                    m.name AS knife_name,
                    m.family AS knife_family,
                    i.master_knife_id,
                    i.nickname,
                    i.quantity,
                    i.acquired_date,
                    i.purchase_price,
                    i.estimated_value,
                    i.condition,
                    i.handle_color,
                    i.blade_steel,
                    i.blade_finish,
                    i.blade_color,
                    i.blade_length,
                    i.is_collab,
                    i.collaboration_name,
                    i.serial_number,
                    i.location,
                    i.purchase_source,
                    i.last_sharpened,
                    i.notes,
                    i.created_at,
                    i.updated_at
                FROM inventory_items i
                JOIN master_knives m ON m.id = i.master_knife_id
                ORDER BY m.name COLLATE NOCASE, i.id DESC
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
        data = buffer.getvalue()
        return Response(
            content=data.encode("utf-8"),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": 'attachment; filename="mkc_inventory.csv"'},
        )


    @router.post("/api/inventory/{item_id}/duplicate")
    def duplicate_inventory_item(item_id: int):
        with get_conn() as conn:
            row = conn.execute("SELECT * FROM inventory_items WHERE id = ?", (item_id,)).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Inventory item not found.")
            nick = (row.get("nickname") or "").strip()
            new_nick = f"{nick} (copy)" if nick else "Copy"
            cur = conn.execute(
                """
                INSERT INTO inventory_items
                (master_knife_id, nickname, quantity, acquired_date, purchase_price, estimated_value, condition,
                 handle_color, blade_steel, blade_finish, blade_color, blade_length, is_collab, collaboration_name,
                 serial_number, location, purchase_source, last_sharpened, notes, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    row["master_knife_id"],
                    new_nick,
                    row["quantity"],
                    row["acquired_date"],
                    row["purchase_price"],
                    row["estimated_value"],
                    row["condition"],
                    row["handle_color"],
                    row["blade_steel"],
                    row["blade_finish"],
                    row["blade_color"],
                    row["blade_length"],
                    int(row["is_collab"]),
                    row["collaboration_name"],
                    None,
                    row["location"],
                    row["purchase_source"],
                    row["last_sharpened"],
                    row["notes"],
                ),
            )
            return {"id": cur.lastrowid, "message": "Duplicated"}


    @router.get("/api/derive-blade-family")
    def derive_blade_family(name: Optional[str] = None):
        """Return derived blade family for a model name. Used for auto-suggest when editing name."""
        return {"family": derive_blade_family_from_name(name)}


    @router.get("/api/options")
    def get_options():
        with get_conn() as conn:
            result: dict[str, list[dict[str, Any]]] = {}
            for key, table in OPTION_TABLES.items():
                if key == "blade-families":
                    tbl_rows = conn.execute(
                        "SELECT * FROM option_blade_families ORDER BY name COLLATE NOCASE"
                    ).fetchall()
                    masters = conn.execute(
                        "SELECT DISTINCT name FROM master_knives WHERE name IS NOT NULL"
                    ).fetchall()
                    derived = {derive_blade_family_from_name(r["name"]) for r in masters if r["name"]}
                    derived = {f for f in derived if f}
                    from_table = {r["name"] for r in tbl_rows}
                    combined = sorted(from_table | derived, key=lambda x: x.lower())
                    result[key] = [{"id": n, "name": n} for n in combined]
                else:
                    result[key] = conn.execute(
                        f"SELECT * FROM {table} ORDER BY name COLLATE NOCASE"
                    ).fetchall()
            return result


    @router.get("/api/inventory/options")
    def get_inventory_options(master_knife_id: Optional[int] = None):
        """Return option lists for inventory form (all options)."""
        with get_conn() as conn:
            result: dict[str, list[dict[str, Any]]] = {}
            for key, table in OPTION_TABLES.items():
                result[key] = conn.execute(
                    f"SELECT * FROM {table} ORDER BY name COLLATE NOCASE"
                ).fetchall()
            result["_filtered"] = False  # All options shown (allowed variants removed)
            return result


    @router.post("/api/options/{option_type}")
    def add_option(option_type: str, payload: OptionIn):
        table = OPTION_TABLES.get(option_type)
        if not table:
            raise HTTPException(status_code=404, detail="Unknown option type.")
        with get_conn() as conn:
            try:
                cur = conn.execute(
                    f"INSERT INTO {table} (name) VALUES (?)",
                    (payload.name.strip(),),
                )
            except sqlite3.IntegrityError:
                raise HTTPException(status_code=400, detail="Option already exists.")
            return {"id": cur.lastrowid, "message": "Created"}


    @router.delete("/api/options/{option_type}/{option_id}")
    def delete_option(option_type: str, option_id: int):
        table = OPTION_TABLES.get(option_type)
        if not table:
            raise HTTPException(status_code=404, detail="Unknown option type.")
        with get_conn() as conn:
            cur = conn.execute(f"DELETE FROM {table} WHERE id = ?", (option_id,))
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Option not found.")
            return {"message": "Deleted"}
    return router
