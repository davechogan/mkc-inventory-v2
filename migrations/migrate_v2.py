"""Migration helpers extracted from app.py for v2 normalization and media migration.

This module is callable from admin scripts and is intentionally separated from
the runtime `app.py` to keep startup lightweight.
"""
from __future__ import annotations

import logging
import re
import sqlite3
from typing import Any, Optional

import normalized_model
from sqlite_schema import column_exists

logger = logging.getLogger("mkc_app.migrations")


def ensure_v2_exclusive_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS knife_model_images (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            knife_model_id INTEGER NOT NULL UNIQUE,
            image_blob BLOB,
            image_mime TEXT,
            silhouette_hu_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(knife_model_id) REFERENCES knife_models_v2(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS knife_model_descriptors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            knife_model_id INTEGER NOT NULL UNIQUE,
            distinguishing_features TEXT,
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(knife_model_id) REFERENCES knife_models_v2(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS v2_option_values (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            option_type TEXT NOT NULL,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(option_type, name)
        );
        """
    )

    if not column_exists(conn, "inventory_items_v2", "blade_length"):
        conn.execute("ALTER TABLE inventory_items_v2 ADD COLUMN blade_length REAL")
    if not column_exists(conn, "inventory_items_v2", "mkc_order_number"):
        conn.execute("ALTER TABLE inventory_items_v2 ADD COLUMN mkc_order_number TEXT")
    if not column_exists(conn, "knife_models_v2", "handle_type"):
        conn.execute("ALTER TABLE knife_models_v2 ADD COLUMN handle_type TEXT")

    seed_sql = {
        "blade-steels": "SELECT DISTINCT steel AS v FROM knife_models_v2 WHERE steel IS NOT NULL AND trim(steel) != ''",
        "blade-finishes": "SELECT DISTINCT blade_finish AS v FROM knife_models_v2 WHERE blade_finish IS NOT NULL AND trim(blade_finish) != ''",
        "blade-colors": "SELECT DISTINCT blade_color AS v FROM knife_models_v2 WHERE blade_color IS NOT NULL AND trim(blade_color) != ''",
        "handle-colors": (
            "SELECT DISTINCT v FROM ("
            "  SELECT trim(handle_color) AS v FROM knife_models_v2 "
            "  WHERE handle_color IS NOT NULL AND trim(handle_color) != '' "
            "  UNION "
            "  SELECT trim(handle_color) AS v FROM inventory_items_v2 "
            "  WHERE handle_color IS NOT NULL AND trim(handle_color) != ''"
            ")"
        ),
        "conditions": "SELECT DISTINCT condition AS v FROM inventory_items_v2 WHERE condition IS NOT NULL AND trim(condition) != ''",
        "handle-types": "SELECT DISTINCT handle_type AS v FROM knife_models_v2 WHERE handle_type IS NOT NULL AND trim(handle_type) != ''",
        "blade-types": "SELECT DISTINCT name AS v FROM option_blade_types WHERE name IS NOT NULL AND trim(name) != ''",
        "categories": "SELECT DISTINCT name AS v FROM option_categories WHERE name IS NOT NULL AND trim(name) != ''",
        "blade-families": "SELECT DISTINCT name AS v FROM option_blade_families WHERE name IS NOT NULL AND trim(name) != ''",
        "primary-use-cases": "SELECT DISTINCT name AS v FROM option_primary_use_cases WHERE name IS NOT NULL AND trim(name) != ''",
        "collaborators": "SELECT DISTINCT name AS v FROM collaborators WHERE name IS NOT NULL AND trim(name) != ''",
        "generations": "SELECT DISTINCT generation_label AS v FROM knife_models_v2 WHERE generation_label IS NOT NULL AND trim(generation_label) != ''",
        "size-modifiers": "SELECT DISTINCT size_modifier AS v FROM knife_models_v2 WHERE size_modifier IS NOT NULL AND trim(size_modifier) != ''",
        "platform-variants": "SELECT DISTINCT platform_variant AS v FROM knife_models_v2 WHERE platform_variant IS NOT NULL AND trim(platform_variant) != ''",
    }
    for option_type, sql in seed_sql.items():
        rows = conn.execute(sql).fetchall()
        for row in rows:
            v = (row.get("v") or "").strip()
            if not v:
                continue
            conn.execute(
                "INSERT OR IGNORE INTO v2_option_values (option_type, name) VALUES (?, ?)",
                (option_type, v),
            )


def migrate_legacy_media_to_v2(conn: sqlite3.Connection) -> dict[str, int]:
    models = conn.execute(
        """
        SELECT id, legacy_master_id
        FROM knife_models_v2
        WHERE legacy_master_id IS NOT NULL
        """
    ).fetchall()
    images_copied = 0
    descriptors_copied = 0
    for m in models:
        master = conn.execute(
            """
            SELECT identifier_image_blob, identifier_image_mime, identifier_silhouette_hu_json,
                   identifier_distinguishing_features
            FROM master_knives
            WHERE id = ?
            """,
            (m["legacy_master_id"],),
        ).fetchone()
        if not master:
            continue

        has_blob = bool(master.get("identifier_image_blob"))
        if has_blob:
            existing_img = conn.execute(
                "SELECT image_blob, image_mime, silhouette_hu_json FROM knife_model_images WHERE knife_model_id = ?",
                (m["id"],),
            ).fetchone()
            should_write_img = (
                not existing_img
                or not existing_img.get("image_blob")
                or not existing_img.get("image_mime")
                or (
                    (master.get("identifier_silhouette_hu_json") or "").strip()
                    and not (existing_img.get("silhouette_hu_json") or "").strip()
                )
            )
            if should_write_img:
                conn.execute(
                    """
                    INSERT INTO knife_model_images
                    (knife_model_id, image_blob, image_mime, silhouette_hu_json, updated_at)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(knife_model_id) DO UPDATE SET
                        image_blob = COALESCE(knife_model_images.image_blob, excluded.image_blob),
                        image_mime = COALESCE(knife_model_images.image_mime, excluded.image_mime),
                        silhouette_hu_json = COALESCE(knife_model_images.silhouette_hu_json, excluded.silhouette_hu_json),
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        m["id"],
                        master.get("identifier_image_blob"),
                        master.get("identifier_image_mime") or "image/jpeg",
                        master.get("identifier_silhouette_hu_json"),
                    ),
                )
                images_copied += 1

        legacy_features = (master.get("identifier_distinguishing_features") or "").strip()
        if legacy_features:
            existing_desc = conn.execute(
                "SELECT distinguishing_features FROM knife_model_descriptors WHERE knife_model_id = ?",
                (m["id"],),
            ).fetchone()
            if not existing_desc or not (existing_desc.get("distinguishing_features") or "").strip():
                conn.execute(
                    """
                    INSERT INTO knife_model_descriptors
                    (knife_model_id, distinguishing_features, updated_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(knife_model_id) DO UPDATE SET
                        distinguishing_features = COALESCE(knife_model_descriptors.distinguishing_features, excluded.distinguishing_features),
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (m["id"], legacy_features),
                )
                descriptors_copied += 1

    return {"images_copied": images_copied, "descriptors_copied": descriptors_copied}


def ensure_phase1_schema(conn: sqlite3.Connection) -> None:
    PHASE1_MASTER_COLUMNS = [
        ("canonical_slug", "TEXT"),
        ("version", "TEXT"),
        ("parent_model_id", "INTEGER"),
        ("first_release_date", "TEXT"),
        ("last_seen_date", "TEXT"),
        ("is_discontinued", "INTEGER DEFAULT 0"),
        ("is_current_catalog", "INTEGER DEFAULT 1"),
        ("msrp", "REAL"),
        ("blade_shape", "TEXT"),
        ("tip_style", "TEXT"),
        ("edge_style", "TEXT"),
        ("grind_style", "TEXT"),
        ("size_class", "TEXT"),
        ("overall_size", "TEXT"),
        ("primary_use_case", "TEXT"),
        ("spine_profile", "TEXT"),
        ("has_ring", "INTEGER DEFAULT 0"),
        ("is_fillet", "INTEGER DEFAULT 0"),
        ("is_hatchet", "INTEGER DEFAULT 0"),
        ("default_product_url", "TEXT"),
        ("primary_image_url", "TEXT"),
    ]
    PHASE1_MAPPING_TABLES = [
        ("master_knife_allowed_handle_colors", "handle_color_id", "option_handle_colors"),
        ("master_knife_allowed_blade_steels", "blade_steel_id", "option_blade_steels"),
        ("master_knife_allowed_blade_finishes", "blade_finish_id", "option_blade_finishes"),
        ("master_knife_allowed_blade_colors", "blade_color_id", "option_blade_colors"),
    ]

    for col, sql_type in PHASE1_MASTER_COLUMNS:
        if not column_exists(conn, "master_knives", col):
            conn.execute(f"ALTER TABLE master_knives ADD COLUMN {col} {sql_type}")

    for table, id_col, option_table in PHASE1_MAPPING_TABLES:
        if table_exists(conn := conn, table):
            continue
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                master_knife_id INTEGER NOT NULL,
                {id_col} INTEGER NOT NULL,
                FOREIGN KEY (master_knife_id) REFERENCES master_knives(id) ON DELETE CASCADE,
                FOREIGN KEY ({id_col}) REFERENCES {option_table}(id) ON DELETE CASCADE,
                UNIQUE(master_knife_id, {id_col})
            )
            """
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS ix_{table}_master ON {table}(master_knife_id)"
        )

    rows = conn.execute("SELECT id, name, canonical_slug FROM master_knives").fetchall()
    used_slugs: set[str] = set()
    for row in rows:
        rid = row["id"]
        name = row["name"]
        existing_slug = row.get("canonical_slug")
        if existing_slug and str(existing_slug).strip():
            used_slugs.add(str(existing_slug).strip().lower())
            continue
        base = re.sub(r"[^\w\s-]", "", (name or "").lower().strip())
        base = re.sub(r"\s+", "-", base)
        base = re.sub(r"-+", "-", base).strip("-")
        if not base:
            continue
        slug = base
        n = 2
        while slug.lower() in used_slugs:
            slug = f"{base}-{n}"
            n += 1
        used_slugs.add(slug.lower())
        conn.execute(
            "UPDATE master_knives SET canonical_slug = ? WHERE id = ?",
            (slug, rid),
        )

    try:
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_master_knives_canonical_slug "
            "ON master_knives(canonical_slug) WHERE canonical_slug IS NOT NULL AND canonical_slug != ''"
        )
    except sqlite3.OperationalError:
        pass

    conn.execute(
        """
        UPDATE master_knives
        SET is_discontinued = 0, is_current_catalog = 1
        WHERE is_discontinued IS NULL OR is_current_catalog IS NULL
        """
    )


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone() is not None


def ensure_version_parent_model_columns(conn: sqlite3.Connection) -> None:
    for col, sql_type, desc in [
        ("version", "TEXT", "version (TEXT, nullable)"),
        ("parent_model_id", "INTEGER", "parent_model_id (INTEGER, nullable, references master_knives.id)"),
    ]:
        if column_exists(conn, "master_knives", col):
            logger.debug("[migration] master_knives.%s: already existed", col)
        else:
            conn.execute(f"ALTER TABLE master_knives ADD COLUMN {col} {sql_type}")
            logger.info("[migration] master_knives.%s: added (%s)", col, desc)


def ensure_identifier_columns(conn: sqlite3.Connection) -> None:
    IDENTIFIER_COLUMNS = {
        "category": "TEXT",
        "blade_profile": "TEXT",
        "blade_shape": "TEXT",
        "has_ring": "INTEGER NOT NULL DEFAULT 0",
        "is_filleting_knife": "INTEGER NOT NULL DEFAULT 0",
        "is_hatchet": "INTEGER NOT NULL DEFAULT 0",
        "is_kitchen": "INTEGER NOT NULL DEFAULT 0",
        "is_tactical": "INTEGER NOT NULL DEFAULT 0",
        "identifier_keywords": "TEXT",
    }
    for column, sql_type in IDENTIFIER_COLUMNS.items():
        if not column_exists(conn, "master_knives", column):
            conn.execute(f"ALTER TABLE master_knives ADD COLUMN {column} {sql_type}")


def ensure_master_extra_columns(conn: sqlite3.Connection) -> None:
    if not column_exists(conn, "master_knives", "default_handle_color"):
        conn.execute("ALTER TABLE master_knives ADD COLUMN default_handle_color TEXT")


def ensure_master_catalog_columns(conn: sqlite3.Connection) -> None:
    MASTER_CATALOG_TEXT_COLUMNS = {
        "record_type": "TEXT",
        "catalog_status": "TEXT",
        "confidence": "TEXT",
        "evidence_summary": "TEXT",
        "collector_notes": "TEXT",
    }
    for column, sql_type in MASTER_CATALOG_TEXT_COLUMNS.items():
        if not column_exists(conn, "master_knives", column):
            conn.execute(f"ALTER TABLE master_knives ADD COLUMN {column} {sql_type}")


def backfill_v2_model_identity(conn: sqlite3.Connection) -> dict[str, int]:
    # reuse normalized_model.normalize/backfill helpers where practical
    return normalized_model.backfill_v2_model_identity(conn) if hasattr(normalized_model, 'backfill_v2_model_identity') else {"rows_updated":0, "values_inferred":0}


def normalize_v2_additional_fields(conn: sqlite3.Connection) -> dict[str, int]:
    # For now reuse normalized_model functionality if available, else provide a no-op.
    if hasattr(normalized_model, 'normalize_v2_additional_fields'):
        return normalized_model.normalize_v2_additional_fields(conn)
    return {"model_attr_rows_updated":0, "inventory_attr_rows_updated":0}
