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
        CREATE TABLE IF NOT EXISTS handle_colors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS blade_colors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS blade_steels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS blade_finishes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS conditions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        );

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

        CREATE TABLE IF NOT EXISTS knife_model_image_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            model_slug TEXT NOT NULL,
            color_name TEXT,
            filename TEXT NOT NULL UNIQUE,
            url_path TEXT NOT NULL,
            is_primary INTEGER NOT NULL DEFAULT 0,
            sha256 TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_kmif_slug ON knife_model_image_files(model_slug);
        CREATE INDEX IF NOT EXISTS idx_kmif_slug_color ON knife_model_image_files(model_slug, color_name);

        CREATE TABLE IF NOT EXISTS model_colorways (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            knife_model_id  INTEGER NOT NULL REFERENCES knife_models_v2(id),
            handle_color_id INTEGER NOT NULL REFERENCES handle_colors(id),
            blade_color_id  INTEGER REFERENCES blade_colors(id),
            image_blob      BLOB,
            is_transparent  INTEGER NOT NULL DEFAULT 0,
            created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE UNIQUE INDEX IF NOT EXISTS uq_model_colorway
            ON model_colorways (knife_model_id, handle_color_id, COALESCE(blade_color_id, -1));
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
    """
    Normalize and backfill v2 identity dimensions using normalized-model heuristics.

    Idempotent. Fills missing values and fixes malformed legacy-like values.
    """
    rows = conn.execute(
        """
        SELECT km.id, km.official_name, km.normalized_name, km.generation_label, km.size_modifier, km.platform_variant,
               km.type_id, km.form_id, km.family_id, km.series_id, km.collaborator_id,
               kt.name AS type_name, frm.name AS form_name, fam.name AS family_name,
               ks.name AS series_name, c.name AS collaborator_name
        FROM knife_models_v2 km
        LEFT JOIN knife_types kt ON kt.id = km.type_id
        LEFT JOIN knife_forms frm ON frm.id = km.form_id
        LEFT JOIN knife_families fam ON fam.id = km.family_id
        LEFT JOIN knife_series ks ON ks.id = km.series_id
        LEFT JOIN collaborators c ON c.id = km.collaborator_id
        ORDER BY km.id
        """
    ).fetchall()
    updated = 0
    inferred = 0

    canonical_series_names = {
        "traditions": "Traditions",
        "vip": "VIP",
        "ultra": "Ultra",
        "blood brothers": "Blood Brothers",
    }
    collab_like_series = {
        "archery country": "Archery Country",
        "archery": "Archery Country",
        "bearded butchers": "Bearded Butchers",
        "meat church": "Meat Church",
        "nock on": "Nock On",
    }
    generic_family_labels = {
        "hunting", "culinary", "tactical", "everyday carry", "edc", "bushcraft",
        "bushcraft & camp", "camp", "utility", "work", "kitchen", "fillet", "skinner",
    }

    def _dim_id(table: str, name: Optional[str]) -> Optional[int]:
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
            cur = conn.execute("INSERT INTO knife_forms (name, slug) VALUES (?, ?)",
                               (n, normalized_model.slugify(n)))
            return cur.lastrowid
        if table == "knife_families":
            cur = conn.execute(
                "INSERT INTO knife_families (name, normalized_name, slug) VALUES (?, ?, ?)",
                (n, n, normalized_model.slugify(n)),
            )
            return cur.lastrowid
        if table == "knife_series":
            cur = conn.execute("INSERT INTO knife_series (name, slug) VALUES (?, ?)",
                               (n, normalized_model.slugify(n)))
            return cur.lastrowid
        if table == "collaborators":
            cur = conn.execute("INSERT INTO collaborators (name, slug) VALUES (?, ?)",
                               (n, normalized_model.slugify(n)))
            return cur.lastrowid
        return None

    def _is_generic_or_legacy(v: Optional[str]) -> bool:
        if not v or not str(v).strip():
            return True
        s = str(v).strip()
        return "/" in s or s.lower() in generic_family_labels

    def _is_lowercase_word(v: Optional[str]) -> bool:
        if not v or not str(v).strip():
            return False
        s = str(v).strip()
        return any(ch.isalpha() for ch in s) and s == s.lower()

    for row in rows:
        official = (row.get("official_name") or "").strip()
        normalized = (row.get("normalized_name") or official).strip()
        if not official:
            continue

        current_type = (row.get("type_name") or "").strip()
        current_form = (row.get("form_name") or "").strip()
        current_family = (row.get("family_name") or "").strip()
        current_series = (row.get("series_name") or "").strip()
        current_collaborator = (row.get("collaborator_name") or "").strip()

        normalized_type = normalized_model.normalize_category_value(current_type) if current_type else None
        if not normalized_type:
            normalized_type = normalized_model.detect_type(None, current_family, 0, 0, 0, normalized)

        series_guess = (normalized_model.detect_series(official, current_series or None) or current_series or "").strip() or None
        family_guess = normalized_model.detect_family(normalized)
        form_guess = normalized_model.detect_form(
            normalized, current_form or None, None, None, normalized_type or "Hunting"
        )
        collab_guess = normalized_model.detect_collaborator(
            1 if (series_guess and series_guess.lower() in collab_like_series) else 0,
            current_collaborator or None,
            series_guess,
        )
        if collab_guess and collab_guess.strip().lower() in {"nock on", "nock on archery", "knock on archery"}:
            collab_guess = "Cam Hanes"
        if series_guess:
            sk = series_guess.lower()
            if sk in canonical_series_names:
                series_guess = canonical_series_names[sk]
            elif sk in collab_like_series:
                if not collab_guess:
                    collab_guess = collab_like_series[sk]
                series_guess = None

        needs_type = row.get("type_id") is None or (normalized_type and normalized_type != current_type)
        needs_form = row.get("form_id") is None or _is_generic_or_legacy(current_form)
        needs_family = (
            row.get("family_id") is None
            or _is_generic_or_legacy(current_family)
            or _is_lowercase_word(current_family)
        )
        needs_series = row.get("series_id") is None or bool(current_series and current_series.lower() in collab_like_series)
        needs_collab = row.get("collaborator_id") is None and bool(collab_guess)
        needs_gen = not (row.get("generation_label") or "").strip()
        needs_size = not (row.get("size_modifier") or "").strip()
        needs_platform = not (row.get("platform_variant") or "").strip()

        if not any((needs_type, needs_form, needs_family, needs_series, needs_collab, needs_gen, needs_size, needs_platform)):
            continue

        _, _, _, size_or_generation, platform_guess = normalized_model.normalize_model_name(official, series_guess)
        if platform_guess and str(platform_guess).strip().lower() == "ultra":
            series_guess = "Ultra"
            needs_series = True
            platform_guess = None

        type_id = row.get("type_id")
        form_id = row.get("form_id")
        family_id = row.get("family_id")
        series_id = row.get("series_id")
        collaborator_id = row.get("collaborator_id")
        generation_label = row.get("generation_label")
        size_modifier = row.get("size_modifier")
        platform_variant = row.get("platform_variant")

        if needs_type and normalized_type:
            type_id = _dim_id("knife_types", normalized_type)
            inferred += 1
        if needs_form and form_guess:
            form_id = _dim_id("knife_forms", form_guess)
            inferred += 1
        if needs_family and family_guess:
            family_id = _dim_id("knife_families", family_guess)
            inferred += 1
        if needs_series and series_guess:
            series_id = _dim_id("knife_series", series_guess)
            inferred += 1
        if needs_collab and collab_guess:
            collaborator_id = _dim_id("collaborators", collab_guess)
            inferred += 1
        if needs_gen and size_or_generation and str(size_or_generation).replace(".", "", 1).isdigit():
            generation_label = str(size_or_generation)
            inferred += 1
        if needs_size and size_or_generation and not str(size_or_generation).replace(".", "", 1).isdigit():
            size_modifier = str(size_or_generation)
            inferred += 1
        if needs_platform and platform_guess:
            platform_variant = platform_guess
            inferred += 1

        conn.execute(
            """
            UPDATE knife_models_v2
            SET type_id = COALESCE(?, type_id),
                form_id = COALESCE(?, form_id),
                family_id = COALESCE(?, family_id),
                series_id = COALESCE(?, series_id),
                collaborator_id = COALESCE(?, collaborator_id),
                generation_label = COALESCE(?, generation_label),
                size_modifier = COALESCE(?, size_modifier),
                platform_variant = COALESCE(?, platform_variant),
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (type_id, form_id, family_id, series_id, collaborator_id,
             generation_label, size_modifier, platform_variant, row["id"]),
        )
        updated += 1
    return {"rows_updated": updated, "values_inferred": inferred}


def normalize_v2_additional_fields(conn: sqlite3.Connection) -> dict[str, int]:
    """
    Normalize additional non-category fields: collaborator/series alias cleanup,
    model + inventory attribute text normalization (steel, finish, colors, condition).
    Idempotent.
    """
    changes = {
        "series_alias_merged": 0,
        "collaborator_alias_merged": 0,
        "series_collab_reclassified": 0,
        "ultra_platform_to_series": 0,
        "ultra_platform_cleared": 0,
        "model_attr_rows_updated": 0,
        "inventory_attr_rows_updated": 0,
    }

    def _collapse(v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        s = " ".join(str(v).strip().split())
        return s or None

    def _norm_map(v: Optional[str], mapping: dict[str, str]) -> Optional[str]:
        s = _collapse(v)
        if s is None:
            return None
        return mapping.get(s.lower(), s)

    def _ensure_dim(table: str, name: Optional[str]) -> Optional[int]:
        if not name or not str(name).strip():
            return None
        n = str(name).strip()
        row = conn.execute(f"SELECT id FROM {table} WHERE lower(name) = lower(?) LIMIT 1", (n,)).fetchone()
        if row:
            return row["id"]
        if table == "knife_series":
            cur = conn.execute("INSERT INTO knife_series (name, slug) VALUES (?, ?)",
                               (n, normalized_model.slugify(n)))
            return cur.lastrowid
        if table == "collaborators":
            cur = conn.execute("INSERT INTO collaborators (name, slug) VALUES (?, ?)",
                               (n, normalized_model.slugify(n)))
            return cur.lastrowid
        return None

    def _merge_dim_aliases(table: str, fk_col: str, aliases: dict[str, str]) -> int:
        merged = 0
        for alias, canonical in aliases.items():
            a = conn.execute(
                f"SELECT id, name, slug FROM {table} WHERE lower(name) = lower(?) LIMIT 1", (alias,)
            ).fetchone()
            if not a:
                continue
            c = conn.execute(
                f"SELECT id, name, slug FROM {table} WHERE lower(name) = lower(?) LIMIT 1", (canonical,)
            ).fetchone()
            canonical_slug = normalized_model.slugify(canonical)
            if not c:
                c = conn.execute(
                    f"SELECT id, name, slug FROM {table} WHERE slug = ? LIMIT 1", (canonical_slug,)
                ).fetchone()
            if c and c["id"] != a["id"]:
                conn.execute(f"UPDATE knife_models_v2 SET {fk_col} = ? WHERE {fk_col} = ?", (c["id"], a["id"]))
                conn.execute(f"DELETE FROM {table} WHERE id = ?", (a["id"],))
                merged += 1
            elif not c:
                try:
                    conn.execute(f"UPDATE {table} SET name = ?, slug = ? WHERE id = ?",
                                 (canonical, canonical_slug, a["id"]))
                    merged += 1
                except sqlite3.IntegrityError:
                    conflict = conn.execute(
                        f"SELECT id FROM {table} WHERE (lower(name) = lower(?) OR slug = ?) AND id != ? LIMIT 1",
                        (canonical, canonical_slug, a["id"]),
                    ).fetchone()
                    if conflict:
                        conn.execute(f"UPDATE knife_models_v2 SET {fk_col} = ? WHERE {fk_col} = ?",
                                     (conflict["id"], a["id"]))
                        conn.execute(f"DELETE FROM {table} WHERE id = ?", (a["id"],))
                        merged += 1
                    else:
                        i = 2
                        slug = canonical_slug
                        while conn.execute(f"SELECT 1 FROM {table} WHERE slug = ? AND id != ?",
                                           (slug, a["id"])).fetchone():
                            slug = f"{canonical_slug}-{i}"
                            i += 1
                        conn.execute(f"UPDATE {table} SET name = ?, slug = ? WHERE id = ?",
                                     (canonical, slug, a["id"]))
                        merged += 1
        return merged

    series_aliases = {"nock on archery": "Nock On", "knock on archery": "Nock On"}
    collaborator_aliases = {
        "bearded butcher": "Bearded Butchers",
        "nock on archery": "Cam Hanes",
        "knock on archery": "Cam Hanes",
        "nock on": "Cam Hanes",
    }
    changes["series_alias_merged"] = _merge_dim_aliases("knife_series", "series_id", series_aliases)
    changes["collaborator_alias_merged"] = _merge_dim_aliases("collaborators", "collaborator_id", collaborator_aliases)

    ultra_series_id = _ensure_dim("knife_series", "Ultra")
    if ultra_series_id is not None:
        cur = conn.execute(
            """
            UPDATE knife_models_v2
            SET series_id = ?, updated_at = CURRENT_TIMESTAMP
            WHERE lower(trim(COALESCE(platform_variant, ''))) = 'ultra'
              AND (series_id IS NULL OR series_id != ?)
            """,
            (ultra_series_id, ultra_series_id),
        )
        changes["ultra_platform_to_series"] = cur.rowcount or 0
        cur = conn.execute(
            "UPDATE knife_models_v2 SET platform_variant = NULL, updated_at = CURRENT_TIMESTAMP "
            "WHERE lower(trim(COALESCE(platform_variant, ''))) = 'ultra'"
        )
        changes["ultra_platform_cleared"] = cur.rowcount or 0

    for bad_series, collab_name in (
        ("Meat Church", "Meat Church"), ("Archery Country", "Archery Country"), ("Archery", "Archery Country")
    ):
        bad = conn.execute(
            "SELECT id FROM knife_series WHERE lower(name) = lower(?) LIMIT 1", (bad_series,)
        ).fetchone()
        if not bad:
            continue
        collab_id = _ensure_dim("collaborators", collab_name)
        cur = conn.execute(
            "UPDATE knife_models_v2 SET collaborator_id = COALESCE(collaborator_id, ?), "
            "series_id = NULL, updated_at = CURRENT_TIMESTAMP WHERE series_id = ?",
            (collab_id, bad["id"]),
        )
        changes["series_collab_reclassified"] += cur.rowcount or 0

    steel_map = {"magnacut": "MagnaCut", "magna cut": "MagnaCut", "aebl": "AEB-L", "aeb-l": "AEB-L", "440c": "440C"}
    finish_map = {
        "stonewashed": "Stonewashed", "pvd": "PVD", "cerakote": "Cerakote", "polished": "Polished",
        "satin": "Satin", "black parkerized": "Black Parkerized", "working grind": "Working Grind", "etched": "Etched",
    }
    blade_color_map = {
        "black": "Black", "steel": "Steel", "distressed gray": "Distressed Gray",
        "red": "Red", "damascus wood grain": "Damascus Wood Grain",
    }
    handle_color_map = {"black": "Black", "red": "Red", "carbon fiber": "Carbon Fiber", "desert ironwood": "Desert Ironwood"}
    condition_map = {"new": "New", "like new": "Like New", "very good": "Very Good", "good": "Good", "user": "User"}

    for row in conn.execute("SELECT id, steel, blade_finish, blade_color, handle_color FROM knife_models_v2").fetchall():
        steel = _norm_map(row.get("steel"), steel_map)
        finish = _norm_map(row.get("blade_finish"), finish_map)
        blade_color = _norm_map(row.get("blade_color"), blade_color_map)
        handle_color = _norm_map(row.get("handle_color"), handle_color_map)
        if (steel, finish, blade_color, handle_color) != (
            _collapse(row.get("steel")), _collapse(row.get("blade_finish")),
            _collapse(row.get("blade_color")), _collapse(row.get("handle_color")),
        ):
            conn.execute(
                "UPDATE knife_models_v2 SET steel=?, blade_finish=?, blade_color=?, handle_color=?, "
                "updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (steel, finish, blade_color, handle_color, row["id"]),
            )
            changes["model_attr_rows_updated"] += 1

    for row in conn.execute(
        "SELECT id, steel, blade_finish, blade_color, handle_color, condition FROM inventory_items_v2"
    ).fetchall():
        steel = _norm_map(row.get("steel"), steel_map)
        finish = _norm_map(row.get("blade_finish"), finish_map)
        blade_color = _norm_map(row.get("blade_color"), blade_color_map)
        handle_color = _norm_map(row.get("handle_color"), handle_color_map)
        condition = _norm_map(row.get("condition"), condition_map)
        if (steel, finish, blade_color, handle_color, condition) != (
            _collapse(row.get("steel")), _collapse(row.get("blade_finish")),
            _collapse(row.get("blade_color")), _collapse(row.get("handle_color")),
            _collapse(row.get("condition")),
        ):
            conn.execute(
                "UPDATE inventory_items_v2 SET steel=?, blade_finish=?, blade_color=?, handle_color=?, "
                "condition=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (steel, finish, blade_color, handle_color, condition, row["id"]),
            )
            changes["inventory_attr_rows_updated"] += 1

    return changes


if __name__ == "__main__":
    import argparse
    import sys
    from pathlib import Path

    _REPO_ROOT = Path(__file__).parent.parent
    _DB_PATH = _REPO_ROOT / "data" / "mkc_inventory.db"

    parser = argparse.ArgumentParser(description="Run all v2 migration steps.")
    parser.add_argument("--db", default=str(_DB_PATH), help="Path to SQLite DB")
    args = parser.parse_args()

    import sqlite3 as _sqlite3

    def _row_factory(cursor, row):
        return {col[0]: row[i] for i, col in enumerate(cursor.description)}

    conn = _sqlite3.connect(args.db)
    conn.row_factory = _row_factory
    conn.execute("PRAGMA foreign_keys = ON")

    print(f"DB: {args.db}\n")
    print("ensure_phase1_schema ...")
    ensure_phase1_schema(conn)
    print("ensure_v2_exclusive_schema ...")
    ensure_v2_exclusive_schema(conn)
    print("migrate_legacy_media_to_v2 ...")
    r = migrate_legacy_media_to_v2(conn)
    print(f"  images_copied={r['images_copied']}  descriptors_copied={r['descriptors_copied']}")
    print("backfill_v2_model_identity ...")
    r = backfill_v2_model_identity(conn)
    print(f"  rows_updated={r['rows_updated']}  values_inferred={r['values_inferred']}")
    print("normalize_v2_additional_fields ...")
    r = normalize_v2_additional_fields(conn)
    print(f"  {r}")
    conn.commit()
    print("\nDONE")
    sys.exit(0)
