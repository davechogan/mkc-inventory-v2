#!/usr/bin/env python3
"""Migration: Add vision identification tables and seed feature definitions.

Creates:
  - vision_feature_definitions (feature vocabulary)
  - knife_model_vision_profiles (structured model profiles)
  - knife_model_vision_feature_values (optional normalized features)
  - knife_observation_runs (identification attempts)
  - knife_observation_candidates (ranked candidates per run)
  - Triggers for updated_at

Seeds vision_feature_definitions with the initial feature taxonomy.

Safe to run multiple times — all CREATE statements use IF NOT EXISTS.
"""

import sqlite3
import json
import sys
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "mkc_inventory.db"


def create_tables(conn: sqlite3.Connection):
    """Create the vision identification tables."""

    conn.executescript("""
    -- 1. Feature vocabulary
    CREATE TABLE IF NOT EXISTS vision_feature_definitions (
        id INTEGER PRIMARY KEY,
        feature_key TEXT NOT NULL UNIQUE,
        display_name TEXT NOT NULL,
        category TEXT NOT NULL CHECK (
            category IN ('observable', 'derived', 'semantic_soft', 'decisioning')
        ),
        value_type TEXT NOT NULL CHECK (
            value_type IN ('boolean', 'enum', 'number', 'string', 'json')
        ),
        allowed_values_json TEXT,
        decision_role TEXT NOT NULL CHECK (
            decision_role IN ('hard_exclusion', 'soft_exclusion', 'ranking_signal', 'display_only')
        ),
        observability_score REAL NOT NULL DEFAULT 0.5,
        reliability_score REAL NOT NULL DEFAULT 0.5,
        discriminative_power_score REAL NOT NULL DEFAULT 0.5,
        sort_order INTEGER,
        is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
        notes TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_vfd_category
        ON vision_feature_definitions(category);
    CREATE INDEX IF NOT EXISTS idx_vfd_decision_role
        ON vision_feature_definitions(decision_role);

    -- 2. Structured model profiles
    CREATE TABLE IF NOT EXISTS knife_model_vision_profiles (
        id INTEGER PRIMARY KEY,
        knife_model_id INTEGER NOT NULL UNIQUE,
        observable_profile_json TEXT NOT NULL DEFAULT '{}',
        derived_profile_json TEXT NOT NULL DEFAULT '{}',
        semantic_profile_json TEXT,
        decision_profile_json TEXT,
        catalog_alignment_json TEXT,
        annotation_status TEXT NOT NULL DEFAULT 'draft' CHECK (
            annotation_status IN ('draft', 'reviewed', 'approved', 'deprecated')
        ),
        profile_version INTEGER NOT NULL DEFAULT 1,
        source_summary TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (knife_model_id) REFERENCES knife_models_v2(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_kmvp_status
        ON knife_model_vision_profiles(annotation_status);

    -- 3. Normalized feature values (optional phase 1, useful for querying)
    CREATE TABLE IF NOT EXISTS knife_model_vision_feature_values (
        id INTEGER PRIMARY KEY,
        knife_model_id INTEGER NOT NULL,
        feature_definition_id INTEGER NOT NULL,
        feature_value_text TEXT,
        feature_value_num REAL,
        confidence REAL CHECK (confidence >= 0.0 AND confidence <= 1.0),
        source_type TEXT NOT NULL CHECK (
            source_type IN ('human_annotation', 'cv_extraction', 'llm_vision', 'catalog_alignment', 'manual_measurement')
        ),
        source_ref TEXT,
        notes TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (knife_model_id) REFERENCES knife_models_v2(id) ON DELETE CASCADE,
        FOREIGN KEY (feature_definition_id) REFERENCES vision_feature_definitions(id) ON DELETE CASCADE,
        UNIQUE (knife_model_id, feature_definition_id)
    );

    CREATE INDEX IF NOT EXISTS idx_kmvfv_model
        ON knife_model_vision_feature_values(knife_model_id);
    CREATE INDEX IF NOT EXISTS idx_kmvfv_feature
        ON knife_model_vision_feature_values(feature_definition_id);

    -- 4. Observation runs
    CREATE TABLE IF NOT EXISTS knife_observation_runs (
        id INTEGER PRIMARY KEY,
        tenant_id TEXT DEFAULT 'default',
        source_image_ref TEXT NOT NULL,
        source_image_hash TEXT,
        user_inputs_json TEXT,
        extracted_observable_profile_json TEXT,
        extracted_derived_profile_json TEXT,
        extraction_notes TEXT,
        final_status TEXT NOT NULL DEFAULT 'pending' CHECK (
            final_status IN ('pending', 'pruned', 'ranked', 'confirmed', 'rejected', 'error')
        ),
        chosen_knife_model_id INTEGER,
        chosen_colorway_id INTEGER,
        final_confidence REAL CHECK (final_confidence >= 0.0 AND final_confidence <= 1.0),
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (chosen_knife_model_id) REFERENCES knife_models_v2(id),
        FOREIGN KEY (chosen_colorway_id) REFERENCES model_colorways(id),
        FOREIGN KEY (tenant_id) REFERENCES tenants(id)
    );

    CREATE INDEX IF NOT EXISTS idx_kor_status
        ON knife_observation_runs(final_status);

    -- 5. Observation candidates
    CREATE TABLE IF NOT EXISTS knife_observation_candidates (
        id INTEGER PRIMARY KEY,
        observation_run_id INTEGER NOT NULL,
        knife_model_id INTEGER NOT NULL,
        colorway_id INTEGER,
        gate_stage TEXT NOT NULL CHECK (
            gate_stage IN ('initial', 'post_gate', 'post_score', 'big_family_pass', 'final_compare')
        ),
        candidate_rank INTEGER,
        score_total REAL,
        score_feature REAL,
        score_geometry REAL,
        score_vision_compare REAL,
        include_reason_json TEXT,
        exclude_reason_json TEXT,
        rationale_text TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (observation_run_id) REFERENCES knife_observation_runs(id) ON DELETE CASCADE,
        FOREIGN KEY (knife_model_id) REFERENCES knife_models_v2(id) ON DELETE CASCADE,
        FOREIGN KEY (colorway_id) REFERENCES model_colorways(id) ON DELETE SET NULL,
        UNIQUE (observation_run_id, knife_model_id, gate_stage)
    );

    CREATE INDEX IF NOT EXISTS idx_koc_run
        ON knife_observation_candidates(observation_run_id);
    CREATE INDEX IF NOT EXISTS idx_koc_rank
        ON knife_observation_candidates(observation_run_id, gate_stage, candidate_rank);

    -- 6. Triggers for updated_at
    CREATE TRIGGER IF NOT EXISTS trg_kmvp_updated_at
    AFTER UPDATE ON knife_model_vision_profiles
    FOR EACH ROW BEGIN
        UPDATE knife_model_vision_profiles SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
    END;

    CREATE TRIGGER IF NOT EXISTS trg_kmvfv_updated_at
    AFTER UPDATE ON knife_model_vision_feature_values
    FOR EACH ROW BEGIN
        UPDATE knife_model_vision_feature_values SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
    END;

    CREATE TRIGGER IF NOT EXISTS trg_kor_updated_at
    AFTER UPDATE ON knife_observation_runs
    FOR EACH ROW BEGIN
        UPDATE knife_observation_runs SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
    END;
    """)
    print("  Tables and triggers created.")


def seed_feature_definitions(conn: sqlite3.Connection):
    """Seed the feature vocabulary. Skips existing rows."""

    features = [
        # ── Observable features (validated = higher reliability scores) ──
        # Validated by testing (reliability_score >= 0.8)
        ("handle_type_visual", "Handle Type (Visual)", "observable", "enum",
         json.dumps(["scales", "paracord", "cord_wrap", "skeletal", "wood", "unknown"]),
         "hard_exclusion", 0.95, 0.95, 0.7, 1),

        ("is_hatchet", "Hatchet/Axe Shape", "observable", "boolean",
         json.dumps([True, False]),
         "hard_exclusion", 0.99, 0.99, 0.3, 2),

        ("knife_type_visual", "Kitchen vs Field (Visual)", "observable", "enum",
         json.dumps(["kitchen", "field", "unknown"]),
         "hard_exclusion", 0.90, 0.85, 0.5, 3),

        ("blade_finish_visual", "Blade Color/Finish (Visual)", "observable", "enum",
         json.dumps(["silver_satin", "black_dark", "coyote_tan", "two_tone", "other", "unknown"]),
         "soft_exclusion", 0.85, 0.80, 0.4, 4),

        ("blade_wider_than_handle", "Blade Wider Than Handle", "observable", "boolean",
         json.dumps([True, False]),
         "hard_exclusion", 0.90, 0.90, 0.2, 5),

        ("blade_to_handle_ratio_bucket", "Blade-to-Handle Length Ratio", "observable", "enum",
         json.dumps(["much_shorter", "shorter", "similar", "longer", "much_longer", "unknown"]),
         "soft_exclusion", 0.80, 0.65, 0.5, 6),

        ("handle_color_primary", "Handle Primary Color", "observable", "enum",
         json.dumps(["black", "orange", "green", "tan", "brown", "red", "grey", "coyote", "wood_grain", "camo", "other", "unknown"]),
         "ranking_signal", 0.85, 0.75, 0.3, 7),

        # Not yet validated (reliability_score = 0.5 until tested)
        ("lanyard_hole_presence", "Lanyard Hole Visible", "observable", "boolean",
         json.dumps([True, False]),
         "ranking_signal", 0.80, 0.50, 0.3, 10),

        ("lanyard_hole_shape", "Lanyard Hole Shape", "observable", "enum",
         json.dumps(["round", "oval", "slot", "irregular", "none", "unknown"]),
         "ranking_signal", 0.60, 0.50, 0.2, 11),

        ("handle_fastener_count_visible", "Handle Fastener Count", "observable", "number",
         json.dumps({"min": 0, "max": 5}),
         "ranking_signal", 0.70, 0.50, 0.3, 12),

        ("choil_presence", "Choil Present", "observable", "boolean",
         json.dumps([True, False]),
         "soft_exclusion", 0.75, 0.50, 0.4, 13),

        ("choil_depth", "Choil Depth", "observable", "enum",
         json.dumps(["none", "shallow", "medium", "deep", "unknown"]),
         "ranking_signal", 0.65, 0.50, 0.4, 14),

        ("guard_prominence", "Guard/Bolster Prominence", "observable", "enum",
         json.dumps(["none", "low", "medium", "high", "unknown"]),
         "ranking_signal", 0.70, 0.50, 0.3, 15),

        ("jimping_presence", "Jimping Present", "observable", "boolean",
         json.dumps([True, False]),
         "ranking_signal", 0.70, 0.50, 0.3, 16),

        ("jimping_location", "Jimping Location", "observable", "enum",
         json.dumps(["spine_near_handle", "spine_mid", "underside", "multiple", "none", "unknown"]),
         "ranking_signal", 0.55, 0.50, 0.2, 17),

        ("handle_profile_primary", "Handle Profile Shape", "observable", "enum",
         json.dumps(["mostly_straight", "tapered", "center_swell", "palm_swell", "curved", "mixed", "unknown"]),
         "ranking_signal", 0.60, 0.50, 0.3, 18),

        ("handle_butt_shape", "Handle Butt Shape", "observable", "enum",
         json.dumps(["rounded", "squared", "tapered", "flared", "unknown"]),
         "ranking_signal", 0.60, 0.50, 0.2, 19),

        ("handle_texture_visual", "Handle Texture (Visual)", "observable", "enum",
         json.dumps(["smooth", "textured", "woven", "scaled", "wood_grain", "carbon_fiber_pattern", "unknown"]),
         "ranking_signal", 0.70, 0.50, 0.3, 20),

        ("spine_profile_primary", "Spine Profile", "observable", "enum",
         json.dumps(["mostly_straight", "gentle_drop", "strong_drop", "hump_then_drop", "rising", "mixed", "unknown"]),
         "ranking_signal", 0.60, 0.50, 0.4, 21),

        ("edge_profile_primary", "Edge Profile", "observable", "enum",
         json.dumps(["mostly_straight", "gentle_belly", "pronounced_belly", "recurve", "mixed", "unknown"]),
         "ranking_signal", 0.60, 0.50, 0.4, 22),

        ("tip_acuteness", "Tip Acuteness", "observable", "enum",
         json.dumps(["fine", "medium", "stout", "unknown"]),
         "ranking_signal", 0.55, 0.50, 0.3, 23),

        ("tip_drop_relative_to_spine", "Tip Position vs Spine", "observable", "enum",
         json.dumps(["below", "near", "above", "unknown"]),
         "ranking_signal", 0.55, 0.50, 0.4, 24),

        # ── Semantic soft labels ──
        ("blade_style_soft_label", "Blade Style (Soft Label)", "semantic_soft", "enum",
         json.dumps(["drop_point", "clip_point", "trailing_point", "spear_point", "sheepsfoot", "cleaver", "dagger", "skinner", "fillet", "chef", "santoku", "petty", "paring", "hawkbill", "hatchet", "other", "ambiguous", "unknown"]),
         "display_only", 0.50, 0.40, 0.3, 50),

        ("blade_style_candidates", "Blade Style Candidates (with confidence)", "semantic_soft", "json",
         None,
         "display_only", 0.50, 0.40, 0.3, 51),
    ]

    cursor = conn.cursor()
    inserted = 0
    for f in features:
        try:
            cursor.execute("""
                INSERT INTO vision_feature_definitions
                (feature_key, display_name, category, value_type, allowed_values_json,
                 decision_role, observability_score, reliability_score, discriminative_power_score, sort_order)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, f)
            inserted += 1
        except sqlite3.IntegrityError:
            pass  # Already exists

    conn.commit()
    print(f"  Seeded {inserted} feature definitions ({len(features)} total, {len(features) - inserted} already existed).")


def verify(conn: sqlite3.Connection):
    """Verify tables exist and have expected structure."""
    tables = [
        "vision_feature_definitions",
        "knife_model_vision_profiles",
        "knife_model_vision_feature_values",
        "knife_observation_runs",
        "knife_observation_candidates",
    ]
    for t in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  {t}: {count} rows")


def main():
    db = sys.argv[1] if len(sys.argv) > 1 else str(DB_PATH)
    print(f"Database: {db}")

    conn = sqlite3.connect(db)
    conn.execute("PRAGMA foreign_keys = ON")

    print("\n1. Creating tables...")
    create_tables(conn)

    print("\n2. Seeding feature definitions...")
    seed_feature_definitions(conn)

    print("\n3. Verifying...")
    verify(conn)

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
