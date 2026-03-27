# V2 Migration Audit
*Generated: 2026-03-27 during Phase B — Complete v2 table migration*

---

## Summary

The v2 migration is approximately 96% complete. The data model, views, and primary
application routes are already using v2 tables. Four legacy inventory items were missed
in the initial migration and need to be inserted into `inventory_items_v2`. Four
`master_knives` records have no v2 counterpart — two are intentional (archived historical
root models), one is a set/bundle that doesn't map to a single model, and one (`Whitetail
Knife`, id=247) needs manual review.

---

## Table Row Counts

| Table | Rows | Notes |
|-------|------|-------|
| `master_knives` (legacy) | 92 | Archival only after Phase B |
| `inventory_items` (legacy) | 65 | Archival only after Phase B |
| `knife_models_v2` | 90 | 4 legacy models not migrated (see below) |
| `inventory_items_v2` | 84 | 65 migrated − 4 missed + 23 added directly to v2 |
| `knife_model_images` | 87 | Hu vectors fully migrated from master_knives |
| `knife_model_descriptors` | 87 | Descriptors migrated |
| `reporting_inventory` (view) | 84 | Matches inventory_items_v2 ✅ |
| `reporting_models` (view) | 90 | Matches knife_models_v2 ✅ |

---

## V2 Model Coverage Gaps (4 records)

| master_knives.id | name | catalog_status | record_type | Disposition |
|-----------------|------|----------------|-------------|-------------|
| 184 | Speedgoat | Historical root model | Base model | **Intentional** — archived predecessor to Speedgoat 2.0 |
| 199 | Blackfoot | Historical root model | Base model | **Intentional** — archived predecessor to Blackfoot 2.0 |
| 247 | Whitetail Knife | Current | Standalone model | **Needs review** — `MKC Whitetail` exists in v2 with a different legacy_master_id; may be duplicate |
| 249 | Traditions Knives Full Set of 5 | Upcoming / limited drop | Limited set | **Bundle** — does not map to a single knife model; not expected in v2 |

---

## Unmigrated Legacy Inventory Items (4 records)

These 4 legacy items have v2 model counterparts but were missed by the initial migration.
They all have `legacy_inventory_id IS NULL` in v2 (i.e., no v2 counterpart exists).

| inventory_items.id | master_knife_id | knife_name | qty | knife_models_v2.id | Action |
|-------------------|----------------|------------|-----|--------------------|--------|
| 37 | 196 | Flathead Fillet | 2 | 319 | **Migrate to v2** |
| 53 | 160 | Freezout | 1 | 288 | **Migrate to v2** |
| 54 | 148 | Speedgoat 2.0 | 1 | 277 | **Migrate to v2** |
| 84 | 150 | Stoned Goat 2.0 PVD | 1 | 279 | **Migrate to v2** |

Fix: `tools/complete_migrate_v2.py` migrates these idempotently.

---

## Hu Vectors / Images

- `master_knives.identifier_silhouette_hu_json`: 87 rows with data
- `knife_model_images.silhouette_hu_json`: 87 rows with data ✅ Fully migrated
- `migrate_legacy_media_to_v2()` has already run correctly

---

## Routes Using Legacy Tables

| Route file | Legacy table | Status |
|-----------|-------------|--------|
| `routes/v2_routes.py` | None | ✅ Clean |
| `routes/normalized_routes.py` | None | ✅ Clean |
| `reporting/routes.py` | None (views only) | ✅ Clean |
| `routes/admin_routes.py` | `master_knives` (Hu/images) | **Fix in B3** — switch to `knife_model_images` |
| `routes/ai_routes.py` | `master_knives` (Hu vectors) | **Fix in B3** — switch to `knife_model_images` |
| `routes/legacy_catalog_routes.py` | `master_knives`, `inventory_items` | **Retire in B4** — UI uses v2 routes; only `/api/inventory/options` still called (queries option tables, not legacy) |

---

## Reporting Views — Join Logic Verified

Both `reporting_inventory` and `reporting_models` join exclusively from v2 tables:
- `reporting_inventory`: `inventory_items_v2 → knife_models_v2 → dimension tables`
- `reporting_models`: `knife_models_v2 → dimension tables`

COALESCE fallback pattern (e.g., `COALESCE(i.steel, km.steel)`) allows per-item overrides of model defaults. ✅ Correct.

---

## app.py Migration Code (A3 — deferred, complete in Phase B)

The following functions remain in `app.py` and are called during `init_db()`. They belong
in `migrations/migrate_v2.py` or should be removed entirely once the schema is stable.

**Defined in app.py (should move or remove):**
- `backfill_v2_model_identity` (~line 307) — duplicated in migrations/migrate_v2.py
- `normalize_v2_additional_fields` (~line 538) — duplicated in migrations/migrate_v2.py
- `ensure_inventory_extra_columns`, `ensure_blade_shape_templates`,
  `ensure_master_identifier_media_columns`, `ensure_master_catalog_line_column`,
  `ensure_tier_option_tables`, `_backfill_family_from_name_once`,
  `_seed_tier_options_once`, `ensure_blade_types_option_table`,
  `ensure_mkc_missing_items_models` (~lines 929–1344)

**Called at startup (lines ~1557–1722):** 14+ migration/ensure functions.

**Also imported from migrations.migrate_v2:** 10 functions (lines 202–213).

`migrations/migrate_v2.py` has no `__main__` CLI entry point — add one during Phase B.
