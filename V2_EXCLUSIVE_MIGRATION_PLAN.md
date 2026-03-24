# V2-Exclusive Migration Plan

## 1. Definition of "v2-Exclusive"

**v2-exclusive** means:

- `knife_models_v2` and related v2 dimension tables (`knife_types`, `knife_forms`, `knife_families`, `knife_series`, `collaborators`) are the **canonical writable catalog**.
- `inventory_items_v2` is the **canonical writable inventory**.
- Legacy tables (`master_knives`, `inventory_items`, `option_*`) are **not** the source of truth. They may exist only for:
  - One-time or manual migration from legacy data
  - Temporary read-only fallback during transition
  - Historical backup/audit (read-only)

**Non-acceptable:** Using v2 as a derived view over `master_knives` while legacy remains the writable source. That is not v2-exclusive.

**Canonical IDs everywhere:**
- `knife_models_v2.id` = canonical model ID (no exposure of `legacy_master_id` in new flows)
- `inventory_items_v2.id` = canonical inventory ID (no exposure of `legacy_inventory_id` in new flows)

---

## 2. What MUST Move to V2 Before Migration is Complete

| Area | Requirement |
|------|-------------|
| **Catalog CRUD** | Create, read, update, delete models must target `knife_models_v2` and dimension tables. No writes to `master_knives`. |
| **Inventory CRUD** | Create, update, delete inventory must target `inventory_items_v2` only. No dual-write to `inventory_items`. |
| **Model editor** | Master/Catalog page must use a v2-native model editor. No editing of `master_knives`. |
| **Identify flow** | Identify must return and link using `knife_models_v2.id`. No dependence on legacy master IDs in new flows. |
| **Images & descriptors** | Reference images, silhouettes, and descriptors must live in v2-linked storage keyed by `knife_model_id`, not `legacy_master_id`. |
| **Options / descriptors** | Steels, finishes, colors, conditions must use v2-native lookup or option tables, not legacy `option_*` tables. Free-text is only a temporary fallback. |
| **Exports** | Inventory export from `inventory_items_v2`; catalog export from `knife_models_v2`. |
| **Imports** | V2-native import path for operational use. Legacy import is a one-time bridge only. |
| **Startup** | No automatic legacy → v2 rebuild on normal startup. Legacy sync is manual only. |
| **API contracts** | New endpoints and flows use v2 IDs only. No propagation of legacy IDs in new screens, links, or payloads. |

---

## 3. What Can Remain Legacy-Only (Temporarily)

| Item | Allowed temporarily | Notes |
|------|----------------------|-------|
| Legacy tables | Yes | Keep for migration/rollback. Read-only after v2 is authoritative. |
| Legacy endpoints | Yes | Keep until v2 replacements are verified and all consumers updated. |
| Legacy migration script | Yes | One-time or manual tool to import from legacy into v2. |
| `legacy_master_id` / `legacy_inventory_id` | Yes | Columns may exist for migration audit, but must not be part of new flows. |
| Identify using legacy during transition | Yes | Only while Identify v2 migration is in progress. |

---

## 4. Revised Remaining Work List

### 4.1 Inventory writes (P0)

- [ ] **Stop dual-writing:** `POST/PUT /api/v2/inventory` must write only to `inventory_items_v2`.
- [ ] Remove all writes to `inventory_items` from v2 inventory endpoints.
- [ ] Update v2 duplicate endpoint to write only to `inventory_items_v2`.
- [ ] Resolve `knife_model_id` via `knife_models_v2` only; remove dependence on `legacy_master_id` for create path.

### 4.2 Catalog CRUD — v2-native model editor (P0)

- [ ] **Implement create:** `POST /api/v2/models` → insert into `knife_models_v2` and dimension tables as needed.
- [ ] **Implement update:** `PUT /api/v2/models/{id}` → update `knife_models_v2` and dimension links.
- [ ] **Implement delete:** `DELETE /api/v2/models/{id}` with appropriate constraints (e.g., no inventory references).
- [ ] Add or reuse CRUD for dimension tables: `knife_types`, `knife_forms`, `knife_families`, `knife_series`, `collaborators`.
- [ ] Build or finish v2-native Catalog/model editor UI (replace Master page’s legacy editor).
- [ ] Model editor must support: identity, type/family/form/series/collaboration links, default attributes, lifecycle (is_current_catalog, is_discontinued).
- [ ] Master/Catalog page must stop editing `master_knives`; all edits go through v2 endpoints.

### 4.3 Images and descriptors — v2-linked storage (P0)

- [ ] Create v2-linked storage (e.g. `knife_model_images`, `knife_model_descriptors` or equivalent), keyed by `knife_model_id`.
- [ ] Migrate reference images from `master_knives.identifier_image_blob` into v2 storage.
- [ ] Migrate Hu silhouettes and distinguishing features into v2 descriptor storage.
- [ ] Add endpoints: upload image, fetch image, update descriptors—all by `knife_model_id`.
- [ ] Remove reliance on `legacy_master_id` for images and descriptors.
- [ ] Identify and admin features must use v2 image/descriptor endpoints.

### 4.4 Options / descriptors — v2-native lookups (P1)

- [ ] Introduce v2-native option/lookup tables for: steels, finishes, blade colors, handle colors, conditions.
- [ ] Add v2 option endpoints or fold into existing v2 API (e.g. `GET /api/v2/options` or filter endpoints).
- [ ] Update forms and filters to use v2 options only.
- [ ] Deprecate reliance on legacy `option_*` tables for new flows.
- [ ] Free-text is only a temporary fallback during transition, not the canonical design.

### 4.5 Identify flow — v2 canonical (P1)

- [ ] Identify API must match against `knife_models_v2` (and dimension tables), not `master_knives`.
- [ ] Identify results must return `knife_models_v2.id` as the canonical model ID.
- [ ] Identify UI links (e.g. "Add to collection") must use v2 model ID, not legacy master ID.
- [ ] Remove `GET /api/v2/models/by-legacy-master/{id}` from active flows (keep only for migration tooling if needed).

### 4.6 Exports — v2-native (P1)

- [ ] Inventory export: read from `inventory_items_v2` with joins; output flattened CSV.
- [ ] Catalog export: read from `knife_models_v2` with joins; output flattened CSV.
- [ ] Update Collection/Catalog export links to use v2 export endpoints.
- [ ] Deprecate legacy export endpoints once v2 equivalents are in use.

### 4.7 Imports — v2-native (P2)

- [ ] Add v2-native catalog import (CSV or equivalent) that writes to `knife_models_v2` and dimension tables.
- [ ] Add v2-native inventory import if needed.
- [ ] Keep legacy CSV import as a one-time bridge tool only; do not use it for normal operations.
- [ ] Normal operational imports must not target `master_knives`.

### 4.8 Startup and rebuild logic (P1)

- [ ] Remove automatic `migrate_legacy_to_v2` from normal startup.
- [ ] Keep legacy → v2 migration as a manual operation (e.g. `POST /api/normalized/rebuild` or a dedicated tool).
- [ ] Startup should not assume legacy is canonical.

### 4.9 Canonical ID cleanup (P1)

- [ ] Audit all UI and API flows for legacy ID exposure.
- [ ] Ensure all new screens, identify results, edit links, and API contracts use `knife_models_v2.id` and `inventory_items_v2.id`.
- [ ] Stop exposing `legacy_master_id` or legacy inventory IDs in new flows.
- [ ] Remove or clearly mark legacy ID-based endpoints as legacy-only.

### 4.10 Legacy endpoint deprecation (P2)

- [ ] Audit remaining reads from legacy tables.
- [ ] Move all active UI to v2 endpoints.
- [ ] Mark legacy endpoints as deprecated (e.g. response headers, docs).
- [ ] Remove legacy read endpoints once v2 replacements are verified and no longer needed.

---

## 5. Recommended Implementation Order

| Phase | Work | Rationale |
|-------|------|-----------|
| **1** | Inventory writes v2-only (4.1) | Inventory is already close; removing dual-write is the fastest step. |
| **2** | v2-native model editor + Catalog CRUD (4.2) | Makes v2 the writable catalog source; unblocks all downstream changes. |
| **3** | Images/descriptors v2 storage (4.3) | Breaks dependency on `master_knives` for reference data and Identify. |
| **4** | v2 options/lookups (4.4) | Replaces legacy options; needed for forms and filters. |
| **5** | Identify flow v2 (4.5) | Depends on v2 models and image/descriptor storage. |
| **6** | Exports v2 (4.6) | Straightforward once inventory and catalog are v2. |
| **7** | Startup/rebuild cleanup (4.8) | Remove legacy sync from normal startup. |
| **8** | Canonical ID cleanup (4.9) | Final pass to remove legacy ID exposure. |
| **9** | V2-native imports (4.7) | Operational import path for v2 catalog/inventory. |
| **10** | Legacy deprecation/removal (4.10) | Last step after everything else is verified. |

---

## 6. Summary

**v2-exclusive** = v2 tables are the only writable source of truth. Legacy tables are retained only for migration and temporary fallback, not for ongoing writes.

**Required before migration is complete:**
1. Inventory writes only to `inventory_items_v2`.
2. Catalog CRUD only to `knife_models_v2` and dimension tables.
3. v2-native model editor replacing legacy Master page editor.
4. Images and descriptors in v2-linked storage.
5. v2-native options/lookups.
6. Identify using v2 model IDs.
7. Exports from v2 tables.
8. No automatic legacy sync on startup.
9. All new flows use canonical v2 IDs only.

**Not acceptable:** Keeping `master_knives` as the long-term writable source of truth.

---

## 7. Strict Completion Pass (2026-03-23)

Completed in this pass:

- [x] Canonical model editor UX cleanup: removed duplicate legacy identity fields from Catalog editor and kept canonical v2 identity fields as the single editable source.
- [x] Fixed identity layout rendering so the canonical identity card spans the full form width and is readable across breakpoints.
- [x] `GET /api/v2/models/search` no longer exposes `legacy_master_id` in v2 search responses.
- [x] Active UI option flows now use `/api/v2/options` only.
- [x] Extended v2 options API to support all active UI option types (`blade-types`, `categories`, `blade-families`, `primary-use-cases`) in addition to steel/finish/color/condition types.
- [x] Added `canonical_slug` to v2 model editor payload mapping so the field is persisted instead of being UI-only.
- [x] Simplified model editor to canonical identity + descriptor characteristics + image + one notes field.
- [x] Removed duplicate/deprecated editor fields (primary use case, parent model, record type, catalog status, lifecycle toggles, release dates, confidence, evidence/collector notes, product URL, low-value shape detail fields).
- [x] Converted collaborator, generation, size modifier, and platform variant to controlled dropdowns (no free-text identity entry).
- [x] Added inline “Add” support for collaborator/size/platform options from the model editor.
- [x] Added controlled option support and backend validation for `collaborators`, `generations`, `size-modifiers`, `platform-variants`, and `handle-types`.

Still intentionally retained for migration compatibility:

- [ ] Legacy endpoints remain available, but are not used by active v2 editor/inventory flows.
- [ ] Legacy table columns (`legacy_master_id`, etc.) still exist for migration/audit compatibility.
