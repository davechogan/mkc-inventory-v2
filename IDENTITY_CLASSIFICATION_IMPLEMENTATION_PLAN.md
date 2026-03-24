# Identity Classification Implementation Plan

## Goal
Ensure the v2 model editor and stored v2 model data can accurately classify models like `Blood Brothers Speedgoat 2.0` using explicit identity dimensions.

## Canonical identity fields (v2)
- `official_name`
- `knife_type` (via `type_id`)
- `form_name` (via `form_id`)
- `family_name` (via `family_id`)
- `series_name` (via `series_id`)
- `collaborator_name` (via `collaborator_id`)
- `generation_label`
- `size_modifier`
- `platform_variant`

## Phase 1 — UI Identity Section
1. Add a dedicated "Identity classification" section in the model editor (`static/master.html`) with explicit controls for:
   - Name, type, form, family, series, collaborator, generation, size modifier, platform variant
2. Keep legacy-adjacent fields for compatibility, but make identity fields first-class.
3. In the editor (`static/app.js`):
   - populate identity dropdowns from `/api/v2/catalog/filters`
   - map loaded model rows to identity controls
   - map submit payload directly to v2 identity fields
4. Add client-side save validation for required identity:
   - `official_name`, `knife_type`, `form_name`, `family_name`

## Phase 2 — Backend Validation and Rules
1. In `V2ModelIn`:
   - enforce required identity on create/update (`official_name`, `knife_type`, `form_name`, `family_name`)
   - normalize category/type values using existing category normalization helpers
2. Add simple consistency checks:
   - if `series_name` indicates collab and `collaborator_name` empty, allow save but include warning metadata in QA report
3. Keep canonical v2 create/update endpoints as source of truth.

## Phase 3 — Data Backfill/Normalization
1. Add a backfill routine to normalize and fill missing identity dimensions in `knife_models_v2`:
   - infer family with `normalized_model.detect_family(...)`
   - infer type with `normalized_model.detect_type(...)`
   - infer form with `normalized_model.detect_form(...)`
   - infer series/collaborator with `normalized_model.detect_series(...)` / `detect_collaborator(...)`
   - infer `generation_label` from name if absent
2. Run this backfill at startup (idempotent) and expose manual endpoint.

## Phase 4 — Identity QA Visibility
1. Add endpoint:
   - `GET /api/v2/models/qa/identity`
2. Return:
   - total models
   - complete identity count
   - incomplete rows with missing fields
   - consistency warnings (e.g., collab-like series without collaborator)

## Acceptance Criteria
1. Model editor shows explicit identity fields and saves them through v2 endpoints.
2. New/edited models cannot save without required identity fields.
3. Existing models are backfilled so core identity completeness is high and measurable.
4. QA endpoint reports completeness and warnings.
5. Classification examples (e.g. `Blood Brothers Speedgoat 2.0`) are represented by explicit identity dimensions, not mixed free-form text.
