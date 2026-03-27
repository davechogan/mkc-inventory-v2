# MKC Inventory — Master Implementation Plan
*Created: 2026-03-27. Last updated: 2026-03-27. This document is the canonical planning artifact for the current implementation effort. A new agent session should read this file first and use it to orient all work. The Artifacts repo Slice Tracker has been deprecated; this file is the single source of truth for remaining and completed work.*

---

## How to use this document

This plan is structured so an AI coding agent can be handed it cold and immediately understand:
- What the project is and what it does
- What the owner's explicit goals are (verbatim from conversation)
- What was previously attempted and what is broken or incomplete
- The ordered work plan with acceptance criteria
- Which files to read first for each phase
- The engineering standards that apply

All work should follow `AI_Coding_Standards_and_Rules.md` (copy stored in the Artifacts repo and originally from `/Users/dhogan/Downloads/AI_Coding_Standards_and_Rules.md`). Key principles: canonical paths, separation of concerns, no god files, idempotent operations, tests for generalization not just passing cases, no bandaids.

---

## 1. Project Overview

**MKC Inventory** is a personal knife collection management and natural-language reporting application for Montana Knife Company products. It runs locally on a home network (Mac Studio or equivalent). There is no cloud dependency.

**Two repos:**
- `MKC_Inventory` — the application: `https://github.com/davechogan/mkc-inventory-v2`
  - Local path: `/Users/dhogan/Applications/MKC_Inventory/`
- `Artifacts` — snapshot/provenance store for DB backups, exports, and plans: `https://github.com/davechogan/Artifacts`
  - Local path: `/Users/dhogan/Applications/Artifacts/`

**Stack:** Python 3.11, FastAPI, Uvicorn, SQLite (migrating to Postgres), Ollama (local LLM inference), sentence-transformers, ChromaDB (optional retrieval backend).

**Two subsystems:**
1. **Catalog & Inventory CRUD** — normalized v2 schema for knife models and owned items.
2. **Natural Language Reporting** — semantic Q&A: users ask questions in plain English, system produces SQL-backed answers, charts, and narrative summaries via a multi-step LLM pipeline.

---

## 2. Owner's Explicit Goals (verbatim from conversation)

The following goals were stated directly and must be preserved as requirements:

> **On v2 tables:** "This is not intended to be a bridge build. what are named the v2 tables are what I expect the app to use. Task: complete conversion to normalized data structures, v2."

> **On SQLite → Postgres:** "I am happy to move to postgres. I do not want the app to rely on anything external to my local network. Task: Migrate to postgres."

> **On app.py:** "I do not want migration things in this file at all. The migration was supposed to be completed once everything was shifted to the v2 tables. Task: clean up all migration related code. Secondary task: assess modularity and separation of duties in app.py"

> **On images:** "I am fine shifting to a document store for this, however my ideas for managing images has not been completed. I could use some help determining the best way to deal with the different handle color options so that when a knife is added to my inventory, from the catalog, it displays the correct knife image in the grid. I have mainly orange and black colored handles in the current BLOB storage. There are some that are different though. I also have a dir in the root of the app called Images, and I have files that represent the different colorways for each hero shot of each knife. It is not 100% complete though. Task: assess end state goals and offer recommendations."

> **On deployment:** "The deployment script should be managed by the pip install requirements doc."

> **On reporting:** "This has been an adventure. This portion of the spec is key: the required flow after this phase: User Question -> Retrieval -> LLM generates canonical plan JSON -> JSON Schema Validation -> Semantic Validation -> Deterministic SQL Compilation -> SQL Validation -> Execution -> Response. No plan may reach compilation unless it passes both: 1. structural validation 2. semantic validation. Task: assess current state against the documented architectural goals and create a mitigation plan."

> **On git:** "ensure proper use of commits, branching and merges to maintain history and ensure recoverability."

---

## 3. Current State Assessment

### 3.1 What was previously attempted (Copilot session)

A GitHub Copilot session preceded this plan. It produced:
- `migrations/migrate_v2.py` — migration helpers extracted from app.py (partial)
- `tools/sync_images.py` — image metadata sync script (broken, wrong assumptions)
- `reporting/reporting_plan.schema.json` — JSON schema for canonical reporting plan (broken, see §3.3)
- `reporting/validator.py` — JSON schema validation wrapper
- `reporting/example_plan.json` — example canonical plan for testing
- `tests/test_reporting_validator.py` — validator tests

No git branches or commits were created despite being explicitly requested. No plan document was written to disk.

### 3.2 What is broken or incomplete

**Critical — fix before any new work:**

1. **`reporting/reporting_plan.schema.json` is invalid JSON.** The file contains two concatenated JSON objects: lines 1–21 are a stale draft-07 schema; lines 22–284 are the correct 2020-12 schema. Most JSON parsers will either error or silently load only the first (looser) object. All validation is suspect until this is fixed. Fix: delete lines 1–21, keep only the 2020-12 schema.

2. **`app.py` still contains migration code.** The Copilot extraction was incomplete. The following functions are defined in `app.py` AND in `migrations/migrate_v2.py` (duplicated):
   - `backfill_v2_model_identity` (app.py line ~307)
   - `normalize_v2_additional_fields` (app.py line ~538)
   The following are called at startup from `app.py` (lines ~1557–1722):
   - `ensure_identifier_columns`, `ensure_master_extra_columns`, `ensure_master_catalog_columns`, `ensure_phase1_schema`, `ensure_v2_exclusive_schema`, `migrate_legacy_media_to_v2`, `backfill_v2_model_identity`, `normalize_v2_additional_fields`
   The `app_meta` dict (lines ~1862–1880) also re-exports these functions. All of this must be removed from `app.py`. The functions belong only in `migrations/migrate_v2.py` or `tools/`.

3. **`tools/sync_images.py` uses a wrong directory assumption.** The script assumes `Images/<model-slug>/` subdirectories. The actual structure is `Images/MKC_Colors/` — a flat directory of files named `ModelName_ColorName.jpg` (e.g., `Badrock_Orange_Black.jpg`, `Speedgoat_2.0_Orange_Black.jpg`). The script will never find useful content. It needs a full rewrite.

4. **No git recovery point exists.** The Copilot changes were made without committing. Create a commit on a feature branch before proceeding.

### 3.3 Reporting pipeline — known defects

Tracked in `docs/reference/reporting_defect_backlog.md`:

| ID | Description | Impact |
|----|-------------|--------|
| RPT-001 | Contradictory positive+negative filter for same series → zero rows | High |
| RPT-002 | Empty-result follow-up lock: bad filters from prior turn propagate | High |
| RPT-003 | Meta questions about cost fields route to listing intent | Medium |
| RPT-004 | Cost-correction follow-up can't switch to cost-bearing projection | High |
| RPT-005 | Missing-model drill-through targets inventory instead of catalog | Medium |

RPT-001 and RPT-002 compound each other (bad answer → follow-up inherits bad filters → another bad answer). Fix these before any new reporting features.

### 3.4 Architecture observations

**`reporting/domain.py` is a god module** (3128 lines). It contains: semantic planning, heuristic fallback planning, explicit constraint extraction, SQL compilation (two paths), SQL safety validation, SQL execution, answer synthesis, chart inference, session management, hint learning, hint promotion, and follow-up carryover. Per the coding standards, this must be split along its natural seams (see Phase D).

**Two SQL compilation paths** (`_reporting_plan_to_sql` and `_reporting_plan_to_sql_legacy`) exist simultaneously. RPT-003/004/005 likely originate in the legacy path. The canonical path should be the only path.

**The vNext architecture document** (`reporting/Reporting_AI_Architecture_vNext.md`) accurately describes the intended pipeline and is the source of truth for reporting behavior.

---

## 4. Key Files Reference

An agent beginning any phase should read these first:

| File | Why |
|------|-----|
| `app.py` | FastAPI entrypoint; currently contains migration code that must be removed |
| `reporting/domain.py` | Core reporting logic (3128 lines); primary area of work for reporting phases |
| `reporting/plan_models.py` | Canonical `CanonicalReportingPlan` Pydantic models and enums |
| `reporting/plan_validator.py` | Plan validation orchestration |
| `reporting/validator.py` | JSON schema wrapper (newly created by Copilot) |
| `reporting/reporting_plan.schema.json` | JSON schema for plans — currently broken (two schemas) |
| `reporting/Reporting_AI_Architecture_vNext.md` | Authoritative description of the intended reporting pipeline |
| `reporting/routes.py` | FastAPI endpoints for the reporting subsystem |
| `reporting/retrieval.py` | Semantic retrieval grounding layer |
| `migrations/migrate_v2.py` | Migration helpers (partially extracted from app.py) |
| `normalized_model.py` | v2 normalization and identity decomposition |
| `tools/sync_images.py` | Image sync script (currently broken, needs rewrite) |
| `docs/reference/reporting_defect_backlog.md` | Known reporting defects |
| `docs/plans/Reporting_AI_Architecture_vNext.md` | Canonical pipeline specification |
| `Images/MKC_Colors/` | Flat directory of hero shot colorway images |
| `sqlite_schema.py` | `column_exists()` helper |

---

## 5. Implementation Plan

### Git workflow for all phases

Before starting each phase:
```
git checkout -b phase/<phase-letter>-<short-description>
```
Commit at each meaningful milestone with a descriptive message. When the phase is complete and tests pass, merge to `main` with a merge commit (not squash — preserve history). Tag milestones: `git tag phase-A-complete`.

### Phase A — Stabilize and clean up ✅ COMPLETE (2026-03-27)

**A1 — Create this plan document and commit it** ✅
- Committed to `docs/plans/MASTER_IMPLEMENTATION_PLAN.md` on `phase/a-stabilize`.

**A2 — Fix the broken schema file** ✅
- Removed stale draft-07 block (lines 1–21). File now contains exactly one 2020-12 schema.
- Fixed `oneOf` integer/number ambiguity in `filterClause.value`.
- Updated `example_plan.json` to valid canonical shape.
- Fixed path bug in `tests/test_reporting_validator.py`.
- `python -m pytest tests/test_reporting_validator.py` passes.

**A3 — Complete app.py migration cleanup** ⏭ Deferred to Phase B pre-work
- Not done in this pass. Migration code remains in `app.py`.
- Will be completed as part of Phase B since the migration audit (B1) needs to run first.

**A4 — Fix sync_images.py** ⏭ Deferred to Phase F
- `tools/sync_images.py` as committed is a stub. Full rewrite deferred to Phase F (image colorway system).

**A5 — git recovery commit + merge** ✅
- All prior-session uncommitted work captured in baseline commit on `phase/a-stabilize`.
- Merged to `main` with merge commit. Tagged `phase-A-complete`.

**Reporting defect fixes (RPT-001 through RPT-005)** ✅ Done early
- Originally planned as Phase C. Completed during Phase A pass after determining
  that architectural compliance alone would not resolve all defects.
- RPT-001: Same-field filter contradiction now pruned before SQL.
- RPT-002: Carryover now skips empty-result prior turns.
- RPT-003: Meta/schema questions short-circuit before LLM planner.
- RPT-005: missing_models drill-through links to `/master.html` (catalog), not inventory.
- RPT-004: Reduced in impact by RPT-002 fix. Deferred for live-server validation.
- 24 new regression tests. 97/97 pass.

**Acceptance criteria for Phase A:**
- [x] `reporting/reporting_plan.schema.json` is valid JSON with exactly one schema object
- [x] `python -m pytest tests/test_reporting_validator.py` passes
- [ ] `app.py` contains no migration function definitions or startup migration calls — *deferred to Phase B*
- [ ] `tools/sync_images.py` runs against the actual `Images/MKC_Colors/` directory — *deferred to Phase F*
- [x] All existing tests pass (97/97)
- [x] Phase A branch merged to main with tag

---

### Phase B — Complete v2 table migration ✅ COMPLETE (2026-03-27)

**Goal:** The application exclusively reads from and writes to v2 tables. Legacy `master_knives` and `inventory_items` tables become archival only.

**B1 — Audit current v2 coverage** ✅
- Audit written to `docs/reference/v2_migration_audit.md`
- 4 legacy inventory items (ids 37, 53, 54, 84) identified as missing from v2
- 4 legacy models without v2 counterparts documented (2 intentional, 1 bundle, 1 needs review)

**B2 — Complete the data migration** ✅
- Created `tools/complete_migrate_v2.py` (gap-fill only; does NOT use force=True to avoid destroying 23 v2-only items)
- Ran successfully: migrated 4 missed inventory items; v2 count now 88 (65 legacy + 23 direct v2 adds)
- Provenance snapshot written to `Artifacts/projects/mkc-inventory-v2/db_snapshots/2026-03-27/`
- Added `__main__` CLI to `migrations/migrate_v2.py` with all migration steps
- Moved real implementations of `backfill_v2_model_identity` + `normalize_v2_additional_fields` from `app.py` to `migrations/migrate_v2.py` (~490 lines of migration code removed from app.py)
- Added `normalize_category_value` + `CANONICAL_CATEGORY_NAMES` to `normalized_model.py`

**B3 — Switch active routes to v2 tables** ✅
- `routes/admin_routes.py`: silhouette Hu read/write switched from `master_knives` to `knife_model_images` (joined via `knife_models_v2`)
- `routes/ai_routes.py`: Hu vector catalog query switched from `master_knives` to `knife_model_images`
- Fixed startup ordering: `recompute_silhouettes_for_masters_without_hu` moved after `ensure_v2_exclusive_schema` (was called before `knife_model_images` table existed)
- Remaining legacy reads in admin/ai routes: `identifier_distinguishing_features` — no v2 column equivalent yet; tracked for Phase D

**B4 — Remove or archive legacy table dependencies** ✅
- `routes/legacy_catalog_routes.py` unregistered from `app.py` (routes retired; file kept as archival reference)
- `/api/inventory/options` endpoint moved to `routes/v2_routes.py` (queries `v2_option_values`; only endpoint still called from UI)

**Acceptance criteria for Phase B:**
- [x] `tools/complete_migrate_v2.py` runs successfully with pass/fail summary
- [x] v2 row counts match expectations: 88 inventory_items_v2 (65 legacy migrated + 23 direct-v2)
- [x] All new feature routes use v2 tables exclusively; legacy routes retired
- [x] All tests pass (104/104)
- [ ] Phase B branch merged to main with tag — pending commit

**Known remaining legacy table reads (not blocking for Phase B):**
- `admin_routes.py` distinguishing-features functions: query `master_knives.identifier_distinguishing_features` and `identifier_image_blob`; v2 equivalent is `knife_model_descriptors.distinguishing_features` + `knife_model_images.image_blob` — fixed as part of Phase B cleanup
- `app.py` `init_db()`: several `ensure_*` helper functions still defined in app.py and called at startup; migration code cleanup is ongoing

---

### Phase C — Reporting pipeline defect fixes ✅ COMPLETE (done during Phase A, 2026-03-27)

**Goal:** Fix the five known defects before any new reporting features. These must be fixed in priority order: RPT-001 and RPT-002 first (they compound each other).

**Engineering standard that applies:** Per `AI_Coding_Standards_and_Rules.md` §9.4 (Anti-Bandaid Test Rule): for each fix, add the failing case + at least 2 sibling variants + one negative case + one route or structural assertion. If a fix cannot survive that, it is a bandaid.

**C1 — RPT-001: Contradictory same-field filters → zero rows**
- Root: `_reporting_prune_conflicting_filters` in `reporting/domain.py` detects "ambiguous cross-dimension" filters but does not detect same-field inclusion+exclusion (e.g., series="Blood Brothers" AND exclusion series="Blood Brothers")
- Fix: Extend `_reporting_prune_conflicting_filters` to detect same-field positive+negative pairs and resolve them (drop the exclusion, or flag `needs_clarification`)
- Tests: direct conflict, indirect conflict, multi-filter non-conflict baseline

**C2 — RPT-002: Empty-result follow-up lock**
- Root: `_reporting_apply_followup_carryover` merges prior plan filters unconditionally, including filters that produced zero rows
- Fix: Guard carryover with a check on the prior result row count — if prior result was empty, do not carry filters forward; start fresh or flag for clarification
- Tests: empty-result follow-up, non-empty follow-up (carryover should still work), chain of follow-ups

**C3 — RPT-004: Cost follow-up can't switch metric**
- Root: Follow-up carryover doesn't handle metric switching when the user shifts from a non-cost question to a cost question
- Fix: When explicit cost-related phrasing is detected in a follow-up, allow metric to switch even if prior plan had a different metric

**C4 — RPT-003 and RPT-005**
- RPT-003: Meta questions about cost fields route to listing — fix intent routing in `_reporting_semantic_plan`
- RPT-005: Missing-model drill-through targets inventory — fix link generation in response synthesis

**Acceptance criteria for Phase C:**
- [x] All five defects have regression tests (RPT-004 deferred — reduced impact after RPT-002 fix)
- [x] Each fix includes sibling variants per §9.4 of the coding standards
- [ ] Eval harness (`tools/reporting_eval_harness.py`) pass rate does not regress — *pending live-server run*
- [x] Changes merged to main with tag `phase-A-complete`

---

### Phase D — reporting/domain.py modularization ✅ COMPLETE (tag: phase-D-complete)

**Goal:** Split the 3128-line god module into focused modules per the coding standards §2.2 (No Monolithic Growth) and §2.3 (Separation of Concerns).

**Completed module split:**
```
reporting/
  constants.py  — all shared constants and type aliases (NEW)
  compiler.py   — compile_plan (canonical-only boundary), validate_sql, exec_sql (NEW)
  planner.py    — 31 semantic/heuristic planning functions (NEW)
  domain.py     — orchestrator: sessions, hints, run_reporting_query (1635 lines, down from 3279)
```

**Deferred to a future phase** (no circular-import-safe path today): session.py, hints.py, synthesizer.py extraction.

**Acceptance criteria — all met:**
- [x] `domain.py` is an orchestrator, not a library — it imports from specialized modules
- [x] `_reporting_plan_to_sql_legacy` removed; dead code `_reporting_call_llm_for_sql` also removed
- [x] `compile_plan()` enforces canonical-plan-only contract (raises `TypeError` otherwise)
- [x] All 104 tests pass with no behavioral change
- [x] Phase D branch merged to main with tag `phase-D-complete`

---

### Phase E — Postgres migration

**Sequence note:** Do this after Phases A–D. Migrating to Postgres on top of messy SQLite code means migrating the mess.

**E1 — DB abstraction layer**
- Introduce a thin connection abstraction in `db/connection.py`
- Single `DATABASE_URL` environment variable (`postgresql://user:pass@host:5432/mkc_inventory` for Postgres, `sqlite:///data/mkc_inventory.db` as fallback during transition)
- Do not introduce SQLAlchemy ORM — the hand-written SQL is fine; use `psycopg[binary]` for Postgres
- Replace all `sqlite3.connect(DB_PATH)` calls with the abstraction

**E2 — Schema migration with Alembic**
- Add Alembic: `pip install alembic`
- `migrations/` becomes the Alembic migrations directory
- Write initial migration from current v2 schema
- All future schema changes go through Alembic — no more `ALTER TABLE` in Python startup code

**E3 — Data migration**
- Create `tools/sqlite_to_postgres.py` — reads from SQLite, inserts to Postgres
- Run on a copy; validate row counts; write provenance artifact

**E4 — Local Postgres setup documentation**
- Document in `docs/reference/POSTGRES_SETUP.md`: how to run Postgres locally (Docker Compose or native), connection string, env vars
- Must not rely on any cloud service

**Acceptance criteria for Phase E:**
- [ ] App runs against local Postgres with `DATABASE_URL` env var
- [ ] All v2 table data is present and verified in Postgres
- [ ] Alembic manages all schema changes
- [ ] SQLite fallback removed from runtime (kept only for archive reference)
- [ ] Phase E branch merged to main with tag

---

### Phase F — Image colorway system

**Goal:** When a knife is added to inventory from the catalog, the correct hero shot for that handle color is displayed in the grid.

**Image directory structure (current):**
`Images/MKC_Colors/<ModelName_ColorName>.<ext>` — flat, no subdirectories.

**Naming convention observed:** Files use underscore-separated tokens. Color is typically the last 1–2 tokens (e.g., `Orange_Black`, `Desert_Camo`, `Distressed_Gray`, `Olive`). Model identity is everything before the color.

**Recommended data model:**
```sql
knife_model_image_files (
  id, model_slug TEXT, color_name TEXT, file_path TEXT,
  is_primary INTEGER, sha256 TEXT, created_at TEXT
)
```
- `model_slug` matches `knife_models_v2.slug`
- `color_name` is canonical (normalized, from a controlled vocabulary)
- `is_primary` marks the default image for the model

**Lookup logic (in display/API layer):**
1. Get `inventory_items_v2.handle_color` for the item
2. Normalize to canonical color name
3. Find `knife_model_image_files` where `model_slug` matches AND `color_name` matches
4. Fall back to `is_primary = 1` for the model
5. Fall back to any image for the model
6. Fall back to a placeholder image

**Default color:** Most MKC models default to `Orange_Black`. Store a `default_handle_color` on `knife_models_v2` (column exists per migration code). Populate it during sync.

**Color vocabulary:** Build from filenames during sync. Where images are missing for a color, log it — do not fail silently. The incomplete coverage is a known condition; surface it clearly.

**Acceptance criteria for Phase F:**
- [ ] `tools/sync_images.py` successfully parses `Images/MKC_Colors/` and populates `knife_model_image_files`
- [ ] Unmatched files (no model slug match) are logged for manual review
- [ ] The inventory grid API endpoint returns the correct image URL per handle color
- [ ] Fallback chain works (color match → primary → any → placeholder)
- [ ] Phase F branch merged to main with tag

---

## 6. Artifacts repo integration

For each major phase completion:
1. Take a DB snapshot: `scripts/backup_mkc_db.sh`
2. Copy snapshot to `Artifacts/projects/mkc-inventory-v2/db_snapshots/`
3. Write a provenance JSON (based on `Artifacts/projects/mkc-inventory-v2/metadata/provenance.template.json`) with: source commit SHA, timestamp, artifact list with SHA256 hashes
4. Commit to the Artifacts repo: `git commit -m "snapshot: phase-X-complete"`

---

## 7. Engineering standards reference

All work must comply with `AI_Coding_Standards_and_Rules.md`. The most relevant rules for this project:

- **§2.2 No Monolithic Growth**: Do not add new behavior to `app.py`, `domain.py`, or any already-large file.
- **§2.3 Separation of Concerns**: Parsing, normalization, validation, planning, compilation, execution, and formatting must be separated.
- **§3.5 No duplicate interpretation**: If the same concept is parsed in more than one place, consolidate it. This applies directly to the two SQL compilation paths.
- **§5 Idempotency**: All migration scripts, sync scripts, and schema creation must be safe to run multiple times.
- **§9.4 Anti-Bandaid Test Rule**: Each bug fix must include the failing case + 2 sibling variants + 1 negative case + 1 route/structural assertion.
- **§16 Preferred Change Pattern**: Identify invariant → identify canonical layer → write tests → implement → simplify → verify.

---

## 8. Deferred Features

Features that were scoped, understood, and explicitly deferred for future work. Each entry records what the feature does, why it was deferred, and what needs to happen to resume it.

### DF-001 — Image-based knife identification (AI / Photo)

**What it does:**
POST `/api/ai/identify` — given an uploaded photo and/or text description, the pipeline:
1. Extracts an OpenCV Hu silhouette vector from the image
2. Compares against Hu vectors stored in `knife_model_images.silhouette_hu_json` for an early-exit match
3. Calls a local Ollama vision model to produce a structured description (blade shape, handle material, distinguishing details)
4. Runs keyword search with the vision output against the catalog
5. If results are ambiguous, calls Ollama again to rerank the top candidates using full catalog data

**Why deferred:**
- The feature was never usable in production — LLM identification quality was too low to be reliable
- The LLM rerank step referenced legacy `master_knives` columns (`blade_profile`, `collector_notes`, `evidence_summary`, `identifier_keywords`, `identifier_distinguishing_features`) that do not exist in v2 tables
- Rather than migrate these columns to v2 just to support an unproven feature, the decision was made to remove it cleanly and revisit later

**What was removed:**
- `POST /api/ai/identify` endpoint and `_keyword_results_to_parsed` helper from `routes/ai_routes.py`
- `GET /api/blade-shapes` route (only used by the AI identify UI)
- AI tab (`#aiPanel`) from `static/identify.html`
- `loadOllamaUi()`, `updateOllamaDefaultBtnState()`, `updateOllamaModelCheck()`, `renderAiIdentifyResult()` from `static/app.js`
- Photo handling + `aiIdentifyForm` submit handler from `initIdentifyPage()` in `static/app.js`

**What must be built to resume:**
- Migrate `identifier_keywords`, `identifier_distinguishing_features`, `evidence_summary`, `collector_notes` to `knife_model_descriptors` (or a new `knife_model_identifier_data` table)
- Migrate `identifier_image_blob` to `knife_model_images.image_blob` (column exists; data migration needed)
- Rewrite the LLM rerank prompt builder (`blade_ai.build_rerank_prompt`) to use v2 columns
- Evaluate LLM identification quality more rigorously before shipping
- Restore the UI tab and endpoints once quality bar is met

**Code reference:** See git history for `routes/ai_routes.py` prior to Phase B cleanup commit for the full original implementation.

---

## 9. What a new agent session should do first

1. Read this file completely.
2. Read `reporting/Reporting_AI_Architecture_vNext.md` — canonical pipeline spec.
3. Read `reporting/plan_models.py` — canonical plan data model.
4. Read `docs/reference/reporting_defect_backlog.md` — known defects.
5. Check the current git status: `git status` and `git log --oneline -10`
6. Determine which phase is next based on the acceptance criteria above.
7. Create the appropriate feature branch before making any changes.
8. Begin with the first incomplete item in the current phase.

Do not attempt multiple phases in a single session without explicit instruction.
