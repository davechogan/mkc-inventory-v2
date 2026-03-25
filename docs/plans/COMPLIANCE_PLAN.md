# Project compliance roadmap (Cursor rules alignment)

This document tracks alignment with project Cursor rules under `.cursor/rules/` (engineering standards, testing/NLP quality, AI change checklist, git workflow, python-env-and-ci).

**Last updated:** 2026-03-26

---

## Before starting compliance work (git hygiene)

1. **Working tree clean** — `git status` shows no unintended modifications; commit or stash WIP.
2. **Branch** — work from **`dev`** or cut **`feature/compliance-phase-1`** (or similar) from **`dev`** after it is pushed and clean—use for Phase 1+ so `dev` stays mergeable (see git workflow rule: no large refactors directly on `main`).
3. **Local gate** — `./scripts/ci_local.sh` passes.
4. **API gate (reporting changes)** — when validating behavior, run `tools/reporting_eval_harness.py` against a live instance (e.g. `http://macstudio:8008`) with the relevant `--suite`.
5. **Push** — push `dev` (or your feature branch) so `origin` matches what you tested.

---

## Assessment summary (baseline)

| Area | Target rule files | Status (baseline) |
|------|-------------------|-------------------|
| Monolith / layers | `engineering-standards.mdc` | **Gap** — `app.py` is still a large monolith; domain split mainly `normalized_model`, `blade_ai`, … (order–inventory gap tooling lives under `archive/order-inventory-gap/`). |
| Canonical reporting vs regex | `engineering-standards.mdc` | **Partial** — semantic plan + SQL compiler exist; regex grouped + documented in `reporting/regex_contract.py`; extend plan fields before new question regex. |
| Scope preprocessing | `engineering-standards.mdc` | **Done** — `REPORTING_SCOPE_PREPROCESSING` in `reporting/domain.py`; default **off** (legacy `if False`); set `1`/`true`/`yes`/`on` to enable. |
| Automated tests | `testing-and-nlp-quality.mdc` | **Improved** — `pytest tests/` in CI + `scripts/ci_local.sh`; SQL helper tests + scope-env parsing tests. |
| Git / env | `git-workflow.mdc`, `python-env-and-ci.mdc` | **OK** — scripts + rules documented; venv path portability called out. |

---

## Phased plan and progress

### Phase 0 — Baseline and tracking

| Step | Done | Notes |
|------|------|--------|
| Document plan in-repo | [x] | This file. |
| Commit Cursor rules + run script fixes | [x] | Ensures clean tree for Phase 1+. |

### Phase 1 — Explicit reporting flags

| Step | Done | Notes |
|------|------|--------|
| Replace `if False and _reporting_*` with env var (e.g. `REPORTING_SCOPE_PREPROCESSING`) | [x] | `reporting/domain.py`; default **off**; `reporting_scope_preprocessing_enabled()` + tests. |

### Phase 2 — Pytest + CI

| Step | Done | Notes |
|------|------|--------|
| Add `pytest` (dev/deps) and `tests/` | [x] | |
| Unit tests: `plan_to_sql`, unsafe-request guardrail, scope helpers | [x] | No server required. |
| Extend GitHub `checks` job with `pytest tests/` | [x] | |

### Phase 3 — Extract reporting module

| Step | Done | Notes |
|------|------|--------|
| Move reporting semantics out of `app.py` into `reporting/` package | [x] | **`reporting/`** package (`domain.py` + `__init__.py`); `app.py` imports `reporting` for routes. Regex/plan surface debt remains Phase 4. |

### Phase 4 — Regex / plan surface

| Step | Done | Notes |
|------|------|--------|
| Prefer structured plan fields over new one-off regex | [x] | **`reporting/regex_contract.py`** — layers A–D docstring; shared patterns + `extract_first_json_object` / SQL fence cleanup; `domain.py` delegates. **Remaining** ad-hoc patterns in explicit-constraints block — migrate behind named constants when touched. |

### Phase 5 — Monolith split (long arc)

| Step | Done | Notes |
|------|------|--------|
| Split remaining `app.py` by domain routers | [ ] | After reporting extract. |

### Phase 6 — Harness in CI (optional)

| Step | Done | Notes |
|------|------|--------|
| Run harness subset when `REPORTING_EVAL_BASE_URL` available | [ ] | Self-hosted job already exists. |

---

## Merge advice: `dev` → `main`

**Current situation (verify locally with `git log main..dev`):** `main` is **far behind** `dev` (e.g. `main` at initial CI workflow; `dev` carries reporting semantics, harness, guardrails, `semantic_plan` API, gap tools, etc.).

**Is it a good time to merge?**

- **Yes, if:** you want `main` to represent the **current production-capable app**; `./scripts/ci_local.sh` passes on `dev`; you are comfortable that **self-hosted harness** (if used) has been run recently on Mac Studio; and you accept **one large merge** (many features at once) or you **squash/merge with a clear summary**.
- **Caution:** Merging now bundles **many changes** without an incremental history on `main`. Prefer a **PR checklist**: compile CI green, optional harness smoke, note known debt (Phase 5 monolith split, optional harness in hosted CI).
- **Wait, if:** `main` must stay minimal until an external release gate; or `dev` has known broken behavior you have not smoke-tested.

**Recommendation:** **Merge `dev` into `main` soon** so `main` is not a misleading stub—**after** pushing latest `dev`, CI green, and a quick smoke on Mac Studio. Track compliance work on **`dev`** (or feature branches) and merge **`main`** only when you want that line to match what you ship.

---

## Changelog (this document)

| Date | Change |
|------|--------|
| 2026-03-26 | Initial plan, progress table, merge advice, git hygiene. |
| 2026-03-26 | Phase 0 baseline commit: `d9302b9` — rules, compliance plan, `run.sh` hardening, nav/swagger. |
| 2026-03-26 | Phases 1–3: scope env flag + pytest/CI + `reporting/` package; `REPORTING_SCOPE_PREPROCESSING` default aligns with legacy **off**. |
| 2026-03-26 | Phase 4: `reporting/regex_contract.py` + tests; CI compiles module. |
