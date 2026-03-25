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
| Monolith / layers | `engineering-standards.mdc` | **Gap** — `app.py` ~8.7k lines; some domain split (`gap_analysis_core`, `normalized_model`, `blade_ai`, …). |
| Canonical reporting vs regex | `engineering-standards.mdc` | **Partial** — semantic plan + SQL compiler exist; many heuristics/guardrails still in `app.py`. |
| Scope preprocessing | `engineering-standards.mdc` | **Debt** — `if False and _reporting_*` disables clarification; replace with env flag (Phase 1). |
| Automated tests | `testing-and-nlp-quality.mdc` | **Gap** — CI = `py_compile` only; no `pytest`; harness is integration-only. |
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
| Replace `if False and _reporting_*` with env var (e.g. `REPORTING_SCOPE_PREPROCESSING`) | [ ] | Default = production behavior; document in one place. |

### Phase 2 — Pytest + CI

| Step | Done | Notes |
|------|------|--------|
| Add `pytest` (dev/deps) and `tests/` | [ ] | |
| Unit tests: `plan_to_sql`, unsafe-request guardrail, scope helpers | [ ] | No server required. |
| Extend GitHub `checks` job with `pytest tests/` | [ ] | |

### Phase 3 — Extract reporting module

| Step | Done | Notes |
|------|------|--------|
| Move reporting semantics out of `app.py` into `reporting/` package | [ ] | Thin FastAPI routes only. |

### Phase 4 — Regex / plan surface

| Step | Done | Notes |
|------|------|--------|
| Prefer structured plan fields over new one-off regex | [ ] | Document legacy vs contract regex blocks. |

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
- **Caution:** Merging now bundles **many changes** without an incremental history on `main`. Prefer a **PR checklist**: compile CI green, optional harness smoke, note known debt (`if False` scope gates, no pytest yet).
- **Wait, if:** `main` must stay minimal until an external release gate; or `dev` has known broken behavior you have not smoke-tested.

**Recommendation:** **Merge `dev` into `main` soon** so `main` is not a misleading stub—**after** pushing latest `dev`, CI green, and a quick smoke on Mac Studio. Track compliance work on **`dev`** (or feature branches) and merge **`main`** only when you want that line to match what you ship.

---

## Changelog (this document)

| Date | Change |
|------|--------|
| 2026-03-26 | Initial plan, progress table, merge advice, git hygiene. |
| 2026-03-26 | Phase 0 baseline commit: `d9302b9` — rules, `COMPLIANCE_PLAN.md`, `run.sh` hardening, nav/swagger. |
