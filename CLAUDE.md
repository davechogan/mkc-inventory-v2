# MKC Inventory — Agent & Developer Orientation

Read this file first. It covers everything an agent or new developer needs to know before touching code.

---

## What this app is

A personal knife collection inventory and catalog app for Mountain Knife Company (MKC) gear. Built on FastAPI + SQLite. The main features are:

- Inventory management (owned knives, purchase history, valuation)
- MKC catalog (all models ever produced)
- **AI-powered reporting** — natural language queries over inventory and catalog via an LLM pipeline

The reporting pipeline is the most active area of development. It is the system you are most likely working on.

---

## Critical: how to commit

**Never use bare `git commit`. Always use:**

```bash
scripts/commit.sh "your commit message"
```

This repo has a **private git submodule** at `artifacts/` (`mkc-inventory-artifacts`). The commit script commits any pending artifact changes to the submodule first, then commits the main repo with the same message — keeping both repos in sync. Using bare `git commit` orphans artifact changes.

If you are certain no artifact changes are pending, bare `git commit` is acceptable. When in doubt, use the script.

---

## Artifacts submodule

`artifacts/` is a private submodule pointing to `git@github.com:davechogan/mkc-inventory-artifacts.git`.

**Do not edit files inside `artifacts/` and commit them to the main repo.** Changes to `artifacts/` are tracked by the submodule's own git history. The main repo only tracks the submodule pointer (a commit hash).

First-time setup (requires access to the private repo):

```bash
git submodule update --init
```

What lives there:

| Path | Contents |
|---|---|
| `artifacts/db_snapshots/mkc_inventory_seed.db` | Canonical test seed DB |
| `artifacts/db_snapshots/<YYYY-MM-DD>/` | Dated point-in-time DB backups |
| `artifacts/plans/` | Architecture plans and implementation specs |
| `artifacts/scripts/` | Non-runtime helper scripts |
| `artifacts/metadata/` | Provenance templates |
| `artifacts/logs/`, `artifacts/exports/` | Local use only — gitignored |

The test suite (`tests/conftest.py`) automatically copies the seed DB to a temp location before running. Tests write session data to the copy, never to the canonical seed.

---

## Python environment

Always use the project virtualenv:

```bash
.venv/bin/python      # interpreter
.venv/bin/pytest      # tests
./scripts/run.sh      # start the API server
./scripts/ci_local.sh # local CI gate (run before committing)
```

Do not use system Python or globally installed tools. If `.venv` is missing: `python3.11 -m venv .venv && .venv/bin/pip install -r requirements.txt`.

---

## Running tests

```bash
# Fast unit tests (no Ollama required)
.venv/bin/pytest tests/ -m "not live_llm" -q

# Full suite including live LLM quality tests (requires Ollama running)
.venv/bin/pytest tests/ -v -m live_llm -s

# All tests
.venv/bin/pytest tests/ -q
```

Expected: **53 unit tests** pass without Ollama. **3 live LLM tests** require Ollama at the configured host.

---

## Architecture: reporting pipeline

The reporting pipeline is the core of active development. Data flows in one direction through distinct layers — fix problems at the correct layer, not the symptom.

```
User question
     │
     ▼
[1] Query rewriter (qwen2.5:7b-instruct)
     │  Rewrites follow-up questions into standalone queries for retrieval
     │  File: reporting/planner.py → _reporting_rewrite_query_for_retrieval()
     ▼
[2] Chroma retrieval
     │  Semantic search over corpus_docs/ artifacts to ground the planner
     │  File: reporting/retrieval.py
     ▼
[3] LLM planner (qwen2.5:32b-instruct)
     │  Converts question + grounding into a CanonicalReportingPlan (JSON)
     │  File: reporting/planner.py → _reporting_llm_plan()
     ▼
[4] Plan validation
     │  Pydantic validation + coercion of the canonical plan
     │  File: reporting/plan_models.py, reporting/plan_validator.py
     ▼
[5] SQL compiler
     │  Translates CanonicalReportingPlan → SQL, no LLM involvement
     │  File: reporting/compiler.py
     ▼
[6] SQL execution + response generation
     │  Runs SQL against SQLite, generates natural language answer
     │  File: reporting/domain.py
     ▼
Answer + rows
```

**Key types:**
- `CanonicalReportingPlan` — the only input to the compiler; defined in `reporting/plan_models.py`
- `PlanField` / `PlanDimension` — enums of all valid filter fields and group-by dimensions
- `FilterClause` — a single filter or exclusion with `field`, `op`, `value`

**Key views (SQLite):**
- `reporting_inventory` — owned knives with purchase/valuation data
- `reporting_models` — full MKC catalog

---

## DB safety

`data/mkc_inventory.db` is the live production DB containing real collection data. Before any operation that could modify it:

```bash
./scripts/backup_mkc_db.sh
```

The test suite always uses a temp copy of the seed DB — never the live DB. `MKC_INVENTORY_DB` env var controls which DB the app uses; if unset, it defaults to `data/mkc_inventory.db`.

---

## Key files

| File | Purpose |
|---|---|
| `app.py` | FastAPI app, DB init, all routes |
| `reporting/domain.py` | Reporting pipeline orchestration |
| `reporting/planner.py` | LLM planner + query rewriter |
| `reporting/plan_models.py` | Canonical plan types and validation |
| `reporting/compiler.py` | Plan → SQL compiler |
| `reporting/retrieval.py` | Chroma retrieval |
| `reporting/constants.py` | Model names, allowed sources, groupable dimensions |
| `reporting/corpus_docs/` | Grounding artifacts indexed by Chroma |
| `tests/conftest.py` | Test DB setup (copies seed from artifacts/) |
| `scripts/commit.sh` | Synchronized commit wrapper — use this |
| `scripts/ci_local.sh` | Local CI gate |
| `scripts/backup_mkc_db.sh` | DB backup utility |

---

## Detailed standards

Full rules are in `.cursor/rules/` and apply to all agents and developers:

| Rule file | Covers |
|---|---|
| `git-workflow.mdc` | Branching, commit messages, merge criteria |
| `artifacts-repo-workflow.mdc` | What goes in `artifacts/`, submodule usage |
| `engineering-standards.mdc` | Architecture layers, anti-patterns |
| `testing-and-nlp-quality.mdc` | Test depth, NLP/planner invariants |
| `sqlite-inventory-db-safety.mdc` | DB backup requirements |
| `python-env-and-ci.mdc` | Virtualenv, CI, deploy |
| `ai-change-checklist.mdc` | Self-check before finishing any change |

---

## Deployment

The app runs on a Mac Studio at `macstudio:8008`. To push a tested release:

```bash
scripts/push_release_to_macstudio.sh --dry-run   # verify
scripts/push_release_to_macstudio.sh --release   # deploy
```

Requires `DEPLOY_HOST`, `DEPLOY_USER`, `DEPLOY_PATH` env vars set. The script backs up the remote DB before syncing.
