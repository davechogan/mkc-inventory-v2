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

### NEVER overwrite the production DB on Mac Studio

The production DB lives on the Mac Studio at `/Users/dhogan/invapp_v2/data/mkc_inventory.db`. The deploy script **excludes** it — code deploys never touch the production data. **Do not manually copy, rsync, or scp the dev DB to the Mac Studio.** All inventory and catalog data changes are made by the user on `macstudio:8008`.

- **Schema migrations** (new columns, new tables): add to `ensure_v2_exclusive_schema()` in `migrations/migrate_v2.py` — runs automatically on app startup.
- **Destructive schema changes** (drops, rebuilds): write a migration script, deploy the code, then run the script on the Mac Studio DB manually.

### Backup before schema changes

```bash
./scripts/backup_mkc_db.sh
```

`data/mkc_inventory.db` is the live production DB containing real collection data. Back it up before any `ALTER TABLE`, `CREATE TABLE`, `DROP TABLE`, or bulk `UPDATE`/`DELETE`.

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

## Architectural decisions — read before changing anything

These are intentional design choices. Do not "fix" or simplify them without understanding why they exist.

### 1. Only two intents: `list` and `missing_models`

`PlanIntent` has exactly two values and this is deliberate. We previously had more intents (`aggregate`, `compare`, `list_inventory`, `catalog_gap`, etc.) and the LLM was unreliable at distinguishing them — subtle phrasing differences produced wrong intent classifications and broken SQL paths.

The collapse works because:
- `list` covers everything data-returning: individual rows, counts, totals, breakdowns, year comparisons. The semantic meaning is carried by `metric` (count/total_spend/estimated_value/msrp) and `group_by`, not by the intent.
- `missing_models` is the only genuinely distinct path — it JOINs catalog against inventory to find what you don't own.

**Do not add new intents** without a compelling reason. If a query type feels like it needs a new intent, first check whether `metric` + `group_by` + `filters` can express it within `list`.

### 2. The LLM never writes SQL

The LLM produces a structured JSON plan (`CanonicalReportingPlan`). A deterministic compiler (`reporting/compiler.py`) translates that plan to SQL. The LLM has no visibility into the SQL and cannot influence it except through the validated plan fields.

This is intentional for two reasons:
- **Safety**: LLM-generated SQL is a SQL injection vector. The compiler only emits parameterized-style queries using a strict field allowlist.
- **Reliability**: SQL generation from a validated typed struct is deterministic and testable. LLM-generated SQL is not.

Never add a path where the LLM writes SQL directly, even as a fallback.

### 3. The query rewriter is a separate LLM call before retrieval

Before Chroma retrieval runs, a lightweight LLM (`qwen2.5:7b-instruct`) rewrites the user's question into a standalone question. This exists because Chroma is context-blind — a follow-up like "do I own one of those?" embeds near generic ownership artifacts, not the field-specific artifacts needed to answer correctly.

The rewriter resolves pronouns ("those", "them", "it") to concrete entity names from the last query state, giving Chroma a question it can actually match against the corpus.

**Critical**: the rewriter context is intentionally limited to `scope`, `filters`, `group_by`, and `year_compare` from the last query state. `sort` and `limit` are excluded. When they were included, the rewriter injected noise like "highest msrp" or "in the catalog" that pulled the wrong retrieval artifacts and confused the planner. Do not add `sort` or `limit` back to the rewriter context.

### 4. `CanonicalReportingPlan` is the only compiler input

The compiler has one entry point: `compile_plan(plan: CanonicalReportingPlan, ...)`. There is no legacy dict path, no alternate input format. This was a deliberate refactor — previously the compiler accepted an untyped dict which made it impossible to reason about what fields were valid.

If you need the compiler to handle something new, add it to `CanonicalReportingPlan` with proper validation, not as a side-channel.

### 5. Plan validation coerces, compiler skips — neither raises

`CanonicalReportingPlan` validators silently coerce common LLM output errors (null metric → count, unknown intent → list, `==` op → `=`, flat dict filters → list of clauses). This is intentional — the LLM is imperfect and hard validation would cause unnecessary failures on recoverable mistakes.

Similarly, the compiler's `_clause_expr()` returns `None` for fields that don't exist in the target view (e.g. `condition` in a catalog query) rather than raising. Callers skip `None` results silently.

The line: fix recoverable LLM mistakes at the plan layer, not the compiler. The compiler only compiles valid plans.

### 6. Scope auto-correction in the plan model

If the LLM sets `scope=catalog` but includes inventory-only fields (`condition`, `knife_name`, `acquired_date`, etc.) in filters or exclusions, `CanonicalReportingPlan` silently corrects `scope` to `inventory`. This handles a common LLM error where it correctly identifies inventory-specific fields but forgets to update the scope.

This correction lives in `plan_models.py` (`_coerce_scope_from_fields`), not the compiler. Keep it there.

### 7. Chroma auto-rebuilds on corpus fingerprint change

The Chroma index is not rebuilt on every startup or query. It is rebuilt only when the corpus content changes (detected via SHA-256 fingerprint of all artifact content). The fingerprint is stored in `reporting/.chroma/retrieval_chroma_manifest.json`.

When you add or edit files in `reporting/corpus_docs/`, the fingerprint changes and Chroma rebuilds automatically on the next query. You do not need to manually trigger a rebuild. Do not delete `.chroma/` as a "fix" for retrieval issues — diagnose the actual problem first.

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
