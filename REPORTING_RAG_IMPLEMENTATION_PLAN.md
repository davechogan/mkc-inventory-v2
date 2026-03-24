# Reporting + Collection Q&A (RAG) Implementation Plan

## 1. Goal

Add a new `Reporting` page that lets you ask natural-language questions about your collection and get results as:

- plain text answer
- grid/table
- graph/chart

Data source should be the app database (`mkc_inventory.db`) with safe query + summarization behavior.

---

## 2. Model Choice (Speed vs Accuracy)

### What was checked

- `ollama list` was attempted from this workspace shell and is not available in this runtime (`command not found`).
- Model inventory was retrieved from the running app's Ollama endpoints (`/api/ai/ollama/check`, `/api/ai/ollama/models`) on the configured host.

### Available relevant text models (subset)

- `qwen2.5:7b-instruct`
- `qwen2.5:32b-instruct`
- `glm-4.7-flash:latest`
- `gemma3:27B`
- `qwen2.5:72b-instruct`
- `deepseek-r1:70b`

### Recommended default

- **Default:** `qwen2.5:7b-instruct`
  - Best speed/latency for interactive reporting and follow-up questions.
  - Good enough quality for SQL planning + concise narrative responses.
- **Optional high-accuracy mode:** `qwen2.5:32b-instruct`
  - Slower, but stronger for complex multi-step analytical prompts.

### Model selector behavior

- Page-level model selector with persisted preference (similar to existing Ollama preference pattern).
- Per-question override allowed.

---

## 3. Scope of First Version

### In scope (v1)

- New `/reporting` page and nav link.
- Chat-style question input + response history in-session.
- Structured result payload from backend:
  - `answer_text`
  - `result_rows` (table-safe rows)
  - `chart_spec` (basic bar/line/pie)
  - `sql_executed` (read-only SQL, optional shown behind a toggle)
- Output tabs: `Text`, `Grid`, `Graph`.
- Safe read-only query generation/execution against curated views.
- Export current result (CSV for grid, PNG/SVG optional for chart in v2).

### Out of scope (v1)

- Arbitrary write-back actions to DB.
- Cross-database connections.
- Full BI dashboard builder.

---

## 4. Data/RAG Strategy

Use a hybrid approach:

1. **Schema-aware prompting**
   - Provide model with compact schema dictionary + semantic field descriptions.
2. **Curated semantic views for analytics**
   - Example: inventory value by family, purchases over time, steel mix, condition distribution, collab breakdown.
3. **SQL generation with strict guardrails**
   - Allow only `SELECT` against approved tables/views.
4. **Answer synthesis**
   - LLM converts tabular output to narrative summary.
5. **Visualization hinting**
   - Backend infers chart type from result shape (time series -> line, category breakdown -> bar/pie).

---

## 5. Backend Design

### New endpoints (proposed)

- `GET /reporting` -> serve `static/reporting.html`
- `GET /api/reporting/schema` -> allowed schema metadata for prompt context
- `POST /api/reporting/query`
  - input: `question`, `model`, `max_rows`, `chart_preference?`
  - output: `answer_text`, `rows`, `columns`, `chart_spec`, `sql_executed`, `timings`
- `POST /api/reporting/suggested-questions` (optional seed prompts)

### Safety rules

- Block non-`SELECT` SQL.
- Block semicolons/multi-statements.
- Block PRAGMA/ATTACH/DETACH/DDL/DML.
- Enforce table/view allowlist.
- Enforce row/time limits.
- Add timeout and friendly partial-result handling.

### Recommended internal modules

- `reporting_schema.py` (schema dictionary + field semantics)
- `reporting_sql.py` (SQL validation + execution)
- `reporting_prompt.py` (prompt builders)
- `reporting_chart.py` (chart-spec inference)

---

## 6. Frontend Design

### New files

- `static/reporting.html`
- `static/reporting.js`

### UX layout

- Left: conversation thread + input
- Right/top: result tabs (`Text`, `Grid`, `Graph`)
- Header: model selector + "new chat" + "export"

### Grid behavior

- Sort columns
- Client filter within returned rows
- CSV export

### Graph behavior (v1)

- Use simple chart spec:
  - `{type, x, y, series?, data}`
- Support:
  - bar (category)
  - line (date/time)
  - pie (share breakdown)

---

## 7. Recommended Extra Capabilities

Add these to make the page genuinely useful:

- [x] **Question templates:** "What did I spend by month?", "Most valuable knives", "How many by steel/family?"
- [x] **Follow-up memory in session:** "Now only for Traditions."
- [x] **Query transparency toggle:** show SQL + execution time.
- [x] **Saved prompts/views:** bookmark frequent analyses.
- [x] **Result confidence/limitations notes:** explicitly state assumptions when inferred.
- [x] **Comparison mode:** compare two families/series/periods.
- [x] **Natural language date ranges:** "last 90 days", "this year", "since Jan 2025".
- [x] **Drill-through links:** click chart/table row to open matching inventory subset.

### Implementation Notes (Mar 2026)

- Reporting memory is persisted in `reporting_sessions` and `reporting_messages` (DB-backed, not in-browser-only), with rolling summaries (`memory_summary`) and bounded recent-turn context.
- Context management follows scalable patterns:
  - rolling window for recent messages
  - summary compression for longer threads
  - bounded prompt size for SQL and answer generation
  - explicit metadata capture (`generation_mode`, `confidence`, `limitations`, `execution_ms`)
- Session list + reload supports cross-session continuation and future expansion to user-scoped memory.

---

## 8. Implementation Phases

### Phase 1 - Foundation

- [x] Add `/reporting` page route and nav link.
- [x] Create base UI shell with chat panel + output tabs.
- [x] Add model selector with persisted default.
- [x] Add backend schema metadata endpoint.

### Phase 2 - Safe Query Engine

- [x] Build SQL allowlist validator.
- [x] Implement read-only execution with row/time limits.
- [x] Add structured response contract (`text/grid/graph`).
- [x] Add robust error mapping for user-friendly failures.

### Phase 3 - RAG + Answer Synthesis

- [x] Build prompt templates using schema + examples.
- [x] Generate SQL from question and validate.
- [x] Execute SQL and synthesize narrative answer.
- [x] Add fallback model path on generation failure.

### Phase 4 - Visualization + Grid UX

- [ ] Render tabular results with sorting/filtering.
- [x] Infer/render basic charts from result shape.
- [x] Add CSV export.
- [x] Add SQL transparency toggle.

### Phase 5 - Quality + Guardrails

- [ ] Add test set of representative reporting questions.
- [ ] Validate outputs against known totals (spend/value/count).
- [ ] Add prompt-injection resistance checks.
- [ ] Add telemetry/logging for performance and failures.

### Phase 6 - Nice-to-Have Enhancements

- [x] Saved queries
- [x] Suggested follow-up prompts
- [x] Drill-through links into Collection/Catalog filters

---

## 9. Acceptance Criteria

- [ ] User can ask a natural-language question and get a useful answer in <10s on default model for common queries.
- [ ] Same result can be viewed as text, grid, or graph when data shape allows.
- [ ] Query execution is read-only and cannot mutate DB.
- [ ] Model can be changed from the page and preference persists.
- [ ] At least 15 canonical reporting questions pass expected-value checks.

---

## 10. Open Decisions

- [ ] Keep default model fixed (`qwen2.5:7b-instruct`) or auto-upshift to `qwen2.5:32b-instruct` for complex prompts?
- [ ] Which chart library to use (lightweight vs existing/no-dependency approach)?
- [ ] Do we expose SQL by default or behind an advanced toggle only?
- [ ] Should saved reports be global or user-local (browser local storage initially)?

---

## 11. Architecture Hardening Track (Current Priority)

This is the implementation track we are now using to improve reliability and reduce brittle SQL generation behavior.

### 11.1 Semantic JSON DSL + Validator + SQL Compiler

- [ ] Define canonical semantic plan schema (`intent`, `filters`, `group_by`, `metric`, `time_window`, `limit`)
- [ ] Add strict semantic validator (allowed intents, dimensions, metrics, filter keys)
- [ ] Compile semantic plans to safe SQL over curated reporting views only
- [ ] Keep free-form SQL generation as fallback only, not primary path
- [ ] Add plan metadata to response/session for debugging (`generation_mode`, planner path, validator stage)

### 11.2 Follow-up DSL Mutation Memory

- [ ] Persist last semantic plan per reporting session (`last_query_state_json`)
- [ ] Implement follow-up mutators (examples: “only Traditions”, “look at series”, “now by family”)
- [ ] Detect short follow-up vs topic reset and avoid stale-scope errors
- [ ] Merge explicit user constraints first; reject ungrounded implicit filter injection
- [ ] Include semantic state summaries in context blocks for planner continuity
- [x] Investigate follow-up carryover regression:  
      Example repro: `how many "goat" knives do I have?` -> `list them` returns no rows instead of applying prior scope.

### 11.3 Evaluation Harness + Telemetry + Model Routing Policy

- [x] Build canonical prompt evaluation harness (initial script in `tools/reporting_eval_harness.py`)
- [x] Expand canonical prompt suite to >= 40 prompts across major intents
- [x] Add golden expected checks for critical metrics (count/value/missing models; completion cost)
- [x] Add telemetry for planner failures, validator rejects, empty-result rates, retries, latency
- [x] Implement multi-model routing policy:
  - planner model (higher reasoning)
  - responder model (fast summarization)
  - optional retry/escalation model
- [x] Add confidence/retry thresholds and fallback policy before deterministic template fallback

### 11.4 Multi-Model Companion Plan

- Detailed execution document: `REPORTING_MULTI_MODEL_ARCHITECTURE_PLAN.md`
- This companion plan defines:
  - model split (planner/responder/retry)
  - runtime configuration variables
  - phased rollout and acceptance criteria

### 11.5 Adaptive Semantic Hint Learning (New)

Goal: let the system "learn" successful interpretation patterns from chat outcomes, but treat them as confidence-weighted hints rather than hard rules.

- [x] Add persistent semantic-hint store (`reporting_semantic_hints`) with confidence, evidence count, and success/failure feedback fields.
- [x] Add hint extraction from colloquial phrasing (`entity + cue` patterns like `"Blood Brothers family"`).
- [x] Apply hints as soft priors during semantic planning (only fill missing filters; never override explicit constraints).
- [x] Add confidence feedback loop: reinforce hints on successful non-empty responses, decay on failed applications.
- [x] Add observability endpoint (`GET /api/reporting/hints`) to inspect learned hints by session.
- [x] Add explicit user feedback signal (thumbs up/down) to accelerate confidence updates.
- [ ] Promote high-confidence repeated session hints into optional global hints with moderation.
