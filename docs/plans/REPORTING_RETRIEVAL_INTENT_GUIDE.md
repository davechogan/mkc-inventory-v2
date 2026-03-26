# DEPRECATED / IGNORED

This document is retained for historical context only.

Do not use this guide for current implementation decisions.

Use these canonical sources instead:
- [`Reporting_AI_Architecture_vNext.md`](./Reporting_AI_Architecture_vNext.md)
- [`Reporting_AI_Phase_Next_Plan_Schema_Validation_Spec.md`](./Reporting_AI_Phase_Next_Plan_Schema_Validation_Spec.md)

---

# Reporting Retrieval-Intent Guide (vNext)

## Goal
Improve reporting accuracy and stability by using retrieval-first guidance for the semantic layer (intent + field/schema hints) and keeping SQL generation safe and deterministic via validation/execution.

This guide captures the pattern we discussed:
1. Use embeddings / vector DB to retrieve intent + relevant schema/rules.
2. Provide the LLM with a fixed, minimal "prompt package" (original question + retrieved semantic layer info).
3. Let the LLM propose SQL (or a canonical plan), then run the existing SQL validator/executor.
4. Synthesize an answer strictly from returned rows.

This explicitly avoids using conversation-history-derived constraints as implicit SQL filters (the source of the drift we observed in direct-LLM-SQL mode after long/complex turns).

## Why retrieval-first (problem we are solving)
We observed that when the model is exposed to prior chat context and extra preprocessing/constraints, it can:
- Persist or reinterpret constraints across turns.
- Pick the wrong canonical dimension (e.g., family vs series).
- Produce "plausible" SQL that is semantically wrong (runs but returns empty or unchanged totals).
- Drift after complicated prompts (A/B test evidence).

Retrieval-first reduces this by grounding the semantic layer in:
- Small, curated artifacts (intent prototypes, schema slices, rule snippets, example query shapes).
- Short, deterministic metadata about what to do next.

## Core pattern (data flow)
### 1. Normalize the user question
Perform only lightweight normalization:
- Trim whitespace.
- Canonicalize simple aliases where unambiguous (e.g., "traditions" -> "Traditions" if this is a known series alias).
- Detect explicit scope if required (inventory vs full catalog), but do not infer extra filters beyond what is explicit.

### 2. Build an embedding query and retrieve semantic artifacts
Compute an embedding for the user question (and optionally a small intent label string).

Use a vector DB to retrieve:
- Intent candidates (top-k intents).
- Relevant schema slices (allowed views, key join keys, which field corresponds to which user phrase).
- Relevant rules (dimension interpretation rules, e.g. treat "Traditions", "VIP", "Ultra", "Blood Brothers" as series_name unless user explicitly asks for type/family).
- Relevant example patterns (small SQL templates / plan shapes for the intent).

Retrieval should be:
- Shallow: small number of chunks (for example top-3 to top-8).
- Curated: stored artifacts should be stable and reviewed.

### 3. Assemble a fixed prompt package (minimal and structured)
Provide the LLM with a fixed structure every time:
- `original_question`: the raw question as typed.
- `retrieved_intents`: top-k intents with short labels/scores.
- `retrieved_schema_guidance`: a compact list of only the relevant schema facts:
  - allowed views
  - canonical dimension mapping (user phrase -> column/view semantics)
  - join/selection keys if needed
- `retrieved_rules`: only the relevant interpretation/rule snippets.
- `generation_constraints`: strict safety rules (only SELECT, only allowed views/columns, no semicolons, etc).
- `response_contract`: JSON-only contract with fields required by our backend (for example: sql + follow_ups + limitations + confidence).

Important: do NOT include:
- rolling conversation history
- "last_query_state_json" filters as implicit guidance
- previous SQL or previous result-derived inferred filters as implicit WHERE conditions

If we need follow-up handling, treat it as:
- explicit user references (e.g., "only Traditions" when the question still uses explicit constraint terms), or
- a narrow heuristic that changes output shape while keeping the scope stable, but never silently compounds filters across unrelated turns.

### 4. SQL generation, validation, and execution
The LLM (or the semantic layer compiler) proposes SQL.

Backend must then:
1. Validate SQL against allowlist and safety rules (existing `_reporting_validate_sql` and exec path).
2. Execute using the safe executor with row limit.
3. If SQL is invalid or returns empty:
   - optionally attempt one constrained retry using a different retrieved chunk (or a template fallback)
   - return a helpful "no rows matched" response without hallucinating additional filters.

### 5. Answer synthesis (grounded)
The final narrative answer must be grounded in:
- returned rows
- returned columns

The responder must not invent missing entities. If rows are empty, the answer must explain empty result and suggest broader explicit alternatives.

## What to retrieve (recommended vector DB collections)
Split retrieval artifacts into separate indexes or tagged collections:
1. `intent_prototypes`
   - Examples: "aggregate by family", "last purchase", "missing models", "count by dimension", "value breakdown"
2. `dimension_mapping_rules`
   - Canonical mapping rules:
     - user phrase -> planner/SQL dimension
     - series vs family vs type distinctions
   - Provide disambiguation examples.
3. `schema_slices`
   - Small docs describing:
     - `reporting_inventory` key columns
     - `reporting_models` key columns
     - which view to use for which intent
4. `sql_template_shapes` (optional but useful)
   - small snippets that show query structure (GROUP BY patterns, top-N patterns)
   - still subject to allowlist validation.

## Guardrails (explicit rules to prevent drift)
1. "No hidden filters from chat"
   - In direct-to-SQL mode and any future retrieval-driven mode, do not allow prior turn summaries to silently become WHERE clauses.
2. "Explicit constraints win"
   - If the current question includes explicit exclusions ("excluding X"), those exclusions should be retrieved/normalized and applied.
3. "Canonicalization before SQL"
   - Normalize exclusion entities and dimension names before SQL generation.
   - For example, if your canonical knife names include "2.0" suffixes, do not drop the suffix in exclusions.
4. "Dimension selection is deterministic when possible"
   - If the question contains "family ... knives", prefer `family_name`.
   - If it contains "series ... knives", prefer `series_name`.
   - When ambiguous, ask a clarification question instead of guessing.
5. "Validate what the model emits"
   - Safety validator remains the final gate.
   - Also consider adding a semantic validator: verify that the generated SQL uses the dimension implied by the retrieved mapping.

## Acceptance criteria
For direct or retrieval-driven modes, we should hit:
- Correct dimension selection for common phrases ("family knives" returns `family_name`).
- Exclusion questions consistently exclude intended canonical entities (e.g., suffix-safe match).
- After long/complex question sequences, later questions should not drift:
  - "list the blackfoot family knives" should not start returning empty rows when earlier prompts discussed exclusions.
- When SQL is invalid:
  - user sees a friendly error, and we can retry/fallback.

## Current repo mapping (important)
As of now, the repository codebase uses:
- LLM planning and heuristic gates
- deterministic SQL compilation for the canonical path
- direct LLM SQL in the A/B toggle
- SQLite semantic hints (entity/cue -> dimension/value)

However, a true "embeddings + vector DB" semantic retrieval layer is not implemented as dense retrieval for NL intent/schema.

This guide is intended to be the blueprint for the next phase:
- add an embedding model
- add vector DB retrieval for intent and schema docs
- swap the "context-summary" prompt input with a "retrieved semantic artifacts" prompt package
- keep the existing SQL validator/executor unchanged.

## Minimal implementation plan (phaseable)
Phase 1 (fast, low-risk):
- Implement the retrieval prompt package assembly (even with a stub retriever).
- Add prompt logging: store retrieved intents/rules/schema slices used per query.

Phase 2:
- Add embedding computation and indexing for the curated semantic artifacts.
- Integrate into direct-mode A/B endpoint (or a new toggle) behind a feature flag.

Phase 3:
- Add dimension/canonicalization pre-normalization for exclusions and ambiguous dimension terms.
- Add unit tests that assert stable `sql_executed` selection across question sequences.

Phase 4:
- Add evaluation harness:
  - run the same question series before/after complicated turns
  - ensure later outputs remain stable.

