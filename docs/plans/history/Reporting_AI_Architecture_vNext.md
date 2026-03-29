# Reporting AI Architecture (vNext – Canonical Plan First)

## Purpose

Describe how the reporting chat **actually** works in this repository so the doc stays **literally true**. Normative shapes live in:

- `reporting/plan_models.py` — `CanonicalReportingPlan`, enums, `SortSpec`, adapters
- `reporting/reporting_plan.schema.json` — JSON schema mirror (when present)

---

## Core principles (implemented)

1. **Single plan input to SQL** — After validation, the only compiler input is a `CanonicalReportingPlan` (via `from_legacy_semantic_plan` → `validate_canonical_structure` → `validate_canonical_semantics` → `_reporting_plan_to_sql`).
2. **No SQL from the planner LLM** — The planner model emits **JSON plan only**; SQL comes from `_reporting_plan_to_sql` / `_reporting_plan_to_sql_legacy`.
3. **Deterministic compilation** — Same validated plan → same SQL (modulo limits and date injection from the HTTP layer).
4. **Advisory hints, not silent merges** — `_reporting_heuristic_plan`, session learned hints, and detected dates are assembled into `planner_hints` and sent to the planner as **JSON context**; they **do not** overwrite the model’s plan. **`_reporting_explicit_constraints`** (phrase/regex extractions from the user text, e.g. exclusions) are merged **after** the LLM JSON via `_reporting_merge_explicit_constraints_into_plan` so structured user language is not dropped. The executable plan is built from the LLM JSON via `_reporting_legacy_plan_from_llm_dict`, then explicit merge, then:
   - `_reporting_repair_completion_cost_vs_ranked_purchases` corrects the known bad pair “`completion_cost` + ranked purchase language” → `list_inventory` + `sort.purchase_price` + limit.
   - `_reporting_prune_conflicting_filters` removes ambiguous cross-dimension filters.
5. **No post-hoc plan mutation after execution** — The former “relax and re-run” pass is **removed**; empty or unhelpful results are not silently “fixed” by mutating filters.

---

## End-to-end pipeline

```
Question + session context block
  → retrieval grounding (corpus/Chroma via retrieve_artifacts_with_meta + format_retrieval_context, in the planner user message)
  → planner hints payload (explicit + heuristic summary + learned + dates + prior summary on follow-ups)
  → LLM JSON plan (retry once with retry_model if the first parse fails)
  → legacy dict from LLM only (+ repairs above)
  → CanonicalReportingPlan
  → structural + semantic validation
  → deterministic SQL
  → execution
  → responder LLM (natural language answer; optional)
```

If the planner returns no JSON object after retries, the system returns a **clarification** plan (`needs_clarification`) instead of fabricating a heuristic-first plan.

---

## Retrieval & templates (bounded)

- **Compare UI / template path** — When the client sends `compare_dimension` + values, `_reporting_template_sql` may run **without** going through the semantic planner (predictable compare UX).
- **Other template shortcuts** — `_reporting_template_sql` still handles a small set of legacy phrasings (e.g. monthly spend, tactical list) when hit by those code paths; primary NL queries use the planner + compiler above.
- **Optional scope preprocessing** — `REPORTING_SCOPE_PREPROCESSING` can short-circuit with fixed clarification copy (see `reporting/domain.py`).

---

## Follow-ups and session state

- **Conversation text** — Prior turns are included as a `context_block` for the planner/responder.
- **Last structured plan** — Stored for UX; summarized into `prior_turn_plan_summary` inside planner hints on follow-ups. For short contextual follow-ups, `_reporting_apply_followup_carryover` may merge **prior filters** and switch aggregate → `list_inventory` when the user clearly asks for rows (e.g. “list the knives that made up that number”).

---

## Testing expectations

- Planner output → canonical plan → SQL: route and compiled SQL should match intent (see `tests/test_reporting_sql_helpers.py`, reporting harness on a live server).
- **Equivalence** — Paraphrases that should share semantics should converge on the same canonical intent/metric/sort after validation and repair.

---

## Non-goals (current code)

- **Direct LLM SQL** — `_reporting_call_llm_for_sql` exists in the module but is **not** wired into the primary `run_reporting_query` path in this tree.

---

## Success criteria

- NL → **one** structured plan → deterministic SQL → results; traceability via stored `semantic_plan` and `generation_mode`.
- Document stays aligned with `plan_models.py` / schema; when behavior changes, update this file in the same change.
