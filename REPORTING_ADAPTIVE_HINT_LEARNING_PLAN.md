# Reporting Adaptive Hint Learning Plan

## Purpose

Improve natural-language robustness by learning from successful chat outcomes without hard-coding phrase-specific rules.

This design intentionally treats learned behavior as a **hint** (prior) with confidence, not a deterministic rule.

## Core principles

1. **Soft prior, never hard override**
   - Learned hints may influence planning only when explicit user constraints do not already specify the dimension.

2. **Confidence-weighted use**
   - Hints are used only above a minimum confidence threshold.
   - Confidence increases with repeated successful use and decays with failed use.

3. **Session-first memory**
   - Hints are scoped to session first to avoid accidental cross-session pollution.
   - Optional global promotion is a later phase with stronger guardrails.

4. **Evidence over assumptions**
   - Learn only from successful, non-empty query outcomes tied to semantic plans.
   - Avoid learning from empty/error answers.

## Data model

Table: `reporting_semantic_hints`

- `scope_type` (`session` / `global`)
- `scope_id` (session id for session hints)
- `entity_norm` (normalized phrase entity, e.g., `blood brothers`)
- `cue_word` (user term, e.g., `family`, `type`, `series`)
- `target_dimension` (resolved planner dimension)
- `target_value` (resolved filter value)
- `confidence`
- `evidence_count`
- `success_count`
- `failure_count`
- timestamps

## Runtime flow

1. Parse question for `(entity, cue)` candidates (e.g., `"Blood Brothers family"`).
2. Fetch matching session/global hints above confidence threshold.
3. Merge hint filters as **fill-only** priors into semantic plan filters.
4. Execute compiled safe SQL path.
5. Feedback:
   - if hint used + non-empty result: reinforce confidence
   - if hint used + empty result: decay confidence
6. Learn new hints from successful outcomes for future turns.

## Guardrails

- Explicit constraints from current question always win.
- Ambiguous cross-dimension filters are pruned before SQL compilation.
- SQL remains read-only and allowlisted.
- Prompt-injection guardrails run before planning.

## Observability

- `GET /api/reporting/hints` provides visibility into learned hints and confidence.
- Query telemetry includes semantic hint metadata for debugging.

## Next steps

1. Add user feedback endpoint (`helpful` / `not helpful`) to directly adjust confidence.
2. Add promotion policy from session to global hints after evidence threshold.
3. Add offline evaluation for hint precision/recall drift over time.
