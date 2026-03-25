# Reporting Multi-Model Architecture Plan

## Goal

Stabilize Reporting Q&A accuracy and follow-up behavior by moving from free-form SQL generation to a semantic-query architecture with model routing.

## Constraints

- Ollama host has NVIDIA RTX Pro 6000 with 96GB VRAM.
- We can run multiple models concurrently.
- Existing UI contract should remain stable:
  - `answer_text`, `rows`, `columns`, `chart_spec`, `follow_ups`, `confidence`, `limitations`.

## Target Architecture

1. User question -> semantic planner (LLM + heuristics) -> validated semantic plan JSON
2. Semantic compiler -> parameterized safe SQL over curated reporting views
3. SQL validator/executor (allowlist + timeout + limits)
4. Narrative responder model summarizes rows
5. Persist semantic state for follow-ups

## Model Routing Strategy

- Planner model (higher reasoning): `qwen2.5:32b-instruct`
- Responder model (fast summary): `qwen2.5:7b-instruct`
- Optional planner retry model: `qwen2.5:72b-instruct` or fallback to deterministic template path

### Runtime config

- `REPORTING_PLANNER_MODEL` (default `qwen2.5:32b-instruct`)
- `REPORTING_RESPONDER_MODEL` (default `qwen2.5:7b-instruct`)
- `REPORTING_PLANNER_RETRY_MODEL` (optional)
- `REPORTING_PLANNER_TIMEOUT_S` (default 60)
- `REPORTING_RESPONDER_TIMEOUT_S` (default 45)

## Phase Plan

### Phase A - Semantic correctness hardening

- [ ] Keep semantic planner authoritative; only accept LLM filters when explicit in user prompt or true short follow-up.
- [ ] Add synonym map for key business dimensions (series/family/type/collaborator).
- [ ] Track unresolved constraints and return user-visible clarification hints.

### Phase B - SQL safety and resilience

- [ ] Add query timeout enforcement (worker timeout).
- [ ] Add stricter source parsing for CTE/aliases while preserving allowlist guarantees.
- [ ] Add structured execution diagnostics in `meta` (`validator_stage`, `retry_stage`).

### Phase C - Multi-model routing

- [ ] Route semantic planning to planner model.
- [ ] Route answer synthesis to responder model.
- [ ] Add confidence-based planner retry policy before fallback templates.

### Phase D - Follow-up memory reliability

- [ ] Persist last semantic plan + resolved filters + date window in session.
- [ ] Implement plan-diff follow-up updates (e.g., “look at series”, “only traditions”).
- [ ] Add stale-context guardrails when user starts a new topic.

### Phase E - Validation + observability

- [ ] Build canonical prompt test suite (>=40 prompts).
- [ ] Add golden expected checks for key metrics (value, spend, counts, missing models).
- [ ] Add telemetry for planner failures, SQL validation failures, empty-result frequency.

## Immediate acceptance criteria

- “What is my total collection value by family?” returns non-empty grouped rows when inventory exists.
- “Which traditions knives am I missing?” returns only series=Traditions missing models.
- Short follow-up “look at series” updates grouping without dropping prior scope.
- No ASGI crashes on malformed LLM output.

## Notes

- Deterministic templates remain as constrained fallback, not primary planner.
- Keep frontend API unchanged to avoid churn.
