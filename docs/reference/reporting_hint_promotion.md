# Reporting Semantic Hint Promotion

This document describes the guarded promotion flow from session-scoped semantic hints to global soft priors.

## Why this exists

Session hint learning improves within-thread interpretation. Promotion lets repeated, high-confidence patterns become reusable global priors without making them hard rules.

## Guardrails

- Promotion is opt-in by policy (`REPORTING_HINT_PROMOTION_ENABLED`, default enabled).
- Only `scope_type='session'` hints are candidates.
- Candidates must pass thresholds:
  - confidence >= `REPORTING_HINT_PROMOTION_MIN_CONFIDENCE` (default `0.80`)
  - evidence_count >= `REPORTING_HINT_PROMOTION_MIN_EVIDENCE` (default `3`)
  - `success_count > failure_count`
- Conflicting global value for same `(entity_norm, cue_word, target_dimension)` is skipped.
- Promoted hints remain **soft priors**, not deterministic constraints.

## Endpoint

- `POST /api/reporting/hints/promote`
- Body:
  - `session_id` (optional): limit promotion scan to one session
  - `dry_run` (default `true`): evaluate only, no writes
  - `min_confidence`, `min_evidence`, `max_promotions` (optional overrides)

## Response fields

- `enabled`: promotion policy state
- `dry_run`: whether writes were performed
- `considered`: candidate rows scanned
- `promoted`: candidate rows accepted by policy
- `skipped`: candidate rows rejected
- `reasons`: aggregated skip reasons
- `candidates`: promoted candidate descriptors (source IDs and target mappings)

