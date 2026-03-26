# Reporting Defect Backlog

Track defects discovered during implementation/testing that are intentionally deferred until the planned stabilization pass.

## Open defects

### RPT-001: Contradictory positive+negative series filter in exclusion aggregate

- **Found during:** local runtime sequence test (`Blackfoot` follow-up chain)
- **Symptom:** generated SQL can contain both `series_name = 'Traditions'` and `NOT(series_name = 'Traditions')`, forcing zero rows and propagating bad state into follow-ups.
- **Impact:** high (incorrect totals, follow-up list returns no rows)
- **Status:** open / deferred
- **Planned fix phase:** post-implementation stabilization (after full architecture rollout)
- **Notes:** regression signal captured from local session run; sequence should be re-tested after retrieval + follow-up normalization completion.

### RPT-002: Exclusion follow-up context propagates empty-state lock

- **Found during:** local runtime sequence test (`Blackfoot` follow-up chain)
- **Symptom:** after contradictory exclusion turn returns `$0.00`, follow-up prompts (`list the knives that made up that number`, `double check the cost`) keep reusing the broken narrowed filter and return no rows.
- **Impact:** high (multi-turn thread becomes unrecoverable without user restart/rephrase)
- **Status:** open / deferred
- **Planned fix phase:** post-implementation stabilization (follow-up normalization and context repair)
- **Notes:** indicates conversation-state carryover is not guarding against impossible prior constraints.

### RPT-003: Cost-field meta question routes to data listing intent

- **Found during:** local runtime sequence test (`Blackfoot` follow-up chain)
- **Symptom:** `what field are you using for the cost of the knives?` is routed to `semantic_compiled_list_inventory` and executes a wide listing query instead of metadata/schema explanation path.
- **Impact:** medium (incorrect route, unnecessary DB query, noisy answer quality)
- **Status:** open / deferred
- **Planned fix phase:** post-implementation stabilization (intent/route contract tightening)
- **Notes:** response says no cost field selected because SQL omitted cost columns; route should inspect prior SQL contract or schema map.

### RPT-004: Cost-correction follow-up cannot switch to cost-bearing projection

- **Found during:** local runtime sequence test (`Blackfoot` follow-up chain)
- **Symptom:** prompt requesting actual database costs still executes a list query without any cost projection (`purchase_price`, extended cost, or total), producing non-actionable answer.
- **Impact:** high (user asks for numeric verification; system cannot provide numbers)
- **Status:** open / deferred
- **Planned fix phase:** post-implementation stabilization (projection selection and follow-up intent reclassification)
- **Notes:** indicates missing transition from "list items" to "list with spend fields" in follow-up planner behavior.

