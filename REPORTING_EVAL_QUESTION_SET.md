# Reporting Evaluation Question Set (40 prompts)

This question set validates semantic planning, SQL compilation safety, follow-up behavior, and model-routing consistency using paired phrasing (canonical + alternate) across key reporting intents.

## Prompt pairs (20 x 2 = 40)

1. **Total value by family**
   - A: `What is my total collection value by family?`
   - B: `Show me my collection value grouped by family.`

2. **Count by steel**
   - A: `How many knives do I have by steel?`
   - B: `Give me a steel breakdown of my inventory counts.`

3. **Missing Traditions models**
   - A: `Which traditions knives am I missing?`
   - B: `Which models from the Traditions series are not in my inventory?`

4. **Missing Speedgoat models**
   - A: `Am I missing any Speedgoat knives?`
   - B: `Which Speedgoat models do I still not have in inventory?`

5. **Completion cost**
   - A: `How much will it cost me to complete my collection?`
   - B: `Estimate the MSRP cost to finish my collection.`

6. **Value by steel**
   - A: `What is my total collection value by steel?`
   - B: `Show estimated value grouped by steel.`

7. **Count by family**
   - A: `How many knives do I have by family?`
   - B: `Give me inventory counts by knife family.`

8. **Condition distribution**
   - A: `Show condition distribution across my inventory.`
   - B: `How many knives do I have by condition?`

9. **Location distribution**
   - A: `How many knives are in each location?`
   - B: `Show my inventory counts by storage location.`

10. **Top-value knives**
    - A: `Which knives have the highest estimated value?`
    - B: `Show my top valued knives.`

11. **Monthly spend (last year)**
    - A: `Show monthly spend for the last 12 months.`
    - B: `How much did I spend by month over the last year?`

12. **Purchase-source breakdown**
    - A: `How many knives did I buy from each purchase source?`
    - B: `Show purchase source breakdown for my inventory.`

13. **Value by series**
    - A: `What is my total collection value by series?`
    - B: `Show estimated value grouped by series name.`

14. **Hunting inventory list**
    - A: `List my hunting knives.`
    - B: `Show inventory items where knife type is Hunting.`

15. **Collaborator distribution**
    - A: `How many knives do I have by collaborator?`
    - B: `Show collaborator distribution in my inventory.`

16. **Missing models (overall)**
    - A: `Which knives am I missing from the full catalog?`
    - B: `What models are not yet in my inventory?`

17. **Value by form**
    - A: `What is my total collection value by form?`
    - B: `Show value grouped by form name.`

18. **Blade-finish distribution**
    - A: `How many knives do I have by blade finish?`
    - B: `Show blade finish distribution.`

19. **Handle-color distribution**
    - A: `How many knives do I have by handle color?`
    - B: `Show handle color distribution in my inventory.`

20. **Series distribution**
    - A: `How many knives do I have by series?`
    - B: `Show series breakdown for my collection.`

## Follow-up continuity checks

These are multi-turn checks run in one reporting session:

1. **Scoped carryover (goat -> list them)**
   - Turn 1: `how many "goat" knives do I have?`
   - Turn 2: `list them`
   - Expectation:
     - turn 1 uses aggregate count intent
     - turn 2 switches to list intent
     - turn 2 keeps prior scope and returns non-empty rows

## Golden checks for critical metrics

The harness enforces additional pair-consistency checks for high-value intents:

- Pair 1: `sum(total_estimated_value)` must match across phrasings.
- Pair 2: `sum(rows_count)` must match across phrasings.
- Pair 3: missing Traditions `official_name` set must match across phrasings.
- Pair 4: missing Speedgoat `official_name` set must match across phrasings.
- Pair 5: completion-cost row-0 fields (`missing_models_count`, `estimated_completion_cost_msrp`) must match.

## Usage

- Harness file: `tools/reporting_eval_harness.py`
- Default URL: `http://localhost:8008`
- Remote URL: `python3 tools/reporting_eval_harness.py http://macstudio:8008`
- Smoke gate: `python3 tools/reporting_eval_harness.py http://macstudio:8008 --suite smoke`
- Core gate: `python3 tools/reporting_eval_harness.py http://macstudio:8008 --suite core`
- Full gate: `python3 tools/reporting_eval_harness.py http://macstudio:8008 --suite full`

### Security (adversarial) gate

Dedicated prompt-injection / SQL-safety checks live in the harness (`ADVERSARIAL_CASES`): instructions that attempt destructive SQL, multi-statement SQL, or schema exfiltration (`sqlite_master`, `PRAGMA`, etc.). A case **passes** if the API responds with a **safe client error** (HTTP 4xx, typically 400 with a controlled message) **or** HTTP 200 with **no rows** (empty result set, no tabular exfil). Server errors (5xx) or HTTP 200 with one or more data rows **fail** the check.

- **Security-only run** (adversarial prompts only; does not run the 40 core prompts):

  `python3 tools/reporting_eval_harness.py http://localhost:8008 --suite security`

- **Append security checks** after smoke, core, or full:

  `python3 tools/reporting_eval_harness.py http://localhost:8008 --suite full --with-security`

### Latency gate

Successful responses may include `execution_ms` (SQL execution time). The harness always prints **p50** and **p95** over those samples when any are present. To **fail the run** when latency is too high, enable the gate and optionally override thresholds (defaults: p50 ≤ 500 ms, p95 ≤ 2000 ms):

- Full eval with latency enforcement:

  `python3 tools/reporting_eval_harness.py http://localhost:8008 --suite full --latency-gate`

- Custom thresholds (milliseconds):

  `python3 tools/reporting_eval_harness.py http://localhost:8008 --suite core --latency-gate --latency-p50-ms 750 --latency-p95-ms 3000`

- Combine with security appended after core:

  `python3 tools/reporting_eval_harness.py http://localhost:8008 --suite core --with-security --latency-gate`

If no successful responses include `execution_ms`, the latency gate is skipped (no failure from missing metadata).

### Robustness suite (multi-turn)

The harness also supports a conversation-level robustness suite for context carryover and scope control:

- Run robustness scenarios only:

  `python3 tools/reporting_eval_harness.py http://localhost:8008 --suite robustness`

Current robustness scenarios include:

- **Scope switch scenario:** inventory-scoped question, then explicit catalog question, then scope-status confirmation.
- **Ambiguity scenario:** ambiguous catalog-vs-inventory question must trigger clarification, then explicit follow-up resolves scope.
- **Negation scenario:** exclusion phrasing (`except Speedgoat`) must carry into follow-up listing and exclude matching rows.
