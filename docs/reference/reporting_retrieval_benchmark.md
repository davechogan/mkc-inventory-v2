# Reporting retrieval benchmark (offline)

## Purpose

Compare retrieval backends (lexical, in-process embedding, persisted vector index,
Chroma) on a **fixed canonical prompt set** without starting the API or loading
the full app. This supports slice **E5** (vector retrieval quality vs lexical)
and keeps grounding regressions visible as the artifact catalog grows.

## Run

From the repo root, using the project virtualenv:

```bash
.venv/bin/python tools/reporting_retrieval_benchmark.py
```

Useful flags:

- `--backends lexical,embedding` — subset of backends
- `--json` — machine-readable output
- `--enforce` — exit non-zero unless **lexical** hits every case (suitable for CI)
- `--enforce-all` — every selected backend must hit every case (needs optional deps)
- `--strict` — fail if a non-lexical backend falls back to lexical

## Editing cases

Prompts and expected artifact IDs live in `tools/reporting_retrieval_benchmark.py`
(`CANONICAL_CASES`). When changing `reporting/retrieval_artifacts.json`, update
the benchmark expectations so lexical still passes `--enforce`.

## Related

- Implementation: `reporting/retrieval.py` (`retrieve_artifacts_with_meta(..., backend=...)`)
- Tracker (canonical): `Artifacts` repo `projects/mkc-inventory-v2/plans/Reporting_AI_Slice_Tracker.md`
