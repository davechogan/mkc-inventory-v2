# Reporting retrieval layer

## Role

Semantic retrieval **grounds** the reporting planner (LLM + heuristics). It does **not**:

- emit SQL,
- produce the final canonical plan by itself,
- bypass `CanonicalReportingPlan` structural/semantic validation or the SQL compiler.

## Corpus

- **Primary:** `reporting/retrieval_corpus.json` — schema version 1, typed items (`field`, `metric`, `intent`, `scope`, `mapping`, `rule`, `example`).
- **Merged layers (default):** `reporting/corpus_docs/retrieval_corpus_seed.json` and the fragment JSON files (`intent_prototypes.json`, `field_dictionary.json`, …) are loaded after the base file and merged by `artifact_id` (later layers override). Fragment files may be a **top-level JSON array** of items.
- **Override paths:** `REPORTING_RETRIEVAL_CORPUS_PATH`, `REPORTING_RETRIEVAL_CORPUS_DOCS_DIR`. Set `REPORTING_RETRIEVAL_CORPUS_SKIP_MERGE=1` to use only the base corpus path.
- **Legacy fallback:** flat `reporting/retrieval_artifacts.json` (pre-v1) if the corpus file is missing.
- **Emergency builtin:** tiny scope rule if neither file loads.

Each item maps to a `RetrievalCandidate` with `artifact_id`, `kind`, `content`, `tags`, and a `hints` dict (canonical field names, metrics, scope, mapping targets, example hints).

## Fingerprints and indexes

- **Corpus fingerprint:** SHA-256 over sorted artifact payloads + embed model name (`corpus_fingerprint()` in `retrieval_corpus_schema.py`).
- **Vector index file** (`retrieval_vector_index.json`): stores vectors + `fingerprint`; reload skips when fingerprint matches.
- **Chroma:** `reporting/.chroma/` (or `REPORTING_RETRIEVAL_CHROMA_PATH`). A manifest `retrieval_chroma_manifest.json` stores the last indexed fingerprint, embed model, and artifact count. **Full-corpus upsert runs only when that manifest is stale** (corpus fingerprint changed, embed model changed, row count mismatch, or first run). A normal user question only does a cheap manifest/collection check plus **query** (embed the question, search) — it does **not** re-embed or re-upsert every artifact each time.

## Planner contract

- `retrieve_artifacts_with_meta` returns `(candidates, meta)`.
- `meta["semantic_candidates"]` lists structured dicts for telemetry/UI.
- `format_retrieval_context(candidates)` builds the planner prompt block (per-candidate `hints` as JSON + prose).

## Diagnostics

- `tools/reporting_retrieval_diagnose.py` — prints status, sample retrieval, and Chroma disk info.
- API: `GET /api/reporting/retrieval/status` includes `corpus_fingerprint`, `corpus_path`, `corpus_docs_dir`, and artifact counts.
- **Full pipeline trace:** `REPORTING_DEBUG_PIPELINE=1` or `POST /api/reporting/query` with `"debug": true` adds `pipeline_debug` to the response: retrieval (including `semantic_query_text` and per-candidate `embedding_text` in debug mode), `planner_llm` (system/user prompts + raw JSON), and `responder_llm` (prompts or deterministic path). The Reporting page exposes a **Pipeline debug** checkbox for the same flag.

## UI (Reporting page)

- After each answer, the **Text** tab shows a collapsible **“Semantic retrieval (planner grounding)”** section when retrieval metadata exists.
- It lists a short summary (corpus fingerprint prefix, backend, Chroma upsert skip when applicable, counts) and pretty-printed JSON for `artifact_ids` and **`semantic_candidates`** (structured hints per artifact).
- The sidebar **Retrieval runtime (admin)** line includes `corpus_fp` and the corpus filename when status is loaded or refreshed.
- Stored assistant `meta_json` and `result_json` both carry **`retrieval`** (including `semantic_candidates`) for session reload; **`pipeline_debug`** is stored when debug was enabled for that turn.

## Editing the dictionary

- Prefer adding or adjusting **corpus JSON** entries rather than growing Python heuristics for user phrasing that is stable and reusable.
- After edits, call `POST /api/reporting/retrieval/reload` or restart the process; Chroma will re-index on the next query when the fingerprint changes.
