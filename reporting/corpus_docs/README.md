# Reporting Semantic Retrieval Seed

This folder contains a richer starter corpus for the reporting vector store.

Files:
- `retrieval_corpus_seed.json`: merged corpus compatible with the current v1 schema
- `intent_prototypes.json`
- `field_dictionary.json`
- `metric_definitions.json`
- `scope_rules.json`
- `mapping_rules.json`
- `schema_slices.json`
- `dimension_rules.json`
- `example_patterns.json`

**Runtime loading:** The app merges these automatically into the retrieval index (Chroma / embeddings / lexical), in this order: `reporting/retrieval_corpus.json` → `retrieval_corpus_seed.json` → the fragment files listed above. Later entries **override** earlier ones with the same `artifact_id`. You do **not** need to copy the seed into `retrieval_corpus.json` for indexing unless you want a single canonical file under version control.

Environment:
- `REPORTING_RETRIEVAL_CORPUS_DOCS_DIR` — override the directory (default: `reporting/corpus_docs` next to `retrieval.py`).
- `REPORTING_RETRIEVAL_CORPUS_SKIP_MERGE=1` — load only `REPORTING_RETRIEVAL_CORPUS_PATH` with no `corpus_docs` merge (debugging).

Recommended use:
1. Review and trim any items that don't match your actual business model.
2. Rebuild the Chroma index once after corpus changes (fingerprint changes), not per query.
3. Add real examples from your own usage over time.
4. Keep retrieval as grounding for canonical planning, not as direct execution logic.

This seed is intentionally not exhaustive. It is designed to:
- materially improve retrieval quality now
- provide enough structure and examples for an AI agent to continue expanding it safely
- push more semantic meaning into the corpus so less interpretation stays buried in Python heuristics
