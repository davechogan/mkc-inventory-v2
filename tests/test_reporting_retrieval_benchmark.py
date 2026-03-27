from __future__ import annotations

import reporting.retrieval as retrieval
from reporting.retrieval import retrieve_artifacts_with_meta
from tools.reporting_retrieval_benchmark import CANONICAL_CASES, run_benchmark


def test_retrieve_artifacts_with_meta_backend_override(monkeypatch) -> None:
    """Explicit ``backend=`` must win over ``resolve_retrieval_backend``."""
    monkeypatch.setattr(retrieval, "resolve_retrieval_backend", lambda c: "chroma")
    arts, meta = retrieve_artifacts_with_meta(
        "list the knives in the blackfoot family",
        top_k=8,
        backend="lexical",
    )
    assert meta.get("configured_backend") == "lexical"
    assert meta.get("effective_backend") == "lexical"
    ids = [a.artifact_id for a in arts]
    assert "mapping.family_vs_series" in ids


def test_benchmark_lexical_hits_all_canonical_cases() -> None:
    """Lexical baseline must satisfy every ``must_include`` set (CI gate)."""
    result = run_benchmark(backends=("lexical",), cases=CANONICAL_CASES, strict=False)
    lex = result["per_backend"]["lexical"]
    assert lex["hits"] == lex["total"] == len(CANONICAL_CASES)
