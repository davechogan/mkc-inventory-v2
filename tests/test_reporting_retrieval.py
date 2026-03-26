from __future__ import annotations

import reporting.retrieval as retrieval
from reporting.retrieval import format_retrieval_context, retrieve_artifacts


def test_retrieve_artifacts_prefers_family_mapping_for_family_prompt() -> None:
    arts = retrieve_artifacts("list the knives in the blackfoot family", top_k=4)
    ids = [a.artifact_id for a in arts]
    assert "mapping.family_vs_series" in ids


def test_retrieve_artifacts_prefers_exclusion_rule_for_exclude_prompt() -> None:
    arts = retrieve_artifacts(
        "how much have i spent on blackfoot knives excluding traditions and damascus",
        top_k=4,
    )
    ids = [a.artifact_id for a in arts]
    assert "constraint.exclusions" in ids


def test_format_retrieval_context_is_stable_and_nonempty() -> None:
    arts = retrieve_artifacts("total collection value by family", top_k=3)
    text = format_retrieval_context(arts)
    assert text
    assert "[intent]" in text or "[rule]" in text or "[mapping]" in text


def test_embedding_backend_falls_back_to_lexical_when_embedder_unavailable(monkeypatch) -> None:
    monkeypatch.setattr(retrieval, "RETRIEVAL_BACKEND", "embedding")
    monkeypatch.setattr(retrieval, "_EMBEDDER", None)
    monkeypatch.setattr(retrieval, "_EMBEDDER_INIT_ERROR", "missing sentence-transformers")
    arts = retrieval.retrieve_artifacts("list the knives in the blackfoot family", top_k=4)
    ids = [a.artifact_id for a in arts]
    assert "mapping.family_vs_series" in ids

