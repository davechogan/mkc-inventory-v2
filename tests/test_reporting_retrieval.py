from __future__ import annotations

import reporting.retrieval as retrieval
from reporting.retrieval import format_retrieval_context, retrieve_artifacts, retrieve_artifacts_with_meta


def test_retrieve_artifacts_prefers_family_mapping_for_family_prompt() -> None:
    arts = retrieve_artifacts("list the knives in the blackfoot family", top_k=8)
    ids = [a.artifact_id for a in arts]
    assert "mapping.family_vs_series" in ids


def test_retrieve_artifacts_prefers_exclusion_rule_for_exclude_prompt() -> None:
    arts = retrieve_artifacts(
        "how much have i spent on blackfoot knives excluding traditions and damascus",
        top_k=8,
    )
    ids = [a.artifact_id for a in arts]
    assert "constraint.exclusions" in ids


def test_format_retrieval_context_is_stable_and_nonempty() -> None:
    arts = retrieve_artifacts("total collection value by family", top_k=8)
    text = format_retrieval_context(arts)
    assert text
    assert "[intent]" in text or "[rule]" in text or "[mapping]" in text


def test_embedding_backend_falls_back_to_lexical_when_embedder_unavailable(monkeypatch) -> None:
    monkeypatch.setattr(retrieval, "resolve_retrieval_backend", lambda c: "embedding")
    monkeypatch.setattr(retrieval, "_EMBEDDER", None)
    monkeypatch.setattr(retrieval, "_EMBEDDER_INIT_ERROR", "missing sentence-transformers")
    arts = retrieval.retrieve_artifacts("list the knives in the blackfoot family", top_k=8)
    ids = [a.artifact_id for a in arts]
    assert "mapping.family_vs_series" in ids


def test_retrieve_artifacts_with_meta_reports_fallback(monkeypatch) -> None:
    monkeypatch.setattr(retrieval, "resolve_retrieval_backend", lambda c: "embedding")
    monkeypatch.setattr(retrieval, "_EMBEDDER", None)
    monkeypatch.setattr(retrieval, "_EMBEDDER_INIT_ERROR", "missing sentence-transformers")
    arts, meta = retrieve_artifacts_with_meta("list the knives in the blackfoot family", top_k=8)
    assert arts
    assert meta.get("configured_backend") == "embedding"
    assert meta.get("effective_backend") == "lexical"
    assert meta.get("fallback_used") is True
    assert "artifact_ids" in meta and isinstance(meta["artifact_ids"], list)
    assert isinstance(meta.get("artifact_source"), str)


def test_vector_backend_falls_back_with_index_or_embedder_error(monkeypatch) -> None:
    monkeypatch.setattr(retrieval, "resolve_retrieval_backend", lambda c: "vector")
    monkeypatch.setattr(retrieval, "_EMBEDDER", None)
    monkeypatch.setattr(retrieval, "_EMBEDDER_INIT_ERROR", "missing sentence-transformers")
    monkeypatch.setattr(retrieval, "_ARTIFACT_VECTORS", None)
    arts, meta = retrieve_artifacts_with_meta("list the knives in the blackfoot family", top_k=8)
    assert arts
    assert meta.get("configured_backend") == "vector"
    assert meta.get("effective_backend") == "lexical"
    assert meta.get("fallback_used") is True
    assert isinstance(meta.get("vector_index_path"), str)


def test_chroma_backend_falls_back_when_client_unavailable(monkeypatch) -> None:
    monkeypatch.setattr(retrieval, "resolve_retrieval_backend", lambda c: "chroma")
    monkeypatch.setattr(retrieval, "_CHROMA_CLIENT", None)
    monkeypatch.setattr(retrieval, "_CHROMA_INIT_ERROR", "missing chromadb")
    arts, meta = retrieve_artifacts_with_meta("list the knives in the blackfoot family", top_k=8)
    assert arts
    assert meta.get("configured_backend") == "chroma"
    assert meta.get("effective_backend") == "lexical"
    assert meta.get("fallback_used") is True
    assert isinstance(meta.get("chroma_path"), str)
    assert isinstance(meta.get("chroma_collection"), str)


def test_validate_artifact_payload_accepts_well_formed_catalog() -> None:
    artifacts, err = retrieval._validate_artifact_payload(
        [
            {
                "artifact_id": "x.test",
                "kind": "rule",
                "content": "x",
                "tags": ["A", "B"],
            }
        ]
    )
    assert err is None
    assert len(artifacts) == 1
    assert artifacts[0].tags == ("a", "b")


def test_validate_artifact_payload_rejects_malformed_catalog() -> None:
    artifacts, err = retrieval._validate_artifact_payload(
        [{"artifact_id": "x.only-id"}]
    )
    assert artifacts == ()
    assert isinstance(err, str) and err

