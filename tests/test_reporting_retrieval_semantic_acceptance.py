"""Acceptance: retrieval corpus grounds planner-relevant semantics (lexical baseline)."""

from __future__ import annotations

import json

import reporting.retrieval as retrieval
from reporting.retrieval import retrieve_artifacts, retrieve_artifacts_with_meta


def test_blood_brothers_group_centers_on_series_name() -> None:
    arts = retrieve_artifacts("knives in the Blood Brothers group for my collection", top_k=12)
    ids = [a.artifact_id for a in arts]
    assert "mapping.series_blood_brothers" in ids
    bb = next(a for a in arts if a.artifact_id == "mapping.series_blood_brothers")
    assert bb.hints.get("target_dimension") == "series_name"


def test_year_compare_spend_retrieves_metric_and_example() -> None:
    arts = retrieve_artifacts("how much did I spend in 2024 vs 2025", top_k=8)
    ids = [a.artifact_id for a in arts]
    assert "metric.total_spend" in ids
    assert "example.year_compare_spend" in ids


def test_what_do_i_own_retrieves_inventory_scope() -> None:
    arts = retrieve_artifacts("what do I own", top_k=8)
    ids = [a.artifact_id for a in arts]
    assert "scope.default_inventory" in ids
    sc = next(a for a in arts if a.artifact_id == "scope.default_inventory")
    assert sc.hints.get("canonical_scope") == "inventory"


def test_retrieve_meta_includes_semantic_candidates() -> None:
    _, meta = retrieve_artifacts_with_meta("total collection value", top_k=3, backend="lexical", conn=None)
    assert "semantic_candidates" in meta
    sc = meta["semantic_candidates"]
    assert isinstance(sc, list) and len(sc) <= 3
    assert all("hints" in x and "artifact_id" in x for x in sc)


def test_chroma_skip_manifest_matches_fingerprint(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(retrieval, "RETRIEVAL_CHROMA_PATH", str(tmp_path))
    fp = retrieval.current_corpus_fingerprint()
    n = len(retrieval.RETRIEVAL_CANDIDATES)
    manifest = {
        "fingerprint": fp,
        "embed_model": retrieval.RETRIEVAL_EMBED_MODEL,
        "artifact_count": n,
    }
    tmp_path.mkdir(parents=True, exist_ok=True)
    with open(tmp_path / "retrieval_chroma_manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f)

    class _Col:
        def count(self) -> int:
            return n

    assert retrieval._chroma_should_skip_upsert(_Col(), fp, n) is True


def test_vector_index_payload_includes_fingerprint_roundtrip(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(retrieval, "RETRIEVAL_VECTOR_INDEX_PATH", str(tmp_path / "idx.json"))
    monkeypatch.setattr(retrieval, "_ARTIFACT_VECTORS", None)
    fp = retrieval.current_corpus_fingerprint()
    vecs: list[tuple] = []
    for c in retrieval.RETRIEVAL_CANDIDATES[:2]:
        vecs.append((c, [0.1, 0.2, 0.3]))
    err = retrieval._save_vector_index(str(tmp_path / "idx.json"), vectors=vecs, model_name=retrieval.RETRIEVAL_EMBED_MODEL)
    assert err is None
    raw = json.loads((tmp_path / "idx.json").read_text())
    assert raw.get("fingerprint") == fp
    loaded, lerr = retrieval._load_vector_index(str(tmp_path / "idx.json"))
    assert lerr is None
    assert loaded is not None and len(loaded) == 2
