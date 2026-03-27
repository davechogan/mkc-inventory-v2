"""Verify merged ``corpus_docs/`` artifacts are persisted in Chroma (vectordb)."""

from __future__ import annotations

import os

import pytest

import reporting.retrieval as retrieval

# Present under ``reporting/corpus_docs/*.json`` but not in ``retrieval_corpus.json`` (grep-checked).
_CORPUS_DOCS_ONLY_IDS: tuple[str, ...] = (
    "mapping.family_examples.core",
    "schema.reporting_inventory",
)


def test_merged_corpus_includes_corpus_docs_artifacts() -> None:
    """Precondition: merge layer actually loaded these artifact_ids into RETRIEVAL_CANDIDATES."""
    by_id = {a.artifact_id: a for a in retrieval.RETRIEVAL_CANDIDATES}
    missing = [i for i in _CORPUS_DOCS_ONLY_IDS if i not in by_id]
    assert not missing, f"corpus_docs-only ids missing from merged corpus: {missing}"
    assert retrieval._ARTIFACTS_SOURCE.startswith("merged:"), (
        f"expected merged corpus source, got {retrieval._ARTIFACTS_SOURCE!r}"
    )


def test_chroma_persists_corpus_docs_artifacts_without_query(tmp_path, monkeypatch) -> None:
    """Sync corpus to disk via upsert only; reopen store and read back with ``get`` (no ``query``)."""
    pytest.importorskip("chromadb")

    chroma_dir = tmp_path / "chroma_vectordb_test"
    chroma_path = str(chroma_dir)
    monkeypatch.setattr(retrieval, "RETRIEVAL_CHROMA_PATH", chroma_path)
    monkeypatch.setattr(retrieval, "_get_embedder", lambda: None)
    retrieval._CHROMA_CLIENT = None
    retrieval._CHROMA_INIT_ERROR = None
    retrieval._CHROMA_LAST_ERROR = None

    test_merged_corpus_includes_corpus_docs_artifacts()

    client, err = retrieval._get_chroma_client()
    assert err is None and client is not None
    collection = client.get_or_create_collection(name=retrieval.RETRIEVAL_CHROMA_COLLECTION)
    sync_diag = retrieval._chroma_sync_indexed_collection(collection)
    assert sync_diag.get("chroma_upsert_skipped") is False
    assert sync_diag.get("chroma_embed_mode") == "chroma_builtin"
    n_expected = len(retrieval.RETRIEVAL_CANDIDATES)
    assert sync_diag.get("chroma_collection_count") == n_expected

    manifest_path = retrieval._chroma_manifest_path()
    assert os.path.isfile(manifest_path)

    # Drop in-process client so the next open must read persisted state from disk.
    retrieval._CHROMA_CLIENT = None

    import chromadb

    client2 = chromadb.PersistentClient(path=chroma_path)
    col2 = client2.get_collection(name=retrieval.RETRIEVAL_CHROMA_COLLECTION)
    assert int(col2.count()) == n_expected

    got = col2.get(ids=list(_CORPUS_DOCS_ONLY_IDS), include=["documents", "metadatas"])

    assert set(got["ids"]) == set(_CORPUS_DOCS_ONLY_IDS)
    assert got["documents"] is not None and len(got["documents"]) == len(_CORPUS_DOCS_ONLY_IDS)
    for doc in got["documents"]:
        assert doc and len(doc) > 10

    metas = got.get("metadatas") or []
    kinds = {m.get("artifact_id"): m.get("kind") for m in metas if isinstance(m, dict)}
    assert kinds.get("mapping.family_examples.core") == "mapping"
    assert kinds.get("schema.reporting_inventory") == "rule"

    # Human-readable summary for ``pytest -s``
    print(
        "\n--- chroma corpus_docs persistence (no query) ---\n"
        f"  artifact_source: {retrieval._ARTIFACTS_SOURCE}\n"
        f"  corpus fingerprint: {sync_diag.get('corpus_fingerprint')}\n"
        f"  manifest: {manifest_path}\n"
        f"  collection_count (after reopen): {col2.count()}\n"
        f"  corpus_docs probe ids loaded via get(): {sorted(_CORPUS_DOCS_ONLY_IDS)}\n"
        f"  doc preview (family_examples): {(got['documents'] or [''])[0][:120]!r}...\n"
    )
