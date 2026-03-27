"""Retrieval grounding for reporting planner prompts.

Semantic candidates are **grounding only**: they inform the planner/LLM and must not
bypass ``CanonicalReportingPlan`` validation or SQL compilation.

Chroma indexes the corpus once per corpus fingerprint (see ``retrieval_chroma_manifest.json``).
"""

from __future__ import annotations

import json
import math
import os
import sqlite3
from typing import Any, Iterable

from reporting.retrieval_corpus_schema import (
    RetrievalCandidate,
    corpus_fingerprint,
    load_corpus_file,
    load_legacy_artifacts_json,
    merge_reporting_corpus,
    parse_legacy_artifact_list,
)

# Backward-compatible alias (same object as live corpus list).
RetrievalArtifact = RetrievalCandidate

# Stored in app_meta under this key when the UI saves a preference (see ``resolve_retrieval_backend``).
RETRIEVAL_BACKEND_META_KEY = "reporting_retrieval_backend"

VALID_RETRIEVAL_BACKENDS: tuple[str, ...] = ("lexical", "embedding", "vector", "chroma")
DEFAULT_RETRIEVAL_BACKEND = "embedding"

_CHROMA_MANIFEST_NAME = "retrieval_chroma_manifest.json"


def _normalize_backend(name: str) -> str:
    n = (name or "").strip().lower()
    return n if n in VALID_RETRIEVAL_BACKENDS else DEFAULT_RETRIEVAL_BACKEND


def resolve_retrieval_backend(conn: sqlite3.Connection | None) -> str:
    """Effective backend for retrieval: env ``REPORTING_RETRIEVAL_BACKEND`` wins, then app_meta, then default."""
    env_raw = os.environ.get("REPORTING_RETRIEVAL_BACKEND")
    if env_raw is not None and str(env_raw).strip() != "":
        return _normalize_backend(str(env_raw).strip().lower())
    if conn is not None:
        try:
            row = conn.execute(
                "SELECT value FROM app_meta WHERE key = ?",
                (RETRIEVAL_BACKEND_META_KEY,),
            ).fetchone()
            val = row["value"] if isinstance(row, dict) else (row[0] if row else None)
            if val is not None and str(val).strip():
                return _normalize_backend(str(val).strip().lower())
        except Exception:
            pass
    return DEFAULT_RETRIEVAL_BACKEND


RETRIEVAL_EMBED_MODEL = (os.environ.get("REPORTING_RETRIEVAL_EMBED_MODEL") or "all-MiniLM-L6-v2").strip()
_REPORTING_DIR = os.path.dirname(__file__)
RETRIEVAL_CORPUS_PATH = (
    os.environ.get("REPORTING_RETRIEVAL_CORPUS_PATH") or os.path.join(_REPORTING_DIR, "retrieval_corpus.json")
).strip()
RETRIEVAL_CORPUS_DOCS_DIR = (
    os.environ.get("REPORTING_RETRIEVAL_CORPUS_DOCS_DIR") or os.path.join(_REPORTING_DIR, "corpus_docs")
).strip()
RETRIEVAL_ARTIFACTS_PATH = (
    os.environ.get("REPORTING_RETRIEVAL_ARTIFACTS_PATH") or os.path.join(_REPORTING_DIR, "retrieval_artifacts.json")
).strip()
RETRIEVAL_VECTOR_INDEX_PATH = (
    os.environ.get("REPORTING_RETRIEVAL_VECTOR_INDEX_PATH") or os.path.join(_REPORTING_DIR, "retrieval_vector_index.json")
).strip()
RETRIEVAL_CHROMA_PATH = (
    os.environ.get("REPORTING_RETRIEVAL_CHROMA_PATH") or os.path.join(_REPORTING_DIR, ".chroma")
).strip()
RETRIEVAL_CHROMA_COLLECTION = (os.environ.get("REPORTING_RETRIEVAL_CHROMA_COLLECTION") or "reporting_retrieval_artifacts").strip()

_EMBEDDER = None
_EMBEDDER_INIT_ERROR: str | None = None
_ARTIFACT_VECTORS: list[tuple[RetrievalCandidate, list[float]]] | None = None
_ARTIFACTS_LOAD_ERROR: str | None = None
_ARTIFACTS_SOURCE: str = "builtin_default"
_VECTOR_INDEX_LOAD_ERROR: str | None = None
_CHROMA_CLIENT = None
_CHROMA_INIT_ERROR: str | None = None
_CHROMA_LAST_ERROR: str | None = None


def _resolve_candidates() -> tuple[list[RetrievalCandidate], str, str | None]:
    """Load typed corpus (v1 JSON) merged with ``corpus_docs/``, then legacy flat JSON, else minimal builtin."""
    skip_merge = (os.environ.get("REPORTING_RETRIEVAL_CORPUS_SKIP_MERGE") or "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    merge_err: str | None = None
    if not skip_merge:
        merged, loaded_paths, merge_err = merge_reporting_corpus(
            RETRIEVAL_CORPUS_PATH,
            corpus_docs_dir=RETRIEVAL_CORPUS_DOCS_DIR,
        )
        if merged:
            names = "+".join(os.path.basename(p) for p in loaded_paths)
            return merged, f"merged:{names}", merge_err
    candidates, err = load_corpus_file(RETRIEVAL_CORPUS_PATH)
    if candidates:
        return candidates, f"corpus:{RETRIEVAL_CORPUS_PATH}", err or merge_err
    legacy, lerr = load_legacy_artifacts_json(RETRIEVAL_ARTIFACTS_PATH)
    if legacy:
        return legacy, f"legacy:{RETRIEVAL_ARTIFACTS_PATH}", lerr
    # Emergency fallback: tiny rule so lexical never empties
    fb = [
        RetrievalCandidate(
            artifact_id="fallback.scope.inventory",
            kind="scope",
            content="Default scope is inventory unless user asks for full catalog.",
            tags=("inventory", "catalog", "scope"),
            hints={"kind": "scope", "canonical_scope": "inventory"},
        )
    ]
    return fb, "builtin_emergency", (err or lerr or merge_err or "no_corpus_file")


RETRIEVAL_CANDIDATES, _ARTIFACTS_SOURCE, _ARTIFACTS_LOAD_ERROR = _resolve_candidates()
RETRIEVAL_ARTIFACTS = RETRIEVAL_CANDIDATES


def _validate_artifact_payload(payload: object) -> tuple[tuple[RetrievalCandidate, ...], str | None]:
    """Legacy flat catalog validation (tests); prefer ``retrieval_corpus.json`` schema v1."""
    cands, err = parse_legacy_artifact_list(payload)
    if err:
        return (), err
    return tuple(cands), None


def current_corpus_fingerprint() -> str:
    return corpus_fingerprint(RETRIEVAL_CANDIDATES, embed_model=RETRIEVAL_EMBED_MODEL)


def get_retrieval_status(conn: sqlite3.Connection | None = None) -> dict[str, object]:
    """Return retrieval runtime status for diagnostics."""
    resolved = resolve_retrieval_backend(conn)
    env_raw = os.environ.get("REPORTING_RETRIEVAL_BACKEND")
    env_override = env_raw is not None and str(env_raw).strip() != ""
    stored: str | None = None
    if conn is not None:
        try:
            row = conn.execute(
                "SELECT value FROM app_meta WHERE key = ?",
                (RETRIEVAL_BACKEND_META_KEY,),
            ).fetchone()
            v = row["value"] if isinstance(row, dict) else (row[0] if row else None)
            if v is not None and str(v).strip():
                stored = str(v).strip().lower()
        except Exception:
            stored = None
    fp = current_corpus_fingerprint()
    return {
        "configured_backend": resolved,
        "default_backend": DEFAULT_RETRIEVAL_BACKEND,
        "stored_backend": stored,
        "env_override_active": env_override,
        "embed_model": RETRIEVAL_EMBED_MODEL,
        "corpus_path": RETRIEVAL_CORPUS_PATH,
        "corpus_docs_dir": RETRIEVAL_CORPUS_DOCS_DIR,
        "legacy_artifacts_path": RETRIEVAL_ARTIFACTS_PATH,
        "artifact_source": _ARTIFACTS_SOURCE,
        "artifact_load_error": _ARTIFACTS_LOAD_ERROR,
        "artifact_count": len(RETRIEVAL_CANDIDATES),
        "corpus_fingerprint": fp,
        "vector_index_path": RETRIEVAL_VECTOR_INDEX_PATH,
        "vector_index_error": _VECTOR_INDEX_LOAD_ERROR,
        "chroma_path": RETRIEVAL_CHROMA_PATH,
        "chroma_collection": RETRIEVAL_CHROMA_COLLECTION,
        "chroma_ready": _CHROMA_CLIENT is not None and _CHROMA_INIT_ERROR is None,
        "chroma_error": (_CHROMA_LAST_ERROR or _CHROMA_INIT_ERROR),
        "embedder_ready": _EMBEDDER is not None and _EMBEDDER_INIT_ERROR is None,
        "embedder_error": _EMBEDDER_INIT_ERROR,
    }


def reload_retrieval_artifacts(conn: sqlite3.Connection | None = None) -> dict[str, object]:
    """Reload retrieval corpus from disk and clear derived caches."""
    global RETRIEVAL_CANDIDATES, RETRIEVAL_ARTIFACTS, _ARTIFACTS_SOURCE, _ARTIFACTS_LOAD_ERROR
    global _ARTIFACT_VECTORS, _VECTOR_INDEX_LOAD_ERROR, _CHROMA_LAST_ERROR
    RETRIEVAL_CANDIDATES, _ARTIFACTS_SOURCE, _ARTIFACTS_LOAD_ERROR = _resolve_candidates()
    RETRIEVAL_ARTIFACTS = RETRIEVAL_CANDIDATES
    _ARTIFACT_VECTORS = None
    _VECTOR_INDEX_LOAD_ERROR = None
    _CHROMA_LAST_ERROR = None
    status = get_retrieval_status(conn)
    status["reloaded"] = True
    return status


def _tokens(text: str) -> set[str]:
    out: list[str] = []
    cur: list[str] = []
    for ch in (text or "").lower():
        if ch.isalnum():
            cur.append(ch)
        else:
            if cur:
                out.append("".join(cur))
                cur = []
    if cur:
        out.append("".join(cur))
    return {t for t in out if len(t) > 1}


def _corpus_match_tokens(c: RetrievalCandidate) -> set[str]:
    base = set(c.tags) | _tokens(c.content) | _tokens(c.embedding_text())
    ep = c.hints.get("entity_phrases")
    if isinstance(ep, list):
        for p in ep:
            base |= _tokens(str(p))
    al = c.hints.get("aliases")
    if isinstance(al, list):
        for p in al:
            base |= _tokens(str(p))
    return base


def _lexical_retrieve(question: str, top_k: int) -> list[RetrievalCandidate]:
    q_tokens = _tokens(question)
    if not q_tokens:
        return list(RETRIEVAL_CANDIDATES[: min(top_k, len(RETRIEVAL_CANDIDATES))])
    scored: list[tuple[int, int, RetrievalCandidate]] = []
    for art in RETRIEVAL_CANDIDATES:
        corpus_tokens = _corpus_match_tokens(art)
        overlap = len(q_tokens & corpus_tokens)
        if overlap <= 0:
            continue
        scored.append((overlap, -len(art.content), art))
    scored.sort(reverse=True, key=lambda x: (x[0], x[1], x[2].artifact_id))
    selected = [a for _, _, a in scored[:top_k]]
    if not selected:
        selected = list(RETRIEVAL_CANDIDATES[: min(top_k, len(RETRIEVAL_CANDIDATES))])
    return selected


def _get_embedder():
    global _EMBEDDER, _EMBEDDER_INIT_ERROR
    if _EMBEDDER is not None:
        return _EMBEDDER
    if _EMBEDDER_INIT_ERROR is not None:
        return None
    try:
        from sentence_transformers import SentenceTransformer  # type: ignore

        _EMBEDDER = SentenceTransformer(RETRIEVAL_EMBED_MODEL)
        return _EMBEDDER
    except Exception as exc:  # pragma: no cover - environment-dependent
        _EMBEDDER_INIT_ERROR = str(exc)
        return None


def _cosine(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return -1.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0.0 or nb == 0.0:
        return -1.0
    return dot / (na * nb)


def _embedding_text(c: RetrievalCandidate) -> str:
    return c.embedding_text()


def _embedding_retrieve(question: str, top_k: int) -> list[RetrievalCandidate]:
    global _ARTIFACT_VECTORS
    embedder = _get_embedder()
    if embedder is None:
        return _lexical_retrieve(question, top_k)
    try:
        q_vec_raw = embedder.encode([question], normalize_embeddings=True)[0]
        q_vec = [float(x) for x in list(q_vec_raw)]
        if _ARTIFACT_VECTORS is None:
            texts = [_embedding_text(a) for a in RETRIEVAL_CANDIDATES]
            vecs_raw = embedder.encode(texts, normalize_embeddings=True)
            _ARTIFACT_VECTORS = []
            for art, v in zip(RETRIEVAL_CANDIDATES, vecs_raw):
                _ARTIFACT_VECTORS.append((art, [float(x) for x in list(v)]))
        scored: list[tuple[float, RetrievalCandidate]] = []
        for art, vec in (_ARTIFACT_VECTORS or []):
            scored.append((_cosine(q_vec, vec), art))
        scored.sort(key=lambda x: x[0], reverse=True)
        selected = [a for _, a in scored[:top_k] if _ > -0.5]
        return selected or _lexical_retrieve(question, top_k)
    except Exception:  # pragma: no cover - backend may fail at runtime
        return _lexical_retrieve(question, top_k)


def _save_vector_index(path: str, *, vectors: list[tuple[RetrievalCandidate, list[float]]], model_name: str) -> str | None:
    payload = {
        "model": model_name,
        "fingerprint": current_corpus_fingerprint(),
        "corpus_path": RETRIEVAL_CORPUS_PATH,
        "artifact_source": _ARTIFACTS_SOURCE,
        "entries": [{"artifact_id": art.artifact_id, "vector": vec} for art, vec in vectors],
    }
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        return None
    except Exception as exc:
        return str(exc)


def _load_vector_index(path: str) -> tuple[list[tuple[RetrievalCandidate, list[float]]] | None, str | None]:
    if not os.path.exists(path):
        return None, "index_missing"
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as exc:
        return None, f"index_read_error: {exc}"
    if not isinstance(payload, dict):
        return None, "index_payload_invalid"
    if str(payload.get("model") or "") != RETRIEVAL_EMBED_MODEL:
        return None, "index_model_mismatch"
    if str(payload.get("fingerprint") or "") != current_corpus_fingerprint():
        return None, "index_fingerprint_mismatch"
    entries = payload.get("entries")
    if not isinstance(entries, list):
        return None, "index_entries_invalid"
    by_id = {a.artifact_id: a for a in RETRIEVAL_CANDIDATES}
    loaded: list[tuple[RetrievalCandidate, list[float]]] = []
    for item in entries:
        if not isinstance(item, dict):
            continue
        aid = str(item.get("artifact_id") or "")
        vec = item.get("vector")
        art = by_id.get(aid)
        if art is None or not isinstance(vec, list):
            continue
        try:
            loaded.append((art, [float(x) for x in vec]))
        except Exception:
            continue
    if not loaded:
        return None, "index_empty_or_incompatible"
    return loaded, None


def _ensure_vector_index() -> tuple[list[tuple[RetrievalCandidate, list[float]]] | None, str | None]:
    global _ARTIFACT_VECTORS
    if _ARTIFACT_VECTORS is not None:
        return _ARTIFACT_VECTORS, None
    loaded, err = _load_vector_index(RETRIEVAL_VECTOR_INDEX_PATH)
    if loaded is not None:
        _ARTIFACT_VECTORS = loaded
        return loaded, None
    embedder = _get_embedder()
    if embedder is None:
        return None, (_EMBEDDER_INIT_ERROR or err or "embedder_unavailable")
    try:
        texts = [_embedding_text(a) for a in RETRIEVAL_CANDIDATES]
        vecs_raw = embedder.encode(texts, normalize_embeddings=True)
        built: list[tuple[RetrievalCandidate, list[float]]] = []
        for art, v in zip(RETRIEVAL_CANDIDATES, vecs_raw):
            built.append((art, [float(x) for x in list(v)]))
        _ARTIFACT_VECTORS = built
        write_err = _save_vector_index(RETRIEVAL_VECTOR_INDEX_PATH, vectors=built, model_name=RETRIEVAL_EMBED_MODEL)
        return built, write_err
    except Exception as exc:
        return None, str(exc)


def _vector_retrieve(question: str, top_k: int) -> tuple[list[RetrievalCandidate], str | None]:
    vectors, err = _ensure_vector_index()
    if not vectors:
        return _lexical_retrieve(question, top_k), err
    embedder = _get_embedder()
    if embedder is None:
        return _lexical_retrieve(question, top_k), (_EMBEDDER_INIT_ERROR or "embedder_unavailable")
    try:
        q_vec_raw = embedder.encode([question], normalize_embeddings=True)[0]
        q_vec = [float(x) for x in list(q_vec_raw)]
        scored: list[tuple[float, RetrievalCandidate]] = []
        for art, vec in vectors:
            scored.append((_cosine(q_vec, vec), art))
        scored.sort(key=lambda x: x[0], reverse=True)
        selected = [a for score, a in scored[:top_k] if score > -0.5]
        return selected or _lexical_retrieve(question, top_k), err
    except Exception as exc:
        return _lexical_retrieve(question, top_k), str(exc)


def _get_chroma_client():
    global _CHROMA_CLIENT, _CHROMA_INIT_ERROR
    if _CHROMA_CLIENT is not None:
        return _CHROMA_CLIENT, None
    if _CHROMA_INIT_ERROR is not None:
        return None, _CHROMA_INIT_ERROR
    try:
        import chromadb  # type: ignore

        _CHROMA_CLIENT = chromadb.PersistentClient(path=RETRIEVAL_CHROMA_PATH)
        return _CHROMA_CLIENT, None
    except Exception as exc:
        _CHROMA_INIT_ERROR = str(exc)
        return None, _CHROMA_INIT_ERROR


def _chroma_manifest_path() -> str:
    return os.path.join(RETRIEVAL_CHROMA_PATH, _CHROMA_MANIFEST_NAME)


def _chroma_should_skip_upsert(collection: Any, fingerprint: str, n_docs: int) -> bool:
    mp = _chroma_manifest_path()
    if not os.path.isfile(mp):
        return False
    try:
        with open(mp, "r", encoding="utf-8") as f:
            m = json.load(f)
    except Exception:
        return False
    if m.get("fingerprint") != fingerprint:
        return False
    if str(m.get("embed_model") or "") != RETRIEVAL_EMBED_MODEL:
        return False
    if int(m.get("artifact_count") or 0) != n_docs:
        return False
    try:
        if int(collection.count()) != n_docs:
            return False
    except Exception:
        return False
    return True


def _chroma_write_manifest(fingerprint: str, n_docs: int) -> None:
    os.makedirs(RETRIEVAL_CHROMA_PATH, exist_ok=True)
    payload = {
        "fingerprint": fingerprint,
        "embed_model": RETRIEVAL_EMBED_MODEL,
        "collection": RETRIEVAL_CHROMA_COLLECTION,
        "artifact_count": n_docs,
        "corpus_fingerprint_algo": "sha256_v1",
    }
    with open(_chroma_manifest_path(), "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def _chroma_sync_indexed_collection(collection: Any) -> dict[str, object]:
    """Upsert the live corpus into Chroma only when the on-disk manifest is stale.

    When ``retrieval_chroma_manifest.json`` matches the current corpus fingerprint,
    embed model, and collection row count, **no upsert** runs (persistence is reused).
    Per-query work is then only the user question embedding / Chroma query — not
    re-indexing the full corpus. Callers still run ``collection.query`` afterward.
    """
    fp = current_corpus_fingerprint()
    n_docs = len(RETRIEVAL_CANDIDATES)
    skip = _chroma_should_skip_upsert(collection, fp, n_docs)
    diag: dict[str, object] = {
        "chroma_upsert_skipped": skip,
        "corpus_fingerprint": fp,
    }
    if not skip:
        ids = [a.artifact_id for a in RETRIEVAL_CANDIDATES]
        docs = [_embedding_text(a) for a in RETRIEVAL_CANDIDATES]
        metas = [{"kind": a.kind, "artifact_id": a.artifact_id} for a in RETRIEVAL_CANDIDATES]
        embedder = _get_embedder()
        if embedder is not None:
            embeds_raw = embedder.encode(docs, normalize_embeddings=True)
            embeds = [[float(x) for x in list(v)] for v in embeds_raw]
            collection.upsert(ids=ids, documents=docs, embeddings=embeds, metadatas=metas)
            diag["chroma_embed_mode"] = "sentence_transformers"
            diag["embedding_dim"] = len(embeds[0]) if embeds else None
        else:
            collection.upsert(ids=ids, documents=docs, metadatas=metas)
            diag["chroma_embed_mode"] = "chroma_builtin"
            diag["embedding_dim"] = None
        _chroma_write_manifest(fp, len(ids))
    else:
        diag["chroma_embed_mode"] = "unchanged"
    try:
        diag["chroma_collection_count"] = int(collection.count())
    except Exception as count_exc:
        diag["chroma_collection_count"] = None
        diag["chroma_collection_count_error"] = str(count_exc)[:120]
    return diag


def _chroma_retrieve(question: str, top_k: int) -> tuple[list[RetrievalCandidate], str | None, dict[str, object]]:
    empty_diag: dict[str, object] = {"chroma_client_ready": False}
    client, err = _get_chroma_client()
    if client is None:
        return _lexical_retrieve(question, top_k), err, {**empty_diag, "chroma_client_error": err}
    by_id = {a.artifact_id: a for a in RETRIEVAL_CANDIDATES}
    diag: dict[str, object] = {"chroma_client_ready": True, "chroma_path": RETRIEVAL_CHROMA_PATH}
    try:
        collection = client.get_or_create_collection(name=RETRIEVAL_CHROMA_COLLECTION)
        sync_diag = _chroma_sync_indexed_collection(collection)
        diag.update(sync_diag)
        embedder = _get_embedder()
        if embedder is not None:
            q_vec_raw = embedder.encode([question], normalize_embeddings=True)[0]
            q_vec = [float(x) for x in list(q_vec_raw)]
            res = collection.query(query_embeddings=[q_vec], n_results=max(1, int(top_k)))
        else:
            res = collection.query(query_texts=[question], n_results=max(1, int(top_k)))
        out_ids = (((res or {}).get("ids") or [[]])[0]) if isinstance(res, dict) else []
        diag["chroma_query_top_ids"] = [str(x) for x in (out_ids or [])][: max(1, int(top_k))]
        selected: list[RetrievalCandidate] = []
        for aid in out_ids:
            art = by_id.get(str(aid))
            if art and art not in selected:
                selected.append(art)
        return selected or _lexical_retrieve(question, top_k), None, diag
    except Exception as exc:
        diag["chroma_exception"] = str(exc)[:240]
        return _lexical_retrieve(question, top_k), str(exc), diag


def retrieve_artifacts_with_meta(
    question: str,
    top_k: int = 5,
    *,
    backend: str | None = None,
    conn: sqlite3.Connection | None = None,
    debug: bool = False,
) -> tuple[list[RetrievalCandidate], dict[str, object]]:
    """Retrieve semantic candidates plus backend metadata for telemetry/debugging.

    When ``debug`` is true, metadata includes ``semantic_query_text`` (the string
    passed to lexical / embedding / Chroma query) and per-candidate ``embedding_text``
    (the string used when indexing corpus rows with sentence-transformers).
    """
    global _VECTOR_INDEX_LOAD_ERROR, _CHROMA_LAST_ERROR
    k = max(1, int(top_k))
    configured_backend = (
        _normalize_backend(backend) if backend is not None and str(backend).strip() != "" else resolve_retrieval_backend(conn)
    )
    def _finalize_meta(artifacts: list[RetrievalCandidate], extra: dict[str, object]) -> dict[str, object]:
        meta: dict[str, object] = {
            "artifact_source": _ARTIFACTS_SOURCE,
            "configured_backend": configured_backend,
            **extra,
            "artifact_ids": [a.artifact_id for a in artifacts],
            "semantic_candidates": [_candidate_payload(a, debug=debug) for a in artifacts],
            "artifact_load_error": _ARTIFACTS_LOAD_ERROR,
            "corpus_fingerprint": current_corpus_fingerprint(),
            "vector_index_path": RETRIEVAL_VECTOR_INDEX_PATH,
            "vector_index_error": _VECTOR_INDEX_LOAD_ERROR,
            "chroma_path": RETRIEVAL_CHROMA_PATH,
            "chroma_collection": RETRIEVAL_CHROMA_COLLECTION,
            "chroma_error": (_CHROMA_LAST_ERROR or _CHROMA_INIT_ERROR),
        }
        if debug:
            meta["semantic_query_text"] = question
            meta["retrieval_note"] = (
                "Lexical: token overlap on tags/content/embedding_text. "
                "Embedding/Chroma: this same question string is encoded for the query vector "
                "(see chroma_diag / embed_model when applicable)."
            )
        return meta

    if configured_backend not in {"embedding", "vector", "chroma"}:
        artifacts = _lexical_retrieve(question, k)
        return artifacts, _finalize_meta(
            artifacts,
            {
                "effective_backend": "lexical",
                "embedder_ready": False,
                "fallback_used": False,
            },
        )
    if configured_backend == "chroma":
        artifacts, chroma_err, chroma_diag = _chroma_retrieve(question, k)
        _CHROMA_LAST_ERROR = chroma_err
        base_chroma = _finalize_meta(
            artifacts,
            {
                "effective_backend": ("chroma" if not chroma_err else "lexical"),
                "embedder_ready": _EMBEDDER is not None and _EMBEDDER_INIT_ERROR is None,
                "fallback_used": bool(chroma_err),
                "fallback_reason": (chroma_err or "")[:240] if chroma_err else None,
                "embed_model": RETRIEVAL_EMBED_MODEL,
            },
        )
        base_chroma.update(chroma_diag)
        return artifacts, base_chroma
    if configured_backend == "vector":
        artifacts, vector_err = _vector_retrieve(question, k)
        _VECTOR_INDEX_LOAD_ERROR = vector_err
        return artifacts, _finalize_meta(
            artifacts,
            {
                "effective_backend": ("vector" if not vector_err else "lexical"),
                "embedder_ready": _EMBEDDER is not None and _EMBEDDER_INIT_ERROR is None,
                "fallback_used": bool(vector_err),
                "fallback_reason": (vector_err or "")[:240] if vector_err else None,
                "embed_model": RETRIEVAL_EMBED_MODEL,
            },
        )
    embedder = _get_embedder()
    if embedder is None:
        artifacts = _lexical_retrieve(question, k)
        return artifacts, _finalize_meta(
            artifacts,
            {
                "effective_backend": "lexical",
                "embedder_ready": False,
                "fallback_used": True,
                "fallback_reason": (_EMBEDDER_INIT_ERROR or "embedder_unavailable")[:240],
            },
        )
    artifacts = _embedding_retrieve(question, k)
    return artifacts, _finalize_meta(
        artifacts,
        {
            "effective_backend": "embedding",
            "embedder_ready": True,
            "fallback_used": False,
            "embed_model": RETRIEVAL_EMBED_MODEL,
        },
    )


def _candidate_payload(c: RetrievalCandidate, *, debug: bool = False) -> dict[str, Any]:
    out: dict[str, Any] = {
        "artifact_id": c.artifact_id,
        "kind": c.kind,
        "tags": list(c.tags),
        "content": c.content,
        "hints": dict(c.hints),
    }
    if debug:
        et = c.embedding_text()
        out["embedding_text"] = et if len(et) <= 8000 else f"{et[:8000]}…[truncated]"
    return out


def retrieve_artifacts(
    question: str,
    top_k: int = 5,
    *,
    backend: str | None = None,
    conn: sqlite3.Connection | None = None,
) -> list[RetrievalCandidate]:
    """Return best-fit semantic candidates for a question (grounding only)."""
    artifacts, _meta = retrieve_artifacts_with_meta(question, top_k=top_k, backend=backend, conn=conn)
    return artifacts


def format_retrieval_context(artifacts: Iterable[RetrievalCandidate]) -> str:
    """Human- and model-readable grounding block with structured hints (JSON per candidate)."""
    blocks: list[str] = []
    for c in artifacts:
        hint = {k: v for k, v in c.hints.items() if k != "kind"}
        if hint:
            blocks.append(
                f"[{c.kind}] {c.artifact_id}\nhints: {json.dumps(hint, ensure_ascii=False)}\n{c.content}"
            )
        else:
            blocks.append(f"[{c.kind}] {c.artifact_id}: {c.content}")
    return "\n\n".join(blocks).strip()


retrieve_semantic_candidates = retrieve_artifacts_with_meta
