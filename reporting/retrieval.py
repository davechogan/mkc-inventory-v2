"""Retrieval grounding for reporting planner prompts.

This module intentionally keeps retrieval as grounding input only. It does not
execute SQL or bypass canonical planning/validation boundaries.
"""

from __future__ import annotations

import json
import math
import os
import sqlite3
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class RetrievalArtifact:
    artifact_id: str
    kind: str
    content: str
    tags: tuple[str, ...] = ()


DEFAULT_RETRIEVAL_ARTIFACTS: tuple[RetrievalArtifact, ...] = (
    RetrievalArtifact(
        artifact_id="intent.aggregate.value_by_family",
        kind="intent",
        tags=("aggregate", "value", "family"),
        content=(
            "When users ask for total collection value by family, use aggregate intent, "
            "group_by family_name, metric total_estimated_value, scope inventory unless catalog is explicit."
        ),
    ),
    RetrievalArtifact(
        artifact_id="intent.aggregate.spend",
        kind="intent",
        tags=("aggregate", "spend", "purchase_price"),
        content=(
            "Spend questions map to aggregate intent with metric total_spend "
            "computed from purchase_price * quantity."
        ),
    ),
    RetrievalArtifact(
        artifact_id="mapping.series_aliases",
        kind="mapping",
        tags=("series", "traditions", "vip", "ultra", "blood brothers"),
        content=(
            "Traditions, VIP, Ultra, and Blood Brothers are series_name values unless "
            "the user explicitly asks for family/type."
        ),
    ),
    RetrievalArtifact(
        artifact_id="mapping.family_vs_series",
        kind="mapping",
        tags=("family", "series", "disambiguation"),
        content=(
            "Phrase 'in the <X> family' maps to family_name filters. "
            "Phrase 'in the <X> series/line' maps to series_name filters."
        ),
    ),
    RetrievalArtifact(
        artifact_id="constraint.exclusions",
        kind="rule",
        tags=("exclude", "excluding", "without", "not"),
        content=(
            "User exclusions (exclude/without/not including) must preserve negation in canonical plan "
            "using exclusions list. Never invert exclusions into positive filters."
        ),
    ),
    RetrievalArtifact(
        artifact_id="scope.default_inventory",
        kind="rule",
        tags=("scope", "inventory", "catalog"),
        content=(
            "Default scope is inventory unless user explicitly asks for full catalog."
        ),
    ),
    RetrievalArtifact(
        artifact_id="followup.list_underlying_items",
        kind="rule",
        tags=("followup", "list", "those", "that number"),
        content=(
            "Follow-ups asking to list items that made up an aggregate should use list intent with "
            "the same explicit filters/exclusions, not another scalar aggregate."
        ),
    ),
)

# Stored in app_meta under this key when the UI saves a preference (see ``resolve_retrieval_backend``).
RETRIEVAL_BACKEND_META_KEY = "reporting_retrieval_backend"

VALID_RETRIEVAL_BACKENDS: tuple[str, ...] = ("lexical", "embedding", "vector", "chroma")
# Default when no env var and no app_meta row (embedding retrieval for planner grounding).
DEFAULT_RETRIEVAL_BACKEND = "embedding"


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


# Back-compat for tests that monkeypatch this name; not used for resolution when unset.
RETRIEVAL_EMBED_MODEL = (os.environ.get("REPORTING_RETRIEVAL_EMBED_MODEL") or "all-MiniLM-L6-v2").strip()
RETRIEVAL_ARTIFACTS_PATH = (
    os.environ.get("REPORTING_RETRIEVAL_ARTIFACTS_PATH")
    or os.path.join(os.path.dirname(__file__), "retrieval_artifacts.json")
).strip()
RETRIEVAL_VECTOR_INDEX_PATH = (
    os.environ.get("REPORTING_RETRIEVAL_VECTOR_INDEX_PATH")
    or os.path.join(os.path.dirname(__file__), "retrieval_vector_index.json")
).strip()
RETRIEVAL_CHROMA_PATH = (
    os.environ.get("REPORTING_RETRIEVAL_CHROMA_PATH")
    or os.path.join(os.path.dirname(__file__), ".chroma")
).strip()
RETRIEVAL_CHROMA_COLLECTION = (os.environ.get("REPORTING_RETRIEVAL_CHROMA_COLLECTION") or "reporting_retrieval_artifacts").strip()

_EMBEDDER = None
_EMBEDDER_INIT_ERROR: str | None = None
_ARTIFACT_VECTORS: list[tuple[RetrievalArtifact, list[float]]] | None = None
_ARTIFACTS_LOAD_ERROR: str | None = None
_ARTIFACTS_SOURCE: str = "builtin_default"
_VECTOR_INDEX_LOAD_ERROR: str | None = None
_CHROMA_CLIENT = None
_CHROMA_INIT_ERROR: str | None = None
_CHROMA_LAST_ERROR: str | None = None


def _validate_artifact_payload(payload: object) -> tuple[tuple[RetrievalArtifact, ...], str | None]:
    if not isinstance(payload, list):
        return (), "catalog payload must be a list"
    out: list[RetrievalArtifact] = []
    for idx, item in enumerate(payload):
        if not isinstance(item, dict):
            return (), f"artifact[{idx}] must be an object"
        artifact_id = str(item.get("artifact_id") or "").strip()
        kind = str(item.get("kind") or "").strip()
        content = str(item.get("content") or "").strip()
        tags_in = item.get("tags") or []
        if not artifact_id or not kind or not content:
            return (), f"artifact[{idx}] missing required fields"
        if not isinstance(tags_in, list):
            return (), f"artifact[{idx}].tags must be a list"
        tags = tuple(str(t).strip().lower() for t in tags_in if str(t).strip())
        out.append(
            RetrievalArtifact(
                artifact_id=artifact_id,
                kind=kind,
                content=content,
                tags=tags,
            )
        )
    if not out:
        return (), "catalog is empty"
    return tuple(out), None


def _load_artifacts_from_file(path: str) -> tuple[tuple[RetrievalArtifact, ...], str | None]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as exc:
        return (), f"unable to read artifact catalog: {exc}"
    return _validate_artifact_payload(payload)


def _resolve_artifacts() -> tuple[tuple[RetrievalArtifact, ...], str, str | None]:
    loaded, err = _load_artifacts_from_file(RETRIEVAL_ARTIFACTS_PATH)
    if loaded:
        return loaded, f"file:{RETRIEVAL_ARTIFACTS_PATH}", None
    return DEFAULT_RETRIEVAL_ARTIFACTS, "builtin_default", err


RETRIEVAL_ARTIFACTS, _ARTIFACTS_SOURCE, _ARTIFACTS_LOAD_ERROR = _resolve_artifacts()


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
    return {
        "configured_backend": resolved,
        "default_backend": DEFAULT_RETRIEVAL_BACKEND,
        "stored_backend": stored,
        "env_override_active": env_override,
        "embed_model": RETRIEVAL_EMBED_MODEL,
        "artifacts_path": RETRIEVAL_ARTIFACTS_PATH,
        "artifact_source": _ARTIFACTS_SOURCE,
        "artifact_load_error": _ARTIFACTS_LOAD_ERROR,
        "artifact_count": len(RETRIEVAL_ARTIFACTS),
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
    """Reload retrieval artifacts from disk and clear derived caches."""
    global RETRIEVAL_ARTIFACTS, _ARTIFACTS_SOURCE, _ARTIFACTS_LOAD_ERROR, _ARTIFACT_VECTORS, _VECTOR_INDEX_LOAD_ERROR, _CHROMA_LAST_ERROR
    RETRIEVAL_ARTIFACTS, _ARTIFACTS_SOURCE, _ARTIFACTS_LOAD_ERROR = _resolve_artifacts()
    # Force re-embedding with current catalog on next embedding retrieval call.
    _ARTIFACT_VECTORS = None
    _VECTOR_INDEX_LOAD_ERROR = None
    _CHROMA_LAST_ERROR = None
    status = get_retrieval_status(conn)
    status["reloaded"] = True
    return status


def _tokens(text: str) -> set[str]:
    out: list[str] = []
    cur = []
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


def _lexical_retrieve(question: str, top_k: int) -> list[RetrievalArtifact]:
    q_tokens = _tokens(question)
    if not q_tokens:
        return list(RETRIEVAL_ARTIFACTS[: min(top_k, len(RETRIEVAL_ARTIFACTS))])
    scored: list[tuple[int, int, RetrievalArtifact]] = []
    for art in RETRIEVAL_ARTIFACTS:
        corpus_tokens = set(art.tags) | _tokens(art.content)
        overlap = len(q_tokens & corpus_tokens)
        if overlap <= 0:
            continue
        scored.append((overlap, -len(art.content), art))
    scored.sort(reverse=True, key=lambda x: (x[0], x[1], x[2].artifact_id))
    selected = [a for _, _, a in scored[:top_k]]
    if not selected:
        selected = list(RETRIEVAL_ARTIFACTS[: min(top_k, len(RETRIEVAL_ARTIFACTS))])
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


def _artifact_text(art: RetrievalArtifact) -> str:
    return f"{art.kind} {' '.join(art.tags)} {art.content}".strip()


def _embedding_retrieve(question: str, top_k: int) -> list[RetrievalArtifact]:
    global _ARTIFACT_VECTORS
    embedder = _get_embedder()
    if embedder is None:
        return _lexical_retrieve(question, top_k)
    try:
        q_vec_raw = embedder.encode([question], normalize_embeddings=True)[0]
        q_vec = [float(x) for x in list(q_vec_raw)]
        if _ARTIFACT_VECTORS is None:
            texts = [_artifact_text(a) for a in RETRIEVAL_ARTIFACTS]
            vecs_raw = embedder.encode(texts, normalize_embeddings=True)
            _ARTIFACT_VECTORS = []
            for art, v in zip(RETRIEVAL_ARTIFACTS, vecs_raw):
                _ARTIFACT_VECTORS.append((art, [float(x) for x in list(v)]))
        scored: list[tuple[float, RetrievalArtifact]] = []
        for art, vec in (_ARTIFACT_VECTORS or []):
            scored.append((_cosine(q_vec, vec), art))
        scored.sort(key=lambda x: x[0], reverse=True)
        selected = [a for _, a in scored[:top_k] if _ > -0.5]
        return selected or _lexical_retrieve(question, top_k)
    except Exception:  # pragma: no cover - backend may fail at runtime
        return _lexical_retrieve(question, top_k)


def _save_vector_index(path: str, *, vectors: list[tuple[RetrievalArtifact, list[float]]], model_name: str) -> str | None:
    payload = {
        "model": model_name,
        "artifacts_path": RETRIEVAL_ARTIFACTS_PATH,
        "artifact_source": _ARTIFACTS_SOURCE,
        "entries": [{"artifact_id": art.artifact_id, "vector": vec} for art, vec in vectors],
    }
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        return None
    except Exception as exc:
        return str(exc)


def _load_vector_index(path: str) -> tuple[list[tuple[RetrievalArtifact, list[float]]] | None, str | None]:
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
    entries = payload.get("entries")
    if not isinstance(entries, list):
        return None, "index_entries_invalid"
    by_id = {a.artifact_id: a for a in RETRIEVAL_ARTIFACTS}
    loaded: list[tuple[RetrievalArtifact, list[float]]] = []
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


def _ensure_vector_index() -> tuple[list[tuple[RetrievalArtifact, list[float]]] | None, str | None]:
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
        texts = [_artifact_text(a) for a in RETRIEVAL_ARTIFACTS]
        vecs_raw = embedder.encode(texts, normalize_embeddings=True)
        built: list[tuple[RetrievalArtifact, list[float]]] = []
        for art, v in zip(RETRIEVAL_ARTIFACTS, vecs_raw):
            built.append((art, [float(x) for x in list(v)]))
        _ARTIFACT_VECTORS = built
        write_err = _save_vector_index(RETRIEVAL_VECTOR_INDEX_PATH, vectors=built, model_name=RETRIEVAL_EMBED_MODEL)
        return built, write_err
    except Exception as exc:
        return None, str(exc)


def _vector_retrieve(question: str, top_k: int) -> tuple[list[RetrievalArtifact], str | None]:
    vectors, err = _ensure_vector_index()
    if not vectors:
        return _lexical_retrieve(question, top_k), err
    embedder = _get_embedder()
    if embedder is None:
        return _lexical_retrieve(question, top_k), (_EMBEDDER_INIT_ERROR or "embedder_unavailable")
    try:
        q_vec_raw = embedder.encode([question], normalize_embeddings=True)[0]
        q_vec = [float(x) for x in list(q_vec_raw)]
        scored: list[tuple[float, RetrievalArtifact]] = []
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


def _chroma_retrieve(question: str, top_k: int) -> tuple[list[RetrievalArtifact], str | None]:
    """Target architecture path: Chroma collection with sentence-transformer embeddings when available."""
    client, err = _get_chroma_client()
    if client is None:
        return _lexical_retrieve(question, top_k), err
    by_id = {a.artifact_id: a for a in RETRIEVAL_ARTIFACTS}
    try:
        collection = client.get_or_create_collection(name=RETRIEVAL_CHROMA_COLLECTION)
        ids = [a.artifact_id for a in RETRIEVAL_ARTIFACTS]
        docs = [_artifact_text(a) for a in RETRIEVAL_ARTIFACTS]
        metas = [{"kind": a.kind} for a in RETRIEVAL_ARTIFACTS]
        embedder = _get_embedder()
        if embedder is not None:
            embeds_raw = embedder.encode(docs, normalize_embeddings=True)
            embeds = [[float(x) for x in list(v)] for v in embeds_raw]
            collection.upsert(ids=ids, documents=docs, embeddings=embeds, metadatas=metas)
            q_vec_raw = embedder.encode([question], normalize_embeddings=True)[0]
            q_vec = [float(x) for x in list(q_vec_raw)]
            res = collection.query(query_embeddings=[q_vec], n_results=max(1, int(top_k)))
        else:
            # Chroma can run with its own embedding function; this keeps sentence-transformers optional.
            collection.upsert(ids=ids, documents=docs, metadatas=metas)
            res = collection.query(query_texts=[question], n_results=max(1, int(top_k)))
        out_ids = (((res or {}).get("ids") or [[]])[0]) if isinstance(res, dict) else []
        selected: list[RetrievalArtifact] = []
        for aid in out_ids:
            art = by_id.get(str(aid))
            if art and art not in selected:
                selected.append(art)
        return selected or _lexical_retrieve(question, top_k), None
    except Exception as exc:
        return _lexical_retrieve(question, top_k), str(exc)


def retrieve_artifacts_with_meta(
    question: str,
    top_k: int = 5,
    *,
    backend: str | None = None,
    conn: sqlite3.Connection | None = None,
) -> tuple[list[RetrievalArtifact], dict[str, object]]:
    """Retrieve artifacts plus backend metadata for telemetry/debugging.

    When ``backend`` is provided, it overrides env/app_meta for this call only
    (benchmarks/tests). Production callers pass ``conn`` so the UI-stored backend applies.
    """
    global _VECTOR_INDEX_LOAD_ERROR, _CHROMA_LAST_ERROR
    k = max(1, int(top_k))
    configured_backend = (
        _normalize_backend(backend) if backend is not None and str(backend).strip() != "" else resolve_retrieval_backend(conn)
    )
    if configured_backend not in {"embedding", "vector", "chroma"}:
        artifacts = _lexical_retrieve(question, k)
        return artifacts, {
            "artifact_source": _ARTIFACTS_SOURCE,
            "configured_backend": configured_backend,
            "effective_backend": "lexical",
            "embedder_ready": False,
            "fallback_used": False,
            "artifact_ids": [a.artifact_id for a in artifacts],
            "artifact_load_error": _ARTIFACTS_LOAD_ERROR,
            "vector_index_path": RETRIEVAL_VECTOR_INDEX_PATH,
            "vector_index_error": _VECTOR_INDEX_LOAD_ERROR,
            "chroma_path": RETRIEVAL_CHROMA_PATH,
            "chroma_collection": RETRIEVAL_CHROMA_COLLECTION,
            "chroma_error": (_CHROMA_LAST_ERROR or _CHROMA_INIT_ERROR),
        }
    if configured_backend == "chroma":
        artifacts, chroma_err = _chroma_retrieve(question, k)
        _CHROMA_LAST_ERROR = chroma_err
        return artifacts, {
            "artifact_source": _ARTIFACTS_SOURCE,
            "configured_backend": "chroma",
            "effective_backend": ("chroma" if not chroma_err else "lexical"),
            "embedder_ready": _EMBEDDER is not None and _EMBEDDER_INIT_ERROR is None,
            "fallback_used": bool(chroma_err),
            "fallback_reason": (chroma_err or "")[:240] if chroma_err else None,
            "chroma_path": RETRIEVAL_CHROMA_PATH,
            "chroma_collection": RETRIEVAL_CHROMA_COLLECTION,
            "chroma_error": (_CHROMA_LAST_ERROR or _CHROMA_INIT_ERROR),
            "embed_model": RETRIEVAL_EMBED_MODEL,
            "artifact_ids": [a.artifact_id for a in artifacts],
            "artifact_load_error": _ARTIFACTS_LOAD_ERROR,
            "vector_index_path": RETRIEVAL_VECTOR_INDEX_PATH,
            "vector_index_error": _VECTOR_INDEX_LOAD_ERROR,
        }
    if configured_backend == "vector":
        artifacts, vector_err = _vector_retrieve(question, k)
        _VECTOR_INDEX_LOAD_ERROR = vector_err
        return artifacts, {
            "artifact_source": _ARTIFACTS_SOURCE,
            "configured_backend": "vector",
            "effective_backend": ("vector" if not vector_err else "lexical"),
            "embedder_ready": _EMBEDDER is not None and _EMBEDDER_INIT_ERROR is None,
            "fallback_used": bool(vector_err),
            "fallback_reason": (vector_err or "")[:240] if vector_err else None,
            "vector_index_path": RETRIEVAL_VECTOR_INDEX_PATH,
            "vector_index_error": _VECTOR_INDEX_LOAD_ERROR,
            "embed_model": RETRIEVAL_EMBED_MODEL,
            "artifact_ids": [a.artifact_id for a in artifacts],
            "artifact_load_error": _ARTIFACTS_LOAD_ERROR,
        }
    embedder = _get_embedder()
    if embedder is None:
        artifacts = _lexical_retrieve(question, k)
        return artifacts, {
            "artifact_source": _ARTIFACTS_SOURCE,
            "configured_backend": "embedding",
            "effective_backend": "lexical",
            "embedder_ready": False,
            "fallback_used": True,
            "fallback_reason": (_EMBEDDER_INIT_ERROR or "embedder_unavailable")[:240],
            "artifact_ids": [a.artifact_id for a in artifacts],
            "artifact_load_error": _ARTIFACTS_LOAD_ERROR,
            "vector_index_path": RETRIEVAL_VECTOR_INDEX_PATH,
            "vector_index_error": _VECTOR_INDEX_LOAD_ERROR,
        }
    artifacts = _embedding_retrieve(question, k)
    return artifacts, {
        "artifact_source": _ARTIFACTS_SOURCE,
        "configured_backend": "embedding",
        "effective_backend": "embedding",
        "embedder_ready": True,
        "fallback_used": False,
        "embed_model": RETRIEVAL_EMBED_MODEL,
        "artifact_ids": [a.artifact_id for a in artifacts],
        "artifact_load_error": _ARTIFACTS_LOAD_ERROR,
        "vector_index_path": RETRIEVAL_VECTOR_INDEX_PATH,
        "vector_index_error": _VECTOR_INDEX_LOAD_ERROR,
    }


def retrieve_artifacts(
    question: str,
    top_k: int = 5,
    *,
    backend: str | None = None,
    conn: sqlite3.Connection | None = None,
) -> list[RetrievalArtifact]:
    """Retrieve best-fit semantic artifacts for a question.

    Retrieval is lexical for now and deterministic. It is intentionally simple
    but establishes the canonical retrieval boundary and artifact contract.
    """
    artifacts, _meta = retrieve_artifacts_with_meta(question, top_k=top_k, backend=backend, conn=conn)
    return artifacts


def format_retrieval_context(artifacts: Iterable[RetrievalArtifact]) -> str:
    lines: list[str] = []
    for art in artifacts:
        lines.append(f"[{art.kind}] {art.artifact_id}: {art.content}")
    return "\n".join(lines).strip()

