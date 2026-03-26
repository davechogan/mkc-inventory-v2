"""Retrieval grounding for reporting planner prompts.

This module intentionally keeps retrieval as grounding input only. It does not
execute SQL or bypass canonical planning/validation boundaries.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class RetrievalArtifact:
    artifact_id: str
    kind: str
    content: str
    tags: tuple[str, ...] = ()


RETRIEVAL_ARTIFACTS: tuple[RetrievalArtifact, ...] = (
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

RETRIEVAL_BACKEND = (os.environ.get("REPORTING_RETRIEVAL_BACKEND") or "lexical").strip().lower()
RETRIEVAL_EMBED_MODEL = (os.environ.get("REPORTING_RETRIEVAL_EMBED_MODEL") or "all-MiniLM-L6-v2").strip()

_EMBEDDER = None
_EMBEDDER_INIT_ERROR: str | None = None
_ARTIFACT_VECTORS: list[tuple[RetrievalArtifact, list[float]]] | None = None


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


def retrieve_artifacts(question: str, top_k: int = 5) -> list[RetrievalArtifact]:
    """Retrieve best-fit semantic artifacts for a question.

    Retrieval is lexical for now and deterministic. It is intentionally simple
    but establishes the canonical retrieval boundary and artifact contract.
    """
    k = max(1, int(top_k))
    if RETRIEVAL_BACKEND == "embedding":
        return _embedding_retrieve(question, k)
    return _lexical_retrieve(question, k)


def format_retrieval_context(artifacts: Iterable[RetrievalArtifact]) -> str:
    lines: list[str] = []
    for art in artifacts:
        lines.append(f"[{art.kind}] {art.artifact_id}: {art.content}")
    return "\n".join(lines).strip()

