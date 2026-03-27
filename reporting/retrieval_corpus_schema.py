"""Typed semantic retrieval corpus for reporting AI planner grounding.

Artifacts are data-only: they inform the LLM/heuristic layer and must not bypass
``CanonicalReportingPlan`` validation or SQL compilation.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field


class FieldArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["field"] = "field"
    artifact_id: str
    tags: list[str] = Field(default_factory=list)
    content: str
    canonical_field: str
    aliases: list[str] = Field(default_factory=list)


class MetricArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["metric"] = "metric"
    artifact_id: str
    tags: list[str] = Field(default_factory=list)
    content: str
    canonical_metric: str


class IntentArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["intent"] = "intent"
    artifact_id: str
    tags: list[str] = Field(default_factory=list)
    content: str
    canonical_intent: str


class ScopeArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["scope"] = "scope"
    artifact_id: str
    tags: list[str] = Field(default_factory=list)
    content: str
    canonical_scope: str


class MappingArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["mapping"] = "mapping"
    artifact_id: str
    tags: list[str] = Field(default_factory=list)
    content: str
    target_dimension: str
    entity_phrases: list[str] = Field(default_factory=list)
    canonical_value: str | None = None


class RuleArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["rule"] = "rule"
    artifact_id: str
    tags: list[str] = Field(default_factory=list)
    content: str


class ExampleArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["example"] = "example"
    artifact_id: str
    tags: list[str] = Field(default_factory=list)
    content: str
    natural_query: str = ""
    hint_intent: str | None = None
    hint_metric: str | None = None
    hint_scope: str | None = None
    hint_year_compare: list[str] | None = None
    hint_group_by: str | None = None


CorpusItem = Annotated[
    Union[
        FieldArtifact,
        MetricArtifact,
        IntentArtifact,
        ScopeArtifact,
        MappingArtifact,
        RuleArtifact,
        ExampleArtifact,
    ],
    Field(discriminator="kind"),
]


class CorpusFileV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    items: list[CorpusItem]


@dataclass
class RetrievalCandidate:
    """Structured semantic retrieval result (planner grounding only)."""

    artifact_id: str
    kind: str
    content: str
    tags: tuple[str, ...] = ()
    hints: dict[str, Any] = field(default_factory=dict)

    def embedding_text(self) -> str:
        """Dense text for vector embedding and lexical token overlap."""
        parts = [self.kind, *self.tags, self.content]
        if self.hints:
            # Stable subset for embedding (avoid dumping huge blobs)
            for k in sorted(self.hints.keys()):
                v = self.hints[k]
                if v is None:
                    continue
                if isinstance(v, (list, tuple)):
                    parts.append(f"{k}={' '.join(str(x) for x in v)}")
                else:
                    parts.append(f"{k}={v}")
        return " ".join(str(p) for p in parts if p).strip()


def _item_to_candidate(item: CorpusItem) -> RetrievalCandidate:
    d = item.model_dump()
    kind = d.pop("kind")
    aid = d.pop("artifact_id")
    tags_raw = d.pop("tags", []) or []
    content = d.pop("content", "") or ""
    tags = tuple(str(t).strip().lower() for t in tags_raw if str(t).strip())
    hints: dict[str, Any] = {"kind": kind}
    for key, val in d.items():
        if val is not None and val != [] and val != "":
            hints[key] = val
    return RetrievalCandidate(artifact_id=aid, kind=kind, content=content, tags=tags, hints=hints)


def parse_corpus_v1(payload: object) -> tuple[list[RetrievalCandidate], str | None]:
    try:
        root = CorpusFileV1.model_validate(payload)
    except Exception as exc:
        return [], f"corpus_v1_invalid: {exc}"
    candidates = [_item_to_candidate(it) for it in root.items]
    if not candidates:
        return [], "corpus_v1_empty"
    return candidates, None


def corpus_fingerprint(candidates: list[RetrievalCandidate], *, embed_model: str) -> str:
    """Stable hash for index invalidation (corpus + embed model identity)."""
    rows: list[dict[str, Any]] = []
    for c in sorted(candidates, key=lambda x: x.artifact_id):
        rows.append(
            {
                "artifact_id": c.artifact_id,
                "kind": c.kind,
                "content": c.content,
                "tags": list(c.tags),
                "hints": json.loads(json.dumps(c.hints, sort_keys=True)),
            }
        )
    payload = json.dumps({"embed_model": embed_model, "items": rows}, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def load_corpus_file(path: str) -> tuple[list[RetrievalCandidate], str | None]:
    try:
        raw = Path(path).read_text(encoding="utf-8")
        payload = json.loads(raw)
    except Exception as exc:
        return [], f"corpus_read_error: {exc}"
    if isinstance(payload, list):
        payload = {"schema_version": 1, "items": payload}
    return parse_corpus_v1(payload)


# Files under ``corpus_docs/`` merged after ``retrieval_corpus.json`` and ``retrieval_corpus_seed.json``.
# Later files override earlier entries with the same ``artifact_id`` (last wins).
CORPUS_DOCS_FRAGMENT_FILENAMES: tuple[str, ...] = (
    "intent_prototypes.json",
    "field_dictionary.json",
    "metric_definitions.json",
    "scope_rules.json",
    "mapping_rules.json",
    "schema_slices.json",
    "dimension_rules.json",
    "example_patterns.json",
)


def merge_reporting_corpus(
    base_corpus_path: str,
    *,
    corpus_docs_dir: str,
    seed_filename: str = "retrieval_corpus_seed.json",
) -> tuple[list[RetrievalCandidate], list[str], str | None]:
    """Load base JSON, optional seed, and fragment arrays; merge by artifact_id (last wins).

    Returns (candidates, loaded_file_paths, error_or_warning_string).
    """
    merged: dict[str, RetrievalCandidate] = {}
    errs: list[str] = []
    loaded_paths: list[str] = []

    def _add_file(p: str) -> None:
        if not os.path.isfile(p):
            return
        c, e = load_corpus_file(p)
        if e:
            errs.append(f"{Path(p).name}: {e}")
        if not c:
            return
        loaded_paths.append(p)
        for it in c:
            merged[it.artifact_id] = it

    _add_file(base_corpus_path)
    _add_file(os.path.join(corpus_docs_dir, seed_filename))
    for fn in CORPUS_DOCS_FRAGMENT_FILENAMES:
        _add_file(os.path.join(corpus_docs_dir, fn))

    if not merged:
        return [], [], ("; ".join(errs) if errs else "corpus_empty")

    out = sorted(merged.values(), key=lambda x: x.artifact_id)
    return out, loaded_paths, ("; ".join(errs) if errs else None)


def parse_legacy_artifact_list(payload: object) -> tuple[list[RetrievalCandidate], str | None]:
    """Validate pre-v1 list-of-dicts format (used by tests and migration)."""
    if not isinstance(payload, list):
        return [], "catalog payload must be a list"
    out: list[RetrievalCandidate] = []
    for idx, item in enumerate(payload):
        if not isinstance(item, dict):
            return [], f"artifact[{idx}] must be an object"
        aid = str(item.get("artifact_id") or "").strip()
        kind = str(item.get("kind") or "").strip()
        content = str(item.get("content") or "").strip()
        tags_in = item.get("tags") or []
        if not aid or not kind or not content:
            return [], f"artifact[{idx}] missing required fields"
        if not isinstance(tags_in, list):
            return [], f"artifact[{idx}].tags must be a list"
        tags = tuple(str(t).strip().lower() for t in tags_in if str(t).strip())
        out.append(RetrievalCandidate(artifact_id=aid, kind=kind, content=content, tags=tags, hints={"kind": kind}))
    if not out:
        return [], "catalog is empty"
    return out, None


def load_legacy_artifacts_json(path: str) -> tuple[list[RetrievalCandidate], str | None]:
    """Pre-v1 flat artifacts: artifact_id, kind, content, tags only."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as exc:
        return [], f"legacy_read_error: {exc}"
    return parse_legacy_artifact_list(payload)
