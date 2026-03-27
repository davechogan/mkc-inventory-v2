"""Tests for merged reporting corpus (base + corpus_docs fragments)."""

from __future__ import annotations

import json

from reporting.retrieval_corpus_schema import load_corpus_file, merge_reporting_corpus


def test_load_corpus_file_accepts_top_level_json_array(tmp_path) -> None:
    path = tmp_path / "frag.json"
    path.write_text(
        json.dumps(
            [
                {
                    "kind": "rule",
                    "artifact_id": "test.array.only",
                    "tags": ["a"],
                    "content": "hello",
                }
            ]
        ),
        encoding="utf-8",
    )
    cands, err = load_corpus_file(str(path))
    assert err is None
    assert len(cands) == 1
    assert cands[0].artifact_id == "test.array.only"


def test_merge_reporting_corpus_last_layer_wins_on_duplicate_id(tmp_path) -> None:
    base = tmp_path / "base.json"
    base.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "items": [
                    {
                        "kind": "rule",
                        "artifact_id": "dup.id",
                        "tags": ["first"],
                        "content": "first content",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    docs = tmp_path / "corpus_docs"
    docs.mkdir()
    frag = docs / "field_dictionary.json"
    frag.write_text(
        json.dumps(
            [
                {
                    "kind": "rule",
                    "artifact_id": "dup.id",
                    "tags": ["second"],
                    "content": "second wins",
                }
            ]
        ),
        encoding="utf-8",
    )
    merged, paths, err = merge_reporting_corpus(str(base), corpus_docs_dir=str(docs))
    assert err is None
    assert len(merged) == 1
    assert merged[0].content == "second wins"
    assert str(base) in paths and str(frag) in paths
