#!/usr/bin/env python3
"""Offline benchmark: compare retrieval backends on a fixed prompt set.

This does **not** call the HTTP API or import the full app. It exercises
``reporting.retrieval.retrieve_artifacts_with_meta`` with explicit per-call
``backend`` overrides so lexical, embedding, vector-index, and Chroma paths are
comparable without mutating process environment.

Usage (from repo root, project venv):

  .venv/bin/python tools/reporting_retrieval_benchmark.py
  .venv/bin/python tools/reporting_retrieval_benchmark.py --json
  .venv/bin/python tools/reporting_retrieval_benchmark.py --backends lexical,chroma

Exit codes (non-``--json`` human mode uses the same rules):

- Default: ``0`` (informational; mismatches are printed but do not fail CI).
- ``--enforce``: fail unless **lexical** hits every ``must_include`` set.
- ``--enforce-all``: fail unless **every selected backend** hits every case
  (embedding/vector need sentence-transformers; Chroma needs ``chromadb``).
- ``--strict``: fail if a non-lexical backend falls back or reports a different
  ``effective_backend``.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Repo root on sys.path so `import reporting` works when run as a script.
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from reporting.retrieval import retrieve_artifacts_with_meta  # noqa: E402


DEFAULT_BACKENDS = ("lexical", "embedding", "vector", "chroma")


@dataclass(frozen=True)
class BenchmarkCase:
    case_id: str
    name: str
    question: str
    must_include: tuple[str, ...]
    top_k: int = 5


# Canonical prompts: each lists artifact_ids that should appear in top-k for
# grounding quality. Tuned to the default file-backed catalog
# ``reporting/retrieval_artifacts.json``.
CANONICAL_CASES: tuple[BenchmarkCase, ...] = (
    BenchmarkCase(
        case_id="fam-01",
        name="Family phrasing",
        question="list the knives in the blackfoot family",
        must_include=("mapping.family_vs_series",),
    ),
    BenchmarkCase(
        case_id="exc-01",
        name="Multi exclusion spend",
        question="total spend on blackfoot excluding traditions and damascus",
        must_include=("constraint.exclusions", "intent.aggregate.spend"),
    ),
    BenchmarkCase(
        case_id="agg-01",
        name="Value by family",
        question="what is my total collection value by family",
        must_include=("intent.aggregate.value_by_family",),
    ),
    BenchmarkCase(
        case_id="fu-01",
        name="List underlying items follow-up",
        question="list the knives that made up that total excluding vip models",
        must_include=("followup.list_underlying_items", "constraint.exclusions"),
    ),
    BenchmarkCase(
        case_id="scope-01",
        name="Default inventory scope",
        question="how many knives do i have in my inventory",
        must_include=("scope.default_inventory",),
    ),
)


def _must_include_hit(must_include: tuple[str, ...], got_ids: list[str]) -> tuple[bool, list[str]]:
    got_set = set(got_ids)
    missing = [m for m in must_include if m not in got_set]
    return (not missing, missing)


def run_benchmark(
    *,
    backends: tuple[str, ...],
    cases: tuple[BenchmarkCase, ...],
    strict: bool,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    summary_hits: dict[str, int] = {b: 0 for b in backends}
    summary_total: dict[str, int] = {b: 0 for b in backends}
    strict_failures: list[str] = []

    for case in cases:
        for be in backends:
            arts, meta = retrieve_artifacts_with_meta(case.question, top_k=case.top_k, backend=be)
            got_ids = [a.artifact_id for a in arts]
            ok, missing = _must_include_hit(case.must_include, got_ids)
            eff = str(meta.get("effective_backend") or "")
            fb = bool(meta.get("fallback_used"))
            summary_total[be] += 1
            if ok:
                summary_hits[be] += 1
            if strict and be != "lexical" and (fb or eff != be):
                strict_failures.append(f"{case.case_id}:{be} expected effective={be} got eff={eff} fallback={fb}")
            rows.append(
                {
                    "case_id": case.case_id,
                    "backend": be,
                    "question": case.question,
                    "must_include": list(case.must_include),
                    "got_ids": got_ids,
                    "hit": ok,
                    "missing": missing,
                    "effective_backend": eff,
                    "fallback_used": fb,
                    "meta": meta,
                }
            )

    out: dict[str, Any] = {
        "backends": list(backends),
        "cases": len(cases),
        "per_backend": {},
        "strict_failures": strict_failures,
        "rows": rows,
    }
    for be in backends:
        total = summary_total[be]
        hits = summary_hits[be]
        out["per_backend"][be] = {
            "hits": hits,
            "total": total,
            "hit_rate": (hits / total) if total else 0.0,
        }
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Reporting retrieval backend benchmark (offline).")
    p.add_argument(
        "--backends",
        default=",".join(DEFAULT_BACKENDS),
        help=f"Comma-separated backends: {','.join(DEFAULT_BACKENDS)}",
    )
    p.add_argument("--json", action="store_true", help="Emit JSON only (no human summary).")
    p.add_argument(
        "--strict",
        action="store_true",
        help="Fail if non-lexical backend falls back or effective_backend differs.",
    )
    p.add_argument(
        "--enforce",
        action="store_true",
        help="Exit 1 unless lexical backend satisfies all must_include sets.",
    )
    p.add_argument(
        "--enforce-all",
        action="store_true",
        help="Exit 1 unless every selected backend satisfies all must_include sets.",
    )
    args = p.parse_args(argv)
    backends = tuple(b.strip().lower() for b in str(args.backends).split(",") if b.strip())
    for b in backends:
        if b not in DEFAULT_BACKENDS:
            print(f"Unknown backend: {b}", file=sys.stderr)
            return 2

    result = run_benchmark(backends=backends, cases=CANONICAL_CASES, strict=bool(args.strict))

    if args.json:
        print(json.dumps(result, indent=2, default=str))
    else:
        pb = result["per_backend"]
        print("Reporting retrieval benchmark (offline)")
        print(f"Cases: {result['cases']}  Backends: {', '.join(backends)}")
        for be, stats in pb.items():
            print(f"  {be}: {stats['hits']} / {stats['total']} hit_rate={stats['hit_rate']:.2f}")
        if args.strict and result["strict_failures"]:
            print("Strict failures:")
            for line in result["strict_failures"]:
                print(f"  - {line}")

    if args.strict and result["strict_failures"]:
        return 1
    if args.enforce_all:
        for be, stats in result["per_backend"].items():
            if stats["hits"] != stats["total"]:
                return 1
        return 0
    if args.enforce:
        lex = result["per_backend"].get("lexical")
        if not lex or lex["hits"] != lex["total"]:
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
