#!/usr/bin/env python3
"""Offline diagnostics: which retrieval backend resolves, and Chroma/embeddings health.

Uses the same ``reporting.retrieval`` module as the app. Does not start uvicorn.

Examples:

  .venv/bin/python tools/reporting_retrieval_diagnose.py
  MKC_INVENTORY_DB=/path/to/mkc_inventory.db .venv/bin/python tools/reporting_retrieval_diagnose.py
  REPORTING_RETRIEVAL_BACKEND=chroma .venv/bin/python tools/reporting_retrieval_diagnose.py
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# Import after path fix
import reporting.retrieval as r  # noqa: E402


def _conn() -> sqlite3.Connection | None:
    db = os.environ.get("MKC_INVENTORY_DB")
    if not db or not Path(db).is_file():
        return None
    c = sqlite3.connect(db)
    c.row_factory = sqlite3.Row
    return c


def main() -> int:
    conn = _conn()
    sample_q = (
        os.environ.get("REPORTING_DIAGNOSE_QUESTION")
        or "how much have i spent on blackfoot knives excluding traditions"
    )
    resolved = r.resolve_retrieval_backend(conn)
    print("=== Retrieval resolution ===")
    print(f"  MKC_INVENTORY_DB: {os.environ.get('MKC_INVENTORY_DB') or '(unset; using in-memory rules only)'}")
    print(f"  REPORTING_RETRIEVAL_BACKEND env: {os.environ.get('REPORTING_RETRIEVAL_BACKEND') or '(unset)'}")
    print(f"  resolve_retrieval_backend(conn): {resolved}")
    if conn:
        row = conn.execute(
            "SELECT value FROM app_meta WHERE key = ?",
            (r.RETRIEVAL_BACKEND_META_KEY,),
        ).fetchone()
        print(f"  app_meta {r.RETRIEVAL_BACKEND_META_KEY}: {row['value'] if row else '(no row)'}")
    print(f"  DEFAULT_RETRIEVAL_BACKEND: {r.DEFAULT_RETRIEVAL_BACKEND}")
    print()

    st = r.get_retrieval_status(conn)
    print("=== get_retrieval_status ===")
    print(json.dumps({k: st[k] for k in sorted(st.keys())}, indent=2, default=str))
    print()

    print("=== retrieve_artifacts_with_meta (live path) ===")
    arts, meta = r.retrieve_artifacts_with_meta(sample_q, top_k=6, conn=conn)
    print(f"  question: {sample_q[:80]}...")
    print(f"  artifact_ids: {[a.artifact_id for a in arts]}")
    print(f"  corpus_fingerprint: {meta.get('corpus_fingerprint')}")
    if meta.get("chroma_upsert_skipped") is not None:
        print(f"  chroma_upsert_skipped: {meta.get('chroma_upsert_skipped')}")
    meta_compact = {k: v for k, v in dict(meta).items() if k != "semantic_candidates"}
    print("  meta (excluding semantic_candidates):")
    print(json.dumps(meta_compact, indent=2, default=str))
    print()

    chroma_path = Path(r.RETRIEVAL_CHROMA_PATH)
    print("=== Chroma on disk ===")
    print(f"  RETRIEVAL_CHROMA_PATH: {chroma_path}")
    print(f"  path exists: {chroma_path.is_dir()}")
    if chroma_path.is_dir():
        # lightweight: total size of tree
        total = 0
        for p in chroma_path.rglob("*"):
            if p.is_file():
                try:
                    total += p.stat().st_size
                except OSError:
                    pass
        print(f"  approximate bytes on disk: {total}")
    print()

    if conn:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
