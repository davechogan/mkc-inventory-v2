"""
Live LLM answer-quality tests.

These tests use the real Ollama instance (no monkeypatching) against the seed
DB snapshot at tests/fixtures/mkc_inventory_seed.db. They verify that
multi-turn conversations produce correct answers against known real data.

The seed DB is a point-in-time snapshot — update it deliberately when the
inventory or catalog changes in a way tests should reflect.

Run with:
    pytest tests/test_reporting_live_llm_quality.py -v -m live_llm

Skip automatically when Ollama is unreachable:
    pytest tests/ -m "not live_llm"

Known data facts (from seed DB — verified against real inventory):
  Blood Brothers catalog:  3 models — Blackfoot 2.0 ($400 MSRP),
                           Mini Speedgoat 2.0 ($250 MSRP), Wargoat ($400 MSRP)
  Blood Brothers owned:    2 — Blackfoot 2.0 (qty 2 × $400 = $800), Mini Speedgoat 2.0 ($250) = $1,050
  Blackfoot owned (8):     $3,775 total spend (Blood Brothers Blackfoot qty 2 × $400)
    Excluding Damascus ($1,000) + Traditions ($475): $2,300 remaining
"""
from __future__ import annotations

import json

import pytest

from reporting.domain import ReportingQueryIn, run_reporting_query


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rows_contain(rows: list, value: str) -> bool:
    """True if any cell in any row contains value (case-insensitive)."""
    v = value.lower()
    for row in rows:
        if isinstance(row, dict):
            if any(v in str(cell).lower() for cell in row.values()):
                return True
        elif isinstance(row, (list, tuple)):
            if any(v in str(cell).lower() for cell in row):
                return True
    return False


def _print_turn_debug(label: str, result: dict) -> None:
    """Print full pipeline debug for a turn so failures show exact LLM I/O."""
    dbg = result.get("pipeline_debug") or {}
    rewriter = dbg.get("rewriter_llm") or {}
    # planner_llm is {"primary": {...}, "retry": {...}}; use primary attempt
    planner_attempts = dbg.get("planner_llm") or {}
    planner = planner_attempts.get("primary") or planner_attempts.get("retry") or {}
    print(f"\n{'='*60}")
    print(f"[{label}] SQL: {result.get('sql_executed')}")
    print(f"[{label}] semantic_plan: {json.dumps(result.get('semantic_plan'), indent=2)}")
    print(f"[{label}] rows ({len(result.get('rows') or [])}): {(result.get('rows') or [])[:5]}")
    print(f"[{label}] answer: {result.get('answer_text')}")
    if rewriter.get("rewritten_query"):
        print(f"[{label}] retrieval_query (rewritten): {rewriter['rewritten_query']}")
    if planner:
        print(f"[{label}] planner system:\n{str(planner.get('system', ''))[:2000]}")
        print(f"[{label}] planner user:\n{str(planner.get('user', ''))[:4000]}")
        print(f"[{label}] planner raw response: {str(planner.get('raw_response', ''))[:500]}")
    print('='*60)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.live_llm
def test_spend_by_family_then_blackfoot_excluding_variants(invapp):
    """
    Multi-turn regression: exclusions are carried forward and applied correctly.

    Turn 1: "show me my total spend broken down by knife family"
            → Blackfoot ($3,375) and Speedgoat ($1,950) must appear
    Turn 2: "for the Blackfoot family, if you exclude the Damascus and
             Traditions versions, how much would I have spent?"
            → $2,300 (Blood Brothers Blackfoot qty×2=$800, removes Damascus $1,000
              + Traditions $475 from $3,775 total)
    """
    # --- Turn 1 ---
    q1 = ReportingQueryIn(
        question="show me my total spend broken down by knife family",
        max_rows=50,
        debug=True,
    )
    r1 = run_reporting_query(q1, get_conn=invapp.get_conn)
    _print_turn_debug("T1", r1)

    assert r1.get("session_id"), "Expected a session_id"
    session_id = r1["session_id"]

    rows1 = r1.get("rows") or []
    answer1 = (r1.get("answer_text") or "").lower()
    assert (
        _rows_contain(rows1, "blackfoot") or "blackfoot" in answer1
    ), f"Blackfoot family missing from Turn 1.\nAnswer: {r1.get('answer_text')}\nRows: {rows1[:5]}"
    assert (
        _rows_contain(rows1, "speedgoat") or "speedgoat" in answer1
    ), f"Speedgoat family missing from Turn 1.\nAnswer: {r1.get('answer_text')}\nRows: {rows1[:5]}"

    # --- Turn 2 ---
    q2 = ReportingQueryIn(
        question=(
            "for the Blackfoot family, if you exclude the Damascus and Traditions versions, "
            "how much would I have spent?"
        ),
        session_id=session_id,
        max_rows=50,
        debug=True,
    )
    r2 = run_reporting_query(q2, get_conn=invapp.get_conn)
    _print_turn_debug("T2", r2)

    sql2 = (r2.get("sql_executed") or "").lower()
    answer2 = (r2.get("answer_text") or "").lower()
    rows2 = r2.get("rows") or []

    assert "blackfoot" in sql2, f"SQL does not filter to Blackfoot.\nSQL: {r2.get('sql_executed')}"
    assert "traditions" in sql2 or "damascus" in sql2, (
        f"SQL missing exclusion of Traditions/Damascus.\nSQL: {r2.get('sql_executed')}"
    )
    assert "2300" in answer2 or "2,300" in answer2 or _rows_contain(rows2, "2300"), (
        f"Expected $2,300 in Turn 2 answer.\nAnswer: {r2.get('answer_text')}\nRows: {rows2}"
    )


@pytest.mark.live_llm
def test_list_knives_then_filter_by_series(invapp):
    """
    Multi-turn regression: filter narrows correctly and spend is carried forward.

    Turn 1: "list all the knives in my collection"
            → Blackfoot and Speedgoat families both represented
    Turn 2: "show me only the Blood Brothers versions"
            → Blood Brothers Blackfoot 2.0 and Blood Brothers Mini Speedgoat 2.0;
              no Traditions, no Damascus
    Turn 3: "what did I spend on those?"
            → $1,050 (Blood Brothers Blackfoot qty 2 × $400=$800 + Mini Speedgoat $250)
    """
    # --- Turn 1 ---
    q1 = ReportingQueryIn(
        question="list all the knives in my collection",
        max_rows=50,
        debug=True,
    )
    r1 = run_reporting_query(q1, get_conn=invapp.get_conn)
    _print_turn_debug("T1", r1)
    session_id = r1.get("session_id")
    assert session_id

    rows1 = r1.get("rows") or []
    answer1 = (r1.get("answer_text") or "").lower()
    assert (
        _rows_contain(rows1, "blackfoot") or "blackfoot" in answer1
    ), f"Turn 1 missing Blackfoot.\nAnswer: {r1.get('answer_text')}"

    # --- Turn 2 ---
    q2 = ReportingQueryIn(
        question="show me only the Blood Brothers versions",
        session_id=session_id,
        max_rows=50,
        debug=True,
    )
    r2 = run_reporting_query(q2, get_conn=invapp.get_conn)
    _print_turn_debug("T2", r2)

    sql2 = (r2.get("sql_executed") or "").lower()
    rows2 = r2.get("rows") or []
    answer2 = (r2.get("answer_text") or "").lower()

    assert "blood brothers" in sql2 or "blood_brothers" in sql2, (
        f"Turn 2 SQL does not filter to Blood Brothers.\nSQL: {r2.get('sql_executed')}"
    )
    assert (
        _rows_contain(rows2, "blood brothers blackfoot") or "blood brothers blackfoot" in answer2
    ), f"Blood Brothers Blackfoot 2.0 missing from Turn 2.\nRows: {rows2}\nAnswer: {r2.get('answer_text')}"
    assert (
        _rows_contain(rows2, "mini speedgoat") or "mini speedgoat" in answer2
    ), f"Blood Brothers Mini Speedgoat 2.0 missing from Turn 2.\nRows: {rows2}\nAnswer: {r2.get('answer_text')}"
    assert not _rows_contain(rows2, "traditions"), (
        f"Traditions item leaked into Blood Brothers filter.\nRows: {rows2}"
    )

    # --- Turn 3 ---
    q3 = ReportingQueryIn(
        question="what did I spend on those?",
        session_id=session_id,
        max_rows=50,
        debug=True,
    )
    r3 = run_reporting_query(q3, get_conn=invapp.get_conn)
    _print_turn_debug("T3", r3)

    answer3 = (r3.get("answer_text") or "").lower()
    rows3 = r3.get("rows") or []
    assert "1050" in answer3 or "1,050" in answer3 or _rows_contain(rows3, "1050"), (
        f"Expected $1,050 in Turn 3 answer.\nAnswer: {r3.get('answer_text')}\nRows: {rows3}"
    )


@pytest.mark.live_llm
def test_catalog_blood_brothers_chain_with_scope_switch(invapp):
    """
    Multi-turn catalog → inventory scope-switch regression.

    Turn 1: "how many blood brothers knives does MKC offer?"
            → catalog scope, count = 3 (Blackfoot 2.0, Mini Speedgoat 2.0, Wargoat)
    Turn 2: "which ones are they?"
            → catalog list — all three models appear
    Turn 3: "which of those is the most expensive?"
            → Blackfoot 2.0 or Wargoat (both $400 MSRP — accept either)
    Turn 4: "do I own one of those?"
            → switches to inventory — confirms ownership of Blood Brothers items
    """
    # --- Turn 1 ---
    q1 = ReportingQueryIn(
        question="how many blood brothers knives does MKC offer?",
        max_rows=50,
        debug=True,
    )
    r1 = run_reporting_query(q1, get_conn=invapp.get_conn)
    _print_turn_debug("T1", r1)
    session_id = r1.get("session_id")
    assert session_id

    sql1 = (r1.get("sql_executed") or "").lower()
    answer1 = (r1.get("answer_text") or "").lower()
    assert "3" in answer1 or "three" in answer1, (
        f"Turn 1 expected count of 3.\nAnswer: {r1.get('answer_text')}"
    )
    assert "reporting_models" in sql1, (
        f"Turn 1 should use catalog scope.\nSQL: {r1.get('sql_executed')}"
    )

    # --- Turn 2 ---
    q2 = ReportingQueryIn(
        question="which ones are they?",
        session_id=session_id,
        max_rows=50,
        debug=True,
    )
    r2 = run_reporting_query(q2, get_conn=invapp.get_conn)
    _print_turn_debug("T2", r2)
    rows2 = r2.get("rows") or []
    answer2 = (r2.get("answer_text") or "").lower()

    assert r2.get("sql_executed"), f"Turn 2 produced no SQL.\nAnswer: {r2.get('answer_text')}"
    assert (
        _rows_contain(rows2, "blackfoot") or "blackfoot" in answer2
    ), f"Turn 2 missing Blood Brothers Blackfoot 2.0.\nRows: {rows2}\nAnswer: {r2.get('answer_text')}"
    assert (
        _rows_contain(rows2, "speedgoat") or "speedgoat" in answer2
    ), f"Turn 2 missing Blood Brothers Mini Speedgoat 2.0.\nRows: {rows2}\nAnswer: {r2.get('answer_text')}"
    assert (
        _rows_contain(rows2, "wargoat") or "wargoat" in answer2
    ), f"Turn 2 missing Blood Brothers Wargoat.\nRows: {rows2}\nAnswer: {r2.get('answer_text')}"

    # --- Turn 3 ---
    q3 = ReportingQueryIn(
        question="which of those is the most expensive?",
        session_id=session_id,
        max_rows=50,
        debug=True,
    )
    r3 = run_reporting_query(q3, get_conn=invapp.get_conn)
    _print_turn_debug("T3", r3)
    sql3 = (r3.get("sql_executed") or "").lower()
    answer3 = (r3.get("answer_text") or "").lower()
    rows3 = r3.get("rows") or []

    assert r3.get("sql_executed"), f"Turn 3 produced no SQL.\nAnswer: {r3.get('answer_text')}"
    assert "blood brothers" in sql3, (
        f"Turn 3 SQL lost Blood Brothers filter.\nSQL: {r3.get('sql_executed')}"
    )
    # Blackfoot 2.0 and Wargoat are both $400 — accept either
    assert (
        _rows_contain(rows3, "blackfoot") or "blackfoot" in answer3
        or _rows_contain(rows3, "wargoat") or "wargoat" in answer3
    ), f"Turn 3 expected Blackfoot 2.0 or Wargoat.\nRows: {rows3}\nAnswer: {r3.get('answer_text')}"

    # --- Turn 4 ---
    q4 = ReportingQueryIn(
        question="do I own one of those?",
        session_id=session_id,
        max_rows=50,
        debug=True,
    )
    r4 = run_reporting_query(q4, get_conn=invapp.get_conn)
    _print_turn_debug("T4", r4)
    sql4 = (r4.get("sql_executed") or "").lower()
    answer4 = (r4.get("answer_text") or "").lower()
    rows4 = r4.get("rows") or []

    assert r4.get("sql_executed"), f"Turn 4 produced no SQL.\nAnswer: {r4.get('answer_text')}"
    assert "reporting_inventory" in sql4, (
        f"Turn 4 should switch to inventory scope.\nSQL: {r4.get('sql_executed')}"
    )
    assert "blood brothers" in sql4, (
        f"Turn 4 SQL lost Blood Brothers filter.\nSQL: {r4.get('sql_executed')}"
    )
    # Seed DB has 2 Blood Brothers items owned — answer should confirm ownership
    assert (
        _rows_contain(rows4, "blood brothers") or "yes" in answer4 or "own" in answer4
        or "blood brothers" in answer4 or any(n in answer4 for n in ["1", "2", "two", "one"])
    ), f"Turn 4 should confirm ownership.\nRows: {rows4}\nAnswer: {r4.get('answer_text')}"
