"""
Live LLM answer-quality tests.

These tests use the real Ollama instance (no monkeypatching) to verify that
multi-turn conversations produce numerically correct answers. They cover the
class of failures seen before the pipeline refactor: questions that degraded
over 4-5 follow-ups due to regex preprocessing corrupting the plan context.

Run with:
    pytest tests/test_reporting_live_llm_quality.py -v -m live_llm

Skip automatically when Ollama is unreachable:
    pytest tests/ -m "not live_llm"
"""
from __future__ import annotations

import pytest

from reporting.domain import ReportingQueryIn, run_reporting_query


# ---------------------------------------------------------------------------
# Shared fixture: insert a deterministic knife inventory for quality tests
# ---------------------------------------------------------------------------

# Known data — values are chosen so expected totals are easy to verify by hand.
#
# Families:
#   Blackfoot — 4 items ($100 + $120 + $130 + $140 = $490 total)
#     Blackfoot 2.0           — no series     — $100
#     Damascus Blackfoot 2.0  — no series     — $120  ← text "damascus"
#     Traditions Blackfoot    — Traditions    — $130  ← series exclusion
#     Blood Brothers Blackfoot— Blood Brothers— $140
#
#   Raven — 2 items ($200 + $250 = $450 total)
#     Raven 2.0               — no series     — $200
#     Blood Brothers Raven    — Blood Brothers— $250
#
# Total all families: $490 + $450 = $940
#
# Blackfoot excluding Damascus text + Traditions series: $100 + $140 = $240

_ROWS = [
    # (official_name, slug, family_name, series_name, purchase_price)
    ("Blackfoot 2.0",              "blackfoot-2-0",               "Blackfoot",  None,            100.0),
    ("Damascus Blackfoot 2.0",     "damascus-blackfoot-2-0",      "Blackfoot",  None,            120.0),
    ("Traditions Blackfoot",       "traditions-blackfoot",        "Blackfoot",  "Traditions",    130.0),
    ("Blood Brothers Blackfoot",   "blood-brothers-blackfoot",    "Blackfoot",  "Blood Brothers", 140.0),
    ("Raven 2.0",                  "raven-2-0",                   "Raven",      None,            200.0),
    ("Blood Brothers Raven",       "blood-brothers-raven",        "Raven",      "Blood Brothers", 250.0),
]


@pytest.fixture(scope="module")
def live_data(invapp):
    """Insert deterministic test rows; clean up after module."""
    inserted_model_ids: list[int] = []
    inserted_inventory_ids: list[int] = []

    with invapp.get_conn() as conn:
        family_ids: dict[str, int] = {}
        series_ids: dict[str, int] = {}

        for official_name, slug, family_name, series_name, price in _ROWS:
            cur = conn.execute(
                """
                INSERT INTO knife_models_v2
                    (official_name, normalized_name, sortable_name, slug, record_status)
                VALUES (?, ?, ?, ?, 'active')
                """,
                (official_name, official_name.lower(), official_name.lower(), slug),
            )
            model_id = int(cur.lastrowid)
            inserted_model_ids.append(model_id)

            if family_name not in family_ids:
                fam_slug = family_name.lower().replace(" ", "-")
                fam_id = int(conn.execute(
                    "INSERT INTO knife_families (name, normalized_name, slug) VALUES (?, ?, ?)",
                    (family_name, family_name.lower(), fam_slug),
                ).lastrowid)
                family_ids[family_name] = fam_id
            conn.execute(
                "UPDATE knife_models_v2 SET family_id = ? WHERE id = ?",
                (family_ids[family_name], model_id),
            )

            if series_name:
                if series_name not in series_ids:
                    series_slug = series_name.lower().replace(" ", "-")
                    series_id = int(conn.execute(
                        "INSERT INTO knife_series (name, slug) VALUES (?, ?)",
                        (series_name, series_slug),
                    ).lastrowid)
                    series_ids[series_name] = series_id
                conn.execute(
                    "UPDATE knife_models_v2 SET series_id = ? WHERE id = ?",
                    (series_ids[series_name], model_id),
                )

            inv_cur = conn.execute(
                """
                INSERT INTO inventory_items_v2
                    (knife_model_id, quantity, purchase_price, condition, created_at, updated_at)
                VALUES (?, 1, ?, 'Like New', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                (model_id, float(price)),
            )
            inserted_inventory_ids.append(int(inv_cur.lastrowid))

    yield invapp

    with invapp.get_conn() as conn:
        for iid in inserted_inventory_ids:
            conn.execute("DELETE FROM inventory_items_v2 WHERE id = ?", (iid,))
        for mid in inserted_model_ids:
            conn.execute("DELETE FROM knife_models_v2 WHERE id = ?", (mid,))


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


def _answer_contains(result: dict, *fragments: str) -> bool:
    text = (result.get("answer_text") or "").lower()
    return all(f.lower() in text for f in fragments)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.live_llm
def test_spend_by_family_then_blackfoot_excluding_variants(live_data):
    """
    Multi-turn quality regression — the scenario that broke most often:

    Turn 1: "show me my total spend broken down by knife family"
            → expect Blackfoot and Raven both appear, totals roughly correct
    Turn 2: "for the Blackfoot family, if you exclude the Damascus and Traditions
             versions, how much would I have spent?"
            → expect $240 (Blackfoot 2.0 $100 + Blood Brothers Blackfoot $140)
    """
    invapp = live_data

    # --- Turn 1 ---
    q1 = ReportingQueryIn(
        question="show me my total spend broken down by knife family",
        max_rows=50,
    )
    r1 = run_reporting_query(q1, get_conn=invapp.get_conn)

    assert r1.get("session_id"), "Expected a session_id"
    session_id = r1["session_id"]

    # Both families must appear somewhere in the returned rows or answer text.
    rows1 = r1.get("rows") or []
    answer1 = (r1.get("answer_text") or "").lower()
    assert (
        _rows_contain(rows1, "blackfoot") or "blackfoot" in answer1
    ), f"Blackfoot family missing from Turn 1 result.\nAnswer: {r1.get('answer_text')}\nRows: {rows1[:5]}"
    assert (
        _rows_contain(rows1, "raven") or "raven" in answer1
    ), f"Raven family missing from Turn 1 result.\nAnswer: {r1.get('answer_text')}\nRows: {rows1[:5]}"

    # --- Turn 2 (follow-up in same session) ---
    q2 = ReportingQueryIn(
        question=(
            "for the Blackfoot family, if you exclude the Damascus and Traditions versions, "
            "how much would I have spent?"
        ),
        session_id=session_id,
        max_rows=50,
    )
    r2 = run_reporting_query(q2, get_conn=invapp.get_conn)

    sql2 = (r2.get("sql_executed") or "").lower()
    answer2 = (r2.get("answer_text") or "").lower()
    rows2 = r2.get("rows") or []

    # SQL must scope to Blackfoot and carry the exclusions.
    assert "blackfoot" in sql2, f"SQL does not filter to Blackfoot.\nSQL: {r2.get('sql_executed')}"
    assert "traditions" in sql2 or "damascus" in sql2, (
        f"SQL does not exclude Traditions/Damascus.\nSQL: {r2.get('sql_executed')}"
    )

    # Answer must contain 240 — the only correct total for the two remaining items.
    assert "240" in answer2 or _rows_contain(rows2, "240"), (
        f"Expected $240 in Turn 2 answer but got:\n{r2.get('answer_text')}\nRows: {rows2}"
    )


@pytest.mark.live_llm
def test_list_knives_then_filter_by_series(live_data):
    """
    Multi-turn quality regression — list all, then narrow by series:

    Turn 1: "list all the knives in my collection"
            → expect all 6 items (or at least both families represented)
    Turn 2: "show me only the Blood Brothers versions"
            → expect exactly 2 items: Blood Brothers Blackfoot ($140) and
              Blood Brothers Raven ($250), no others
    Turn 3: "what did I spend on those?"
            → expect $390 ($140 + $250)
    """
    invapp = live_data

    # --- Turn 1 ---
    q1 = ReportingQueryIn(
        question="list all the knives in my collection",
        max_rows=50,
    )
    r1 = run_reporting_query(q1, get_conn=invapp.get_conn)
    session_id = r1.get("session_id")
    assert session_id

    rows1 = r1.get("rows") or []
    answer1 = (r1.get("answer_text") or "").lower()
    assert (
        _rows_contain(rows1, "blackfoot") or "blackfoot" in answer1
    ), f"Turn 1 missing Blackfoot.\nAnswer: {r1.get('answer_text')}"
    assert (
        _rows_contain(rows1, "raven") or "raven" in answer1
    ), f"Turn 1 missing Raven.\nAnswer: {r1.get('answer_text')}"

    # --- Turn 2 ---
    q2 = ReportingQueryIn(
        question="show me only the Blood Brothers versions",
        session_id=session_id,
        max_rows=50,
    )
    r2 = run_reporting_query(q2, get_conn=invapp.get_conn)

    sql2 = (r2.get("sql_executed") or "").lower()
    rows2 = r2.get("rows") or []
    answer2 = (r2.get("answer_text") or "").lower()

    assert "blood brothers" in sql2 or "blood_brothers" in sql2 or "blood-brothers" in sql2, (
        f"Turn 2 SQL does not filter to Blood Brothers.\nSQL: {r2.get('sql_executed')}"
    )
    assert (
        _rows_contain(rows2, "blood brothers blackfoot") or "blood brothers blackfoot" in answer2
    ), f"Blood Brothers Blackfoot missing from Turn 2.\nRows: {rows2}\nAnswer: {r2.get('answer_text')}"
    assert (
        _rows_contain(rows2, "blood brothers raven") or "blood brothers raven" in answer2
    ), f"Blood Brothers Raven missing from Turn 2.\nRows: {rows2}\nAnswer: {r2.get('answer_text')}"

    # Traditions and Damascus must NOT appear.
    assert not _rows_contain(rows2, "traditions"), (
        f"Traditions item leaked into Blood Brothers filter.\nRows: {rows2}"
    )
    assert not _rows_contain(rows2, "damascus"), (
        f"Damascus item leaked into Blood Brothers filter.\nRows: {rows2}"
    )

    # --- Turn 3 ---
    q3 = ReportingQueryIn(
        question="what did I spend on those?",
        session_id=session_id,
        max_rows=50,
    )
    r3 = run_reporting_query(q3, get_conn=invapp.get_conn)

    answer3 = (r3.get("answer_text") or "").lower()
    rows3 = r3.get("rows") or []

    assert "390" in answer3 or _rows_contain(rows3, "390"), (
        f"Expected $390 in Turn 3 answer but got:\n{r3.get('answer_text')}\nRows: {rows3}"
    )
