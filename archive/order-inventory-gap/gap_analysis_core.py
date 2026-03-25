"""
Shared logic for comparing MKC email order knife lines to inventory_items_v2.

Used by tools/gap_analysis_orders_vs_inventory.py and the FastAPI gap reconciliation UI.

Trailing gift VIP: catalog names that end with \" VIP\" but do not start with \"vip \"
are freebies that never appear in order emails — their inventory is excluded from gaps.

Default full mode: blade bucket is used only for Blood Brothers and tactical catalog models
(is_tactical on knife_models_v2, with name fallback). Other models match on model + handle
with blade \"*\" because confirmations do not carry blade color for normal knives.

Traditions and Ultra series models match on catalog model only (handle and blade forced to \"*\"
on orders and inventory) — single factory colorway per SKU regardless of DB fields.

Damascus catalog models (official_name contains \"damascus\", except trailing VIP gifts): same
model-only bucket. Blade strings containing \"damascus\" also normalize to \"*\" when bucketing.

Inventory rows with mkc_order_number and/or notes containing an MKC order # that appears in the
email knives CSV for the same knife_model_id are reconciled (FIFO): matching qty is subtracted
from both the order and inventory bucket counts so purchase-linked pieces do not look like gaps.
"""

from __future__ import annotations

import csv
import re
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any

BLADE_WILDCARD = "*"

DEFAULT_BLADE_BUCKET_VALUES = frozenset(
    {
        "",
        "steel",
        "stonewashed",
        "stone",
        "metal",
        "silver",
        "raw",
        "satin",
        "n/a",
        "na",
        BLADE_WILDCARD.lower(),
        "distressedgray",
        "damascuswoodgrain",
        "pvd",
    }
)


def norm_token(s: str | None) -> str:
    x = (s or "").strip().lower()
    x = x.replace(" and ", " & ")
    x = x.replace(" & ", "/")
    x = re.sub(r"\s+", "", x)
    return x


def is_trailing_gift_vip_official_name(official_name: str | None) -> bool:
    """
    True for catalog models like \"Bighorn Chef VIP\" — free gift knives, not sold on orders.
    Excludes names that start with \"VIP \" (different product naming).
    """
    s = " ".join((official_name or "").split()).strip()
    if not s:
        return False
    low = s.lower()
    if not low.endswith(" vip"):
        return False
    if low.startswith("vip "):
        return False
    return True


def norm_blade_bucket(raw: str | None, *, strict: bool) -> str:
    n = norm_token(raw)
    if strict:
        return n
    if n and "damascus" in n:
        return BLADE_WILDCARD
    if n in DEFAULT_BLADE_BUCKET_VALUES:
        return BLADE_WILDCARD
    return n


def load_blood_brothers_ids(conn: sqlite3.Connection) -> set[int]:
    out: set[int] = set()
    for r in conn.execute(
        "SELECT id FROM knife_models_v2 WHERE lower(COALESCE(official_name,'')) LIKE '%blood brothers%'"
    ):
        out.add(int(r[0]))
    return out


def load_trailing_gift_vip_model_ids(conn: sqlite3.Connection) -> set[int]:
    out: set[int] = set()
    for r in conn.execute("SELECT id, official_name FROM knife_models_v2"):
        if is_trailing_gift_vip_official_name(r["official_name"]):
            out.add(int(r["id"]))
    return out


def bucket_full(
    mid: int,
    handle_raw: str | None,
    blade_raw: str | None,
    blood_brothers_ids: set[int],
    *,
    strict: bool,
) -> tuple[int, str, str]:
    nh = norm_token(handle_raw)
    nb = norm_blade_bucket(blade_raw, strict=strict)
    if not strict and mid in blood_brothers_ids:
        return (mid, BLADE_WILDCARD, nb)
    return (mid, nh, nb)


def _knife_models_v2_column_names(conn: sqlite3.Connection) -> set[str]:
    return {row[1] for row in conn.execute("PRAGMA table_info(knife_models_v2)").fetchall()}


def _official_name_implies_tactical(official_name: str | None) -> bool:
    """Fallback when is_tactical is missing; keep in sync with tactical SKU patterns."""
    n = " ".join((official_name or "").split()).strip().lower()
    if not n:
        return False
    if "blood brothers" in n:
        return False
    if "battle goat" in n or "wargoat" in n:
        return True
    if "tf24" in n.replace(" ", ""):
        return True
    if n == "v24" or n.startswith("v24 "):
        return True
    if "sere" in n and "25" in n:
        return True
    if "tactical speedgoat" in n:
        return True
    return False


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (name,),
        ).fetchone()
        is not None
    )


def load_traditions_ultra_name_only_model_ids(conn: sqlite3.Connection) -> set[int]:
    """
    Models sold only in one colorway: MKC Traditions line and Ultra series.
    Matched by knife_series name and by official_name fallbacks.
    """
    out: set[int] = set()
    if _table_exists(conn, "knife_series"):
        try:
            for r in conn.execute(
                """
                SELECT km.id FROM knife_models_v2 km
                INNER JOIN knife_series ks ON ks.id = km.series_id
                WHERE lower(trim(COALESCE(ks.name, ''))) IN ('traditions', 'ultra')
                """
            ):
                out.add(int(r[0]))
        except sqlite3.Error:
            pass
    for r in conn.execute("SELECT id, official_name FROM knife_models_v2"):
        on = (r["official_name"] or "").strip()
        ol = on.lower()
        if ol.startswith("traditions "):
            out.add(int(r["id"]))
            continue
        if is_trailing_gift_vip_official_name(on):
            continue
        if ol.endswith(" ultra") or ol == "ultra":
            out.add(int(r["id"]))
    return out


def load_damascus_singleton_model_ids(conn: sqlite3.Connection) -> set[int]:
    """Catalog models with Damascus in the name: single lineup; match orders to inventory by model only."""
    out: set[int] = set()
    for r in conn.execute("SELECT id, official_name FROM knife_models_v2"):
        on = (r["official_name"] or "").strip()
        if is_trailing_gift_vip_official_name(on):
            continue
        if "damascus" in on.lower():
            out.add(int(r["id"]))
    return out


def load_tactical_model_ids(conn: sqlite3.Connection) -> set[int]:
    """
    Models for which order emails include meaningful blade Cerakote / two-tone blade info.
    Prefer knife_models_v2.is_tactical when the column exists.
    """
    cols = _knife_models_v2_column_names(conn)
    out: set[int] = set()
    if "is_tactical" in cols:
        for r in conn.execute(
            "SELECT id FROM knife_models_v2 WHERE COALESCE(is_tactical, 0) = 1"
        ):
            out.add(int(r[0]))
        return out
    for r in conn.execute("SELECT id, official_name FROM knife_models_v2"):
        if _official_name_implies_tactical(r["official_name"]):
            out.add(int(r["id"]))
    return out


def bucket_gap_match(
    mid: int,
    handle_raw: str | None,
    blade_raw: str | None,
    bb_ids: set[int],
    tactical_ids: set[int],
    singleton_sku_ids: set[int],
    *,
    strict: bool,
) -> tuple[int, str, str]:
    """
    Traditions / Ultra / Damascus catalog SKUs: ignore handle+blade — one bucket per model.
    Blade bucket only for Blood Brothers + tactical; otherwise blade = '*' for orders and inventory.
    """
    if mid in singleton_sku_ids:
        return (mid, BLADE_WILDCARD, BLADE_WILDCARD)
    if mid in bb_ids:
        return bucket_full(mid, handle_raw, blade_raw, bb_ids, strict=strict)
    if mid in tactical_ids:
        nh = norm_token(handle_raw)
        nb = norm_blade_bucket(blade_raw, strict=strict)
        return (mid, nh, nb)
    nh = norm_token(handle_raw)
    return (mid, nh, BLADE_WILDCARD)


def bucket_name_handle(mid: int, handle_raw: str | None) -> tuple[int, str]:
    return (mid, norm_token(handle_raw))


def resolve_model_id(conn: sqlite3.Connection, matched_catalog_name: str) -> int | None:
    name = (matched_catalog_name or "").strip()
    if not name or "bundle:" in name.lower():
        return None
    r = conn.execute(
        "SELECT id FROM knife_models_v2 WHERE official_name = ? COLLATE NOCASE LIMIT 1",
        (name,),
    ).fetchone()
    if r:
        return int(r[0])
    r = conn.execute(
        "SELECT id FROM knife_models_v2 WHERE normalized_name = ? COLLATE NOCASE LIMIT 1",
        (name,),
    ).fetchone()
    return int(r[0]) if r else None


def sort_bucket_key(id_to_name: dict[int, str], k: tuple[int, ...]) -> tuple:
    mid = k[0]
    a = k[1] if len(k) > 1 else ""
    b = k[2] if len(k) > 2 else ""
    return (id_to_name.get(mid, ""), a, b)


def make_bucket_key(*, name_handle: bool, mid: int, handle_bucket: str, blade_bucket: str) -> str:
    if name_handle:
        return f"nh:{mid}:{handle_bucket}"
    return f"fb:{mid}:{handle_bucket}:{blade_bucket}"


def extract_inventory_order_refs(mkc_order_number: str | None, notes: str | None) -> set[str]:
    """Order numbers from inventory purchase fields (digits 5–7, optional 'order #')."""
    parts: list[str] = []
    if mkc_order_number and str(mkc_order_number).strip():
        parts.append(str(mkc_order_number).strip())
    if notes and str(notes).strip():
        parts.append(str(notes).strip())
    blob = " ".join(parts)
    if not blob.strip():
        return set()
    found: set[str] = set()
    for mo in re.finditer(r"(?i)\b(?:order\s*#?\s*)?(\d{5,7})\b", blob):
        found.add(mo.group(1))
    return found


def reconcile_buckets_by_matched_order_numbers(
    order_side: dict[tuple[str, int], list[list]],
    inv_side: dict[tuple[str, int], list[list]],
    orders_by_key: dict[tuple[Any, ...], int],
    inv_by_key: dict[tuple[Any, ...], int],
) -> int:
    """
    For each (order_number, knife_model_id) present on both email lines and inventory refs,
    reduce orders_by_key and inv_by_key by min(order qty, inv qty) in FIFO order within that pair.
    Returns total pieces reconciled.
    """
    total = 0
    for om in set(order_side.keys()) & set(inv_side.keys()):
        o_queue: list[list] = [list(x) for x in order_side[om]]
        i_queue: list[list] = [list(x) for x in inv_side[om]]
        while o_queue and i_queue:
            ok, oq = o_queue[0]
            ik, iq = i_queue[0]
            if oq <= 0:
                o_queue.pop(0)
                continue
            if iq <= 0:
                i_queue.pop(0)
                continue
            take = min(oq, iq)
            orders_by_key[ok] -= take
            inv_by_key[ik] -= take
            total += take
            oq -= take
            iq -= take
            o_queue[0][1] = oq
            i_queue[0][1] = iq
    for d in (orders_by_key, inv_by_key):
        for k in [x for x in d if d[x] <= 0]:
            del d[k]
    return total


def compute_gap_analysis(
    orders_path: Path,
    db_path: Path,
    *,
    name_handle: bool = False,
    strict: bool = False,
) -> dict[str, Any]:
    """
    Returns dict with rows (list), stats, skipped lists, vip_inventory_excluded (list of dicts).
    """
    if not orders_path.is_file():
        raise FileNotFoundError(f"Missing orders CSV: {orders_path}")
    if not db_path.is_file():
        raise FileNotFoundError(f"Missing DB: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    bb_ids = load_blood_brothers_ids(conn)
    tactical_ids = load_tactical_model_ids(conn)
    traditions_ultra_ids = load_traditions_ultra_name_only_model_ids(conn)
    damascus_singleton_ids = load_damascus_singleton_model_ids(conn)
    singleton_sku_ids = traditions_ultra_ids | damascus_singleton_ids
    gift_vip_ids = load_trailing_gift_vip_model_ids(conn)

    orders_by_key: dict[tuple[Any, ...], int] = defaultdict(int)
    order_examples: dict[tuple[Any, ...], list[str]] = defaultdict(list)
    skipped_bundle: list[str] = []
    skipped_unresolved: list[tuple[str, str]] = []
    orders_by_model: dict[int, int] = defaultdict(int)
    csv_orders_by_mid: dict[int, set[str]] = defaultdict(set)
    order_side: dict[tuple[str, int], list[list]] = defaultdict(list)

    with open(orders_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            mname = row.get("matched_catalog_name") or ""
            mid = resolve_model_id(conn, mname)
            if mid is None:
                if "bundle:" in mname.lower():
                    skipped_bundle.append(row.get("line_title", "")[:120])
                else:
                    skipped_unresolved.append((mname or "", row.get("line_title", "")[:120]))
                continue
            qty = int(row.get("quantity") or 1)
            orders_by_model[mid] += qty
            on = (row.get("order_number") or "").strip()
            if on:
                csv_orders_by_mid[mid].add(on)

            hc = row.get("handle_color_v2") or row.get("handle_color")
            bc = row.get("blade_color_v2") or row.get("blade_color")
            if name_handle:
                if mid in singleton_sku_ids:
                    key = (mid, norm_token(BLADE_WILDCARD))
                else:
                    key = bucket_name_handle(mid, hc)
            else:
                key = bucket_gap_match(
                    mid, hc, bc, bb_ids, tactical_ids, singleton_sku_ids, strict=strict
                )
            orders_by_key[key] += qty
            if on:
                order_side[(on, mid)].append([key, qty])
            if len(order_examples[key]) < 3:
                order_examples[key].append(
                    f"order {row.get('order_number')} {row.get('order_date')} qty={qty} | {row.get('line_title', '')[:80]}"
                )

    inv_by_key: dict[tuple[Any, ...], int] = defaultdict(int)
    inv_examples: dict[tuple[Any, ...], list[int]] = defaultdict(list)
    inv_by_model: dict[int, int] = defaultdict(int)
    vip_inv_qty_by_model: dict[int, int] = defaultdict(int)
    inv_side: dict[tuple[str, int], list[list]] = defaultdict(list)

    # Include acquired_date so we can treat rows that already have an order/purchase date
    # as "resolved" even when the email-order filter missed the order lines in the CSV.
    inv_sql_full = (
        "SELECT id, knife_model_id, handle_color, blade_color, quantity, acquired_date, "
        "mkc_order_number, notes FROM inventory_items_v2 WHERE knife_model_id IS NOT NULL"
    )
    inv_sql_fallback = (
        "SELECT id, knife_model_id, handle_color, blade_color, quantity, acquired_date "
        "FROM inventory_items_v2 WHERE knife_model_id IS NOT NULL"
    )
    try:
        inv_rows = conn.execute(inv_sql_full)
        inv_has_order_fields = True
    except sqlite3.OperationalError:
        inv_rows = conn.execute(inv_sql_fallback)
        inv_has_order_fields = False
    resolved_by_inventory_date_qty = 0
    for r in inv_rows:
        mid = int(r["knife_model_id"])
        qty = int(r["quantity"] or 1)
        acquired_date = r["acquired_date"]
        acquired_date_present = bool(acquired_date and str(acquired_date).strip())
        if mid in gift_vip_ids:
            vip_inv_qty_by_model[mid] += qty
            continue

        # If the inventory row already has an order/purchase date, treat it as resolved
        # when its order-number refs do not appear in the email CSV (common when the
        # email filter missed that order format). This prevents "inventory > orders"
        # buckets from showing for those entries during gap assessment.
        if inv_has_order_fields and acquired_date_present:
            refs = extract_inventory_order_refs(r["mkc_order_number"], r["notes"])
            matched_refs = refs & csv_orders_by_mid.get(mid, set())
            if refs and not matched_refs:
                resolved_by_inventory_date_qty += qty
                continue

        inv_by_model[mid] += qty
        if name_handle:
            if mid in singleton_sku_ids:
                key = (mid, norm_token(BLADE_WILDCARD))
            else:
                key = bucket_name_handle(mid, r["handle_color"])
        else:
            key = bucket_gap_match(
                mid,
                r["handle_color"],
                r["blade_color"],
                bb_ids,
                tactical_ids,
                singleton_sku_ids,
                strict=strict,
            )
        inv_by_key[key] += qty
        if len(inv_examples[key]) < 5:
            inv_examples[key].append(int(r["id"]))
        if inv_has_order_fields:
            refs = extract_inventory_order_refs(r["mkc_order_number"], r["notes"])
            matched_refs = refs & csv_orders_by_mid.get(mid, set())
            if matched_refs:
                on_pick = min(matched_refs)
                inv_side[(on_pick, mid)].append([key, qty])

    matched_order_qty = reconcile_buckets_by_matched_order_numbers(
        order_side, inv_side, orders_by_key, inv_by_key
    )

    id_to_name: dict[int, str] = {}
    for r in conn.execute("SELECT id, official_name FROM knife_models_v2"):
        id_to_name[int(r["id"])] = r["official_name"] or ""

    conn.close()

    all_keys = set(orders_by_key) | set(inv_by_key)
    rows_out: list[dict[str, Any]] = []
    gaps_pos: list[dict[str, Any]] = []
    gaps_neg: list[dict[str, Any]] = []

    for key in sorted(all_keys, key=lambda k: sort_bucket_key(id_to_name, k)):
        mid = int(key[0])
        if name_handle:
            hc, bc_disp = key[1], "(blade ignored)"
            oq = orders_by_key.get(key, 0)
            iq = inv_by_key.get(key, 0)
        else:
            hc, bc_disp = key[1], key[2]
            oq = orders_by_key.get(key, 0)
            iq = inv_by_key.get(key, 0)
        gap = oq - iq
        bucket_key = make_bucket_key(
            name_handle=name_handle, mid=mid, handle_bucket=hc, blade_bucket=bc_disp
        )
        if mid in singleton_sku_ids:
            row_match_mode = "model_only_singleton_sku"
        elif name_handle:
            row_match_mode = "model_plus_handle"
        else:
            row_match_mode = "model_handle_blade_bb_tactical_only"
        rec = {
            "bucket_key": bucket_key,
            "knife_model_id": mid,
            "knife_name": id_to_name.get(mid, ""),
            "match_mode": row_match_mode,
            "handle_bucket": hc,
            "blade_bucket": bc_disp,
            "strict_blade": "yes" if strict and not name_handle else "no",
            "ordered_qty": oq,
            "inventory_qty": iq,
            "gap_ordered_minus_inventory": gap,
            "order_examples": " | ".join(order_examples.get(key, [])),
            "inventory_item_ids": " ".join(str(x) for x in inv_examples.get(key, [])),
        }
        rows_out.append(rec)
        if gap > 0:
            gaps_pos.append(rec)
        elif gap < 0:
            gaps_neg.append(rec)

    model_ids = set(orders_by_model) | set(inv_by_model)
    model_gaps: list[tuple[int, str, int, int, int]] = []
    for mid in sorted(model_ids, key=lambda m: id_to_name.get(m, "")):
        oq, iq = orders_by_model.get(mid, 0), inv_by_model.get(mid, 0)
        g = oq - iq
        if oq > 0 or iq > 0:
            model_gaps.append((mid, id_to_name.get(mid, ""), oq, iq, g))

    vip_inventory_excluded = [
        {
            "knife_model_id": mid,
            "knife_name": id_to_name.get(mid, ""),
            "inventory_qty_excluded": q,
        }
        for mid, q in sorted(vip_inv_qty_by_model.items(), key=lambda x: id_to_name.get(x[0], ""))
        if q > 0
    ]

    return {
        "rows": rows_out,
        "stats": {
            "distinct_bucket_keys": len(all_keys),
            "buckets_orders_gt_inventory": len(gaps_pos),
            "buckets_inventory_gt_orders": len(gaps_neg),
            "gift_vip_model_count": len(gift_vip_ids),
            "gift_vip_inventory_qty_total": sum(vip_inv_qty_by_model.values()),
            "tactical_model_ids_count": len(tactical_ids),
            "traditions_ultra_model_ids_count": len(traditions_ultra_ids),
            "damascus_singleton_model_ids_count": len(damascus_singleton_ids),
            "singleton_sku_model_ids_count": len(singleton_sku_ids),
            "matched_by_inventory_order_number_qty": int(matched_order_qty),
            "resolved_by_inventory_acquired_date_qty": int(resolved_by_inventory_date_qty),
        },
        "model_gaps": [
            {"knife_model_id": a, "knife_name": b, "ordered_qty": c, "inventory_qty": d, "gap": e}
            for a, b, c, d, e in model_gaps
        ],
        "skipped_bundle": skipped_bundle,
        "skipped_unresolved": [{"catalog": a, "line": b} for a, b in skipped_unresolved],
        "vip_inventory_excluded": vip_inventory_excluded,
        "orders_path": str(orders_path.resolve()),
        "db_path": str(db_path.resolve()),
        "analysis_name_handle": name_handle,
        "analysis_strict_blade": strict,
    }


def write_gap_outputs(
    result: dict[str, Any],
    *,
    summary_path: Path,
    csv_path: Path,
) -> None:
    """Write summary .txt and buckets .csv from compute_gap_analysis result."""
    rows = result["rows"]
    stats = result["stats"]
    name_handle = result["analysis_name_handle"]
    strict = result["analysis_strict_blade"]

    summary_path.parent.mkdir(parents=True, exist_ok=True)
    if rows:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            # bucket_key first for stable reconciliation UI keys
            fieldnames = list(rows[0].keys())
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(rows)

    mode_desc = (
        "model + handle only (blade ignored); Traditions/Ultra/Damascus = model only"
        if name_handle
        else (
            "Traditions/Ultra/Damascus = model only (handle+blade '*'); Blood Brothers + tactical use blade; "
            "other models model+handle + blade '*'"
            if not strict
            else "strict blade for BB + tactical; singleton SKUs model only; other blade '*'; BB handle *"
        )
    )

    tac_n = (result.get("stats") or {}).get("tactical_model_ids_count")
    tu_n = (result.get("stats") or {}).get("traditions_ultra_model_ids_count")
    dm_n = (result.get("stats") or {}).get("damascus_singleton_model_ids_count")
    sing_n = (result.get("stats") or {}).get("singleton_sku_model_ids_count")

    lines = [
        "Gap analysis: MKC email orders (knives CSV) vs inventory_items_v2",
        f"Orders file: {result['orders_path']}",
        f"Match mode: {mode_desc}",
    ]
    if sing_n is not None:
        lines.append(f"Singleton SKU models (Traditions + Ultra + Damascus, model-only match): {sing_n}")
    if tu_n is not None:
        lines.append(f"  — Traditions / Ultra subset: {tu_n}")
    if dm_n is not None:
        lines.append(f"  — Damascus name subset: {dm_n}")
    mo_match = (result.get("stats") or {}).get("matched_by_inventory_order_number_qty")
    if mo_match is not None and mo_match > 0:
        lines.append(
            f"Inventory SKUs linked by mkc_order_number / notes matching email order #(s): "
            f"{mo_match} pc(s) netted out of bucket gaps (FIFO by order # + model)."
        )
    resolved_date = (result.get("stats") or {}).get("resolved_by_inventory_acquired_date_qty")
    if resolved_date is not None and resolved_date > 0:
        lines.append(
            f"Inventory SKUs with acquired_date set but order #(s) not found in email CSV: "
            f"{resolved_date} pc(s) excluded from bucket gap math (no email confirmation)."
        )
    if not name_handle and tac_n is not None:
        lines.append(f"Tactical catalog models (blade bucket active): {tac_n}")
    lines.extend(
        [
            "Trailing gift VIP models: inventory excluded (not expected on order emails).",
            f"Output CSV: {csv_path}",
            "",
            f"Distinct bucket keys: {stats['distinct_bucket_keys']}",
            f"Buckets orders > inventory: {stats['buckets_orders_gt_inventory']}",
            f"Buckets inventory > orders: {stats['buckets_inventory_gt_orders']}",
            f"Gift VIP inventory qty excluded (total pieces): {stats['gift_vip_inventory_qty_total']}",
            "",
            "=== Model-level totals (all handles / colors combined) ===",
            "",
        ]
    )
    for mg in result["model_gaps"]:
        name, mid = mg["knife_name"], mg["knife_model_id"]
        oq, iq, g = mg["ordered_qty"], mg["inventory_qty"], mg["gap"]
        if name and (oq != iq or g != 0):
            tag = "OK" if g == 0 else (f"orders+{g}" if g > 0 else f"inv extra {-g}")
            lines.append(f"  {name} (id {mid}): ordered {oq} | inventory {iq} | {tag}")

    lines.append("")
    gaps_pos = [r for r in rows if r["gap_ordered_minus_inventory"] > 0]
    if gaps_pos:
        lines.append("=== Buckets: orders > inventory ===")
        for r in sorted(gaps_pos, key=lambda x: -x["gap_ordered_minus_inventory"]):
            lines.append(
                f"  {r['knife_name']} | h={r['handle_bucket']!r} b={r['blade_bucket']!r} | "
                f"ordered {r['ordered_qty']} inv {r['inventory_qty']} GAP +{r['gap_ordered_minus_inventory']}"
            )
            if r["order_examples"]:
                lines.append(f"      e.g. {r['order_examples'][:200]}")
        lines.append("")

    if result.get("vip_inventory_excluded"):
        lines.append("=== Gift VIP — inventory excluded from gap math ===")
        for v in result["vip_inventory_excluded"]:
            lines.append(
                f"  {v['knife_name']} (id {v['knife_model_id']}): {v['inventory_qty_excluded']} pc(s)"
            )
        lines.append("")

    sb = result.get("skipped_bundle") or []
    if sb:
        lines.append(f"=== Skipped bundle lines ({len(sb)}) ===")
        for s in sb[:15]:
            lines.append(f"  - {s}")
        if len(sb) > 15:
            lines.append(f"  ... +{len(sb) - 15} more")
        lines.append("")

    su = result.get("skipped_unresolved") or []
    if su:
        lines.append(f"=== Unresolved catalog name ({len(su)}) ===")
        for item in su[:10]:
            lines.append(f"  catalog={item['catalog']!r} | {item['line']}")
        lines.append("")

    lines.extend(
        [
            "=== Next steps ===",
            "- Culinary set: filter script splits set → 3 catalog models.",
            "- Full mode already ignores blade for non-tactical knives; use --name-handle to drop blade for every model.",
            "- Trailing \" VIP\" catalog models: free gifts; inventory-only is expected.",
        ]
    )

    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
