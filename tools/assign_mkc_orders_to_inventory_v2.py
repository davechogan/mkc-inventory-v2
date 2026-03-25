#!/usr/bin/env python3
"""
Assign MKC order numbers + acquired dates to inventory_items_v2 using
mkc_email_orders_knives.csv as the source of truth.

Matching mode implemented: name + handle only.
  - For Traditions / Ultra / Damascus "singleton SKU models": ignore handle (model-only).
  - For other models: match by knife_model_id + handle bucket.

Splitting:
  - If an inventory row's quantity spans multiple distinct email order_numbers,
    that inventory row is split into multiple rows (one per distinct order_number).
  - Only inventory rows missing `mkc_order_number` are split and reallocated.
  - Inventory rows that have `mkc_order_number` but are missing `acquired_date` are updated in-place.

VIP:
  - Trailing gift VIP models are skipped (per user instruction).

This script writes directly to mkc_inventory.db.
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, DefaultDict


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from gap_analysis_core import (  # noqa: E402
    BLADE_WILDCARD,
    bucket_name_handle,
    load_damascus_singleton_model_ids,
    load_traditions_ultra_name_only_model_ids,
    load_trailing_gift_vip_model_ids,
    norm_token,
    resolve_model_id,
)


def _is_blank(x: Any) -> bool:
    return x is None or str(x).strip() == ""


def _row_get(row: sqlite3.Row, col: str) -> Any:
    # sqlite3.Row behaves like a mapping but does not support `.get()`.
    return row[col] if col in row.keys() else None


def _parse_int_maybe(s: str | None) -> int | None:
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def _compute_name_handle_bucket_key(
    mid: int,
    handle_raw: str | None,
    *,
    singleton_sku_ids: set[int],
) -> tuple[int, str]:
    """
    Mirrors gap_analysis_core name_handle keying:
      - singleton SKU: (mid, norm_token('*'))
      - otherwise: (mid, norm_token(handle_raw))
    """
    if mid in singleton_sku_ids:
        return (mid, norm_token(BLADE_WILDCARD))
    # bucket_name_handle returns (mid, norm_token(handle_raw))
    return bucket_name_handle(mid, handle_raw)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--orders", type=Path, default=ROOT / "data/mkc_email_orders_knives.csv")
    ap.add_argument("--db", type=Path, default=ROOT / "data/mkc_inventory.db")
    ap.add_argument("--apply", action="store_true", help="Actually write updates/inserts to DB.")
    args = ap.parse_args()

    if not args.orders.is_file():
        print(f"Missing orders CSV: {args.orders}", file=sys.stderr)
        return 2
    if not args.db.is_file():
        print(f"Missing DB: {args.db}", file=sys.stderr)
        return 2

    conn = sqlite3.connect(str(args.db))
    conn.row_factory = sqlite3.Row

    singleton_sku_ids = load_traditions_ultra_name_only_model_ids(conn) | load_damascus_singleton_model_ids(conn)
    vip_ids = load_trailing_gift_vip_model_ids(conn)

    # Snapshot inventory schema columns so we can copy rows for inserts.
    inv_cols = [r["name"] for r in conn.execute("PRAGMA table_info(inventory_items_v2)").fetchall()]
    if "mkc_order_number" not in inv_cols:
        raise RuntimeError("inventory_items_v2 missing mkc_order_number (DB not prepared?)")

    # Build email bucket queue:
    #   email_queue[(mid, handle_bucket)][order_number] = {qty, order_date}
    email_queue: DefaultDict[tuple[int, str], dict[str, dict[str, Any]]] = defaultdict(dict)

    with args.orders.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            matched_catalog_name = row.get("matched_catalog_name") or ""
            mid = resolve_model_id(conn, matched_catalog_name)
            if mid is None:
                continue
            if mid in vip_ids:
                # Trailing VIP gifts are excluded from gap math and allocation.
                continue

            order_number = (row.get("order_number") or "").strip()
            order_date = (row.get("order_date") or "").strip()
            if not order_number:
                continue

            qty = int(row.get("quantity") or 1)
            handle_raw = row.get("handle_color_v2") or row.get("handle_color")
            key = _compute_name_handle_bucket_key(mid, handle_raw, singleton_sku_ids=singleton_sku_ids)

            bucket_orders = email_queue[key]
            if order_number not in bucket_orders:
                bucket_orders[order_number] = {"qty": 0, "order_date": order_date}
            bucket_orders[order_number]["qty"] += qty

            # Keep first non-empty order_date.
            if not bucket_orders[order_number]["order_date"] and order_date:
                bucket_orders[order_number]["order_date"] = order_date

    # Fetch inventory items (skip VIP models).
    inv_rows = conn.execute(
        """
        SELECT *
        FROM inventory_items_v2
        WHERE knife_model_id IS NOT NULL
          AND knife_model_id NOT IN (SELECT id FROM knife_models_v2 WHERE 1=0)
        """
    ).fetchall()
    # Above query isn't useful for filtering; build explicit VIP list in Python to avoid SQL gymnastics.
    inv_rows = conn.execute(
        """
        SELECT *
        FROM inventory_items_v2
        WHERE knife_model_id IS NOT NULL
        """
    ).fetchall()
    inv_rows = [r for r in inv_rows if int(r["knife_model_id"]) not in vip_ids]

    # Initialize remaining email quantities as copy.
    remaining: DefaultDict[tuple[int, str], dict[str, dict[str, Any]]] = defaultdict(dict)
    for key, bucket_orders in email_queue.items():
        for on, v in bucket_orders.items():
            remaining[key][on] = {"qty": int(v["qty"]), "order_date": v.get("order_date") or ""}

    # 1) Consume already-assigned inventory (rows with mkc_order_number set),
    #    so we don't allocate the same email pieces twice.
    consumed_from_email = 0
    warned_missing_email_order = 0
    for r in inv_rows:
        mid = int(r["knife_model_id"])
        mkc_order_number = _row_get(r, "mkc_order_number")
        if _is_blank(mkc_order_number):
            continue
        on = str(mkc_order_number).strip()

        handle_key = _compute_name_handle_bucket_key(mid, _row_get(r, "handle_color"), singleton_sku_ids=singleton_sku_ids)
        if handle_key not in remaining or on not in remaining[handle_key]:
            warned_missing_email_order += 1
            continue
        qty = int(_row_get(r, "quantity") or 1)
        remaining[handle_key][on]["qty"] -= qty
        consumed_from_email += qty

    # 2) Update rows that have mkc_order_number but missing acquired_date.
    # 3) Allocate + split rows missing mkc_order_number.
    to_update_in_place: list[tuple[int, dict[str, Any]]] = []
    to_split: list[sqlite3.Row] = []

    for r in inv_rows:
        mid = int(r["knife_model_id"])
        qty = int(_row_get(r, "quantity") or 1)
        mkc_order_number = _row_get(r, "mkc_order_number")
        acquired_date = _row_get(r, "acquired_date")

        needs_order_alloc = _is_blank(mkc_order_number)
        needs_date_only = (not _is_blank(mkc_order_number)) and _is_blank(acquired_date)

        if needs_order_alloc:
            if qty <= 0:
                continue
            to_split.append(r)
        elif needs_date_only:
            on = str(mkc_order_number).strip()
            key = _compute_name_handle_bucket_key(mid, _row_get(r, "handle_color"), singleton_sku_ids=singleton_sku_ids)
            if key in remaining and on in remaining[key]:
                od = remaining[key][on].get("order_date") or ""
                if od:
                    to_update_in_place.append((int(r["id"]), {"acquired_date": od, "mkc_order_number": on}))
                else:
                    # order_date missing in email; can't fill.
                    pass

    # Allocate missing order numbers bucket-by-bucket in deterministic inventory order (id asc).
    # We'll keep a sorted list of remaining order_numbers for each email bucket key.
    remaining_order_lists: dict[tuple[int, str], list[str]] = {}
    for key in remaining.keys():
        order_nums = [on for on, v in remaining[key].items() if int(v.get("qty") or 0) > 0]
        order_nums.sort(key=lambda s: (_parse_int_maybe(s) or 10**18, s))
        remaining_order_lists[key] = order_nums

    # Consume pointers per bucket.
    pointer_idx: dict[tuple[int, str], int] = {k: 0 for k in remaining_order_lists.keys()}

    # Prepare DB operations.
    inserts: list[dict[str, Any]] = []
    updates: list[tuple[int, dict[str, Any]]] = [(rid, patch) for rid, patch in to_update_in_place]

    def _alloc_for_row(r: sqlite3.Row) -> None:
        nonlocal inserts, updates
        rid = int(r["id"])
        mid = int(r["knife_model_id"])
        handle_color = _row_get(r, "handle_color")
        key = _compute_name_handle_bucket_key(mid, handle_color, singleton_sku_ids=singleton_sku_ids)

        if key not in remaining or not remaining[key]:
            raise RuntimeError(f"No remaining email orders for inventory row id {rid} bucket={key}")

        order_nums = remaining_order_lists.get(key) or []
        if not order_nums:
            raise RuntimeError(f"No remaining email order numbers left for bucket={key} (row id {rid})")

        remaining_qty = int(_row_get(r, "quantity") or 1)
        portions: list[tuple[str, str, int]] = []  # (order_number, order_date, qty)

        idx = pointer_idx.get(key, 0)
        while remaining_qty > 0:
            # Advance until we find an order with qty > 0.
            while idx < len(order_nums) and int(remaining[key][order_nums[idx]].get("qty") or 0) <= 0:
                idx += 1
            if idx >= len(order_nums):
                raise RuntimeError(f"Ran out of email quantity while allocating row id={rid} bucket={key}")

            on = order_nums[idx]
            avail = int(remaining[key][on].get("qty") or 0)
            take = min(remaining_qty, avail)
            od = str(remaining[key][on].get("order_date") or "").strip()
            if not od:
                raise RuntimeError(f"Missing order_date for order_number={on} bucket={key}")

            portions.append((on, od, take))
            remaining[key][on]["qty"] -= take
            remaining_qty -= take

        pointer_idx[key] = idx

        # Apply portions: update the existing row for the first portion; insert rows for the rest.
        if not portions:
            return

        first_on, first_od, first_take = portions[0]
        # Update existing row.
        updates.append(
            (
                rid,
                {
                    "mkc_order_number": first_on,
                    "acquired_date": first_od,
                    "quantity": first_take,
                },
            )
        )

        if len(portions) == 1 and first_take == int(_row_get(r, "quantity") or 1):
            return

        # Insert additional split rows.
        # Copy all columns from the original row, except:
        #   - id is auto
        #   - legacy_inventory_id must be cleared (unique)
        #   - quantity/acquired_date/mkc_order_number updated per portion
        for (on, od, take) in portions[1:]:
            new_row = {c: _row_get(r, c) for c in inv_cols if c != "id"}
            # Avoid unique collision if legacy_inventory_id is present.
            if "legacy_inventory_id" in new_row:
                new_row["legacy_inventory_id"] = None
            new_row["quantity"] = take
            new_row["mkc_order_number"] = on
            new_row["acquired_date"] = od
            # For split rows, keep schema defaults for timestamps by omitting
            # created_at/updated_at from INSERT (see insert logic below).
            inserts.append(new_row)

    # Process inventory rows missing order numbers in stable order.
    to_split_sorted = sorted(to_split, key=lambda rr: int(rr["id"]))
    for r in to_split_sorted:
        _alloc_for_row(r)

    # Safety checks before applying.
    remaining_left = 0
    for key, bucket_orders in remaining.items():
        for on, v in bucket_orders.items():
            left = int(v.get("qty") or 0)
            if left > 0:
                remaining_left += left
    # total_missing_rows (informational only) intentionally omitted in favor of real counts above.

    print("=== Allocation plan ===")
    print(f"VIP models skipped: {len(vip_ids)}")
    print(f"Singleton SKU models (handle ignored for matching): {len(singleton_sku_ids)}")
    print(f"Email bucket keys: {len(email_queue)}")
    print(f"Inventory rows considered (non-VIP): {len(inv_rows)}")
    print(f"Inventory rows to allocate/split (mkc_order_number missing): {len(to_split)}")
    print(f"Inventory rows to date-fill only: {len(to_update_in_place)}")
    print(f"Already-assigned inventory consumed email qty: {consumed_from_email}")
    if warned_missing_email_order:
        print(f"WARNING: {warned_missing_email_order} inventory rows had mkc_order_number set but no matching email order in computed bucket.")
    print(f"Email quantity remaining after allocation: {remaining_left}")
    print(f"Updates: {len(updates)} inserts: {len(inserts)} (splits)")

    if not args.apply:
        print("Dry-run only. Re-run with --apply to write DB changes.")
        conn.close()
        return 0

    # Write to DB.
    # We update only the few fields we touch.
    with conn:
        for rid, patch in updates:
            cols = list(patch.keys())
            vals = [patch[c] for c in cols]
            set_expr = ", ".join([f"{c} = ?" for c in cols])
            conn.execute(
                f"UPDATE inventory_items_v2 SET {set_expr}, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (*vals, rid),
            )

        if inserts:
            # Insert rows in batches (each split portion becomes a row).
            for new_row in inserts:
                # Build insert columns deterministically.
                # Omit created_at/updated_at so we rely on schema defaults.
                cols = [c for c in inv_cols if c not in {"id", "created_at", "updated_at"} and c in new_row]
                vals = [new_row[c] for c in cols]
                placeholders = ", ".join(["?"] * len(cols))
                col_list = ", ".join(cols)
                conn.execute(
                    f"INSERT INTO inventory_items_v2 ({col_list}) VALUES ({placeholders})",
                    vals,
                )

    conn.close()
    print("=== DB updated ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

