#!/usr/bin/env python3
"""
Extract line items from Montana Knife Company Shopify order-confirmation .eml files.

Produces a CSV with order metadata, product title as printed in the email,
quantity, line total, and computed unit price.

Usage:
  python tools/extract_mkc_order_emails.py /path/to/eml/dir -o data/mkc_email_orders.csv

Notes:
  - "VIP - ..." in a title denotes early-access ordering, not necessarily the VIP product series.
  - Color / variant text stays in the title for a later normalization pass.
"""

from __future__ import annotations

import argparse
import csv
import email.utils
import re
import sys
from email import policy
from email.parser import BytesParser
from pathlib import Path
from typing import Any

# Full dollar amount including cents (commas only in integer part).
PRICE_RE = re.compile(r"^\s*\$(?P<num>[\d,]+\.\d{2})\s*$")
FREE_RE = re.compile(r"^\s*Free\s*$", re.IGNORECASE)
# Line-final quantity marker (Shopify uses ×); wrapped titles may end with "FINISH × 1".
QTY_TAIL_RE = re.compile(r"(?P<before>.*?)\s*[×x]\s*(?P<qty>\d+)\s*$", re.IGNORECASE)


def _get_plain_text(msg: Any) -> str | None:
    for part in msg.walk():
        if part.get_content_type() == "text/plain":
            return part.get_content()
    return None


def _order_number_from_message(msg: Any, body: str) -> str | None:
    subj = msg.get("Subject") or ""
    m = re.search(r"Order\s*#\s*(\d+)", subj, re.I)
    if m:
        return m.group(1)
    m = re.search(r"Order\s*#\s*(\d+)", body)
    if m:
        return m.group(1)
    return None


def _parse_order_date(msg: Any) -> tuple[str | None, str | None]:
    """Return (iso_date, raw_header) or (None, raw)."""
    raw = msg.get("Date")
    if not raw:
        return None, None
    try:
        dt = email.utils.parsedate_to_datetime(raw)
        if dt:
            return dt.date().isoformat(), raw
    except (TypeError, ValueError, OverflowError):
        pass
    return None, raw


def _extract_summary_block(plain: str) -> str | None:
    if "Order summary" not in plain:
        return None
    after = plain.split("Order summary", 1)[1]
    if "Subtotal" not in after:
        return None
    return after.split("Subtotal", 1)[0].strip()


def _parse_line_items(summary_block: str) -> list[dict[str, Any]]:
    """
    After 'Order summary', Shopify plain text can look like:

      PRODUCT × QTY

      $123.45

    or a wrapped title:

      LINE1 OF TITLE
      LAST LINE × QTY

      DESCRIPTOR OR COLOR

      $123.45

    or a color/variant line between title and price:

      HUK - MODEL × 1

      Olive

      $300.00
    """
    lines = [ln.rstrip() for ln in summary_block.splitlines()]
    # Drop leading separator rows (dashes) and blank lines
    while lines and (not lines[0].strip() or set(lines[0].strip()) == {"-"}):
        lines.pop(0)
    items: list[dict[str, Any]] = []
    i = 0
    while i < len(lines):
        while i < len(lines) and not lines[i].strip():
            i += 1
        if i >= len(lines):
            break

        prod_parts: list[str] = []
        qty: int | None = None
        while i < len(lines):
            raw = lines[i].strip()
            i += 1
            qm = QTY_TAIL_RE.search(raw)
            if qm:
                before = (qm.group("before") or "").strip()
                if before:
                    prod_parts.append(before)
                qty = int(qm.group("qty"))
                break
            prod_parts.append(raw)
        else:
            break

        if qty is None:
            continue

        name_core = " ".join(p for p in prod_parts if p).strip()

        descriptors: list[str] = []
        price_line: str | None = None
        line_total: float | None = None
        while i < len(lines):
            raw = lines[i].strip()
            i += 1
            if not raw:
                continue
            if PRICE_RE.match(raw):
                price_line = raw
                pr = PRICE_RE.match(raw)
                assert pr is not None
                num = pr.group("num").replace(",", "")
                line_total = float(num)
                break
            if FREE_RE.match(raw):
                price_line = raw
                line_total = 0.0
                break
            descriptors.append(raw)

        if price_line is None or line_total is None:
            items.append(
                {
                    "line_title": name_core,
                    "quantity": qty,
                    "line_total": None,
                    "parse_error": "missing_price",
                }
            )
            continue

        if descriptors:
            full_title = f"{name_core} / " + " / ".join(descriptors)
        else:
            full_title = name_core

        items.append(
            {
                "line_title": full_title,
                "quantity": qty,
                "line_total": line_total,
                "parse_error": None,
            }
        )
    return items


def parse_eml(path: Path) -> dict[str, Any]:
    with open(path, "rb") as f:
        msg = BytesParser(policy=policy.default).parse(f)
    plain = _get_plain_text(msg)
    out: dict[str, Any] = {
        "source_file": path.name,
        "order_number": None,
        "order_date": None,
        "order_date_raw": None,
        "items": [],
        "file_error": None,
    }
    if not plain:
        out["file_error"] = "no_text_plain"
        return out
    order_no = _order_number_from_message(msg, plain)
    out["order_number"] = order_no
    d_iso, d_raw = _parse_order_date(msg)
    out["order_date"] = d_iso
    out["order_date_raw"] = d_raw
    block = _extract_summary_block(plain)
    if not block:
        out["file_error"] = "no_order_summary_block"
        return out
    out["items"] = _parse_line_items(block)
    if not out["items"]:
        out["file_error"] = "no_line_items_parsed"
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Extract MKC order lines from .eml files.")
    ap.add_argument(
        "eml_dir",
        type=Path,
        help="Directory containing Order #*.eml files",
    )
    ap.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("data/mkc_email_orders.csv"),
        help="Output CSV path",
    )
    args = ap.parse_args()
    eml_dir = args.eml_dir.expanduser().resolve()
    if not eml_dir.is_dir():
        print(f"Not a directory: {eml_dir}", file=sys.stderr)
        return 2

    rows: list[dict[str, Any]] = []
    for eml_path in sorted(eml_dir.glob("*.eml")):
        parsed = parse_eml(eml_path)
        order_no = parsed["order_number"] or ""
        order_date = parsed["order_date"] or ""
        base = {
            "order_number": order_no,
            "order_date": order_date,
            "source_file": parsed["source_file"],
            "file_error": parsed["file_error"] or "",
        }
        if parsed["file_error"] and not parsed["items"]:
            rows.append(
                {
                    **base,
                    "line_title": "",
                    "quantity": "",
                    "line_total_usd": "",
                    "unit_price_usd": "",
                    "vip_benefit_style_title": "",
                    "line_parse_error": parsed["file_error"],
                }
            )
            continue
        for it in parsed["items"]:
            title = it["line_title"]
            vip = "Y" if re.match(r"^\s*VIP\s*-\s*", title, re.I) else "N"
            qty = it["quantity"]
            total = it["line_total"]
            unit = ""
            if total is not None and qty:
                unit = f"{(total / qty):.2f}"
            lt = it["line_total"]
            rows.append(
                {
                    **base,
                    "line_title": title,
                    "quantity": qty,
                    "line_total_usd": f"{lt:.2f}" if lt is not None else "",
                    "unit_price_usd": unit,
                    "vip_benefit_style_title": vip,
                    "line_parse_error": it.get("parse_error") or "",
                }
            )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "order_number",
        "order_date",
        "line_title",
        "quantity",
        "unit_price_usd",
        "line_total_usd",
        "vip_benefit_style_title",
        "source_file",
        "line_parse_error",
        "file_error",
    ]
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"Wrote {len(rows)} row(s) to {args.output.resolve()}")
    err_rows = sum(1 for r in rows if r.get("line_parse_error") or r.get("file_error"))
    if err_rows:
        print(f"Rows with errors flagged: {err_rows}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
