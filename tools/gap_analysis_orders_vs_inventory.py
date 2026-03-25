#!/usr/bin/env python3
"""
CLI wrapper for gap_analysis_core.compute_gap_analysis.

Default full mode matches blade only for Blood Brothers + tactical catalog models
(is_tactical); other knives use model + handle + blade '*' (emails omit blade).

Outputs (default mode):
  data/gap_orders_vs_inventory_summary.txt
  data/gap_orders_vs_inventory_buckets.csv

Outputs (--name-handle):
  data/gap_orders_vs_inventory_name_handle_summary.txt
  data/gap_orders_vs_inventory_name_handle_buckets.csv

Usage:
  python tools/gap_analysis_orders_vs_inventory.py
  python tools/gap_analysis_orders_vs_inventory.py --name-handle
  python tools/gap_analysis_orders_vs_inventory.py --strict
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Repo root (parent of tools/)
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from gap_analysis_core import compute_gap_analysis, write_gap_outputs


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--orders", type=Path, default=ROOT / "data/mkc_email_orders_knives.csv")
    ap.add_argument("--db", type=Path, default=ROOT / "data/mkc_inventory.db")
    ap.add_argument("--summary", type=Path, default=ROOT / "data/gap_orders_vs_inventory_summary.txt")
    ap.add_argument("--csv-out", type=Path, default=ROOT / "data/gap_orders_vs_inventory_buckets.csv")
    ap.add_argument(
        "--name-handle",
        action="store_true",
        help="Match only catalog model + handle color (ignore blade entirely).",
    )
    ap.add_argument(
        "--strict",
        action="store_true",
        help="Full mode only: exact blade bucketing (no defaults wildcard); BB still uses handle *.",
    )
    args = ap.parse_args()

    if args.name_handle:
        summary_path = ROOT / "data/gap_orders_vs_inventory_name_handle_summary.txt"
        csv_path = ROOT / "data/gap_orders_vs_inventory_name_handle_buckets.csv"
    else:
        summary_path = args.summary
        csv_path = args.csv_out

    try:
        result = compute_gap_analysis(
            args.orders,
            args.db,
            name_handle=args.name_handle,
            strict=args.strict,
        )
    except FileNotFoundError as e:
        print(e, file=sys.stderr)
        return 2

    write_gap_outputs(result, summary_path=summary_path, csv_path=csv_path)
    print(f"Wrote {summary_path} and {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
