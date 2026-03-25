#!/usr/bin/env python3
"""
Map order CSV handle_color / blade_color strings to v2_option_values (handle-colors,
blade-colors). Does not INSERT missing options; flags rows and writes a deduped report.

Adds columns:
  handle_color_v2, blade_color_v2
  handle_v2_resolution, blade_v2_resolution
    matched_exact | matched_alias | matched_normalization | matched_fuzzy | needs_add | blank
  handle_v2_add_candidate, blade_v2_add_candidate  (suggested name when needs_add)

Usage:
  python tools/normalize_order_colors_to_v2.py \\
    -i data/mkc_email_orders_knives.csv \\
    --db data/mkc_inventory.db \\
    --missing-report data/v2_color_options_missing_from_orders.txt
"""

from __future__ import annotations

import argparse
import csv
import re
import sqlite3
import sys
from difflib import SequenceMatcher
from pathlib import Path


def _norm_key(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = s.replace(" and ", " & ")
    return s


def _flip_ampersand_to_slash(s: str) -> str:
    return re.sub(r"\s*&\s*", "/", s.strip())


def _load_options(conn: sqlite3.Connection, option_type: str) -> list[str]:
    cur = conn.execute(
        "SELECT name FROM v2_option_values WHERE option_type = ? ORDER BY name COLLATE NOCASE",
        (option_type,),
    )
    return [r[0] for r in cur.fetchall()]


def _build_lookup(options: list[str]) -> dict[str, str]:
    """lower(name) -> exact canonical name as stored in v2."""
    return {n.lower(): n for n in options}


def _fuzzy_best(s: str, options: list[str], min_ratio: float) -> tuple[str | None, float]:
    best_name: str | None = None
    best_r = 0.0
    nk = _norm_key(s)
    for opt in options:
        r = SequenceMatcher(None, nk, _norm_key(opt)).ratio()
        if r > best_r:
            best_r = r
            best_name = opt
    if best_r >= min_ratio and best_name:
        return best_name, best_r
    return None, best_r


def _resolve_color(
    raw: str,
    option_type: str,
    lookup: dict[str, str],
    options: list[str],
    aliases: dict[str, str],
    fuzzy_min: float,
) -> tuple[str, str, str]:
    """
    Returns (v2_name, resolution, add_candidate).
    v2_name empty when needs_add or blank input.
    add_candidate is non-empty when resolution == needs_add.
    """
    s = (raw or "").strip()
    if not s:
        return "", "blank", ""

    key = _norm_key(s)
    if key in lookup:
        return lookup[key], "matched_exact", ""

    # Explicit alias (normalized key -> canonical v2 name)
    if key in aliases:
        canon = aliases[key]
        if canon.lower() in lookup:
            return lookup[canon.lower()], "matched_alias", ""

    # Shop uses "Orange & Black" as one handle; v2 may use "Orange/Black"
    flipped = _flip_ampersand_to_slash(s)
    fk = _norm_key(flipped)
    if fk in lookup:
        return lookup[fk], "matched_normalization", ""

    # Try fuzzy (strict — short option lists, avoid bad merges)
    hit, _ratio = _fuzzy_best(s, options, fuzzy_min)
    if hit:
        return hit, "matched_fuzzy", ""

    return "", "needs_add", s


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "-i",
        "--input",
        dest="input",
        type=Path,
        default=Path("data/mkc_email_orders_knives.csv"),
    )
    ap.add_argument("-o", "--output", type=Path, default=None)
    ap.add_argument("--db", type=Path, default=Path("data/mkc_inventory.db"))
    ap.add_argument(
        "--missing-report",
        type=Path,
        default=Path("data/v2_color_options_missing_from_orders.txt"),
    )
    ap.add_argument("--fuzzy-min", type=float, default=0.92)
    args = ap.parse_args()
    outp = args.output or args.input

    if not args.input.is_file():
        print(f"Missing CSV: {args.input}", file=sys.stderr)
        return 2
    if not args.db.is_file():
        print(f"Missing DB: {args.db}", file=sys.stderr)
        return 2

    conn = sqlite3.connect(str(args.db))
    handle_opts = _load_options(conn, "handle-colors")
    blade_opts = _load_options(conn, "blade-colors")
    conn.close()

    h_lookup = _build_lookup(handle_opts)
    b_lookup = _build_lookup(blade_opts)

    # Normalized-key aliases where email wording != v2 spelling (values must exist in v2)
    handle_aliases = {
        _norm_key("Orange & Black"): "Orange/Black",
        _norm_key("Tan & Black"): "Tan/Black",
    }
    handle_aliases = {k: v for k, v in handle_aliases.items() if v.lower() in h_lookup}

    with open(args.input, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        print("No rows", file=sys.stderr)
        return 1

    base = list(rows[0].keys())
    new_cols = [
        "handle_color_v2",
        "blade_color_v2",
        "handle_v2_resolution",
        "blade_v2_resolution",
        "handle_v2_add_candidate",
        "blade_v2_add_candidate",
    ]
    out_fields = base + [c for c in new_cols if c not in base]

    missing_handles: set[tuple[str, str]] = set()
    missing_blades: set[tuple[str, str]] = set()

    for row in rows:
        hv, hres, hadd = _resolve_color(
            row.get("handle_color") or "",
            "handle-colors",
            h_lookup,
            handle_opts,
            handle_aliases,
            args.fuzzy_min,
        )
        bv, bres, badd = _resolve_color(
            row.get("blade_color") or "",
            "blade-colors",
            b_lookup,
            blade_opts,
            {},
            args.fuzzy_min,
        )
        row["handle_color_v2"] = hv
        row["blade_color_v2"] = bv
        row["handle_v2_resolution"] = hres
        row["blade_v2_resolution"] = bres
        row["handle_v2_add_candidate"] = hadd
        row["blade_v2_add_candidate"] = badd

        if hres == "needs_add" and hadd:
            missing_handles.add((hadd, row.get("line_title", "")[:80]))
        if bres == "needs_add" and badd:
            missing_blades.add((badd, row.get("line_title", "")[:80]))

    outp.parent.mkdir(parents=True, exist_ok=True)
    with open(outp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=out_fields, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)

    # Sidecar report (deduped by suggested option name)
    if args.missing_report:
        args.missing_report.parent.mkdir(parents=True, exist_ok=True)
        lines = [
            "# v2 option values to add (from order email color pass)",
            "# Format: option_type<TAB>suggested_name<TAB>example_line_title",
            "",
        ]
        for opt_type, bag in (
            ("handle-colors", missing_handles),
            ("blade-colors", missing_blades),
        ):
            for name, example in sorted(bag, key=lambda x: x[0].lower()):
                lines.append(f"{opt_type}\t{name}\t{example}")
        if len(lines) <= 3:
            lines.append("(none)")
        args.missing_report.write_text("\n".join(lines) + "\n", encoding="utf-8")

    nh = sum(1 for r in rows if r["handle_v2_resolution"] == "needs_add")
    nb = sum(1 for r in rows if r["blade_v2_resolution"] == "needs_add")
    print(f"Wrote {len(rows)} row(s) -> {outp.resolve()}")
    print(f"Rows with handle needs_add: {nh}, blade needs_add: {nb}")
    if args.missing_report:
        print(f"Missing report -> {args.missing_report.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
