#!/usr/bin/env python3
"""
Filter mkc_email_orders.csv to rows that correspond to catalog knife models.

Uses knife_models_v2 (official_name + normalized_name) with:
  - accessory / merch deny patterns (avoid 'Jackstone' matching a kydex sheath line)
  - normalized substring and difflib scores, plus VIP-/variant stripping

Writes: data/mkc_email_orders_knives.csv by default.
Drops excluded rows to: data/mkc_email_orders_excluded.csv (with reasons).

Usage:
  python tools/filter_email_orders_to_catalog_knives.py \\
    --csv data/mkc_email_orders.csv \\
    --db data/mkc_inventory.db \\
    --min-score 0.62

The MKC Culinary set (email product name) is expanded into three rows — one per catalog model:
Bighorn Chef, Smith River Santoku, Little Bighorn Petty (same order # / date; prices split by 3).
"""

from __future__ import annotations

import argparse
import csv
import re
import sqlite3
import sys
from difflib import SequenceMatcher
from pathlib import Path

# Email title "MKC CULINARY SET" — interim label before splitting into component models.
CULINARY_SET_CATALOG_NOTE = (
    "MKC Culinary set (bundle: Bighorn Chef, Smith River Santoku, Little Bighorn Petty)"
)
CULINARY_SET_COMPONENT_NAMES = [
    "Bighorn Chef",
    "Smith River Santoku",
    "Little Bighorn Petty",
]


def _is_culinary_set_row(row: dict) -> bool:
    cat = (row.get("matched_catalog_name") or "").strip()
    return cat == CULINARY_SET_CATALOG_NOTE or cat.startswith("MKC Culinary set")


def _split_price_thirds(val: str | None) -> str:
    try:
        x = float(str(val).replace(",", "").strip() or 0)
        return f"{x / 3.0:.2f}"
    except (TypeError, ValueError):
        return (val or "").strip()


def expand_culinary_set_rows(kept: list[dict]) -> list[dict]:
    """One CSV line per set -> three lines (one per knife model).

    line_title stays the original email line (e.g. MKC CULINARY SET - BLACK) so
    handle/blade color enrichment does not pick up model names from a synthetic suffix.
    """
    out: list[dict] = []
    for r in kept:
        if not _is_culinary_set_row(r):
            out.append(r)
            continue
        pq = int(r.get("quantity") or 1)
        lt = _split_price_thirds(r.get("line_total_usd"))
        up = _split_price_thirds(r.get("unit_price_usd"))
        for comp in CULINARY_SET_COMPONENT_NAMES:
            nr = dict(r)
            nr["matched_catalog_name"] = comp
            nr["match_method"] = "culinary_set_split"
            nr["match_score"] = r.get("match_score", "")
            nr["quantity"] = str(pq)
            nr["line_total_usd"] = lt
            nr["unit_price_usd"] = up
            out.append(nr)
    return out

# Lines matching these (case-insensitive) are not knives / not catalog-mappable as a blade.
ACCESSORY_RES = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"\bADDITIONAL\s+KYDEX\b",
        r"\bKYDEX\s+SHEATH\b",
        r"\bLEATHER\s+SHEATH\b",
        r"\bVERTICAL\s+LEATHER\b",
        r"\bHORIZONTAL\s+LEATHER\b",
        r"\bMINI-SPEEDGOAT\s+LEATHER\s+SHEATH\b",
        r"\bSTOCKYARD\s+LEATHER\s+SHEATH\b",
        r"\bJACKSTONE\s+LEATHER\s+SHEATH\b",
        r"CULINARY\s+KNIFE\s+STAND",
        r"CULINARY\s+KNIFE\s+HANG",
        r"\bKNIFE\s+STAND\b",
        r"\bSTAND\s+ONLY\b",
        r"\bKNIFE\s+HANG\s+ONLY\b",
        r"WORK\s+SHARP",
        r"HONING\s+ROD",
        r"\bPATCH\b",
        r"\bHAT\b",
        r"\bHOODIE\b",
        r"DIECUT",
        r"\bDECAL\b",
        r"\bBUNDLE\b",
        r"EDC\s+PEN",
        r"BLADE\s+WAX",
        r"ZIPPO",
        r"LIGHTER",
        r"CANVAS\s+KNIFE\s+ROLL\b",
        r"POCKET\s+CLIP",
        r"DISCREET\s+CARRY",
        r"MOLLE",
        r"FIELD\s+KNIFE\s+SHARPENER",
        r"\bMED\s+KIT\b",
        r"VIP\s+PROGRAM\s+PACK",
        r"VIP\s+LEVEL",
        r"BLACK\s+RIFLE\s+COFFEE",
        r"SIGNATURE\s+DARK\s+ROAST",
        r"HORL",
        r"WHETSTONE",
        r"MOLLE-LOK",
        r"\bATTACHMENT\b",
    ]
]


def normalize_key(s: str) -> str:
    s = s.lower().strip()
    s = s.replace("®", " ").replace("™", " ")
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def order_match_variants(line: str) -> list[str]:
    """Candidate strings to compare against catalog names."""
    s = line.strip()
    out: list[str] = [s]
    if " / " in s:
        out.append(s.split(" / ", 1)[0].strip())
    # HUK prefix normalization:
    # Inventory/catalog models often store "Model - HUK" (token order: model then HUK),
    # but the email line is "HUK - Model / Color". Add a variant that moves the HUK
    # token to the end and also drops the color suffix, so substring matching can
    # select the correct "- HUK" model.
    if re.match(r"^\s*HUK\s*-\s*", s, re.I):
        rest = re.sub(r"^\s*HUK\s*-\s*", "", s, flags=re.I).strip()
        out.append(f"{rest} HUK")
        if " / " in rest:
            out.append(f"{rest.split(' / ', 1)[0].strip()} HUK")
    # Shopify VIP early-access prefix (not the VIP product series)
    if re.match(r"^\s*VIP\s*-\s*", s, re.I):
        rest = re.sub(r"^\s*VIP\s*-\s*", "", s, flags=re.I).strip()
        out.append(rest)
        if " / " in rest:
            out.append(rest.split(" / ", 1)[0].strip())
    # First segment before ' - ' often drops colorway (not always)
    if " - " in s:
        out.append(s.split(" - ", 1)[0].strip())
    # Dedupe preserving order
    seen: set[str] = set()
    uniq: list[str] = []
    for x in out:
        k = normalize_key(x)
        if k and k not in seen:
            seen.add(k)
            uniq.append(x)
    return uniq


def is_accessory_line(line: str) -> bool:
    return any(rx.search(line) for rx in ACCESSORY_RES)


def load_catalog(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """(label, normalized_key) for each distinct catalog string to match."""
    rows = conn.execute(
        "SELECT official_name, normalized_name FROM knife_models_v2 "
        "WHERE trim(COALESCE(official_name,'')) != ''"
    ).fetchall()
    seen: set[str] = set()
    out: list[tuple[str, str]] = []
    for off, norm in rows:
        o = (off or "").strip()
        if o.upper() == "REDACTED":
            continue
        if o:
            nk = normalize_key(o)
            if nk and nk not in seen:
                seen.add(nk)
                out.append((o, nk))
        n = (norm or "").strip()
        if n:
            nk = normalize_key(n)
            if nk and nk not in seen:
                seen.add(nk)
                out.append((n, nk))
    return out


def _label_for_normalized_key(catalog: list[tuple[str, str]], want_key: str) -> str | None:
    for label, ck in catalog:
        if ck == want_key:
            return label
    return None


def score_line_against_catalog(
    line: str, catalog: list[tuple[str, str]], accessory: bool
) -> tuple[float, str | None, str]:
    """
    Return (best_score, matched_catalog_label, method).
    """
    best = 0.0
    best_label: str | None = None
    method = "none"

    vn_full = normalize_key(line)

    # "MKC CULINARY SET" in Shopify — three-knife bundle (see CULINARY_SET_CATALOG_NOTE).
    if "culinary set" in vn_full and "knife stand" not in vn_full and "knife hang" not in vn_full:
        best, best_label, method = 0.94, CULINARY_SET_CATALOG_NOTE, "alias"

    # Aggregate substring hits across all title variants so a shortened variant (e.g. "Jackstone")
    # cannot out-score a longer catalog name ("Jackstone Snyder Edition") matched on the full line.
    all_substr: list[tuple[int, float, str]] = []
    for variant in order_match_variants(line):
        vn = normalize_key(variant)
        if not vn:
            continue
        for label, ck in catalog:
            if len(ck) < 3:
                continue
            if ck in vn:
                if accessory:
                    continue
                score = 0.88 + min(0.12, len(ck) / max(len(vn), 1) * 0.12)
                all_substr.append((len(ck), score, label))
    if all_substr:
        all_substr.sort(key=lambda t: (-t[0], -t[1]))
        _ln, sc, lab = all_substr[0]
        if sc > best:
            best, best_label, method = sc, lab, "substring"

    for variant in order_match_variants(line):
        vn = normalize_key(variant)
        if not vn:
            continue
        for label, ck in catalog:
            if len(ck) < 3:
                continue
            if ck in vn:
                continue
            r1 = SequenceMatcher(None, vn, ck).ratio()
            vn_stem = vn.split(" - ")[0].strip() if " - " in vn else vn
            r2 = SequenceMatcher(None, vn_stem, ck).ratio() if vn_stem != vn else r1
            r = max(r1, r2)
            if r > best:
                best = r
                best_label = label
                method = "fuzzy"

    # SERE 25 vs email "SERE25"
    if "sere25" in vn_full.replace(" ", "") or re.search(r"\bSERE\s*25\b", line, re.I):
        lbl = _label_for_normalized_key(catalog, normalize_key("SERE 25"))
        if lbl and 0.92 > best:
            best, best_label, method = 0.92, lbl, "alias"

    # mikeroweWORKS collab — must win over plain "Blackfoot 2.0" substring.
    if "mikerowe" in vn_full or "mike rowe" in vn_full:
        lbl = _label_for_normalized_key(catalog, normalize_key("Blackfoot 2.0 Mike Rowe Works"))
        if lbl and "blackfoot" in vn_full:
            best, best_label, method = 0.97, lbl, "alias"

    return best, best_label, method


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", type=Path, default=Path("data/mkc_email_orders.csv"))
    ap.add_argument("--db", type=Path, default=Path("data/mkc_inventory.db"))
    ap.add_argument("--out", type=Path, default=Path("data/mkc_email_orders_knives.csv"))
    ap.add_argument("--excluded", type=Path, default=Path("data/mkc_email_orders_excluded.csv"))
    ap.add_argument("--min-score", type=float, default=0.62)
    args = ap.parse_args()

    if not args.csv.is_file():
        print(f"Missing CSV: {args.csv}", file=sys.stderr)
        return 2
    if not args.db.is_file():
        print(f"Missing DB: {args.db}", file=sys.stderr)
        return 2

    conn = sqlite3.connect(str(args.db))
    catalog = load_catalog(conn)
    conn.close()

    with open(args.csv, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    fieldnames = list(rows[0].keys()) if rows else []
    extra = ["matched_catalog_name", "match_score", "match_method", "exclude_reason"]
    out_fields = fieldnames + [x for x in extra if x not in fieldnames]

    kept: list[dict] = []
    dropped: list[dict] = []

    for row in rows:
        title = (row.get("line_title") or "").strip()
        acc = is_accessory_line(title)
        score, label, method = score_line_against_catalog(title, catalog, accessory=acc)

        reason = ""
        if acc:
            reason = "accessory_or_merch_pattern"
        elif score < args.min_score:
            reason = f"below_min_score({score:.3f}<{args.min_score})"
        else:
            row.update(
                {
                    "matched_catalog_name": label or "",
                    "match_score": f"{score:.4f}",
                    "match_method": method,
                    "exclude_reason": "",
                }
            )
            kept.append(row)
            continue

        row.update(
            {
                "matched_catalog_name": label or "",
                "match_score": f"{score:.4f}",
                "match_method": method,
                "exclude_reason": reason,
            }
        )
        dropped.append(row)

    kept_expanded = expand_culinary_set_rows(kept)
    cul_lines = sum(1 for r in kept if _is_culinary_set_row(r))

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=out_fields, extrasaction="ignore")
        w.writeheader()
        for r in kept_expanded:
            w.writerow(r)

    with open(args.excluded, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=out_fields, extrasaction="ignore")
        w.writeheader()
        for r in dropped:
            w.writerow(r)

    print(f"Catalog entries used for matching: {len(catalog)}")
    if cul_lines:
        print(f"Expanded {cul_lines} culinary set line(s) into {cul_lines * 3} model rows.")
    print(f"Kept {len(kept_expanded)} knife row(s) -> {args.out.resolve()}")
    print(f"Excluded {len(dropped)} row(s) -> {args.excluded.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
