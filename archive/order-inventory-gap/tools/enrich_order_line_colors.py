#!/usr/bin/env python3
"""
Derive handle_color and blade_color from Shopify-style order line_title values.

Rules (per project conventions):
  - "Orange & Black" style (ampersand or word AND between colors) = one handle color choice.
  - A single "/" between exactly two color tokens (no "&" on that side) = two-tone split.
    For MKC tactical models (Battle Goat, Wargoat family, TF24, V24, SERE 25, etc.),
    the listing matches the website: first token = blade color, second = handle color.
    For other models (e.g. kitchen), the legacy order is kept: first = handle, second = blade.
  - Blood Brothers BLACK/RED (BLK/RED): Cerakote red blade, black/red paracord handle — we set
    blade Red, handle \"Black/Red\" (listing text is not handle-first like tactical).

Reads a knives CSV (e.g. data/mkc_email_orders_knives.csv) and writes the same path or -o,
adding columns: handle_color, blade_color, color_raw, color_parse_note.

Usage:
  python tools/enrich_order_line_colors.py -i data/mkc_email_orders_knives.csv
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path


def _collapse_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def _title_case_color(s: str) -> str:
    """Title-case tokens but preserve &, /, AND common acronyms."""
    s = _collapse_spaces(s)
    if not s:
        return s
    su = s.upper()
    if su == "OD" or su == "OD GREEN":
        return "OD Green"
    parts = re.split(r"(\s*&\s*|\s*/\s*)", s, flags=re.IGNORECASE)
    out: list[str] = []
    for p in parts:
        if re.fullmatch(r"\s*&\s*", p, flags=re.I):
            out.append(" & ")
        elif re.fullmatch(r"\s*/\s*", p):
            out.append("/")
        else:
            words = p.split()
            fixed: list[str] = []
            i = 0
            while i < len(words):
                w = words[i]
                wu = w.upper()
                if wu == "OD" and i + 1 < len(words) and words[i + 1].upper() == "GREEN":
                    fixed.append("OD Green")
                    i += 2
                    continue
                if wu in {"OD", "PVD", "USA", "MKC", "COY", "BLK"}:
                    if wu == "COY":
                        fixed.append("Coyote")
                    elif wu == "BLK":
                        fixed.append("Black")
                    elif wu == "OD":
                        fixed.append("OD Green")
                    else:
                        fixed.append(wu)
                    i += 1
                else:
                    fixed.append(w.capitalize() if w else w)
                    i += 1
            out.append(" ".join(fixed))
    return "".join(out).replace("  ", " ").strip()


def _normalize_ampersand_form(s: str) -> str:
    """ORANGE AND BLACK -> ORANGE & BLACK (single token choice)."""
    s = _collapse_spaces(s)
    s = re.sub(r"\s+AND\s+", " & ", s, flags=re.IGNORECASE)
    return s


def _hyphen_space_around(s: str) -> str:
    """CUTBANK -ORANGE -> CUTBANK - ORANGE"""
    s = re.sub(r"(?<=\S)\s*-\s*(?=\S)", " - ", s)
    s = _collapse_spaces(s)
    return s


def _last_dash_segment(title: str) -> str | None:
    t = _hyphen_space_around(title)
    segs = [x.strip() for x in t.split(" - ") if x.strip()]
    if len(segs) < 2:
        return None
    return segs[-1]


def _slash_region_after_last_slash_delimiter(title: str) -> tuple[str | None, str | None]:
    """
    For '...MODEL / COLOR STUFF' return (prefix_without_color, color_region).
    Uses first ' / ' when multiple (VIP - X / colors).
    """
    if " / " not in title:
        return None, None
    left, right = title.split(" / ", 1)
    return left.strip(), right.strip()


def _last_segment_looks_like_model_name(seg: str) -> bool:
    """
    Last ' - ' chunk is sometimes the product name (e.g. VIP - DAMASCUS BLACKFOOT), not a color.
    """
    u = seg.upper()
    if re.search(
        r"\b(ORANGE|TAN|OLIVE|RED|DESERT|COYOTE|COY|BLK|PINK|BLUE|WHITE|GRAY|GREY)\b",
        u,
    ):
        return False
    if re.search(
        r"\b(BLACKFOOT|DAMASCUS|WARGOAT|V24|TRIUMPH|GREAT FALLS|STOCKYARD|ELKHORN)\b",
        u,
    ):
        return True
    if re.search(r"\bMKC\b", u) and re.search(
        r"\b(WHITETAIL|JACKSTONE|CULINARY|STEAK)\b",
        u,
    ):
        return True
    return False


def _is_blood_brothers(title: str, matched_catalog: str) -> bool:
    t = title.lower()
    if "blood brothers" in t:
        return True
    m = (matched_catalog or "").lower()
    return "blood brothers" in m


def _mkc_tactical_blade_first_colorway(matched_catalog_name: str) -> bool:
    """
    True when a COY/OD-style two-tone string follows MKC tactical listings:
    first color = blade, second = handle (verified for Battle Goat, Wargoat, etc.).
    """
    n = " ".join((matched_catalog_name or "").split()).strip().lower()
    if not n:
        return False
    if "blood brothers" in n:
        return False
    if "battle goat" in n or "wargoat" in n:
        return True
    if "tf24" in n.replace(" ", ""):
        return True
    if re.fullmatch(r"v24(\s+|$)", n) or n == "v24":
        return True
    if n.startswith("v24 "):
        return True
    if "sere" in n and "25" in n:
        return True
    if "tactical speedgoat" in n:
        return True
    return False


def _tactical_slash_tokens(color_blob: str) -> tuple[str | None, str | None] | None:
    """
    Exactly one '/' separating two non-empty sides, no '&' in blob.
    Returns (left, right) title-cased tokens (before /, after /).
    """
    blob = _collapse_spaces(_normalize_ampersand_form(color_blob))
    if "&" in blob:
        return None
    if blob.count("/") != 1:
        return None
    a, b = [x.strip() for x in blob.split("/", 1)]
    if not a or not b:
        return None
    return _title_case_color(a), _title_case_color(b)


def _handle_blade_from_two_tone(
    left: str, right: str, matched_catalog_name: str
) -> tuple[str, str]:
    """Return (handle_color, blade_color) for a two-token slash or space-slash pair."""
    if _mkc_tactical_blade_first_colorway(matched_catalog_name):
        return right, left
    return left, right


def _compound_ampersand_only(blob: str) -> str | None:
    """Returns single handle string if blob uses & (or AND) only, no tactical slash."""
    blob = _normalize_ampersand_form(blob)
    if "/" in blob:
        return None
    if "&" not in blob and not re.search(r"\sAND\s", blob, re.I):
        return None
    return _title_case_color(blob)


def parse_line_colors(line_title: str, matched_catalog_name: str = "") -> dict[str, str]:
    """
    Returns keys: handle_color, blade_color, color_raw, color_parse_note.
    Empty string means unknown / not applicable.
    """
    title_raw = line_title.strip()
    cat = matched_catalog_name or ""
    note = ""

    if not title_raw:
        return {
            "handle_color": "",
            "blade_color": "",
            "color_raw": "",
            "color_parse_note": "empty_title",
        }

    if _is_blood_brothers(title_raw, cat):
        t_bb = _hyphen_space_around(title_raw)
        last_bb = _last_dash_segment(t_bb) or title_raw
        raw_seg = _collapse_spaces(_normalize_ampersand_form(last_bb.strip()))
        blob_u = raw_seg.upper().replace(" ", "")
        # MKC product: red blade, red+black paracord — not tactical A/B = blade/handle order.
        if blob_u in {"BLACK/RED", "BLK/RED"}:
            return {
                # Inventory uses `Black/Red` for Blood Brothers black/red.
                "handle_color": "Black/Red",
                "blade_color": "Red",
                "color_raw": last_bb.strip(),
                "color_parse_note": "blood_brothers_black_red",
            }
        return {
            "handle_color": "Black",
            "blade_color": "Red",
            "color_raw": last_bb.strip(),
            "color_parse_note": "blood_brothers_legacy",
        }

    t = _hyphen_space_around(title_raw)

    handle = ""
    blade = ""
    color_raw = ""

    # Case: 'MODEL / tail' — tail may be 'Orange & Black', 'Olive', 'COY / OD', 'Coyote/OD Green'
    left_main, slash_right = _slash_region_after_last_slash_delimiter(t)
    if slash_right is not None:
        color_raw = slash_right
        sr = _normalize_ampersand_form(slash_right)
        # Embedded slash inside right part only (e.g. Coyote/OD Green)
        if sr.count("/") == 1 and "&" not in sr:
            pair = _tactical_slash_tokens(sr)
            if pair:
                handle, blade = _handle_blade_from_two_tone(pair[0], pair[1], cat)
                return {
                    "handle_color": handle,
                    "blade_color": blade,
                    "color_raw": color_raw,
                    "color_parse_note": "tactical_slash_after_model_slash",
                }
        if "&" in sr or re.search(r"\sAND\s", sr, re.I):
            handle = _title_case_color(sr)
            note = "compound_handle_after_slash"
            return {
                "handle_color": handle,
                "blade_color": "",
                "color_raw": color_raw,
                "color_parse_note": note,
            }
        # Multiple segments 'COY / OD' with space-slash-space
        if " / " in sr:
            parts = [p.strip() for p in sr.split(" / ")]
            if len(parts) == 2:
                left, right = _title_case_color(parts[0]), _title_case_color(parts[1])
                handle, blade = _handle_blade_from_two_tone(left, right, cat)
                return {
                    "handle_color": handle,
                    "blade_color": blade,
                    "color_raw": color_raw,
                    "color_parse_note": "tactical_space_slash_pair",
                }
        # Single token after model slash (e.g. ORANGE for Great Falls)
        handle = _title_case_color(sr)
        return {
            "handle_color": handle,
            "blade_color": "",
            "color_raw": color_raw,
            "color_parse_note": "single_suffix_after_slash",
        }

    # Default: last ' - ' segment is the colorway
    last = _last_dash_segment(t)
    if not last:
        return {
            "handle_color": "",
            "blade_color": "",
            "color_raw": "",
            "color_parse_note": "no_color_segment",
        }

    color_raw = last
    seg = _normalize_ampersand_form(last)

    if _last_segment_looks_like_model_name(seg):
        return {
            "handle_color": "",
            "blade_color": "",
            "color_raw": color_raw,
            "color_parse_note": "model_name_not_colorway",
        }

    # Tactical slash only in dash segment: ORANGE/BLACK, COY/OD, BLK/COY
    if "&" not in seg and seg.count("/") == 1:
        pair = _tactical_slash_tokens(seg)
        if pair:
            handle, blade = _handle_blade_from_two_tone(pair[0], pair[1], cat)
            return {
                "handle_color": handle,
                "blade_color": blade,
                "color_raw": color_raw,
                "color_parse_note": "tactical_slash_in_dash_segment",
            }

    # Compound handle: ORANGE & BLACK, OLIVE & TAN, TAN & BLACK
    if "&" in seg or re.search(r"\sAND\s", seg, re.I):
        handle = _title_case_color(seg)
        return {
            "handle_color": handle,
            "blade_color": "",
            "color_raw": color_raw,
            "color_parse_note": "compound_handle_ampersand",
        }

    # Single handle color: BLACK, BUCK SKIN, DESERT CAMO, OLIVE, RED
    handle = _title_case_color(seg)
    return {
        "handle_color": handle,
        "blade_color": "",
        "color_raw": color_raw,
        "color_parse_note": "single_handle",
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--input", type=Path, default=Path("data/mkc_email_orders_knives.csv"))
    ap.add_argument("-o", "--output", type=Path, default=None)
    args = ap.parse_args()
    outp = args.output or args.input
    if not args.input.is_file():
        print(f"Missing: {args.input}", file=sys.stderr)
        return 2

    with open(args.input, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        print("No rows", file=sys.stderr)
        return 1

    extra = ["handle_color", "blade_color", "color_raw", "color_parse_note"]
    base_fields = list(rows[0].keys())
    out_fields = base_fields + [c for c in extra if c not in base_fields]

    for row in rows:
        parsed = parse_line_colors(
            row.get("line_title") or "",
            row.get("matched_catalog_name") or "",
        )
        row.update(parsed)

    outp.parent.mkdir(parents=True, exist_ok=True)
    with open(outp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=out_fields, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)

    print(f"Wrote {len(rows)} row(s) to {outp.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
