#!/usr/bin/env bash
# Usage: scripts/import_pngs.sh
# Copies new/updated PNGs from Images/MKC-images-no-bkgrnd/ into Images/MKC_Colors/
# then syncs knife_model_image_files:
#   1. Flips existing .jpg url_paths to .png where a PNG now exists
#   2. Inserts new entries for PNGs not yet registered in the DB

set -e
cd "$(dirname "$0")/.."

SRC="Images/MKC-images-no-bkgrnd"
DEST="Images/MKC_Colors"

# ── 1. Copy new/updated PNGs ───────────────────────────────────────────────────
copied=0
for f in "$SRC"/*.png; do
  [ -f "$f" ] || continue
  fname="$(basename "$f")"
  dest_file="$DEST/$fname"
  if [ ! -f "$dest_file" ] || [ "$f" -nt "$dest_file" ]; then
    cp "$f" "$dest_file"
    echo "  copied: $fname"
    ((copied++)) || true
  fi
done
echo "Copied $copied file(s)."

# ── 2. Sync DB ────────────────────────────────────────────────────────────────
.venv/bin/python3 - <<'PYEOF'
import sqlite3, os, re

DEST = "Images/MKC_Colors"
DB   = "data/mkc_inventory.db"

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

# Load all known slugs once
slugs = [r["slug"] for r in conn.execute("SELECT slug FROM knife_models_v2 WHERE slug IS NOT NULL").fetchall()]

# ── 2a. Flip .jpg → .png for entries whose PNG file now exists ─────────────────
rows = conn.execute("SELECT id, url_path FROM knife_model_image_files WHERE url_path LIKE '%.jpg'").fetchall()
updated = 0
for row in rows:
    png_url = row["url_path"].replace(".jpg", ".png")
    fname   = png_url.split("/")[-1]
    if os.path.exists(os.path.join(DEST, fname)):
        conn.execute("UPDATE knife_model_image_files SET url_path=?, filename=? WHERE id=?",
                     (png_url, fname, row["id"]))
        updated += 1
print(f"Updated {updated} .jpg → .png DB row(s).")

# ── 2b. Insert new entries for PNGs not yet in knife_model_image_files ─────────
registered = {r["filename"] for r in conn.execute("SELECT filename FROM knife_model_image_files").fetchall()}

inserted = 0
skipped  = []

for fname in sorted(os.listdir(DEST)):
    if not fname.lower().endswith(".png"):
        continue
    if fname in registered:
        continue  # already known

    # Try to match filename prefix to a model slug.
    # Strategy: greedily try longer prefixes (underscore-split → hyphen-joined, lowercased)
    stem   = os.path.splitext(fname)[0]          # e.g. "Cutbank_Paring_Knife_Black"
    parts  = stem.split("_")
    matched_slug  = None
    matched_color = None

    for i in range(len(parts) - 1, 0, -1):       # longest prefix first
        candidate = "-".join(p.lower() for p in parts[:i])
        if candidate in slugs:
            matched_slug  = candidate
            matched_color = " ".join(parts[i:]).replace("_", " ")
            break

    if not matched_slug:
        skipped.append(fname)
        continue

    url_path = f"/images/colors/{fname}"
    conn.execute(
        """INSERT OR IGNORE INTO knife_model_image_files
           (model_slug, color_name, filename, url_path)
           VALUES (?, ?, ?, ?)""",
        (matched_slug, matched_color, fname, url_path),
    )
    print(f"  registered: {fname}  →  slug={matched_slug}, color={matched_color}")
    inserted += 1

conn.commit()
print(f"Inserted {inserted} new DB row(s).")
if skipped:
    print(f"Could not match {len(skipped)} file(s) to a model slug:")
    for f in skipped:
        print(f"  {f}")
PYEOF
