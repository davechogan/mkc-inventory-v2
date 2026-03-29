"""
Phase F migration: Create model_colorways table and populate from existing images.

Steps:
1. Create model_colorways table
2. For each PNG in Images/MKC_Colors/: resolve to knife_model_id + handle_color_id
   (+ blade_color_id for tactical models), read bytes into image_blob, is_transparent=1
3. For JPG entries in knife_model_image_files with no PNG counterpart: create row
   with image_blob=NULL (surfaces as "needs image")
4. Log unresolved files — do not fail

Usage:
    .venv/bin/python3 scripts/migrate_colorways.py [--dry-run]
"""

import argparse
import os
import sqlite3
import sys

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "mkc_inventory.db")
IMAGE_DIR = os.path.join(os.path.dirname(__file__), "..", "Images", "MKC_Colors")

# Tactical type_id — these models have handle_color + blade_color in the color_name
TACTICAL_TYPE_ID = 18


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def build_color_lookup(conn):
    """Build normalized name -> id maps for handle and blade colors."""
    handle = {}
    for r in conn.execute("SELECT id, name FROM handle_colors"):
        handle[r["name"].lower()] = r["id"]

    blade = {}
    for r in conn.execute("SELECT id, name FROM blade_colors"):
        blade[r["name"].lower()] = r["id"]

    return handle, blade


def normalize_color_name(raw):
    """Normalize a color_name from image_files to match handle_colors lookup.

    Examples:
        'Orange Black' -> 'orange/black'
        'Tan Black'    -> 'tan/black'
        'Green Black'  -> 'green/black'
        'Black Red'    -> 'black/red'  (Blood Brothers handle color)
        'Od Green'     -> 'od green'
        'Olive Tan'    -> 'olive/tan'
        'HUK Black'    -> 'black'  (HUK is a collab label, not a color)
    """
    s = raw.strip()

    # Strip trailing " (1)" etc. — duplicate file artifacts
    import re
    s = re.sub(r"\s*\(\d+\)$", "", s)

    # Special cases
    if s.upper().startswith("HUK "):
        s = s[4:]

    # Strip leading "20 " — artifact of Stoned Goat 2.0 etc image naming
    if s.startswith("20 "):
        s = s[3:]

    return s.lower()


# Two-word color_names that are a SINGLE handle color (with slash), not handle+blade
# These map to handle_colors entries like "Orange/Black", "Tan/Black", etc.
COMPOUND_HANDLE_COLORS = {
    "orange black": "orange/black",
    "tan black": "tan/black",
    "green black": "green/black",
    "black red": "black/red",
    "red black": "red/black",
    "olive tan": "olive/tan",
    "blaze orange": "blaze orange",  # single handle color, not compound
    "steel carbon fiber": None,      # special: Ultra series
    "steel desert ironwood": None,   # special: Traditions series
    "damascus": None,                # special: Damascus series — blade color only
    "distressed gray": None,         # special: stonewashed Magnacut — blade color only
}


def resolve_colors(color_name_raw, is_tactical, handle_lookup, blade_lookup):
    """Resolve a color_name to (handle_color_id, blade_color_id).

    Returns (handle_color_id, blade_color_id, error_msg).
    blade_color_id is None for non-tactical models.
    error_msg is None on success.
    """
    norm = normalize_color_name(color_name_raw)

    # Check if this is a known compound handle color first
    if norm in COMPOUND_HANDLE_COLORS:
        mapped = COMPOUND_HANDLE_COLORS[norm]
        if mapped is None:
            # Special single-image models (Traditions, Ultra)
            # Try to resolve as-is from the parts
            return _resolve_special(norm, handle_lookup, blade_lookup)
        hc_id = handle_lookup.get(mapped)
        if hc_id:
            return hc_id, None, None
        return None, None, f"compound handle color '{mapped}' not in handle_colors"

    # Tactical models: color_name = "blade_color handle_color"
    if is_tactical:
        return _resolve_tactical(norm, handle_lookup, blade_lookup)

    # Regular model: color_name is just a handle color
    hc_id = handle_lookup.get(norm)
    if hc_id:
        return hc_id, None, None

    return None, None, f"handle color '{norm}' not in handle_colors"


def _resolve_tactical(norm, handle_lookup, blade_lookup):
    """For tactical models, split 'blade_color handle_color' and resolve both."""
    # Try splitting at each word boundary
    parts = norm.split()
    for i in range(1, len(parts)):
        blade_part = " ".join(parts[:i])
        handle_part = " ".join(parts[i:])
        bc_id = blade_lookup.get(blade_part)
        hc_id = handle_lookup.get(handle_part)
        if bc_id and hc_id:
            return hc_id, bc_id, None

    # Also try with "od green" -> "od green"
    for i in range(1, len(parts)):
        blade_part = " ".join(parts[:i])
        handle_part = " ".join(parts[i:])
        bc_id = blade_lookup.get(blade_part)
        hc_id = handle_lookup.get(handle_part)
        if bc_id and hc_id:
            return hc_id, bc_id, None

    return None, None, f"tactical color '{norm}' could not be split into blade+handle"


def _resolve_special(norm, handle_lookup, blade_lookup):
    """Resolve special single-image models (Traditions: steel+desert ironwood, Ultra: steel+carbon fiber, Damascus)."""
    if "desert ironwood" in norm:
        hc_id = handle_lookup.get("desert ironwood")
        bc_id = blade_lookup.get("steel")
        if hc_id and bc_id:
            return hc_id, bc_id, None
        return None, None, f"special '{norm}': desert ironwood or steel not found"

    if "carbon fiber" in norm:
        hc_id = handle_lookup.get("carbon fiber")
        bc_id = blade_lookup.get("steel")
        if hc_id and bc_id:
            return hc_id, bc_id, None
        return None, None, f"special '{norm}': carbon fiber or steel not found"

    if norm == "damascus":
        # Damascus Blackfoot 2.0 — Desert Ironwood Burl handle, Damascus Wood Grain blade
        hc_id = handle_lookup.get("desert ironwood")
        bc_id = blade_lookup.get("damascus")
        if hc_id and bc_id:
            return hc_id, bc_id, None
        return None, None, f"special '{norm}': desert ironwood or damascus not found"

    if norm == "distressed gray":
        # Stonewashed Magnacut — "Distressed Gray" is a blade color, not handle
        hc_id = handle_lookup.get("black")  # default handle
        bc_id = blade_lookup.get("distressed gray")
        if hc_id and bc_id:
            return hc_id, bc_id, None
        return None, None, f"special '{norm}': black or distressed gray not found"

    return None, None, f"unrecognized special color '{norm}'"


def add_missing_handle_color(conn, name, dry_run):
    """Add a handle color that exists in image data but not in the lookup table."""
    if dry_run:
        print(f"  [DRY RUN] Would add handle_color: '{name}'")
        # Return a placeholder ID so dry-run resolution still works
        return -(hash(name) & 0xFFFF)
    conn.execute("INSERT INTO handle_colors (name) VALUES (?)", (name,))
    row = conn.execute("SELECT id FROM handle_colors WHERE name = ?", (name,)).fetchone()
    print(f"  Added handle_color: '{name}' (id={row['id']})")
    return row["id"]


# Handle colors that appear in image data but aren't in the lookup tables yet
MISSING_HANDLE_COLORS = ["Blaze Orange", "Dark Camo", "Multicam", "Forest Camo"]


def migrate(dry_run=False):
    conn = get_conn()
    handle_lookup, blade_lookup = build_color_lookup(conn)

    # Add all missing handle colors upfront so resolution works
    for name in MISSING_HANDLE_COLORS:
        if name.lower() not in handle_lookup:
            hc_id = add_missing_handle_color(conn, name, dry_run)
            if hc_id:
                handle_lookup[name.lower()] = hc_id

    # Alias 'damascus' to existing 'Damascus Wood Grain' blade color
    if "damascus" not in blade_lookup and "damascus wood grain" in blade_lookup:
        blade_lookup["damascus"] = blade_lookup["damascus wood grain"]

    # Step 1: Create table
    print("\n=== Step 1: Create model_colorways table ===")
    if not dry_run:
        conn.execute("DROP TABLE IF EXISTS model_colorways")
        conn.execute("""
            CREATE TABLE model_colorways (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                knife_model_id  INTEGER NOT NULL REFERENCES knife_models_v2(id),
                handle_color_id INTEGER NOT NULL REFERENCES handle_colors(id),
                blade_color_id  INTEGER REFERENCES blade_colors(id),
                image_blob      BLOB,
                is_transparent  INTEGER NOT NULL DEFAULT 0,
                created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE UNIQUE INDEX uq_model_colorway
            ON model_colorways (knife_model_id, handle_color_id, COALESCE(blade_color_id, -1))
        """)
        print("  Created model_colorways table")
    else:
        print("  [DRY RUN] Would create model_colorways table")

    # Build model slug -> (id, type_id) map
    models = {}
    for r in conn.execute("SELECT id, slug, type_id FROM knife_models_v2"):
        models[r["slug"]] = {"id": r["id"], "type_id": r["type_id"]}

    # Step 2: Process PNG files from image_files table (DB entries with known slug+color)
    print("\n=== Step 2: Ingest PNGs from knife_model_image_files ===")
    png_rows = conn.execute(
        "SELECT id, model_slug, color_name, filename FROM knife_model_image_files WHERE filename LIKE '%.png'"
    ).fetchall()

    inserted = 0
    errors = []
    for r in png_rows:
        slug = r["model_slug"]
        color_name = r["color_name"]
        filename = r["filename"]
        filepath = os.path.join(IMAGE_DIR, filename)

        model = models.get(slug)
        if not model:
            errors.append(f"No model for slug '{slug}' (file: {filename})")
            continue

        is_tactical = model["type_id"] == TACTICAL_TYPE_ID

        hc_id, bc_id, err = resolve_colors(color_name, is_tactical, handle_lookup, blade_lookup)
        if err:
            errors.append(f"slug={slug} color='{color_name}': {err}")
            continue

        # Read image bytes if file exists on disk
        image_bytes = None
        if os.path.isfile(filepath):
            with open(filepath, "rb") as f:
                image_bytes = f.read()

        if not dry_run:
            try:
                conn.execute(
                    """INSERT INTO model_colorways (knife_model_id, handle_color_id, blade_color_id, image_blob, is_transparent)
                       VALUES (?, ?, ?, ?, ?)""",
                    (model["id"], hc_id, bc_id, image_bytes, 1 if image_bytes else 0),
                )
                inserted += 1
            except sqlite3.IntegrityError as e:
                if "UNIQUE constraint" in str(e):
                    # Duplicate colorway — update with image if we have one and existing doesn't
                    if image_bytes:
                        conn.execute(
                            """UPDATE model_colorways SET image_blob = ?, is_transparent = 1, updated_at = CURRENT_TIMESTAMP
                               WHERE knife_model_id = ? AND handle_color_id = ? AND blade_color_id IS ?""",
                            (image_bytes, model["id"], hc_id, bc_id),
                        )
                    # Still count as success
                else:
                    errors.append(f"slug={slug} color='{color_name}': {e}")
        else:
            inserted += 1

    print(f"  Inserted: {inserted}")
    if errors:
        print(f"  Errors: {len(errors)}")
        for e in errors:
            print(f"    WARNING: {e}")

    # Step 3: Process new PNG files on disk that aren't in knife_model_image_files
    print("\n=== Step 3: Ingest new PNGs from disk (not in DB) ===")
    db_filenames = set(r["filename"] for r in png_rows)
    disk_pngs = sorted(f for f in os.listdir(IMAGE_DIR) if f.endswith(".png"))
    new_pngs = [f for f in disk_pngs if f not in db_filenames]
    print(f"  Found {len(new_pngs)} PNG files on disk not in knife_model_image_files")

    new_inserted = 0
    new_errors = []

    # Build filename prefix -> model mapping from existing image_files data
    prefix_to_model = {}
    for r in conn.execute("SELECT model_slug, filename FROM knife_model_image_files"):
        fn = r["filename"]
        slug = r["model_slug"]
        # Strip extension and color part — the prefix is everything before the last color token
        # But this is complex. Instead, just record all known filename prefixes per model.
        if slug not in prefix_to_model:
            prefix_to_model[slug] = set()
        prefix_to_model[slug].add(fn)

    # For each new PNG, find which existing model prefix it matches
    # by comparing against known filenames for each model
    model_filename_prefixes = {}
    for slug in models:
        known_files = conn.execute(
            "SELECT filename, color_name FROM knife_model_image_files WHERE model_slug = ? LIMIT 1",
            (slug,),
        ).fetchone()
        if known_files:
            fn = known_files["filename"]
            color = known_files["color_name"]
            # The prefix is filename minus the color part minus extension
            color_under = color.replace(" ", "_")
            ext = fn.rsplit(".", 1)[-1]
            suffix = f"_{color_under}.{ext}"
            if fn.endswith(suffix):
                prefix = fn[: -len(suffix)]
                model_filename_prefixes[prefix] = slug

    # Aliases for files whose filename prefix doesn't match any DB entry.
    # "Elkhorn Skinner" was renamed to "MKC Elk Knife" but MKC still ships images
    # with the old name. "Archery_Country_Magnacut_Speedgoat_20" is an alternate
    # naming of the "Archery_Country_Magnacut_Speedgoat_2.0" prefix.
    FILENAME_PREFIX_ALIASES = {
        "Elkhorn_Skinner": "elk-knife",
        "Archery_Country_Magnacut_Speedgoat_20": "speedgoat-2-0-3",
    }
    for alias, slug in FILENAME_PREFIX_ALIASES.items():
        if slug in models:
            model_filename_prefixes[alias] = slug

    # Sort by prefix length descending so longer (more specific) prefixes match first
    sorted_prefixes = sorted(model_filename_prefixes.keys(), key=len, reverse=True)

    for filename in new_pngs:
        stem = filename.rsplit(".", 1)[0]
        matched_slug = None
        matched_color_part = None

        for prefix in sorted_prefixes:
            if stem.startswith(prefix + "_"):
                matched_slug = model_filename_prefixes[prefix]
                matched_color_part = stem[len(prefix) + 1 :]  # everything after prefix_
                break

        if not matched_slug:
            new_errors.append(f"Could not match file to model: {filename}")
            continue

        model = models[matched_slug]
        is_tactical = model["type_id"] == TACTICAL_TYPE_ID

        # Convert underscore color back to space-separated
        color_name = matched_color_part.replace("_", " ")
        hc_id, bc_id, err = resolve_colors(color_name, is_tactical, handle_lookup, blade_lookup)
        if err:
            new_errors.append(f"file={filename} slug={matched_slug} color='{color_name}': {err}")
            continue

        filepath = os.path.join(IMAGE_DIR, filename)
        with open(filepath, "rb") as f:
            image_bytes = f.read()

        if not dry_run:
            try:
                conn.execute(
                    """INSERT INTO model_colorways (knife_model_id, handle_color_id, blade_color_id, image_blob, is_transparent)
                       VALUES (?, ?, ?, ?, 1)""",
                    (model["id"], hc_id, bc_id, image_bytes),
                )
                new_inserted += 1
            except sqlite3.IntegrityError:
                # Already exists (was inserted in step 2), update image
                conn.execute(
                    """UPDATE model_colorways SET image_blob = ?, is_transparent = 1, updated_at = CURRENT_TIMESTAMP
                       WHERE knife_model_id = ? AND handle_color_id = ? AND blade_color_id IS ?""",
                    (image_bytes, model["id"], hc_id, bc_id),
                )
                new_inserted += 1
        else:
            new_inserted += 1

    print(f"  Inserted: {new_inserted}")
    if new_errors:
        print(f"  Unresolved: {len(new_errors)}")
        for e in new_errors:
            print(f"    WARNING: {e}")

    # Step 4: Create placeholder rows for JPG-only colorways (no PNG counterpart)
    print("\n=== Step 4: Placeholder rows for JPG-only colorways ===")
    jpg_rows = conn.execute(
        """SELECT DISTINCT model_slug, color_name FROM knife_model_image_files
           WHERE filename LIKE '%.jpg'
           AND model_slug || '|' || color_name NOT IN (
               SELECT model_slug || '|' || color_name FROM knife_model_image_files WHERE filename LIKE '%.png'
           )"""
    ).fetchall()
    print(f"  JPG-only colorways (no PNG): {len(jpg_rows)}")

    placeholder_inserted = 0
    placeholder_errors = []
    for r in jpg_rows:
        slug = r["model_slug"]
        color_name = r["color_name"]
        model = models.get(slug)
        if not model:
            placeholder_errors.append(f"No model for slug '{slug}'")
            continue

        is_tactical = model["type_id"] == TACTICAL_TYPE_ID
        hc_id, bc_id, err = resolve_colors(color_name, is_tactical, handle_lookup, blade_lookup)
        if err:
            placeholder_errors.append(f"slug={slug} color='{color_name}': {err}")
            continue

        if not dry_run:
            try:
                conn.execute(
                    """INSERT INTO model_colorways (knife_model_id, handle_color_id, blade_color_id, image_blob, is_transparent)
                       VALUES (?, ?, ?, NULL, 0)""",
                    (model["id"], hc_id, bc_id),
                )
                placeholder_inserted += 1
            except sqlite3.IntegrityError:
                pass  # Already exists from PNG step — good
        else:
            placeholder_inserted += 1

    print(f"  Placeholders inserted: {placeholder_inserted}")
    if placeholder_errors:
        print(f"  Errors: {len(placeholder_errors)}")
        for e in placeholder_errors:
            print(f"    WARNING: {e}")

    # Step 5: Summary
    print("\n=== Summary ===")
    if not dry_run:
        total = conn.execute("SELECT COUNT(*) as c FROM model_colorways").fetchone()["c"]
        with_image = conn.execute(
            "SELECT COUNT(*) as c FROM model_colorways WHERE image_blob IS NOT NULL"
        ).fetchone()["c"]
        without_image = total - with_image
        distinct_models = conn.execute(
            "SELECT COUNT(DISTINCT knife_model_id) as c FROM model_colorways"
        ).fetchone()["c"]
        print(f"  Total colorway rows: {total}")
        print(f"  With image: {with_image}")
        print(f"  Without image (needs upload): {without_image}")
        print(f"  Distinct models covered: {distinct_models} / {len(models)}")

        conn.commit()
        print("\n  Migration committed.")
    else:
        print("  [DRY RUN] No changes made.")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate to model_colorways table")
    parser.add_argument("--dry-run", action="store_true", help="Preview without making changes")
    args = parser.parse_args()
    migrate(dry_run=args.dry_run)
