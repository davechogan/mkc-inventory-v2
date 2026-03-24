from __future__ import annotations

import csv
import io
import json
import re
import sqlite3
from typing import Any, Optional

TYPE_NAMES = [
    'Hunting', 'Culinary', 'Tactical', 'Everyday Carry', 'Bushcraft & Camp'
]

SERIES_PATTERNS = [
    ('Blood Brothers', re.compile(r'\bBlood Brothers\b', re.I)),
    ('VIP', re.compile(r'\bVIP\b', re.I)),
    ('Traditions', re.compile(r'\bTraditions\b', re.I)),
    ('Archery Country', re.compile(r'\bArchery Country\b', re.I)),
    ('Bearded Butchers', re.compile(r'\bBearded Butchers\b', re.I)),
    ('Meat Church', re.compile(r'\bMeat Church\b', re.I)),
    ('Nock On', re.compile(r'\bNock On\b', re.I)),
]

STEEL_PREFIX_PATTERNS = [
    re.compile(r'^Magnacut\s+', re.I),
    re.compile(r'^MagnaCut\s+', re.I),
    re.compile(r'^52100\s+', re.I),
    re.compile(r'^AEB-L\s+', re.I),
    re.compile(r'^440C\s+', re.I),
]

ARTICLE_PREFIXES = [
    re.compile(r'^The\s+', re.I),
    re.compile(r'^MKC\s+', re.I),
]

FAMILY_RULES = [
    ('Mini Speedgoat', 'Speedgoat'),
    ('Tactical Speedgoat', 'Speedgoat'),
    ('Speedgoat', 'Speedgoat'),
    ('Mini Wargoat', 'Wargoat'),
    ('Battle Goat', 'Wargoat'),
    ('Wargoat', 'Wargoat'),
    ('Stoned Goat', 'Stoned Goat'),
    ('Blackfoot', 'Blackfoot'),
    ('Stonewall', 'Stonewall'),
    ('Packout', 'Packout'),
    ('Great Falls', 'Great Falls'),
    ('Whitetail', 'Whitetail'),
    ('Elk Knife', 'Elk Knife'),
    ('Elkhorn', 'Elk Knife'),
    ('Beartooth', 'Beartooth'),
    ('Super Cub', 'Super Cub'),
    ('Stubhorn', 'Stubhorn'),
    ('Jackstone', 'Jackstone'),
    ('Stockyard', 'Stockyard'),
    ('Rocker', 'Rocker'),
    ('Marshall', 'Marshall'),
    ('Fieldcraft', 'Fieldcraft Survival'),
    ('Triumph', 'Triumph'),
    ('Mule Deer', 'Mule Deer'),
    ('Castle Rock', 'Castle Rock'),
    ('Flathead Fillet', 'Flathead Fillet'),
    ('Westslope', 'Westslope'),
    ('Freezout', 'Freezout'),
    ('Bighorn Chef', 'Bighorn Chef'),
    ('Little Bighorn Petty', 'Little Bighorn Petty'),
    ('Smith River Santoku', 'Smith River Santoku'),
    ('Cutbank Paring', 'Cutbank Paring Knife'),
    ('Boning Butcher', 'Boning Butcher Knife'),
    ('Breaking Butcher', 'Breaking Butcher Knife'),
    ('Sawtooth Slicer', 'Sawtooth Slicer'),
    ('Cattlemen Cleaver', 'Cattlemen Cleaver'),
    ('Hellgate Hatchet', 'Hellgate Hatchet'),
    ('Flattail', 'Flattail'),
]

FORM_OVERRIDES = [
    ('Hatchet', 'Hatchet'),
    ('Fillet', 'Fillet Knife'),
    ('Santoku', 'Santoku Knife'),
    ('Petty', 'Petty Knife'),
    ('Paring', 'Paring Knife'),
    ('Chef', "Chef's Knife"),
    ('Boning', 'Boning Knife'),
    ('Butcher', 'Butcher Knife'),
    ('Skinner', 'Skinner'),
    ('Cleaver', 'Cleaver'),
    ('Wargoat', 'Tactical Fixed Blade'),
    ('Tactical', 'Tactical Fixed Blade'),
    ('Speedgoat', 'EDC Fixed Blade'),
    ('Stockyard', 'Belt Knife'),
    ('Jackstone', 'Belt Knife'),
]


def slugify(value: str) -> str:
    value = (value or '').strip().lower()
    value = re.sub(r'[^a-z0-9]+', '-', value)
    value = re.sub(r'-+', '-', value).strip('-')
    return value


def normalize_whitespace(s: str) -> str:
    return re.sub(r'\s+', ' ', (s or '').strip())


def detect_series(name: str, catalog_line: Optional[str] = None) -> Optional[str]:
    if catalog_line and str(catalog_line).strip():
        return normalize_whitespace(str(catalog_line))
    for label, pattern in SERIES_PATTERNS:
        if pattern.search(name or ''):
            return label
    return None


def strip_series_tokens(name: str) -> str:
    out = name or ''
    for _, pattern in SERIES_PATTERNS:
        out = pattern.sub('', out)
    out = re.sub(r'^[-:\s]+', '', out)
    out = re.sub(r'[-:\s]+$', '', out)
    return normalize_whitespace(out)


def detect_type(category: Optional[str], family: Optional[str], is_kitchen: Any = 0, is_tactical: Any = 0, is_hatchet: Any = 0, name: Optional[str] = None) -> str:
    blob = ' / '.join([str(x) for x in [category, family, name] if x]).lower()
    if is_kitchen or 'culinary' in blob or 'kitchen' in blob or 'chef' in blob or 'santoku' in blob or 'petty' in blob:
        return 'Culinary'
    if is_tactical or 'tactical' in blob:
        return 'Tactical'
    if is_hatchet or 'hatchet' in blob or 'camp' in blob or 'bushcraft' in blob:
        return 'Bushcraft & Camp'
    if 'hunting' in blob or 'skinner' in blob or 'whitetail' in blob or 'elk' in blob or 'speedgoat' in blob or 'blackfoot' in blob or 'stoned goat' in blob:
        return 'Hunting'
    if 'edc' in blob or 'everyday carry' in blob:
        return 'Everyday Carry'
    return 'Hunting'


def normalize_model_name(name: str, catalog_line: Optional[str] = None) -> tuple[str, Optional[str], Optional[str], Optional[str]]:
    official = normalize_whitespace(name)
    series = detect_series(official, catalog_line)
    s = strip_series_tokens(official)
    for pat in STEEL_PREFIX_PATTERNS:
        s = pat.sub('', s)
    for pat in ARTICLE_PREFIXES:
        s = pat.sub('', s)
    s = normalize_whitespace(s)
    size_modifier = None
    generation_label = None
    platform_variant = None
    if re.search(r'\bMini\b', s):
        size_modifier = 'Mini'
    if re.search(r'\bUltra\b', s, re.I):
        platform_variant = 'Ultra'
    m = re.search(r'\b(\d+(?:\.\d+)?)\b', s)
    if m:
        generation_label = m.group(1)
    normalized = s
    sortable = re.sub(r'^The\s+', '', normalized, flags=re.I)
    sortable = re.sub(r'^MKC\s+', '', sortable, flags=re.I)
    sortable = normalize_whitespace(sortable)
    return normalized, sortable, series, size_modifier or generation_label, platform_variant


def detect_family(normalized_name: str) -> str:
    n = normalized_name or ''
    for token, family in FAMILY_RULES:
        if token.lower() in n.lower():
            return family
    # fallback: first 1-2 words after removing version and size tokens
    temp = re.sub(r'\b(?:Mini|Ultra|VIP|2\.0|3\.0|Pro)\b', '', n, flags=re.I)
    temp = normalize_whitespace(temp)
    parts = temp.split()
    return ' '.join(parts[:2]).strip() or n


def detect_form(normalized_name: str, blade_profile: Optional[str], primary_use_case: Optional[str], blade_shape: Optional[str], knife_type: str) -> str:
    blob = ' '.join(x for x in [normalized_name, blade_profile or '', primary_use_case or '', blade_shape or ''] if x)
    for token, form in FORM_OVERRIDES:
        if token.lower() in blob.lower():
            return form
    bp = (blade_profile or blade_shape or '').lower()
    if 'drop point' in bp:
        return 'Drop Point Fixed Blade'
    if 'trailing' in bp:
        return 'Trailing Point Fixed Blade'
    if knife_type == 'Culinary':
        return "Chef's Knife"
    if knife_type == 'Tactical':
        return 'Tactical Fixed Blade'
    if knife_type == 'Bushcraft & Camp':
        return 'Camp Fixed Blade'
    if knife_type == 'Everyday Carry':
        return 'EDC Fixed Blade'
    return 'Hunting Fixed Blade'


def detect_collaborator(is_collab: Any, collaboration_name: Optional[str], series: Optional[str]) -> Optional[str]:
    if collaboration_name and str(collaboration_name).strip():
        return normalize_whitespace(str(collaboration_name))
    if is_collab and series in {'Blood Brothers', 'Archery Country', 'Bearded Butchers', 'Meat Church', 'Nock On'}:
        return series
    return None


def ensure_normalized_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        '''
        CREATE TABLE IF NOT EXISTS knife_types (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            slug TEXT NOT NULL UNIQUE,
            sort_order INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS knife_forms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            slug TEXT NOT NULL UNIQUE,
            type_id INTEGER,
            notes TEXT,
            FOREIGN KEY(type_id) REFERENCES knife_types(id) ON DELETE SET NULL
        );
        CREATE TABLE IF NOT EXISTS knife_families (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            normalized_name TEXT NOT NULL,
            slug TEXT NOT NULL UNIQUE,
            type_id INTEGER,
            default_form_id INTEGER,
            notes TEXT,
            FOREIGN KEY(type_id) REFERENCES knife_types(id) ON DELETE SET NULL,
            FOREIGN KEY(default_form_id) REFERENCES knife_forms(id) ON DELETE SET NULL
        );
        CREATE TABLE IF NOT EXISTS knife_series (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            slug TEXT NOT NULL UNIQUE,
            description TEXT
        );
        CREATE TABLE IF NOT EXISTS collaborators (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            slug TEXT NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS knife_models_v2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            legacy_master_id INTEGER UNIQUE,
            official_name TEXT NOT NULL,
            normalized_name TEXT NOT NULL,
            sortable_name TEXT NOT NULL,
            slug TEXT NOT NULL UNIQUE,
            type_id INTEGER,
            form_id INTEGER,
            family_id INTEGER,
            series_id INTEGER,
            collaborator_id INTEGER,
            parent_model_id INTEGER,
            generation_label TEXT,
            size_modifier TEXT,
            platform_variant TEXT,
            steel TEXT,
            blade_finish TEXT,
            blade_color TEXT,
            handle_color TEXT,
            blade_length REAL,
            record_status TEXT,
            is_current_catalog INTEGER NOT NULL DEFAULT 1,
            is_discontinued INTEGER NOT NULL DEFAULT 0,
            msrp REAL,
            official_product_url TEXT,
            official_image_url TEXT,
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(type_id) REFERENCES knife_types(id) ON DELETE SET NULL,
            FOREIGN KEY(form_id) REFERENCES knife_forms(id) ON DELETE SET NULL,
            FOREIGN KEY(family_id) REFERENCES knife_families(id) ON DELETE SET NULL,
            FOREIGN KEY(series_id) REFERENCES knife_series(id) ON DELETE SET NULL,
            FOREIGN KEY(collaborator_id) REFERENCES collaborators(id) ON DELETE SET NULL,
            FOREIGN KEY(parent_model_id) REFERENCES knife_models_v2(id) ON DELETE SET NULL,
            FOREIGN KEY(legacy_master_id) REFERENCES master_knives(id) ON DELETE SET NULL
        );
        CREATE TABLE IF NOT EXISTS inventory_items_v2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            legacy_inventory_id INTEGER UNIQUE,
            legacy_master_id INTEGER,
            knife_model_id INTEGER,
            nickname TEXT,
            quantity INTEGER NOT NULL DEFAULT 1,
            acquired_date TEXT,
            purchase_price REAL,
            estimated_value REAL,
            condition TEXT,
            steel TEXT,
            blade_finish TEXT,
            blade_color TEXT,
            handle_color TEXT,
            collaboration_name TEXT,
            serial_number TEXT,
            location TEXT,
            purchase_source TEXT,
            last_sharpened TEXT,
            notes TEXT,
            created_at TEXT,
            updated_at TEXT,
            FOREIGN KEY(knife_model_id) REFERENCES knife_models_v2(id) ON DELETE SET NULL
        );
        CREATE TABLE IF NOT EXISTS migration_runs_v2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            source_master_count INTEGER,
            source_inventory_count INTEGER,
            imported_model_count INTEGER,
            imported_inventory_count INTEGER,
            notes TEXT
        );
        '''
    )


def _get_or_create(conn: sqlite3.Connection, table: str, name: str, **extra) -> int:
    row = conn.execute(f"SELECT id FROM {table} WHERE name = ?", (name,)).fetchone()
    if row:
        return int(_row_get(row, 'id', 0))
    cols = ['name'] + list(extra.keys())
    vals = [name] + list(extra.values())
    q = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})"
    cur = conn.execute(q, vals)
    return cur.lastrowid


def _row_get(row: Any, key: str, default=None):
    if isinstance(row, sqlite3.Row):
        return row[key] if key in row.keys() else default
    if isinstance(row, dict):
        return row.get(key, default)
    return default


def migrate_legacy_to_v2(conn: sqlite3.Connection, force: bool = False) -> dict[str, int]:
    ensure_normalized_schema(conn)
    existing = _row_get(conn.execute('SELECT COUNT(*) AS c FROM knife_models_v2').fetchone(), 'c', 0)
    if existing and not force:
        return {
            'source_master_count': _row_get(conn.execute('SELECT COUNT(*) AS c FROM master_knives').fetchone(), 'c', 0),
            'source_inventory_count': _row_get(conn.execute('SELECT COUNT(*) AS c FROM inventory_items').fetchone(), 'c', 0),
            'imported_model_count': existing,
            'imported_inventory_count': _row_get(conn.execute('SELECT COUNT(*) AS c FROM inventory_items_v2').fetchone(), 'c', 0),
        }
    if force:
        for t in ['inventory_items_v2','knife_models_v2','knife_families','knife_forms','knife_series','collaborators','knife_types']:
            conn.execute(f'DELETE FROM {t}')
    for idx, name in enumerate(TYPE_NAMES, start=1):
        conn.execute('INSERT OR IGNORE INTO knife_types (name, slug, sort_order) VALUES (?, ?, ?)', (name, slugify(name), idx))
    masters = conn.execute('SELECT * FROM master_knives ORDER BY id').fetchall()
    type_cache = {_row_get(r, 'name'): _row_get(r, 'id') for r in conn.execute('SELECT id, name FROM knife_types').fetchall()}

    legacy_to_v2 = {}
    pending_parent = []
    for row in masters:
        name = _row_get(row, 'name', '')
        normalized, sortable, series_name, size_or_generation, platform_variant = normalize_model_name(name, _row_get(row,'catalog_line'))
        knife_type = detect_type(_row_get(row,'category'), _row_get(row,'family'), _row_get(row,'is_kitchen',0), _row_get(row,'is_tactical',0), _row_get(row,'is_hatchet',0), normalized)
        type_id = type_cache[knife_type]
        form_name = detect_form(normalized, _row_get(row,'blade_profile'), _row_get(row,'primary_use_case'), _row_get(row,'blade_shape'), knife_type)
        form_id = _get_or_create(conn, 'knife_forms', form_name, slug=slugify(form_name), type_id=type_id, notes=None)
        family_name = detect_family(normalized)
        family_id = _get_or_create(conn, 'knife_families', family_name, normalized_name=family_name, slug=slugify(family_name), type_id=type_id, default_form_id=form_id, notes=None)
        series_id = None
        if series_name:
            series_id = _get_or_create(conn, 'knife_series', series_name, slug=slugify(series_name), description=None)
        collaborator_id = None
        collab_name = detect_collaborator(_row_get(row,'is_collab',0), _row_get(row,'collaboration_name'), series_name)
        if collab_name:
            collaborator_id = _get_or_create(conn, 'collaborators', collab_name, slug=slugify(collab_name))
        generation_label = _row_get(row,'version') or None
        if not generation_label and size_or_generation and re.fullmatch(r'\d+(?:\.\d+)?', str(size_or_generation)):
            generation_label = str(size_or_generation)
            size_modifier = None
        else:
            size_modifier = 'Mini' if 'Mini' in normalized else None
        if _row_get(row,'version') and 'Ultra' in str(_row_get(row,'version')):
            platform_variant = _row_get(row,'version')
        if 'Ultra' in normalized and not platform_variant:
            platform_variant = 'Ultra'
        slug_base = slugify(sortable or normalized or name)
        slug = slug_base
        suffix = 2
        while conn.execute('SELECT 1 FROM knife_models_v2 WHERE slug=?', (slug,)).fetchone():
            slug = f'{slug_base}-{suffix}'
            suffix += 1
        cur = conn.execute(
            '''INSERT INTO knife_models_v2 (
                legacy_master_id, official_name, normalized_name, sortable_name, slug,
                type_id, form_id, family_id, series_id, collaborator_id, parent_model_id,
                generation_label, size_modifier, platform_variant,
                steel, blade_finish, blade_color, handle_color, blade_length,
                record_status, is_current_catalog, is_discontinued, msrp,
                official_product_url, official_image_url, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (
                _row_get(row,'id'), name, normalized, sortable, slug,
                type_id, form_id, family_id, series_id, collaborator_id,
                generation_label, size_modifier, platform_variant,
                _row_get(row,'default_steel'), _row_get(row,'default_blade_finish'), _row_get(row,'default_blade_color'), _row_get(row,'default_handle_color'), _row_get(row,'default_blade_length'),
                _row_get(row,'status') or 'active', int(_row_get(row,'is_current_catalog',1) or 1), int(_row_get(row,'is_discontinued',0) or 0), _row_get(row,'msrp'),
                _row_get(row,'default_product_url') or _row_get(row,'identifier_product_url'), _row_get(row,'primary_image_url'), _row_get(row,'notes')
            )
        )
        model_id = cur.lastrowid
        legacy_to_v2[_row_get(row,'id')] = model_id
        if _row_get(row,'parent_model_id'):
            pending_parent.append((model_id, _row_get(row,'parent_model_id')))
    for model_id, legacy_parent_id in pending_parent:
        parent_v2 = legacy_to_v2.get(legacy_parent_id)
        if parent_v2:
            conn.execute('UPDATE knife_models_v2 SET parent_model_id=? WHERE id=?', (parent_v2, model_id))
    # infer parent by family + base model where not explicitly set
    rows = conn.execute('SELECT id, normalized_name, family_id, generation_label, size_modifier, platform_variant, series_id FROM knife_models_v2').fetchall()
    fam_groups = {}
    for r in rows:
        fam_groups.setdefault(_row_get(r, 'family_id'), []).append(r)
    for fam_id, group in fam_groups.items():
        base = None
        for r in group:
            nm = (_row_get(r, 'normalized_name') or '').lower()
            if 'mini ' not in nm and 'ultra' not in nm and not re.search(r'\b2\.0\b|\b3\.0\b', nm):
                base = r
                break
        if not base:
            continue
        for r in group:
            if _row_get(r, 'id') == _row_get(base, 'id'):
                continue
            curr_parent = _row_get(conn.execute('SELECT parent_model_id FROM knife_models_v2 WHERE id=?', (_row_get(r, 'id'),)).fetchone(), 'parent_model_id')
            if not curr_parent:
                conn.execute('UPDATE knife_models_v2 SET parent_model_id=? WHERE id=?', (_row_get(base, 'id'), _row_get(r, 'id')))
    inv_rows = conn.execute('SELECT * FROM inventory_items ORDER BY id').fetchall()
    for row in inv_rows:
        legacy_master_id = _row_get(row, 'master_knife_id')
        knife_model_id = legacy_to_v2.get(legacy_master_id)
        conn.execute(
            '''INSERT OR REPLACE INTO inventory_items_v2 (
                legacy_inventory_id, legacy_master_id, knife_model_id, nickname, quantity, acquired_date,
                purchase_price, estimated_value, condition, steel, blade_finish, blade_color, handle_color,
                collaboration_name, serial_number, location, purchase_source, last_sharpened, notes, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (
                _row_get(row,'id'), legacy_master_id, knife_model_id, _row_get(row,'nickname'), _row_get(row,'quantity') or 1,
                _row_get(row,'acquired_date'), _row_get(row,'purchase_price'), _row_get(row,'estimated_value'), _row_get(row,'condition'),
                _row_get(row,'blade_steel'), _row_get(row,'blade_finish'), _row_get(row,'blade_color'), _row_get(row,'handle_color'),
                _row_get(row,'collaboration_name'), _row_get(row,'serial_number'), _row_get(row,'location'), _row_get(row,'purchase_source'),
                _row_get(row,'last_sharpened'), _row_get(row,'notes'), _row_get(row,'created_at'), _row_get(row,'updated_at')
            )
        )
    summary = {
        'source_master_count': len(masters),
        'source_inventory_count': len(inv_rows),
        'imported_model_count': _row_get(conn.execute('SELECT COUNT(*) AS c FROM knife_models_v2').fetchone(), 'c', 0),
        'imported_inventory_count': _row_get(conn.execute('SELECT COUNT(*) AS c FROM inventory_items_v2').fetchone(), 'c', 0),
    }
    conn.execute(
        'INSERT INTO migration_runs_v2 (source_master_count, source_inventory_count, imported_model_count, imported_inventory_count, notes) VALUES (?, ?, ?, ?, ?)',
        (summary['source_master_count'], summary['source_inventory_count'], summary['imported_model_count'], summary['imported_inventory_count'], 'legacy->normalized migration')
    )
    return summary


def export_models_csv(conn: sqlite3.Connection) -> str:
    rows = conn.execute(
        '''SELECT km.id, km.official_name, km.normalized_name, km.sortable_name, km.slug,
                  kt.name AS knife_type, kf.name AS family_name, frm.name AS form_name,
                  ks.name AS series_name, c.name AS collaborator_name,
                  km.generation_label, km.size_modifier, km.platform_variant,
                  km.steel, km.blade_finish, km.blade_color, km.handle_color,
                  km.blade_length, km.msrp, km.is_current_catalog, km.is_discontinued,
                  km.official_product_url
           FROM knife_models_v2 km
           LEFT JOIN knife_types kt ON kt.id = km.type_id
           LEFT JOIN knife_families kf ON kf.id = km.family_id
           LEFT JOIN knife_forms frm ON frm.id = km.form_id
           LEFT JOIN knife_series ks ON ks.id = km.series_id
           LEFT JOIN collaborators c ON c.id = km.collaborator_id
           ORDER BY km.sortable_name'''
    ).fetchall()
    out = io.StringIO()
    writer = csv.writer(out)
    if rows:
        writer.writerow(rows[0].keys())
        for r in rows:
            writer.writerow([r[k] for k in r.keys()])
    return out.getvalue()
