from pathlib import Path
import shutil
import sqlite3
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import app

root = Path(__file__).resolve().parents[1]
blank_db = root / 'data' / 'mkc_inventory.blank.db'
# Ensure schema exists in primary db first
app.init_db()
source = root / 'data' / 'mkc_inventory.db'
shutil.copy2(source, blank_db)
conn = sqlite3.connect(blank_db)
cur = conn.cursor()
for table in [
    'inventory_items_v2','knife_models_v2','knife_families','knife_forms','knife_series','collaborators','knife_types','migration_runs_v2',
    'inventory_items','master_knives','master_knife_allowed_blade_colors','master_knife_allowed_blade_finishes','master_knife_allowed_blade_steels','master_knife_allowed_handle_colors',
    'blade_shape_templates'
]:
    try:
        cur.execute(f'DELETE FROM {table}')
    except Exception:
        pass
# keep option tables and app_meta so app can start cleanly
conn.commit()
conn.close()
print(blank_db)
