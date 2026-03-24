from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import app
import normalized_model
from app import get_conn

with get_conn() as conn:
    summary = normalized_model.migrate_legacy_to_v2(conn, force=True)
print(summary)
