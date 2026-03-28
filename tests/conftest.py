import os
import shutil
import tempfile
import uuid
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).parent.parent
_ARTIFACTS_SEED = _REPO_ROOT / "artifacts" / "db_snapshots" / "mkc_inventory_seed.db"
_FIXTURES_SEED = _REPO_ROOT / "tests" / "fixtures" / "mkc_inventory_seed.db"
# Prefer the artifacts submodule seed; fall back to tests/fixtures for CI
# environments where the private submodule is not initialized.
_SEED_DB = _ARTIFACTS_SEED if _ARTIFACTS_SEED.exists() else _FIXTURES_SEED

# Ensure importing `app.py` uses a throwaway DB during unit tests.
# The canonical seed DB lives in artifacts/db_snapshots/mkc_inventory_seed.db
# (private submodule). Update it deliberately when the catalog or inventory
# changes in a way that should be reflected in tests.
_TEST_DB_DIR = Path(tempfile.mkdtemp(prefix="mkc_pytest_db_"))
_TEST_DB_PATH = _TEST_DB_DIR / f"mkc_inventory.test.{uuid.uuid4().hex}.db"

if _SEED_DB.exists():
    shutil.copy2(_SEED_DB, _TEST_DB_PATH)

os.environ["MKC_INVENTORY_DB"] = str(_TEST_DB_PATH)

# Fast unit tests use lexical retrieval to avoid loading sentence-transformers/Chroma.
# Live LLM tests (pytest -m live_llm) should be run without this override so
# Chroma is used, matching production behaviour.
if "REPORTING_RETRIEVAL_BACKEND" not in os.environ:
    os.environ["REPORTING_RETRIEVAL_BACKEND"] = "lexical"


@pytest.fixture(scope="session")
def invapp():
    # Import lazily so MKC_INVENTORY_DB is set before `init_db()` runs.
    import app as invapp_module

    return invapp_module
