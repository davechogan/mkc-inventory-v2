import os
import shutil
import tempfile
import uuid
from pathlib import Path

import pytest

_FIXTURES_DIR = Path(__file__).parent / "fixtures"
_SEED_DB = _FIXTURES_DIR / "mkc_inventory_seed.db"

# Ensure importing `app.py` uses a throwaway DB during unit tests.
# The DB is seeded from tests/fixtures/mkc_inventory_seed.db — a known-good
# snapshot committed to the repo. Update the snapshot deliberately when the
# catalog or inventory changes in a way that should be reflected in tests.
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
