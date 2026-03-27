import os
import tempfile
import uuid
from pathlib import Path

import pytest


# Ensure importing `app.py` uses a throwaway DB during unit tests.
_TEST_DB_DIR = Path(tempfile.mkdtemp(prefix="mkc_pytest_db_"))
_TEST_DB_PATH = _TEST_DB_DIR / f"mkc_inventory.test.{uuid.uuid4().hex}.db"
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

