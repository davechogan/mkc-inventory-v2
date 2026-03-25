import os
import tempfile
import uuid
from pathlib import Path

import pytest


# Ensure importing `app.py` uses a throwaway DB during unit tests.
_TEST_DB_DIR = Path(tempfile.mkdtemp(prefix="mkc_pytest_db_"))
_TEST_DB_PATH = _TEST_DB_DIR / f"mkc_inventory.test.{uuid.uuid4().hex}.db"
os.environ["MKC_INVENTORY_DB"] = str(_TEST_DB_PATH)


@pytest.fixture(scope="session")
def invapp():
    # Import lazily so MKC_INVENTORY_DB is set before `init_db()` runs.
    import app as invapp_module

    return invapp_module

