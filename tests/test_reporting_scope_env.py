"""Unit tests for reporting env parsing (no server, no live DB)."""

import pytest

from reporting.domain import reporting_scope_preprocessing_enabled


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, False),
        ("", False),
        ("  ", False),
        ("0", False),
        ("false", False),
        ("no", False),
        ("off", False),
        ("1", True),
        ("true", True),
        ("TRUE", True),
        ("yes", True),
        ("on", True),
        ("bogus", False),
    ],
)
def test_reporting_scope_preprocessing_parsing(raw, expected):
    assert reporting_scope_preprocessing_enabled(raw) is expected
