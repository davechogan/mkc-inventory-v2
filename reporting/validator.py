"""Reporting plan validator utilities.

Provides a small wrapper around jsonschema to validate plan documents
against the canonical reporting_plan.schema.json. This module intentionally
keeps runtime dependencies minimal and provides a clear ValidationError
type for callers in the reporting pipeline.
"""
from pathlib import Path
import json
from typing import Any, Dict, Tuple

try:
    from jsonschema import Draft7Validator, exceptions as jsonschema_exceptions
except Exception:  # pragma: no cover - fallback import message
    Draft7Validator = None
    jsonschema_exceptions = None


SCHEMA_PATH = Path(__file__).with_name("reporting_plan.schema.json")


class ValidationError(Exception):
    pass


def load_schema() -> Dict[str, Any]:
    with SCHEMA_PATH.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def validate_plan(plan: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """Validate a reporting plan dict against the canonical schema.

    Returns (is_valid, details). If invalid, details contains "errors": [..].
    """
    if Draft7Validator is None:
        raise ValidationError("jsonschema is not installed; cannot validate plans")

    schema = load_schema()
    validator = Draft7Validator(schema)
    errors = []
    for err in validator.iter_errors(plan):
        # Convert to a simple message
        path = ".".join([str(p) for p in err.path]) if err.path else "<root>"
        errors.append({"path": path, "message": err.message})

    if errors:
        return False, {"errors": errors}
    return True, {"errors": []}


def ensure_plan_or_raise(plan: Dict[str, Any]) -> None:
    ok, details = validate_plan(plan)
    if not ok:
        raise ValidationError(json.dumps(details))


if __name__ == "__main__":
    # simple CLI for manual validation
    import sys

    if len(sys.argv) < 2:
        print("Usage: validator.py plan.json")
        raise SystemExit(2)

    plan_path = Path(sys.argv[1])
    with plan_path.open("r", encoding="utf-8") as fh:
        plan = json.load(fh)

    ok, details = validate_plan(plan)
    if ok:
        print("OK: plan is valid")
        raise SystemExit(0)
    else:
        print("INVALID PLAN:")
        print(json.dumps(details, indent=2))
        raise SystemExit(3)
