import json
from pathlib import Path

from reporting import validator


def test_example_plan_is_valid():
    plan_path = Path(__file__).with_name("../reporting/example_plan.json").resolve()
    # fallback path when tests are run from project root
    if not plan_path.exists():
        plan_path = Path("reporting/example_plan.json")

    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    ok, details = validator.validate_plan(plan)
    assert ok, f"Example plan failed validation: {details}"
