"""Round-trip CRUD tests for /api/v2/inventory (isolated test DB, cleanup in finally)."""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def any_knife_model_id(invapp) -> int:
    """
    A real knife_models_v2.id. Uses catalog when seeded; otherwise inserts a minimal v2 row
    so tests do not depend on CSV/migration populating the catalog (CI-safe).
    """
    with invapp.get_conn() as conn:
        row = conn.execute("SELECT id FROM knife_models_v2 ORDER BY id LIMIT 1").fetchone()
        if row:
            return int(row["id"])
        slug = f"pytest-crud-{uuid.uuid4().hex[:12]}"
        cur = conn.execute(
            """
            INSERT INTO knife_models_v2
                (official_name, normalized_name, sortable_name, slug, record_status)
            VALUES (?, ?, ?, ?, 'active')
            """,
            ("Pytest CRUD Model", "pytest crud model", "pytest crud model", slug),
        )
        return int(cur.lastrowid)


def _row_to_put_payload(row: dict[str, Any]) -> dict[str, Any]:
    """Map GET /api/v2/inventory row shape to InventoryItemV2In (PUT body)."""
    return {
        "knife_model_id": row["knife_model_id"],
        "nickname": row.get("nickname"),
        "quantity": row["quantity"],
        "acquired_date": row.get("acquired_date"),
        "mkc_order_number": row.get("mkc_order_number"),
        "purchase_price": row.get("purchase_price"),
        "estimated_value": row.get("estimated_value"),
        "condition": row.get("condition") or "Like New",
        "handle_color": row.get("handle_color"),
        "steel": row.get("blade_steel"),
        "blade_finish": row.get("blade_finish"),
        "blade_color": row.get("blade_color"),
        "blade_length": row.get("blade_length"),
        "collaboration_name": row.get("collaboration_name"),
        "serial_number": row.get("serial_number"),
        "location": row.get("location"),
        "purchase_source": row.get("purchase_source"),
        "last_sharpened": row.get("last_sharpened"),
        "notes": row.get("notes"),
    }


@pytest.fixture
def client(invapp):
    return TestClient(invapp.app)


def test_v2_inventory_crud_roundtrip(client: TestClient, any_knife_model_id: int) -> None:
    knife_model_id = any_knife_model_id
    tag = uuid.uuid4().hex[:10]
    create_body = {
        "knife_model_id": knife_model_id,
        "nickname": f"pytest-inventory-{tag}",
        "quantity": 1,
        "notes": "crud-create",
        "condition": "Like New",
    }

    item_id: int | None = None
    try:
        r_create = client.post("/api/v2/inventory", json=create_body)
        assert r_create.status_code == 200, r_create.text
        created = r_create.json()
        assert "id" in created
        item_id = int(created["id"])

        r_list = client.get("/api/v2/inventory")
        assert r_list.status_code == 200
        rows = r_list.json()
        row = next((x for x in rows if x["id"] == item_id), None)
        assert row is not None
        assert row["nickname"] == create_body["nickname"]
        assert row["notes"] == "crud-create"

        put_payload = _row_to_put_payload(row)
        put_payload["notes"] = "crud-updated"
        r_put = client.put(f"/api/v2/inventory/{item_id}", json=put_payload)
        assert r_put.status_code == 200, r_put.text

        r_list2 = client.get("/api/v2/inventory")
        assert r_list2.status_code == 200
        row2 = next((x for x in r_list2.json() if x["id"] == item_id), None)
        assert row2 is not None
        assert row2["notes"] == "crud-updated"

        r_del = client.delete(f"/api/v2/inventory/{item_id}")
        assert r_del.status_code == 200, r_del.text
        item_id = None

        r_list3 = client.get("/api/v2/inventory")
        assert r_list3.status_code == 200
        assert not any(x["id"] == created["id"] for x in r_list3.json())
    finally:
        if item_id is not None:
            client.delete(f"/api/v2/inventory/{item_id}")


def test_v2_inventory_create_rejects_unknown_model(client: TestClient) -> None:
    r = client.post(
        "/api/v2/inventory",
        json={"knife_model_id": 999999999, "nickname": "nope", "quantity": 1},
    )
    assert r.status_code == 400, r.text
    assert "knife model" in (r.json().get("detail") or "").lower()
