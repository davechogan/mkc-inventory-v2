from __future__ import annotations

from fastapi.testclient import TestClient


def test_reporting_retrieval_status_endpoint_returns_runtime_metadata(invapp) -> None:
    client = TestClient(invapp.app)
    res = client.get("/api/reporting/retrieval/status")
    assert res.status_code == 200, res.text
    body = res.json()
    retrieval = body.get("retrieval")
    assert isinstance(retrieval, dict)
    assert isinstance(retrieval.get("artifact_count"), int)
    assert retrieval.get("artifact_count", 0) >= 1
    assert isinstance(retrieval.get("artifact_source"), str)
    assert "configured_backend" in retrieval
    assert retrieval.get("default_backend") == "embedding"
    assert "env_override_active" in retrieval
    assert "vector_index_path" in retrieval
    assert "vector_index_error" in retrieval
    assert "chroma_path" in retrieval
    assert "chroma_collection" in retrieval
    assert "chroma_error" in retrieval


def test_reporting_retrieval_backend_get_and_post(invapp, monkeypatch) -> None:
    monkeypatch.delenv("REPORTING_RETRIEVAL_BACKEND", raising=False)
    client = TestClient(invapp.app)
    g = client.get("/api/reporting/retrieval/backend")
    assert g.status_code == 200, g.text
    data = g.json()
    assert data.get("default_backend") == "embedding"
    assert isinstance(data.get("valid_backends"), list)
    assert "lexical" in data["valid_backends"]
    p = client.post("/api/reporting/retrieval/backend", json={"backend": "chroma"})
    assert p.status_code == 200, p.text
    assert p.json().get("stored_backend") == "chroma"
    assert p.json().get("backend") == "chroma"
    g2 = client.get("/api/reporting/retrieval/backend")
    assert g2.json().get("stored_backend") == "chroma"


def test_reporting_retrieval_reload_endpoint_returns_status(invapp) -> None:
    client = TestClient(invapp.app)
    res = client.post("/api/reporting/retrieval/reload")
    assert res.status_code == 200, res.text
    body = res.json()
    retrieval = body.get("retrieval")
    assert isinstance(retrieval, dict)
    assert retrieval.get("reloaded") is True
    assert isinstance(retrieval.get("artifact_count"), int)
    assert "vector_index_path" in retrieval
    assert "chroma_path" in retrieval

