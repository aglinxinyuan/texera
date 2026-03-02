import os
import sys
from types import SimpleNamespace

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

import src.udf_compiling_service as service
from src.compiler import BASELINE_REFERENCE_UDF, RECOMMENDED_AUTO_CUT_UDF


def test_post_compile_success(monkeypatch):
    monkeypatch.setattr(service, "infer_line_number_from_code", lambda _code: 7)
    monkeypatch.setattr(
        service, "compile_udf", lambda code, line_number: SimpleNamespace(operator_class=f"compiled:{line_number}:{code}")
    )

    client = service.app.test_client()
    resp = client.post("/compile", json={"code": "print('x')"})

    assert resp.status_code == 200
    assert resp.get_data(as_text=True) == "compiled:7:print('x')"


def test_post_compile_bad_request():
    client = service.app.test_client()

    resp = client.post("/compile", json={})
    assert resp.status_code == 400
    assert "No JSON data provided" in resp.get_data(as_text=True)

    resp2 = client.post("/compile", json={"x": 1})
    assert resp2.status_code == 400
    assert "No code provided in request" in resp2.get_data(as_text=True)

    resp3 = client.post("/compile", json={"code": 123})
    assert resp3.status_code == 400
    assert "Code must be a string" in resp3.get_data(as_text=True)


def test_get_compile_success(monkeypatch):
    monkeypatch.setattr(service, "infer_line_number_from_code", lambda _code: None)
    monkeypatch.setattr(
        service, "compile_udf", lambda code, line_number: SimpleNamespace(operator_class=f"compiled:{line_number}:{code}")
    )

    client = service.app.test_client()
    resp = client.get("/compile", query_string={"code": "a=1"})

    assert resp.status_code == 200
    assert resp.get_data(as_text=True) == "compiled:None:a=1"


def test_get_compile_bad_request():
    client = service.app.test_client()
    resp = client.get("/compile")
    assert resp.status_code == 400
    assert "No code provided in query parameter" in resp.get_data(as_text=True)


def test_post_compile_internal_error(monkeypatch):
    monkeypatch.setattr(service, "compile_udf", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    client = service.app.test_client()
    resp = client.post("/compile", json={"code": "a=1"})
    assert resp.status_code == 500
    assert "Internal server error: boom" in resp.get_data(as_text=True)


def test_get_compile_internal_error(monkeypatch):
    monkeypatch.setattr(service, "compile_udf", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    client = service.app.test_client()
    resp = client.get("/compile", query_string={"code": "a=1"})
    assert resp.status_code == 500
    assert "Internal server error: boom" in resp.get_data(as_text=True)


def test_example_endpoint_uses_canonical_use_cases():
    client = service.app.test_client()
    resp = client.get("/example")

    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["examples"]["example_auto"]["code"] == RECOMMENDED_AUTO_CUT_UDF
    assert payload["examples"]["example_baseline"]["code"] == BASELINE_REFERENCE_UDF
