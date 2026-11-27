"""Tests for the HTTP API, with FastAPI's test client on a small synthetic store."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from test_point import RUN, make_store

from api.app import Settings, create_app


@pytest.fixture
def client(tmp_path):
    make_store(tmp_path / "arome")
    return TestClient(create_app(Settings(data_dir=tmp_path)))


def test_models(client):
    r = client.get("/v1/models")
    assert r.status_code == 200
    m = r.json()["models"][0]
    assert m["name"] == "arome" and m["run"] == "2026-11-27T06:00:00Z"
    assert m["domain"]["lat_min"] == pytest.approx(45.0)
    assert m["lead_hours"] == [0, 1, 2, 3]
    assert m["variables"]["t2m"] == "degC" and m["variables"]["orog"] == "m"
    assert "Météo-France" in r.json()["attribution"]


def test_raw_point(client):
    r = client.get("/v1/raw/arome/point", params={"lat": 45.2, "lon": 6.2})
    assert r.status_code == 200
    body = r.json()
    assert body["run"] == "2026-11-27T06:00:00Z"
    assert len(body["times"]) == 4
    t2m = body["variables"]["t2m"]
    assert t2m["units"] == "degC"
    assert t2m["values"][0] == pytest.approx(10 + 20 + 2, abs=0.01)
    assert body["static"]["orog"] == pytest.approx(1000.0)
    assert body["variables"]["rain"]["values"][0] is None
    assert r.headers["cache-control"].startswith("public")


def test_raw_point_variable_selection_and_errors(client):
    r = client.get("/v1/raw/arome/point", params={"lat": 45.2, "lon": 6.2, "variables": "t2m"})
    assert set(r.json()["variables"]) == {"t2m"}
    r = client.get("/v1/raw/arome/point", params={"lat": 45.2, "lon": 6.2, "variables": "zz"})
    assert r.status_code == 422
    assert client.get("/v1/raw/ifs/point", params={"lat": 45.2, "lon": 6.2}).status_code == 404
    r = client.get("/v1/raw/arome/point", params={"lat": 48.8, "lon": 2.3})
    assert r.status_code == 422 and "hors du domaine" in r.json()["detail"]
    assert client.get("/v1/raw/arome/point", params={"lat": 95, "lon": 6.2}).status_code == 422
    r = client.get("/v1/raw/arome/point", params={"lat": 45.2, "lon": 6.2, "method": "cubic"})
    assert r.status_code == 422


def test_forecast_point_with_altitude(client):
    r = client.get("/v1/forecast/point", params={"lat": 45.2, "lon": 6.2, "alt": 1500})
    assert r.status_code == 200
    body = r.json()
    assert body["elevation"] == {"model": 1000.0, "target": 1500.0, "source": "user"}
    temp = body["variables"]["temperature"]["values"][0]
    assert temp == pytest.approx(32.0 - 0.0065 * 500, abs=0.01)
    assert body["variables"]["precipitation_type"]["values"][1] == "rain"
    assert "wind_speed" in body["variables"] and "feels_like" in body["variables"]


def test_forecast_without_altitude_uses_model(client):
    body = client.get("/v1/forecast/point", params={"lat": 45.2, "lon": 6.2}).json()
    assert body["elevation"]["source"] == "model"


def test_etag_and_304(client):
    params = {"lat": 45.2, "lon": 6.2}
    first = client.get("/v1/forecast/point", params=params)
    etag = first.headers["etag"]
    again = client.get("/v1/forecast/point", params=params, headers={"If-None-Match": etag})
    assert again.status_code == 304
    other = client.get("/v1/forecast/point", params={**params, "alt": 10})
    assert other.headers["etag"] != etag


def test_new_run_changes_etag(client, tmp_path):
    from datetime import timedelta

    etag = client.get("/v1/models").headers["etag"]
    make_store(tmp_path / "arome", RUN + timedelta(hours=3))
    r = client.get("/v1/models")
    assert r.json()["models"][0]["run"] == "2026-11-27T09:00:00Z"
    assert r.headers["etag"] != etag


def test_no_run_is_503(tmp_path):
    client = TestClient(create_app(Settings(data_dir=tmp_path)))
    r = client.get("/v1/forecast/point", params={"lat": 45.2, "lon": 6.2})
    assert r.status_code == 503
    assert client.get("/v1/models").json()["models"][0]["run"] is None


def test_openapi_is_served(client):
    assert client.get("/v1/openapi.json").status_code == 200


def test_timing_in_body_and_header(client):
    r = client.get("/v1/forecast/point", params={"lat": 45.2, "lon": 6.2})
    timing = r.json()["timing"]
    assert set(timing) == {"read_ms", "derive_ms", "total_ms"}
    assert all(isinstance(v, float) and v >= 0 for v in timing.values())
    assert timing["total_ms"] >= timing["read_ms"] + timing["derive_ms"] - 0.2
    header = r.headers["server-timing"]
    assert header.startswith("read;dur=") and "derive;dur=" in header and "total;dur=" in header

    raw = client.get("/v1/raw/arome/point", params={"lat": 45.2, "lon": 6.2}).json()
    assert set(raw["timing"]) == {"read_ms", "total_ms"}
    assert set(client.get("/v1/models").json()["timing"]) == {"read_ms", "total_ms"}


def test_timing_does_not_change_etag(client, monkeypatch):
    from api import app as app_module

    params = {"lat": 45.2, "lon": 6.2}
    first = client.get("/v1/raw/arome/point", params=params)
    monkeypatch.setattr(app_module.Timer, "as_json", lambda self: {"total_ms": 9999.0})
    second = client.get("/v1/raw/arome/point", params=params)
    assert second.json()["timing"] != first.json()["timing"]
    assert second.headers["etag"] == first.headers["etag"]


def test_304_skips_the_read(client, monkeypatch):
    from api import app as app_module

    params = {"lat": 45.2, "lon": 6.2}
    etag = client.get("/v1/forecast/point", params=params).headers["etag"]

    def boom(*args, **kwargs):
        raise AssertionError("read_raw called despite a matching ETag")

    monkeypatch.setattr(app_module, "read_raw", boom)
    r = client.get("/v1/forecast/point", params=params, headers={"If-None-Match": etag})
    assert r.status_code == 304 and r.headers["etag"] == etag
    assert "server-timing" not in r.headers
