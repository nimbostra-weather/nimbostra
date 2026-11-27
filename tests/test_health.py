"""Tests for the freshness check, alerts, /v1/health and the deploy files."""

from __future__ import annotations

import importlib
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient
from test_point import RUN, make_store

from api.app import Settings, create_app
from ingest.health import check_freshness, configured_models, main, send_alert

DEPLOY = Path(__file__).resolve().parents[1] / "deploy"


def test_fresh_and_stale(tmp_path):
    make_store(tmp_path)
    fresh = check_freshness(tmp_path, now=RUN + timedelta(hours=5))
    assert fresh.ok and fresh.age_hours == pytest.approx(5.0)
    assert "à jour" in fresh.message()
    stale = check_freshness(tmp_path, now=RUN + timedelta(hours=13))
    assert not stale.ok and "en retard" in stale.message()


def test_no_run_is_not_ok(tmp_path):
    f = check_freshness(tmp_path)
    assert not f.ok and f.run is None and "aucun run" in f.message()


def test_send_alert_posts_text():
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.content.decode())
        return httpx.Response(200)

    http = httpx.Client(transport=httpx.MockTransport(handler))
    assert send_alert("arome : en retard", "https://example.invalid/topic", http)
    assert seen == ["arome : en retard"]
    assert not send_alert("no url", None)


def test_send_alert_failure_is_not_fatal():
    http = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    assert not send_alert("x", "https://example.invalid/topic", http)


def test_cli_exit_codes(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    make_store(tmp_path / "arome", datetime.now(UTC).replace(minute=0, second=0, microsecond=0))
    monkeypatch.setenv("NIMBOSTRA_MODELS", "arome")
    assert main(["check", "--data", str(tmp_path)]) == 0
    assert main(["check", "--data", str(tmp_path / "empty"), "--notify"]) == 1
    assert main(["alert", "échec de test"]) == 0
    assert "aucun run" in capsys.readouterr().out


def test_configured_models():
    assert configured_models({}) == ("arome", "arpege")
    assert configured_models({"NIMBOSTRA_MODELS": " arome , "}) == ("arome",)
    assert configured_models({"NIMBOSTRA_MODELS": ""}) == ("arome", "arpege")


def test_cli_checks_every_model_and_alerts_once(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("NIMBOSTRA_MODELS", raising=False)
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    sent = []
    monkeypatch.setattr("ingest.health.send_alert", lambda text, url: sent.append(text))
    make_store(tmp_path / "arome", datetime.now(UTC).replace(minute=0, second=0, microsecond=0))
    assert main(["check", "--data", str(tmp_path), "--notify"]) == 1
    out = capsys.readouterr().out
    assert "arome : dernier run" in out and "arpege : aucun run" in out
    assert len(sent) == 1 and "arpege" in sent[0] and "arome" not in sent[0]
    assert main(["check", "--data", str(tmp_path), "--model", "arome"]) == 0


def test_health_endpoint(tmp_path, monkeypatch):
    monkeypatch.delenv("ALERT_MAX_AGE_HOURS", raising=False)
    client = TestClient(create_app(Settings(data_dir=tmp_path)))
    r = client.get("/v1/health")
    assert r.status_code == 503 and r.json()["status"] == "stale"
    make_store(tmp_path / "arome", datetime.now(UTC) - timedelta(hours=2))
    r = client.get("/v1/health")
    assert r.status_code == 200 and r.json()["status"] == "ok"
    assert r.json()["models"]["arome"]["age_hours"] == pytest.approx(2.0, abs=0.1)
    assert r.headers["cache-control"] == "no-store"


def test_deploy_units_point_to_real_modules():
    units = list((DEPLOY / "systemd").glob("*.service"))
    assert units
    modules = set()
    for unit in units:
        text = unit.read_text()
        modules |= set(re.findall(r"python -m ([\w.]+)", text))
        assert "uvicorn api.app:app" in text or "python -m" in text
    assert modules == {"ingest.arome_fetch", "ingest.arome_convert", "ingest.health"}
    for name in modules:
        assert hasattr(importlib.import_module(name), "main")
    for timer in (DEPLOY / "systemd").glob("*.timer"):
        assert (timer.with_suffix(".service")).exists()


def test_deploy_files_hold_no_secret_or_address():
    for path in DEPLOY.rglob("*"):
        if path.is_file():
            text = path.read_text()
            assert not re.search(
                r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b", text.replace("127.0.0.1", "")
            ), path
            assert "API_KEY=" not in text and "APPLICATION_ID=" not in text, path
