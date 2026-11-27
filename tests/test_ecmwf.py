"""Tests for ECMWF IFS open data: lead times, index, range client, conversion, API."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import numpy as np
import pytest
import zarr

eccodes = pytest.importorskip("eccodes")

from fastapi.testclient import TestClient
from test_mf_paquets_zarr import NI, NJ, make_message
from test_point import make_store

from api.app import KNOWN_MODELS, Settings, attribution, create_app
from ingest.ecmwf_fetch import main as fetch_main
from ingest.ecmwf_opendata import (
    IFS_01,
    SOURCES,
    IndexRecord,
    NotPublished,
    OpenDataClient,
    OpenDataError,
    candidate_runs,
    env_sources,
    parse_index,
    select,
)
from ingest.health import max_age_for
from ingest.meteofrance_paquets import RunStage, staged_runs
from ingest.meteofrance_wcs import RateLimiter, RetryPolicy
from ingest.mf_paquets_convert import main as convert_main
from ingest.mf_paquets_zarr import PROFILES, convert_run
from point import derived
from point.reader import derive, read_raw

RUN = datetime(2026, 11, 27, 0, tzinfo=UTC)
AWS = SOURCES["aws"]
ECMWF = SOURCES["ecmwf"]


# --------------------------------------------------------------------------- lead times, URLs


def test_steps_follow_the_open_data_schedule():
    steps = IFS_01.steps(RUN)
    assert steps[:3] == [0, 3, 6] and steps[-6:] == [141, 144, 150, 156, 162, 168]
    assert len(steps) == 53
    short = IFS_01.steps(RUN.replace(hour=6))
    assert short[-1] == 144 and len(short) == 49


def test_steps_respect_a_shorter_horizon():
    from dataclasses import replace

    model = replace(IFS_01, max_lead_hours=24)
    assert model.steps(RUN) == [0, 3, 6, 9, 12, 15, 18, 21, 24]


def test_file_url_at_0p1_has_the_level_suffix():
    url = IFS_01.file_url(AWS, RUN, 24)
    assert url == (
        "https://ecmwf-forecasts.s3.eu-central-1.amazonaws.com/20261127/00z/ifs/0p1/oper/"
        "20261127000000-24h-oper-fc-sfc.grib2"
    )


def test_candidate_runs_are_the_nominal_hours_newest_first():
    now = datetime(2026, 11, 27, 9, 40, tzinfo=UTC)
    assert [r.hour for r in candidate_runs(IFS_01, now, 3)] == [6, 0, 18]


def test_env_sources():
    assert env_sources({}) == ("aws", "ecmwf")
    assert env_sources({"ECMWF_SOURCES": "ecmwf, google"}) == ("ecmwf", "google")


# --------------------------------------------------------------------------- index


def index_line(param, offset, length, levtype="sfc", **extra):
    return json.dumps(
        {"param": param, "levtype": levtype, "_offset": offset, "_length": length, **extra}
    )


def test_parse_index_and_select():
    text = "\n".join(
        [
            index_line("2t", 100, 10),
            index_line("t", 0, 50, levtype="pl", levelist="850"),
            *(index_line(p, 200 + 20 * i, 20) for i, p in enumerate(IFS_01.params[1:])),
            index_line("skt", 900, 5),
        ]
    )
    records = parse_index(text)
    assert records[1] == IndexRecord("t", "pl", 0, 50, levelist="850")
    chosen = select(records, IFS_01, 24)
    assert {r.param for r in chosen} == set(IFS_01.params)
    assert [r.offset for r in chosen] == sorted(r.offset for r in chosen)


def test_select_reports_missing_fields_but_tolerates_accumulations_at_zero():
    recs = [IndexRecord(p, "sfc", i * 10, 10) for i, p in enumerate(IFS_01.params)]
    with pytest.raises(OpenDataError, match="z"):
        select(recs, IFS_01, 0)  # orography is wanted at +0 h
    no_accum = [r for r in recs if r.param not in ("tp", "sf", "10fg")]
    z = IndexRecord("z", "sfc", 999, 10)
    assert {r.param for r in select([*no_accum, z], IFS_01, 0)} == set(IFS_01.params) - {
        "tp",
        "sf",
        "10fg",
    } | {"z"}
    with pytest.raises(OpenDataError, match="tp"):
        select(no_accum, IFS_01, 3)


def test_parse_index_rejects_garbage():
    with pytest.raises(OpenDataError):
        parse_index('{"param": "2t"}')


# --------------------------------------------------------------------------- fake server


def full(value):
    return np.full((NJ, NI), value, dtype=float)


def hag(short, values, step, level, step_type="instant"):
    return make_message(
        short,
        values,
        end_step=step,
        step_type=step_type,
        type_of_level="heightAboveGround",
        level=level,
    )


CUM_TP = {0: 0.0, 3: 0.002, 6: 0.005}  # m since run start
CUM_SF = {0: 0.0, 3: 0.0, 6: 0.001}


def step_messages(step: int) -> dict[str, bytes]:
    """IFS-like surface messages of one lead time, with the open data units."""
    msgs = {
        "2t": hag("2t", full(273.15 + step), step, 2),
        "2d": hag("2d", full(268.15), step, 2),
        "10u": hag("10u", full(3.0), step, 10),
        "10v": hag("10v", full(-4.0), step, 10),
        "msl": make_message("msl", full(101300.0), end_step=step),
        "sp": make_message("sp", full(90000.0), end_step=step, type_of_level="surface"),
        "tcc": make_message("tcc", full(0.5), end_step=step),
        "lcc": make_message("lcc", full(0.25), end_step=step),
        "skt": make_message("skt", full(270.0), end_step=step),  # not wanted
        "tp": make_message("tp", full(CUM_TP[step]), end_step=step, step_type="accum"),
        "sf": make_message("sf", full(CUM_SF[step]), end_step=step, step_type="accum"),
        "10fg": hag("10fg", full(10.0 + step), step, 10, "max"),
    }
    if step == 0:
        msgs["z"] = make_message("z", full(9.80665 * 500), type_of_level="surface")
    return msgs


class FakeMirror:
    """Serves ``.grib2`` files (with Range) and their ``.index``, recording requests."""

    def __init__(self, base: str, steps, *, slow_down: int = 0, missing: set[int] = frozenset()):
        self.base = base
        self.files: dict[str, bytes] = {}
        self.indexes: dict[str, str] = {}
        self.requests: list[tuple[str, str | None]] = []
        self.slow_down = slow_down
        for step in steps:
            if step in missing:
                continue
            url = IFS_01.file_url(base, RUN, step)
            blob, lines = b"", []
            for param, msg in step_messages(step).items():
                lines.append(index_line(param, len(blob), len(msg)))
                blob += msg
            self.files[url] = blob
            self.indexes[url.removesuffix(".grib2") + ".index"] = "\n".join(lines)

    def handle(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        rng = request.headers.get("Range")
        self.requests.append((url, rng))
        if self.slow_down:
            self.slow_down -= 1
            return httpx.Response(503, text="<Code>SlowDown</Code>")
        if url in self.indexes:
            return httpx.Response(200, text=self.indexes[url])
        if url in self.files and rng:
            start, end = map(int, rng.removeprefix("bytes=").split("-"))
            return httpx.Response(206, content=self.files[url][start : end + 1])
        return httpx.Response(404)


def make_client(mirrors: list[FakeMirror], model=IFS_01, **kwargs) -> OpenDataClient:
    def handler(request: httpx.Request) -> httpx.Response:
        for m in mirrors:
            if str(request.url).startswith(m.base):
                return m.handle(request)
        return httpx.Response(404)

    http = httpx.Client(transport=httpx.MockTransport(handler))
    return OpenDataClient(
        model,
        http,
        sources=[m.base for m in mirrors],
        limiter=RateLimiter(1e6, sleep=lambda s: None),
        retry=RetryPolicy(max_attempts=4, base_delay=0.0, max_delay=0.0),
        sleep=lambda s: None,
        workers=2,
        **kwargs,
    )


@pytest.fixture
def short_model():
    from dataclasses import replace

    return replace(IFS_01, max_lead_hours=6)


def test_download_run_stages_only_wanted_messages(tmp_path, short_model):
    aws = FakeMirror(AWS, [0, 3, 6], slow_down=2)
    client = make_client([aws], short_model)
    report = client.download_run(RUN, tmp_path)
    assert report.ok and report.complete
    assert report.fetched == 3 and report.throttled == 2
    # 11 surface messages per lead time, plus the orography at +0 h.
    assert report.messages == 12 + 11 + 11
    stage = RunStage(tmp_path, RUN)
    manifest = stage.read_manifest()
    assert manifest["model"] == "ifs" and manifest["packages"] == {"sfc": ["000H", "003H", "006H"]}
    data = (stage.path / "sfc" / "003H.grib2").read_bytes()
    assert data.count(b"GRIB") == 11 and data.endswith(b"7777")
    # The unwanted message is never requested.
    skt = [r for r in aws.requests if r[1] and "skt" in r[1]]
    assert not skt
    # Rerun: nothing fetched again.
    again = client.download_run(RUN, tmp_path)
    assert again.skipped == 3 and again.fetched == 0


def test_fallback_source_serves_a_step_missing_on_the_first(tmp_path, short_model):
    aws = FakeMirror(AWS, [0, 3, 6], missing={6})
    ecmwf = FakeMirror(ECMWF, [0, 3, 6])
    client = make_client([aws, ecmwf], short_model)
    report = client.download_run(RUN, tmp_path)
    assert report.complete
    assert all(u.startswith(ECMWF) for u, _ in ecmwf.requests)
    assert any("-6h-" in u for u, _ in ecmwf.requests)
    assert not any("-3h-" in u for u, _ in ecmwf.requests)


def test_incomplete_run_has_no_manifest(tmp_path, short_model):
    aws = FakeMirror(AWS, [0, 3, 6], missing={3})
    report = make_client([aws], short_model).download_run(RUN, tmp_path)
    assert not report.ok and not report.complete
    assert report.failed[0][0] == 3
    assert staged_runs(tmp_path) == []


def test_bad_range_is_refetched_then_fails(tmp_path, short_model):
    aws = FakeMirror(AWS, [0])

    def broken(request: httpx.Request) -> httpx.Response:
        if request.headers.get("Range"):
            return httpx.Response(206, content=b"GRIBxx")
        return aws.handle(request)

    http = httpx.Client(transport=httpx.MockTransport(broken))
    client = OpenDataClient(
        short_model,
        http,
        sources=[AWS],
        limiter=RateLimiter(1e6, sleep=lambda s: None),
        sleep=lambda s: None,
    )
    with pytest.raises(OpenDataError, match="bad message twice"):
        client.fetch_step(RUN, 0, tmp_path / "x.grib2")
    assert not (tmp_path / "x.grib2").exists()


def test_latest_published_run_checks_the_last_lead_time(short_model):
    aws = FakeMirror(AWS, [0, 3, 6])
    client = make_client([aws], short_model)
    now = datetime(2026, 11, 27, 11, tzinfo=UTC)  # 06z not on the mirror, 00z is
    assert client.latest_published_run(now) == RUN
    assert aws.requests[0][0].endswith("20261127060000-6h-oper-fc-sfc.index")


def test_not_published_on_404():
    client = make_client([FakeMirror(AWS, [])])
    with pytest.raises(NotPublished):
        client._get(AWS + "/nothing.index")


# --------------------------------------------------------------------------- conversion


def stage_ifs(tmp_path, short_model) -> Path:
    raw = tmp_path / "raw" / "ifs"
    make_client([FakeMirror(AWS, [0, 3, 6])], short_model).download_run(RUN, raw)
    return raw


def test_convert_ifs_run(tmp_path, short_model):
    raw = stage_ifs(tmp_path, short_model)
    profile = PROFILES["ifs"]
    report = convert_run(
        raw,
        RUN,
        tmp_path / "zarr",
        variables=profile.variables,
        static_packages=profile.static_packages,
        required=profile.required,
        bbox=None,
        attrs=profile.attrs,
    )
    assert report.steps == [0, 3, 6]
    root = zarr.open_group(str(report.path), mode="r")
    assert root.attrs["model"] == "ifs" and root.attrs["license"] == "CC BY 4.0"
    assert root.attrs["source"].startswith("ECMWF")
    g = root["surface"]
    assert list(g["time"][:]) == [0, 3 * 3600, 6 * 3600]
    assert set(report.variables) == {
        "t2m",
        "d2m",
        "u10",
        "v10",
        "gust10",
        "precip_total",
        "snow",
        "tcc",
        "lcc",
        "sp",
        "mslp",
        "orog",
    }
    assert g["t2m"][0, 1, 0, 0] == pytest.approx(3.0, abs=1e-3)
    assert g["mslp"][0, 0, 0, 0] == pytest.approx(1013.0, abs=1e-3)
    assert g["tcc"][0, 0, 0, 0] == pytest.approx(50.0, abs=1e-3)
    assert g["orog"][0, 0] == pytest.approx(500.0, abs=0.01)
    # Accumulations since the run start (m) -> mm per step.
    tp = g["precip_total"][0, :, 0, 0]
    assert np.isnan(tp[0]) and tp[1:] == pytest.approx([2.0, 3.0], abs=1e-3)
    sf = g["snow"][0, :, 0, 0]
    assert sf[1:] == pytest.approx([0.0, 1.0], abs=1e-3)
    assert g["snow"].attrs["grib_units"] == "m of water equivalent"
    assert "last hour" in g["gust10"].attrs["description"]
    gust = g["gust10"][0, :, 0, 0]
    assert np.isnan(gust[0]) and gust[1:] == pytest.approx([13.0, 16.0], abs=1e-3)


def test_cli_fetch_then_convert(tmp_path, monkeypatch, short_model):
    # The CLI builds its own client: plug the fake mirror into httpx.
    aws = FakeMirror(AWS, IFS_01.steps(RUN)[:3])
    real_client = httpx.Client

    def fake_client(*args, **kwargs):
        kwargs.pop("follow_redirects", None)
        return real_client(transport=httpx.MockTransport(aws.handle))

    monkeypatch.setattr(httpx, "Client", fake_client)
    monkeypatch.setattr("ingest.ecmwf_opendata.datetime", _FrozenDatetime)
    raw, out = tmp_path / "raw", tmp_path / "zarr"
    args = ["--root", str(raw), "--max-lead", "6", "--sources", "aws", "--rate", "1000"]
    assert fetch_main(["fetch", *args]) == 0
    assert staged_runs(raw / "ifs") == [RUN]
    assert convert_main(["convert", "--model", "ifs", "--raw", str(raw), "--out", str(out)]) == 0
    latest = json.loads((out / "ifs" / "latest.json").read_text())
    assert latest["run"] == "2026-11-27T00:00:00Z"
    # A second fetch finds the run already staged.
    assert fetch_main(["fetch", *args]) == 0


class _FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 11, 27, 11, tzinfo=UTC)


# --------------------------------------------------------------------------- reading, API, health


def test_relative_humidity_inverts_dewpoint():
    t = np.array([-5.0, 10.0, 25.0])
    rh = np.array([40.0, 75.0, 95.0])
    np.testing.assert_allclose(derived.relative_humidity(t, derived.dewpoint(t, rh)), rh, rtol=1e-6)
    assert derived.relative_humidity(np.array([5.0]), np.array([5.0]))[0] == pytest.approx(100.0)


def ifs_extra(plane, nt):
    shape = (1, nt, *plane)
    return {
        "d2m": (np.full(shape, 0.0), "degC"),
        "precip_total": (np.full(shape, 1.0), "mm"),
        "gust10": (np.full(shape, 15.0), "m s-1"),
        "tcc": (np.full(shape, 80.0), "%"),
    }


def make_ifs_store(root):
    from test_point import LATS, LONS, NT

    path = make_store(root, RUN, extra=ifs_extra((LATS.size, LONS.size), NT))
    g = zarr.open_group(str(path), mode="a")["surface"]
    for name in ("r2", "rain", "gust_u10", "gust_v10"):
        del g[name]
    return path


def test_derive_humidity_from_dewpoint(tmp_path):
    path = make_ifs_store(tmp_path)
    g = zarr.open_group(str(path), mode="r")["surface"]
    raw = read_raw(g, "ifs", RUN, 45.2, 6.2)
    out = derive(raw)
    s = out.series
    expected = derived.relative_humidity(raw.series["t2m"], raw.series["d2m"])
    np.testing.assert_allclose(s["humidity"], expected, rtol=1e-5)
    assert "dewpoint" in s and "feels_like" in s
    assert s["wind_gust"][0] == pytest.approx(15.0)


def test_api_serves_ifs_with_ecmwf_attribution(tmp_path):
    make_store(tmp_path / "arome")
    make_ifs_store(tmp_path / "ifs")
    client = TestClient(create_app(Settings(data_dir=tmp_path, models=("arome", "ifs"))))
    models = client.get("/v1/models").json()
    assert [m["name"] for m in models["models"]] == ["arome", "ifs"]
    assert "Météo-France" in models["attribution"] and "ECMWF" in models["attribution"]
    assert "ECMWF" in models["models"][1]["attribution"]
    r = client.get("/v1/forecast/point", params={"lat": 45.2, "lon": 6.2, "model": "ifs"})
    assert r.status_code == 200
    body = r.json()
    assert body["model"] == "ifs"
    assert "European Centre for Medium-Range Weather Forecasts" in body["attribution"]
    assert "© 2026 ECMWF" in body["attribution"] and "CC BY 4.0" in body["attribution"]
    raw = client.get("/v1/raw/ifs/point", params={"lat": 45.2, "lon": 6.2}).json()
    assert "ECMWF" in raw["attribution"] and "d2m" in raw["variables"]
    arome = client.get("/v1/forecast/point", params={"lat": 45.2, "lon": 6.2}).json()
    assert "ECMWF" not in arome["attribution"]


def test_attribution_lists_each_provider_once():
    text = attribution(["arome", "arpege", "ifs"])
    assert text.count("Météo-France") == 1 and text.count("ECMWF)") == 1
    assert KNOWN_MODELS["ifs"].attribution(2027).count("© 2027 ECMWF") == 1


def test_ifs_alert_threshold_has_a_floor():
    assert max_age_for("arome", {}) == 12.0
    assert max_age_for("ifs", {}) == 18.0
    assert max_age_for("ifs", {"ALERT_MAX_AGE_HOURS": "24"}) == 24.0
    assert max_age_for("ifs", {"ALERT_MAX_AGE_HOURS_IFS": "10"}) == 10.0
