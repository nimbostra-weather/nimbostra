"""Tests for the AROME-PE ingestion, on simulated WCS responses only (no network)."""

from __future__ import annotations

import io
import json
import random
import threading
from datetime import UTC, datetime

import httpx
import numpy as np
import pytest
import zarr

from ingest import arome_pe
from ingest.arome_pe import (
    AROME_PE_VARIABLES,
    RESOLUTION_FILE,
    CoverageKey,
    Resolution,
    build_plan,
    canary,
    convert_run,
    deaccumulate,
    default_steps,
    discover,
    download_run,
    latest_complete_runs,
    offline_resolution,
    parse_capabilities,
    parse_coverage_id,
    parse_members,
    parse_time_axis,
    prune,
    resolve,
    to_canonical,
)
from ingest.meteofrance_wcs import (
    AROME_PE,
    ApiKeyAuth,
    BBox,
    RateLimiter,
    RawStore,
    RetryPolicy,
    WcsClient,
)
from ingest.zarr_writer import DecodedField

RUN = datetime(2026, 11, 27, 9, tzinfo=UTC)
RUN_TXT = "2026-11-27T09.00.00Z"
OLD_RUN_TXT = "2026-11-27T03.00.00Z"
LATS = np.arange(44.0, 46.0, 0.25)  # 8
LONS = np.arange(2.0, 5.0, 0.25)  # 12

AVAILABLE = [
    "TEMPERATURE__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND",
    "RELATIVE_HUMIDITY__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND",
    "U_COMPONENT_OF_WIND__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND",
    "V_COMPONENT_OF_WIND__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND",
    "U_COMPONENT_OF_WIND_GUST__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND",
    "V_COMPONENT_OF_WIND_GUST__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND",
    "TOTAL_PRECIPITATION__GROUND_OR_WATER_SURFACE_PT1H",
    "TOTAL_PRECIPITATION__GROUND_OR_WATER_SURFACE_PT3H",
    "LOW_CLOUD_COVER__GROUND_OR_WATER_SURFACE",
    "PRESSURE__MEAN_SEA_LEVEL",
    "GEOPOTENTIAL__ISOBARIC_SURFACE",  # not a surface orography
]


def capabilities_xml(runs=(RUN_TXT,), available=AVAILABLE) -> str:
    ids = []
    for run in runs:
        for item in available:
            if item.endswith(("_PT1H", "_PT3H")):
                ind, iv = item.rsplit("_", 1)
                ids.append(f"{ind}___{run}_{iv}")
            else:
                ids.append(f"{item}___{run}")
    summaries = "".join(
        f"<wcs:CoverageSummary><wcs:CoverageId>{cid}</wcs:CoverageId>"
        f"<ows:Title>x</ows:Title></wcs:CoverageSummary>"
        for cid in ids
    )
    return (
        '<?xml version="1.0"?><wcs:Capabilities xmlns:wcs="http://www.opengis.net/wcs/2.0" '
        'xmlns:ows="http://www.opengis.net/ows/2.0"><wcs:Contents>'
        f"{summaries}</wcs:Contents></wcs:Capabilities>"
    )


def describe_xml(hours) -> str:
    coeffs = " ".join(str(h * 3600) for h in hours)
    return (
        '<wcs:CoverageDescriptions xmlns:wcs="http://www.opengis.net/wcs/2.0" '
        'xmlns:gml="http://www.opengis.net/gml/3.2" '
        'xmlns:gmlrgrid="http://www.opengis.net/gml/3.3/rgrid">'
        "<wcs:CoverageDescription><gml:domainSet><gmlrgrid:ReferenceableGridByVectors>"
        "<gmlrgrid:generalGridAxis><gmlrgrid:GeneralGridAxis>"
        "<gmlrgrid:offsetVector>0 0 1</gmlrgrid:offsetVector>"
        "<gmlrgrid:coefficients>0 2 10</gmlrgrid:coefficients>"
        "<gmlrgrid:gridAxesSpanned>height</gmlrgrid:gridAxesSpanned>"
        "</gmlrgrid:GeneralGridAxis></gmlrgrid:generalGridAxis>"
        "<gmlrgrid:generalGridAxis><gmlrgrid:GeneralGridAxis>"
        f"<gmlrgrid:coefficients>{coeffs}</gmlrgrid:coefficients>"
        "<gmlrgrid:gridAxesSpanned>time</gmlrgrid:gridAxesSpanned>"
        "</gmlrgrid:GeneralGridAxis></gmlrgrid:generalGridAxis>"
        "</gmlrgrid:ReferenceableGridByVectors></gml:domainSet>"
        "</wcs:CoverageDescription></wcs:CoverageDescriptions>"
    )


# A fake "GRIB": magic bytes + an npy array. The fake decoder reads it back.
def fake_grib(value: float) -> bytes:
    buf = io.BytesIO()
    np.save(buf, np.full((LATS.size, LONS.size), value, dtype=np.float32))
    return b"GRIB" + buf.getvalue()


def fake_decoder(data: bytes) -> DecodedField:
    assert data.startswith(b"GRIB")
    return DecodedField(np.load(io.BytesIO(data[4:])), LATS, LONS)


def field_value(request: httpx.Request) -> float:
    """Deterministic value per member and lead time (precip cumulates if no interval)."""
    member = int(request.url.path.split("PEARO")[1][:3])
    step = next(
        int(s[5:-1]) for s in request.url.params.get_list("subset") if s.startswith("time(")
    )
    return member * 100 + step / 3600


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self._lock = threading.Lock()

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        with self._lock:
            self.now += seconds


class FakeWcs:
    """Simulated Météo-France WCS: capabilities, describe, unit GetCoverage."""

    def __init__(self, hours=range(52), missing=lambda r: False, available=AVAILABLE):
        self.hours = list(hours)
        self.missing = missing
        self.available = available
        self.calls: list[httpx.Request] = []
        self._lock = threading.Lock()

    def __call__(self, request: httpx.Request) -> httpx.Response:
        with self._lock:
            self.calls.append(request)
        op = request.url.path.rsplit("/", 1)[-1]
        if op == "GetCapabilities":
            return httpx.Response(
                200, text=capabilities_xml((RUN_TXT, OLD_RUN_TXT), self.available)
            )
        if op == "DescribeCoverage":
            return httpx.Response(200, text=describe_xml(self.hours))
        assert op == "GetCoverage"
        params = request.url.params
        assert params["format"] == "application/wmo-grib"
        subsets = params.get_list("subset")
        assert sum(s.startswith("time(") for s in subsets) == 1  # unit calls only
        if self.missing(request):
            return httpx.Response(404, text="Synopsis backend error")
        return httpx.Response(200, content=fake_grib(field_value(request)))

    def count(self, op: str) -> int:
        return sum(r.url.path.endswith(op) for r in self.calls)


def make_client(handler) -> WcsClient:
    clock = FakeClock()
    return WcsClient(
        AROME_PE,
        ApiKeyAuth("test-key"),
        httpx.Client(transport=httpx.MockTransport(handler)),
        bbox=BBox(44.0, 46.0, 2.0, 5.0),
        limiter=RateLimiter(10.0, clock=clock.clock, sleep=clock.sleep),
        retry=RetryPolicy(max_attempts=3, base_delay=0.1),
        sleep=clock.sleep,
        rng=random.Random(0),
    )


# --------------------------------------------------------------------------- parsing


def test_parse_coverage_id_with_and_without_interval():
    k = parse_coverage_id(f"TOTAL_PRECIPITATION__GROUND_OR_WATER_SURFACE___{RUN_TXT}_PT1H")
    assert k == CoverageKey("TOTAL_PRECIPITATION__GROUND_OR_WATER_SURFACE", RUN, "PT1H")
    k = parse_coverage_id(f"PRESSURE__MEAN_SEA_LEVEL___{RUN_TXT}")
    assert k.interval is None and k.run == RUN
    assert parse_coverage_id("garbage") is None
    assert parse_coverage_id("X___not-a-date") is None


def test_parse_capabilities_and_time_axis():
    keys = parse_capabilities(capabilities_xml())
    assert len(keys) == len(AVAILABLE)
    assert {k.run for k in keys} == {RUN}
    assert parse_time_axis(describe_xml([0, 1, 2, 51])) == [0, 3600, 7200, 51 * 3600]
    with pytest.raises(ValueError):
        parse_time_axis("<a><b/></a>")


def test_resolve_prefers_pt1h_and_reports_unresolved():
    res = resolve(parse_capabilities(capabilities_xml()), RUN)
    assert res.intervals["precip"] == "PT1H"
    assert res.intervals["gust_u10"] is None
    assert res.indicators["gust_v10"].startswith("V_COMPONENT_OF_WIND_GUST")
    assert res.unresolved == ["snow"]  # not in the simulated capabilities
    assert res.steps_seconds == default_steps(RUN)
    assert "snow" not in {s.name for s, _ in res.variables()}


def test_resolve_falls_back_to_second_candidate_and_cumulative_precip():
    available = [
        a for a in AVAILABLE if not a.startswith(("U_COMPONENT_OF_WIND_G", "TOTAL_PRECIP"))
    ]
    available += [
        "U_COMPONENT_OF_WIND_GUST__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND_PT1H",
        "TOTAL_PRECIPITATION__GROUND_OR_WATER_SURFACE",
    ]
    res = resolve(parse_capabilities(capabilities_xml(available=available)), RUN)
    assert res.intervals["gust_u10"] == "PT1H"
    assert res.intervals["precip"] is None


def test_latest_complete_runs_ignores_runs_without_required_variables():
    keys = parse_capabilities(capabilities_xml((RUN_TXT, OLD_RUN_TXT)))
    partial = [
        k
        for k in parse_capabilities(capabilities_xml(("2026-11-27T15.00.00Z",)))
        if not k.indicator.startswith("LOW_CLOUD")
    ]
    runs = latest_complete_runs(keys + partial)
    assert runs == [RUN, datetime(2026, 11, 27, 3, tzinfo=UTC)]


def test_default_lead_times_depend_on_run_hour():
    assert default_steps(RUN)[-1] == 51 * 3600
    assert default_steps(RUN.replace(hour=3))[-1] == 45 * 3600
    assert len(default_steps(RUN.replace(hour=15))) == 46


def test_parse_members():
    assert parse_members("0,1,5-7") == [0, 1, 5, 6, 7]


# --------------------------------------------------------------------------- plan and budget


def test_full_run_call_count_matches_budget_estimate():
    res = offline_resolution(RUN)
    plan = build_plan(res)
    n_inst = sum(1 for v in AROME_PE_VARIABLES if v.kind == "instant")  # 6
    n_acc = sum(1 for v in AROME_PE_VARIABLES if v.kind in ("accum", "max"))  # 4
    assert (n_inst, n_acc) == (6, 4)
    # lcc has no field at lead time 0 (404 seen on 2026-11-27).
    assert len(plan.fields) == 25 * (n_inst * 52 + n_acc * 51 - 1)
    assert plan.static == []  # no orography in the AROME-PE WCS
    assert len(plan) == 12_875
    assert len(build_plan(offline_resolution(RUN.replace(hour=3)))) == 25 * (6 * 46 + 4 * 45 - 1)
    # Member-major: member 0 comes entirely first, so a partial run is usable.
    first_m1 = next(i for i, r in enumerate(plan.fields) if r.member == 1)
    assert all(r.member == 0 for r in plan.fields[:first_m1])


def test_plan_no_lead_zero_for_accumulations_and_maxima():
    plan = build_plan(offline_resolution(RUN), members=[0], max_lead_hours=2)
    by_var: dict[str, list[int]] = {}
    for r in plan.fields:
        by_var.setdefault(r.variable.name, []).append(r.step_seconds)
    assert by_var["t2m"] == [0, 3600, 7200]
    assert by_var["precip"] == [3600, 7200]
    assert by_var["gust_u10"] == [3600, 7200]
    assert by_var["lcc"] == [3600, 7200]


def test_requests_are_unit_calls_with_height_and_interval():
    plan = build_plan(offline_resolution(RUN), members=[3], max_lead_hours=1)
    t2m = next(r for r in plan.fields if r.variable.name == "t2m" and r.step_seconds == 3600)
    params = dict(t2m.params(None))
    assert params["coverageid"] == f"TEMPERATURE__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND___{RUN_TXT}"
    assert "PEARO003" in t2m.url
    subsets = [v for k, v in t2m.params(None) if k == "subset"]
    assert subsets == ["height(2)", "time(3600)"]
    tp = next(r for r in plan.fields if r.variable.name == "precip")
    assert dict(tp.params(None))["coverageid"].endswith("Z_PT1H")


# --------------------------------------------------------------------------- units and accumulations


def test_to_canonical_units():
    v = np.array([273.15, 283.15], dtype=np.float32)
    out, unit = to_canonical(v, "K")
    assert unit == "degC" and out == pytest.approx([0.0, 10.0])
    assert to_canonical(np.array([101325.0]), "Pa")[0] == pytest.approx([1013.25])
    assert to_canonical(np.array([0.5]), "(0 - 1)") == (pytest.approx([50.0]), "%")
    assert to_canonical(np.array([9.80665]), "m**2 s**-2")[0] == pytest.approx([1.0])
    assert to_canonical(np.array([3.0]), "kg m**-2")[1] == "mm"
    with pytest.raises(ValueError):
        to_canonical(np.array([1.0]), "furlongs")


def test_deaccumulate_per_member():
    steps = [0, 3600, 7200, 10800]
    cum = np.array(
        [
            [[0.0], [1.0], [1.5], [4.0]],
            [[0.0], [0.0], [np.nan], [2.0]],
        ],
        dtype=np.float32,
    )
    out = deaccumulate(cum, steps)
    assert np.isnan(out[:, 0]).all()
    np.testing.assert_allclose(out[0, 1:, 0], [1.0, 0.5, 2.5])
    assert out[1, 1, 0] == 0.0 and np.isnan(out[1, 2, 0]) and np.isnan(out[1, 3, 0])
    # Without lead time 0, the first step is taken from the start.
    np.testing.assert_allclose(deaccumulate(cum[:, 1:], steps[1:])[0, :, 0], [1.0, 0.5, 2.5])
    # Negative packing noise is clipped.
    assert deaccumulate(np.array([[[1.0], [0.99]]], dtype=np.float32), [3600, 7200])[0, 1] == 0


# --------------------------------------------------------------------------- discovery and download


def test_discover_costs_two_calls_and_reads_time_axis():
    wcs = FakeWcs(hours=range(49))
    client = make_client(wcs)
    res = discover(client)
    assert res.run == RUN  # the latest run with all required variables
    assert res.steps_source == "describe" and res.steps_seconds[-1] == 48 * 3600
    assert client.http_calls == 2
    assert wcs.count("GetCapabilities") == 1 and wcs.count("DescribeCoverage") == 1
    assert "PEARO000" in wcs.calls[0].url.path


def test_canary_detects_incomplete_run(tmp_path):
    last_missing = FakeWcs(missing=lambda r: "PEARO024" in r.url.path)
    client = make_client(last_missing)
    res = offline_resolution(RUN)
    store = RawStore(tmp_path)
    assert canary(client, res, store) is False
    assert last_missing.count("GetCoverage") == 2
    ok = FakeWcs()
    client = make_client(ok)
    assert canary(client, res, store) is True
    assert ok.count("GetCoverage") == 1  # member 0 already staged by the first canary


def test_download_resumes_without_spending_calls(tmp_path):
    wcs = FakeWcs()
    client = make_client(wcs)
    plan = build_plan(offline_resolution(RUN), members=[0, 1], max_lead_hours=2)
    store = RawStore(tmp_path)

    rep = download_run(client, plan, store, workers=3)
    assert rep.ok and rep.fetched == len(plan) and rep.http_calls == len(plan)

    again = download_run(client, plan, store, workers=3)
    assert again.ok and again.fetched == 0 and again.http_calls == 0
    assert wcs.count("GetCoverage") == len(plan)


def test_download_refuses_plan_over_budget(tmp_path):
    wcs = FakeWcs()
    plan = build_plan(offline_resolution(RUN))
    rep = download_run(make_client(wcs), plan, RawStore(tmp_path), budget=1000)
    assert not rep.ok and "budget" in rep.aborted
    assert wcs.calls == []


def test_circuit_breaker_stops_after_a_mostly_missing_member(tmp_path):
    # Every field of every member is missing (e.g. run withdrawn): only member 0 is tried.
    wcs = FakeWcs(missing=lambda r: True)
    plan = build_plan(offline_resolution(RUN), members=[0, 1, 2], max_lead_hours=3)
    rep = download_run(make_client(wcs), plan, RawStore(tmp_path), workers=2)
    assert not rep.ok and "membre 0" in rep.aborted
    per_member = len([r for r in plan.fields if r.member == 0])
    assert wcs.count("GetCoverage") == per_member + len(plan.static)


def test_download_stops_on_auth_error(tmp_path):
    def forbidden(request):
        return httpx.Response(403, text="forbidden")

    plan = build_plan(offline_resolution(RUN), members=[0, 1], max_lead_hours=1)
    rep = download_run(make_client(forbidden), plan, RawStore(tmp_path), workers=1)
    assert rep.aborted == "erreur d'authentification"


# --------------------------------------------------------------------------- conversion


def stage_run(tmp_path, res: Resolution, members, max_lead):
    wcs = FakeWcs()
    client = make_client(wcs)
    run_dir = tmp_path / "raw" / arome_pe.run_tag(res.run)
    plan = build_plan(res, members, max_lead)
    store = RawStore(run_dir)
    assert download_run(client, plan, store, workers=2).ok
    meta = json.loads(res.to_json())
    meta["members"] = members
    meta["max_lead_hours"] = max_lead
    (run_dir / RESOLUTION_FILE).write_text(json.dumps(meta))
    return run_dir


def test_convert_run_layout_units_and_attributes(tmp_path):
    res = resolve(parse_capabilities(capabilities_xml()), RUN)
    run_dir = stage_run(tmp_path, res, [0, 1], 3)
    out = convert_run(run_dir, tmp_path / "zarr", decoder=fake_decoder, bbox=None)

    assert out.name == "20261127T09Z.zarr"
    root = zarr.open_group(str(out), mode="r")
    assert root.attrs["unresolved"] == ["snow"]
    t2m = root["surface/t2m"]
    assert t2m.shape == (2, 4, 8, 12)
    assert t2m.chunks == (2, 4, 8, 8)
    assert t2m.attrs["units"] == "degC"
    assert t2m[1, 2, 0, 0] == pytest.approx(102.0)
    # PT1H precipitation is already per step; lead time 0 has no field.
    tp = root["surface/precip"]
    assert np.isnan(tp[:, 0]).all()
    assert tp[0, 3, 0, 0] == pytest.approx(3.0)
    assert tp.attrs["interval"] == "PT1H"
    assert root["surface/gust_u10"].attrs["kind"] == "max"
    assert "orog" not in root["surface"]
    assert list(root["surface/time"][:]) == [0, 3600, 7200, 10800]
    latest = json.loads((tmp_path / "zarr" / "latest.json").read_text())
    assert latest["path"] == "20261127T09Z.zarr"


def test_convert_deaccumulates_cumulative_precipitation(tmp_path):
    res = resolve(parse_capabilities(capabilities_xml()), RUN)
    res.intervals["precip"] = None  # pretend the coverage is cumulated since the start
    run_dir = stage_run(tmp_path, res, [1], 3)
    out = convert_run(run_dir, tmp_path / "zarr", decoder=fake_decoder, bbox=None)
    tp = zarr.open_group(str(out), mode="r")["surface/precip"]
    # Simulated cumulative values for member 1: 101, 102, 103 -> 101 from start, then 1, 1.
    np.testing.assert_allclose(tp[0, 1:, 0, 0], [101.0, 1.0, 1.0])


def test_prune_keeps_latest_runs(tmp_path):
    for tag in ("20261126T21Z", "20261127T03Z", "20261127T09Z"):
        (tmp_path / f"{tag}.zarr").mkdir()
    (tmp_path / "latest.json").write_text("{}")
    removed = prune(tmp_path, 2, suffix=".zarr")
    assert [p.name for p in removed] == ["20261126T21Z.zarr"]
    assert (tmp_path / "latest.json").exists()


def test_cli_offline_plan_makes_no_call(capsys, monkeypatch):
    def boom(*a, **k):
        raise AssertionError("network used")

    monkeypatch.setattr(httpx.Client, "send", boom)
    assert arome_pe.main(["plan", "--offline", "--run", "20261127T09Z"]) == 0
    out = capsys.readouterr().out
    assert "12875 appels" in out


def test_decode_canonical_eccodes_converts_kelvin():
    eccodes = pytest.importorskip("eccodes")
    gid = eccodes.codes_grib_new_from_samples("regular_ll_sfc_grib2")
    try:
        eccodes.codes_set(gid, "Ni", 2)
        eccodes.codes_set(gid, "Nj", 2)
        eccodes.codes_set_double(gid, "latitudeOfFirstGridPointInDegrees", 46.0)
        eccodes.codes_set_double(gid, "latitudeOfLastGridPointInDegrees", 45.0)
        eccodes.codes_set_double(gid, "longitudeOfFirstGridPointInDegrees", 2.0)
        eccodes.codes_set_double(gid, "longitudeOfLastGridPointInDegrees", 3.0)
        eccodes.codes_set_double(gid, "iDirectionIncrementInDegrees", 1.0)
        eccodes.codes_set_double(gid, "jDirectionIncrementInDegrees", 1.0)
        eccodes.codes_set(gid, "shortName", "2t")
        eccodes.codes_set_values(gid, np.array([273.15, 283.15, 293.15, 303.15]))
        msg = eccodes.codes_get_message(gid)
    finally:
        eccodes.codes_release(gid)
    assert arome_pe.grib_info_eccodes(msg).units == "K"
    f = arome_pe.decode_canonical_eccodes(msg)
    # North row first in the GRIB, ascending latitudes after decoding.
    np.testing.assert_allclose(f.values, [[20.0, 30.0], [0.0, 10.0]], atol=1e-3)


def _real_grib(short_name: str, values: np.ndarray) -> bytes:
    eccodes = pytest.importorskip("eccodes")
    gid = eccodes.codes_grib_new_from_samples("regular_ll_sfc_grib2")
    try:
        eccodes.codes_set(gid, "Ni", LONS.size)
        eccodes.codes_set(gid, "Nj", LATS.size)
        eccodes.codes_set_double(gid, "latitudeOfFirstGridPointInDegrees", LATS[-1])
        eccodes.codes_set_double(gid, "latitudeOfLastGridPointInDegrees", LATS[0])
        eccodes.codes_set_double(gid, "longitudeOfFirstGridPointInDegrees", LONS[0])
        eccodes.codes_set_double(gid, "longitudeOfLastGridPointInDegrees", LONS[-1])
        eccodes.codes_set_double(gid, "iDirectionIncrementInDegrees", 0.25)
        eccodes.codes_set_double(gid, "jDirectionIncrementInDegrees", 0.25)
        eccodes.codes_set(gid, "shortName", short_name)
        eccodes.codes_set_values(gid, values[::-1].ravel())  # north row first
        return eccodes.codes_get_message(gid)
    finally:
        eccodes.codes_release(gid)


def test_convert_adds_orography_and_land_sea_mask_from_constants_file(tmp_path):
    shape = (LATS.size, LONS.size)
    orog = np.arange(LATS.size * LONS.size, dtype=float).reshape(shape)
    constants = tmp_path / "constant-eurw1s40.grib2"
    constants.write_bytes(_real_grib("h", orog) + _real_grib("lsm", np.full(shape, 0.5)))

    res = resolve(parse_capabilities(capabilities_xml()), RUN)
    run_dir = stage_run(tmp_path, res, [0], 1)

    def decoder(data: bytes) -> DecodedField:
        if data[4:10] == b"\x93NUMPY":  # simulated WCS field
            return fake_decoder(data)
        return arome_pe.decode_canonical_eccodes(data)

    out = convert_run(
        run_dir, tmp_path / "zarr", decoder=decoder, bbox=None, constants_file=constants
    )
    surf = zarr.open_group(str(out), mode="r")["surface"]
    np.testing.assert_allclose(surf["orog"][:], orog, atol=0.5)
    assert surf["orog"].attrs["units"] == "m"
    assert surf["orog"].attrs["source"].startswith("Météo-France")
    assert surf["lsm"][0, 0] == pytest.approx(50.0)  # fraction -> %
    assert surf["orog"].metadata.dimension_names == ("lat", "lon")


def test_convert_without_constants_file_still_works(tmp_path):
    res = resolve(parse_capabilities(capabilities_xml()), RUN)
    run_dir = stage_run(tmp_path, res, [0], 1)
    out = convert_run(
        run_dir,
        tmp_path / "zarr",
        decoder=fake_decoder,
        bbox=None,
        constants_file=tmp_path / "absent.grib2",
    )
    assert "orog" not in zarr.open_group(str(out), mode="r")["surface"]


# --------------------------------------------------------------------------- metadata throttling


class ThrottledMetadata(FakeWcs):
    """GetCoverage works, but metadata operations answer 429 (as on 2026-11-27)."""

    def __call__(self, request):
        if not request.url.path.endswith("GetCoverage"):
            with self._lock:
                self.calls.append(request)
            return httpx.Response(429, text="throttled")
        return super().__call__(request)


def test_metadata_429_backs_off_progressively_then_fails_cleanly():
    client = make_client(ThrottledMetadata())
    clock = client.limiter._clock.__self__
    with pytest.raises(arome_pe.WcsError, match="still throttled"):
        discover(client)
    # 10 + 20 + 40 + 80 + 120 s of waiting, not 8 x 5 s.
    assert clock.now >= 270


def test_capabilities_cache_and_stale_fallback(tmp_path):
    now = [1_000_000.0]
    cache = arome_pe.MetadataCache(tmp_path, clock=lambda: now[0])
    wcs = FakeWcs()
    res = discover(make_client(wcs), cache=cache)
    assert res.steps_source == "describe"
    assert wcs.count("GetCapabilities") == 1 and wcs.count("DescribeCoverage") == 1

    # Fresh cache: no call at all, time axis from the per-run-hour cache.
    import os

    os.utime(tmp_path / "capabilities.xml", (now[0], now[0]))
    again = FakeWcs()
    res = discover(make_client(again), cache=cache)
    assert again.calls == [] and res.steps_source == "cache" and res.run == RUN

    # Older than max_age: asked again; if throttled, the stale copy is used.
    now[0] += 3600
    throttled = ThrottledMetadata()
    res = discover(make_client(throttled), cache=cache)
    assert res.run == RUN and throttled.count("GetCapabilities") == 6


def test_time_axis_failure_falls_back_to_defaults():
    class DescribeThrottled(FakeWcs):
        def __call__(self, request):
            if request.url.path.endswith("DescribeCoverage"):
                return httpx.Response(429, text="throttled")
            return super().__call__(request)

    res = discover(make_client(DescribeThrottled()))
    assert res.steps_source == "default" and res.steps_seconds[-1] == 51 * 3600


def test_cli_reports_throttling_without_traceback(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("METEOFRANCE_API_KEY", "test-key")
    monkeypatch.setattr(arome_pe, "_client", lambda args: make_client(ThrottledMetadata()))
    assert arome_pe.main(["plan", "--raw", str(tmp_path)]) == 1
    assert "quota" in capsys.readouterr().err


def test_cli_fetch_with_outdated_saved_resolution(tmp_path, monkeypatch):
    # resolution.json written before gusts were renamed (gust10 -> gust_u10/gust_v10).
    run_dir = tmp_path / "20261127T09Z"
    run_dir.mkdir()
    old = {
        "run": RUN.isoformat(),
        "indicators": {"t2m": "TEMPERATURE__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND"},
        "intervals": {"t2m": None},
        "steps_seconds": [0, 3600],
        "unresolved": ["gust10", "orog"],
        "steps_source": "describe",
        "members": [0],
        "max_lead_hours": 1,
    }
    (run_dir / RESOLUTION_FILE).write_text(json.dumps(old))
    assert not Resolution.from_json(json.dumps(old)).is_current()
    assert Resolution.from_json(json.dumps(old)).missing_required() == []

    wcs = FakeWcs()
    monkeypatch.setattr(arome_pe, "_client", lambda args: make_client(wcs))
    argv = ["fetch", "--run", "20261127T09Z", "--members", "1", "--max-lead", "1"]
    assert arome_pe.main([*argv, "--raw", str(tmp_path)]) == 0
    saved = Resolution.from_json((run_dir / RESOLUTION_FILE).read_text())
    assert saved.is_current() and "gust_u10" in saved.indicators
    assert wcs.count("GetCapabilities") == 1
    assert any("gust_u10" in str(p) for p in run_dir.rglob("*.grib2"))
