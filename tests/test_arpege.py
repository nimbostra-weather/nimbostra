"""Tests for ARPEGE 0.1° through the API Paquets: catalogue, conversion, reading, API."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import numpy as np
import pytest
import zarr

eccodes = pytest.importorskip("eccodes")

from fastapi.testclient import TestClient
from test_arome_zarr import NI, NJ, gradient, make_message
from test_point import LATS, LONS, make_store

from api.app import Settings, create_app
from ingest.arome_convert import main as cli_main
from ingest.arome_zarr import ARPEGE_VARIABLES, PROFILES, MessageMeta, _match, convert_run
from ingest.meteofrance_paquets import (
    ARPEGE_01,
    MODELS,
    RunStage,
    format_run,
    parse_files,
    parse_time_label,
)
from point.reader import derive, read_raw

RUN = datetime(2026, 11, 27, 12, tzinfo=UTC)
# Two staged files as in the real packages (one lead-time range per file):
# t2m and winds hourly, precipitation 3-hourly in the second file.
FILES = {"000H002H": [0, 1, 2], "003H006H": [3, 4, 5, 6]}
PRECIP_HOURS = {1, 2, 3, 6}
CUM_TP = {1: 1.0, 2: 3.0, 3: 6.0, 6: 12.0}  # mm since run start
CUM_SNOW = {1: 0.0, 2: 1.0, 3: 1.0, 6: 4.0}
OROG = 321.0


# --------------------------------------------------------------------------- catalogue


def test_arpege_model_urls():
    assert MODELS["arpege"] is ARPEGE_01
    assert ARPEGE_01.product_url("SP1") == (
        "https://public-api.meteofrance.fr/previnum/DPPaquetARPEGE/v1"
        "/models/ARPEGE/grids/0.1/packages/SP1/productARP"
    )
    assert [p.name for p in ARPEGE_01.packages] == ["SP1", "SP2"]
    assert all(p.expected_files == 9 for p in ARPEGE_01.packages)


def test_arpege_time_labels():
    assert parse_time_label("000H012H") == (0, 12)
    assert parse_time_label("097H102H") == (97, 102)


def test_arpege_catalogue_links_get_the_version_back():
    base = "https://public-api.meteofrance.fr/previnum/DPPaquetARPEGE/models/ARPEGE/grids/0.1"
    labels = ["013H024H", "000H012H", "097H102H"]
    payload = {
        "links": [
            {
                "href": f"{base}/packages/SP1/productARP?&referencetime=2026-11-27T12:00:00Z"
                f"&time={label}&format=grib2",
                "time": label,
            }
            for label in labels
        ]
    }
    files = parse_files(payload, ARPEGE_01, "SP1", RUN)
    assert [f.time for f in files] == ["000H012H", "013H024H", "097H102H"]
    assert all("/DPPaquetARPEGE/v1/models/" in f.url for f in files)
    assert files[0].key == "SP1/000H012H.grib2"


# --------------------------------------------------------------------------- conversion


def hag(short, values, h, level, step_type="instant"):
    return make_message(
        short,
        values,
        end_step=h,
        step_type=step_type,
        type_of_level="heightAboveGround",
        level=level,
    )


def full(value):
    return np.full((NJ, NI), value, dtype=float)


def stage_arpege(raw_root, run=RUN):
    stage = RunStage(raw_root, run)
    packages = {"SP1": {}, "SP2": {}}
    for label, hours in FILES.items():
        sp1, sp2 = [], []
        for h in hours:
            sp1 += [
                hag("2t", full(273.15 + h), h, 2),
                hag("2r", full(70.0), h, 2),
                hag("10u", gradient(), h, 10),
                hag("10v", full(-2.0), h, 10),
                make_message("prmsl", full(101000.0), end_step=h),
            ]
            if h > 0:
                sp1.append(hag("10fg", full(10.0 + h), h, 10, "max"))
            if h in PRECIP_HOURS:
                sp1 += [
                    make_message(
                        "tp", full(CUM_TP[h]), end_step=h, step_type="accum", param_id=228228
                    ),
                    make_message("tsnowp", full(CUM_SNOW[h]), end_step=h, step_type="accum"),
                ]
            sp2 += [
                hag("2d", full(268.15), h, 2),
                make_message("sp", full(95000.0), end_step=h, type_of_level="surface"),
                make_message("lcc", full(0.25), end_step=h),
                make_message("tcc", full(0.75), end_step=h),
            ]
        packages["SP1"][label] = sp1
        packages["SP2"][label] = sp2
    for package, files in packages.items():
        for label, msgs in files.items():
            p = stage.path / package / f"{label}.grib2"
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(b"".join(msgs))
    manifest = {
        "model": "arpege",
        "grid": "0.1",
        "run": format_run(run),
        "packages": {k: list(v) for k, v in packages.items()},
    }
    (stage.path / "manifest.json").write_text(json.dumps(manifest))
    return stage


def write_constants(path, value=OROG):
    path.write_bytes(make_message("h", full(value)) + make_message("lsm", full(1.0)))
    return path


def convert(tmp_path, **kwargs):
    profile = PROFILES["arpege"]
    return convert_run(
        tmp_path / "raw",
        RUN,
        tmp_path / "zarr",
        variables=profile.variables,
        static_packages=profile.static_packages,
        required=profile.required,
        bbox=None,
        **kwargs,
    )


def test_convert_arpege_hourly_then_three_hourly(tmp_path):
    stage_arpege(tmp_path / "raw")
    report = convert(tmp_path, constants=write_constants(tmp_path / "constant-eurat01.grib2"))

    assert report.steps == [0, 1, 2, 3, 4, 5, 6]
    assert set(report.variables) >= {
        "t2m",
        "r2",
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
    # Precipitation is 3-hourly after +3 h: reported, not an error.
    assert report.missing["precip_total"] == [4, 5]

    root = zarr.open_group(str(report.path), mode="r")
    assert root.attrs["model"] == "arpege" and root.attrs["grid"] == "0.1"
    g = root["surface"]
    np.testing.assert_allclose(g["t2m"][0, :, 0, 0], np.arange(7), atol=1e-3)
    tp = g["precip_total"][0, :, 4, 4]
    assert np.isnan(tp[0]) and np.isnan(tp[4]) and np.isnan(tp[5])
    np.testing.assert_allclose(tp[[1, 2, 3, 6]], [1.0, 2.0, 3.0, 6.0], atol=1e-3)
    snow = g["snow"][0, :, 4, 4]
    np.testing.assert_allclose(snow[[1, 2, 3, 6]], [0.0, 1.0, 0.0, 3.0], atol=1e-3)
    assert "previous time" in g["precip_total"].attrs["accumulation"]
    assert g["tcc"][0, 1, 0, 0] == pytest.approx(75.0, abs=1e-2)
    assert g["d2m"][0, 1, 0, 0] == pytest.approx(-5.0, abs=1e-3)
    assert g["mslp"][0, 1, 0, 0] == pytest.approx(1010.0, abs=1e-2)
    gust = g["gust10"][0, :, 0, 0]
    assert np.isnan(gust[0]) and gust[6] == pytest.approx(16.0, abs=1e-3)
    assert g["orog"][0, 0] == pytest.approx(OROG, abs=1e-2)
    # Other fields of the constants file (land-sea mask) are not stored.
    assert "lsm" not in report.variables


def test_arpege_without_orography_fails(tmp_path):
    stage_arpege(tmp_path / "raw")
    with pytest.raises(ValueError, match="orog"):
        convert(tmp_path)


def test_constants_file_wins_over_packages(tmp_path):
    stage = stage_arpege(tmp_path / "raw")
    sp2 = stage.path / "SP2" / "000H002H.grib2"
    sp2.write_bytes(sp2.read_bytes() + make_message("h", full(999.0)))
    report = convert(tmp_path, constants=write_constants(tmp_path / "c.grib2"))
    g = zarr.open_group(str(report.path), mode="r")["surface"]
    assert g["orog"][0, 0] == pytest.approx(OROG, abs=1e-2)
    report = convert(tmp_path)
    g = zarr.open_group(str(report.path), mode="r")["surface"]
    assert g["orog"][0, 0] == pytest.approx(999.0, abs=1e-2)


def test_arpege_variable_table():
    def match(short, tol="heightAboveGround", level=10, step="instant"):
        spec = _match(MessageMeta(short, tol, level, step, 1, ""), ARPEGE_VARIABLES)
        return spec.name if spec else None

    assert match("10fg", step="max") == "gust10"
    assert match("max_10efg", step="max") == "gust_u10"
    assert match("tp", "surface", 0, "accum") == "precip_total"
    assert match("2d", level=2) == "d2m"


# ``arome_convert inventory --model arpege`` on the real run of 2026-11-27 12z
# (SP1 and SP2, file 013H024H): (shortName, typeOfLevel, level, stepType, units).
REAL_INVENTORY = [
    ("10wdir", "heightAboveGround", 10, "instant", "Degree true", None),
    ("10si", "heightAboveGround", 10, "instant", "m s**-1", None),
    ("tsnowp", "surface", 0, "accum", "kg m**-2", "snow"),
    ("tp", "surface", 0, "accum", "kg m**-2", "precip_total"),
    ("prmsl", "meanSea", 0, "instant", "Pa", "mslp"),
    ("10u", "heightAboveGround", 10, "instant", "m s**-1", "u10"),
    ("10v", "heightAboveGround", 10, "instant", "m s**-1", "v10"),
    ("2t", "heightAboveGround", 2, "instant", "K", "t2m"),
    ("2r", "heightAboveGround", 2, "instant", "%", "r2"),
    ("max_i10fg", "heightAboveGround", 10, "max", "m s**-1", "gust10"),
    ("tirf", "surface", 0, "accum", "kg m**-2", "rain"),
    ("cp", "surface", 0, "accum", "kg m**-2", None),
    ("lsp", "surface", 0, "accum", "kg m**-2", None),
    ("max_10efg", "heightAboveGround", 10, "max", "m s**-1", "gust_u10"),
    ("max_10nfg", "heightAboveGround", 10, "max", "m s**-1", "gust_v10"),
    ("sd", "surface", 0, "instant", "kg m**-2", None),
    ("2d", "heightAboveGround", 2, "instant", "K", "d2m"),
    ("lcc", "surface", 0, "instant", "%", "lcc"),
    ("hcc", "surface", 0, "instant", "%", "hcc"),
    ("mcc", "surface", 0, "instant", "%", "mcc"),
    ("t", "surface", 0, "instant", "K", None),
    ("sp", "surface", 0, "instant", "Pa", "sp"),
    ("CAPE_INS", "unknown", 0, "instant", "m**2 s**-2", "cape"),
    ("blh", "surface", 0, "instant", "m", None),
    ("ptype", "surface", 0, "avg", "(Code table 4.201)", None),
    ("min_vis", "surface", 0, "min", "m", None),
    ("d0c6n1", "surface", 0, "instant", "unknown", None),
]


@pytest.mark.parametrize(("short", "tol", "level", "step", "units", "expected"), REAL_INVENTORY)
def test_real_arpege_inventory(short, tol, level, step, units, expected):
    from ingest.arome_zarr import convert_units

    spec = _match(MessageMeta(short, tol, level, step, 13, units), ARPEGE_VARIABLES)
    assert (spec.name if spec else None) == expected
    if spec is not None:
        convert_units(np.float32([1.0]), units, spec.units)


def test_cli_convert_arpege_with_constants_env(tmp_path, monkeypatch, capsys):
    stage_arpege(tmp_path / "raw" / "arpege")
    monkeypatch.setenv("ARPEGE_CONSTANTS", str(write_constants(tmp_path / "c.grib2")))
    args = ["--model", "arpege", "--raw", str(tmp_path / "raw"), "--out", str(tmp_path / "zarr")]
    assert cli_main(["convert", *args]) == 0
    assert json.loads((tmp_path / "zarr" / "arpege" / "latest.json").read_text())["run"] == (
        "2026-11-27T12:00:00Z"
    )
    assert cli_main(["inventory", *args]) == 0
    out = capsys.readouterr().out
    assert "tp_surface_0_accum" in out and "-> precip_total" in out and "+3,6h" in out
    monkeypatch.setenv("ARPEGE_CONSTANTS", str(tmp_path / "missing.grib2"))
    assert cli_main(["convert", *args]) == 1


# --------------------------------------------------------------------------- reading and API


def arpege_extra():
    plane = (LATS.size, LONS.size)
    shape = (1, 4, *plane)
    tp = np.stack([np.full(plane, v) for v in (np.nan, 2.0, 0.05, 3.0)])[None]
    snow = np.stack([np.full(plane, v) for v in (np.nan, 0.5, 0.0, 3.0)])[None]
    return {
        "precip_total": (tp, "mm"),
        "snow": (snow, "mm"),
        "gust10": (np.full(shape, 12.0), "m s-1"),
        "tcc": (np.full(shape, 90.0), "%"),
    }


def make_arpege_store(root):
    path = make_store(root, RUN, extra=arpege_extra())
    g = zarr.open_group(str(path), mode="a")["surface"]
    del g["rain"]  # exercise the remainder (total - snow)
    return path


def test_derive_from_total_precipitation(tmp_path):
    make_arpege_store(tmp_path)
    g = zarr.open_group(str(next(tmp_path.glob("*.zarr"))), mode="r")["surface"]
    out = derive(read_raw(g, "arpege", RUN, 45.2, 6.2))
    s = out.series
    np.testing.assert_allclose(s["precipitation"][1:], [2.0, 0.05, 3.0], atol=1e-5)
    np.testing.assert_allclose(s["rain"][1:], [1.5, 0.05, 0.0], atol=1e-5)
    assert list(s["precipitation_type"]) == [None, "mixed", "none", "snow"]
    # The gust speed wins over the hypot of the components (6, 8 -> 10).
    assert s["wind_gust"][0] == pytest.approx(12.0)
    assert s["cloud_total"][0] == pytest.approx(90.0)


def test_api_serves_arpege(tmp_path):
    make_store(tmp_path / "arome")
    make_arpege_store(tmp_path / "arpege")
    client = TestClient(create_app(Settings(data_dir=tmp_path, models=("arome", "arpege"))))
    names = [m["name"] for m in client.get("/v1/models").json()["models"]]
    assert names == ["arome", "arpege"]
    r = client.get("/v1/forecast/point", params={"lat": 45.2, "lon": 6.2, "model": "arpege"})
    assert r.status_code == 200
    body = r.json()
    assert body["model"] == "arpege"
    assert "cloud_total" in body["variables"] and "precipitation" in body["variables"]
    r = client.get("/v1/raw/arpege/point", params={"lat": 45.2, "lon": 6.2})
    assert r.status_code == 200 and "precip_total" in r.json()["variables"]


def test_derive_prefers_model_rain_over_remainder(tmp_path):
    path = make_store(tmp_path, RUN, extra=arpege_extra())  # keeps the stored rain
    g = zarr.open_group(str(path), mode="r")["surface"]
    out = derive(read_raw(g, "arpege", RUN, 45.2, 6.2))
    np.testing.assert_allclose(out.series["rain"][1:], [2.0, 0.05, 0.1], atol=1e-5)
    np.testing.assert_allclose(out.series["precipitation"][1:], [2.0, 0.05, 3.0], atol=1e-5)
