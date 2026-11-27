"""Tests for the AROME GRIB -> Zarr conversion, on small GRIB2 files built with eccodes."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import numpy as np
import pytest
import zarr

eccodes = pytest.importorskip("eccodes")

from ingest.arome_convert import main as cli_main
from ingest.arome_zarr import (
    convert_run,
    convert_units,
    next_run_to_convert,
    per_step,
    prune_stores,
    published_runs,
    read_latest,
    write_latest,
)
from ingest.meteofrance_paquets import RunStage, format_run, run_tag

RUN = datetime(2026, 11, 27, 6, tzinfo=UTC)
NJ, NI = 40, 48  # more than one 32x32 chunk in each direction
LAT_N, LON_W, STEP = 45.39, 5.0, 0.01  # north-west corner, AROME scans north to south
HOURS = [0, 1, 2, 3]


def make_message(
    short_name, values, *, end_step=0, step_type="instant", type_of_level=None, level=None
):
    gid = eccodes.codes_grib_new_from_samples("regular_ll_sfc_grib2")
    try:
        eccodes.codes_set(gid, "Ni", NI)
        eccodes.codes_set(gid, "Nj", NJ)
        eccodes.codes_set(gid, "iScansNegatively", 0)
        eccodes.codes_set(gid, "jScansPositively", 0)
        eccodes.codes_set_double(gid, "latitudeOfFirstGridPointInDegrees", LAT_N)
        eccodes.codes_set_double(gid, "latitudeOfLastGridPointInDegrees", LAT_N - STEP * (NJ - 1))
        eccodes.codes_set_double(gid, "longitudeOfFirstGridPointInDegrees", LON_W)
        eccodes.codes_set_double(gid, "longitudeOfLastGridPointInDegrees", LON_W + STEP * (NI - 1))
        eccodes.codes_set_double(gid, "iDirectionIncrementInDegrees", STEP)
        eccodes.codes_set_double(gid, "jDirectionIncrementInDegrees", STEP)
        eccodes.codes_set(gid, "shortName", short_name)
        if type_of_level:
            eccodes.codes_set(gid, "typeOfLevel", type_of_level)
        if level is not None:
            eccodes.codes_set(gid, "level", level)
        if step_type != "instant":
            eccodes.codes_set(gid, "stepType", step_type)
            eccodes.codes_set(gid, "startStep", 0 if step_type == "accum" else end_step - 1)
        eccodes.codes_set(gid, "endStep", end_step)
        eccodes.codes_set(gid, "bitsPerValue", 24)
        eccodes.codes_set_values(gid, np.asarray(values, dtype=float).ravel())
        return eccodes.codes_get_message(gid)
    finally:
        eccodes.codes_release(gid)


def gradient():
    """North-to-south rows as stored in the GRIB: value = row index (0 = north)."""
    return np.repeat(np.arange(NJ, dtype=float)[:, None], NI, axis=1)


def stage_run(raw_root, run=RUN, *, skip=()):
    stage = RunStage(raw_root, run)
    sp1, sp2 = {}, {}
    for h in HOURS:
        m1 = [
            make_message(
                "2t",
                np.full((NJ, NI), 273.15 + h),
                end_step=h,
                type_of_level="heightAboveGround",
                level=2,
            ),
            make_message(
                "2r",
                np.full((NJ, NI), 80.0),
                end_step=h,
                type_of_level="heightAboveGround",
                level=2,
            ),
            make_message(
                "10u", gradient(), end_step=h, type_of_level="heightAboveGround", level=10
            ),
            make_message(
                "10v",
                np.full((NJ, NI), -2.0),
                end_step=h,
                type_of_level="heightAboveGround",
                level=10,
            ),
        ]
        m2 = [
            make_message("sp", np.full((NJ, NI), 101325.0), end_step=h),
            make_message("lcc", np.full((NJ, NI), 0.5), end_step=h),
        ]
        if h > 0:
            m1.append(
                make_message(
                    "10efg",
                    np.full((NJ, NI), 5.0 + h),
                    end_step=h,
                    step_type="max",
                    type_of_level="heightAboveGround",
                    level=10,
                )
            )
            # Cumulative rain 0, 1, 3, 6 mm -> per step 1, 2, 3 mm.
            m2.append(
                make_message(
                    "tirf", np.full((NJ, NI), h * (h + 1) / 2), end_step=h, step_type="accum"
                )
            )
        sp1[f"{h:02d}H"] = [m for m in m1 if (h, "SP1") not in skip]
        sp2[f"{h:02d}H"] = m2
    sp3 = {
        "00H": [
            make_message("h", np.full((NJ, NI), 1234.0)),
            make_message(
                "2t", np.full((NJ, NI), 280.0), type_of_level="heightAboveGround", level=2
            ),
        ]
    }
    packages = {"SP1": sp1, "SP2": sp2, "SP3": sp3}
    for package, files in packages.items():
        for label, msgs in files.items():
            p = stage.path / package / f"{label}.grib2"
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(b"".join(msgs))
    manifest = {
        "model": "arome",
        "grid": "0.01",
        "run": format_run(run),
        "packages": {k: list(v) for k, v in packages.items()},
    }
    (stage.path / "manifest.json").write_text(json.dumps(manifest))
    return stage


# --------------------------------------------------------------------------- pure helpers


def test_per_step_from_cumulative():
    cum = np.array([np.nan, 1.0, 3.0, 2.999, 6.0], dtype=np.float32)[:, None]
    out = per_step(cum, np.array([0, 1, 2, 3, 4]))
    assert np.isnan(out[0, 0])
    np.testing.assert_allclose(out[1:, 0], [1.0, 2.0, 0.0, 3.001], atol=1e-5)


def test_per_step_missing_field_propagates_to_next_step():
    cum = np.array([1.0, np.nan, 4.0], dtype=np.float32)[:, None]
    out = per_step(cum, np.array([1, 2, 3]))
    assert out[0, 0] == pytest.approx(1.0)
    assert np.isnan(out[1, 0]) and np.isnan(out[2, 0])


def test_convert_units():
    v = np.array([273.15, 300.0], dtype=np.float32)
    np.testing.assert_allclose(convert_units(v, "K", "degC"), [0.0, 26.85], atol=1e-4)
    np.testing.assert_allclose(convert_units(np.float32([101325]), "Pa", "hPa"), [1013.25])
    np.testing.assert_allclose(convert_units(np.float32([0.5]), "(0 - 1)", "%"), [50.0])
    np.testing.assert_allclose(convert_units(np.float32([98066.5]), "m**2 s**-2", "m"), [10000.0])
    assert convert_units(v, "m s**-1", "m s-1") is v
    with pytest.raises(ValueError, match="no conversion"):
        convert_units(v, "C", "degC")


# --------------------------------------------------------------------------- conversion


def test_convert_run_layout_and_values(tmp_path):
    stage_run(tmp_path / "raw")
    report = convert_run(tmp_path / "raw", RUN, tmp_path / "zarr", bbox=None)

    out = tmp_path / "zarr" / "20261127T06Z.zarr"
    assert report.path == out and out.exists()
    assert not (tmp_path / "zarr" / "20261127T06Z.zarr.tmp").exists()
    assert report.steps == HOURS
    assert set(report.variables) == {
        "t2m",
        "r2",
        "u10",
        "v10",
        "gust_u10",
        "sp",
        "lcc",
        "rain",
        "orog",
    }
    assert report.missing == {}

    root = zarr.open_group(str(out), mode="r")
    assert root.attrs["run"] == "2026-11-27T06:00:00Z"
    assert root.attrs["source"] == "Météo-France"
    g = root["surface"]
    assert list(g["time"][:]) == [0, 3600, 7200, 10800]
    lats, lons = g["lat"][:], g["lon"][:]
    assert np.all(np.diff(lats) > 0) and np.all(np.diff(lons) > 0)
    assert lats[-1] == pytest.approx(LAT_N)

    t2m = g["t2m"]
    assert t2m.shape == (1, 4, NJ, NI)
    assert t2m.chunks == (1, 4, 32, 32)
    assert t2m.metadata.dimension_names == ("member", "time", "lat", "lon")
    assert t2m.attrs["units"] == "degC"
    np.testing.assert_allclose(t2m[0, :, 0, 0], [0.0, 1.0, 2.0, 3.0], atol=1e-3)

    # Rows flipped to ascending latitudes: southern row was the last GRIB row.
    assert g["u10"][0, 0, 0, 0] == pytest.approx(NJ - 1, abs=1e-3)
    assert g["u10"][0, 0, -1, 0] == pytest.approx(0.0, abs=1e-3)

    assert g["sp"][0, 2, 5, 5] == pytest.approx(1013.25, abs=1e-2)
    assert g["lcc"][0, 1, 5, 5] == pytest.approx(50.0, abs=1e-2)
    rain = g["rain"]
    assert rain.attrs["units"] == "mm" and rain.attrs["kind"] == "accum"
    assert np.isnan(rain[0, 0, 0, 0])
    np.testing.assert_allclose(rain[0, 1:, 3, 3], [1.0, 2.0, 3.0], atol=1e-3)
    gust = g["gust_u10"]
    assert np.isnan(gust[0, 0, 0, 0])
    np.testing.assert_allclose(gust[0, 1:, 0, 0], [6.0, 7.0, 8.0], atol=1e-3)

    orog = g["orog"]
    assert orog.shape == (NJ, NI) and orog.metadata.dimension_names == ("lat", "lon")
    assert orog[0, 0] == pytest.approx(1234.0, abs=1e-2)
    # The 2t inside SP3 is ignored: it must not override the SP1 field (checked above).
    assert "2t_heightAboveGround_2" not in report.unknown


def test_convert_run_crops_to_bbox(tmp_path):
    from ingest.meteofrance_wcs import BBox

    stage_run(tmp_path / "raw")
    convert_run(tmp_path / "raw", RUN, tmp_path / "zarr", bbox=BBox(45.0, 45.2, 5.1, 5.3))
    g = zarr.open_group(str(tmp_path / "zarr" / "20261127T06Z.zarr"), mode="r")["surface"]
    assert g["lat"].shape == (21,) and g["lon"].shape == (21,)
    assert g["t2m"].chunks == (1, 4, 21, 21)


def test_missing_lead_time_is_reported(tmp_path):
    stage_run(tmp_path / "raw", skip={(2, "SP1")})
    report = convert_run(tmp_path / "raw", RUN, tmp_path / "zarr", bbox=None)
    assert report.missing["u10"] == [2]
    assert report.missing["gust_u10"] == [2]
    g = zarr.open_group(str(report.path), mode="r")["surface"]
    assert np.isnan(g["u10"][0, 2]).all()


def test_incomplete_run_is_refused(tmp_path):
    stage = stage_run(tmp_path / "raw")
    (stage.path / "manifest.json").unlink()
    with pytest.raises(ValueError, match="not complete"):
        convert_run(tmp_path / "raw", RUN, tmp_path / "zarr")


def test_missing_required_variable_fails_and_keeps_previous_store(tmp_path):
    stage = stage_run(tmp_path / "raw")
    convert_run(tmp_path / "raw", RUN, tmp_path / "zarr", bbox=None)
    (stage.path / "SP3" / "00H.grib2").write_bytes(make_message("sp", np.ones((NJ, NI))))
    with pytest.raises(ValueError, match="orog"):
        convert_run(tmp_path / "raw", RUN, tmp_path / "zarr", bbox=None)
    out = tmp_path / "zarr" / "20261127T06Z.zarr"
    assert "orog" in zarr.open_group(str(out), mode="r")["surface"]
    assert not out.with_name(out.name + ".tmp").exists()


# --------------------------------------------------------------------------- published stores


def test_latest_and_prune(tmp_path):
    runs = [datetime(2026, 11, 27, h, tzinfo=UTC) for h in (0, 3, 6, 12)]
    for r in runs:
        (tmp_path / f"{run_tag(r)}.zarr").mkdir()
    (tmp_path / "20261127T18Z.zarr.tmp").mkdir()
    assert published_runs(tmp_path) == runs
    write_latest(tmp_path, runs[1])
    assert read_latest(tmp_path) == runs[1]
    removed = prune_stores(tmp_path, keep=1)
    assert removed == [runs[0], runs[2]]
    assert published_runs(tmp_path) == [runs[1], runs[3]]
    with pytest.raises(ValueError):
        prune_stores(tmp_path, keep=0)


def test_next_run_to_convert(tmp_path):
    raw, out = tmp_path / "raw", tmp_path / "zarr"
    assert next_run_to_convert(raw, out) is None
    stage_run(raw)
    assert next_run_to_convert(raw, out) == RUN
    (out).mkdir()
    (out / "20261127T06Z.zarr").mkdir()
    assert next_run_to_convert(raw, out) is None


def test_cli_convert_then_noop(tmp_path, capsys):
    stage_run(tmp_path / "raw" / "arome")
    args = ["--raw", str(tmp_path / "raw"), "--out", str(tmp_path / "zarr")]
    assert cli_main(["convert", *args, "--keep", "2"]) == 0
    assert read_latest(tmp_path / "zarr" / "arome") == RUN
    assert cli_main(["convert", *args]) == 0
    assert cli_main(["inventory", *args]) == 0
    out = capsys.readouterr().out
    assert "tirf_surface_0_accum" in out and "-> rain" in out
