"""Tests for point reading, interpolation, altitude correction and derived quantities."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime

import numpy as np
import pytest
import zarr

from point import derived
from point.reader import (
    NoRun,
    OutOfDomain,
    RunStore,
    apply_stencil,
    derive,
    make_stencil,
    read_raw,
)

RUN = datetime(2026, 11, 27, 6, tzinfo=UTC)
LATS = np.round(np.arange(45.0, 45.64, 0.01), 2)  # 64 points: two chunks of 32
LONS = np.round(np.arange(6.0, 6.64, 0.01), 2)
NT = 4


def make_store(root, run=RUN, *, t2m=None, orog=1000.0, extra=None):
    """Write a store with the layout of ingest.arome_zarr (surface group, 32x32 chunks)."""
    path = root / f"{run.strftime('%Y%m%dT%HZ')}.zarr"
    g = zarr.open_group(str(path), mode="w", zarr_format=3).create_group("surface")
    for name, values in (
        ("member", np.zeros(1, "int16")),
        ("time", np.arange(NT, dtype="int32") * 3600),
        ("lat", LATS),
        ("lon", LONS),
    ):
        a = g.create_array(name, shape=values.shape, dtype=values.dtype, dimension_names=[name])
        a[...] = values

    shape = (1, NT, LATS.size, LONS.size)
    lat2d, lon2d = np.meshgrid(LATS, LONS, indexing="ij")
    fields = {
        # Linear in lat and lon: bilinear interpolation must be exact.
        "t2m": (
            t2m
            if t2m is not None
            else np.broadcast_to(10 + 100 * (lat2d - 45) + 10 * (lon2d - 6), shape),
            "degC",
        ),
        "r2": (np.full(shape, 50.0), "%"),
        "u10": (np.full(shape, -3.0), "m s-1"),
        "v10": (np.full(shape, -4.0), "m s-1"),
        "gust_u10": (np.full(shape, 6.0), "m s-1"),
        "gust_v10": (np.full(shape, 8.0), "m s-1"),
        "rain": (np.stack([np.full(lat2d.shape, v) for v in (np.nan, 2.0, 0.05, 0.1)])[None], "mm"),
        "snow": (np.stack([np.full(lat2d.shape, v) for v in (np.nan, 0.0, 0.0, 1.0)])[None], "mm"),
        "lcc": (np.full(shape, 75.0), "%"),
        "sp": (np.full(shape, 900.0), "hPa"),
        **(extra or {}),
    }
    for name, (values, units) in fields.items():
        a = g.create_array(
            name,
            shape=shape,
            dtype="float32",
            chunks=(1, NT, 32, 32),
            fill_value=np.nan,
            dimension_names=["member", "time", "lat", "lon"],
        )
        a[...] = np.asarray(values, dtype="float32")
        a.attrs["units"] = units
    o = g.create_array(
        "orog",
        shape=lat2d.shape,
        dtype="float32",
        chunks=(32, 32),
        fill_value=np.nan,
        dimension_names=["lat", "lon"],
    )
    o[...] = np.full(lat2d.shape, orog, dtype="float32")
    o.attrs["units"] = "m"
    (root / "latest.json").write_text(
        json.dumps({"run": run.strftime("%Y-%m-%dT%H:%M:%SZ"), "path": path.name})
    )
    return path


# --------------------------------------------------------------------------- interpolation


def test_bilinear_is_exact_on_linear_field_across_chunks(tmp_path):
    make_store(tmp_path)
    store = RunStore(tmp_path, "arome")
    run, path = store.latest()
    assert run == RUN
    # 45.315 / 6.3155 straddles the chunk boundary at index 32 in latitude.
    p = read_raw(store.group(path), "arome", run, 45.315, 6.3155)
    expected = 10 + 100 * 0.315 + 10 * 0.3155
    np.testing.assert_allclose(p.series["t2m"], expected, atol=1e-3)
    assert p.static["orog"] == pytest.approx(1000.0)
    assert p.times[1] == datetime(2026, 11, 27, 7, tzinfo=UTC)
    assert p.units["t2m"] == "degC"


def test_nearest_returns_grid_value(tmp_path):
    make_store(tmp_path)
    store = RunStore(tmp_path, "arome")
    run, path = store.latest()
    p = read_raw(store.group(path), "arome", run, 45.3149, 6.3151, method="nearest")
    assert (p.grid_lat, p.grid_lon) == (pytest.approx(45.31), pytest.approx(6.32))
    np.testing.assert_allclose(p.series["t2m"], 10 + 31 + 3.2, atol=1e-3)


def test_nan_corner_is_dropped_and_weights_renormalised():
    s = make_stencil(np.array([0.0, 1.0]), np.array([0.0, 1.0]), 0.5, 0.5)
    block = np.array([[1.0, 2.0], [3.0, np.nan]])
    assert apply_stencil(block, s) == pytest.approx(2.0)
    # Only the far corner valid: below MIN_WEIGHT, no value.
    s = make_stencil(np.array([0.0, 1.0]), np.array([0.0, 1.0]), 0.05, 0.05)
    block = np.array([[np.nan, np.nan], [np.nan, 4.0]])
    assert np.isnan(apply_stencil(block, s))


def test_point_on_last_grid_line_and_outside():
    lats, lons = np.array([45.0, 45.01, 45.02]), np.array([6.0, 6.01])
    s = make_stencil(lats, lons, 45.02, 6.01)
    assert (s.i, s.j) == (1, 0)
    assert s.weights[1, 1] == pytest.approx(1.0)
    with pytest.raises(OutOfDomain):
        make_stencil(lats, lons, 44.99, 6.0)
    with pytest.raises(OutOfDomain):
        make_stencil(lats, lons, 45.0, 6.02)


def test_variable_selection_and_unknown(tmp_path):
    make_store(tmp_path)
    store = RunStore(tmp_path, "arome")
    run, path = store.latest()
    p = read_raw(store.group(path), "arome", run, 45.2, 6.2, variables=["t2m"])
    assert set(p.series) == {"t2m"} and "orog" in p.static
    with pytest.raises(KeyError):
        read_raw(store.group(path), "arome", run, 45.2, 6.2, variables=["nope"])


def test_no_run(tmp_path):
    with pytest.raises(NoRun):
        RunStore(tmp_path, "arome").latest()


def test_group_cache_follows_latest(tmp_path):
    make_store(tmp_path)
    store = RunStore(tmp_path, "arome", cache_size=1)
    _, p1 = store.latest()
    g1 = store.group(p1)
    assert store.group(p1) is g1
    later = datetime(2026, 11, 27, 9, tzinfo=UTC)
    make_store(tmp_path, later)
    run, p2 = store.latest()
    assert run == later and store.group(p2) is not g1


# --------------------------------------------------------------------------- derive


def test_derive_altitude_correction_and_derived(tmp_path):
    make_store(tmp_path, t2m=np.full((1, NT, LATS.size, LONS.size), 5.0))
    store = RunStore(tmp_path, "arome")
    run, path = store.latest()
    raw = read_raw(store.group(path), "arome", run, 45.2, 6.2)

    f = derive(raw, elevation=1500.0)
    assert f.meta["elevation"] == {"model": 1000.0, "target": 1500.0, "source": "user"}
    np.testing.assert_allclose(f.series["temperature"], 5.0 - 0.0065 * 500, atol=1e-4)
    assert f.series["surface_pressure"][0] < 900.0
    np.testing.assert_allclose(f.series["wind_speed"], 5.0, atol=1e-5)
    np.testing.assert_allclose(f.series["wind_direction"], 36.87, atol=0.01)  # from NNE
    np.testing.assert_allclose(f.series["wind_gust"], 10.0, atol=1e-5)
    assert np.isnan(f.series["precipitation"][0])
    np.testing.assert_allclose(f.series["precipitation"][1:], [2.0, 0.05, 1.1], atol=1e-5)
    assert list(f.series["precipitation_type"]) == [None, "rain", "none", "snow"]
    assert f.series["cloud_l"][0] == pytest.approx(75.0)
    assert np.all(f.series["feels_like"] < f.series["temperature"])  # wind chill

    doc = f.to_json()
    assert doc["run"] == "2026-11-27T06:00:00Z"
    assert doc["variables"]["precipitation"]["values"][0] is None
    assert doc["variables"]["precipitation_type"]["values"][1] == "rain"
    assert doc["times"][0] == "2026-11-27T06:00:00Z"
    assert "Météo-France" in doc["attribution"]
    json.dumps(doc)  # serialisable, no NaN


def test_derive_uses_dem_then_model(tmp_path):
    make_store(tmp_path)
    store = RunStore(tmp_path, "arome")
    run, path = store.latest()
    raw = read_raw(store.group(path), "arome", run, 45.2, 6.2)
    f = derive(raw, dem=lambda lat, lon: 800.0)
    assert f.meta["elevation"]["source"] == "dem"
    np.testing.assert_allclose(f.series["temperature"], raw.series["t2m"] + 0.0065 * 200, atol=1e-4)
    g = derive(raw, dem=lambda lat, lon: None)
    assert g.meta["elevation"]["source"] == "model"
    np.testing.assert_allclose(g.series["temperature"], raw.series["t2m"], atol=1e-6)


# --------------------------------------------------------------------------- formulas


def test_formulas_against_reference_values():
    assert derived.dewpoint(np.array(20.0), np.array(50.0)) == pytest.approx(9.3, abs=0.1)
    # Stull (2011) worked example: 20 °C, 50 % -> 13.7 °C.
    assert derived.wet_bulb(np.array(20.0), np.array(50.0)) == pytest.approx(13.7, abs=0.1)
    assert derived.wind_direction(np.array(0.0), np.array(-5.0)) == pytest.approx(0.0)
    assert derived.wind_direction(np.array(-5.0), np.array(0.0)) == pytest.approx(90.0)
    assert derived.wind_direction(np.array(5.0), np.array(0.0)) == pytest.approx(270.0)
    # Wind chill -10 °C at 30 km/h: -19.5 (Environment Canada table: -20).
    chill = derived.feels_like(np.array([-10.0]), np.array([80.0]), np.array([30 / 3.6]))
    assert chill[0] == pytest.approx(-19.5, abs=0.1)
    # Heat index 32 °C, 70 % RH: about 40.5-41 °C (NWS table, 90 °F / 70 % -> 105-106 °F).
    hot = derived.feels_like(np.array([32.0]), np.array([70.0]), np.array([1.0]))
    assert hot[0] == pytest.approx(40.8, abs=0.7)
    mild = derived.feels_like(np.array([18.0]), np.array([60.0]), np.array([10.0]))
    assert mild[0] == pytest.approx(18.0)


def test_rain_snow_limit_estimate():
    # Wet-bulb 1 °C at 500 m -> limit at 500 m; 2.3 °C -> 200 m higher.
    np.testing.assert_allclose(
        derived.rain_snow_limit_estimate(np.array([1.0, 2.3]), 500.0), [500.0, 700.0]
    )


def test_precipitation_type_mixed_and_graupel():
    types = derived.precipitation_type(
        np.array([1.0, 0.0, 0.0]), np.array([1.0, 0.0, np.nan]), np.array([0.0, 2.0, np.nan])
    )
    assert types == ["mixed", "graupel", "none"]


# --------------------------------------------------------------------------- timing


def test_read_and_derive_is_fast(tmp_path):
    extra = {f"x{i}": (np.zeros((1, NT, LATS.size, LONS.size)), "1") for i in range(5)}
    make_store(tmp_path, extra=extra)
    store = RunStore(tmp_path, "arome")
    run, path = store.latest()
    group = store.group(path)
    read_raw(group, "arome", run, 45.1, 6.1)  # warm-up
    t0 = time.perf_counter()
    for k in range(20):
        derive(read_raw(group, "arome", run, 45.05 + k * 0.02, 6.05 + k * 0.02), elevation=500)
    per_point = (time.perf_counter() - t0) / 20
    assert per_point < 0.2, per_point
