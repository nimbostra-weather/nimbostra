"""Tests for the atomic Zarr writer, with a fake GRIB decoder and a real eccodes one."""

from __future__ import annotations

import io
from datetime import UTC, datetime

import numpy as np
import pytest
import zarr

from ingest.meteofrance_wcs import AROME_PE, BBox, RawStore, Variable, plan_run
from ingest.zarr_writer import DecodedField, crop, write_run

RUN = datetime(2026, 11, 27, 3, tzinfo=UTC)
T2M = Variable("t2m", "TEMPERATURE__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND", height=2)
T_PL = Variable("t", "TEMPERATURE__ISOBARIC_SURFACE", group="pressure", levels=(850, 500))
LATS = np.arange(44.0, 46.0, 0.25)  # 8
LONS = np.arange(2.0, 5.0, 0.25)  # 12


def fake_grib(values: np.ndarray) -> bytes:
    buf = io.BytesIO()
    np.save(buf, values.astype(np.float32))
    return b"GRIB" + buf.getvalue()


def fake_decoder(data: bytes) -> DecodedField:
    assert data.startswith(b"GRIB")
    return DecodedField(np.load(io.BytesIO(data[4:])), LATS, LONS)


def field_value(req) -> float:
    return req.member * 1000 + req.step_seconds / 3600 + (req.level or 0) / 10000


def stage(plan, store, skip=()):
    for r in plan:
        if r.key in skip:
            store.mark_missing(r)
            continue
        store.write(r, fake_grib(np.full((LATS.size, LONS.size), field_value(r))))


def test_write_run_layout_values_and_missing(tmp_path):
    plan = plan_run(AROME_PE, RUN, [T2M, T_PL], [3600, 7200, 10800], members=[0, 1])
    store = RawStore(tmp_path / "raw")
    missing = plan_run(AROME_PE, RUN, [T2M], [7200], members=[1])[0]
    stage(plan, store, skip={missing.key})
    out = tmp_path / "2026112703.zarr"

    report = write_run(out, plan, store, decoder=fake_decoder)

    assert report.missing == 1 and report.fields == len(plan) - 1
    assert out.exists() and not (tmp_path / "2026112703.zarr.tmp").exists()
    root = zarr.open_group(str(out), mode="r")
    assert root.attrs["model"] == "arome-pe"
    surf = root["surface/t2m"]
    assert surf.shape == (2, 3, 8, 12)
    assert surf.chunks == (2, 3, 8, 8)
    assert surf.metadata.dimension_names == ("member", "time", "lat", "lon")
    assert np.isnan(surf[1, 1]).all()
    assert surf[1, 2, 0, 0] == pytest.approx(1003.0)
    pres = root["pressure/t"]
    assert pres.shape == (2, 3, 2, 8, 12)
    assert pres.chunks == (2, 3, 2, 8, 8)
    assert list(root["pressure/level"][:]) == [500, 850]
    assert pres[0, 0, 1, 3, 3] == pytest.approx(1 + 0.085)
    assert list(root["surface/time"][:]) == [3600, 7200, 10800]
    np.testing.assert_allclose(root["surface/lat"][:], LATS)


def test_write_run_replaces_existing_store(tmp_path):
    plan = plan_run(AROME_PE, RUN, [T2M], [3600], members=[0])
    store = RawStore(tmp_path / "raw")
    stage(plan, store)
    out = tmp_path / "run.zarr"
    out.mkdir()
    (out / "stale").write_text("old")

    write_run(out, plan, store, decoder=fake_decoder)

    assert not (out / "stale").exists()
    assert not (tmp_path / "run.zarr.old").exists()
    assert zarr.open_group(str(out), mode="r")["surface/t2m"].shape == (1, 1, 8, 12)


def test_failed_write_keeps_previous_run_and_cleans_tmp(tmp_path):
    plan = plan_run(AROME_PE, RUN, [T2M], [3600, 7200], members=[0])
    store = RawStore(tmp_path / "raw")
    stage(plan, store)
    out = tmp_path / "run.zarr"
    write_run(out, plan, store, decoder=fake_decoder)

    def broken(data: bytes) -> DecodedField:
        raise RuntimeError("corrupt GRIB")

    with pytest.raises(RuntimeError):
        write_run(out, plan, store, decoder=broken)
    assert not (tmp_path / "run.zarr.tmp").exists()
    assert zarr.open_group(str(out), mode="r")["surface/t2m"][0, 0, 0, 0] == pytest.approx(1.0)


def test_grid_mismatch_is_rejected(tmp_path):
    plan = plan_run(AROME_PE, RUN, [T2M], [3600, 7200], members=[0])
    store = RawStore(tmp_path / "raw")
    stage(plan, store)
    calls = iter([LATS, LATS + 0.01])

    def shifting(data: bytes) -> DecodedField:
        return DecodedField(np.load(io.BytesIO(data[4:])), next(calls), LONS)

    with pytest.raises(ValueError, match="grid differs"):
        write_run(tmp_path / "run.zarr", plan, store, decoder=shifting)
    assert not (tmp_path / "run.zarr").exists()


def test_crop_client_side():
    f = DecodedField(np.arange(96, dtype=np.float32).reshape(8, 12), LATS, LONS)
    c = crop(f, BBox(44.5, 45.0, 3.0, 3.5))
    np.testing.assert_allclose(c.lats, [44.5, 44.75, 45.0])
    np.testing.assert_allclose(c.lons, [3.0, 3.25, 3.5])
    assert c.values[0, 0] == f.values[2, 4]
    assert crop(f, None) is f
    with pytest.raises(ValueError):
        crop(f, BBox(10, 11, 0, 1))


def test_decode_grib_eccodes_orientation():
    eccodes = pytest.importorskip("eccodes")
    from ingest.zarr_writer import decode_grib_eccodes

    gid = eccodes.codes_grib_new_from_samples("regular_ll_sfc_grib2")
    try:
        eccodes.codes_set(gid, "Ni", 4)
        eccodes.codes_set(gid, "Nj", 3)
        eccodes.codes_set(gid, "iScansNegatively", 0)
        eccodes.codes_set(gid, "jScansPositively", 0)
        eccodes.codes_set_double(gid, "latitudeOfFirstGridPointInDegrees", 46.0)
        eccodes.codes_set_double(gid, "latitudeOfLastGridPointInDegrees", 44.0)
        eccodes.codes_set_double(gid, "longitudeOfFirstGridPointInDegrees", 359.0)
        eccodes.codes_set_double(gid, "longitudeOfLastGridPointInDegrees", 2.0)
        eccodes.codes_set_double(gid, "iDirectionIncrementInDegrees", 1.0)
        eccodes.codes_set_double(gid, "jDirectionIncrementInDegrees", 1.0)
        # Row-major from the north-west corner: value = 10 * row + col.
        values = np.array([[10 * j + i for i in range(4)] for j in range(3)], dtype=float)
        eccodes.codes_set_values(gid, values.ravel())
        msg = eccodes.codes_get_message(gid)
    finally:
        eccodes.codes_release(gid)

    f = decode_grib_eccodes(msg)
    np.testing.assert_allclose(f.lats, [44.0, 45.0, 46.0])
    np.testing.assert_allclose(f.lons, [-1.0, 0.0, 1.0, 2.0])
    assert f.values[0, 0] == pytest.approx(20.0)  # south-west corner
    assert f.values[2, 3] == pytest.approx(3.0)  # north-east corner


def test_static_field_and_transform(tmp_path):
    plan = plan_run(AROME_PE, RUN, [T2M], [3600, 7200], members=[0, 1])
    orog_var = Variable("orog", "SOME_OROGRAPHY__GROUND_OR_WATER_SURFACE")
    orog = plan_run(AROME_PE, RUN, [orog_var], [0], members=[0])
    store = RawStore(tmp_path / "raw")
    stage(plan + orog, store)
    seen = []

    def transform(var, data, steps):
        seen.append((var.name, data.shape, list(steps)))
        return data + 1

    out = tmp_path / "run.zarr"
    report = write_run(
        out,
        plan,
        store,
        decoder=fake_decoder,
        transform=transform,
        static=orog,
        var_attrs={"orog": {"units": "m"}},
    )
    assert report.fields == len(plan) + 1
    root = zarr.open_group(str(out), mode="r")
    a = root["surface/orog"]
    assert a.shape == (8, 12) and a.metadata.dimension_names == ("lat", "lon")
    assert a.attrs["units"] == "m"
    assert root["surface/t2m"].shape == (2, 2, 8, 12)  # static adds no member or lead time
    assert root["surface/t2m"][1, 0, 0, 0] == pytest.approx(1001.0 + 1)
    assert seen[0] == ("t2m", (2, 2, 8, 12), [3600, 7200])
    assert seen[1] == ("orog", (1, 1, 8, 12), [0])
