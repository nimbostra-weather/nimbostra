"""Tests for the GLO-90 DEM download, assembly and lookup (no network, synthetic tiles)."""

from __future__ import annotations

import httpx
import numpy as np
import pytest
import zarr

from ingest.dem import (
    PER_DEGREE,
    DemSource,
    Extent,
    build_dem,
    download_tiles,
    open_dem,
    tile_name,
    tile_url,
)

EXTENT = Extent(lat_min=45, lat_max=47, lon_min=-1, lon_max=1)


def test_tile_names():
    assert tile_name(45, 6) == "Copernicus_DSM_COG_30_N45_00_E006_00_DEM"
    assert tile_name(48, -5) == "Copernicus_DSM_COG_30_N48_00_W005_00_DEM"
    assert tile_url(tile_name(45, 6)).endswith(
        "/Copernicus_DSM_COG_30_N45_00_E006_00_DEM/Copernicus_DSM_COG_30_N45_00_E006_00_DEM.tif"
    )


def fake_tile(lat: int, lon: int, cols: int = PER_DEGREE):
    """Tile as read from a GeoTIFF: north->south rows; value = latitude + longitude/100."""
    lats = lat + 1 - np.arange(PER_DEGREE) / PER_DEGREE
    lons = lon + np.arange(cols) / cols
    values = (lats[:, None] + lons[None, :] / 100).astype(np.float32)
    return values, float(lon), float(lat + 1), 1 / cols, 1 / PER_DEGREE


def make_reader(cols_by_tile=None):
    def reader(path):
        name = path.name
        lat = int(name.split("_N")[1][:2])
        ew = name.split("_00_")[1]
        lon = int(ew[1:4]) * (1 if ew[0] == "E" else -1)
        return fake_tile(lat, lon, (cols_by_tile or {}).get((lat, lon), PER_DEGREE))

    return reader


def touch_tiles(tiles_dir, tiles):
    tiles_dir.mkdir(parents=True, exist_ok=True)
    for lat, lon in tiles:
        (tiles_dir / f"{tile_name(lat, lon)}.tif").write_bytes(b"II*\x00")


def test_build_and_lookup(tmp_path):
    # Tile (46, 0) missing: sea, elevation 0. Tile (46, -1) with 800 columns.
    touch_tiles(tmp_path / "tiles", [(45, -1), (45, 0), (46, -1)])
    path = build_dem(
        tmp_path / "tiles", tmp_path / "dem", extent=EXTENT, reader=make_reader({(46, -1): 800})
    )
    g = zarr.open_group(str(path), mode="r")
    assert g["elevation"].shape == (2 * PER_DEGREE, 2 * PER_DEGREE)
    assert g["elevation"].chunks == (300, 300)
    assert g.attrs["tiles"] == 3
    lats = g["lat"][:]
    assert lats[0] == pytest.approx(45 + 1 / PER_DEGREE) and lats[-1] == pytest.approx(47.0)

    dem = DemSource(path)
    assert dem(45.5, -0.5) == pytest.approx(45.5 - 0.005, abs=1e-4)
    assert dem(45.25, 0.75) == pytest.approx(45.25 + 0.0075, abs=1e-4)
    assert dem(46.5, 0.5) == pytest.approx(0.0)  # sea
    assert dem(46.5, -0.5) == pytest.approx(46.5 - 0.005, abs=1e-3)  # resampled tile
    assert dem(44.0, 0.0) is None
    assert open_dem(tmp_path) is not None
    assert open_dem(tmp_path / "nowhere") is None


def test_build_rejects_misplaced_tile(tmp_path):
    touch_tiles(tmp_path / "tiles", [(45, 0)])

    def wrong(path):
        values, lon0, lat0, dlon, dlat = fake_tile(45, 0)
        return values, lon0 + 1, lat0, dlon, dlat

    with pytest.raises(ValueError, match="origin"):
        build_dem(tmp_path / "tiles", tmp_path / "dem", extent=EXTENT, reader=wrong)
    assert not (tmp_path / "dem" / "glo90.zarr").exists()
    assert not (tmp_path / "dem" / "glo90.zarr.tmp").exists()


def test_download_skips_sea_and_existing(tmp_path):
    existing = tile_name(45, -1)
    touch_tiles(tmp_path, [(45, -1)])
    listed = [tile_name(45, -1), tile_name(45, 0), tile_name(46, -1)]
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path == "/tileList.txt":
            return httpx.Response(200, text="\n".join(listed) + "\n")
        return httpx.Response(200, content=b"II*\x00" + b"\x00" * 16)

    http = httpx.Client(transport=httpx.MockTransport(handler))
    counts = download_tiles(EXTENT, tmp_path, http)
    assert counts == {"fetched": 2, "skipped": 1, "sea": 1}
    assert not any(existing in c for c in calls)
    assert (tmp_path / f"{tile_name(45, 0)}.tif").exists()


def test_download_rejects_non_tiff(tmp_path):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/tileList.txt":
            return httpx.Response(200, text=tile_name(45, 0))
        return httpx.Response(200, content=b"<html>error</html>")

    http = httpx.Client(transport=httpx.MockTransport(handler))
    with pytest.raises(ValueError, match="not a TIFF"):
        download_tiles(EXTENT, tmp_path, http)
    assert not list(tmp_path.glob("*.tif"))


def test_forecast_uses_dem(tmp_path):
    from fastapi.testclient import TestClient
    from test_point import make_store

    from api.app import Settings, create_app

    make_store(tmp_path / "arome")
    client = TestClient(create_app(Settings(data_dir=tmp_path, dem=lambda lat, lon: 1200.0)))
    body = client.get("/v1/forecast/point", params={"lat": 45.2, "lon": 6.2}).json()
    assert body["elevation"] == {"model": 1000.0, "target": 1200.0, "source": "dem"}
