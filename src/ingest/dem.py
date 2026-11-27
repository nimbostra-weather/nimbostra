"""Copernicus DEM GLO-90 -> one Zarr elevation grid over France + ~100 km.

Facts relied on (readme of the ``copernicus-dem-90m`` AWS bucket and one tile
inspected on 2026-11-27):
- tiles of 1° x 1°, listed in ``tileList.txt``; file
  ``<name>/<name>.tif`` with ``<name>`` like ``Copernicus_DSM_COG_30_N45_00_E006_00_DEM``;
- no tile over the sea: height 0 there;
- float32 Cloud Optimized GeoTIFF, DEFLATE with floating-point predictor;
- 1200 rows of 3 arc-seconds (1/1200°); 1200 columns below 50° N, 800 from
  50° to 60° N; ``PixelIsPoint``, row 0 is the northern edge of the tile and the
  southern row is dropped, so tile ``N45`` covers latitudes 45° + 1/1200 .. 46°.

Output: ``<out>/glo90.zarr`` with ``elevation`` (lat, lon) on a regular
1/1200° grid, latitudes and longitudes ascending, chunks of 300 x 300 aligned
with the tiles. Tiles north of 50° N are resampled linearly in longitude.
Licence: Copernicus DEM, free of charge (see docs/lecture-point.md).
"""

from __future__ import annotations

import logging
import math
import os
import shutil
import warnings
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path

import httpx
import numpy as np
import zarr
from zarr.codecs import ZstdCodec

from ingest.zarr_writer import _publish

logger = logging.getLogger(__name__)

BUCKET_URL = "https://copernicus-dem-90m.s3.amazonaws.com"
PER_DEGREE = 1200  # 3 arc-seconds
CHUNK = 300
STORE_NAME = "glo90.zarr"


@dataclass(frozen=True)
class Extent:
    """Whole-degree extent covered by tiles: south/west edges of the first tiles."""

    lat_min: int = 40
    lat_max: int = 52  # exclusive: last tile N51
    lon_min: int = -7
    lon_max: int = 11  # exclusive: last tile E010

    def tiles(self) -> list[tuple[int, int]]:
        return [
            (lat, lon)
            for lat in range(self.lat_min, self.lat_max)
            for lon in range(self.lon_min, self.lon_max)
        ]

    @property
    def shape(self) -> tuple[int, int]:
        return (
            (self.lat_max - self.lat_min) * PER_DEGREE,
            (self.lon_max - self.lon_min) * PER_DEGREE,
        )

    def lats(self) -> np.ndarray:
        n = self.shape[0]
        return self.lat_min + (np.arange(n, dtype=np.float64) + 1) / PER_DEGREE

    def lons(self) -> np.ndarray:
        n = self.shape[1]
        return self.lon_min + np.arange(n, dtype=np.float64) / PER_DEGREE


# France + ~100 km (40.5-52 N, 6.5 W-10.5 E) rounded out to whole tiles.
FRANCE_EXTENT = Extent()


def tile_name(lat: int, lon: int) -> str:
    ns = f"N{lat:02d}" if lat >= 0 else f"S{-lat:02d}"
    ew = f"E{lon:03d}" if lon >= 0 else f"W{-lon:03d}"
    return f"Copernicus_DSM_COG_30_{ns}_00_{ew}_00_DEM"


def tile_url(name: str) -> str:
    return f"{BUCKET_URL}/{name}/{name}.tif"


# --------------------------------------------------------------------------- download


def download_tiles(
    extent: Extent,
    tiles_dir: Path,
    http: httpx.Client,
    *,
    sleep: Callable[[float], None] | None = None,
) -> dict[str, int]:
    """Fetch the tiles of ``extent`` not yet on disk, one at a time.

    ``tileList.txt`` tells which tiles exist (no tile over the sea), so absent
    ones are not requested. Returns counts of fetched, skipped and sea tiles.
    """
    tiles_dir = Path(tiles_dir)
    tiles_dir.mkdir(parents=True, exist_ok=True)
    resp = http.get(f"{BUCKET_URL}/tileList.txt", timeout=60)
    resp.raise_for_status()
    available = {line.strip() for line in resp.text.splitlines() if line.strip()}
    counts = {"fetched": 0, "skipped": 0, "sea": 0}
    for lat, lon in extent.tiles():
        name = tile_name(lat, lon)
        dest = tiles_dir / f"{name}.tif"
        if name not in available:
            counts["sea"] += 1
            continue
        if dest.exists():
            counts["skipped"] += 1
            continue
        part = dest.with_name(dest.name + ".part")
        with http.stream("GET", tile_url(name), timeout=httpx.Timeout(30, read=120)) as r:
            r.raise_for_status()
            with open(part, "wb") as fh:
                fh.writelines(r.iter_bytes(1 << 20))
        with open(part, "rb") as fh:
            if fh.read(4) not in (b"II*\x00", b"MM\x00*"):
                part.unlink()
                raise ValueError(f"{name}: not a TIFF file")
        os.replace(part, dest)
        counts["fetched"] += 1
        logger.info("%s downloaded", name)
        if sleep:
            sleep(0.1)
    return counts


# --------------------------------------------------------------------------- build


TileReader = Callable[[Path], tuple[np.ndarray, float, float, float, float]]


def read_tile_tifffile(path: Path) -> tuple[np.ndarray, float, float, float, float]:
    """Return (values north->south, lon0, lat0, dlon, dlat) of a GLO-90 GeoTIFF."""
    import tifffile  # optional dependency, only for building the DEM

    with tifffile.TiffFile(path) as tif:
        meta = tif.geotiff_metadata or {}
        values = tif.pages[0].asarray()
    scale = meta.get("ModelPixelScale")
    tie = meta.get("ModelTiepoint")
    if not scale or not tie:
        raise ValueError(f"{path.name}: no GeoTIFF georeferencing")
    return values.astype(np.float32), float(tie[3]), float(tie[4]), float(scale[0]), float(scale[1])


def _tile_block(
    values: np.ndarray, lon0: float, lat0: float, dlon: float, dlat: float, lat: int, lon: int
) -> np.ndarray:
    """Tile values on the target grid: (1200, 1200), latitudes ascending."""
    rows, cols = values.shape
    if rows != PER_DEGREE or not math.isclose(dlat, 1 / PER_DEGREE, rel_tol=1e-6):
        raise ValueError(f"tile N{lat} E{lon}: unexpected rows {rows} or spacing {dlat}")
    if not (math.isclose(lat0, lat + 1, abs_tol=1e-6) and math.isclose(lon0, lon, abs_tol=1e-6)):
        raise ValueError(f"tile N{lat} E{lon}: origin ({lat0}, {lon0}) does not match its name")
    block = values[::-1, :]  # ascending latitudes
    if cols != PER_DEGREE:
        src = lon0 + np.arange(cols) * dlon
        dst = lon + np.arange(PER_DEGREE) / PER_DEGREE
        block = np.stack([np.interp(dst, src, row) for row in block])
    return np.ascontiguousarray(block, dtype=np.float32)


def build_dem(
    tiles_dir: Path,
    out_dir: Path,
    *,
    extent: Extent = FRANCE_EXTENT,
    reader: TileReader = read_tile_tifffile,
) -> Path:
    """Assemble the tiles into ``<out_dir>/glo90.zarr`` (missing tiles = sea = 0 m)."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    final = out_dir / STORE_NAME
    tmp = final.with_name(final.name + ".tmp")
    if tmp.exists():
        shutil.rmtree(tmp)
    try:
        root = zarr.open_group(str(tmp), mode="w", zarr_format=3)
        root.attrs.update(
            {
                "source": "Copernicus DEM GLO-90 (2021 release), via registry.opendata.aws",
                "resolution": "3 arc-seconds",
                "sea": "no tile: 0 m",
            }
        )
        for name, values in (("lat", extent.lats()), ("lon", extent.lons())):
            a = root.create_array(name, shape=values.shape, dtype="float64", dimension_names=[name])
            a[...] = values
        elev = root.create_array(
            "elevation",
            shape=extent.shape,
            dtype="float32",
            chunks=(CHUNK, CHUNK),
            fill_value=0.0,
            compressors=ZstdCodec(level=3),
            dimension_names=["lat", "lon"],
        )
        elev.attrs["units"] = "m"
        n = 0
        for lat, lon in extent.tiles():
            path = Path(tiles_dir) / f"{tile_name(lat, lon)}.tif"
            if not path.exists():
                continue
            block = _tile_block(*reader(path), lat, lon)
            i = (lat - extent.lat_min) * PER_DEGREE
            j = (lon - extent.lon_min) * PER_DEGREE
            elev[i : i + PER_DEGREE, j : j + PER_DEGREE] = block
            n += 1
        root.attrs["tiles"] = n
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="Consolidated metadata")
            zarr.consolidate_metadata(str(tmp))
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    _publish(tmp, final)
    logger.info("DEM built from %d tiles -> %s", n, final)
    return final


# --------------------------------------------------------------------------- lookup


class DemSource:
    """Bilinear elevation lookup, usable as ``point.reader.ElevationSource``."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._group: zarr.Group | None = None
        self._lats: np.ndarray | None = None
        self._lons: np.ndarray | None = None

    def _open(self) -> None:
        if self._group is None:
            g = zarr.open_group(str(self.path), mode="r")
            self._lats, self._lons = g["lat"][:], g["lon"][:]
            self._group = g

    def __call__(self, lat: float, lon: float) -> float | None:
        self._open()
        lats, lons = self._lats, self._lons
        if not (lats[0] <= lat <= lats[-1] and lons[0] <= lon <= lons[-1]):
            return None
        i = int(min(np.searchsorted(lats, lat, side="right") - 1, lats.size - 2))
        j = int(min(np.searchsorted(lons, lon, side="right") - 1, lons.size - 2))
        fy = (lat - lats[i]) / (lats[i + 1] - lats[i])
        fx = (lon - lons[j]) / (lons[j + 1] - lons[j])
        b = self._group["elevation"][i : i + 2, j : j + 2].astype(float)
        value = (
            b[0, 0] * (1 - fy) * (1 - fx)
            + b[0, 1] * (1 - fy) * fx
            + b[1, 0] * fy * (1 - fx)
            + b[1, 1] * fy * fx
        )
        return float(value)


def open_dem(data_dir: Path) -> DemSource | None:
    """DEM of ``<data_dir>/dem`` if it was built, else None."""
    path = Path(data_dir) / "dem" / STORE_NAME
    return DemSource(path) if path.exists() else None


# --------------------------------------------------------------------------- CLI


def main(argv: Iterable[str] | None = None) -> int:
    import argparse
    import sys

    from ingest.dotenv import load_dotenv

    load_dotenv()
    ap = argparse.ArgumentParser(description="Build the GLO-90 elevation grid")
    ap.add_argument("command", choices=["download", "build"])
    ap.add_argument(
        "--tiles", type=Path, default=Path(os.environ.get("RAW_DIR", "data/raw")) / "dem"
    )
    ap.add_argument(
        "--out", type=Path, default=Path(os.environ.get("DATA_DIR", "data/zarr")) / "dem"
    )
    args = ap.parse_args(list(argv) if argv is not None else None)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.command == "download":
        with httpx.Client(follow_redirects=True) as http:
            counts = download_tiles(FRANCE_EXTENT, args.tiles, http)
        print(counts)
    else:
        print(build_dem(args.tiles, args.out))
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
