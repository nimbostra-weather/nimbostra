"""Assemble staged GRIB fields of one ensemble run into an atomically published Zarr.

Layout (see CLAUDE.md): one store per model/run with two groups,
``surface`` (member, time, lat, lon) and ``pressure`` (member, time, level, lat, lon).
Native grid is kept (only cropped to the bbox). Chunks hold all members and all
lead times over 8x8 points, so reading one point touches a single chunk.

The store is written to ``<name>.tmp`` and renamed once complete: readers never
see a half-written run.
"""

from __future__ import annotations

import logging
import shutil
import warnings
from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import zarr

from ingest.meteofrance_wcs import BBox, CoverageRequest, RawStore

logger = logging.getLogger(__name__)

SPATIAL_CHUNK = 8


@dataclass(frozen=True)
class DecodedField:
    """A 2D field on a regular lat/lon grid, latitudes and longitudes ascending."""

    values: np.ndarray  # (lat, lon)
    lats: np.ndarray
    lons: np.ndarray

    def __post_init__(self) -> None:
        if self.values.shape != (self.lats.size, self.lons.size):
            raise ValueError(
                f"values {self.values.shape} do not match grid ({self.lats.size}, {self.lons.size})"
            )


Decoder = Callable[[bytes], DecodedField]


def decode_grib_eccodes(data: bytes) -> DecodedField:
    """Decode a single-message regular_ll GRIB2 with eccodes."""
    import eccodes  # local import: heavy native dependency

    gid = eccodes.codes_new_from_message(data)
    try:
        return field_from_handle(gid)
    finally:
        eccodes.codes_release(gid)


def field_from_handle(gid: int) -> DecodedField:
    """Decode an open eccodes handle (regular_ll) into an ascending-axes field.

    The caller owns ``gid`` and releases it.
    """
    import eccodes  # local import: heavy native dependency

    grid_type = eccodes.codes_get(gid, "gridType")
    if grid_type != "regular_ll":
        raise ValueError(f"unsupported gridType {grid_type!r}")
    ni = eccodes.codes_get(gid, "Ni")
    nj = eccodes.codes_get(gid, "Nj")
    lat1 = eccodes.codes_get_double(gid, "latitudeOfFirstGridPointInDegrees")
    lat2 = eccodes.codes_get_double(gid, "latitudeOfLastGridPointInDegrees")
    lon1 = eccodes.codes_get_double(gid, "longitudeOfFirstGridPointInDegrees")
    lon2 = eccodes.codes_get_double(gid, "longitudeOfLastGridPointInDegrees")
    i_neg = eccodes.codes_get(gid, "iScansNegatively")
    j_pos = eccodes.codes_get(gid, "jScansPositively")
    if eccodes.codes_get(gid, "jPointsAreConsecutive"):
        raise ValueError("column-major GRIB scanning is not supported")
    missing = eccodes.codes_get_double(gid, "missingValue")
    values = eccodes.codes_get_double_array(gid, "values").reshape(nj, ni)
    has_bitmap = eccodes.codes_get(gid, "bitmapPresent")

    values = values.astype(np.float32)
    if has_bitmap:
        values[values == missing] = np.nan
    # Grids crossing the antimeridian of the 0..360 convention (e.g. 328 -> 42).
    if not i_neg and lon2 < lon1:
        lon2 += 360.0
    if i_neg and lon1 < lon2:
        lon1 += 360.0
    lats = np.linspace(lat1, lat2, nj)
    lons = np.linspace(lon1, lon2, ni)
    if bool(j_pos) == (lats[0] > lats[-1]):
        logger.debug("jScansPositively disagrees with first/last points; trusting the points")
    # Normalise to ascending latitudes and longitudes.
    if lats[0] > lats[-1]:
        lats = lats[::-1]
        values = values[::-1, :]
    if lons[0] > lons[-1]:
        lons = lons[::-1]
        values = values[:, ::-1]
    lons = np.where(lons >= 180.0, lons - 360.0, lons)
    if np.any(np.diff(lons) <= 0):
        order = np.argsort(lons)
        lons, values = lons[order], values[:, order]
    return DecodedField(np.ascontiguousarray(values), lats, lons)


def crop(field: DecodedField, bbox: BBox | None) -> DecodedField:
    """Client-side crop, a no-op when the server already cropped."""
    if bbox is None:
        return field
    eps = 1e-6
    ilat = np.flatnonzero((field.lats >= bbox.lat_min - eps) & (field.lats <= bbox.lat_max + eps))
    ilon = np.flatnonzero((field.lons >= bbox.lon_min - eps) & (field.lons <= bbox.lon_max + eps))
    if ilat.size == 0 or ilon.size == 0:
        raise ValueError("bbox does not intersect the field")
    sl_lat = slice(ilat[0], ilat[-1] + 1)
    sl_lon = slice(ilon[0], ilon[-1] + 1)
    return DecodedField(field.values[sl_lat, sl_lon], field.lats[sl_lat], field.lons[sl_lon])


@dataclass
class _Grid:
    lats: np.ndarray
    lons: np.ndarray

    def check(self, f: DecodedField, what: str) -> None:
        if (
            f.lats.shape != self.lats.shape
            or f.lons.shape != self.lons.shape
            or not np.allclose(f.lats, self.lats, atol=1e-6)
            or not np.allclose(f.lons, self.lons, atol=1e-6)
        ):
            raise ValueError(f"{what}: grid differs from the rest of its group")


@dataclass
class WriteReport:
    path: Path
    fields: int
    missing: int


def write_run(
    out_path: Path,
    plan: Iterable[CoverageRequest],
    store: RawStore,
    *,
    decoder: Decoder = decode_grib_eccodes,
    bbox: BBox | None = None,
    attrs: dict | None = None,
) -> WriteReport:
    """Build the run's Zarr from staged GRIBs and publish it atomically.

    Missing fields (404 or never fetched) become NaN. Memory peak is one
    variable at a time: members x steps x (levels) x lat x lon float32.
    """
    out_path = Path(out_path)
    plan = list(plan)
    if not plan:
        raise ValueError("empty plan")
    members = sorted({r.member for r in plan})
    steps = sorted({r.step_seconds for r in plan})
    m_idx = {m: i for i, m in enumerate(members)}
    t_idx = {s: i for i, s in enumerate(steps)}

    by_var: dict[str, list[CoverageRequest]] = defaultdict(list)
    for r in plan:
        by_var[r.variable.name].append(r)

    tmp = out_path.with_name(out_path.name + ".tmp")
    if tmp.exists():
        logger.warning("removing stale %s", tmp)
        shutil.rmtree(tmp)

    root = zarr.open_group(store=str(tmp), mode="w", zarr_format=3)
    first = plan[0]
    root.attrs.update(
        {
            "model": first.model.name,
            "run": first.run.astimezone(UTC).isoformat(),
            "created": datetime.now(UTC).isoformat(),
            **(attrs or {}),
        }
    )
    groups: dict[str, zarr.Group] = {}
    grids: dict[str, _Grid] = {}
    n_fields = n_missing = 0

    try:
        for name, reqs in by_var.items():
            var = reqs[0].variable
            levels = sorted({r.level for r in reqs if r.level is not None})
            l_idx = {lv: i for i, lv in enumerate(levels)}
            data: np.ndarray | None = None
            for r in reqs:
                raw = store.read(r)
                if raw is None:
                    n_missing += 1
                    continue
                f = crop(decoder(raw), bbox)
                grid = grids.get(var.group)
                if grid is None:
                    grid = grids[var.group] = _Grid(f.lats, f.lons)
                grid.check(f, r.key)
                if data is None:
                    shape = (len(members), len(steps))
                    if var.group == "pressure":
                        shape += (len(levels),)
                    data = np.full(shape + f.values.shape, np.nan, dtype=np.float32)
                if var.group == "pressure":
                    data[m_idx[r.member], t_idx[r.step_seconds], l_idx[r.level]] = f.values
                else:
                    data[m_idx[r.member], t_idx[r.step_seconds]] = f.values
                n_fields += 1
            if data is None:
                logger.warning("%s: no field at all, variable skipped", name)
                continue
            group = groups.get(var.group)
            if group is None:
                group = groups[var.group] = _create_group(
                    root, var.group, members, steps, grids[var.group], levels
                )
            elif var.group == "pressure" and list(group["level"][:]) != levels:
                raise ValueError(f"{name}: pressure levels differ within the group")
            dims = ["member", "time", "lat", "lon"]
            if var.group == "pressure":
                dims.insert(2, "level")
            chunks = data.shape[:-2] + (SPATIAL_CHUNK, SPATIAL_CHUNK)
            arr = group.create_array(
                name,
                shape=data.shape,
                dtype="float32",
                chunks=chunks,
                fill_value=np.nan,
                dimension_names=dims,
            )
            arr[...] = data
            arr.attrs.update({"indicator": var.indicator})
            del data
        with warnings.catch_warnings():
            # Consolidated metadata is a zarr-python extension in format 3; xarray uses it.
            warnings.filterwarnings("ignore", message="Consolidated metadata")
            zarr.consolidate_metadata(str(tmp))
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise

    _publish(tmp, out_path)
    return WriteReport(out_path, n_fields, n_missing)


def _create_group(
    root: zarr.Group,
    name: str,
    members: list[int],
    steps: list[int],
    grid: _Grid,
    levels: list[int],
) -> zarr.Group:
    g = root.create_group(name)
    coords: list[tuple[str, np.ndarray, dict]] = [
        ("member", np.asarray(members, dtype="int16"), {}),
        ("time", np.asarray(steps, dtype="int32"), {"units": "seconds since run"}),
        ("lat", grid.lats.astype("float64"), {"units": "degrees_north"}),
        ("lon", grid.lons.astype("float64"), {"units": "degrees_east"}),
    ]
    if name == "pressure":
        coords.append(("level", np.asarray(levels, dtype="int16"), {"units": "hPa"}))
    for cname, values, cattrs in coords:
        a = g.create_array(cname, shape=values.shape, dtype=values.dtype, dimension_names=[cname])
        a[...] = values
        a.attrs.update(cattrs)
    return g


def _publish(tmp: Path, final: Path) -> None:
    """Rename tmp to final. An existing run is swapped out, then deleted.

    A directory cannot be atomically replaced by rename on POSIX when the
    target exists, so the old store is moved aside first. Readers may see the
    path absent for the instant between the two renames, never a partial store.
    """
    if final.exists():
        old = final.with_name(final.name + ".old")
        if old.exists():
            shutil.rmtree(old)
        final.rename(old)
        tmp.rename(final)
        shutil.rmtree(old)
    else:
        tmp.rename(final)
