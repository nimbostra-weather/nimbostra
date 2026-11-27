"""Read a forecast at one point from the published AROME Zarr stores.

- ``RunStore`` finds the run to read (``latest.json``) and caches open groups.
- ``read_raw`` interpolates every stored variable at (lat, lon): bilinear on the
  native grid, or nearest grid point. A NaN corner is dropped and the weights of
  the others renormalised, so coastal and edge points still get a value.
- ``derive`` adds the read-time quantities of CLAUDE.md: altitude correction,
  wind, gusts, dewpoint, wet-bulb, precipitation type, rain-snow limit
  estimate and apparent temperature.

No GRIB is read here: only Zarr chunks (1 x all lead times x 32 x 32 points).
"""

from __future__ import annotations

import json
import threading
from collections import OrderedDict
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

import numpy as np
import zarr

from point import derived

LATEST = "latest.json"
Method = Literal["bilinear", "nearest"]
# Below this share of the total bilinear weight, valid corners are too far: NaN.
MIN_WEIGHT = 0.25


class OutOfDomain(ValueError):
    """The point is outside the model grid."""


class NoRun(LookupError):
    """No published run to read."""


# --------------------------------------------------------------------------- stores


def _parse_run(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)


class RunStore:
    """Published runs of one model under ``root`` (e.g. ``$DATA_DIR/arome``)."""

    def __init__(self, root: Path, model: str, cache_size: int = 2) -> None:
        self.root = Path(root)
        self.model = model
        self._cache: OrderedDict[str, zarr.Group] = OrderedDict()
        self._cache_size = cache_size
        self._lock = threading.Lock()

    def latest(self) -> tuple[datetime, Path]:
        p = self.root / LATEST
        try:
            payload = json.loads(p.read_text())
        except FileNotFoundError as exc:
            raise NoRun(f"{self.model}: no published run ({p} missing)") from exc
        return _parse_run(payload["run"]), self.root / payload["path"]

    def group(self, path: Path) -> zarr.Group:
        """Open the ``surface`` group of a store, caching the most recent ones.

        Stores are replaced by rename, never modified in place, so a cached
        handle stays valid until its directory is pruned; the key is the path,
        and a new run always has a new path.
        """
        key = str(path)
        with self._lock:
            g = self._cache.get(key)
            if g is not None:
                self._cache.move_to_end(key)
                return g
        if not path.exists():
            raise NoRun(f"{self.model}: store {path} missing")
        g = zarr.open_group(str(path), mode="r")["surface"]
        with self._lock:
            self._cache[key] = g
            while len(self._cache) > self._cache_size:
                self._cache.popitem(last=False)
        return g


# --------------------------------------------------------------------------- interpolation


@dataclass(frozen=True)
class Stencil:
    """Grid cell around the point: lower-left indices and the 2x2 weights."""

    i: int  # latitude index of the southern row
    j: int  # longitude index of the western column
    weights: np.ndarray  # (2, 2), rows south->north, cols west->east
    grid_lat: float  # nearest grid point, for information
    grid_lon: float


def _axis_position(axis: np.ndarray, x: float, what: str) -> tuple[int, float]:
    if axis.size < 2:
        raise OutOfDomain(f"{what} axis has fewer than 2 points")
    eps = 1e-9
    if x < axis[0] - eps or x > axis[-1] + eps:
        raise OutOfDomain(f"{what} {x} outside [{axis[0]:.3f}, {axis[-1]:.3f}]")
    i = int(np.clip(np.searchsorted(axis, x, side="right") - 1, 0, axis.size - 2))
    frac = float(np.clip((x - axis[i]) / (axis[i + 1] - axis[i]), 0.0, 1.0))
    return i, frac


def make_stencil(
    lats: np.ndarray, lons: np.ndarray, lat: float, lon: float, method: Method = "bilinear"
) -> Stencil:
    i, fy = _axis_position(lats, lat, "latitude")
    j, fx = _axis_position(lons, lon, "longitude")
    if method == "bilinear":
        w = np.array([[(1 - fy) * (1 - fx), (1 - fy) * fx], [fy * (1 - fx), fy * fx]])
    elif method == "nearest":
        w = np.zeros((2, 2))
        w[int(fy >= 0.5), int(fx >= 0.5)] = 1.0
    else:
        raise ValueError(f"unknown method {method!r}")
    gi, gj = i + int(fy >= 0.5), j + int(fx >= 0.5)
    return Stencil(i, j, w, float(lats[gi]), float(lons[gj]))


def apply_stencil(block: np.ndarray, s: Stencil) -> np.ndarray:
    """Weighted value of a (..., 2, 2) block, renormalised over non-NaN corners."""
    block = np.asarray(block, dtype=float)
    valid = ~np.isnan(block)
    w = np.broadcast_to(s.weights, block.shape)
    den = np.sum(np.where(valid, w, 0.0), axis=(-2, -1))
    num = np.sum(np.where(valid, block * w, 0.0), axis=(-2, -1))
    with np.errstate(invalid="ignore", divide="ignore"):
        out = num / den
    if s.weights.max() == 1.0:  # nearest: no fallback to another point
        return np.where(den > 0.5, out, np.nan)
    return np.where(den >= MIN_WEIGHT, out, np.nan)


# --------------------------------------------------------------------------- reading


@dataclass
class PointData:
    """Values at one point. ``series`` are per lead time, ``static`` scalars."""

    model: str
    run: datetime
    lat: float
    lon: float
    method: Method
    grid_lat: float
    grid_lon: float
    times: list[datetime]
    series: dict[str, np.ndarray] = field(default_factory=dict)
    units: dict[str, str] = field(default_factory=dict)
    static: dict[str, float] = field(default_factory=dict)
    meta: dict[str, object] = field(default_factory=dict)

    def to_json(self) -> dict:
        """Columnar JSON-ready dict; NaN become null."""

        def clean(a: Iterable) -> list:
            return [None if (isinstance(x, float) and np.isnan(x)) else x for x in a]

        variables = {}
        for name, values in self.series.items():
            if values.dtype.kind == "f":
                values = clean(np.round(values.astype(float), 2).tolist())
            else:
                values = clean(values.tolist())
            variables[name] = {"units": self.units.get(name, ""), "values": values}
        return {
            "model": self.model,
            "run": self.run.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "point": {"lat": self.lat, "lon": self.lon},
            "grid": {
                "method": self.method,
                "nearest_lat": self.grid_lat,
                "nearest_lon": self.grid_lon,
            },
            **self.meta,
            "times": [t.strftime("%Y-%m-%dT%H:%M:%SZ") for t in self.times],
            "variables": variables,
            "attribution": "Source : Météo-France (Licence Ouverte Etalab 2.0)",
        }


def read_raw(
    group: zarr.Group,
    model: str,
    run: datetime,
    lat: float,
    lon: float,
    *,
    method: Method = "bilinear",
    variables: Iterable[str] | None = None,
) -> PointData:
    """Interpolate the stored variables of one run at (lat, lon), without correction."""
    lats, lons = group["lat"][:], group["lon"][:]
    s = make_stencil(lats, lons, lat, lon, method)
    seconds = group["time"][:]
    times = [run + timedelta(seconds=int(x)) for x in seconds]
    wanted = set(variables) if variables is not None else None
    data = PointData(model, run, lat, lon, method, s.grid_lat, s.grid_lon, times)
    for name, arr in group.arrays():
        if name in ("lat", "lon", "time", "member"):
            continue
        if wanted is not None and name not in wanted and name != "orog":
            continue
        dims = tuple(arr.metadata.dimension_names or ())
        if dims == ("lat", "lon"):
            value = apply_stencil(arr[s.i : s.i + 2, s.j : s.j + 2], s)
            data.static[name] = float(value)
        elif dims == ("member", "time", "lat", "lon"):
            block = arr[0, :, s.i : s.i + 2, s.j : s.j + 2]
            data.series[name] = apply_stencil(block, s).astype(np.float32)
        else:
            continue
        data.units[name] = arr.attrs.get("units", "")
    if wanted is not None:
        unknown = wanted - set(data.series) - set(data.static)
        if unknown:
            raise KeyError(f"unknown variables: {sorted(unknown)}")
    return data


ElevationSource = Callable[[float, float], float | None]


def derive(
    raw: PointData, *, elevation: float | None = None, dem: ElevationSource | None = None
) -> PointData:
    """Altitude-corrected forecast with derived quantities.

    Target elevation: ``elevation`` if given, else ``dem(lat, lon)``, else the
    model orography (no correction).
    """
    model_elev = raw.static.get("orog")
    if elevation is not None:
        target, source = float(elevation), "user"
    elif dem is not None and (dem_value := dem(raw.lat, raw.lon)) is not None:
        target, source = float(dem_value), "dem"
    else:
        target, source = model_elev, "model"
    s = raw.series
    out = PointData(
        raw.model, raw.run, raw.lat, raw.lon, raw.method, raw.grid_lat, raw.grid_lon, raw.times
    )
    out.meta["elevation"] = {
        "model": None if model_elev is None else round(model_elev, 1),
        "target": None if target is None else round(target, 1),
        "source": source,
    }

    def put(name: str, values, units: str) -> None:
        out.series[name] = np.asarray(values, dtype=float)
        out.units[name] = units

    t = s.get("t2m")
    if t is not None and model_elev is not None and target is not None:
        t = derived.correct_temperature(t, model_elev, target)
    if t is not None:
        put("temperature", t, "degC")
    rh = s.get("r2")
    if rh is not None:
        put("humidity", rh, "%")
    if t is not None and rh is not None:
        put("dewpoint", derived.dewpoint(t, rh), "degC")
        tw = derived.wet_bulb(t, rh)
        put("wet_bulb", tw, "degC")
        if target is not None:
            put("rain_snow_limit", derived.rain_snow_limit_estimate(tw, target), "m")
    wind = None
    if "u10" in s and "v10" in s:
        wind = derived.wind_speed(s["u10"], s["v10"])
        put("wind_speed", wind, "m s-1")
        put("wind_direction", derived.wind_direction(s["u10"], s["v10"]), "deg")
    if "gust_u10" in s and "gust_v10" in s:
        put("wind_gust", derived.wind_speed(s["gust_u10"], s["gust_v10"]), "m s-1")
    if t is not None and rh is not None and wind is not None:
        put("feels_like", derived.feels_like(t, rh, wind), "degC")
    if "rain" in s:
        snow, graupel = s.get("snow"), s.get("graupel")
        put("precipitation", derived.total_precipitation(s["rain"], snow, graupel), "mm")
        for name in ("rain", "snow", "graupel"):
            if name in s:
                put(name, s[name], "mm")
        out.series["precipitation_type"] = np.array(
            derived.precipitation_type(s["rain"], snow, graupel), dtype=object
        )
        out.units["precipitation_type"] = "|".join(derived.PRECIP_TYPES)
    for name in ("lcc", "mcc", "hcc"):
        if name in s:
            put(f"cloud_{name[0]}", s[name], "%")
    if "sp" in s:
        sp = s["sp"]
        if model_elev is not None and target is not None and "t2m" in s:
            sp = derived.correct_pressure(sp, s["t2m"], model_elev, target)
        put("surface_pressure", sp, "hPa")
    if "mslp" in s:
        put("pressure_msl", s["mslp"], "hPa")
    if "cape" in s:
        put("cape", s["cape"], "J kg-1")
    return out
