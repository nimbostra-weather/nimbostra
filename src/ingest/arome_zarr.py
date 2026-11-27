"""Convert one staged AROME 0.01° run (API Paquets GRIB2) into an atomically published Zarr.

Input: a complete staging directory written by ``ingest.meteofrance_paquets``
(``<raw>/<run tag>/{SP1,SP2,SP3}/<time>.grib2`` plus ``manifest.json``). Each
file holds many GRIB messages: one per parameter for one lead time.

Output: ``<out>/<run tag>.zarr`` with one group ``surface``:
- coordinates ``member`` (a single member 0, to share the ensemble layout),
  ``time`` (seconds since run), ``lat``, ``lon`` (ascending, native 0.01° grid
  cropped to France + ~100 km);
- one array per variable, dims (member, time, lat, lon), chunks of
  1 x all lead times x 32 x 32 points, zstd;
- ``orog`` (lat, lon): model orography from SP3, for the altitude correction.

Normalisation at ingestion (see CLAUDE.md): accumulations since run start are
turned into amounts per step, K -> °C, Pa -> hPa, cloud fractions -> %.
Units are converted from the GRIB ``units`` key, and an unexpected unit is an
error rather than a silent mistake.

Memory: the whole run is held in RAM before writing, about 400 MB per variable
on the cropped grid (52 lead times x ~1150 x 1700 float32), so ~6 GB in total.
"""

from __future__ import annotations

import json
import logging
import re
import shutil
import warnings
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import numpy as np
import zarr
from zarr.codecs import ZstdCodec

from ingest.meteofrance_paquets import RunStage, format_run, parse_run, run_tag, staged_runs
from ingest.meteofrance_wcs import FRANCE_BBOX, BBox, _atomic_write
from ingest.zarr_writer import DecodedField, _publish, crop, field_from_handle

logger = logging.getLogger(__name__)

SPATIAL_CHUNK = 32
ZSTD_LEVEL = 3
GRAVITY = 9.80665
LATEST = "latest.json"
_STORE = re.compile(r"^(\d{8}T\d{2}Z)\.zarr$")

Kind = Literal["instant", "max", "accum", "static"]


# --------------------------------------------------------------------------- variables


@dataclass(frozen=True)
class VarSpec:
    """How to recognise one variable among the GRIB messages, and its stored unit."""

    name: str
    short_names: tuple[str, ...]
    units: str
    kind: Kind = "instant"
    type_of_level: str | None = None
    level: int | None = None
    description: str = ""

    def matches(self, meta: MessageMeta) -> bool:
        if meta.short_name not in self.short_names:
            return False
        if self.type_of_level is not None and meta.type_of_level != self.type_of_level:
            return False
        return self.level is None or meta.level == self.level


# eccodes 2.50 short names; MF local names (e.g. CAPE_INS) accepted as aliases.
# Time-maximum gusts come out as ``max_10efg`` / ``max_10nfg`` on real AROME files.
# Run ``python -m ingest.arome_convert inventory`` on a real run to check them.
AROME_VARIABLES: tuple[VarSpec, ...] = (
    VarSpec("t2m", ("2t",), "degC", "instant", "heightAboveGround", 2, "2 m temperature"),
    VarSpec("r2", ("2r",), "%", "instant", "heightAboveGround", 2, "2 m relative humidity"),
    VarSpec("u10", ("10u",), "m s-1", "instant", "heightAboveGround", 10, "10 m eastward wind"),
    VarSpec("v10", ("10v",), "m s-1", "instant", "heightAboveGround", 10, "10 m northward wind"),
    VarSpec(
        "gust_u10",
        ("10efg", "max_10efg"),
        "m s-1",
        "max",
        "heightAboveGround",
        10,
        "10 m eastward gust component, max over the past step",
    ),
    VarSpec(
        "gust_v10",
        ("10nfg", "max_10nfg"),
        "m s-1",
        "max",
        "heightAboveGround",
        10,
        "10 m northward gust component, max over the past step",
    ),
    VarSpec("rain", ("tirf",), "mm", "accum", description="liquid precipitation per step"),
    VarSpec("snow", ("tsnowp",), "mm", "accum", description="snowfall (water equivalent) per step"),
    VarSpec("graupel", ("tgrp",), "mm", "accum", description="graupel per step"),
    VarSpec("lcc", ("lcc",), "%", description="low cloud cover"),
    VarSpec("mcc", ("mcc",), "%", description="medium cloud cover"),
    VarSpec("hcc", ("hcc",), "%", description="high cloud cover"),
    VarSpec("cape", ("CAPE_INS", "cape"), "J kg-1", description="convective available energy"),
    VarSpec("sp", ("sp",), "hPa", type_of_level="surface", description="surface pressure"),
    VarSpec("mslp", ("prmsl",), "hPa", description="mean sea level pressure"),
    VarSpec("orog", ("orog", "h", "z"), "m", "static", description="model orography"),
)
REQUIRED = frozenset({"t2m", "u10", "v10", "orog"})
# Packages read only for static fields.
STATIC_PACKAGES = frozenset({"SP3"})


def normalize_units(units: str) -> str:
    """Spelling-independent form of a unit string.

    eccodes writes exponents as ``m**2 s**-2`` while other tables use
    ``m2 s-2``: drop ``**`` and collapse whitespace so both compare equal.
    """
    return " ".join(units.replace("**", "").split())


# Conversions from the (normalized) GRIB ``units`` key to the stored unit.
# Keyed on the target too: ``m2 s-2`` is a geopotential for ``m`` (divided by g)
# but a specific energy for ``J kg-1`` (1 J kg-1 = 1 m2 s-2, identity).
_CONVERSIONS: dict[tuple[str, str], Callable[[np.ndarray], np.ndarray]] = {
    ("K", "degC"): lambda v: v - np.float32(273.15),
    ("Pa", "hPa"): lambda v: v / np.float32(100.0),
    ("(0 - 1)", "%"): lambda v: v * np.float32(100.0),
    ("0-1", "%"): lambda v: v * np.float32(100.0),
    ("1", "%"): lambda v: v * np.float32(100.0),
    ("Proportion", "%"): lambda v: v * np.float32(100.0),
    ("m2 s-2", "m"): lambda v: v / np.float32(GRAVITY),
}
# Same quantity, same scale: stored as is.
_SAME_UNITS = {
    ("kg m-2", "mm"),
    ("m2 s-2", "J kg-1"),
}


def convert_units(values: np.ndarray, grib_units: str, target: str) -> np.ndarray:
    key = (normalize_units(grib_units), normalize_units(target))
    if key[0] == key[1] or key in _SAME_UNITS:
        return values
    fn = _CONVERSIONS.get(key)
    if fn is None:
        raise ValueError(f"no conversion from GRIB units {grib_units!r} to {target!r}")
    return fn(values)


def per_step(cumulative: np.ndarray, steps: np.ndarray) -> np.ndarray:
    """Accumulations since run start (time first) -> amount over each step.

    The value at lead time 0 is NaN (nothing accumulated yet, no field). The
    accumulation at the first listed step after 0 is taken from 0. A missing
    field (NaN) makes its own step and the next one NaN. Small negative values
    from GRIB packing are clipped to 0.
    """
    out = np.empty_like(cumulative)
    prev = np.zeros_like(cumulative[0])
    for i, step in enumerate(steps):
        if step == 0:
            out[i] = np.nan
            prev = np.zeros_like(cumulative[0])
            continue
        cur = cumulative[i]
        out[i] = cur - prev
        prev = cur
    np.maximum(out, 0, out=out, where=~np.isnan(out))
    return out


# --------------------------------------------------------------------------- GRIB reading


@dataclass(frozen=True)
class MessageMeta:
    short_name: str
    type_of_level: str
    level: int
    step_type: str
    end_step: int  # hours
    units: str
    name: str = ""

    @property
    def key(self) -> str:
        base = f"{self.short_name}_{self.type_of_level}_{self.level}"
        return base if self.step_type == "instant" else f"{base}_{self.step_type}"


MessageReader = Callable[
    [Path, Callable[[MessageMeta], bool]], Iterator[tuple[MessageMeta, DecodedField | None]]
]


def read_messages_eccodes(
    path: Path, want: Callable[[MessageMeta], bool]
) -> Iterator[tuple[MessageMeta, DecodedField | None]]:
    """Yield every message of a multi-message GRIB file; decode values only when wanted."""
    import eccodes  # local import: heavy native dependency

    with open(path, "rb") as fh:
        while True:
            gid = eccodes.codes_grib_new_from_file(fh)
            if gid is None:
                return
            try:
                meta = _meta(eccodes, gid)
                yield meta, (field_from_handle(gid) if want(meta) else None)
            finally:
                eccodes.codes_release(gid)


def _meta(eccodes, gid) -> MessageMeta:
    def get(key: str, default: str = "") -> str:
        try:
            return str(eccodes.codes_get(gid, key))
        except Exception:  # noqa: BLE001 - eccodes raises many key-not-found variants
            return default

    short = get("shortName", "unknown")
    if short in ("unknown", ""):
        short = "d{}c{}n{}".format(
            get("discipline"), get("parameterCategory"), get("parameterNumber")
        )
    unit = get("stepUnits", "1")
    end_step = int(get("endStep", "0"))
    if unit == "0":  # minutes
        end_step //= 60
    return MessageMeta(
        short_name=short,
        type_of_level=get("typeOfLevel"),
        level=int(get("level", "0")),
        step_type=get("stepType", "instant"),
        end_step=end_step,
        units=get("units"),
        name=get("name"),
    )


# --------------------------------------------------------------------------- conversion


@dataclass
class ConvertReport:
    path: Path
    run: datetime
    variables: list[str]
    steps: list[int]
    fields: int = 0
    unknown: dict[str, int] = field(default_factory=dict)
    missing: dict[str, list[int]] = field(default_factory=dict)


def _match(meta: MessageMeta, specs: Iterable[VarSpec]) -> VarSpec | None:
    for spec in specs:
        if spec.matches(meta):
            return spec
    return None


def convert_run(
    raw_root: Path,
    run: datetime,
    out_root: Path,
    *,
    reader: MessageReader = read_messages_eccodes,
    variables: tuple[VarSpec, ...] = AROME_VARIABLES,
    bbox: BBox | None = FRANCE_BBOX,
    static_packages: frozenset[str] = STATIC_PACKAGES,
    attrs: dict | None = None,
) -> ConvertReport:
    """Build ``<out_root>/<run tag>.zarr`` from a complete staged run and publish it.

    Packages in ``static_packages`` only provide static fields (orography): SP3
    also carries other parameters that must not override SP1/SP2 fields.
    """
    stage = RunStage(Path(raw_root), run)
    manifest = stage.read_manifest()
    if manifest is None:
        raise ValueError(f"{stage.path}: run is not complete (no manifest)")
    if parse_run(manifest["run"]) != run:
        raise ValueError(f"{stage.path}: manifest is for {manifest['run']}")
    files = [
        (package, stage.path / package / f"{label}.grib2")
        for package, labels in manifest["packages"].items()
        for label in labels
    ]
    time_specs = tuple(v for v in variables if v.kind != "static")
    static_specs = tuple(v for v in variables if v.kind == "static")

    # Pass over every file, collecting fields per (variable, lead hour).
    fields: dict[str, dict[int, np.ndarray]] = {}
    static: dict[str, np.ndarray] = {}
    units_seen: dict[str, str] = {}
    unknown: dict[str, int] = {}
    grid: tuple[np.ndarray, np.ndarray] | None = None
    n_fields = 0

    for package, path in files:
        specs_here = static_specs if package in static_packages else variables

        def want(meta: MessageMeta, specs_here=specs_here) -> bool:
            spec = _match(meta, specs_here)
            return spec is not None and not (spec.kind == "static" and spec.name in static)

        for meta, decoded in reader(path, want):
            spec = _match(meta, specs_here)
            if spec is None and package in static_packages and _match(meta, time_specs):
                continue  # e.g. a 2 m temperature inside SP3: ignored on purpose
            if spec is None:
                unknown[meta.key] = unknown.get(meta.key, 0) + 1
                continue
            if decoded is None:
                continue  # static field already read
            f = crop(decoded, bbox)
            if grid is None:
                grid = (f.lats, f.lons)
            elif not (
                f.lats.shape == grid[0].shape
                and f.lons.shape == grid[1].shape
                and np.allclose(f.lats, grid[0], atol=1e-6)
                and np.allclose(f.lons, grid[1], atol=1e-6)
            ):
                raise ValueError(f"{path.name} {meta.key}: grid differs from the rest of the run")
            prev_units = units_seen.setdefault(spec.name, meta.units)
            if prev_units != meta.units:
                raise ValueError(
                    f"{spec.name}: units changed from {prev_units!r} to {meta.units!r}"
                )
            values = convert_units(f.values.astype(np.float32, copy=False), meta.units, spec.units)
            if spec.kind == "static":
                static[spec.name] = values
            else:
                by_step = fields.setdefault(spec.name, {})
                if meta.end_step in by_step:
                    logger.warning(
                        "%s: duplicate field at +%dh, keeping the last", spec.name, meta.end_step
                    )
                by_step[meta.end_step] = values
            n_fields += 1

    if grid is None:
        raise ValueError(f"{stage.path}: no known variable in the run")
    absent = sorted(REQUIRED - set(fields) - set(static))
    if absent:
        raise ValueError(
            f"{stage.path}: required variables missing: {absent}; "
            f"unknown GRIB keys: {sorted(unknown)}"
        )

    # Lead times: every hour seen in any instant field, plus 0 if any file has it.
    steps = np.array(sorted({h for by_step in fields.values() for h in by_step}), dtype=np.int32)
    t_idx = {int(h): i for i, h in enumerate(steps)}
    shape = (steps.size, grid[0].size, grid[1].size)

    out_root = Path(out_root)
    out_root.mkdir(parents=True, exist_ok=True)
    final = out_root / f"{run_tag(run)}.zarr"
    tmp = final.with_name(final.name + ".tmp")
    if tmp.exists():
        logger.warning("removing stale %s", tmp)
        shutil.rmtree(tmp)

    report = ConvertReport(
        final, run, variables=[], steps=[int(h) for h in steps], fields=n_fields, unknown=unknown
    )
    try:
        root = zarr.open_group(store=str(tmp), mode="w", zarr_format=3)
        root.attrs.update(
            {
                "model": manifest["model"],
                "grid": manifest["grid"],
                "run": format_run(run),
                "created": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "source": "Météo-France",
                "license": "Licence Ouverte Etalab 2.0",
                **(attrs or {}),
            }
        )
        group = _create_surface_group(root, steps, grid)
        codec = ZstdCodec(level=ZSTD_LEVEL)
        chunk_ll = (min(SPATIAL_CHUNK, shape[1]), min(SPATIAL_CHUNK, shape[2]))
        specs = {s.name: s for s in variables}

        for name, by_step in fields.items():
            spec = specs[name]
            data = np.full(shape, np.nan, dtype=np.float32)
            for h, values in by_step.items():
                data[t_idx[h]] = values
            by_step.clear()  # release per-step arrays as we go
            if spec.kind == "accum":
                data = per_step(data, steps)
            expected = [int(h) for h in steps if not (spec.kind in ("accum", "max") and h == 0)]
            gaps = [h for h in expected if np.isnan(data[t_idx[h]]).all()]
            if gaps:
                report.missing[name] = gaps
            arr = group.create_array(
                name,
                shape=(1, *shape),
                dtype="float32",
                chunks=(1, shape[0], *chunk_ll),
                fill_value=np.nan,
                compressors=codec,
                dimension_names=["member", "time", "lat", "lon"],
            )
            arr[0] = data
            arr.attrs.update(_var_attrs(spec, units_seen[name]))
            report.variables.append(name)
            del data

        for name, values in static.items():
            spec = specs[name]
            arr = group.create_array(
                name,
                shape=values.shape,
                dtype="float32",
                chunks=chunk_ll,
                fill_value=np.nan,
                compressors=codec,
                dimension_names=["lat", "lon"],
            )
            arr[...] = values
            arr.attrs.update(_var_attrs(spec, units_seen[name]))
            report.variables.append(name)

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="Consolidated metadata")
            zarr.consolidate_metadata(str(tmp))
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise

    _publish(tmp, final)
    return report


def _var_attrs(spec: VarSpec, grib_units: str) -> dict:
    attrs = {"units": spec.units, "kind": spec.kind, "grib_units": grib_units}
    if spec.description:
        attrs["description"] = spec.description
    if spec.kind == "accum":
        attrs["accumulation"] = "per step, ending at the time coordinate"
    if spec.kind == "max":
        attrs["cell_method"] = "time: maximum over the step ending at the time coordinate"
    return attrs


def _create_surface_group(
    root: zarr.Group, steps: np.ndarray, grid: tuple[np.ndarray, np.ndarray]
) -> zarr.Group:
    g = root.create_group("surface")
    coords = [
        ("member", np.zeros(1, dtype="int16"), {}),
        ("time", (steps.astype("int32") * 3600), {"units": "seconds since run"}),
        ("lat", grid[0].astype("float64"), {"units": "degrees_north"}),
        ("lon", grid[1].astype("float64"), {"units": "degrees_east"}),
    ]
    for cname, values, cattrs in coords:
        a = g.create_array(cname, shape=values.shape, dtype=values.dtype, dimension_names=[cname])
        a[...] = values
        a.attrs.update(cattrs)
    return g


# --------------------------------------------------------------------------- published runs


def published_runs(out_root: Path) -> list[datetime]:
    """Runs with a published Zarr under ``out_root``, oldest first."""
    out_root = Path(out_root)
    if not out_root.is_dir():
        return []
    runs = []
    for p in out_root.iterdir():
        m = _STORE.match(p.name)
        if m and p.is_dir():
            runs.append(datetime.strptime(m.group(1), "%Y%m%dT%HZ").replace(tzinfo=UTC))
    return sorted(runs)


def write_latest(out_root: Path, run: datetime) -> None:
    """Point readers at ``run``: ``latest.json`` is replaced atomically."""
    payload = {"run": format_run(run), "path": f"{run_tag(run)}.zarr"}
    _atomic_write(Path(out_root) / LATEST, json.dumps(payload).encode())


def read_latest(out_root: Path) -> datetime | None:
    p = Path(out_root) / LATEST
    if not p.exists():
        return None
    return parse_run(json.loads(p.read_text())["run"])


def prune_stores(out_root: Path, keep: int) -> list[datetime]:
    """Delete published stores except the ``keep`` most recent and the one in latest.json."""
    if keep < 1:
        raise ValueError("keep must be >= 1")
    runs = published_runs(out_root)
    kept = set(runs[-keep:])
    latest = read_latest(out_root)
    if latest is not None:
        kept.add(latest)
    removed = []
    for run in runs:
        if run not in kept:
            shutil.rmtree(Path(out_root) / f"{run_tag(run)}.zarr")
            removed.append(run)
    return removed


def next_run_to_convert(raw_root: Path, out_root: Path) -> datetime | None:
    """Latest complete staged run that has no published store yet."""
    staged = staged_runs(raw_root)
    if not staged:
        return None
    latest = staged[-1]
    return None if latest in published_runs(out_root) else latest
