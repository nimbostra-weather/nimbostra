"""AROME-PE (PE-AROME, 25 members, 0.025°) ingestion through the Météo-France WCS services.

Usage (from ``src``, credentials in the environment or a ``.env`` file):
  python -m ingest.arome_pe check                # 3 calls: capabilities, time axis, one field
  python -m ingest.arome_pe plan [--offline]     # call count and duration of the next run
  python -m ingest.arome_pe fetch [--members 0,1 --max-lead 3]  # stage raw GRIBs (resumable)
  python -m ingest.arome_pe convert [--keep 2]   # staged run -> Zarr, then latest.json

Pipeline for one run:
1. ``GetCapabilities`` on member 0 (1 call) lists the coverage ids. Each canonical
   variable is resolved to the first of its candidate indicators that exists for
   the run, with a preferred accumulation interval. Indicator names are not
   documented publicly: the candidates below come from
   the indicators listed by GetCapabilities on 2026-11-27. An unresolved
   optional variable is skipped and reported, never guessed.
2. ``DescribeCoverage`` of t2m on member 0 (1 call) gives the time axis. Without
   it, the lead time defaults to 51 h for 09z/21z and 45 h for 03z/15z.
3. Two canary calls (t2m at the last lead time, first and last member) check
   that the run is fully published before spending the quota.
4. Unit calls (one member, one lead time, one variable), member by member. A
   member whose fields are mostly missing stops the download (circuit breaker),
   and a call budget is checked before starting.
5. Conversion to Zarr via ``zarr_writer.write_run``: units normalised from the
   GRIB ``units`` key (K -> °C, Pa -> hPa, fraction -> %, kg m-2 -> mm),
   accumulations since run start converted to amounts per step, static
   fields (none for AROME-PE today) stored as 2D (lat, lon) arrays.

Resumption: raw GRIBs are staged under ``$RAW_DIR/arome-pe/<run>/`` with the
resolution (indicators, intervals, steps) in ``resolution.json``. A rerun skips
every staged field and does not call GetCapabilities again.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import sys
import xml.etree.ElementTree as ET
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

import httpx
import numpy as np

from ingest.dotenv import load_dotenv
from ingest.meteofrance_wcs import (
    AROME_PE,
    FRANCE_BBOX,
    ApiKeyAuth,
    ApplicationTokenAuth,
    Auth,
    BBox,
    CoverageNotFound,
    CoverageRequest,
    IngestReport,
    RateLimiter,
    RawStore,
    Variable,
    WcsAuthError,
    WcsClient,
    WcsError,
    _atomic_write,
    _is_expired_token,
    coverage_id,
    format_run,
    parse_retry_after,
)
from ingest.zarr_writer import DecodedField, field_from_handle, write_run

logger = logging.getLogger("ingest.arome_pe")

MODEL = AROME_PE
RUN_HOURS = (3, 9, 15, 21)
# Meteociel (PE-AROME page, read 2026-11-27): 51 h for 09z/21z, 45 h for 03z/15z.
# DescribeCoverage is authoritative when available.
DEFAULT_MAX_LEAD_HOURS = {3: 45, 9: 51, 15: 45, 21: 51}
STEP_SECONDS = 3600
GRAVITY = 9.80665
RESOLUTION_FILE = "resolution.json"
LATEST = "latest.json"
_RUN_TAG = re.compile(r"^(\d{8}T\d{2}Z)$")

Kind = Literal["instant", "accum", "max", "static"]


# --------------------------------------------------------------------------- variables


@dataclass(frozen=True)
class PeVariable:
    """A canonical variable and the WCS indicators that may provide it."""

    name: str
    candidates: tuple[str, ...]
    units: str
    kind: Kind = "instant"
    height: int | None = None
    # Accepted coverage-id intervals, in order of preference. ``None`` means no
    # suffix: for an accumulation, cumulated since the run start.
    intervals: tuple[str | None, ...] = (None,)
    required: bool = False
    description: str = ""
    # False when the field is not produced at lead time 0 (seen for lcc: 404 at +0 h).
    lead0: bool = True


# Names checked against GetCapabilities on 2026-11-27 (run 09z). Canonical names follow
# the AROME 0.01° chain (t2m, r2, u10, v10, gust_u10, gust_v10, lcc, mslp).
AROME_PE_VARIABLES: tuple[PeVariable, ...] = (
    PeVariable(
        "t2m",
        ("TEMPERATURE__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND",),
        "degC",
        height=2,
        required=True,
        description="2 m temperature",
    ),
    PeVariable(
        "r2",
        ("RELATIVE_HUMIDITY__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND",),
        "%",
        height=2,
        description="2 m relative humidity",
    ),
    PeVariable(
        "u10",
        ("U_COMPONENT_OF_WIND__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND",),
        "m s-1",
        height=10,
        required=True,
        description="10 m eastward wind",
    ),
    PeVariable(
        "v10",
        ("V_COMPONENT_OF_WIND__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND",),
        "m s-1",
        height=10,
        required=True,
        description="10 m northward wind",
    ),
    PeVariable(
        "gust_u10",
        ("U_COMPONENT_OF_WIND_GUST__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND",),
        "m s-1",
        kind="max",
        height=10,
        intervals=(None, "PT1H"),
        description="10 m eastward gust component, max over the past step",
    ),
    PeVariable(
        "gust_v10",
        ("V_COMPONENT_OF_WIND_GUST__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND",),
        "m s-1",
        kind="max",
        height=10,
        intervals=(None, "PT1H"),
        description="10 m northward gust component, max over the past step",
    ),
    PeVariable(
        "precip",
        ("TOTAL_PRECIPITATION__GROUND_OR_WATER_SURFACE",),
        "mm",
        kind="accum",
        intervals=("PT1H", None),
        required=True,
        description="total precipitation per step",
    ),
    PeVariable(
        "snow",
        ("TOTAL_SNOW_PRECIPITATION__GROUND_OR_WATER_SURFACE",),
        "mm",
        kind="accum",
        intervals=("PT1H", None),
        description="snowfall (water equivalent) per step",
    ),
    PeVariable(
        "lcc",
        ("LOW_CLOUD_COVER__GROUND_OR_WATER_SURFACE",),
        "%",
        required=True,
        description="low cloud cover (the only cloud field of AROME-PE)",
        lead0=False,
    ),
    PeVariable(
        "mslp",
        ("PRESSURE__MEAN_SEA_LEVEL",),
        "hPa",
        description="mean sea level pressure",
    ),
)
# No orography indicator in the AROME-PE WCS (GetCapabilities of 2026-11-27): static
# fields come from the constant-fields file of the 0.025° grid (see CONSTANTS).

VARIABLES_BY_NAME = {v.name: v for v in AROME_PE_VARIABLES}


# --------------------------------------------------------------------------- units


def to_canonical(values: np.ndarray, grib_units: str) -> tuple[np.ndarray, str]:
    """Convert a field from its GRIB ``units`` key to the stored unit.

    An unknown unit raises instead of storing a silently wrong value.
    """
    u = grib_units.strip()
    if u == "K":
        return values - np.float32(273.15), "degC"
    if u == "Pa":
        return values / np.float32(100.0), "hPa"
    if u in ("(0 - 1)", "0-1", "1", "Proportion"):
        return values * np.float32(100.0), "%"
    if u in ("kg m**-2", "kg m-2", "mm"):
        return values, "mm"
    if u in ("m**2 s**-2", "m2 s-2"):
        return values / np.float32(GRAVITY), "m"
    if u in ("%", "m", "hPa", "degC"):
        return values, u
    if u in ("m s**-1", "m s-1"):
        return values, "m s-1"
    raise ValueError(f"no conversion for GRIB units {grib_units!r}")


@dataclass(frozen=True)
class GribInfo:
    units: str
    end_step_hours: int


def grib_info_eccodes(data: bytes) -> GribInfo:
    import eccodes  # local import: heavy native dependency

    gid = eccodes.codes_new_from_message(data)
    try:
        return GribInfo(
            str(eccodes.codes_get(gid, "units")), int(eccodes.codes_get(gid, "endStep"))
        )
    finally:
        eccodes.codes_release(gid)


def decode_canonical_eccodes(data: bytes) -> DecodedField:
    """Decode one GRIB message and convert it to the canonical unit."""
    import eccodes  # local import: heavy native dependency

    gid = eccodes.codes_new_from_message(data)
    try:
        units = str(eccodes.codes_get(gid, "units"))
        f = field_from_handle(gid)
    finally:
        eccodes.codes_release(gid)
    values, _ = to_canonical(f.values, units)
    return DecodedField(values.astype(np.float32), f.lats, f.lons)


def deaccumulate(cumulative: np.ndarray, steps: Sequence[int]) -> np.ndarray:
    """Accumulations since run start -> amount over each step, along axis 1 (time).

    ``cumulative`` is (member, time, ...). At lead time 0 the result is NaN.
    The first listed step after 0 is taken from 0. A missing field (NaN)
    makes its own step and the next one NaN. Small negative values from GRIB
    packing are clipped to 0.
    """
    out = np.empty_like(cumulative)
    prev = np.zeros_like(cumulative[:, 0])
    for i, step in enumerate(steps):
        if step == 0:
            out[:, i] = np.nan
            prev = np.zeros_like(cumulative[:, 0])
            continue
        cur = cumulative[:, i]
        out[:, i] = cur - prev
        prev = cur
    np.maximum(out, 0, out=out, where=~np.isnan(out))
    return out


# --------------------------------------------------------------------------- resolution


@dataclass(frozen=True)
class CoverageKey:
    indicator: str
    run: datetime
    interval: str | None


def parse_coverage_id(cid: str) -> CoverageKey | None:
    """``INDICATOR___2026-11-27T09.00.00Z[_PT1H]`` -> parts, or None if malformed."""
    indicator, sep, rest = cid.partition("___")
    if not sep:
        return None
    m = re.match(r"^(\d{4}-\d{2}-\d{2}T\d{2}\.\d{2}\.\d{2}Z)(?:_(.+))?$", rest)
    if not m:
        return None
    run = datetime.strptime(m.group(1), "%Y-%m-%dT%H.%M.%SZ").replace(tzinfo=UTC)
    return CoverageKey(indicator, run, m.group(2))


def parse_capabilities(xml_text: str) -> list[CoverageKey]:
    """Coverage ids of a WCS 2.0 GetCapabilities document (namespace agnostic)."""
    root = ET.fromstring(xml_text)
    keys = []
    for el in root.iter():
        if el.tag.rsplit("}", 1)[-1] == "CoverageId" and el.text:
            key = parse_coverage_id(el.text.strip())
            if key is not None:
                keys.append(key)
    return keys


def parse_time_axis(xml_text: str) -> list[int]:
    """Lead times in seconds from a DescribeCoverage document (gmlrgrid time axis)."""
    root = ET.fromstring(xml_text)
    for axis in root.iter():
        if axis.tag.rsplit("}", 1)[-1] != "GeneralGridAxis":
            continue
        spanned = coeffs = None
        for child in axis:
            tag = child.tag.rsplit("}", 1)[-1]
            if tag == "gridAxesSpanned":
                spanned = (child.text or "").strip()
            elif tag == "coefficients":
                coeffs = (child.text or "").split()
        if spanned == "time" and coeffs:
            return sorted(int(float(c)) for c in coeffs)
    raise ValueError("no time axis in DescribeCoverage response")


@dataclass
class Resolution:
    """Which indicator and interval provide each variable for one run, and the lead times."""

    run: datetime
    indicators: dict[str, str]
    intervals: dict[str, str | None]
    steps_seconds: list[int]
    unresolved: list[str] = field(default_factory=list)
    steps_source: str = "default"

    def variables(self) -> list[tuple[PeVariable, Variable]]:
        out = []
        for spec in AROME_PE_VARIABLES:
            if spec.name in self.indicators:
                out.append(
                    (
                        spec,
                        Variable(
                            spec.name,
                            self.indicators[spec.name],
                            height=spec.height,
                            interval=self.intervals.get(spec.name),
                        ),
                    )
                )
        return out

    def is_current(self) -> bool:
        """False for a resolution saved by an older version of the variable table."""
        names = set(self.indicators) | set(self.unresolved)
        return names == set(VARIABLES_BY_NAME)

    def missing_required(self) -> list[str]:
        return [
            n for n in self.unresolved if n in VARIABLES_BY_NAME and VARIABLES_BY_NAME[n].required
        ]

    def to_json(self) -> str:
        d = asdict(self)
        d["run"] = self.run.astimezone(UTC).isoformat()
        return json.dumps(d, indent=2, sort_keys=True)

    @classmethod
    def from_json(cls, text: str) -> Resolution:
        d = json.loads(text)
        d["run"] = datetime.fromisoformat(d["run"])
        known = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in d.items() if k in known})


def default_steps(run: datetime, max_lead_hours: int | None = None) -> list[int]:
    lead = max_lead_hours if max_lead_hours is not None else DEFAULT_MAX_LEAD_HOURS[run.hour]
    return [h * STEP_SECONDS for h in range(lead + 1)]


def resolve(
    keys: Iterable[CoverageKey],
    run: datetime,
    variables: Iterable[PeVariable] = AROME_PE_VARIABLES,
    steps_seconds: list[int] | None = None,
) -> Resolution:
    available = {(k.indicator, k.interval) for k in keys if k.run == run}
    indicators: dict[str, str] = {}
    intervals: dict[str, str | None] = {}
    unresolved: list[str] = []
    for spec in variables:
        found = next(
            (
                (ind, iv)
                for ind in spec.candidates
                for iv in spec.intervals
                if (ind, iv) in available
            ),
            None,
        )
        if found is None:
            unresolved.append(spec.name)
            continue
        indicators[spec.name], intervals[spec.name] = found
    return Resolution(
        run=run,
        indicators=indicators,
        intervals=intervals,
        steps_seconds=steps_seconds or default_steps(run),
        unresolved=unresolved,
        steps_source="describe" if steps_seconds else "default",
    )


def offline_resolution(run: datetime, max_lead_hours: int | None = None) -> Resolution:
    """First candidate and first interval of every variable: for planning without any call."""
    return Resolution(
        run=run,
        indicators={v.name: v.candidates[0] for v in AROME_PE_VARIABLES},
        intervals={v.name: v.intervals[0] for v in AROME_PE_VARIABLES},
        steps_seconds=default_steps(run, max_lead_hours),
        steps_source="offline",
    )


def latest_complete_runs(keys: Iterable[CoverageKey]) -> list[datetime]:
    """Runs (most recent first) where every required variable has a coverage."""
    by_run: dict[datetime, set[str]] = {}
    for k in keys:
        by_run.setdefault(k.run, set()).add(k.indicator)
    out = []
    for run, inds in by_run.items():
        if all(
            any(c in inds for c in spec.candidates) for spec in AROME_PE_VARIABLES if spec.required
        ):
            out.append(run)
    return sorted(out, reverse=True)


# --------------------------------------------------------------------------- planning


@dataclass(frozen=True)
class RunPlan:
    fields: list[CoverageRequest]  # member, time (, level) fields
    static: list[CoverageRequest]  # orography

    @property
    def all(self) -> list[CoverageRequest]:
        return self.fields + self.static

    def __len__(self) -> int:
        return len(self.fields) + len(self.static)


def steps_for(spec: PeVariable, steps: Sequence[int]) -> list[int]:
    """Accumulations, maxima and some diagnostics (lcc) have no field at lead time 0."""
    if spec.kind in ("accum", "max") or not spec.lead0:
        return [s for s in steps if s > 0]
    return list(steps)


def build_plan(
    resolution: Resolution,
    members: Iterable[int] | None = None,
    max_lead_hours: int | None = None,
) -> RunPlan:
    """Unit requests of one run, member-major so a partial run is usable."""
    member_ids = list(range(MODEL.members)) if members is None else sorted(set(members))
    steps = resolution.steps_seconds
    if max_lead_hours is not None:
        steps = [s for s in steps if s <= max_lead_hours * STEP_SECONDS]
    fields: list[CoverageRequest] = []
    static: list[CoverageRequest] = []
    pairs = resolution.variables()
    for member in member_ids:
        for spec, var in pairs:
            if spec.kind == "static":
                continue
            for step in steps_for(spec, steps):
                fields.append(CoverageRequest(MODEL, resolution.run, member, var, step))
    for spec, var in pairs:
        if spec.kind == "static":
            static.append(CoverageRequest(MODEL, resolution.run, 0, var, steps[0] if steps else 0))
    return RunPlan(fields, static)


def estimate(plan: RunPlan, rate_per_second: float, mb_per_field: float = 0.6) -> str:
    n = len(plan)
    minutes = n / rate_per_second / 60
    return f"{n} appels, ~{minutes:.0f} min à {rate_per_second:g} req/s, ~{n * mb_per_field / 1e3:.1f} Go"


# --------------------------------------------------------------------------- metadata calls


# Metadata operations are throttled separately from GetCoverage (2026-11-27: 8
# consecutive 429 on DescribeCoverage over 35 s while GetCoverage calls succeeded).
# Hence a longer, progressive wait, and a disk cache so that they are rarely needed.
METADATA_429_DELAYS = (10.0, 20.0, 40.0, 80.0, 120.0)


def get_xml(
    client: WcsClient,
    operation: str,
    params: list[tuple[str, str]],
    member: int = 0,
    delays_429: Sequence[float] = METADATA_429_DELAYS,
) -> str:
    """GetCapabilities / DescribeCoverage through the client's auth and rate limiter."""
    url = client.model.service_url(member, operation)
    refreshed = False
    throttled = 0
    for attempt in range(client.retry.max_attempts + len(delays_429)):
        client.limiter.acquire()
        headers = client.auth.headers()
        try:
            resp = client.http.get(url, params=params, headers=headers, timeout=120.0)
        except httpx.TransportError as exc:
            client._count()
            logger.warning("%s: %s", operation, exc)
            client._sleep(client.retry.backoff(attempt, client._rng))
            continue
        client._count(throttled=resp.status_code == 429)
        if resp.status_code == 200:
            return resp.text
        if resp.status_code == 401 and _is_expired_token(resp) and not refreshed:
            client.auth.refresh(headers)
            refreshed = True
            continue
        if resp.status_code == 429:
            if throttled >= len(delays_429):
                raise WcsError(f"{operation}: still throttled after {throttled + 1} attempts")
            hint = parse_retry_after(resp)
            wait = max(delays_429[throttled], hint or 0.0)
            throttled += 1
            logger.warning("429 on %s, waiting %.0fs", operation, wait)
            client.limiter.pause(wait)
            continue
        if resp.status_code in (500, 502, 503, 504):
            client._sleep(client.retry.backoff(attempt, client._rng))
            continue
        if resp.status_code in (401, 403):
            raise WcsAuthError(f"{operation}: HTTP {resp.status_code} {resp.text[:200]!r}")
        if resp.status_code == 404:
            raise CoverageNotFound(f"{operation}: 404 {resp.text[:200]!r}")
        raise WcsError(f"{operation}: HTTP {resp.status_code} {resp.text[:200]!r}")
    raise WcsError(f"{operation}: retries exhausted")


def fetch_capabilities(client: WcsClient) -> list[CoverageKey]:
    text = get_xml(
        client,
        "GetCapabilities",
        [("service", "WCS"), ("version", "2.0.1"), ("language", "eng")],
    )
    return parse_capabilities(text)


def fetch_time_axis(client: WcsClient, coverage: str) -> list[int]:
    text = get_xml(
        client,
        "DescribeCoverage",
        [("service", "WCS"), ("version", "2.0.1"), ("coverageid", coverage)],
    )
    return parse_time_axis(text)


class MetadataCache:
    """Disk cache of GetCapabilities and of the time axis per run hour.

    ``capabilities.xml`` is reused while younger than ``max_age`` seconds, and as a
    fallback (up to ``fallback_age``) when the live call fails. The time axis of a
    run hour (03, 09, 15, 21 UTC) does not change from day to day, so it is asked
    once and then read from ``time_axes.json``.
    """

    def __init__(
        self,
        root: Path,
        max_age: float = 300.0,
        fallback_age: float = 6 * 3600.0,
        clock=None,
    ) -> None:
        import time

        self.root = Path(root)
        self.max_age = max_age
        self.fallback_age = fallback_age
        self._clock = clock or time.time

    @property
    def _caps(self) -> Path:
        return self.root / "capabilities.xml"

    @property
    def _axes(self) -> Path:
        return self.root / "time_axes.json"

    def capabilities(self, max_age: float) -> str | None:
        p = self._caps
        if p.exists() and self._clock() - p.stat().st_mtime <= max_age:
            return p.read_text()
        return None

    def save_capabilities(self, text: str) -> None:
        _atomic_write(self._caps, text.encode())

    def time_axis(self, run: datetime) -> list[int] | None:
        if not self._axes.exists():
            return None
        return json.loads(self._axes.read_text()).get(f"{run.hour:02d}")

    def save_time_axis(self, run: datetime, steps: list[int]) -> None:
        axes = json.loads(self._axes.read_text()) if self._axes.exists() else {}
        axes[f"{run.hour:02d}"] = steps
        _atomic_write(self._axes, json.dumps(axes, sort_keys=True).encode())


def _capabilities_text(client: WcsClient, cache: MetadataCache | None) -> str:
    if cache is not None:
        fresh = cache.capabilities(cache.max_age)
        if fresh is not None:
            logger.info("GetCapabilities from cache")
            return fresh
    try:
        text = get_xml(
            client,
            "GetCapabilities",
            [("service", "WCS"), ("version", "2.0.1"), ("language", "eng")],
        )
    except WcsAuthError:
        raise
    except WcsError as exc:
        stale = cache.capabilities(cache.fallback_age) if cache is not None else None
        if stale is None:
            raise
        logger.warning("GetCapabilities failed (%s), using the cached copy", exc)
        return stale
    if cache is not None:
        cache.save_capabilities(text)
    return text


def discover(
    client: WcsClient,
    run: datetime | None = None,
    cache: MetadataCache | None = None,
    describe: bool = True,
) -> Resolution:
    """Resolve indicators and lead times for ``run`` (or the latest complete run).

    At most 2 calls, usually 1 or 0 with a cache. A failed DescribeCoverage falls
    back to the default lead times instead of failing.
    """
    keys = parse_capabilities(_capabilities_text(client, cache))
    if run is None:
        runs = latest_complete_runs(keys)
        if not runs:
            raise WcsError("no run with every required variable in GetCapabilities")
        run = runs[0]
    res = resolve(keys, run)
    cached = cache.time_axis(run) if cache is not None else None
    if cached:
        res.steps_seconds, res.steps_source = list(cached), "cache"
        return res
    if not describe:
        return res
    t2m = next(v for s, v in res.variables() if s.name == "t2m")
    try:
        steps = fetch_time_axis(client, coverage_id(t2m, run))
    except WcsAuthError:
        raise
    except (WcsError, ValueError) as exc:
        logger.warning("time axis unavailable (%s), using the default lead time", exc)
        return res
    res.steps_seconds, res.steps_source = steps, "describe"
    if cache is not None:
        cache.save_time_axis(run, steps)
    return res


# --------------------------------------------------------------------------- download


def run_tag(run: datetime) -> str:
    return run.astimezone(UTC).strftime("%Y%m%dT%HZ")


@dataclass
class FetchReport:
    run: datetime
    planned: int
    reports: list[IngestReport] = field(default_factory=list)
    aborted: str | None = None

    @property
    def http_calls(self) -> int:
        return sum(r.http_calls for r in self.reports)

    @property
    def fetched(self) -> int:
        return sum(r.fetched for r in self.reports)

    @property
    def missing(self) -> int:
        return sum(len(r.missing) for r in self.reports)

    @property
    def failed(self) -> int:
        return sum(len(r.failed) for r in self.reports)

    @property
    def throttled(self) -> int:
        return sum(r.throttled for r in self.reports)

    @property
    def ok(self) -> bool:
        return self.aborted is None and self.failed == 0


def canary(client: WcsClient, resolution: Resolution, store: RawStore) -> bool:
    """t2m at the last lead time for the first and last member: is the run complete?

    The fields are staged, so these two calls are not wasted.
    """
    t2m = next(v for s, v in resolution.variables() if s.name == "t2m")
    last = resolution.steps_seconds[-1]
    for member in (0, MODEL.members - 1):
        req = CoverageRequest(MODEL, resolution.run, member, t2m, last)
        if store.path(req).exists():
            continue
        try:
            store.write(req, client.fetch(req))
        except CoverageNotFound:
            logger.info("canary %s not published yet", req.key)
            return False
    return True


def download_run(
    client: WcsClient,
    plan: RunPlan,
    store: RawStore,
    *,
    workers: int = 2,
    budget: int = 14_000,
    max_missing_ratio: float = 0.2,
) -> FetchReport:
    """Stage every field of ``plan``, member by member, with budget and circuit breaker."""
    run = plan.fields[0].run if plan.fields else plan.static[0].run
    report = FetchReport(run=run, planned=len(plan))
    todo = [r for r in plan.all if not store.path(r).exists()]
    if len(todo) > budget:
        report.aborted = f"{len(todo)} appels nécessaires, budget {budget}"
        return report

    batches: dict[int, list[CoverageRequest]] = {}
    for r in plan.static:
        batches.setdefault(-1, []).append(r)
    for r in plan.fields:
        batches.setdefault(r.member, []).append(r)
    for member, reqs in batches.items():
        rep = client.download(reqs, store, workers=workers)
        report.reports.append(rep)
        logger.info(
            "member %s: %d fetched, %d skipped, %d missing, %d failed, %d calls, %d throttled",
            "static" if member < 0 else member,
            rep.fetched,
            rep.skipped,
            len(rep.missing),
            len(rep.failed),
            rep.http_calls,
            rep.throttled,
        )
        if any(_is_auth_failure(msg) for _, msg in rep.failed):
            report.aborted = "erreur d'authentification"
            break
        asked = len(reqs) - rep.skipped
        if member >= 0 and asked and len(rep.missing) / asked > max_missing_ratio:
            report.aborted = (
                f"membre {member} : {len(rep.missing)}/{asked} champs absents, arrêt pour "
                "préserver le quota"
            )
            break
    return report


def _is_auth_failure(message: str) -> bool:
    return "HTTP 401" in message or "HTTP 403" in message or "API key rejected" in message


# --------------------------------------------------------------------------- conversion


def make_transform(resolution: Resolution):
    def transform(var: Variable, data: np.ndarray, steps: list[int]) -> np.ndarray:
        spec = VARIABLES_BY_NAME.get(var.name)
        if spec is None:  # constant fields (orography, land-sea mask)
            return data
        if spec.kind == "accum" and var.interval is None:
            return deaccumulate(data, steps)
        if spec.kind == "accum" and var.interval not in ("PT1H",):
            raise ValueError(f"{var.name}: unsupported accumulation interval {var.interval}")
        if spec.kind == "accum":
            np.maximum(data, 0, out=data, where=~np.isnan(data))
        return data

    return transform


@dataclass(frozen=True)
class ConstantSpec:
    """A static field taken from the grid's constant-fields GRIB file."""

    name: str
    short_names: tuple[str, ...]
    units: str
    description: str


# File ``constant-eurw1s40.grib2`` (grid EURW1S40, 0.025°, 1121 x 717 points,
# 37.5..55.4 N, 12 W..16 E), checked on 2026-11-27: messages ``h`` (m) and ``lsm``.
CONSTANTS_FILE = "constant-eurw1s40.grib2"
CONSTANTS: tuple[ConstantSpec, ...] = (
    ConstantSpec("orog", ("h", "orog"), "m", "model orography (geometrical height)"),
    ConstantSpec("lsm", ("lsm",), "%", "land-sea mask"),
)
CONSTANTS_SOURCE = (
    "Météo-France, champs constants de la grille EURW1S40 (0,025°), documentation de "
    "l'API (Confluence Météo-France), Licence Ouverte Etalab 2.0"
)


def iter_grib_messages(path: Path):
    """Yield (shortName, raw message bytes) for every message of a GRIB file."""
    import eccodes  # local import: heavy native dependency

    with open(path, "rb") as fh:
        while True:
            gid = eccodes.codes_grib_new_from_file(fh)
            if gid is None:
                return
            try:
                yield str(eccodes.codes_get(gid, "shortName")), eccodes.codes_get_message(gid)
            finally:
                eccodes.codes_release(gid)


def stage_constants(
    constants_file: Path, run: datetime, store: RawStore, reader=iter_grib_messages
) -> list[CoverageRequest]:
    """Copy the wanted constant fields into the run's staging area as static requests.

    The grid is checked later, by ``write_run``, against the run's fields.
    """
    messages = dict(reader(constants_file))
    requests = []
    for spec in CONSTANTS:
        short = next((s for s in spec.short_names if s in messages), None)
        if short is None:
            logger.warning("%s: no %s in %s", spec.name, spec.short_names, constants_file)
            continue
        var = Variable(spec.name, f"{constants_file.name}:{short}")
        req = CoverageRequest(MODEL, run, 0, var, 0)
        store.write(req, messages[short])
        requests.append(req)
    return requests


def default_constants_file() -> Path:
    env = os.environ.get("AROME_PE_CONSTANTS")
    if env:
        return Path(env)
    return Path(os.environ.get("DATA_DIR", "data/zarr")) / "static" / MODEL.name / CONSTANTS_FILE


def convert_run(
    raw_run_dir: Path,
    out_dir: Path,
    *,
    decoder=decode_canonical_eccodes,
    bbox: BBox | None = FRANCE_BBOX,
    keep: int | None = None,
    constants_file: Path | None = None,
    constants_reader=iter_grib_messages,
) -> Path:
    text = (raw_run_dir / RESOLUTION_FILE).read_text()
    resolution = Resolution.from_json(text)
    meta = json.loads(text)
    plan = build_plan(resolution, meta.get("members"), meta.get("max_lead_hours"))
    store = RawStore(raw_run_dir)
    static = list(plan.static)
    if constants_file is not None and constants_file.exists():
        static += stage_constants(constants_file, resolution.run, store, constants_reader)
    elif constants_file is not None:
        logger.warning("constant fields file %s not found: no orography", constants_file)
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{run_tag(resolution.run)}.zarr"
    var_attrs = {
        s.name: {
            "units": s.units,
            "description": s.description,
            "interval": v.interval or "",
            "kind": s.kind,
        }
        for s, v in resolution.variables()
    }
    for c in CONSTANTS:
        var_attrs[c.name] = {
            "units": c.units,
            "description": c.description,
            "kind": "static",
            "source": CONSTANTS_SOURCE,
        }
    report = write_run(
        out,
        plan.fields,
        store,
        decoder=decoder,
        bbox=bbox,
        attrs={
            "source": "Météo-France, PE-AROME (WCS)",
            "licence": "Licence Ouverte Etalab 2.0",
            "unresolved": resolution.unresolved,
        },
        transform=make_transform(resolution),
        static=static,
        var_attrs=var_attrs,
    )
    logger.info("%s: %d fields, %d missing", out, report.fields, report.missing)
    _atomic_write(
        out_dir / LATEST,
        json.dumps({"run": resolution.run.isoformat(), "path": out.name}).encode(),
    )
    if keep is not None:
        prune(out_dir, keep, suffix=".zarr")
    return out


def prune(directory: Path, keep: int, suffix: str = "") -> list[Path]:
    """Keep the ``keep`` most recent run directories (by tag) in ``directory``."""
    runs = sorted(
        p
        for p in directory.iterdir()
        if p.is_dir() and p.name.endswith(suffix) and _RUN_TAG.match(p.name.removesuffix(suffix))
    )
    removed = runs[:-keep] if keep > 0 else runs
    for p in removed:
        shutil.rmtree(p)
    return removed


# --------------------------------------------------------------------------- CLI


def make_auth(http: httpx.Client, env: dict[str, str]) -> Auth:
    if env.get("METEOFRANCE_APPLICATION_ID"):
        return ApplicationTokenAuth(env["METEOFRANCE_APPLICATION_ID"], http)
    if env.get("METEOFRANCE_API_KEY"):
        return ApiKeyAuth(env["METEOFRANCE_API_KEY"])
    raise SystemExit("set METEOFRANCE_APPLICATION_ID or METEOFRANCE_API_KEY")


def parse_run(text: str) -> datetime:
    run = datetime.strptime(text, "%Y%m%dT%HZ").replace(tzinfo=UTC)
    if run.hour not in RUN_HOURS:
        raise argparse.ArgumentTypeError(f"AROME-PE runs are at {RUN_HOURS} UTC")
    return run


def parse_members(text: str) -> list[int]:
    out: set[int] = set()
    for part in text.split(","):
        if "-" in part:
            a, b = part.split("-")
            out.update(range(int(a), int(b) + 1))
        elif part:
            out.add(int(part))
    return sorted(out)


def latest_run_guess(now: datetime | None = None) -> datetime:
    """Most recent nominal run time, for offline planning only."""
    now = (now or datetime.now(UTC)).replace(minute=0, second=0, microsecond=0)
    while now.hour not in RUN_HOURS:
        now -= timedelta(hours=1)
    return now


def _client(args) -> WcsClient:
    http = httpx.Client(follow_redirects=True)
    auth = make_auth(http, dict(os.environ))
    return WcsClient(MODEL, auth, http, limiter=RateLimiter(args.rate))


def _print_resolution(res: Resolution) -> None:
    print(
        f"run {format_run(res.run)}  échéances : {len(res.steps_seconds)} ({res.steps_source}), "
        f"dernière {res.steps_seconds[-1] // 3600} h"
    )
    for spec in AROME_PE_VARIABLES:
        if spec.name in res.indicators:
            iv = res.intervals.get(spec.name) or "-"
            print(f"  {spec.name:7s} {res.indicators[spec.name]}  [{iv}]")
        else:
            flag = "OBLIGATOIRE" if spec.required else "ignorée"
            print(
                f"  {spec.name:7s} introuvable ({flag}) ; candidats : {', '.join(spec.candidates)}"
            )


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("command", choices=["check", "plan", "fetch", "convert"])
    ap.add_argument("--run", type=parse_run, help="run as YYYYMMDDTHHZ (default: latest)")
    ap.add_argument("--members", type=parse_members, help="e.g. 0,1 or 0-24 (default: all)")
    ap.add_argument("--max-lead", type=int, help="last lead time in hours (tests)")
    ap.add_argument("--rate", type=float, default=5.0, help="requests per second (default 5)")
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--budget", type=int, default=14_000, help="max calls for one fetch")
    ap.add_argument("--offline", action="store_true", help="plan without any call")
    ap.add_argument("--keep", type=int, help="runs to keep after convert")
    ap.add_argument(
        "--constants",
        type=Path,
        default=default_constants_file(),
        help="constant-fields GRIB of the 0.025° grid (orography, land-sea mask); "
        "default $AROME_PE_CONSTANTS or $DATA_DIR/static/arome-pe/" + CONSTANTS_FILE,
    )
    ap.add_argument(
        "--raw", type=Path, default=Path(os.environ.get("RAW_DIR", "data/raw")) / MODEL.name
    )
    ap.add_argument(
        "--out", type=Path, default=Path(os.environ.get("DATA_DIR", "data/zarr")) / MODEL.name
    )
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.command == "plan" and args.offline:
        res = offline_resolution(args.run or latest_run_guess(), args.max_lead)
        plan = build_plan(res, args.members, args.max_lead)
        _print_resolution(res)
        print(estimate(plan, args.rate))
        return 0

    if args.command == "convert":
        runs = sorted(p for p in args.raw.iterdir() if (p / RESOLUTION_FILE).exists())
        if args.run:
            runs = [p for p in runs if p.name == run_tag(args.run)]
        if not runs:
            print("aucun run en attente", file=sys.stderr)
            return 1
        print(convert_run(runs[-1], args.out, keep=args.keep, constants_file=args.constants))
        return 0

    try:
        client = _client(args)
        cache = MetadataCache(args.raw)
        if args.command == "check":
            res = discover(client, args.run, cache)
            _print_resolution(res)
            t2m = next(v for s, v in res.variables() if s.name == "t2m")
            req = CoverageRequest(MODEL, res.run, 0, t2m, STEP_SECONDS)
            data = client.fetch(req)
            info = grib_info_eccodes(data)
            f = decode_canonical_eccodes(data)
            print(
                f"t2m m000 +1 h : {len(data) / 1e3:.0f} Ko, grille {f.values.shape}, "
                f"lat {f.lats[0]:.3f}..{f.lats[-1]:.3f}, lon {f.lons[0]:.3f}..{f.lons[-1]:.3f}, "
                f"unités GRIB {info.units!r}, endStep {info.end_step_hours} h, "
                f"moyenne {np.nanmean(f.values):.1f} °C"
            )
            print(f"appels HTTP : {client.http_calls}, 429 : {client.throttled}")
            return 0 if not res.missing_required() else 1

        run_dir = args.raw / run_tag(args.run) if args.run else None
        res = None
        if run_dir is not None and (run_dir / RESOLUTION_FILE).exists():
            res = Resolution.from_json((run_dir / RESOLUTION_FILE).read_text())
            if not res.is_current():
                # Saved by an older variable table (e.g. gust10 before gust_u10/gust_v10).
                logger.info("saved resolution is outdated, resolving the run again")
                res = None
        if res is None:
            res = discover(client, args.run, cache)
            run_dir = args.raw / run_tag(res.run)
        missing_required = res.missing_required()
        _print_resolution(res)
        if missing_required:
            print(f"variables obligatoires introuvables : {missing_required}", file=sys.stderr)
            return 2
        plan = build_plan(res, args.members, args.max_lead)
        print(estimate(plan, args.rate))
        if args.command == "plan":
            print(f"appels HTTP : {client.http_calls}")
            return 0

        store = RawStore(run_dir)
        meta = json.loads(res.to_json())
        # What this fetch covers, so that convert builds the same plan.
        meta["members"] = sorted({r.member for r in plan.fields})
        meta["max_lead_hours"] = args.max_lead
        _atomic_write(run_dir / RESOLUTION_FILE, json.dumps(meta, indent=2).encode())
        if args.members is None and args.max_lead is None and not canary(client, res, store):
            print("run pas encore complet, rien téléchargé au-delà des 2 sondes", file=sys.stderr)
            return 1
        rep = download_run(client, plan, store, workers=args.workers, budget=args.budget)
        print(
            f"{rep.fetched} téléchargés, {rep.missing} absents, {rep.failed} en échec, "
            f"{rep.http_calls} appels, {rep.throttled} réponses 429"
            + (f" ; arrêt : {rep.aborted}" if rep.aborted else "")
        )
        return 0 if rep.ok else 1
    except WcsAuthError as exc:
        print(f"authentification : {exc}", file=sys.stderr)
        return 2
    except WcsError as exc:
        print(f"Météo-France indisponible ou quota atteint : {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
