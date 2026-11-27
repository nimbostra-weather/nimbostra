"""Download client for ECMWF open data (IFS), staged like an API Paquets run.

Facts this module relies on (ECMWF Confluence "ECMWF open data: real-time
forecasts from IFS and AIFS" and "what's new at 0.1°", the ECMWF open data
page, and listings of the AWS mirror, checked 2026-11-27):
- the same files are served by ``https://data.ecmwf.int/forecasts`` (kept 2 to
  4 days, 500 to 2000 simultaneous connections for everyone) and by cloud
  mirrors, among them AWS ``https://ecmwf-forecasts.s3.eu-central-1.amazonaws.com``
  (kept much longer); no key, no quota, but S3 answers ``503 SlowDown`` when
  requests come too fast;
- path ``{root}/{YYYYMMDD}/{HH}z/ifs/{0p25|0p1}/{stream}/{YYYYMMDDHH}0000-{step}h-{stream}-{type}[-sfc|-pl].grib2``;
  at 0.1° (since 2026-10-20, published ~2 h after 0.25°) surface and pressure
  fields are in separate ``-sfc`` and ``-pl`` files;
- each GRIB file has a ``.index`` next to it: one JSON record per message with
  ``_offset`` and ``_length``; messages are global fields in no useful order,
  so each wanted message costs one HTTP ``Range`` request;
- ``stream=oper, type=fc`` is the IFS control forecast (formerly HRES), also the
  control member of the ensemble; runs 00/12z go to +144 h by 3 h then to
  +360 h by 6 h, runs 06/18z stop at +144 h;
- licence CC BY 4.0 with the ECMWF terms of use: attribution required.

A run is staged under ``<root>/<run tag>/sfc/<step>H.grib2``, one file per lead
time holding only the wanted messages, plus an atomically written
``manifest.json`` in the API Paquets format, so ``ingest.mf_paquets_zarr``
converts it unchanged (profile ``ifs``).
"""

from __future__ import annotations

import json
import logging
import os
import random
import threading
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx

from ingest.meteofrance_paquets import (
    GRIB_HEAD,
    GRIB_TAIL,
    MANIFEST,
    RunStage,
    format_run,
)
from ingest.meteofrance_wcs import RateLimiter, RetryPolicy, _atomic_write

logger = logging.getLogger(__name__)

SOURCES = {
    "aws": "https://ecmwf-forecasts.s3.eu-central-1.amazonaws.com",
    "ecmwf": "https://data.ecmwf.int/forecasts",
    "google": "https://storage.googleapis.com/ecmwf-open-data",
}
DEFAULT_SOURCES = ("aws", "ecmwf")
DEFAULT_TIMEOUT = httpx.Timeout(30.0, read=120.0)
PACKAGE = "sfc"


@dataclass(frozen=True)
class OpenDataModel:
    """One ECMWF open data product: which file family, which messages, which lead times."""

    name: str
    grid: str
    resol: str  # path segment: 0p1 or 0p25
    stream: str
    type: str
    params: tuple[str, ...]
    # Read at lead time 0 only (orography).
    static_params: tuple[str, ...] = ()
    # Accumulations and maxima: may be absent at lead time 0.
    optional_at_zero: tuple[str, ...] = ()
    run_hours: tuple[int, ...] = (0, 6, 12, 18)
    max_lead_hours: int = 168
    # Lead time from which the step becomes 6-hourly, and last lead time of 06/18z runs.
    three_hourly_until: int = 144
    short_run_max_lead: int = 144
    rate_per_second: float = 10.0
    workers: int = 4

    def steps(self, run: datetime) -> list[int]:
        """Lead times (hours) of ``run`` up to ``max_lead_hours``."""
        last = self.max_lead_hours
        if run.hour in (6, 18):
            last = min(last, self.short_run_max_lead)
        steps = list(range(0, min(last, self.three_hourly_until) + 1, 3))
        steps += list(range(self.three_hourly_until + 6, last + 1, 6))
        return steps

    def file_url(self, base: str, run: datetime, step: int) -> str:
        suffix = "-sfc" if self.resol == "0p1" else ""
        return (
            f"{base}/{run:%Y%m%d}/{run:%H}z/ifs/{self.resol}/{self.stream}/"
            f"{run:%Y%m%d%H}0000-{step}h-{self.stream}-{self.type}{suffix}.grib2"
        )

    def wanted(self, step: int) -> frozenset[str]:
        names = set(self.params)
        if step == 0:
            names |= set(self.static_params)
        return frozenset(names)


# Surface fields of the CLAUDE.md list; humidity comes from the dewpoint
# (no 2 m relative humidity in the open data). ``10fg`` is the gust over the
# last hour at +24 h (stepRange 23-24) and over 6 h at +150 h.
IFS_PARAMS = ("2t", "2d", "10u", "10v", "10fg", "msl", "sp", "tp", "sf", "tcc", "lcc")
IFS_01 = OpenDataModel(
    name="ifs",
    grid="0.1",
    resol="0p1",
    stream="oper",
    type="fc",
    params=IFS_PARAMS,
    static_params=("z",),
    optional_at_zero=("tp", "sf", "10fg"),
)
MODELS = {m.name: m for m in (IFS_01,)}


# --------------------------------------------------------------------------- errors


class OpenDataError(RuntimeError):
    pass


class NotPublished(OpenDataError):
    pass


class RetriesExhausted(OpenDataError):
    pass


class BadMessage(OpenDataError):
    """A fetched range is not one whole GRIB message."""


# --------------------------------------------------------------------------- index


@dataclass(frozen=True)
class IndexRecord:
    param: str
    levtype: str
    offset: int
    length: int
    number: int | None = None
    levelist: str | None = None


def parse_index(text: str) -> list[IndexRecord]:
    """Records of a ``.index`` file (one JSON object per line)."""
    out = []
    for n, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        try:
            rec = json.loads(line)
            out.append(
                IndexRecord(
                    param=rec["param"],
                    levtype=rec.get("levtype", ""),
                    offset=int(rec["_offset"]),
                    length=int(rec["_length"]),
                    number=int(rec["number"]) if "number" in rec else None,
                    levelist=rec.get("levelist"),
                )
            )
        except (ValueError, KeyError) as exc:
            raise OpenDataError(f"index line {n}: {exc}") from exc
    return out


def select(records: Sequence[IndexRecord], model: OpenDataModel, step: int) -> list[IndexRecord]:
    """Surface records wanted at ``step``, in file order; error if one is missing."""
    wanted = model.wanted(step)
    chosen = [r for r in records if r.levtype == "sfc" and r.param in wanted]
    found = {r.param for r in chosen}
    optional = set(model.optional_at_zero) if step == 0 else set()
    missing = sorted(wanted - found - optional)
    if missing:
        raise OpenDataError(f"+{step}h: index lacks {missing}")
    dupes = [p for p in found if sum(r.param == p for r in chosen) > 1]
    if dupes:
        raise OpenDataError(f"+{step}h: several messages for {sorted(dupes)}")
    return sorted(chosen, key=lambda r: r.offset)


def step_label(step: int) -> str:
    return f"{step:03d}H"


def check_message(data: bytes, rec: IndexRecord) -> None:
    if len(data) != rec.length:
        raise BadMessage(f"{rec.param}: got {len(data)} bytes, expected {rec.length}")
    if not data.startswith(GRIB_HEAD) or not data.endswith(GRIB_TAIL):
        raise BadMessage(f"{rec.param}: range is not a whole GRIB message")


# --------------------------------------------------------------------------- client


@dataclass
class DownloadReport:
    run: datetime | None = None
    fetched: int = 0  # lead-time files
    skipped: int = 0
    messages: int = 0
    bytes_fetched: int = 0
    http_calls: int = 0
    throttled: int = 0
    failed: list[tuple[int, str]] = field(default_factory=list)
    complete: bool = False

    @property
    def ok(self) -> bool:
        return not self.failed


class OpenDataClient:
    """Range-request client with shared pacing, retries and a fallback source.

    The sources are tried in order for each lead-time file: a file that cannot
    be fetched from the first one (not published there, or still failing after
    the retries) is fetched from the next one.
    """

    def __init__(
        self,
        model: OpenDataModel,
        http: httpx.Client,
        *,
        sources: Sequence[str] = DEFAULT_SOURCES,
        limiter: RateLimiter | None = None,
        retry: RetryPolicy | None = None,
        workers: int | None = None,
        sleep: Callable[[float], None] = time.sleep,
        rng: random.Random | None = None,
        timeout: httpx.Timeout | float = DEFAULT_TIMEOUT,
    ) -> None:
        unknown = [s for s in sources if s not in SOURCES and not s.startswith("https://")]
        if unknown or not sources:
            raise ValueError(f"unknown sources {unknown}; known: {sorted(SOURCES)}")
        self.model = model
        self.http = http
        self.sources = [SOURCES.get(s, s).rstrip("/") for s in sources]
        self.limiter = limiter or RateLimiter(model.rate_per_second)
        self.retry = retry or RetryPolicy(max_attempts=6, base_delay=1.0, max_delay=60.0)
        self.workers = workers or model.workers
        self._sleep = sleep
        self._rng = rng or random.Random()
        self._timeout = timeout
        self._lock = threading.Lock()
        self.http_calls = 0
        self.throttled = 0

    # ----------------------------------------------------------------- low level

    def _get(self, url: str, headers: dict[str, str] | None = None, what: str = "") -> bytes:
        what = what or url
        last = "no attempt"
        for attempt in range(self.retry.max_attempts):
            self.limiter.acquire()
            with self._lock:
                self.http_calls += 1
            try:
                resp = self.http.get(url, headers=headers, timeout=self._timeout)
            except httpx.TransportError as exc:
                last = f"{type(exc).__name__}: {exc}"
                self._sleep(self.retry.backoff(attempt, self._rng))
                continue
            status = resp.status_code
            if status in (200, 206):
                if headers and "Range" in headers and status != 206:
                    raise OpenDataError(f"{what}: Range ignored by the server")
                return resp.content
            if status in (429, 503):
                # S3 "SlowDown" is a 503: slow every worker down, not just this one.
                with self._lock:
                    self.throttled += 1
                rate = self.limiter.slow_down(0.7, min_rate=1.0)
                wait = max(2.0, self.retry.backoff(attempt, self._rng))
                logger.warning("%d on %s, pausing %.1fs, rate now %.1f/s", status, what, wait, rate)
                self.limiter.pause(wait)
                last = f"HTTP {status}"
                continue
            if status in (500, 502, 504):
                last = f"HTTP {status}"
                self._sleep(self.retry.backoff(attempt, self._rng))
                continue
            if status in (403, 404):
                # S3 answers 403 for a missing key when listing is not allowed.
                raise NotPublished(f"{what}: HTTP {status}")
            raise OpenDataError(f"{what}: HTTP {status} {resp.text[:200]!r}")
        raise RetriesExhausted(f"{what}: {self.retry.max_attempts} attempts, last: {last}")

    # ----------------------------------------------------------------- run discovery

    def is_published(self, run: datetime) -> bool:
        """True if the index of the run's last lead time exists on some source."""
        last = self.model.steps(run)[-1]
        for base in self.sources:
            url = self.model.file_url(base, run, last).removesuffix(".grib2") + ".index"
            try:
                self._get(url, what=f"{format_run(run)} +{last}h index")
                return True
            except NotPublished:
                continue
            except OpenDataError as exc:
                logger.warning("%s", exc)
        return False

    def latest_published_run(
        self, now: datetime | None = None, max_candidates: int = 4
    ) -> datetime | None:
        for run in candidate_runs(self.model, now or datetime.now(UTC), max_candidates):
            if self.is_published(run):
                return run
        return None

    # ----------------------------------------------------------------- download

    def fetch_step(self, run: datetime, step: int, dest: Path) -> tuple[int, int]:
        """Stage the wanted messages of one lead time. Returns (messages, bytes)."""
        errors = []
        for base in self.sources:
            try:
                return self._fetch_step_from(base, run, step, dest)
            except OpenDataError as exc:
                errors.append(str(exc))
                logger.warning("%s", exc)
        raise OpenDataError(" ; ".join(errors))

    def _fetch_step_from(self, base: str, run: datetime, step: int, dest: Path) -> tuple[int, int]:
        url = self.model.file_url(base, run, step)
        tag = f"{format_run(run)} +{step}h"
        index = self._get(url.removesuffix(".grib2") + ".index", what=f"{tag} index")
        records = select(parse_index(index.decode()), self.model, step)

        def fetch(rec: IndexRecord) -> bytes:
            rng = f"bytes={rec.offset}-{rec.offset + rec.length - 1}"
            for _ in range(2):
                data = self._get(url, {"Range": rng}, what=f"{tag} {rec.param}")
                try:
                    check_message(data, rec)
                    return data
                except BadMessage as exc:
                    logger.warning("%s %s, refetching", tag, exc)
            raise BadMessage(f"{tag} {rec.param}: bad message twice")

        with ThreadPoolExecutor(self.workers) as pool:
            parts = list(pool.map(fetch, records))
        dest.parent.mkdir(parents=True, exist_ok=True)
        data = b"".join(parts)
        _atomic_write(dest, data)
        return len(parts), len(data)

    def download_run(self, run: datetime, root: Path) -> DownloadReport:
        """Stage every lead time of ``run`` not yet staged; write the manifest when all are.

        Safe to rerun: staged lead times are skipped.
        """
        stage = RunStage(Path(root), run)
        report = DownloadReport(run=run)
        calls0, throttled0 = self.http_calls, self.throttled
        steps = self.model.steps(run)
        try:
            for step in steps:
                dest = stage.path / PACKAGE / f"{step_label(step)}.grib2"
                if dest.exists():
                    report.skipped += 1
                    continue
                try:
                    n, size = self.fetch_step(run, step, dest)
                except OpenDataError as exc:
                    report.failed.append((step, str(exc)))
                    logger.error("%s +%dh: %s", format_run(run), step, exc)
                    continue
                report.fetched += 1
                report.messages += n
                report.bytes_fetched += size
        finally:
            report.http_calls = self.http_calls - calls0
            report.throttled = self.throttled - throttled0
        if not report.failed:
            write_manifest(stage, self.model, steps)
            report.complete = True
        return report

    def fetch_latest(
        self, root: Path, now: datetime | None = None, max_candidates: int = 4
    ) -> DownloadReport:
        """Stage the latest fully published run (no-op if already complete)."""
        run = self.latest_published_run(now, max_candidates)
        if run is None:
            logger.info("%s: no published run among the last %d", self.model.name, max_candidates)
            return DownloadReport()
        stage = RunStage(Path(root), run)
        if stage.is_complete():
            logger.info("%s %s already staged", self.model.name, format_run(run))
            return DownloadReport(run=run, skipped=len(self.model.steps(run)), complete=True)
        return self.download_run(run, root)


def candidate_runs(model: OpenDataModel, now: datetime, count: int) -> list[datetime]:
    """The ``count`` most recent nominal runs at or before ``now``, newest first."""
    t = now.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    out: list[datetime] = []
    while len(out) < count:
        if t.hour in model.run_hours:
            out.append(t)
        t -= timedelta(hours=1)
    return out


def write_manifest(stage: RunStage, model: OpenDataModel, steps: Sequence[int]) -> None:
    """Manifest in the API Paquets format read by ``mf_paquets_zarr.convert_run``."""
    manifest = {
        "model": model.name,
        "grid": model.grid,
        "run": format_run(stage.run),
        "packages": {PACKAGE: [step_label(s) for s in steps]},
        "source": "ECMWF open data",
        "completed_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    _atomic_write(stage.path / MANIFEST, json.dumps(manifest, indent=2).encode())


def estimate(model: OpenDataModel, run: datetime) -> str:
    steps = model.steps(run)
    messages = len(steps) * len(model.params) + len(model.static_params)
    return (
        f"{len(steps)} lead times (+{steps[0]}..+{steps[-1]} h), about {messages} messages, "
        f"{messages + len(steps)} requests with the indexes"
    )


def env_sources(env: dict[str, str] | None = None) -> tuple[str, ...]:
    """``ECMWF_SOURCES`` (comma-separated names or URLs), default aws then ecmwf."""
    env = os.environ if env is None else env
    raw = env.get("ECMWF_SOURCES", "")
    names = tuple(s.strip() for s in raw.split(",") if s.strip())
    return names or DEFAULT_SOURCES
