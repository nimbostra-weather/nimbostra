"""Download client for the Météo-France "API Paquets" (deterministic AROME and ARPEGE).

Facts this module relies on (Météo-France Confluence page "API Paquet Modèles",
data.gouv dataservices "API Paquet AROME" and "API Package ARPEGE", checked
2026-11-27, and the AROME spike):
- base ``https://public-api.meteofrance.fr/previnum/DPPaquet{AROME,ARPEGE}/v1``;
- catalogue ``/models/{model}/grids/{grid}/packages/{package}`` lists runs, and with
  ``?referencetime=`` lists one file per lead time (AROME, ``07H``) or per
  lead-time range (ARPEGE, ``000H012H``);
- download ``.../packages/{package}/product{ARO,ARP}?referencetime=..&time=..&format=grib2``;
  the ARPEGE names (``DPPaquetARPEGE``, ``productARP``, grid ``0.1``) come from a
  request reported working on the Infoclimat forum, not from Météo-France pages;
- catalogue hrefs may omit the ``/v1`` segment (seen in the spike), we add it back;
- quota 100 requests per minute; packages should be downloaded one after another;
- interrupted downloads can be resumed with a ``Range`` header (206 response);
- 404 means "not published (yet)" or a wrong keyword; 400 a malformed parameter;
- data are kept 3 days on the server, without archive.

Each run is staged under ``<root>/<run tag>/<package>/<time>.grib2``. A run is
marked complete by an atomically written ``manifest.json``; the Zarr writer only
reads complete runs.
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
import shutil
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import httpx

from ingest.meteofrance_wcs import (
    Auth,
    RateLimiter,
    RetryPolicy,
    WcsAuthError,
    _atomic_write,
    _is_expired_token,
    parse_retry_after,
)

logger = logging.getLogger(__name__)

API_BASE_URL = "https://public-api.meteofrance.fr/previnum"
MANIFEST = "manifest.json"
GRIB_HEAD = b"GRIB"
GRIB_TAIL = b"7777"
_TIME_LABEL = re.compile(r"^(\d+)H(?:(\d+)H)?$")
_RUN_DIR = re.compile(r"^\d{8}T\d{2}Z$")
# A whole 0.01° file is ~70 MB: generous read timeout between chunks.
DEFAULT_TIMEOUT = httpx.Timeout(30.0, read=180.0)


# --------------------------------------------------------------------------- models


@dataclass(frozen=True)
class PackageSpec:
    """One package of a model grid and how many of its files a run needs."""

    name: str
    # Files required for the run to count as complete. ``None`` means every file
    # listed in the catalogue for that run.
    expected_files: int | None = None
    # Download only the first ``limit`` files (lowest lead times), e.g. SP3 is
    # only needed for the model orography, which does not change with lead time.
    limit: int | None = None

    def __post_init__(self) -> None:
        if self.limit is not None and self.limit < 1:
            raise ValueError(f"{self.name}: limit must be >= 1")
        if (
            self.limit is not None
            and self.expected_files is not None
            and self.expected_files > self.limit
        ):
            raise ValueError(f"{self.name}: expected_files cannot exceed limit")


@dataclass(frozen=True)
class PaquetsModel:
    """Static description of one model grid served by the API Paquets."""

    name: str
    api: str
    model: str
    grid: str
    product: str
    packages: tuple[PackageSpec, ...]
    # Target rate, deliberately below the 100 req/min quota to leave room for retries.
    rate_per_second: float = 1.5
    api_version: str = "v1"

    @property
    def base_url(self) -> str:
        return f"{API_BASE_URL}/{self.api}/{self.api_version}/models/{self.model}/grids/{self.grid}"

    def package_url(self, package: str) -> str:
        return f"{self.base_url}/packages/{package}"

    def product_url(self, package: str) -> str:
        return f"{self.package_url(package)}/{self.product}"

    def package(self, name: str) -> PackageSpec:
        for spec in self.packages:
            if spec.name == name:
                return spec
        raise KeyError(f"{self.name}: unknown package {name!r}")


# 52 hourly files per run in SP1 and SP2 (spike of 2026-11), one SP3 file for ALTITUDE.
AROME_001 = PaquetsModel(
    name="arome",
    api="DPPaquetAROME",
    model="AROME",
    grid="0.01",
    product="productARO",
    packages=(
        PackageSpec("SP1", expected_files=52),
        PackageSpec("SP2", expected_files=52),
        PackageSpec("SP3", expected_files=1, limit=1),
    ),
)
# ARPEGE 0.1° (EURAT01, 20-72 N, 32 W-42 E), runs 0/6/12/18z up to +102 h: nine
# files per package, 000H012H, 013H024H, ... 085H096H, 097H102H (data.gouv
# "Paquets Arpège 0,1°", 2026-11-27). SP1 and SP2 hold the surface fields.
ARPEGE_01 = PaquetsModel(
    name="arpege",
    api="DPPaquetARPEGE",
    model="ARPEGE",
    grid="0.1",
    product="productARP",
    packages=(
        PackageSpec("SP1", expected_files=9),
        PackageSpec("SP2", expected_files=9),
    ),
)
MODELS = {m.name: m for m in (AROME_001, ARPEGE_01)}


# --------------------------------------------------------------------------- run / time helpers


def parse_run(value: str) -> datetime:
    """Parse a catalogue reference time such as ``2026-11-26T12:00:00Z``."""
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)


def format_run(run: datetime) -> str:
    if run.tzinfo is None:
        raise ValueError("run must be timezone-aware")
    return run.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def run_tag(run: datetime) -> str:
    """Directory name of a run, e.g. ``20261126T12Z``."""
    if run.tzinfo is None:
        raise ValueError("run must be timezone-aware")
    return run.astimezone(UTC).strftime("%Y%m%dT%HZ")


def parse_time_label(label: str) -> tuple[int, int]:
    """Lead-time range in hours of a ``time`` value: ``07H`` -> (7, 7), ``00H06H`` -> (0, 6)."""
    m = _TIME_LABEL.match(label)
    if not m:
        raise ValueError(f"unexpected time label {label!r}")
    start = int(m.group(1))
    end = int(m.group(2)) if m.group(2) else start
    if end < start:
        raise ValueError(f"time label {label!r} ends before it starts")
    return start, end


# --------------------------------------------------------------------------- errors


class PaquetsError(RuntimeError):
    """Non-retryable API Paquets failure."""


class PaquetsAuthError(PaquetsError):
    pass


class NotPublished(PaquetsError):
    """404: run or file not (yet) published, or a wrong catalogue keyword."""


class BadRequest(PaquetsError):
    """400: malformed parameter (often the ``time`` format)."""


class RetriesExhausted(PaquetsError):
    pass


class IncompleteFile(PaquetsError):
    """Payload is not a whole GRIB file (bad start or missing end marker)."""


# --------------------------------------------------------------------------- catalogue


@dataclass(frozen=True)
class PackageFile:
    """One downloadable file of a package: one run, one lead time (or lead-time range)."""

    package: str
    run: datetime
    time: str
    url: str

    @property
    def lead_hours(self) -> tuple[int, int]:
        return parse_time_label(self.time)

    @property
    def key(self) -> str:
        """Path relative to the run directory."""
        return f"{self.package}/{self.time}.grib2"


def _with_version(href: str, model: PaquetsModel) -> str:
    """Catalogue hrefs may lack the API version segment."""
    marker = f"/{model.api}/"
    versioned = f"/{model.api}/{model.api_version}/"
    if versioned in href or marker not in href:
        return href
    return href.replace(marker, versioned, 1)


def _query_value(href: str, name: str) -> str | None:
    values = parse_qs(urlsplit(href).query).get(name)
    return values[0] if values else None


def parse_runs(payload: dict) -> list[datetime]:
    """Reference times listed by ``/packages/{package}``, oldest first."""
    runs: set[datetime] = set()
    for link in payload.get("links", []):
        value = _query_value(link.get("href", ""), "referencetime")
        if value:
            runs.add(parse_run(value))
    return sorted(runs)


def parse_files(
    payload: dict, model: PaquetsModel, package: str, run: datetime
) -> list[PackageFile]:
    """Files listed by ``/packages/{package}?referencetime=..``, sorted by lead time."""
    files: dict[str, PackageFile] = {}
    for link in payload.get("links", []):
        href = link.get("href", "")
        if f"/{model.product}" not in href:
            continue
        label = link.get("time") or _query_value(href, "time")
        if not label:
            logger.warning("%s: file link without time: %s", package, href)
            continue
        listed_run = _query_value(href, "referencetime")
        if listed_run and parse_run(listed_run) != run:
            continue
        files[label] = PackageFile(package, run, label, _with_version(href, model))
    return sorted(files.values(), key=lambda f: (f.lead_hours, f.time))


# --------------------------------------------------------------------------- staging


@dataclass(frozen=True)
class RunStage:
    """Staging directory of one run."""

    root: Path
    run: datetime

    @property
    def path(self) -> Path:
        return Path(self.root) / run_tag(self.run)

    def file_path(self, f: PackageFile) -> Path:
        return self.path / f.key

    def part_path(self, f: PackageFile) -> Path:
        p = self.file_path(f)
        return p.with_name(p.name + ".part")

    def has(self, f: PackageFile) -> bool:
        return self.file_path(f).exists()

    @property
    def manifest_path(self) -> Path:
        return self.path / MANIFEST

    def is_complete(self) -> bool:
        return self.manifest_path.exists()

    def write_manifest(self, model: PaquetsModel, files: Iterable[PackageFile]) -> None:
        by_package: dict[str, list[str]] = {}
        for f in files:
            by_package.setdefault(f.package, []).append(f.time)
        manifest = {
            "model": model.name,
            "grid": model.grid,
            "run": format_run(self.run),
            "packages": by_package,
            "completed_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        _atomic_write(self.manifest_path, json.dumps(manifest, indent=2).encode())

    def read_manifest(self) -> dict | None:
        p = self.manifest_path
        return json.loads(p.read_text()) if p.exists() else None


def staged_runs(root: Path, complete_only: bool = True) -> list[datetime]:
    """Runs present under ``root``, oldest first."""
    root = Path(root)
    if not root.is_dir():
        return []
    runs = []
    for p in root.iterdir():
        if not (p.is_dir() and _RUN_DIR.match(p.name)):
            continue
        if complete_only and not (p / MANIFEST).exists():
            continue
        runs.append(datetime.strptime(p.name, "%Y%m%dT%HZ").replace(tzinfo=UTC))
    return sorted(runs)


def prune_runs(root: Path, keep: int) -> list[datetime]:
    """Delete all staged runs except the ``keep`` most recent complete ones.

    Incomplete runs older than the oldest kept one are deleted too; newer
    incomplete runs are kept (a download may be in progress).
    """
    if keep < 1:
        raise ValueError("keep must be >= 1")
    complete = staged_runs(root, complete_only=True)
    kept = set(complete[-keep:])
    oldest_kept = min(kept) if kept else None
    removed = []
    for run in staged_runs(root, complete_only=False):
        if run in kept or (oldest_kept is not None and run > oldest_kept):
            continue
        shutil.rmtree(Path(root) / run_tag(run))
        removed.append(run)
    return removed


# --------------------------------------------------------------------------- client


@dataclass
class DownloadReport:
    run: datetime | None = None
    fetched: int = 0
    skipped: int = 0
    bytes_fetched: int = 0
    resumed: int = 0
    failed: list[tuple[PackageFile, str]] = field(default_factory=list)
    http_calls: int = 0
    throttled: int = 0
    complete: bool = False

    @property
    def ok(self) -> bool:
        return self.complete and not self.failed


class PaquetsClient:
    """Rate-limited, retrying, strictly sequential client for one model grid."""

    def __init__(
        self,
        model: PaquetsModel,
        auth: Auth,
        http: httpx.Client,
        *,
        limiter: RateLimiter | None = None,
        retry: RetryPolicy | None = None,
        sleep: Callable[[float], None] = time.sleep,
        rng: random.Random | None = None,
        timeout: httpx.Timeout | float = DEFAULT_TIMEOUT,
        chunk_size: int = 1 << 20,
    ) -> None:
        self.model = model
        self.auth = auth
        self.http = http
        self.limiter = limiter or RateLimiter(model.rate_per_second)
        self.retry = retry or RetryPolicy()
        self._sleep = sleep
        self._rng = rng or random.Random()
        self._timeout = timeout
        self._chunk_size = chunk_size
        self.http_calls = 0
        self.throttled = 0

    # ----------------------------------------------------------------- low level

    def _request(
        self,
        url: str,
        *,
        params: dict[str, str] | None = None,
        accept: str,
        on_response: Callable[[httpx.Response], object],
        extra_headers: Callable[[], dict[str, str]] | None = None,
        on_range_error: Callable[[], None] | None = None,
        what: str,
    ) -> object:
        """Send one GET with retries; ``on_response`` consumes a 200/206 response."""
        refreshed = False
        last_error = "no attempt"
        for attempt in range(self.retry.max_attempts):
            self.limiter.acquire()
            try:
                headers = {"accept": accept, **self.auth.headers()}
            except WcsAuthError as exc:
                raise PaquetsAuthError(str(exc)) from exc
            if extra_headers:
                headers.update(extra_headers())
            self.http_calls += 1
            try:
                with self.http.stream(
                    "GET", url, params=params, headers=headers, timeout=self._timeout
                ) as resp:
                    status = resp.status_code
                    if status in (200, 206):
                        return on_response(resp)
                    resp.read()
            except (httpx.TransportError, IncompleteFile) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                logger.warning("%s: %s, retrying", what, last_error)
                self._sleep(self.retry.backoff(attempt, self._rng))
                continue

            if status == 401 and _is_expired_token(resp) and not refreshed:
                try:
                    self.auth.refresh(headers)
                except WcsAuthError as exc:
                    raise PaquetsAuthError(str(exc)) from exc
                refreshed = True
                continue
            if status == 429:
                self.throttled += 1
                hint = parse_retry_after(resp)
                wait = max(
                    self.retry.min_429_delay,
                    hint if hint is not None else self.retry.backoff(attempt, self._rng),
                )
                wait = min(wait, self.retry.max_delay)
                logger.warning("429 on %s, pausing %.1fs", what, wait)
                self.limiter.pause(wait)
                last_error = "429 Too Many Requests"
                continue
            if status == 416 and on_range_error is not None:
                # Partial file no longer matches the remote one: restart from zero.
                on_range_error()
                last_error = "HTTP 416"
                continue
            if status in (500, 502, 503, 504):
                last_error = f"HTTP {status}"
                self._sleep(self.retry.backoff(attempt, self._rng))
                continue
            body = resp.text[:200]
            if status == 404:
                raise NotPublished(f"{what}: 404 {body!r}")
            if status == 400:
                raise BadRequest(f"{what}: 400 {body!r}")
            if status in (401, 403):
                raise PaquetsAuthError(f"{what}: HTTP {status} {body!r}")
            raise PaquetsError(f"{what}: HTTP {status} {body!r}")
        raise RetriesExhausted(f"{what}: {self.retry.max_attempts} attempts, last: {last_error}")

    def _get_json(self, url: str, params: dict[str, str] | None, what: str) -> dict:
        def read(resp: httpx.Response) -> dict:
            resp.read()
            try:
                return resp.json()
            except ValueError as exc:
                raise PaquetsError(f"{what}: invalid JSON") from exc

        return self._request(  # type: ignore[return-value]
            url, params=params, accept="application/json", on_response=read, what=what
        )

    # ----------------------------------------------------------------- catalogue

    def list_runs(self, package: str) -> list[datetime]:
        payload = self._get_json(self.model.package_url(package), None, f"runs of {package}")
        return parse_runs(payload)

    def list_files(self, package: str, run: datetime) -> list[PackageFile]:
        payload = self._get_json(
            self.model.package_url(package),
            {"referencetime": format_run(run)},
            f"files of {package} {format_run(run)}",
        )
        return parse_files(payload, self.model, package, run)

    def plan(self, run: datetime) -> list[PackageFile] | None:
        """Files to download for ``run``, or None if a package is not complete yet."""
        plan: list[PackageFile] = []
        for spec in self.model.packages:
            try:
                files = self.list_files(spec.name, run)
            except NotPublished:
                return None
            if spec.expected_files is not None and len(files) < spec.expected_files:
                logger.info(
                    "%s %s: %d/%d files published",
                    spec.name,
                    format_run(run),
                    len(files),
                    spec.expected_files,
                )
                return None
            if not files:
                return None
            plan.extend(files[: spec.limit] if spec.limit else files)
        return plan

    def latest_complete_run(
        self, max_candidates: int = 4
    ) -> tuple[datetime, list[PackageFile]] | None:
        """Most recent run whose packages are all published, with its download plan."""
        first = self.model.packages[0].name
        runs = self.list_runs(first)
        for run in reversed(runs[-max_candidates:]):
            plan = self.plan(run)
            if plan is not None:
                return run, plan
        return None

    # ----------------------------------------------------------------- download

    def download_file(self, f: PackageFile, stage: RunStage) -> tuple[int, bool]:
        """Download one file to the stage, resuming a ``.part`` file. Returns (bytes, resumed)."""
        dest = stage.file_path(f)
        part = stage.part_path(f)
        dest.parent.mkdir(parents=True, exist_ok=True)
        state = {"resumed": False}

        def range_header() -> dict[str, str]:
            size = part.stat().st_size if part.exists() else 0
            return {"Range": f"bytes={size}-"} if size else {}

        def write(resp: httpx.Response) -> int:
            offset = part.stat().st_size if part.exists() else 0
            append = resp.status_code == 206 and offset > 0
            if resp.status_code == 206 and not append:
                raise PaquetsError(f"{f.key}: unexpected 206 without a partial file")
            if append:
                state["resumed"] = True
            with open(part, "ab" if append else "wb") as fh:
                fh.writelines(resp.iter_bytes(self._chunk_size))
                fh.flush()
                os.fsync(fh.fileno())
            _check_grib(part, f)
            os.replace(part, dest)
            return dest.stat().st_size

        size = self._request(
            f.url,
            accept="*/*",
            on_response=write,
            extra_headers=range_header,
            on_range_error=lambda: part.unlink(missing_ok=True),
            what=f"{format_run(f.run)} {f.key}",
        )
        return size, state["resumed"]  # type: ignore[return-value]

    def download_run(self, run: datetime, plan: list[PackageFile], root: Path) -> DownloadReport:
        """Download every file of ``plan`` not yet staged, one at a time.

        Safe to rerun: finished files are skipped and ``.part`` files resumed.
        The manifest is written only when every file is present.
        """
        stage = RunStage(Path(root), run)
        report = DownloadReport(run=run)
        calls_before, throttled_before = self.http_calls, self.throttled
        try:
            for f in plan:
                if stage.has(f):
                    report.skipped += 1
                    continue
                try:
                    size, resumed = self.download_file(f, stage)
                except PaquetsAuthError:
                    raise
                except PaquetsError as exc:
                    report.failed.append((f, str(exc)))
                    logger.error("%s", exc)
                    continue
                report.fetched += 1
                report.bytes_fetched += size
                report.resumed += int(resumed)
        finally:
            report.http_calls = self.http_calls - calls_before
            report.throttled = self.throttled - throttled_before
        if not report.failed and all(stage.has(f) for f in plan):
            stage.write_manifest(self.model, plan)
            report.complete = True
        return report

    def fetch_latest(self, root: Path, max_candidates: int = 4) -> DownloadReport:
        """Find the latest fully published run and stage it (no-op if already complete)."""
        found = self.latest_complete_run(max_candidates)
        if found is None:
            logger.info(
                "%s: no fully published run among the last %d", self.model.name, max_candidates
            )
            return DownloadReport()
        run, plan = found
        stage = RunStage(Path(root), run)
        if stage.is_complete():
            logger.info("%s %s already staged", self.model.name, format_run(run))
            return DownloadReport(run=run, skipped=len(plan), complete=True)
        return self.download_run(run, plan, root)


def _check_grib(path: Path, f: PackageFile) -> None:
    """Reject truncated payloads: a GRIB file starts with ``GRIB`` and ends with ``7777``."""
    size = path.stat().st_size
    with open(path, "rb") as fh:
        head = fh.read(4)
        fh.seek(max(0, size - 4))
        tail = fh.read(4)
    if head != GRIB_HEAD:
        path.unlink()
        raise PaquetsError(f"{f.key}: payload is not GRIB (starts with {head!r})")
    if tail != GRIB_TAIL:
        # Keep the partial file: the next attempt resumes it with a Range header.
        raise IncompleteFile(f"{f.key}: missing GRIB end marker after {size} bytes")
