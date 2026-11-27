"""WCS ingestion client for Météo-France ensemble models (AROME-PE, PEARP).

Facts this module relies on (checked against the meteole 0.2.8 source, MAIF):
- base URL ``https://public-api.meteofrance.fr/public/<api>/1.0/wcs/<service>/GetCoverage``;
- one WCS service per member: ``MF-NWP-HIGHRES-PEARO{NNN}-0025-FRANCE-WCS`` and
  ``MF-NWP-GLOBAL-PEARP{NNN}-01-EUROPE-WCS``;
- coverage id ``<INDICATOR>___<run as YYYY-MM-DDTHH.MM.SSZ>[_<interval>]``;
- ``subset=time(<lead time in seconds>)``, ``height(..)``, ``pressure(..)``,
  ``lat(min,max)``, ``long(min,max)`` and ``format=application/wmo-grib``;
- tokens last one hour, an expired one returns 401 with JSON code ``900901``;
- a 429 body may carry ``nextAccessTime``.

Established by probing (not documented): only single-instant calls work, a time
range returns 404 "Synopsis backend error". So the client issues exactly one
call per member / lead time / variable (/ level).
"""

from __future__ import annotations

import email.utils
import logging
import os
import random
import threading
import time
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, Protocol

import httpx

logger = logging.getLogger(__name__)

API_BASE_URL = "https://public-api.meteofrance.fr/public"
TOKEN_URL = "https://portail-api.meteofrance.fr/token"  # URL, not a secret
GRIB_FORMAT = "application/wmo-grib"
INVALID_JWT_CODE = "900901"


# --------------------------------------------------------------------------- models


@dataclass(frozen=True)
class EnsembleModel:
    """Static description of one Météo-France ensemble exposed through WCS."""

    name: str
    api_name: str
    service_prefix: str
    service_suffix: str
    members: int
    # Target rate, deliberately below the portal quota to leave room for retries.
    rate_per_second: float
    # Whether lat/long subsets are sent to the server. When False, the whole
    # domain is downloaded and cropped client-side (see notes on PEARP).
    server_side_bbox: bool = True
    api_version: str = "1.0"

    def service_url(self, member: int, operation: str = "GetCoverage") -> str:
        if not 0 <= member < self.members:
            raise ValueError(f"{self.name}: member {member} out of range 0..{self.members - 1}")
        service = f"{self.service_prefix}{member:03d}-{self.service_suffix}"
        return f"{API_BASE_URL}/{self.api_name}/{self.api_version}/wcs/{service}/{operation}"


# Portal quotas: AROME-PE 900 req/min, ARPEGE-PE 400 req/min. Targets: 10 and 5 req/s.
AROME_PE = EnsembleModel(
    name="arome-pe",
    api_name="pearome",
    service_prefix="MF-NWP-HIGHRES-PEARO",
    service_suffix="0025-FRANCE-WCS",
    members=25,
    rate_per_second=10.0,
)
# meteole notes that the pearpege API rejected lat/long subsets. Kept True here
# on purpose (server-side cropping is the plan) until a single probe confirms it.
PEARP = EnsembleModel(
    name="pearp",
    api_name="pearpege",
    service_prefix="MF-NWP-GLOBAL-PEARP",
    service_suffix="01-EUROPE-WCS",
    members=35,
    rate_per_second=5.0,
)
MODELS = {m.name: m for m in (AROME_PE, PEARP)}


@dataclass(frozen=True)
class BBox:
    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float

    def __post_init__(self) -> None:
        if self.lat_min >= self.lat_max or self.lon_min >= self.lon_max:
            raise ValueError(f"invalid bbox {self}")


# France + ~100 km.
FRANCE_BBOX = BBox(lat_min=40.5, lat_max=52.0, lon_min=-6.5, lon_max=10.5)


@dataclass(frozen=True)
class Variable:
    """One WCS indicator mapped to an internal variable name."""

    name: str
    indicator: str
    group: Literal["surface", "pressure"] = "surface"
    height: int | None = None
    levels: tuple[int, ...] = ()
    interval: str | None = None

    def __post_init__(self) -> None:
        if self.group == "pressure" and not self.levels:
            raise ValueError(f"{self.name}: pressure variable needs levels")
        if self.group == "surface" and self.levels:
            raise ValueError(f"{self.name}: surface variable cannot have levels")


def format_run(run: datetime) -> str:
    """Run time as used inside coverage ids (dots instead of colons)."""
    if run.tzinfo is None:
        raise ValueError("run must be timezone-aware")
    return run.astimezone(UTC).strftime("%Y-%m-%dT%H.%M.%SZ")


def coverage_id(variable: Variable, run: datetime) -> str:
    cid = f"{variable.indicator}___{format_run(run)}"
    if variable.interval:
        cid += f"_{variable.interval}"
    return cid


@dataclass(frozen=True)
class CoverageRequest:
    """A single GetCoverage call: one member, one lead time, one variable (one level)."""

    model: EnsembleModel
    run: datetime
    member: int
    variable: Variable
    step_seconds: int
    level: int | None = None

    @property
    def url(self) -> str:
        return self.model.service_url(self.member)

    def params(self, bbox: BBox | None) -> list[tuple[str, str]]:
        subsets: list[str] = []
        if self.level is not None:
            subsets.append(f"pressure({self.level})")
        if self.variable.height is not None:
            subsets.append(f"height({self.variable.height})")
        subsets.append(f"time({self.step_seconds})")
        if bbox is not None and self.model.server_side_bbox:
            subsets.append(f"lat({bbox.lat_min},{bbox.lat_max})")
            subsets.append(f"long({bbox.lon_min},{bbox.lon_max})")
        params = [
            ("service", "WCS"),
            ("version", "2.0.1"),
            ("coverageid", coverage_id(self.variable, self.run)),
            ("format", GRIB_FORMAT),
        ]
        params.extend(("subset", s) for s in subsets)
        return params

    @property
    def key(self) -> str:
        """Stable relative path used to stage the raw GRIB of this request."""
        level = f"_{self.level}" if self.level is not None else ""
        return f"{self.variable.name}{level}/m{self.member:03d}/{self.step_seconds:07d}.grib2"


def plan_run(
    model: EnsembleModel,
    run: datetime,
    variables: Iterable[Variable],
    steps_seconds: Iterable[int],
    members: Iterable[int] | None = None,
) -> list[CoverageRequest]:
    """Expand a run into unit requests, member-major so a partial run is usable."""
    steps = list(steps_seconds)
    variables = list(variables)
    member_ids = list(range(model.members)) if members is None else list(members)
    plan: list[CoverageRequest] = []
    for member in member_ids:
        for var in variables:
            for level in var.levels or (None,):
                for step in steps:
                    plan.append(CoverageRequest(model, run, member, var, step, level))
    return plan


# --------------------------------------------------------------------------- auth


class Auth(Protocol):
    def headers(self) -> dict[str, str]: ...

    def refresh(self, stale_headers: dict[str, str]) -> None: ...


class ApiKeyAuth:
    """Static API key sent in the ``apikey`` header (no refresh possible)."""

    def __init__(self, api_key: str) -> None:
        self._api_key = api_key

    def headers(self) -> dict[str, str]:
        return {"apikey": self._api_key}

    def refresh(self, stale_headers: dict[str, str]) -> None:
        raise WcsAuthError("API key rejected; cannot refresh a static key")


class ApplicationTokenAuth:
    """OAuth2 client-credentials token, refreshed once per expiry across threads.

    Requesting a new token invalidates the previous one, so concurrent workers
    must not all refresh: only the first caller holding the stale token does.
    """

    def __init__(
        self,
        application_id: str,
        http: httpx.Client,
        token_url: str = TOKEN_URL,
    ) -> None:
        self._application_id = application_id
        self._http = http
        self._token_url = token_url
        self._token: str | None = None
        self._lock = threading.Lock()

    def _fetch_token(self) -> str:
        resp = self._http.post(
            self._token_url,
            data={"grant_type": "client_credentials"},
            headers={"Authorization": f"Basic {self._application_id}"},
            timeout=10.0,
        )
        if resp.status_code != 200:
            raise WcsAuthError(f"token endpoint returned {resp.status_code}")
        return resp.json()["access_token"]

    def headers(self) -> dict[str, str]:
        with self._lock:
            if self._token is None:
                self._token = self._fetch_token()
            return {"Authorization": f"Bearer {self._token}"}

    def refresh(self, stale_headers: dict[str, str]) -> None:
        with self._lock:
            if stale_headers.get("Authorization") == f"Bearer {self._token}":
                logger.info("token expired, requesting a new one")
                self._token = self._fetch_token()


# --------------------------------------------------------------------------- errors


class WcsError(RuntimeError):
    """Non-retryable WCS failure."""


class WcsAuthError(WcsError):
    pass


class CoverageNotFound(WcsError):
    """404: coverage not (yet) published, or an unsupported request."""


class RetriesExhausted(WcsError):
    pass


# --------------------------------------------------------------------------- rate limiting


class RateLimiter:
    """Thread-safe pacing at a fixed rate, with a shared pause after a 429.

    Each ``acquire`` reserves the next free slot, spaced ``1/rate`` seconds
    apart, so N workers together never exceed the rate. ``pause`` pushes every
    future slot past a deadline: one 429 slows all workers, not just one.
    """

    def __init__(
        self,
        rate_per_second: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if rate_per_second <= 0:
            raise ValueError("rate must be positive")
        self._interval = 1.0 / rate_per_second
        self._clock = clock
        self._sleep = sleep
        self._next_slot = clock()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = self._clock()
            slot = max(now, self._next_slot)
            self._next_slot = slot + self._interval
        delay = slot - now
        if delay > 0:
            self._sleep(delay)

    def pause(self, seconds: float) -> None:
        with self._lock:
            self._next_slot = max(self._next_slot, self._clock() + seconds)


# --------------------------------------------------------------------------- raw staging


class RawStore:
    """Raw GRIB staging area: makes a run resumable without re-spending quota."""

    MISSING_SUFFIX = ".missing"

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def path(self, req: CoverageRequest) -> Path:
        return self.root / req.key

    def has(self, req: CoverageRequest) -> bool:
        p = self.path(req)
        return p.exists() or p.with_name(p.name + self.MISSING_SUFFIX).exists()

    def is_missing(self, req: CoverageRequest) -> bool:
        p = self.path(req)
        return p.with_name(p.name + self.MISSING_SUFFIX).exists()

    def read(self, req: CoverageRequest) -> bytes | None:
        p = self.path(req)
        return p.read_bytes() if p.exists() else None

    def write(self, req: CoverageRequest, data: bytes) -> None:
        _atomic_write(self.path(req), data)

    def mark_missing(self, req: CoverageRequest) -> None:
        p = self.path(req)
        _atomic_write(p.with_name(p.name + self.MISSING_SUFFIX), b"")


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{threading.get_ident()}.part")
    with open(tmp, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


# --------------------------------------------------------------------------- client


def parse_retry_after(resp: httpx.Response, now: datetime | None = None) -> float | None:
    """Seconds to wait from ``Retry-After`` or the portal's ``nextAccessTime``."""
    now = now or datetime.now(UTC)
    header = resp.headers.get("Retry-After")
    if header:
        try:
            return max(0.0, float(header))
        except ValueError:
            try:
                when = email.utils.parsedate_to_datetime(header)
                return max(0.0, (when - now).total_seconds())
            except (TypeError, ValueError):
                pass
    try:
        next_access = resp.json().get("nextAccessTime")
    except (ValueError, AttributeError):
        return None
    if not next_access:
        return None
    # Format seen in meteole: "2024-Jan.-16 09:00:00+0000 UTC".
    for fmt in ("%Y-%b.-%d %H:%M:%S%z UTC", "%Y-%b-%d %H:%M:%S%z UTC"):
        try:
            when = datetime.strptime(next_access, fmt)
            return max(0.0, (when - now).total_seconds() + 1.0)
        except ValueError:
            continue
    return None


@dataclass
class RetryPolicy:
    max_attempts: int = 8
    base_delay: float = 2.0
    max_delay: float = 120.0
    # Floor for a 429 without usable hint: the quota window is one minute.
    min_429_delay: float = 5.0

    def backoff(self, attempt: int, rng: random.Random) -> float:
        """Exponential backoff with 'equal jitter' (half fixed, half random)."""
        capped = min(self.max_delay, self.base_delay * 2**attempt)
        return capped / 2 + rng.uniform(0, capped / 2)


@dataclass
class IngestReport:
    fetched: int = 0
    skipped: int = 0
    missing: list[CoverageRequest] = field(default_factory=list)
    failed: list[tuple[CoverageRequest, str]] = field(default_factory=list)
    http_calls: int = 0
    throttled: int = 0

    @property
    def ok(self) -> bool:
        return not self.failed


class WcsClient:
    """Rate-limited, retrying GetCoverage client for one ensemble model."""

    def __init__(
        self,
        model: EnsembleModel,
        auth: Auth,
        http: httpx.Client,
        *,
        bbox: BBox | None = FRANCE_BBOX,
        limiter: RateLimiter | None = None,
        retry: RetryPolicy | None = None,
        sleep: Callable[[float], None] = time.sleep,
        rng: random.Random | None = None,
        timeout: float = 60.0,
    ) -> None:
        self.model = model
        self.auth = auth
        self.http = http
        self.bbox = bbox
        self.limiter = limiter or RateLimiter(model.rate_per_second)
        self.retry = retry or RetryPolicy()
        self._sleep = sleep
        self._rng = rng or random.Random()
        self._timeout = timeout
        self._stats_lock = threading.Lock()
        self.http_calls = 0
        self.throttled = 0

    def _count(self, throttled: bool = False) -> None:
        with self._stats_lock:
            self.http_calls += 1
            if throttled:
                self.throttled += 1

    def fetch(self, req: CoverageRequest) -> bytes:
        """Return the GRIB bytes of one coverage, retrying 429, 5xx and network errors."""
        if req.model is not self.model:
            raise ValueError("request belongs to another model")
        params = req.params(self.bbox)
        refreshed = False
        last_error = "no attempt"
        for attempt in range(self.retry.max_attempts):
            self.limiter.acquire()
            headers = self.auth.headers()
            try:
                resp = self.http.get(req.url, params=params, headers=headers, timeout=self._timeout)
            except httpx.TransportError as exc:
                self._count()
                last_error = f"{type(exc).__name__}: {exc}"
                self._sleep(self.retry.backoff(attempt, self._rng))
                continue

            status = resp.status_code
            self._count(throttled=status == 429)
            if status == 200:
                if not resp.content.startswith(b"GRIB"):
                    raise WcsError(f"{req.key}: 200 without GRIB payload: {resp.text[:200]!r}")
                return resp.content
            if status == 401 and _is_expired_token(resp) and not refreshed:
                self.auth.refresh(headers)
                refreshed = True
                continue
            if status == 429:
                hint = parse_retry_after(resp)
                wait = max(
                    self.retry.min_429_delay,
                    hint if hint is not None else self.retry.backoff(attempt, self._rng),
                )
                wait = min(wait, self.retry.max_delay)
                logger.warning("429 on %s, pausing all workers %.1fs", req.key, wait)
                self.limiter.pause(wait)
                last_error = "429 Too Many Requests"
                continue
            if status == 404:
                raise CoverageNotFound(f"{req.key}: 404 {resp.text[:200]!r}")
            if status in (500, 502, 503, 504):
                last_error = f"HTTP {status}"
                self._sleep(self.retry.backoff(attempt, self._rng))
                continue
            if status in (401, 403):
                raise WcsAuthError(f"{req.key}: HTTP {status} {resp.text[:200]!r}")
            raise WcsError(f"{req.key}: HTTP {status} {resp.text[:200]!r}")
        raise RetriesExhausted(f"{req.key}: {self.retry.max_attempts} attempts, last: {last_error}")

    def download(
        self,
        plan: Iterable[CoverageRequest],
        store: RawStore,
        *,
        workers: int = 4,
        max_auth_errors: int = 1,
        retry_missing: bool = True,
    ) -> IngestReport:
        """Fetch every request not already staged. Safe to rerun after a crash.

        A 404 may only mean "not published yet", so previously missing fields
        are asked again unless ``retry_missing`` is False.
        """
        report = IngestReport()
        todo: list[CoverageRequest] = []
        for req in plan:
            if store.path(req).exists() or (not retry_missing and store.is_missing(req)):
                report.skipped += 1
            else:
                todo.append(req)
        calls_before, throttled_before = self.http_calls, self.throttled
        stop = threading.Event()

        def task(req: CoverageRequest) -> None:
            if stop.is_set():
                raise WcsError("aborted")
            store.write(req, self.fetch(req))

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(task, req): req for req in todo}
            auth_errors = 0
            for fut in as_completed(futures):
                req = futures[fut]
                try:
                    fut.result()
                    report.fetched += 1
                except CoverageNotFound:
                    store.mark_missing(req)
                    report.missing.append(req)
                except WcsAuthError as exc:
                    report.failed.append((req, str(exc)))
                    auth_errors += 1
                    if auth_errors >= max_auth_errors:
                        stop.set()
                except WcsError as exc:
                    report.failed.append((req, str(exc)))
        report.http_calls = self.http_calls - calls_before
        report.throttled = self.throttled - throttled_before
        return report


def _is_expired_token(resp: httpx.Response) -> bool:
    if "application/json" not in resp.headers.get("Content-Type", ""):
        return False
    try:
        return str(resp.json().get("code")) == INVALID_JWT_CODE
    except ValueError:
        return False


def iter_staged(
    plan: Iterable[CoverageRequest], store: RawStore
) -> Iterator[tuple[CoverageRequest, bytes | None]]:
    for req in plan:
        yield req, store.read(req)
