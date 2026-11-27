"""Tests for the Météo-France WCS client, on simulated HTTP responses only."""

from __future__ import annotations

import json
import random
import threading
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from ingest.meteofrance_wcs import (
    AROME_PE,
    PEARP,
    ApiKeyAuth,
    ApplicationTokenAuth,
    BBox,
    CoverageNotFound,
    CoverageRequest,
    RateLimiter,
    RawStore,
    RetriesExhausted,
    RetryPolicy,
    Variable,
    WcsAuthError,
    WcsClient,
    WcsError,
    coverage_id,
    parse_retry_after,
    plan_run,
)

RUN = datetime(2026, 11, 27, 3, tzinfo=UTC)
T2M = Variable("t2m", "TEMPERATURE__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND", height=2)
TP = Variable("tp", "TOTAL_PRECIPITATION__GROUND_OR_WATER_SURFACE", interval="PT1H")
T_PL = Variable("t", "TEMPERATURE__ISOBARIC_SURFACE", group="pressure", levels=(850, 700, 500))
BBOX = BBox(41.0, 51.5, -5.5, 10.0)
GRIB = b"GRIB" + b"\x00" * 60 + b"7777"


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []
        self._lock = threading.Lock()

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        with self._lock:
            self.sleeps.append(seconds)
            self.now += seconds


def make_client(handler, *, model=AROME_PE, auth=None, retry=None, bbox=BBOX):
    clock = FakeClock()
    http = httpx.Client(transport=httpx.MockTransport(handler))
    client = WcsClient(
        model,
        auth or ApiKeyAuth("test-key"),
        http,
        bbox=bbox,
        limiter=RateLimiter(model.rate_per_second, clock=clock.clock, sleep=clock.sleep),
        retry=retry or RetryPolicy(max_attempts=4, base_delay=1.0, max_delay=60.0),
        sleep=clock.sleep,
        rng=random.Random(0),
    )
    return client, clock


def req(member=0, step=3600, var=T2M, level=None, model=AROME_PE):
    return CoverageRequest(model, RUN, member, var, step, level)


# --------------------------------------------------------------------------- URLs and params


def test_service_url_per_member():
    assert AROME_PE.service_url(0) == (
        "https://public-api.meteofrance.fr/public/pearome/1.0/wcs/"
        "MF-NWP-HIGHRES-PEARO000-0025-FRANCE-WCS/GetCoverage"
    )
    assert PEARP.service_url(34).endswith(
        "/pearpege/1.0/wcs/MF-NWP-GLOBAL-PEARP034-01-EUROPE-WCS/GetCoverage"
    )
    with pytest.raises(ValueError):
        AROME_PE.service_url(25)
    with pytest.raises(ValueError):
        PEARP.service_url(-1)


def test_coverage_id_format():
    assert coverage_id(T2M, RUN) == (
        "TEMPERATURE__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND___2026-11-27T03.00.00Z"
    )
    assert coverage_id(TP, RUN).endswith("___2026-11-27T03.00.00Z_PT1H")
    with pytest.raises(ValueError):
        coverage_id(T2M, datetime(2026, 11, 27, 3))


def test_params_single_instant_and_server_side_bbox():
    params = req(step=7200).params(BBOX)
    subsets = [v for k, v in params if k == "subset"]
    assert subsets == ["height(2)", "time(7200)", "lat(41.0,51.5)", "long(-5.5,10.0)"]
    assert ("format", "application/wmo-grib") in params
    assert ("coverageid", coverage_id(T2M, RUN)) in params
    # Exactly one time subset, never a range.
    assert sum(s.startswith("time(") for s in subsets) == 1
    assert "," not in next(s for s in subsets if s.startswith("time("))


def test_params_pressure_level_and_no_bbox_when_disabled():
    from dataclasses import replace

    model = replace(PEARP, server_side_bbox=False)
    params = CoverageRequest(model, RUN, 3, T_PL, 10800, 850).params(BBOX)
    subsets = [v for k, v in params if k == "subset"]
    assert subsets == ["pressure(850)", "time(10800)"]


def test_variable_validation():
    with pytest.raises(ValueError):
        Variable("t", "X", group="pressure")
    with pytest.raises(ValueError):
        Variable("t", "X", levels=(850,))
    with pytest.raises(ValueError):
        BBox(50, 40, 0, 1)


def test_plan_run_is_one_call_per_member_step_variable_level():
    steps = [3600, 7200, 10800]
    plan = plan_run(AROME_PE, RUN, [T2M, T_PL], steps)
    assert len(plan) == 25 * (1 + 3) * 3
    assert len({r.key for r in plan}) == len(plan)
    assert plan[0].member == 0 and plan[-1].member == 24
    assert len(plan_run(PEARP, RUN, [T2M], steps, members=[0, 1])) == 6


# --------------------------------------------------------------------------- rate limiter


def test_rate_limiter_spaces_calls():
    clock = FakeClock()
    lim = RateLimiter(10.0, clock=clock.clock, sleep=clock.sleep)
    for _ in range(5):
        lim.acquire()
    assert clock.now == pytest.approx(0.4)
    assert len(clock.sleeps) == 4


def test_rate_limiter_does_not_bank_idle_time():
    clock = FakeClock()
    lim = RateLimiter(5.0, clock=clock.clock, sleep=clock.sleep)
    lim.acquire()
    clock.now = 100.0
    lim.acquire()
    lim.acquire()
    assert clock.now == pytest.approx(100.2)


def test_rate_limiter_pause_delays_everyone():
    clock = FakeClock()
    lim = RateLimiter(10.0, clock=clock.clock, sleep=clock.sleep)
    lim.acquire()
    lim.pause(30.0)
    lim.acquire()
    assert clock.now == pytest.approx(30.0)


def test_rate_limiter_threads_never_exceed_rate():
    """Real clock, 40 acquisitions over 8 threads at 200/s: >= 39 intervals."""
    import time

    lim = RateLimiter(200.0)
    stamps: list[float] = []
    lock = threading.Lock()

    def work():
        for _ in range(5):
            lim.acquire()
            with lock:
                stamps.append(time.monotonic())

    threads = [threading.Thread(target=work) for _ in range(8)]
    start = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(stamps) == 40
    assert max(stamps) - start >= 39 / 200 - 0.01


# --------------------------------------------------------------------------- fetch


def test_fetch_ok_sends_auth_and_params():
    seen = []

    def handler(request: httpx.Request):
        seen.append(request)
        return httpx.Response(200, content=GRIB)

    client, _ = make_client(handler)
    assert client.fetch(req()) == GRIB
    (r,) = seen
    assert r.headers["apikey"] == "test-key"
    assert r.url.params.get_list("subset")[1] == "time(3600)"
    assert r.url.path.endswith("PEARO000-0025-FRANCE-WCS/GetCoverage")


def test_fetch_rejects_200_without_grib():
    client, _ = make_client(lambda r: httpx.Response(200, text="<ows:Exception/>"))
    with pytest.raises(WcsError, match="without GRIB"):
        client.fetch(req())


def test_fetch_429_retry_after_pauses_then_succeeds():
    responses = iter(
        [httpx.Response(429, headers={"Retry-After": "12"}), httpx.Response(200, content=GRIB)]
    )
    client, clock = make_client(lambda r: next(responses))
    assert client.fetch(req()) == GRIB
    assert client.throttled == 1 and client.http_calls == 2
    assert clock.now == pytest.approx(12.0)


def test_fetch_429_without_hint_uses_floor():
    responses = iter([httpx.Response(429), httpx.Response(200, content=GRIB)])
    client, clock = make_client(lambda r: next(responses))
    client.fetch(req())
    assert clock.now >= RetryPolicy().min_429_delay


def test_fetch_429_exhausts_retries():
    client, _ = make_client(lambda r: httpx.Response(429, headers={"Retry-After": "1"}))
    with pytest.raises(RetriesExhausted, match="429"):
        client.fetch(req())
    assert client.http_calls == 4


def test_fetch_404_is_not_retried():
    calls = []

    def handler(r):
        calls.append(r)
        return httpx.Response(404, text="Synopsis backend error")

    client, _ = make_client(handler)
    with pytest.raises(CoverageNotFound):
        client.fetch(req())
    assert len(calls) == 1


def test_fetch_5xx_and_network_errors_back_off():
    state = {"n": 0}

    def handler(r):
        state["n"] += 1
        if state["n"] == 1:
            raise httpx.ConnectError("boom")
        if state["n"] == 2:
            return httpx.Response(503)
        return httpx.Response(200, content=GRIB)

    client, clock = make_client(handler)
    assert client.fetch(req()) == GRIB
    assert state["n"] == 3
    assert len(clock.sleeps) >= 2 and clock.now > 0


def test_fetch_400_fails_fast():
    client, _ = make_client(lambda r: httpx.Response(400, text="bad subset"))
    with pytest.raises(WcsError, match="400"):
        client.fetch(req())


def test_expired_token_is_refreshed_once():
    tokens = iter(["tok-1", "tok-2"])
    token_calls = []

    def handler(r: httpx.Request):
        if r.url.path == "/token":
            token_calls.append(r)
            assert r.headers["Authorization"] == "Basic app-id"
            return httpx.Response(200, json={"access_token": next(tokens)})
        if r.headers["Authorization"] == "Bearer tok-1":
            return httpx.Response(
                401,
                headers={"Content-Type": "application/json"},
                content=json.dumps({"code": "900901"}),
            )
        return httpx.Response(200, content=GRIB)

    http = httpx.Client(transport=httpx.MockTransport(handler))
    auth = ApplicationTokenAuth("app-id", http, token_url="https://portail.test/token")
    client, _ = make_client(handler, auth=auth)
    assert client.fetch(req()) == GRIB
    assert len(token_calls) == 2


def test_refresh_ignored_when_token_already_renewed():
    tokens = iter(["tok-1", "tok-2", "tok-3"])
    http = httpx.Client(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(200, json={"access_token": next(tokens)})
        )
    )
    auth = ApplicationTokenAuth("app-id", http, token_url="https://portail.test/token")
    stale = auth.headers()
    auth.refresh(stale)
    auth.refresh(stale)  # second worker with the same stale token: no new token
    assert auth.headers() == {"Authorization": "Bearer tok-2"}


def test_api_key_rejected_is_auth_error():
    client, _ = make_client(lambda r: httpx.Response(403, text="forbidden"))
    with pytest.raises(WcsAuthError):
        client.fetch(req())


def test_parse_retry_after_variants():
    now = datetime(2024, 1, 16, 8, 59, 30, tzinfo=UTC)
    r = httpx.Response(429, json={"nextAccessTime": "2024-Jan.-16 09:00:00+0000 UTC"})
    assert parse_retry_after(r, now) == pytest.approx(31.0)
    later = (now + timedelta(seconds=20)).strftime("%a, %d %b %Y %H:%M:%S GMT")
    assert parse_retry_after(httpx.Response(429, headers={"Retry-After": later}), now) == 20
    assert parse_retry_after(httpx.Response(429, text="nope"), now) is None


# --------------------------------------------------------------------------- download


def test_download_stages_marks_missing_and_resumes(tmp_path):
    calls = []
    lock = threading.Lock()

    def handler(r: httpx.Request):
        with lock:
            calls.append(r)
        if "PEARO001" in r.url.path and r.url.params.get_list("subset")[1] == "time(7200)":
            return httpx.Response(404, text="Synopsis backend error")
        return httpx.Response(200, content=GRIB)

    plan = plan_run(AROME_PE, RUN, [T2M], [3600, 7200], members=[0, 1, 2])
    store = RawStore(tmp_path / "raw")
    client, _ = make_client(handler)

    report = client.download(plan, store, workers=4)
    assert report.ok
    assert report.fetched == 5 and len(report.missing) == 1 and report.http_calls == 6
    assert all(store.has(r) for r in plan)
    assert not list((tmp_path / "raw").rglob("*.part"))

    again = client.download(plan, store, workers=4)
    assert again.skipped == 6 and again.http_calls == 0
    assert len(calls) == 6


def test_download_stops_on_auth_error(tmp_path):
    client, _ = make_client(lambda r: httpx.Response(403, text="forbidden"))
    plan = plan_run(AROME_PE, RUN, [T2M], [3600], members=range(10))
    report = client.download(plan, RawStore(tmp_path), workers=1)
    assert not report.ok
    assert report.http_calls < 10
    assert report.fetched == 0
