"""Tests for the API Paquets client, on simulated HTTP responses only."""

from __future__ import annotations

import json
import random
from datetime import UTC, datetime

import httpx
import pytest

from ingest.meteofrance_paquets import (
    AROME_001,
    BadRequest,
    NotPublished,
    PackageFile,
    PackageSpec,
    PaquetsAuthError,
    PaquetsClient,
    PaquetsModel,
    RetriesExhausted,
    RunStage,
    format_run,
    parse_files,
    parse_runs,
    parse_time_label,
    prune_runs,
    run_tag,
    staged_runs,
)
from ingest.meteofrance_wcs import ApiKeyAuth, ApplicationTokenAuth, RateLimiter, RetryPolicy
from ingest.mf_paquets_fetch import main as cli_main

RUN = datetime(2026, 11, 27, 6, tzinfo=UTC)
OLD_RUN = datetime(2026, 11, 27, 3, tzinfo=UTC)
CATALOG_HOST = "https://public-api.meteofrance.fr/previnum/DPPaquetAROME"

SMALL = PaquetsModel(
    name="arome",
    api="DPPaquetAROME",
    model="AROME",
    grid="0.01",
    product="productARO",
    packages=(
        PackageSpec("SP1", expected_files=3),
        PackageSpec("SP3", expected_files=1, limit=1),
    ),
)


def grib(n: int = 64, tag: bytes = b"") -> bytes:
    body = tag + b"\x00" * (n - len(tag))
    return b"GRIB" + body + b"7777"


def runs_payload(runs: list[datetime], package: str = "SP1") -> dict:
    # Catalogue hrefs without the /v1 segment, as seen in the spike.
    return {
        "links": [
            {
                "href": f"{CATALOG_HOST}/models/AROME/grids/0.01/packages/{package}"
                f"?referencetime={format_run(r)}"
            }
            for r in runs
        ]
    }


def files_payload(package: str, run: datetime, labels: list[str]) -> dict:
    links = [{"href": f"{CATALOG_HOST}/models/AROME/grids/0.01/packages/{package}"}]
    for label in labels:
        links.append(
            {
                "href": f"{CATALOG_HOST}/models/AROME/grids/0.01/packages/{package}/productARO"
                f"?referencetime={format_run(run)}&time={label}&format=grib2",
                "time": label,
            }
        )
    return {"links": links}


class FakeApi:
    """Minimal in-memory API Paquets."""

    def __init__(self, published: dict[datetime, dict[str, list[str]]]):
        self.published = published
        self.blobs: dict[tuple[str, str, str], bytes] = {}
        self.calls: list[httpx.Request] = []
        self.scripted: list[httpx.Response] = []  # returned first, in order
        self.truncate_once: set[tuple[str, str, str]] = set()
        for run, packages in published.items():
            for package, labels in packages.items():
                for label in labels:
                    self.blobs[(format_run(run), package, label)] = grib(
                        256, f"{package}{label}".encode()
                    )

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request)
        if self.scripted:
            return self.scripted.pop(0)
        path = request.url.path
        assert "/DPPaquetAROME/v1/" in path, path
        q = dict(request.url.params)
        package = path.split("/packages/")[1].split("/")[0]
        if path.endswith("/productARO"):
            key = (q["referencetime"], package, q["time"])
            if key not in self.blobs:
                return httpx.Response(404, text="not found")
            data = self.blobs[key]
            rng = request.headers.get("Range")
            if key in self.truncate_once:
                self.truncate_once.discard(key)
                return httpx.Response(200, content=data[: len(data) // 2])
            if rng:
                start = int(rng.removeprefix("bytes=").rstrip("-"))
                return httpx.Response(206, content=data[start:])
            return httpx.Response(200, content=data)
        if "referencetime" in q:
            run = datetime.strptime(q["referencetime"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
            labels = self.published.get(run, {}).get(package)
            if labels is None:
                return httpx.Response(404, json={"message": "no data"})
            return httpx.Response(200, json=files_payload(package, run, labels))
        runs = [r for r, pk in self.published.items() if package in pk]
        return httpx.Response(200, json=runs_payload(sorted(runs), package))


def make_client(api: FakeApi, model: PaquetsModel = SMALL, auth=None) -> PaquetsClient:
    http = httpx.Client(transport=httpx.MockTransport(api.handler))
    sleeps: list[float] = []
    return PaquetsClient(
        model,
        auth or ApiKeyAuth("k"),
        http,
        limiter=RateLimiter(1000.0, sleep=lambda s: None),
        retry=RetryPolicy(max_attempts=4),
        sleep=sleeps.append,
        rng=random.Random(0),
    )


FULL = {"SP1": ["00H", "01H", "02H"], "SP3": ["00H", "01H", "02H"]}


# --------------------------------------------------------------------------- parsing


def test_parse_time_label():
    assert parse_time_label("07H") == (7, 7)
    assert parse_time_label("00H06H") == (0, 6)
    with pytest.raises(ValueError):
        parse_time_label("6h")
    with pytest.raises(ValueError):
        parse_time_label("06H00H")


def test_parse_runs_sorted_and_deduplicated():
    payload = runs_payload([RUN, OLD_RUN, RUN])
    assert parse_runs(payload) == [OLD_RUN, RUN]


def test_parse_files_adds_version_and_sorts_by_lead_time():
    payload = files_payload("SP1", RUN, ["10H", "02H", "00H"])
    files = parse_files(payload, SMALL, "SP1", RUN)
    assert [f.time for f in files] == ["00H", "02H", "10H"]
    assert all("/DPPaquetAROME/v1/models/" in f.url for f in files)
    assert files[0].key == "SP1/00H.grib2"


def test_parse_files_ignores_other_runs():
    payload = files_payload("SP1", OLD_RUN, ["00H"])
    assert parse_files(payload, SMALL, "SP1", RUN) == []


def test_model_urls():
    assert AROME_001.product_url("SP1") == (
        "https://public-api.meteofrance.fr/previnum/DPPaquetAROME/v1/models/AROME/grids/0.01"
        "/packages/SP1/productARO"
    )
    assert AROME_001.rate_per_second * 60 < 100  # below the portal quota


def test_package_spec_validation():
    with pytest.raises(ValueError):
        PackageSpec("SP3", expected_files=2, limit=1)
    with pytest.raises(ValueError):
        PackageSpec("SP3", limit=0)


def test_run_tag_requires_aware_datetime():
    assert run_tag(RUN) == "20261127T06Z"
    with pytest.raises(ValueError):
        run_tag(datetime(2026, 11, 27, 6))


# --------------------------------------------------------------------------- run selection


def test_latest_complete_run_skips_partial_run():
    api = FakeApi({OLD_RUN: FULL, RUN: {"SP1": ["00H", "01H"], "SP3": ["00H"]}})
    found = make_client(api).latest_complete_run()
    assert found is not None
    run, plan = found
    assert run == OLD_RUN
    # SP3 limited to its first file.
    assert [f.key for f in plan] == [
        "SP1/00H.grib2",
        "SP1/01H.grib2",
        "SP1/02H.grib2",
        "SP3/00H.grib2",
    ]


def test_latest_complete_run_when_package_missing():
    api = FakeApi({RUN: {"SP1": ["00H", "01H", "02H"]}})
    assert make_client(api).latest_complete_run() is None


# --------------------------------------------------------------------------- download


def test_fetch_latest_downloads_and_writes_manifest(tmp_path):
    api = FakeApi({OLD_RUN: FULL, RUN: FULL})
    client = make_client(api)
    report = client.fetch_latest(tmp_path)
    assert report.ok and report.run == RUN
    assert report.fetched == 4 and report.skipped == 0
    stage = RunStage(tmp_path, RUN)
    assert (
        stage.file_path(PackageFile("SP1", RUN, "02H", "")).read_bytes()
        == api.blobs[(format_run(RUN), "SP1", "02H")]
    )
    manifest = stage.read_manifest()
    assert manifest["run"] == "2026-11-27T06:00:00Z"
    assert manifest["packages"] == {"SP1": ["00H", "01H", "02H"], "SP3": ["00H"]}
    assert not list(tmp_path.rglob("*.part"))
    # Every request carries the API key and is sequential (no Range on fresh files).
    assert all(r.headers["apikey"] == "k" for r in api.calls)

    # Second call: nothing downloaded again.
    calls = len(api.calls)
    again = client.fetch_latest(tmp_path)
    assert again.ok and again.fetched == 0
    assert not any(r.url.path.endswith("/productARO") for r in api.calls[calls:])


def test_rerun_skips_files_already_staged(tmp_path):
    api = FakeApi({RUN: FULL})
    client = make_client(api)
    _, plan = client.latest_complete_run()
    stage = RunStage(tmp_path, RUN)
    first = plan[0]
    stage.file_path(first).parent.mkdir(parents=True)
    stage.file_path(first).write_bytes(api.blobs[(format_run(RUN), "SP1", "00H")])
    report = client.download_run(RUN, plan, tmp_path)
    assert report.ok and report.skipped == 1 and report.fetched == 3


def test_truncated_download_is_resumed_with_range(tmp_path):
    api = FakeApi({RUN: FULL})
    key = (format_run(RUN), "SP1", "01H")
    api.truncate_once.add(key)
    client = make_client(api)
    report = client.fetch_latest(tmp_path)
    assert report.ok and report.resumed == 1
    ranged = [r for r in api.calls if "Range" in r.headers]
    assert len(ranged) == 1
    assert ranged[0].headers["Range"] == f"bytes={len(api.blobs[key]) // 2}-"
    path = RunStage(tmp_path, RUN).file_path(PackageFile("SP1", RUN, "01H", ""))
    assert path.read_bytes() == api.blobs[key]


def test_part_file_from_crash_is_resumed(tmp_path):
    api = FakeApi({RUN: FULL})
    client = make_client(api)
    _, plan = client.latest_complete_run()
    stage = RunStage(tmp_path, RUN)
    data = api.blobs[(format_run(RUN), "SP1", "00H")]
    part = stage.part_path(plan[0])
    part.parent.mkdir(parents=True)
    part.write_bytes(data[:10])
    size, resumed = client.download_file(plan[0], stage)
    assert resumed and size == len(data)
    assert stage.file_path(plan[0]).read_bytes() == data


def test_416_restarts_download_from_zero(tmp_path):
    api = FakeApi({RUN: FULL})
    client = make_client(api)
    _, plan = client.latest_complete_run()
    stage = RunStage(tmp_path, RUN)
    part = stage.part_path(plan[0])
    part.parent.mkdir(parents=True)
    part.write_bytes(b"stale")
    api.scripted.append(httpx.Response(416))
    _size, resumed = client.download_file(plan[0], stage)
    assert not resumed
    assert "Range" not in api.calls[-1].headers
    assert stage.file_path(plan[0]).read_bytes() == api.blobs[(format_run(RUN), "SP1", "00H")]


def test_non_grib_payload_is_a_failure_without_manifest(tmp_path):
    api = FakeApi({RUN: FULL})
    api.blobs[(format_run(RUN), "SP1", "01H")] = b"<html>maintenance</html>"
    report = make_client(api).fetch_latest(tmp_path)
    assert not report.ok and len(report.failed) == 1
    assert report.fetched == 3
    assert not RunStage(tmp_path, RUN).is_complete()


def test_429_pauses_then_succeeds(tmp_path):
    api = FakeApi({RUN: FULL})
    client = make_client(api)
    api.scripted.append(httpx.Response(429, headers={"Retry-After": "7"}))
    runs = client.list_runs("SP1")
    assert runs == [RUN]
    assert client.throttled == 1


def test_5xx_retries_then_gives_up():
    api = FakeApi({RUN: FULL})
    client = make_client(api)
    api.scripted.extend(httpx.Response(503) for _ in range(4))
    with pytest.raises(RetriesExhausted):
        client.list_runs("SP1")


def test_400_and_404_are_not_retried():
    api = FakeApi({RUN: FULL})
    client = make_client(api)
    api.scripted.append(httpx.Response(400, text="bad time"))
    with pytest.raises(BadRequest):
        client.list_runs("SP1")
    with pytest.raises(NotPublished):
        client.list_files("SP1", OLD_RUN)
    assert len(api.calls) == 2


def test_403_is_an_auth_error_and_stops_the_run(tmp_path):
    api = FakeApi({RUN: FULL})
    client = make_client(api)
    _, plan = client.latest_complete_run()
    api.scripted.append(httpx.Response(403, text="forbidden"))
    with pytest.raises(PaquetsAuthError):
        client.download_run(RUN, plan, tmp_path)


def test_expired_token_is_refreshed_once(tmp_path):
    api = FakeApi({RUN: FULL})
    tokens = iter(["t1", "t2"])

    def token_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"access_token": next(tokens)})

    auth = ApplicationTokenAuth("app", httpx.Client(transport=httpx.MockTransport(token_handler)))
    client = make_client(api, auth=auth)
    api.scripted.append(httpx.Response(401, json={"code": "900901"}))
    assert client.list_runs("SP1") == [RUN]
    assert api.calls[0].headers["Authorization"] == "Bearer t1"
    assert api.calls[1].headers["Authorization"] == "Bearer t2"


# --------------------------------------------------------------------------- retention


def _stage(root, run, complete=True):
    p = root / run_tag(run)
    p.mkdir(parents=True)
    if complete:
        (p / "manifest.json").write_text(json.dumps({"run": format_run(run)}))


def test_prune_keeps_latest_complete_and_newer_partial(tmp_path):
    runs = [datetime(2026, 11, 26, h, tzinfo=UTC) for h in (0, 3, 6, 12)]
    for r in runs:
        _stage(tmp_path, r)
    partial_old = datetime(2026, 11, 25, 18, tzinfo=UTC)
    partial_new = datetime(2026, 11, 26, 18, tzinfo=UTC)
    _stage(tmp_path, partial_old, complete=False)
    _stage(tmp_path, partial_new, complete=False)
    (tmp_path / "not-a-run").mkdir()

    removed = prune_runs(tmp_path, keep=2)
    assert sorted(removed) == [partial_old, runs[0], runs[1]]
    assert staged_runs(tmp_path) == runs[2:]
    assert staged_runs(tmp_path, complete_only=False) == [*runs[2:], partial_new]
    assert (tmp_path / "not-a-run").exists()


def test_prune_rejects_zero():
    with pytest.raises(ValueError):
        prune_runs(None, keep=0)


# --------------------------------------------------------------------------- CLI


def test_cli_requires_credentials(monkeypatch, tmp_path):
    monkeypatch.delenv("METEOFRANCE_APPLICATION_ID", raising=False)
    monkeypatch.delenv("METEOFRANCE_API_KEY", raising=False)
    monkeypatch.chdir(tmp_path)  # no .env here or above (tmp dirs)
    with pytest.raises(SystemExit):
        cli_main(["plan", "--root", str(tmp_path)])


def test_cli_reads_credentials_from_dotenv_in_parent(monkeypatch, tmp_path):
    monkeypatch.delenv("METEOFRANCE_APPLICATION_ID", raising=False)
    monkeypatch.delenv("METEOFRANCE_API_KEY", raising=False)
    (tmp_path / ".env").write_text("METEOFRANCE_API_KEY=from-file\n")
    (tmp_path / "src").mkdir()
    monkeypatch.chdir(tmp_path / "src")  # as documented: commands run from src/
    seen = {}

    def fake_make_auth(http, env):
        seen.update(env)
        raise SystemExit("stop before any HTTP call")

    monkeypatch.setattr("ingest.mf_paquets_fetch.make_auth", fake_make_auth)
    with pytest.raises(SystemExit, match="stop before"):
        cli_main(["plan", "--root", str(tmp_path)])
    assert seen["METEOFRANCE_API_KEY"] == "from-file"
