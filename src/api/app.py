"""Public HTTP API (FastAPI).

Routes:
- ``GET /v1/models``: available models, their current run, grid extent and variables;
- ``GET /v1/raw/{model}/point``: stored values interpolated at a point, no correction;
- ``GET /v1/forecast/point``: altitude-corrected forecast with derived quantities
  (one deterministic model, AROME by default; the multi-model clusters come later).

Run with ``uvicorn api.app:app`` from ``src``. Paths come from ``DATA_DIR``
(environment or ``.env``), the served models from ``NIMBOSTRA_MODELS``
(comma-separated, default ``arome,arpege``; add ``ifs`` for ECMWF IFS).
Every response names its data sources in ``attribution``: the ECMWF CC BY 4.0
terms require it for IFS. Endpoints are plain ``def``: FastAPI runs them in its
thread pool, which suits the blocking Zarr reads.

Responses carry ``Cache-Control`` and an ``ETag`` derived from the run and the
query, so Caddy or the browser can reuse them until the next run is published.
The ETag never depends on the body, so the per-request ``timing`` field (also
sent as a ``Server-Timing`` header) does not defeat revalidation; a matching
``If-None-Match`` is answered with 304 before any Zarr read.
"""

from __future__ import annotations

import hashlib
import os
import time
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Literal

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi import Path as PathParam
from fastapi.responses import JSONResponse

from ingest.dem import open_dem
from ingest.dotenv import load_dotenv
from ingest.health import check_freshness, configured_models, max_age_for
from point.reader import ElevationSource, NoRun, OutOfDomain, RunStore, derive, read_raw

API_VERSION = "v1"
CACHE_SECONDS = 300
METEOFRANCE_ATTRIBUTION = "Source : Météo-France (Licence Ouverte Etalab 2.0)"
# Wording required by the ECMWF open data terms (CC BY 4.0): the English
# sentence, copyright, source, licence, disclaimer and the changes made.
ECMWF_ATTRIBUTION = (
    "This service is based on data and products of the European Centre for "
    "Medium-Range Weather Forecasts (ECMWF). Source : www.ecmwf.int, © {year} ECMWF, "
    "licence CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/). "
    "Données recadrées, interpolées au point et converties d'unités par Nimbostra ; "
    "l'ECMWF n'est pas responsable de leur exactitude ni de leur utilisation."
)
ATTRIBUTIONS = {"meteofrance": METEOFRANCE_ATTRIBUTION, "ecmwf": ECMWF_ATTRIBUTION}

Lat = Annotated[float, Query(ge=-90, le=90, description="Latitude en degrés décimaux")]
Lon = Annotated[float, Query(ge=-180, le=180, description="Longitude en degrés décimaux")]
Method = Annotated[Literal["bilinear", "nearest"], Query(description="Interpolation")]


@dataclass(frozen=True)
class ModelInfo:
    name: str
    label: str
    resolution: str
    provider: str = "meteofrance"

    def attribution(self, year: int | None = None) -> str:
        year = year or datetime.now(UTC).year
        return ATTRIBUTIONS[self.provider].format(year=year)


KNOWN_MODELS = {
    "arome": ModelInfo("arome", "AROME (Météo-France)", "0.01°"),
    "arpege": ModelInfo("arpege", "ARPEGE (Météo-France)", "0.1°"),
    "ifs": ModelInfo("ifs", "IFS (ECMWF)", "0.1°", provider="ecmwf"),
}


def attribution(models: Iterable[str]) -> str:
    """Attributions of the given models' providers, each once, in model order."""
    texts = [KNOWN_MODELS[m].attribution() for m in models if m in KNOWN_MODELS]
    return " | ".join(dict.fromkeys(texts))


@dataclass
class Settings:
    data_dir: Path
    models: tuple[str, ...] = ("arome",)
    dem: ElevationSource | None = None
    stores: dict[str, RunStore] = field(default_factory=dict)

    def store(self, model: str) -> RunStore:
        if model not in self.models or model not in KNOWN_MODELS:
            raise HTTPException(404, f"modèle inconnu : {model}")
        if model not in self.stores:
            self.stores[model] = RunStore(self.data_dir / model, model)
        return self.stores[model]


class Timer:
    """Wall-clock steps of one request, in milliseconds.

    ``step(name)`` closes the step started at the previous call (or at creation);
    ``total_ms`` runs from creation.
    """

    def __init__(self) -> None:
        self.start = self._last = time.perf_counter()
        self.steps: dict[str, float] = {}

    def step(self, name: str) -> None:
        now = time.perf_counter()
        self.steps[name] = self.steps.get(name, 0.0) + (now - self._last) * 1000
        self._last = now

    def as_json(self) -> dict[str, float]:
        out = {f"{k}_ms": round(v, 1) for k, v in self.steps.items()}
        out["total_ms"] = round((time.perf_counter() - self.start) * 1000, 1)
        return out

    @staticmethod
    def header(timing: dict[str, float]) -> str:
        """``Server-Timing`` value, e.g. ``read;dur=12.3, total;dur=14.1``."""
        return ", ".join(f"{k.removesuffix('_ms')};dur={v}" for k, v in timing.items())


def _etag(*parts: object) -> str:
    digest = hashlib.sha1("|".join(map(str, parts)).encode()).hexdigest()[:20]
    return f'"{digest}"'


def _cache_headers(etag: str) -> dict[str, str]:
    return {"Cache-Control": f"public, max-age={CACHE_SECONDS}", "ETag": etag}


def _not_modified(request: Request, etag: str) -> Response | None:
    """304 if the client already holds this ETag, else None."""
    if request.headers.get("if-none-match") == etag:
        return Response(status_code=304, headers=_cache_headers(etag))
    return None


def _cached(request: Request, body: dict, etag: str, timer: Timer) -> Response:
    """JSON response with cache headers and timings (body ``timing`` + ``Server-Timing``).

    The ETag is computed from the run and the query only, never from the body,
    so the timings changing on every call leave it stable.
    """
    if (hit := _not_modified(request, etag)) is not None:
        return hit
    timing = timer.as_json()
    body["timing"] = timing
    headers = _cache_headers(etag) | {"Server-Timing": Timer.header(timing)}
    return JSONResponse(body, headers=headers)


def create_app(settings: Settings | None = None) -> FastAPI:
    if settings is None:
        load_dotenv()
        data_dir = Path(os.environ.get("DATA_DIR", "data/zarr"))
        models = tuple(m for m in configured_models() if m in KNOWN_MODELS)
        settings = Settings(data_dir=data_dir, models=models, dem=open_dem(data_dir))
    app = FastAPI(
        title="Nimbostra API",
        version="0.1.0",
        description="Prévisions météo par point issues de modèles en données ouvertes.",
        docs_url="/v1/docs",
        openapi_url="/v1/openapi.json",
    )
    app.state.settings = settings

    def open_latest(model: str):
        store = settings.store(model)
        try:
            run, path = store.latest()
            return run, store.group(path)
        except NoRun as exc:
            raise HTTPException(503, f"aucun run disponible pour {model}") from exc

    @app.get("/v1/health")
    def health() -> Response:
        """Freshness of each model's latest run: 200 if all fresh, else 503.

        Meant for an external uptime monitor; never cached.
        """
        checks = [
            check_freshness(settings.data_dir / name, name, max_age_hours=max_age_for(name))
            for name in settings.models
        ]
        body = {
            "status": "ok" if all(c.ok for c in checks) else "stale",
            "models": {
                c.model: {
                    "run": c.run.strftime("%Y-%m-%dT%H:%M:%SZ") if c.run else None,
                    "age_hours": None if c.age_hours is None else round(c.age_hours, 2),
                    "max_age_hours": c.max_age_hours,
                }
                for c in checks
            },
        }
        status = 200 if body["status"] == "ok" else 503
        return JSONResponse(body, status_code=status, headers={"Cache-Control": "no-store"})

    @app.get("/v1/models")
    def models(request: Request) -> Response:
        timer = Timer()
        out, runs = [], []
        for name in settings.models:
            info = KNOWN_MODELS[name]
            entry: dict = {
                "name": name,
                "label": info.label,
                "resolution": info.resolution,
                "attribution": info.attribution(),
            }
            try:
                run, group = open_latest(name)
            except HTTPException:
                entry["run"] = None
                out.append(entry)
                continue
            lats, lons = group["lat"][:], group["lon"][:]
            seconds = group["time"][:]
            entry.update(
                {
                    "run": run.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "domain": {
                        "lat_min": round(float(lats[0]), 4),
                        "lat_max": round(float(lats[-1]), 4),
                        "lon_min": round(float(lons[0]), 4),
                        "lon_max": round(float(lons[-1]), 4),
                    },
                    "lead_hours": [int(s) // 3600 for s in seconds],
                    "variables": {
                        n: a.attrs.get("units", "")
                        for n, a in group.arrays()
                        if n not in ("lat", "lon", "time", "member")
                    },
                }
            )
            runs.append(entry["run"])
            out.append(entry)
        timer.step("read")
        body = {"models": out, "attribution": attribution(settings.models)}
        return _cached(request, body, _etag("models", *runs), timer)

    @app.get("/v1/raw/{model}/point")
    def raw_point(
        request: Request,
        model: Annotated[str, PathParam(description="Nom du modèle, ex. arome")],
        lat: Lat,
        lon: Lon,
        method: Method = "bilinear",
        variables: Annotated[
            str | None, Query(description="Liste séparée par des virgules, ex. t2m,u10")
        ] = None,
    ) -> Response:
        timer = Timer()
        run, group = open_latest(model)
        etag = _etag("raw", model, run, lat, lon, method, variables)
        if (hit := _not_modified(request, etag)) is not None:
            return hit
        wanted = [v for v in variables.split(",") if v] if variables else None
        try:
            data = read_raw(group, model, run, lat, lon, method=method, variables=wanted)
        except OutOfDomain as exc:
            raise HTTPException(422, f"point hors du domaine de {model} : {exc}") from exc
        except KeyError as exc:
            raise HTTPException(422, str(exc.args[0])) from exc
        timer.step("read")
        body = data.to_json()
        body["static"] = {k: round(v, 1) for k, v in data.static.items()}
        body["attribution"] = KNOWN_MODELS[model].attribution(run.year)
        return _cached(request, body, etag, timer)

    @app.get("/v1/forecast/point")
    def forecast_point(
        request: Request,
        lat: Lat,
        lon: Lon,
        alt: Annotated[
            float | None,
            Query(ge=-500, le=5000, description="Altitude du lieu en m (sinon MNT ou modèle)"),
        ] = None,
        model: Annotated[
            str, Query(description="Modèle déterministe : arome, arpege ou ifs")
        ] = "arome",
    ) -> Response:
        timer = Timer()
        run, group = open_latest(model)
        etag = _etag("forecast", model, run, lat, lon, alt)
        if (hit := _not_modified(request, etag)) is not None:
            return hit
        try:
            raw = read_raw(group, model, run, lat, lon)
        except OutOfDomain as exc:
            raise HTTPException(422, f"point hors du domaine de {model} : {exc}") from exc
        timer.step("read")
        forecast = derive(raw, elevation=alt, dem=settings.dem)
        timer.step("derive")
        body = forecast.to_json()
        body["attribution"] = KNOWN_MODELS[model].attribution(run.year)
        return _cached(request, body, etag, timer)

    return app


app = create_app()
