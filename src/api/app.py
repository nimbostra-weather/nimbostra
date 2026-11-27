"""Public HTTP API (FastAPI).

Routes:
- ``GET /v1/models``: available models, their current run, grid extent and variables;
- ``GET /v1/raw/{model}/point``: stored values interpolated at a point, no correction;
- ``GET /v1/forecast/point``: altitude-corrected forecast with derived quantities
  (deterministic AROME for now; the multi-model clusters come later).

Run with ``uvicorn api.app:app`` from ``src``. Paths come from ``DATA_DIR``
(environment or ``.env``). Endpoints are plain ``def``: FastAPI runs them in its
thread pool, which suits the blocking Zarr reads.

Responses carry ``Cache-Control`` and an ``ETag`` derived from the run and the
query, so Caddy or the browser can reuse them until the next run is published.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Literal

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi import Path as PathParam
from fastapi.responses import JSONResponse

from ingest.dem import open_dem
from ingest.dotenv import load_dotenv
from point.reader import ElevationSource, NoRun, OutOfDomain, RunStore, derive, read_raw

API_VERSION = "v1"
CACHE_SECONDS = 300
ATTRIBUTION = "Source : Météo-France (Licence Ouverte Etalab 2.0)"

Lat = Annotated[float, Query(ge=-90, le=90, description="Latitude en degrés décimaux")]
Lon = Annotated[float, Query(ge=-180, le=180, description="Longitude en degrés décimaux")]
Method = Annotated[Literal["bilinear", "nearest"], Query(description="Interpolation")]


@dataclass(frozen=True)
class ModelInfo:
    name: str
    label: str
    resolution: str


KNOWN_MODELS = {
    "arome": ModelInfo("arome", "AROME (Météo-France)", "0.01°"),
}


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


def _etag(*parts: object) -> str:
    digest = hashlib.sha1("|".join(map(str, parts)).encode()).hexdigest()[:20]
    return f'"{digest}"'


def _cached(request: Request, body: dict, etag: str) -> Response:
    headers = {"Cache-Control": f"public, max-age={CACHE_SECONDS}", "ETag": etag}
    if request.headers.get("if-none-match") == etag:
        return Response(status_code=304, headers=headers)
    return JSONResponse(body, headers=headers)


def create_app(settings: Settings | None = None) -> FastAPI:
    if settings is None:
        load_dotenv()
        data_dir = Path(os.environ.get("DATA_DIR", "data/zarr"))
        settings = Settings(data_dir=data_dir, dem=open_dem(data_dir))
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

    @app.get("/v1/models")
    def models(request: Request) -> Response:
        out, runs = [], []
        for name in settings.models:
            info = KNOWN_MODELS[name]
            entry: dict = {"name": name, "label": info.label, "resolution": info.resolution}
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
        body = {"models": out, "attribution": ATTRIBUTION}
        return _cached(request, body, _etag("models", *runs))

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
        run, group = open_latest(model)
        wanted = [v for v in variables.split(",") if v] if variables else None
        try:
            data = read_raw(group, model, run, lat, lon, method=method, variables=wanted)
        except OutOfDomain as exc:
            raise HTTPException(422, f"point hors du domaine de {model} : {exc}") from exc
        except KeyError as exc:
            raise HTTPException(422, str(exc.args[0])) from exc
        body = data.to_json()
        body["static"] = {k: round(v, 1) for k, v in data.static.items()}
        return _cached(request, body, _etag("raw", model, run, lat, lon, method, variables))

    @app.get("/v1/forecast/point")
    def forecast_point(
        request: Request,
        lat: Lat,
        lon: Lon,
        alt: Annotated[
            float | None,
            Query(ge=-500, le=5000, description="Altitude du lieu en m (sinon MNT ou modèle)"),
        ] = None,
        model: Annotated[str, Query(description="Modèle (AROME seul pour l'instant)")] = "arome",
    ) -> Response:
        run, group = open_latest(model)
        try:
            raw = read_raw(group, model, run, lat, lon)
        except OutOfDomain as exc:
            raise HTTPException(422, f"point hors du domaine de {model} : {exc}") from exc
        body = derive(raw, elevation=alt, dem=settings.dem).to_json()
        return _cached(request, body, _etag("forecast", model, run, lat, lon, alt))

    return app


app = create_app()
