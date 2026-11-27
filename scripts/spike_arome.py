#!/usr/bin/env python3
"""Spike M1 : AROME 0,01° (API Paquets Météo-France) -> Zarr -> lecture d'un point.

Usage :
  python scripts/spike_arome.py discover
  python scripts/spike_arome.py download --packages SP1,SP2,SP3
  python scripts/spike_arome.py convert
  python scripts/spike_arome.py bench
Source des données : Météo-France (Licence Ouverte Etalab).
"""
import argparse
import os
import re
import shutil
import sys
import time
import warnings
from datetime import datetime, timedelta
from pathlib import Path

import eccodes
import numpy as np
import requests
import xarray as xr

API = "https://public-api.meteofrance.fr/previnum/DPPaquetAROME/v1/models/AROME/grids/{grid}"
LAT_MIN, LAT_MAX = 40.5, 52.0      # France + marge d'environ 100 km
LON_MIN, LON_MAX = -6.5, 10.5


# ---------- API ----------
def load_env():
    env = Path(".env")
    if env.exists():
        for line in env.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def make_session(auth):
    key = os.environ.get("METEOFRANCE_API_KEY")
    if not key:
        sys.exit("METEOFRANCE_API_KEY absente (fichier .env ou variable d'environnement)")
    s = requests.Session()
    if auth == "bearer":
        s.headers["Authorization"] = f"Bearer {key}"
    else:
        s.headers["apikey"] = key
    return s


def get_json(s, url, **params):
    r = s.get(url, params=params or None, headers={"accept": "application/json"}, timeout=60)
    if r.status_code != 200:
        sys.exit(f"HTTP {r.status_code} sur {r.url}\n{r.text[:300]}\n"
                 "(si 401/403, essaie --auth bearer ou --auth apikey)")
    return r.json()


def to_v1(url):
    return url if "/DPPaquetAROME/v1/" in url else url.replace("/DPPaquetAROME/", "/DPPaquetAROME/v1/")


def list_packages(s, base):
    d = get_json(s, f"{base}/packages")
    out = []
    for link in d.get("links", []):
        if "/packages/" in link["href"]:
            name = link["href"].split("?")[0].rstrip("/").split("/")[-1]
            out.append((name, link.get("title", "")))
    return out


def list_runs(s, base, pkg):
    d = get_json(s, f"{base}/packages/{pkg}")
    runs = set()
    for link in d.get("links", []):
        m = re.search(r"referencetime=([0-9T:\-]+Z)", link["href"])
        if m:
            runs.add(m.group(1))
    return sorted(runs), d.get("description", "")


def list_steps(s, base, pkg, run):
    d = get_json(s, f"{base}/packages/{pkg}", referencetime=run)
    return [(l.get("time", "?"), to_v1(l["href"])) for l in d.get("links", [])
            if "product" in l["href"]]


def pick_run(s, base, pkg, min_steps):
    runs, _ = list_runs(s, base, pkg)
    for run in reversed(runs[-6:]):
        steps = list_steps(s, base, pkg, run)
        print(f"  run {run} : {len(steps)} fichiers pour {pkg}")
        if len(steps) >= min_steps:
            return run
    sys.exit("Aucun run complet trouvé (essaie --min-steps plus petit)")


def run_tag(run):
    return run[:13].replace("-", "") + "Z"          # 2026-11-26T12:00:00Z -> 20261126T12Z


def fetch(s, url, dest, retries=3):
    for attempt in range(retries):
        try:
            with s.get(url, headers={"accept": "*/*"}, stream=True, timeout=(15, 180)) as r:
                if r.status_code != 200:
                    raise RuntimeError(f"HTTP {r.status_code} : {r.text[:200]}")
                tmp = dest.with_suffix(".part")
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(1 << 20):
                        f.write(chunk)
                tmp.rename(dest)
                return dest.stat().st_size
        except Exception as e:
            print(f"  tentative {attempt + 1}/{retries} échouée : {e}")
            time.sleep(5 * (attempt + 1))
    sys.exit(f"Échec du téléchargement : {url}")


# ---------- commandes ----------
def cmd_discover(args):
    s = make_session(args.auth)
    base = API.format(grid=args.grid)
    print(f"Grille {args.grid} : paquets disponibles")
    for name, title in list_packages(s, base):
        runs, desc = list_runs(s, base, name)
        print(f"- {name} : {title}")
        print(f"    paramètres : {desc[:220]}")
        print(f"    derniers runs : {', '.join(runs[-3:])}")
    print("\nRun retenu pour SP1 :")
    pick_run(s, base, "SP1", args.min_steps)


def cmd_download(args):
    s = make_session(args.auth)
    base = API.format(grid=args.grid)
    print("Recherche du dernier run complet...")
    run = pick_run(s, base, "SP1", args.min_steps)
    root = Path(args.workdir) / run_tag(run)
    total, t_start = 0, time.time()
    for pkg in args.packages.split(","):
        steps = list_steps(s, base, pkg, run)
        print(f"{pkg} : {len(steps)} fichiers")
        for i, (tm, url) in enumerate(steps, 1):
            dest = root / pkg / f"{tm}.grib2"
            dest.parent.mkdir(parents=True, exist_ok=True)
            if dest.exists():
                total += dest.stat().st_size
                continue
            t0 = time.time()
            size = fetch(s, url, dest)
            total += size
            if i == 1 or i % 10 == 0 or i == len(steps):
                print(f"  {pkg} {i}/{len(steps)} {tm} : {size / 1e6:.0f} Mo "
                      f"en {time.time() - t0:.1f} s")
            time.sleep(0.2)
    dt = time.time() - t_start
    print(f"\nTotal : {total / 1e9:.2f} Go en {dt:.0f} s ({total / 1e6 / dt:.1f} Mo/s en moyenne)")
    print(f"Fichiers dans {root}")


def latest_dir(work):
    dirs = sorted(p for p in Path(work).iterdir() if p.is_dir() and re.match(r"\d{8}T\d{2}Z", p.name))
    if not dirs:
        sys.exit("Aucun run téléchargé (lance d'abord : download)")
    return dirs[-1]


def iter_messages(path):
    with open(path, "rb") as f:
        while True:
            gid = eccodes.codes_grib_new_from_file(f)
            if gid is None:
                break
            try:
                yield gid
            finally:
                eccodes.codes_release(gid)


def msg_key(gid):
    sn = eccodes.codes_get(gid, "shortName")
    if sn in ("unknown", ""):
        d, c, n = (eccodes.codes_get(gid, k) for k in
                   ("discipline", "parameterCategory", "parameterNumber"))
        sn = f"d{d}c{c}n{n}"
    key = f"{sn}_{eccodes.codes_get(gid, 'typeOfLevel')}_{eccodes.codes_get(gid, 'level')}"
    st = eccodes.codes_get(gid, "stepType")
    return key if st == "instant" else f"{key}_{st}"


def safe_get(gid, k):
    try:
        return eccodes.codes_get(gid, k)
    except Exception:
        return "?"


def grid_axes(gid):
    ni, nj = eccodes.codes_get(gid, "Ni"), eccodes.codes_get(gid, "Nj")
    lat0 = eccodes.codes_get(gid, "latitudeOfFirstGridPointInDegrees")
    lon0 = eccodes.codes_get(gid, "longitudeOfFirstGridPointInDegrees")
    dlat = eccodes.codes_get(gid, "jDirectionIncrementInDegrees")
    dlon = eccodes.codes_get(gid, "iDirectionIncrementInDegrees")
    north_to_south = eccodes.codes_get(gid, "jScansPositively") == 0
    lat = lat0 + (-dlat if north_to_south else dlat) * np.arange(nj)
    lon = ((lon0 + dlon * np.arange(ni) + 180) % 360) - 180
    return ni, nj, lat, lon, north_to_south


def dir_size_mb(path):
    return sum(f.stat().st_size for f in Path(path).rglob("*") if f.is_file()) / 1e6


def cmd_convert(args):
    root = latest_dir(args.workdir)
    ref = datetime.strptime(root.name, "%Y%m%dT%HZ")
    nt = args.nsteps
    print(f"Conversion du run {root.name} (échéances 0..{nt - 1})")
    files = sorted(root.glob("*/*.grib2"))
    t0 = time.time()

    arrays, meta, axes = {}, {}, None
    for path in files:
        for gid in iter_messages(path):
            if axes is None:
                ni, nj, lat, lon, n2s = grid_axes(gid)
                ilat = np.where((lat >= LAT_MIN) & (lat <= LAT_MAX))[0]
                ilon = np.where((lon >= LON_MIN) & (lon <= LON_MAX))[0]
                if n2s:
                    ilat = ilat[::-1]                 # latitudes croissantes
                axes = (ni, nj, ilat, ilon, lat[ilat], lon[ilon])
                print(f"Grille source {nj}x{ni}, recadrée en {len(ilat)}x{len(ilon)}")
            ni, nj, ilat, ilon, lat_c, lon_c = axes
            step = int(eccodes.codes_get(gid, "endStep"))
            if step >= nt:
                continue
            key = msg_key(gid)
            vals = eccodes.codes_get_values(gid).reshape(nj, ni)
            miss = eccodes.codes_get(gid, "missingValue")
            vals = vals[np.ix_(ilat, ilon)].astype("float32")
            vals[vals == miss] = np.nan
            if key not in arrays:
                arrays[key] = np.full((nt, len(ilat), len(ilon)), np.nan, dtype="float32")
                meta[key] = (safe_get(gid, "name"), safe_get(gid, "units"), path.parent.name)
            arrays[key][step] = vals
        print(f"  {path.parent.name}/{path.name} lu ({len(arrays)} variables)")
    print(f"Lecture GRIB : {time.time() - t0:.0f} s")

    times = np.array([ref + timedelta(hours=h) for h in range(nt)], dtype="datetime64[ns]")
    ds = xr.Dataset(
        {k: (("member", "time", "lat", "lon"), v[None]) for k, v in arrays.items()},
        coords={"member": [0], "time": times, "lat": axes[4], "lon": axes[5]},
        attrs={"source": "Météo-France AROME 0.01", "run": root.name},
    )

    print("\nInventaire (clé | paquet | nom | unité | échéances | min / moyenne / max) :")
    with warnings.catch_warnings(), np.errstate(all="ignore"):
        warnings.simplefilter("ignore")
        for k, a in arrays.items():
            ok = [i for i in range(nt) if not np.all(np.isnan(a[i]))]
            if not ok:
                continue
            last = a[ok[-1]]
            print(f"  {k} | {meta[k][2]} | {meta[k][0]} | {meta[k][1]} | {len(ok)} | "
                  f"{np.nanmin(last):.3g} / {np.nanmean(last):.3g} / {np.nanmax(last):.3g}")

    out = root / "arome001.zarr"
    if out.exists():
        shutil.rmtree(out)
    tile = args.tile
    chunks = (1, nt, min(tile, ds.sizes["lat"]), min(tile, ds.sizes["lon"]))
    t0 = time.time()
    ds.to_zarr(out, mode="w", encoding={v: {"chunks": chunks} for v in ds.data_vars})
    print(f"\nZarr : {dir_size_mb(out):.0f} Mo, chunks {chunks}, écrit en {time.time() - t0:.0f} s")
    print(f"GRIB source : {dir_size_mb(root) - dir_size_mb(out):.0f} Mo")


def cmd_bench(args):
    out = latest_dir(args.workdir) / "arome001.zarr"
    ds = xr.open_zarr(out, chunks=None)
    print(f"{out} : {dict(ds.sizes)}, {len(ds.data_vars)} variables")
    t_key = next((k for k in ds.data_vars if re.match(r"(2t|t)_heightAboveGround_2", k)),
                 list(ds.data_vars)[0])
    pt = ds.sel(lat=45.92, lon=6.87, method="nearest")
    print(f"{t_key} près de Chamonix :", pt[t_key].isel(member=0).values[:8].round(1))

    rng = np.random.default_rng(0)
    near, interp = [], []
    for _ in range(args.n):
        lat, lon = rng.uniform(41.0, 51.5), rng.uniform(-6.0, 10.0)
        t0 = time.perf_counter()
        ds.sel(lat=lat, lon=lon, method="nearest").load()
        near.append(time.perf_counter() - t0)
        t0 = time.perf_counter()
        win = ds.sel(lat=slice(lat - 0.03, lat + 0.03), lon=slice(lon - 0.03, lon + 0.03))
        win.interp(lat=lat, lon=lon, method="linear").load()
        interp.append(time.perf_counter() - t0)
    for label, arr in (("point le plus proche", near), ("interpolation bilinéaire", interp)):
        p50, p95 = np.percentile(arr, [50, 95]) * 1000
        print(f"Lecture, {label}, toutes variables : médiane {p50:.0f} ms, p95 {p95:.0f} ms")


def main():
    load_env()
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["discover", "download", "convert", "bench"])
    ap.add_argument("--grid", default="0.01")
    ap.add_argument("--packages", default="SP1,SP2,SP3")
    ap.add_argument("--auth", choices=["apikey", "bearer"], default="apikey")
    ap.add_argument("--min-steps", type=int, default=52)
    ap.add_argument("--nsteps", type=int, default=52)
    ap.add_argument("--tile", type=int, default=32)
    ap.add_argument("--n", type=int, default=30)
    ap.add_argument("--workdir", default="data/spike/arome")
    args = ap.parse_args()
    Path(args.workdir).mkdir(parents=True, exist_ok=True)
    {"discover": cmd_discover, "download": cmd_download,
     "convert": cmd_convert, "bench": cmd_bench}[args.cmd](args)


if __name__ == "__main__":
    main()
