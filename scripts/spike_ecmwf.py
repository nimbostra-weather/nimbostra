#!/usr/bin/env python3
"""Spike M1 : ECMWF open data -> Zarr -> lecture d'un point.

Usage :
  python scripts/spike_ecmwf.py --mode hres --max-step 72
  python scripts/spike_ecmwf.py --mode ens --max-step 24 --members 10
"""
import argparse
import shutil
import time
from pathlib import Path

import numpy as np
import xarray as xr
from ecmwf.opendata import Client

LAT_MIN, LAT_MAX = 40.5, 52.0      # France + marge d'environ 100 km
LON_MIN, LON_MAX = -6.5, 10.5
PARAMS = ["2t", "10u", "10v", "msl", "tp"]
NAMES = {"2t": "t2m", "10u": "u10", "10v": "v10", "msl": "msl", "tp": "tp"}


def download(mode, max_step, members, target):
    if target.exists():
        print(f"GRIB déjà présent, téléchargement ignoré : {target}")
        return
    steps = list(range(0, max_step + 1, 3))
    client = Client(source="ecmwf")  # sans date ni heure : dernier run disponible
    if mode == "hres":
        req = dict(stream="oper", type="fc", step=steps, param=PARAMS)
    else:
        req = dict(stream="enfo", type="pf", step=steps, param=PARAMS,
                   number=list(range(1, members + 1)))
    t0 = time.time()
    client.retrieve(target=str(target), **req)
    mb = target.stat().st_size / 1e6
    dt = time.time() - t0
    print(f"Téléchargement : {mb:.0f} Mo en {dt:.0f} s ({mb / dt:.1f} Mo/s)")


def load_param(grib, short):
    ds = xr.open_dataset(
        grib,
        engine="cfgrib",
        backend_kwargs={"filter_by_keys": {"shortName": short}, "indexpath": ""},
    )
    return ds[list(ds.data_vars)[0]]


def crop(da):
    # longitudes 0..360 -> -180..180, puis recadrage sur la France
    da = da.assign_coords(longitude=((da.longitude + 180) % 360) - 180)
    da = da.sortby("longitude")
    return da.sel(latitude=slice(LAT_MAX, LAT_MIN), longitude=slice(LON_MIN, LON_MAX))


def clean(da):
    """Renvoie un tableau aux dimensions (member, time, lat, lon)."""
    valid = da.valid_time.values
    da = da.drop_vars([c for c in da.coords if c not in da.dims])
    if "number" in da.dims:
        da = da.rename(number="member")
    else:
        da = da.expand_dims(member=[0])  # déterministe : un seul membre
    da = da.rename(step="time", latitude="lat", longitude="lon")
    da = da.assign_coords(time=valid).sortby("lat")
    return da.transpose("member", "time", "lat", "lon")


def build_dataset(grib):
    arrays, run_time = {}, None
    for short, name in NAMES.items():
        raw = load_param(grib, short)
        run_time = run_time or str(raw.time.values)[:13]
        arrays[name] = clean(crop(raw).load())

    arrays["t2m"] = arrays["t2m"] - 273.15           # K -> °C
    arrays["msl"] = arrays["msl"] / 100.0            # Pa -> hPa
    tp = arrays["tp"]                                # m cumulés depuis le début du run
    tp = (tp.diff("time", label="upper") * 1000.0).clip(min=0)
    arrays["tp"] = tp.reindex(time=arrays["tp"].time, fill_value=0.0)  # mm par pas

    ds = xr.Dataset(arrays).astype("float32")
    ds.attrs.update(source="ecmwf-opendata", run=run_time)
    return ds


def dir_size_mb(path):
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file()) / 1e6


def write_zarr(ds, path, tile):
    if path.exists():
        shutil.rmtree(path)
    chunks = (ds.sizes["member"], ds.sizes["time"], tile, tile)
    encoding = {v: {"chunks": chunks} for v in ds.data_vars}
    t0 = time.time()
    ds.to_zarr(path, mode="w", encoding=encoding)
    print(f"Zarr : {dir_size_mb(path):.0f} Mo, chunks {chunks}, "
          f"écrit en {time.time() - t0:.1f} s")


def bench(path, n=30):
    ds = xr.open_zarr(path, chunks=None)   # lecture paresseuse, sans dask

    pt = ds.sel(lat=45.92, lon=6.87, method="nearest")   # point de contrôle (Chamonix)
    print("t2m, membre 0, premières échéances :",
          pt["t2m"].isel(member=0).values[:8].round(1))

    rng = np.random.default_rng(0)
    lats, lons = ds.lat.values, ds.lon.values
    near, interp = [], []
    for _ in range(n):
        lat = rng.uniform(lats.min() + 0.5, lats.max() - 0.5)
        lon = rng.uniform(lons.min() + 0.5, lons.max() - 0.5)

        t0 = time.perf_counter()
        ds.sel(lat=lat, lon=lon, method="nearest").load()
        near.append(time.perf_counter() - t0)

        t0 = time.perf_counter()
        win = ds.sel(lat=slice(lat - 0.5, lat + 0.5), lon=slice(lon - 0.5, lon + 0.5))
        win.interp(lat=lat, lon=lon, method="linear").load()
        interp.append(time.perf_counter() - t0)

    for label, arr in (("point le plus proche", near), ("interpolation bilinéaire", interp)):
        p50, p95 = np.percentile(arr, [50, 95]) * 1000
        print(f"Lecture, {label} : médiane {p50:.0f} ms, p95 {p95:.0f} ms ({n} points)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["hres", "ens"], default="hres")
    ap.add_argument("--max-step", type=int, default=72)
    ap.add_argument("--members", type=int, default=10)
    ap.add_argument("--tile", type=int, default=8)
    ap.add_argument("--workdir", default="data/spike")
    args = ap.parse_args()

    work = Path(args.workdir)
    work.mkdir(parents=True, exist_ok=True)
    grib = work / f"ecmwf_{args.mode}.grib2"
    zarr_path = work / f"ecmwf_{args.mode}.zarr"

    download(args.mode, args.max_step, args.members, grib)

    t0 = time.time()
    ds = build_dataset(grib)
    print(f"Conversion en mémoire : {time.time() - t0:.1f} s")
    print(ds)

    write_zarr(ds, zarr_path, args.tile)
    bench(zarr_path)


if __name__ == "__main__":
    main()
