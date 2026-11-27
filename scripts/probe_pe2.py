#!/usr/bin/env python3
"""Sonde 2 : variables, membres et téléchargement test des ensembles Météo-France."""
import datetime as dt
import inspect
import os
import re
import time
from pathlib import Path

import meteole

KEYWORDS = re.compile(
    r"TEMPERATURE|WIND|GUST|PRECIP|SNOW|RAIN|CLOUD|HUMIDITY|PRESSURE|ISOTHERMAL|HEIGHT",
    re.I)


def load_env():
    env = Path(".env")
    if env.exists():
        for line in env.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def show_result(res, seconds):
    print(f"    OK en {seconds:.1f} s, type {type(res).__name__}")
    if hasattr(res, "shape"):
        print(f"    forme : {res.shape}")
    if hasattr(res, "columns"):
        print(f"    colonnes : {list(res.columns)}")
        for c in res.columns:
            try:
                print(f"      {c} : {res[c].nunique()} valeurs distinctes, ex. {list(res[c].unique()[:4])}")
            except Exception:
                pass
        print(res.head(8).to_string())


def try_download(client, cov, desc):
    horizons = [dt.timedelta(hours=h) for h in (0, 3, 6)]
    heights = list(desc.get("heights") or [])[:1]
    base = {"coverage_id": cov, "forecast_horizons": horizons}
    if heights:
        base["heights"] = heights
    variants = [
        ("2 membres + zone Alpes", {**base, "ensemble_numbers": range(2),
                                    "lat": (45.0, 47.0), "lon": (5.0, 8.0)}),
        ("2 membres sans zone", {**base, "ensemble_numbers": range(2)}),
        ("sans membres + zone", {**base, "lat": (45.0, 47.0), "lon": (5.0, 8.0)}),
    ]
    for label, kwargs in variants:
        print(f"\n  Essai : {label}")
        t0 = time.time()
        try:
            res = client.get_coverage(**kwargs)
        except Exception as e:
            print(f"    échec : {type(e).__name__} : {str(e)[:300]}")
            continue
        show_result(res, time.time() - t0)
        return


def probe(cls_name):
    print(f"\n{'=' * 20} {cls_name} {'=' * 20}")
    client = getattr(meteole, cls_name)(application_id=os.environ["METEOFRANCE_APPLICATION_ID"])
    caps = client.get_capabilities()
    runs = sorted(caps["run"].unique())
    print(f"{len(caps)} couvertures, {len(runs)} runs : {runs[0]} ... {runs[-1]}")

    inds = caps.drop_duplicates("indicator")[["indicator", "title"]].sort_values("indicator")
    kept = inds[inds["indicator"].str.contains(KEYWORDS)]
    print(f"{len(inds)} indicateurs, dont {len(kept)} pertinents :")
    for _, r in kept.iterrows():
        print(f"  {r['indicator']} | {r['title']}")

    temp = [i for i in inds["indicator"] if i.startswith("TEMPERATURE__SPECIFIC_HEIGHT")]
    ind = temp[0] if temp else kept["indicator"].iloc[0]
    rows = caps[caps["indicator"] == ind].sort_values("run")
    cov = rows["id"].iloc[-2] if len(rows) > 1 else rows["id"].iloc[-1]
    print(f"\nCouverture de test : {cov}")

    desc = client.get_coverage_description(cov)
    for k, v in desc.items():
        txt = f"{len(v)} valeurs, de {v[0]} à {v[-1]}" if isinstance(v, list) and v else str(v)
        print(f"  {k} : {txt}")

    print("\nSignature de get_coverage :", inspect.signature(client.get_coverage))
    doc = inspect.getdoc(client.get_coverage) or ""
    print(doc[:1500])

    try_download(client, cov, desc)


def main():
    load_env()
    for name in ("AromePEForecast", "ArpegePEForecast"):
        try:
            probe(name)
        except Exception as e:
            print(f"{name} : {type(e).__name__} : {e}")


if __name__ == "__main__":
    main()
