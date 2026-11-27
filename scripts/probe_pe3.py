#!/usr/bin/env python3
"""Sonde 3 : volumes, durées et nombre d'appels WCS pour les ensembles Météo-France."""
import os
import re
import time
from collections import Counter
from pathlib import Path

import meteole
import requests

LAT, LON = (40.5, 52.0), (-6.5, 10.5)        # France + marge
ALPES_LAT, ALPES_LON = (45.0, 47.0), (5.0, 8.0)

calls = []
_orig_send = requests.sessions.Session.send


def _send(self, request, **kwargs):
    t0 = time.time()
    resp = _orig_send(self, request, **kwargs)
    calls.append({"status": resp.status_code, "sec": time.time() - t0,
                  "headers": dict(resp.headers), "url": request.url})
    return resp


requests.sessions.Session.send = _send


def load_env():
    env = Path(".env")
    if env.exists():
        for line in env.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def measure(label, fn):
    calls.clear()
    print(f"\n--- {label}")
    t0 = time.time()
    try:
        df = fn()
    except Exception as e:
        print(f"  échec : {type(e).__name__} : {str(e)[:300]}")
        print(f"  appels HTTP avant l'échec : {len(calls)} {dict(Counter(c['status'] for c in calls))}")
        return None
    sec = time.time() - t0
    mem = df.memory_usage(deep=True).sum() / 1e6
    print(f"  durée {sec:.1f} s, forme {df.shape}, mémoire {mem:.0f} Mo")
    avg = sum(c["sec"] for c in calls) / max(len(calls), 1)
    print(f"  appels HTTP : {len(calls)} {dict(Counter(c['status'] for c in calls))}, "
          f"{avg:.2f} s en moyenne par appel")
    quota = {}
    for c in calls[-3:]:
        for k, v in c["headers"].items():
            if re.search(r"rate|quota|retry|limit", k, re.I):
                quota[k] = v
    if quota:
        print("  en-têtes de quota :", quota)
    for c in calls[:2]:
        print("  URL :", c["url"][:600])
    for col in ("latitude", "longitude", "ensemble_number", "forecast_horizon"):
        if col in df.columns:
            print(f"    {col} : {df[col].nunique()} valeurs")
    print("    colonnes :", list(df.columns))
    return df


def pick(caps, indicator, interval=None):
    rows = caps[caps["indicator"] == indicator]
    if interval is not None:
        rows = rows[rows["interval"] == interval]
    rows = rows.sort_values("run")
    return rows["id"].iloc[-2] if len(rows) > 1 else rows["id"].iloc[-1]


def run_model(cls_name, n_members):
    print(f"\n{'=' * 18} {cls_name} {'=' * 18}")
    client = getattr(meteole, cls_name)(application_id=os.environ["METEOFRANCE_APPLICATION_ID"])
    caps = client.get_capabilities()
    members = list(range(n_members))

    cov = pick(caps, "TEMPERATURE__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND")
    horizons = client.get_coverage_description(cov)["forecast_horizons"]
    measure(f"T 2 m, {n_members} membres, {len(horizons)} échéances, France entière",
            lambda: client.get_coverage(coverage_id=cov, ensemble_numbers=members,
                                        heights=[2], forecast_horizons=horizons,
                                        lat=LAT, lon=LON))

    pind = "TOTAL_PRECIPITATION__GROUND_OR_WATER_SURFACE"
    ivs = sorted(caps.loc[caps["indicator"] == pind, "interval"].astype(str).unique())
    print(f"\nIntervalles disponibles pour les précipitations : {ivs}")
    for iv in ivs[:4]:
        pcov = pick(caps, pind, interval=iv if iv != "None" else None)
        phor = client.get_coverage_description(pcov)["forecast_horizons"]
        measure(f"précipitations, intervalle {iv}, 2 membres, 6 échéances, Alpes",
                lambda: client.get_coverage(coverage_id=pcov, ensemble_numbers=[0, 1],
                                            forecast_horizons=phor[1:7],
                                            lat=ALPES_LAT, lon=ALPES_LON))


def main():
    load_env()
    run_model("ArpegePEForecast", 5)
    run_model("AromePEForecast", 2)


if __name__ == "__main__":
    main()
