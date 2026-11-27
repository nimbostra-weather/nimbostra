#!/usr/bin/env python3
"""Sonde 5 : syntaxe des membres (via meteole) et plages de temps ISO (appel direct)."""
import datetime as dt
import os
import re
import sys
import tempfile
from pathlib import Path

import eccodes
import meteole
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.ingest.meteofrance_auth import BearerAuth, TokenProvider  # noqa: E402

MODELS = {
    "AromePEForecast": "https://public-api.meteofrance.fr/public/pearome/1.0/wcs/"
                       "MF-NWP-HIGHRES-PEARO000-0025-FRANCE-WCS",
    "ArpegePEForecast": "https://public-api.meteofrance.fr/public/pearpege/1.0/wcs/"
                        "MF-NWP-GLOBAL-PEARP000-01-EUROPE-WCS",
}
IND = "TEMPERATURE__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND"
AREA = ["lat(45.0,47.0)", "long(5.0,8.0)"]

captured = []
_orig_send = requests.sessions.Session.send


def _send(self, request, **kwargs):
    captured.append(request.url)
    return _orig_send(self, request, **kwargs)


requests.sessions.Session.send = _send


def load_env():
    env = Path(".env")
    if env.exists():
        for line in env.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def count_messages(content):
    n = 0
    with tempfile.NamedTemporaryFile(suffix=".grib2") as tmp:
        tmp.write(content)
        tmp.flush()
        with open(tmp.name, "rb") as f:
            while True:
                gid = eccodes.codes_grib_new_from_file(f)
                if gid is None:
                    break
                eccodes.codes_release(gid)
                n += 1
    return n


def part_a():
    print("=== A. URL des appels meteole pour le membre 1 ===")
    for cls_name in MODELS:
        client = getattr(meteole, cls_name)(application_id=os.environ["METEOFRANCE_APPLICATION_ID"])
        caps = client.get_capabilities()
        rows = caps[caps["indicator"] == IND].sort_values("run")
        cov = rows["id"].iloc[-2]
        captured.clear()
        df = client.get_coverage(coverage_id=cov, ensemble_numbers=[1], heights=[2],
                                 forecast_horizons=[dt.timedelta(hours=1)],
                                 lat=(45.0, 47.0), lon=(5.0, 8.0))
        print(f"\n{cls_name} : forme {df.shape}, membres {list(df['ensemble_number'].unique())}")
        for u in captured:
            print("  ", u)


def part_b():
    print("\n=== B. Plages de temps ISO (appel direct, AROME-PE) ===")
    base = MODELS["AromePEForecast"]
    session = requests.Session()
    session.auth = BearerAuth(TokenProvider())
    r = session.get(f"{base}/GetCapabilities",
                    params=[("service", "WCS"), ("version", "2.0.1"), ("language", "eng")], timeout=60)
    ids = sorted(set(re.findall(rf"({IND}___[0-9T.:\-]+Z)", r.text)))
    cov = ids[-2]
    run = dt.datetime.strptime(cov.split("___")[1], "%Y-%m-%dT%H.%M.%SZ")
    a = run.strftime("%Y-%m-%dT%H:%M:%SZ")
    b = (run + dt.timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    print("Couverture :", cov)
    tests = {
        "instant ISO seul": f"time({a})",
        "plage ISO sans guillemets": f"time({a},{b})",
        "plage ISO avec guillemets": f'time("{a}","{b}")',
        "plage en secondes (rappel)": "time(0,7200)",
    }
    for label, subset in tests.items():
        params = [("service", "WCS"), ("version", "2.0.1"), ("coverageid", cov),
                  ("format", "application/wmo-grib"), ("subset", "height(2)"),
                  ("subset", subset)] + [("subset", s) for s in AREA]
        resp = session.get(f"{base}/GetCoverage", params=params, timeout=120)
        if resp.status_code == 200 and resp.content[:4] == b"GRIB":
            print(f"  [{label}] OK : {count_messages(resp.content)} message(s), "
                  f"{len(resp.content) / 1e3:.0f} Ko")
        else:
            txt = re.sub(r"<[^>]+>", " ", resp.text)
            print(f"  [{label}] statut {resp.status_code} : {' '.join(txt.split())[:250]}")


def main():
    load_env()
    part_a()
    part_b()


if __name__ == "__main__":
    main()
