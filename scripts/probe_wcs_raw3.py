#!/usr/bin/env python3
"""Sonde 6 : (A) GetCoverage sans axe temps ; (B) tolérance au parallélisme (429)."""
import os
import re
import sys
import tempfile
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import eccodes
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.ingest.meteofrance_auth import BearerAuth, TokenProvider  # noqa: E402

ROOT = "https://public-api.meteofrance.fr/public"
MODELS = {
    "AROME-PE": (ROOT + "/pearome/1.0/wcs/MF-NWP-HIGHRES-PEARO{m:03d}-0025-FRANCE-WCS", 25),
    "ARPEGE-PE": (ROOT + "/pearpege/1.0/wcs/MF-NWP-GLOBAL-PEARP{m:03d}-01-EUROPE-WCS", 35),
}
IND = "TEMPERATURE__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND"
AREA = ["lat(45.0,47.0)", "long(5.0,8.0)"]

SESSION = requests.Session()


def load_env():
    env = Path(".env")
    if env.exists():
        for line in env.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def fetch(url, params, retries=1):
    """GET avec nouvelle tentative sur 429 et erreurs serveur."""
    r = None
    for attempt in range(retries):
        r = SESSION.get(url, params=params, timeout=120)
        if r.status_code not in (429, 500, 502, 503, 504):
            return r
        try:
            wait = int(r.headers.get("Retry-After", ""))
        except ValueError:
            wait = 10 * (attempt + 1)
        print(f"  HTTP {r.status_code}, nouvelle tentative dans {wait} s")
        time.sleep(wait)
    return r


def get_coverage(base, cov, subsets, retry=False):
    params = [("service", "WCS"), ("version", "2.0.1"), ("coverageid", cov),
              ("format", "application/wmo-grib")] + [("subset", s) for s in subsets]
    url = f"{base}/GetCoverage"
    return fetch(url, params) if retry else SESSION.get(url, params=params, timeout=120)


def find_coverage(base0):
    r = fetch(f"{base0}/GetCapabilities",
              [("service", "WCS"), ("version", "2.0.1"), ("language", "eng")])
    ids = sorted(set(re.findall(rf"({re.escape(IND)}___[0-9T.:\-]+Z)", r.text)))
    if not ids:
        sample = sorted(set(re.findall(r"CoverageId>([^<]+)<", r.text)))
        print(f"  Aucune couverture {IND} : HTTP {r.status_code}, {len(r.text)} caractères")
        print(f"  début de la réponse : {' '.join(r.text[:1000].split())}")
        print(f"  exemples d'identifiants : {sample[:5]}")
        return None
    return ids[-2] if len(ids) > 1 else ids[-1]


def grib_steps(content):
    steps = []
    with tempfile.NamedTemporaryFile(suffix=".grib2") as tmp:
        tmp.write(content)
        tmp.flush()
        with open(tmp.name, "rb") as f:
            while True:
                gid = eccodes.codes_grib_new_from_file(f)
                if gid is None:
                    break
                steps.append(eccodes.codes_get(gid, "endStep"))
                eccodes.codes_release(gid)
    return steps


def part_a(name, base0, cov):
    print(f"\n[A] {name} : membre 0 sans axe temps")
    t0 = time.time()
    r = get_coverage(base0, cov, ["height(2)"] + AREA, retry=True)
    sec = time.time() - t0
    if r.status_code == 200 and r.content[:4] == b"GRIB":
        steps = grib_steps(r.content)
        print(f"  OK {sec:.1f} s, {len(r.content) / 1e3:.0f} Ko, {len(steps)} message(s), "
              f"échéances : {steps[:12]}{' ...' if len(steps) > 12 else ''}")
    else:
        txt = " ".join(re.sub(r"<[^>]+>", " ", r.text).split())
        print(f"  statut {r.status_code} : {txt[:250]}")


def one_call(args):
    tpl, n_members, cov, i = args
    base = tpl.format(m=i % n_members)
    t0 = time.time()
    try:
        r = get_coverage(base, cov, ["height(2)", f"time({(i % 20) * 3600})"] + AREA)
        return r.status_code, time.time() - t0, r.headers.get("Retry-After")
    except Exception as e:
        return type(e).__name__, time.time() - t0, None


def part_b(name, tpl, n_members, cov, n=100):
    print(f"\n[B] {name} : {n} requêtes par palier, membres et échéances variés")
    for workers in (4, 8, 16):
        t0 = time.time()
        with ThreadPoolExecutor(workers) as pool:
            res = list(pool.map(one_call, [(tpl, n_members, cov, i) for i in range(n)]))
        total = time.time() - t0
        codes = Counter(str(r[0]) for r in res)
        lat = sum(r[1] for r in res) / len(res)
        retry = {r[2] for r in res if r[2]}
        print(f"  {workers:2d} travailleurs : {dict(codes)} en {total:.1f} s "
              f"({n / total:.1f} req/s, latence moyenne {lat:.2f} s, Retry-After {retry or '-'})")
        time.sleep(65)


def main():
    load_env()
    SESSION.auth = BearerAuth(TokenProvider())
    for name, (tpl, n_members) in MODELS.items():
        print(f"\n{'=' * 18} {name}")
        base0 = tpl.format(m=0)
        cov = find_coverage(base0)
        if cov is None:
            continue
        print(f"Couverture : {cov}")
        part_a(name, base0, cov)
        part_b(name, tpl, n_members, cov)


if __name__ == "__main__":
    main()
