#!/usr/bin/env python3
"""Sonde 4 : appels WCS bruts (sans meteole) pour réduire le nombre de requêtes."""
import os
import re
import sys
import tempfile
import time
from pathlib import Path

import eccodes
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.ingest.meteofrance_auth import BearerAuth, TokenProvider  # noqa: E402

MODELS = {
    "AROME-PE": "https://public-api.meteofrance.fr/public/pearome/1.0/wcs/"
                "MF-NWP-HIGHRES-PEARO000-0025-FRANCE-WCS",
    "ARPEGE-PE": "https://public-api.meteofrance.fr/public/pearpege/1.0/wcs/"
                 "MF-NWP-GLOBAL-PEARP000-01-EUROPE-WCS",
}
INDICATOR = "TEMPERATURE__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND___"
AREA = ["lat(45.0,47.0)", "long(5.0,8.0)"]


def load_env():
    env = Path(".env")
    if env.exists():
        for line in env.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


SESSION = requests.Session()


def get(url, params):
    t0 = time.time()
    r = SESSION.get(url, params=params, timeout=120)
    return r, time.time() - t0


def wcs_get_coverage(base, cov, subsets):
    params = [("service", "WCS"), ("version", "2.0.1"), ("coverageid", cov),
              ("format", "application/wmo-grib")] + [("subset", s) for s in subsets]
    return get(f"{base}/GetCoverage", params)


def grib_messages(content):
    out = []
    with tempfile.NamedTemporaryFile(suffix=".grib2") as tmp:
        tmp.write(content)
        tmp.flush()
        with open(tmp.name, "rb") as f:
            while True:
                gid = eccodes.codes_grib_new_from_file(f)
                if gid is None:
                    break
                try:
                    info = {}
                    for k in ("shortName", "perturbationNumber", "endStep", "Ni", "Nj"):
                        try:
                            info[k] = eccodes.codes_get(gid, k)
                        except Exception:
                            info[k] = "?"
                    out.append(info)
                finally:
                    eccodes.codes_release(gid)
    return out


def report(label, resp, sec):
    print(f"\n  [{label}] statut {resp.status_code}, {sec:.2f} s, {len(resp.content) / 1e3:.0f} Ko")
    if resp.status_code != 200 or resp.content[:4] != b"GRIB":
        print("   réponse :", resp.text[:1800].replace("\n", " "))
        return
    msgs = grib_messages(resp.content)
    print(f"   {len(msgs)} message(s) GRIB ; premiers : {msgs[:4]}")


def probe(name, base):
    print(f"\n{'=' * 20} {name} {'=' * 20}")
    r, _ = get(f"{base}/GetCapabilities",
               [("service", "WCS"), ("version", "2.0.1"), ("language", "eng")])
    ids = sorted(set(re.findall(rf"({INDICATOR}[0-9T.:\-]+Z)", r.text)))
    if not ids:
        print("Aucune couverture T2m trouvée :", r.status_code, r.text[:500])
        return
    cov = ids[-2] if len(ids) > 1 else ids[-1]
    print("Couverture :", cov)

    r, _ = get(f"{base}/DescribeCoverage",
               [("service", "WCS"), ("version", "2.0.1"), ("coverageid", cov)])
    xml = r.text
    Path("data").mkdir(exist_ok=True)
    Path(f"data/describe_{name}.xml").write_text(xml)
    print(f"DescribeCoverage : {len(xml)} caractères (copie dans data/describe_{name}.xml)")
    print("  axes :", re.findall(r"axisLabels>([^<]+)<", xml)[:3])
    for line in xml.splitlines():
        if re.search(r"number|ensemble|member|perturb", line, re.I):
            print("  >", line.strip()[:200])

    h = ["height(2)"]
    resp, sec = wcs_get_coverage(base, cov, h + ["time(1)"] + AREA)
    report("instant invalide pour lister les horaires valides", resp, sec)

    resp, sec = wcs_get_coverage(base, cov, h + ["time(0)"] + AREA)
    report("un instant (référence)", resp, sec)

    resp, sec = wcs_get_coverage(base, cov, h + ["time(0,7200)"] + AREA)
    report("plage de temps 0 à 2 h", resp, sec)

    for label in ("number", "ensemble", "member", "ensemble_number"):
        resp, sec = wcs_get_coverage(base, cov, h + ["time(0)", f"{label}(1)"] + AREA)
        report(f"membre via {label}(1)", resp, sec)
        if resp.status_code == 200 and resp.content[:4] == b"GRIB":
            resp, sec = wcs_get_coverage(base, cov, h + ["time(0)", f"{label}(0,2)"] + AREA)
            report(f"plage de membres via {label}(0,2)", resp, sec)
            break


def main():
    load_env()
    SESSION.auth = BearerAuth(TokenProvider())
    for name, base in MODELS.items():
        probe(name, base)


if __name__ == "__main__":
    main()
