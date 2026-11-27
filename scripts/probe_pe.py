#!/usr/bin/env python3
"""Sonde : ensembles Météo-France (AROME-PE, ARPEGE-PE) via la bibliothèque meteole."""
import inspect
import os
from pathlib import Path

import meteole


def load_env():
    env = Path(".env")
    if env.exists():
        for line in env.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def make_client(cls):
    print(f"  signature du constructeur : {inspect.signature(cls.__init__)}")
    candidates = []
    if os.environ.get("METEOFRANCE_APPLICATION_ID"):
        candidates.append(("application_id", os.environ["METEOFRANCE_APPLICATION_ID"]))
    if os.environ.get("METEOFRANCE_API_KEY"):
        candidates.append(("api_key", os.environ["METEOFRANCE_API_KEY"]))
        candidates.append(("token", os.environ["METEOFRANCE_API_KEY"]))
    if not candidates:
        raise SystemExit("Aucun identifiant dans l'environnement : renseigne "
                         "METEOFRANCE_APPLICATION_ID dans .env (lancé depuis la racine du dépôt)")
    last_error = None
    for name, value in candidates:
        try:
            client = cls(**{name: value})
            print(f"  client créé avec {name}")
            return client
        except Exception as e:
            last_error = e
            print(f"  échec avec {name} : {e}")
    raise SystemExit(f"Impossible de créer le client : {last_error}")


def main():
    load_env()
    print("Version meteole :", getattr(meteole, "__version__", "?"))
    print("Application ID présent :", bool(os.environ.get("METEOFRANCE_APPLICATION_ID")))
    classes = [n for n in dir(meteole) if n.endswith("Forecast")]
    print("Classes disponibles :", classes)

    for name in classes:
        if "PE" not in name.upper():
            continue
        print(f"\n=== {name} ===")
        try:
            client = make_client(getattr(meteole, name))
            caps = client.get_capabilities()
        except Exception as e:
            print(f"  erreur : {type(e).__name__} : {e}")
            continue
        print(f"  {len(caps)} couvertures ; colonnes : {list(caps.columns)}")
        print(caps.head(40).to_string())
        first = caps["id"].iloc[0]
        try:
            desc = client.get_coverage_description(first)
            print(f"\n  Description de {first} :")
            for k, v in desc.items():
                print(f"    {k} : {str(v)[:300]}")
        except Exception as e:
            print(f"  description impossible : {type(e).__name__} : {e}")


if __name__ == "__main__":
    main()
