# Nimbostra : contexte pour Claude

## Projet
Site météo français (nimbostra.fr) : API de données brutes par modèle,
météogrammes par point (coordonnées), avec scénario le plus probable
issu d'un clustering multi-modèles. Open source, non commercial.

## Stack
Python 3.12+ (FastAPI, xarray, zarr, eccodes/cfgrib), Postgres, Redis,
Caddy. Front en JS vanilla (ES6, sans bundler). Serveur Debian 13.

## Décisions
- Zone : France + ~100 km. Horizon : J+5 à J+7.
- Phase 1 : AROME(+ENS), ARPEGE(+PEARP), IFS(+ENS), ICON-EU(+EPS).
  Phase 2 : AIFS, ICON-D2, GEFS. Phase 3 : ICON-CH.
- AROME en 0,01° ; runs 0/3/6/12/18z ; AROME-ENS 3/9/15/21z.
- Stockage : un Zarr par modèle/run, deux groupes `surface`
  (member,time,lat,lon) et `pressure` (member,time,level,lat,lon).
  Grille native conservée, interpolation au point à la lecture.
  Chunks : ensembles tous membres+toutes échéances sur ~8x8 points.
  AROME déterministe : chunks 1 membre × toutes échéances × 32x32 points, zstd.
- Cumuls convertis en « par pas » et unités normalisées à l'ingestion.
- Correction d'altitude à la lecture (MNT vs orographie du modèle).
- Clusters : à la demande, par point, avec cache ; distance sur
  t2m, précipitations (cumul 3 h, transformées), vent (u,v), nébulosité
  totale (`tcc`) et basse (`lcc`), toutes deux avec leur poids plein.
  Humidité, pression, rafales : stockées mais hors distance.
- Poids : un poids par modèle (config/weights.yml), poids par membre
  = poids_modèle / nombre_de_membres, avec décroissance en fin de portée.

## Conventions
Code et commentaires en anglais, documentation en français.
Tests avec pytest, lint avec ruff. Pas de secret dans le dépôt.

## À ne pas faire
Ne pas lire de GRIB à la requête. Ne pas committer de données
(GRIB, Zarr). Ne pas toucher à la production sans demande explicite.
