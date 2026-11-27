# Ingestion AROME 0,01° (API Paquets Météo-France)

Cette page décrit les deux premières étapes de la chaîne AROME déterministe :
le téléchargement des fichiers GRIB2 bruts d'un run, puis leur conversion
en Zarr. La lecture d'un point et l'API viennent dans les étapes suivantes.

## Source

- API Paquets AROME, base
  `https://public-api.meteofrance.fr/previnum/DPPaquetAROME/v1/models/AROME/grids/0.01`.
- Catalogue : `/packages/{paquet}` liste les runs disponibles,
  `/packages/{paquet}?referencetime=…` liste un fichier par échéance.
- Téléchargement : `/packages/{paquet}/productARO?referencetime=…&time=…&format=grib2`.
- Quota : 100 requêtes par minute. Météo-France recommande de télécharger
  les paquets l'un après l'autre. Le client vise 90 requêtes par minute, en série.
- Rétention côté Météo-France : 3 jours, sans archive. Un run non récupéré
  dans ce délai est perdu.
- Licence Ouverte Etalab 2.0. Source à citer : « Météo-France ».

Sources consultées le 2026-11-27 : page Confluence Météo-France « API Paquet
Modèles », fiche data.gouv « API Paquet AROME », et le spike AROME
(`scripts/spike_arome.py`).

## Paquets récupérés

| Paquet | Fichiers par run | Contenu utile |
|--------|------------------|---------------|
| SP1 | 52 (un par heure) | u10, v10, rafales (composantes), t2m, r2 |
| SP2 | 52 | cumuls pluie, neige, graupel ; lcc, mcc, hcc ; CAPE ; sp |
| SP3 | 1 seul (le premier) | ALTITUDE (orographie du modèle) |

Un run n'est retenu que si tous ces fichiers sont publiés dans le catalogue.

## Stockage brut

```
$RAW_DIR/arome/20261127T06Z/SP1/00H.grib2
                           /SP2/...
                           /SP3/00H.grib2
                           /manifest.json
```

- Chaque fichier est d'abord écrit en `.part`, vérifié (début `GRIB`, fin `7777`)
  puis renommé. Un téléchargement interrompu reprend avec un en-tête `Range`.
- `manifest.json` n'est écrit que lorsque tous les fichiers sont présents.
  L'étape Zarr ne lira que les runs qui en ont un.
- Relancer la commande ne retélécharge rien de ce qui est déjà présent.

## Utilisation

Identifiants dans l'environnement (jamais dans le dépôt) :
`METEOFRANCE_APPLICATION_ID` (jeton renouvelé automatiquement) ou, à défaut,
`METEOFRANCE_API_KEY` (en-tête `apikey`, comme dans le spike).

```bash
cd src
python -m ingest.arome_fetch plan              # lecture seule : dernier run complet
python -m ingest.arome_fetch fetch --keep 2    # téléchargement puis rétention
```

Codes de sortie : 0 si le run est complet ou s'il n'y a rien de nouveau,
1 si le téléchargement est incomplet, 2 en cas d'erreur d'authentification.
Le déclenchement automatique (timer systemd toutes les 10 minutes) et
l'alerte en cas de run manquant viendront dans l'étape d'exploitation.

## Coût d'un run

Environ 108 requêtes : 105 fichiers et 3 à 9 appels au catalogue.
À 90 requêtes par minute, le rythme n'est pas le facteur limitant ;
le spike a mesuré 2,2 Go en 118 s pour SP1 et SP2.

## Conversion en Zarr

```bash
cd src
python -m ingest.arome_convert inventory        # clés GRIB du dernier run, et variable retenue
python -m ingest.arome_convert convert --keep 2 # conversion du dernier run complet
```

Sortie : `$DATA_DIR/arome/20261127T06Z.zarr`, publié par renommage atomique,
puis `$DATA_DIR/arome/latest.json` qui désigne le run à lire.

- Un seul groupe `surface`, dimensions `(member, time, lat, lon)` avec un
  membre unique (0), pour garder la même forme que les ensembles.
- `time` en secondes depuis le run ; `lat` et `lon` croissantes ; grille
  native 0,01° recadrée sur France + ~100 km (40,5–52 N, 6,5 W–10,5 E).
- Chunks : 1 membre × toutes les échéances × 32 × 32 points, compression zstd.
- `orog` (lat, lon) : orographie du modèle, lue dans SP3. Les autres
  paramètres de SP3 sont ignorés.

| Variable | GRIB (eccodes) | Unité stockée | Remarque |
|----------|----------------|---------------|----------|
| t2m | 2t | °C | |
| r2 | 2r | % | |
| u10, v10 | 10u, 10v | m/s | |
| gust_u10, gust_v10 | 10efg, 10nfg | m/s | maximum sur le pas, absent à l'échéance 0 |
| rain, snow, graupel | tirf, tsnowp, tgrp | mm par pas | cumuls depuis le début du run convertis par différence |
| lcc, mcc, hcc | lcc, mcc, hcc | % | |
| cape | CAPE_INS ou cape | J/kg | |
| sp, mslp | sp, prmsl | hPa | mslp seulement s'il est présent |
| orog | orog, h ou z | m | z divisé par g |

Les unités sont converties d'après la clé GRIB `units`. Une unité inattendue
arrête la conversion plutôt que de produire une valeur fausse.
Il faut au minimum t2m, u10, v10 et orog, sinon la conversion échoue et
l'ancien Zarr reste en place.

**À vérifier sur un vrai run** : les noms courts eccodes ci-dessus viennent
des tables WMO d'eccodes 2.50, pas encore d'un fichier AROME réel. La commande
`inventory` affiche, pour chaque paquet, les clés trouvées et la variable
retenue (`-` si ignorée).

Ordre de grandeur : environ 2 s d'écriture par variable, 0,4 Go par variable
au plus, et ~6 Go de mémoire pendant la conversion.
