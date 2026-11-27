# Ingestion AROME-PE (services WCS Météo-France)

Module : `src/ingest/arome_pe.py`, au-dessus du client WCS
(`meteofrance_wcs.py`) et de l'écrivain Zarr (`zarr_writer.py`).

## Source

- API « AROME Prévision d'Ensemble » du portail Météo-France,
  un service WCS par membre : `MF-NWP-HIGHRES-PEARO000-0025-FRANCE-WCS`
  à `...PEARO024...`, grille 0,025°.
- Quota affiché sur data.gouv : 900 requêtes par minute.
- 25 membres (1 contrôle + 24 perturbés), runs 03, 09, 15 et 21 UTC.
  Échéance maximale 51 h pour 09z et 21z, 45 h pour 03z et 15z (page
  PE-AROME de Meteociel). Le module lit la vraie liste d'échéances par
  `DescribeCoverage` ; ces valeurs ne servent qu'à défaut.
- Seuls les appels unitaires fonctionnent (un membre, une échéance, une
  variable) ; le découpage lat/long est fait côté serveur.

Sources consultées le 2026-11-27 : fiche data.gouv « API Modèle AROME
Prévision d'ensemble », page PE-AROME de Meteociel, code de meteole 0.2.8.

## Variables

Les noms d'indicateurs WCS ne sont pas publiés en dehors de
`GetCapabilities`. Les noms ci-dessous ont été vérifiés sur la liste
renvoyée le 2026-11-27 (run 09z). Le module retient, pour chaque variable,
le premier candidat présent pour le run.
Une variable facultative introuvable est ignorée et notée dans l'attribut
`unresolved` du Zarr ; une variable obligatoire introuvable arrête tout.

| Nom | Indicateur candidat | Unité stockée | Obligatoire |
|-----|---------------------|---------------|-------------|
| t2m | TEMPERATURE__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND (2 m) | °C | oui |
| r2 | RELATIVE_HUMIDITY__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND (2 m) | % | non |
| u10, v10 | U/V_COMPONENT_OF_WIND__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND (10 m) | m/s | oui |
| gust_u10, gust_v10 | U/V_COMPONENT_OF_WIND_GUST__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND (10 m) | m/s | non |
| precip | TOTAL_PRECIPITATION__GROUND_OR_WATER_SURFACE, PT1H de préférence | mm par pas | oui |
| snow | TOTAL_SNOW_PRECIPITATION__GROUND_OR_WATER_SURFACE | mm par pas | non |
| lcc | LOW_CLOUD_COVER__GROUND_OR_WATER_SURFACE | % | oui |
| mslp | PRESSURE__MEAN_SEA_LEVEL | hPa | non |

- AROME-PE ne fournit que la nébulosité basse (`lcc`), pas la totale (`tcc`).
  Les clusters utilisent les deux (décision du 2026-11-27) : la basse est
  importante et mal vue par certains modèles. Pour AROME-PE, `tcc` manque ;
  la façon de traiter ce manque dans la distance reste à décider à l'étape
  des clusters.
- Cumuls : avec l'intervalle PT1H, la valeur est déjà « par pas ». Sans
  intervalle, le cumul depuis le début du run est converti par différence.
  Pas de champ à l'échéance 0 pour les cumuls, les rafales et `lcc`
  (404 constaté le 2026-11-27) : ils ne sont pas demandés.
- Unités converties d'après la clé GRIB `units` (K → °C, Pa → hPa,
  fraction → %, kg/m² → mm). Une unité inconnue arrête la conversion.
- Rafales : seulement en composantes U et V, comme AROME déterministe ;
  la vitesse se calcule à la lecture.
- Orographie : **aucun indicateur** dans le WCS AROME-PE (le géopotentiel
  n'existe qu'en niveaux pression). Elle vient du fichier des champs
  constants de la grille, voir ci-dessous.

## Orographie et masque terre-mer

Source : `constant-eurw1s40.grib2`, fichier Météo-France des champs
constants de la grille EURW1S40 (0,025°, 1121 × 717 points, 37,5–55,4 N,
12 W–16 E), publié dans la documentation de l'API sur le Confluence
de Météo-France et fourni par Lionel le 2026-11-27. Licence Ouverte
Etalab 2.0, mention « Météo-France ».

- Deux messages : `h` (hauteur géométrique, m) et `lsm` (masque
  terre-mer, fraction, stocké en %).
- Recadré sur France + ~100 km, il donne exactement la grille du WCS
  (461 × 681 points, 40,5–52 N, 6,5 W–10,5 E) ; la conversion refuse un
  champ dont la grille diffère.
- Valeurs vérifiées : 4 119 m au plus haut (massif du Mont-Blanc),
  48 m à Paris, 0 en mer, -133 m au plus bas, vers 50,9 N 6,5 E, ce qui
  correspond aux mines de lignite à ciel ouvert de Rhénanie.
- C'est le relief de la grille, commun aux produits 0,025° de Météo-France.
  Il est peut-être un peu plus fin que celui qu'utilise réellement
  AROME-PE à 2,5 km ; pour la correction d'altitude, l'écart reste faible
  devant celui du MNT.

Emplacement sur le serveur (jamais dans le dépôt) :
`$DATA_DIR/static/arome-pe/constant-eurw1s40.grib2`, ou le chemin donné
par `AROME_PE_CONSTANTS` ou `--constants`. Sans ce fichier, la conversion
fonctionne mais le Zarr n'a ni `orog` ni `lsm`.

## Coût d'un run

| Run | Appels |
|-----|--------|
| 09z, 21z (51 h) | 12 875 champs + 0 à 2 métadonnées + 2 sondes |
| 03z, 15z (45 h) | 11 375 champs + 0 à 4 |

Soit environ 43 minutes à 5 requêtes par seconde (réglage par défaut),
22 minutes à 10 par seconde. Les rafales en composantes coûtent à elles
seules 2 550 appels par run de 51 h. Le budget par défaut est de 14 000 appels :
au-delà, rien n'est lancé.

## Garde-fous

- Deux sondes (t2m à la dernière échéance, membres 0 et 24) avant un
  run complet : si elles renvoient 404, le run n'est pas encore publié et
  rien d'autre n'est demandé.
- Téléchargement membre par membre : si plus de 20 % des champs d'un
  membre manquent, l'ingestion s'arrête au lieu de brûler le quota.
- Arrêt immédiat sur erreur d'authentification.
- Une 429 met en pause tous les fils (client WCS).

## Appels de métadonnées

`GetCapabilities` et `DescribeCoverage` sont limités à part, et bien plus
tôt que `GetCoverage` : le 2026-11-27, 8 refus 429 de suite en 35 s alors que
les champs passaient sans problème. D'où :

- un cache disque dans le dossier brut : `capabilities.xml`, réutilisé
  pendant 5 minutes, ou jusqu'à 6 h si l'appel échoue ; et `time_axes.json`,
  qui garde les échéances par heure de run (03, 09, 15, 21 UTC), demandées
  une seule fois ;
- sur un 429, des attentes croissantes de 10, 20, 40, 80 puis 120 s ;
- en cas d'échec, les échéances par défaut, et un message clair avec le
  code de sortie 1 au lieu d'une trace Python.

## Reprise

```
$RAW_DIR/arome-pe/20261127T09Z/resolution.json
                              /t2m/m000/0003600.grib2
                              /precip/m000/...
```

Relancer `fetch` ne redemande ni `GetCapabilities` ni un champ déjà
présent. Un champ en 404 est redemandé à la relance suivante.

## Utilisation

```bash
cd src
python -m ingest.arome_pe plan --offline            # aucun appel
python -m ingest.arome_pe check                     # 3 appels
python -m ingest.arome_pe fetch --members 0 --max-lead 2   # essai réduit
python -m ingest.arome_pe fetch                     # run complet
python -m ingest.arome_pe convert --keep 2
```

Sortie : `$DATA_DIR/arome-pe/20261127T09Z.zarr` (groupe `surface`,
dimensions `member, time, lat, lon`, chunks tous membres × toutes
échéances × 8 × 8 points ; `orog` et `lsm` en `lat, lon`), puis `latest.json`.
Mémoire : environ 1,6 Go par variable pendant la conversion.
