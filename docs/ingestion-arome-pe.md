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
`GetCapabilities`. Le module garde, pour chaque variable, une liste de
candidats et retient le premier présent pour le run. Seul
`TEMPERATURE__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND` a été vu en vrai.
Une variable facultative introuvable est ignorée et notée dans l'attribut
`unresolved` du Zarr ; une variable obligatoire introuvable arrête tout.

| Nom | Indicateur candidat | Unité stockée | Obligatoire |
|-----|---------------------|---------------|-------------|
| t2m | TEMPERATURE__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND (2 m) | °C | oui |
| r2 | RELATIVE_HUMIDITY__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND (2 m) | % | non |
| u10, v10 | U/V_COMPONENT_OF_WIND__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND (10 m) | m/s | oui |
| gust10 | WIND_SPEED_GUST__… (10 m), intervalle PT1H de préférence | m/s | non |
| precip | TOTAL_PRECIPITATION__GROUND_OR_WATER_SURFACE, PT1H de préférence | mm par pas | oui |
| snow | TOTAL_SNOW_PRECIPITATION__GROUND_OR_WATER_SURFACE | mm par pas | non |
| lcc | LOW_CLOUD_COVER__GROUND_OR_WATER_SURFACE | % | oui |
| mslp | PRESSURE__MEAN_SEA_LEVEL | hPa | non |
| orog | GEOMETRIC_HEIGHT__GROUND_OR_WATER_SURFACE (un seul appel, membre 0) | m | non |

- AROME-PE ne fournit que la nébulosité basse (`lcc`), pas la totale (`tcc`).
  Les clusters utilisent les deux (décision du 2026-11-27) : la basse est
  importante et mal vue par certains modèles. Pour AROME-PE, `tcc` manque ;
  la façon de traiter ce manque dans la distance reste à décider à l'étape
  des clusters.
- Cumuls : avec l'intervalle PT1H, la valeur est déjà « par pas ». Sans
  intervalle, le cumul depuis le début du run est converti par différence.
  Pas de champ à l'échéance 0 pour les cumuls et les rafales.
- Unités converties d'après la clé GRIB `units` (K → °C, Pa → hPa,
  fraction → %, kg/m² → mm). Une unité inconnue arrête la conversion.

## Coût d'un run

| Run | Appels |
|-----|--------|
| 09z, 21z (51 h) | 11 626 champs + 2 métadonnées + 2 sondes |
| 03z, 15z (45 h) | 10 276 champs + 4 |

Soit environ 39 minutes à 5 requêtes par seconde (réglage par défaut),
20 minutes à 10 par seconde. Le budget par défaut est de 13 000 appels :
au-delà, rien n'est lancé.

## Garde-fous

- Deux sondes (t2m à la dernière échéance, membres 0 et 24) avant un
  run complet : si elles renvoient 404, le run n'est pas encore publié et
  rien d'autre n'est demandé.
- Téléchargement membre par membre : si plus de 20 % des champs d'un
  membre manquent, l'ingestion s'arrête au lieu de brûler le quota.
- Arrêt immédiat sur erreur d'authentification.
- Une 429 met en pause tous les fils (client WCS).

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
échéances × 8 × 8 points ; `orog` en `lat, lon`), puis `latest.json`.
Mémoire : environ 1,6 Go par variable pendant la conversion.
