# Ingestion ARPEGE 0,1° (API Paquets Météo-France)

ARPEGE passe par la même chaîne qu'AROME : `ingest.mf_paquets_fetch` (téléchargement)
puis `ingest.mf_paquets_convert` (Zarr), avec `--model arpege`. La lecture d'un point
et l'API sont communes (`/v1/forecast/point?model=arpege`).

## Abonnement

Sur le portail Météo-France, souscrire l'API **« Paquet ARPEGE »**
(`https://portail-api.meteofrance.fr/web/fr/api/PaquetARPEGE`) **sur la même
application** que le Paquet AROME. Les identifiants de `.env`
(`METEOFRANCE_APPLICATION_ID` ou `METEOFRANCE_API_KEY`) servent alors aux deux.

## Source

- Base : `https://public-api.meteofrance.fr/previnum/DPPaquetARPEGE/v1/models/ARPEGE/grids/0.1`.
- Catalogue : `/packages/{paquet}` puis `/packages/{paquet}?referencetime=…`.
- Téléchargement :
  `/packages/{paquet}/productARP?referencetime=2026-11-27T12:00:00Z&time=000H012H&format=grib2`.
- Grille EURAT01 : 0,1°, 20–72 N, 32 W–42 E. Recadrée à l'ingestion sur
  France + ~100 km (40,5–52 N, 6,5 W–10,5 E), soit 116 × 171 points.
- Runs 0, 6, 12 et 18 UTC, jusqu'à +102 h. Pas horaire jusqu'à +48 h, puis
  3 h pour une partie des paramètres.
- Quota : 100 requêtes par minute ; rétention 3 jours sans archive.
- Licence Ouverte Etalab 2.0, source « Météo-France ».

Sources consultées le 2026-11-27 : page Confluence Météo-France « API Paquet
Modèles » (motif générique des URL), fiche data.gouv « API Package ARPEGE »
(quota), page « Évolutions dans l'API Paquets ARPEGE » (avril 2025), jeu
data.gouv « Paquets Arpège 0,1° » (groupes d'échéances, tailles).
**Non confirmé par Météo-France** : les noms `DPPaquetARPEGE`, `productARP`,
la grille `0.1` et le format `000H012H` viennent d'une requête rapportée
comme fonctionnelle sur le forum Infoclimat (2024). `mf_paquets_fetch plan --model
arpege` les vérifie au premier essai.

## Paquets récupérés

| Paquet | Fichiers par run | Taille (run 12z du 2026-11-27) |
|--------|------------------|--------------------------------|
| SP1 | 9 : 000H012H, 013H024H, 025H036H, 037H048H, 049H060H, 061H072H, 073H084H, 085H096H, 097H102H | 671 Mo |
| SP2 | 9, mêmes groupes | 657 Mo |

Un run complet : 18 fichiers, 1,3 Go, environ 21 requêtes (18 fichiers et
3 appels au catalogue). Le contrôle toutes les 10 minutes coûte 1 à 3 appels
quand rien n'est nouveau. Chaque fichier contient toutes les échéances de son
groupe.

## Variables

Les noms GRIB ci-dessous ont été vérifiés sur le run réel du 2026-11-27 12z
(fichier 013H024H de SP1 et SP2). Pour revoir l'inventaire d'un run :

```bash
cd src
python -m ingest.mf_paquets_fetch plan --model arpege       # lecture seule
python -m ingest.mf_paquets_fetch fetch --model arpege --keep 2
python -m ingest.mf_paquets_convert inventory --model arpege
python -m ingest.mf_paquets_convert convert --model arpege --keep 2
```

`inventory` affiche, pour chaque clé GRIB, l'unité, la variable retenue
(`-` si ignorée) et les échéances trouvées dans le fichier.

| Variable | GRIB (eccodes) | Unité stockée | Remarque |
|----------|----------------|---------------|----------|
| t2m, d2m | 2t, 2d | °C | |
| r2 | 2r | % | |
| u10, v10 | 10u, 10v | m/s | |
| gust10 | max_i10fg | m/s | maximum de la vitesse sur le pas ; c'est la rafale servie |
| gust_u10, gust_v10 | max_10efg, max_10nfg | m/s | composantes, stockées pour mémoire |
| precip_total | tp | mm par pas | total toutes phases |
| rain | tirf | mm par pas | pluie |
| snow | tsnowp | mm par pas | neige (équivalent en eau) |
| lcc, mcc, hcc | lcc, mcc, hcc | % | **pas de nébulosité totale** dans les paquets, comme AROME |
| cape | CAPE_INS | J/kg | |
| sp, mslp | sp, prmsl | hPa | |
| orog | h | m | fichier de champs constants, voir ci-dessous |

À la lecture, la précipitation servie est `precip_total` ; la pluie est
`tirf` (ou, à défaut, le total moins la neige). La rafale servie est
`max_i10fg` : la norme des composantes maximisées séparément surestime le
maximum de la vitesse.

Présents mais non retenus pour l'instant : vitesse et direction du vent
(`10si`, `10wdir`, recalculées à partir de u et v), précipitations
convectives et de grande échelle (`cp`, `lsp`, `crr`, `lsrr`, `csfwe`,
`lsfwe`), neige au sol (`sd`), rayonnements et flux (`ssrd`, `strd`, `ssr`,
`str`, `slhf`, `sshf`…), `blh`, `2sh`, température de surface (`t`),
`min_2t`/`max_2t` (toutes les 3 h), type de précipitation (`ptype`, table de
code 4.201, en moyenne et maximum), visibilité minimale (`min_vis`) et
quelques champs sans nom eccodes (`d0c…`, `d2c…`).

## Orographie

Il faut l'orographie du modèle pour la correction d'altitude, et SP1/SP2 n'en
contiennent pas. Elle vient du fichier de champs constants (relief et masque
terre-mer) de la grille EURAT01, `constant-eurat01.grib2`, publié par
Météo-France (page Confluence « Modèles et données de prévision », section
ARPEGE, vérifiée le 2026-11-27) :
`https://www.data.gouv.fr/api/1/datasets/r/efd2b9d9-5e94-4090-9ea0-eac93f17e33b`.

```bash
sudo -u nimbostra mkdir -p /data/static/arpege
sudo -u nimbostra curl -fL -o /data/static/arpege/constant-eurat01.grib2 \
  'https://www.data.gouv.fr/api/1/datasets/r/efd2b9d9-5e94-4090-9ea0-eac93f17e33b'
```

Puis dans `.env` : `ARPEGE_CONSTANTS=/data/static/arpege/constant-eurat01.grib2`
(ou l'option `--constants`). Ce fichier n'est jamais committé ; l'unité
systemd le lit sans réglage de plus (`ProtectSystem=strict` laisse la lecture
possible hors du dossier personnel). Une orographie trouvée dans ce fichier
l'emporte sur toute autre. Sans orographie, la conversion échoue et l'ancien
Zarr reste en place.

## Échéances et cumuls

- L'axe `time` du Zarr réunit les échéances de toutes les variables : horaire
  jusqu'à +102 h si un paramètre l'est. Une variable absente à une échéance y
  vaut NaN (`null` dans l'API).
- Les cumuls sont convertis « par pas » : chaque valeur est le cumul depuis
  l'échéance précédente qui a une valeur. Après +48 h, une précipitation au
  pas de 3 h donne `null` aux heures intermédiaires et le cumul de 3 h à
  l'heure ronde : les totaux sont conservés.
- La même règle vaut pour AROME : un champ horaire manquant n'efface plus le
  pas suivant, qui porte alors le cumul sur 2 h.

## Stockage

- Brut : `$RAW_DIR/arpege/20261127T12Z/{SP1,SP2}/000H012H.grib2` et `manifest.json`.
- Zarr : `$DATA_DIR/arpege/20261127T12Z.zarr`, groupe `surface`, un membre,
  chunks 1 × toutes échéances × 32 × 32 points, zstd ; `latest.json` désigne
  le run à lire. Environ 8 Mo par variable avant compression.
