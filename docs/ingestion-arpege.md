# Ingestion ARPEGE 0,1° (API Paquets Météo-France)

ARPEGE passe par la même chaîne qu'AROME : `ingest.arome_fetch` (téléchargement)
puis `ingest.arome_convert` (Zarr), avec `--model arpege`. La lecture d'un point
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
comme fonctionnelle sur le forum Infoclimat (2024). `arome_fetch plan --model
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

Les noms GRIB ci-dessous sont attendus d'après les tables eccodes et les listes
publiées pour ARPEGE 0,25°. Ils restent **à vérifier** sur un vrai run :

```bash
cd src
python -m ingest.arome_fetch plan --model arpege       # lecture seule
python -m ingest.arome_fetch fetch --model arpege --keep 2
python -m ingest.arome_convert inventory --model arpege
python -m ingest.arome_convert convert --model arpege --keep 2
```

`inventory` affiche, pour chaque clé GRIB, l'unité, la variable retenue
(`-` si ignorée) et les échéances trouvées dans le fichier.

| Variable | GRIB (eccodes) | Unité stockée | Remarque |
|----------|----------------|---------------|----------|
| t2m, d2m | 2t, 2d | °C | |
| r2 | 2r | % | |
| u10, v10 | 10u, 10v | m/s | |
| gust10 | 10fg (ou max_10fg) | m/s | maximum sur le pas |
| gust_u10, gust_v10 | 10efg, 10nfg | m/s | si présentes |
| precip_total | tp | mm par pas | total toutes phases ; `m` converti en mm |
| snow | tsnowp | mm par pas | |
| tcc, lcc, mcc, hcc | tcc, lcc, mcc, hcc | % | tcc seulement si le paquet l'a |
| cape | CAPE_INS ou cape | J/kg | |
| sp, mslp | sp, prmsl | hPa | |
| orog | h (orog ou z) | m | voir ci-dessous |

`precip_total` n'est jamais additionné aux phases : à la lecture, la pluie est
le total moins la neige.

## Orographie

Il faut l'orographie du modèle pour la correction d'altitude. Si les paquets
n'en contiennent pas (à voir avec `inventory`), télécharger une fois le fichier
de champs constants `constant-eurat01.grib2` (cité sur la page Confluence
« Modèles et données de prévision »), le ranger hors du dépôt, par exemple
dans `/data/static/arpege/`, puis indiquer son chemin dans `.env` :
`ARPEGE_CONSTANTS=/data/static/arpege/constant-eurat01.grib2` (ou l'option
`--constants`). Une orographie trouvée dans ce fichier l'emporte sur celle des
paquets. Sans orographie, la conversion échoue et l'ancien Zarr reste en place.

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
