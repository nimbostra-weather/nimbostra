# Ingestion IFS 0,1° (ECMWF open data)

IFS déterministe passe par `ingest.ecmwf_fetch` (téléchargement par requêtes
Range) puis par la conversion commune `ingest.mf_paquets_convert --model ifs`
(Zarr). Le dossier de staging a le même format que celui de l'API Paquets.
La lecture d'un point et l'API sont communes (`/v1/forecast/point?model=ifs`).
Aucune clé ni aucun compte n'est nécessaire.

## Source

Vérifié le 2026-11-27 sur les pages ECMWF « ECMWF open data: real-time
forecasts from IFS and AIFS », « what's new at 0.1° », la page open data, et
sur les listings et index du miroir AWS.

- Miroirs, dans l'ordre d'essai par défaut (`ECMWF_SOURCES=aws,ecmwf`) :
  - AWS `https://ecmwf-forecasts.s3.eu-central-1.amazonaws.com`, rétention
    longue (des dates depuis 2023 sont listées) ;
  - ECMWF `https://data.ecmwf.int/forecasts`, environ 2 à 4 jours de
    rétention, limite de 500 à 2000 connexions simultanées pour tous les
    utilisateurs ;
  - Google `https://storage.googleapis.com/ecmwf-open-data` (accepté par
    `--sources google`, non testé).
- Fichier d'une échéance :
  `{racine}/20261127/00z/ifs/0p1/oper/20261127000000-24h-oper-fc-sfc.grib2`,
  avec son index `…-sfc.index` (une ligne JSON par message, `_offset` et
  `_length`).
- `oper fc` est la « control forecast » d'IFS (anciennement HRES), c'est
  aussi le membre de contrôle de l'ensemble.
- 0,1° depuis le 20/10/2026, publié environ 2 h après le 0,25°. Run 00z
  complet sur AWS vers 09:35 UTC, 12z vers 21:35 UTC.
- Échéances : runs 00z et 12z de 0 à 144 h par 3 h puis de 150 à 360 h par
  6 h ; runs 06z et 18z de 0 à 144 h par 3 h. Nimbostra s'arrête à +168 h
  (J+7), soit 53 échéances (49 pour 06z et 18z, qui ne vont qu'à J+6).
- Licence CC BY 4.0 et conditions d'utilisation de l'ECMWF : voir
  « Attribution » ci-dessous.

## Ce qui est téléchargé

Chaque message GRIB est un champ **global** (3600 × 1801 points) et le serveur
ne permet aucun recadrage : on télécharge le champ entier, puis on le recadre
sur France + ~100 km (40,5–52 N, 6,5 W–10,5 E) à la conversion. Les messages
sont rangés sans ordre utile dans le fichier : une requête Range par message.

| | Par échéance | Par run (53 échéances) |
|---|---|---|
| Messages | 11 (+ orographie à +0 h) | ~584 |
| Requêtes HTTP | 12 (index compris) | ~640 |
| Volume | ~25 Mo | ~1,3 Go |

Le staging garde ces messages tels quels :
`$RAW_DIR/ifs/20261127T00Z/sfc/024H.grib2` et `manifest.json`. `--keep 2`
garde les deux derniers runs (~2,6 Go).

## Débit et reprises

- 4 requêtes en parallèle, 10 requêtes par seconde au plus (`--workers`,
  `--rate`).
- Un `503 SlowDown` (S3) ou un `429` met tous les fils en pause et baisse le
  débit de 30 %. Une erreur `500/502/504` ou réseau est réessayée.
- Une échéance qui échoue sur le premier miroir est demandée au suivant.
- Chaque message reçu est vérifié (longueur, `GRIB` … `7777`) ; un message
  invalide est redemandé une fois.
- Relancer reprend là où on s'était arrêté : les échéances déjà présentes ne
  sont pas retéléchargées. Le manifeste n'est écrit que si toutes le sont.
- Un run est jugé publié quand l'index de sa dernière échéance existe.

## Variables

Noms vérifiés sur les fichiers réels du run 2026-11-27 00z.

| Variable | GRIB | Unité GRIB | Unité stockée | Remarque |
|----------|------|------------|---------------|----------|
| t2m, d2m | 2t, 2d | K | °C | pas d'humidité à 2 m : elle est déduite de t2m et d2m à la lecture |
| u10, v10 | 10u, 10v | m/s | m/s | |
| gust10 | 10fg | m/s | m/s | voir ci-dessous |
| precip_total | tp | m cumulés | mm par pas | total toutes phases |
| snow | sf | m d'eau cumulés | mm par pas | neige ; la pluie est le reste |
| tcc, lcc | tcc, lcc | 0–1 | % | nébulosité totale et basse |
| sp, mslp | sp, msl | Pa | hPa | |
| orog | z à +0 h | m²/s² | m | géopotentiel de surface / g |

**Rafales** : `10fg` est le maximum sur **la dernière heure** avant
l'échéance (stepRange 23-24 à +24 h), puis sur 6 h après +144 h (144-150 à
+150 h). Entre deux échéances de 3 h, deux heures sur trois ne sont pas
couvertes : les rafales IFS sont sous-estimées. Elles sont hors de la distance
des clusters ; seul l'affichage est concerné.

## Commandes

```bash
cd src
python -m ingest.ecmwf_fetch plan                 # lecture seule : dernier run publié et coût
python -m ingest.ecmwf_fetch fetch --keep 2
python -m ingest.mf_paquets_convert inventory --model ifs
python -m ingest.mf_paquets_convert convert --model ifs --keep 2
```

Options utiles de `ecmwf_fetch` : `--sources ecmwf,aws`, `--max-lead 72` (essai
court), `--workers`, `--rate`, `-v`.

## Attribution

Les conditions de l'ECMWF (licence CC BY 4.0) demandent, de façon visible : la
phrase « This service is based on data and products of the European Centre
for Medium-Range Weather Forecasts (ECMWF) », le © de l'année, la source
(www.ecmwf.int), la licence, une clause de non-responsabilité et
l'indication des modifications. L'API la renvoie dans le champ `attribution`
de chaque réponse IFS et de `/v1/models`. Le Zarr porte `source`, `license`
et `modifications` dans ses attributs. Le futur front devra l'afficher aussi.

## Surveillance

IFS 0,1° est publié environ 9 h 30 après l'heure du run, toutes les 6 h : juste
avant le run suivant, le dernier a près de 16 h. Le seuil d'alerte d'IFS est
donc d'au moins 18 h (`ALERT_MAX_AGE_HOURS_IFS` pour le changer).
