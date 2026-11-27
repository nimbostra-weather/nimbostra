# Ingestion AROME 0,01° (API Paquets Météo-France)

Cette page décrit la première étape de la chaîne AROME déterministe :
le téléchargement des fichiers GRIB2 bruts d'un run. La conversion en Zarr,
la lecture d'un point et l'API viennent dans les étapes suivantes.

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
