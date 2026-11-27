# API Nimbostra (v1)

Application FastAPI : `src/api/app.py`. Lancement depuis `src` :

```bash
uvicorn api.app:app --host 127.0.0.1 --port 8000
```

`DATA_DIR` (environnement ou `.env`) désigne le dossier des Zarr publiés
(`$DATA_DIR/<modèle>/latest.json`) ; `NIMBOSTRA_MODELS` la liste des modèles
servis (`arome,arpege` par défaut, `ifs` pour ECMWF IFS). La documentation interactive est servie sur
`/v1/docs`, le schéma sur `/v1/openapi.json`. L'API est publique, sans clé
et, pour l'instant, sans limite de débit (voir `docs/exploitation.md`).

## Routes

### `GET /v1/models`

Modèles disponibles, run courant, emprise de la grille, échéances et
variables stockées avec leur unité. `run` vaut `null` si aucun run n'est publié.

### `GET /v1/raw/{model}/point?lat=&lon=[&method=bilinear|nearest][&variables=t2m,u10]`

Valeurs stockées interpolées au point, **sans correction** : unités du Zarr
(°C, %, m/s, mm par pas, hPa). `static.orog` donne l'altitude du modèle au point.

### `GET /v1/forecast/point?lat=&lon=[&alt=][&model=arome|arpege|ifs]`

Prévision corrigée de l'altitude, avec les grandeurs dérivées décrites dans
`docs/lecture-point.md`. `alt` (m) l'emporte sur le MNT, puis sur
l'orographie du modèle ; `elevation.source` dit laquelle a servi.

## Format de réponse (point)

```json
{
  "model": "arome",
  "run": "2026-11-27T06:00:00Z",
  "point": {"lat": 45.2, "lon": 6.2},
  "grid": {"method": "bilinear", "nearest_lat": 45.2, "nearest_lon": 6.2},
  "elevation": {"model": 1000.0, "target": 1200.0, "source": "user"},
  "times": ["2026-11-27T06:00:00Z", "2026-11-27T07:00:00Z"],
  "variables": {
    "temperature": {"units": "degC", "values": [3.1, 2.8]},
    "precipitation": {"units": "mm", "values": [null, 0.4]}
  },
  "attribution": "Source : Météo-France (Licence Ouverte Etalab 2.0)",
  "timing": {"read_ms": 18.4, "derive_ms": 1.2, "total_ms": 19.9}
}
```

- `attribution` : sources et licence du modèle de la réponse. Pour IFS, le
  texte exigé par l'ECMWF (CC BY 4.0, ©, non-responsabilité, modifications).
  `/v1/models` donne l'attribution de chaque modèle et, au niveau racine,
  celles de tous les fournisseurs servis.
- Colonnes : une liste de valeurs par variable, alignée sur `times` (UTC).
- `null` = valeur absente (cumuls et rafales à l'échéance 0, champ manquant).
- Une précipitation à l'instant t est le cumul depuis l'instant précédent qui a
  une valeur. AROME : l'heure qui se termine à t. ARPEGE : horaire jusqu'à
  +48 h puis, pour les paramètres qui passent au pas de 3 h, `null` aux heures
  intermédiaires et cumul de 3 h à t. IFS : cumul de 3 h jusqu'à +144 h,
  puis de 6 h.

## Temps de traitement

Chaque réponse 200 de `/v1/models`, `/v1/raw/{model}/point` et
`/v1/forecast/point` porte un objet `timing`, en millisecondes (1 décimale),
mesuré côté serveur :

| Champ | Contenu | Routes |
|-------|---------|--------|
| `read_ms` | ouverture du run courant et lecture Zarr du point (ou des métadonnées pour `/v1/models`) | toutes |
| `derive_ms` | correction d'altitude et grandeurs dérivées | `/v1/forecast/point` |
| `total_ms` | du début du traitement jusqu'à la construction de la réponse (seul l'encodage JSON final n'est pas compté) | toutes |

Les mêmes valeurs sont envoyées dans l'en-tête standard `Server-Timing`
(`read;dur=18.4, derive;dur=1.2, total;dur=19.9`), visible dans l'onglet
Réseau des navigateurs. Le temps réseau et celui de Caddy ne sont pas comptés.
Une réponse servie depuis un cache (navigateur, `max-age=300`) garde le
`timing` de l'appel qui l'a produite.

## Erreurs

| Code | Cas |
|------|-----|
| 404 | modèle inconnu |
| 422 | paramètre invalide, point hors du domaine du modèle, variable inconnue |
| 503 | aucun run publié pour ce modèle |

## Cache

Chaque réponse porte `Cache-Control: public, max-age=300` et un `ETag` qui
dépend du run et des paramètres : un client qui renvoie `If-None-Match`
reçoit `304` tant qu'aucun nouveau run n'est publié. L'`ETag` est calculé à
partir du run et des paramètres seulement, jamais du corps : le `timing`, qui
change à chaque appel, ne le modifie donc pas. Le `304` est renvoyé avant toute
lecture Zarr, sans corps ni `Server-Timing`. Pas de Redis à ce stade
(lecture d'un point en ~20 ms).
