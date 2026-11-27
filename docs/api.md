# API Nimbostra (v1)

Application FastAPI : `src/api/app.py`. Lancement depuis `src` :

```bash
uvicorn api.app:app --host 127.0.0.1 --port 8000
```

`DATA_DIR` (environnement ou `.env`) désigne le dossier des Zarr publiés
(`$DATA_DIR/arome/latest.json`). La documentation interactive est servie sur
`/v1/docs`, le schéma sur `/v1/openapi.json`. L'API est publique, sans clé ;
la limite de débit sera posée dans Caddy (étape d'exploitation).

## Routes

### `GET /v1/models`

Modèles disponibles, run courant, emprise de la grille, échéances et
variables stockées avec leur unité. `run` vaut `null` si aucun run n'est publié.

### `GET /v1/raw/{model}/point?lat=&lon=[&method=bilinear|nearest][&variables=t2m,u10]`

Valeurs stockées interpolées au point, **sans correction** : unités du Zarr
(°C, %, m/s, mm par pas, hPa). `static.orog` donne l'altitude du modèle au point.

### `GET /v1/forecast/point?lat=&lon=[&alt=][&model=arome]`

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
  "attribution": "Source : Météo-France (Licence Ouverte Etalab 2.0)"
}
```

- Colonnes : une liste de valeurs par variable, alignée sur `times` (UTC).
- `null` = valeur absente (cumuls et rafales à l'échéance 0, champ manquant).
- Une précipitation à l'instant t est le cumul de l'heure qui se termine à t.

## Erreurs

| Code | Cas |
|------|-----|
| 404 | modèle inconnu |
| 422 | paramètre invalide, point hors du domaine du modèle, variable inconnue |
| 503 | aucun run publié pour ce modèle |

## Cache

Chaque réponse porte `Cache-Control: public, max-age=300` et un `ETag` qui
dépend du run et des paramètres : un client qui renvoie `If-None-Match`
reçoit `304` tant qu'aucun nouveau run n'est publié. Pas de Redis à ce stade
(lecture d'un point en ~20 ms).
