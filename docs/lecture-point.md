# Lecture d'un point (AROME, ARPEGE)

Code : `src/point/reader.py` (lecture et interpolation) et
`src/point/derived.py` (grandeurs dérivées). Aucune lecture de GRIB :
seuls les chunks Zarr publiés par `ingest.arome_convert` sont lus, pour
AROME (`$DATA_DIR/arome`) comme pour ARPEGE (`$DATA_DIR/arpege`).

## Étapes

1. **Run à lire** : `RunStore` lit `$DATA_DIR/arome/latest.json` et garde
   en cache les groupes Zarr ouverts (deux par défaut). Un nouveau run a un
   nouveau chemin, donc le cache ne sert jamais un run périmé.
2. **Interpolation** (`read_raw`) : bilinéaire sur la grille native 0,01°
   (ou point le plus proche avec `method="nearest"`). Un coin manquant (NaN)
   est écarté et les poids des autres coins renormalisés ; si les coins
   valides pèsent moins de 25 %, la valeur est absente. Un point hors grille
   lève `OutOfDomain`.
3. **Altitude cible** (`derive`) : l'altitude donnée (`elevation`, futur
   paramètre `alt=` de l'API), sinon le MNT, sinon l'orographie du modèle
   (pas de correction). La réponse indique `elevation.source` :
   `user`, `dem` ou `model`.
4. **Correction d'altitude** : température −0,0065 K/m × (altitude cible −
   orographie du modèle) ; pression de surface par la formule hypsométrique.
   L'humidité relative est gardée : point de rosée et thermomètre mouillé sont
   recalculés à partir de la température corrigée.

## Grandeurs servies par `derive`

| Nom | Unité | Calcul |
|-----|-------|--------|
| temperature | °C | t2m corrigée de l'altitude |
| humidity | % | r2 |
| dewpoint | °C | Magnus (Alduchov et Eskridge 1996) |
| wet_bulb | °C | Stull (2011), précision ~1 °C |
| wind_speed, wind_direction | m/s, ° | à partir de u10, v10 ; direction d'où vient le vent |
| wind_gust | m/s | hypot des composantes de rafale, sinon la rafale `gust10` (ARPEGE) |
| feels_like | °C | refroidissement éolien (JAG/TI) si T ≤ 10 °C, indice de chaleur (Rothfusz) si T ≥ 27 °C et HR ≥ 40 % |
| precipitation, rain, snow, graupel | mm par pas | AROME : somme des phases du modèle ; ARPEGE : `precip_total`, et rain = total − neige |
| precipitation_type | texte | phase ≥ 80 % du total, sinon `mixed` ; `none` sous 0,1 mm |
| rain_snow_limit | m | **estimation** : altitude où le thermomètre mouillé atteint 1 °C, extrapolé à 6,5 K/km |
| cloud_total | % | tcc, si le modèle la fournit |
| cloud_l, cloud_m, cloud_h | % | lcc, mcc, hcc |
| surface_pressure, pressure_msl | hPa | sp corrigée, prmsl |
| cape | J/kg | |

## Limites connues

- **Limite pluie-neige** : les paquets de surface n'ont pas de profil
  vertical. L'extrapolation est fausse en cas d'inversion ou de lac d'air
  froid en vallée. Une vraie limite demande un paquet en altitude (HP ou IP).
- **Pas de nébulosité totale** dans AROME : seules les couches basse,
  moyenne et haute sont servies.
- **MNT** : Copernicus GLO-90 est un modèle de surface (arbres et
  bâtiments compris), lissé à ~90 m : le Mont-Blanc y culmine à 4 790 m.

## Temps de lecture

Mesuré en local sur un Zarr de 15 variables × 52 échéances, chunks 32 × 32 :
18 ms en médiane et 24 ms au 95e centile par point, dérivés et JSON compris.

## MNT Copernicus GLO-90

Code : `src/ingest/dem.py`. Construction unique (dépendances optionnelles
`pip install .[dem]`, soit tifffile et imagecodecs) :

```bash
cd src
python -m ingest.dem download   # ~216 tuiles de 1°, ~5 Mo chacune, vers $RAW_DIR/dem
python -m ingest.dem build      # -> $DATA_DIR/dem/glo90.zarr
```

- Source : bucket public `copernicus-dem-90m` (AWS Open Data), liste des
  tuiles dans `tileList.txt`, fichier `<nom>/<nom>.tif`. Pas de tuile en mer :
  altitude 0. Vérifié le 2026-11-27 sur le readme du bucket et une tuile.
- Grille : 3 secondes d'arc (1/1200°), 40–52 N et 7 W–11 E, chunks 300 × 300
  alignés sur les tuiles. Les tuiles au nord de 50° N (800 colonnes) sont
  rééchantillonnées en longitude.
- Lecture : bilinéaire, ~1,5 ms. L'API l'utilise automatiquement si
  `$DATA_DIR/dem/glo90.zarr` existe.
- Contrôle sur une vraie tuile : Chamonix 1 041 m, lac d'Annecy 446 m.
- Licence : Copernicus DEM, gratuite ; citation demandée : « Copernicus
  Digital Elevation Model (DEM) was accessed on DATE from
  https://registry.opendata.aws/copernicus-dem ».
