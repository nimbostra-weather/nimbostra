# Lecture d'un point (AROME)

Code : `src/point/reader.py` (lecture et interpolation) et
`src/point/derived.py` (grandeurs dérivées). Aucune lecture de GRIB :
seuls les chunks Zarr publiés par `ingest.arome_convert` sont lus.

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
| wind_gust | m/s | hypot des composantes de rafale |
| feels_like | °C | refroidissement éolien (JAG/TI) si T ≤ 10 °C, indice de chaleur (Rothfusz) si T ≥ 27 °C et HR ≥ 40 % |
| precipitation, rain, snow, graupel | mm par pas | somme des phases du modèle |
| precipitation_type | texte | phase ≥ 80 % du total, sinon `mixed` ; `none` sous 0,1 mm |
| rain_snow_limit | m | **estimation** : altitude où le thermomètre mouillé atteint 1 °C, extrapolé à 6,5 K/km |
| cloud_l, cloud_m, cloud_h | % | lcc, mcc, hcc |
| surface_pressure, pressure_msl | hPa | sp corrigée, prmsl |
| cape | J/kg | |

## Limites connues

- **Limite pluie-neige** : les paquets de surface n'ont pas de profil
  vertical. L'extrapolation est fausse en cas d'inversion ou de lac d'air
  froid en vallée. Une vraie limite demande un paquet en altitude (HP ou IP).
- **Pas de nébulosité totale** dans AROME : seules les couches basse,
  moyenne et haute sont servies.
- **MNT** : la source d'altitude est un paramètre (`dem`) ; le chargement
  de Copernicus GLO-90 n'est pas encore écrit.

## Temps de lecture

Mesuré en local sur un Zarr de 15 variables × 52 échéances, chunks 32 × 32 :
18 ms en médiane et 24 ms au 95e centile par point, dérivés et JSON compris.
