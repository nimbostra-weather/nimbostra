"""Quantities derived at read time from the stored surface fields (vectorised numpy).

Conventions: temperatures in °C, relative humidity in %, wind in m/s,
precipitation in mm per step. NaN in, NaN out.
"""

from __future__ import annotations

import numpy as np

STANDARD_LAPSE_RATE = 0.0065  # K/m, standard atmosphere
GRAVITY = 9.80665
R_DRY = 287.05  # J/(kg K)


def wind_speed(u: np.ndarray, v: np.ndarray) -> np.ndarray:
    return np.hypot(u, v)


def wind_direction(u: np.ndarray, v: np.ndarray) -> np.ndarray:
    """Meteorological direction the wind blows FROM, degrees in [0, 360)."""
    return np.mod(np.degrees(np.arctan2(-u, -v)), 360.0)


def dewpoint(t: np.ndarray, rh: np.ndarray) -> np.ndarray:
    """Magnus formula (Alduchov and Eskridge 1996 coefficients)."""
    a, b = 17.625, 243.04
    rh = np.clip(rh, 0.1, 100.0)
    gamma = np.log(rh / 100.0) + a * t / (b + t)
    return b * gamma / (a - gamma)


def wet_bulb(t: np.ndarray, rh: np.ndarray) -> np.ndarray:
    """Stull (2011) empirical wet-bulb temperature at ~1013 hPa.

    Valid for RH 5-99 % and T -20..50 °C, error mostly within ±1 °C; enough for
    precipitation type and rain-snow limit estimates, not for physics.
    """
    rh = np.clip(rh, 5.0, 99.0)
    return (
        t * np.arctan(0.151977 * np.sqrt(rh + 8.313659))
        + np.arctan(t + rh)
        - np.arctan(rh - 1.676331)
        + 0.00391838 * rh**1.5 * np.arctan(0.023101 * rh)
        - 4.686035
    )


def feels_like(t: np.ndarray, rh: np.ndarray, wind: np.ndarray) -> np.ndarray:
    """Apparent temperature: wind chill when cold and windy, heat index when hot.

    Wind chill: JAG/TI formula (Environment Canada / NWS), T <= 10 °C and wind
    > 4.8 km/h. Heat index: Rothfusz regression (NWS), T >= 27 °C and RH >= 40 %.
    Otherwise the air temperature.
    """
    t = np.asarray(t, dtype=float)
    out = t.copy()
    kmh = np.asarray(wind, dtype=float) * 3.6
    with np.errstate(invalid="ignore"):
        cold = (t <= 10.0) & (kmh > 4.8)
        v16 = np.power(np.where(cold, kmh, 0.0), 0.16)
        chill = 13.12 + 0.6215 * t - 11.37 * v16 + 0.3965 * t * v16
        out = np.where(cold, chill, out)

        hot = (t >= 27.0) & (rh >= 40.0)
        tf = t * 9 / 5 + 32
        hi_f = (
            -42.379
            + 2.04901523 * tf
            + 10.14333127 * rh
            - 0.22475541 * tf * rh
            - 6.83783e-3 * tf**2
            - 5.481717e-2 * rh**2
            + 1.22874e-3 * tf**2 * rh
            + 8.5282e-4 * tf * rh**2
            - 1.99e-6 * tf**2 * rh**2
        )
        out = np.where(hot, (hi_f - 32) * 5 / 9, out)
    return out


def total_precipitation(rain, snow, graupel) -> np.ndarray:
    """Sum of the available components; NaN only where all are NaN."""
    parts = [np.asarray(p, dtype=float) for p in (rain, snow, graupel) if p is not None]
    if not parts:
        raise ValueError("no precipitation component")
    stacked = np.stack(parts)
    total = np.nansum(stacked, axis=0)
    return np.where(np.all(np.isnan(stacked), axis=0), np.nan, total)


def liquid_remainder(total, snow, graupel) -> np.ndarray:
    """Liquid part of a total precipitation: total minus the known solid phases, >= 0.

    A missing solid component counts as 0; NaN where the total is NaN.
    """
    total = np.asarray(total, dtype=float)
    solid = np.zeros_like(total)
    for part in (snow, graupel):
        if part is not None:
            solid = solid + np.nan_to_num(np.asarray(part, dtype=float))
    return np.where(np.isnan(total), np.nan, np.maximum(total - solid, 0.0))


PRECIP_TYPES = ("none", "rain", "snow", "graupel", "mixed")


def precipitation_type(
    rain, snow, graupel, *, threshold: float = 0.1, dominant: float = 0.8
) -> list[str | None]:
    """Type per step from the model's own phase split.

    ``none`` below ``threshold`` mm per step; a phase is reported when it makes
    at least ``dominant`` of the total, else ``mixed``. None where unknown.
    """
    rain = np.asarray(rain, dtype=float)
    # An absent component is unknown (NaN), not zero: a step stays unknown if all are.
    snow = np.full_like(rain, np.nan) if snow is None else np.asarray(snow, dtype=float)
    graupel = np.full_like(rain, np.nan) if graupel is None else np.asarray(graupel, dtype=float)
    out: list[str | None] = []
    for r, s, g in zip(rain, snow, graupel, strict=True):
        if np.isnan(r) and np.isnan(s) and np.isnan(g):
            out.append(None)
            continue
        r, s, g = (0.0 if np.isnan(x) else float(x) for x in (r, s, g))
        total = r + s + g
        if total < threshold:
            out.append("none")
            continue
        for name, part in (("rain", r), ("snow", s), ("graupel", g)):
            if part >= dominant * total:
                out.append(name)
                break
        else:
            out.append("mixed")
    return out


def rain_snow_limit_estimate(
    wet_bulb_c: np.ndarray,
    elevation_m: float,
    *,
    lapse_rate: float = STANDARD_LAPSE_RATE,
    wet_bulb_limit: float = 1.0,
) -> np.ndarray:
    """Rough rain-snow limit (m): altitude where the wet-bulb reaches ``wet_bulb_limit``.

    Extrapolates the point's wet-bulb temperature with a constant lapse rate,
    because the surface packages carry no vertical profile. Inversions and
    valley cold pools make it wrong; treat as an indication only.
    """
    return elevation_m + (np.asarray(wet_bulb_c, dtype=float) - wet_bulb_limit) / lapse_rate


def correct_temperature(
    t: np.ndarray, model_elev: float, target_elev: float, lapse_rate: float = STANDARD_LAPSE_RATE
) -> np.ndarray:
    """Shift a temperature from the model orography to the target elevation."""
    return np.asarray(t, dtype=float) - lapse_rate * (target_elev - model_elev)


def correct_pressure(
    p: np.ndarray, t_c: np.ndarray, model_elev: float, target_elev: float
) -> np.ndarray:
    """Hypsometric shift of surface pressure, using the model temperature in K."""
    t_k = np.asarray(t_c, dtype=float) + 273.15
    dz = target_elev - model_elev
    return np.asarray(p, dtype=float) * np.exp(-GRAVITY * dz / (R_DRY * t_k))
