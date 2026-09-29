"""Generador de un dataset sintetico con la misma forma que US Accidents.

Sirve para dos cosas:

1. Permitir que el pipeline de CI corra de punta a punta sin consumir la cuota
   de la API de Kaggle en cada build.
2. Desbloquear el desarrollo mientras se tramita el token de Kaggle.

Los puntos se agrupan alrededor de areas metropolitanas reales de EE.UU. con
dispersion gaussiana, de modo que las agregaciones por grilla y la deteccion de
zonas de alta concentracion produzcan resultados con sentido geografico y no
ruido uniforme.

Se inyecta deliberadamente un 3% de registros sucios (coordenadas nulas, fuera
de rango, el relleno (0,0), severidad invalida y duplicados) para poder
demostrar y medir que la limpieza con Dask hace su trabajo.
"""
from __future__ import annotations

import csv
from pathlib import Path

import numpy as np

from src.common.logging_conf import setup_logging

log = setup_logging("ingestion.synthetic")

# (nombre, estado, lat, lon, peso relativo, sigma en grados)
METROS: list[tuple[str, str, float, float, float, float]] = [
    ("Los Angeles", "CA", 34.0522, -118.2437, 0.115, 0.35),
    ("Houston", "TX", 29.7604, -95.3698, 0.075, 0.30),
    ("Charlotte", "NC", 35.2271, -80.8431, 0.055, 0.22),
    ("Dallas", "TX", 32.7767, -96.7970, 0.060, 0.28),
    ("Miami", "FL", 25.7617, -80.1918, 0.065, 0.25),
    ("Orlando", "FL", 28.5383, -81.3792, 0.045, 0.22),
    ("Atlanta", "GA", 33.7490, -84.3880, 0.050, 0.26),
    ("Phoenix", "AZ", 33.4484, -112.0740, 0.045, 0.28),
    ("New York", "NY", 40.7128, -74.0060, 0.070, 0.30),
    ("Chicago", "IL", 41.8781, -87.6298, 0.040, 0.28),
    ("Seattle", "WA", 47.6062, -122.3321, 0.045, 0.25),
    ("Minneapolis", "MN", 44.9778, -93.2650, 0.030, 0.24),
    ("Denver", "CO", 39.7392, -104.9903, 0.030, 0.24),
    ("Nashville", "TN", 36.1627, -86.7816, 0.030, 0.22),
    ("Philadelphia", "PA", 39.9526, -75.1652, 0.035, 0.24),
    ("Portland", "OR", 45.5152, -122.6784, 0.030, 0.22),
    ("Baton Rouge", "LA", 30.4515, -91.1871, 0.025, 0.20),
    ("Richmond", "VA", 37.5407, -77.4360, 0.030, 0.22),
    ("Sacramento", "CA", 38.5816, -121.4944, 0.035, 0.24),
    ("San Diego", "CA", 32.7157, -117.1611, 0.035, 0.22),
    ("Detroit", "MI", 42.3314, -83.0458, 0.030, 0.24),
]

WEATHER = ["Clear", "Fair", "Cloudy", "Overcast", "Light Rain", "Rain",
           "Mostly Cloudy", "Partly Cloudy", "Fog", "Light Snow", "Haze"]
WEATHER_P = [0.20, 0.18, 0.13, 0.10, 0.10, 0.07, 0.09, 0.06, 0.03, 0.02, 0.02]

HEADER = ["ID", "Severity", "Start_Time", "End_Time", "Start_Lat", "Start_Lng",
          "Distance(mi)", "City", "County", "State", "Temperature(F)",
          "Visibility(mi)", "Weather_Condition", "Sunrise_Sunset"]

DIRTY_FRACTION = 0.03
CHUNK = 100_000


def _sample_metros(n: int, rng: np.random.Generator) -> np.ndarray:
    weights = np.array([m[4] for m in METROS], dtype=float)
    weights /= weights.sum()
    return rng.choice(len(METROS), size=n, p=weights)


def generate_synthetic_csv(path: str | Path, n_rows: int = 1_000_000,
                           seed: int = 42) -> Path:
    """Escribe un CSV sintetico de `n_rows` filas y devuelve su ruta."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(seed)

    lat_c = np.array([m[2] for m in METROS])
    lon_c = np.array([m[3] for m in METROS])
    sigma = np.array([m[5] for m in METROS])
    cities = [m[0] for m in METROS]
    states = [m[1] for m in METROS]

    log.info("Generando %s filas sinteticas en %s ...", f"{n_rows:,}", path)
    written = 0
    dirty_written = 0

    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(HEADER)

        while written < n_rows:
            size = min(CHUNK, n_rows - written)
            idx = _sample_metros(size, rng)

            lat = rng.normal(lat_c[idx], sigma[idx])
            lon = rng.normal(lon_c[idx], sigma[idx])

            # Severidad 1..4 con la distribucion real del dataset (domina el 2)
            severity = rng.choice([1, 2, 3, 4], size=size,
                                  p=[0.02, 0.80, 0.15, 0.03])

            # Ventana temporal 2016-2023 con mas accidentes en hora pico.
            day_offset = rng.integers(0, 365 * 8, size=size)
            # Las 24 horas, ponderadas hacia las horas pico. Antes se muestreaba
            # de un conjunto de 12 horas, y eso hacia que la agregacion temporal
            # de Spark produjera 12 franjas en lugar de 24: el dataset real de
            # Kaggle tiene accidentes a todas las horas, y los datos sinteticos
            # deben tener la misma FORMA o las pruebas que validan el pipeline
            # con ellos no dicen nada sobre el pipeline real.
            hour_pool = np.arange(24)
            hour_p = np.array([
                .012, .009, .008, .008, .012, .025,  # 00-05 madrugada
                .048, .085, .078, .050, .040, .042,  # 06-11 pico de manana
                .045, .048, .052, .070, .085, .082,  # 12-17 pico de tarde
                .058, .040, .030, .024, .018, .015,  # 18-23 noche
            ])
            hour_p = hour_p / hour_p.sum()
            hour = rng.choice(hour_pool, size=size, p=hour_p)
            minute = rng.integers(0, 60, size=size)
            duration = rng.integers(10, 360, size=size)

            base = np.datetime64("2016-01-01T00:00:00")
            start = (base + day_offset.astype("timedelta64[D]")
                     + hour.astype("timedelta64[h]")
                     + minute.astype("timedelta64[m]"))
            end = start + duration.astype("timedelta64[m]")

            distance = np.round(np.abs(rng.gamma(1.4, 0.45, size=size)), 3)
            temperature = np.round(rng.normal(62, 18, size=size), 1)
            visibility = np.round(np.clip(rng.normal(9.2, 1.8, size=size),
                                          0.1, 10.0), 1)
            weather_idx = rng.choice(len(WEATHER), size=size, p=WEATHER_P)
            daynight = np.where(rng.random(size) < 0.62, "Day", "Night")

            # --- inyeccion de registros sucios -----------------------------
            dirty_mask = rng.random(size) < DIRTY_FRACTION
            dirty_kind = rng.integers(0, 5, size=size)

            lat_out = lat.astype(object)
            lon_out = lon.astype(object)
            sev_out = severity.astype(object)
            time_out = np.array([str(t) for t in start], dtype=object)

            for i in np.nonzero(dirty_mask)[0]:
                kind = dirty_kind[i]
                if kind == 0:        # coordenadas nulas
                    lat_out[i] = ""
                    lon_out[i] = ""
                elif kind == 1:      # latitud fuera de rango
                    lat_out[i] = round(float(rng.uniform(91, 200)), 6)
                elif kind == 2:      # relleno (0,0) en el Golfo de Guinea
                    lat_out[i] = 0.0
                    lon_out[i] = 0.0
                elif kind == 3:      # severidad invalida
                    sev_out[i] = 0
                else:                # fecha no parseable
                    time_out[i] = "N/A"
                dirty_written += 1

            rows = []
            for i in range(size):
                rid = written + i
                m = idx[i]
                rows.append([
                    f"SYN-{rid+1:09d}",
                    sev_out[i],
                    time_out[i],
                    str(end[i]),
                    lat_out[i] if lat_out[i] == "" else round(float(lat_out[i]), 6),
                    lon_out[i] if lon_out[i] == "" else round(float(lon_out[i]), 6),
                    distance[i],
                    cities[m],
                    f"{cities[m]} County",
                    states[m],
                    temperature[i],
                    visibility[i],
                    WEATHER[weather_idx[i]],
                    daynight[i],
                ])

            # ~0.5% de duplicados exactos, para ejercitar la deduplicacion
            n_dup = max(1, size // 200)
            for j in rng.integers(0, size, size=n_dup):
                rows.append(list(rows[int(j)]))

            writer.writerows(rows)
            written += size
            if written % 500_000 == 0 or written == n_rows:
                log.info("  %s / %s filas", f"{written:,}", f"{n_rows:,}")

    size_mb = path.stat().st_size / 1024 / 1024
    log.info("CSV sintetico listo: %s (%.1f MB, %s filas limpias objetivo, "
             "%s filas sucias inyectadas)",
             path, size_mb, f"{n_rows:,}", f"{dirty_written:,}")
    return path


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Genera un CSV sintetico tipo US Accidents")
    ap.add_argument("--out", default="/data/US_Accidents_March23.csv")
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    generate_synthetic_csv(args.out, args.rows, args.seed)
