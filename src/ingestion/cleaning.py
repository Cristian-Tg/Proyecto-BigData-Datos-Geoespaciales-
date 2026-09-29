"""Reglas de limpieza y transformacion a GeoJSON.

Estas funciones operan sobre un `pandas.DataFrame` que corresponde a UNA
particion de Dask. Estan aisladas de Dask a proposito por dos razones:

* se pueden probar con pytest sin levantar un cluster;
* Dask las aplica en paralelo sobre cada particion via `map_partitions` /
  `to_delayed`, de modo que la logica es la misma en local y distribuida.

Cada regla de descarte queda contabilizada en `CleaningStats`, lo que permite
justificar numericamente en el informe cuantos registros se eliminaron y por que.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import numpy as np
import pandas as pd

from src.common import config
from src.common.geo import geohash_encode, grid_cell, to_geojson_point

# ---------------------------------------------------------------------------
# Mapeo de columnas del CSV original a nombres normalizados en MongoDB
# ---------------------------------------------------------------------------
COLUMN_MAP: dict[str, str] = {
    "ID": "accident_id",
    "Severity": "severity",
    "Start_Time": "start_time",
    "End_Time": "end_time",
    "Start_Lat": "lat",
    "Start_Lng": "lon",
    "Distance(mi)": "distance_mi",
    "City": "city",
    "County": "county",
    "State": "state",
    "Temperature(F)": "temperature_f",
    "Visibility(mi)": "visibility_mi",
    "Weather_Condition": "weather",
    "Sunrise_Sunset": "day_night",
}

# Solo se leen estas columnas del CSV. Leer 47 columnas cuando se usan 14
# multiplica por tres la memoria de cada particion sin aportar nada.
USECOLS: list[str] = list(COLUMN_MAP.keys())

# Tipos explicitos: sin esto Dask infiere a partir de la primera particion y
# revienta mas adelante con "ValueError: Mismatched dtypes found".
DTYPES: dict[str, Any] = {
    "ID": "object",
    "Severity": "float64",      # float y no int: el CSV trae celdas vacias
    "Start_Lat": "float64",
    "Start_Lng": "float64",
    "Distance(mi)": "float64",
    "City": "object",
    "County": "object",
    "State": "object",
    "Temperature(F)": "float64",
    "Visibility(mi)": "float64",
    "Weather_Condition": "object",
    "Sunrise_Sunset": "object",
}

# Caja envolvente de EE.UU. incluyendo Alaska, Hawai y Puerto Rico.
# El dataset es exclusivamente estadounidense: cualquier punto fuera de esta
# caja es un error de captura, no una observacion valida.
US_BBOX = {"lat_min": 17.5, "lat_max": 72.0, "lon_min": -180.0, "lon_max": -64.5}

VALID_SEVERITY = (1, 2, 3, 4)


@dataclass
class CleaningStats:
    """Contadores de cada regla de descarte, agregables entre particiones."""
    rows_in: int = 0
    dropped_null_coords: int = 0
    dropped_coords_out_of_range: int = 0
    dropped_zero_island: int = 0
    dropped_outside_us_bbox: int = 0
    dropped_unparseable_time: int = 0
    dropped_invalid_severity: int = 0
    dropped_duplicate_id: int = 0
    rows_out: int = 0
    partitions: int = 0

    def __add__(self, other: CleaningStats) -> CleaningStats:
        merged = CleaningStats()
        for key in asdict(self):
            setattr(merged, key, getattr(self, key) + getattr(other, key))
        return merged

    @property
    def dropped_total(self) -> int:
        return self.rows_in - self.rows_out

    @property
    def retention_pct(self) -> float:
        return 100.0 * self.rows_out / self.rows_in if self.rows_in else 0.0

    def as_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["dropped_total"] = self.dropped_total
        data["retention_pct"] = round(self.retention_pct, 4)
        return data

    def report(self) -> str:
        lines = [
            "",
            "=" * 72,
            "  INFORME DE LIMPIEZA (Dask)",
            "=" * 72,
            f"  Registros leidos                   : {self.rows_in:>12,}",
            f"  - Coordenadas nulas                : {self.dropped_null_coords:>12,}",
            f"  - Coordenadas fuera de rango WGS84 : {self.dropped_coords_out_of_range:>12,}",
            f"  - Relleno (0,0)                    : {self.dropped_zero_island:>12,}",
            f"  - Fuera de la caja de EE.UU.       : {self.dropped_outside_us_bbox:>12,}",
            f"  - Fecha/hora no parseable          : {self.dropped_unparseable_time:>12,}",
            f"  - Severidad fuera de 1..4          : {self.dropped_invalid_severity:>12,}",
            f"  - Duplicados por ID                : {self.dropped_duplicate_id:>12,}",
            "-" * 72,
            f"  Registros descartados              : {self.dropped_total:>12,}",
            f"  Registros cargados en MongoDB      : {self.rows_out:>12,}",
            f"  Tasa de retencion                  : {self.retention_pct:>11.2f} %",
            f"  Particiones procesadas             : {self.partitions:>12,}",
            "=" * 72,
        ]
        return "\n".join(lines)


def clean_partition(pdf: pd.DataFrame) -> tuple[pd.DataFrame, CleaningStats]:
    """Aplica todas las reglas de limpieza a una particion.

    Devuelve el DataFrame limpio y los contadores de descarte.

    El orden de las reglas importa: se aplican de la mas barata a la mas cara
    para que cada filtro reduzca el volumen que procesa el siguiente.
    """
    stats = CleaningStats(rows_in=len(pdf), partitions=1)
    if pdf.empty:
        return pdf.iloc[0:0], stats

    df = pdf.rename(columns=COLUMN_MAP).copy()

    for needed in ("lat", "lon"):
        if needed not in df.columns:
            raise KeyError(
                f"El CSV no contiene la columna requerida '{needed}'. "
                f"Columnas presentes: {list(df.columns)[:15]}"
            )

    # --- 1. Coordenadas nulas ---------------------------------------------
    # Un accidente sin coordenadas es inutilizable para TODO el sistema: no se
    # puede indexar en 2dsphere ni aparecer en una consulta espacial.
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    before = len(df)
    df = df[df["lat"].notna() & df["lon"].notna()]
    stats.dropped_null_coords = before - len(df)

    # --- 2. Fuera del rango WGS84 -----------------------------------------
    # MongoDB rechaza el documento completo al construir el indice 2dsphere si
    # |lat| > 90 o |lon| > 180, asi que hay que filtrarlos antes de insertar.
    before = len(df)
    df = df[df["lat"].between(-90.0, 90.0) & df["lon"].between(-180.0, 180.0)]
    stats.dropped_coords_out_of_range = before - len(df)

    # --- 3. Relleno (0,0) --------------------------------------------------
    # (0,0) cae en el Golfo de Guinea. En un dataset de accidentes de EE.UU.
    # es siempre un valor por defecto de dato faltante.
    before = len(df)
    df = df[~((df["lat"] == 0.0) & (df["lon"] == 0.0))]
    stats.dropped_zero_island = before - len(df)

    # --- 4. Fuera de la caja de EE.UU. ------------------------------------
    before = len(df)
    df = df[df["lat"].between(US_BBOX["lat_min"], US_BBOX["lat_max"])
            & df["lon"].between(US_BBOX["lon_min"], US_BBOX["lon_max"])]
    stats.dropped_outside_us_bbox = before - len(df)

    # --- 5. Fecha/hora ----------------------------------------------------
    # Sin marca temporal valida no se puede hacer el analisis por hora, dia y
    # mes que exige el enunciado.
    if "start_time" in df.columns:
        before = len(df)
        df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce",
                                          format="mixed")
        df = df[df["start_time"].notna()]
        stats.dropped_unparseable_time = before - len(df)
    if "end_time" in df.columns:
        df["end_time"] = pd.to_datetime(df["end_time"], errors="coerce",
                                        format="mixed")

    # --- 6. Severidad -----------------------------------------------------
    # Es la variable de ponderacion de las zonas de alta concentracion; un
    # valor fuera de 1..4 contaminaria los promedios.
    if "severity" in df.columns:
        before = len(df)
        df["severity"] = pd.to_numeric(df["severity"], errors="coerce")
        df = df[df["severity"].isin(VALID_SEVERITY)]
        df["severity"] = df["severity"].astype("int16")
        stats.dropped_invalid_severity = before - len(df)

    # --- 7. Duplicados ----------------------------------------------------
    # Dentro de la particion. La unicidad global la garantiza el indice unico
    # `ux_accident_id` de MongoDB junto con insert_many(ordered=False).
    if "accident_id" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=["accident_id"], keep="first")
        stats.dropped_duplicate_id = before - len(df)

    stats.rows_out = len(df)
    return df, stats


def enrich_partition(df: pd.DataFrame, cell_deg: float | None = None,
                     precision: int | None = None) -> pd.DataFrame:
    """Agrega las columnas derivadas que usan las agregaciones y los indices."""
    if df.empty:
        return df
    cell_deg = cell_deg if cell_deg is not None else config.geo.grid_cell_deg
    precision = precision if precision is not None else config.geo.geohash_precision

    lat = df["lat"].to_numpy(dtype="float64")
    lon = df["lon"].to_numpy(dtype="float64")

    # La grilla se calcula vectorizada con numpy: es el camino caliente y
    # hacerlo fila por fila con .apply() cuesta un orden de magnitud mas.
    decimals = max(0, -int(np.floor(np.log10(cell_deg))) + 2)
    df["grid_lat"] = np.round(np.floor(lat / cell_deg) * cell_deg, decimals)
    df["grid_lon"] = np.round(np.floor(lon / cell_deg) * cell_deg, decimals)
    df["grid_id"] = (df["grid_lat"].map("{:.4f}".format) + "_"
                     + df["grid_lon"].map("{:.4f}".format))

    # El geohash requiere biseccion secuencial; no se vectoriza de forma simple
    df["geohash"] = [geohash_encode(a, b, precision) for a, b in zip(lat, lon, strict=False)]

    if "start_time" in df.columns:
        ts = df["start_time"]
        df["year"] = ts.dt.year.astype("int16")
        df["month"] = ts.dt.month.astype("int8")
        df["day"] = ts.dt.day.astype("int8")
        df["hour"] = ts.dt.hour.astype("int8")
        df["dow"] = ts.dt.dayofweek.astype("int8")  # 0 = lunes
        df["is_weekend"] = df["dow"].isin((5, 6))

    return df


def _clean_value(value: Any) -> Any:
    """Normaliza NaN/NaT a None para que MongoDB guarde null y no NaN."""
    if value is None:
        return None
    if isinstance(value, float) and not np.isfinite(value):
        return None
    if value is pd.NaT:
        return None
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


def to_documents(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Convierte una particion limpia y enriquecida en documentos GeoJSON.

    El campo `location` es un GeoJSON Point valido: es el que indexa 2dsphere y
    sobre el que operan $near, $geoWithin y $geoNear.
    """
    if df.empty:
        return []

    docs: list[dict[str, Any]] = []
    records = df.to_dict("records")

    for rec in records:
        lat, lon = float(rec["lat"]), float(rec["lon"])
        doc: dict[str, Any] = {
            "accident_id": _clean_value(rec.get("accident_id")),
            # ---- GeoJSON: orden [lon, lat], NO [lat, lon] ----
            "location": to_geojson_point(lat, lon),
            "lat": lat,
            "lon": lon,
            "severity": _clean_value(rec.get("severity")),
            "start_time": _clean_value(rec.get("start_time")),
            "end_time": _clean_value(rec.get("end_time")),
            "city": _clean_value(rec.get("city")),
            "county": _clean_value(rec.get("county")),
            "state": _clean_value(rec.get("state")),
            "distance_mi": _clean_value(rec.get("distance_mi")),
            "temperature_f": _clean_value(rec.get("temperature_f")),
            "visibility_mi": _clean_value(rec.get("visibility_mi")),
            "weather": _clean_value(rec.get("weather")),
            "day_night": _clean_value(rec.get("day_night")),
            "grid_id": _clean_value(rec.get("grid_id")),
            "grid_lat": _clean_value(rec.get("grid_lat")),
            "grid_lon": _clean_value(rec.get("grid_lon")),
            "geohash": _clean_value(rec.get("geohash")),
            "year": _clean_value(rec.get("year")),
            "month": _clean_value(rec.get("month")),
            "day": _clean_value(rec.get("day")),
            "hour": _clean_value(rec.get("hour")),
            "dow": _clean_value(rec.get("dow")),
            "is_weekend": _clean_value(rec.get("is_weekend")),
        }
        docs.append(doc)

    return docs


def grid_cell_for(lat: float, lon: float, cell_deg: float | None = None):
    """Envoltura sobre `geo.grid_cell` para usar la configuracion por defecto."""
    return grid_cell(lat, lon, cell_deg if cell_deg is not None
                     else config.geo.grid_cell_deg)
