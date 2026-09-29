"""Configuracion central del sistema.

Todo se lee de variables de entorno para que el mismo codigo corra en local,
en los contenedores de Docker Compose y en Jenkins sin cambios. Ningun valor
sensible tiene un default util: si falta, el sistema falla temprano y claro.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = _env(name)
    try:
        return float(raw) if raw else default
    except ValueError:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = _env(name).lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on", "si"}


@dataclass(frozen=True)
class MongoConfig:
    uri: str = field(default_factory=lambda: _env(
        "MONGO_URI", "mongodb://localhost:27017/geobigdata"))
    database: str = field(default_factory=lambda: _env("MONGO_DB", "geobigdata"))
    collection: str = field(default_factory=lambda: _env("MONGO_COLLECTION", "accidents"))

    # Colecciones derivadas que produce Spark
    grid_collection: str = "agg_grid"
    geohash_collection: str = "agg_geohash"
    temporal_collection: str = "agg_temporal"
    hotspot_collection: str = "agg_hotspots"
    state_collection: str = "agg_state"
    benchmark_collection: str = "benchmark_runs"


@dataclass(frozen=True)
class KaggleConfig:
    dataset: str = field(default_factory=lambda: _env(
        "KAGGLE_DATASET", "sobhanmoosavi/us-accidents"))
    target_file: str = field(default_factory=lambda: _env(
        "KAGGLE_FILE", "US_Accidents_March23.csv"))
    data_dir: str = field(default_factory=lambda: _env("DATA_DIR", "/data"))
    allow_synthetic: bool = field(
        default_factory=lambda: _env_bool("ALLOW_SYNTHETIC_FALLBACK", True))


@dataclass(frozen=True)
class IngestConfig:
    sample_size: int = field(default_factory=lambda: _env_int("SAMPLE_SIZE", 2_000_000))
    batch_size: int = field(default_factory=lambda: _env_int("BATCH_SIZE", 20_000))
    blocksize: str = field(default_factory=lambda: _env("DASK_BLOCKSIZE", "64MB"))
    scheduler: str = field(default_factory=lambda: _env(
        "DASK_SCHEDULER", "tcp://dask-scheduler:8786"))


@dataclass(frozen=True)
class SparkConfig:
    master: str = field(default_factory=lambda: _env(
        "SPARK_MASTER_URL", "spark://spark-master:7077"))
    executor_memory: str = field(default_factory=lambda: _env("SPARK_EXECUTOR_MEMORY", "2g"))
    driver_memory: str = field(default_factory=lambda: _env("SPARK_DRIVER_MEMORY", "2g"))


@dataclass(frozen=True)
class GeoConfig:
    """Parametros del analisis espacial."""
    grid_cell_deg: float = field(default_factory=lambda: _env_float("GRID_CELL_DEG", 0.1))
    geohash_precision: int = field(default_factory=lambda: _env_int("GEOHASH_PRECISION", 5))


@dataclass(frozen=True)
class ApiConfig:
    host: str = field(default_factory=lambda: _env("API_HOST", "0.0.0.0"))
    port: int = field(default_factory=lambda: _env_int("API_PORT", 5000))
    default_limit: int = field(default_factory=lambda: _env_int("API_DEFAULT_LIMIT", 100))
    max_limit: int = field(default_factory=lambda: _env_int("API_MAX_LIMIT", 1000))


mongo = MongoConfig()
kaggle = KaggleConfig()
ingest = IngestConfig()
spark = SparkConfig()
geo = GeoConfig()
api = ApiConfig()

__all__ = ["mongo", "kaggle", "ingest", "spark", "geo", "api",
           "MongoConfig", "KaggleConfig", "IngestConfig", "SparkConfig",
           "GeoConfig", "ApiConfig"]
