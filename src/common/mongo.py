"""Acceso a MongoDB: conexion resiliente, indices e inicializacion del esquema.

Un solo lugar crea los indices, para que la API, la ingesta con Dask y el
procesamiento con Spark trabajen siempre sobre el mismo modelo de datos.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from pymongo import ASCENDING, DESCENDING, GEOSPHERE, MongoClient
from pymongo.database import Database
from pymongo.errors import OperationFailure, ServerSelectionTimeoutError

from src.common import config

log = logging.getLogger(__name__)

_client: MongoClient | None = None


def get_client(uri: str | None = None, *, retries: int | None = None,
               delay: float | None = None,
               timeout_ms: int | None = None) -> MongoClient:
    """Cliente de MongoDB con reintentos.

    Los reintentos existen porque en `docker compose up` la ingesta arranca
    mientras Mongo todavia esta inicializando su usuario de autenticacion; sin
    esperar, el primer arranque falla siempre.

    El presupuesto de espera NO es el mismo en todos los servicios, y eso
    importa:

    * Un trabajo por lotes (ingesta, Spark) puede y debe esperar minutos: si
      Mongo tarda en arrancar, lo correcto es aguantar, no abortar el pipeline.
    * La API NO puede esperar. Si Mongo esta caido, cada peticion debe fallar
      rapido con un 503; con el presupuesto largo, una peticion se colgaria
      varios minutos y agotaria los workers de Gunicorn con la base caida.

    De ahi que los valores sean configurables y que `src/api/app.py` use un
    presupuesto corto.
    """
    global _client
    target = uri or config.mongo.uri
    retries = retries if retries is not None else config.mongo.connect_retries
    delay = delay if delay is not None else config.mongo.connect_delay
    timeout_ms = timeout_ms if timeout_ms is not None else config.mongo.select_timeout_ms

    if _client is not None:
        try:
            _client.admin.command("ping")
            return _client
        except Exception:  # la conexion cacheada murio; se reconstruye
            _client = None

    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            client: MongoClient = MongoClient(
                target,
                serverSelectionTimeoutMS=timeout_ms,
                connectTimeoutMS=timeout_ms,
                socketTimeoutMS=120_000,
                retryWrites=True,
                maxPoolSize=50,
                tz_aware=True,
            )
            client.admin.command("ping")
            log.info("Conectado a MongoDB (intento %s/%s)", attempt, retries)
            _client = client
            return client
        except (ServerSelectionTimeoutError, OperationFailure) as exc:
            last_error = exc
            log.warning("MongoDB no disponible (intento %s/%s): %s",
                        attempt, retries, type(exc).__name__)
            time.sleep(delay)

    raise RuntimeError(
        f"No fue posible conectar a MongoDB despues de {retries} intentos"
    ) from last_error


def get_db(uri: str | None = None, *, retries: int | None = None,
           delay: float | None = None,
           timeout_ms: int | None = None) -> Database:
    """Base de datos de trabajo. Ver `get_client` sobre el presupuesto de espera."""
    return get_client(uri, retries=retries, delay=delay,
                      timeout_ms=timeout_ms)[config.mongo.database]


def close_client() -> None:
    global _client
    if _client is not None:
        _client.close()
        _client = None


# ---------------------------------------------------------------------------
# Indices
# ---------------------------------------------------------------------------
def ensure_indexes(db: Database | None = None) -> dict[str, list[str]]:
    """Crea (idempotentemente) todos los indices del modelo.

    El indice 2dsphere sobre `location` es obligatorio: sin el, $near y
    $geoNear fallan con error y $geoWithin degrada a escaneo completo.
    """
    db = db if db is not None else get_db()
    created: dict[str, list[str]] = {}

    main = db[config.mongo.collection]
    created[config.mongo.collection] = [
        # Indice geoespacial esferico: habilita $near / $geoNear / $geoWithin
        main.create_index([("location", GEOSPHERE)], name="ix_location_2dsphere"),
        # Filtros temporales y combinados que exponen los endpoints
        main.create_index([("start_time", DESCENDING)], name="ix_start_time"),
        main.create_index([("severity", ASCENDING)], name="ix_severity"),
        main.create_index([("state", ASCENDING), ("severity", ASCENDING)],
                          name="ix_state_severity"),
        main.create_index([("year", ASCENDING), ("month", ASCENDING)],
                          name="ix_year_month"),
        # Clave natural del dataset: evita duplicados al reejecutar la ingesta
        main.create_index([("accident_id", ASCENDING)], name="ux_accident_id",
                          unique=True, sparse=True),
        # Soporta agrupaciones por celda sin recalcular
        main.create_index([("grid_id", ASCENDING)], name="ix_grid_id"),
        main.create_index([("geohash", ASCENDING)], name="ix_geohash"),
    ]

    grid = db[config.mongo.grid_collection]
    created[config.mongo.grid_collection] = [
        grid.create_index([("centroid", GEOSPHERE)], name="ix_centroid_2dsphere"),
        grid.create_index([("count", DESCENDING)], name="ix_count"),
        grid.create_index([("grid_id", ASCENDING)], name="ux_grid_id", unique=True),
    ]

    gh = db[config.mongo.geohash_collection]
    created[config.mongo.geohash_collection] = [
        gh.create_index([("centroid", GEOSPHERE)], name="ix_centroid_2dsphere"),
        gh.create_index([("count", DESCENDING)], name="ix_count"),
        gh.create_index([("geohash", ASCENDING)], name="ux_geohash", unique=True),
    ]

    hot = db[config.mongo.hotspot_collection]
    created[config.mongo.hotspot_collection] = [
        hot.create_index([("centroid", GEOSPHERE)], name="ix_centroid_2dsphere"),
        hot.create_index([("rank", ASCENDING)], name="ix_rank"),
    ]

    temporal = db[config.mongo.temporal_collection]
    created[config.mongo.temporal_collection] = [
        temporal.create_index([("dimension", ASCENDING), ("bucket", ASCENDING)],
                              name="ux_dim_bucket", unique=True),
    ]

    state = db[config.mongo.state_collection]
    created[config.mongo.state_collection] = [
        state.create_index([("state", ASCENDING)], name="ux_state", unique=True),
    ]

    bench = db[config.mongo.benchmark_collection]
    created[config.mongo.benchmark_collection] = [
        bench.create_index([("run_id", ASCENDING)], name="ix_run_id"),
        bench.create_index([("engine", ASCENDING), ("workers", ASCENDING)],
                           name="ix_engine_workers"),
    ]

    log.info("Indices verificados en %s colecciones", len(created))
    return created


def has_geo_index(db: Database, collection: str,
                  field: str = "location") -> bool:
    """True si la coleccion tiene un indice 2dsphere sobre `field`."""
    try:
        for spec in db[collection].list_indexes():
            for key, direction in spec.get("key", {}).items():
                if key == field and direction == "2dsphere":
                    return True
    except OperationFailure:
        return False
    return False


def collection_stats(db: Database | None = None) -> dict[str, Any]:
    """Resumen de estado usado por /api/v1/stats y por el smoke test de Jenkins."""
    db = db if db is not None else get_db()
    names = set(db.list_collection_names())
    out: dict[str, Any] = {"database": db.name, "collections": {}}

    for coll in (config.mongo.collection, config.mongo.grid_collection,
                 config.mongo.geohash_collection, config.mongo.temporal_collection,
                 config.mongo.hotspot_collection, config.mongo.state_collection,
                 config.mongo.benchmark_collection):
        out["collections"][coll] = {
            "exists": coll in names,
            "count": db[coll].estimated_document_count() if coll in names else 0,
        }

    out["geo_index_2dsphere"] = has_geo_index(db, config.mongo.collection)
    return out
