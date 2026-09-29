"""Consultas geoespaciales sobre MongoDB.

Requisito 4.3 del enunciado: al menos una consulta por radio con $near, una
dentro de un poligono con $geoWithin y una agregacion con $geoNear. Todas
reciben parametros; no hay ni un solo valor geografico fijo en el codigo.

Todas dependen del indice 2dsphere sobre el campo `location`.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

from bson import ObjectId
from pymongo.database import Database
from pymongo.errors import ExecutionTimeout

from src.common import config
from src.common.geo import meters_to_radians, validate_polygon

log = logging.getLogger(__name__)

# Campos que se devuelven al cliente. Proyectar explicitamente evita mover
# documentos completos por la red en consultas que devuelven miles de puntos.
DEFAULT_PROJECTION: dict[str, int] = {
    "_id": 0, "accident_id": 1, "location": 1, "lat": 1, "lon": 1,
    "severity": 1, "start_time": 1, "city": 1, "county": 1, "state": 1,
    "weather": 1, "distance_mi": 1, "day_night": 1, "hour": 1, "geohash": 1,
    "grid_id": 1,
}


# ---------------------------------------------------------------------------
# Serializacion
# ---------------------------------------------------------------------------
def jsonify_doc(doc: Any) -> Any:
    """Convierte tipos de BSON a tipos serializables en JSON, recursivamente."""
    if isinstance(doc, dict):
        return {k: jsonify_doc(v) for k, v in doc.items()}
    if isinstance(doc, (list, tuple)):
        return [jsonify_doc(v) for v in doc]
    if isinstance(doc, ObjectId):
        return str(doc)
    if isinstance(doc, datetime):
        if doc.tzinfo is None:
            doc = doc.replace(tzinfo=timezone.utc)
        return doc.isoformat()
    return doc


# ---------------------------------------------------------------------------
# Conteos: metadatos opcionales que NUNCA deben tumbar la consulta
# ---------------------------------------------------------------------------
def _safe_count(coll, query: dict[str, Any]) -> tuple[int | None, bool]:
    """Cuenta documentos y devuelve (total, hubo_timeout).

    El total es informativo: el cliente ya tiene los resultados. Un conteo con
    $geoWithin sobre cientos de miles de documentos puede tardar mas que la
    consulta principal, y en un equipo con la cache de WiredTiger recortada
    llega a superar el limite de tiempo.

    Antes esto se propagaba como OperationFailure y el endpoint devolvia un 500
    con los resultados ya calculados y perfectamente validos en la mano. Ahora
    se degrada: total = None y la respuesta lo indica, para que el cliente
    distinga "no hay resultados" de "no se pudo contar".
    """
    try:
        return coll.count_documents(query,
                                    maxTimeMS=config.mongo.count_timeout_ms), False
    except ExecutionTimeout:
        log.warning("El conteo excedio %s ms; se devuelven los resultados sin "
                    "total_matching", config.mongo.count_timeout_ms)
        return None, True


# ---------------------------------------------------------------------------
# Filtros no geograficos reutilizables
# ---------------------------------------------------------------------------
def build_attribute_filter(severity: list[int] | None = None,
                           min_severity: int | None = None,
                           state: str | None = None,
                           city: str | None = None,
                           weather: str | None = None,
                           start_date: datetime | None = None,
                           end_date: datetime | None = None,
                           year: int | None = None,
                           hour_from: int | None = None,
                           hour_to: int | None = None) -> dict[str, Any]:
    """Filtro de atributos que se combina con cualquier operador geoespacial."""
    flt: dict[str, Any] = {}

    if severity:
        flt["severity"] = {"$in": [int(s) for s in severity]}
    elif min_severity is not None:
        flt["severity"] = {"$gte": int(min_severity)}

    if state:
        flt["state"] = state.upper()
    if city:
        # Anclada e insensible a mayusculas; se escapa la entrada para que el
        # usuario no pueda inyectar una expresion regular costosa (ReDoS).
        import re
        flt["city"] = {"$regex": f"^{re.escape(city)}$", "$options": "i"}
    if weather:
        import re
        flt["weather"] = {"$regex": re.escape(weather), "$options": "i"}

    if start_date or end_date:
        rng: dict[str, Any] = {}
        if start_date:
            rng["$gte"] = start_date
        if end_date:
            rng["$lte"] = end_date
        flt["start_time"] = rng

    if year is not None:
        flt["year"] = int(year)

    if hour_from is not None and hour_to is not None:
        if hour_from <= hour_to:
            flt["hour"] = {"$gte": int(hour_from), "$lte": int(hour_to)}
        else:
            # Franja que cruza la medianoche, p. ej. 22:00 a 05:00
            flt["$or"] = [{"hour": {"$gte": int(hour_from)}},
                          {"hour": {"$lte": int(hour_to)}}]

    return flt


# ---------------------------------------------------------------------------
# CONSULTA 1: por radio con $near
# ---------------------------------------------------------------------------
def query_near(db: Database, lat: float, lon: float, radius_m: float,
               limit: int = 100, skip: int = 0,
               min_distance_m: float = 0.0,
               attribute_filter: dict[str, Any] | None = None,
               collection: str | None = None,
               with_count: bool = True) -> dict[str, Any]:
    """Registros dentro de un radio, ORDENADOS por distancia ($near).

    $near devuelve los resultados del mas cercano al mas lejano sin necesidad de
    un $sort explicito, porque el propio indice 2dsphere recorre las celdas en
    orden de proximidad. Es la consulta indicada para "que hay cerca de aqui".
    """
    coll = db[collection or config.mongo.collection]

    geo_clause: dict[str, Any] = {
        "$near": {
            # $geometry con GeoJSON => distancias en METROS sobre la esfera.
            # Con el formato legado [lon, lat] serian radianes, una fuente
            # clasica de errores de tres ordenes de magnitud.
            "$geometry": {"type": "Point", "coordinates": [float(lon), float(lat)]},
            "$maxDistance": float(radius_m),
        }
    }
    if min_distance_m and min_distance_m > 0:
        geo_clause["$near"]["$minDistance"] = float(min_distance_m)

    query: dict[str, Any] = {"location": geo_clause}
    if attribute_filter:
        query.update(attribute_filter)

    t0 = time.perf_counter()
    cursor = (coll.find(query, DEFAULT_PROJECTION)
              .skip(int(skip)).limit(int(limit)))
    results = [jsonify_doc(d) for d in cursor]
    elapsed_ms = (time.perf_counter() - t0) * 1000

    total: int | None = None
    count_timed_out = False
    if with_count:
        # MongoDB NO admite $near dentro de count/aggregate ($match). Para el
        # total se usa $geoWithin + $centerSphere, que delimita exactamente el
        # mismo circulo y si es contable.
        count_query: dict[str, Any] = {
            "location": {
                "$geoWithin": {
                    "$centerSphere": [[float(lon), float(lat)],
                                      meters_to_radians(radius_m)]
                }
            }
        }
        if attribute_filter:
            count_query.update(attribute_filter)
        total, count_timed_out = _safe_count(coll, count_query)

    return {
        "query": {
            "operator": "$near",
            "center": {"lat": float(lat), "lon": float(lon)},
            "radius_m": float(radius_m),
            "min_distance_m": float(min_distance_m or 0),
            "limit": int(limit),
            "skip": int(skip),
            "attribute_filter": jsonify_doc(attribute_filter or {}),
        },
        "returned": len(results),
        "total_matching": total,
        "total_matching_timed_out": count_timed_out,
        "elapsed_ms": round(elapsed_ms, 2),
        "results": results,
    }


# ---------------------------------------------------------------------------
# CONSULTA 2: dentro de un poligono con $geoWithin
# ---------------------------------------------------------------------------
def query_within(db: Database, geometry: dict[str, Any], limit: int = 100,
                 skip: int = 0,
                 attribute_filter: dict[str, Any] | None = None,
                 collection: str | None = None,
                 with_count: bool = True,
                 with_summary: bool = False) -> dict[str, Any]:
    """Registros contenidos en un GeoJSON Polygon / MultiPolygon ($geoWithin).

    Lanza ValueError si la geometria no es valida; MongoDB, ante un anillo sin
    cerrar, devuelve un error poco informativo.
    """
    ok, error = validate_polygon(geometry)
    if not ok:
        raise ValueError(f"Poligono GeoJSON invalido: {error}")

    coll = db[collection or config.mongo.collection]
    query: dict[str, Any] = {"location": {"$geoWithin": {"$geometry": geometry}}}
    if attribute_filter:
        query.update(attribute_filter)

    t0 = time.perf_counter()
    cursor = coll.find(query, DEFAULT_PROJECTION).skip(int(skip)).limit(int(limit))
    results = [jsonify_doc(d) for d in cursor]
    elapsed_ms = (time.perf_counter() - t0) * 1000

    total, count_timed_out = _safe_count(coll, query) if with_count else (None, False)

    summary: dict[str, Any] | None = None
    summary_timed_out = False
    if with_summary:
        # $geoWithin SI es valido dentro de un $match de aggregate, al contrario
        # que $near: eso permite calcular estadisticas del area completa.
        pipeline: list[dict[str, Any]] = [
            {"$match": query},
            {"$group": {
                "_id": None,
                "count": {"$sum": 1},
                "avg_severity": {"$avg": "$severity"},
                "max_severity": {"$max": "$severity"},
                "severe_count": {
                    "$sum": {"$cond": [{"$gte": ["$severity", 3]}, 1, 0]}},
                "avg_distance_mi": {"$avg": "$distance_mi"},
                "first_event": {"$min": "$start_time"},
                "last_event": {"$max": "$start_time"},
                "cities": {"$addToSet": "$city"},
            }},
            {"$project": {
                "_id": 0, "count": 1, "max_severity": 1, "severe_count": 1,
                "first_event": 1, "last_event": 1,
                "avg_severity": {"$round": ["$avg_severity", 4]},
                "avg_distance_mi": {"$round": ["$avg_distance_mi", 4]},
                "distinct_cities": {"$size": "$cities"},
            }},
        ]
        try:
            docs = list(coll.aggregate(
                pipeline, maxTimeMS=config.mongo.count_timeout_ms * 2,
                allowDiskUse=True))
            summary = jsonify_doc(docs[0]) if docs else {"count": 0}
        except ExecutionTimeout:
            # Igual que el conteo: el resumen es opcional y no debe invalidar
            # unos resultados que ya estan calculados.
            log.warning("El resumen del area excedio el limite de tiempo")
            summary = None
            summary_timed_out = True

    return {
        "query": {
            "operator": "$geoWithin",
            "geometry_type": geometry.get("type"),
            "rings": len(geometry.get("coordinates", [])),
            "limit": int(limit),
            "skip": int(skip),
            "attribute_filter": jsonify_doc(attribute_filter or {}),
        },
        "returned": len(results),
        "total_matching": total,
        "total_matching_timed_out": count_timed_out,
        "elapsed_ms": round(elapsed_ms, 2),
        "summary": summary,
        "summary_timed_out": summary_timed_out,
        "results": results,
    }


# ---------------------------------------------------------------------------
# CONSULTA 3: agregacion con $geoNear
# ---------------------------------------------------------------------------
GEONEAR_GROUPS: dict[str, str] = {
    "none": "",
    "severity": "$severity",
    "state": "$state",
    "city": "$city",
    "hour": "$hour",
    "dow": "$dow",
    "weather": "$weather",
    "grid_id": "$grid_id",
    "geohash": "$geohash",
    # El "$" es imprescindible: sin el, MongoDB agrupa por la CADENA
    # literal "_band" en lugar de por el valor del campo, y devuelve un
    # unico grupo con todos los documentos. El campo lo crea la etapa
    # $addFields de mas abajo.
    "distance_band": "$_band",
}


def aggregate_geo_near(db: Database, lat: float, lon: float,
                       max_distance_m: float, group_by: str = "severity",
                       min_distance_m: float = 0.0,
                       band_width_m: float = 1000.0,
                       limit: int = 50,
                       attribute_filter: dict[str, Any] | None = None,
                       collection: str | None = None) -> dict[str, Any]:
    """Agregacion con $geoNear: estadisticas de distancia por grupo.

    $geoNear tiene que ser la PRIMERA etapa del pipeline y aporta algo que
    $near no puede: expone la distancia calculada como un campo
    (`distance_m`), con lo que se pueden promediar distancias, construir
    anillos concentricos y combinar proximidad con agrupaciones.
    """
    if group_by not in GEONEAR_GROUPS:
        raise ValueError(
            f"group_by invalido: '{group_by}'. Valores permitidos: "
            f"{sorted(GEONEAR_GROUPS)}"
        )

    coll = db[collection or config.mongo.collection]

    geo_near: dict[str, Any] = {
        "near": {"type": "Point", "coordinates": [float(lon), float(lat)]},
        "distanceField": "distance_m",
        "maxDistance": float(max_distance_m),
        "spherical": True,          # obligatorio con indice 2dsphere
        "key": "location",
    }
    if min_distance_m and min_distance_m > 0:
        geo_near["minDistance"] = float(min_distance_m)
    if attribute_filter:
        # El filtro va DENTRO de $geoNear, no en un $match posterior: asi
        # MongoDB lo aplica mientras recorre el indice y no despues.
        geo_near["query"] = attribute_filter

    pipeline: list[dict[str, Any]] = [{"$geoNear": geo_near}]

    if group_by == "distance_band":
        pipeline.append({"$addFields": {
            "_band": {"$multiply": [
                {"$floor": {"$divide": ["$distance_m", float(band_width_m)]}},
                float(band_width_m),
            ]}
        }})

    group_key = None if group_by == "none" else GEONEAR_GROUPS[group_by]

    pipeline += [
        {"$group": {
            "_id": group_key,
            "count": {"$sum": 1},
            "avg_distance_m": {"$avg": "$distance_m"},
            "min_distance_m": {"$min": "$distance_m"},
            "max_distance_m": {"$max": "$distance_m"},
            "avg_severity": {"$avg": "$severity"},
            "severe_count": {"$sum": {"$cond": [{"$gte": ["$severity", 3]}, 1, 0]}},
            "nearest": {"$first": {
                "accident_id": "$accident_id", "city": "$city",
                "state": "$state", "severity": "$severity",
                "distance_m": "$distance_m", "location": "$location",
            }},
        }},
        {"$project": {
            "_id": 0,
            "group": "$_id",
            "count": 1,
            "severe_count": 1,
            "nearest": 1,
            "avg_distance_m": {"$round": ["$avg_distance_m", 2]},
            "min_distance_m": {"$round": ["$min_distance_m", 2]},
            "max_distance_m": {"$round": ["$max_distance_m", 2]},
            "avg_severity": {"$round": ["$avg_severity", 4]},
            "severe_pct": {"$round": [
                {"$multiply": [100, {"$divide": ["$severe_count", "$count"]}]}, 3]},
        }},
        {"$sort": {"count": -1}},
        {"$limit": int(limit)},
    ]

    t0 = time.perf_counter()
    groups = [jsonify_doc(d) for d in coll.aggregate(
        pipeline, maxTimeMS=60_000, allowDiskUse=True)]
    elapsed_ms = (time.perf_counter() - t0) * 1000

    return {
        "query": {
            "operator": "$geoNear",
            "center": {"lat": float(lat), "lon": float(lon)},
            "max_distance_m": float(max_distance_m),
            "min_distance_m": float(min_distance_m or 0),
            "group_by": group_by,
            "band_width_m": float(band_width_m) if group_by == "distance_band" else None,
            "attribute_filter": jsonify_doc(attribute_filter or {}),
        },
        "groups_returned": len(groups),
        "total_in_radius": sum(g.get("count", 0) for g in groups),
        "elapsed_ms": round(elapsed_ms, 2),
        "groups": groups,
        "pipeline": jsonify_doc(pipeline),  # transparencia: se expone el pipeline
    }


# ---------------------------------------------------------------------------
# Resultados calculados por Spark
# ---------------------------------------------------------------------------
AGGREGATION_COLLECTIONS: dict[str, tuple[str, str, int]] = {
    # alias           -> (coleccion, campo de orden, direccion)
    "grid":     (config.mongo.grid_collection, "count", -1),
    "geohash":  (config.mongo.geohash_collection, "count", -1),
    "hotspots": (config.mongo.hotspot_collection, "rank", 1),
    "temporal": (config.mongo.temporal_collection, "bucket", 1),
    "state":    (config.mongo.state_collection, "count", -1),
}


def query_aggregation(db: Database, name: str, limit: int = 100, skip: int = 0,
                      dimension: str | None = None,
                      min_count: int | None = None,
                      bbox: tuple[float, float, float, float] | None = None,
                      sort_by: str | None = None,
                      order: int = -1) -> dict[str, Any]:
    """Lee las colecciones que produjo Spark, con filtros opcionales.

    `bbox` = (min_lon, min_lat, max_lon, max_lat) filtra por $geoWithin sobre el
    centroide, lo que permite pedir "la grilla de esta zona del mapa".
    """
    if name not in AGGREGATION_COLLECTIONS:
        raise ValueError(
            f"Agregacion desconocida: '{name}'. Disponibles: "
            f"{sorted(AGGREGATION_COLLECTIONS)}"
        )

    coll_name, default_sort, default_dir = AGGREGATION_COLLECTIONS[name]
    coll = db[coll_name]

    query: dict[str, Any] = {}
    if dimension:
        query["dimension"] = dimension
    if min_count is not None:
        query["count"] = {"$gte": int(min_count)}
    if bbox:
        from src.common.geo import bbox_to_polygon
        query["centroid"] = {"$geoWithin": {"$geometry": bbox_to_polygon(*bbox)}}

    sort_field = sort_by or default_sort
    sort_dir = order if sort_by else default_dir

    t0 = time.perf_counter()
    cursor = (coll.find(query, {"_id": 0})
              .sort(sort_field, sort_dir).skip(int(skip)).limit(int(limit)))
    results = [jsonify_doc(d) for d in cursor]
    elapsed_ms = (time.perf_counter() - t0) * 1000

    return {
        "aggregation": name,
        "collection": coll_name,
        "source": "Spark (MongoDB Spark Connector)",
        "filter": jsonify_doc(query),
        "sort": {"field": sort_field, "order": sort_dir},
        "returned": len(results),
        "total_matching": _safe_count(coll, query)[0],
        "elapsed_ms": round(elapsed_ms, 2),
        "results": results,
    }
