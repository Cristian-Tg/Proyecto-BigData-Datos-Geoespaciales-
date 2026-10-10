"""API REST en Flask para consultar los datos geoespaciales.

Requisito 4.4 del enunciado: al menos tres endpoints.

  1. GET  /api/v1/near                 lat, lon, radio  -> registros cercanos
  2. POST /api/v1/within               poligono GeoJSON -> registros contenidos
  3. GET  /api/v1/aggregations/<name>  resultados calculados por Spark

Adicionales: /api/v1/geonear (agregacion $geoNear), /api/v1/stats,
/api/v1/health, /api/v1/docs y un mapa Leaflet en /.
"""
from __future__ import annotations

import os
import platform
import re
import time
from datetime import datetime, timezone
from typing import Any

from flask import Flask, Response, jsonify, request
from pymongo.errors import OperationFailure, PyMongoError
from werkzeug.exceptions import HTTPException

from src.api import queries
from src.common import config
from src.common.geo import bbox_to_polygon
from src.common.logging_conf import setup_logging
from src.common.mongo import collection_stats, ensure_indexes, get_db

log = setup_logging("api")

START_TIME = time.time()
API_PREFIX = "/api/v1"

# ---------------------------------------------------------------------------
# Presupuesto de espera al conectar a MongoDB
# ---------------------------------------------------------------------------
# La API falla RAPIDO a proposito. El valor por defecto de `get_client` (30
# reintentos) es el correcto para un trabajo por lotes que espera a que Mongo
# arranque, pero seria nefasto aqui: con la base caida, cada peticion se
# colgaria minutos reintentando y agotaria los workers de Gunicorn en lugar de
# devolver un 503 inmediato.
API_DB_RETRIES = int(os.environ.get("API_MONGO_RETRIES", "2"))
API_DB_DELAY = float(os.environ.get("API_MONGO_DELAY", "1"))
API_DB_TIMEOUT_MS = int(os.environ.get("API_MONGO_TIMEOUT_MS", "3000"))

# En el arranque si se puede esperar algo mas: `depends_on: service_healthy` ya
# garantiza que Mongo responde, pero la creacion del usuario puede ir un paso
# por detras del healthcheck. Se acota a 5 (~35 s) para no agotar la paciencia
# de Gunicorn esperando a que arranque el worker; si falla, la API arranca igual
# y el primer /health lo reporta como degradado.
STARTUP_DB_RETRIES = int(os.environ.get("API_MONGO_STARTUP_RETRIES", "5"))


def _db():
    """Base de datos con el presupuesto corto de la API."""
    return get_db(retries=API_DB_RETRIES, delay=API_DB_DELAY,
                  timeout_ms=API_DB_TIMEOUT_MS)


# ---------------------------------------------------------------------------
# Validacion de parametros
# ---------------------------------------------------------------------------
class BadParam(ValueError):
    """Parametro de entrada invalido -> se traduce a HTTP 400."""


def _float_param(name: str, *, required: bool = True,
                 default: float | None = None,
                 minimum: float | None = None,
                 maximum: float | None = None,
                 source: dict[str, Any] | None = None) -> float | None:
    raw = (source or request.args).get(name)
    if raw is None or raw == "":
        if required:
            raise BadParam(f"Falta el parametro obligatorio '{name}'")
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        raise BadParam(f"'{name}' debe ser numerico, se recibio {raw!r}") from None
    if minimum is not None and value < minimum:
        raise BadParam(f"'{name}' debe ser >= {minimum}, se recibio {value}")
    if maximum is not None and value > maximum:
        raise BadParam(f"'{name}' debe ser <= {maximum}, se recibio {value}")
    return value


def _int_param(name: str, *, required: bool = False,
               default: int | None = None,
               minimum: int | None = None,
               maximum: int | None = None,
               source: dict[str, Any] | None = None) -> int | None:
    raw = (source or request.args).get(name)
    if raw is None or raw == "":
        if required:
            raise BadParam(f"Falta el parametro obligatorio '{name}'")
        return default
    try:
        value = int(float(raw))
    except (TypeError, ValueError):
        raise BadParam(f"'{name}' debe ser entero, se recibio {raw!r}") from None
    if minimum is not None and value < minimum:
        raise BadParam(f"'{name}' debe ser >= {minimum}, se recibio {value}")
    if maximum is not None and value > maximum:
        raise BadParam(f"'{name}' debe ser <= {maximum}, se recibio {value}")
    return value


def _date_param(name: str, source: dict[str, Any] | None = None
                ) -> datetime | None:
    raw = (source or request.args).get(name)
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise BadParam(f"'{name}' debe tener formato ISO (AAAA-MM-DD), se recibio {raw!r}")


def _limit_param(source: dict[str, Any] | None = None) -> int:
    return int(_int_param("limit", default=config.api.default_limit, minimum=1,
                          maximum=config.api.max_limit, source=source))


def _severity_list(source: dict[str, Any] | None = None) -> list[int] | None:
    """Acepta ?severity=3 y ?severity=2,3,4."""
    raw = (source or request.args).get("severity")
    if not raw:
        return None
    if isinstance(raw, (list, tuple)):
        parts = [str(p) for p in raw]
    else:
        parts = [p.strip() for p in str(raw).split(",") if p.strip()]
    out: list[int] = []
    for part in parts:
        try:
            value = int(part)
        except ValueError:
            raise BadParam(f"'severity' debe ser un entero 1..4, se recibio "
                           f"{part!r}") from None
        if value not in (1, 2, 3, 4):
            raise BadParam(f"'severity' debe estar entre 1 y 4, se recibio {value}")
        out.append(value)
    return out


def _attribute_filter(source: dict[str, Any] | None = None) -> dict[str, Any]:
    """Construye el filtro de atributos a partir de los parametros comunes."""
    src = source if source is not None else request.args
    return queries.build_attribute_filter(
        severity=_severity_list(src),
        min_severity=_int_param("min_severity", minimum=1, maximum=4, source=src),
        state=src.get("state") or None,
        city=src.get("city") or None,
        weather=src.get("weather") or None,
        start_date=_date_param("start_date", src),
        end_date=_date_param("end_date", src),
        year=_int_param("year", minimum=1900, maximum=2100, source=src),
        hour_from=_int_param("hour_from", minimum=0, maximum=23, source=src),
        hour_to=_int_param("hour_to", minimum=0, maximum=23, source=src),
    )


# ---------------------------------------------------------------------------
# Fabrica de la aplicacion
# ---------------------------------------------------------------------------
def create_app(ensure_idx: bool | None = None) -> Flask:
    app = Flask(__name__, static_folder="static", static_url_path="/static")
    app.config["JSON_SORT_KEYS"] = False
    app.json.sort_keys = False

    if ensure_idx is None:
        ensure_idx = os.environ.get("API_ENSURE_INDEXES", "1").lower() in {
            "1", "true", "yes"}

    if ensure_idx:
        # Se hace en el arranque y no en la primera peticion: si el modelo de
        # datos esta mal, es mejor descubrirlo al levantar el contenedor.
        try:
            ensure_indexes(get_db(retries=STARTUP_DB_RETRIES, delay=2.0))
            log.info("Indices verificados en el arranque de la API")
        except Exception as exc:  # noqa: BLE001 - la API debe arrancar igual
            log.error("No fue posible verificar los indices: %s", exc)

    # -----------------------------------------------------------------------
    # Middleware
    # -----------------------------------------------------------------------
    @app.after_request
    def _headers(response: Response) -> Response:
        # CORS abierto en lectura: el mapa Leaflet puede servirse desde otro host
        response.headers.setdefault("Access-Control-Allow-Origin", "*")
        response.headers.setdefault("Access-Control-Allow-Headers", "Content-Type")
        response.headers.setdefault("Access-Control-Allow-Methods",
                                    "GET, POST, OPTIONS")
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        return response

    # -----------------------------------------------------------------------
    # Manejo de errores: siempre JSON, nunca una pagina HTML de Flask
    # -----------------------------------------------------------------------
    @app.errorhandler(BadParam)
    def _bad_param(exc: BadParam):
        return jsonify({"error": "parametro_invalido", "message": str(exc)}), 400

    @app.errorhandler(ValueError)
    def _value_error(exc: ValueError):
        return jsonify({"error": "valor_invalido", "message": str(exc)}), 400

    @app.errorhandler(OperationFailure)
    def _mongo_op(exc: OperationFailure):
        message = str(exc)
        # El sintoma mas comun en este sistema: falta el indice 2dsphere
        if "unable to find index for $geoNear" in message or "2dsphere" in message:
            return jsonify({
                "error": "falta_indice_geoespacial",
                "message": ("La coleccion no tiene indice 2dsphere sobre "
                            "'location'. Ejecute la ingesta o POST "
                            f"{API_PREFIX}/admin/reindex."),
                "detail": message,
            }), 503
        log.exception("Error de MongoDB")
        return jsonify({"error": "error_mongodb", "message": message}), 500

    @app.errorhandler(PyMongoError)
    def _mongo_err(exc: PyMongoError):
        log.exception("Error de MongoDB")
        return jsonify({"error": "error_mongodb", "message": str(exc)}), 503

    @app.errorhandler(HTTPException)
    def _http_err(exc: HTTPException):
        return jsonify({"error": exc.name, "message": exc.description}), exc.code

    @app.errorhandler(Exception)
    def _unexpected(exc: Exception):
        log.exception("Error no controlado")
        return jsonify({"error": "error_interno", "message": str(exc)}), 500

    # =======================================================================
    # Salud y metadatos
    # =======================================================================
    @app.get(f"{API_PREFIX}/health")
    def health():
        """Sonda de salud. La usa el healthcheck de Docker y Jenkins."""
        payload: dict[str, Any] = {
            "status": "ok",
            "service": "geobigdata-api",
            "uptime_seconds": round(time.time() - START_TIME, 1),
            "python": platform.python_version(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        try:
            db = _db()
            db.client.admin.command("ping")
            payload["mongodb"] = "ok"
            payload["database"] = db.name
        except Exception as exc:  # noqa: BLE001
            payload["status"] = "degraded"
            payload["mongodb"] = f"error: {exc}"
            return jsonify(payload), 503
        return jsonify(payload)

    @app.get(f"{API_PREFIX}/stats")
    def stats():
        """Conteos por coleccion y estado del indice 2dsphere."""
        db = _db()
        payload = collection_stats(db)
        payload["config"] = {
            "grid_cell_deg": config.geo.grid_cell_deg,
            "geohash_precision": config.geo.geohash_precision,
            "default_limit": config.api.default_limit,
            "max_limit": config.api.max_limit,
        }
        main = payload["collections"][config.mongo.collection]
        payload["ready_for_queries"] = bool(
            main["count"] > 0 and payload["geo_index_2dsphere"])
        return jsonify(payload)

    @app.get(f"{API_PREFIX}/docs")
    def docs():
        """Catalogo de endpoints, para no tener que abrir el README."""
        return jsonify({
            "service": "API geoespacial de accidentes (Big Data)",
            "version": "1.0",
            "dataset": config.kaggle.dataset,
            "endpoints": [
                {"method": "GET", "path": f"{API_PREFIX}/health",
                 "description": "Estado del servicio y de MongoDB"},
                {"method": "GET", "path": f"{API_PREFIX}/stats",
                 "description": "Conteos por coleccion e indice 2dsphere"},
                {"method": "GET", "path": f"{API_PREFIX}/near",
                 "operator": "$near",
                 "description": "Registros dentro de un radio, ordenados por distancia",
                 "required": ["lat", "lon", "radius_m"],
                 "optional": ["limit", "skip", "min_distance_m", "severity",
                              "min_severity", "state", "city", "weather",
                              "start_date", "end_date", "year", "hour_from",
                              "hour_to", "count"],
                 "example": (f"{API_PREFIX}/near?lat=34.0522&lon=-118.2437"
                             "&radius_m=5000&limit=50&min_severity=3")},
                {"method": "POST", "path": f"{API_PREFIX}/within",
                 "operator": "$geoWithin",
                 "description": "Registros dentro de un GeoJSON Polygon/MultiPolygon",
                 "body": {"geometry": {"type": "Polygon", "coordinates": "[[[lon,lat],...]]"},
                          "limit": 100, "summary": True},
                 "note": "Tambien acepta {'bbox':[min_lon,min_lat,max_lon,max_lat]}"},
                {"method": "GET", "path": f"{API_PREFIX}/within",
                 "operator": "$geoWithin",
                 "description": "Variante con bounding box por query string",
                 "required": ["min_lon", "min_lat", "max_lon", "max_lat"]},
                {"method": "GET", "path": f"{API_PREFIX}/geonear",
                 "operator": "$geoNear",
                 "description": "Agregacion por proximidad con estadisticas de distancia",
                 "required": ["lat", "lon", "max_distance_m"],
                 "optional": ["group_by", "min_distance_m", "band_width_m", "limit"],
                 "group_by_values": sorted(queries.GEONEAR_GROUPS)},
                {"method": "GET", "path": f"{API_PREFIX}/aggregations",
                 "description": "Lista las agregaciones calculadas por Spark"},
                {"method": "GET", "path": f"{API_PREFIX}/aggregations/<name>",
                 "description": "Resultados calculados por Spark",
                 "names": sorted(queries.AGGREGATION_COLLECTIONS),
                 "optional": ["limit", "skip", "dimension", "min_count",
                              "sort_by", "order", "bbox"]},
                {"method": "GET", "path": f"{API_PREFIX}/benchmark",
                 "description": "Resultados de la comparacion Dask vs Spark"},
                {"method": "POST", "path": f"{API_PREFIX}/admin/reindex",
                 "description": "Reconstruye los indices, 2dsphere incluido"},
                {"method": "GET", "path": "/",
                 "description": "Mapa interactivo con Leaflet"},
            ],
        })

    # =======================================================================
    # ENDPOINT 1 (obligatorio): consulta por radio con $near
    # =======================================================================
    @app.get(f"{API_PREFIX}/near")
    def near():
        lat = _float_param("lat", minimum=-90, maximum=90)
        lon = _float_param("lon", minimum=-180, maximum=180)
        # Se aceptan radius_m y radius_km; el enunciado solo pide "radio"
        radius_m = _float_param("radius_m", required=False, minimum=1,
                                maximum=2_000_000)
        if radius_m is None:
            radius_km = _float_param("radius_km", required=False, minimum=0.001,
                                     maximum=2_000)
            if radius_km is None:
                raise BadParam("Debe indicar 'radius_m' o 'radius_km'")
            radius_m = radius_km * 1000.0

        result = queries.query_near(
            _db(),
            lat=lat, lon=lon, radius_m=radius_m,
            limit=_limit_param(),
            skip=int(_int_param("skip", default=0, minimum=0)),
            min_distance_m=float(_float_param("min_distance_m", required=False,
                                              default=0.0, minimum=0)),
            attribute_filter=_attribute_filter(),
            with_count=request.args.get("count", "1").lower() not in
            {"0", "false", "no"},
        )
        return jsonify(result)

    # =======================================================================
    # ENDPOINT 2 (obligatorio): poligono con $geoWithin
    # =======================================================================
    @app.route(f"{API_PREFIX}/within", methods=["POST", "OPTIONS"])
    def within_post():
        if request.method == "OPTIONS":
            return ("", 204)

        body: dict[str, Any] = request.get_json(silent=True) or {}
        if not body:
            raise BadParam(
                "Se esperaba un cuerpo JSON con 'geometry' (GeoJSON Polygon) o "
                "'bbox'. Recuerde enviar el header Content-Type: application/json."
            )

        geometry = body.get("geometry")

        # Comodidad: se acepta un Feature de GeoJSON completo tal como lo
        # exporta geojson.io o el dibujo de Leaflet.Draw
        if geometry is None and body.get("type") == "Feature":
            geometry = body.get("geometry")
        if geometry is None and body.get("type") in ("Polygon", "MultiPolygon"):
            geometry = body
        if geometry is None and body.get("bbox"):
            bbox = body["bbox"]
            if not isinstance(bbox, (list, tuple)) or len(bbox) != 4:
                raise BadParam("'bbox' debe ser [min_lon, min_lat, max_lon, max_lat]")
            geometry = bbox_to_polygon(*[float(v) for v in bbox])

        if geometry is None:
            raise BadParam("Falta 'geometry' (GeoJSON Polygon/MultiPolygon) o 'bbox'")

        result = queries.query_within(
            _db(),
            geometry=geometry,
            limit=_limit_param(body),
            skip=int(_int_param("skip", default=0, minimum=0, source=body)),
            attribute_filter=_attribute_filter(body),
            with_count=str(body.get("count", True)).lower() not in
            {"0", "false", "no"},
            with_summary=str(body.get("summary", True)).lower() not in
            {"0", "false", "no"},
        )
        return jsonify(result)

    @app.get(f"{API_PREFIX}/within")
    def within_get():
        """Variante con bounding box, comoda para probar desde el navegador."""
        min_lon = _float_param("min_lon", minimum=-180, maximum=180)
        min_lat = _float_param("min_lat", minimum=-90, maximum=90)
        max_lon = _float_param("max_lon", minimum=-180, maximum=180)
        max_lat = _float_param("max_lat", minimum=-90, maximum=90)
        if min_lon >= max_lon or min_lat >= max_lat:
            raise BadParam("El bounding box debe cumplir min_lon<max_lon y "
                           "min_lat<max_lat")

        result = queries.query_within(
            _db(),
            geometry=bbox_to_polygon(min_lon, min_lat, max_lon, max_lat),
            limit=_limit_param(),
            skip=int(_int_param("skip", default=0, minimum=0)),
            attribute_filter=_attribute_filter(),
            with_summary=request.args.get("summary", "1").lower() not in
            {"0", "false", "no"},
        )
        return jsonify(result)

    # =======================================================================
    # Agregacion con $geoNear
    # =======================================================================
    @app.get(f"{API_PREFIX}/geonear")
    def geonear():
        max_distance_m = _float_param("max_distance_m", required=False,
                                      minimum=1, maximum=2_000_000)
        if max_distance_m is None:
            max_distance_m = _float_param("radius_m", minimum=1,
                                          maximum=2_000_000)

        result = queries.aggregate_geo_near(
            _db(),
            lat=_float_param("lat", minimum=-90, maximum=90),
            lon=_float_param("lon", minimum=-180, maximum=180),
            max_distance_m=max_distance_m,
            group_by=request.args.get("group_by", "severity"),
            min_distance_m=float(_float_param("min_distance_m", required=False,
                                              default=0.0, minimum=0)),
            band_width_m=float(_float_param("band_width_m", required=False,
                                            default=1000.0, minimum=1)),
            limit=int(_int_param("limit", default=50, minimum=1, maximum=500)),
            attribute_filter=_attribute_filter(),
        )
        return jsonify(result)

    # =======================================================================
    # ENDPOINT 3 (obligatorio): resultados calculados con Spark
    # =======================================================================
    @app.get(f"{API_PREFIX}/aggregations")
    def aggregations_index():
        db = _db()
        names = set(db.list_collection_names())
        available = []
        for alias, (coll, _sort, _dir) in queries.AGGREGATION_COLLECTIONS.items():
            available.append({
                "name": alias,
                "collection": coll,
                "exists": coll in names,
                "documents": db[coll].estimated_document_count() if coll in names else 0,
                "url": f"{API_PREFIX}/aggregations/{alias}",
            })
        return jsonify({
            "source": "Spark (MongoDB Spark Connector)",
            "aggregations": available,
            "hint": ("Si estan vacias, ejecute el procesamiento con Spark: "
                     "docker compose run --rm spark-submit"),
        })

    @app.get(f"{API_PREFIX}/aggregations/<name>")
    def aggregation_detail(name: str):
        bbox: tuple[float, float, float, float] | None = None
        raw_bbox = request.args.get("bbox")
        if raw_bbox:
            parts = [p.strip() for p in raw_bbox.split(",")]
            if len(parts) != 4:
                raise BadParam("'bbox' debe ser "
                               "min_lon,min_lat,max_lon,max_lat")
            try:
                bbox = tuple(float(p) for p in parts)  # type: ignore[assignment]
            except ValueError:
                raise BadParam("'bbox' debe contener cuatro numeros") from None

        result = queries.query_aggregation(
            _db(), name=name,
            limit=_limit_param(),
            skip=int(_int_param("skip", default=0, minimum=0)),
            dimension=request.args.get("dimension") or None,
            min_count=_int_param("min_count", minimum=0),
            bbox=bbox,
            sort_by=request.args.get("sort_by") or None,
            order=-1 if request.args.get("order", "desc").lower().startswith("d") else 1,
        )
        return jsonify(result)

    # =======================================================================
    # Accidentes por severidad en un estado
    # =======================================================================
    @app.get(f"{API_PREFIX}/severity_by_state")
    def severity_by_state():
        state = request.args.get("state", "").strip().upper()
        if len(state) != 2:
            raise ValueError("Indique 'state' con el codigo de 2 letras, p. ej. CA")
        db = _db()
        pipeline = [
            {"$match": {"state": state}},
            {"$group": {"_id": "$severity", "n": {"$sum": 1}}},
            {"$sort": {"_id": 1}},
        ]
        grupos = [{"severity": d["_id"], "n": d["n"]}
                  for d in db[config.mongo.collection].aggregate(pipeline)]
        return jsonify({"state": state,
                        "total": sum(g["n"] for g in grupos),
                        "por_severidad": grupos})


    # =======================================================================
    # Benchmark Dask vs Spark
    # =======================================================================
    @app.get(f"{API_PREFIX}/benchmark")
    def benchmark():
        db = _db()
        coll = db[config.mongo.benchmark_collection]
        runs = [queries.jsonify_doc(d) for d in
                coll.find({}, {"_id": 0}).sort("started_at", -1).limit(100)]
        return jsonify({
            "runs": runs,
            "count": len(runs),
            "hint": ("Si esta vacio, ejecute: "
                     "docker compose run --rm benchmark"),
        })

    # =======================================================================
    # Administracion
    # =======================================================================
    @app.post(f"{API_PREFIX}/admin/reindex")
    def reindex():
        """Reconstruye los indices. Util tras cargar datos por otra via."""
        created = ensure_indexes(_db())
        return jsonify({
            "status": "ok",
            "indexes": {k: [str(i) for i in v] for k, v in created.items()},
        })

    # =======================================================================
    # Mapa Leaflet (opcional segun el enunciado, suma en la sustentacion)
    # =======================================================================
    @app.get("/")
    def index():
        static_dir = os.path.join(os.path.dirname(__file__), "static")
        page = os.path.join(static_dir, "index.html")
        if os.path.isfile(page):
            # La clave de CARTO no se versiona: se toma del entorno y solo
            # admite caracteres de clave para no inyectar nada en el script.
            key = os.environ.get("CARTO_API_KEY", "")
            if not re.fullmatch(r"[A-Za-z0-9_\-]*", key):
                key = ""
            with open(page, encoding="utf-8") as fh:
                html = fh.read().replace("__CARTO_API_KEY__", key)
            return Response(html, mimetype="text/html")
        return jsonify({"service": "geobigdata-api",
                        "docs": f"{API_PREFIX}/docs"})

    return app


app = create_app()


if __name__ == "__main__":
    log.info("Arrancando la API en %s:%s", config.api.host, config.api.port)
    app.run(host=config.api.host, port=config.api.port,
            debug=os.environ.get("FLASK_ENV") == "development")
