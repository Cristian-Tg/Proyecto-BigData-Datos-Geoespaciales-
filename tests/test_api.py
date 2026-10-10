"""Pruebas unitarias de la API de Flask.

Se centran en lo que puede fallar sin MongoDB: la validacion de parametros, los
codigos de estado y el contrato JSON de las respuestas. Una API que acepta
`lat=200` y deja que MongoDB reviente despues no cumple el requisito de que las
consultas "acepten parametros".
"""
from __future__ import annotations

from typing import Any

import pytest

API = "/api/v1"


@pytest.fixture
def stub_db(monkeypatch):
    """Sustituye get_db por un doble, para que ninguna ruta toque MongoDB."""
    from tests.test_queries import FakeCollection, FakeDB

    db = FakeDB({
        "accidents": FakeCollection([], count=0),
        "agg_grid": FakeCollection([], count=0),
        "agg_geohash": FakeCollection([], count=0),
        "agg_hotspots": FakeCollection([], count=0),
        "agg_temporal": FakeCollection([], count=0),
        "agg_state": FakeCollection([], count=0),
        "benchmark_runs": FakeCollection([], count=0),
    })
    monkeypatch.setattr("src.api.app.get_db", lambda *a, **k: db)
    return db


@pytest.fixture
def spy(monkeypatch):
    """Intercepta las funciones de consulta y devuelve los argumentos recibidos."""
    captured: dict[str, Any] = {}

    def _make(name):
        def _fn(db, **kwargs):
            captured[name] = kwargs
            return {"ok": True, "operator": name, "results": [], "returned": 0}
        return _fn

    for fn in ("query_near", "query_within", "aggregate_geo_near",
               "query_aggregation"):
        monkeypatch.setattr(f"src.api.queries.{fn}", _make(fn))
    return captured


# ---------------------------------------------------------------------------
# Metadatos
# ---------------------------------------------------------------------------
class TestMetadatos:

    def test_docs_responde_sin_base_de_datos(self, client):
        res = client.get(f"{API}/docs")
        assert res.status_code == 200
        body = res.get_json()
        rutas = {e["path"] for e in body["endpoints"]}
        # Los tres endpoints obligatorios del enunciado
        assert f"{API}/near" in rutas
        assert f"{API}/within" in rutas
        assert any(r.startswith(f"{API}/aggregations") for r in rutas)

    def test_stats_informa_del_indice_2dsphere(self, client, stub_db):
        res = client.get(f"{API}/stats")
        assert res.status_code == 200
        body = res.get_json()
        assert "geo_index_2dsphere" in body
        assert "ready_for_queries" in body
        assert "accidents" in body["collections"]

    def test_el_mapa_leaflet_se_sirve_en_la_raiz(self, client):
        res = client.get("/")
        assert res.status_code == 200
        assert b"leaflet" in res.data.lower()

    def test_el_mapa_recibe_la_clave_de_carto_del_entorno(self, client, monkeypatch):
        monkeypatch.setenv("CARTO_API_KEY", "cb1_clave_de_prueba")
        res = client.get("/")
        assert b'"cb1_clave_de_prueba"' in res.data
        assert b"__CARTO_API_KEY__" not in res.data

    def test_una_clave_de_carto_invalida_no_se_inyecta(self, client, monkeypatch):
        monkeypatch.setenv("CARTO_API_KEY", '";alert(1)//')
        res = client.get("/")
        assert b"alert(1)" not in res.data

    def test_cabeceras_cors_y_de_seguridad(self, client):
        res = client.get(f"{API}/docs")
        assert res.headers["Access-Control-Allow-Origin"] == "*"
        assert res.headers["X-Content-Type-Options"] == "nosniff"

    def test_una_ruta_inexistente_devuelve_json_y_no_html(self, client):
        res = client.get(f"{API}/no-existe")
        assert res.status_code == 404
        assert res.is_json, "los errores deben ser JSON, no la pagina HTML de Flask"
        assert "error" in res.get_json()


# ---------------------------------------------------------------------------
# ENDPOINT 1: /near
# ---------------------------------------------------------------------------
class TestEndpointNear:

    def test_peticion_valida(self, client, stub_db, spy):
        res = client.get(f"{API}/near?lat=34.0522&lon=-118.2437&radius_m=5000")
        assert res.status_code == 200
        args = spy["query_near"]
        assert args["lat"] == pytest.approx(34.0522)
        assert args["lon"] == pytest.approx(-118.2437)
        assert args["radius_m"] == pytest.approx(5000)

    def test_acepta_radius_km_como_alternativa(self, client, stub_db, spy):
        res = client.get(f"{API}/near?lat=34&lon=-118&radius_km=7.5")
        assert res.status_code == 200
        assert spy["query_near"]["radius_m"] == pytest.approx(7500)

    @pytest.mark.parametrize("qs,falta", [
        ("lon=-118&radius_m=5000", "lat"),
        ("lat=34&radius_m=5000", "lon"),
    ])
    def test_faltan_parametros_obligatorios(self, client, stub_db, qs, falta):
        res = client.get(f"{API}/near?{qs}")
        assert res.status_code == 400
        assert falta in res.get_json()["message"]

    def test_sin_radio_de_ningun_tipo(self, client, stub_db):
        res = client.get(f"{API}/near?lat=34&lon=-118")
        assert res.status_code == 400
        assert "radius" in res.get_json()["message"]

    @pytest.mark.parametrize("qs", [
        "lat=200&lon=-118&radius_m=5000",       # latitud > 90
        "lat=-91&lon=-118&radius_m=5000",
        "lat=34&lon=181&radius_m=5000",         # longitud > 180
        "lat=34&lon=-118&radius_m=0",           # radio no positivo
        "lat=34&lon=-118&radius_m=-5",
        "lat=abc&lon=-118&radius_m=5000",       # no numerico
        "lat=34&lon=-118&radius_m=5000&limit=0",
        "lat=34&lon=-118&radius_m=5000&limit=99999",   # supera API_MAX_LIMIT
        "lat=34&lon=-118&radius_m=5000&severity=9",
        "lat=34&lon=-118&radius_m=5000&min_severity=7",
        "lat=34&lon=-118&radius_m=5000&hour_from=25",
        "lat=34&lon=-118&radius_m=5000&start_date=ayer",
    ])
    def test_parametros_invalidos_devuelven_400(self, client, stub_db, qs):
        res = client.get(f"{API}/near?{qs}")
        assert res.status_code == 400, f"deberia rechazar: {qs}"
        assert res.get_json()["error"] in {"parametro_invalido", "valor_invalido"}

    def test_los_filtros_de_atributos_llegan_a_la_consulta(self, client, stub_db, spy):
        res = client.get(f"{API}/near?lat=34&lon=-118&radius_m=5000"
                         "&severity=3,4&state=ca&hour_from=7&hour_to=9"
                         "&start_date=2021-01-01")
        assert res.status_code == 200
        flt = spy["query_near"]["attribute_filter"]
        assert flt["severity"] == {"$in": [3, 4]}
        assert flt["state"] == "CA"
        assert flt["hour"] == {"$gte": 7, "$lte": 9}
        assert "$gte" in flt["start_time"]

    def test_se_puede_desactivar_el_conteo(self, client, stub_db, spy):
        client.get(f"{API}/near?lat=34&lon=-118&radius_m=5000&count=false")
        assert spy["query_near"]["with_count"] is False


# ---------------------------------------------------------------------------
# ENDPOINT 2: /within
# ---------------------------------------------------------------------------
class TestEndpointWithin:

    POLY = {"type": "Polygon", "coordinates": [[
        [-118.55, 33.90], [-118.10, 33.90], [-118.10, 34.15],
        [-118.55, 34.15], [-118.55, 33.90]]]}

    def test_post_con_geometry(self, client, stub_db, spy):
        res = client.post(f"{API}/within", json={"geometry": self.POLY})
        assert res.status_code == 200
        assert spy["query_within"]["geometry"] == self.POLY

    def test_post_con_el_poligono_en_la_raiz_del_cuerpo(self, client, stub_db, spy):
        res = client.post(f"{API}/within", json=self.POLY)
        assert res.status_code == 200
        assert spy["query_within"]["geometry"]["type"] == "Polygon"

    def test_post_con_un_feature_de_geojson(self, client, stub_db, spy):
        feature = {"type": "Feature", "properties": {}, "geometry": self.POLY}
        res = client.post(f"{API}/within", json=feature)
        assert res.status_code == 200
        assert spy["query_within"]["geometry"] == self.POLY

    def test_post_con_bbox(self, client, stub_db, spy):
        res = client.post(f"{API}/within",
                          json={"bbox": [-118.5, 33.9, -118.1, 34.1]})
        assert res.status_code == 200
        ring = spy["query_within"]["geometry"]["coordinates"][0]
        assert len(ring) == 5 and ring[0] == ring[-1]

    def test_bbox_con_numero_de_elementos_incorrecto(self, client, stub_db):
        res = client.post(f"{API}/within", json={"bbox": [-118.5, 33.9]})
        assert res.status_code == 400
        assert "bbox" in res.get_json()["message"]

    def test_cuerpo_vacio(self, client, stub_db):
        res = client.post(f"{API}/within", json={})
        assert res.status_code == 400
        assert "geometry" in res.get_json()["message"]

    def test_sin_geometry_ni_bbox(self, client, stub_db):
        res = client.post(f"{API}/within", json={"limit": 10})
        assert res.status_code == 400

    def test_get_con_bounding_box(self, client, stub_db, spy):
        res = client.get(f"{API}/within?min_lon=-118.5&min_lat=33.9"
                         "&max_lon=-118.1&max_lat=34.1")
        assert res.status_code == 200
        assert spy["query_within"]["geometry"]["type"] == "Polygon"

    @pytest.mark.parametrize("qs", [
        "min_lon=-118.1&min_lat=33.9&max_lon=-118.5&max_lat=34.1",  # lon invertida
        "min_lon=-118.5&min_lat=34.1&max_lon=-118.1&max_lat=33.9",  # lat invertida
        "min_lon=-118.5&min_lat=33.9&max_lon=-118.1",               # falta uno
    ])
    def test_bounding_box_invalido(self, client, stub_db, qs):
        assert client.get(f"{API}/within?{qs}").status_code == 400

    def test_un_poligono_invalido_devuelve_400(self, client, stub_db):
        """Sin mockear la consulta: el ValueError de validate_polygon -> 400."""
        res = client.post(f"{API}/within", json={"geometry": {
            "type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1]]]}})
        assert res.status_code == 400

    def test_options_para_el_preflight_de_cors(self, client):
        assert client.options(f"{API}/within").status_code in (200, 204)


# ---------------------------------------------------------------------------
# /geonear
# ---------------------------------------------------------------------------
class TestEndpointGeoNear:

    def test_peticion_valida(self, client, stub_db, spy):
        res = client.get(f"{API}/geonear?lat=34&lon=-118&max_distance_m=20000")
        assert res.status_code == 200
        args = spy["aggregate_geo_near"]
        assert args["max_distance_m"] == pytest.approx(20000)
        assert args["group_by"] == "severity"

    def test_acepta_radius_m_como_alias(self, client, stub_db, spy):
        res = client.get(f"{API}/geonear?lat=34&lon=-118&radius_m=15000")
        assert res.status_code == 200
        assert spy["aggregate_geo_near"]["max_distance_m"] == pytest.approx(15000)

    @pytest.mark.parametrize("group_by", ["severity", "state", "city", "hour",
                                          "dow", "weather", "distance_band",
                                          "geohash", "grid_id", "none"])
    def test_todos_los_group_by_documentados_se_aceptan(self, client, stub_db,
                                                        spy, group_by):
        res = client.get(f"{API}/geonear?lat=34&lon=-118&radius_m=20000"
                         f"&group_by={group_by}")
        assert res.status_code == 200
        assert spy["aggregate_geo_near"]["group_by"] == group_by

    def test_group_by_invalido_sin_mock_devuelve_400(self, client, stub_db):
        res = client.get(f"{API}/geonear?lat=34&lon=-118&radius_m=20000"
                         "&group_by=inventado")
        assert res.status_code == 400


# ---------------------------------------------------------------------------
# ENDPOINT 3: /aggregations (resultados de Spark)
# ---------------------------------------------------------------------------
class TestEndpointAgregaciones:

    def test_el_indice_lista_todas_las_agregaciones(self, client, stub_db):
        res = client.get(f"{API}/aggregations")
        assert res.status_code == 200
        body = res.get_json()
        nombres = {a["name"] for a in body["aggregations"]}
        assert {"grid", "geohash", "hotspots", "temporal", "state"} <= nombres
        assert "Spark" in body["source"]

    @pytest.mark.parametrize("name", ["grid", "geohash", "hotspots",
                                      "temporal", "state"])
    def test_cada_agregacion_responde(self, client, stub_db, spy, name):
        res = client.get(f"{API}/aggregations/{name}")
        assert res.status_code == 200
        assert spy["query_aggregation"]["name"] == name

    def test_un_nombre_desconocido_devuelve_400(self, client, stub_db):
        res = client.get(f"{API}/aggregations/inventada")
        assert res.status_code == 400
        assert "Agregacion desconocida" in res.get_json()["message"]

    def test_filtro_por_dimension_y_conteo_minimo(self, client, stub_db, spy):
        res = client.get(f"{API}/aggregations/temporal?dimension=hour&min_count=5")
        assert res.status_code == 200
        assert spy["query_aggregation"]["dimension"] == "hour"
        assert spy["query_aggregation"]["min_count"] == 5

    def test_bbox_valido(self, client, stub_db, spy):
        res = client.get(f"{API}/aggregations/grid?bbox=-119,33,-117,35")
        assert res.status_code == 200
        assert spy["query_aggregation"]["bbox"] == (-119.0, 33.0, -117.0, 35.0)

    @pytest.mark.parametrize("bbox", ["-119,33,-117", "a,b,c,d", "1,2,3,4,5"])
    def test_bbox_invalido(self, client, stub_db, bbox):
        assert client.get(f"{API}/aggregations/grid?bbox={bbox}").status_code == 400

    def test_el_orden_se_puede_invertir(self, client, stub_db, spy):
        client.get(f"{API}/aggregations/grid?sort_by=count&order=asc")
        assert spy["query_aggregation"]["order"] == 1
        client.get(f"{API}/aggregations/grid?sort_by=count&order=desc")
        assert spy["query_aggregation"]["order"] == -1


# ---------------------------------------------------------------------------
# Salud
# ---------------------------------------------------------------------------
class TestSalud:

    def test_ok_cuando_mongodb_responde(self, client, monkeypatch):
        class FakeAdmin:
            def command(self, *_a, **_k):
                return {"ok": 1}

        class FakeClient:
            admin = FakeAdmin()

        class FakeDatabase:
            name = "geobigdata"
            client = FakeClient()

        monkeypatch.setattr("src.api.app.get_db", lambda *a, **k: FakeDatabase())
        res = client.get(f"{API}/health")
        assert res.status_code == 200
        body = res.get_json()
        assert body["status"] == "ok"
        assert body["mongodb"] == "ok"

    def test_degradado_cuando_mongodb_no_responde(self, client, monkeypatch):
        def _boom(*_a, **_k):
            raise RuntimeError("conexion rechazada")

        monkeypatch.setattr("src.api.app.get_db", _boom)
        res = client.get(f"{API}/health")
        assert res.status_code == 503
        assert res.get_json()["status"] == "degraded"

    def test_la_api_falla_rapido_si_mongodb_esta_caido(self):
        """La API NO debe heredar el presupuesto de espera de los trabajos por lotes.

        `get_client` reintenta 30 veces por defecto, que es lo correcto para la
        ingesta (esperar a que Mongo arranque en `compose up`). En la API seria
        nefasto: con la base caida, cada peticion se colgaria minutos y agotaria
        los workers de Gunicorn en vez de devolver un 503.

        Esta prueba fija el limite superior para que nadie lo suba por error.
        """
        from src.api import app as app_module

        presupuesto_s = app_module.API_DB_RETRIES * (
            app_module.API_DB_TIMEOUT_MS / 1000 + app_module.API_DB_DELAY)
        assert presupuesto_s <= 15, (
            f"una peticion podria tardar {presupuesto_s:.0f}s en fallar; "
            "baje API_DB_RETRIES o API_DB_TIMEOUT_MS")
        assert app_module.API_DB_RETRIES < 30, (
            "la API no debe usar el presupuesto de los trabajos por lotes")

    def test_las_rutas_usan_el_presupuesto_corto(self, monkeypatch):
        """Verifica que las rutas pasan por `_db()` y no por `get_db()` directo."""
        from src.api import app as app_module

        recibido = {}

        def _spy(uri=None, **kwargs):
            recibido.update(kwargs)
            raise RuntimeError("cortocircuito a proposito")

        monkeypatch.setattr(app_module, "get_db", _spy)
        app_module.create_app(ensure_idx=False).test_client().get(f"{API}/health")

        assert recibido.get("retries") == app_module.API_DB_RETRIES
        assert recibido.get("timeout_ms") == app_module.API_DB_TIMEOUT_MS

    def test_falta_de_indice_2dsphere_devuelve_503(self, client, stub_db, monkeypatch):
        from pymongo.errors import OperationFailure

        def _sin_indice(*_a, **_k):
            raise OperationFailure("unable to find index for $geoNear query")

        monkeypatch.setattr("src.api.queries.query_near", _sin_indice)
        res = client.get(f"{API}/near?lat=34&lon=-118&radius_m=5000")
        assert res.status_code == 503
        assert res.get_json()["error"] == "falta_indice_geoespacial"

class TestSeverityByState:
    def test_agrupa_por_severidad_en_el_estado(self, client, stub_db):
        res = client.get(f"{API}/severity_by_state?state=ca")
        assert res.status_code == 200
        assert res.get_json()["state"] == "CA"
        pipeline = stub_db["accidents"].aggregate_calls[-1]
        assert pipeline[0] == {"$match": {"state": "CA"}}

    def test_exige_el_codigo_del_estado(self, client, stub_db):
        assert client.get(f"{API}/severity_by_state").status_code == 400
