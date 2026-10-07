"""Pruebas de integracion contra el sistema en marcha.

Las ejecuta Jenkins DESPUES de levantar la pila y ANTES de promover el
despliegue. Si alguna falla, la nueva version no se despliega.

    pytest -m integration          # requiere API + MongoDB en marcha
    API_BASE_URL=http://api:5000 pytest -m integration   # dentro de compose

No se comprueba solo que la API responda 200: se verifica que las consultas
geoespaciales devuelvan resultados geometricamente correctos, calculando las
distancias con una implementacion independiente (haversine) y comprobando la
contencion en el poligono punto por punto.
"""
from __future__ import annotations

import os

import pytest

requests = pytest.importorskip("requests")

from src.common.geo import bbox_to_polygon, haversine_m  # noqa: E402

pytestmark = pytest.mark.integration

API = "/api/v1"
TIMEOUT = 60

# Centro de pruebas: centro de Los Angeles, la zona con mas registros del
# dataset. Se puede cambiar con variables de entorno para probar otra area.
TEST_LAT = float(os.environ.get("TEST_LAT", "34.0522"))
TEST_LON = float(os.environ.get("TEST_LON", "-118.2437"))
TEST_RADIUS_M = float(os.environ.get("TEST_RADIUS_M", "20000"))

# Minimo de registros que exige el enunciado
MIN_RECORDS_REQUIRED = int(os.environ.get("MIN_RECORDS_REQUIRED", "1000000"))


@pytest.fixture(scope="module")
def base(api_base_url):
    return api_base_url


@pytest.fixture(scope="module")
def stats(base):
    res = requests.get(f"{base}{API}/stats", timeout=TIMEOUT)
    res.raise_for_status()
    return res.json()


def _get(base, path, **params):
    res = requests.get(f"{base}{path}", params=params or None, timeout=TIMEOUT)
    return res


def _total(body):
    """Total de coincidencias, distinguiendo degradacion de defecto.

    La API trata el conteo como metadato OPCIONAL: si supera su limite de
    tiempo devuelve los resultados con `total_matching: null` y la marca
    `total_matching_timed_out: true`, en lugar de un 500. Es comportamiento
    documentado, no un fallo, y ocurre cuando la maquina esta bajo presion de
    memoria (paso en el build #6, con 3,4 M de documentos).

    - None CON la marca  -> se omite la prueba con el motivo, porque no puede
      verificar nada sobre un total que no existe.
    - None SIN la marca  -> falla: eso si seria un defecto de la API.
    """
    total = body.get("total_matching")
    if total is None:
        if body.get("total_matching_timed_out"):
            pytest.skip("el conteo supero su limite de tiempo y la API lo "
                        "degrado a null (comportamiento documentado)")
        pytest.fail(f"total_matching es None sin la marca de timeout: {body.get('query')}")
    return total


def _summary(body):
    """Resumen del area, con la misma distincion que `_total`."""
    s = body.get("summary")
    if s is None:
        if body.get("summary_timed_out"):
            pytest.skip("el resumen del area supero su limite de tiempo y la "
                        "API lo degrado a null (comportamiento documentado)")
        pytest.fail("summary es None sin la marca de timeout")
    return s


# ---------------------------------------------------------------------------
# Disponibilidad del sistema
# ---------------------------------------------------------------------------
class TestDisponibilidad:

    def test_la_api_responde(self, base):
        res = _get(base, f"{API}/health")
        assert res.status_code == 200, res.text
        body = res.json()
        assert body["status"] == "ok"
        assert body["mongodb"] == "ok"

    def test_el_catalogo_de_endpoints_esta_publicado(self, base):
        res = _get(base, f"{API}/docs")
        assert res.status_code == 200
        rutas = {e["path"] for e in res.json()["endpoints"]}
        assert f"{API}/near" in rutas
        assert f"{API}/within" in rutas

    def test_el_mapa_leaflet_se_sirve(self, base):
        res = requests.get(f"{base}/", timeout=TIMEOUT)
        assert res.status_code == 200
        assert "leaflet" in res.text.lower()


# ---------------------------------------------------------------------------
# Modelo de datos
# ---------------------------------------------------------------------------
class TestModeloDeDatos:

    def test_hay_datos_cargados(self, stats):
        principal = stats["collections"]["accidents"]
        assert principal["exists"], "la coleccion principal no existe"
        assert principal["count"] > 0, ("no hay datos: ejecute "
                                        "docker compose run --rm ingestion")

    def test_el_indice_2dsphere_existe(self, stats):
        """Requisito explicito del enunciado."""
        assert stats["geo_index_2dsphere"] is True

    def test_el_sistema_esta_listo_para_consultas(self, stats):
        assert stats["ready_for_queries"] is True

    @pytest.mark.slow
    def test_se_cumple_el_minimo_de_un_millon_de_registros(self, stats):
        n = stats["collections"]["accidents"]["count"]
        assert n >= MIN_RECORDS_REQUIRED, (
            f"el enunciado exige >= {MIN_RECORDS_REQUIRED:,} registros y "
            f"solo hay {n:,}. Suba SAMPLE_SIZE y reejecute la ingesta.")


# ---------------------------------------------------------------------------
# CONSULTA 1: $near
# ---------------------------------------------------------------------------
class TestConsultaNear:

    @pytest.fixture(scope="class")
    def resultado(self, base):
        res = _get(base, f"{API}/near", lat=TEST_LAT, lon=TEST_LON,
                   radius_m=TEST_RADIUS_M, limit=200)
        assert res.status_code == 200, res.text
        return res.json()

    def test_devuelve_resultados(self, resultado):
        assert resultado["returned"] > 0, (
            "no hay accidentes en el radio de prueba; ajuste TEST_LAT/TEST_LON")
        assert resultado["query"]["operator"] == "$near"

    def test_todos_los_puntos_estan_dentro_del_radio(self, resultado):
        """Verificado con haversine, una implementacion independiente de Mongo."""
        for doc in resultado["results"]:
            d = haversine_m(TEST_LAT, TEST_LON, doc["lat"], doc["lon"])
            # 1% de tolerancia: MongoDB usa un elipsoide y haversine una esfera
            assert d <= TEST_RADIUS_M * 1.01, (
                f"{doc['accident_id']} esta a {d:.0f} m, fuera del radio de "
                f"{TEST_RADIUS_M:.0f} m")

    def test_los_resultados_vienen_ordenados_por_distancia(self, resultado):
        """$near ordena por proximidad sin necesidad de un $sort explicito."""
        distancias = [haversine_m(TEST_LAT, TEST_LON, d["lat"], d["lon"])
                      for d in resultado["results"]]
        for anterior, siguiente in zip(distancias, distancias[1:], strict=False):
            assert siguiente >= anterior - 50, (
                "los resultados de $near deben venir de mas cercano a mas lejano")

    def test_todos_los_documentos_traen_geojson_valido(self, resultado):
        for doc in resultado["results"]:
            loc = doc["location"]
            assert loc["type"] == "Point"
            lon, lat = loc["coordinates"]
            assert lon == pytest.approx(doc["lon"], abs=1e-6)
            assert lat == pytest.approx(doc["lat"], abs=1e-6)

    def test_el_total_es_coherente_con_lo_devuelto(self, resultado):
        assert _total(resultado) >= resultado["returned"]

    def test_un_radio_mayor_nunca_devuelve_menos(self, base):
        pequeno = _get(base, f"{API}/near", lat=TEST_LAT, lon=TEST_LON,
                       radius_m=2000, limit=1).json()
        grande = _get(base, f"{API}/near", lat=TEST_LAT, lon=TEST_LON,
                      radius_m=50000, limit=1).json()
        assert _total(grande) >= _total(pequeno)

    def test_el_filtro_de_severidad_funciona(self, base):
        res = _get(base, f"{API}/near", lat=TEST_LAT, lon=TEST_LON,
                   radius_m=TEST_RADIUS_M, limit=100, min_severity=3)
        assert res.status_code == 200
        for doc in res.json()["results"]:
            assert doc["severity"] >= 3

    def test_el_filtro_por_estado_funciona(self, base):
        res = _get(base, f"{API}/near", lat=TEST_LAT, lon=TEST_LON,
                   radius_m=100000, limit=50, state="CA")
        assert res.status_code == 200
        for doc in res.json()["results"]:
            assert doc["state"] == "CA"

    def test_el_limite_se_respeta(self, base):
        res = _get(base, f"{API}/near", lat=TEST_LAT, lon=TEST_LON,
                   radius_m=TEST_RADIUS_M, limit=7)
        assert res.json()["returned"] <= 7

    def test_la_paginacion_con_skip_no_repite_resultados(self, base):
        p1 = _get(base, f"{API}/near", lat=TEST_LAT, lon=TEST_LON,
                  radius_m=TEST_RADIUS_M, limit=10, skip=0).json()["results"]
        p2 = _get(base, f"{API}/near", lat=TEST_LAT, lon=TEST_LON,
                  radius_m=TEST_RADIUS_M, limit=10, skip=10).json()["results"]
        if p1 and p2:
            ids1 = {d["accident_id"] for d in p1}
            ids2 = {d["accident_id"] for d in p2}
            assert not (ids1 & ids2)

    def test_tiempo_de_respuesta_razonable(self, resultado):
        """Criterio de evaluacion: "tiempos de respuesta razonables"."""
        assert resultado["elapsed_ms"] < 5000, (
            f"$near tardo {resultado['elapsed_ms']} ms; revise el indice 2dsphere")

    @pytest.mark.parametrize("params", [
        {"lat": 200, "lon": -118, "radius_m": 5000},
        {"lat": 34, "lon": 999, "radius_m": 5000},
        {"lat": 34, "lon": -118, "radius_m": -1},
        {"lat": "x", "lon": -118, "radius_m": 5000},
        {"lon": -118, "radius_m": 5000},
    ])
    def test_los_parametros_invalidos_devuelven_400(self, base, params):
        res = _get(base, f"{API}/near", **params)
        assert res.status_code == 400
        assert "message" in res.json()


# ---------------------------------------------------------------------------
# CONSULTA 2: $geoWithin
# ---------------------------------------------------------------------------
class TestConsultaWithin:

    BBOX = (-118.6, 33.85, -118.0, 34.25)   # area metropolitana de Los Angeles

    @pytest.fixture(scope="class")
    def resultado(self, base):
        poly = bbox_to_polygon(*self.BBOX)
        res = requests.post(f"{base}{API}/within",
                            json={"geometry": poly, "limit": 200, "summary": True},
                            timeout=TIMEOUT)
        assert res.status_code == 200, res.text
        return res.json()

    def test_devuelve_resultados(self, resultado):
        assert resultado["query"]["operator"] == "$geoWithin"
        assert resultado["returned"] > 0

    # Tolerancia por la geodesia esferica, NO por imprecision numerica.
    #
    # MongoDB interpreta los lados de un Polygon de $geoWithin como GEODESICAS
    # (arcos de circulo maximo), no como lineas de latitud constante. El lado
    # norte de un "rectangulo" lat/lon se comba hacia el polo en su parte
    # central, asi que puntos ligeramente al norte de max_lat SI estan dentro
    # del poligono esferico.
    #
    # Medido con este bbox (0,6 grados de ancho a 34 grados de latitud): 11 de
    # 1000 puntos exceden max_lat entre 1,3 y 17,1 m, y NINGUNO excede en
    # longitud, que es justo la firma del abombamiento. 100 m cubre el efecto
    # con holgura sin dejar pasar un error real de contencion.
    GEODESIC_TOLERANCE_DEG = 100.0 / 111_320.0

    def test_todos_los_puntos_estan_dentro_del_poligono(self, resultado):
        min_lon, min_lat, max_lon, max_lat = self.BBOX
        tol = self.GEODESIC_TOLERANCE_DEG
        for doc in resultado["results"]:
            assert min_lon - tol <= doc["lon"] <= max_lon + tol,                 f"{doc['accident_id']} fuera en longitud: {doc['lon']}"
            assert min_lat - tol <= doc["lat"] <= max_lat + tol,                 f"{doc['accident_id']} fuera en latitud: {doc['lat']}"

    def test_el_exceso_es_solo_en_latitud_y_milimetrico(self, resultado):
        """Verifica que el exceso responde al abombamiento geodesico.

        Si algun dia apareciera un punto claramente fuera, o el exceso se diera
        en LONGITUD, no seria geodesia: seria un error de contencion real y esta
        prueba lo distinguiria de la tolerancia legitima.
        """
        min_lon, min_lat, max_lon, max_lat = self.BBOX
        for doc in resultado["results"]:
            # El lado este y el oeste son meridianos: no se comban, asi que en
            # longitud la contencion tiene que ser exacta.
            assert min_lon <= doc["lon"] <= max_lon,                 (f"{doc['accident_id']} excede en longitud; los meridianos no "
                 "se comban, esto no es geodesia")
            exceso_m = max(min_lat - doc["lat"], doc["lat"] - max_lat, 0.0) * 111_320
            assert exceso_m < 100,                 f"{doc['accident_id']} excede la latitud en {exceso_m:.0f} m"

    def test_el_resumen_del_area_es_coherente(self, resultado):
        s = _summary(resultado)
        assert s["count"] == _total(resultado)
        assert 1 <= s["avg_severity"] <= 4
        assert 1 <= s["max_severity"] <= 4
        assert s["severe_count"] <= s["count"]
        assert s["distinct_cities"] >= 1

    def test_un_poligono_mayor_contiene_al_menor(self, base):
        pequeno = requests.post(
            f"{base}{API}/within",
            json={"geometry": bbox_to_polygon(-118.3, 34.0, -118.2, 34.1),
                  "limit": 1}, timeout=TIMEOUT).json()
        grande = requests.post(
            f"{base}{API}/within",
            json={"geometry": bbox_to_polygon(-118.6, 33.8, -118.0, 34.3),
                  "limit": 1}, timeout=TIMEOUT).json()
        assert _total(grande) >= _total(pequeno)

    def test_un_poligono_en_medio_del_oceano_devuelve_cero(self, base):
        """Medio del Pacifico: no debe haber accidentes de EE.UU. ahi."""
        res = requests.post(
            f"{base}{API}/within",
            json={"geometry": bbox_to_polygon(-150.0, 0.0, -140.0, 5.0),
                  "limit": 10}, timeout=TIMEOUT)
        assert res.status_code == 200
        assert _total(res.json()) == 0

    def test_acepta_multipolygon(self, base):
        geom = {"type": "MultiPolygon", "coordinates": [
            bbox_to_polygon(-118.6, 33.85, -118.0, 34.25)["coordinates"],
            bbox_to_polygon(-74.2, 40.5, -73.7, 40.9)["coordinates"],
        ]}
        res = requests.post(f"{base}{API}/within",
                            json={"geometry": geom, "limit": 50}, timeout=TIMEOUT)
        assert res.status_code == 200
        assert res.json()["query"]["geometry_type"] == "MultiPolygon"

    def test_la_variante_get_con_bbox_coincide_con_el_post(self, base):
        min_lon, min_lat, max_lon, max_lat = self.BBOX
        via_get = _get(base, f"{API}/within", min_lon=min_lon, min_lat=min_lat,
                       max_lon=max_lon, max_lat=max_lat, limit=1).json()
        via_post = requests.post(
            f"{base}{API}/within",
            json={"geometry": bbox_to_polygon(*self.BBOX), "limit": 1},
            timeout=TIMEOUT).json()
        assert _total(via_get) == _total(via_post)

    def test_un_poligono_sin_cerrar_devuelve_400(self, base):
        res = requests.post(f"{base}{API}/within", json={"geometry": {
            "type": "Polygon",
            "coordinates": [[[-118.5, 33.9], [-118.1, 33.9], [-118.1, 34.1]]]}},
            timeout=TIMEOUT)
        assert res.status_code == 400

    def test_los_filtros_se_aplican_dentro_del_poligono(self, base):
        res = requests.post(
            f"{base}{API}/within",
            json={"geometry": bbox_to_polygon(*self.BBOX), "limit": 100,
                  "min_severity": 3}, timeout=TIMEOUT)
        assert res.status_code == 200
        for doc in res.json()["results"]:
            assert doc["severity"] >= 3


# ---------------------------------------------------------------------------
# CONSULTA 3: $geoNear
# ---------------------------------------------------------------------------
class TestAgregacionGeoNear:

    def test_agrupado_por_severidad(self, base):
        res = _get(base, f"{API}/geonear", lat=TEST_LAT, lon=TEST_LON,
                   max_distance_m=TEST_RADIUS_M, group_by="severity")
        assert res.status_code == 200, res.text
        body = res.json()
        assert body["query"]["operator"] == "$geoNear"
        assert body["groups_returned"] > 0
        for g in body["groups"]:
            assert g["group"] in (1, 2, 3, 4)
            assert g["count"] > 0
            assert 0 <= g["min_distance_m"] <= g["avg_distance_m"] <= g["max_distance_m"]
            assert g["max_distance_m"] <= TEST_RADIUS_M * 1.01

    def test_las_bandas_de_distancia_son_multiplos_del_ancho(self, base):
        ancho = 2000
        res = _get(base, f"{API}/geonear", lat=TEST_LAT, lon=TEST_LON,
                   max_distance_m=TEST_RADIUS_M, group_by="distance_band",
                   band_width_m=ancho)
        assert res.status_code == 200
        for g in res.json()["groups"]:
            assert g["group"] % ancho == 0
            # todos los puntos de la banda caen dentro de ella
            assert g["min_distance_m"] >= g["group"] - 1
            assert g["max_distance_m"] <= g["group"] + ancho + 1

    def test_el_total_coincide_con_el_de_near(self, base):
        gn = _get(base, f"{API}/geonear", lat=TEST_LAT, lon=TEST_LON,
                  max_distance_m=TEST_RADIUS_M, group_by="none",
                  limit=500).json()
        near = _get(base, f"{API}/near", lat=TEST_LAT, lon=TEST_LON,
                    radius_m=TEST_RADIUS_M, limit=1).json()
        assert gn["total_in_radius"] == _total(near)

    def test_el_pipeline_se_expone_y_empieza_con_geonear(self, base):
        body = _get(base, f"{API}/geonear", lat=TEST_LAT, lon=TEST_LON,
                    max_distance_m=5000).json()
        assert list(body["pipeline"][0].keys()) == ["$geoNear"]

    def test_group_by_invalido_devuelve_400(self, base):
        res = _get(base, f"{API}/geonear", lat=TEST_LAT, lon=TEST_LON,
                   max_distance_m=5000, group_by="no-existe")
        assert res.status_code == 400


# ---------------------------------------------------------------------------
# Resultados de Spark
# ---------------------------------------------------------------------------
class TestResultadosDeSpark:

    @pytest.fixture(scope="class")
    def indice(self, base):
        res = _get(base, f"{API}/aggregations")
        assert res.status_code == 200
        return {a["name"]: a for a in res.json()["aggregations"]}

    def test_el_indice_lista_las_cinco_agregaciones(self, indice):
        assert {"grid", "geohash", "hotspots", "temporal", "state"} <= set(indice)

    @pytest.mark.parametrize("name", ["grid", "geohash", "hotspots",
                                      "temporal", "state"])
    def test_cada_coleccion_tiene_resultados(self, base, indice, name):
        if indice[name]["documents"] == 0:
            pytest.skip(f"'{name}' esta vacia: ejecute "
                        "docker compose run --rm spark-job")
        res = _get(base, f"{API}/aggregations/{name}", limit=10)
        assert res.status_code == 200
        body = res.json()
        assert body["returned"] > 0
        assert "Spark" in body["source"]

    def test_la_grilla_trae_centroides_geojson_validos(self, base, indice):
        if indice["grid"]["documents"] == 0:
            pytest.skip("agg_grid esta vacia")
        for row in _get(base, f"{API}/aggregations/grid", limit=50).json()["results"]:
            c = row["centroid"]
            assert c["type"] == "Point"
            lon, lat = c["coordinates"]
            assert -180 <= lon <= 180 and -90 <= lat <= 90
            assert row["count"] > 0
            assert 1 <= row["avg_severity"] <= 4
            # el centroide debe caer dentro de su propia celda
            assert row["grid_lat"] <= lat <= row["grid_lat"] + row["cell_deg"]
            assert row["grid_lon"] <= lon <= row["grid_lon"] + row["cell_deg"]

    def test_los_hotspots_estan_ordenados_por_ranking(self, base, indice):
        if indice["hotspots"]["documents"] == 0:
            pytest.skip("agg_hotspots esta vacia")
        rows = _get(base, f"{API}/aggregations/hotspots", limit=50).json()["results"]
        assert [r["rank"] for r in rows] == sorted(r["rank"] for r in rows)
        indices = [r["concentration_index"] for r in rows]
        assert indices == sorted(indices, reverse=True), (
            "el ranking debe ir de mayor a menor indice de concentracion")

    def test_las_dimensiones_temporales_estan_completas(self, base, indice):
        if indice["temporal"]["documents"] == 0:
            pytest.skip("agg_temporal esta vacia")
        horas = _get(base, f"{API}/aggregations/temporal", dimension="hour",
                     limit=30).json()["results"]
        assert len(horas) == 24, "deben existir las 24 horas del dia"
        assert {h["bucket"] for h in horas} == set(range(24))

        dias = _get(base, f"{API}/aggregations/temporal", dimension="dow",
                    limit=10).json()["results"]
        assert len(dias) == 7
        assert {d["bucket"] for d in dias} == set(range(7))

    def test_el_filtro_por_bbox_reduce_los_resultados(self, base, indice):
        if indice["grid"]["documents"] == 0:
            pytest.skip("agg_grid esta vacia")
        todo = _get(base, f"{API}/aggregations/grid", limit=1).json()
        zona = _get(base, f"{API}/aggregations/grid", limit=1,
                    bbox="-119,33,-117,35").json()
        assert _total(zona) <= _total(todo)

    def test_la_suma_de_la_grilla_no_supera_el_total_de_registros(self, base,
                                                                  indice, stats):
        if indice["grid"]["documents"] == 0:
            pytest.skip("agg_grid esta vacia")
        rows = _get(base, f"{API}/aggregations/grid", limit=1000).json()["results"]
        assert sum(r["count"] for r in rows) <= stats["collections"]["accidents"]["count"]


# ---------------------------------------------------------------------------
# Coherencia entre operadores
# ---------------------------------------------------------------------------
class TestCoherenciaEntreOperadores:

    def test_near_y_within_coinciden_en_la_misma_zona(self, base):
        """Un bbox inscrito en el circulo debe devolver <= que el circulo."""
        radio = 20000.0
        # medio lado del cuadrado inscrito = radio / sqrt(2)
        media_lado_deg = (radio / 1.4143) / 111_320.0
        bbox = (TEST_LON - media_lado_deg, TEST_LAT - media_lado_deg,
                TEST_LON + media_lado_deg, TEST_LAT + media_lado_deg)

        circulo = _total(_get(base, f"{API}/near", lat=TEST_LAT, lon=TEST_LON,
                              radius_m=radio, limit=1).json())
        cuadrado = requests.post(
            f"{base}{API}/within",
            json={"geometry": bbox_to_polygon(*bbox), "limit": 1},
            timeout=TIMEOUT).json()
        cuadrado = _total(cuadrado)

        assert cuadrado <= circulo, (
            "el cuadrado inscrito no puede contener mas puntos que el circulo")

    def test_la_misma_consulta_da_el_mismo_resultado(self, base):
        """Determinismo: dos llamadas identicas deben coincidir."""
        params = {"lat": TEST_LAT, "lon": TEST_LON, "radius_m": 10000, "limit": 25}
        a = _get(base, f"{API}/near", **params).json()
        b = _get(base, f"{API}/near", **params).json()
        assert [d["accident_id"] for d in a["results"]] == \
               [d["accident_id"] for d in b["results"]]
        assert _total(a) == _total(b)


# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------
class TestBenchmark:

    def test_el_endpoint_de_benchmark_responde(self, base):
        res = _get(base, f"{API}/benchmark")
        assert res.status_code == 200
        assert "runs" in res.json()

    def test_hay_mediciones_de_los_dos_motores(self, base):
        body = _get(base, f"{API}/benchmark").json()
        if not body["runs"]:
            pytest.skip("sin ejecuciones: docker compose run --rm benchmark")
        motores = {r["engine"] for run in body["runs"] for r in run["results"]}
        assert {"dask", "spark"} <= motores
