"""Pruebas de las consultas geoespaciales.

Se usa una coleccion falsa que registra lo que se le pide, de modo que se puede
afirmar que la consulta construida usa EXACTAMENTE los operadores que exige el
enunciado ($near, $geoWithin, $geoNear) y con la forma correcta, sin necesidad
de un MongoDB en marcha. Las pruebas contra MongoDB real estan en
test_integration.py.
"""
from __future__ import annotations

from datetime import UTC, datetime

import pytest
from bson import ObjectId

from src.api import queries
from src.common.geo import bbox_to_polygon


# ---------------------------------------------------------------------------
# Dobles de prueba
# ---------------------------------------------------------------------------
class FakeCursor:
    def __init__(self, docs):
        self._docs = list(docs)

    def sort(self, *_a, **_k):
        return self

    def skip(self, n):
        self._docs = self._docs[n:]
        return self

    def limit(self, n):
        self._docs = self._docs[:n]
        return self

    def __iter__(self):
        return iter(self._docs)


class FakeCollection:
    """Registra las llamadas recibidas y devuelve documentos predefinidos."""

    def __init__(self, docs=None, count=0):
        self.docs = docs or []
        self._count = count
        self.find_calls = []
        self.aggregate_calls = []
        self.count_calls = []

    def find(self, query=None, projection=None, **_k):
        self.find_calls.append({"query": query, "projection": projection})
        return FakeCursor(self.docs)

    def count_documents(self, query=None, **_k):
        self.count_calls.append(query)
        return self._count

    def aggregate(self, pipeline, **_k):
        self.aggregate_calls.append(pipeline)
        return iter(self.docs)

    def estimated_document_count(self):
        return self._count

    def list_indexes(self):
        return iter([{"key": {"location": "2dsphere"}, "name": "ix_location_2dsphere"}])


class FakeDB:
    def __init__(self, collections=None):
        self._collections = collections or {}
        self.name = "test_geobigdata"

    def __getitem__(self, name):
        return self._collections.setdefault(name, FakeCollection())

    def list_collection_names(self):
        return list(self._collections)


@pytest.fixture
def sample_docs():
    return [
        {"_id": ObjectId(), "accident_id": "A-1", "lat": 34.05, "lon": -118.24,
         "severity": 3, "city": "Los Angeles", "state": "CA",
         "start_time": datetime(2021, 2, 8, 5, 46, tzinfo=UTC),
         "location": {"type": "Point", "coordinates": [-118.24, 34.05]}},
        {"_id": ObjectId(), "accident_id": "A-2", "lat": 34.06, "lon": -118.25,
         "severity": 2, "city": "Los Angeles", "state": "CA",
         "start_time": datetime(2021, 3, 9, 6, 7, tzinfo=UTC),
         "location": {"type": "Point", "coordinates": [-118.25, 34.06]}},
    ]


# ---------------------------------------------------------------------------
# Serializacion
# ---------------------------------------------------------------------------
class TestSerializacion:

    def test_el_objectid_pasa_a_texto(self):
        oid = ObjectId()
        assert queries.jsonify_doc({"_id": oid}) == {"_id": str(oid)}

    def test_las_fechas_pasan_a_iso8601(self):
        out = queries.jsonify_doc({"t": datetime(2021, 2, 8, 5, 46)})
        assert out["t"].startswith("2021-02-08T05:46:00")
        assert "+00:00" in out["t"], "se asume UTC cuando no hay zona horaria"

    def test_funciona_de_forma_recursiva(self):
        oid = ObjectId()
        out = queries.jsonify_doc({"a": [{"b": oid}], "c": {"d": [oid]}})
        assert out["a"][0]["b"] == str(oid)
        assert out["c"]["d"][0] == str(oid)

    def test_los_tipos_simples_no_se_tocan(self):
        data = {"n": 1, "f": 1.5, "s": "x", "b": True, "z": None}
        assert queries.jsonify_doc(data) == data


# ---------------------------------------------------------------------------
# Filtros de atributos
# ---------------------------------------------------------------------------
class TestFiltroDeAtributos:

    def test_filtro_vacio(self):
        assert queries.build_attribute_filter() == {}

    def test_lista_de_severidades(self):
        assert queries.build_attribute_filter(severity=[2, 3]) == {
            "severity": {"$in": [2, 3]}}

    def test_severidad_minima(self):
        assert queries.build_attribute_filter(min_severity=3) == {
            "severity": {"$gte": 3}}

    def test_la_lista_tiene_prioridad_sobre_el_minimo(self):
        out = queries.build_attribute_filter(severity=[4], min_severity=2)
        assert out == {"severity": {"$in": [4]}}

    def test_el_estado_se_normaliza_a_mayusculas(self):
        assert queries.build_attribute_filter(state="ca") == {"state": "CA"}

    def test_la_ciudad_usa_una_regex_anclada_y_escapada(self):
        out = queries.build_attribute_filter(city="St. Louis")
        assert out["city"]["$options"] == "i"
        # el punto debe quedar escapado para que no sea un comodin
        assert r"\." in out["city"]["$regex"]
        assert out["city"]["$regex"].startswith("^")
        assert out["city"]["$regex"].endswith("$")

    def test_la_regex_del_usuario_no_se_interpreta(self):
        """Sin escapado, '.*' seria un comodin costoso (riesgo de ReDoS)."""
        out = queries.build_attribute_filter(city=".*")
        assert out["city"]["$regex"] == r"^\.\*$"

    def test_rango_de_fechas(self):
        desde = datetime(2021, 1, 1, tzinfo=UTC)
        hasta = datetime(2021, 12, 31, tzinfo=UTC)
        out = queries.build_attribute_filter(start_date=desde, end_date=hasta)
        assert out["start_time"] == {"$gte": desde, "$lte": hasta}

    def test_rango_de_horas_normal(self):
        assert queries.build_attribute_filter(hour_from=7, hour_to=9) == {
            "hour": {"$gte": 7, "$lte": 9}}

    def test_rango_de_horas_que_cruza_la_medianoche(self):
        out = queries.build_attribute_filter(hour_from=22, hour_to=5)
        assert out["$or"] == [{"hour": {"$gte": 22}}, {"hour": {"$lte": 5}}]


# ---------------------------------------------------------------------------
# CONSULTA 1: $near
# ---------------------------------------------------------------------------
class TestConsultaNear:

    def test_usa_el_operador_near_con_geometry_geojson(self, sample_docs):
        coll = FakeCollection(sample_docs, count=42)
        db = FakeDB({"accidents": coll})
        queries.query_near(db, lat=34.05, lon=-118.24, radius_m=5000)

        q = coll.find_calls[0]["query"]
        assert "$near" in q["location"]
        near = q["location"]["$near"]
        # Con $geometry GeoJSON, $maxDistance esta en METROS.
        # Con el formato legado [lon, lat] estaria en radianes: error de 10^6.
        assert near["$geometry"]["type"] == "Point"
        assert near["$geometry"]["coordinates"] == [-118.24, 34.05]
        assert near["$maxDistance"] == 5000.0

    def test_min_distance_solo_si_es_mayor_que_cero(self, sample_docs):
        coll = FakeCollection(sample_docs)
        db = FakeDB({"accidents": coll})

        queries.query_near(db, 34.05, -118.24, 5000, min_distance_m=0)
        assert "$minDistance" not in coll.find_calls[0]["query"]["location"]["$near"]

        queries.query_near(db, 34.05, -118.24, 5000, min_distance_m=1000)
        assert coll.find_calls[1]["query"]["location"]["$near"]["$minDistance"] == 1000.0

    def test_el_conteo_usa_centersphere_porque_near_no_es_contable(self, sample_docs):
        """MongoDB no admite $near dentro de count/aggregate."""
        coll = FakeCollection(sample_docs, count=42)
        db = FakeDB({"accidents": coll})
        out = queries.query_near(db, 34.05, -118.24, 5000, with_count=True)

        assert out["total_matching"] == 42
        count_query = coll.count_calls[0]
        assert "$geoWithin" in count_query["location"]
        center, radius_rad = count_query["location"]["$geoWithin"]["$centerSphere"]
        assert center == [-118.24, 34.05]
        # el radio debe ir en RADIANES
        assert radius_rad == pytest.approx(5000 / 6_378_137.0)

    def test_se_puede_desactivar_el_conteo(self, sample_docs):
        coll = FakeCollection(sample_docs)
        db = FakeDB({"accidents": coll})
        out = queries.query_near(db, 34.05, -118.24, 5000, with_count=False)
        assert out["total_matching"] is None
        assert coll.count_calls == []

    def test_el_filtro_de_atributos_se_combina(self, sample_docs):
        coll = FakeCollection(sample_docs)
        db = FakeDB({"accidents": coll})
        queries.query_near(db, 34.05, -118.24, 5000,
                           attribute_filter={"severity": {"$gte": 3}, "state": "CA"})
        q = coll.find_calls[0]["query"]
        assert q["severity"] == {"$gte": 3}
        assert q["state"] == "CA"
        assert "$near" in q["location"]

    def test_la_respuesta_trae_metadatos_de_la_consulta(self, sample_docs):
        db = FakeDB({"accidents": FakeCollection(sample_docs, count=2)})
        out = queries.query_near(db, 34.05, -118.24, 5000)
        assert out["query"]["operator"] == "$near"
        assert out["query"]["center"] == {"lat": 34.05, "lon": -118.24}
        assert out["returned"] == 2
        assert isinstance(out["elapsed_ms"], float)
        assert out["results"][0]["accident_id"] == "A-1"


# ---------------------------------------------------------------------------
# CONSULTA 2: $geoWithin
# ---------------------------------------------------------------------------
class TestConsultaWithin:

    def test_usa_geowithin_con_geometry(self, sample_docs):
        coll = FakeCollection(sample_docs, count=7)
        db = FakeDB({"accidents": coll})
        poly = bbox_to_polygon(-118.5, 33.9, -118.1, 34.1)
        queries.query_within(db, poly, with_summary=False)

        q = coll.find_calls[0]["query"]
        assert q["location"]["$geoWithin"]["$geometry"] == poly

    def test_rechaza_un_poligono_sin_cerrar(self):
        db = FakeDB({"accidents": FakeCollection()})
        # 4 posiciones (supera la regla de minimo) pero sin repetir la primera
        with pytest.raises(ValueError, match="cerrado"):
            queries.query_within(db, {
                "type": "Polygon",
                "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1]]]})

    def test_rechaza_un_anillo_con_muy_pocas_posiciones(self):
        db = FakeDB({"accidents": FakeCollection()})
        with pytest.raises(ValueError, match="4 posiciones"):
            queries.query_within(db, {"type": "Polygon",
                                      "coordinates": [[[0, 0], [1, 0], [1, 1]]]})

    def test_rechaza_una_geometria_de_tipo_incorrecto(self):
        db = FakeDB({"accidents": FakeCollection()})
        with pytest.raises(ValueError, match="Polygon"):
            queries.query_within(db, {"type": "LineString",
                                      "coordinates": [[0, 0], [1, 1]]})

    def test_acepta_multipolygon(self, sample_docs):
        coll = FakeCollection(sample_docs)
        db = FakeDB({"accidents": coll})
        geom = {"type": "MultiPolygon", "coordinates": [
            bbox_to_polygon(-118.5, 33.9, -118.1, 34.1)["coordinates"],
            bbox_to_polygon(-74.1, 40.6, -73.9, 40.8)["coordinates"],
        ]}
        out = queries.query_within(db, geom, with_summary=False)
        assert out["query"]["geometry_type"] == "MultiPolygon"

    def test_el_resumen_usa_un_pipeline_de_agregacion(self, sample_docs):
        """$geoWithin SI es valido dentro de $match, al contrario que $near."""
        coll = FakeCollection([{"count": 2, "avg_severity": 2.5}], count=2)
        db = FakeDB({"accidents": coll})
        poly = bbox_to_polygon(-118.5, 33.9, -118.1, 34.1)
        out = queries.query_within(db, poly, with_summary=True)

        pipeline = coll.aggregate_calls[0]
        assert "$match" in pipeline[0]
        assert "$geoWithin" in pipeline[0]["$match"]["location"]
        assert "$group" in pipeline[1]
        assert out["summary"]["count"] == 2

    def test_sin_resumen_no_se_agrega(self, sample_docs):
        coll = FakeCollection(sample_docs)
        db = FakeDB({"accidents": coll})
        out = queries.query_within(db, bbox_to_polygon(-119, 33, -118, 35),
                                   with_summary=False)
        assert out["summary"] is None
        assert coll.aggregate_calls == []


# ---------------------------------------------------------------------------
# CONSULTA 3: $geoNear
# ---------------------------------------------------------------------------
class TestAgregacionGeoNear:

    def test_geonear_es_la_primera_etapa(self):
        coll = FakeCollection([{"group": 3, "count": 10}])
        db = FakeDB({"accidents": coll})
        queries.aggregate_geo_near(db, 34.05, -118.24, 20000)

        pipeline = coll.aggregate_calls[0]
        assert list(pipeline[0].keys()) == ["$geoNear"], \
            "$geoNear debe ser obligatoriamente la primera etapa del pipeline"

    def test_parametros_obligatorios_de_geonear(self):
        coll = FakeCollection([])
        db = FakeDB({"accidents": coll})
        queries.aggregate_geo_near(db, 34.05, -118.24, 20000)

        gn = coll.aggregate_calls[0][0]["$geoNear"]
        assert gn["near"] == {"type": "Point", "coordinates": [-118.24, 34.05]}
        assert gn["distanceField"] == "distance_m"
        assert gn["maxDistance"] == 20000.0
        assert gn["spherical"] is True, "obligatorio con un indice 2dsphere"
        assert gn["key"] == "location"

    def test_el_filtro_va_dentro_de_geonear_no_en_un_match_posterior(self):
        coll = FakeCollection([])
        db = FakeDB({"accidents": coll})
        queries.aggregate_geo_near(db, 34.05, -118.24, 20000,
                                   attribute_filter={"severity": {"$gte": 3}})
        gn = coll.aggregate_calls[0][0]["$geoNear"]
        assert gn["query"] == {"severity": {"$gte": 3}}

    @pytest.mark.parametrize("group_by,expected", [
        ("severity", "$severity"),
        ("state", "$state"),
        ("hour", "$hour"),
        ("geohash", "$geohash"),
        ("none", None),
    ])
    def test_claves_de_agrupacion(self, group_by, expected):
        coll = FakeCollection([])
        db = FakeDB({"accidents": coll})
        queries.aggregate_geo_near(db, 34.05, -118.24, 20000, group_by=group_by)
        group_stage = next(s for s in coll.aggregate_calls[0] if "$group" in s)
        assert group_stage["$group"]["_id"] == expected

    def test_las_bandas_de_distancia_anaden_una_etapa_addfields(self):
        coll = FakeCollection([])
        db = FakeDB({"accidents": coll})
        queries.aggregate_geo_near(db, 34.05, -118.24, 20000,
                                   group_by="distance_band", band_width_m=2000)
        pipeline = coll.aggregate_calls[0]
        add = next(s for s in pipeline if "$addFields" in s)
        assert "_band" in add["$addFields"]
        assert 2000.0 in add["$addFields"]["_band"]["$multiply"]

    def test_group_by_invalido_lanza_error(self):
        db = FakeDB({"accidents": FakeCollection()})
        with pytest.raises(ValueError, match="group_by"):
            queries.aggregate_geo_near(db, 34.05, -118.24, 20000,
                                       group_by="columna_inexistente")

    def test_la_respuesta_expone_el_pipeline(self):
        db = FakeDB({"accidents": FakeCollection([{"group": 3, "count": 5}])})
        out = queries.aggregate_geo_near(db, 34.05, -118.24, 20000)
        assert out["query"]["operator"] == "$geoNear"
        assert isinstance(out["pipeline"], list)
        assert "$geoNear" in out["pipeline"][0]


# ---------------------------------------------------------------------------
# Resultados de Spark
# ---------------------------------------------------------------------------
class TestAgregacionesDeSpark:

    def test_todos_los_alias_apuntan_a_una_coleccion(self):
        for alias, (coll, field, direction) in queries.AGGREGATION_COLLECTIONS.items():
            assert isinstance(coll, str) and coll.startswith("agg_")
            assert isinstance(field, str) and field
            assert direction in (1, -1)
            assert alias

    def test_alias_desconocido_lanza_error(self):
        db = FakeDB()
        with pytest.raises(ValueError, match="Agregacion desconocida"):
            queries.query_aggregation(db, "no-existe")

    def test_filtra_por_dimension_y_conteo_minimo(self):
        coll = FakeCollection([{"dimension": "hour", "bucket": 8, "count": 100}],
                              count=1)
        db = FakeDB({"agg_temporal": coll})
        queries.query_aggregation(db, "temporal", dimension="hour", min_count=10)
        q = coll.find_calls[0]["query"]
        assert q["dimension"] == "hour"
        assert q["count"] == {"$gte": 10}

    def test_el_bbox_filtra_el_centroide_con_geowithin(self):
        coll = FakeCollection([], count=0)
        db = FakeDB({"agg_grid": coll})
        queries.query_aggregation(db, "grid", bbox=(-119.0, 33.0, -117.0, 35.0))
        q = coll.find_calls[0]["query"]
        assert "$geoWithin" in q["centroid"]
        ring = q["centroid"]["$geoWithin"]["$geometry"]["coordinates"][0]
        assert ring[0] == ring[-1]

    def test_la_respuesta_indica_que_el_origen_es_spark(self):
        db = FakeDB({"agg_grid": FakeCollection([], count=0)})
        out = queries.query_aggregation(db, "grid")
        assert "Spark" in out["source"]
        assert out["collection"] == "agg_grid"
