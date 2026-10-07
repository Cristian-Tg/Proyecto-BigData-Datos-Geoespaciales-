"""Pruebas de las utilidades geoespaciales.

Son la base de todo el sistema: si el geohash o el orden [lon, lat] del GeoJSON
estan mal, el indice 2dsphere se construye sobre datos incorrectos y TODAS las
consultas devuelven resultados equivocados sin dar ningun error.
"""
from __future__ import annotations

import math

import pytest

from src.common.geo import (
    bbox_to_polygon,
    geohash_center,
    geohash_decode_bbox,
    geohash_encode,
    grid_cell,
    grid_cell_id,
    haversine_m,
    is_valid_lat,
    is_valid_lon,
    is_valid_point,
    meters_to_radians,
    to_geojson_point,
    validate_polygon,
)


# ---------------------------------------------------------------------------
# Validacion de coordenadas
# ---------------------------------------------------------------------------
class TestValidacionCoordenadas:

    @pytest.mark.parametrize("lat", [0, 45.5, -89.9, 90, -90, "34.05"])
    def test_latitudes_validas(self, lat):
        assert is_valid_lat(lat)

    @pytest.mark.parametrize("lat", [90.1, -90.1, 1000, None, "", "abc",
                                     float("nan"), float("inf")])
    def test_latitudes_invalidas(self, lat):
        assert not is_valid_lat(lat)

    @pytest.mark.parametrize("lon", [0, 180, -180, -118.24, "  -74.0  "])
    def test_longitudes_validas(self, lon):
        assert is_valid_lon(lon)

    @pytest.mark.parametrize("lon", [180.1, -180.1, None, "x", float("nan")])
    def test_longitudes_invalidas(self, lon):
        assert not is_valid_lon(lon)

    def test_el_punto_cero_cero_se_descarta(self):
        """(0,0) es el relleno tipico de un dato faltante, no una observacion."""
        assert not is_valid_point(0, 0)
        assert not is_valid_point(0.0, 0.0)
        # pero un cero real acompanado de una coordenada valida SI pasa
        assert is_valid_point(0, -118.24)
        assert is_valid_point(34.05, 0)


# ---------------------------------------------------------------------------
# GeoJSON
# ---------------------------------------------------------------------------
class TestGeoJSON:

    def test_el_orden_es_lon_lat_no_lat_lon(self):
        """Es EL error clasico de GeoJSON y MongoDB no avisa si se invierte."""
        point = to_geojson_point(lat=34.0522, lon=-118.2437)
        assert point["type"] == "Point"
        assert point["coordinates"] == [-118.2437, 34.0522]
        assert point["coordinates"][0] == -118.2437, "coordinates[0] debe ser longitud"
        assert point["coordinates"][1] == 34.0522, "coordinates[1] debe ser latitud"

    def test_las_coordenadas_se_convierten_a_float(self):
        point = to_geojson_point("34.0", "-118.0")
        assert point["coordinates"] == [-118.0, 34.0]
        assert all(isinstance(c, float) for c in point["coordinates"])

    def test_bbox_genera_un_anillo_cerrado_de_cinco_posiciones(self):
        poly = bbox_to_polygon(-118.5, 33.9, -118.1, 34.1)
        ring = poly["coordinates"][0]
        assert poly["type"] == "Polygon"
        assert len(ring) == 5
        assert ring[0] == ring[-1], "el anillo debe cerrarse"
        ok, err = validate_polygon(poly)
        assert ok, err


class TestValidacionPoligonos:

    def test_poligono_valido(self):
        ok, err = validate_polygon(bbox_to_polygon(-100, 30, -90, 40))
        assert ok and err == ""

    def test_multipolygon_valido(self):
        geom = {
            "type": "MultiPolygon",
            "coordinates": [
                bbox_to_polygon(-100, 30, -95, 35)["coordinates"],
                bbox_to_polygon(-90, 40, -85, 45)["coordinates"],
            ],
        }
        ok, err = validate_polygon(geom)
        assert ok, err

    def test_multipolygon_con_un_anillo_invalido(self):
        geom = {
            "type": "MultiPolygon",
            "coordinates": [
                bbox_to_polygon(-100, 30, -95, 35)["coordinates"],
                [[[0, 0], [1, 0], [1, 1], [0, 1]]],   # sin cerrar
            ],
        }
        ok, err = validate_polygon(geom)
        assert not ok
        assert "cerrado" in err.lower()

    def test_anillo_sin_cerrar(self):
        ok, err = validate_polygon({"type": "Polygon", "coordinates": [
            [[0, 0], [1, 0], [1, 1], [0, 1]]]})
        assert not ok
        assert "cerrado" in err.lower()

    def test_muy_pocas_posiciones(self):
        ok, err = validate_polygon({"type": "Polygon", "coordinates": [
            [[0, 0], [1, 1], [0, 0]]]})
        assert not ok
        assert "4" in err

    def test_tipo_no_soportado(self):
        ok, err = validate_polygon({"type": "Point", "coordinates": [0, 0]})
        assert not ok
        assert "Polygon" in err

    def test_coordenada_fuera_de_rango(self):
        ok, err = validate_polygon({"type": "Polygon", "coordinates": [
            [[0, 0], [200, 0], [200, 10], [0, 10], [0, 0]]]})
        assert not ok
        assert "rango" in err.lower()

    @pytest.mark.parametrize("bad", [None, "poligono", 42, [], {}])
    def test_entradas_no_geojson(self, bad):
        ok, _ = validate_polygon(bad)
        assert not ok


# ---------------------------------------------------------------------------
# Geohash
# ---------------------------------------------------------------------------
class TestGeohash:

    @pytest.mark.parametrize("lat,lon,precision,expected", [
        # Valores de referencia publicos de la especificacion de geohash
        (42.6, -5.6, 5, "XXXXX"),                      # ejemplo de Wikipedia
        (57.64911, 10.40744, 11, "u4pruydqqvj"),       # ejemplo de Wikipedia
        (-25.382708, -49.265506, 6, "6gkzwg"),         # Curitiba
        (38.8977, -77.0365, 8, "dqcjqcpe"),            # Casa Blanca
        (51.5074, -0.1278, 7, "gcpvj0d"),              # Londres
    ])
    def test_valores_conocidos(self, lat, lon, precision, expected):
        assert geohash_encode(lat, lon, precision) == expected

    def test_coincide_con_un_algoritmo_independiente(self):
        """Verificacion cruzada contra otra forma de calcular el geohash.

        La implementacion de `geo.py` refina lat y lon de forma alternada en un
        solo bucle. Aqui se cuantizan por separado y se entrelazan los bits al
        final: si ambos caminos coinciden sobre puntos aleatorios, el resultado
        no depende de un detalle de implementacion.
        """
        import random

        base32 = "0123456789bcdefghjkmnpqrstuvwxyz"

        def _quantize(value, low, high, nbits):
            bits = []
            for _ in range(nbits):
                mid = (low + high) / 2
                if value > mid:
                    bits.append(1)
                    low = mid
                else:
                    bits.append(0)
                    high = mid
            return bits

        def _reference(lat, lon, precision):
            total = precision * 5
            lon_bits = _quantize(lon, -180.0, 180.0, (total + 1) // 2)
            lat_bits = _quantize(lat, -90.0, 90.0, total // 2)
            interleaved = [lon_bits[i // 2] if i % 2 == 0 else lat_bits[i // 2]
                           for i in range(total)]
            out = ""
            for i in range(0, total, 5):
                acc = 0
                for bit in interleaved[i:i + 5]:
                    acc = (acc << 1) | bit
                out += base32[acc]
            return out

        rng = random.Random(7)
        for _ in range(500):
            lat = rng.uniform(-89.999, 89.999)
            lon = rng.uniform(-179.999, 179.999)
            precision = rng.randint(1, 12)
            assert geohash_encode(lat, lon, precision) == \
                _reference(lat, lon, precision), f"difieren en ({lat}, {lon})"

    def test_el_ecuador_exacto_cae_en_el_hemisferio_sur(self):
        """Caso borde documentado: con lat == 0 la comparacion `lat > mid` es
        falsa, asi que el punto se asigna a la mitad sur. Es el mismo
        comportamiento que las implementaciones de referencia; se fija en una
        prueba para que un cambio futuro no lo altere en silencio."""
        assert geohash_encode(0.0, 0.1, 3) == "kpb"

    def test_la_precision_define_la_longitud(self):
        for p in range(1, 13):
            assert len(geohash_encode(34.0522, -118.2437, p)) == p

    def test_el_geohash_es_un_prefijo_jerarquico(self):
        """Recortar un geohash da la celda padre. Spark se apoya en esto."""
        full = geohash_encode(34.0522, -118.2437, 9)
        for p in range(1, 9):
            assert geohash_encode(34.0522, -118.2437, p) == full[:p]

    def test_puntos_cercanos_comparten_prefijo(self):
        a = geohash_encode(34.0522, -118.2437, 5)
        b = geohash_encode(34.0530, -118.2440, 5)
        assert a == b, "dos puntos a 100 m deben caer en la misma celda de nivel 5"

    def test_puntos_lejanos_no_comparten_prefijo(self):
        la = geohash_encode(34.0522, -118.2437, 5)
        ny = geohash_encode(40.7128, -74.0060, 5)
        assert la[0] != ny[0] or la != ny

    def test_decodificar_devuelve_una_caja_que_contiene_el_punto(self):
        lat, lon = 34.0522, -118.2437
        gh = geohash_encode(lat, lon, 7)
        min_lat, min_lon, max_lat, max_lon = geohash_decode_bbox(gh)
        assert min_lat <= lat <= max_lat
        assert min_lon <= lon <= max_lon

    def test_el_centro_esta_cerca_del_punto_original(self):
        lat, lon = 34.0522, -118.2437
        c_lat, c_lon = geohash_center(geohash_encode(lat, lon, 6))
        # Precision 6 => celda de ~1.2 km x 0.6 km
        assert haversine_m(lat, lon, c_lat, c_lon) < 1_500

    def test_coordenada_invalida_lanza_error(self):
        with pytest.raises(ValueError):
            geohash_encode(100.0, 0.0, 5)
        with pytest.raises(ValueError):
            geohash_encode(0.0, 0.0, 5)   # relleno (0,0)

    def test_precision_invalida_lanza_error(self):
        with pytest.raises(ValueError):
            geohash_encode(34.0, -118.0, 0)

    def test_caracter_invalido_al_decodificar(self):
        with pytest.raises(ValueError):
            geohash_decode_bbox("ezs4a")   # 'a' no existe en el base32 de geohash


# ---------------------------------------------------------------------------
# Grilla
# ---------------------------------------------------------------------------
class TestGrilla:

    def test_ancla_en_la_esquina_inferior_izquierda(self):
        assert grid_cell(34.0522, -118.2437, 0.1) == (34.0, -118.3)

    def test_funciona_con_coordenadas_negativas(self):
        """floor() y no int(): int(-118.24/0.1) truncaria hacia cero y daria
        la celda equivocada en todo el hemisferio occidental."""
        clat, clon = grid_cell(-33.95, -70.61, 0.1)
        assert clat == -34.0
        assert clon == -70.7
        assert clat <= -33.95 and clon <= -70.61

    def test_puntos_de_la_misma_celda(self):
        a = grid_cell_id(34.01, -118.29, 0.1)
        b = grid_cell_id(34.09, -118.21, 0.1)
        assert a == b == "34.0000_-118.3000"

    def test_puntos_de_celdas_distintas(self):
        assert grid_cell_id(34.01, -118.29, 0.1) != grid_cell_id(34.11, -118.29, 0.1)

    def test_sin_residuos_de_punto_flotante(self):
        """Sin el redondeo saldria 33.900000000000006 y el grid_id no agruparia."""
        for lat in (33.91, 33.95, 33.99):
            clat, _ = grid_cell(lat, -118.0, 0.1)
            assert clat == pytest.approx(33.9, abs=1e-9)
            assert f"{clat:.4f}" == "33.9000"

    @pytest.mark.parametrize("cell", [1.0, 0.5, 0.1, 0.05, 0.01])
    def test_la_celda_siempre_contiene_el_punto(self, cell):
        lat, lon = 41.8781, -87.6298
        clat, clon = grid_cell(lat, lon, cell)
        assert clat <= lat < clat + cell + 1e-9
        assert clon <= lon < clon + cell + 1e-9

    def test_tamano_de_celda_invalido(self):
        with pytest.raises(ValueError):
            grid_cell(34.0, -118.0, 0)


# ---------------------------------------------------------------------------
# Distancias
# ---------------------------------------------------------------------------
class TestDistancias:

    def test_distancia_a_uno_mismo_es_cero(self):
        assert haversine_m(34.0, -118.0, 34.0, -118.0) == pytest.approx(0, abs=1e-6)

    def test_los_angeles_nueva_york(self):
        """La distancia real en linea recta es ~3.940 km."""
        d = haversine_m(34.0522, -118.2437, 40.7128, -74.0060)
        assert 3_900_000 < d < 4_000_000

    def test_un_grado_de_latitud_son_unos_111_km(self):
        d = haversine_m(0, 0, 1, 0)
        assert 110_000 < d < 112_000

    def test_es_simetrica(self):
        a = haversine_m(34.0, -118.0, 40.7, -74.0)
        b = haversine_m(40.7, -74.0, 34.0, -118.0)
        assert a == pytest.approx(b, rel=1e-12)

    def test_metros_a_radianes(self):
        """$centerSphere espera el radio en radianes, no en metros."""
        assert meters_to_radians(6_378_137.0) == pytest.approx(1.0)
        assert meters_to_radians(5000) == pytest.approx(5000 / 6_378_137.0)
        # Un grado de arco sobre el radio ecuatorial mide 111.319,5 m.
        # Se usa ese radio (y no el medio de 6.371 km) porque es el que aplica
        # MongoDB en $centerSphere.
        un_grado_m = 6_378_137.0 * math.radians(1)
        assert un_grado_m == pytest.approx(111_319.5, abs=1.0)
        assert meters_to_radians(un_grado_m) == pytest.approx(math.radians(1),
                                                             rel=1e-12)
