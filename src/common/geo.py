"""Utilidades geoespaciales puras (sin dependencias externas).

Se mantienen aqui, sin numpy ni pandas, para que puedan usarse identicamente
desde Dask, desde una UDF de Spark y desde la API de Flask, y para que sean
faciles de probar con pytest.
"""
from __future__ import annotations

import math
from collections.abc import Sequence
from typing import Any

# Alfabeto estandar de geohash (base 32, sin a/i/l/o)
_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"

EARTH_RADIUS_M = 6_378_137.0

# Limites validos WGS84
LAT_MIN, LAT_MAX = -90.0, 90.0
LON_MIN, LON_MAX = -180.0, 180.0


# ---------------------------------------------------------------------------
# Validacion de coordenadas
# ---------------------------------------------------------------------------
def is_valid_lat(lat: Any) -> bool:
    """True si lat es un numero real dentro de [-90, 90]."""
    try:
        v = float(lat)
    except (TypeError, ValueError):
        return False
    return math.isfinite(v) and LAT_MIN <= v <= LAT_MAX


def is_valid_lon(lon: Any) -> bool:
    """True si lon es un numero real dentro de [-180, 180]."""
    try:
        v = float(lon)
    except (TypeError, ValueError):
        return False
    return math.isfinite(v) and LON_MIN <= v <= LON_MAX


def is_valid_point(lat: Any, lon: Any) -> bool:
    """Valida el par y descarta el (0, 0) exacto.

    El punto (0,0) cae en el Golfo de Guinea y en datasets de accidentes de
    EE.UU. es siempre un relleno por dato faltante, nunca una observacion real.
    """
    if not (is_valid_lat(lat) and is_valid_lon(lon)):
        return False
    return not (float(lat) == 0.0 and float(lon) == 0.0)


# ---------------------------------------------------------------------------
# GeoJSON
# ---------------------------------------------------------------------------
def to_geojson_point(lat: float, lon: float) -> dict[str, Any]:
    """Construye un GeoJSON Point.

    Nota critica: GeoJSON y MongoDB usan el orden [longitud, latitud],
    al reves de como se escriben normalmente las coordenadas.
    """
    return {"type": "Point", "coordinates": [float(lon), float(lat)]}


def validate_polygon(geometry: dict[str, Any]) -> tuple[bool, str]:
    """Valida una geometria GeoJSON Polygon o MultiPolygon para $geoWithin.

    Devuelve (es_valida, mensaje_de_error).
    """
    if not isinstance(geometry, dict):
        return False, "La geometria debe ser un objeto GeoJSON"

    gtype = geometry.get("type")
    if gtype not in ("Polygon", "MultiPolygon"):
        return False, "El tipo debe ser 'Polygon' o 'MultiPolygon'"

    coords = geometry.get("coordinates")
    if not isinstance(coords, (list, tuple)) or not coords:
        return False, "Falta el arreglo 'coordinates' o esta vacio"

    rings: list[Sequence] = list(coords) if gtype == "Polygon" else [
        r for poly in coords for r in poly]

    for idx, ring in enumerate(rings):
        if not isinstance(ring, (list, tuple)) or len(ring) < 4:
            return False, f"El anillo {idx} necesita al menos 4 posiciones"
        for pos in ring:
            if not isinstance(pos, (list, tuple)) or len(pos) < 2:
                return False, f"Posicion invalida en el anillo {idx}"
            lon, lat = pos[0], pos[1]
            if not is_valid_lon(lon) or not is_valid_lat(lat):
                return False, (f"Coordenada fuera de rango en el anillo {idx}: "
                               f"[{lon}, {lat}]")
        first, last = ring[0], ring[-1]
        if float(first[0]) != float(last[0]) or float(first[1]) != float(last[1]):
            return False, (f"El anillo {idx} no esta cerrado: la primera y la "
                           "ultima posicion deben coincidir")

    return True, ""


def bbox_to_polygon(min_lon: float, min_lat: float,
                    max_lon: float, max_lat: float) -> dict[str, Any]:
    """Convierte un bounding box en un GeoJSON Polygon cerrado."""
    return {
        "type": "Polygon",
        "coordinates": [[
            [min_lon, min_lat],
            [max_lon, min_lat],
            [max_lon, max_lat],
            [min_lon, max_lat],
            [min_lon, min_lat],
        ]],
    }


# ---------------------------------------------------------------------------
# Geohash
# ---------------------------------------------------------------------------
def geohash_encode(lat: float, lon: float, precision: int = 5) -> str:
    """Codifica lat/lon en un geohash de la precision indicada.

    Implementacion propia por biseccion sucesiva, para no depender de la
    libreria `python-geohash` (que necesita compilador de C y complica la
    imagen de Spark).

    Precision 5 ~ celdas de 4.9 km x 4.9 km, una escala adecuada para
    detectar zonas de alta concentracion de accidentes.
    """
    if precision < 1:
        raise ValueError("precision debe ser >= 1")
    if not is_valid_point(lat, lon):
        raise ValueError(f"Coordenada invalida: lat={lat}, lon={lon}")

    lat_interval = [LAT_MIN, LAT_MAX]
    lon_interval = [LON_MIN, LON_MAX]
    out: list[str] = []
    bit = 0
    ch = 0
    even = True

    while len(out) < precision:
        if even:  # bits pares refinan la longitud
            mid = (lon_interval[0] + lon_interval[1]) / 2
            if lon > mid:
                ch |= (1 << (4 - bit))
                lon_interval[0] = mid
            else:
                lon_interval[1] = mid
        else:     # bits impares refinan la latitud
            mid = (lat_interval[0] + lat_interval[1]) / 2
            if lat > mid:
                ch |= (1 << (4 - bit))
                lat_interval[0] = mid
            else:
                lat_interval[1] = mid
        even = not even

        if bit < 4:
            bit += 1
        else:
            out.append(_BASE32[ch])
            bit = 0
            ch = 0

    return "".join(out)


def geohash_decode_bbox(gh: str) -> tuple[float, float, float, float]:
    """Devuelve (min_lat, min_lon, max_lat, max_lon) de la celda del geohash."""
    lat_interval = [LAT_MIN, LAT_MAX]
    lon_interval = [LON_MIN, LON_MAX]
    even = True

    for char in gh:
        try:
            cd = _BASE32.index(char)
        except ValueError:
            raise ValueError(
                f"Caracter invalido en geohash: {char!r}") from None
        for mask in (16, 8, 4, 2, 1):
            if even:
                mid = (lon_interval[0] + lon_interval[1]) / 2
                if cd & mask:
                    lon_interval[0] = mid
                else:
                    lon_interval[1] = mid
            else:
                mid = (lat_interval[0] + lat_interval[1]) / 2
                if cd & mask:
                    lat_interval[0] = mid
                else:
                    lat_interval[1] = mid
            even = not even

    return lat_interval[0], lon_interval[0], lat_interval[1], lon_interval[1]


def geohash_center(gh: str) -> tuple[float, float]:
    """Centro (lat, lon) de la celda de un geohash."""
    min_lat, min_lon, max_lat, max_lon = geohash_decode_bbox(gh)
    return (min_lat + max_lat) / 2, (min_lon + max_lon) / 2


# ---------------------------------------------------------------------------
# Grilla regular
# ---------------------------------------------------------------------------
def grid_cell(lat: float, lon: float, cell_deg: float = 0.1) -> tuple[float, float]:
    """Ancla (lat, lon) de la celda de una grilla regular en grados.

    Se usa floor sobre el cociente para que la celda quede definida por su
    esquina inferior-izquierda y el resultado sea estable ante flotantes.
    """
    if cell_deg <= 0:
        raise ValueError("cell_deg debe ser > 0")
    cell_lat = math.floor(lat / cell_deg) * cell_deg
    cell_lon = math.floor(lon / cell_deg) * cell_deg
    # Redondeo al numero de decimales del tamano de celda para evitar
    # residuos binarios del tipo 33.900000000000006
    decimals = max(0, -int(math.floor(math.log10(cell_deg))) + 2)
    return round(cell_lat, decimals), round(cell_lon, decimals)


def grid_cell_id(lat: float, lon: float, cell_deg: float = 0.1) -> str:
    """Identificador textual estable de una celda de grilla."""
    clat, clon = grid_cell(lat, lon, cell_deg)
    return f"{clat:.4f}_{clon:.4f}"


# ---------------------------------------------------------------------------
# Distancias
# ---------------------------------------------------------------------------
def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distancia sobre la esfera en metros. Se usa para verificar $near."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = (math.sin(dphi / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(dlam / 2) ** 2)
    return 2 * EARTH_RADIUS_M * math.asin(min(1.0, math.sqrt(a)))


def meters_to_radians(meters: float) -> float:
    """Convierte metros a radianes terrestres ($centerSphere los necesita asi)."""
    return float(meters) / EARTH_RADIUS_M
