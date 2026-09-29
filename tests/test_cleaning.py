"""Pruebas de las reglas de limpieza que aplica Dask.

Cada regla descrita en el informe tiene aqui una prueba que la respalda: el
enunciado exige que "las decisiones de limpieza queden justificadas", y una
justificacion sin una prueba que la verifique no es verificable.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.ingestion.cleaning import (
    COLUMN_MAP,
    US_BBOX,
    CleaningStats,
    clean_partition,
    enrich_partition,
    to_documents,
)


# ---------------------------------------------------------------------------
# Contadores
# ---------------------------------------------------------------------------
class TestCleaningStats:

    def test_los_contadores_se_suman_entre_particiones(self):
        a = CleaningStats(rows_in=100, dropped_null_coords=5, rows_out=95, partitions=1)
        b = CleaningStats(rows_in=200, dropped_null_coords=8, rows_out=192, partitions=1)
        total = a + b
        assert total.rows_in == 300
        assert total.dropped_null_coords == 13
        assert total.rows_out == 287
        assert total.partitions == 2

    def test_totales_derivados(self):
        s = CleaningStats(rows_in=1000, rows_out=950)
        assert s.dropped_total == 50
        assert s.retention_pct == pytest.approx(95.0)

    def test_sin_filas_no_divide_por_cero(self):
        assert CleaningStats().retention_pct == 0.0

    def test_el_informe_menciona_todas_las_reglas(self):
        report = CleaningStats(rows_in=10, rows_out=8).report()
        for fragmento in ("Coordenadas nulas", "fuera de rango", "(0,0)",
                          "EE.UU.", "no parseable", "Severidad", "Duplicados",
                          "retencion"):
            assert fragmento.lower() in report.lower()


# ---------------------------------------------------------------------------
# Reglas de limpieza
# ---------------------------------------------------------------------------
class TestLimpieza:

    def test_conserva_los_registros_validos(self, raw_dataframe):
        clean, stats = clean_partition(raw_dataframe)
        assert stats.rows_in == len(raw_dataframe)
        # 6 validos; el duplicado A-1 se elimina
        assert stats.rows_out == 6
        assert set(clean["accident_id"]) == {"A-1", "A-2", "A-3", "A-4", "A-5", "A-6"}

    def test_cada_regla_descarta_exactamente_su_caso(self, raw_dataframe):
        _, s = clean_partition(raw_dataframe)
        assert s.dropped_null_coords == 1             # D-1
        assert s.dropped_coords_out_of_range == 2     # D-2 (lat>90), D-3 (lon<-180)
        assert s.dropped_zero_island == 1             # D-4
        assert s.dropped_outside_us_bbox == 1         # D-5 (Paris)
        assert s.dropped_unparseable_time == 1        # D-6
        assert s.dropped_invalid_severity == 1        # D-7
        assert s.dropped_duplicate_id == 1            # A-1 repetido

    def test_los_descartes_suman_el_total(self, raw_dataframe):
        _, s = clean_partition(raw_dataframe)
        suma = (s.dropped_null_coords + s.dropped_coords_out_of_range
                + s.dropped_zero_island + s.dropped_outside_us_bbox
                + s.dropped_unparseable_time + s.dropped_invalid_severity
                + s.dropped_duplicate_id)
        assert suma == s.dropped_total

    def test_renombra_las_columnas(self, raw_dataframe):
        clean, _ = clean_partition(raw_dataframe)
        for destino in ("accident_id", "lat", "lon", "severity", "start_time",
                        "state", "city", "weather"):
            assert destino in clean.columns
        # los nombres originales del CSV ya no deben estar
        for origen in COLUMN_MAP:
            assert origen not in clean.columns

    def test_start_time_queda_como_datetime(self, raw_dataframe):
        clean, _ = clean_partition(raw_dataframe)
        assert pd.api.types.is_datetime64_any_dtype(clean["start_time"])

    def test_severity_queda_como_entero(self, raw_dataframe):
        clean, _ = clean_partition(raw_dataframe)
        assert clean["severity"].dtype == np.int16
        assert clean["severity"].between(1, 4).all()

    def test_todas_las_coordenadas_sobrevivientes_son_validas(self, raw_dataframe):
        clean, _ = clean_partition(raw_dataframe)
        assert clean["lat"].between(-90, 90).all()
        assert clean["lon"].between(-180, 180).all()
        assert clean["lat"].between(US_BBOX["lat_min"], US_BBOX["lat_max"]).all()
        assert clean["lon"].between(US_BBOX["lon_min"], US_BBOX["lon_max"]).all()
        assert not ((clean["lat"] == 0) & (clean["lon"] == 0)).any()

    def test_particion_vacia(self, raw_dataframe):
        clean, stats = clean_partition(raw_dataframe.iloc[0:0])
        assert clean.empty
        assert stats.rows_in == 0 and stats.rows_out == 0

    def test_coordenadas_como_texto_se_convierten(self):
        df = pd.DataFrame({
            "ID": ["T-1", "T-2"],
            "Severity": [2, 2],
            "Start_Time": ["2021-01-01 10:00:00"] * 2,
            "Start_Lat": ["34.05", "no-numero"],
            "Start_Lng": ["-118.24", "-118.24"],
        })
        clean, stats = clean_partition(df)
        assert stats.rows_out == 1
        assert clean.iloc[0]["lat"] == pytest.approx(34.05)

    def test_sin_columna_de_coordenadas_falla_claramente(self):
        df = pd.DataFrame({"ID": ["X"], "Severity": [2]})
        with pytest.raises(KeyError, match="lat"):
            clean_partition(df)

    def test_alaska_y_hawai_se_conservan(self):
        """La caja de EE.UU. debe incluir los estados no contiguos."""
        df = pd.DataFrame({
            "ID": ["AK-1", "HI-1", "PR-1"],
            "Severity": [2, 2, 2],
            "Start_Time": ["2021-01-01 10:00:00"] * 3,
            "Start_Lat": [61.2181, 21.3069, 18.4655],     # Anchorage, Honolulu, PR
            "Start_Lng": [-149.9003, -157.8583, -66.1057],
        })
        _, stats = clean_partition(df)
        assert stats.dropped_outside_us_bbox == 0
        assert stats.rows_out == 3


# ---------------------------------------------------------------------------
# Enriquecimiento
# ---------------------------------------------------------------------------
class TestEnriquecimiento:

    def test_agrega_las_columnas_derivadas(self, raw_dataframe):
        clean, _ = clean_partition(raw_dataframe)
        rich = enrich_partition(clean, cell_deg=0.1, precision=5)
        for col in ("grid_lat", "grid_lon", "grid_id", "geohash",
                    "year", "month", "day", "hour", "dow", "is_weekend"):
            assert col in rich.columns

    def test_el_geohash_tiene_la_precision_pedida(self, raw_dataframe):
        clean, _ = clean_partition(raw_dataframe)
        rich = enrich_partition(clean, precision=6)
        assert (rich["geohash"].str.len() == 6).all()

    def test_la_grilla_vectorizada_coincide_con_la_escalar(self, raw_dataframe):
        """enrich_partition usa numpy; debe dar lo mismo que geo.grid_cell."""
        from src.common.geo import grid_cell

        clean, _ = clean_partition(raw_dataframe)
        rich = enrich_partition(clean, cell_deg=0.1)
        for _, row in rich.iterrows():
            esperado = grid_cell(row["lat"], row["lon"], 0.1)
            assert row["grid_lat"] == pytest.approx(esperado[0], abs=1e-9)
            assert row["grid_lon"] == pytest.approx(esperado[1], abs=1e-9)

    def test_las_columnas_temporales_son_correctas(self):
        df = pd.DataFrame({
            "ID": ["T-1"], "Severity": [3],
            # 2021-02-08 fue un lunes
            "Start_Time": ["2021-02-08 05:46:00"],
            "Start_Lat": [34.0522], "Start_Lng": [-118.2437],
        })
        clean, _ = clean_partition(df)
        rich = enrich_partition(clean)
        row = rich.iloc[0]
        assert row["year"] == 2021
        assert row["month"] == 2
        assert row["day"] == 8
        assert row["hour"] == 5
        assert row["dow"] == 0            # 0 = lunes
        assert bool(row["is_weekend"]) is False

    def test_detecta_el_fin_de_semana(self):
        df = pd.DataFrame({
            "ID": ["T-1"], "Severity": [3],
            "Start_Time": ["2021-02-13 15:00:00"],   # sabado
            "Start_Lat": [34.0522], "Start_Lng": [-118.2437],
        })
        clean, _ = clean_partition(df)
        rich = enrich_partition(clean)
        assert rich.iloc[0]["dow"] == 5
        assert bool(rich.iloc[0]["is_weekend"]) is True

    def test_particion_vacia(self):
        assert enrich_partition(pd.DataFrame(columns=["lat", "lon"])).empty


# ---------------------------------------------------------------------------
# Conversion a documentos de MongoDB
# ---------------------------------------------------------------------------
class TestDocumentos:

    def test_location_es_un_geojson_point_valido(self, raw_dataframe):
        from src.common.geo import validate_polygon  # noqa: F401  (simetria)

        clean, _ = clean_partition(raw_dataframe)
        docs = to_documents(enrich_partition(clean))
        assert len(docs) == 6
        for doc in docs:
            loc = doc["location"]
            assert loc["type"] == "Point"
            assert isinstance(loc["coordinates"], list)
            assert len(loc["coordinates"]) == 2
            lon, lat = loc["coordinates"]
            # el orden debe ser [lon, lat] y coincidir con las columnas planas
            assert lon == pytest.approx(doc["lon"])
            assert lat == pytest.approx(doc["lat"])
            assert -180 <= lon <= 180
            assert -90 <= lat <= 90

    def test_los_nan_se_convierten_en_none(self):
        df = pd.DataFrame({
            "ID": ["N-1"], "Severity": [2],
            "Start_Time": ["2021-01-01 10:00:00"],
            "Start_Lat": [34.05], "Start_Lng": [-118.24],
            "Temperature(F)": [np.nan], "Visibility(mi)": [np.nan],
            "Weather_Condition": [None],
        })
        clean, _ = clean_partition(df)
        doc = to_documents(enrich_partition(clean))[0]
        assert doc["temperature_f"] is None
        assert doc["visibility_mi"] is None
        # MongoDB guardaria NaN como un double NaN que rompe las agregaciones
        for value in doc.values():
            if isinstance(value, float):
                assert np.isfinite(value)

    def test_los_tipos_de_numpy_se_convierten_a_nativos(self, raw_dataframe):
        """pymongo no sabe codificar np.int16 ni np.bool_."""
        clean, _ = clean_partition(raw_dataframe)
        doc = to_documents(enrich_partition(clean))[0]
        assert isinstance(doc["severity"], int) and not isinstance(doc["severity"], np.integer)
        assert isinstance(doc["year"], int)
        assert isinstance(doc["is_weekend"], bool)
        assert isinstance(doc["lat"], float)

    def test_las_fechas_son_datetime_nativos(self, raw_dataframe):
        from datetime import datetime

        clean, _ = clean_partition(raw_dataframe)
        doc = to_documents(enrich_partition(clean))[0]
        assert isinstance(doc["start_time"], datetime)
        assert not isinstance(doc["start_time"], pd.Timestamp)

    def test_particion_vacia_no_produce_documentos(self):
        assert to_documents(pd.DataFrame(columns=["lat", "lon"])) == []

    def test_el_accident_id_se_conserva_como_clave_natural(self, raw_dataframe):
        clean, _ = clean_partition(raw_dataframe)
        docs = to_documents(enrich_partition(clean))
        ids = [d["accident_id"] for d in docs]
        assert len(ids) == len(set(ids)), "los accident_id deben ser unicos"
