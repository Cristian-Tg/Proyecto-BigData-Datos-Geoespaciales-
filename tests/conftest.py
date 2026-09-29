"""Fixtures compartidas por las pruebas."""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

# Permite `import src.*` sin instalar el paquete
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Las pruebas unitarias no deben tocar MongoDB ni crear indices al construir
# la aplicacion de Flask.
os.environ.setdefault("API_ENSURE_INDEXES", "0")
os.environ.setdefault("MONGO_URI", "mongodb://localhost:27017/test_geobigdata")
os.environ.setdefault("MONGO_DB", "test_geobigdata")
os.environ.setdefault("GRID_CELL_DEG", "0.1")
os.environ.setdefault("GEOHASH_PRECISION", "5")


@pytest.fixture
def raw_dataframe():
    """DataFrame con la forma del CSV de Kaggle y un caso sucio de cada tipo."""
    import pandas as pd

    rows = [
        # --- 6 registros validos -------------------------------------------
        ("A-1", 2, "2021-02-08 05:46:00", "2021-02-08 11:00:00", 34.0522, -118.2437,
         0.5, "Los Angeles", "Los Angeles", "CA", 62.0, 10.0, "Clear", "Night"),
        ("A-2", 3, "2021-03-09 06:07:59", "2021-03-09 07:00:00", 40.7128, -74.0060,
         1.2, "New York", "New York", "NY", 45.0, 8.0, "Rain", "Day"),
        ("A-3", 4, "2022-07-11 17:30:00", "2022-07-11 19:00:00", 41.8781, -87.6298,
         2.4, "Chicago", "Cook", "IL", 78.0, 9.5, "Cloudy", "Day"),
        ("A-4", 1, "2022-12-25 23:15:00", "2022-12-26 00:30:00", 29.7604, -95.3698,
         0.1, "Houston", "Harris", "TX", 55.0, 10.0, "Fair", "Night"),
        ("A-5", 2, "2023-01-01 08:00:00", "2023-01-01 09:00:00", 25.7617, -80.1918,
         0.8, "Miami", "Miami-Dade", "FL", 80.0, 10.0, "Clear", "Day"),
        ("A-6", 2, "2023-05-20 12:00:00", "2023-05-20 13:00:00", 47.6062, -122.3321,
         0.3, "Seattle", "King", "WA", 60.0, 7.0, "Overcast", "Day"),

        # --- casos sucios, uno por regla de limpieza -----------------------
        ("D-1", 2, "2021-02-08 05:46:00", "2021-02-08 11:00:00", None, None,
         0.5, "X", "X", "CA", 60.0, 10.0, "Clear", "Day"),            # nulos
        ("D-2", 2, "2021-02-08 05:46:00", "2021-02-08 11:00:00", 120.5, -118.0,
         0.5, "X", "X", "CA", 60.0, 10.0, "Clear", "Day"),            # lat > 90
        ("D-3", 2, "2021-02-08 05:46:00", "2021-02-08 11:00:00", 34.0, -300.0,
         0.5, "X", "X", "CA", 60.0, 10.0, "Clear", "Day"),            # lon < -180
        ("D-4", 2, "2021-02-08 05:46:00", "2021-02-08 11:00:00", 0.0, 0.0,
         0.5, "X", "X", "CA", 60.0, 10.0, "Clear", "Day"),            # (0,0)
        ("D-5", 2, "2021-02-08 05:46:00", "2021-02-08 11:00:00", 48.8566, 2.3522,
         0.5, "Paris", "Paris", "FR", 60.0, 10.0, "Clear", "Day"),    # fuera de EE.UU.
        ("D-6", 2, "no-es-una-fecha", "2021-02-08 11:00:00", 34.05, -118.25,
         0.5, "X", "X", "CA", 60.0, 10.0, "Clear", "Day"),            # fecha invalida
        ("D-7", 9, "2021-02-08 05:46:00", "2021-02-08 11:00:00", 34.06, -118.26,
         0.5, "X", "X", "CA", 60.0, 10.0, "Clear", "Day"),            # severidad 9
        ("A-1", 2, "2021-02-08 05:46:00", "2021-02-08 11:00:00", 34.0522, -118.2437,
         0.5, "Los Angeles", "Los Angeles", "CA", 62.0, 10.0, "Clear", "Night"),  # dup
    ]
    columns = ["ID", "Severity", "Start_Time", "End_Time", "Start_Lat", "Start_Lng",
               "Distance(mi)", "City", "County", "State", "Temperature(F)",
               "Visibility(mi)", "Weather_Condition", "Sunrise_Sunset"]
    return pd.DataFrame(rows, columns=columns)


@pytest.fixture
def flask_app():
    """Aplicacion de Flask sin conexion real a MongoDB."""
    from src.api.app import create_app

    application = create_app(ensure_idx=False)
    application.config.update(TESTING=True)
    return application


@pytest.fixture
def client(flask_app):
    return flask_app.test_client()


@pytest.fixture
def api_base_url() -> str:
    """URL base de la API para las pruebas de integracion."""
    return os.environ.get("API_BASE_URL", "http://localhost:5000").rstrip("/")
