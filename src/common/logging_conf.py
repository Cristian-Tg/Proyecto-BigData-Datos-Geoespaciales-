"""Configuracion unica de logging para todos los servicios."""
from __future__ import annotations

import logging
import os
import sys

_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)-22s | %(message)s"


def setup_logging(name: str = "app", level: str | None = None) -> logging.Logger:
    """Devuelve un logger con salida a stdout (lo que espera `docker logs`)."""
    lvl = (level or os.environ.get("LOG_LEVEL", "INFO")).upper()
    root = logging.getLogger()
    if not root.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(_FORMAT, datefmt="%Y-%m-%d %H:%M:%S"))
        root.addHandler(handler)
    root.setLevel(getattr(logging, lvl, logging.INFO))

    # Estas librerias son extremadamente verbosas en DEBUG
    for noisy in ("pymongo", "urllib3", "botocore", "distributed.utils_perf",
                  "py4j", "bokeh"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    return logging.getLogger(name)
