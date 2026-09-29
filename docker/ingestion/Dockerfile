# ===========================================================================
# Imagen de Dask: scheduler, workers y el trabajo de ingesta/limpieza.
# Una sola imagen para los tres roles garantiza que el codigo que se
# serializa desde el cliente sea EXACTAMENTE el mismo que deserializa el
# worker; versiones distintas de cloudpickle o de pandas rompen el cluster.
# Contexto de build: la raiz del repositorio.
# ===========================================================================
FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

# `procps` aporta ps, que psutil y el panel de Dask usan para las metricas.
# `curl` lo usa el healthcheck del scheduler.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        curl ca-certificates procps tini; \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Las dependencias van en una capa propia y antes del codigo: cambiar una
# linea de Python no invalida la instalacion de pip, que tarda minutos.
COPY requirements/ /app/requirements/
RUN pip install --upgrade pip && pip install -r requirements/ingestion.txt

COPY src/ /app/src/
COPY scripts/ /app/scripts/
RUN chmod +x /app/scripts/*.sh 2>/dev/null || true

# Directorio compartido con el volumen de datos
RUN mkdir -p /data && chmod 0777 /data

# tini como PID 1: sin el, los workers de Dask dejan procesos zombis y
# `docker compose down` tarda el timeout completo en cada parada.
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-c", "print('Especifique un comando: dask scheduler | dask worker | python -m src.ingestion.clean_load')"]
