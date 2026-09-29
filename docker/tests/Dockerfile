# ===========================================================================
# Imagen de pruebas. Jenkins la usa en dos momentos distintos:
#
#   1. ANTES de construir el resto  -> pytest -m "not integration"
#      Pruebas unitarias sin dependencias externas. Si fallan, no se construye
#      ni se despliega nada.
#
#   2. DESPUES de levantar la pila  -> pytest -m integration
#      Pruebas contra la API en vivo, dentro de la red de compose.
#
# Contexto de build: la raiz del repositorio.
# ===========================================================================
FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl ca-certificates; \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements/ /app/requirements/
RUN pip install --upgrade pip && pip install -r requirements/dev.txt

COPY src/ /app/src/
COPY tests/ /app/tests/
COPY pytest.ini /app/pytest.ini
COPY ruff.toml /app/ruff.toml

RUN mkdir -p /reports

CMD ["pytest", "-v", "--junitxml=/reports/junit.xml"]
