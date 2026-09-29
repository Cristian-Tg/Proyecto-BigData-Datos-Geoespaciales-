# ===========================================================================
# API Flask servida con Gunicorn.
# Contexto de build: la raiz del repositorio.
# ===========================================================================
FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl ca-certificates tini; \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements/ /app/requirements/
RUN pip install --upgrade pip && pip install -r requirements/api.txt

COPY src/common/ /app/src/common/
COPY src/api/ /app/src/api/
RUN touch /app/src/__init__.py

# La API no necesita root: se ejecuta con un usuario sin privilegios.
RUN useradd --create-home --uid 10001 apiuser && chown -R apiuser:apiuser /app
USER apiuser

EXPOSE 5000

HEALTHCHECK --interval=15s --timeout=5s --start-period=25s --retries=5 \
  CMD curl -fsS http://localhost:5000/api/v1/health || exit 1

ENTRYPOINT ["/usr/bin/tini", "--"]

# 4 workers sincronos de Gunicorn: las consultas geoespaciales son limitadas
# por MongoDB, no por CPU, asi que el paralelismo por proceso basta y evita la
# complejidad de gevent. El timeout es amplio porque un $geoWithin sobre un
# poligono grande puede tardar varios segundos.
CMD ["gunicorn", "--bind", "0.0.0.0:5000", \
     "--workers", "4", "--threads", "2", "--timeout", "120", \
     "--access-logfile", "-", "--error-logfile", "-", \
     "--capture-output", "--log-level", "info", \
     "src.api.app:app"]
