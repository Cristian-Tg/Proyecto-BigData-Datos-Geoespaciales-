# ===========================================================================
# Imagen de Spark con el MongoDB Spark Connector ya incorporado.
# Sirve para los tres roles: spark-master, spark-worker y spark-submit.
# Contexto de build: la raiz del repositorio.
# ===========================================================================
ARG SPARK_IMAGE=spark:3.5.3-scala2.12-java17-python3-ubuntu
FROM ${SPARK_IMAGE}

# La imagen oficial corre como el usuario 185 (`spark`). Se necesita root para
# instalar paquetes y, despues, para poder escribir en el volumen /data (en
# Docker Desktop los volumenes nombrados pertenecen a root y un uid 185 no
# podria escribir los resultados del benchmark).
USER root

ARG MONGO_SPARK_CONNECTOR_VERSION=10.4.0
ARG MONGO_JAVA_DRIVER_VERSION=5.1.4
ENV MONGO_SPARK_CONNECTOR_VERSION=${MONGO_SPARK_CONNECTOR_VERSION} \
    MONGO_JAVA_DRIVER_VERSION=${MONGO_JAVA_DRIVER_VERSION}

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl ca-certificates procps; \
    rm -rf /var/lib/apt/lists/*

# La imagen oficial de Spark trae `python3` pero NO `python`. Sin este enlace,
# cualquier `python -m ...` falla con exit 127 ("command not found"), y el
# mensaje no delata que falte el binario: parece que falte el modulo.
RUN set -eux; \
    if ! command -v python >/dev/null 2>&1; then \
      ln -s "$(command -v python3)" /usr/local/bin/python; \
    fi; \
    python --version

# --- Jars del conector -----------------------------------------------------
COPY docker/spark/download-mongo-jars.sh /usr/local/bin/download-mongo-jars.sh
RUN chmod +x /usr/local/bin/download-mongo-jars.sh \
 && /usr/local/bin/download-mongo-jars.sh /opt/spark/jars

# --- Dependencias de Python ------------------------------------------------
COPY requirements/ /tmp/requirements/
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r /tmp/requirements/spark.txt \
 && rm -rf /tmp/requirements

# --- Codigo de la aplicacion ----------------------------------------------
WORKDIR /app
COPY src/ /app/src/
COPY scripts/ /app/scripts/

# PYTHONPATH=/app permite `from src.common import config` tanto en el driver
# como en los executors, que ejecutan el mismo codigo.
# PYSPARK_PYTHON debe ser el MISMO interprete en el driver y en los executors:
# si difieren, Spark aborta con "Python in worker has different version than
# that in driver".
ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:/opt/spark/sbin:$PATH \
    HOME=/tmp \
    IVY_HOME=/tmp/.ivy2

# Directorios escribibles para logs, warehouse de Spark y cache de Ivy
RUN mkdir -p /data /tmp/.ivy2 /opt/spark/logs /opt/spark/work-dir \
 && chmod -R 0777 /tmp/.ivy2 /opt/spark/logs /opt/spark/work-dir /data

# Por defecto no se lanza ningun rol: docker-compose fija el comando de cada
# servicio (Master, Worker o spark-submit).
CMD ["/bin/bash"]
