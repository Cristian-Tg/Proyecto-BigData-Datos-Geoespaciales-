# ===========================================================================
# Imagen del benchmark: necesita los DOS motores en el mismo contenedor.
#
# Parte de la imagen de Spark (no de la de Dask) porque el driver de PySpark
# necesita una JVM; instalar Java sobre python:slim seria mas fragil que
# instalar Dask sobre la imagen oficial de Spark.
#
# Contexto de build: la raiz del repositorio.
# ===========================================================================
ARG SPARK_IMAGE=spark:3.5.3-scala2.12-java17-python3-ubuntu
FROM ${SPARK_IMAGE}

USER root

ARG MONGO_SPARK_CONNECTOR_VERSION=10.4.0
ARG MONGO_JAVA_DRIVER_VERSION=5.1.4
ENV MONGO_SPARK_CONNECTOR_VERSION=${MONGO_SPARK_CONNECTOR_VERSION} \
    MONGO_JAVA_DRIVER_VERSION=${MONGO_JAVA_DRIVER_VERSION}

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl ca-certificates procps; \
    rm -rf /var/lib/apt/lists/*

COPY docker/spark/download-mongo-jars.sh /usr/local/bin/download-mongo-jars.sh
RUN chmod +x /usr/local/bin/download-mongo-jars.sh \
 && /usr/local/bin/download-mongo-jars.sh /opt/spark/jars

# Dask + Spark en el mismo entorno. Se instala primero spark.txt para que el
# numpy 1.26.4 que fija ese archivo gane frente a cualquier resolucion de Dask.
COPY requirements/ /tmp/requirements/
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r /tmp/requirements/spark.txt \
 && pip install --no-cache-dir -r /tmp/requirements/ingestion.txt \
 && python -c "import numpy, pandas, dask, distributed, psutil, pymongo; \
print('numpy', numpy.__version__, '| pandas', pandas.__version__, \
'| dask', dask.__version__)" \
 && rm -rf /tmp/requirements

WORKDIR /app
COPY src/ /app/src/

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:/opt/spark/sbin:$PATH \
    HOME=/tmp

RUN mkdir -p /data/benchmark /tmp/.ivy2 && chmod -R 0777 /data /tmp/.ivy2

CMD ["python", "-m", "src.benchmark.compare_dask_spark"]
