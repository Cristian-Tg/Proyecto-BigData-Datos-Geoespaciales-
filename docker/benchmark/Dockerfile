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

# La imagen oficial de Spark trae `python3` pero NO `python`. Sin este enlace,
# cualquier `python -m ...` falla con exit 127 ("command not found"), y el
# mensaje no delata que falte el binario: parece que falte el modulo.
RUN set -eux; \
    if ! command -v python >/dev/null 2>&1; then \
      ln -s "$(command -v python3)" /usr/local/bin/python; \
    fi; \
    python --version

COPY docker/spark/download-mongo-jars.sh /usr/local/bin/download-mongo-jars.sh
RUN chmod +x /usr/local/bin/download-mongo-jars.sh \
 && /usr/local/bin/download-mongo-jars.sh /opt/spark/jars

# Dask + Spark en el mismo entorno. Se instala primero spark.txt para que el
# numpy 1.26.4 que fija ese archivo gane frente a cualquier resolucion de Dask.
COPY requirements/ /tmp/requirements/
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r /tmp/requirements/spark.txt \
 && pip install --no-cache-dir -r /tmp/requirements/ingestion.txt \
 && python3 -c "import numpy, pandas, dask, distributed, psutil, pymongo; \
print('numpy', numpy.__version__, '| pandas', pandas.__version__, \
'| dask', dask.__version__)" \
 && rm -rf /tmp/requirements


# ---------------------------------------------------------------------------
# pyspark NO esta instalado por pip: vive dentro de la distribucion de Spark,
# en /opt/spark/python, y solo `spark-submit` lo anade al PYTHONPATH. Un
# `python3 -m mi_modulo` falla con ModuleNotFoundError: No module named
# 'pyspark'.
#
# Se resuelve con un archivo .pth en site-packages en lugar de codificar la
# ruta en PYTHONPATH, por dos razones:
#   * el nombre del zip de py4j lleva su version (py4j-0.10.9.7-src.zip) y
#     cambiaria al subir de version de Spark; aqui se descubre con un glob;
#   * la comprobacion final hace que un cambio asi rompa el BUILD con un
#     mensaje claro, en vez de reventar en ejecucion durante una sustentacion.
RUN set -eux; \
    SITE="$(python3 -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')"; \
    PY4J="$(ls /opt/spark/python/lib/py4j-*-src.zip)"; \
    # Dos echo en lugar de printf: el parser de Dockerfile trata el
    # caracter de escape y partiria la instruccion en el salto de linea.
    echo /opt/spark/python  > "$SITE/spark-python.pth"; \
    echo "$PY4J"           >> "$SITE/spark-python.pth"; \
    python3 -c 'import pyspark; print("pyspark", pyspark.__version__)'

WORKDIR /app
COPY src/ /app/src/

# PYSPARK_PYTHON debe ser el MISMO interprete en el driver y en los executors:
# si difieren, Spark aborta con "Python in worker has different version than
# that in driver".
ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:/opt/spark/sbin:$PATH \
    HOME=/tmp

RUN mkdir -p /data/benchmark /tmp/.ivy2 && chmod -R 0777 /data /tmp/.ivy2

CMD ["python3", "-m", "src.benchmark.compare_dask_spark"]
