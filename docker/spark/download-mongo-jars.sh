#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Descarga el MongoDB Spark Connector y sus dependencias desde Maven Central.
#
# Se hace en tiempo de BUILD y no en tiempo de ejecucion con
# `--packages org.mongodb.spark:...` a proposito:
#
#   * cada `spark-submit` con --packages vuelve a resolver el arbol de
#     dependencias con Ivy, lo que tarda 30-60 s y necesita red;
#   * si la red falla durante la sustentacion, el trabajo no arranca;
#   * en Jenkins, cada build pagaria ese coste de nuevo.
#
# Con los jars dentro de la imagen, Spark arranca sin red y de forma inmediata.
# ---------------------------------------------------------------------------
set -euo pipefail

JARS_DIR="${1:-/opt/spark/jars}"
CONNECTOR_VERSION="${MONGO_SPARK_CONNECTOR_VERSION:-10.4.0}"
DRIVER_VERSION="${MONGO_JAVA_DRIVER_VERSION:-5.1.4}"
MAVEN="${MAVEN_REPO:-https://repo1.maven.org/maven2}"

mkdir -p "$JARS_DIR"

download() {
  local path="$1" file="$2"
  echo ">> ${file}"
  curl --fail --silent --show-error --location --retry 5 --retry-delay 3 \
       --connect-timeout 20 \
       -o "${JARS_DIR}/${file}" "${MAVEN}/${path}/${file}"
}

# Conector de Spark (Scala 2.12, alineado con Spark 3.5)
download "org/mongodb/spark/mongo-spark-connector_2.12/${CONNECTOR_VERSION}" \
         "mongo-spark-connector_2.12-${CONNECTOR_VERSION}.jar"

# Driver de Java de MongoDB: el conector 10.x lo necesita completo.
# Los tres jars son obligatorios; sin bson-record-codec el conector falla en
# tiempo de ejecucion con NoClassDefFoundError.
for artifact in mongodb-driver-sync mongodb-driver-core bson bson-record-codec; do
  download "org/mongodb/${artifact}/${DRIVER_VERSION}" \
           "${artifact}-${DRIVER_VERSION}.jar"
done

echo ""
echo "Jars de MongoDB instalados en ${JARS_DIR}:"
ls -1sh "${JARS_DIR}" | grep -Ei 'mongo|bson' || true
