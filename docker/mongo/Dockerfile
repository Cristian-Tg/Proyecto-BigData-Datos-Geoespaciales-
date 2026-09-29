# ===========================================================================
# MongoDB con el script de inicializacion del esquema ya incorporado.
#
# El script se COPIA a la imagen en lugar de montarse como bind mount porque
# Jenkins ejecuta `docker compose` contra el demonio del HOST (patron DooD):
# una ruta relativa del repositorio no existiria en el host y el mount fallaria
# silenciosamente creando un directorio vacio.
# ===========================================================================
FROM mongo:7.0

# Los scripts de /docker-entrypoint-initdb.d solo se ejecutan la PRIMERA vez,
# cuando el directorio de datos esta vacio.
COPY docker/mongo/init/ /docker-entrypoint-initdb.d/

# El timeout generoso es deliberado: `mongosh` es un proceso de Node.js y
# arrancarlo dentro del contenedor, bajo presion de CPU, supera facilmente los
# 5 s aunque MongoDB este respondiendo con normalidad. Con 5 s el contenedor se
# marcaba unhealthy y `depends_on: service_healthy` bloqueaba la ingesta.
HEALTHCHECK --interval=20s --timeout=20s --start-period=40s --retries=6 \
  CMD mongosh --quiet --norc --eval "db.adminCommand('ping').ok" || exit 1
