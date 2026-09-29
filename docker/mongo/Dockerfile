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

HEALTHCHECK --interval=10s --timeout=5s --start-period=20s --retries=10 \
  CMD mongosh --quiet --eval "db.adminCommand('ping').ok" || exit 1
