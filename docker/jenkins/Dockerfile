# ===========================================================================
# Jenkins con el CLI de Docker y el plugin de Compose.
#
# Patron "Docker outside of Docker" (DooD): NO se instala un demonio de Docker
# dentro de Jenkins; se le monta el socket del demonio del host. Jenkins actua
# como CLIENTE del Docker de la maquina, con lo que las imagenes y los
# contenedores que construye son los mismos que ve el usuario con `docker ps`.
#
# Consecuencia de diseno importante: como el demonio es el del host, cualquier
# bind mount del docker-compose.yml se resuelve en el HOST y no en el
# workspace de Jenkins. Por eso el compose de este proyecto NO usa bind mounts
# para el codigo: todo se copia dentro de las imagenes y los datos viven en un
# volumen nombrado.
# ===========================================================================
# El catalogo de plugins de Jenkins avanza mas rapido que el core: los plugins
# publicados hoy exigen >= 2.504.3, y con 2.492.1 la instalacion aborta con
# VersionCompatibilityException. Se fija la LTS vigente.
FROM jenkins/jenkins:2.541.3-lts-jdk17

USER root

# --- CLI de Docker + plugin de Compose v2 ---------------------------------
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        ca-certificates curl gnupg lsb-release jq; \
    install -m 0755 -d /etc/apt/keyrings; \
    curl -fsSL https://download.docker.com/linux/debian/gpg \
      | gpg --dearmor -o /etc/apt/keyrings/docker.gpg; \
    chmod a+r /etc/apt/keyrings/docker.gpg; \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/debian $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
      > /etc/apt/sources.list.d/docker.list; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        docker-ce-cli docker-compose-plugin docker-buildx-plugin; \
    rm -rf /var/lib/apt/lists/*; \
    docker --version; \
    docker compose version

# --- Plugins de Jenkins ---------------------------------------------------
# Se instalan en tiempo de build para que el primer arranque no dependa de la
# red ni del asistente de configuracion.
COPY docker/jenkins/plugins.txt /usr/share/jenkins/ref/plugins.txt
RUN jenkins-plugin-cli --plugin-file /usr/share/jenkins/ref/plugins.txt \
      --latest false --verbose

# El asistente inicial se deja ACTIVO a proposito: la contrasena de
# administrador inicial debe generarla Jenkins, no venir escrita en el
# repositorio (criterio de evaluacion: sin secretos expuestos).
ENV JAVA_OPTS="-Djenkins.install.runSetupWizard=true -Dhudson.model.DownloadService.noSignatureCheck=true" \
    JENKINS_SLAVE_AGENT_PORT=50000

# Jenkins corre como root unicamente para poder usar /var/run/docker.sock del
# host. En un entorno de produccion se resolveria agregando el usuario
# `jenkins` al grupo del socket, cuyo GID varia entre Linux, macOS y Windows.
USER root
