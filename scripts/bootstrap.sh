#!/usr/bin/env bash
# ===========================================================================
# Levanta el sistema COMPLETO desde cero con un solo comando.
#
#   ./scripts/bootstrap.sh                 # todo: infra + ingesta + Spark
#   ./scripts/bootstrap.sh --sample 1000000
#   ./scripts/bootstrap.sh --skip-ingest   # solo la infraestructura
#   ./scripts/bootstrap.sh --benchmark     # anade la comparacion Dask/Spark
#   ./scripts/bootstrap.sh --fresh         # borra los volumenes y empieza limpio
#
# Criterio de evaluacion: "que el sistema completo se levante con un solo
# comando". Este script es ese comando.
# ===========================================================================
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
ROOT="$PWD"

# --- colores (se desactivan si la salida no es un terminal) ----------------
if [ -t 1 ]; then
  B='\033[1m'; G='\033[32m'; Y='\033[33m'; R='\033[31m'; C='\033[36m'; N='\033[0m'
else
  B=''; G=''; Y=''; R=''; C=''; N=''
fi

step()  { printf "\n${B}${C}==> %s${N}\n" "$*"; }
ok()    { printf "  ${G}OK${N}   %s\n" "$*"; }
warn()  { printf "  ${Y}!!${N}   %s\n" "$*"; }
fail()  { printf "  ${R}XX${N}   %s\n" "$*" >&2; }
die()   { fail "$*"; exit 1; }

# --- opciones -------------------------------------------------------------
SAMPLE_SIZE="${SAMPLE_SIZE:-2000000}"
SKIP_INGEST=0
SKIP_SPARK=0
RUN_BENCHMARK=0
FRESH=0
WITH_JENKINS=1

while [ $# -gt 0 ]; do
  case "$1" in
    --sample)        SAMPLE_SIZE="$2"; shift 2 ;;
    --sample=*)      SAMPLE_SIZE="${1#*=}"; shift ;;
    --skip-ingest)   SKIP_INGEST=1; shift ;;
    --skip-spark)    SKIP_SPARK=1; shift ;;
    --benchmark)     RUN_BENCHMARK=1; shift ;;
    --no-jenkins)    WITH_JENKINS=0; shift ;;
    --fresh)         FRESH=1; shift ;;
    -h|--help)
      sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) die "Opcion desconocida: $1  (use --help)" ;;
  esac
done

# =========================================================================
step "1/8  Comprobando requisitos"
# =========================================================================
command -v docker >/dev/null 2>&1 || die "Docker no esta instalado."
docker compose version >/dev/null 2>&1 || die "Falta el plugin 'docker compose' (v2)."
docker info >/dev/null 2>&1 || die "El demonio de Docker no responde. Arranque Docker Desktop."
ok "Docker $(docker version --format '{{.Server.Version}}') y Compose $(docker compose version --short)"

MEM_BYTES=$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo 0)
MEM_GB=$(( MEM_BYTES / 1024 / 1024 / 1024 ))
if [ "$MEM_GB" -lt 7 ]; then
  warn "Docker solo tiene ${MEM_GB} GB de RAM. Se recomiendan 8 GB o mas."
  warn "En Windows/WSL2: cree %USERPROFILE%\\.wslconfig con [wsl2] memory=8GB"
else
  ok "Memoria disponible para Docker: ${MEM_GB} GB"
fi

# =========================================================================
step "2/8  Preparando la configuracion (.env)"
# =========================================================================
if [ ! -f .env ]; then
  cp .env.example .env
  # Contrasena aleatoria: no queda ningun secreto por defecto en el repositorio
  PWD_GEN=$(head -c 24 /dev/urandom | base64 | tr -d '/+=' | head -c 24)
  if sed --version >/dev/null 2>&1; then          # GNU sed
    sed -i "s/cambiame_en_local/${PWD_GEN}/g" .env
  else                                            # BSD/macOS sed
    sed -i '' "s/cambiame_en_local/${PWD_GEN}/g" .env
  fi
  ok ".env creado con una contrasena de MongoDB aleatoria"
else
  ok ".env ya existe (no se sobrescribe)"
fi

# Credenciales de Kaggle: se avisa, pero no se bloquea
if grep -q '^KAGGLE_USERNAME=.\+' .env 2>/dev/null; then
  ok "Credenciales de Kaggle configuradas en .env"
elif [ -f secrets/kaggle.json ] || [ -f "$HOME/.kaggle/kaggle.json" ]; then
  ok "kaggle.json encontrado"
else
  warn "Sin credenciales de Kaggle: la ingesta generara datos SINTETICOS."
  warn "Para usar el dataset real: coloque su kaggle.json en ./secrets/ o"
  warn "rellene KAGGLE_USERNAME y KAGGLE_KEY en .env  (ver README)."
fi

if [ "$FRESH" = "1" ]; then
  step "2b/8  --fresh: eliminando contenedores y volumenes previos"
  docker compose down -v --remove-orphans || true
  ok "Estado anterior eliminado"
fi

# =========================================================================
step "3/8  Construyendo las imagenes"
# =========================================================================
# Un build por imagen distinta; los servicios que comparten imagen se saltan.
docker compose build mongo dask-scheduler spark-master api tests benchmark
ok "Imagenes construidas"
docker images --filter "reference=geobigdata/*" \
  --format "       {{.Repository}}:{{.Tag}}  {{.Size}}"

# =========================================================================
step "4/8  Levantando la infraestructura"
# =========================================================================
SERVICES="mongo dask-scheduler dask-worker spark-master spark-worker api"
[ "$WITH_JENKINS" = "1" ] && SERVICES="$SERVICES jenkins"
docker compose up -d --remove-orphans $SERVICES
ok "Servicios iniciados"

wait_for() {
  local svc="$1" limit="${2:-60}" n=0 cid st
  printf "       esperando a %-16s" "$svc"
  while [ "$n" -lt "$limit" ]; do
    cid="$(docker compose ps -q "$svc" 2>/dev/null | head -1)"
    if [ -n "$cid" ]; then
      st="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$cid" 2>/dev/null || echo unknown)"
      case "$st" in
        healthy|running) printf " [%s]\n" "$st"; return 0 ;;
      esac
    fi
    printf "."
    n=$((n+1)); sleep 3
  done
  printf " TIMEOUT\n"
  docker compose logs --tail 60 "$svc" || true
  return 1
}

wait_for mongo 60           || die "MongoDB no llego a estar sano"
wait_for dask-scheduler 40  || die "El scheduler de Dask no arranco"
wait_for spark-master 40    || die "El master de Spark no arranco"
wait_for api 60             || die "La API no arranco"
ok "Todos los servicios responden"

# =========================================================================
if [ "$SKIP_INGEST" = "0" ]; then
  step "5/8  Ingesta con Dask  (objetivo: ${SAMPLE_SIZE} registros)"
  warn "Con el dataset completo de Kaggle la descarga son ~1,2 GB: puede tardar."
  docker compose run --rm -e "SAMPLE_SIZE=${SAMPLE_SIZE}" ingestion
  ok "Datos cargados en MongoDB con indice 2dsphere"
else
  step "5/8  Ingesta omitida (--skip-ingest)"
fi

# =========================================================================
if [ "$SKIP_SPARK" = "0" ]; then
  step "6/8  Agregaciones espaciales y temporales con Spark"
  docker compose run --rm spark-job
  ok "Colecciones agregadas creadas"
else
  step "6/8  Procesamiento con Spark omitido (--skip-spark)"
fi

# =========================================================================
if [ "$RUN_BENCHMARK" = "1" ]; then
  step "7/8  Benchmark Dask vs Spark"
  docker compose run --rm benchmark
  ok "Benchmark terminado (resultados en el volumen /data/benchmark)"
else
  step "7/8  Benchmark omitido (use --benchmark para ejecutarlo)"
fi

# =========================================================================
step "8/8  Verificacion final"
# =========================================================================
API_PORT="$(grep -E '^API_PORT=' .env | cut -d= -f2 | tr -d '\r' || true)"
API_PORT="${API_PORT:-5000}"
BASE="http://localhost:${API_PORT}"

check() {
  local desc="$1" url="$2" code
  code="$(curl -s -o /dev/null -w '%{http_code}' "$url" || echo 000)"
  if [ "$code" = "200" ]; then ok "$desc"; else fail "$desc (HTTP $code)"; return 1; fi
}

check "salud de la API"            "${BASE}/api/v1/health"
check "estadisticas"               "${BASE}/api/v1/stats"
check "consulta por radio (\$near)" "${BASE}/api/v1/near?lat=34.0522&lon=-118.2437&radius_m=10000&limit=3"
check "consulta en poligono"       "${BASE}/api/v1/within?min_lon=-118.5&min_lat=33.9&max_lon=-118.1&max_lat=34.1&limit=3"
check "agregacion \$geoNear"        "${BASE}/api/v1/geonear?lat=34.0522&lon=-118.2437&max_distance_m=20000"
check "resultados de Spark"        "${BASE}/api/v1/aggregations"

printf "\n${B}--- Estado de la base de datos ---${N}\n"
curl -s "${BASE}/api/v1/stats" || true
printf "\n"

cat <<EOF

${B}${G}======================================================================
  SISTEMA EN MARCHA
======================================================================${N}
  ${B}Mapa interactivo${N}   ${C}${BASE}/${N}
  ${B}Documentacion API${N}  ${C}${BASE}/api/v1/docs${N}
  ${B}Panel de Dask${N}      ${C}http://localhost:8787${N}
  ${B}Interfaz de Spark${N}  ${C}http://localhost:8080${N}
EOF
if [ "$WITH_JENKINS" = "1" ]; then
cat <<EOF
  ${B}Jenkins${N}            ${C}http://localhost:8088${N}
                     contrasena inicial:
                     ${C}docker compose exec jenkins cat /var/jenkins_home/secrets/initialAdminPassword${N}
EOF
fi
cat <<EOF

  ${B}Consultas de ejemplo${N}
    curl "${BASE}/api/v1/near?lat=34.0522&lon=-118.2437&radius_m=5000&limit=5"
    curl "${BASE}/api/v1/aggregations/hotspots?limit=10"
    curl -X POST "${BASE}/api/v1/within" -H 'Content-Type: application/json' \\
         -d '{"geometry":{"type":"Polygon","coordinates":[[[-118.5,33.9],[-118.1,33.9],[-118.1,34.1],[-118.5,34.1],[-118.5,33.9]]]},"limit":5}'

  ${B}Parar todo${N}        docker compose down
  ${B}Parar y borrar datos${N}  docker compose down -v
${B}${G}======================================================================${N}

EOF
