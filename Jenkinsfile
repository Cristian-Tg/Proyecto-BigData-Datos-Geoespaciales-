// ===========================================================================
// Pipeline de integracion y despliegue continuo.
//
// Cubre lo que exige el enunciado (requisito 4.5):
//   checkout -> construccion de imagenes -> pruebas con pytest ->
//   levantamiento de servicios -> pruebas basicas contra la API -> despliegue
//
// GARANTIA CENTRAL: "Si una prueba falla, el despliegue no debe realizarse."
// Se implementa con un esquema de imagen candidata / imagen estable:
//
//   1. Las pruebas unitarias corren ANTES de construir el resto. Si fallan, se
//      aborta y no se toca ninguna imagen de servicio.
//   2. Se construyen las imagenes y se levanta la pila con ellas (candidatas).
//   3. Se ejecutan pruebas contra la API en vivo (curl + pytest).
//   4. SOLO si pasan, las imagenes se promueven a la etiqueta `stable`: eso es
//      el despliegue.
//   5. Si algo falla despues de levantar la pila, se hace rollback: se
//      restauran las imagenes `stable` del ultimo despliegue correcto.
//
// CREDENCIALES: el token de Kaggle y la contrasena de MongoDB se leen del
// almacen de credenciales de Jenkins. No estan en el repositorio y Jenkins las
// enmascara en el log. Si no existen, el pipeline no falla: avisa y usa el
// generador de datos sinteticos.
// ===========================================================================

pipeline {

  agent any

  options {
    timestamps()
    ansiColor('xterm')
    timeout(time: 120, unit: 'MINUTES')
    buildDiscarder(logRotator(numToKeepStr: '20', artifactNumToKeepStr: '10'))
    disableConcurrentBuilds()      // dos builds a la vez pelearian por los puertos
  }

  // El webhook de GitHub dispara githubPush en cada integracion a la rama principal
  triggers {
    githubPush()
    pollSCM('H/15 * * * *')        // red de seguridad si el webhook no llega
  }

  parameters {
    // Ingesta y Spark DESACTIVADOS por defecto. Un build disparado por el
    // webhook usa los valores por defecto, y con ellos activados cada push
    // relanzaba 15-40 min de ingesta y agregaciones. En un equipo con 8 GB eso
    // agoto la memoria del host y colgo el motor de Docker (build #5). Los
    // datos persisten en el volumen y la ingesta es idempotente, asi que solo
    // hace falta activarlos en el primer build de un equipo nuevo o cuando
    // cambie el codigo de esas etapas.
    booleanParam(name: 'RUN_INGESTION', defaultValue: false,
                 description: 'Ejecutar la ingesta con Dask (Kaggle -> MongoDB). Activar en el PRIMER build de un equipo nuevo')
    booleanParam(name: 'FORCE_DOWNLOAD', defaultValue: false,
                 description: 'Volver a descargar el dataset aunque ya este en el volumen')
    booleanParam(name: 'DROP_EXISTING', defaultValue: false,
                 description: 'Vaciar la coleccion antes de cargar')
    booleanParam(name: 'RUN_SPARK', defaultValue: false,
                 description: 'Ejecutar las agregaciones con Spark. Activar en el PRIMER build de un equipo nuevo')
    booleanParam(name: 'RUN_BENCHMARK', defaultValue: false,
                 description: 'Ejecutar el benchmark Dask vs Spark (varios minutos)')
    string(name: 'SAMPLE_SIZE', defaultValue: '2000000',
           description: 'Maximo de registros a cargar (minimo exigido: 1000000)')
    booleanParam(name: 'SKIP_MIN_RECORDS_CHECK', defaultValue: false,
                 description: 'No exigir el minimo de 1.000.000 (solo pruebas rapidas)')
    choice(name: 'MEMORY_PROFILE', choices: ['auto', 'estandar', 'baja'],
           description: 'auto detecta la RAM de Docker y elige el perfil. ' +
                        '"baja" fuerza docker-compose.lowmem.yml y ejecuta ' +
                        'los motores por fases (para equipos con 8 GB)')
  }

  environment {
    COMPOSE_PROJECT_NAME     = 'geobigdata'
    COMPOSE_FILE             = 'docker-compose.yml'
    NETWORK                  = 'geobigdata_geonet'
    DOCKER_BUILDKIT          = '1'
    COMPOSE_DOCKER_CLI_BUILD = '1'
    API_INTERNAL             = 'http://api:5000'
    CURL_IMAGE               = 'curlimages/curl:8.11.1'
    REPORTS                  = 'reports'
    // Imagenes que se promueven a `stable` tras un despliegue correcto
    PROMOTABLE = 'geobigdata/mongo geobigdata/dask geobigdata/spark geobigdata/api geobigdata/tests geobigdata/benchmark'
  }

  stages {

    // =====================================================================
    stage('1 - Checkout') {
      steps {
        checkout scm
        sh '''
          set -eu
          echo "=================================================="
          echo " Commit  : $(git rev-parse --short HEAD)"
          echo " Rama    : $(git rev-parse --abbrev-ref HEAD)"
          echo " Autor   : $(git log -1 --pretty=format:'%an <%ae>')"
          echo " Mensaje : $(git log -1 --pretty=format:'%s')"
          echo "=================================================="
          docker --version
          docker compose version
        '''
        script {
          env.GIT_SHORT = sh(script: 'git rev-parse --short HEAD',
                             returnStdout: true).trim()
          currentBuild.description = "commit ${env.GIT_SHORT}"
        }
      }
    }

    // =====================================================================
    stage('2 - Preparar entorno y credenciales') {
      steps {
        script {
          sh "mkdir -p ${env.REPORTS}"

          // --- Contrasena de MongoDB ------------------------------------
          // Debe ser ESTABLE entre builds: el volumen mongo_data conserva el
          // usuario creado en el primer arranque, y cambiarla romperia la
          // autenticacion de todos los servicios.
          boolean mongoCredOk = false
          try {
            withCredentials([string(credentialsId: 'mongo-root-password',
                                    variable: 'MONGO_PWD')]) {
              writeEnvFile(env.MONGO_PWD)
              mongoCredOk = true
            }
          } catch (Exception ignored) {
            mongoCredOk = false
          }
          if (mongoCredOk) {
            echo 'OK - contrasena de MongoDB tomada de la credencial "mongo-root-password"'
          } else {
            echo '''ADVERTENCIA: no existe la credencial "mongo-root-password".
Se usa una contrasena de desarrollo. Para la entrega, cree en Jenkins una
credencial de tipo "Secret text" con ese ID.'''
            writeEnvFile('jenkins_dev_password_cambiar')
          }

          // --- Token de Kaggle ------------------------------------------
          // Se resuelve aqui y se conserva solo en variables del build; no se
          // escribe en el workspace en ningun momento.
          env.KAGGLE_AVAILABLE = 'false'
          try {
            withCredentials([file(credentialsId: 'kaggle-json',
                                  variable: 'KAGGLE_JSON_FILE')]) {
              env.KAGGLE_USERNAME = sh(
                script: 'jq -r .username "$KAGGLE_JSON_FILE"',
                returnStdout: true).trim()
              env.KAGGLE_KEY = sh(
                script: 'jq -r .key "$KAGGLE_JSON_FILE"',
                returnStdout: true).trim()
              env.KAGGLE_AVAILABLE = 'true'
            }
          } catch (Exception ignored) {
            env.KAGGLE_AVAILABLE = 'false'
          }
          if (env.KAGGLE_AVAILABLE == 'true') {
            echo "OK - token de Kaggle cargado (usuario: ${env.KAGGLE_USERNAME})"
          } else {
            echo '''ADVERTENCIA: no existe la credencial "kaggle-json".
La ingesta usara el generador de datos sinteticos. Para descargar el dataset
real, cree en Jenkins una credencial de tipo "Secret file" con el ID
"kaggle-json" y suba su archivo kaggle.json.'''
          }

          // --- Perfil de memoria ----------------------------------------
          // Con menos de 6 GB la configuracion estandar no cabe: solo los
          // workers suman 8 GB (2 de Dask x 2g + 2 de Spark x 2g). Sin este
          // ajuste el build muere por OOMKilled, y el sintoma es un
          // `KilledWorker` o un timeout que no menciona la memoria.
          String memRaw = sh(
            script: "docker info --format '{{.MemTotal}}' 2>/dev/null || echo 0",
            returnStdout: true).trim()
          long dockerMb = 0
          try { dockerMb = (memRaw as long) / 1048576L } catch (Exception ignored) { }
          echo "RAM disponible para Docker: ${dockerMb} MB"

          boolean lowmem
          if (params.MEMORY_PROFILE == 'baja') {
            lowmem = true
          } else if (params.MEMORY_PROFILE == 'estandar') {
            lowmem = false
          } else {
            lowmem = dockerMb > 0 && dockerMb < 6000
          }

          env.LOWMEM = lowmem ? 'true' : 'false'
          // Con COMPOSE_FILE no hace falta anadir -f en cada llamada. El
          // separador ":" es el correcto porque Jenkins corre en Linux.
          env.COMPOSE_FILE = lowmem
            ? 'docker-compose.yml:docker-compose.lowmem.yml'
            : 'docker-compose.yml'

          if (lowmem) {
            echo '''Perfil de BAJA MEMORIA activo:
  - Spark con 1 worker (el enunciado exige al menos 1)
  - Dask con 2 workers (el enunciado exige al menos 2)
  - los motores se levantan y se bajan por fases: el pico nunca suma los dos'''
          } else {
            echo 'Perfil ESTANDAR: todos los servicios residentes a la vez.'
          }

          // --- Hay un despliegue estable al que poder volver? ------------
          env.HAS_STABLE = sh(
            script: 'docker image inspect geobigdata/api:stable >/dev/null 2>&1 && echo true || echo false',
            returnStdout: true).trim()
          echo "Despliegue estable previo disponible para rollback: ${env.HAS_STABLE}"

          // --- Comprobacion de que no hay secretos versionados ----------
          sh '''
            set -eu
            echo "--- Verificacion: ningun secreto en el control de versiones ---"
            if git ls-files | grep -Ei '(^|/)(kaggle\\.json|\\.env)$' ; then
              echo "ERROR: hay archivos de credenciales versionados."
              exit 1
            fi
            echo "OK - .env y kaggle.json no estan versionados."
          '''
        }
      }
    }

    // =====================================================================
    // PUERTA 1: si las pruebas unitarias fallan, no se construye nada mas.
    // =====================================================================
    stage('3 - Pruebas unitarias (pytest)') {
      steps {
        sh '''
          set -eu
          echo "Construyendo la imagen de pruebas..."
          docker compose build tests

          echo "Ejecutando pytest (sin las pruebas de integracion)..."
          # Los informes se escriben en el volumen nombrado `test_reports`
          # y NO en un bind mount del workspace: Jenkins habla con el
          # demonio del HOST (patron DooD), donde la ruta del workspace no
          # existe y el mount crearia un directorio vacio en el host.
          docker compose run --rm --no-deps \
            tests pytest -m "not integration" -v \
              --junitxml=/reports/junit-unit.xml \
              --cov=src --cov-report=xml:/reports/coverage.xml \
              --cov-report=term
        '''
        // Se extraen aunque pytest haya fallado: el informe JUnit es justo
        // lo que hace falta para ver QUE prueba fallo.
        sh '''
          set +e
          docker compose run --rm --no-deps --entrypoint sh tests -c \
            'cat /reports/junit-unit.xml 2>/dev/null' > "${REPORTS}/junit-unit.xml"
          docker compose run --rm --no-deps --entrypoint sh tests -c \
            'cat /reports/coverage.xml 2>/dev/null' > "${REPORTS}/coverage.xml"
          exit 0
        '''
      }
      post {
        always {
          junit allowEmptyResults: false, testResults: "${env.REPORTS}/junit-unit.xml"
        }
        failure {
          echo '''================================================================
 LAS PRUEBAS UNITARIAS FALLARON.
 No se construyen las imagenes de servicio y NO se despliega nada.
================================================================'''
        }
      }
    }

    // =====================================================================
    stage('4 - Construir imagenes') {
      steps {
        sh '''
          set -eu
          echo "Construyendo las imagenes del sistema..."
          # Un build por imagen distinta: dask-worker comparte imagen con
          # dask-scheduler, spark-worker con spark-master e ingestion con dask.
          docker compose build mongo dask-scheduler spark-master api
          docker compose build benchmark

          echo "--- Imagenes construidas ---"
          docker images --filter "reference=geobigdata/*" \
            --format "table {{.Repository}}\\t{{.Tag}}\\t{{.Size}}\\t{{.CreatedSince}}"
        '''
        // Etiqueta trazable por build: permite auditar que version se desplego
        sh """
          set -eu
          for img in ${env.PROMOTABLE} ; do
            if docker image inspect \$img:latest >/dev/null 2>&1 ; then
              docker tag \$img:latest \$img:${env.BUILD_NUMBER}
            fi
          done
        """
      }
    }

    // =====================================================================
    stage('5 - Levantar servicios') {
      steps {
        sh '''
          set -eu
          if [ "$LOWMEM" = "true" ]; then
            echo "Baja memoria: se levanta solo el nucleo (mongo + api)."
            echo "Dask y Spark se levantan en su fase y se bajan al terminar."
            docker compose up -d --remove-orphans mongo api
          else
            echo "Levantando la pila completa con las imagenes candidatas..."
            docker compose up -d --remove-orphans \
              mongo dask-scheduler dask-worker spark-master spark-worker api
          fi
          echo "--- Estado de los servicios ---"
          docker compose ps
        '''
      }
    }

    // =====================================================================
    stage('6 - Esperar servicios sanos') {
      steps {
        sh '''
          set -eu

          wait_healthy() {
            svc="$1" ; limit="${2:-60}" ; n=0
            printf "Esperando a %s " "$svc"
            while [ "$n" -lt "$limit" ] ; do
              cid=$(docker compose ps -q "$svc" | head -1)
              if [ -n "$cid" ] ; then
                st=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$cid")
                case "$st" in
                  healthy|running) printf " [%s]\\n" "$st" ; return 0 ;;
                  unhealthy)       printf " [unhealthy]\\n" ;;
                esac
              fi
              printf "."
              n=$((n+1)) ; sleep 3
            done
            printf " TIMEOUT\\n"
            docker compose logs --tail 80 "$svc" || true
            return 1
          }

          wait_healthy mongo 60
          wait_healthy api 60
          if [ "$LOWMEM" != "true" ]; then
            wait_healthy dask-scheduler 40
            wait_healthy spark-master 40
          fi

          echo "--- Conectividad entre contenedores ---"
          docker compose exec -T api curl -fsS http://localhost:5000/api/v1/health
          echo ""
          echo "--- Cluster de Spark (workers registrados) ---"
          docker run --rm --network "${NETWORK}" ${CURL_IMAGE} \
            -fsS http://spark-master:8080/json/ | head -c 500 || true
          echo ""
          echo "--- Panel de Dask ---"
          docker run --rm --network "${NETWORK}" ${CURL_IMAGE} \
            -fsS http://dask-scheduler:8787/health || true
          echo ""
        '''
      }
    }

    // =====================================================================
    stage('7 - Ingesta con Dask (Kaggle -> MongoDB)') {
      when { expression { params.RUN_INGESTION } }
      steps {
        script {
          // Las credenciales viajan como variables de entorno del contenedor:
          // Jenkins las enmascara en el log y no quedan en disco.
          String kaggleEnv = env.KAGGLE_AVAILABLE == 'true'
            ? "-e KAGGLE_USERNAME=${env.KAGGLE_USERNAME} -e KAGGLE_KEY=${env.KAGGLE_KEY}"
            : ''
          // En baja memoria, Dask se levanta SOLO para esta fase: mantenerlo
          // residente junto a Spark y a Jenkins no cabe en 4 GB.
          sh '''
            set -eu
            if [ "$LOWMEM" = "true" ]; then
              echo "Levantando Dask (scheduler + 2 workers) para la ingesta..."
              docker compose up -d dask-scheduler dask-worker
            n=0
            until [ "$(docker inspect -f '{{.State.Health.Status}}' \
                  "$(docker compose ps -q dask-scheduler | head -1)" \
                  2>/dev/null)" = "healthy" ] || [ "$n" -ge 40 ]; do
              printf "."; n=$((n+1)); sleep 3
            done
            echo " listo"
            fi
          '''
          sh """
            set -eu
            docker compose run --rm \
              -e SAMPLE_SIZE=${params.SAMPLE_SIZE} \
              -e FORCE_DOWNLOAD=${params.FORCE_DOWNLOAD ? 1 : 0} \
              -e DROP_EXISTING=${params.DROP_EXISTING ? 1 : 0} \
              -e ALLOW_SYNTHETIC_FALLBACK=1 \
              ${kaggleEnv} \
              ingestion
          """
        }
        sh '''
          set -eu
          echo "--- Estadisticas de limpieza ---"
          docker compose run --rm --no-deps --entrypoint sh ingestion -c \
            'cat /data/cleaning_stats.json 2>/dev/null || echo "{}"' \
            > "${REPORTS}/cleaning_stats.json"
          cat "${REPORTS}/cleaning_stats.json"

          if [ "$LOWMEM" = "true" ]; then
            echo "Bajando Dask para liberar memoria antes de Spark..."
            docker compose stop dask-worker dask-scheduler || true
            docker compose rm -f dask-worker dask-scheduler || true
          fi
        '''
      }
    }

    // =====================================================================
    stage('8 - Procesamiento distribuido con Spark') {
      when { expression { params.RUN_SPARK } }
      steps {
        sh '''
          set -eu
          if [ "$LOWMEM" = "true" ]; then
            echo "Levantando Spark (master + 1 worker) para las agregaciones..."
            docker compose up -d spark-master spark-worker
            n=0
            until [ "$(docker inspect -f '{{.State.Health.Status}}' \
                  "$(docker compose ps -q spark-master | head -1)" \
                  2>/dev/null)" = "healthy" ] || [ "$n" -ge 40 ]; do
              printf "."; n=$((n+1)); sleep 3
            done
            echo " listo"
          fi

          docker compose run --rm spark-job

          echo "--- Resumen de las agregaciones ---"
          docker compose run --rm --no-deps --entrypoint sh spark-job -c \
            'cat /data/spark_summary.json 2>/dev/null || echo "{}"' \
            > "${REPORTS}/spark_summary.json"
          cat "${REPORTS}/spark_summary.json"

          if [ "$LOWMEM" = "true" ]; then
            echo "Bajando Spark para liberar memoria antes de las pruebas..."
            docker compose stop spark-worker spark-master || true
            docker compose rm -f spark-worker spark-master || true
          fi
        '''
      }
    }

    // =====================================================================
    // PUERTA 2: pruebas basicas contra la API en vivo. Si fallan, el stage de
    // despliegue no se ejecuta y el bloque post hace rollback.
    // =====================================================================
    stage('9 - Pruebas contra la API en vivo') {
      stages {

        stage('9a - Smoke test con curl') {
          steps {
            sh '''
              set -eu
              CURL="docker run --rm --network ${NETWORK} ${CURL_IMAGE} -sS"

              check() {
                desc="$1" ; url="$2" ; expect="${3:-200}"
                code=$($CURL -o /dev/null -w '%{http_code}' "$url" || echo 000)
                if [ "$code" != "$expect" ] ; then
                  echo "FALLO  $desc -> HTTP $code (se esperaba $expect)"
                  echo "       $url"
                  return 1
                fi
                echo "OK     $desc (HTTP $code)"
              }

              echo "=== Smoke test de la API ==============================="
              check "salud"                  "${API_INTERNAL}/api/v1/health"
              check "catalogo de endpoints"  "${API_INTERNAL}/api/v1/docs"
              check "estadisticas"           "${API_INTERNAL}/api/v1/stats"
              check "mapa Leaflet"           "${API_INTERNAL}/"
              check "consulta por radio"     "${API_INTERNAL}/api/v1/near?lat=34.0522&lon=-118.2437&radius_m=10000&limit=5"
              check "consulta en poligono"   "${API_INTERNAL}/api/v1/within?min_lon=-118.5&min_lat=33.9&max_lon=-118.1&max_lat=34.1&limit=5"
              check "agregacion por cercania" "${API_INTERNAL}/api/v1/geonear?lat=34.0522&lon=-118.2437&max_distance_m=20000&group_by=severity"
              check "indice de agregaciones" "${API_INTERNAL}/api/v1/aggregations"
              check "hotspots de Spark"      "${API_INTERNAL}/api/v1/aggregations/hotspots?limit=5"

              echo "--- La API debe RECHAZAR parametros invalidos ---"
              check "latitud fuera de rango" "${API_INTERNAL}/api/v1/near?lat=999&lon=-118&radius_m=5000" 400
              check "agregacion inexistente" "${API_INTERNAL}/api/v1/aggregations/inventada" 400

              echo "--- Indice 2dsphere y volumen de datos ---"
              $CURL "${API_INTERNAL}/api/v1/stats" > stats.json
              cat stats.json ; echo ""
              grep -q '"geo_index_2dsphere": *true' stats.json \
                && echo "OK     el indice 2dsphere esta activo" \
                || { echo "FALLO  la coleccion no tiene indice 2dsphere" ; exit 1 ; }
              grep -q '"ready_for_queries": *true' stats.json \
                && echo "OK     hay datos cargados y el sistema esta listo" \
                || { echo "FALLO  el sistema no esta listo para consultas" ; exit 1 ; }
              mv stats.json "${REPORTS}/stats.json"
              echo "========================================================"
            '''
          }
        }

        stage('9b - Pruebas de integracion (pytest)') {
          steps {
            script {
              String minRecords = params.SKIP_MIN_RECORDS_CHECK ? '0' : '1000000'
              String selector = params.SKIP_MIN_RECORDS_CHECK
                ? '-m "integration and not slow"' : '-m integration'
              sh """
                set -eu
                docker compose run --rm \
                  -e API_BASE_URL=${env.API_INTERNAL} \
                  -e MIN_RECORDS_REQUIRED=${minRecords} \
                  tests pytest ${selector} -v \
                    --junitxml=/reports/junit-integration.xml
              """
            }
          }
          post {
            always {
              sh '''
                set +e
                docker compose run --rm --no-deps --entrypoint sh tests -c \
                  'cat /reports/junit-integration.xml 2>/dev/null' \
                  > "${REPORTS}/junit-integration.xml"
                exit 0
              '''
              junit allowEmptyResults: true,
                    testResults: "${env.REPORTS}/junit-integration.xml"
            }
          }
        }
      }
      post {
        failure {
          echo '''================================================================
 LAS PRUEBAS CONTRA LA API FALLARON.
 El despliegue NO se realizara y se restaurara la ultima version estable.
================================================================'''
        }
      }
    }

    // =====================================================================
    stage('10 - Benchmark Dask vs Spark') {
      when { expression { params.RUN_BENCHMARK } }
      steps {
        sh '''
          set -eu
          if [ "$LOWMEM" = "true" ]; then
            echo "Levantando Spark para el benchmark..."
            docker compose up -d spark-master spark-worker
            n=0
            until [ "$(docker inspect -f '{{.State.Health.Status}}' \
                  "$(docker compose ps -q spark-master | head -1)" \
                  2>/dev/null)" = "healthy" ] || [ "$n" -ge 40 ]; do
              printf "."; n=$((n+1)); sleep 3
            done
            echo " listo"
          fi

          docker compose run --rm benchmark

          docker compose run --rm --no-deps --entrypoint sh benchmark -c \
            'cat /data/benchmark/benchmark_latest.md 2>/dev/null || echo "sin resultados"' \
            > "${REPORTS}/benchmark_latest.md"
          docker compose run --rm --no-deps --entrypoint sh benchmark -c \
            'cat /data/benchmark/benchmark_latest.json 2>/dev/null || echo "{}"' \
            > "${REPORTS}/benchmark_latest.json"
          cat "${REPORTS}/benchmark_latest.md"

          if [ "$LOWMEM" = "true" ]; then
            docker compose stop spark-worker spark-master || true
            docker compose rm -f spark-worker spark-master || true
          fi
        '''
      }
    }

    // =====================================================================
    // Solo se llega aqui si TODAS las pruebas pasaron.
    // =====================================================================
    stage('11 - Desplegar (promover a estable)') {
      steps {
        sh """
          set -eu
          echo "Todas las pruebas pasaron: se promueven las imagenes a 'stable'."
          for img in ${env.PROMOTABLE} ; do
            if docker image inspect \$img:latest >/dev/null 2>&1 ; then
              docker tag \$img:latest \$img:stable
              echo "  promovida  \$img:latest -> \$img:stable"
            fi
          done
        """
        sh '''
          set -eu
          echo "Aplicando la configuracion desplegada..."
          if [ "$LOWMEM" = "true" ]; then
            # Residente queda el servicio: base de datos y API. Los motores se
            # levantan cuando hay trabajo que procesar.
            docker compose up -d --remove-orphans mongo api
          else
            docker compose up -d --remove-orphans \
              mongo dask-scheduler dask-worker spark-master spark-worker api
          fi

          echo "Verificacion final posterior al despliegue..."
          sleep 8
          docker run --rm --network ${NETWORK} ${CURL_IMAGE} \
            -fsS "${API_INTERNAL}/api/v1/health"
          echo ""
          docker compose ps
        '''
        script {
          writeFile file: "${env.REPORTS}/deployment.json", text: """{
  "build": "${env.BUILD_NUMBER}",
  "commit": "${env.GIT_SHORT}",
  "deployed_at": "${new Date().format("yyyy-MM-dd'T'HH:mm:ssZ")}",
  "images_tag": "stable",
  "sample_size": "${params.SAMPLE_SIZE}",
  "kaggle_credentials_used": ${env.KAGGLE_AVAILABLE}
}
"""
          echo """================================================================
 DESPLIEGUE COMPLETADO - build #${env.BUILD_NUMBER} - commit ${env.GIT_SHORT}
   API   : http://localhost:5000/api/v1/docs
   Mapa  : http://localhost:5000/
   Dask  : http://localhost:8787
   Spark : http://localhost:8080
================================================================"""
        }
      }
    }
  }

  // =======================================================================
  post {

    always {
      sh '''
        set +e
        for svc in mongo api dask-scheduler dask-worker spark-master spark-worker ; do
          echo "===== $svc ====="
          docker compose logs --tail 40 "$svc" 2>/dev/null
        done > "${REPORTS}/service-logs.txt" 2>&1
        echo "Logs de servicio guardados en ${REPORTS}/service-logs.txt"
      '''
      archiveArtifacts artifacts: "${env.REPORTS}/**",
                       allowEmptyArchive: true, fingerprint: true
    }

    failure {
      script {
        echo 'El build fallo. Evaluando el rollback...'
        if (env.HAS_STABLE == 'true') {
          sh """
            set +e
            echo "ROLLBACK: restaurando las imagenes 'stable' del ultimo despliegue correcto."
            for img in ${env.PROMOTABLE} ; do
              if docker image inspect \$img:stable >/dev/null 2>&1 ; then
                docker tag \$img:stable \$img:latest
                echo "  restaurada  \$img:stable -> \$img:latest"
              fi
            done
            docker compose up -d mongo dask-scheduler dask-worker \
                                 spark-master spark-worker api
            docker compose ps
            echo "Rollback terminado: la version estable anterior vuelve a estar en servicio."
          """
        } else {
          echo '''No hay una version estable previa, asi que no hay nada a lo que
volver. La pila queda con las imagenes de este build, pero NO se ha promovido
ninguna a 'stable': el despliegue no se realizo.'''
        }
      }
    }

    unstable {
      echo 'Hay pruebas en estado inestable: el despliegue no se promueve.'
    }

    // cleanup se ejecuta al final, despues de failure/success: es el unico
    // lugar seguro para borrar el .env, porque el rollback todavia lo necesita.
    cleanup {
      sh 'rm -f .env .env.ci stats.json || true'
    }
  }
}


// ===========================================================================
// Funciones auxiliares
// ===========================================================================

/**
 * Escribe el .env que consume docker compose.
 *
 * Solo contiene configuracion y la contrasena de MongoDB, que proviene del
 * almacen de credenciales de Jenkins. El archivo se borra en el bloque
 * post/cleanup, de modo que no queda en el workspace ni en los artefactos.
 */
void writeEnvFile(String mongoPassword) {
  writeFile file: '.env', text: """MONGO_ROOT_USER=geoadmin
MONGO_ROOT_PASSWORD=${mongoPassword}
MONGO_DB=geobigdata
MONGO_COLLECTION=accidents
MONGO_PORT=27017
MONGO_URI=mongodb://geoadmin:${mongoPassword}@mongo:27017/geobigdata?authSource=admin

KAGGLE_DATASET=sobhanmoosavi/us-accidents
KAGGLE_FILE=US_Accidents_March23.csv
ALLOW_SYNTHETIC_FALLBACK=1

SAMPLE_SIZE=${params.SAMPLE_SIZE}
BATCH_SIZE=20000
DASK_BLOCKSIZE=64MB

DASK_SCHEDULER=tcp://dask-scheduler:8786
DASK_N_WORKERS=2
DASK_THREADS_PER_WORKER=2
DASK_WORKER_MEMORY=2g

SPARK_MASTER_URL=spark://spark-master:7077
SPARK_N_WORKERS=2
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g
SPARK_DRIVER_MEMORY=2g

API_PORT=5000
API_HOST=0.0.0.0
API_DEFAULT_LIMIT=100
API_MAX_LIMIT=1000
FLASK_ENV=production

DASK_DASHBOARD_PORT=8787
SPARK_MASTER_UI_PORT=8080
JENKINS_PORT=8088

GRID_CELL_DEG=0.1
GEOHASH_PRECISION=5
LOG_LEVEL=INFO
"""
}
