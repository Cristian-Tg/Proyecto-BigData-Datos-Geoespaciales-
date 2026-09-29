# ===========================================================================
# Atajos para operar el sistema.   `make help` lista todo.
# ===========================================================================
.DEFAULT_GOAL := help
SHELL := /bin/bash

COMPOSE      := docker compose
INFRA        := mongo dask-scheduler dask-worker spark-master spark-worker api
API_URL      := http://localhost:5000
SAMPLE_SIZE  ?= 2000000

.PHONY: help all up down restart build logs ps clean nuke \
        ingest ingest-fresh spark benchmark test test-unit test-integration \
        lint stats shell-mongo shell-dask shell-spark jenkins-password \
        urls demo-queries

## ---------------------------------------------------------------------------
help:  ## Muestra esta ayuda
	@echo ""
	@echo "  Sistema de datos geoespaciales — Big Data"
	@echo "  ========================================"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  Ejemplos:"
	@echo "    make all                    levanta todo desde cero"
	@echo "    make all SAMPLE_SIZE=1000000"
	@echo "    make ingest SAMPLE_SIZE=500000"
	@echo ""

## --- Ciclo completo --------------------------------------------------------
all:  ## Levanta TODO desde cero (infra + ingesta + Spark) — un solo comando
	@bash scripts/bootstrap.sh --sample $(SAMPLE_SIZE)

all-bench:  ## Como `all` pero incluye el benchmark Dask vs Spark
	@bash scripts/bootstrap.sh --sample $(SAMPLE_SIZE) --benchmark

fresh:  ## Borra volumenes y vuelve a levantar todo desde cero
	@bash scripts/bootstrap.sh --sample $(SAMPLE_SIZE) --fresh

## --- Infraestructura -------------------------------------------------------
build:  ## Construye todas las imagenes
	$(COMPOSE) build mongo dask-scheduler spark-master api tests benchmark

up:  ## Levanta la infraestructura (sin ingesta ni Spark)
	$(COMPOSE) up -d --remove-orphans $(INFRA) jenkins
	@$(MAKE) --no-print-directory ps

down:  ## Para todos los servicios (conserva los datos)
	$(COMPOSE) down --remove-orphans

restart:  ## Reinicia la API (util tras cambiar codigo con `make build`)
	$(COMPOSE) up -d --build --force-recreate api

ps:  ## Estado de los contenedores
	@$(COMPOSE) ps

logs:  ## Sigue los logs de los servicios principales
	$(COMPOSE) logs -f --tail=100 api mongo dask-scheduler spark-master

logs-api:  ## Sigue solo los logs de la API
	$(COMPOSE) logs -f --tail=200 api

clean:  ## Para y elimina contenedores e imagenes huerfanas
	$(COMPOSE) down --remove-orphans
	docker image prune -f

nuke:  ## CUIDADO: para todo y BORRA los volumenes (se pierden los datos)
	@read -p "Esto borrara MongoDB, el CSV descargado y Jenkins. Escriba 'si': " c; \
	 [ "$$c" = "si" ] && $(COMPOSE) down -v --remove-orphans || echo "Cancelado."

## --- Pipeline de datos -----------------------------------------------------
ingest:  ## Descarga de Kaggle, limpia con Dask y carga en MongoDB
	$(COMPOSE) run --rm -e SAMPLE_SIZE=$(SAMPLE_SIZE) ingestion

ingest-fresh:  ## Como `ingest` pero vacia antes la coleccion
	$(COMPOSE) run --rm -e SAMPLE_SIZE=$(SAMPLE_SIZE) -e DROP_EXISTING=1 ingestion

download:  ## Solo descarga el dataset (sin cargarlo)
	$(COMPOSE) run --rm ingestion python -m src.ingestion.download_kaggle

spark:  ## Agregaciones espaciales y temporales con Spark
	$(COMPOSE) run --rm spark-job

benchmark:  ## Comparacion Dask vs Spark (1 y 2 workers, 2 repeticiones)
	$(COMPOSE) run --rm benchmark

benchmark-full:  ## Benchmark mas exhaustivo (1, 2 y 4 workers, 3 repeticiones)
	$(COMPOSE) run --rm benchmark python -m src.benchmark.compare_dask_spark \
	  --dask-workers 1,2,4 --spark-cores 1,2,4 --repeats 3 \
	  --out-dir /data/benchmark

benchmark-report:  ## Muestra el informe del ultimo benchmark
	@$(COMPOSE) run --rm --no-deps --entrypoint sh benchmark -c \
	  'cat /data/benchmark/benchmark_latest.md 2>/dev/null || echo "Sin resultados: ejecute make benchmark"'

## --- Pruebas y calidad -----------------------------------------------------
test: test-unit  ## Alias de test-unit

test-unit:  ## Pruebas unitarias (sin dependencias externas)
	$(COMPOSE) run --rm --no-deps tests pytest -m "not integration" -v

test-integration:  ## Pruebas contra el sistema en marcha
	$(COMPOSE) run --rm -e API_BASE_URL=http://api:5000 tests \
	  pytest -m integration -v

test-all:  ## Todas las pruebas
	$(COMPOSE) run --rm -e API_BASE_URL=http://api:5000 tests pytest -v

test-cov:  ## Pruebas unitarias con informe de cobertura
	$(COMPOSE) run --rm --no-deps tests pytest -m "not integration" \
	  --cov=src --cov-report=term-missing

lint:  ## Revisa el estilo del codigo con ruff
	$(COMPOSE) run --rm --no-deps tests ruff check src tests

lint-fix:  ## Corrige automaticamente lo que ruff pueda arreglar
	$(COMPOSE) run --rm --no-deps tests ruff check src tests --fix

## --- Inspeccion ------------------------------------------------------------
stats:  ## Estado de las colecciones y del indice 2dsphere
	@curl -s $(API_URL)/api/v1/stats | python -m json.tool 2>/dev/null \
	  || curl -s $(API_URL)/api/v1/stats

indexes:  ## Lista los indices de la coleccion principal
	@$(COMPOSE) exec -T mongo mongosh --quiet \
	  -u "$$(grep '^MONGO_ROOT_USER=' .env | cut -d= -f2)" \
	  -p "$$(grep '^MONGO_ROOT_PASSWORD=' .env | cut -d= -f2)" \
	  --authenticationDatabase admin geobigdata \
	  --eval 'db.accidents.getIndexes().forEach(i => print(i.name, JSON.stringify(i.key)))'

shell-mongo:  ## Abre mongosh en la base de datos
	@$(COMPOSE) exec mongo mongosh \
	  -u "$$(grep '^MONGO_ROOT_USER=' .env | cut -d= -f2)" \
	  -p "$$(grep '^MONGO_ROOT_PASSWORD=' .env | cut -d= -f2)" \
	  --authenticationDatabase admin geobigdata

shell-dask:  ## Abre una shell en un worker de Dask
	$(COMPOSE) exec dask-scheduler bash

shell-spark:  ## Abre una shell en el master de Spark
	$(COMPOSE) exec spark-master bash

pyspark:  ## Abre una sesion interactiva de PySpark conectada al cluster
	$(COMPOSE) run --rm spark-job /opt/spark/bin/pyspark \
	  --master spark://spark-master:7077

mongo-express:  ## Levanta la interfaz web de MongoDB en :8081
	$(COMPOSE) --profile tools up -d mongo-express
	@echo "MongoDB Express: http://localhost:8081"

## --- Jenkins ---------------------------------------------------------------
jenkins:  ## Levanta solo Jenkins
	$(COMPOSE) up -d jenkins
	@echo "Jenkins: http://localhost:8088"
	@$(MAKE) --no-print-directory jenkins-password

jenkins-password:  ## Muestra la contrasena inicial de administrador de Jenkins
	@echo "Contrasena inicial de Jenkins:"
	@$(COMPOSE) exec -T jenkins cat /var/jenkins_home/secrets/initialAdminPassword \
	  2>/dev/null || echo "  (Jenkins ya esta configurado o aun esta arrancando)"

jenkins-logs:  ## Sigue los logs de Jenkins
	$(COMPOSE) logs -f --tail=100 jenkins

## --- Utilidades ------------------------------------------------------------
urls:  ## Lista todas las interfaces web del sistema
	@echo ""
	@echo "  Mapa interactivo   $(API_URL)/"
	@echo "  Documentacion API  $(API_URL)/api/v1/docs"
	@echo "  Panel de Dask      http://localhost:8787"
	@echo "  Interfaz de Spark  http://localhost:8080"
	@echo "  Jenkins            http://localhost:8088"
	@echo "  MongoDB Express    http://localhost:8081  (make mongo-express)"
	@echo ""

demo-queries:  ## Ejecuta una consulta de cada tipo y muestra el resultado
	@echo ""
	@echo "=== 1. \$$near — accidentes en 5 km del centro de Los Angeles ==="
	@curl -s "$(API_URL)/api/v1/near?lat=34.0522&lon=-118.2437&radius_m=5000&limit=3" \
	  | python -m json.tool | head -40
	@echo ""
	@echo "=== 2. \$$geoWithin — accidentes graves en el area de Los Angeles ==="
	@curl -s -X POST "$(API_URL)/api/v1/within" -H 'Content-Type: application/json' \
	  -d '{"geometry":{"type":"Polygon","coordinates":[[[-118.55,33.90],[-118.10,33.90],[-118.10,34.15],[-118.55,34.15],[-118.55,33.90]]]},"limit":3,"min_severity":3,"summary":true}' \
	  | python -m json.tool | head -40
	@echo ""
	@echo "=== 3. \$$geoNear — agrupado por severidad en 20 km ==="
	@curl -s "$(API_URL)/api/v1/geonear?lat=34.0522&lon=-118.2437&max_distance_m=20000&group_by=severity" \
	  | python -m json.tool | head -40
	@echo ""
	@echo "=== 4. Spark — 5 zonas de mayor concentracion ==="
	@curl -s "$(API_URL)/api/v1/aggregations/hotspots?limit=5" \
	  | python -m json.tool | head -50
	@echo ""
	@echo "=== 5. Spark — accidentes por hora del dia ==="
	@curl -s "$(API_URL)/api/v1/aggregations/temporal?dimension=hour&limit=24" \
	  | python -m json.tool | head -40
	@echo ""
