# Sistema de procesamiento y consulta de datos geoespaciales

Pipeline completo de Big Data sobre **US Accidents** (7,7 M de registros, ~3 GB):
descarga automatizada desde Kaggle, limpieza distribuida con **Dask**,
almacenamiento **GeoJSON** en **MongoDB** con índice `2dsphere`, agregaciones
espaciales y temporales con **Spark**, consultas geoespaciales expuestas por una
API **Flask**, y despliegue continuo con **Jenkins**. Todo en contenedores,
orquestado con **Docker Compose**.

```
   Kaggle API                                          Jenkins (CI/CD)
       │                                                     │
       ▼                                            webhook de GitHub
  CSV (~3 GB)                                                │
       │                                                     ▼
       │  lectura particionada          ┌────────────────────────────────┐
       ▼                                │ pytest → build → up → smoke →  │
  ┌─────────────┐   limpieza + GeoJSON  │ pruebas de API → despliegue    │
  │    DASK     │ ───────────────────▶  └────────────────────────────────┘
  │ 1 scheduler │                                          │
  │  2 workers  │                          si una prueba falla → rollback
  └─────────────┘
       │ insert_many por lotes
       ▼
  ┌──────────────────────────────────┐
  │           MongoDB                │
  │  accidents  (GeoJSON + 2dsphere) │◀────┐
  │  agg_grid, agg_geohash,          │     │  MongoDB Spark Connector
  │  agg_hotspots, agg_temporal,     │     │
  │  agg_state, benchmark_runs       │─────┤
  └──────────────────────────────────┘     │
       │                            ┌─────────────┐
       │  $near / $geoWithin /      │    SPARK    │
       │  $geoNear                  │  1 master   │
       ▼                            │  2 workers  │
  ┌─────────────┐                   └─────────────┘
  │ API Flask   │  +  mapa Leaflet
  │ :5000       │
  └─────────────┘
```

---

## Tabla de contenido

1. [Requisitos previos](#1-requisitos-previos)
2. [Arranque desde cero](#2-arranque-desde-cero)
3. [Credenciales](#3-credenciales)
4. [Servicios y puertos](#4-servicios-y-puertos)
5. [La API](#5-la-api)
6. [Configuración de Jenkins](#6-configuración-de-jenkins)
7. [Benchmark Dask vs Spark](#7-benchmark-dask-vs-spark)
8. [Pruebas](#8-pruebas)
9. [Estructura del repositorio](#9-estructura-del-repositorio)
10. [Decisiones de diseño](#10-decisiones-de-diseño)
11. [Solución de problemas](#11-solución-de-problemas)

---

## 1. Requisitos previos

| Requisito | Mínimo | Comprobación |
|---|---|---|
| Docker Engine | 24+ | `docker --version` |
| Docker Compose | v2 (plugin) | `docker compose version` |
| RAM asignada a Docker | **8 GB** recomendado · 4 GB con `--lowmem` | `docker info --format '{{.MemTotal}}'` |
| Espacio en disco | 20 GB | |
| Puertos libres | 5000, 7077, 8080, 8088, 8786, 8787, 27017 | |

No hace falta instalar Python, Java, Spark, Dask ni MongoDB en el equipo: todo
vive en los contenedores.

> **Windows / WSL2.** Docker Desktop reserva por defecto la mitad de la RAM.
> Cree `%USERPROFILE%\.wslconfig` con:
> ```ini
> [wsl2]
> memory=8GB
> processors=4
> swap=2GB
> ```
> y después `wsl --shutdown`, y reinicie Docker Desktop. Compruebe con
> `docker info --format "{{.MemTotal}}"`.

### Equipos con 8 GB de RAM (o menos de 6 GB para Docker)

El proyecto trae un **perfil de baja memoria** que sí cabe en ~4 GB:

```bash
./scripts/bootstrap.sh --lowmem
```

El script lo activa **solo** si detecta menos de 6 GB en Docker, así que en la
práctica no hay que recordar la opción. Qué cambia:

| | Estándar | Baja memoria |
|---|---|---|
| Workers de Dask | 2 × 2 hilos × 2 GB | 2 × 1 hilo × 400 MB |
| Workers de Spark | 2 × 2 GB | **1** × 700 MB |
| Caché de WiredTiger | 1 GB | 0,25 GB |
| Workers de Gunicorn | 4 | 2 |
| Partición de Dask | 64 MB | 32 MB |
| `SAMPLE_SIZE` | 2 000 000 | 1 000 000 |
| Motores | todos residentes | **por fases** |
| Jenkins | junto al resto | al final, cuando los motores bajaron |

Dos decisiones merecen explicación:

**Spark baja a un worker, Dask conserva dos.** El enunciado exige *«Spark, con
un nodo maestro y al menos **un** worker»* y *«Dask, con un scheduler y al menos
**dos** workers»*. El recorte respeta los dos mínimos.

**Los trabajos se ejecutan por fases.** `--lowmem` levanta Dask solo para la
ingesta y lo baja, levanta Spark solo para las agregaciones y lo baja. El pico
de memoria nunca suma los dos motores. El sistema sigue levantándose con un
comando; lo que cambia es que los dos motores no son residentes a la vez.

Para la sustentación, con los servicios de datos ya arriba:

```bash
docker compose -f docker-compose.yml -f docker-compose.lowmem.yml up -d jenkins
```

Si prefiere tenerlo todo residente a la vez, suba Docker a 5 GB y use el perfil
estándar. Con 8 GB de RAM total eso deja ~3 GB para Windows: funciona, pero hay
que cerrar el navegador durante las demos.

---

## 2. Arranque desde cero

### Un solo comando

```bash
git clone <URL-DEL-REPOSITORIO>
cd Parcial_BigData

# Linux / macOS / Git Bash
./scripts/bootstrap.sh

# Windows PowerShell
.\scripts\bootstrap.ps1
```

El script hace **todo**: comprueba requisitos, genera un `.env` con una
contraseña aleatoria, construye las seis imágenes, levanta la infraestructura,
espera a que cada servicio esté sano, ejecuta la ingesta con Dask, lanza las
agregaciones de Spark y verifica los endpoints al final.

Con `make` disponible:

```bash
make all                      # equivalente a ./scripts/bootstrap.sh
make all SAMPLE_SIZE=1000000  # carga exactamente el mínimo exigido
make all-bench                # además ejecuta el benchmark Dask vs Spark
make help                     # lista todos los objetivos
```

**Duración aproximada** (portátil con 4 núcleos y 8 GB para Docker):

| Etapa | Tiempo |
|---|---|
| Construcción de imágenes (primera vez) | 6 – 12 min |
| Descarga del dataset desde Kaggle (~1,2 GB) | 3 – 10 min |
| Ingesta y limpieza con Dask (2 M de registros) | 5 – 12 min |
| Agregaciones con Spark | 2 – 5 min |
| **Total** | **~20 – 40 min** |

En arranques posteriores el CSV ya está en el volumen y no se vuelve a
descargar; el ciclo completo baja a unos 10 minutos.

### Paso a paso (si prefiere controlar cada etapa)

```bash
cp .env.example .env
$EDITOR .env                       # cambie MONGO_ROOT_PASSWORD

docker compose build mongo dask-scheduler spark-master api tests benchmark
docker compose up -d mongo dask-scheduler dask-worker spark-master spark-worker api jenkins

docker compose run --rm ingestion   # Kaggle → Dask → MongoDB
docker compose run --rm spark-job   # agregaciones espaciales y temporales
docker compose run --rm benchmark   # comparación Dask vs Spark (opcional)
```

### Verificar que quedó bien

```bash
curl http://localhost:5000/api/v1/stats
```

```json
{
  "database": "geobigdata",
  "collections": {
    "accidents": { "exists": true, "count": 2000000 },
    "agg_grid":  { "exists": true, "count": 14832 },
    "agg_hotspots": { "exists": true, "count": 200 }
  },
  "geo_index_2dsphere": true,
  "ready_for_queries": true
}
```

`geo_index_2dsphere: true` y `ready_for_queries: true` son la señal de que el
sistema está operativo. Abra <http://localhost:5000/> para el mapa.

### Parar

```bash
docker compose down        # para todo, conserva los datos
docker compose down -v     # para todo y BORRA los volúmenes
make nuke                  # igual, pidiendo confirmación
```

---

## 3. Credenciales

**En este repositorio no hay ningún secreto.** `.env`, `kaggle.json` y
`secrets/` están en `.gitignore`, y el pipeline de Jenkins aborta el build si
detecta alguno de esos archivos versionado.

### Token de Kaggle en local

1. Entre a <https://www.kaggle.com/settings/api> y, en la sección
   **Legacy API Credentials**, pulse **Create Legacy API Key**. Se descarga un
   `kaggle.json` con `username` y `key`.

   > Los tokens nuevos con prefijo `KGAT_` **no sirven**: la librería instalada
   > (`kaggle==1.7.4.5`) solo reconoce `KAGGLE_USERNAME` y `KAGGLE_KEY`, no
   > `KAGGLE_API_TOKEN`. Hace falta la Legacy API Key, que trae el par completo.
2. Colóquelo en **una** de estas rutas:

```bash
mkdir -p secrets && cp ~/Downloads/kaggle.json secrets/kaggle.json
#   o bien
cp ~/Downloads/kaggle.json ~/.kaggle/kaggle.json
#   o bien, rellene en .env:
#   KAGGLE_USERNAME=su_usuario
#   KAGGLE_KEY=su_clave
```

3. Acepte las condiciones del dataset en
   <https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents> (si no, la API
   devuelve 403).

`src/ingestion/download_kaggle.py` busca el token en este orden:

1. Variables `KAGGLE_USERNAME` / `KAGGLE_KEY`
2. `/run/secrets/kaggle.json` (secreto de Docker o *secret file* de Jenkins)
3. La ruta que indique `$KAGGLE_JSON`
4. `./secrets/kaggle.json`
5. `~/.kaggle/kaggle.json`

### Sin token: modo sintético

Si no hay credenciales y `ALLOW_SYNTHETIC_FALLBACK=1` (el valor por defecto), la
ingesta genera un dataset sintético de más de un millón de filas con la misma
forma que US Accidents: puntos agrupados alrededor de 21 áreas metropolitanas
reales con dispersión gaussiana, y un 3 % de registros sucios inyectados a
propósito (coordenadas nulas, fuera de rango, el relleno `(0,0)`, severidades
inválidas y duplicados) para poder demostrar que la limpieza funciona.

Sirve para desarrollar y para que el CI corra sin gastar cuota de la API, **pero
la entrega final debe usar el dataset real de Kaggle.** Cuando los datos son
sintéticos, la ingesta lo advierte en el log.

### Credenciales en Jenkins

Ver [sección 6](#6-configuración-de-jenkins).

---

## 4. Servicios y puertos

| Servicio | Contenedor | Puerto | Interfaz |
|---|---|---|---|
| API Flask + mapa Leaflet | `geo-api` | 5000 | <http://localhost:5000/> |
| MongoDB | `geo-mongo` | 27017 | — |
| Dask scheduler | `geo-dask-scheduler` | 8786 / 8787 | <http://localhost:8787> |
| Dask workers (×2) | `geobigdata-dask-worker-N` | — | vía el panel |
| Spark master | `geo-spark-master` | 7077 / 8080 | <http://localhost:8080> |
| Spark workers (×2) | `geobigdata-spark-worker-N` | — | vía el master |
| Jenkins | `geo-jenkins` | 8088 | <http://localhost:8088> |
| MongoDB Express | `geo-mongo-express` | 8081 | `make mongo-express` |

Trabajos puntuales (perfil `jobs`, no arrancan con `compose up`):
`ingestion`, `spark-job`, `benchmark`, `tests`.

```bash
make ps       # estado de los contenedores
make urls     # todas las interfaces web
make logs     # logs en vivo de los servicios principales
```

---

## 5. La API

Catálogo completo y autodescriptivo en <http://localhost:5000/api/v1/docs>.

### Endpoint 1 — consulta por radio (`$near`)

```bash
curl "http://localhost:5000/api/v1/near?lat=34.0522&lon=-118.2437&radius_m=5000&limit=10"
```

Devuelve los registros dentro del radio **ordenados del más cercano al más
lejano**; el propio índice `2dsphere` recorre las celdas en orden de
proximidad, sin necesidad de un `$sort`.

Parámetros: `lat`, `lon`, `radius_m` (o `radius_km`) obligatorios;
`limit`, `skip`, `min_distance_m`, `severity` (`2,3,4`), `min_severity`,
`state`, `city`, `weather`, `start_date`, `end_date`, `year`, `hour_from`,
`hour_to`, `count` opcionales.

```bash
# accidentes graves en hora pico de la tarde, en California
curl "http://localhost:5000/api/v1/near?lat=34.0522&lon=-118.2437&radius_m=25000\
&min_severity=3&state=CA&hour_from=16&hour_to=19&limit=50"
```

### Endpoint 2 — consulta dentro de un polígono (`$geoWithin`)

```bash
curl -X POST http://localhost:5000/api/v1/within \
  -H 'Content-Type: application/json' \
  -d '{
        "geometry": {
          "type": "Polygon",
          "coordinates": [[[-118.55,33.90],[-118.10,33.90],
                           [-118.10,34.15],[-118.55,34.15],[-118.55,33.90]]]
        },
        "limit": 100,
        "min_severity": 3,
        "summary": true
      }'
```

Acepta `Polygon`, `MultiPolygon`, un `Feature` de GeoJSON completo (tal como lo
exporta geojson.io) o un atajo `{"bbox":[min_lon,min_lat,max_lon,max_lat]}`.
Con `summary: true` añade estadísticas del área completa (conteo, severidad
media y máxima, ciudades distintas, primer y último evento) calculadas con un
pipeline de agregación.

Variante cómoda para el navegador:

```bash
curl "http://localhost:5000/api/v1/within?min_lon=-118.5&min_lat=33.9&max_lon=-118.1&max_lat=34.1&limit=20"
```

### Endpoint 3 — resultados calculados con Spark

```bash
curl "http://localhost:5000/api/v1/aggregations"                      # índice
curl "http://localhost:5000/api/v1/aggregations/hotspots?limit=20"    # zonas críticas
curl "http://localhost:5000/api/v1/aggregations/grid?min_count=500"   # celdas de grilla
curl "http://localhost:5000/api/v1/aggregations/geohash?limit=50"     # por geohash
curl "http://localhost:5000/api/v1/aggregations/temporal?dimension=hour"
curl "http://localhost:5000/api/v1/aggregations/state?limit=10"
curl "http://localhost:5000/api/v1/aggregations/grid?bbox=-119,33,-117,35"
```

| Alias | Colección | Contenido |
|---|---|---|
| `grid` | `agg_grid` | conteo y severidad media por celda de 0,1° |
| `geohash` | `agg_geohash` | conteo por celda de geohash (precisión 5 ≈ 4,9 km) |
| `hotspots` | `agg_hotspots` | top-N celdas por índice de concentración |
| `temporal` | `agg_temporal` | conteos por hora, día de la semana, mes y año |
| `state` | `agg_state` | resumen por estado |

### Agregación por cercanía (`$geoNear`)

```bash
curl "http://localhost:5000/api/v1/geonear?lat=34.0522&lon=-118.2437\
&max_distance_m=20000&group_by=severity"

# anillos concéntricos de 2 km
curl "http://localhost:5000/api/v1/geonear?lat=34.0522&lon=-118.2437\
&max_distance_m=20000&group_by=distance_band&band_width_m=2000"
```

`$geoNear` aporta lo que `$near` no puede: expone la distancia calculada como
un campo, lo que permite promediarla y construir anillos.
`group_by` admite `severity`, `state`, `city`, `hour`, `dow`, `weather`,
`grid_id`, `geohash`, `distance_band` y `none`. La respuesta incluye el
pipeline usado, para poder verificarlo.

### Utilidad

| Método | Ruta | Descripción |
|---|---|---|
| GET | `/api/v1/health` | sonda de salud (la usan Docker y Jenkins) |
| GET | `/api/v1/stats` | conteos por colección e índice `2dsphere` |
| GET | `/api/v1/docs` | catálogo de endpoints |
| GET | `/api/v1/benchmark` | resultados de la comparación Dask/Spark |
| POST | `/api/v1/admin/reindex` | reconstruye los índices |
| GET | `/` | mapa interactivo Leaflet |

Todos los errores salen en JSON con `error` y `message`; nunca una página HTML
de Flask. Un parámetro inválido devuelve **400** antes de tocar MongoDB, y la
falta del índice `2dsphere` devuelve **503** con la instrucción para arreglarlo.

### Mapa Leaflet

<http://localhost:5000/> permite fijar el centro con un clic, dibujar un
rectángulo con arrastre, superponer las zonas de alta concentración calculadas
por Spark y ver el tiempo de respuesta de cada consulta.

```bash
make demo-queries    # ejecuta una consulta de cada tipo y muestra el resultado
```

---

## 6. Configuración de Jenkins

### Primer acceso

```bash
docker compose up -d jenkins
docker compose exec jenkins cat /var/jenkins_home/secrets/initialAdminPassword
```

Abra <http://localhost:8088>, pegue esa contraseña y cree su usuario. Los
plugins necesarios ya vienen instalados en la imagen
(`docker/jenkins/plugins.txt`), así que puede elegir «seleccionar plugins» y no
marcar ninguno.

> La contraseña inicial la genera Jenkins en el primer arranque. No está en el
> repositorio a propósito: el criterio de evaluación pide que no haya secretos
> expuestos.

### Credenciales

**Manage Jenkins → Credentials → System → Global credentials → Add Credentials**

| Tipo | ID | Contenido | ¿Obligatoria? |
|---|---|---|---|
| Secret file | `kaggle-json` | su `kaggle.json` | recomendada |
| Secret text | `mongo-root-password` | la contraseña de MongoDB | recomendada |

Si falta alguna, el pipeline **no falla**: avisa y sigue (Kaggle → datos
sintéticos; MongoDB → contraseña de desarrollo). Para la entrega, cree las dos.

> La contraseña de MongoDB debe ser **estable** entre builds: el volumen
> `mongo_data` conserva el usuario creado en el primer arranque, y cambiarla
> rompería la autenticación de todos los servicios.

### Crear el job

**New Item → Pipeline** (nombre: `geobigdata`)

- **Build Triggers** → marcar *GitHub hook trigger for GITScm polling*
- **Pipeline** → *Pipeline script from SCM*
  - SCM: Git
  - Repository URL: la de su repositorio
  - Branch: `*/main`
  - Script Path: `Jenkinsfile`

### Webhook de GitHub

Jenkins corre en `localhost`, así que GitHub no lo alcanza directamente.
Exponga el puerto con un túnel:

```bash
# opción A
ngrok http 8088
# opción B
cloudflared tunnel --url http://localhost:8088
```

En GitHub: **Settings → Webhooks → Add webhook**

- Payload URL: `https://<su-túnel>/github-webhook/` (la barra final es obligatoria)
- Content type: `application/json`
- Events: *Just the push event*

En Jenkins, **Manage Jenkins → System → GitHub → Add GitHub Server** y ponga la
misma URL pública en *Jenkins URL*.

El pipeline también tiene `pollSCM('H/15 * * * *')` como red de seguridad si el
webhook no llega.

### Qué hace el pipeline

| # | Etapa | Detalle |
|---|---|---|
| 1 | Checkout | clona y registra commit, rama y autor |
| 2 | Preparar entorno | genera `.env` desde las credenciales; **aborta si hay secretos versionados** |
| 3 | **Pruebas unitarias** | `pytest -m "not integration"` + cobertura → **puerta 1** |
| 4 | Construir imágenes | las seis imágenes, etiquetadas con el número de build |
| 5 | Levantar servicios | `docker compose up -d` |
| 6 | Esperar salud | sondea los healthchecks de Mongo, Dask, Spark y la API |
| 7 | Ingesta con Dask | Kaggle → limpieza → MongoDB |
| 8 | Procesamiento Spark | agregaciones espaciales y temporales |
| 9 | **Pruebas contra la API** | smoke test con curl + `pytest -m integration` → **puerta 2** |
| 10 | Benchmark | opcional (parámetro `RUN_BENCHMARK`) |
| 11 | **Desplegar** | promueve las imágenes a la etiqueta `stable` |

**«Si una prueba falla, el despliegue no debe realizarse».** Así se garantiza:

- Las pruebas unitarias corren **antes** de construir. Si fallan, no se toca
  ninguna imagen de servicio.
- Las imágenes recién construidas son **candidatas**. Solo se promueven a
  `stable` en la etapa 11, y solo si las etapas 9a y 9b pasaron.
- Si algo falla después de levantar la pila, el bloque `post { failure }` hace
  **rollback**: retiquetea las imágenes `stable` del último despliegue correcto
  y vuelve a levantar la pila con ellas.

Puede comprobarlo rompiendo una prueba a propósito: el build se detiene en la
etapa 3 o en la 9, la etapa 11 nunca se ejecuta y `geobigdata/api:stable` sigue
apuntando a la versión anterior.

El smoke test verifica los tres endpoints obligatorios, el índice `2dsphere`,
que haya datos cargados y que la API **rechace** parámetros inválidos con 400.

Parámetros del build: `RUN_INGESTION`, `FORCE_DOWNLOAD`, `DROP_EXISTING`,
`RUN_SPARK`, `RUN_BENCHMARK`, `SAMPLE_SIZE`, `SKIP_MIN_RECORDS_CHECK`.

---

## 7. Benchmark Dask vs Spark

```bash
make benchmark        # 1 y 2 workers, 2 repeticiones
make benchmark-full   # 1, 2 y 4 workers, 3 repeticiones
make benchmark-report # muestra el informe del último benchmark
```

**Operación medida:** agregación por celda de grilla de 0,1° (conteo y severidad
media). Es la más costosa del sistema: recorre todos los registros y hace un
shuffle sobre decenas de miles de claves.

Para que la comparación sea honesta:

- Ambos motores leen de **la misma fuente**, la colección de MongoDB.
- Calculan **exactamente la misma** agregación, y el resultado se compara al
  final (`results_match` en el informe).
- Cada configuración se ejecuta varias veces y se reporta la **mediana**, con
  mínimo, máximo y desviación típica.
- La memoria se mide **en los workers**, no en el proceso cliente:
  - Dask, con `client.run()` sobre cada worker (RSS de `psutil`);
  - Spark, con la API REST de executors del driver (*peak JVM heap*).

Salidas: `/data/benchmark/benchmark_latest.md` (tabla lista para el informe),
`benchmark_latest.json`, la colección `benchmark_runs` de MongoDB y el endpoint
`/api/v1/benchmark`.

El análisis con los números medidos está en
[`docs/informe-tecnico.md`](docs/informe-tecnico.md).

---

## 8. Pruebas

```bash
make test-unit          # 208 pruebas, sin dependencias externas
make test-integration   # contra el sistema en marcha
make test-all
make test-cov           # con informe de cobertura
make lint               # ruff
```

Sin Docker, con un entorno virtual local:

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -r requirements/dev.txt   # Windows
# .venv/bin/pip install -r requirements/dev.txt               # Linux/macOS
.venv/Scripts/python -m pytest -m "not integration" -v
```

| Archivo | Qué cubre |
|---|---|
| `tests/test_geo.py` | geohash (contra valores de referencia públicos y contra un algoritmo independiente), grilla, validación de coordenadas y polígonos, distancias |
| `tests/test_cleaning.py` | cada regla de limpieza con su caso, enriquecimiento y conversión a GeoJSON |
| `tests/test_queries.py` | que las consultas usen `$near`, `$geoWithin` y `$geoNear` con la forma correcta, con una colección falsa |
| `tests/test_api.py` | validación de parámetros, códigos de estado y contrato JSON |
| `tests/test_integration.py` | sistema en marcha: verifica con haversine que los puntos de `$near` están dentro del radio y vienen ordenados, y la contención en el polígono punto por punto |

Las de integración están marcadas con `@pytest.mark.integration` y se saltan
solas si no hay sistema levantado.

---

## 9. Estructura del repositorio

```
.
├── docker-compose.yml           orquestación (7 servicios + 4 trabajos)
├── docker-compose.dev.yml       sobrecarga opcional para desarrollo
├── Jenkinsfile                  pipeline de CI/CD
├── Makefile                     atajos (make help)
├── .env.example                 plantilla de configuración
│
├── docker/
│   ├── mongo/       Dockerfile + init/01-init-geo.js (esquema y 2dsphere)
│   ├── ingestion/   imagen de Dask (scheduler, workers, ingesta)
│   ├── spark/       Spark + MongoDB Spark Connector precargado
│   ├── api/         Flask + Gunicorn
│   ├── benchmark/   Spark + Dask en el mismo contenedor
│   ├── tests/       pytest + ruff
│   └── jenkins/     Jenkins + CLI de Docker + plugins
│
├── src/
│   ├── common/
│   │   ├── config.py         configuración por variables de entorno
│   │   ├── geo.py            geohash, grilla, GeoJSON, haversine (sin dependencias)
│   │   ├── mongo.py          conexión con reintentos e índices
│   │   └── logging_conf.py
│   ├── ingestion/
│   │   ├── download_kaggle.py descarga con la API de Kaggle
│   │   ├── synthetic.py       generador de datos de respaldo
│   │   ├── cleaning.py        reglas de limpieza (pandas puro, testeable)
│   │   └── clean_load.py      orquestación con Dask
│   ├── processing/
│   │   └── spark_aggregations.py  cinco agregaciones espaciales y temporales
│   ├── api/
│   │   ├── app.py            endpoints y validación
│   │   ├── queries.py        $near, $geoWithin, $geoNear
│   │   └── static/index.html mapa Leaflet
│   └── benchmark/
│       └── compare_dask_spark.py
│
├── tests/           208 pruebas unitarias + suite de integración
├── scripts/         bootstrap.sh y bootstrap.ps1
└── docs/            informe técnico
```

---

## 10. Decisiones de diseño

Las justificaciones completas están en
[`docs/informe-tecnico.md`](docs/informe-tecnico.md). Resumen de las cuatro que
más condicionan el sistema:

**1. El `docker-compose.yml` no tiene bind mounts de código.** Jenkins ejecuta
`docker compose` contra el demonio del **host** (patrón *Docker outside of
Docker*): una ruta relativa del workspace de Jenkins no existe en el host, y el
mount crearía un directorio vacío en silencio. Todo el código se copia dentro de
las imágenes y los datos viven en volúmenes nombrados. Para desarrollo con
recarga en caliente está `docker-compose.dev.yml`, que Jenkins no usa.

**2. Los jars del MongoDB Spark Connector se descargan en tiempo de build.** Con
`--packages`, cada `spark-submit` vuelve a resolver el árbol de dependencias con
Ivy: 30–60 s y necesita red. Con los jars dentro de la imagen, Spark arranca sin
red y de inmediato — importante en una sustentación en vivo.

**3. El índice único `accident_id` se crea antes de la carga; el `2dsphere`
después.** El índice único es lo que hace la ingesta **idempotente**:
reejecutarla no duplica registros, porque `insert_many(ordered=False)` descarta
los duplicados sin abortar el lote. El `2dsphere`, en cambio, es mucho más
rápido de construir sobre una colección ya poblada que de mantener actualizado
durante millones de inserciones.

**4. Las reglas de limpieza están en pandas puro, no en la API de Dask.** Así se
pueden probar con pytest sin levantar un cluster, y Dask las aplica en paralelo
sobre cada partición. Cada regla lleva su contador, de modo que el informe de
limpieza sale de mediciones reales y no de estimaciones.

### El orden `[lon, lat]`

GeoJSON y MongoDB usan `[longitud, latitud]`, al revés de como se escriben
normalmente las coordenadas. Invertirlo **no produce ningún error**: el índice
se construye igual y las consultas devuelven resultados, solo que equivocados.
Por eso hay tres defensas:

1. `to_geojson_point(lat, lon)` recibe los argumentos con nombre y es la única
   función que construye puntos.
2. El validador `$jsonSchema` de MongoDB verifica los rangos por posición:
   `coordinates[0]` en `[-180,180]`, `coordinates[1]` en `[-90,90]`. Una
   latitud en la posición 0 con valor > 90 se rechaza al insertar.
3. Las pruebas comprueban explícitamente el orden, y las de integración
   recalculan las distancias con haversine.

---

## 11. Solución de problemas

<details>
<summary><b>Los workers de Dask o Spark mueren con <code>OOMKilled</code></b></summary>

Falta memoria. Suba la RAM de Docker a 8 GB (ver [sección 1](#1-requisitos-previos))
o reduzca el consumo en `.env`:

```ini
DASK_WORKER_MEMORY=1g
SPARK_WORKER_MEMORY=1g
SPARK_EXECUTOR_MEMORY=1g
SAMPLE_SIZE=1000000
DASK_BLOCKSIZE=32MB
```
</details>

<details>
<summary><b><code>Authentication failed</code> al conectar a MongoDB</b></summary>

El volumen `mongo_data` conserva el usuario del primer arranque. Si cambió
`MONGO_ROOT_PASSWORD` después, ya no coincide. Recree el volumen:

```bash
docker compose down -v
docker compose up -d mongo
```

⚠️ Esto borra los datos: habrá que reejecutar la ingesta.
</details>

<details>
<summary><b>La ingesta muere sin mensaje, o con <code>exit 137</code></b></summary>

`137` es `128 + 9`: el kernel mató el proceso con SIGKILL, casi siempre por
memoria. Confirme cuál contenedor fue:

```bash
docker inspect <contenedor> --format '{{.State.OOMKilled}} {{.State.ExitCode}}'
```

Si el log se corta justo después de «Descargando…», el problema era la descarga.
Ya está resuelto: el proyecto descarga en *streaming* por trozos de 1 MiB en vez
de usar `KaggleApi.dataset_download_file`, que acumula la respuesta completa en
memoria (medido: 700 MiB de límite alcanzados y SIGKILL, frente a 166 MiB de pico
con streaming).

Si muere durante la carga, use el perfil de baja memoria o reduzca el tamaño de
partición:

```bash
./scripts/bootstrap.sh --lowmem
# o
docker compose run --rm -e DASK_BLOCKSIZE=16MB -e BATCH_SIZE=5000 ingestion
```
</details>

<details>
<summary><b><code>container geo-mongo is unhealthy</code> pero MongoDB funciona</b></summary>

Mire el motivo real del fallo de la sonda:

```bash
docker inspect geo-mongo --format '{{range .State.Health.Log}}{{.ExitCode}} {{.Output}}{{end}}'
```

Si dice `Health check exceeded timeout`, **no es MongoDB**: es la sonda. Usa
`mongosh`, que es un proceso de Node.js, y arrancarlo dentro del contenedor bajo
presión de CPU tarda varios segundos. Además consume ~150 MB **del propio límite
de memoria del contenedor**.

El proyecto ya usa `timeout: 20s`, `start_period: 40s` y `--norc`. Si aun así
falla en un equipo muy justo, suba el timeout o el límite de memoria de `mongo`.
Compruebe que se recuperó con:

```bash
docker inspect geo-mongo --format '{{.State.Health.Status}} {{.State.Health.FailingStreak}}'
```
</details>

<details>
<summary><b><code>403 Forbidden</code> al descargar de Kaggle</b></summary>

Hay que aceptar las condiciones del dataset en
<https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents> con la misma cuenta
del token. Verifique el token con:

```bash
docker compose run --rm ingestion python -m src.ingestion.download_kaggle
```
</details>

<details>
<summary><b><code>unable to find index for $geoNear query</code></b></summary>

Falta el índice `2dsphere`:

```bash
curl -X POST http://localhost:5000/api/v1/admin/reindex
curl http://localhost:5000/api/v1/stats     # geo_index_2dsphere debe ser true
```
</details>

<details>
<summary><b>Las colecciones <code>agg_*</code> están vacías</b></summary>

No se ha ejecutado Spark todavía:

```bash
docker compose run --rm spark-job
```
</details>

<details>
<summary><b>Spark no encuentra el conector de MongoDB</b></summary>

Reconstruya la imagen; los jars se descargan durante el build:

```bash
docker compose build --no-cache spark-master
docker compose exec spark-master ls /opt/spark/jars | grep -i mongo
```

Deben aparecer cinco jars: el conector, `mongodb-driver-sync`,
`mongodb-driver-core`, `bson` y `bson-record-codec`.
</details>

<details>
<summary><b>Jenkins: <code>Cannot connect to the Docker daemon</code></b></summary>

El contenedor necesita el socket del host. Compruebe que en
`docker-compose.yml` está el mount `/var/run/docker.sock:/var/run/docker.sock`
y que el servicio corre con `user: root`. Verifique desde dentro:

```bash
docker compose exec jenkins docker ps
```
</details>

<details>
<summary><b>El webhook de GitHub no dispara el build</b></summary>

1. El túnel debe estar activo y la *Payload URL* terminar en `/github-webhook/`
   (con la barra final).
2. En GitHub, **Settings → Webhooks → Recent Deliveries** muestra la respuesta;
   debe ser 200.
3. En Jenkins, la *Jenkins URL* (Manage Jenkins → System) debe ser la pública.
4. Mientras lo arregla, `pollSCM` recoge los cambios cada 15 minutos.
</details>

<details>
<summary><b>Un puerto está ocupado</b></summary>

Cámbielo en `.env`:

```ini
API_PORT=5001
JENKINS_PORT=8090
MONGO_PORT=27018
SPARK_MASTER_UI_PORT=8085
DASK_DASHBOARD_PORT=8788
```
</details>

<details>
<summary><b>Diagnóstico general</b></summary>

```bash
docker compose ps                        # estado y healthchecks
docker compose logs --tail 100 api       # logs de un servicio
make stats                               # conteos e índices
make indexes                             # índices de la colección principal
docker stats --no-stream                 # consumo de CPU y memoria
docker compose exec api curl -fsS localhost:5000/api/v1/health
```
</details>

---

## Créditos

- **Dataset:** [US Accidents (2016–2023)](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents),
  Sobhan Moosavi. Citar los artículos indicados en la página del dataset.
- **Mapa base:** © OpenStreetMap contributors, © CARTO.
- **Código de terceros:** solo las dependencias declaradas en `requirements/` y
  las imágenes base oficiales (`mongo`, `python`, `spark`, `jenkins`). El resto
  es propio.
