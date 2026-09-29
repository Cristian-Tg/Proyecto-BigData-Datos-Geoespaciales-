# Informe técnico
## Procesamiento y consulta de datos geoespaciales con despliegue continuo

**Dataset:** US Accidents (2016–2023) — `sobhanmoosavi/us-accidents`
**Volumen:** 7,7 M de registros · ~3 GB de CSV · 2 M cargados en MongoDB
**Repositorio:** `<URL-DEL-REPOSITORIO>`

> **Antes de entregar:** las tablas marcadas con ⚠️ se rellenan con las
> mediciones de su propia ejecución. **No hay que transcribirlas a mano:**
>
> ```bash
> python scripts/report_numbers.py --out docs/mediciones.md
> ```
>
> genera `docs/mediciones.md` con las cuatro tablas ya formateadas, leyéndolas de
> los artefactos que produce el propio pipeline (`cleaning_stats.json`,
> `spark_summary.json`, `benchmark_latest.md`) y midiendo las consultas contra la
> API en vivo. Después basta con copiar cada tabla a su sitio en este documento.
>
> No hay ningún número inventado ni estimado en este informe.

---

## 1. Arquitectura

```
                          ┌──────────────────────────────┐
                          │        GitHub (main)         │
                          │   ramas + pull requests      │
                          └──────────────┬───────────────┘
                                         │ webhook (push)
                                         ▼
   ┌──────────────────────────────────────────────────────────────────┐
   │                       JENKINS  (:8088)                           │
   │  1 checkout → 2 credenciales → 3 PYTEST ──┐                      │
   │  4 build → 5 up → 6 healthchecks          │ puerta 1             │
   │  7 ingesta → 8 Spark → 9 PRUEBAS API ─────┤ puerta 2             │
   │  10 benchmark → 11 DESPLIEGUE ◀───────────┘                      │
   │  cualquier fallo tras la etapa 5 ⇒ rollback a la imagen `stable` │
   └──────────────────────────────────────────────────────────────────┘
                                         │  docker compose (socket del host)
   ══════════════════════════════════════╪══════════════════════════════════
                        red de Docker    ▼    `geobigdata_geonet`
   ┌────────────┐
   │ Kaggle API │
   └─────┬──────┘
         │ dataset_download_file()        token: credencial de Jenkins
         ▼                                       (nunca en el repositorio)
   ┌───────────────┐   volumen `data_vol`
   │  CSV ~3 GB    │
   └───────┬───────┘
           │ dd.read_csv(blocksize=64MB)  →  ~48 particiones
           ▼
   ┌────────────────────────────────────────────┐
   │                   DASK                     │
   │  scheduler :8786  ·  panel :8787           │
   │  worker 1 (2 hilos)    worker 2 (2 hilos)  │
   │                                            │
   │  por partición, en el worker:              │
   │    clean_partition()   7 reglas de descarte│
   │    enrich_partition()  grilla, geohash,    │
   │                        hora/día/mes        │
   │    to_documents()      GeoJSON Point       │
   │    insert_many(ordered=False)  lotes 20 k  │
   └────────────────────┬───────────────────────┘
                        ▼
   ┌───────────────────────────────────────────────────────────┐
   │                    MONGODB  :27017                        │
   │                                                           │
   │  accidents        GeoJSON Point + índice 2dsphere         │
   │                   + validador $jsonSchema                 │
   │                   + 8 índices de apoyo                    │
   │                   + único en accident_id (idempotencia)   │
   │                                                           │
   │  agg_grid  agg_geohash  agg_hotspots                      │◀──┐
   │  agg_temporal  agg_state  benchmark_runs                  │   │
   └────────┬──────────────────────────────────────────────────┘   │
            │                                                      │
            │  $near / $geoWithin / $geoNear      MongoDB Spark    │
            │                                     Connector 10.4.0 │
            ▼                                                      │
   ┌──────────────────┐                        ┌──────────────────┐│
   │   API FLASK      │                        │      SPARK       ││
   │   Gunicorn :5000 │                        │  master :7077    ├┘
   │                  │                        │  UI     :8080    │
   │  /near           │                        │  worker 1 (2 nú) │
   │  /within         │                        │  worker 2 (2 nú) │
   │  /geonear        │                        │                  │
   │  /aggregations/* │                        │  5 agregaciones  │
   │  /stats /health  │                        │  espaciales y    │
   │  / (Leaflet)     │                        │  temporales      │
   └──────────────────┘                        └──────────────────┘
```

**Siete servicios** en `docker compose up`: `mongo`, `dask-scheduler`,
`dask-worker` (×2), `spark-master`, `spark-worker` (×2), `api`, `jenkins`.
**Cuatro trabajos puntuales** bajo el perfil `jobs`: `ingestion`, `spark-job`,
`benchmark`, `tests`.

### 1.1 Flujo de datos

| Paso | Componente | Entrada | Salida |
|---|---|---|---|
| 1 | `download_kaggle.py` | API de Kaggle | CSV en `data_vol` |
| 2 | `clean_load.py` + Dask | CSV particionado | `accidents` (GeoJSON) |
| 3 | `spark_aggregations.py` | `accidents` | 5 colecciones `agg_*` |
| 4 | `app.py` + `queries.py` | colecciones | JSON por HTTP |
| 5 | `compare_dask_spark.py` | `accidents` | `benchmark_runs` |

---

## 2. Decisiones tomadas

### 2.1 Modelado de datos

**Un documento por accidente, con la geometría y los campos derivados
precalculados.** Se rechazó normalizar en varias colecciones: MongoDB no hace
joins baratos y cada consulta espacial tendría que hacer `$lookup`, que no puede
aprovechar el índice `2dsphere`.

```javascript
{
  accident_id: "A-3",                                   // clave natural, único
  location: { type: "Point", coordinates: [-87.6298, 41.8781] },  // [lon, lat]
  lat: 41.8781, lon: -87.6298,        // duplicados planos: la respuesta JSON
                                       // los necesita y evitan desanidar
  severity: 3, start_time: ISODate("2022-07-11T17:30:00Z"),
  city: "Chicago", county: "Cook", state: "IL",
  distance_mi: 2.4, temperature_f: 78.0, visibility_mi: 9.5,
  weather: "Cloudy", day_night: "Day",
  grid_id: "41.8000_-87.7000", grid_lat: 41.8, grid_lon: -87.7,  // grilla 0,1°
  geohash: "dp3wj",                                              // precisión 5
  year: 2022, month: 7, day: 11, hour: 17, dow: 0, is_weekend: false
}
```

**Por qué se precalculan `grid_id`, `geohash` y las columnas temporales en la
ingesta:** son deterministas y se calculan una sola vez sobre cada registro
mientras ya está en memoria del worker. Calcularlos en cada consulta obligaría a
un `$addFields` sobre millones de documentos; calcularlos en Spark con una UDF de
Python costaría la serialización JVM↔Python de todas las filas. Se pagan una vez
y se indexan.

**Por qué `lat`/`lon` aparecen además de `location`:** `location` es lo que
indexa `2dsphere`, pero devolverlo desanidado en cada respuesta JSON obligaría al
cliente a leer `location.coordinates[1]` para la latitud. Los campos planos
cuestan 16 bytes por documento y simplifican la API y el mapa.

#### El orden `[lon, lat]`

GeoJSON y MongoDB usan `[longitud, latitud]`, al revés de la convención hablada.
Invertirlo **no genera ningún error**: el índice se construye, las consultas
devuelven resultados, y todos son incorrectos. Tres defensas:

1. `to_geojson_point(lat, lon)` es la **única** función que construye puntos, y
   recibe los argumentos con nombre.
2. El validador `$jsonSchema` valida por posición: `coordinates[0]` en
   `[-180,180]` y `coordinates[1]` en `[-90,90]`. Una latitud mal puesta en la
   posición 0 con valor > 90 se rechaza al insertar.
3. `tests/test_geo.py::test_el_orden_es_lon_lat_no_lat_lon` lo comprueba, y las
   pruebas de integración recalculan las distancias con haversine.

#### Índices

| Índice | Campos | Para qué |
|---|---|---|
| `ix_location_2dsphere` | `location` **2dsphere** | `$near`, `$geoWithin`, `$geoNear` |
| `ux_accident_id` | `accident_id` (único, sparse) | idempotencia de la ingesta |
| `ix_start_time` | `start_time` ↓ | filtros por fecha |
| `ix_severity` | `severity` | filtro por gravedad |
| `ix_state_severity` | `state`, `severity` | filtro combinado (el más usado) |
| `ix_year_month` | `year`, `month` | particionado del benchmark |
| `ix_grid_id`, `ix_geohash` | — | agrupaciones sin recalcular |

Las cinco colecciones `agg_*` con geometría llevan su propio `2dsphere` sobre
`centroid`, lo que permite pedir «la grilla de esta zona del mapa»
(`/aggregations/grid?bbox=...`).

### 2.2 Limpieza con Dask — y su justificación

Las reglas viven en `src/ingestion/cleaning.py` como **pandas puro**, no en la
API de Dask. Razón: así se prueban con pytest sin levantar un cluster, y Dask las
aplica en paralelo sobre cada partición. El papel de Dask es la lectura
particionada del CSV, la distribución del trabajo y la carga concurrente por
lotes.

Las reglas se aplican **de la más barata a la más cara**, para que cada filtro
reduzca el volumen que procesa la siguiente.

| # | Regla | Justificación |
|---|---|---|
| 1 | Descartar coordenadas nulas | Un accidente sin coordenadas es inservible para todo el sistema: no se indexa en `2dsphere` ni puede aparecer en ninguna consulta espacial. No se imputa porque no hay forma defendible de inventar una ubicación. |
| 2 | Descartar fuera de rango WGS84 | MongoDB **rechaza el documento completo** al construir el `2dsphere` si \|lat\|>90 o \|lon\|>180. Filtrarlos antes evita que un registro corrupto aborte un lote. |
| 3 | Descartar el relleno `(0,0)` | (0,0) cae en el Golfo de Guinea. En un dataset de accidentes de EE. UU. es siempre un valor por defecto de dato faltante, nunca una observación. Es el caso que el rango WGS84 no atrapa. |
| 4 | Descartar fuera de la caja de EE. UU. | El dataset es exclusivamente estadounidense; un punto en París es un error de captura. La caja (lat 17,5–72 · lon −180 a −64,5) incluye Alaska, Hawái y Puerto Rico, verificado en `test_alaska_y_hawai_se_conservan`. |
| 5 | Descartar fecha no parseable | Sin marca temporal válida no es posible el análisis por hora, día y mes que exige el enunciado. Se usa `format="mixed"` porque el CSV combina segundos enteros y fraccionarios. |
| 6 | Descartar severidad fuera de 1–4 | Es la variable de ponderación de las zonas de alta concentración; un valor fuera del dominio contaminaría todos los promedios. |
| 7 | Deduplicar por `accident_id` | Dentro de la partición con `drop_duplicates`; la unicidad **global** la garantiza el índice único junto con `insert_many(ordered=False)`. |

**Resultados medidos** — ⚠️ rellenar con `reports/cleaning_stats.json`, que la
ingesta escribe al terminar (`docker compose run --rm ingestion`):

| Concepto | Registros | % |
|---|---:|---:|
| Leídos del CSV | | 100 % |
| − Coordenadas nulas | | |
| − Fuera de rango WGS84 | | |
| − Relleno (0,0) | | |
| − Fuera de la caja de EE. UU. | | |
| − Fecha no parseable | | |
| − Severidad inválida | | |
| − Duplicados por ID | | |
| **Cargados en MongoDB** | | |
| Duplicados rechazados por el índice único | | |
| Tiempo de ingesta / registros por segundo | | |

**Dos decisiones de carga que importan:**

*`insert_many(ordered=False)`.* Con `ordered=True`, el primer duplicado aborta el
resto del lote de 20 000 documentos. Con `ordered=False`, MongoDB inserta todo lo
insertable, paraleliza internamente y devuelve los errores al final; el código
distingue el código 11000 (duplicado, esperado y contado) de cualquier otro
(fallo real, que propaga).

*Índice único antes de la carga, `2dsphere` después.* El índice único sobre
`accident_id` se crea **antes**: es lo que hace la ingesta idempotente, porque
reejecutarla no duplica nada. El `2dsphere` se crea **después**: construirlo
sobre una colección ya poblada es mucho más rápido que mantenerlo actualizado
durante dos millones de inserciones.

*Tamaño de partición de 64 MB.* Con 256 MB cada worker necesitaba más de 2 GB
para el DataFrame de pandas intermedio. Con 16 MB, la sobrecarga del scheduler
por tarea dominaba el tiempo total. 64 MB da ~48 particiones para el CSV de 3 GB:
suficientes para repartir entre los workers y bastante pequeñas para no agotar la
memoria.

*Envío en oleadas.* Las particiones no se envían todas a la vez, sino en oleadas
de `3 × n_workers`. Así el scheduler no acumula miles de tareas pendientes y se
puede cortar en seco al alcanzar `SAMPLE_SIZE`.

### 2.3 Procesamiento con Spark

`spark_aggregations.py` produce cinco colecciones nuevas:

| Colección | Agregación | Notas |
|---|---|---|
| `agg_grid` | conteo, severidad media y máxima, distancia media y ciudades distintas por celda de 0,1° | `floor(coord/celda)*celda`; centroide GeoJSON indexable |
| `agg_geohash` | conteo y severidad media por celda de geohash (precisión 5 ≈ 4,9 km) | centroide = media de los puntos reales, que representa la concentración mejor que el centro geométrico |
| `agg_hotspots` | top-200 celdas por **índice de concentración** | `count × avg_severity`: una celda con muchos accidentes leves no es igual de crítica que una con la mitad pero graves, y el conteo puro esconde esa diferencia |
| `agg_temporal` | conteos por hora, día de la semana, mes y año | una sola colección con forma `(dimension, bucket, label, count, …)`: mucho más fácil de servir desde un único endpoint que cuatro colecciones |
| `agg_state` | conteo, severidad media y % de graves por estado | |

**Decisiones:**

*Esquema explícito al leer.* Dejar que el conector lo infiera por muestreo es
lento sobre millones de documentos y produce tipos inestables cuando hay nulos.
`ACCIDENT_SCHEMA` fija los 19 campos que se usan.

*`spark.sql.shuffle.partitions = 16`.* El valor por defecto (200) genera miles de
tareas diminutas en un cluster de dos workers, y la planificación domina el
tiempo total.

*`cache()` sobre el DataFrame de entrada.* Las cinco agregaciones recorren el
mismo DataFrame; sin cache, MongoDB se leería cinco veces.

*El geohash se reutiliza si ya está calculado.* Es un prefijo jerárquico, así que
recortar el de precisión 5 a la longitud pedida es válido. Solo se recurre a una
UDF de Python si la columna no existe, porque la serialización JVM↔Python sobre
millones de filas es el cuello de botella.

*Los índices de las colecciones nuevas los crea pymongo, no Spark.* El conector
escribe documentos pero no gestiona índices; `ensure_indexes()` se llama al
final desde el driver.

*El día de la semana se normaliza.* `dayofweek` de Spark devuelve 1 = domingo;
se convierte con `pmod(dayofweek + 5, 7)` a 0 = lunes, igual que `pandas.dt.dayofweek`,
para que Dask y Spark den el mismo resultado.

**Resultados** — ⚠️ rellenar con `reports/spark_summary.json`
(`docker compose run --rm spark-job`):

| Colección | Documentos | Tiempo (s) |
|---|---:|---:|
| `agg_grid` | | |
| `agg_hotspots` | | |
| `agg_geohash` | | |
| `agg_temporal` | | |
| `agg_state` | | |
| **Total** | | |

### 2.4 Infraestructura

**Sin bind mounts de código en `docker-compose.yml`.** Jenkins ejecuta
`docker compose` contra el demonio del **host** (patrón *Docker outside of
Docker*: se le monta `/var/run/docker.sock` en lugar de instalar un Docker
anidado). La consecuencia es que cualquier ruta relativa del compose se resuelve
en el **host**, no en el workspace de Jenkins: un `./src:/app/src` crearía un
directorio vacío en silencio y los contenedores arrancarían sin código. Por eso
todo el código se copia dentro de las imágenes y los datos viven en volúmenes
nombrados, que sí son del demonio. El único bind mount del proyecto es el propio
socket, cuya ruta existe en el host. Para desarrollo con recarga en caliente está
`docker-compose.dev.yml`, que Jenkins nunca usa.

**Jars del conector en tiempo de build.** Con `--packages`, cada `spark-submit`
resuelve el árbol de dependencias con Ivy: 30–60 s y necesita red. Con los cinco
jars (`mongo-spark-connector`, `mongodb-driver-sync`, `mongodb-driver-core`,
`bson`, `bson-record-codec`) dentro de la imagen, Spark arranca sin red y de
inmediato. Falta cualquiera de los cinco y el conector falla en ejecución con
`NoClassDefFoundError`.

**`SPARK_DRIVER_HOST` explícito.** Los executors abren conexiones **de vuelta**
al driver. Sin esta variable el driver se anuncia como `127.0.0.1` y los
executors no lo alcanzan; el trabajo se queda colgado sin error claro.

**`wiredTigerCacheSizeGB=1`.** Por defecto MongoDB reclama la mitad de la RAM del
host, lo que en un portátil deja sin memoria a Spark y a Dask a la vez.

**`tini` como PID 1 en la imagen de Dask.** Sin un init, los workers dejan
procesos zombis y `docker compose down` espera el timeout completo en cada
parada.

**Contraseñas.** `.env` está en `.gitignore` y `MONGO_ROOT_PASSWORD` se declara
con `:?` en el compose, de modo que `docker compose up` **falla** con un mensaje
claro si no está definida, en lugar de arrancar con un valor por defecto
inseguro. Los scripts de bootstrap generan una aleatoria en el primer arranque.

---

## 3. Consultas geoespaciales implementadas

Las tres son parametrizadas: **no hay ni una coordenada fija en el código**.
Todas comparten `build_attribute_filter()`, que añade filtros por severidad,
estado, ciudad, clima, rango de fechas, año y franja horaria (incluida una que
cruce la medianoche).

### 3.1 `$near` — por radio

```python
{"location": {"$near": {
    "$geometry": {"type": "Point", "coordinates": [lon, lat]},
    "$maxDistance": radius_m,
    "$minDistance": min_distance_m      # solo si > 0
}}}
```

```bash
GET /api/v1/near?lat=34.0522&lon=-118.2437&radius_m=5000&limit=10&min_severity=3
```

**Dos detalles que son fuente habitual de errores:**

*`$geometry` y no el formato legado.* Con `$geometry` GeoJSON, `$maxDistance`
está en **metros**. Con la forma antigua `[lon, lat]` estaría en **radianes**: un
error de seis órdenes de magnitud que no produce ninguna excepción, solo
resultados absurdos.

*El conteo no puede usar `$near`.* MongoDB **prohíbe** `$near` dentro de
`count`/`aggregate`, así que `countDocuments` falla. Para el total se usa
`$geoWithin` con `$centerSphere`, que delimita exactamente el mismo círculo y sí
es contable — con el radio convertido a radianes por `meters_to_radians()`.

`$near` devuelve los resultados **ordenados de más cercano a más lejano sin
`$sort`**: el índice recorre las celdas en orden de proximidad. Verificado en
`test_los_resultados_vienen_ordenados_por_distancia`, que recalcula las
distancias con haversine.

### 3.2 `$geoWithin` — dentro de un polígono

```python
{"location": {"$geoWithin": {"$geometry": geometry}}}
```

```bash
POST /api/v1/within
{"geometry": {"type":"Polygon","coordinates":[[[-118.55,33.90], …]]},
 "limit": 100, "min_severity": 3, "summary": true}
```

Acepta `Polygon`, `MultiPolygon`, un `Feature` completo de GeoJSON y el atajo
`{"bbox": [min_lon, min_lat, max_lon, max_lat]}`.

`validate_polygon()` valida **antes** de consultar: tipo, número de anillos,
mínimo de 4 posiciones, rangos de cada coordenada y que el anillo esté cerrado.
Se hace porque MongoDB, ante un anillo sin cerrar, devuelve un error poco
informativo; la API devuelve 400 diciendo exactamente qué está mal.

Al contrario que `$near`, **`$geoWithin` sí es válido dentro de `$match`**, lo que
permite el resumen del área con un pipeline de agregación: conteo, severidad
media y máxima, número de graves, ciudades distintas y ventana temporal.

#### Un detalle de geodesia que conviene conocer

MongoDB interpreta los lados de un `Polygon` como **geodésicas** (arcos de
círculo máximo), **no** como líneas de latitud constante. El lado norte de un
«rectángulo» lat/lon se comba hacia el polo en su parte central, así que un punto
ligeramente al norte de `max_lat` **sí está dentro** del polígono esférico.

Se detectó al escribir la prueba de integración, que comprobaba la contención
con el rectángulo lat/lon y fallaba. Medido con un bbox de 0,6° de ancho a 34° de
latitud, sobre 1000 puntos devueltos:

| | Resultado |
|---|---|
| Puntos fuera del rectángulo lat/lon | 11 de 1000 |
| Exceso en **latitud** | entre 1,3 m y 17,1 m |
| Exceso en **longitud** | **0 m en todos** |

El que el exceso aparezca solo en latitud, solo en las longitudes centrales y
nunca en longitud es la firma exacta del abombamiento: los lados este y oeste son
meridianos, que son círculos máximos y no se comban.

La conclusión es que **la prueba estaba mal, no la consulta**. Se corrigió con una
tolerancia documentada de 100 m en latitud, y se añadió una segunda prueba que
exige contención **exacta** en longitud, de modo que un error de contención real
seguiría detectándose en lugar de quedar tapado por la tolerancia.

#### El conteo es metadato opcional, y se trata como tal

`count_documents` con `$geoWithin` sobre ~970 000 documentos puede tardar más que
la consulta principal. Con la caché de WiredTiger recortada a 0,25 GB superaba el
límite de tiempo y el endpoint devolvía **500 teniendo los resultados válidos ya
calculados en la mano**.

Ahora el conteo degrada: `total_matching: null` más una marca
`total_matching_timed_out: true`, para que el cliente distinga «no hay
resultados» de «no se pudo contar». La caché se subió a 0,4 GB, que era la causa
real del timeout.

### 3.3 `$geoNear` — agregación por cercanía

```python
[{"$geoNear": {
    "near": {"type": "Point", "coordinates": [lon, lat]},
    "distanceField": "distance_m",
    "maxDistance": max_distance_m,
    "spherical": True,          # obligatorio con índice 2dsphere
    "key": "location",
    "query": attribute_filter   # DENTRO de $geoNear, no en un $match posterior
 }},
 …$addFields (bandas) …$group …$project …$sort …$limit]
```

```bash
GET /api/v1/geonear?lat=34.0522&lon=-118.2437&max_distance_m=20000&group_by=severity
GET /api/v1/geonear?…&group_by=distance_band&band_width_m=2000
```

**Qué aporta sobre `$near`:** expone la distancia calculada como un **campo**
(`distance_m`). Eso permite promediar distancias, construir anillos concéntricos
y combinar proximidad con agrupaciones — nada de lo cual se puede hacer con
`$near`.

**Dos restricciones:** `$geoNear` tiene que ser la **primera etapa** del
pipeline (verificado en `test_geonear_es_la_primera_etapa`), y el filtro va
**dentro** de `$geoNear` y no en un `$match` posterior, para que MongoDB lo
aplique mientras recorre el índice en vez de después.

`group_by` admite `severity`, `state`, `city`, `hour`, `dow`, `weather`,
`grid_id`, `geohash`, `distance_band` y `none`. La respuesta incluye el pipeline
generado, para poder auditarlo.

### 3.4 Rendimiento

⚠️ rellenar con el campo `elapsed_ms` que devuelve cada consulta:

| Consulta | Parámetros | `elapsed_ms` | Resultados |
|---|---|---:|---:|
| `$near` | radio 5 km | | |
| `$near` | radio 50 km | | |
| `$near` | radio 5 km + `min_severity=3` | | |
| `$geoWithin` | área de Los Ángeles | | |
| `$geoWithin` | + `summary=true` | | |
| `$geoNear` | 20 km, por severidad | | |
| `$geoNear` | 20 km, bandas de 2 km | | |
| `/aggregations/hotspots` | top 20 | | |

```bash
curl -s "http://localhost:5000/api/v1/near?lat=34.0522&lon=-118.2437&radius_m=5000&limit=10" \
  | python -m json.tool | grep -E 'elapsed_ms|returned|total_matching'
```

Para comprobar que el índice se está usando de verdad:

```javascript
db.accidents.find({location: {$near: {
  $geometry: {type: "Point", coordinates: [-118.2437, 34.0522]},
  $maxDistance: 5000}}}).explain("executionStats")
// winningPlan.inputStage.stage debe ser "GEO_NEAR_2DSPHERE",
// no "COLLSCAN"
```

---

## 4. Análisis comparativo Dask vs Spark

### 4.1 Método

**Operación medida:** agregación por celda de grilla de 0,1° (conteo y suma de
severidad). Es la operación más costosa del sistema: recorre todos los registros
y hace un shuffle sobre decenas de miles de claves distintas.

**Condiciones para que la comparación sea justa:**

- Ambos motores leen de **la misma fuente**: la colección `accidents` de MongoDB.
  No se compara «Dask leyendo CSV» contra «Spark leyendo Mongo».
- Calculan **exactamente la misma** agregación, y el resultado se compara celda
  por celda al final (`verification.results_match`).
- Cada configuración se ejecuta `--repeats` veces; se reporta la **mediana**, con
  mínimo, máximo y desviación típica.
- Se agregan **sumas parciales** y no medias: las medias de medias no se pueden
  combinar entre particiones, las sumas sí.

**Cómo se varían los workers:**

- *Dask:* un `LocalCluster` con `n_workers` ∈ {1, 2}. Se usa un cluster local, y
  no el de compose, porque el número de workers está fijado en el YAML y el
  cliente no puede escalarlo; el `LocalCluster` permite fijar exactamente la
  variable que se quiere comparar.
- *Spark:* `spark.cores.max` ∈ {1, 2} con `spark.executor.cores=1` contra el
  cluster standalone real. Con un núcleo por executor, `cores.max = N` equivale a
  N executors activos.

**Cómo se mide la memoria** — en los workers, no en el proceso cliente:

- *Dask:* `client.run()` sobre cada worker devuelve su RSS de `psutil`; un hilo
  muestrea cada 0,5 s y se reporta el pico de la suma.
- *Spark:* la API REST de executors del driver
  (`/api/v1/applications/{id}/executors`) da `peakMemoryMetrics.JVMHeapMemory`
  por executor, excluyendo el driver; se muestrea cada segundo.

**Particionado de la lectura de MongoDB para Dask.** Dask no tiene conector de
MongoDB, así que se particiona por `(year, month)`, aprovechando el índice
compuesto `ix_year_month`. Se eligió así, y no por rangos de `_id` con `skip`,
porque un `skip` de millones de documentos es O(n) en MongoDB y arruinaría la
medición de Dask por un motivo ajeno al motor.

**Reproducir:**

```bash
make benchmark        # 1 y 2 workers, 2 repeticiones
make benchmark-full   # 1, 2 y 4 workers, 3 repeticiones
make benchmark-report
```

### 4.2 Resultados

⚠️ **Copiar aquí la tabla de `/data/benchmark/benchmark_latest.md`**, que el
benchmark genera automáticamente con este mismo formato.

| Motor | Workers | Mediana (s) | mín | máx | σ | Memoria pico workers (MB) | Celdas | Registros |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Dask | 1 | | | | | | | |
| Dask | 2 | | | | | | | |
| Spark | 1 | | | | | | | |
| Spark | 2 | | | | | | | |

**Escalabilidad**

| Motor | Workers | Speedup real | Speedup ideal | Eficiencia paralela |
|---|---|---:|---:|---:|
| Dask | 1 → 2 | | 2,00× | |
| Spark | 1 → 2 | | 2,00× | |

**Comparación directa**

| Workers | Dask (s) | Spark (s) | Más rápido | Ventaja |
|---:|---:|---:|---|---:|
| 1 | | | | |
| 2 | | | | |

**Verificación de equivalencia:** `results_match` = ⚠️ · celdas por
configuración: ⚠️ · registros procesados: ⚠️

> Las dos últimas cifras son la prueba de que se comparó la misma operación: si
> los dos motores no producen el mismo número de celdas y de registros, la
> comparación de tiempos no significa nada.

### 4.3 Interpretación

⚠️ **Escribir con los números propios.** Guía de qué mirar y qué conclusión
respalda cada observación:

**Si Dask gana con 1 worker.** Es lo esperado. Spark paga un coste fijo de
arranque —JVM, registro de executors, planificación del shuffle— del orden de
decenas de segundos, que sobre un volumen de 2 M de registros pesa mucho en
proporción. Dask arranca procesos de Python en un par de segundos. Conclusión
defendible: *por debajo de cierto umbral de datos, el coste de arranque de Spark
no se amortiza.*

**Si Spark escala mejor de 1 a 2 workers.** También es lo esperado: su motor de
shuffle está diseñado para eso, y el planificador Catalyst reordena la
agregación. Si la eficiencia paralela de Dask es menor, revise si el cuello de
botella es la lectura de MongoDB —compartida por los dos motores y no
paralelizable más allá de las particiones `(year, month)`— y no el cómputo.

**Sobre la memoria.** Compare el pico por worker. Spark reserva su heap por
adelantado según `SPARK_EXECUTOR_MEMORY`, así que su pico tiende a la
configuración más que a la necesidad real. Dask crece según lo que de verdad
carga en los DataFrames de pandas. Es una diferencia de modelo de gestión, no de
eficiencia: dígalo así en la sustentación.

**Umbrales y avisos.** Si `σ` es alta respecto a la mediana, hubo interferencia
(otros contenedores, cache del sistema de archivos) y conviene subir
`--repeats`. Si la eficiencia paralela supera el 100 %, es un artefacto de
caché, no escalabilidad superlineal.

### 4.4 En qué casos conviene cada uno

Conclusión basada en lo observado en **este** sistema:

**Dask conviene cuando:**
- El equipo ya trabaja en Python con pandas y NumPy: la API es la misma y la
  curva de aprendizaje es casi nula.
- El volumen cabe en el cluster disponible y el coste de arranque de la JVM pesa
  en proporción — como en la limpieza de la ingesta, donde el trabajo por
  partición es local y no hay shuffle.
- Se necesita una integración fina con librerías de Python que no tienen
  equivalente en la JVM.
- Depurar importa: las trazas son de Python, no de Scala a través de Py4J.

**Spark conviene cuando:**
- La operación implica **shuffles grandes** —`groupBy` sobre decenas de miles de
  claves, joins— que es exactamente su punto fuerte y donde está el mejor
  escalado.
- El volumen crece por encima de la memoria agregada del cluster: el vuelco a
  disco de Spark es maduro y predecible.
- Se quiere el conector nativo de MongoDB con *pushdown* de predicados.
- Hay que crecer a decenas de nodos, donde su tolerancia a fallos y su gestor de
  recursos están más probados.

**Por eso este sistema usa los dos, y no uno:** Dask para la ingesta (trabajo
por partición, sin shuffle, con reglas escritas en pandas y probadas con pytest)
y Spark para las agregaciones (shuffle masivo sobre las claves de grilla y de
geohash, leyendo desde MongoDB con el conector oficial). No es redundancia: cada
motor está en la etapa donde su modelo de ejecución encaja.

---

## 5. Verificación y calidad

**208 pruebas unitarias** (`make test-unit`) más una suite de integración.

| Archivo | Cubre |
|---|---|
| `test_geo.py` | geohash contra valores de referencia públicos (`ezs42`, `u4pruydqqvj`, `6gkzwg`, `dqcjqcpe`, `gcpvj0d`) **y contra un algoritmo independiente** sobre 500 puntos aleatorios; grilla con coordenadas negativas; validación de polígonos; haversine |
| `test_cleaning.py` | cada una de las 7 reglas con su caso sucio; que los descartes sumen el total; que los tipos de NumPy se conviertan a nativos (pymongo no sabe codificar `np.int16`) |
| `test_queries.py` | que las consultas usen `$near`/`$geoWithin`/`$geoNear` con la forma exacta; que el conteo use `$centerSphere` en radianes; que `$geoNear` sea la primera etapa; que la regex de ciudad esté escapada (riesgo de ReDoS) |
| `test_api.py` | validación de parámetros, códigos 400/503, que los errores sean JSON y no HTML de Flask |
| `test_integration.py` | sistema en marcha: **recalcula con haversine** que los puntos de `$near` están dentro del radio y vienen ordenados; comprueba la contención en el polígono punto por punto; que un polígono en medio del Pacífico devuelva cero; que el cuadrado inscrito no contenga más puntos que el círculo |

`ruff` pasa limpio sobre `src/` y `tests/`.

**Un detalle de la verificación del geohash que vale la pena señalar:** la
implementación refina latitud y longitud de forma alternada en un solo bucle; la
prueba cruzada las cuantiza por separado y entrelaza los bits al final. Que dos
caminos distintos coincidan sobre puntos aleatorios significa que el resultado no
depende de un detalle de implementación. Se ejecutó sobre 20 000 puntos con cero
discrepancias.

---

## 6. Trabajo en equipo

⚠️ Rellenar antes de entregar. El historial de commits se revisa y debe
reflejar el aporte real de cada integrante.

| Integrante | Componentes | Commits |
|---|---|---|
| | | |

```bash
git shortlog -sne                         # commits por autor
git log --pretty='%h %an %ad %s' --date=short
```

---

## Anexo — Comandos de referencia

```bash
# Levantar todo desde cero
./scripts/bootstrap.sh                # Linux/macOS/Git Bash
.\scripts\bootstrap.ps1               # Windows
make all

# Etapas por separado
docker compose run --rm ingestion     # Kaggle → Dask → MongoDB
docker compose run --rm spark-job     # agregaciones
docker compose run --rm benchmark     # comparación

# Pruebas
make test-unit
make test-integration
make lint

# Inspección
make stats        # conteos e índice 2dsphere
make indexes      # índices de la colección principal
make urls         # interfaces web
make demo-queries # una consulta de cada tipo
```
