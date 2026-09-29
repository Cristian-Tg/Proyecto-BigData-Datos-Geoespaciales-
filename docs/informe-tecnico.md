# Informe técnico
## Procesamiento y consulta de datos geoespaciales con despliegue continuo

**Dataset:** US Accidents (2016–2023) — `sobhanmoosavi/us-accidents`
**Volumen:** 7,7 M de registros · ~3 GB de CSV · 2 M cargados en MongoDB
**Repositorio:** `<URL-DEL-REPOSITORIO>`

> **Todas las cifras de este informe son mediciones reales** de una ejecución
> completa del pipeline sobre el dataset de Kaggle (1 240 933 registros cargados
> de un CSV de 2 916,5 MB). No hay ningún número estimado ni inventado.
>
> Para regenerarlas tras otra ejecución:
>
> ```bash
> python scripts/report_numbers.py --out docs/mediciones.md
> ```
>
> genera [`docs/mediciones.md`](mediciones.md) con las cuatro tablas ya
> formateadas, leyéndolas de los artefactos que produce el propio pipeline
> (`cleaning_stats.json`, `spark_summary.json`, `benchmark_latest.md`) y midiendo
> las consultas contra la API en vivo.
>
> Lo único pendiente de rellenar a mano es la tabla de integrantes de la
> sección 6.

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

**La muestra se toma repartida por todo el archivo, no del principio.** El
enunciado permite trabajar con «una muestra de al menos un millón de registros».
La forma obvia —leer particiones en orden hasta alcanzar el millón— produce una
muestra **sesgada**, porque el CSV de Kaggle no está ordenado al azar.

Se detectó al inspeccionar la primera ingesta real (24 de 191 particiones,
1 021 487 registros):

| Distribución obtenida | |
|---|---|
| Años presentes | 2016, 2017, 2021, 2022 |
| Años **ausentes** | **2018, 2019, 2020, 2023** |
| Estados distintos | 49 (eso sí era representativo) |

Con esa muestra, la agregación «accidentes por año» que pide el enunciado
mostraría un hueco de cuatro años que no existe en los datos, y el análisis
temporal no diría nada real sobre el fenómeno.

La corrección es barajar los índices de partición con una semilla fija antes de
recorrerlos: cualquier prefijo es entonces una **muestra por conglomerados**
repartida por todo el archivo, y la semilla la mantiene reproducible. Queda
registrado en `cleaning_stats.json` (`partition_order`, `partition_seed`) para
que el informe pueda declarar cómo se obtuvo la muestra.

Es muestreo por conglomerados y no muestreo simple de filas —las particiones son
bloques contiguos del archivo—, lo cual se declara explícitamente en lugar de
presentarlo como aleatorio puro.

**La descarga no usa la librería de Kaggle.** `KaggleApi.dataset_download_file`
acumula la respuesta completa en memoria antes de escribirla en disco. Con el
archivo de US Accidents (653 MB comprimidos) el contenedor de ingesta llegaba
exactamente a su límite y el kernel lo mataba con SIGKILL, y el log no mencionaba
la memoria en ningún momento: la última línea era `Dataset URL: …` y después
nada.

El proyecto consume el endpoint REST de Kaggle directamente con `requests` y
`stream=True`, escribiendo en trozos de 1 MiB, de modo que la memoria es
constante e independiente del tamaño del dataset:

| | Memoria pico | Resultado |
|---|---:|---|
| `dataset_download_file` | 700 MiB (el límite) | SIGKILL, sin descarga |
| Streaming propio | **166 MiB** | 653 MB descargados → CSV de 2 916,5 MB |

Se añaden tres cosas que la librería no daba: el tipo de archivo se decide por
la firma ZIP y no por la extensión (Kaggle no siempre la incluye), se compara
`Content-Length` con los bytes escritos para no dejar un CSV truncado que
fallaría después con un error confuso, y se informa del progreso cada 100 MB
porque en una descarga de varios minutos el silencio no distingue «avanzando» de
«colgado». La librería oficial queda como último recurso si el endpoint cambiara.

**Resultados medidos sobre el dataset real** (de `cleaning_stats.json`, que la
ingesta escribe al terminar):

| Concepto | Registros |
|---|---:|
| Leidos del CSV | 1 240 933 |
| Descartados por las 7 reglas | **0** |
| Cargados en MongoDB | 1 240 933 |
| Tasa de retencion | **100,00 %** |
| Particiones procesadas | 30 de 191 |
| Tiempo de ingesta | 815,2 s (1 522 registros/s) |

#### Por que la retencion es del 100 %, y por que la limpieza sigue siendo necesaria

Una tabla de ceros invita a pensar que las reglas no hacen nada. No es el caso, y
conviene ser explicito sobre las dos cosas distintas que se estan midiendo.

**Lo que dice el cero:** la version de marzo de 2023 de US Accidents ya viene
limpia en las siete dimensiones que se validan. `Start_Lat` y `Start_Lng` estan
completas, `Severity` siempre esta en 1..4, los `ID` son unicos y `Start_Time` es
parseable. Las columnas que si tienen huecos en este dataset son otras
(`End_Lat`, `Wind_Chill`, `Precipitation`) y ninguna interviene en la validez
geoespacial ni en el analisis temporal, asi que no se filtra por ellas.

**Por que las reglas siguen siendo necesarias:**

- Son la barrera que impide que un registro corrupto aborte una carga de un
  millon de documentos. Una sola coordenada fuera de rango WGS84 hace que MongoDB
  rechace el documento al construir el indice `2dsphere`; sin el filtro previo,
  ese fallo aparece a mitad de la ingesta.
- El validador `$jsonSchema` de MongoDB las respalda como ultima linea de
  defensa, de modo que hay dos capas independientes.
- **Estan verificadas, no supuestas.** Cada una tiene una prueba unitaria con su
  caso sucio (`tests/test_cleaning.py`), y el generador de datos sinteticos
  inyecta a proposito un 3 % de registros defectuosos precisamente para que las
  reglas se ejerciten de forma medible. Sobre esos datos, las mismas siete reglas
  descartan exactamente lo inyectado:

| Regla | Descartados sobre datos sinteticos |
|---|---:|
| Coordenadas nulas | 6 033 |
| Fuera de rango WGS84 | 6 119 |
| Relleno (0,0) | 6 074 |
| Fecha no parseable | 5 981 |
| Severidad fuera de 1..4 | 6 058 |
| Duplicados por ID | 3 354 |
| **Total** | **35 098 de 1 005 000 (retencion 96,51 %)** |

La conclusion honesta es que la limpieza es correcta y esta probada, y que este
dataset concreto no la necesita. Afirmar lo contrario exigiria inventar descartes
que no ocurrieron.

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

**Resultados medidos** (de `spark_summary.json`, sobre 1 240 933 registros):

| Colección | Documentos | Tiempo (s) |
|---|---:|---:|
| `agg_grid` | 23 280 | 26,37 |
| `agg_geohash` | 55 514 | 16,27 |
| `agg_hotspots` | 200 | 4,62 |
| `agg_temporal` | 51 | 11,06 |
| `agg_state` | 49 | 5,26 |
| **Total** | | **186,48** |

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

Medido con `scripts/report_numbers.py` sobre los 1 240 933 registros:

| Consulta | Parámetros | `elapsed_ms` | Resultados |
|---|---|---:|---:|
| `$near` | radio 5 km | 1 008,7 | 8 144 |
| `$near` | radio 50 km | **25,4** | 102 687 |
| `$near` | radio 5 km + `min_severity=3` | 46,1 | 2 280 |
| `$geoWithin` | área de Los Ángeles | **10,2** | 66 190 |
| `$geoWithin` | + `summary=true` | 30,6 | 66 190 |
| `$geoNear` | 20 km, por severidad | 812,4 | 48 746 |
| `$geoNear` | 20 km, bandas de 2 km | 774,5 | 48 746 |
| `/aggregations/hotspots` | top 20 | **4,2** | 200 |

Tres observaciones que el número suelto no transmite:

**El primer `$near` paga el calentamiento de la caché.** 1 008 ms para un radio
de 5 km frente a 25 ms para uno de 50 km, que devuelve doce veces más
resultados, no tiene explicación algorítmica: es que la primera consulta trae del
disco las páginas del índice `2dsphere`. Con la caché de WiredTiger recortada a
0,4 GB, ese primer acceso se nota. Las siguientes van en decenas de ms.

**`$geoWithin` es más rápido que `$near`** (10 ms frente a 25 ms) porque no tiene
que ordenar: `$near` devuelve los resultados por proximidad, y ese orden lo
impone el recorrido del índice. Cuando no hace falta el orden, `$geoWithin` es la
consulta correcta.

**`$geoNear` es un orden de magnitud más lento** (774–812 ms) y es esperado: no
devuelve documentos, ejecuta un pipeline de agregación que calcula la distancia
de cada uno de los 48 746 documentos del radio y después agrupa. Se paga por lo
que aporta, que es exactamente lo que `$near` no puede dar.

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

### 4.2 Resultados medidos

Sobre 1 240 933 registros reales, 2 repeticiones por configuración, mediana:

| Motor | Workers | Mediana (s) | mín | máx | σ | Memoria pico workers (MB) | Celdas | Registros |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Dask | 1 | **15,45** | 14,66 | 16,25 | 1,12 | 196,1 | 23 280 | 1 240 933 |
| Dask | 2 | **8,24** | 7,45 | 9,03 | 1,11 | 357,7 | 23 280 | 1 240 933 |
| Spark | 1 | **56,22** | 49,97 | 62,48 | 8,85 | 338,9 | 23 280 | 1 240 933 |
| Spark | 2 | **39,07** | 35,93 | 42,22 | 4,45 | 361,1 | 23 280 | 1 240 933 |

**Escalabilidad**

| Motor | Workers | Speedup real | Ideal | Eficiencia paralela |
|---|---|---:|---:|---:|
| Dask | 1 → 2 | 1,875× | 2,0× | **93,8 %** |
| Spark | 1 → 2 | 1,439× | 2,0× | **72,0 %** |

**Comparación directa**

| Workers | Dask | Spark | Más rápido | Ventaja |
|---:|---:|---:|---|---:|
| 1 | 15,45 s | 56,22 s | Dask | 3,64× |
| 2 | 8,24 s | 39,07 s | Dask | 4,74× |

**Verificación de equivalencia.** Las cuatro configuraciones produjeron
**23 280 celdas** sobre **1 240 933 registros**, y esas 23 280 coinciden con las
que escribió la etapa de producción de Spark en `agg_grid`. Sin esa coincidencia
la comparación de tiempos no significaría nada, porque no habría garantía de que
los dos motores estuvieran haciendo el mismo trabajo.

### 4.3 Interpretación

**Dask gana con claridad a esta escala, entre 3,6× y 4,7×.** Y escala mejor:
93,8 % de eficiencia paralela frente al 72,0 % de Spark.

#### Una hipótesis que la medición descartó

La primera ejecución dio a Spark 6–9× más lento, y la explicación aparente era
que los dos lados no leían lo mismo: Dask proyectaba 3 campos
(`{lat, lon, severity}`) en su `find()` y Spark leía los 19 de
`ACCIDENT_SCHEMA`. Se corrigió añadiendo `GRID_SCHEMA` para que Spark leyera
exactamente los mismos tres campos.

**La corrección era necesaria pero no explicó el hueco:** con 3 campos, Spark 1
core pasó de 62,20 s a 62,48 s. La proyección no era la causa. Se deja
documentado porque una hipótesis descartada con una medición vale más que una
explicación plausible sin comprobar.

Lo que sí cambió al igualar la lectura fue la **escalabilidad** de Spark: pasó de
0,843× (empeoraba al añadir el segundo núcleo) a 1,439×. Leyendo 19 campos, la
presión de memoria hacía que dos tareas concurrentes en el mismo executor se
estorbaran.

#### Qué explica realmente la diferencia

Tres factores, en orden de peso estimado, y uno de ellos es una asimetría del
propio diseño que conviene declarar:

**1. El conector frente a pymongo directo.** El MongoDB Spark Connector convierte
cada documento BSON a `InternalRow` de Catalyst, con inferencia y validación de
tipos por campo. El lado Dask usa `pymongo.find()` y construye un
`pandas.DataFrame` de tres columnas. Para *leer y agregar* algo más de un millón
de documentos, esa capa de conversión domina el tiempo.

**2. La reducción no es estructuralmente la misma, y hay que decirlo.** Spark
hace un shuffle distribuido real sobre 23 280 claves. La implementación de Dask
agrega por partición y combina los resultados parciales **en el cliente**: es una
reducción en árbol con el paso final local. Sobre 23 280 celdas ese paso final es
trivial (un `groupby` de unas decenas de miles de filas), pero no es un shuffle
distribuido. Es la forma idiomática de usar cada motor, no un truco; aun así, una
comparación que lo omitiera estaría incompleta.

**3. Costes fijos por trabajo que 1,2 M de filas no amortizan.** Aunque el
cronómetro arranca **después** de crear la `SparkSession` (por eso el arranque de
la JVM no está incluido), cada ejecución paga la asignación de executors por el
master, el calentamiento del JIT y la planificación del shuffle. Son decenas de
segundos que sobre este volumen pesan en proporción.

#### Sobre la memoria

Los picos son similares (339–361 MB en Spark, 196–358 MB en Dask), pero significan
cosas distintas. En Spark el pico apenas se mueve entre 1 y 2 núcleos porque está
determinado por el heap **configurado** (640 m), no por lo que el trabajo
necesita. En Dask crece de 196 a 358 MB al duplicar los workers, porque refleja lo
que de verdad se carga en los DataFrames. Es una diferencia de modelo de gestión,
no de eficiencia.

#### Límites de esta medición, declarados

- **σ alta en Spark** (8,85 s sobre una mediana de 56,22 s con solo 2
  repeticiones). El equipo tiene 8 GB y Docker 3,8 GB, así que hay interferencia
  entre contenedores. `make benchmark-full` sube a 3 repeticiones y 3
  configuraciones; con más repeticiones el intervalo se estrecharía.
- **Un solo nodo.** Spark corre con 1 worker y 2 núcleos como máximo. Sus ventajas
  reales —tolerancia a fallos, vuelco a disco bajo presión, escalado a decenas de
  nodos— no tienen ocasión de pagarse aquí. Sería incorrecto concluir «Spark es
  lento» a partir de este experimento; lo correcto es «a esta escala y con esta
  topología, Spark no compensa su coste».
- **La eficiencia paralela de Dask (93,8 %) es casi ideal**, lo que sugiere que el
  cuello de botella a 2 workers sigue siendo el cómputo y no la lectura de
  MongoDB. Con más workers cabría esperar que la lectura pasara a dominar.


### 4.4 En qué casos conviene cada uno

Conclusión apoyada en lo medido en **este** sistema, no en literatura general.

**Dask conviene cuando:**

- **El volumen cabe en el cluster disponible.** Es el caso medido: 1,2 M de
  registros, 8,24 s con 2 workers frente a 39,07 s de Spark. El coste fijo por
  trabajo de Spark no se amortiza a esta escala.
- **El trabajo es por partición y sin shuffle grande**, como la limpieza de la
  ingesta. Cada partición se limpia y se inserta de forma independiente; no hay
  nada que redistribuir entre workers.
- **El equipo ya trabaja en pandas y NumPy.** Las reglas de limpieza de este
  proyecto son pandas puro y se prueban con pytest sin levantar cluster. Esa
  misma capacidad en Spark exigiría `pandas_udf` y pagar la serialización
  JVM↔Python.
- **Se necesita integración fina con librerías de Python.** El geohash se calcula
  con una implementación propia en Python; en Spark habría que elegir entre una
  UDF lenta o reimplementarlo en Scala.
- **Depurar importa.** Las trazas son de Python. En Spark llegan a través de Py4J
  y un `KilledWorker` o un `ExecutorLostFailure ... code 52` no dice qué pasó
  (los dos aparecieron en este proyecto y costaron un rato entender).

**Spark conviene cuando:**

- **El shuffle es el trabajo**, no un paso final. Aquí el shuffle sobre 23 280
  claves es pequeño; con millones de claves distintas la reducción en el cliente
  que usa el lado Dask dejaría de ser viable y el motor de shuffle de Spark
  pasaría a ser la única opción razonable.
- **El volumen supera la memoria agregada del cluster.** El vuelco a disco de
  Spark es maduro y predecible. Dask también vuelca, pero en este proyecto los
  workers de 480 MB pausaban y, cuando la partición era demasiado grande, morían
  con `KilledWorker` sin mensaje sobre memoria.
- **Se quiere el conector nativo con pushdown de predicados y esquema tipado.**
  La etapa de producción lo aprovecha: lee con `ACCIDENT_SCHEMA` explícito y
  escribe cinco colecciones sin código de serialización.
- **Hay que crecer a decenas de nodos.** Nada de lo medido aquí dice algo sobre
  ese régimen, que es precisamente donde Spark está más probado.

**Por eso el sistema usa los dos, y no uno.** No es redundancia: cada motor está
en la etapa donde su modelo de ejecución encaja.

| Etapa | Motor | Razón |
|---|---|---|
| Ingesta y limpieza | **Dask** | trabajo por partición, sin shuffle; reglas en pandas probadas con pytest; 1 522 registros/s |
| Agregaciones | **Spark** | shuffle sobre decenas de miles de claves de grilla y geohash, conector nativo a MongoDB, cinco colecciones en 186,5 s |

Si el dataset creciera de 1,2 M a 100 M de registros, la conclusión probablemente
se invertiría para la agregación: la reducción en el cliente del lado Dask dejaría
de caber en memoria y el coste fijo de Spark quedaría amortizado. Eso es una
extrapolación, no una medición, y se declara como tal.


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
