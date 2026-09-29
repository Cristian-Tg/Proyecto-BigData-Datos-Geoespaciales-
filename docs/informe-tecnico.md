# Informe técnico
## Procesamiento y consulta de datos geoespaciales con despliegue continuo

**Dataset:** US Accidents (2016–2023) — `sobhanmoosavi/us-accidents`
**Volumen:** CSV de 2 916,5 MB · **1 240 933 registros** cargados en MongoDB
**Repositorio:** https://github.com/Cristian-Tg/Proyecto-BigData-Datos-Geoespaciales-

> Todas las cifras de este informe son mediciones de una ejecución real del
> pipeline; ninguna es estimada. Se regeneran con
> `python scripts/report_numbers.py --out docs/mediciones.md`. El detalle de las
> incidencias encontradas durante el desarrollo está en
> [`anexo-incidencias.md`](anexo-incidencias.md).

---

## 1. Arquitectura

```
                       ┌──────────────────────────────┐
                       │       GitHub (main)          │
                       │   ramas + pull requests      │
                       └──────────────┬───────────────┘
                                      │ webhook (push)
                                      ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │                        JENKINS  (:8088)                          │
  │  1 checkout → 2 credenciales → 3 PYTEST ──┐ puerta 1             │
  │  4 build → 5 up → 6 healthchecks          │                      │
  │  7 ingesta → 8 Spark → 9 PRUEBAS API ─────┤ puerta 2             │
  │  10 benchmark → 11 DESPLIEGUE ◀───────────┘                      │
  │  cualquier fallo tras la etapa 5 ⇒ rollback a la imagen `stable` │
  └──────────────────────────────────────────────────────────────────┘
                                      │ docker compose (socket del host)
  ════════════════════════════════════╪══════════════════════════════════
                     red de Docker    ▼   `geobigdata_geonet`
  ┌────────────┐
  │ Kaggle API │  token: credencial de Jenkins, nunca en el repositorio
  └─────┬──────┘
        │ descarga en streaming (trozos de 1 MiB)
        ▼
  ┌───────────────┐  volumen `data_vol`
  │  CSV 2,9 GB   │
  └───────┬───────┘
          │ dd.read_csv(blocksize=16MB) → 191 particiones
          ▼
  ┌────────────────────────────────────────────┐
  │                   DASK                     │
  │  scheduler :8786 · panel :8787             │
  │  worker 1              worker 2            │
  │                                            │
  │  por partición, en el worker:              │
  │    clean_partition()   7 reglas de descarte│
  │    enrich_partition()  grilla, geohash,    │
  │                        hora/día/mes        │
  │    to_documents()      GeoJSON Point       │
  │    insert_many(ordered=False)  lotes 5 000 │
  └────────────────────┬───────────────────────┘
                       ▼
  ┌───────────────────────────────────────────────────────────┐
  │                    MONGODB  :27017                        │
  │  accidents     GeoJSON Point + índice 2dsphere            │
  │                + validador $jsonSchema + 8 índices        │◀──┐
  │                + único en accident_id (idempotencia)      │   │
  │  agg_grid  agg_geohash  agg_hotspots                      │   │
  │  agg_temporal  agg_state  benchmark_runs                  │   │
  └────────┬──────────────────────────────────────────────────┘   │
           │                                                      │
           │  $near / $geoWithin / $geoNear     MongoDB Spark     │
           ▼                                    Connector 10.4.0  │
  ┌──────────────────┐                        ┌──────────────────┐│
  │   API FLASK      │                        │      SPARK       ├┘
  │   Gunicorn :5000 │                        │  master :7077    │
  │  /near  /within  │                        │  UI     :8080    │
  │  /geonear        │                        │  worker (2 núc.) │
  │  /aggregations/* │                        │  5 agregaciones  │
  │  / (Leaflet)     │                        │                  │
  └──────────────────┘                        └──────────────────┘
```

**Siete servicios** con `docker compose up`: `mongo`, `dask-scheduler`,
`dask-worker` (×2), `spark-master`, `spark-worker`, `api`, `jenkins`. **Cuatro
trabajos puntuales** bajo el perfil `jobs`: `ingestion`, `spark-job`,
`benchmark`, `tests`.

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
joins baratos y cada consulta espacial necesitaría un `$lookup`, que no puede
aprovechar el índice `2dsphere`.

```javascript
{
  accident_id: "A-3",                              // clave natural, única
  location: { type: "Point", coordinates: [-87.6298, 41.8781] },  // [lon, lat]
  lat: 41.8781, lon: -87.6298,                     // duplicados planos
  severity: 3, start_time: ISODate("2022-07-11T17:30:00Z"),
  city: "Chicago", county: "Cook", state: "IL",
  distance_mi: 2.4, temperature_f: 78.0, weather: "Cloudy",
  grid_id: "41.8000_-87.7000", grid_lat: 41.8, grid_lon: -87.7,
  geohash: "dp3wj",
  year: 2022, month: 7, hour: 17, dow: 0, is_weekend: false
}
```

**Por qué se precalculan `grid_id`, `geohash` y las columnas temporales.** Son
deterministas y se calculan una sola vez sobre cada registro mientras ya está en
memoria del worker. Hacerlo en cada consulta obligaría a un `$addFields` sobre
más de un millón de documentos; hacerlo en Spark con una UDF de Python costaría
la serialización JVM↔Python de todas las filas. Se pagan una vez y se indexan.

**Por qué `lat`/`lon` además de `location`.** `location` es lo que indexa
`2dsphere`, pero devolverlo desanidado obligaría al cliente a leer
`location.coordinates[1]` para la latitud. Cuestan 16 bytes por documento y
simplifican la API y el mapa.

#### El orden `[lon, lat]`

GeoJSON y MongoDB usan `[longitud, latitud]`, al revés de la convención hablada.
Invertirlo **no genera ningún error**: el índice se construye, las consultas
devuelven resultados, y todos son incorrectos. Tres defensas:

1. `to_geojson_point(lat, lon)` es la **única** función que construye puntos, y
   recibe los argumentos con nombre.
2. El validador `$jsonSchema` valida **por posición**: `coordinates[0]` en
   `[-180,180]` y `coordinates[1]` en `[-90,90]`. Una latitud mal puesta en la
   posición 0 con valor > 90 se rechaza al insertar.
3. Las pruebas comprueban el orden explícitamente, y las de integración
   recalculan las distancias con haversine.

#### Índices

| Índice | Campos | Para qué |
|---|---|---|
| `ix_location_2dsphere` | `location` **2dsphere** | `$near`, `$geoWithin`, `$geoNear` |
| `ux_accident_id` | `accident_id` (único, sparse) | idempotencia de la ingesta |
| `ix_start_time` | `start_time` ↓ | filtros por fecha |
| `ix_state_severity` | `state`, `severity` | filtro combinado, el más usado |
| `ix_year_month` | `year`, `month` | particionado del benchmark |
| `ix_grid_id`, `ix_geohash` | — | agrupaciones sin recalcular |

Las colecciones `agg_*` con geometría llevan su propio `2dsphere` sobre
`centroid`, lo que permite pedir «la grilla de esta zona del mapa».

### 2.2 Limpieza con Dask, y su justificación

Las reglas viven en `src/ingestion/cleaning.py` como **pandas puro**, no en la
API de Dask: así se prueban con pytest sin levantar un cluster, y Dask las aplica
en paralelo sobre cada partición. El papel de Dask es la lectura particionada, la
distribución del trabajo y la carga concurrente por lotes.

Se aplican **de la más barata a la más cara**, para que cada filtro reduzca el
volumen que procesa la siguiente.

| # | Regla | Justificación |
|---|---|---|
| 1 | Coordenadas nulas | Un accidente sin coordenadas es inservible para todo el sistema: no se indexa en `2dsphere` ni puede aparecer en ninguna consulta espacial. No se imputa porque no hay forma defendible de inventar una ubicación. |
| 2 | Fuera de rango WGS84 | MongoDB **rechaza el documento completo** al construir el `2dsphere` si \|lat\|>90 o \|lon\|>180. Filtrarlos antes evita que un registro corrupto aborte un lote. |
| 3 | Relleno `(0,0)` | Cae en el Golfo de Guinea. En un dataset de EE. UU. es siempre un valor por defecto de dato faltante. Es el caso que el rango WGS84 no atrapa. |
| 4 | Fuera de la caja de EE. UU. | El dataset es exclusivamente estadounidense; un punto en París es un error de captura. La caja (lat 17,5–72 · lon −180 a −64,5) incluye Alaska, Hawái y Puerto Rico. |
| 5 | Fecha no parseable | Sin marca temporal válida no es posible el análisis por hora, día y mes que exige el enunciado. Se usa `format="mixed"` porque el CSV combina segundos enteros y fraccionarios. |
| 6 | Severidad fuera de 1–4 | Es la variable de ponderación de las zonas de alta concentración; un valor fuera del dominio contaminaría todos los promedios. |
| 7 | Duplicados por `accident_id` | Dentro de la partición con `drop_duplicates`; la unicidad **global** la garantiza el índice único junto con `insert_many(ordered=False)`. |

**Resultado medido sobre el dataset real:**

| Concepto | Registros |
|---|---:|
| Leídos del CSV | 1 240 933 |
| Descartados por las 7 reglas | **0** |
| Cargados en MongoDB | 1 240 933 |
| Tasa de retención | **100,00 %** |
| Particiones procesadas | 30 de 191 |
| Tiempo de ingesta | 815,2 s (1 522 registros/s) |

#### Por qué la retención es del 100 % y la limpieza sigue siendo necesaria

**Lo que dice el cero:** la versión de marzo de 2023 de US Accidents ya viene
limpia en las siete dimensiones que se validan. `Start_Lat` y `Start_Lng` están
completas, `Severity` siempre está en 1..4, los `ID` son únicos y `Start_Time` es
parseable. Las columnas con huecos en este dataset son otras (`End_Lat`,
`Wind_Chill`, `Precipitation`) y ninguna interviene en la validez geoespacial.

**Por qué las reglas siguen haciendo falta:** una sola coordenada fuera de rango
hace que MongoDB rechace el documento al construir el `2dsphere`, y sin filtro
previo ese fallo aparece a mitad de una carga de un millón de documentos. Además
**están verificadas, no supuestas**: cada regla tiene una prueba unitaria con su
caso sucio, y el generador de datos sintéticos inyecta un 3 % de registros
defectuosos precisamente para que se ejerciten de forma medible. Sobre esos datos
las mismas reglas descartan 35 098 de 1 005 000 (retención 96,51 %).

Afirmar que la limpieza descartó registros en el dataset real exigiría inventar
descartes que no ocurrieron.

#### Muestreo: repartido, no del principio del archivo

El enunciado permite trabajar con una muestra de al menos un millón de registros.
Leer particiones en orden hasta alcanzar el millón produce una muestra
**sesgada**, porque el CSV no está ordenado al azar. Medido sobre las 24 primeras
de 191 particiones, la muestra contenía solo los años 2016, 2017, 2021 y 2022, y
faltaban 2018, 2019, 2020 y 2023 **por completo**: la agregación «accidentes por
año» habría mostrado un hueco inexistente.

La corrección es barajar los índices de partición con una semilla fija antes de
recorrerlos. Cualquier prefijo es entonces una **muestra por conglomerados**
repartida por todo el archivo, y la semilla la mantiene reproducible:

| Cobertura | Antes (secuencial) | Después (repartido) |
|---|---|---|
| Años | 4 de 8 | **8 de 8 (2016–2023)** |
| Estados | 49 | 49 |
| Horas del día | — | **24** |

Es muestreo por conglomerados y no muestreo simple de filas, lo cual se declara
explícitamente en lugar de presentarlo como aleatorio puro.

**Otras dos decisiones de carga.** `insert_many(ordered=False)`: con
`ordered=True` el primer duplicado aborta el resto del lote; con `ordered=False`
MongoDB inserta todo lo insertable y el código distingue el código 11000
(duplicado esperado, contado) de cualquier otro (fallo real, que propaga).
**Índice único antes de la carga, `2dsphere` después**: el único es lo que hace la
ingesta idempotente; el `2dsphere` es mucho más rápido de construir sobre una
colección ya poblada que de mantener durante un millón de inserciones.

### 2.3 Procesamiento con Spark

Cinco colecciones nuevas, en 186,5 s sobre 1 240 933 registros:

| Colección | Agregación | Documentos | Tiempo |
|---|---|---:|---:|
| `agg_grid` | conteo, severidad media y máxima por celda de 0,1° | 23 280 | 26,37 s |
| `agg_geohash` | conteo por celda de geohash (precisión 5 ≈ 4,9 km) | 55 514 | 16,27 s |
| `agg_hotspots` | top-200 por **índice de concentración** | 200 | 4,62 s |
| `agg_temporal` | conteos por hora, día, mes y año | 51 | 11,06 s |
| `agg_state` | conteo y % de graves por estado | 49 | 5,26 s |
| | | | **186,48 s** |

Los hotspots se ordenan por `count × avg_severity` y no por conteo puro: una
celda con muchos accidentes leves no es igual de crítica que una con la mitad
pero graves, y el conteo puro esconde esa diferencia.

`agg_temporal = 51` es 24 horas + 7 días + 12 meses + **8 años**, la confirmación
de que la muestra cubre todo el rango.

**Decisiones.** Esquema explícito al leer, porque dejar que el conector lo infiera
por muestreo es lento sobre millones de documentos y da tipos inestables con
nulos. `spark.sql.shuffle.partitions = 8`, porque el valor por defecto (200)
genera miles de tareas diminutas en un cluster de un worker. `cache()` sobre el
DataFrame de entrada, porque las cinco agregaciones recorren el mismo y sin él
MongoDB se leería cinco veces. El geohash se **reutiliza** si ya está calculado
—es un prefijo jerárquico, así que recortarlo es válido— y solo se recurre a una
UDF si falta, porque la serialización JVM↔Python sobre millones de filas es el
cuello de botella. Los índices de las colecciones nuevas los crea pymongo: el
conector escribe documentos pero no gestiona índices.

### 2.4 Infraestructura

**Sin bind mounts de código en `docker-compose.yml`.** Jenkins ejecuta
`docker compose` contra el demonio del **host** (patrón *Docker outside of
Docker*: se le monta `/var/run/docker.sock`). La consecuencia es que cualquier
ruta relativa del compose se resuelve en el **host**, no en el workspace de
Jenkins: un `./src:/app/src` crearía un directorio vacío en silencio y los
contenedores arrancarían sin código. Todo el código se copia dentro de las
imágenes y los datos viven en volúmenes nombrados. El único bind mount es el
socket, cuya ruta sí existe en el host. Para desarrollo con recarga en caliente
está `docker-compose.dev.yml`, que Jenkins nunca usa.

**Jars del conector en tiempo de build.** Con `--packages`, cada `spark-submit`
resuelve el árbol de dependencias con Ivy: 30–60 s y dependencia de la red. Con
los cinco jars dentro de la imagen, Spark arranca sin red y de inmediato, lo que
importa en una sustentación en vivo. Faltando cualquiera de los cinco, el
conector falla en ejecución con `NoClassDefFoundError`.

**El driver de Spark se anuncia con su IP, no con el nombre del servicio.**
`docker compose run` crea un contenedor efímero que **no** recibe el alias DNS del
servicio; dentro de él ese nombre no resuelve y el driver falla al enlazarse. Se
separa `bindAddress` (0.0.0.0) de la dirección anunciada.

**Presupuesto de memoria explícito.** El equipo de desarrollo tiene 8 GB y Docker
3,8 GB, así que el proyecto trae un perfil de baja memoria que recorta límites y
ejecuta los motores **por fases** (Dask para la ingesta, se baja; Spark para las
agregaciones, se baja). El enunciado exige «al menos dos workers» para Dask y «al
menos uno» para Spark, así que Spark baja a uno y Dask conserva dos. Se activa
solo si se detectan menos de 6 GB, de modo que no hay que recordar la opción.

**Contraseñas.** `.env` está en `.gitignore` y `MONGO_ROOT_PASSWORD` se declara
con `:?` en el compose, de modo que `docker compose up` **falla** con un mensaje
claro si no está definida, en lugar de arrancar con un valor por defecto
inseguro. El pipeline aborta el build si detecta `.env` o `kaggle.json`
versionados.

---

## 3. Consultas geoespaciales implementadas

Las tres son parametrizadas: **no hay ni una coordenada fija en el código**. Todas
comparten `build_attribute_filter()`, que añade filtros por severidad, estado,
ciudad, clima, rango de fechas, año y franja horaria (incluida una que cruce la
medianoche).

### 3.1 `$near` — por radio

```python
{"location": {"$near": {
    "$geometry": {"type": "Point", "coordinates": [lon, lat]},
    "$maxDistance": radius_m,
    "$minDistance": min_distance_m      # solo si > 0
}}}
```

```
GET /api/v1/near?lat=34.0522&lon=-118.2437&radius_m=5000&limit=10&min_severity=3
```

**Dos detalles que son fuente habitual de errores.** Con `$geometry` GeoJSON,
`$maxDistance` está en **metros**; con la forma antigua `[lon, lat]` estaría en
**radianes**, un error de seis órdenes de magnitud que no produce ninguna
excepción, solo resultados absurdos. Y el conteo **no puede usar `$near`**:
MongoDB lo prohíbe dentro de `count`/`aggregate`, así que el total se obtiene con
`$geoWithin` + `$centerSphere`, que delimita el mismo círculo y sí es contable,
con el radio convertido a radianes.

`$near` devuelve los resultados **ordenados de más cercano a más lejano sin
`$sort`**: el índice recorre las celdas en orden de proximidad.

### 3.2 `$geoWithin` — dentro de un polígono

```python
{"location": {"$geoWithin": {"$geometry": geometry}}}
```

```
POST /api/v1/within
{"geometry": {"type":"Polygon","coordinates":[[[-118.55,33.90], …]]},
 "limit": 100, "min_severity": 3, "summary": true}
```

Acepta `Polygon`, `MultiPolygon`, un `Feature` completo de GeoJSON y el atajo
`{"bbox": [min_lon, min_lat, max_lon, max_lat]}`.

`validate_polygon()` valida **antes** de consultar: tipo, número de anillos,
mínimo de 4 posiciones, rangos y anillo cerrado. Se hace porque MongoDB, ante un
anillo sin cerrar, devuelve un error poco informativo; la API devuelve 400
diciendo exactamente qué está mal.

Al contrario que `$near`, **`$geoWithin` sí es válido dentro de `$match`**, lo que
permite el resumen del área con un pipeline de agregación.

#### Un detalle de geodesia

MongoDB interpreta los lados de un `Polygon` como **geodésicas** (arcos de círculo
máximo), **no** como líneas de latitud constante: el lado norte de un
«rectángulo» lat/lon se comba hacia el polo, así que un punto ligeramente al
norte de `max_lat` **sí está dentro** del polígono esférico.

Se detectó al escribir la prueba de integración, que fallaba. Medido con un bbox
de 0,6° de ancho a 34° de latitud, sobre 1000 puntos: 11 excedían `max_lat` entre
1,3 y 17,1 m, y **ninguno** excedía en longitud. Que el exceso aparezca solo en
latitud, solo en las longitudes centrales y nunca en longitud es la firma exacta
del abombamiento: los lados este y oeste son meridianos, que son círculos máximos
y no se comban.

**La prueba estaba mal, no la consulta.** Se corrigió con una tolerancia
documentada de 100 m en latitud, y se añadió una segunda prueba que exige
contención **exacta** en longitud, de modo que un error real seguiría
detectándose.

### 3.3 `$geoNear` — agregación por cercanía

```python
[{"$geoNear": {
    "near": {"type": "Point", "coordinates": [lon, lat]},
    "distanceField": "distance_m",
    "maxDistance": max_distance_m,
    "spherical": True,          # obligatorio con índice 2dsphere
    "query": attribute_filter   # DENTRO de $geoNear, no en un $match posterior
 }}, …$addFields …$group …$project …$sort …$limit]
```

```
GET /api/v1/geonear?lat=34.0522&lon=-118.2437&max_distance_m=20000&group_by=severity
GET /api/v1/geonear?…&group_by=distance_band&band_width_m=2000
```

**Qué aporta sobre `$near`:** expone la distancia calculada como un **campo**, lo
que permite promediarla, construir anillos concéntricos y combinar proximidad con
agrupaciones. Nada de eso se puede hacer con `$near`.

**Dos restricciones:** `$geoNear` tiene que ser la **primera etapa** del pipeline
(verificado en una prueba), y el filtro va **dentro** de `$geoNear` y no en un
`$match` posterior, para que MongoDB lo aplique mientras recorre el índice.

`group_by` admite `severity`, `state`, `city`, `hour`, `dow`, `weather`,
`grid_id`, `geohash`, `distance_band` y `none`. La respuesta incluye el pipeline
generado, para poder auditarlo.

### 3.4 Rendimiento

| Consulta | Parámetros | `elapsed_ms` | Resultados |
|---|---|---:|---:|
| `$near` | radio 5 km (primera, caché fría) | 1 008,7 | 8 144 |
| `$near` | radio 50 km | **25,4** | 102 687 |
| `$near` | radio 5 km + `min_severity=3` | 46,1 | 2 280 |
| `$geoWithin` | área de Los Ángeles | **10,2** | 66 190 |
| `$geoWithin` | + `summary=true` | 30,6 | 66 190 |
| `$geoNear` | 20 km, por severidad | 812,4 | 48 746 |
| `$geoNear` | 20 km, bandas de 2 km | 774,5 | 48 746 |
| `/aggregations/hotspots` | top 20 | **4,2** | 200 |

Tres observaciones que el número suelto no transmite:

**El primer `$near` paga el calentamiento de la caché.** 1 008 ms para 5 km frente
a 25 ms para 50 km, que devuelve doce veces más resultados, no tiene explicación
algorítmica: la primera consulta trae del disco las páginas del índice
`2dsphere`. Con la caché de WiredTiger recortada a 0,4 GB se nota. Las siguientes
van en decenas de ms.

**`$geoWithin` es más rápido que `$near`** (10 ms frente a 25 ms) porque no tiene
que ordenar. Cuando no hace falta el orden por proximidad, `$geoWithin` es la
consulta correcta.

**`$geoNear` es un orden de magnitud más lento** y es esperado: no devuelve
documentos, calcula la distancia de cada uno de los 48 746 del radio y después
agrupa. Se paga por lo que aporta.

El conteo es **metadato opcional** y se trata como tal: si supera su límite de
tiempo, la respuesta llega con `total_matching: null` y una marca explícita, en
lugar de devolver un 500 teniendo los resultados ya calculados.

---

## 4. Análisis comparativo Dask vs Spark

### 4.1 Método

**Operación medida:** agregación por celda de grilla de 0,1° (conteo y suma de
severidad). Es la más costosa del sistema: recorre todos los registros y hace un
shuffle sobre decenas de miles de claves.

Condiciones para que la comparación sea justa:

- Ambos motores leen de la **misma fuente**, la colección de MongoDB, y **los
  mismos tres campos** (`lat`, `lon`, `severity`).
- Calculan **exactamente la misma** agregación, y el resultado se compara al
  final.
- Cada configuración se ejecuta 2 veces; se reporta la **mediana**, con mínimo,
  máximo y desviación típica.
- Se agregan **sumas parciales** y no medias: las medias de medias no se pueden
  combinar entre particiones.

**Cómo se varían los workers.** Dask: un `LocalCluster` con 1 y 2 workers, porque
el número del cluster de compose está fijado en el YAML y el cliente no puede
escalarlo. Spark: `spark.cores.max` ∈ {1, 2} contra el cluster standalone real.

**Cómo se mide la memoria** — en los workers, no en el cliente. Dask:
`client.run()` sobre cada worker devuelve su RSS de `psutil`. Spark: la API REST
de executors del driver da `peakMemoryMetrics.JVMHeapMemory`, excluyendo el
driver.

**Particionado para Dask.** Dask no tiene conector de MongoDB, así que se
particiona por `(year, month)` aprovechando el índice compuesto. Se eligió así, y
no por rangos de `_id` con `skip`, porque un `skip` de millones de documentos es
O(n) en MongoDB y arruinaría la medición por un motivo ajeno al motor.

### 4.2 Resultados

Sobre 1 240 933 registros, 2 repeticiones, mediana:

| Motor | Workers | Mediana (s) | mín | máx | σ | Memoria pico (MB) | Celdas |
|---|---:|---:|---:|---:|---:|---:|---:|
| Dask | 1 | **15,45** | 14,66 | 16,25 | 1,12 | 196,1 | 23 280 |
| Dask | 2 | **8,24** | 7,45 | 9,03 | 1,11 | 357,7 | 23 280 |
| Spark | 1 | **56,22** | 49,97 | 62,48 | 8,85 | 338,9 | 23 280 |
| Spark | 2 | **39,07** | 35,93 | 42,22 | 4,45 | 361,1 | 23 280 |

| Motor | Workers | Speedup real | Ideal | Eficiencia paralela |
|---|---|---:|---:|---:|
| Dask | 1 → 2 | 1,875× | 2,0× | **93,8 %** |
| Spark | 1 → 2 | 1,439× | 2,0× | **72,0 %** |

| Workers | Dask | Spark | Más rápido | Ventaja |
|---:|---:|---:|---|---:|
| 1 | 15,45 s | 56,22 s | Dask | 3,64× |
| 2 | 8,24 s | 39,07 s | Dask | 4,74× |

**Verificación de equivalencia.** Las cuatro configuraciones produjeron **23 280
celdas** sobre **1 240 933 registros**, y esas 23 280 coinciden con las que
escribió la etapa de producción de Spark en `agg_grid`. Sin esa coincidencia la
comparación de tiempos no significaría nada.

### 4.3 Interpretación

**Dask gana con claridad a esta escala, entre 3,6× y 4,7×, y escala mejor:**
93,8 % de eficiencia paralela frente al 72,0 % de Spark.

**Una hipótesis que la medición descartó.** La primera ejecución dio a Spark 6–9×
más lento, y la explicación aparente era que los dos lados no leían lo mismo: Dask
proyectaba 3 campos y Spark leía los 19 del esquema completo. Se corrigió para
que Spark leyera los mismos tres. **La corrección era necesaria pero no explicó
el hueco:** Spark pasó de 62,20 s a 62,48 s. Lo que sí cambió fue su
escalabilidad, de 0,843× (empeoraba al añadir el segundo núcleo) a 1,439×:
leyendo 19 campos, la presión de memoria hacía que dos tareas concurrentes en el
mismo executor se estorbaran.

**Qué explica realmente la diferencia**, en orden de peso estimado:

1. **El conector frente a pymongo directo.** El MongoDB Spark Connector convierte
   cada documento BSON a `InternalRow` de Catalyst, con inferencia y validación de
   tipos por campo. El lado Dask usa `pymongo.find()` y construye un DataFrame de
   tres columnas. Para *leer y agregar* algo más de un millón de documentos, esa
   capa de conversión domina el tiempo.
2. **La reducción no es estructuralmente la misma, y hay que decirlo.** Spark hace
   un shuffle distribuido real sobre 23 280 claves. La implementación de Dask
   agrega por partición y combina los parciales **en el cliente**: una reducción
   en árbol con el paso final local. Sobre 23 280 celdas ese paso es trivial, pero
   no es un shuffle distribuido. Es la forma idiomática de usar cada motor, no un
   truco; aun así, omitirlo dejaría la comparación incompleta.
3. **Costes fijos por trabajo que 1,2 M de filas no amortizan.** Aunque el
   cronómetro arranca **después** de crear la `SparkSession`, cada ejecución paga
   la asignación de executors, el calentamiento del JIT y la planificación del
   shuffle.

**Sobre la memoria.** Los picos son similares, pero significan cosas distintas. En
Spark el pico apenas se mueve entre 1 y 2 núcleos porque lo determina el heap
**configurado** (640 m), no lo que el trabajo necesita. En Dask crece de 196 a
358 MB al duplicar los workers, porque refleja lo que de verdad se carga. Es una
diferencia de modelo de gestión, no de eficiencia.

**Límites de esta medición, declarados.** σ alta en Spark (8,85 s sobre una
mediana de 56,22 s con solo 2 repeticiones): el equipo tiene 8 GB y Docker
3,8 GB, así que hay interferencia entre contenedores. Y **un solo nodo**: Spark
corre con 1 worker y 2 núcleos, de modo que sus ventajas reales —tolerancia a
fallos, vuelco a disco, escalado a decenas de nodos— no tienen ocasión de
pagarse. Sería incorrecto concluir «Spark es lento»; lo correcto es **«a esta
escala y con esta topología, Spark no compensa su coste»**.

### 4.4 En qué casos conviene cada uno

**Dask conviene cuando** el volumen cabe en el cluster disponible (es el caso
medido: 8,24 s frente a 39,07 s); el trabajo es **por partición y sin shuffle
grande**, como la limpieza de la ingesta; el equipo ya trabaja en pandas y NumPy
—las reglas de limpieza son pandas puro y se prueban sin cluster, mientras en
Spark exigirían `pandas_udf` y pagar la serialización JVM↔Python—; se necesita
integración con librerías de Python (el geohash es una implementación propia); y
depurar importa, porque las trazas son de Python.

**Spark conviene cuando** el shuffle **es** el trabajo y no un paso final: aquí
son 23 280 claves, pero con millones la reducción en el cliente del lado Dask
dejaría de ser viable; el volumen supera la memoria agregada y hace falta un
vuelco a disco maduro; se quiere el conector nativo con esquema tipado, que la
etapa de producción aprovecha para escribir cinco colecciones sin código de
serialización; y hay que crecer a decenas de nodos, régimen sobre el que esta
medición no dice nada.

**Por eso el sistema usa los dos, y no uno.** No es redundancia:

| Etapa | Motor | Razón |
|---|---|---|
| Ingesta y limpieza | **Dask** | trabajo por partición, sin shuffle; reglas en pandas probadas con pytest; 1 522 registros/s |
| Agregaciones | **Spark** | shuffle sobre decenas de miles de claves, conector nativo, cinco colecciones en 186,5 s |

Si el dataset creciera de 1,2 M a 100 M de registros, la conclusión probablemente
se invertiría para la agregación: la reducción en el cliente del lado Dask dejaría
de caber en memoria y el coste fijo de Spark quedaría amortizado. Eso es una
extrapolación, no una medición, y se declara como tal.

---

## 5. Verificación y calidad

**261 pruebas unitarias** más **52 de integración**, todas en verde.

| Archivo | Cubre |
|---|---|
| `test_geo.py` | geohash contra valores de referencia públicos (`ezs42`, `u4pruydqqvj`, `6gkzwg`, `dqcjqcpe`, `gcpvj0d`) **y contra un algoritmo independiente**; grilla con coordenadas negativas; validación de polígonos; haversine |
| `test_cleaning.py` | las 7 reglas con su caso sucio; que los descartes sumen el total; que los tipos de NumPy se conviertan a nativos (pymongo no codifica `np.int16`) |
| `test_queries.py` | que se usen `$near`/`$geoWithin`/`$geoNear` con la forma exacta; que el conteo use `$centerSphere` en radianes; que `$geoNear` sea la primera etapa; que la regex de ciudad esté escapada (ReDoS) |
| `test_api.py` | validación de parámetros, códigos 400/503, que los errores sean JSON y no HTML de Flask |
| `test_compat.py` | que el código sea válido en Python 3.10, porque el proyecto corre **dos** intérpretes (3.11 en dask/api/tests, 3.10 en spark/benchmark) |
| `test_integration.py` | **recalcula con haversine** que los puntos de `$near` están dentro del radio y ordenados; contención en el polígono punto por punto; que un polígono en el Pacífico devuelva cero; que el cuadrado inscrito no contenga más puntos que el círculo |

`ruff` pasa limpio con `target-version = "py310"`: la versión **mínima** del
proyecto, no la de la mayoría de las imágenes.

**Un detalle de la verificación del geohash.** La implementación refina latitud y
longitud de forma alternada en un solo bucle; la prueba cruzada las cuantiza por
separado y entrelaza los bits al final. Que dos caminos distintos coincidan sobre
puntos aleatorios significa que el resultado no depende de un detalle de
implementación: se ejecutó sobre 20 000 puntos con cero discrepancias.

---

## 6. Trabajo en equipo

| Integrante | Componentes | Commits |
|---|---|---|
| | | |

```bash
git shortlog -sne                          # commits por autor
git log --pretty='%h %an %ad %s' --date=short
```

---

## Anexo — Comandos de referencia

```bash
# Levantar todo desde cero (un solo comando)
./scripts/bootstrap.sh --lowmem            # Linux/macOS/Git Bash
.\scripts\bootstrap.ps1                    # Windows PowerShell

# Etapas por separado
DC="docker compose -f docker-compose.yml -f docker-compose.lowmem.yml"
$DC run --rm ingestion      # Kaggle → Dask → MongoDB
$DC run --rm spark-job      # agregaciones
$DC run --rm benchmark      # comparación

# Pruebas
$DC run --rm --no-deps tests pytest -m "not integration" -q
$DC run --rm -e API_BASE_URL=http://api:5000 tests pytest -m integration -q

# Regenerar las mediciones de este informe
python scripts/report_numbers.py --out docs/mediciones.md
```
