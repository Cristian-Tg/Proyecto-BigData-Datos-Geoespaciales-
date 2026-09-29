# Anexo — Incidencias encontradas durante el desarrollo

> Material de apoyo, **no forma parte de las diez páginas del informe técnico**.
>
> Se recoge aquí porque el enunciado advierte que en la sustentación habrá
> preguntas individuales sobre cualquier parte del sistema. Cada entrada sigue el
> mismo esquema: qué síntoma se vio, por qué el síntoma no señalaba la causa, y
> qué se cambió.

Dieciséis defectos. **Ninguno era detectable sin ejecutar el sistema**: el linter
estaba en verde y las pruebas unitarias pasaban en todos los casos.

---

## Bloque 1 — Aparecieron al construir las imágenes

### 1. La imagen oficial de Spark no tiene `python`

**Síntoma.** La construcción de la imagen de benchmark fallaba con `exit code 127`.

**Por qué engañaba.** 127 es «command not found», pero el comando que fallaba era
`python -c "import numpy, pandas…"`, así que parecía que faltaba un módulo de
Python, no el intérprete.

**Causa.** La imagen `spark:3.5.3` trae `python3` pero **no** `python`. Verificado:
`command -v python` → vacío, `command -v python3` → `/usr/bin/python3`.

**Alcance real.** No era solo la build: los comandos de `spark-job` y `benchmark`
en el compose eran `["python", "-m", …]`, así que habrían fallado en ejecución.

**Corrección.** Enlace `python` → `python3` en las dos imágenes basadas en Spark,
`python3` explícito en los comandos, y `PYSPARK_PYTHON` / `PYSPARK_DRIVER_PYTHON`
fijados para que driver y executors usen el mismo intérprete (si difieren, Spark
aborta con *«Python in worker has different version than that in driver»*).

### 2. Jenkins 2.492.1 es demasiado antiguo para el catálogo de plugins actual

**Síntoma.** `jenkins-plugin-cli` abortaba con `VersionCompatibilityException`.

**Causa.** Los plugins publicados hoy exigen core ≥ 2.504.3:
`ansicolor` pedía 2.504.3, `junit` 2.504.3, `workflow-api` 2.504.1.

**Corrección.** Se fija la LTS vigente, `2.541.3-lts-jdk17`. Se aprovechó para
quitar `htmlpublisher` y `warnings-ng`: ninguno se usa, y `warnings-ng` arrastra
el aviso de seguridad SECURITY-3937 (XSS almacenado). Instalar un plugin con una
vulnerabilidad conocida que además no hace falta es superficie de ataque gratis.

### 3. `kaggle` sin versión fija no era reproducible

**Síntoma.** Ninguno visible.

**Causa.** Con `kaggle>=1.6.17,<2.0`, la imagen de Dask resolvió 1.8.4 y la de
benchmark 1.7.4.5, porque pip retrocedió ante el `numpy==1.26.4` que fija
`spark.txt`. Dos imágenes del **mismo commit** con distinta versión de la librería
de descarga es exactamente lo que el fijado de versiones debe evitar.

**Corrección.** `kaggle==1.7.4.5`.

### 4. Los `.sh` con CRLF rompen los contenedores Linux

**Síntoma.** Ninguno todavía; se previno.

**Causa.** Con `core.autocrlf` activo, un clon en Windows entrega
`download-mongo-jars.sh` con CRLF y dentro del contenedor falla con
`/usr/bin/env: bad interpreter: No such file or directory` — el `\r` final, que no
se ve.

**Corrección.** `.gitattributes` fuerza LF en `.sh`, Dockerfiles, Makefile y
código; CRLF solo en `.ps1`. Verificado sobre el índice de git.

---

## Bloque 2 — Aparecieron al ejecutar el pipeline

### 5. `KilledWorker`: la ingesta mataba a los workers de Dask

**Síntoma.**
`distributed.scheduler.KilledWorker: Attempted to run task … on 4 different
workers, but all those workers died while running it`.

**Por qué engañaba.** El mensaje no menciona la memoria en ningún momento.

**Causa.** `to_documents()` materializaba **todos** los documentos de la partición
antes de insertar. Una partición de 250 000 filas son 250 000 diccionarios de
Python con un subdocumento GeoJSON anidado: 500–700 MB solo en sobrecarga de
objetos, muy por encima del límite de 480 MB del worker.

**Corrección.** Los documentos se construyen **por lotes**, justo antes de
insertar cada uno, y se descartan después. El consumo pasa de O(partición) a
O(`batch_size`).

**Medido tras el arreglo:** 969 902 registros en 73,8 s (13 137 reg/s) con
workers de 480 MB.

### 6. `pyspark` no estaba en el `PYTHONPATH`

**Síntoma.** `ModuleNotFoundError: No module named 'pyspark'` al ejecutar
`python3 -m src.processing.spark_aggregations`.

**Causa.** `pyspark` no se instala por pip: vive dentro de la distribución de
Spark, en `/opt/spark/python`, y solo `spark-submit` lo añade al `PYTHONPATH`.

**Corrección.** Un archivo `.pth` en `site-packages`, generado en tiempo de build,
que descubre el zip de py4j con un glob (su nombre lleva la versión:
`py4j-0.10.9.7-src.zip`) y **verifica el import en la propia build**. Si un futuro
cambio de versión de Spark lo rompe, falla el build y no la sustentación.

### 7. El generador sintético dejaba el resultado por debajo del mínimo

**Causa.** Producía exactamente `SAMPLE_SIZE` filas, pero inyecta un 3 % de
registros sucios que la limpieza descarta: 1 000 000 generadas → **969 902**
cargadas, por debajo del millón que exige el enunciado.

**Corrección.** Se genera un 12 % de margen.

### 8. El generador sintético solo producía 12 horas distintas

**Síntoma.** La agregación temporal de Spark daba 39 documentos en lugar de 51, y
la prueba de integración que exige las 24 franjas horarias habría fallado.

**Causa.** Su `hour_pool` tenía 12 valores.

**Por qué importa.** Los datos sintéticos deben tener la misma **forma** que los
reales, o las pruebas que los usan no dicen nada sobre el pipeline real.

**Corrección.** Las 24 horas, ponderadas hacia las horas pico.

### 9. `UnresolvedAddressException` al arrancar el driver de Spark

**Síntoma.** `java.nio.channels.UnresolvedAddressException` en `Net.checkAddress`
durante el bind.

**Causa.** `SPARK_DRIVER_HOST` estaba fijado al nombre del servicio (`spark-job`),
pero `docker compose run` crea un contenedor **efímero** que no recibe el alias
DNS del servicio. Verificado desde dentro: `getent hosts spark-job` → nada,
`getent hosts spark-master` → `172.21.0.4`.

**Corrección.** El driver usa la IP del propio contenedor, y se separa
`bindAddress` (0.0.0.0) de la dirección anunciada.

### 10. `spark.jars.packages` contradecía el diseño

**Causa.** Los jars están dentro de la imagen justo para no pagar Ivy en cada
envío, pero el código lo activaba igual: 30–60 s y dependencia de la red en cada
ejecución.

**Corrección.** Solo con `SPARK_USE_IVY_PACKAGES=1`. Ahora el log dice
*«Conector de MongoDB tomado de los jars de la imagen (sin resolución de Ivy)»*.

### 11. La librería de Kaggle bufferea la descarga en memoria

**Síntoma.** El contenedor llegaba a **700 MiB / 700 MiB** exactos y el kernel lo
mataba con SIGKILL (`exit 137`). El log se cortaba tras
`Dataset URL: https://www.kaggle.com/…` y no decía nada más.

**Causa.** `KaggleApi.dataset_download_file` acumula la respuesta completa en
memoria antes de escribirla. El archivo comprimido son 653 MB.

**Corrección.** Se consume el endpoint REST de Kaggle directamente con `requests`
y `stream=True`, en trozos de 1 MiB. La memoria es constante e independiente del
tamaño.

| | Memoria pico | Resultado |
|---|---:|---|
| `dataset_download_file` | 700 MiB (el límite) | SIGKILL, sin descarga |
| Streaming propio | **166 MiB** | 653 MB → CSV de 2 916,5 MB |

Se añadieron tres cosas que la librería no daba: el tipo de archivo se decide por
la firma ZIP (`PK\x03\x04`) y no por la extensión, que Kaggle no siempre incluye;
se compara `Content-Length` con los bytes escritos para no dejar un CSV truncado
que fallaría después con un error confuso; y se informa del progreso cada 100 MB,
porque en una descarga de varios minutos el silencio no distingue «avanzando» de
«colgado».

### 12. El healthcheck marcaba `unhealthy` un MongoDB perfectamente sano

**Síntoma.** `dependency failed to start: container geo-mongo is unhealthy`,
mientras los logs de MongoDB mostraban checkpoints normales y consultas atendidas.

**Causa.** La sonda es `mongosh`, que es un proceso de **Node.js**. Arrancarlo
dentro del contenedor cuesta segundos y, bajo presión de CPU (se descomprimía un
CSV de 2,9 GB en paralelo), superaba el timeout de 5 s. El registro de salud solo
decía `Health check exceeded timeout (5s)`.

**Dos cosas que no eran evidentes.** El healthcheck se ejecuta **dentro** del
contenedor, así que los ~150 MB de Node salen del mismo límite de memoria que usa
el servidor. Y `depends_on: service_healthy` convierte un falso negativo de la
sonda en un fallo de todo el pipeline, así que una sonda demasiado estricta es
peor que no tenerla.

**Corrección.** `interval 20s`, `timeout 20s`, `start_period 40s`, `retries 6`,
`--norc`, y el límite de memoria de Mongo sube a 900M.

---

## Bloque 3 — Aparecieron al ejecutar las pruebas de integración

### 13. Las 52 pruebas de integración nunca se ejecutaban

**Síntoma.** 52 errores idénticos: `ScopeMismatch: You tried to access the
function scoped fixture api_base_url…`.

**Por qué engañaba.** Parecía un problema de configuración de pytest, no de
código. Y como pytest abortaba **antes** de ejecutar una sola prueba, los tres
defectos siguientes estaban ocultos detrás de este.

**Causa.** La fixture `base` es de ámbito *module* y dependía de `api_base_url`,
que era de ámbito *function*.

**Corrección.** `api_base_url` pasa a ámbito de sesión.

### 14. `$geoNear` con bandas agrupaba por una constante

**Síntoma.** Un único grupo con los 16 718 documentos dentro, y
`TypeError: not all arguments converted during string formatting` en la prueba
que comprobaba que las bandas fueran múltiplos del ancho.

**Causa.** En `GEONEAR_GROUPS`, `"distance_band"` apuntaba a `"_band"` **sin el
`$`**, así que MongoDB agrupaba por la cadena literal en lugar de por el campo.

**Por qué la prueba unitaria no lo detectó.** Solo verificaba la etapa
`$addFields`, no la clave de agrupación. Ahora comprueba que la clave sea una
referencia a campo (`"$_band"`).

### 15. Un timeout en el conteo tumbaba la consulta con un 500

**Síntoma.** `error_mongodb: PlanExecutor error during aggregation … operation
exceeded time limit` sobre `/api/v1/near`, de forma intermitente.

**Por qué engañaba.** El error aparecía en la consulta `$near`, pero el operador
que fallaba era el **conteo**: `count_documents` se ejecuta internamente como una
agregación, de ahí el mensaje.

**Causa.** `count_documents` con `$geoWithin` sobre ~970 000 documentos superaba
`maxTimeMS=15s` con la caché de WiredTiger recortada a 0,25 GB.

**La decisión de diseño.** El total es metadato **opcional**: el cliente ya tiene
los resultados. Devolver 500 teniendo datos válidos calculados es la respuesta
equivocada.

**Corrección.** El conteo degrada a `total_matching: null` con una marca
`total_matching_timed_out: true`, para que el cliente distinga «no hay
resultados» de «no se pudo contar». La caché sube a 0,4 GB, que era la causa real.

### 16. `datetime.UTC` no existe en Python 3.10

**Síntoma.** `ImportError: cannot import name 'UTC' from 'datetime'` al ejecutar
el benchmark.

**Por qué era el más traicionero de todos.** El linter estaba **en verde** y las
pruebas unitarias **también** (corren en 3.11). El error aparecía solo en
ejecución, dentro del contenedor de Spark.

**Causa raíz.** El proyecto corre **dos** versiones de Python y `ruff.toml`
apuntaba a la de la mayoría en lugar de al mínimo común:

| Imagen | Python |
|---|---|
| `dask`, `api`, `tests` | 3.11.16 |
| `spark`, `benchmark` | **3.10.12** |

Con `target-version = "py311"`, el autofix de ruff convirtió `timezone.utc` en
`datetime.UTC`, que solo existe desde 3.11.

**Corrección.** `ruff.toml` apunta a `py310`, y `datetime.UTC` → `timezone.utc`
en los cuatro archivos afectados. La cadena `"UTC"` de
`spark.sql.session.timeZone` no se toca: es un valor de configuración.

Y una prueba de regresión, `tests/test_compat.py` (51 casos), que cierra la clase
de fallo entera y no este caso concreto: ningún archivo importa `UTC` de
`datetime`; todo `src/` parsea como AST válido; todo archivo con anotaciones
modernas declara `from __future__ import annotations`, sin el cual en 3.10 se
evalúan al importar y fallan; `isinstance` con unión por barra no se cuela; y
`ruff.toml` apunta a la versión mínima, que es la causa raíz. Como Jenkins ejecuta
pytest en la etapa 3, la comprobación queda en el CI.

---

## Bloque 4 — Presupuesto de memoria de Spark

Tres correcciones encadenadas sobre el mismo problema, que ilustran cómo una
corrección incompleta puede empeorar las cosas.

### 17. `spark.executor.memoryOverhead` no contabilizado

**Síntoma.** El worker llegaba al 99 % de su límite (medido: **847 de 850 MiB**) y
el trabajo moría con `exit 137`. Ningún log de Spark mencionaba la memoria.

**Causa.** El proceso del executor no consume `spark.executor.memory` (el heap),
consume heap **+** `memoryOverhead` (off-heap, metaspace, buffers de red). El
overhead por defecto es `max(384 MB, 10 % del heap)`, así que un executor
configurado con 700m pedía en realidad **~1084 MB** dentro de un contenedor
limitado a 850M. Es el error silencioso más fácil de cometer al limitar la memoria
de un contenedor de Spark.

### 18. La sobrecorrección: heap insuficiente

**Síntoma.** `ExecutorLostFailure (executor 3 exited …) Reason: Command exited
with code 52`.

**Por qué engañaba.** El 52 no aparece documentado en el mensaje: es el código con
el que la JVM sale ante un `OutOfMemoryError`.

**Causa.** Al bajar el heap de 700m a 512m para que cupiera el overhead, el heap
quedó por debajo de lo que necesita una partición de lectura.

### 19. El tamaño de partición era la palanca real

**El aprendizaje.** Dar más memoria no era el arreglo correcto. El executor no
falla por el volumen **total** de la colección, falla por el tamaño de **una**
partición. El conector de MongoDB usa 64 MB por partición por defecto, y
materializar eso más la conversión de esquema no cabía en el heap.

**Corrección, en orden de importancia:**

1. `MONGO_READ_PARTITION_MB=32` (antes 64). Duplica el número de particiones —de 9
   a 21— y reduce a la mitad el pico por tarea **sin pedir más memoria al
   sistema**.
2. Heap del executor a 640m. Dentro del contenedor del worker conviven **dos** JVM:
   el demonio Worker (~200 MB) y el proceso del Executor. 200 + 640 + 256 =
   1096 MB, que cabe en el límite de 1200M.

**Presupuesto final de la fase de Spark, explícito:**

```
mongo 900 + api 280 + master 400 + worker 1200 + driver 700 = 3480 MB de 3830
```

**Resultado:** el worker pasó de 847/850 MiB (99 %) a **332 MiB / 1,07 GiB (30 %)**
y las cinco agregaciones completaron en 186,5 s.

---

## Bloque 5 — Defectos de método en el propio benchmark

### 20. Los dos motores no leían los mismos campos

**Síntoma.** Spark salía 6–9× más lento que Dask, una diferencia demasiado grande
para atribuirla al motor.

**Causa.** El lado Dask proyectaba 3 campos en su `find()`; el lado Spark leía los
19 de `ACCIDENT_SCHEMA`. No se comparaban motores, se comparaban volúmenes de
lectura distintos.

**Corrección.** `GRID_SCHEMA` con `lat`, `lon`, `severity`, y `read_accidents()`
acepta un esquema explícito.

**Y el resultado que descarta la hipótesis.** Con los mismos tres campos, Spark
pasó de 62,20 s a **62,48 s**. **La proyección no era la causa del hueco.** Se
documenta porque una hipótesis descartada con una medición vale más que una
explicación plausible sin comprobar.

Lo que sí cambió al igualar la lectura fue la **escalabilidad** de Spark: de
0,843× (empeoraba al añadir el segundo núcleo) a 1,439×. Leyendo 19 campos, la
presión de memoria hacía que dos tareas concurrentes en el mismo executor se
estorbaran.

### 21. La muestra estaba sesgada

Tratado en el informe técnico, §2.2. Resumen: leer particiones en orden dejaba la
muestra con solo 4 de los 8 años del dataset. Al barajar los índices con semilla
fija, los 8 años quedan cubiertos y `agg_temporal` pasa de 39 a 51 documentos
(24 horas + 7 días + 12 meses + 8 años).

---

## Errores de operación, no del sistema

Se anotan porque son fáciles de repetir.

**Editar `bootstrap.sh` mientras se está ejecutando.** Bash lee el script por
desplazamientos de byte, así que una edición en caliente desplaza el resto y
produce errores engañosos: `$1: unbound variable` señalando una línea que solo
asigna una cadena. No era un fallo del script. Para validarlo hay que dejarlo
quieto.

**`comando > log 2>&1; echo "EXIT=$?"` enmascara el fallo.** El `$?` que se
imprime es el del `echo`, no el del comando. Y con `comando | tail`, el estado de
salida es el de `tail`. Por eso una build de Spark que había fallado se reportó
como correcta.

---

## Resumen

| Bloque | Defectos | Qué tenían en común |
|---|---:|---|
| Construcción de imágenes | 4 | supuestos sobre la imagen base sin verificar |
| Ejecución del pipeline | 8 | límites de memoria y nombres de red |
| Pruebas de integración | 4 | una fixture mal definida ocultaba el resto |
| Presupuesto de Spark | 3 | el overhead de la JVM no es opcional |
| Método del benchmark | 2 | comparar cosas que no eran comparables |

**El patrón.** Trece de los veintiún defectos daban un mensaje de error que **no
señalaba la causa**: `exit 127` por un binario ausente que parecía un módulo
ausente; `KilledWorker` y `exit 137` sin mencionar la memoria; `exit 52` sin decir
`OutOfMemoryError`; `unhealthy` con el servidor sano; un 500 en `$near` cuando
fallaba el conteo; un `ImportError` con el linter en verde.

La conclusión operativa es que ninguno se habría encontrado revisando el código.
Hicieron falta la ejecución real, medir la memoria de cada contenedor y leer los
logs del servicio y no solo el del cliente.
