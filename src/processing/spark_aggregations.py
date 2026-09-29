"""Procesamiento distribuido con Spark sobre los datos que viven en MongoDB.

Requisito 4.2 del enunciado: leer desde MongoDB con el MongoDB Spark Connector,
calcular agregaciones espaciales y temporales (conteos por celda de grilla y por
geohash, zonas de alta concentracion, comportamiento por hora / dia / mes) y
guardar los resultados en colecciones NUEVAS.

Colecciones que produce:
    agg_grid      conteo y severidad media por celda de grilla (centroide GeoJSON)
    agg_geohash   conteo por celda de geohash (centroide GeoJSON)
    agg_hotspots  top-N celdas por indice de concentracion ponderado
    agg_temporal  conteos por hora, dia de la semana, mes y ano
    agg_state     conteo y severidad media por estado
"""
from __future__ import annotations

import argparse
import sys
import time
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.common import config
from src.common.logging_conf import setup_logging

log = setup_logging("processing.spark")

# Version del conector alineada con Spark 3.5 / Scala 2.12
MONGO_SPARK_PACKAGE = "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0"

# Esquema explicito. Dejar que el conector lo infiera por muestreo es lento
# sobre millones de documentos y produce tipos inestables cuando hay nulos.
ACCIDENT_SCHEMA = StructType([
    StructField("accident_id", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("severity", IntegerType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("city", StringType(), True),
    StructField("county", StringType(), True),
    StructField("state", StringType(), True),
    StructField("distance_mi", DoubleType(), True),
    StructField("temperature_f", DoubleType(), True),
    StructField("visibility_mi", DoubleType(), True),
    StructField("weather", StringType(), True),
    StructField("day_night", StringType(), True),
    StructField("geohash", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("hour", IntegerType(), True),
    StructField("dow", IntegerType(), True),
    StructField("is_weekend", BooleanType(), True),
])

DOW_NAMES = ["Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado", "Domingo"]
MONTH_NAMES = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio",
               "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]


# ---------------------------------------------------------------------------
# Sesion de Spark
# ---------------------------------------------------------------------------
def build_spark(app_name: str = "GeoBigData-Aggregations",
                master: str | None = None,
                executor_memory: str | None = None,
                driver_memory: str | None = None,
                shuffle_partitions: int = 16,
                extra: dict[str, str] | None = None) -> SparkSession:
    """Crea la SparkSession con el MongoDB Spark Connector configurado."""
    master = master or config.spark.master
    uri = config.mongo.uri

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        # URIs que usan read/write del conector 10.x
        .config("spark.mongodb.read.connection.uri", uri)
        .config("spark.mongodb.write.connection.uri", uri)
        .config("spark.mongodb.read.database", config.mongo.database)
        .config("spark.mongodb.write.database", config.mongo.database)
        .config("spark.executor.memory", executor_memory or config.spark.executor_memory)
        .config("spark.driver.memory", driver_memory or config.spark.driver_memory)
        # 200 particiones de shuffle (default) es absurdo para un cluster de 2
        # workers: genera miles de tareas diminutas y domina el tiempo total.
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", _driver_host())
        # Si los jars no estan en la imagen, Ivy los resuelve en tiempo de envio
        .config("spark.jars.packages", MONGO_SPARK_PACKAGE)
        .config("spark.jars.ivy", "/tmp/.ivy2")
    )

    for key, value in (extra or {}).items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession activa | master=%s | version=%s", master, spark.version)
    return spark


def _driver_host() -> str:
    """Hostname que los executors usan para devolver resultados al driver.

    Dentro de Docker el driver debe anunciarse con su nombre de servicio, no con
    127.0.0.1, o los executors no logran conectarse de vuelta.
    """
    import os
    import socket

    explicit = os.environ.get("SPARK_DRIVER_HOST", "").strip()
    if explicit:
        return explicit
    try:
        return socket.gethostbyname(socket.gethostname())
    except Exception:  # noqa: BLE001
        return "127.0.0.1"


# ---------------------------------------------------------------------------
# Lectura / escritura
# ---------------------------------------------------------------------------
def read_accidents(spark: SparkSession, collection: str | None = None,
                   use_schema: bool = True) -> DataFrame:
    """Lee la coleccion principal desde MongoDB con el conector oficial."""
    reader = (spark.read.format("mongodb")
              .option("database", config.mongo.database)
              .option("collection", collection or config.mongo.collection))
    if use_schema:
        reader = reader.schema(ACCIDENT_SCHEMA)
    df = reader.load()
    log.info("Leida la coleccion '%s' desde MongoDB",
             collection or config.mongo.collection)
    return df


def write_collection(df: DataFrame, collection: str,
                     mode: str = "overwrite") -> int:
    """Escribe un DataFrame en una coleccion NUEVA de MongoDB."""
    n = df.count()
    (df.write.format("mongodb")
       .mode(mode)
       .option("database", config.mongo.database)
       .option("collection", collection)
       .save())
    log.info("Escritos %s documentos en la coleccion '%s'", f"{n:,}", collection)
    return n


def geojson_point(lon_col, lat_col):
    """Construye un subdocumento GeoJSON Point valido dentro de Spark SQL."""
    return F.struct(
        F.lit("Point").alias("type"),
        F.array(lon_col.cast("double"), lat_col.cast("double")).alias("coordinates"),
    )


# ---------------------------------------------------------------------------
# Agregacion espacial 1: grilla regular
# ---------------------------------------------------------------------------
def aggregate_grid(df: DataFrame, cell_deg: float | None = None) -> DataFrame:
    """Conteo por celda de una grilla regular en grados.

    Esta es la operacion pesada que se compara despues contra Dask: implica un
    shuffle sobre todos los registros agrupados por decenas de miles de claves.
    """
    cell = cell_deg if cell_deg is not None else config.geo.grid_cell_deg

    binned = (df
              .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
              .withColumn("grid_lat", F.floor(F.col("lat") / F.lit(cell)) * F.lit(cell))
              .withColumn("grid_lon", F.floor(F.col("lon") / F.lit(cell)) * F.lit(cell)))

    agg = (binned
           .groupBy("grid_lat", "grid_lon")
           .agg(
               F.count(F.lit(1)).alias("count"),
               F.avg("severity").alias("avg_severity"),
               F.max("severity").alias("max_severity"),
               F.avg("distance_mi").alias("avg_distance_mi"),
               F.approx_count_distinct("city").alias("distinct_cities"),
               F.min("start_time").alias("first_event"),
               F.max("start_time").alias("last_event"),
               F.first("state", ignorenulls=True).alias("sample_state"),
           ))

    result = (agg
              .withColumn("grid_lat", F.round("grid_lat", 4))
              .withColumn("grid_lon", F.round("grid_lon", 4))
              .withColumn("grid_id", F.concat_ws(
                  "_", F.format_number(F.col("grid_lat"), 4),
                  F.format_number(F.col("grid_lon"), 4)))
              # Centro geometrico de la celda, como GeoJSON indexable
              .withColumn("centroid", geojson_point(
                  F.col("grid_lon") + F.lit(cell / 2),
                  F.col("grid_lat") + F.lit(cell / 2)))
              .withColumn("avg_severity", F.round("avg_severity", 4))
              .withColumn("avg_distance_mi", F.round("avg_distance_mi", 4))
              .withColumn("cell_deg", F.lit(cell))
              .orderBy(F.desc("count")))

    return result


# ---------------------------------------------------------------------------
# Agregacion espacial 2: geohash
# ---------------------------------------------------------------------------
def aggregate_geohash(df: DataFrame, precision: int | None = None) -> DataFrame:
    """Conteo por celda de geohash.

    Si la ingesta ya calculo la columna `geohash` con la precision pedida se
    reutiliza; en caso contrario se aplica una UDF. Reutilizar evita pagar el
    coste de serializacion Python<->JVM sobre millones de filas.
    """
    prec = precision if precision is not None else config.geo.geohash_precision
    has_column = "geohash" in df.columns

    if has_column:
        source = df.filter(F.col("geohash").isNotNull())
        # Recortar es valido: el geohash es un prefijo jerarquico
        source = source.withColumn("gh", F.substring(F.col("geohash"), 1, prec))
        log.info("Se reutiliza la columna 'geohash' precalculada (prefijo %s)", prec)
    else:
        from pyspark.sql.functions import udf  # noqa: PLC0415

        from src.common.geo import geohash_encode  # noqa: PLC0415

        @udf(StringType())
        def _gh(lat, lon):
            if lat is None or lon is None:
                return None
            try:
                return geohash_encode(float(lat), float(lon), prec)
            except ValueError:
                return None

        source = (df.filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
                    .withColumn("gh", _gh(F.col("lat"), F.col("lon"))))
        log.info("Geohash calculado con UDF de Python (precision %s)", prec)

    agg = (source
           .filter(F.col("gh").isNotNull())
           .groupBy("gh")
           .agg(
               F.count(F.lit(1)).alias("count"),
               F.avg("severity").alias("avg_severity"),
               F.avg("lat").alias("mean_lat"),
               F.avg("lon").alias("mean_lon"),
               F.approx_count_distinct("city").alias("distinct_cities"),
           ))

    return (agg
            .withColumnRenamed("gh", "geohash")
            .withColumn("precision", F.lit(prec))
            .withColumn("avg_severity", F.round("avg_severity", 4))
            # El centroide se toma como la media de los puntos reales de la
            # celda, que representa mejor la concentracion que el centro
            # geometrico del geohash.
            .withColumn("centroid", geojson_point(F.col("mean_lon"), F.col("mean_lat")))
            .withColumn("mean_lat", F.round("mean_lat", 6))
            .withColumn("mean_lon", F.round("mean_lon", 6))
            .orderBy(F.desc("count")))


# ---------------------------------------------------------------------------
# Agregacion espacial 3: zonas de alta concentracion
# ---------------------------------------------------------------------------
def aggregate_hotspots(grid_df: DataFrame, top_n: int = 200,
                       min_count: int = 10) -> DataFrame:
    """Top-N celdas por indice de concentracion ponderado por severidad.

    El indice es `count * avg_severity`: una celda con muchos accidentes leves
    no es igual de critica que una con la mitad de accidentes pero graves, y el
    conteo puro esconde esa diferencia.
    """
    from pyspark.sql.window import Window  # noqa: PLC0415

    scored = (grid_df
              .filter(F.col("count") >= F.lit(min_count))
              .withColumn("concentration_index",
                          F.round(F.col("count") * F.col("avg_severity"), 4)))

    total = scored.agg(F.sum("count").alias("t")).collect()[0]["t"] or 1

    ranked = (scored
              .withColumn("rank", F.row_number().over(
                  Window.orderBy(F.desc("concentration_index"), F.desc("count"))))
              .withColumn("pct_of_total",
                          F.round(100.0 * F.col("count") / F.lit(float(total)), 6))
              .filter(F.col("rank") <= F.lit(top_n)))

    return ranked.select(
        "rank", "grid_id", "grid_lat", "grid_lon", "centroid", "count",
        "avg_severity", "max_severity", "concentration_index", "pct_of_total",
        "distinct_cities", "sample_state", "cell_deg",
    ).orderBy("rank")


# ---------------------------------------------------------------------------
# Agregacion temporal
# ---------------------------------------------------------------------------
def aggregate_temporal(df: DataFrame) -> DataFrame:
    """Comportamiento por hora del dia, dia de la semana, mes y ano.

    Todo se devuelve en una sola coleccion con la forma
    (dimension, bucket, label, count, avg_severity), que es mucho mas facil de
    consumir desde un unico endpoint que cuatro colecciones separadas.
    """
    base = df.filter(F.col("start_time").isNotNull())

    # Se recalculan desde start_time en lugar de confiar en las columnas
    # derivadas: asi la agregacion es correcta aunque se ejecute sobre una
    # coleccion cargada por otra via.
    base = (base
            .withColumn("_hour", F.hour("start_time"))
            .withColumn("_month", F.month("start_time"))
            .withColumn("_year", F.year("start_time"))
            # dayofweek de Spark: 1=domingo ... 7=sabado -> se pasa a 0=lunes
            .withColumn("_dow", F.pmod(F.dayofweek("start_time") + F.lit(5), F.lit(7))))

    def _bucket(col_name: str, dimension: str) -> DataFrame:
        return (base
                .groupBy(F.col(col_name).alias("bucket"))
                .agg(F.count(F.lit(1)).alias("count"),
                     F.avg("severity").alias("avg_severity"),
                     F.avg("distance_mi").alias("avg_distance_mi"))
                .withColumn("dimension", F.lit(dimension))
                .withColumn("avg_severity", F.round("avg_severity", 4))
                .withColumn("avg_distance_mi", F.round("avg_distance_mi", 4)))

    hours = _bucket("_hour", "hour").withColumn(
        "label", F.concat(F.lpad(F.col("bucket").cast("string"), 2, "0"), F.lit(":00")))

    dow_map = F.create_map(*[x for i, n in enumerate(DOW_NAMES)
                            for x in (F.lit(i), F.lit(n))])
    dows = _bucket("_dow", "dow").withColumn("label", dow_map[F.col("bucket")])

    month_map = F.create_map(*[x for i, n in enumerate(MONTH_NAMES, start=1)
                               for x in (F.lit(i), F.lit(n))])
    months = _bucket("_month", "month").withColumn("label", month_map[F.col("bucket")])

    years = _bucket("_year", "year").withColumn("label", F.col("bucket").cast("string"))

    cols = ["dimension", "bucket", "label", "count", "avg_severity", "avg_distance_mi"]
    return (hours.select(*cols)
            .unionByName(dows.select(*cols))
            .unionByName(months.select(*cols))
            .unionByName(years.select(*cols))
            .orderBy("dimension", "bucket"))


# ---------------------------------------------------------------------------
# Agregacion por estado
# ---------------------------------------------------------------------------
def aggregate_state(df: DataFrame) -> DataFrame:
    """Resumen por estado, con centroide medio de los eventos."""
    return (df
            .filter(F.col("state").isNotNull() & (F.length("state") > 0))
            .groupBy("state")
            .agg(F.count(F.lit(1)).alias("count"),
                 F.avg("severity").alias("avg_severity"),
                 F.sum(F.when(F.col("severity") >= 3, 1).otherwise(0)).alias("severe_count"),
                 F.avg("lat").alias("mean_lat"),
                 F.avg("lon").alias("mean_lon"),
                 F.approx_count_distinct("city").alias("distinct_cities"))
            .withColumn("avg_severity", F.round("avg_severity", 4))
            .withColumn("severe_pct",
                        F.round(100.0 * F.col("severe_count") / F.col("count"), 4))
            .withColumn("centroid", geojson_point(F.col("mean_lon"), F.col("mean_lat")))
            .withColumn("mean_lat", F.round("mean_lat", 6))
            .withColumn("mean_lon", F.round("mean_lon", 6))
            .orderBy(F.desc("count")))


# ---------------------------------------------------------------------------
# Orquestacion
# ---------------------------------------------------------------------------
def run_all(cell_deg: float | None = None, precision: int | None = None,
            top_n: int = 200, shuffle_partitions: int = 16,
            master: str | None = None) -> dict[str, Any]:
    """Ejecuta todas las agregaciones y devuelve un resumen con tiempos."""
    spark = build_spark(master=master, shuffle_partitions=shuffle_partitions)
    summary: dict[str, Any] = {"stages": {}}
    t_total = time.perf_counter()

    try:
        df = read_accidents(spark)
        # Se cachea porque las cinco agregaciones recorren el mismo DataFrame;
        # sin cache se leeria MongoDB cinco veces.
        df = df.cache()
        total_rows = df.count()
        log.info("Registros leidos desde MongoDB: %s", f"{total_rows:,}")
        summary["input_rows"] = total_rows

        if total_rows == 0:
            raise RuntimeError(
                "La coleccion principal esta vacia. Ejecute primero la ingesta "
                "con Dask (make ingest)."
            )

        # --- grilla --------------------------------------------------------
        t0 = time.perf_counter()
        grid = aggregate_grid(df, cell_deg).cache()
        n_grid = write_collection(grid, config.mongo.grid_collection)
        summary["stages"]["agg_grid"] = {
            "documents": n_grid, "seconds": round(time.perf_counter() - t0, 3)}

        # --- hotspots (derivadas de la grilla) -----------------------------
        t0 = time.perf_counter()
        hotspots = aggregate_hotspots(grid, top_n=top_n)
        n_hot = write_collection(hotspots, config.mongo.hotspot_collection)
        summary["stages"]["agg_hotspots"] = {
            "documents": n_hot, "seconds": round(time.perf_counter() - t0, 3)}
        grid.unpersist()

        # --- geohash -------------------------------------------------------
        t0 = time.perf_counter()
        gh = aggregate_geohash(df, precision)
        n_gh = write_collection(gh, config.mongo.geohash_collection)
        summary["stages"]["agg_geohash"] = {
            "documents": n_gh, "seconds": round(time.perf_counter() - t0, 3)}

        # --- temporal ------------------------------------------------------
        t0 = time.perf_counter()
        temporal = aggregate_temporal(df)
        n_tmp = write_collection(temporal, config.mongo.temporal_collection)
        summary["stages"]["agg_temporal"] = {
            "documents": n_tmp, "seconds": round(time.perf_counter() - t0, 3)}

        # --- por estado ----------------------------------------------------
        t0 = time.perf_counter()
        states = aggregate_state(df)
        n_st = write_collection(states, config.mongo.state_collection)
        summary["stages"]["agg_state"] = {
            "documents": n_st, "seconds": round(time.perf_counter() - t0, 3)}

        df.unpersist()

        # Los indices 2dsphere de las colecciones nuevas los crea pymongo: el
        # conector de Spark no gestiona indices.
        from src.common.mongo import ensure_indexes  # noqa: PLC0415

        ensure_indexes()
        log.info("Indices 2dsphere creados sobre las colecciones agregadas")

        summary["total_seconds"] = round(time.perf_counter() - t_total, 3)
        summary["spark_version"] = spark.version
        # getExecutorInfos() incluye al driver; se descuenta para reportar
        # solo los executors reales del cluster.
        summary["executors"] = len(list(
            spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos())) - 1

        log.info("=" * 72)
        log.info("  AGREGACIONES COMPLETADAS EN %.1f s", summary["total_seconds"])
        for name, info in summary["stages"].items():
            log.info("  %-14s %8s documentos  %7.2f s",
                     name, f"{info['documents']:,}", info["seconds"])
        log.info("=" * 72)
        return summary

    finally:
        spark.stop()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Agregaciones espaciales y temporales con Spark")
    ap.add_argument("--cell-deg", type=float, default=None,
                    help="Tamano de celda de la grilla en grados")
    ap.add_argument("--geohash-precision", type=int, default=None)
    ap.add_argument("--top-n", type=int, default=200)
    ap.add_argument("--shuffle-partitions", type=int, default=16)
    ap.add_argument("--master", default=None)
    ap.add_argument("--summary-out", default=None)
    args = ap.parse_args(argv)

    summary = run_all(cell_deg=args.cell_deg, precision=args.geohash_precision,
                      top_n=args.top_n,
                      shuffle_partitions=args.shuffle_partitions,
                      master=args.master)

    if args.summary_out:
        import json
        from pathlib import Path

        Path(args.summary_out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.summary_out).write_text(json.dumps(summary, indent=2),
                                          encoding="utf-8")
        log.info("Resumen escrito en %s", args.summary_out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
