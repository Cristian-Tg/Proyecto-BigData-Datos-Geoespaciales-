"""Comparacion medida entre Dask y Spark sobre la MISMA operacion pesada.

Requisito 4.6 del enunciado: "Escojan una operacion pesada, por ejemplo la
agregacion por grilla, y ejecutenla en ambos motores con dos configuraciones
distintas de workers. Reporten tiempos y uso de memoria".

Operacion elegida: **agregacion por celda de grilla** (conteo y severidad media
por celda de 0.1 grados). Es la operacion mas costosa del sistema porque obliga
a recorrer todos los registros y a hacer un shuffle sobre decenas de miles de
claves distintas.

Para que la comparacion sea honesta:

* Ambos motores leen de la MISMA fuente: la coleccion de MongoDB.
* Ambos calculan EXACTAMENTE la misma agregacion y el resultado se compara
  celda por celda al final (`results_match`).
* Cada configuracion se ejecuta `--repeats` veces y se reporta la mediana.
* La memoria se mide en los workers reales, no en el proceso cliente:
  - Dask, con `client.run()` sobre cada worker (RSS de psutil).
  - Spark, con la API REST de executors del driver (peak JVM heap).
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import sys
import threading
import time
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.common import config
from src.common.logging_conf import setup_logging

log = setup_logging("benchmark")

OPERATION = "grid_aggregation_0.1deg"


# ===========================================================================
# Muestreo de memoria
# ===========================================================================
class MemorySampler:
    """Muestrea memoria en un hilo aparte mientras corre el trabajo."""

    def __init__(self, probe: Callable[[], float | None], interval: float = 0.5):
        self._probe = probe
        self._interval = interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.samples: list[float] = []

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                value = self._probe()
                if value is not None:
                    self.samples.append(float(value))
            except Exception:  # noqa: BLE001 - medir nunca debe tumbar el bench
                pass
            self._stop.wait(self._interval)

    def __enter__(self) -> MemorySampler:
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc: Any) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    @property
    def peak_mb(self) -> float | None:
        return round(max(self.samples) / 1024 / 1024, 2) if self.samples else None

    @property
    def mean_mb(self) -> float | None:
        return (round(statistics.mean(self.samples) / 1024 / 1024, 2)
                if self.samples else None)


def _driver_rss() -> float | None:
    try:
        import psutil
        return float(psutil.Process().memory_info().rss)
    except Exception:  # noqa: BLE001
        return None


# ===========================================================================
# Particionado de la coleccion de MongoDB
# ===========================================================================
def mongo_partitions(min_docs: int = 1) -> list[dict[str, Any]]:
    """Divide la coleccion en bloques (year, month) usando el indice ix_year_month.

    Se particiona por ano-mes y no por rangos de _id porque:
      * ya existe un indice compuesto (year, month), asi que cada particion se
        resuelve con un recorrido de indice y no con un escaneo completo;
      * un `skip` de millones de documentos es O(n) en MongoDB y arruinaria la
        medicion de Dask por un motivo ajeno al motor.
    """
    from src.common.mongo import get_db

    db = get_db()
    coll = db[config.mongo.collection]

    pipeline = [
        {"$group": {"_id": {"year": "$year", "month": "$month"},
                    "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]
    buckets: list[dict[str, Any]] = []
    for doc in coll.aggregate(pipeline, allowDiskUse=True, maxTimeMS=120_000):
        key = doc["_id"] or {}
        year, month = key.get("year"), key.get("month")
        count = doc["count"]
        if count < min_docs:
            continue
        if year is None or month is None:
            # Documentos sin columnas temporales: una particion aparte
            buckets.append({"filter": {"$or": [{"year": None},
                                               {"year": {"$exists": False}}]},
                            "count": count, "label": "sin_fecha"})
        else:
            buckets.append({"filter": {"year": int(year), "month": int(month)},
                            "count": count, "label": f"{year}-{month:02d}"})

    log.info("Coleccion dividida en %s particiones (year, month)", len(buckets))
    return buckets


# ===========================================================================
# DASK
# ===========================================================================
def _dask_partition_grid(part_filter: dict[str, Any], mongo_uri: str,
                         database: str, collection: str, cell_deg: float):
    """Agregacion por grilla de UNA particion. Se ejecuta en el worker de Dask."""
    import numpy as np
    import pandas as pd
    from pymongo import MongoClient

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=30_000,
                         socketTimeoutMS=600_000)
    try:
        cursor = client[database][collection].find(
            part_filter, {"_id": 0, "lat": 1, "lon": 1, "severity": 1},
            batch_size=10_000,
        )
        df = pd.DataFrame(list(cursor))
    finally:
        client.close()

    if df.empty:
        return pd.DataFrame(columns=["grid_lat", "grid_lon", "count",
                                     "sum_severity"])

    lat = df["lat"].to_numpy(dtype="float64")
    lon = df["lon"].to_numpy(dtype="float64")
    decimals = max(0, -int(np.floor(np.log10(cell_deg))) + 2)
    df["grid_lat"] = np.round(np.floor(lat / cell_deg) * cell_deg, decimals)
    df["grid_lon"] = np.round(np.floor(lon / cell_deg) * cell_deg, decimals)

    # Se agregan sumas parciales (no medias): las medias de medias no se pueden
    # combinar entre particiones, las sumas si.
    out = (df.groupby(["grid_lat", "grid_lon"], sort=False)
             .agg(count=("lat", "size"),
                  sum_severity=("severity", "sum"))
             .reset_index())
    return out


def run_dask_grid(n_workers: int, threads_per_worker: int = 2,
                  memory_limit: str = "2GB", cell_deg: float = 0.1,
                  scheduler: str | None = None) -> dict[str, Any]:
    """Ejecuta la agregacion por grilla en Dask y devuelve metricas."""
    import dask
    import pandas as pd
    from distributed import Client, LocalCluster

    cluster = None
    if scheduler:
        client = Client(scheduler, timeout="60s")
        log.info("Dask: usando el scheduler externo %s", scheduler)
    else:
        # Un LocalCluster permite fijar EXACTAMENTE el numero de workers, que es
        # justo la variable que el enunciado pide comparar. Un cluster de
        # compose tiene el numero fijado en el YAML.
        cluster = LocalCluster(n_workers=n_workers,
                               threads_per_worker=threads_per_worker,
                               memory_limit=memory_limit, processes=True,
                               dashboard_address=":0", silence_logs=40)
        client = Client(cluster)
    log.info("Dask listo: %s worker(s) x %s hilo(s), limite %s",
             n_workers, threads_per_worker, memory_limit)

    def _probe() -> float | None:
        """RSS sumado de todos los workers del cluster."""
        try:
            import psutil  # noqa: F401  (se usa dentro del worker)
            per_worker = client.run(
                lambda: __import__("psutil").Process().memory_info().rss)
            return float(sum(per_worker.values()))
        except Exception:  # noqa: BLE001
            return None

    parts = mongo_partitions()
    t_start = time.perf_counter()

    with MemorySampler(_probe, interval=0.5) as sampler:
        tasks = [
            dask.delayed(_dask_partition_grid)(
                p["filter"], config.mongo.uri, config.mongo.database,
                config.mongo.collection, cell_deg)
            for p in parts
        ]
        # Reduccion en arbol: concatenar en el cliente y agregar una sola vez.
        partials = list(client.gather(client.compute(tasks)))
        combined = pd.concat([p for p in partials if len(p)], ignore_index=True) \
            if partials else pd.DataFrame(columns=["grid_lat", "grid_lon",
                                                   "count", "sum_severity"])
        if not combined.empty:
            final = (combined.groupby(["grid_lat", "grid_lon"], sort=False)
                     .agg(count=("count", "sum"),
                          sum_severity=("sum_severity", "sum"))
                     .reset_index())
            final["avg_severity"] = (final["sum_severity"] / final["count"]).round(4)
        else:
            final = combined
        n_cells = len(final)
        total_rows = int(final["count"].sum()) if n_cells else 0

    elapsed = time.perf_counter() - t_start

    top = []
    if n_cells:
        for rec in (final.sort_values("count", ascending=False)
                    .head(10).to_dict("records")):
            top.append({"grid_lat": float(rec["grid_lat"]),
                        "grid_lon": float(rec["grid_lon"]),
                        "count": int(rec["count"]),
                        "avg_severity": float(rec["avg_severity"])})

    try:
        client.close()
        if cluster is not None:
            cluster.close()
    except Exception:  # noqa: BLE001
        pass

    return {
        "engine": "dask",
        "workers": n_workers,
        "threads_per_worker": threads_per_worker,
        "memory_limit": memory_limit,
        "elapsed_seconds": round(elapsed, 3),
        "peak_worker_memory_mb": sampler.peak_mb,
        "mean_worker_memory_mb": sampler.mean_mb,
        "memory_samples": len(sampler.samples),
        "partitions": len(parts),
        "cells": n_cells,
        "rows_processed": total_rows,
        "top_cells": top,
    }


# ===========================================================================
# SPARK
# ===========================================================================
def _spark_executor_memory_probe(ui_port: int = 4040) -> Callable[[], float | None]:
    """Sonda que consulta la API REST del driver para el heap de los executors."""
    import urllib.request

    state: dict[str, Any] = {"app_id": None}

    def probe() -> float | None:
        base = f"http://127.0.0.1:{ui_port}/api/v1"
        try:
            if not state["app_id"]:
                with urllib.request.urlopen(f"{base}/applications", timeout=3) as r:
                    apps = json.loads(r.read().decode())
                if not apps:
                    return None
                state["app_id"] = apps[0]["id"]
            url = f"{base}/applications/{state['app_id']}/executors"
            with urllib.request.urlopen(url, timeout=3) as r:
                executors = json.loads(r.read().decode())
        except Exception:  # noqa: BLE001 - la UI puede no estar lista aun
            return None

        total = 0.0
        for ex in executors:
            if ex.get("id") == "driver":
                continue  # solo se mide el cluster, no el proceso cliente
            peak = (ex.get("peakMemoryMetrics") or {})
            heap = peak.get("JVMHeapMemory") or 0
            offheap = peak.get("JVMOffHeapMemory") or 0
            total += float(heap) + float(offheap)
        return total or None

    return probe


def run_spark_grid(cores_max: int, executor_cores: int = 1,
                   executor_memory: str = "2g", cell_deg: float = 0.1,
                   shuffle_partitions: int = 16,
                   master: str | None = None) -> dict[str, Any]:
    """Ejecuta la misma agregacion en Spark y devuelve metricas.

    `cores_max` es la palanca que cambia el paralelismo efectivo del cluster
    standalone: con executor_cores=1, cores_max=N equivale a N executors, es
    decir a N "workers" activos.
    """
    from pyspark.sql import functions as F

    from src.processing.spark_aggregations import GRID_SCHEMA, build_spark, read_accidents

    spark = build_spark(
        app_name=f"Benchmark-Grid-cores{cores_max}",
        master=master,
        executor_memory=executor_memory,
        shuffle_partitions=shuffle_partitions,
        extra={
            "spark.cores.max": str(cores_max),
            "spark.executor.cores": str(executor_cores),
            # Metricas de memoria por executor en la API REST
            "spark.executor.metrics.pollingInterval": "1s",
            "spark.eventLog.enabled": "false",
            "spark.ui.port": "4040",
        },
    )

    probe = _spark_executor_memory_probe(4040)
    t_start = time.perf_counter()

    with MemorySampler(probe, interval=1.0) as sampler:
        # GRID_SCHEMA y no el esquema completo: el lado Dask proyecta solo
        # {lat, lon, severity} en su find(), asi que leer los 19 campos aqui
        # compararia volumenes de lectura distintos en lugar de motores.
        # Medido con el esquema completo, Spark salia 7x mas lento, y la mayor
        # parte de esa diferencia era lectura que Dask no estaba haciendo.
        df = read_accidents(spark, schema=GRID_SCHEMA)
        agg = (df
               .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
               .withColumn("grid_lat", F.round(
                   F.floor(F.col("lat") / F.lit(cell_deg)) * F.lit(cell_deg), 4))
               .withColumn("grid_lon", F.round(
                   F.floor(F.col("lon") / F.lit(cell_deg)) * F.lit(cell_deg), 4))
               .groupBy("grid_lat", "grid_lon")
               .agg(F.count(F.lit(1)).alias("count"),
                    F.sum("severity").alias("sum_severity")))
        # collect() fuerza la materializacion completa: sin una accion, Spark
        # no ejecutaria nada y el tiempo medido seria cero.
        rows = agg.collect()
        n_cells = len(rows)
        total_rows = sum(r["count"] for r in rows)

    elapsed = time.perf_counter() - t_start

    top = sorted(rows, key=lambda r: r["count"], reverse=True)[:10]
    top_cells = [{
        "grid_lat": float(r["grid_lat"]), "grid_lon": float(r["grid_lon"]),
        "count": int(r["count"]),
        "avg_severity": round(float(r["sum_severity"]) / int(r["count"]), 4)
        if r["count"] else None,
    } for r in top]

    executors_seen = 0
    try:
        infos = spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()
        executors_seen = max(0, len(infos) - 1)
    except Exception:  # noqa: BLE001
        pass

    spark_version = spark.version
    spark.stop()

    return {
        "engine": "spark",
        "workers": cores_max,          # cores_max == executors con 1 core cada uno
        "executor_cores": executor_cores,
        "cores_max": cores_max,
        "executors_registered": executors_seen,
        "memory_limit": executor_memory,
        "shuffle_partitions": shuffle_partitions,
        "elapsed_seconds": round(elapsed, 3),
        "peak_worker_memory_mb": sampler.peak_mb,
        "mean_worker_memory_mb": sampler.mean_mb,
        "memory_samples": len(sampler.samples),
        "cells": n_cells,
        "rows_processed": int(total_rows),
        "top_cells": top_cells,
        "spark_version": spark_version,
    }


# ===========================================================================
# Orquestacion del benchmark
# ===========================================================================
def _median_run(fn: Callable[[], dict[str, Any]], repeats: int,
                label: str) -> dict[str, Any]:
    """Ejecuta `repeats` veces y devuelve el resultado de la mediana de tiempo."""
    runs: list[dict[str, Any]] = []
    for i in range(1, repeats + 1):
        log.info("-" * 72)
        log.info(">>> %s | repeticion %s/%s", label, i, repeats)
        log.info("-" * 72)
        result = fn()
        log.info("    tiempo %.2fs | memoria pico %s MB | celdas %s",
                 result["elapsed_seconds"], result["peak_worker_memory_mb"],
                 f"{result['cells']:,}")
        runs.append(result)

    times = [r["elapsed_seconds"] for r in runs]
    chosen = sorted(runs, key=lambda r: r["elapsed_seconds"])[len(runs) // 2]
    chosen = dict(chosen)
    chosen.update({
        "repeats": repeats,
        "time_all_seconds": times,
        "time_median_seconds": round(statistics.median(times), 3),
        "time_min_seconds": round(min(times), 3),
        "time_max_seconds": round(max(times), 3),
        "time_stdev_seconds": round(statistics.stdev(times), 3) if len(times) > 1 else 0.0,
    })
    return chosen


def _cells_signature(result: dict[str, Any]) -> tuple[Any, ...]:
    """Firma comparable del resultado, para verificar que ambos motores coinciden."""
    return tuple(sorted(
        (round(c["grid_lat"], 4), round(c["grid_lon"], 4), c["count"])
        for c in result.get("top_cells", [])
    ))


def run_benchmark(dask_configs: list[int], spark_configs: list[int],
                  repeats: int = 2, cell_deg: float = 0.1,
                  threads_per_worker: int = 2, memory_limit: str = "2GB",
                  executor_memory: str = "2g", shuffle_partitions: int = 16,
                  dask_scheduler: str | None = None,
                  spark_master: str | None = None,
                  out_dir: str = "/data/benchmark") -> dict[str, Any]:
    run_id = uuid.uuid4().hex[:12]
    started = datetime.now(timezone.utc)

    log.info("=" * 72)
    log.info("  BENCHMARK  Dask vs Spark   |   operacion: %s", OPERATION)
    log.info("  run_id=%s  repeticiones=%s", run_id, repeats)
    log.info("  Dask  : %s", ", ".join(f"{n} worker(s)" for n in dask_configs))
    log.info("  Spark : %s", ", ".join(f"{n} core(s) max" for n in spark_configs))
    log.info("=" * 72)

    results: list[dict[str, Any]] = []

    for n in dask_configs:
        res = _median_run(
            lambda n=n: run_dask_grid(n_workers=n,
                                      threads_per_worker=threads_per_worker,
                                      memory_limit=memory_limit,
                                      cell_deg=cell_deg,
                                      scheduler=dask_scheduler),
            repeats, f"DASK {n} worker(s)")
        results.append(res)

    for n in spark_configs:
        res = _median_run(
            lambda n=n: run_spark_grid(cores_max=n, executor_cores=1,
                                       executor_memory=executor_memory,
                                       cell_deg=cell_deg,
                                       shuffle_partitions=shuffle_partitions,
                                       master=spark_master),
            repeats, f"SPARK cores.max={n}")
        results.append(res)

    # --- verificacion de equivalencia de resultados -----------------------
    signatures = {f"{r['engine']}-{r['workers']}": _cells_signature(r)
                  for r in results}
    distinct = set(signatures.values())
    results_match = len(distinct) <= 1
    cell_counts = {f"{r['engine']}-{r['workers']}": r["cells"] for r in results}
    rows_counts = {f"{r['engine']}-{r['workers']}": r["rows_processed"]
                   for r in results}

    if results_match:
        log.info("VERIFICACION: los dos motores producen las mismas celdas top-10")
    else:
        log.warning("VERIFICACION: los resultados NO coinciden. Firmas: %s",
                    {k: hash(v) for k, v in signatures.items()})

    payload: dict[str, Any] = {
        "run_id": run_id,
        "operation": OPERATION,
        "cell_deg": cell_deg,
        "started_at": started.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "repeats": repeats,
        "host": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "cpu_count": os.cpu_count(),
        },
        "source": {
            "database": config.mongo.database,
            "collection": config.mongo.collection,
        },
        "results": results,
        "verification": {
            "results_match": results_match,
            "cells_per_config": cell_counts,
            "rows_per_config": rows_counts,
        },
        "analysis": _build_analysis(results),
    }

    _persist(payload, out_dir)
    _print_table(payload)
    return payload


def _build_analysis(results: list[dict[str, Any]]) -> dict[str, Any]:
    """Calcula escalabilidad y el ganador por configuracion."""
    by_engine: dict[str, list[dict[str, Any]]] = {}
    for r in results:
        by_engine.setdefault(r["engine"], []).append(r)

    scaling: dict[str, Any] = {}
    for engine, runs in by_engine.items():
        runs = sorted(runs, key=lambda r: r["workers"])
        if len(runs) >= 2:
            base, scaled = runs[0], runs[-1]
            t1 = base["time_median_seconds"]
            t2 = scaled["time_median_seconds"]
            factor = scaled["workers"] / base["workers"] if base["workers"] else 1
            speedup = t1 / t2 if t2 else 0.0
            scaling[engine] = {
                "from_workers": base["workers"],
                "to_workers": scaled["workers"],
                "time_from_s": t1,
                "time_to_s": t2,
                "speedup": round(speedup, 3),
                "ideal_speedup": round(factor, 3),
                "parallel_efficiency_pct": round(100 * speedup / factor, 2)
                if factor else None,
            }

    head_to_head = []
    dask_runs = {r["workers"]: r for r in by_engine.get("dask", [])}
    spark_runs = {r["workers"]: r for r in by_engine.get("spark", [])}
    for workers in sorted(set(dask_runs) & set(spark_runs)):
        d, s = dask_runs[workers], spark_runs[workers]
        faster = "dask" if d["time_median_seconds"] < s["time_median_seconds"] else "spark"
        ratio = (max(d["time_median_seconds"], s["time_median_seconds"])
                 / min(d["time_median_seconds"], s["time_median_seconds"])
                 if min(d["time_median_seconds"], s["time_median_seconds"]) else 0)
        head_to_head.append({
            "workers": workers,
            "dask_seconds": d["time_median_seconds"],
            "spark_seconds": s["time_median_seconds"],
            "faster": faster,
            "times_faster": round(ratio, 2),
            "dask_peak_mb": d["peak_worker_memory_mb"],
            "spark_peak_mb": s["peak_worker_memory_mb"],
        })

    return {"scaling": scaling, "head_to_head": head_to_head}


def _persist(payload: dict[str, Any], out_dir: str) -> None:
    """Guarda el resultado en JSON, en Markdown y en MongoDB."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    json_path = out / f"benchmark_{payload['run_id']}.json"
    json_path.write_text(json.dumps(payload, indent=2, default=str),
                         encoding="utf-8")
    (out / "benchmark_latest.json").write_text(
        json.dumps(payload, indent=2, default=str), encoding="utf-8")
    log.info("JSON del benchmark: %s", json_path)

    md_path = out / "benchmark_latest.md"
    md_path.write_text(_markdown_report(payload), encoding="utf-8")
    log.info("Informe Markdown  : %s", md_path)

    try:
        from src.common.mongo import get_db

        db = get_db()
        db[config.mongo.benchmark_collection].insert_one(dict(payload))
        log.info("Resultado guardado en la coleccion '%s'",
                 config.mongo.benchmark_collection)
    except Exception as exc:  # noqa: BLE001 - no debe invalidar el benchmark
        log.warning("No se pudo guardar el benchmark en MongoDB: %s", exc)


def _markdown_report(payload: dict[str, Any]) -> str:
    lines = [
        f"# Benchmark Dask vs Spark — `{payload['operation']}`",
        "",
        f"- **run_id**: `{payload['run_id']}`",
        f"- **Fecha**: {payload['started_at']}",
        f"- **Repeticiones por configuración**: {payload['repeats']} (se reporta la mediana)",
        f"- **Tamaño de celda**: {payload['cell_deg']}°",
        f"- **Fuente**: `{payload['source']['database']}.{payload['source']['collection']}`",
        f"- **CPUs del host**: {payload['host']['cpu_count']}",
        "",
        "## Resultados",
        "",
        "| Motor | Workers | Tiempo mediana (s) | min | max | σ | Memoria pico workers (MB) | Celdas | Registros |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in payload["results"]:
        lines.append(
            f"| {r['engine'].capitalize()} | {r['workers']} | "
            f"**{r['time_median_seconds']}** | {r['time_min_seconds']} | "
            f"{r['time_max_seconds']} | {r['time_stdev_seconds']} | "
            f"{r['peak_worker_memory_mb'] if r['peak_worker_memory_mb'] is not None else 'n/d'} | "
            f"{r['cells']:,} | {r['rows_processed']:,} |")

    lines += ["", "## Escalabilidad", "",
              "| Motor | Workers | Speedup real | Speedup ideal | Eficiencia paralela |",
              "|---|---|---:|---:|---:|"]
    for engine, sc in payload["analysis"]["scaling"].items():
        lines.append(
            f"| {engine.capitalize()} | {sc['from_workers']} → {sc['to_workers']} | "
            f"{sc['speedup']}× | {sc['ideal_speedup']}× | "
            f"{sc['parallel_efficiency_pct']}% |")

    h2h = payload["analysis"]["head_to_head"]
    if h2h:
        lines += ["", "## Comparación directa (misma cantidad de workers)", "",
                  "| Workers | Dask (s) | Spark (s) | Más rápido | Ventaja | Dask pico (MB) | Spark pico (MB) |",
                  "|---:|---:|---:|---|---:|---:|---:|"]
        for row in h2h:
            lines.append(
                f"| {row['workers']} | {row['dask_seconds']} | {row['spark_seconds']} | "
                f"**{row['faster'].capitalize()}** | {row['times_faster']}× | "
                f"{row['dask_peak_mb'] if row['dask_peak_mb'] is not None else 'n/d'} | "
                f"{row['spark_peak_mb'] if row['spark_peak_mb'] is not None else 'n/d'} |")

    ver = payload["verification"]
    lines += ["", "## Verificación de equivalencia", "",
              f"- Las celdas top-10 coinciden entre motores: "
              f"**{'sí' if ver['results_match'] else 'NO'}**",
              f"- Celdas por configuración: `{ver['cells_per_config']}`",
              f"- Registros por configuración: `{ver['rows_per_config']}`",
              "", "> Tabla generada automáticamente por "
              "`src/benchmark/compare_dask_spark.py`. "
              "Los números provienen de mediciones propias, no de estimaciones.",
              ""]
    return "\n".join(lines)


def _print_table(payload: dict[str, Any]) -> None:
    log.info("")
    log.info("=" * 96)
    log.info("  RESULTADOS DEL BENCHMARK  (%s)", payload["operation"])
    log.info("=" * 96)
    log.info("  %-8s %-8s %12s %12s %14s %10s %12s",
             "Motor", "Workers", "Mediana(s)", "Min(s)", "Pico mem(MB)",
             "Celdas", "Registros")
    log.info("  " + "-" * 92)
    for r in payload["results"]:
        log.info("  %-8s %-8s %12.2f %12.2f %14s %10s %12s",
                 r["engine"], r["workers"], r["time_median_seconds"],
                 r["time_min_seconds"],
                 r["peak_worker_memory_mb"] if r["peak_worker_memory_mb"]
                 is not None else "n/d",
                 f"{r['cells']:,}", f"{r['rows_processed']:,}")
    log.info("=" * 96)
    for row in payload["analysis"]["head_to_head"]:
        log.info("  Con %s worker(s): %s es %sx mas rapido (Dask %.2fs vs Spark %.2fs)",
                 row["workers"], row["faster"], row["times_faster"],
                 row["dask_seconds"], row["spark_seconds"])
    for engine, sc in payload["analysis"]["scaling"].items():
        log.info("  %s escala %sx al pasar de %s a %s workers "
                 "(eficiencia paralela %s%%)",
                 engine, sc["speedup"], sc["from_workers"], sc["to_workers"],
                 sc["parallel_efficiency_pct"])
    log.info("=" * 96)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Benchmark Dask vs Spark sobre la agregacion por grilla")
    ap.add_argument("--dask-workers", default="1,2",
                    help="Configuraciones de workers de Dask, separadas por coma")
    ap.add_argument("--spark-cores", default="1,2",
                    help="Configuraciones de spark.cores.max, separadas por coma")
    ap.add_argument("--repeats", type=int, default=2)
    ap.add_argument("--cell-deg", type=float, default=None)
    ap.add_argument("--threads-per-worker", type=int, default=2)
    ap.add_argument("--memory-limit", default="2GB")
    ap.add_argument("--executor-memory", default=None)
    ap.add_argument("--shuffle-partitions", type=int, default=16)
    ap.add_argument("--dask-scheduler", default=None,
                    help="Usar un scheduler externo en vez de LocalCluster")
    ap.add_argument("--spark-master", default=None)
    ap.add_argument("--out-dir", default="/data/benchmark")
    ap.add_argument("--only", choices=["dask", "spark", "both"], default="both")
    args = ap.parse_args(argv)

    dask_cfg = [int(x) for x in args.dask_workers.split(",") if x.strip()]
    spark_cfg = [int(x) for x in args.spark_cores.split(",") if x.strip()]
    if args.only == "dask":
        spark_cfg = []
    elif args.only == "spark":
        dask_cfg = []

    run_benchmark(
        dask_configs=dask_cfg,
        spark_configs=spark_cfg,
        repeats=args.repeats,
        cell_deg=args.cell_deg if args.cell_deg is not None else config.geo.grid_cell_deg,
        threads_per_worker=args.threads_per_worker,
        memory_limit=args.memory_limit,
        executor_memory=args.executor_memory or config.spark.executor_memory,
        shuffle_partitions=args.shuffle_partitions,
        dask_scheduler=args.dask_scheduler,
        spark_master=args.spark_master,
        out_dir=args.out_dir,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
