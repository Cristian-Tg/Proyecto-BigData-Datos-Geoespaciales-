"""Ingesta con Dask: lectura particionada, limpieza y carga por lotes a MongoDB.

Flujo (requisito 4.1 del enunciado):

    CSV (GB)
      -> dask.dataframe.read_csv(blocksize=...)   lectura PARTICIONADA
      -> una tarea Dask por particion, en los workers:
             clean_partition()   descarta nulos / fuera de rango / etc.
             enrich_partition()  grilla, geohash, hora/dia/mes
             to_documents()      GeoJSON Point
             insert_many()        carga POR LOTES
      -> MongoDB + indice 2dsphere

Las particiones se envian al cluster en oleadas acotadas en lugar de todas a la
vez: asi el scheduler no acumula miles de tareas pendientes y se puede cortar
en seco cuando ya se alcanzo SAMPLE_SIZE.
"""
from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import dask
import dask.dataframe as dd
from distributed import Client, as_completed

from src.common import config
from src.common.logging_conf import setup_logging
from src.ingestion.cleaning import (
    DTYPES,
    USECOLS,
    CleaningStats,
    clean_partition,
    enrich_partition,
    to_documents,
)

log = setup_logging("ingestion.clean_load")


# ---------------------------------------------------------------------------
# Tarea que se ejecuta EN EL WORKER de Dask
# ---------------------------------------------------------------------------
def process_and_load(pdf, mongo_uri: str, database: str, collection: str,
                     batch_size: int, cell_deg: float,
                     precision: int) -> dict[str, Any]:
    """Limpia una particion y la inserta en MongoDB. Corre en el worker.

    Devuelve un diccionario serializable (no objetos de pymongo) porque el
    resultado viaja de vuelta al cliente por la red del cluster.
    """
    from pymongo import MongoClient  # import local: cada worker el suyo
    from pymongo.errors import BulkWriteError

    clean, stats = clean_partition(pdf)
    inserted = 0
    dup_errors = 0

    if not clean.empty:
        clean = enrich_partition(clean, cell_deg=cell_deg, precision=precision)
        docs = to_documents(clean)

        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=20_000,
                             socketTimeoutMS=300_000)
        try:
            coll = client[database][collection]
            for start in range(0, len(docs), batch_size):
                chunk = docs[start:start + batch_size]
                try:
                    # ordered=False: un duplicado no aborta el lote completo,
                    # y permite que MongoDB inserte en paralelo.
                    res = coll.insert_many(chunk, ordered=False)
                    inserted += len(res.inserted_ids)
                except BulkWriteError as bwe:
                    details = bwe.details or {}
                    errs = details.get("writeErrors", [])
                    # 11000 = duplicate key -> registro ya cargado, no es fallo
                    dups = [e for e in errs if e.get("code") == 11000]
                    others = [e for e in errs if e.get("code") != 11000]
                    dup_errors += len(dups)
                    inserted += details.get("nInserted", 0)
                    if others:
                        raise RuntimeError(
                            f"Errores de escritura no esperados: {others[:3]}"
                        ) from bwe
        finally:
            client.close()

    out = stats.as_dict()
    out["inserted"] = inserted
    out["duplicate_key_skipped"] = dup_errors
    return out


# ---------------------------------------------------------------------------
# Cliente / cluster
# ---------------------------------------------------------------------------
def build_client(scheduler: str | None = None, n_workers: int | None = None,
                 threads: int = 2, memory_limit: str = "2GB") -> tuple[Client, bool]:
    """Devuelve (cliente, es_local).

    Se prefiere el scheduler del cluster de Docker Compose. Si no responde se
    cae a un LocalCluster para que la ingesta siga siendo ejecutable en un
    portatil sin Docker.
    """
    target = scheduler if scheduler is not None else config.ingest.scheduler

    if target and target.lower() not in {"local", "none", ""}:
        try:
            client = Client(target, timeout="30s")
            log.info("Conectado al scheduler de Dask en %s", target)
            log.info("Dashboard: %s", client.dashboard_link)
            return client, False
        except Exception as exc:  # noqa: BLE001
            log.warning("No se pudo usar el scheduler %s (%s). "
                        "Se usa un LocalCluster.", target, exc)

    from distributed import LocalCluster  # noqa: PLC0415

    cluster = LocalCluster(
        n_workers=n_workers or int(os.environ.get("DASK_N_WORKERS", "2")),
        threads_per_worker=threads,
        memory_limit=memory_limit,
        processes=True,
        dashboard_address=":0",
    )
    client = Client(cluster)
    log.info("LocalCluster de Dask activo: %s", client)
    return client, True


def wait_for_workers(client: Client, minimum: int = 1, timeout: int = 180) -> int:
    """Espera a que se registren workers. En `compose up` tardan unos segundos."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        n = len(client.scheduler_info().get("workers", {}))
        if n >= minimum:
            log.info("Workers de Dask disponibles: %s", n)
            return n
        time.sleep(2)
    n = len(client.scheduler_info().get("workers", {}))
    log.warning("Solo %s worker(s) tras %ss; se continua igualmente", n, timeout)
    return n


# ---------------------------------------------------------------------------
# Orquestacion
# ---------------------------------------------------------------------------
def prepare_target(drop_existing: bool) -> None:
    """Prepara la coleccion destino y el indice que garantiza idempotencia."""
    from pymongo import ASCENDING

    from src.common.mongo import get_db

    db = get_db()
    coll_name = config.mongo.collection

    if drop_existing and coll_name in db.list_collection_names():
        log.warning("DROP_EXISTING=1 -> se elimina la coleccion '%s'", coll_name)
        db[coll_name].drop()

    # Este indice se crea ANTES de la carga a proposito: es el que convierte la
    # ingesta en idempotente (reejecutarla no duplica registros). El resto de
    # los indices, incluido 2dsphere, se crean DESPUES porque construirlos
    # sobre una coleccion ya poblada es mucho mas rapido que mantenerlos
    # actualizados durante millones de inserciones.
    db[coll_name].create_index([("accident_id", ASCENDING)],
                               name="ux_accident_id", unique=True, sparse=True)
    log.info("Coleccion destino lista: %s.%s", db.name, coll_name)


def run_ingestion(csv_path: str | Path,
                  sample_size: int | None = None,
                  batch_size: int | None = None,
                  blocksize: str | None = None,
                  scheduler: str | None = None,
                  drop_existing: bool = False,
                  stats_out: str | None = None) -> CleaningStats:
    """Ejecuta la ingesta completa y devuelve las estadisticas agregadas."""
    csv_path = Path(csv_path)
    if not csv_path.is_file():
        raise FileNotFoundError(f"No existe el CSV de entrada: {csv_path}")

    sample_size = sample_size if sample_size is not None else config.ingest.sample_size
    batch_size = batch_size or config.ingest.batch_size
    blocksize = blocksize or config.ingest.blocksize

    size_mb = csv_path.stat().st_size / 1024 / 1024
    log.info("=" * 72)
    log.info("INGESTA CON DASK")
    log.info("  Archivo        : %s (%.1f MB)", csv_path, size_mb)
    log.info("  Objetivo       : %s registros", f"{sample_size:,}")
    log.info("  Tam. particion : %s", blocksize)
    log.info("  Tam. lote      : %s", f"{batch_size:,}")
    log.info("=" * 72)

    prepare_target(drop_existing)

    client, is_local = build_client(scheduler)
    n_workers = wait_for_workers(client, minimum=1)

    t0 = time.perf_counter()
    try:
        # --- Lectura PARTICIONADA ------------------------------------------
        ddf = dd.read_csv(
            str(csv_path),
            usecols=lambda c: c in USECOLS,   # tolera columnas ausentes
            dtype=DTYPES,
            blocksize=blocksize,
            assume_missing=True,
            on_bad_lines="skip",
            low_memory=False,
        )
        npartitions = ddf.npartitions
        log.info("Dask creo %s particiones a partir del CSV", npartitions)

        parts = ddf.to_delayed()
        total = CleaningStats()
        inserted_total = 0
        dup_total = 0

        # Oleadas de tareas: 3 por worker mantiene el cluster saturado sin
        # inundar al scheduler ni la memoria del cliente.
        wave = max(2, n_workers * 3)
        idx = 0

        while idx < npartitions and inserted_total < sample_size:
            batch_parts = parts[idx:idx + wave]
            tasks = [
                dask.delayed(process_and_load)(
                    part, config.mongo.uri, config.mongo.database,
                    config.mongo.collection, batch_size,
                    config.geo.grid_cell_deg, config.geo.geohash_precision,
                )
                for part in batch_parts
            ]
            futures = client.compute(tasks)

            for future in as_completed(futures):
                res = future.result()
                partial = CleaningStats(**{
                    k: v for k, v in res.items()
                    if k in CleaningStats.__dataclass_fields__
                })
                total = total + partial
                inserted_total += res["inserted"]
                dup_total += res["duplicate_key_skipped"]

            idx += wave
            pct = 100.0 * min(1.0, inserted_total / sample_size)
            log.info("Progreso: %s / %s registros cargados (%.1f%%) | "
                     "particiones %s/%s",
                     f"{inserted_total:,}", f"{sample_size:,}", pct,
                     min(idx, npartitions), npartitions)

        elapsed = time.perf_counter() - t0
        total.rows_out = inserted_total

        # --- Indices definitivos, incluido 2dsphere ------------------------
        log.info("Construyendo indices (2dsphere incluido)...")
        from src.common.mongo import collection_stats, ensure_indexes

        ensure_indexes()
        summary = collection_stats()

        log.info(total.report())
        log.info("Duplicados rechazados por el indice unico: %s", f"{dup_total:,}")
        log.info("Tiempo total de ingesta: %.1f s (%.0f registros/s)",
                 elapsed, inserted_total / elapsed if elapsed else 0)
        log.info("Indice 2dsphere activo: %s", summary["geo_index_2dsphere"])
        log.info("Documentos en %s: %s", config.mongo.collection,
                 f"{summary['collections'][config.mongo.collection]['count']:,}")

        if inserted_total < 1_000_000:
            log.warning("Se cargaron menos de 1.000.000 de registros. El "
                        "enunciado exige ese minimo: suba SAMPLE_SIZE o use el "
                        "dataset completo.")

        if stats_out:
            payload = total.as_dict()
            payload.update({
                "elapsed_seconds": round(elapsed, 3),
                "records_per_second": round(inserted_total / elapsed, 1) if elapsed else 0,
                "duplicate_key_skipped": dup_total,
                "dask_workers": n_workers,
                "partitions_total": npartitions,
                "blocksize": blocksize,
                "batch_size": batch_size,
                "source_file": str(csv_path),
                "source_size_mb": round(size_mb, 2),
                "geo_index_2dsphere": summary["geo_index_2dsphere"],
            })
            Path(stats_out).parent.mkdir(parents=True, exist_ok=True)
            Path(stats_out).write_text(json.dumps(payload, indent=2, default=str),
                                       encoding="utf-8")
            log.info("Estadisticas de limpieza escritas en %s", stats_out)

        return total

    finally:
        client.close()
        if is_local:
            # El cierre del cluster local puede fallar si un worker ya murio;
            # no debe enmascarar el resultado de la ingesta.
            with contextlib.suppress(Exception):
                client.cluster.close()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Ingesta y limpieza con Dask hacia MongoDB (GeoJSON)")
    ap.add_argument("--csv", default=None,
                    help="Ruta al CSV. Por defecto se descarga desde Kaggle.")
    ap.add_argument("--sample-size", type=int, default=None)
    ap.add_argument("--batch-size", type=int, default=None)
    ap.add_argument("--blocksize", default=None)
    ap.add_argument("--scheduler", default=None,
                    help="URL del scheduler de Dask, o 'local'")
    ap.add_argument("--drop-existing", action="store_true",
                    help="Elimina la coleccion antes de cargar")
    ap.add_argument("--skip-download", action="store_true")
    ap.add_argument("--stats-out", default="/data/cleaning_stats.json")
    args = ap.parse_args(argv)

    csv_path = args.csv
    if not csv_path:
        if args.skip_download:
            csv_path = str(Path(config.kaggle.data_dir) / config.kaggle.target_file)
        else:
            from src.ingestion.download_kaggle import obtain_dataset

            path, origin = obtain_dataset(
                force=os.environ.get("FORCE_DOWNLOAD", "").lower()
                in {"1", "true", "yes"})
            log.info("Dataset obtenido desde: %s", origin)
            csv_path = str(path)

    drop = args.drop_existing or os.environ.get("DROP_EXISTING", "").lower() in {
        "1", "true", "yes"}

    run_ingestion(
        csv_path=csv_path,
        sample_size=args.sample_size,
        batch_size=args.batch_size,
        blocksize=args.blocksize,
        scheduler=args.scheduler,
        drop_existing=drop,
        stats_out=args.stats_out,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
