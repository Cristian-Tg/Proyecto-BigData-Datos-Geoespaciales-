#!/usr/bin/env python3
"""Extrae las mediciones reales del sistema y las imprime en Markdown.

El informe tecnico tiene varias tablas marcadas con una advertencia para
rellenar con mediciones propias. Este script las genera a partir de los
artefactos que producen las propias etapas del pipeline, de modo que no haya que
transcribir numeros a mano (y no haya forma de equivocarse al hacerlo).

    # con el sistema levantado
    python scripts/report_numbers.py

    # o desde dentro del contenedor, leyendo el volumen de datos
    docker compose run --rm --no-deps --entrypoint python3 \\
        spark-job scripts/report_numbers.py --data-dir /data

Fuentes que consulta, todas opcionales:
    /data/cleaning_stats.json        estadisticas de la limpieza con Dask
    /data/spark_summary.json         tiempos y documentos por agregacion
    /data/benchmark/benchmark_latest.md   tabla del benchmark, ya en Markdown
    la API en vivo                   tiempos de respuesta de cada consulta
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def _load(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _get(url: str, timeout: int = 60) -> dict[str, Any] | None:
    try:
        with urlopen(Request(url, headers={"Accept": "application/json"}),
                     timeout=timeout) as r:
            return json.loads(r.read().decode())
    except (URLError, ValueError, OSError):
        return None


def _post(url: str, payload: dict[str, Any], timeout: int = 60) -> dict[str, Any] | None:
    try:
        body = json.dumps(payload).encode()
        req = Request(url, data=body,
                      headers={"Content-Type": "application/json"})
        with urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except (URLError, ValueError, OSError):
        return None


def _n(value: Any) -> str:
    """Formatea un entero con separador de miles, o '—' si no hay dato."""
    if value is None:
        return "—"
    try:
        return f"{int(value):,}".replace(",", " ")
    except (TypeError, ValueError):
        return str(value)


# ---------------------------------------------------------------------------
# Tabla de limpieza
# ---------------------------------------------------------------------------
def tabla_limpieza(stats: dict[str, Any]) -> str:
    total = stats.get("rows_in") or 0

    def pct(v: int) -> str:
        return f"{100.0 * v / total:.2f} %" if total else "—"

    filas = [
        ("Leídos del CSV", stats.get("rows_in")),
        ("− Coordenadas nulas", stats.get("dropped_null_coords")),
        ("− Fuera de rango WGS84", stats.get("dropped_coords_out_of_range")),
        ("− Relleno (0,0)", stats.get("dropped_zero_island")),
        ("− Fuera de la caja de EE. UU.", stats.get("dropped_outside_us_bbox")),
        ("− Fecha no parseable", stats.get("dropped_unparseable_time")),
        ("− Severidad inválida", stats.get("dropped_invalid_severity")),
        ("− Duplicados por ID", stats.get("dropped_duplicate_id")),
    ]

    out = ["| Concepto | Registros | % |", "|---|---:|---:|"]
    for nombre, valor in filas:
        out.append(f"| {nombre} | {_n(valor)} | {pct(valor or 0)} |")
    out.append(f"| **Cargados en MongoDB** | **{_n(stats.get('rows_out'))}** | "
               f"**{stats.get('retention_pct', '—')} %** |")
    out.append(f"| Duplicados rechazados por el índice único | "
               f"{_n(stats.get('duplicate_key_skipped'))} | |")

    seg = stats.get("elapsed_seconds")
    rps = stats.get("records_per_second")
    out.append(f"| Tiempo de ingesta | {seg} s | {_n(rps)} reg/s |")

    extra = [
        "",
        f"- **Origen**: `{stats.get('source_file', '—')}` "
        f"({stats.get('source_size_mb', '—')} MB)",
        f"- **Particiones de Dask**: {stats.get('partitions_total', '—')} "
        f"de {stats.get('blocksize', '—')} · "
        f"**workers**: {stats.get('dask_workers', '—')} · "
        f"**lote**: {_n(stats.get('batch_size'))} documentos",
        f"- **Índice 2dsphere activo**: "
        f"{'sí' if stats.get('geo_index_2dsphere') else 'NO'}",
    ]
    return "\n".join(out + extra)


# ---------------------------------------------------------------------------
# Tabla de agregaciones de Spark
# ---------------------------------------------------------------------------
def tabla_spark(summary: dict[str, Any]) -> str:
    out = ["| Colección | Documentos | Tiempo (s) |", "|---|---:|---:|"]
    for nombre, info in (summary.get("stages") or {}).items():
        out.append(f"| `{nombre}` | {_n(info.get('documents'))} | "
                   f"{info.get('seconds')} |")
    out.append(f"| **Total** | | **{summary.get('total_seconds', '—')}** |")
    out += [
        "",
        f"- **Registros leídos de MongoDB**: {_n(summary.get('input_rows'))}",
        f"- **Spark**: {summary.get('spark_version', '—')} · "
        f"**executors**: {summary.get('executors', '—')}",
    ]
    return "\n".join(out)


# ---------------------------------------------------------------------------
# Tabla de rendimiento de las consultas
# ---------------------------------------------------------------------------
CONSULTAS: list[tuple[str, str, str, dict[str, Any]]] = [
    ("$near", "radio 5 km", "GET", {"path": "/api/v1/near",
     "params": {"lat": 34.0522, "lon": -118.2437, "radius_m": 5000, "limit": 100}}),
    ("$near", "radio 50 km", "GET", {"path": "/api/v1/near",
     "params": {"lat": 34.0522, "lon": -118.2437, "radius_m": 50000, "limit": 100}}),
    ("$near", "radio 5 km + `min_severity=3`", "GET", {"path": "/api/v1/near",
     "params": {"lat": 34.0522, "lon": -118.2437, "radius_m": 5000,
                "limit": 100, "min_severity": 3}}),
    ("$geoWithin", "área de Los Ángeles", "POST", {"path": "/api/v1/within",
     "body": {"bbox": [-118.6, 33.85, -118.0, 34.25], "limit": 100,
              "summary": False}}),
    ("$geoWithin", "+ `summary=true`", "POST", {"path": "/api/v1/within",
     "body": {"bbox": [-118.6, 33.85, -118.0, 34.25], "limit": 100,
              "summary": True}}),
    ("$geoNear", "20 km, por severidad", "GET", {"path": "/api/v1/geonear",
     "params": {"lat": 34.0522, "lon": -118.2437, "max_distance_m": 20000,
                "group_by": "severity"}}),
    ("$geoNear", "20 km, bandas de 2 km", "GET", {"path": "/api/v1/geonear",
     "params": {"lat": 34.0522, "lon": -118.2437, "max_distance_m": 20000,
                "group_by": "distance_band", "band_width_m": 2000}}),
    ("Spark", "`/aggregations/hotspots` top 20", "GET",
     {"path": "/api/v1/aggregations/hotspots", "params": {"limit": 20}}),
]


def tabla_consultas(base: str) -> str:
    out = ["| Consulta | Parámetros | `elapsed_ms` | Resultados |",
           "|---|---|---:|---:|"]
    for operador, desc, metodo, spec in CONSULTAS:
        if metodo == "GET":
            url = f"{base}{spec['path']}?{urlencode(spec['params'])}"
            data = _get(url)
        else:
            data = _post(f"{base}{spec['path']}", spec["body"])

        if not data:
            out.append(f"| `{operador}` | {desc} | — | *API no disponible* |")
            continue

        n = (data.get("total_matching")
             if data.get("total_matching") is not None
             else data.get("total_in_radius", data.get("returned")))
        out.append(f"| `{operador}` | {desc} | {data.get('elapsed_ms', '—')} "
                   f"| {_n(n)} |")
    return "\n".join(out)


# ---------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Genera en Markdown las tablas de mediciones del informe")
    ap.add_argument("--data-dir", default="/data",
                    help="Directorio con los JSON que produce el pipeline")
    ap.add_argument("--api", default="http://localhost:5000",
                    help="URL base de la API para medir las consultas")
    ap.add_argument("--skip-api", action="store_true")
    ap.add_argument("--out", default=None,
                    help="Archivo de salida (por defecto, stdout)")
    args = ap.parse_args(argv)

    data = Path(args.data_dir)
    bloques: list[str] = [
        "<!-- Generado por scripts/report_numbers.py — no editar a mano -->",
        "",
        "# Mediciones del sistema",
        "",
        "Todas las cifras de este documento provienen de ejecuciones reales del",
        "pipeline, no de estimaciones. Para regenerarlo:",
        "",
        "```bash",
        "python scripts/report_numbers.py --out docs/mediciones.md",
        "```",
        "",
    ]

    # --- limpieza ---------------------------------------------------------
    bloques += ["## 1. Limpieza con Dask", ""]
    stats = _load(data / "cleaning_stats.json")
    if stats:
        bloques.append(tabla_limpieza(stats))
    else:
        bloques.append(f"*No se encontró `{data / 'cleaning_stats.json'}`. "
                       "Ejecute `docker compose run --rm ingestion`.*")
    bloques.append("")

    # --- Spark ------------------------------------------------------------
    bloques += ["## 2. Agregaciones con Spark", ""]
    summary = _load(data / "spark_summary.json")
    if summary:
        bloques.append(tabla_spark(summary))
    else:
        bloques.append(f"*No se encontró `{data / 'spark_summary.json'}`. "
                       "Ejecute `docker compose run --rm spark-job`.*")
    bloques.append("")

    # --- consultas --------------------------------------------------------
    bloques += ["## 3. Rendimiento de las consultas geoespaciales", ""]
    if args.skip_api:
        bloques.append("*Omitido (--skip-api).*")
    else:
        bloques.append(tabla_consultas(args.api.rstrip("/")))
    bloques.append("")

    # --- benchmark --------------------------------------------------------
    bloques += ["## 4. Benchmark Dask vs Spark", ""]
    bench_md = data / "benchmark" / "benchmark_latest.md"
    try:
        texto = bench_md.read_text(encoding="utf-8")
        # Se omite el encabezado propio del informe de benchmark
        bloques.append("\n".join(texto.split("\n")[1:]).strip())
    except OSError:
        bloques.append(f"*No se encontró `{bench_md}`. "
                       "Ejecute `docker compose run --rm benchmark`.*")
    bloques.append("")

    salida = "\n".join(bloques)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(salida, encoding="utf-8")
        print(f"Escrito en {args.out}", file=sys.stderr)
    else:
        print(salida)
    return 0


if __name__ == "__main__":
    sys.exit(main())
