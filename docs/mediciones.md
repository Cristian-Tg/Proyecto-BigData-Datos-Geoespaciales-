<!-- Generado por scripts/report_numbers.py — no editar a mano -->

# Mediciones del sistema

Todas las cifras de este documento provienen de ejecuciones reales del
pipeline, no de estimaciones. Para regenerarlo:

```bash
python scripts/report_numbers.py --out docs/mediciones.md
```

## 1. Limpieza con Dask

| Concepto | Registros | % |
|---|---:|---:|
| Leídos del CSV | 1 240 933 | 100.00 % |
| − Coordenadas nulas | 0 | 0.00 % |
| − Fuera de rango WGS84 | 0 | 0.00 % |
| − Relleno (0,0) | 0 | 0.00 % |
| − Fuera de la caja de EE. UU. | 0 | 0.00 % |
| − Fecha no parseable | 0 | 0.00 % |
| − Severidad inválida | 0 | 0.00 % |
| − Duplicados por ID | 0 | 0.00 % |
| **Cargados en MongoDB** | **1 240 933** | **100.0 %** |
| Duplicados rechazados por el índice único | 0 | |
| Tiempo de ingesta | 815.206 s | 1 522 reg/s |

- **Origen**: `/data/US_Accidents_March23.csv` (2916.51 MB)
- **Particiones de Dask**: 191 de 16MB · **workers**: 2 · **lote**: 5 000 documentos
- **Índice 2dsphere activo**: sí

## 2. Agregaciones con Spark

| Colección | Documentos | Tiempo (s) |
|---|---:|---:|
| `agg_grid` | 23 280 | 26.366 |
| `agg_hotspots` | 200 | 4.617 |
| `agg_geohash` | 55 514 | 16.272 |
| `agg_temporal` | 51 | 11.062 |
| `agg_state` | 49 | 5.263 |
| **Total** | | **186.484** |

- **Registros leídos de MongoDB**: 1 240 933
- **Spark**: 3.5.3 · **executors**: 1

## 2b. Cobertura temporal de la muestra

| Año | Registros | % de la muestra |
|---:|---:|---:|
| 2016 | 116 962 | 9.4 % |
| 2017 | 150 959 | 12.2 % |
| 2018 | 119 013 | 9.6 % |
| 2019 | 234 344 | 18.9 % |
| 2020 | 266 818 | 21.5 % |
| 2021 | 151 529 | 12.2 % |
| 2022 | 192 428 | 15.5 % |
| 2023 | 8 880 | 0.7 % |

- **Rango cubierto**: 2016–2023 (8 años)
- **Años ausentes en el rango**: ninguno
- La muestra cubre el rango completo sin huecos, que es el efecto buscado del muestreo por particiones repartidas.

## 3. Rendimiento de las consultas geoespaciales

| Consulta | Parámetros | `elapsed_ms` | Resultados |
|---|---|---:|---:|
| `$near` | radio 5 km | 1008.71 | 8 144 |
| `$near` | radio 50 km | 25.42 | 102 687 |
| `$near` | radio 5 km + `min_severity=3` | 46.12 | 2 280 |
| `$geoWithin` | área de Los Ángeles | 10.22 | 66 190 |
| `$geoWithin` | + `summary=true` | 30.59 | 66 190 |
| `$geoNear` | 20 km, por severidad | 812.42 | 48 746 |
| `$geoNear` | 20 km, bandas de 2 km | 774.49 | 48 746 |
| `Spark` | `/aggregations/hotspots` top 20 | 4.22 | 200 |

## 4. Benchmark Dask vs Spark

- **run_id**: `3ade87f7fe4f`
- **Fecha**: 2026-09-29T19:51:48.244313+00:00
- **Repeticiones por configuración**: 2 (se reporta la mediana)
- **Tamaño de celda**: 0.1°
- **Fuente**: `geobigdata.accidents`
- **CPUs del host**: 8

## Resultados

| Motor | Workers | Tiempo mediana (s) | min | max | σ | Memoria pico workers (MB) | Celdas | Registros |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Dask | 1 | **15.453** | 14.658 | 16.247 | 1.124 | 196.13 | 23,280 | 1,240,933 |
| Dask | 2 | **8.241** | 7.453 | 9.028 | 1.114 | 357.71 | 23,280 | 1,240,933 |
| Spark | 1 | **56.224** | 49.969 | 62.478 | 8.845 | 338.92 | 23,280 | 1,240,933 |
| Spark | 2 | **39.071** | 35.925 | 42.217 | 4.449 | 361.09 | 23,280 | 1,240,933 |

## Escalabilidad

| Motor | Workers | Speedup real | Speedup ideal | Eficiencia paralela |
|---|---|---:|---:|---:|
| Dask | 1 → 2 | 1.875× | 2.0× | 93.76% |
| Spark | 1 → 2 | 1.439× | 2.0× | 71.95% |

## Comparación directa (misma cantidad de workers)

| Workers | Dask (s) | Spark (s) | Más rápido | Ventaja | Dask pico (MB) | Spark pico (MB) |
|---:|---:|---:|---|---:|---:|---:|
| 1 | 15.453 | 56.224 | **Dask** | 3.64× | 196.13 | 338.92 |
| 2 | 8.241 | 39.071 | **Dask** | 4.74× | 357.71 | 361.09 |

## Verificación de equivalencia

- Las celdas top-10 coinciden entre motores: **sí**
- Celdas por configuración: `{'dask-1': 23280, 'dask-2': 23280, 'spark-1': 23280, 'spark-2': 23280}`
- Registros por configuración: `{'dask-1': 1240933, 'dask-2': 1240933, 'spark-1': 1240933, 'spark-2': 1240933}`

> Tabla generada automáticamente por `src/benchmark/compare_dask_spark.py`. Los números provienen de mediciones propias, no de estimaciones.
