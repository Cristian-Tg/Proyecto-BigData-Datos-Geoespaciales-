# Descripción

<!-- Qué cambia y por qué. Si corrige un fallo, describe el síntoma que se veía. -->

## Tipo de cambio

- [ ] `feat` — funcionalidad nueva
- [ ] `fix` — corrección de un fallo
- [ ] `docs` — documentación o informe
- [ ] `test` — pruebas
- [ ] `chore` / `ci` — infraestructura, dependencias, pipeline

## Componentes afectados

- [ ] Ingesta / limpieza con Dask (`src/ingestion/`)
- [ ] Procesamiento con Spark (`src/processing/`)
- [ ] Consultas geoespaciales / API (`src/api/`)
- [ ] Benchmark (`src/benchmark/`)
- [ ] Infraestructura (`docker/`, `docker-compose*.yml`)
- [ ] Pipeline de Jenkins (`Jenkinsfile`)
- [ ] Documentación (`README.md`, `docs/`)

## Verificación

Marca solo lo que hayas ejecutado de verdad.

```bash
DC="docker compose -f docker-compose.yml -f docker-compose.lowmem.yml"
```

- [ ] **Pruebas unitarias**
      `$DC run --rm --no-deps tests pytest -m "not integration" -q`
      Resultado: <!-- p.ej. 261 passed -->
- [ ] **Pruebas de integración** (requiere el sistema levantado)
      `$DC run --rm -e API_BASE_URL=http://api:5000 tests pytest -m integration -q`
      Resultado: <!-- p.ej. 52 passed -->
- [ ] **Estilo**
      `$DC run --rm --no-deps tests ruff check src tests`
- [ ] Si toca la ingesta o Spark, he **reejecutado la etapa** y comprobado
      `/api/v1/stats`

## Comprobaciones obligatorias

- [ ] **No añado ningún secreto.** Verificado con:
      `git ls-files | grep -Ei "kaggle\.json|^\.env$"` → sin resultados
- [ ] Si cambié los límites de memoria, he recalculado el presupuesto de la fase
      y cabe en la RAM asignada a Docker
- [ ] Si cambié código que corre en la imagen de Spark, sigue siendo válido en
      **Python 3.10** (`tests/test_compat.py` lo comprueba)

## Evidencia

<!--
Pega la salida relevante, o una captura del build de Jenkins en verde.
Si el cambio se ve en la interfaz, una captura del mapa o de la respuesta JSON.
-->

## Revisión

- [ ] Otro integrante del equipo ha revisado este cambio
- [ ] El build de Jenkins pasó las dos puertas (pruebas unitarias y de API)
