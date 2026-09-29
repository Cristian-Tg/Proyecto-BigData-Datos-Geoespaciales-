"""Compatibilidad entre las dos versiones de Python del proyecto.

El sistema corre DOS interpretes distintos:

    python:3.11-slim   ->  dask, api, tests     (3.11.16)
    imagen de Spark    ->  spark, benchmark     (3.10.12)

Eso significa que todo `src/` tiene que ser valido en **3.10**, aunque la
mayoria de los contenedores use 3.11. Es una restriccion facil de romper sin
darse cuenta, y el sintoma aparece tarde: `ruff` en verde, las pruebas
unitarias en verde (corren en 3.11) y un ImportError en tiempo de ejecucion
dentro del contenedor de Spark.

Ocurrio de verdad: con `target-version = "py311"` en ruff.toml, el autofix
convirtio `timezone.utc` en `datetime.UTC` --que solo existe desde 3.11-- y el
benchmark fallo con:

    ImportError: cannot import name 'UTC' from 'datetime'

Estas pruebas cierran esa puerta.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

# Version minima que debe soportar el codigo, fijada por la imagen de Spark.
MIN_PYTHON = (3, 10)


def _archivos_python() -> list[Path]:
    return sorted(
        f for f in SRC.rglob("*.py")
        if "__pycache__" not in f.parts
    )


def test_hay_archivos_que_revisar():
    """Red de seguridad: si el glob deja de encontrar nada, las demas pruebas
    de este archivo pasarian vacias y no protegerian nada."""
    archivos = _archivos_python()
    assert len(archivos) >= 10, f"solo se encontraron {len(archivos)} archivos"


@pytest.mark.parametrize("archivo", _archivos_python(), ids=lambda p: p.name)
def test_sin_datetime_utc(archivo: Path):
    """`datetime.UTC` no existe en Python 3.10; hay que usar `timezone.utc`."""
    texto = archivo.read_text(encoding="utf-8")

    assert not re.search(r"from\s+datetime\s+import\s+([\w\s,]*\b)UTC\b", texto), (
        f"{archivo.relative_to(ROOT)} importa UTC de datetime, que solo existe "
        f"desde Python 3.11. La imagen de Spark usa "
        f"{MIN_PYTHON[0]}.{MIN_PYTHON[1]}: use `timezone.utc`."
    )
    assert "datetime.UTC" not in texto, (
        f"{archivo.relative_to(ROOT)} usa datetime.UTC, que solo existe desde "
        "Python 3.11. Use timezone.utc."
    )


@pytest.mark.parametrize("archivo", _archivos_python(), ids=lambda p: p.name)
def test_el_codigo_compila(archivo: Path):
    """Todo `src/` tiene que parsear como AST valido.

    No detecta funciones ausentes de 3.10, pero si sintaxis posterior a ella
    cuando las pruebas corran en un interprete 3.10.
    """
    fuente = archivo.read_text(encoding="utf-8")
    try:
        ast.parse(fuente, filename=str(archivo))
    except SyntaxError as exc:
        pytest.fail(f"{archivo.relative_to(ROOT)} no compila: {exc}")


@pytest.mark.parametrize("archivo", _archivos_python(), ids=lambda p: p.name)
def test_annotations_futuras(archivo: Path):
    """Las anotaciones modernas necesitan `from __future__ import annotations`.

    `dict[str, int]` y `int | None` en anotaciones funcionan en 3.10 SOLO si el
    modulo aplaza su evaluacion. Sin el import, la anotacion se evalua al
    importar y falla. Se exige en todo archivo que use esa sintaxis.
    """
    texto = archivo.read_text(encoding="utf-8")
    if archivo.name == "__init__.py" and not texto.strip():
        pytest.skip("archivo vacio")

    usa_moderna = re.search(r":\s*(dict|list|tuple|set)\[", texto) \
        or re.search(r"->\s*[\w.\[\]]+\s*\|", texto) \
        or re.search(r":\s*[\w.\[\]]+\s*\|\s*None", texto)

    if usa_moderna:
        assert "from __future__ import annotations" in texto, (
            f"{archivo.relative_to(ROOT)} usa anotaciones modernas sin "
            "`from __future__ import annotations`; en Python 3.10 se evaluan "
            "al importar y fallan."
        )


def test_isinstance_sin_union():
    """`isinstance(x, A | B)` necesita 3.10+ en tiempo de EJECUCION.

    A diferencia de las anotaciones, esto no lo aplaza `from __future__`: la
    union se construye al llamar. Funciona en 3.10, pero la forma con tupla es
    la que se usa en el proyecto (por eso UP038 esta en la lista de ignorados
    de ruff), y conviene que siga siendo consistente.
    """
    infractores = []
    for archivo in _archivos_python():
        for n, linea in enumerate(archivo.read_text(encoding="utf-8").splitlines(), 1):
            if re.search(r"isinstance\([^,]+,\s*[\w.]+\s*\|", linea):
                infractores.append(f"{archivo.relative_to(ROOT)}:{n}")
    assert not infractores, (
        "isinstance con union por barra; el proyecto usa la forma con tupla: "
        + ", ".join(infractores)
    )


def test_ruff_apunta_a_la_version_minima():
    """`ruff.toml` debe apuntar a 3.10, no a la version de la mayoria.

    Es la causa raiz del fallo original: con py311 el linter aprueba (y
    ademas propone) codigo que no corre en la imagen de Spark.
    """
    config = (ROOT / "ruff.toml").read_text(encoding="utf-8")
    objetivo = re.search(r'target-version\s*=\s*"py(\d)(\d+)"', config)
    assert objetivo, "ruff.toml no declara target-version"
    version = (int(objetivo.group(1)), int(objetivo.group(2)))
    assert version <= MIN_PYTHON, (
        f"ruff apunta a {version[0]}.{version[1]} pero la imagen de Spark usa "
        f"{MIN_PYTHON[0]}.{MIN_PYTHON[1]}. Hay que apuntar al minimo comun."
    )
