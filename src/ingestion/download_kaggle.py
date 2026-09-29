"""Descarga automatizada del dataset desde la API de Kaggle.

Requisito del enunciado: "La descarga no se hace a mano: se realiza con la API
de Kaggle desde el propio pipeline. El token de Kaggle se guarda como
credencial en Jenkins y en ningun caso puede quedar en el repositorio."

Orden de resolucion de credenciales (de mas a menos prioritario):
  1. Variables de entorno KAGGLE_USERNAME / KAGGLE_KEY
  2. /run/secrets/kaggle.json     (Docker secret / Jenkins secret file)
  3. $KAGGLE_JSON                 (ruta al archivo que inyecta Jenkins)
  4. ./secrets/kaggle.json        (uso local; la carpeta esta en .gitignore)
  5. ~/.kaggle/kaggle.json        (instalacion estandar del CLI de Kaggle)

Si no hay credenciales y ALLOW_SYNTHETIC_FALLBACK=1, se genera un dataset
sintetico equivalente para no bloquear el desarrollo ni el pipeline de CI.
"""
from __future__ import annotations

import contextlib
import json
import os
import stat
import sys
import zipfile
from pathlib import Path

from src.common import config
from src.common.logging_conf import setup_logging

log = setup_logging("ingestion.download")

CREDENTIAL_PATHS = (
    Path("/run/secrets/kaggle.json"),
    Path("./secrets/kaggle.json"),
    Path.home() / ".kaggle" / "kaggle.json",
)


# ---------------------------------------------------------------------------
# Credenciales
# ---------------------------------------------------------------------------
def _read_credential_file(path: Path) -> tuple[str, str] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        log.warning("No se pudo leer %s: %s", path, exc)
        return None
    user, key = payload.get("username"), payload.get("key")
    if user and key:
        return str(user), str(key)
    log.warning("%s no contiene 'username' y 'key'", path)
    return None


def resolve_credentials() -> tuple[str, str] | None:
    """Busca el token de Kaggle en todas las ubicaciones soportadas."""
    user = os.environ.get("KAGGLE_USERNAME", "").strip()
    key = os.environ.get("KAGGLE_KEY", "").strip()
    if user and key:
        log.info("Credenciales de Kaggle tomadas de variables de entorno")
        return user, key

    candidates = list(CREDENTIAL_PATHS)
    injected = os.environ.get("KAGGLE_JSON", "").strip()
    if injected:
        candidates.insert(0, Path(injected))

    for path in candidates:
        if path.is_file():
            creds = _read_credential_file(path)
            if creds:
                log.info("Credenciales de Kaggle tomadas de %s", path)
                return creds

    return None


def install_credentials(user: str, key: str) -> Path:
    """Escribe ~/.kaggle/kaggle.json con permisos 600.

    La libreria de Kaggle exige este archivo con permisos restringidos; emite
    una advertencia y en algunas versiones falla si el archivo es legible por
    todos.
    """
    target_dir = Path.home() / ".kaggle"
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / "kaggle.json"
    target.write_text(json.dumps({"username": user, "key": key}), encoding="utf-8")
    # Windows no implementa el modo POSIX: que falle no es un error
    with contextlib.suppress(OSError):
        target.chmod(stat.S_IRUSR | stat.S_IWUSR)  # 600
    # La libreria tambien lee estas variables directamente
    os.environ["KAGGLE_USERNAME"] = user
    os.environ["KAGGLE_KEY"] = key
    os.environ.setdefault("KAGGLE_CONFIG_DIR", str(target_dir))
    return target


# ---------------------------------------------------------------------------
# Descarga
# ---------------------------------------------------------------------------
def _unzip_all(zip_path: Path, dest: Path) -> list[Path]:
    extracted: list[Path] = []
    with zipfile.ZipFile(zip_path) as zf:
        for member in zf.namelist():
            # Proteccion contra zip-slip: nunca escribir fuera de `dest`
            out = (dest / member).resolve()
            if not str(out).startswith(str(dest.resolve())):
                raise ValueError(f"Entrada de zip sospechosa: {member}")
            zf.extract(member, dest)
            extracted.append(dest / member)
    log.info("Descomprimido %s -> %s archivo(s)", zip_path.name, len(extracted))
    return extracted


ZIP_MAGIC = b"PK\x03\x04"
CHUNK_BYTES = 1024 * 1024          # 1 MiB por trozo
KAGGLE_API = "https://www.kaggle.com/api/v1"


def _is_zip(path: Path) -> bool:
    """True si el archivo empieza por la firma de un ZIP."""
    try:
        with path.open("rb") as fh:
            return fh.read(4) == ZIP_MAGIC
    except OSError:
        return False


def stream_download(dataset: str, target_file: str, dest: Path,
                    user: str, key: str,
                    timeout: tuple[int, int] = (30, 600)) -> Path:
    """Descarga un archivo del dataset en STREAMING y devuelve la ruta cruda.

    Por que no se usa `KaggleApi.dataset_download_file`:

    esa funcion acumula la respuesta completa en memoria antes de escribirla.
    Con el CSV de US Accidents (1,2 GB comprimidos) eso hace que el contenedor
    de ingesta llegue a su limite y el kernel lo mate. Medido: el contenedor
    alcanza exactamente 700 MiB / 700 MiB y muere con SIGKILL (exit 137), sin
    ningun mensaje que mencione la memoria.

    Aqui se llama al mismo endpoint REST de Kaggle con `stream=True` y se
    escribe por trozos de 1 MiB, de modo que la memoria usada es constante e
    independiente del tamano del dataset. Ademas el progreso es visible, lo que
    importa en una descarga de varios minutos.
    """
    import requests  # noqa: PLC0415 - solo hace falta en este camino

    url = f"{KAGGLE_API}/datasets/download/{dataset}"
    params = {"file_name": target_file} if target_file else {}

    log.info("Descargando en streaming desde %s (archivo %s)...", dataset,
             target_file or "<todo el dataset>")

    tmp = dest / f".{target_file or 'dataset'}.part"
    tmp.parent.mkdir(parents=True, exist_ok=True)

    with requests.get(url, params=params, auth=(user, key), stream=True,
                      timeout=timeout, allow_redirects=True) as resp:
        if resp.status_code == 403:
            raise PermissionError(
                f"Kaggle devolvio 403 para '{dataset}'. Lo habitual es no haber "
                "aceptado las condiciones del dataset: entre a "
                f"https://www.kaggle.com/datasets/{dataset} con la misma cuenta "
                "del token y acepte las reglas."
            )
        if resp.status_code == 404:
            raise FileNotFoundError(
                f"Kaggle devolvio 404. Verifique el slug '{dataset}' y el nombre "
                f"del archivo '{target_file}'."
            )
        resp.raise_for_status()

        total = int(resp.headers.get("Content-Length") or 0)
        escritos = 0
        siguiente_aviso = 100 * 1024 * 1024        # avisar cada 100 MB

        with tmp.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=CHUNK_BYTES):
                if not chunk:
                    continue
                fh.write(chunk)
                escritos += len(chunk)
                if escritos >= siguiente_aviso:
                    if total:
                        log.info("  %.0f MB de %.0f MB (%.0f%%)",
                                 escritos / 1048576, total / 1048576,
                                 100 * escritos / total)
                    else:
                        log.info("  %.0f MB descargados", escritos / 1048576)
                    siguiente_aviso += 100 * 1024 * 1024

    if total and escritos != total:
        tmp.unlink(missing_ok=True)
        raise OSError(
            f"Descarga incompleta: {escritos} de {total} bytes. Reintente."
        )

    log.info("Descarga terminada: %.1f MB", escritos / 1048576)

    # Kaggle entrega un ZIP o el archivo crudo segun el tamano; se decide por
    # la firma del archivo y no por la extension, que no siempre viene.
    final = dest / (f"{target_file}.zip" if _is_zip(tmp) else target_file)
    tmp.replace(final)
    return final


def download_dataset(dataset: str | None = None,
                     target_file: str | None = None,
                     data_dir: str | None = None,
                     force: bool = False) -> Path:
    """Descarga el dataset de Kaggle y devuelve la ruta al CSV resultante.

    Si el archivo ya existe y `force` es False, no se vuelve a descargar: eso
    hace que el pipeline de Jenkins sea rapido en builds sucesivos sin perder
    reproducibilidad (basta con FORCE_DOWNLOAD=1 para refrescar).
    """
    dataset = dataset or config.kaggle.dataset
    target_file = target_file or config.kaggle.target_file
    dest = Path(data_dir or config.kaggle.data_dir)
    dest.mkdir(parents=True, exist_ok=True)

    final_csv = dest / target_file
    if final_csv.is_file() and not force:
        size_mb = final_csv.stat().st_size / 1024 / 1024
        log.info("El archivo ya existe (%.1f MB), se omite la descarga: %s",
                 size_mb, final_csv)
        return final_csv

    creds = resolve_credentials()
    if not creds:
        raise PermissionError(
            "No se encontraron credenciales de Kaggle. Defina KAGGLE_USERNAME y "
            "KAGGLE_KEY, o monte kaggle.json en /run/secrets/kaggle.json. "
            "Consulte el README, seccion 'Credenciales'."
        )
    user, key = creds
    install_credentials(user, key)

    # --- camino principal: streaming propio -------------------------------
    # Es el unico que funciona con memoria acotada; ver stream_download().
    try:
        stream_download(dataset, target_file, dest, user, key)
    except (PermissionError, FileNotFoundError):
        # Errores de credenciales o de nombre: no tiene sentido reintentar con
        # la libreria, fallaria igual. Se propagan para que obtain_dataset()
        # decida (avisar y caer al dataset sintetico).
        raise
    except Exception as exc:  # noqa: BLE001
        # Cualquier otro fallo (cambio del endpoint, red) -> se intenta con la
        # libreria oficial, aceptando su consumo de memoria como ultimo recurso.
        log.warning("La descarga en streaming fallo (%s). Se reintenta con la "
                    "libreria oficial de Kaggle, que consume mas memoria.", exc)

        from kaggle.api.kaggle_api_extended import KaggleApi  # noqa: PLC0415

        api = KaggleApi()
        api.authenticate()
        try:
            api.dataset_download_file(dataset, target_file, path=str(dest),
                                      force=force)
        except Exception as exc2:  # noqa: BLE001
            log.warning("Fallo la descarga del archivo puntual (%s). "
                        "Se intenta el dataset completo.", exc2)
            api.dataset_download_files(dataset, path=str(dest), unzip=False,
                                       force=force)

    # Kaggle entrega .zip para archivos grandes y el .csv crudo para pequenos
    for zip_path in sorted(dest.glob("*.zip")):
        _unzip_all(zip_path, dest)
        zip_path.unlink(missing_ok=True)

    if final_csv.is_file():
        size_mb = final_csv.stat().st_size / 1024 / 1024
        log.info("Descarga lista: %s (%.1f MB)", final_csv, size_mb)
        return final_csv

    # El nombre del archivo dentro del dataset puede variar entre versiones
    csvs = sorted(dest.glob("*.csv"), key=lambda p: p.stat().st_size, reverse=True)
    if csvs:
        log.warning("No se encontro '%s'; se usa el CSV mas grande: %s",
                    target_file, csvs[0].name)
        return csvs[0]

    raise FileNotFoundError(
        f"Kaggle no entrego ningun CSV en {dest}. Verifique el slug "
        f"'{dataset}' y que haya aceptado las condiciones del dataset."
    )


def obtain_dataset(force: bool = False) -> tuple[Path, str]:
    """Punto de entrada tolerante a fallos.

    Devuelve (ruta_csv, origen) donde origen es "kaggle" o "synthetic".
    """
    try:
        return download_dataset(force=force), "kaggle"
    except (PermissionError, FileNotFoundError) as exc:
        if not config.kaggle.allow_synthetic:
            raise
        log.error("Descarga desde Kaggle no disponible: %s", exc)
        log.warning("ALLOW_SYNTHETIC_FALLBACK=1 -> se genera un dataset "
                    "sintetico para poder validar el pipeline completo.")
        from src.ingestion.synthetic import generate_synthetic_csv  # noqa: PLC0415

        # Se generan MAS filas de las pedidas a proposito. El generador inyecta
        # un 3% de registros sucios y la limpieza los descarta, asi que pedir
        # exactamente SAMPLE_SIZE deja el resultado por debajo del minimo de
        # 1.000.000 que exige el enunciado (medido: 1.000.000 generadas ->
        # 969.902 cargadas). El 12% de margen cubre el descarte con holgura.
        objetivo = max(1_000_000, config.ingest.sample_size)
        con_margen = int(objetivo * 1.12) + 1_000
        log.info("Objetivo tras limpieza: %s registros -> se generan %s filas "
                 "para absorber el 3%% de registros sucios inyectados",
                 f"{objetivo:,}", f"{con_margen:,}")
        path = generate_synthetic_csv(
            Path(config.kaggle.data_dir) / config.kaggle.target_file,
            n_rows=con_margen,
        )
        return path, "synthetic"


def main() -> int:
    force = os.environ.get("FORCE_DOWNLOAD", "").lower() in {"1", "true", "yes"}
    path, origin = obtain_dataset(force=force)
    size_mb = path.stat().st_size / 1024 / 1024
    log.info("=" * 70)
    log.info("Dataset disponible  : %s", path)
    log.info("Origen              : %s", origin)
    log.info("Tamano              : %.1f MB", size_mb)
    log.info("=" * 70)
    if origin == "synthetic":
        log.warning("ATENCION: los datos son sinteticos. Para la entrega final "
                    "configure el token de Kaggle.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
