"""
backup.py
---------
Sistema de respaldo de la base de datos.

Motores soportados:
  - SQLite : copia consistente via el API nativo de backup (con soporte WAL).
  - PostgreSQL : volcado SQL via pg_dump (requiere postgresql-client instalado).

Importante para PostgreSQL en multi-tenant:
  - La CREACION de backups (pg_dump) si esta disponible desde la app.
  - La RESTAURACION desde la UI esta DESHABILITADA en produccion PostgreSQL.
    Usa pg_restore / psql segun el runbook en docs/runbook-backup-restore.md.
"""

import json
import logging
import os
import shutil
import sqlite3
import subprocess
import time
from datetime import datetime
from pathlib import Path

from data.db_adapter import get_database_info


logger = logging.getLogger(__name__)

BACKUP_DIR = Path(__file__).parent / "backups"
BACKUP_CONFIG_FILE = BACKUP_DIR / "backup_config.json"
DEFAULT_RETENTION_DAYS = 30
MAX_BACKUPS = 50


# ── Helpers internos ───────────────────────────────────────────────────────────

def _ensure_backup_dir():
    BACKUP_DIR.mkdir(exist_ok=True)


def _database_info() -> dict:
    return get_database_info()


def _sqlite_db_path() -> Path | None:
    info = _database_info()
    if info.get("type") != "sqlite":
        return None
    sqlite_path = info.get("sqlite_path")
    if not sqlite_path:
        return None
    return Path(sqlite_path)


def _sqlite_backup_to_file(origen: Path, destino: Path) -> None:
    with sqlite3.connect(str(origen), timeout=30) as src:
        with sqlite3.connect(str(destino), timeout=30) as dst:
            src.backup(dst)


def _sqlite_restore_from_file(origen: Path, destino: Path) -> None:
    with sqlite3.connect(str(origen), timeout=30) as src:
        with sqlite3.connect(str(destino), timeout=30) as dst:
            src.backup(dst)


def _pg_database_url() -> str:
    """Devuelve la DATABASE_URL normalizada para psycopg2 / pg_dump."""
    url = os.environ.get("DATABASE_URL", "").strip()
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    return url


def _pg_dump_to_file(destino: Path) -> None:
    """Ejecuta pg_dump y guarda el volcado SQL en `destino`."""
    pg_url = _pg_database_url()
    if not pg_url:
        raise ValueError("DATABASE_URL no configurada")

    result = subprocess.run(
        [
            "pg_dump",
            "--no-password",
            "--format=plain",
            "--no-acl",
            "--no-owner",
            "--encoding=UTF8",
            pg_url,
        ],
        capture_output=True,
        timeout=300,
    )
    if result.returncode != 0:
        stderr = result.stderr.decode(errors="replace")[:600]
        raise subprocess.CalledProcessError(result.returncode, "pg_dump", stderr)
    destino.write_bytes(result.stdout)


def _pg_dump_disponible() -> bool:
    """Retorna True si pg_dump esta instalado y accesible en PATH."""
    return shutil.which("pg_dump") is not None


def _archivo_de_backup(timestamp: str) -> Path | None:
    """Encuentra el archivo de backup (.db o .sql) para el timestamp dado."""
    # Intentar leer desde metadata primero
    meta_file = BACKUP_DIR / f"panaderia_backup_{timestamp}.json"
    if meta_file.exists():
        try:
            with open(meta_file, "r", encoding="utf-8") as f:
                meta = json.load(f)
            candidate = BACKUP_DIR / meta["archivo"]
            if candidate.exists():
                return candidate
        except Exception:
            pass
    # Fallback: buscar por extension
    for ext in ("db", "sql"):
        candidate = BACKUP_DIR / f"panaderia_backup_{timestamp}.{ext}"
        if candidate.exists():
            return candidate
    return None


# ── API pública ────────────────────────────────────────────────────────────────

def crear_backup(nota: str = "") -> dict:
    """Crea un backup de la base de datos. Retorna info del backup."""
    _ensure_backup_dir()
    info = _database_info()
    db_type = info.get("type")

    ahora = datetime.now()
    timestamp = ahora.strftime("%Y%m%d_%H%M%S")

    if db_type == "postgresql":
        if not _pg_dump_disponible():
            return {
                "ok": False,
                "error": (
                    "pg_dump no encontrado en este servidor. "
                    "Instala postgresql-client o usa pg_dump manualmente. "
                    "Ver docs/runbook-backup-restore.md."
                ),
            }
        pg_url = _pg_database_url()
        if not pg_url:
            return {"ok": False, "error": "DATABASE_URL no configurada"}

        nombre = f"panaderia_backup_{timestamp}.sql"
        destino = BACKUP_DIR / nombre
        try:
            _pg_dump_to_file(destino)
        except subprocess.CalledProcessError as e:
            return {"ok": False, "error": f"pg_dump fallo (codigo {e.returncode}): {e.output}"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    elif db_type == "sqlite":
        db_path = _sqlite_db_path()
        if not db_path or not db_path.exists():
            return {"ok": False, "error": "Base de datos SQLite no encontrada"}

        nombre = f"panaderia_backup_{timestamp}.db"
        destino = BACKUP_DIR / nombre
        try:
            _sqlite_backup_to_file(db_path, destino)
        except Exception as e:
            return {"ok": False, "error": str(e)}

    else:
        return {"ok": False, "error": f"Motor de base de datos no soportado: {db_type}"}

    try:
        meta = {
            "archivo": nombre,
            "fecha": ahora.strftime("%Y-%m-%d"),
            "hora": ahora.strftime("%H:%M:%S"),
            "timestamp": timestamp,
            "tamano_bytes": destino.stat().st_size,
            "nota": nota,
            "motor": db_type,
        }
        meta_file = BACKUP_DIR / f"panaderia_backup_{timestamp}.json"
        with open(meta_file, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
        return {"ok": True, "backup": meta}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def listar_backups() -> list[dict]:
    """Lista todos los backups disponibles, del mas reciente al mas antiguo."""
    _ensure_backup_dir()

    backups = []
    for f in sorted(BACKUP_DIR.glob("*.json"), reverse=True):
        if f.name == "backup_config.json":
            continue
        try:
            with open(f, "r", encoding="utf-8") as fp:
                meta = json.load(fp)
            backup_file = BACKUP_DIR / meta["archivo"]
            meta["disponible"] = backup_file.exists()
            meta["tamano_mb"] = round(meta.get("tamano_bytes", 0) / (1024 * 1024), 2)
            backups.append(meta)
        except Exception:
            continue

    return backups


def restaurar_backup(timestamp: str) -> dict:
    """Restaura la base de datos desde un backup.

    Solo disponible para SQLite. En PostgreSQL la restauracion debe hacerse
    con psql o pg_restore segun el runbook (docs/runbook-backup-restore.md).
    """
    _ensure_backup_dir()
    info = _database_info()

    if info.get("type") != "sqlite":
        return {
            "ok": False,
            "error": (
                "La restauracion desde la app NO esta disponible para PostgreSQL "
                "en entornos de produccion multi-tenant. "
                "Usa psql o pg_restore manualmente. "
                "Ver docs/runbook-backup-restore.md."
            ),
        }

    nombre = f"panaderia_backup_{timestamp}.db"
    origen = BACKUP_DIR / nombre

    if not origen.exists():
        return {"ok": False, "error": "Backup no encontrado"}

    db_path = _sqlite_db_path()
    if not db_path:
        return {"ok": False, "error": "Ruta de base SQLite no disponible"}

    try:
        respaldo_previo = crear_backup(nota="Auto-backup antes de restauracion")
        if not respaldo_previo.get("ok"):
            return respaldo_previo
        _sqlite_restore_from_file(origen, db_path)
        return {"ok": True, "restaurado": nombre}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def eliminar_backup(timestamp: str) -> dict:
    """Elimina un backup especifico (archivo + metadata)."""
    _ensure_backup_dir()

    meta_file = BACKUP_DIR / f"panaderia_backup_{timestamp}.json"
    archivo_backup = _archivo_de_backup(timestamp)

    try:
        if archivo_backup and archivo_backup.exists():
            archivo_backup.unlink()
        if meta_file.exists():
            meta_file.unlink()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def limpiar_backups_antiguos(dias_retencion: int = DEFAULT_RETENTION_DAYS) -> dict:
    """Elimina backups mas antiguos que los dias de retencion, conservando al menos los 5 mas recientes."""
    _ensure_backup_dir()

    backups = listar_backups()
    if len(backups) <= 5:
        return {"ok": True, "eliminados": 0}

    eliminados = 0
    ahora = datetime.now()

    for backup in backups[5:]:
        try:
            fecha = datetime.strptime(backup["fecha"], "%Y-%m-%d")
            if (ahora - fecha).days > dias_retencion:
                eliminar_backup(backup["timestamp"])
                eliminados += 1
        except Exception:
            continue

    backups = listar_backups()
    while len(backups) > MAX_BACKUPS:
        eliminar_backup(backups[-1]["timestamp"])
        backups = listar_backups()
        eliminados += 1

    return {"ok": True, "eliminados": eliminados}


def obtener_info_backup() -> dict:
    """Informacion general del sistema de backups."""
    _ensure_backup_dir()
    backups = listar_backups()
    info = _database_info()
    db_type = info.get("type")

    tamano_total = sum(
        (BACKUP_DIR / b["archivo"]).stat().st_size
        for b in backups if (BACKUP_DIR / b["archivo"]).exists()
    )

    pg_dump_ok = _pg_dump_disponible() if db_type == "postgresql" else False

    return {
        "total_backups": len(backups),
        "ultimo_backup": backups[0] if backups else None,
        "tamano_total_mb": round(tamano_total / (1024 * 1024), 2),
        "directorio": str(BACKUP_DIR),
        "motor_activo": db_type,
        # SQLite: backup y restore desde UI. PostgreSQL: backup via pg_dump, restore manual.
        "backup_en_app_disponible": True,
        "restore_en_app_disponible": db_type == "sqlite",
        "pg_dump_disponible": pg_dump_ok,
    }


def ejecutar_ciclo_jobs(nota: str | None = None, dias_retencion: int | None = None) -> dict:
    """Ejecuta un ciclo de jobs externos: backup y limpieza de retencion."""
    retention_days = int(dias_retencion or os.environ.get("BACKUP_RETENTION_DAYS", DEFAULT_RETENTION_DAYS))
    nota_resuelta = nota or f"Backup automatico externo - {datetime.now().strftime('%Y-%m-%d')}"
    resultado = crear_backup(nota_resuelta)
    if not resultado.get("ok"):
        logger.error("Job de backup fallo: %s", resultado.get("error", "Error desconocido"))
        return resultado

    limpieza = limpiar_backups_antiguos(dias_retencion=retention_days)
    logger.info(
        "Job de backup completado: archivo=%s eliminados=%s",
        ((resultado.get("backup") or {}).get("archivo") or ""),
        limpieza.get("eliminados", 0),
    )
    return {
        "ok": True,
        "backup": resultado.get("backup"),
        "cleanup": limpieza,
    }


def ejecutar_jobs_programados_externos(intervalo_segundos: int = 60) -> None:
    """Loop simple para correr jobs fuera del proceso web sin APScheduler."""
    runtime_dir = Path(__file__).parent / "runtime"
    runtime_dir.mkdir(exist_ok=True)
    state_file = runtime_dir / "job_backup_state.json"
    backup_hour = int(os.environ.get("BACKUP_AUTO_HOUR", "23") or 23)
    intervalo = max(int(intervalo_segundos or 60), 15)

    logger.info(
        "Runner externo de jobs activo: backup diario programado a las %02d:00, polling=%ss",
        backup_hour,
        intervalo,
    )

    while True:
        now = datetime.now()
        today_key = now.strftime("%Y-%m-%d")
        last_run = ""
        if state_file.exists():
            try:
                last_run = json.loads(state_file.read_text(encoding="utf-8")).get("last_backup_date", "")
            except Exception:
                last_run = ""

        if now.hour == backup_hour and last_run != today_key:
            resultado = ejecutar_ciclo_jobs()
            if resultado.get("ok"):
                state_file.write_text(
                    json.dumps({"last_backup_date": today_key}, ensure_ascii=True, indent=2),
                    encoding="utf-8",
                )
        time.sleep(intervalo)
