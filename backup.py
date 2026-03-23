"""
backup.py
---------
Sistema de respaldo de la base de datos.

Notas operativas:
  - SQLite: usa el API nativo de backup para copias consistentes con WAL.
  - PostgreSQL: la app no ejecuta backups/restores directos; deben hacerse con
    pg_dump o snapshots del proveedor.
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path

from data.db_adapter import get_database_info


BACKUP_DIR = Path(__file__).parent / "backups"
BACKUP_CONFIG_FILE = BACKUP_DIR / "backup_config.json"
DEFAULT_RETENTION_DAYS = 30
MAX_BACKUPS = 50


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


def crear_backup(nota: str = "") -> dict:
    """Crea un backup de la base de datos. Retorna info del backup."""
    _ensure_backup_dir()
    info = _database_info()

    if info.get("type") != "sqlite":
        return {
            "ok": False,
            "error": (
                "El backup desde la app solo esta disponible para SQLite. "
                "En PostgreSQL usa pg_dump o snapshots administrados."
            ),
        }

    db_path = _sqlite_db_path()
    if not db_path or not db_path.exists():
        return {"ok": False, "error": "Base de datos no encontrada"}

    ahora = datetime.now()
    timestamp = ahora.strftime("%Y%m%d_%H%M%S")
    nombre = f"panaderia_backup_{timestamp}.db"
    destino = BACKUP_DIR / nombre

    try:
        _sqlite_backup_to_file(db_path, destino)

        meta = {
            "archivo": nombre,
            "fecha": ahora.strftime("%Y-%m-%d"),
            "hora": ahora.strftime("%H:%M:%S"),
            "timestamp": timestamp,
            "tamano_bytes": destino.stat().st_size,
            "nota": nota,
            "motor": info.get("type"),
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
            db_file = BACKUP_DIR / meta["archivo"]
            meta["disponible"] = db_file.exists()
            meta["tamano_mb"] = round(meta.get("tamano_bytes", 0) / (1024 * 1024), 2)
            backups.append(meta)
        except Exception:
            continue

    return backups


def restaurar_backup(timestamp: str) -> dict:
    """Restaura la base de datos desde un backup."""
    _ensure_backup_dir()
    info = _database_info()

    if info.get("type") != "sqlite":
        return {
            "ok": False,
            "error": (
                "La restauracion desde la app no esta disponible para PostgreSQL. "
                "Usa restore de pg_dump o snapshots del proveedor."
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
    """Elimina un backup especifico."""
    _ensure_backup_dir()

    db_file = BACKUP_DIR / f"panaderia_backup_{timestamp}.db"
    meta_file = BACKUP_DIR / f"panaderia_backup_{timestamp}.json"

    try:
        if db_file.exists():
            db_file.unlink()
        if meta_file.exists():
            meta_file.unlink()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def limpiar_backups_antiguos(dias_retencion: int = DEFAULT_RETENTION_DAYS) -> dict:
    """Elimina backups mas antiguos que los dias de retencion, manteniendo al menos los ultimos 5."""
    _ensure_backup_dir()

    backups = listar_backups()
    if len(backups) <= 5:
        return {"ok": True, "eliminados": 0}

    eliminados = 0
    ahora = datetime.now()

    for backup in backups[5:]:
        try:
            fecha = datetime.strptime(backup["fecha"], "%Y-%m-%d")
            dias = (ahora - fecha).days
            if dias > dias_retencion:
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

    tamano_total = sum(
        (BACKUP_DIR / b["archivo"]).stat().st_size
        for b in backups if (BACKUP_DIR / b["archivo"]).exists()
    )

    return {
        "total_backups": len(backups),
        "ultimo_backup": backups[0] if backups else None,
        "tamano_total_mb": round(tamano_total / (1024 * 1024), 2),
        "directorio": str(BACKUP_DIR),
        "motor_activo": info.get("type"),
        "backup_en_app_disponible": bool(info.get("supports_app_file_backup")),
    }
