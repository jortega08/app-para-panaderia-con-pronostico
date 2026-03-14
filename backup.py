"""
backup.py
---------
Sistema de respaldo de la base de datos.
Soporta:
  - Backup local con rotacion por fecha
  - Restauracion desde un backup
  - Limpieza de backups antiguos (retencion configurable)
  - Preparado para integracion con servicios en la nube (Google Drive, etc.)
"""

import shutil
import os
import json
from datetime import datetime
from pathlib import Path

from data.database import DB_PATH


BACKUP_DIR = Path(__file__).parent / "backups"
BACKUP_CONFIG_FILE = BACKUP_DIR / "backup_config.json"
DEFAULT_RETENTION_DAYS = 30
MAX_BACKUPS = 50


def _ensure_backup_dir():
    BACKUP_DIR.mkdir(exist_ok=True)


def crear_backup(nota: str = "") -> dict:
    """Crea un backup de la base de datos. Retorna info del backup."""
    _ensure_backup_dir()

    if not DB_PATH.exists():
        return {"ok": False, "error": "Base de datos no encontrada"}

    ahora = datetime.now()
    timestamp = ahora.strftime("%Y%m%d_%H%M%S")
    nombre = f"panaderia_backup_{timestamp}.db"
    destino = BACKUP_DIR / nombre

    try:
        shutil.copy2(DB_PATH, destino)

        # Guardar metadata
        meta = {
            "archivo": nombre,
            "fecha": ahora.strftime("%Y-%m-%d"),
            "hora": ahora.strftime("%H:%M:%S"),
            "timestamp": timestamp,
            "tamano_bytes": destino.stat().st_size,
            "nota": nota,
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
            # Verificar que el archivo .db existe
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

    nombre = f"panaderia_backup_{timestamp}.db"
    origen = BACKUP_DIR / nombre

    if not origen.exists():
        return {"ok": False, "error": "Backup no encontrado"}

    try:
        # Crear backup de seguridad antes de restaurar
        crear_backup(nota="Auto-backup antes de restauracion")

        shutil.copy2(origen, DB_PATH)
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

    for backup in backups[5:]:  # Mantener al menos los ultimos 5
        try:
            fecha = datetime.strptime(backup["fecha"], "%Y-%m-%d")
            dias = (ahora - fecha).days
            if dias > dias_retencion:
                eliminar_backup(backup["timestamp"])
                eliminados += 1
        except Exception:
            continue

    # Limitar total de backups
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

    tamano_total = sum(
        (BACKUP_DIR / b["archivo"]).stat().st_size
        for b in backups if (BACKUP_DIR / b["archivo"]).exists()
    )

    return {
        "total_backups": len(backups),
        "ultimo_backup": backups[0] if backups else None,
        "tamano_total_mb": round(tamano_total / (1024 * 1024), 2),
        "directorio": str(BACKUP_DIR),
    }
