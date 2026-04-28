from __future__ import annotations

import logging
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from backup import ejecutar_ciclo_jobs
from data.database import inicializar_base_de_datos


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [jobs] %(message)s",
)


def ejecutar_recordatorios_encargos() -> dict:
    from app import ejecutar_recordatorios_encargos as _ejecutar
    return _ejecutar()


def ejecutar_jobs_programados(intervalo_segundos: int = 60) -> None:
    runtime_dir = Path(__file__).parent / "runtime"
    runtime_dir.mkdir(exist_ok=True)
    state_file = runtime_dir / "job_backup_state.json"
    backup_hour = int(os.environ.get("BACKUP_AUTO_HOUR", "23") or 23)
    intervalo = max(int(intervalo_segundos or 60), 15)

    logging.info(
        "Runner externo activo: recordatorios cada minuto, backup diario a las %02d:00, polling=%ss",
        backup_hour,
        intervalo,
    )

    while True:
        now = datetime.now()
        today_key = now.strftime("%Y-%m-%d")

        try:
            resultado_recordatorios = ejecutar_recordatorios_encargos()
            procesados = int(resultado_recordatorios.get("procesados", 0) or 0)
            if procesados:
                logging.info("Recordatorios de encargos procesados: %s", procesados)
        except Exception as exc:
            logging.exception("Fallo el job de recordatorios de encargos: %s", exc)

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


def main(argv: list[str] | None = None) -> int:
    args = list(argv or sys.argv[1:])
    modo = (args[0] if args else "once").strip().lower()

    inicializar_base_de_datos()

    if modo == "once":
        resultado = ejecutar_ciclo_jobs()
        return 0 if resultado.get("ok") else 1

    if modo == "daemon":
        intervalo = int(os.environ.get("JOBS_POLL_SECONDS", "60") or 60)
        ejecutar_jobs_programados(intervalo_segundos=intervalo)
        return 0

    print("Uso: python jobs_runner.py [once|daemon]")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
