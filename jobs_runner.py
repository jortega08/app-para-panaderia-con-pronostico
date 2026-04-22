from __future__ import annotations

import logging
import os
import sys

from backup import ejecutar_ciclo_jobs, ejecutar_jobs_programados_externos
from data.database import inicializar_base_de_datos


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [jobs] %(message)s",
)


def main(argv: list[str] | None = None) -> int:
    args = list(argv or sys.argv[1:])
    modo = (args[0] if args else "once").strip().lower()

    inicializar_base_de_datos()

    if modo == "once":
        resultado = ejecutar_ciclo_jobs()
        return 0 if resultado.get("ok") else 1

    if modo == "daemon":
        intervalo = int(os.environ.get("JOBS_POLL_SECONDS", "60") or 60)
        ejecutar_jobs_programados_externos(intervalo_segundos=intervalo)
        return 0

    print("Uso: python jobs_runner.py [once|daemon]")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
