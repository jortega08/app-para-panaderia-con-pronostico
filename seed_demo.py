"""
seed_demo.py
------------
Genera datos de demostración para los últimos 45 días.
Ejecutar UNA SOLA VEZ para poblar la base de datos.

Uso:
    python seed_demo.py
"""

import random
import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data.database import inicializar_base_de_datos, guardar_registro

# Patrones de demanda por día de la semana (factor multiplicador)
PATRON_SEMANAL = {
    "Monday": 0.85,
    "Tuesday": 0.80,
    "Wednesday": 0.90,
    "Thursday": 0.88,
    "Friday": 1.15,
    "Saturday": 1.35,
    "Sunday": 1.20,
}

# Demanda base por producto
DEMANDA_BASE = {
    "Pan Francés": 100,
    "Pan Dulce":    80,
    "Croissant":    45,
    "Integral":     35,
}


def generar_datos_demo(dias: int = 45):
    inicializar_base_de_datos()
    hoy = datetime.now().date()
    registros_creados = 0

    for delta in range(dias, 0, -1):
        fecha = hoy - timedelta(days=delta)
        dia_semana = fecha.strftime("%A")
        factor = PATRON_SEMANAL.get(dia_semana, 1.0)

        for producto, base in DEMANDA_BASE.items():
            # Demanda real con algo de ruido gaussiano
            demanda_real = int(base * factor * random.gauss(1.0, 0.12))
            demanda_real = max(10, demanda_real)

            # Producción: a veces sobreproducen, a veces quedan cortos
            sesgo = random.choice([1.0, 1.0, 1.05, 1.10, 0.95])
            producido = int(demanda_real * sesgo)
            producido = max(producido, demanda_real - 5)  # mínimo razonable

            # Vendido = mínimo entre producido y demanda real
            vendido = min(producido, demanda_real)

            obs = ""
            if dia_semana in ("Saturday", "Sunday"):
                obs = "Fin de semana — alta demanda"

            guardar_registro(
                fecha=fecha.strftime("%Y-%m-%d"),
                producto=producto,
                producido=producido,
                vendido=vendido,
                observaciones=obs
            )
            registros_creados += 1

    print(f"✅ {registros_creados} registros de demostración creados ({dias} días).")


if __name__ == "__main__":
    generar_datos_demo(45)
    print("Ahora ejecuta: python app.py")
