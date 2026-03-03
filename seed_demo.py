"""
seed_demo.py
------------
Genera datos de demostracion para los ultimos 45 dias.
Incluye: registros de produccion, ventas individuales y usuarios.

Uso:
    python seed_demo.py
"""

import random
import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data.database import (
    inicializar_base_de_datos,
    guardar_registro,
    get_connection,
)

# Patrones de demanda por dia de la semana
PATRON_SEMANAL = {
    "Monday": 0.85,
    "Tuesday": 0.80,
    "Wednesday": 0.90,
    "Thursday": 0.88,
    "Friday": 1.15,
    "Saturday": 1.35,
    "Sunday": 1.20,
}

# Demanda base y precios por producto
PRODUCTOS = {
    "Pan Frances":  {"base": 100, "precio": 8.0},
    "Pan Dulce":    {"base": 80,  "precio": 12.0},
    "Croissant":    {"base": 45,  "precio": 15.0},
    "Integral":     {"base": 35,  "precio": 10.0},
}


def generar_datos_demo(dias: int = 45):
    inicializar_base_de_datos()

    # Limpiar datos anteriores para evitar conflictos
    with get_connection() as conn:
        conn.execute("DELETE FROM ventas")
        conn.execute("DELETE FROM registros_diarios")
        conn.commit()

    hoy = datetime.now().date()
    registros_creados = 0
    ventas_creadas = 0

    for delta in range(dias, 0, -1):
        fecha = hoy - timedelta(days=delta)
        fecha_str = fecha.strftime("%Y-%m-%d")
        dia_semana = fecha.strftime("%A")
        factor = PATRON_SEMANAL.get(dia_semana, 1.0)

        for producto, info in PRODUCTOS.items():
            # Demanda real con ruido
            demanda_real = int(info["base"] * factor * random.gauss(1.0, 0.12))
            demanda_real = max(10, demanda_real)

            # Produccion: a veces sobreproducen, a veces quedan cortos
            sesgo = random.choice([1.0, 1.0, 1.05, 1.10, 0.95])
            producido = int(demanda_real * sesgo)
            producido = max(producido, demanda_real - 5)

            # Vendido = minimo entre producido y demanda real
            vendido = min(producido, demanda_real)

            obs = ""
            if dia_semana in ("Saturday", "Sunday"):
                obs = "Fin de semana"

            # Registro diario de produccion
            guardar_registro(
                fecha=fecha_str,
                producto=producto,
                producido=producido,
                vendido=vendido,
                observaciones=obs
            )
            registros_creados += 1

            # Simular ventas individuales del cajero
            panes_restantes = vendido
            with get_connection() as conn:
                while panes_restantes > 0:
                    # Cada venta es de 1-6 panes
                    cant = min(random.randint(1, 6), panes_restantes)
                    hora = f"{random.randint(7, 19):02d}:{random.randint(0, 59):02d}:00"

                    conn.execute("""
                        INSERT INTO ventas
                            (fecha, hora, producto, cantidad,
                             precio_unitario, total, registrado_por)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (fecha_str, hora, producto, cant,
                          info["precio"], cant * info["precio"], "Cajero"))

                    panes_restantes -= cant
                    ventas_creadas += 1
                conn.commit()

    print(f"Datos de demostracion creados:")
    print(f"  {registros_creados} registros de produccion ({dias} dias)")
    print(f"  {ventas_creadas} ventas individuales")
    print(f"\nAhora ejecuta: python app.py")


if __name__ == "__main__":
    generar_datos_demo(45)
