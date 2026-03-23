"""
seed_demo.py
------------
Genera datos de demostracion para los ultimos dias usando:
  - productos activos de categoria Panaderia
  - precios reales ya registrados en el catalogo
  - patrones de demanda mas utiles para revisar el pronostico

Uso:
    python seed_demo.py
"""

import os
import random
import sys
import unicodedata
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data.database import (
    DB_TYPE,
    inicializar_base_de_datos,
    get_connection,
    obtener_productos_con_precio,
)


PATRON_SEMANAL = {
    "Monday": 0.84,
    "Tuesday": 0.80,
    "Wednesday": 0.88,
    "Thursday": 0.92,
    "Friday": 1.10,
    "Saturday": 1.28,
    "Sunday": 1.18,
}

FACTOR_POR_NOMBRE = (
    ("frances", 1.35),
    ("costeno", 1.18),
    ("dulce", 1.08),
    ("quesito", 0.98),
    ("rollo", 0.92),
    ("coco", 0.88),
    ("croissant", 0.72),
    ("integral", 0.62),
    ("mantecada", 0.58),
)

PERFILES_TENDENCIA = (1.08, 1.05, 1.02, 1.00, 0.98, 0.95)

DIAS_ES = {
    "Monday": "Lunes",
    "Tuesday": "Martes",
    "Wednesday": "Miercoles",
    "Thursday": "Jueves",
    "Friday": "Viernes",
    "Saturday": "Sabado",
    "Sunday": "Domingo",
}


def _normalizar(texto: str) -> str:
    texto = unicodedata.normalize("NFKD", texto or "")
    return texto.encode("ascii", "ignore").decode("ascii").strip().lower()


def _mediana(valores: list[float]) -> float:
    if not valores:
        return 1.0
    ordenados = sorted(valores)
    mitad = len(ordenados) // 2
    if len(ordenados) % 2:
        return float(ordenados[mitad])
    return float((ordenados[mitad - 1] + ordenados[mitad]) / 2)


def _factor_nombre_producto(nombre: str) -> float:
    normalizado = _normalizar(nombre)
    for clave, factor in FACTOR_POR_NOMBRE:
        if clave in normalizado:
            return factor
    return 1.0


def _factor_fin_semana(nombre: str) -> float:
    normalizado = _normalizar(nombre)
    if any(clave in normalizado for clave in ("croissant", "quesito", "dulce", "coco", "mantecada")):
        return 1.10
    if "integral" in normalizado:
        return 0.93
    return 1.02


def _construir_catalogo_panaderia() -> list[dict]:
    productos = obtener_productos_con_precio(categoria="Panaderia")
    productos = [p for p in productos if p.get("nombre")]
    if not productos:
        raise RuntimeError(
            "No hay productos activos en la categoria Panaderia. "
            "Registra primero el catalogo real antes de ejecutar el seed."
        )

    precios_reales = [float(p.get("precio") or 0) for p in productos if float(p.get("precio") or 0) > 0]
    precio_referencia = _mediana(precios_reales)

    productos_ordenados = sorted(
        productos,
        key=lambda item: (
            float(item.get("precio") or 0) if float(item.get("precio") or 0) > 0 else 10**9,
            _normalizar(item["nombre"]),
        ),
    )

    catalogo = []
    total = max(1, len(productos_ordenados) - 1)
    for indice, producto in enumerate(productos_ordenados):
        precio = float(producto.get("precio") or 0)
        factor_nombre = _factor_nombre_producto(producto["nombre"])
        factor_precio = 1.0
        if precio_referencia > 0 and precio > 0:
            factor_precio = max(0.76, min(1.28, precio_referencia / precio))

        factor_posicion = 1.18 - ((indice / total) * 0.26)
        base = round(28 * factor_nombre * factor_precio * factor_posicion + 16)
        base = max(18, min(base, 140))

        catalogo.append({
            "nombre": producto["nombre"],
            "precio": precio,
            "base": base,
            "factor_fin_semana": _factor_fin_semana(producto["nombre"]),
            "tendencia_objetivo": PERFILES_TENDENCIA[indice % len(PERFILES_TENDENCIA)],
        })

    return catalogo


def _hora_demo(rng: random.Random) -> str:
    tramo = rng.choices(
        population=[(7, 9), (9, 11), (11, 13), (13, 16), (16, 19)],
        weights=[12, 22, 18, 20, 28],
        k=1,
    )[0]
    hora = rng.randint(tramo[0], tramo[1])
    minuto = rng.randint(0, 59)
    return f"{hora:02d}:{minuto:02d}:00"


def _demanda_diaria(info: dict, fecha, dias: int, posicion_dia: int, rng: random.Random) -> int:
    dia_semana = fecha.strftime("%A")
    factor_semana = PATRON_SEMANAL.get(dia_semana, 1.0)
    progreso = posicion_dia / max(1, dias - 1)
    factor_tendencia = 1.0 + ((info["tendencia_objetivo"] - 1.0) * progreso)
    factor_weekend = info["factor_fin_semana"] if dia_semana in ("Saturday", "Sunday") else 1.0
    factor_ruido = rng.gauss(1.0, 0.08)

    demanda = info["base"] * factor_semana * factor_tendencia * factor_weekend * factor_ruido

    if fecha.day in (1, 15, 30) and dia_semana in ("Friday", "Saturday"):
        demanda *= 1.10
    if rng.random() < 0.04:
        demanda *= rng.uniform(0.86, 1.18)

    return max(6, int(round(demanda)))


def _producido_y_vendido(demanda: int, rng: random.Random) -> tuple[int, int]:
    sesgo = rng.choices(
        population=[0.94, 0.98, 1.02, 1.06, 1.10],
        weights=[10, 26, 34, 20, 10],
        k=1,
    )[0]
    producido = max(4, int(round(demanda * sesgo)))

    if rng.random() < 0.10:
        producido = max(4, producido - rng.randint(2, max(3, int(demanda * 0.08))))

    vendido = min(producido, max(1, int(round(demanda * rng.uniform(0.97, 1.02)))))
    return producido, vendido


def _observacion_dia(fecha, vendido: int, producido: int) -> str:
    etiquetas = []
    dia_semana = fecha.strftime("%A")
    if dia_semana in ("Saturday", "Sunday"):
        etiquetas.append("Fin de semana")
    if fecha.day in (1, 15, 30) and dia_semana in ("Friday", "Saturday"):
        etiquetas.append("Mayor flujo")
    if producido - vendido >= max(4, int(producido * 0.12)):
        etiquetas.append("Sobrante alto")
    elif vendido == producido:
        etiquetas.append("Venta completa")
    return ", ".join(etiquetas)


def _insertar_registro_demo(conn, fecha_str: str, producto: str, producido: int, vendido: int, observaciones: str) -> None:
    fecha = datetime.strptime(fecha_str, "%Y-%m-%d")
    dia_semana = DIAS_ES.get(fecha.strftime("%A"), fecha.strftime("%A"))
    conn.execute(
        """
        INSERT INTO registros_diarios
            (fecha, dia_semana, producto, producido, vendido, observaciones)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(fecha, producto) DO UPDATE SET
            producido = excluded.producido,
            vendido = excluded.vendido,
            observaciones = excluded.observaciones
        """,
        (fecha_str, dia_semana, producto, producido, vendido, observaciones),
    )


def _insertar_ventas_demo(conn, fecha_str: str, producto: str, precio_unitario: float, vendido: int, rng: random.Random) -> int:
    ventas_creadas = 0
    panes_restantes = vendido
    while panes_restantes > 0:
        cantidad = min(rng.randint(1, 6), panes_restantes)
        conn.execute(
            """
            INSERT INTO ventas
                (fecha, hora, producto, cantidad, precio_unitario, total, registrado_por)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                fecha_str,
                _hora_demo(rng),
                producto,
                cantidad,
                precio_unitario,
                round(cantidad * precio_unitario, 2),
                "Cajero",
            ),
        )
        panes_restantes -= cantidad
        ventas_creadas += 1
    return ventas_creadas


def _tablas_existentes(conn, candidatas: list[str]) -> list[str]:
    if not candidatas:
        return []

    if DB_TYPE == "postgresql":
        rows = conn.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = current_schema()
        """).fetchall()
    else:
        rows = conn.execute("""
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
        """).fetchall()

    existentes = set()
    for row in rows:
        nombre = None
        try:
            nombre = row["table_name"]
        except (KeyError, TypeError, IndexError):
            nombre = None
        if not nombre:
            try:
                nombre = row["name"]
            except (KeyError, TypeError, IndexError):
                nombre = None
        if nombre:
            existentes.add(str(nombre))

    return [tabla for tabla in candidatas if tabla in existentes]


def _limpiar_datos_demo(conn) -> None:
    tablas = _tablas_existentes(
        conn,
        ["venta_item_modificaciones", "ventas", "registros_diarios"],
    )
    if not tablas:
        return

    if DB_TYPE == "postgresql":
        conn.execute(
            "TRUNCATE TABLE " + ", ".join(tablas) + " RESTART IDENTITY CASCADE"
        )
        return

    for tabla in tablas:
        conn.execute(f"DELETE FROM {tabla}")


def generar_datos_demo(dias: int = 45, semilla: int = 42):
    inicializar_base_de_datos()
    rng = random.Random(semilla)
    catalogo = _construir_catalogo_panaderia()

    with get_connection() as conn:
        _limpiar_datos_demo(conn)
        conn.commit()

    hoy = datetime.now().date()
    registros_creados = 0
    ventas_creadas = 0

    with get_connection() as conn:
        for offset in range(dias, 0, -1):
            fecha = hoy - timedelta(days=offset)
            fecha_str = fecha.strftime("%Y-%m-%d")
            posicion_dia = dias - offset

            for info in catalogo:
                demanda = _demanda_diaria(info, fecha, dias, posicion_dia, rng)
                producido, vendido = _producido_y_vendido(demanda, rng)
                observaciones = _observacion_dia(fecha, vendido, producido)

                _insertar_registro_demo(
                    conn,
                    fecha_str=fecha_str,
                    producto=info["nombre"],
                    producido=producido,
                    vendido=vendido,
                    observaciones=observaciones,
                )
                registros_creados += 1

                ventas_creadas += _insertar_ventas_demo(
                    conn,
                    fecha_str=fecha_str,
                    producto=info["nombre"],
                    precio_unitario=info["precio"],
                    vendido=vendido,
                    rng=rng,
                )

        conn.commit()

    print("Datos de demostracion creados:")
    print(f"  {registros_creados} registros de produccion ({dias} dias)")
    print(f"  {ventas_creadas} ventas individuales")
    print("  Productos sembrados con precios reales:")
    for info in catalogo:
        print(
            f"    - {info['nombre']}: precio={info['precio']}, "
            f"base={info['base']}, tendencia={info['tendencia_objetivo']}"
        )
    print("\nNota: el seed inserta historico para pronostico sin ajustar inventario actual.")
    print("Ahora ejecuta: python app.py")


if __name__ == "__main__":
    generar_datos_demo(45)
