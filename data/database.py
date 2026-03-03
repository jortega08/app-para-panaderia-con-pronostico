"""
database.py
-----------
Capa de datos: gestión de la base de datos SQLite.
Responsabilidad única: crear, leer y escribir registros de producción.
"""

import sqlite3
import os
from datetime import datetime
from pathlib import Path


# Ruta de la base de datos en la misma carpeta del proyecto
DB_PATH = Path(__file__).parent / "panaderia.db"


def get_connection() -> sqlite3.Connection:
    """Retorna una conexión a la base de datos con row_factory para dict-like access."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def inicializar_base_de_datos() -> None:
    """
    Crea las tablas necesarias si no existen.
    Se llama una vez al iniciar la aplicación.
    """
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS productos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT UNIQUE NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS registros_diarios (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha      TEXT    NOT NULL,
                dia_semana TEXT    NOT NULL,
                producto   TEXT    NOT NULL,
                producido  INTEGER NOT NULL,
                vendido    INTEGER NOT NULL,
                sobrante   INTEGER GENERATED ALWAYS AS (producido - vendido) VIRTUAL,
                observaciones TEXT DEFAULT '',
                UNIQUE(fecha, producto)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS alertas (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha      TEXT NOT NULL,
                producto   TEXT NOT NULL,
                tipo       TEXT NOT NULL,
                mensaje    TEXT NOT NULL
            )
        """)

        # Productos de ejemplo para arrancar
        productos_iniciales = [
            ("Pan Francés",), ("Pan Dulce",), ("Croissant",), ("Integral",)
        ]
        conn.executemany(
            "INSERT OR IGNORE INTO productos (nombre) VALUES (?)",
            productos_iniciales
        )
        conn.commit()


def guardar_registro(fecha: str, producto: str,
                     producido: int, vendido: int,
                     observaciones: str = "") -> bool:
    """
    Inserta o actualiza un registro diario.
    Retorna True si fue exitoso, False si hubo error.
    """
    dia_semana = datetime.strptime(fecha, "%Y-%m-%d").strftime("%A")
    dias_es = {
        "Monday": "Lunes", "Tuesday": "Martes", "Wednesday": "Miércoles",
        "Thursday": "Jueves", "Friday": "Viernes",
        "Saturday": "Sábado", "Sunday": "Domingo"
    }
    dia_semana = dias_es.get(dia_semana, dia_semana)

    try:
        with get_connection() as conn:
            conn.execute("""
                INSERT INTO registros_diarios
                    (fecha, dia_semana, producto, producido, vendido, observaciones)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(fecha, producto) DO UPDATE SET
                    producido     = excluded.producido,
                    vendido       = excluded.vendido,
                    observaciones = excluded.observaciones
            """, (fecha, dia_semana, producto, producido, vendido, observaciones))
            conn.commit()
        return True
    except Exception as e:
        print(f"[ERROR] guardar_registro: {e}")
        return False


def obtener_registros(producto: str = None, dias: int = 30) -> list[dict]:
    """
    Retorna los últimos `dias` registros, opcionalmente filtrados por producto.
    """
    query = """
        SELECT fecha, dia_semana, producto, producido, vendido, sobrante, observaciones
        FROM registros_diarios
        WHERE fecha >= date('now', ? )
        {filtro}
        ORDER BY fecha DESC, producto ASC
    """
    filtro = "AND producto = ?" if producto else ""
    query = query.format(filtro=filtro)
    params = (f"-{dias} days",)
    if producto:
        params += (producto,)

    with get_connection() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def obtener_productos() -> list[str]:
    """Retorna la lista de productos registrados."""
    with get_connection() as conn:
        rows = conn.execute("SELECT nombre FROM productos ORDER BY nombre").fetchall()
    return [r["nombre"] for r in rows]


def agregar_producto(nombre: str) -> bool:
    """Agrega un nuevo producto. Retorna False si ya existe."""
    try:
        with get_connection() as conn:
            conn.execute("INSERT INTO productos (nombre) VALUES (?)", (nombre,))
            conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def obtener_resumen_por_dia_semana(producto: str) -> dict:
    """
    Promedio de ventas agrupado por día de la semana para un producto.
    Útil para el modelo de pronóstico por día.
    """
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT dia_semana,
                   ROUND(AVG(vendido), 1) AS promedio_vendido,
                   COUNT(*) AS muestras
            FROM registros_diarios
            WHERE producto = ?
            GROUP BY dia_semana
        """, (producto,)).fetchall()
    return {r["dia_semana"]: {"promedio": r["promedio_vendido"],
                               "muestras": r["muestras"]} for r in rows}


def contar_registros(producto: str) -> int:
    """Cuenta los días de historial disponibles para un producto."""
    with get_connection() as conn:
        result = conn.execute(
            "SELECT COUNT(*) as total FROM registros_diarios WHERE producto = ?",
            (producto,)
        ).fetchone()
    return result["total"] if result else 0
