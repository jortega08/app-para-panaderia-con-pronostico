"""
database.py
-----------
Capa de datos: gestion de la base de datos SQLite.
Tablas:
  - productos: catalogo con precios
  - usuarios: cajeros y panaderos con PIN
  - ventas: registro individual de cada venta (cajero)
  - registros_diarios: produccion diaria por producto (panadero)
  - alertas: reservada para futuras alertas
"""

import sqlite3
import os
from datetime import datetime
from pathlib import Path


DB_PATH = Path(__file__).parent / "panaderia.db"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


# ──────────────────────────────────────────────
# Inicializacion
# ──────────────────────────────────────────────

def inicializar_base_de_datos() -> None:
    with get_connection() as conn:
        # Productos con precio
        conn.execute("""
            CREATE TABLE IF NOT EXISTS productos (
                id     INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT UNIQUE NOT NULL,
                precio REAL NOT NULL DEFAULT 0.0,
                activo INTEGER NOT NULL DEFAULT 1
            )
        """)

        # Usuarios con roles simples
        conn.execute("""
            CREATE TABLE IF NOT EXISTS usuarios (
                id     INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT NOT NULL,
                pin    TEXT NOT NULL,
                rol    TEXT NOT NULL CHECK(rol IN ('panadero', 'cajero'))
            )
        """)

        # Ventas individuales (registradas por el cajero)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ventas (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha           TEXT NOT NULL,
                hora            TEXT NOT NULL,
                producto        TEXT NOT NULL,
                cantidad        INTEGER NOT NULL,
                precio_unitario REAL NOT NULL,
                total           REAL NOT NULL,
                registrado_por  TEXT DEFAULT ''
            )
        """)

        # Registros diarios de produccion (panadero)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS registros_diarios (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha         TEXT NOT NULL,
                dia_semana    TEXT NOT NULL,
                producto      TEXT NOT NULL,
                producido     INTEGER NOT NULL,
                vendido       INTEGER NOT NULL,
                sobrante      INTEGER GENERATED ALWAYS AS (producido - vendido) VIRTUAL,
                observaciones TEXT DEFAULT '',
                UNIQUE(fecha, producto)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS alertas (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha    TEXT NOT NULL,
                producto TEXT NOT NULL,
                tipo     TEXT NOT NULL,
                mensaje  TEXT NOT NULL
            )
        """)

        # Migrar tabla productos existente: agregar columnas si faltan
        _migrar_productos(conn)

        # Productos iniciales con precios de ejemplo
        productos_iniciales = [
            ("Pan Frances", 8.0),
            ("Pan Dulce", 12.0),
            ("Croissant", 15.0),
            ("Integral", 10.0),
        ]
        for nombre, precio in productos_iniciales:
            conn.execute(
                "INSERT OR IGNORE INTO productos (nombre, precio) VALUES (?, ?)",
                (nombre, precio)
            )

        # Usuario admin por defecto
        existe = conn.execute(
            "SELECT COUNT(*) as c FROM usuarios"
        ).fetchone()
        if existe["c"] == 0:
            conn.execute(
                "INSERT INTO usuarios (nombre, pin, rol) VALUES (?, ?, ?)",
                ("Admin", "1234", "panadero")
            )
            conn.execute(
                "INSERT INTO usuarios (nombre, pin, rol) VALUES (?, ?, ?)",
                ("Cajero", "0000", "cajero")
            )

        conn.commit()


def _migrar_productos(conn):
    """Agrega columnas precio y activo si la tabla productos ya existia sin ellas."""
    cursor = conn.execute("PRAGMA table_info(productos)")
    columnas = [row["name"] for row in cursor.fetchall()]

    if "precio" not in columnas:
        conn.execute("ALTER TABLE productos ADD COLUMN precio REAL NOT NULL DEFAULT 0.0")
    if "activo" not in columnas:
        conn.execute("ALTER TABLE productos ADD COLUMN activo INTEGER NOT NULL DEFAULT 1")


# ──────────────────────────────────────────────
# Productos
# ──────────────────────────────────────────────

def obtener_productos() -> list[str]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT nombre FROM productos WHERE activo = 1 ORDER BY nombre"
        ).fetchall()
    return [r["nombre"] for r in rows]


def obtener_productos_con_precio() -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT nombre, precio FROM productos WHERE activo = 1 ORDER BY nombre"
        ).fetchall()
    return [dict(r) for r in rows]


def obtener_precio(producto: str) -> float:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT precio FROM productos WHERE nombre = ?", (producto,)
        ).fetchone()
    return row["precio"] if row else 0.0


def agregar_producto(nombre: str, precio: float = 0.0) -> bool:
    try:
        with get_connection() as conn:
            conn.execute(
                "INSERT INTO productos (nombre, precio) VALUES (?, ?)",
                (nombre, precio)
            )
            conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def actualizar_precio(producto: str, nuevo_precio: float) -> bool:
    try:
        with get_connection() as conn:
            conn.execute(
                "UPDATE productos SET precio = ? WHERE nombre = ?",
                (nuevo_precio, producto)
            )
            conn.commit()
        return True
    except Exception:
        return False


def eliminar_producto(producto: str) -> bool:
    """Desactiva un producto (soft delete)."""
    try:
        with get_connection() as conn:
            conn.execute(
                "UPDATE productos SET activo = 0 WHERE nombre = ?",
                (producto,)
            )
            conn.commit()
        return True
    except Exception:
        return False


# ──────────────────────────────────────────────
# Usuarios
# ──────────────────────────────────────────────

def verificar_pin(pin: str) -> dict | None:
    """Verifica un PIN y retorna el usuario si es valido."""
    with get_connection() as conn:
        row = conn.execute(
            "SELECT nombre, pin, rol FROM usuarios WHERE pin = ?", (pin,)
        ).fetchone()
    return dict(row) if row else None


def obtener_usuarios() -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT id, nombre, rol FROM usuarios ORDER BY rol, nombre"
        ).fetchall()
    return [dict(r) for r in rows]


def agregar_usuario(nombre: str, pin: str, rol: str) -> bool:
    try:
        with get_connection() as conn:
            conn.execute(
                "INSERT INTO usuarios (nombre, pin, rol) VALUES (?, ?, ?)",
                (nombre, pin, rol)
            )
            conn.commit()
        return True
    except Exception:
        return False


def eliminar_usuario(usuario_id: int) -> bool:
    try:
        with get_connection() as conn:
            conn.execute("DELETE FROM usuarios WHERE id = ?", (usuario_id,))
            conn.commit()
        return True
    except Exception:
        return False


# ──────────────────────────────────────────────
# Ventas (cajero)
# ──────────────────────────────────────────────

def registrar_venta(producto: str, cantidad: int,
                    precio_unitario: float, registrado_por: str = "") -> bool:
    """Registra una venta individual."""
    ahora = datetime.now()
    fecha = ahora.strftime("%Y-%m-%d")
    hora = ahora.strftime("%H:%M:%S")
    total = cantidad * precio_unitario

    try:
        with get_connection() as conn:
            conn.execute("""
                INSERT INTO ventas (fecha, hora, producto, cantidad,
                                    precio_unitario, total, registrado_por)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (fecha, hora, producto, cantidad, precio_unitario,
                  total, registrado_por))
            conn.commit()
        return True
    except Exception as e:
        print(f"[ERROR] registrar_venta: {e}")
        return False


def obtener_ventas_dia(fecha: str = None) -> list[dict]:
    """Retorna todas las ventas de un dia."""
    if fecha is None:
        fecha = datetime.now().strftime("%Y-%m-%d")
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT hora, producto, cantidad, precio_unitario, total, registrado_por
            FROM ventas
            WHERE fecha = ?
            ORDER BY hora DESC
        """, (fecha,)).fetchall()
    return [dict(r) for r in rows]


def obtener_resumen_ventas_dia(fecha: str = None) -> list[dict]:
    """Resumen agrupado por producto para un dia."""
    if fecha is None:
        fecha = datetime.now().strftime("%Y-%m-%d")
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT producto,
                   SUM(cantidad) as total_cantidad,
                   SUM(total) as total_dinero,
                   COUNT(*) as num_ventas
            FROM ventas
            WHERE fecha = ?
            GROUP BY producto
            ORDER BY total_dinero DESC
        """, (fecha,)).fetchall()
    return [dict(r) for r in rows]


def obtener_total_ventas_dia(fecha: str = None) -> dict:
    """Total general de ventas del dia."""
    if fecha is None:
        fecha = datetime.now().strftime("%Y-%m-%d")
    with get_connection() as conn:
        row = conn.execute("""
            SELECT COALESCE(SUM(cantidad), 0) as panes,
                   COALESCE(SUM(total), 0.0) as dinero,
                   COUNT(*) as transacciones
            FROM ventas
            WHERE fecha = ?
        """, (fecha,)).fetchone()
    return dict(row)


def obtener_vendido_dia_producto(fecha: str, producto: str) -> int:
    """Cantidad vendida de un producto en un dia (desde tabla ventas)."""
    with get_connection() as conn:
        row = conn.execute("""
            SELECT COALESCE(SUM(cantidad), 0) as vendido
            FROM ventas
            WHERE fecha = ? AND producto = ?
        """, (fecha, producto)).fetchone()
    return row["vendido"]


# ──────────────────────────────────────────────
# Registros diarios (produccion - panadero)
# ──────────────────────────────────────────────

def guardar_registro(fecha: str, producto: str,
                     producido: int, vendido: int,
                     observaciones: str = "") -> bool:
    dia_semana = datetime.strptime(fecha, "%Y-%m-%d").strftime("%A")
    dias_es = {
        "Monday": "Lunes", "Tuesday": "Martes", "Wednesday": "Miercoles",
        "Thursday": "Jueves", "Friday": "Viernes",
        "Saturday": "Sabado", "Sunday": "Domingo"
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
    query = """
        SELECT fecha, dia_semana, producto, producido, vendido,
               sobrante, observaciones
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


def obtener_resumen_por_dia_semana(producto: str) -> dict:
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
    with get_connection() as conn:
        result = conn.execute(
            "SELECT COUNT(*) as total FROM registros_diarios WHERE producto = ?",
            (producto,)
        ).fetchone()
    return result["total"] if result else 0
