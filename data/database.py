"""
database.py
-----------
Capa de datos SQLite para Panaderia Rich.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parent / "panaderia.db"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def inicializar_base_de_datos() -> None:
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS productos (
                id     INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT UNIQUE NOT NULL,
                precio REAL NOT NULL DEFAULT 0.0,
                activo INTEGER NOT NULL DEFAULT 1
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS usuarios (
                id     INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT NOT NULL,
                pin    TEXT NOT NULL,
                rol    TEXT NOT NULL CHECK(rol IN ('panadero', 'cajero'))
            )
        """)

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

        conn.execute("""
            CREATE TABLE IF NOT EXISTS registros_diarios (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha          TEXT NOT NULL,
                dia_semana     TEXT NOT NULL,
                producto       TEXT NOT NULL,
                producido      INTEGER NOT NULL,
                vendido        INTEGER NOT NULL,
                sobrante       INTEGER GENERATED ALWAYS AS (producido - vendido) VIRTUAL,
                observaciones  TEXT DEFAULT '',
                registrado_por TEXT DEFAULT '',
                registrado_en  TEXT DEFAULT '',
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

        conn.execute("""
            CREATE TABLE IF NOT EXISTS fichas_tecnicas (
                id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                producto                TEXT UNIQUE NOT NULL,
                ingredientes            TEXT DEFAULT '',
                cantidades              TEXT DEFAULT '',
                tiempo_amasado_min      INTEGER NOT NULL DEFAULT 0,
                tiempo_fermentacion_min INTEGER NOT NULL DEFAULT 0,
                temperatura_horneado_c  INTEGER NOT NULL DEFAULT 0,
                tiempo_horneado_min     INTEGER NOT NULL DEFAULT 0,
                pasos_proceso           TEXT DEFAULT '',
                actualizado_por         TEXT DEFAULT '',
                actualizado_en          TEXT DEFAULT ''
            )
        """)

        _migrar_productos(conn)
        _migrar_registros_diarios(conn)

        productos_iniciales = [
            ("Pan Frances", 8.0),
            ("Pan Dulce", 12.0),
            ("Croissant", 15.0),
            ("Integral", 10.0),
            ("Rollito", 7.0),
            ("Quesito", 9.0),
            ("Rollito con Bocadillo", 8.5),
            ("Pan Blandito", 6.5),
        ]
        for nombre, precio in productos_iniciales:
            conn.execute(
                "INSERT OR IGNORE INTO productos (nombre, precio) VALUES (?, ?)",
                (nombre, precio),
            )

        existe = conn.execute("SELECT COUNT(*) as c FROM usuarios").fetchone()
        if existe and existe["c"] == 0:
            conn.execute(
                "INSERT INTO usuarios (nombre, pin, rol) VALUES (?, ?, ?)",
                ("Admin", "1234", "panadero"),
            )
            conn.execute(
                "INSERT INTO usuarios (nombre, pin, rol) VALUES (?, ?, ?)",
                ("Cajero", "0000", "cajero"),
            )

        conn.commit()


def _migrar_productos(conn: sqlite3.Connection) -> None:
    cursor = conn.execute("PRAGMA table_info(productos)")
    columnas = [row["name"] for row in cursor.fetchall()]

    if "precio" not in columnas:
        conn.execute("ALTER TABLE productos ADD COLUMN precio REAL NOT NULL DEFAULT 0.0")
    if "activo" not in columnas:
        conn.execute("ALTER TABLE productos ADD COLUMN activo INTEGER NOT NULL DEFAULT 1")


def _migrar_registros_diarios(conn: sqlite3.Connection) -> None:
    cursor = conn.execute("PRAGMA table_info(registros_diarios)")
    columnas = [row["name"] for row in cursor.fetchall()]

    if "registrado_por" not in columnas:
        conn.execute("ALTER TABLE registros_diarios ADD COLUMN registrado_por TEXT DEFAULT ''")
    if "registrado_en" not in columnas:
        conn.execute("ALTER TABLE registros_diarios ADD COLUMN registrado_en TEXT DEFAULT ''")


# Productos

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
    return float(row["precio"]) if row else 0.0


def agregar_producto(nombre: str, precio: float = 0.0) -> bool:
    try:
        with get_connection() as conn:
            conn.execute(
                "INSERT INTO productos (nombre, precio) VALUES (?, ?)",
                (nombre, precio),
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
                (nuevo_precio, producto),
            )
            conn.commit()
        return True
    except Exception:
        return False


def eliminar_producto(producto: str) -> bool:
    try:
        with get_connection() as conn:
            conn.execute(
                "UPDATE productos SET activo = 0 WHERE nombre = ?",
                (producto,),
            )
            conn.commit()
        return True
    except Exception:
        return False


# Usuarios

def verificar_pin(pin: str) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT nombre, pin, rol FROM usuarios WHERE pin = ?",
            (pin,),
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
                (nombre, pin, rol),
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


# Ventas

def registrar_venta(
    producto: str,
    cantidad: int,
    precio_unitario: float,
    registrado_por: str = "",
) -> bool:
    ahora = datetime.now()
    fecha = ahora.strftime("%Y-%m-%d")
    hora = ahora.strftime("%H:%M:%S")
    total = cantidad * precio_unitario

    try:
        with get_connection() as conn:
            conn.execute(
                """
                INSERT INTO ventas (fecha, hora, producto, cantidad,
                                    precio_unitario, total, registrado_por)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (fecha, hora, producto, cantidad, precio_unitario, total, registrado_por),
            )
            conn.commit()
        return True
    except Exception as e:
        print(f"[ERROR] registrar_venta: {e}")
        return False


def registrar_ventas_lote(items: list[dict], registrado_por: str = "") -> bool:
    ahora = datetime.now()
    fecha = ahora.strftime("%Y-%m-%d")
    hora = ahora.strftime("%H:%M:%S")

    try:
        with get_connection() as conn:
            for item in items:
                cantidad = int(item["cantidad"])
                precio_unitario = float(item["precio"])
                total = cantidad * precio_unitario
                conn.execute(
                    """
                    INSERT INTO ventas (fecha, hora, producto, cantidad,
                                        precio_unitario, total, registrado_por)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        fecha,
                        hora,
                        item["producto"],
                        cantidad,
                        precio_unitario,
                        total,
                        registrado_por,
                    ),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[ERROR] registrar_ventas_lote: {e}")
        return False


def obtener_ventas_dia(fecha: str | None = None) -> list[dict]:
    if fecha is None:
        fecha = datetime.now().strftime("%Y-%m-%d")
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT fecha, hora, producto, cantidad, precio_unitario, total, registrado_por
            FROM ventas
            WHERE fecha = ?
            ORDER BY hora DESC
            """,
            (fecha,),
        ).fetchall()
    return [dict(r) for r in rows]


def obtener_resumen_ventas_dia(fecha: str | None = None) -> list[dict]:
    if fecha is None:
        fecha = datetime.now().strftime("%Y-%m-%d")
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT producto,
                   SUM(cantidad) as total_cantidad,
                   SUM(total) as total_dinero,
                   COUNT(*) as num_ventas
            FROM ventas
            WHERE fecha = ?
            GROUP BY producto
            ORDER BY total_dinero DESC
            """,
            (fecha,),
        ).fetchall()
    return [dict(r) for r in rows]


def obtener_total_ventas_dia(fecha: str | None = None) -> dict:
    if fecha is None:
        fecha = datetime.now().strftime("%Y-%m-%d")
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(cantidad), 0) as panes,
                   COALESCE(SUM(total), 0.0) as dinero,
                   COUNT(*) as transacciones
            FROM ventas
            WHERE fecha = ?
            """,
            (fecha,),
        ).fetchone()
    return dict(row)


def obtener_vendido_dia_producto(fecha: str, producto: str) -> int:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(cantidad), 0) as vendido
            FROM ventas
            WHERE fecha = ? AND producto = ?
            """,
            (fecha, producto),
        ).fetchone()
    return int(row["vendido"]) if row else 0


def obtener_ventas_rango(dias: int = 30, producto: str | None = None) -> list[dict]:
    query = """
        SELECT fecha, hora, producto, cantidad, precio_unitario, total, registrado_por
        FROM ventas
        WHERE fecha >= date('now', ?)
        {filtro}
        ORDER BY fecha DESC, hora DESC
    """
    filtro = "AND producto = ?" if producto else ""
    query = query.format(filtro=filtro)
    params = [f"-{dias} days"]
    if producto:
        params.append(producto)

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def obtener_totales_ventas_rango(dias: int = 30, producto: str | None = None) -> dict:
    query = """
        SELECT COALESCE(SUM(cantidad), 0) as panes,
               COALESCE(SUM(total), 0.0) as dinero,
               COUNT(*) as transacciones
        FROM ventas
        WHERE fecha >= date('now', ?)
        {filtro}
    """
    filtro = "AND producto = ?" if producto else ""
    query = query.format(filtro=filtro)
    params = [f"-{dias} days"]
    if producto:
        params.append(producto)

    with get_connection() as conn:
        row = conn.execute(query, tuple(params)).fetchone()
    return dict(row) if row else {"panes": 0, "dinero": 0.0, "transacciones": 0}


def obtener_serie_ventas_diarias(dias: int = 30, producto: str | None = None) -> list[dict]:
    query = """
        SELECT fecha,
               COALESCE(SUM(cantidad), 0) as panes,
               COALESCE(SUM(total), 0.0) as dinero,
               COUNT(*) as transacciones
        FROM ventas
        WHERE fecha >= date('now', ?)
        {filtro}
        GROUP BY fecha
        ORDER BY fecha ASC
    """
    filtro = "AND producto = ?" if producto else ""
    query = query.format(filtro=filtro)
    params = [f"-{dias} days"]
    if producto:
        params.append(producto)

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def obtener_resumen_productos_rango(dias: int = 30) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT producto,
                   COALESCE(SUM(cantidad), 0) as panes,
                   COALESCE(SUM(total), 0.0) as dinero,
                   COUNT(*) as transacciones
            FROM ventas
            WHERE fecha >= date('now', ?)
            GROUP BY producto
            ORDER BY dinero DESC
            """,
            (f"-{dias} days",),
        ).fetchall()
    return [dict(r) for r in rows]


# Registros diarios de produccion

def guardar_registro(
    fecha: str,
    producto: str,
    producido: int,
    vendido: int,
    observaciones: str = "",
    registrado_por: str = "",
) -> bool:
    dia_semana = datetime.strptime(fecha, "%Y-%m-%d").strftime("%A")
    dias_es = {
        "Monday": "Lunes",
        "Tuesday": "Martes",
        "Wednesday": "Miercoles",
        "Thursday": "Jueves",
        "Friday": "Viernes",
        "Saturday": "Sabado",
        "Sunday": "Domingo",
    }
    dia_semana = dias_es.get(dia_semana, dia_semana)
    registrado_en = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        with get_connection() as conn:
            conn.execute(
                """
                INSERT INTO registros_diarios
                    (fecha, dia_semana, producto, producido, vendido,
                     observaciones, registrado_por, registrado_en)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(fecha, producto) DO UPDATE SET
                    producido      = excluded.producido,
                    vendido        = excluded.vendido,
                    observaciones  = excluded.observaciones,
                    registrado_por = excluded.registrado_por,
                    registrado_en  = excluded.registrado_en
                """,
                (
                    fecha,
                    dia_semana,
                    producto,
                    producido,
                    vendido,
                    observaciones,
                    registrado_por,
                    registrado_en,
                ),
            )
            conn.commit()
        return True
    except Exception as e:
        print(f"[ERROR] guardar_registro: {e}")
        return False


def obtener_registros(producto: str | None = None, dias: int = 30) -> list[dict]:
    query = """
        SELECT fecha,
               dia_semana,
               producto,
               producido,
               vendido,
               sobrante,
               CASE WHEN vendido > producido THEN (vendido - producido) ELSE 0 END as faltante,
               observaciones,
               registrado_por,
               registrado_en
        FROM registros_diarios
        WHERE fecha >= date('now', ?)
        {filtro}
        ORDER BY fecha DESC, producto ASC
    """
    filtro = "AND producto = ?" if producto else ""
    query = query.format(filtro=filtro)
    params = [f"-{dias} days"]
    if producto:
        params.append(producto)

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def obtener_resumen_por_dia_semana(producto: str) -> dict:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT dia_semana,
                   ROUND(AVG(vendido), 1) AS promedio_vendido,
                   COUNT(*) AS muestras
            FROM registros_diarios
            WHERE producto = ?
            GROUP BY dia_semana
            """,
            (producto,),
        ).fetchall()
    return {
        r["dia_semana"]: {
            "promedio": r["promedio_vendido"],
            "muestras": r["muestras"],
        }
        for r in rows
    }


def contar_registros(producto: str) -> int:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT COUNT(*) as total FROM registros_diarios WHERE producto = ?",
            (producto,),
        ).fetchone()
    return int(row["total"]) if row else 0


def hay_produccion_dia(fecha: str) -> bool:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT COUNT(*) as total FROM registros_diarios WHERE fecha = ?",
            (fecha,),
        ).fetchone()
    return bool(row and row["total"] > 0)


def obtener_producido_dia_producto(fecha: str, producto: str) -> int:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT COALESCE(SUM(producido), 0) as producido
            FROM registros_diarios
            WHERE fecha = ? AND producto = ?
            """,
            (fecha, producto),
        ).fetchone()
    return int(row["producido"]) if row else 0


def validar_venta_producto(fecha: str, producto: str, cantidad: int) -> dict:
    producido = obtener_producido_dia_producto(fecha, producto)
    vendido = obtener_vendido_dia_producto(fecha, producto)
    disponible = max(producido - vendido, 0)

    if producido <= 0:
        return {
            "ok": False,
            "producto": producto,
            "error": (
                f"No hay produccion registrada hoy para {producto}. "
                "Primero registra la produccion del dia."
            ),
            "producido": producido,
            "vendido": vendido,
            "disponible": disponible,
        }

    if cantidad > disponible:
        return {
            "ok": False,
            "producto": producto,
            "error": (
                f"Stock insuficiente para {producto}. "
                f"Disponible: {disponible}, solicitado: {cantidad}."
            ),
            "producido": producido,
            "vendido": vendido,
            "disponible": disponible,
        }

    return {
        "ok": True,
        "producto": producto,
        "producido": producido,
        "vendido": vendido,
        "disponible": disponible,
    }


# Estandarizacion

def obtener_ficha_tecnica(producto: str) -> dict:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT producto, ingredientes, cantidades,
                   tiempo_amasado_min, tiempo_fermentacion_min,
                   temperatura_horneado_c, tiempo_horneado_min,
                   pasos_proceso, actualizado_por, actualizado_en
            FROM fichas_tecnicas
            WHERE producto = ?
            """,
            (producto,),
        ).fetchone()

    if row:
        return dict(row)

    return {
        "producto": producto,
        "ingredientes": "",
        "cantidades": "",
        "tiempo_amasado_min": 0,
        "tiempo_fermentacion_min": 0,
        "temperatura_horneado_c": 0,
        "tiempo_horneado_min": 0,
        "pasos_proceso": (
            "1. Recepcion de materia prima\n"
            "2. Pesaje y dosificacion\n"
            "3. Mezclado y amasado\n"
            "4. Fermentacion\n"
            "5. Formado\n"
            "6. Crecimiento\n"
            "7. Horneado\n"
            "8. Enfriado y despacho"
        ),
        "actualizado_por": "",
        "actualizado_en": "",
    }


def obtener_fichas_tecnicas() -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT producto, ingredientes, cantidades,
                   tiempo_amasado_min, tiempo_fermentacion_min,
                   temperatura_horneado_c, tiempo_horneado_min,
                   pasos_proceso, actualizado_por, actualizado_en
            FROM fichas_tecnicas
            ORDER BY producto
            """
        ).fetchall()
    return [dict(r) for r in rows]


def guardar_ficha_tecnica(
    producto: str,
    ingredientes: str,
    cantidades: str,
    tiempo_amasado_min: int,
    tiempo_fermentacion_min: int,
    temperatura_horneado_c: int,
    tiempo_horneado_min: int,
    pasos_proceso: str,
    actualizado_por: str,
) -> bool:
    actualizado_en = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with get_connection() as conn:
            conn.execute(
                """
                INSERT INTO fichas_tecnicas (
                    producto, ingredientes, cantidades,
                    tiempo_amasado_min, tiempo_fermentacion_min,
                    temperatura_horneado_c, tiempo_horneado_min,
                    pasos_proceso, actualizado_por, actualizado_en
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(producto) DO UPDATE SET
                    ingredientes = excluded.ingredientes,
                    cantidades = excluded.cantidades,
                    tiempo_amasado_min = excluded.tiempo_amasado_min,
                    tiempo_fermentacion_min = excluded.tiempo_fermentacion_min,
                    temperatura_horneado_c = excluded.temperatura_horneado_c,
                    tiempo_horneado_min = excluded.tiempo_horneado_min,
                    pasos_proceso = excluded.pasos_proceso,
                    actualizado_por = excluded.actualizado_por,
                    actualizado_en = excluded.actualizado_en
                """,
                (
                    producto,
                    ingredientes,
                    cantidades,
                    tiempo_amasado_min,
                    tiempo_fermentacion_min,
                    temperatura_horneado_c,
                    tiempo_horneado_min,
                    pasos_proceso,
                    actualizado_por,
                    actualizado_en,
                ),
            )
            conn.commit()
        return True
    except Exception as e:
        print(f"[ERROR] guardar_ficha_tecnica: {e}")
        return False