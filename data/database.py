"""
database.py
-----------
Capa de datos: gestion de la base de datos SQLite.
Tablas:
  - productos: catalogo con precios
  - usuarios: cajeros, panaderos y meseros con PIN
  - ventas: registro individual de cada venta (cajero)
  - registros_diarios: produccion diaria por producto (panadero)
  - alertas: reservada para futuras alertas
  - mesas: catalogo de mesas del local
  - pedidos: pedidos con estado, mesa y mesero
  - pedido_items: productos dentro de un pedido
  - adicionales: catalogo de extras con precio
  - pedido_item_modificaciones: adicionales/exclusiones por item
  - insumos: catalogo de ingredientes con stock
  - recetas: composicion producto → insumos
  - adicional_insumos: insumos consumidos por cada adicional
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
                rol    TEXT NOT NULL CHECK(rol IN ('panadero', 'cajero', 'mesero'))
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

        # Mesas del local
        conn.execute("""
            CREATE TABLE IF NOT EXISTS mesas (
                id     INTEGER PRIMARY KEY AUTOINCREMENT,
                numero INTEGER UNIQUE NOT NULL,
                nombre TEXT NOT NULL DEFAULT '',
                activa INTEGER NOT NULL DEFAULT 1
            )
        """)

        # Pedidos con estado y trazabilidad
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pedidos (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                mesa_id     INTEGER,
                mesero      TEXT NOT NULL DEFAULT '',
                estado      TEXT NOT NULL DEFAULT 'pendiente'
                            CHECK(estado IN ('pendiente','en_preparacion','listo','pagado','cancelado')),
                fecha       TEXT NOT NULL,
                hora        TEXT NOT NULL,
                hora_pagado TEXT DEFAULT NULL,
                notas       TEXT DEFAULT '',
                total       REAL NOT NULL DEFAULT 0.0,
                FOREIGN KEY (mesa_id) REFERENCES mesas(id)
            )
        """)

        # Items del pedido
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pedido_items (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                pedido_id   INTEGER NOT NULL,
                producto    TEXT NOT NULL,
                cantidad    INTEGER NOT NULL DEFAULT 1,
                precio_unitario REAL NOT NULL DEFAULT 0.0,
                subtotal    REAL NOT NULL DEFAULT 0.0,
                notas       TEXT DEFAULT '',
                FOREIGN KEY (pedido_id) REFERENCES pedidos(id) ON DELETE CASCADE
            )
        """)

        # Catalogo de adicionales (extras con precio)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS adicionales (
                id     INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT UNIQUE NOT NULL,
                precio REAL NOT NULL DEFAULT 0.0,
                activo INTEGER NOT NULL DEFAULT 1
            )
        """)

        # Modificaciones por item del pedido (adicionales y exclusiones)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pedido_item_modificaciones (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                pedido_item_id  INTEGER NOT NULL,
                tipo            TEXT NOT NULL CHECK(tipo IN ('adicional', 'exclusion')),
                descripcion     TEXT NOT NULL,
                cantidad        INTEGER NOT NULL DEFAULT 1,
                precio_extra    REAL NOT NULL DEFAULT 0.0,
                FOREIGN KEY (pedido_item_id) REFERENCES pedido_items(id) ON DELETE CASCADE
            )
        """)

        # Catalogo de insumos (ingredientes)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS insumos (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre  TEXT UNIQUE NOT NULL,
                unidad  TEXT NOT NULL DEFAULT 'unidad',
                stock   REAL NOT NULL DEFAULT 0.0,
                stock_minimo REAL NOT NULL DEFAULT 0.0,
                activo  INTEGER NOT NULL DEFAULT 1
            )
        """)

        # Recetas: composicion producto → insumos
        conn.execute("""
            CREATE TABLE IF NOT EXISTS recetas (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                producto    TEXT NOT NULL,
                insumo_id   INTEGER NOT NULL,
                cantidad    REAL NOT NULL DEFAULT 1.0,
                UNIQUE(producto, insumo_id),
                FOREIGN KEY (insumo_id) REFERENCES insumos(id)
            )
        """)

        # Insumos consumidos por cada adicional
        conn.execute("""
            CREATE TABLE IF NOT EXISTS adicional_insumos (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                adicional_id  INTEGER NOT NULL,
                insumo_id     INTEGER NOT NULL,
                cantidad      REAL NOT NULL DEFAULT 1.0,
                UNIQUE(adicional_id, insumo_id),
                FOREIGN KEY (adicional_id) REFERENCES adicionales(id),
                FOREIGN KEY (insumo_id) REFERENCES insumos(id)
            )
        """)

        # Migrar tabla productos existente: agregar columnas si faltan
        _migrar_productos(conn)
        # Migrar tabla usuarios: agregar rol mesero al CHECK
        _migrar_usuarios(conn)

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
            conn.execute(
                "INSERT INTO usuarios (nombre, pin, rol) VALUES (?, ?, ?)",
                ("Mesero", "1111", "mesero")
            )

        # Mesas iniciales (5 mesas por defecto)
        for num in range(1, 6):
            conn.execute(
                "INSERT OR IGNORE INTO mesas (numero, nombre) VALUES (?, ?)",
                (num, f"Mesa {num}")
            )

        # Adicionales por defecto
        adicionales_iniciales = [
            ("Huevo extra", 5.0),
            ("Queso extra", 8.0),
            ("Jamon extra", 10.0),
            ("Pan adicional", 8.0),
            ("Cafe adicional", 15.0),
            ("Mantequilla extra", 3.0),
        ]
        for nombre, precio in adicionales_iniciales:
            conn.execute(
                "INSERT OR IGNORE INTO adicionales (nombre, precio) VALUES (?, ?)",
                (nombre, precio)
            )

        # Insumos iniciales
        insumos_iniciales = [
            ("Harina", "kg", 50.0, 10.0),
            ("Azucar", "kg", 20.0, 5.0),
            ("Mantequilla", "kg", 15.0, 3.0),
            ("Huevos", "unidad", 100.0, 20.0),
            ("Leche", "litro", 20.0, 5.0),
            ("Levadura", "kg", 5.0, 1.0),
            ("Sal", "kg", 10.0, 2.0),
            ("Cafe molido", "kg", 5.0, 1.0),
            ("Queso", "kg", 10.0, 2.0),
            ("Jamon", "kg", 8.0, 2.0),
        ]
        for nombre, unidad, stock, minimo in insumos_iniciales:
            conn.execute(
                "INSERT OR IGNORE INTO insumos (nombre, unidad, stock, stock_minimo) VALUES (?, ?, ?, ?)",
                (nombre, unidad, stock, minimo)
            )

        # Recetas por defecto (composicion basica)
        recetas_default = {
            "Pan Frances": [("Harina", 0.15), ("Levadura", 0.005), ("Sal", 0.003), ("Mantequilla", 0.01)],
            "Pan Dulce": [("Harina", 0.12), ("Azucar", 0.04), ("Huevos", 0.5), ("Mantequilla", 0.03), ("Levadura", 0.005)],
            "Croissant": [("Harina", 0.10), ("Mantequilla", 0.06), ("Huevos", 0.3), ("Levadura", 0.004), ("Azucar", 0.02)],
            "Integral": [("Harina", 0.18), ("Levadura", 0.005), ("Sal", 0.003)],
        }
        for producto, ingredientes in recetas_default.items():
            for insumo_nombre, cant in ingredientes:
                insumo = conn.execute(
                    "SELECT id FROM insumos WHERE nombre = ?", (insumo_nombre,)
                ).fetchone()
                if insumo:
                    conn.execute(
                        "INSERT OR IGNORE INTO recetas (producto, insumo_id, cantidad) VALUES (?, ?, ?)",
                        (producto, insumo["id"], cant)
                    )

        # Insumos por adicional
        adicional_insumos_default = {
            "Huevo extra": [("Huevos", 1.0)],
            "Queso extra": [("Queso", 0.05)],
            "Jamon extra": [("Jamon", 0.05)],
            "Pan adicional": [("Harina", 0.15), ("Levadura", 0.005)],
            "Cafe adicional": [("Cafe molido", 0.02), ("Leche", 0.1)],
            "Mantequilla extra": [("Mantequilla", 0.03)],
        }
        for adicional_nombre, ingredientes in adicional_insumos_default.items():
            adic = conn.execute(
                "SELECT id FROM adicionales WHERE nombre = ?", (adicional_nombre,)
            ).fetchone()
            if adic:
                for insumo_nombre, cant in ingredientes:
                    insumo = conn.execute(
                        "SELECT id FROM insumos WHERE nombre = ?", (insumo_nombre,)
                    ).fetchone()
                    if insumo:
                        conn.execute(
                            "INSERT OR IGNORE INTO adicional_insumos (adicional_id, insumo_id, cantidad) VALUES (?, ?, ?)",
                            (adic["id"], insumo["id"], cant)
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


def _migrar_usuarios(conn):
    """Recrea la tabla usuarios con el CHECK actualizado si mesero no esta permitido."""
    try:
        conn.execute(
            "INSERT INTO usuarios (nombre, pin, rol) VALUES ('__test__', '9999', 'mesero')"
        )
        conn.execute("DELETE FROM usuarios WHERE nombre = '__test__'")
    except sqlite3.IntegrityError:
        # CHECK constraint fallo: necesitamos migrar
        rows = conn.execute("SELECT id, nombre, pin, rol FROM usuarios").fetchall()
        conn.execute("DROP TABLE usuarios")
        conn.execute("""
            CREATE TABLE usuarios (
                id     INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT NOT NULL,
                pin    TEXT NOT NULL,
                rol    TEXT NOT NULL CHECK(rol IN ('panadero', 'cajero', 'mesero'))
            )
        """)
        for r in rows:
            conn.execute(
                "INSERT INTO usuarios (id, nombre, pin, rol) VALUES (?, ?, ?, ?)",
                (r["id"], r["nombre"], r["pin"], r["rol"])
            )


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


# ──────────────────────────────────────────────
# Mesas
# ──────────────────────────────────────────────

def obtener_mesas() -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT id, numero, nombre, activa FROM mesas WHERE activa = 1 ORDER BY numero"
        ).fetchall()
    return [dict(r) for r in rows]


def agregar_mesa(numero: int, nombre: str = "") -> bool:
    try:
        if not nombre:
            nombre = f"Mesa {numero}"
        with get_connection() as conn:
            conn.execute(
                "INSERT INTO mesas (numero, nombre) VALUES (?, ?)",
                (numero, nombre)
            )
            conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def eliminar_mesa(mesa_id: int) -> bool:
    try:
        with get_connection() as conn:
            conn.execute("UPDATE mesas SET activa = 0 WHERE id = ?", (mesa_id,))
            conn.commit()
        return True
    except Exception:
        return False


# ──────────────────────────────────────────────
# Pedidos
# ──────────────────────────────────────────────

def crear_pedido(mesa_id: int, mesero: str, items: list[dict],
                 notas: str = "") -> int | None:
    """Crea un pedido con sus items y modificaciones. Retorna el id del pedido o None.

    Cada item puede tener:
      - producto, cantidad, precio_unitario, notas
      - modificaciones: lista de {tipo, descripcion, cantidad, precio_extra}
    """
    ahora = datetime.now()
    fecha = ahora.strftime("%Y-%m-%d")
    hora = ahora.strftime("%H:%M:%S")

    # Calcular total incluyendo modificaciones
    total = 0.0
    for item in items:
        item_base = item["cantidad"] * item["precio_unitario"]
        extras = sum(
            m.get("cantidad", 1) * m.get("precio_extra", 0)
            for m in item.get("modificaciones", [])
            if m.get("tipo") == "adicional"
        )
        total += item_base + extras

    try:
        with get_connection() as conn:
            cursor = conn.execute("""
                INSERT INTO pedidos (mesa_id, mesero, estado, fecha, hora, notas, total)
                VALUES (?, ?, 'pendiente', ?, ?, ?, ?)
            """, (mesa_id, mesero, fecha, hora, notas, total))
            pedido_id = cursor.lastrowid

            for item in items:
                extras = sum(
                    m.get("cantidad", 1) * m.get("precio_extra", 0)
                    for m in item.get("modificaciones", [])
                    if m.get("tipo") == "adicional"
                )
                subtotal = item["cantidad"] * item["precio_unitario"] + extras
                cur_item = conn.execute("""
                    INSERT INTO pedido_items
                        (pedido_id, producto, cantidad, precio_unitario, subtotal, notas)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (pedido_id, item["producto"], item["cantidad"],
                      item["precio_unitario"], subtotal, item.get("notas", "")))
                item_id = cur_item.lastrowid

                # Insertar modificaciones
                for mod in item.get("modificaciones", []):
                    conn.execute("""
                        INSERT INTO pedido_item_modificaciones
                            (pedido_item_id, tipo, descripcion, cantidad, precio_extra)
                        VALUES (?, ?, ?, ?, ?)
                    """, (item_id, mod["tipo"], mod["descripcion"],
                          mod.get("cantidad", 1), mod.get("precio_extra", 0)))

            conn.commit()
        return pedido_id
    except Exception as e:
        print(f"[ERROR] crear_pedido: {e}")
        return None


def obtener_pedidos(estado: str = None, mesa_id: int = None,
                    fecha: str = None) -> list[dict]:
    """Obtiene pedidos filtrados por estado, mesa y/o fecha."""
    if fecha is None:
        fecha = datetime.now().strftime("%Y-%m-%d")

    query = """
        SELECT p.id, p.mesa_id, m.numero as mesa_numero, m.nombre as mesa_nombre,
               p.mesero, p.estado, p.fecha, p.hora, p.hora_pagado, p.notas, p.total
        FROM pedidos p
        LEFT JOIN mesas m ON p.mesa_id = m.id
        WHERE p.fecha = ?
    """
    params = [fecha]

    if estado:
        query += " AND p.estado = ?"
        params.append(estado)
    if mesa_id:
        query += " AND p.mesa_id = ?"
        params.append(mesa_id)

    query += " ORDER BY p.hora DESC"

    with get_connection() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def obtener_pedido(pedido_id: int) -> dict | None:
    """Obtiene un pedido con sus items y modificaciones."""
    with get_connection() as conn:
        pedido = conn.execute("""
            SELECT p.id, p.mesa_id, m.numero as mesa_numero, m.nombre as mesa_nombre,
                   p.mesero, p.estado, p.fecha, p.hora, p.hora_pagado, p.notas, p.total
            FROM pedidos p
            LEFT JOIN mesas m ON p.mesa_id = m.id
            WHERE p.id = ?
        """, (pedido_id,)).fetchone()

        if not pedido:
            return None

        items = conn.execute("""
            SELECT id, producto, cantidad, precio_unitario, subtotal, notas
            FROM pedido_items
            WHERE pedido_id = ?
            ORDER BY id
        """, (pedido_id,)).fetchall()

        items_list = []
        for item in items:
            item_dict = dict(item)
            mods = conn.execute("""
                SELECT id, tipo, descripcion, cantidad, precio_extra
                FROM pedido_item_modificaciones
                WHERE pedido_item_id = ?
                ORDER BY tipo, id
            """, (item_dict["id"],)).fetchall()
            item_dict["modificaciones"] = [dict(m) for m in mods]
            items_list.append(item_dict)

    result = dict(pedido)
    result["items"] = items_list
    return result


def cambiar_estado_pedido(pedido_id: int, nuevo_estado: str) -> bool:
    """Cambia el estado de un pedido."""
    try:
        with get_connection() as conn:
            hora_pagado = None
            if nuevo_estado == "pagado":
                hora_pagado = datetime.now().strftime("%H:%M:%S")
                conn.execute(
                    "UPDATE pedidos SET estado = ?, hora_pagado = ? WHERE id = ?",
                    (nuevo_estado, hora_pagado, pedido_id)
                )
            else:
                conn.execute(
                    "UPDATE pedidos SET estado = ? WHERE id = ?",
                    (nuevo_estado, pedido_id)
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[ERROR] cambiar_estado_pedido: {e}")
        return False


def pagar_pedido(pedido_id: int, registrado_por: str = "") -> bool:
    """Marca pedido como pagado, registra ventas y descuenta inventario."""
    try:
        pedido = obtener_pedido(pedido_id)
        if not pedido or pedido["estado"] == "pagado":
            return False

        with get_connection() as conn:
            ahora = datetime.now()
            hora_pagado = ahora.strftime("%H:%M:%S")
            fecha = pedido["fecha"]

            for item in pedido["items"]:
                # Registrar venta
                conn.execute("""
                    INSERT INTO ventas (fecha, hora, producto, cantidad,
                                        precio_unitario, total, registrado_por)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (fecha, hora_pagado, item["producto"], item["cantidad"],
                      item["precio_unitario"], item["subtotal"], registrado_por))

                # Descontar inventario por receta del producto base
                receta = conn.execute("""
                    SELECT r.insumo_id, r.cantidad, i.nombre
                    FROM recetas r JOIN insumos i ON r.insumo_id = i.id
                    WHERE r.producto = ?
                """, (item["producto"],)).fetchall()

                for r in receta:
                    consumo = r["cantidad"] * item["cantidad"]
                    conn.execute(
                        "UPDATE insumos SET stock = MAX(0, stock - ?) WHERE id = ?",
                        (consumo, r["insumo_id"])
                    )

                # Descontar inventario por adicionales
                for mod in item.get("modificaciones", []):
                    if mod["tipo"] == "adicional":
                        adicional = conn.execute(
                            "SELECT id FROM adicionales WHERE nombre = ?",
                            (mod["descripcion"],)
                        ).fetchone()
                        if adicional:
                            ai = conn.execute("""
                                SELECT ai.insumo_id, ai.cantidad
                                FROM adicional_insumos ai
                                WHERE ai.adicional_id = ?
                            """, (adicional["id"],)).fetchall()
                            for a in ai:
                                consumo = a["cantidad"] * mod.get("cantidad", 1)
                                conn.execute(
                                    "UPDATE insumos SET stock = MAX(0, stock - ?) WHERE id = ?",
                                    (consumo, a["insumo_id"])
                                )

            # Marcar como pagado
            conn.execute(
                "UPDATE pedidos SET estado = 'pagado', hora_pagado = ? WHERE id = ?",
                (hora_pagado, pedido_id)
            )
            conn.commit()
        return True
    except Exception as e:
        print(f"[ERROR] pagar_pedido: {e}")
        return False


def obtener_pedidos_activos_mesa(mesa_id: int) -> list[dict]:
    """Pedidos no pagados/cancelados de una mesa."""
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT id, mesero, estado, hora, total
            FROM pedidos
            WHERE mesa_id = ? AND estado NOT IN ('pagado', 'cancelado')
              AND fecha = ?
            ORDER BY hora DESC
        """, (mesa_id, datetime.now().strftime("%Y-%m-%d"))).fetchall()
    return [dict(r) for r in rows]


# ──────────────────────────────────────────────
# Adicionales
# ──────────────────────────────────────────────

def obtener_adicionales() -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT id, nombre, precio FROM adicionales WHERE activo = 1 ORDER BY nombre"
        ).fetchall()
    return [dict(r) for r in rows]


def agregar_adicional(nombre: str, precio: float) -> bool:
    try:
        with get_connection() as conn:
            conn.execute(
                "INSERT INTO adicionales (nombre, precio) VALUES (?, ?)",
                (nombre, precio)
            )
            conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def actualizar_adicional(adicional_id: int, precio: float) -> bool:
    try:
        with get_connection() as conn:
            conn.execute(
                "UPDATE adicionales SET precio = ? WHERE id = ?",
                (precio, adicional_id)
            )
            conn.commit()
        return True
    except Exception:
        return False


def eliminar_adicional(adicional_id: int) -> bool:
    try:
        with get_connection() as conn:
            conn.execute("UPDATE adicionales SET activo = 0 WHERE id = ?", (adicional_id,))
            conn.commit()
        return True
    except Exception:
        return False


# ──────────────────────────────────────────────
# Insumos (inventario)
# ──────────────────────────────────────────────

def obtener_insumos() -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT id, nombre, unidad, stock, stock_minimo, activo
            FROM insumos WHERE activo = 1
            ORDER BY nombre
        """).fetchall()
    return [dict(r) for r in rows]


def agregar_insumo(nombre: str, unidad: str, stock: float = 0,
                   stock_minimo: float = 0) -> bool:
    try:
        with get_connection() as conn:
            conn.execute(
                "INSERT INTO insumos (nombre, unidad, stock, stock_minimo) VALUES (?, ?, ?, ?)",
                (nombre, unidad, stock, stock_minimo)
            )
            conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def actualizar_stock(insumo_id: int, nuevo_stock: float) -> bool:
    try:
        with get_connection() as conn:
            conn.execute(
                "UPDATE insumos SET stock = ? WHERE id = ?",
                (nuevo_stock, insumo_id)
            )
            conn.commit()
        return True
    except Exception:
        return False


def eliminar_insumo(insumo_id: int) -> bool:
    try:
        with get_connection() as conn:
            conn.execute("UPDATE insumos SET activo = 0 WHERE id = ?", (insumo_id,))
            conn.commit()
        return True
    except Exception:
        return False


def obtener_insumos_bajo_stock() -> list[dict]:
    """Insumos cuyo stock esta por debajo del minimo."""
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT id, nombre, unidad, stock, stock_minimo
            FROM insumos
            WHERE activo = 1 AND stock <= stock_minimo
            ORDER BY (stock / CASE WHEN stock_minimo > 0 THEN stock_minimo ELSE 1 END) ASC
        """).fetchall()
    return [dict(r) for r in rows]


# ──────────────────────────────────────────────
# Recetas
# ──────────────────────────────────────────────

def obtener_receta(producto: str) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT r.id, r.insumo_id, i.nombre as insumo_nombre,
                   i.unidad, r.cantidad
            FROM recetas r
            JOIN insumos i ON r.insumo_id = i.id
            WHERE r.producto = ?
            ORDER BY i.nombre
        """, (producto,)).fetchall()
    return [dict(r) for r in rows]


def guardar_receta(producto: str, ingredientes: list[dict]) -> bool:
    """Reemplaza la receta de un producto. ingredientes: [{insumo_id, cantidad}]"""
    try:
        with get_connection() as conn:
            conn.execute("DELETE FROM recetas WHERE producto = ?", (producto,))
            for ing in ingredientes:
                conn.execute(
                    "INSERT INTO recetas (producto, insumo_id, cantidad) VALUES (?, ?, ?)",
                    (producto, ing["insumo_id"], ing["cantidad"])
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[ERROR] guardar_receta: {e}")
        return False


def obtener_consumo_diario(fecha: str = None) -> list[dict]:
    """Calcula el consumo teorico de insumos del dia basado en pedidos pagados."""
    if fecha is None:
        fecha = datetime.now().strftime("%Y-%m-%d")

    consumo = {}
    with get_connection() as conn:
        # Obtener items de pedidos pagados del dia
        items = conn.execute("""
            SELECT pi.producto, pi.cantidad, pi.id as item_id
            FROM pedido_items pi
            JOIN pedidos p ON pi.pedido_id = p.id
            WHERE p.fecha = ? AND p.estado = 'pagado'
        """, (fecha,)).fetchall()

        for item in items:
            # Consumo por receta del producto base
            receta = conn.execute("""
                SELECT r.insumo_id, i.nombre, i.unidad, r.cantidad
                FROM recetas r JOIN insumos i ON r.insumo_id = i.id
                WHERE r.producto = ?
            """, (item["producto"],)).fetchall()

            for r in receta:
                key = r["insumo_id"]
                if key not in consumo:
                    consumo[key] = {"nombre": r["nombre"], "unidad": r["unidad"], "cantidad": 0}
                consumo[key]["cantidad"] += r["cantidad"] * item["cantidad"]

            # Consumo por adicionales
            mods = conn.execute("""
                SELECT m.tipo, m.descripcion, m.cantidad
                FROM pedido_item_modificaciones m
                WHERE m.pedido_item_id = ? AND m.tipo = 'adicional'
            """, (item["item_id"],)).fetchall()

            for mod in mods:
                adicional = conn.execute(
                    "SELECT id FROM adicionales WHERE nombre = ?",
                    (mod["descripcion"],)
                ).fetchone()
                if adicional:
                    ai = conn.execute("""
                        SELECT ai.insumo_id, i.nombre, i.unidad, ai.cantidad
                        FROM adicional_insumos ai
                        JOIN insumos i ON ai.insumo_id = i.id
                        WHERE ai.adicional_id = ?
                    """, (adicional["id"],)).fetchall()
                    for a in ai:
                        key = a["insumo_id"]
                        if key not in consumo:
                            consumo[key] = {"nombre": a["nombre"], "unidad": a["unidad"], "cantidad": 0}
                        consumo[key]["cantidad"] += a["cantidad"] * mod["cantidad"]

    return sorted(consumo.values(), key=lambda x: x["nombre"])


def obtener_estadisticas_pedidos(fecha: str = None) -> dict:
    """Estadisticas de pedidos del dia."""
    if fecha is None:
        fecha = datetime.now().strftime("%Y-%m-%d")

    with get_connection() as conn:
        row = conn.execute("""
            SELECT
                COUNT(*) as total_pedidos,
                SUM(CASE WHEN estado = 'pendiente' THEN 1 ELSE 0 END) as pendientes,
                SUM(CASE WHEN estado = 'en_preparacion' THEN 1 ELSE 0 END) as en_preparacion,
                SUM(CASE WHEN estado = 'listo' THEN 1 ELSE 0 END) as listos,
                SUM(CASE WHEN estado = 'pagado' THEN 1 ELSE 0 END) as pagados,
                SUM(CASE WHEN estado = 'cancelado' THEN 1 ELSE 0 END) as cancelados,
                COALESCE(SUM(CASE WHEN estado = 'pagado' THEN total ELSE 0 END), 0) as total_cobrado
            FROM pedidos WHERE fecha = ?
        """, (fecha,)).fetchone()
    return dict(row)


def obtener_resumen_mesas() -> list[dict]:
    """Resumen de mesas con sus pedidos activos."""
    mesas = obtener_mesas()
    hoy = datetime.now().strftime("%Y-%m-%d")
    resultado = []
    with get_connection() as conn:
        for mesa in mesas:
            pedidos = conn.execute("""
                SELECT COUNT(*) as num_pedidos,
                       COALESCE(SUM(total), 0) as total_mesa
                FROM pedidos
                WHERE mesa_id = ? AND estado NOT IN ('pagado', 'cancelado')
                  AND fecha = ?
            """, (mesa["id"], hoy)).fetchone()
            mesa["num_pedidos"] = pedidos["num_pedidos"]
            mesa["total_mesa"] = pedidos["total_mesa"]
            # Estado de la mesa
            ultimo = conn.execute("""
                SELECT estado FROM pedidos
                WHERE mesa_id = ? AND estado NOT IN ('pagado', 'cancelado')
                  AND fecha = ?
                ORDER BY hora DESC LIMIT 1
            """, (mesa["id"], hoy)).fetchone()
            mesa["estado_mesa"] = ultimo["estado"] if ultimo else "libre"
            resultado.append(mesa)
    return resultado
