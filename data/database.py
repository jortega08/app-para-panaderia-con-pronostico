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
  - adicional_componentes: productos base consumidos por cada adicional
"""

import hashlib
import logging
import sqlite3
import os
from datetime import datetime
from pathlib import Path
from uuid import uuid4

logger = logging.getLogger(__name__)

from data.db_adapter import get_connection as _get_connection, DB_TYPE

DB_PATH = Path(__file__).parent / "panaderia.db"


def _hash_pin(pin: str) -> str:
    return hashlib.sha256(str(pin).strip().encode('utf-8')).hexdigest()

CATEGORIAS_PREDETERMINADAS = [
    "Panaderia",
    "Bebidas Calientes",
    "Bebidas Frias",
    "Desayunos",
    "Almuerzos",
]

UNIDADES_MASA = {
    "kg": 1000.0,
    "kilogramo": 1000.0,
    "kilogramos": 1000.0,
    "g": 1.0,
    "gramo": 1.0,
    "gramos": 1.0,
}

UNIDADES_VOLUMEN = {
    "litro": 1000.0,
    "litros": 1000.0,
    "l": 1000.0,
    "ml": 1.0,
    "mililitro": 1.0,
    "mililitros": 1.0,
}

UNIDADES_CONTEO = {
    "unidad": 1.0,
    "unidades": 1.0,
    "und": 1.0,
    "u": 1.0,
}


def _normalizar_unidad(unidad: str) -> str:
    return str(unidad or "").strip().lower()


def _grupo_unidad(unidad: str) -> tuple[str | None, float | None]:
    unidad_norm = _normalizar_unidad(unidad)
    if unidad_norm in UNIDADES_MASA:
        return "masa", UNIDADES_MASA[unidad_norm]
    if unidad_norm in UNIDADES_VOLUMEN:
        return "volumen", UNIDADES_VOLUMEN[unidad_norm]
    if unidad_norm in UNIDADES_CONTEO:
        return "conteo", UNIDADES_CONTEO[unidad_norm]
    return None, None


def unidad_receta_sugerida(unidad_inventario: str) -> str:
    grupo, _ = _grupo_unidad(unidad_inventario)
    if grupo == "masa":
        return "g"
    if grupo == "volumen":
        return "ml"
    if grupo == "conteo":
        return "unidad"
    return _normalizar_unidad(unidad_inventario) or "unidad"


def convertir_cantidad(cantidad: float, unidad_origen: str, unidad_destino: str) -> float:
    origen = _normalizar_unidad(unidad_origen)
    destino = _normalizar_unidad(unidad_destino)
    if not origen or not destino or origen == destino:
        return float(cantidad or 0)

    grupo_origen, factor_origen = _grupo_unidad(origen)
    grupo_destino, factor_destino = _grupo_unidad(destino)
    if not grupo_origen or grupo_origen != grupo_destino or not factor_origen or not factor_destino:
        return float(cantidad or 0)

    cantidad_base = float(cantidad or 0) * factor_origen
    return cantidad_base / factor_destino


def _ficha_receta_vacia(producto: str = "") -> dict:
    return {
        "producto": producto,
        "rendimiento_texto": "",
        "tiempo_preparacion_min": 0.0,
        "tiempo_amasado_min": 0.0,
        "tiempo_fermentacion_min": 0.0,
        "tiempo_horneado_min": 0.0,
        "temperatura_horneado": 0.0,
        "pasos": "",
        "observaciones": "",
    }


def _obtener_configuracion_conn(conn, clave: str, valor_default: str = "") -> str:
    row = conn.execute(
        "SELECT valor FROM configuracion_sistema WHERE clave = ?",
        (clave,)
    ).fetchone()
    return str(row["valor"]) if row and row["valor"] is not None else valor_default


def get_connection():
    """Retorna conexión activa (SQLite o PostgreSQL según DATABASE_URL)."""
    return _get_connection()


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
                categoria TEXT NOT NULL DEFAULT 'Panaderia',
                activo INTEGER NOT NULL DEFAULT 1
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS categorias_producto (
                id     INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT UNIQUE NOT NULL,
                activa INTEGER NOT NULL DEFAULT 1
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS configuracion_sistema (
                clave TEXT PRIMARY KEY,
                valor TEXT NOT NULL DEFAULT ''
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

        conn.execute("""
            CREATE TABLE IF NOT EXISTS arqueos_caja (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha           TEXT NOT NULL,
                abierto_en      TEXT NOT NULL,
                abierto_por     TEXT NOT NULL DEFAULT '',
                monto_apertura  REAL NOT NULL DEFAULT 0.0,
                estado          TEXT NOT NULL DEFAULT 'abierto'
                                CHECK(estado IN ('abierto', 'cerrado')),
                notas           TEXT DEFAULT '',
                cerrado_en      TEXT DEFAULT NULL,
                cerrado_por     TEXT DEFAULT '',
                monto_cierre    REAL DEFAULT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS movimientos_caja (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                arqueo_id       INTEGER,
                fecha           TEXT NOT NULL,
                creado_en       TEXT NOT NULL,
                tipo            TEXT NOT NULL CHECK(tipo IN ('ingreso', 'egreso')),
                concepto        TEXT NOT NULL,
                monto           REAL NOT NULL DEFAULT 0.0,
                registrado_por  TEXT NOT NULL DEFAULT '',
                notas           TEXT DEFAULT '',
                FOREIGN KEY (arqueo_id) REFERENCES arqueos_caja(id)
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

        conn.execute("""
            CREATE TABLE IF NOT EXISTS pedido_estado_historial (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                pedido_id   INTEGER NOT NULL,
                estado      TEXT NOT NULL,
                cambiado_en TEXT NOT NULL,
                cambiado_por TEXT NOT NULL DEFAULT '',
                detalle     TEXT DEFAULT '',
                FOREIGN KEY (pedido_id) REFERENCES pedidos(id) ON DELETE CASCADE
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
                unidad_receta TEXT NOT NULL DEFAULT 'unidad',
                UNIQUE(producto, insumo_id),
                FOREIGN KEY (insumo_id) REFERENCES insumos(id)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS receta_fichas (
                id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                producto                TEXT UNIQUE NOT NULL,
                rendimiento_texto       TEXT DEFAULT '',
                tiempo_preparacion_min  REAL NOT NULL DEFAULT 0.0,
                tiempo_amasado_min      REAL NOT NULL DEFAULT 0.0,
                tiempo_fermentacion_min REAL NOT NULL DEFAULT 0.0,
                tiempo_horneado_min     REAL NOT NULL DEFAULT 0.0,
                temperatura_horneado    REAL NOT NULL DEFAULT 0.0,
                pasos                   TEXT DEFAULT '',
                observaciones           TEXT DEFAULT ''
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS producto_componentes (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                producto            TEXT NOT NULL,
                componente_producto TEXT NOT NULL,
                cantidad            REAL NOT NULL DEFAULT 1.0,
                UNIQUE(producto, componente_producto)
            )
        """)

        # Insumos consumidos por cada adicional
        conn.execute("""
            CREATE TABLE IF NOT EXISTS adicional_insumos (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                adicional_id  INTEGER NOT NULL,
                insumo_id     INTEGER NOT NULL,
                cantidad      REAL NOT NULL DEFAULT 1.0,
                unidad_config TEXT NOT NULL DEFAULT 'unidad',
                UNIQUE(adicional_id, insumo_id),
                FOREIGN KEY (adicional_id) REFERENCES adicionales(id),
                FOREIGN KEY (insumo_id) REFERENCES insumos(id)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS adicional_componentes (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                adicional_id        INTEGER NOT NULL,
                componente_producto TEXT NOT NULL,
                cantidad            REAL NOT NULL DEFAULT 1.0,
                UNIQUE(adicional_id, componente_producto),
                FOREIGN KEY (adicional_id) REFERENCES adicionales(id)
            )
        """)

        # ── Nuevas tablas (Fase 2) ─────────────────────────────────────────
        conn.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha        TEXT NOT NULL,
                creado_en    TEXT NOT NULL,
                usuario      TEXT NOT NULL DEFAULT '',
                accion       TEXT NOT NULL,
                entidad      TEXT NOT NULL DEFAULT '',
                entidad_id   TEXT NOT NULL DEFAULT '',
                detalle      TEXT NOT NULL DEFAULT '',
                valor_antes  TEXT NOT NULL DEFAULT '',
                valor_nuevo  TEXT NOT NULL DEFAULT ''
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS mermas (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha        TEXT NOT NULL,
                creado_en    TEXT NOT NULL,
                producto     TEXT NOT NULL,
                cantidad     REAL NOT NULL DEFAULT 0,
                tipo         TEXT NOT NULL DEFAULT 'sobrante',
                registrado_por TEXT NOT NULL DEFAULT '',
                notas        TEXT NOT NULL DEFAULT ''
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS dias_especiales (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha        TEXT UNIQUE NOT NULL,
                descripcion  TEXT NOT NULL DEFAULT '',
                factor       REAL NOT NULL DEFAULT 1.0,
                tipo         TEXT NOT NULL DEFAULT 'festivo',
                activo       INTEGER NOT NULL DEFAULT 1
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS ajustes_pronostico (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha        TEXT NOT NULL,
                creado_en    TEXT NOT NULL,
                producto     TEXT NOT NULL,
                sugerido     INTEGER NOT NULL DEFAULT 0,
                ajustado     INTEGER NOT NULL DEFAULT 0,
                motivo       TEXT NOT NULL DEFAULT '',
                registrado_por TEXT NOT NULL DEFAULT '',
                UNIQUE(fecha, producto)
            )
        """)

        # ── Índices para rendimiento ──
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ventas_fecha ON ventas(fecha)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ventas_producto ON ventas(producto)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ventas_fecha_producto ON ventas(fecha, producto)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ventas_venta_grupo ON ventas(venta_grupo)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_registros_fecha ON registros_diarios(fecha)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_registros_fecha_producto ON registros_diarios(fecha, producto)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pedidos_fecha_estado ON pedidos(fecha, estado)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pedidos_mesa ON pedidos(mesa_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pedido_items_pedido ON pedido_items(pedido_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pedido_mods_item ON pedido_item_modificaciones(pedido_item_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_arqueos_fecha ON arqueos_caja(fecha)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_movimientos_arqueo ON movimientos_caja(arqueo_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_fecha ON audit_log(fecha)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_mermas_fecha ON mermas(fecha)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_mermas_producto ON mermas(producto)")

        # Migrar tabla productos existente: agregar columnas si faltan
        _migrar_productos(conn)
        # Migrar tabla usuarios: agregar rol mesero al CHECK
        _migrar_usuarios(conn)
        _migrar_recetas(conn)
        _migrar_adicionales(conn)
        _migrar_ventas_pedidos_caja(conn)
        _sembrar_categorias_producto(conn)
        conn.execute("""
            INSERT OR IGNORE INTO configuracion_sistema (clave, valor)
            VALUES ('codigo_verificacion_caja', '2468')
        """)

        # Productos iniciales con precios de ejemplo
        productos_iniciales = [
            ("Pan Frances", 8.0, "Panaderia"),
            ("Pan Dulce", 12.0, "Panaderia"),
            ("Croissant", 15.0, "Panaderia"),
            ("Integral", 10.0, "Panaderia"),
        ]
        for nombre, precio, categoria in productos_iniciales:
            conn.execute(
                "INSERT OR IGNORE INTO productos (nombre, precio, categoria) VALUES (?, ?, ?)",
                (nombre, precio, categoria)
            )

        # Usuario admin por defecto
        existe = conn.execute(
            "SELECT COUNT(*) as c FROM usuarios"
        ).fetchone()
        if existe["c"] == 0:
            conn.execute(
                "INSERT INTO usuarios (nombre, pin, rol) VALUES (?, ?, ?)",
                ("Admin", _hash_pin("1234"), "panadero")
            )
            conn.execute(
                "INSERT INTO usuarios (nombre, pin, rol) VALUES (?, ?, ?)",
                ("Cajero", _hash_pin("0000"), "cajero")
            )
            conn.execute(
                "INSERT INTO usuarios (nombre, pin, rol) VALUES (?, ?, ?)",
                ("Mesero", _hash_pin("1111"), "mesero")
            )

        # Mesas iniciales (5 mesas por defecto)
        for num in range(1, 6):
            conn.execute(
                "INSERT OR IGNORE INTO mesas (numero, nombre) VALUES (?, ?)",
                (num, f"Mesa {num}")
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
            "Pan Frances": [("Harina", 150.0, "g"), ("Levadura", 5.0, "g"), ("Sal", 3.0, "g"), ("Mantequilla", 10.0, "g")],
            "Pan Dulce": [("Harina", 120.0, "g"), ("Azucar", 40.0, "g"), ("Huevos", 1.0, "unidad"), ("Mantequilla", 30.0, "g"), ("Levadura", 5.0, "g")],
            "Croissant": [("Harina", 100.0, "g"), ("Mantequilla", 60.0, "g"), ("Huevos", 1.0, "unidad"), ("Levadura", 4.0, "g"), ("Azucar", 20.0, "g")],
            "Integral": [("Harina", 180.0, "g"), ("Levadura", 5.0, "g"), ("Sal", 3.0, "g")],
        }
        for producto, ingredientes in recetas_default.items():
            for insumo_nombre, cant, unidad_receta in ingredientes:
                insumo = conn.execute(
                    "SELECT id FROM insumos WHERE nombre = ?", (insumo_nombre,)
                ).fetchone()
                if insumo:
                    conn.execute(
                        "INSERT OR IGNORE INTO recetas (producto, insumo_id, cantidad, unidad_receta) VALUES (?, ?, ?, ?)",
                        (producto, insumo["id"], cant, unidad_receta)
                    )

        fichas_default = {
            "Pan Frances": {
                "rendimiento_texto": "1 unidad",
                "tiempo_preparacion_min": 8,
                "tiempo_amasado_min": 12,
                "tiempo_fermentacion_min": 45,
                "tiempo_horneado_min": 18,
                "temperatura_horneado": 190,
                "pasos": "1. Pesar los ingredientes.\n2. Mezclar harina, sal y levadura.\n3. Amasar hasta obtener una masa uniforme.\n4. Dejar fermentar.\n5. Formar la pieza.\n6. Hornear hasta dorar.",
                "observaciones": "Verificar color dorado uniforme antes de retirar del horno.",
            },
            "Pan Dulce": {
                "rendimiento_texto": "1 unidad",
                "tiempo_preparacion_min": 10,
                "tiempo_amasado_min": 14,
                "tiempo_fermentacion_min": 50,
                "tiempo_horneado_min": 20,
                "temperatura_horneado": 180,
                "pasos": "1. Alistar los ingredientes.\n2. Mezclar harina, azucar y levadura.\n3. Incorporar huevo y mantequilla.\n4. Amasar hasta suavizar la masa.\n5. Dejar crecer.\n6. Formar y hornear.",
                "observaciones": "Si la superficie dora muy rapido, bajar ligeramente la temperatura.",
            },
            "Croissant": {
                "rendimiento_texto": "1 unidad",
                "tiempo_preparacion_min": 12,
                "tiempo_amasado_min": 15,
                "tiempo_fermentacion_min": 60,
                "tiempo_horneado_min": 22,
                "temperatura_horneado": 185,
                "pasos": "1. Preparar la masa base.\n2. Incorporar la mantequilla por capas.\n3. Laminar y plegar.\n4. Cortar, enrollar y dejar fermentar.\n5. Hornear hasta lograr capas definidas.",
                "observaciones": "Trabajar con la masa fria para conservar el laminado.",
            },
            "Integral": {
                "rendimiento_texto": "1 unidad",
                "tiempo_preparacion_min": 8,
                "tiempo_amasado_min": 12,
                "tiempo_fermentacion_min": 50,
                "tiempo_horneado_min": 20,
                "temperatura_horneado": 190,
                "pasos": "1. Pesar y mezclar los secos.\n2. Amasar hasta integrar por completo.\n3. Dejar fermentar.\n4. Formar la pieza.\n5. Hornear hasta coccion completa.",
                "observaciones": "Revisar que el interior quede seco y uniforme antes de sacar.",
            },
        }
        for producto, ficha in fichas_default.items():
            conn.execute("""
                INSERT OR IGNORE INTO receta_fichas (
                    producto, rendimiento_texto, tiempo_preparacion_min,
                    tiempo_amasado_min, tiempo_fermentacion_min,
                    tiempo_horneado_min, temperatura_horneado,
                    pasos, observaciones
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                producto,
                ficha["rendimiento_texto"],
                ficha["tiempo_preparacion_min"],
                ficha["tiempo_amasado_min"],
                ficha["tiempo_fermentacion_min"],
                ficha["tiempo_horneado_min"],
                ficha["temperatura_horneado"],
                ficha["pasos"],
                ficha["observaciones"],
            ))

        conn.commit()


def _migrar_productos(conn):
    """Agrega columnas de soporte si la tabla productos ya existia."""
    try:
        conn.execute("ALTER TABLE productos ADD COLUMN precio REAL NOT NULL DEFAULT 0.0")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE productos ADD COLUMN categoria TEXT NOT NULL DEFAULT 'Panaderia'")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE productos ADD COLUMN activo INTEGER NOT NULL DEFAULT 1")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE productos ADD COLUMN es_adicional INTEGER NOT NULL DEFAULT 0")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE productos ADD COLUMN stock_minimo INTEGER NOT NULL DEFAULT 0")
    except Exception:
        pass


def _sembrar_categorias_producto(conn):
    for categoria in CATEGORIAS_PREDETERMINADAS:
        conn.execute(
            "INSERT OR IGNORE INTO categorias_producto (nombre, activa) VALUES (?, 1)",
            (categoria,)
        )


def _migrar_usuarios(conn):
    """Recrea la tabla usuarios con el CHECK actualizado si mesero no esta permitido."""
    try:
        conn.execute(
            "INSERT INTO usuarios (nombre, pin, rol) VALUES ('__test__', ?, 'mesero')", (_hash_pin('9999'),)
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


def _migrar_recetas(conn):
    """Agrega soporte para unidades de receta y ficha tecnica por producto."""
    try:
        conn.execute("ALTER TABLE recetas ADD COLUMN unidad_receta TEXT")

        rows = conn.execute("""
            SELECT r.id, r.cantidad, i.unidad
            FROM recetas r
            JOIN insumos i ON i.id = r.insumo_id
        """).fetchall()
        for row in rows:
            unidad_receta = unidad_receta_sugerida(row["unidad"])
            cantidad_receta = convertir_cantidad(row["cantidad"], row["unidad"], unidad_receta)
            conn.execute(
                "UPDATE recetas SET cantidad = ?, unidad_receta = ? WHERE id = ?",
                (cantidad_receta, unidad_receta, row["id"])
            )
    except Exception:
        pass

    conn.execute("""
        UPDATE recetas
        SET unidad_receta = CASE
            WHEN unidad_receta IS NULL OR TRIM(unidad_receta) = '' THEN 'unidad'
            ELSE unidad_receta
        END
    """)


def _migrar_adicionales(conn):
    """Agrega soporte de unidades configurables y componentes en adicionales."""
    try:
        conn.execute("ALTER TABLE adicional_insumos ADD COLUMN unidad_config TEXT")
    except Exception:
        pass

    rows = conn.execute("""
        SELECT ai.id, ai.insumo_id, ai.cantidad, ai.unidad_config, i.unidad
        FROM adicional_insumos ai
        JOIN insumos i ON i.id = ai.insumo_id
    """).fetchall()
    for row in rows:
        unidad_config = str(row["unidad_config"] or "").strip()
        if not unidad_config:
            unidad_destino = unidad_receta_sugerida(row["unidad"])
            cantidad_convertida = convertir_cantidad(
                row["cantidad"], row["unidad"], unidad_destino
            )
            conn.execute(
                "UPDATE adicional_insumos SET cantidad = ?, unidad_config = ? WHERE id = ?",
                (cantidad_convertida, unidad_destino, row["id"])
            )


def _migrar_ventas_pedidos_caja(conn):
    """Agrega campos de pago, agrupacion de ventas y arqueo de caja."""
    try:
        conn.execute("ALTER TABLE ventas ADD COLUMN venta_grupo TEXT DEFAULT ''")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE ventas ADD COLUMN metodo_pago TEXT DEFAULT 'efectivo'")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE ventas ADD COLUMN monto_recibido REAL NOT NULL DEFAULT 0.0")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE ventas ADD COLUMN cambio REAL NOT NULL DEFAULT 0.0")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE ventas ADD COLUMN referencia_tipo TEXT DEFAULT 'pos'")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE ventas ADD COLUMN referencia_id INTEGER")
    except Exception:
        pass

    try:
        conn.execute("ALTER TABLE pedidos ADD COLUMN creado_en TEXT")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE pedidos ADD COLUMN pagado_en TEXT")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE pedidos ADD COLUMN pagado_por TEXT DEFAULT ''")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE pedidos ADD COLUMN metodo_pago TEXT DEFAULT ''")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE pedidos ADD COLUMN monto_recibido REAL NOT NULL DEFAULT 0.0")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE pedidos ADD COLUMN cambio REAL NOT NULL DEFAULT 0.0")
    except Exception:
        pass

    conn.execute("""
        UPDATE pedidos
        SET creado_en = COALESCE(creado_en, fecha || ' ' || hora)
        WHERE creado_en IS NULL OR TRIM(creado_en) = ''
    """)
    conn.execute("""
        UPDATE pedidos
        SET pagado_en = COALESCE(pagado_en, CASE
            WHEN hora_pagado IS NOT NULL AND TRIM(hora_pagado) != '' THEN fecha || ' ' || hora_pagado
            ELSE ''
        END)
        WHERE pagado_en IS NULL
    """)

    ventas_sin_grupo = conn.execute("""
        SELECT id, fecha, hora, producto
        FROM ventas
        WHERE venta_grupo IS NULL OR TRIM(venta_grupo) = ''
    """).fetchall()
    for row in ventas_sin_grupo:
        grupo = f"legacy-{row['fecha']}-{row['hora']}-{row['id']}"
        conn.execute(
            "UPDATE ventas SET venta_grupo = ? WHERE id = ?",
            (grupo, row["id"])
        )

    pedidos_historial = conn.execute("SELECT COUNT(*) as c FROM pedido_estado_historial").fetchone()
    if int(pedidos_historial["c"] or 0) == 0:
        pedidos = conn.execute("""
            SELECT id, estado, creado_en, hora_pagado, pagado_en, mesero, pagado_por
            FROM pedidos
            ORDER BY id
        """).fetchall()
        for pedido in pedidos:
            creado_en = pedido["creado_en"] or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            conn.execute("""
                INSERT INTO pedido_estado_historial (pedido_id, estado, cambiado_en, cambiado_por, detalle)
                VALUES (?, 'pendiente', ?, ?, ?)
            """, (pedido["id"], creado_en, pedido["mesero"] or "", "Pedido creado"))

            estado_actual = pedido["estado"]
            if estado_actual == "pagado":
                pagado_en = pedido["pagado_en"] or (
                    f"{creado_en[:10]} {pedido['hora_pagado']}" if pedido["hora_pagado"] else creado_en
                )
                conn.execute("""
                    INSERT INTO pedido_estado_historial (pedido_id, estado, cambiado_en, cambiado_por, detalle)
                    VALUES (?, 'pagado', ?, ?, ?)
                """, (pedido["id"], pagado_en, pedido["pagado_por"] or "", "Migrado desde pedidos existentes"))

    try:
        conn.execute("ALTER TABLE arqueos_caja ADD COLUMN efectivo_esperado REAL DEFAULT NULL")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE arqueos_caja ADD COLUMN diferencia_cierre REAL DEFAULT NULL")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE arqueos_caja ADD COLUMN notas_cierre TEXT DEFAULT ''")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE arqueos_caja ADD COLUMN reabierto_en TEXT DEFAULT ''")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE arqueos_caja ADD COLUMN reabierto_por TEXT DEFAULT ''")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE arqueos_caja ADD COLUMN motivo_reapertura TEXT DEFAULT ''")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE arqueos_caja ADD COLUMN reaperturas INTEGER NOT NULL DEFAULT 0")
    except Exception:
        pass


# ──────────────────────────────────────────────
# Productos
# ──────────────────────────────────────────────

def obtener_categorias_producto() -> list[str]:
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT nombre
            FROM categorias_producto
            WHERE activa = 1
            ORDER BY
                CASE nombre
                    WHEN 'Panaderia' THEN 1
                    WHEN 'Bebidas Calientes' THEN 2
                    WHEN 'Bebidas Frias' THEN 3
                    WHEN 'Desayunos' THEN 4
                    WHEN 'Almuerzos' THEN 5
                    ELSE 99
                END,
                nombre
        """).fetchall()
    return [r["nombre"] for r in rows]


def agregar_categoria_producto(nombre: str) -> bool:
    try:
        with get_connection() as conn:
            conn.execute(
                "INSERT INTO categorias_producto (nombre, activa) VALUES (?, 1)",
                (nombre,)
            )
            conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def obtener_productos(categoria: str = None) -> list[str]:
    filtro = "AND categoria = ?" if categoria else ""
    query = f"SELECT nombre FROM productos WHERE activo = 1 {filtro} ORDER BY nombre"
    params = (categoria,) if categoria else ()
    with get_connection() as conn:
        rows = conn.execute(query, params).fetchall()
    return [r["nombre"] for r in rows]


def obtener_productos_con_precio(categoria: str = None) -> list[dict]:
    filtro = "AND categoria = ?" if categoria else ""
    query = f"""
        SELECT id, nombre, precio, categoria, es_adicional
        FROM productos
        WHERE activo = 1 {filtro}
        ORDER BY categoria, nombre
    """
    params = (categoria,) if categoria else ()
    with get_connection() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def obtener_precio(producto: str) -> float:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT precio FROM productos WHERE nombre = ?", (producto,)
        ).fetchone()
    return row["precio"] if row else 0.0


def obtener_categoria_producto_nombre(producto: str) -> str:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT categoria FROM productos WHERE nombre = ?",
            (producto,)
        ).fetchone()
    return row["categoria"] if row else ""


def obtener_productos_adicionales() -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT id, nombre, precio, categoria
            FROM productos
            WHERE activo = 1 AND es_adicional = 1
            ORDER BY categoria, nombre
        """).fetchall()
    return [
        {
            "id": f"prod-{row['id']}",
            "nombre": row["nombre"],
            "precio": row["precio"],
            "categoria": row["categoria"],
            "tiene_configuracion": True,
            "fuente": "producto",
        }
        for row in rows
    ]


def agregar_producto(nombre: str, precio: float = 0.0, categoria: str = "Panaderia",
                     es_adicional: bool = False) -> bool:
    try:
        with get_connection() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO categorias_producto (nombre, activa) VALUES (?, 1)",
                (categoria,)
            )
            conn.execute(
                "INSERT INTO productos (nombre, precio, categoria, es_adicional) VALUES (?, ?, ?, ?)",
                (nombre, precio, categoria, 1 if es_adicional else 0)
            )
            conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def guardar_catalogo_productos(productos: list[dict]) -> dict:
    resultado = {
        "creados": 0,
        "actualizados": 0,
    }

    with get_connection() as conn:
        for producto in productos:
            nombre = producto["nombre"].strip()
            precio = float(producto["precio"])
            categoria = (producto.get("categoria") or "").strip()
            es_adicional = 1 if bool(producto.get("es_adicional")) else 0

            existente = conn.execute(
                "SELECT id, categoria, es_adicional FROM productos WHERE lower(nombre) = lower(?)",
                (nombre,)
            ).fetchone()

            if existente:
                categoria_final = categoria or existente["categoria"] or "Panaderia"
                es_adicional_final = es_adicional if "es_adicional" in producto else int(existente["es_adicional"] or 0)
                conn.execute(
                    "INSERT OR IGNORE INTO categorias_producto (nombre, activa) VALUES (?, 1)",
                    (categoria_final,)
                )
                conn.execute("""
                    UPDATE productos
                    SET nombre = ?, precio = ?, categoria = ?, es_adicional = ?, activo = 1
                    WHERE id = ?
                """, (nombre, precio, categoria_final, es_adicional_final, existente["id"]))
                resultado["actualizados"] += 1
            else:
                categoria_final = categoria or "Panaderia"
                conn.execute(
                    "INSERT OR IGNORE INTO categorias_producto (nombre, activa) VALUES (?, 1)",
                    (categoria_final,)
                )
                conn.execute(
                    "INSERT INTO productos (nombre, precio, categoria, es_adicional, activo) VALUES (?, ?, ?, ?, 1)",
                    (nombre, precio, categoria_final, es_adicional)
                )
                resultado["creados"] += 1

        conn.commit()

    return resultado


def guardar_catalogo_insumos(insumos: list[dict]) -> dict:
    resultado = {
        "creados": 0,
        "actualizados": 0,
    }

    with get_connection() as conn:
        for insumo in insumos:
            nombre = insumo["nombre"].strip()
            stock = float(insumo["stock"])

            existente = conn.execute("""
                SELECT id, unidad, stock_minimo
                FROM insumos
                WHERE lower(nombre) = lower(?)
            """, (nombre,)).fetchone()

            if existente:
                unidad = insumo.get("unidad")
                if unidad is None or str(unidad).strip() == "":
                    unidad = existente["unidad"] or "unidad"

                stock_minimo = insumo.get("stock_minimo")
                if stock_minimo is None:
                    stock_minimo = float(existente["stock_minimo"] or 0)
                else:
                    stock_minimo = float(stock_minimo)

                conn.execute("""
                    UPDATE insumos
                    SET nombre = ?, unidad = ?, stock = ?, stock_minimo = ?, activo = 1
                    WHERE id = ?
                """, (nombre, unidad, stock, stock_minimo, existente["id"]))
                resultado["actualizados"] += 1
            else:
                unidad = insumo.get("unidad") or "unidad"
                stock_minimo = float(insumo.get("stock_minimo", 0) or 0)
                conn.execute("""
                    INSERT INTO insumos (nombre, unidad, stock, stock_minimo, activo)
                    VALUES (?, ?, ?, ?, 1)
                """, (nombre, unidad, stock, stock_minimo))
                resultado["creados"] += 1

        conn.commit()

    return resultado


def _renombrar_producto_referencias_conn(conn, nombre_anterior: str, nuevo_nombre: str) -> None:
    if not nombre_anterior or not nuevo_nombre or nombre_anterior == nuevo_nombre:
        return

    actualizaciones = [
        ("ventas", "producto"),
        ("registros_diarios", "producto"),
        ("pedido_items", "producto"),
        ("recetas", "producto"),
        ("receta_fichas", "producto"),
        ("producto_componentes", "producto"),
        ("producto_componentes", "componente_producto"),
        ("adicional_componentes", "componente_producto"),
    ]

    _ALLOWED_RENAME_TARGETS = frozenset(actualizaciones)

    for tabla, columna in actualizaciones:
        if (tabla, columna) not in _ALLOWED_RENAME_TARGETS:
            raise ValueError(f"Referencia no permitida: {tabla}.{columna}")
        conn.execute(
            f"UPDATE {tabla} SET {columna} = ? WHERE {columna} = ?",
            (nuevo_nombre, nombre_anterior)
        )

    conn.execute("""
        UPDATE pedido_item_modificaciones
        SET descripcion = ?
        WHERE tipo = 'adicional' AND descripcion = ?
    """, (nuevo_nombre, nombre_anterior))


def actualizar_producto_completo(producto_id: int, nombre: str, precio: float,
                                 categoria: str, es_adicional: bool) -> bool:
    nombre = str(nombre or "").strip()
    categoria = str(categoria or "").strip() or "Panaderia"
    if producto_id <= 0 or not nombre:
        return False

    try:
        with get_connection() as conn:
            actual = conn.execute(
                "SELECT nombre FROM productos WHERE id = ?",
                (producto_id,)
            ).fetchone()
            if not actual:
                return False

            nombre_anterior = str(actual["nombre"] or "")
            conn.execute(
                "INSERT OR IGNORE INTO categorias_producto (nombre, activa) VALUES (?, 1)",
                (categoria,)
            )
            conn.execute("""
                UPDATE productos
                SET nombre = ?, precio = ?, categoria = ?, es_adicional = ?
                WHERE id = ?
            """, (nombre, float(precio), categoria, 1 if es_adicional else 0, producto_id))

            _renombrar_producto_referencias_conn(conn, nombre_anterior, nombre)
            conn.commit()
            return True
    except sqlite3.IntegrityError:
        return False
    except Exception:
        return False


def actualizar_precio(producto: str, nuevo_precio: float) -> bool:
    try:
        with get_connection() as conn:
            cur = conn.execute(
                "UPDATE productos SET precio = ? WHERE nombre = ?",
                (nuevo_precio, producto)
            )
            conn.commit()
        return cur.rowcount > 0
    except Exception:
        return False


def actualizar_categoria_producto(producto: str, nueva_categoria: str) -> bool:
    try:
        with get_connection() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO categorias_producto (nombre, activa) VALUES (?, 1)",
                (nueva_categoria,)
            )
            cur = conn.execute(
                "UPDATE productos SET categoria = ? WHERE nombre = ?",
                (nueva_categoria, producto)
            )
            conn.commit()
        return cur.rowcount > 0
    except Exception:
        return False


def actualizar_producto_adicional(producto: str, es_adicional: bool) -> bool:
    try:
        with get_connection() as conn:
            cur = conn.execute(
                "UPDATE productos SET es_adicional = ? WHERE nombre = ?",
                (1 if es_adicional else 0, producto)
            )
            conn.commit()
        return cur.rowcount > 0
    except Exception:
        return False


def eliminar_producto(producto: str) -> bool:
    """Desactiva un producto (soft delete)."""
    try:
        with get_connection() as conn:
            cur = conn.execute(
                "UPDATE productos SET activo = 0 WHERE nombre = ?",
                (producto,)
            )
            conn.commit()
        return cur.rowcount > 0
    except Exception:
        return False


def eliminar_producto_por_id(producto_id: int) -> bool:
    try:
        with get_connection() as conn:
            cur = conn.execute(
                "UPDATE productos SET activo = 0 WHERE id = ?",
                (producto_id,)
            )
            conn.commit()
        return cur.rowcount > 0
    except Exception:
        return False


def obtener_codigo_verificacion_caja() -> str:
    with get_connection() as conn:
        return _obtener_configuracion_conn(conn, "codigo_verificacion_caja", "2468")


def guardar_codigo_verificacion_caja(codigo: str) -> bool:
    codigo = str(codigo or "").strip()
    if not codigo:
        return False
    try:
        with get_connection() as conn:
            conn.execute("""
                INSERT INTO configuracion_sistema (clave, valor)
                VALUES ('codigo_verificacion_caja', ?)
                ON CONFLICT(clave) DO UPDATE SET valor = excluded.valor
            """, (codigo,))
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
            "SELECT nombre, pin, rol FROM usuarios WHERE pin = ?", (_hash_pin(pin),)
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
                (nombre, _hash_pin(pin), rol)
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


def _metodo_pago_normalizado(metodo_pago: str) -> str:
    metodo = str(metodo_pago or "").strip().lower()
    return metodo if metodo in ("efectivo", "transferencia") else "efectivo"


def _registrar_historial_estado_pedido(conn, pedido_id: int, estado: str,
                                       cambiado_por: str = "", detalle: str = "",
                                       cambiado_en: str | None = None) -> None:
    cambiado_en = cambiado_en or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute("""
        INSERT INTO pedido_estado_historial (pedido_id, estado, cambiado_en, cambiado_por, detalle)
        VALUES (?, ?, ?, ?, ?)
    """, (pedido_id, estado, cambiado_en, cambiado_por, detalle))


def obtener_arqueo_caja_activo(fecha: str | None = None) -> dict | None:
    fecha = fecha or datetime.now().strftime("%Y-%m-%d")
    with get_connection() as conn:
        row = conn.execute("""
            SELECT id, fecha, abierto_en, abierto_por, monto_apertura, estado, notas,
                   cerrado_en, cerrado_por, monto_cierre, efectivo_esperado,
                   diferencia_cierre, notas_cierre, reabierto_en, reabierto_por,
                   motivo_reapertura, reaperturas
            FROM arqueos_caja
            WHERE fecha = ? AND estado = 'abierto'
            ORDER BY abierto_en DESC
            LIMIT 1
        """, (fecha,)).fetchone()
    return dict(row) if row else None


def obtener_arqueo_caja_dia(fecha: str | None = None) -> dict | None:
    fecha = fecha or datetime.now().strftime("%Y-%m-%d")
    with get_connection() as conn:
        row = conn.execute("""
            SELECT id, fecha, abierto_en, abierto_por, monto_apertura, estado, notas,
                   cerrado_en, cerrado_por, monto_cierre, efectivo_esperado,
                   diferencia_cierre, notas_cierre, reabierto_en, reabierto_por,
                   motivo_reapertura, reaperturas
            FROM arqueos_caja
            WHERE fecha = ?
            ORDER BY
                CASE estado WHEN 'abierto' THEN 0 ELSE 1 END,
                abierto_en DESC
            LIMIT 1
        """, (fecha,)).fetchone()
    return dict(row) if row else None


def abrir_arqueo_caja(abierto_por: str, monto_apertura: float, notas: str = "",
                      fecha: str | None = None) -> dict:
    fecha = fecha or datetime.now().strftime("%Y-%m-%d")
    if monto_apertura < 0:
        return {"ok": False, "error": "El monto de apertura no puede ser negativo"}

    with get_connection() as conn:
        existente = conn.execute("""
            SELECT id, abierto_en, abierto_por, monto_apertura
            FROM arqueos_caja
            WHERE fecha = ? AND estado = 'abierto'
            ORDER BY abierto_en DESC
            LIMIT 1
        """, (fecha,)).fetchone()
        if existente:
            return {
                "ok": False,
                "error": "Ya hay un arqueo abierto para hoy",
                "arqueo": dict(existente),
            }

        abierto_en = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur = conn.execute("""
            INSERT INTO arqueos_caja (fecha, abierto_en, abierto_por, monto_apertura, estado, notas)
            VALUES (?, ?, ?, ?, 'abierto', ?)
        """, (fecha, abierto_en, abierto_por, float(monto_apertura), notas.strip()))
        conn.commit()

    arqueo = obtener_arqueo_caja_activo(fecha)
    return {"ok": True, "arqueo_id": cur.lastrowid, "arqueo": arqueo}


def obtener_historial_arqueos(limite: int = 15) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT id, fecha, abierto_en, abierto_por, monto_apertura, estado,
                   notas, cerrado_en, cerrado_por, monto_cierre,
                   efectivo_esperado, diferencia_cierre, notas_cierre,
                   reabierto_en, reabierto_por, motivo_reapertura, reaperturas
            FROM arqueos_caja
            ORDER BY abierto_en DESC
            LIMIT ?
        """, (limite,)).fetchall()
    return [dict(r) for r in rows]


def obtener_movimientos_caja(fecha: str | None = None, limite: int | None = None) -> list[dict]:
    fecha = fecha or datetime.now().strftime("%Y-%m-%d")
    query = """
        SELECT id, arqueo_id, fecha, creado_en, tipo, concepto, monto, registrado_por, notas
        FROM movimientos_caja
        WHERE fecha = ?
        ORDER BY creado_en DESC, id DESC
    """
    params: list[object] = [fecha]
    if limite is not None:
        query += " LIMIT ?"
        params.append(limite)
    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def registrar_movimiento_caja(tipo: str, concepto: str, monto: float,
                              registrado_por: str = "", notas: str = "",
                              fecha: str | None = None) -> dict:
    fecha = fecha or datetime.now().strftime("%Y-%m-%d")
    tipo = str(tipo or "").strip().lower()
    if tipo not in ("ingreso", "egreso"):
        return {"ok": False, "error": "Tipo de movimiento invalido"}
    if float(monto or 0) <= 0:
        return {"ok": False, "error": "El monto debe ser mayor a cero"}
    if not str(concepto or "").strip():
        return {"ok": False, "error": "El concepto es obligatorio"}

    arqueo = obtener_arqueo_caja_activo(fecha)
    if not arqueo:
        return {"ok": False, "error": "Debes tener una caja abierta para registrar movimientos"}

    creado_en = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with get_connection() as conn:
            cur = conn.execute("""
                INSERT INTO movimientos_caja (
                    arqueo_id, fecha, creado_en, tipo, concepto, monto, registrado_por, notas
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                arqueo["id"],
                fecha,
                creado_en,
                tipo,
                str(concepto).strip(),
                round(float(monto), 2),
                registrado_por,
                str(notas or "").strip(),
            ))
            conn.commit()
        return {"ok": True, "movimiento_id": cur.lastrowid}
    except Exception as e:
        logger.error(f"registrar_movimiento_caja: {e}")
        return {"ok": False, "error": str(e)}


def cerrar_arqueo_caja(cerrado_por: str, monto_cierre: float,
                       notas_cierre: str = "", codigo_verificacion: str = "",
                       fecha: str | None = None) -> dict:
    fecha = fecha or datetime.now().strftime("%Y-%m-%d")
    if float(monto_cierre or 0) < 0:
        return {"ok": False, "error": "El monto de cierre no puede ser negativo"}

    arqueo = obtener_arqueo_caja_activo(fecha)
    if not arqueo:
        return {"ok": False, "error": "No hay una caja abierta para cerrar"}

    codigo_real = obtener_codigo_verificacion_caja()
    if str(codigo_verificacion or "").strip() != codigo_real:
        return {"ok": False, "error": "Codigo de verificacion incorrecto"}

    resumen = obtener_resumen_caja_dia(fecha)
    efectivo_esperado = float(resumen.get("efectivo_en_caja", 0) or 0)
    monto_cierre = round(float(monto_cierre), 2)
    diferencia = round(monto_cierre - efectivo_esperado, 2)
    cerrado_en = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        with get_connection() as conn:
            conn.execute("""
                UPDATE arqueos_caja
                SET estado = 'cerrado',
                    cerrado_en = ?,
                    cerrado_por = ?,
                    monto_cierre = ?,
                    efectivo_esperado = ?,
                    diferencia_cierre = ?,
                    notas_cierre = ?
                WHERE id = ?
            """, (
                cerrado_en,
                cerrado_por,
                monto_cierre,
                efectivo_esperado,
                diferencia,
                str(notas_cierre or "").strip(),
                arqueo["id"],
            ))
            conn.commit()
        arqueo_final = obtener_arqueo_caja_dia(fecha)
        return {
            "ok": True,
            "arqueo": arqueo_final,
            "efectivo_esperado": efectivo_esperado,
            "monto_cierre": monto_cierre,
            "diferencia": diferencia,
        }
    except Exception as e:
        logger.error(f"cerrar_arqueo_caja: {e}")
        return {"ok": False, "error": str(e)}


def reabrir_arqueo_caja(reabierto_por: str, codigo_verificacion: str,
                        motivo_reapertura: str, fecha: str | None = None) -> dict:
    fecha = fecha or datetime.now().strftime("%Y-%m-%d")
    arqueo = obtener_arqueo_caja_dia(fecha)
    if not arqueo or arqueo.get("estado") != "cerrado":
        return {"ok": False, "error": "No hay una caja cerrada para reabrir"}
    if not str(motivo_reapertura or "").strip():
        return {"ok": False, "error": "Debes registrar la novedad de reapertura"}

    codigo_real = obtener_codigo_verificacion_caja()
    if str(codigo_verificacion or "").strip() != codigo_real:
        return {"ok": False, "error": "Codigo de verificacion incorrecto"}

    reabierto_en = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with get_connection() as conn:
            conn.execute("""
                UPDATE arqueos_caja
                SET estado = 'abierto',
                    reabierto_en = ?,
                    reabierto_por = ?,
                    motivo_reapertura = ?,
                    reaperturas = COALESCE(reaperturas, 0) + 1
                WHERE id = ?
            """, (
                reabierto_en,
                reabierto_por,
                str(motivo_reapertura).strip(),
                arqueo["id"],
            ))
            conn.commit()
        return {"ok": True, "arqueo": obtener_arqueo_caja_dia(fecha)}
    except Exception as e:
        logger.error(f"reabrir_arqueo_caja: {e}")
        return {"ok": False, "error": str(e)}


def obtener_resumen_caja_dia(fecha: str | None = None) -> dict:
    fecha = fecha or datetime.now().strftime("%Y-%m-%d")
    arqueo = obtener_arqueo_caja_dia(fecha)

    with get_connection() as conn:
        row = conn.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN metodo_pago = 'efectivo' THEN total_grupo ELSE 0 END), 0.0) AS ventas_efectivo,
                COALESCE(SUM(CASE WHEN metodo_pago = 'transferencia' THEN total_grupo ELSE 0 END), 0.0) AS ventas_transferencia,
                COALESCE(SUM(CASE WHEN metodo_pago = 'efectivo' THEN monto_recibido_grupo ELSE 0 END), 0.0) AS efectivo_recibido,
                COALESCE(SUM(cambio_grupo), 0.0) AS cambio_entregado,
                COALESCE(SUM(total_grupo), 0.0) AS total_ventas,
                COUNT(*) AS transacciones
            FROM (
                SELECT
                    COALESCE(NULLIF(venta_grupo, ''), 'legacy-' || id) AS grupo,
                    MAX(metodo_pago) AS metodo_pago,
                    MAX(monto_recibido) AS monto_recibido_grupo,
                    MAX(cambio) AS cambio_grupo,
                    SUM(total) AS total_grupo
                FROM ventas
                WHERE fecha = ?
                GROUP BY COALESCE(NULLIF(venta_grupo, ''), 'legacy-' || id)
            ) base
        """, (fecha,)).fetchone()

        movimientos = conn.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN tipo = 'ingreso' THEN monto ELSE 0 END), 0.0) AS ingresos,
                COALESCE(SUM(CASE WHEN tipo = 'egreso' THEN monto ELSE 0 END), 0.0) AS egresos,
                COUNT(*) AS total_movimientos
            FROM movimientos_caja
            WHERE fecha = ?
        """, (fecha,)).fetchone()

    ventas_efectivo = float(row["ventas_efectivo"] or 0.0)
    ventas_transferencia = float(row["ventas_transferencia"] or 0.0)
    monto_apertura = float((arqueo or {}).get("monto_apertura", 0.0) or 0.0)
    ingresos = float(movimientos["ingresos"] or 0.0)
    egresos = float(movimientos["egresos"] or 0.0)
    efectivo_esperado = monto_apertura + ventas_efectivo + ingresos - egresos

    return {
        "fecha": fecha,
        "arqueo_activo": bool(arqueo and arqueo.get("estado") == "abierto"),
        "arqueo_cerrado": bool(arqueo and arqueo.get("estado") == "cerrado"),
        "arqueo": arqueo,
        "monto_apertura": round(monto_apertura, 2),
        "ventas_efectivo": round(ventas_efectivo, 2),
        "ventas_transferencia": round(ventas_transferencia, 2),
        "efectivo_recibido": round(float(row["efectivo_recibido"] or 0.0), 2),
        "cambio_entregado": round(float(row["cambio_entregado"] or 0.0), 2),
        "total_ventas": round(float(row["total_ventas"] or 0.0), 2),
        "transacciones": int(row["transacciones"] or 0),
        "ingresos_manuales": round(ingresos, 2),
        "egresos_manuales": round(egresos, 2),
        "total_movimientos": int(movimientos["total_movimientos"] or 0),
        "efectivo_en_caja": round(efectivo_esperado, 2),
        "metodos_pago": [
            {"metodo": "Efectivo", "total": round(ventas_efectivo, 2)},
            {"metodo": "Transferencia", "total": round(ventas_transferencia, 2)},
        ],
        "cierre": {
            "monto_cierre": round(float((arqueo or {}).get("monto_cierre", 0.0) or 0.0), 2),
            "efectivo_esperado": round(float((arqueo or {}).get("efectivo_esperado", efectivo_esperado) or 0.0), 2),
            "diferencia": round(float((arqueo or {}).get("diferencia_cierre", 0.0) or 0.0), 2),
            "cerrado_en": (arqueo or {}).get("cerrado_en"),
            "cerrado_por": (arqueo or {}).get("cerrado_por", ""),
            "notas_cierre": (arqueo or {}).get("notas_cierre", ""),
            "reabierto_en": (arqueo or {}).get("reabierto_en", ""),
            "reabierto_por": (arqueo or {}).get("reabierto_por", ""),
            "motivo_reapertura": (arqueo or {}).get("motivo_reapertura", ""),
            "reaperturas": int((arqueo or {}).get("reaperturas", 0) or 0),
        },
    }


# ──────────────────────────────────────────────
# Ventas (cajero)
# ──────────────────────────────────────────────

def registrar_venta_lote(items: list[dict], registrado_por: str = "",
                         metodo_pago: str = "efectivo", monto_recibido: float | None = None,
                         referencia_tipo: str = "pos", referencia_id: int | None = None,
                         fecha_hora: datetime | None = None) -> dict:
    if not items:
        return {"ok": False, "error": "No hay items para registrar"}

    ahora = fecha_hora or datetime.now()
    fecha = ahora.strftime("%Y-%m-%d")
    hora = ahora.strftime("%H:%M:%S")
    metodo_pago = _metodo_pago_normalizado(metodo_pago)

    arqueo = obtener_arqueo_caja_activo(fecha)
    if not arqueo:
        return {
            "ok": False,
            "error": "Debes abrir el arqueo de caja antes de registrar ventas",
        }

    total = round(sum(
        float(item.get("total", item.get("cantidad", 0) * item.get("precio", 0)) or 0)
        for item in items
    ), 2)

    if metodo_pago == "transferencia":
        monto_recibido_final = total
        cambio = 0.0
    else:
        monto_recibido_final = float(monto_recibido if monto_recibido is not None else total)
        if monto_recibido_final + 1e-9 < total:
            return {
                "ok": False,
                "error": "El monto recibido no alcanza para cubrir el total",
            }
        cambio = round(monto_recibido_final - total, 2)

    venta_grupo = f"venta-{uuid4().hex[:12]}"

    try:
        with get_connection() as conn:
            for item in items:
                producto = str(item.get("producto", "") or "").strip()
                cantidad = int(item.get("cantidad", 0) or 0)
                if not producto or cantidad <= 0:
                    continue

                precio_unitario = float(item.get("precio", 0) or 0)
                total_item = round(float(item.get("total", cantidad * precio_unitario) or 0), 2)

                conn.execute("""
                    INSERT INTO ventas (
                        fecha, hora, producto, cantidad, precio_unitario, total,
                        registrado_por, venta_grupo, metodo_pago, monto_recibido,
                        cambio, referencia_tipo, referencia_id
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    fecha,
                    hora,
                    producto,
                    cantidad,
                    precio_unitario,
                    total_item,
                    registrado_por,
                    venta_grupo,
                    metodo_pago,
                    monto_recibido_final,
                    cambio,
                    referencia_tipo,
                    referencia_id,
                ))
            conn.commit()
        return {
            "ok": True,
            "venta_grupo": venta_grupo,
            "fecha": fecha,
            "hora": hora,
            "total": total,
            "metodo_pago": metodo_pago,
            "monto_recibido": round(monto_recibido_final, 2),
            "cambio": cambio,
        }
    except Exception as e:
        print(f"[ERROR] registrar_venta_lote: {e}")
        return {"ok": False, "error": str(e)}


def registrar_venta(producto: str, cantidad: int,
                    precio_unitario: float, registrado_por: str = "") -> bool:
    resultado = registrar_venta_lote([{
        "producto": producto,
        "cantidad": cantidad,
        "precio": precio_unitario,
        "total": round(cantidad * precio_unitario, 2),
    }], registrado_por=registrado_por)
    return bool(resultado.get("ok"))


def obtener_ventas_dia(fecha: str = None) -> list[dict]:
    """Retorna todas las ventas de un dia."""
    if fecha is None:
        fecha = datetime.now().strftime("%Y-%m-%d")
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT hora, producto, cantidad, precio_unitario, total, registrado_por,
                   venta_grupo, metodo_pago, monto_recibido, cambio,
                   referencia_tipo, referencia_id
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
                   COUNT(DISTINCT COALESCE(NULLIF(venta_grupo, ''), 'legacy-' || id)) as num_ventas
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
                   COUNT(DISTINCT COALESCE(NULLIF(venta_grupo, ''), 'legacy-' || id)) as transacciones
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


def obtener_ventas_rango(dias: int = 30, producto: str | None = None) -> list[dict]:
    """Ventas detalladas de los ultimos N dias."""
    query = """
        SELECT fecha, hora, producto, cantidad, precio_unitario, total, registrado_por,
               venta_grupo, metodo_pago, monto_recibido, cambio, referencia_tipo, referencia_id
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
    """Totales agregados de ventas para los ultimos N dias."""
    query = """
        SELECT COALESCE(SUM(cantidad), 0) as panes,
               COALESCE(SUM(total), 0.0) as dinero,
               COUNT(DISTINCT COALESCE(NULLIF(venta_grupo, ''), 'legacy-' || id)) as transacciones
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
    """Serie diaria de panes/ingresos/transacciones."""
    query = """
        SELECT fecha,
               COALESCE(SUM(cantidad), 0) as panes,
               COALESCE(SUM(total), 0.0) as dinero,
               COUNT(DISTINCT COALESCE(NULLIF(venta_grupo, ''), 'legacy-' || id)) as transacciones
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
    """Ranking de productos por ingresos en los ultimos N dias."""
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT producto,
                   COALESCE(SUM(cantidad), 0) as panes,
                   COALESCE(SUM(total), 0.0) as dinero,
                   COUNT(DISTINCT COALESCE(NULLIF(venta_grupo, ''), 'legacy-' || id)) as transacciones
            FROM ventas
            WHERE fecha >= date('now', ?)
            GROUP BY producto
            ORDER BY dinero DESC
        """, (f"-{dias} days",)).fetchall()
    return [dict(r) for r in rows]


# ──────────────────────────────────────────────
# Registros diarios (produccion - panadero)
# ──────────────────────────────────────────────

def obtener_resumen_medios_pago_rango(dias: int = 30, producto: str | None = None) -> list[dict]:
    """Totales por metodo de pago para los ultimos N dias."""
    query = """
        SELECT COALESCE(NULLIF(metodo_pago, ''), 'efectivo') as metodo,
               COALESCE(SUM(total), 0.0) as total,
               COUNT(DISTINCT COALESCE(NULLIF(venta_grupo, ''), 'legacy-' || id)) as transacciones
        FROM ventas
        WHERE fecha >= date('now', ?)
        {filtro}
        GROUP BY COALESCE(NULLIF(metodo_pago, ''), 'efectivo')
        ORDER BY total DESC
    """
    filtro = "AND producto = ?" if producto else ""
    query = query.format(filtro=filtro)
    params = [f"-{dias} days"]
    if producto:
        params.append(producto)

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def obtener_arqueos_rango(dias: int = 30) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT id, fecha, abierto_en, abierto_por, monto_apertura, estado,
                   notas, cerrado_en, cerrado_por, monto_cierre,
                   efectivo_esperado, diferencia_cierre, notas_cierre,
                   reabierto_en, reabierto_por, motivo_reapertura, reaperturas
            FROM arqueos_caja
            WHERE fecha >= date('now', ?)
            ORDER BY fecha DESC, abierto_en DESC
        """, (f"-{dias} days",)).fetchall()
    return [dict(r) for r in rows]


def obtener_movimientos_caja_rango(dias: int = 30) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT id, arqueo_id, fecha, creado_en, tipo, concepto, monto, registrado_por, notas
            FROM movimientos_caja
            WHERE fecha >= date('now', ?)
            ORDER BY fecha DESC, creado_en DESC, id DESC
        """, (f"-{dias} days",)).fetchall()
    return [dict(r) for r in rows]


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
            previo = conn.execute(
                "SELECT producido FROM registros_diarios WHERE fecha = ? AND producto = ?",
                (fecha, producto)
            ).fetchone()
            producido_anterior = int(previo["producido"] or 0) if previo else 0
            delta_producido = int(producido or 0) - producido_anterior

            conn.execute("""
                INSERT INTO registros_diarios
                    (fecha, dia_semana, producto, producido, vendido, observaciones)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(fecha, producto) DO UPDATE SET
                    producido     = excluded.producido,
                    vendido       = excluded.vendido,
                    observaciones = excluded.observaciones
            """, (fecha, dia_semana, producto, producido, vendido, observaciones))

            if delta_producido != 0 and _categoria_producto_conn(conn, producto) == "Panaderia":
                consumo_producto = _consumo_producto(
                    conn, producto, abs(delta_producido), incluir_panaderia=True
                )
                for insumo_id, datos in consumo_producto.items():
                    if delta_producido > 0:
                        conn.execute(
                            "UPDATE insumos SET stock = MAX(0, stock - ?) WHERE id = ?",
                            (datos["cantidad"], insumo_id)
                        )
                    else:
                        conn.execute(
                            "UPDATE insumos SET stock = stock + ? WHERE id = ?",
                            (datos["cantidad"], insumo_id)
                        )
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
            existente = conn.execute(
                "SELECT id, activa FROM mesas WHERE numero = ?",
                (numero,)
            ).fetchone()
            if existente:
                if existente["activa"] == 1:
                    return False
                conn.execute(
                    "UPDATE mesas SET nombre = ?, activa = 1 WHERE id = ?",
                    (nombre, existente["id"])
                )
            else:
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
    creado_en = ahora.strftime("%Y-%m-%d %H:%M:%S")

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
                INSERT INTO pedidos (mesa_id, mesero, estado, fecha, hora, creado_en, notas, total)
                VALUES (?, ?, 'pendiente', ?, ?, ?, ?, ?)
            """, (mesa_id, mesero, fecha, hora, creado_en, notas, total))
            pedido_id = cursor.lastrowid
            _registrar_historial_estado_pedido(
                conn,
                pedido_id,
                "pendiente",
                cambiado_por=mesero,
                detalle="Pedido creado",
                cambiado_en=creado_en,
            )

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
               p.mesero, p.estado, p.fecha, p.hora, p.hora_pagado, p.creado_en,
               p.pagado_en, p.pagado_por, p.metodo_pago, p.monto_recibido,
               p.cambio, p.notas, p.total
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


def obtener_pedidos_con_detalle(estado: str = None, mesa_id: int = None,
                                fecha: str = None) -> list[dict]:
    """Obtiene pedidos con items, modificaciones e historial en pocas queries (sin N+1)."""
    if fecha is None:
        fecha = datetime.now().strftime("%Y-%m-%d")

    # 1) Pedidos base (con JOIN a mesas)
    query = """
        SELECT p.id, p.mesa_id, m.numero as mesa_numero, m.nombre as mesa_nombre,
               p.mesero, p.estado, p.fecha, p.hora, p.hora_pagado, p.creado_en,
               p.pagado_en, p.pagado_por, p.metodo_pago, p.monto_recibido,
               p.cambio, p.notas, p.total
        FROM pedidos p
        LEFT JOIN mesas m ON p.mesa_id = m.id
        WHERE p.fecha = ?
    """
    params: list = [fecha]

    if estado:
        query += " AND p.estado = ?"
        params.append(estado)
    if mesa_id:
        query += " AND p.mesa_id = ?"
        params.append(mesa_id)

    query += " ORDER BY p.hora DESC"

    with get_connection() as conn:
        pedido_rows = conn.execute(query, params).fetchall()
        pedidos = [dict(r) for r in pedido_rows]

        if not pedidos:
            return []

        pedido_ids = [p["id"] for p in pedidos]
        placeholders = ",".join("?" * len(pedido_ids))

        # 2) Todos los items de todos los pedidos en 1 query
        items_rows = conn.execute(f"""
            SELECT id, pedido_id, producto, cantidad, precio_unitario, subtotal, notas
            FROM pedido_items
            WHERE pedido_id IN ({placeholders})
            ORDER BY id
        """, pedido_ids).fetchall()

        items_by_pedido: dict[int, list[dict]] = {}
        all_item_ids: list[int] = []
        for row in items_rows:
            item = dict(row)
            all_item_ids.append(item["id"])
            items_by_pedido.setdefault(item["pedido_id"], []).append(item)

        # 3) Todas las modificaciones de todos los items en 1 query
        mods_by_item: dict[int, list[dict]] = {}
        if all_item_ids:
            item_placeholders = ",".join("?" * len(all_item_ids))
            mods_rows = conn.execute(f"""
                SELECT id, pedido_item_id, tipo, descripcion, cantidad, precio_extra
                FROM pedido_item_modificaciones
                WHERE pedido_item_id IN ({item_placeholders})
                ORDER BY tipo, id
            """, all_item_ids).fetchall()
            for row in mods_rows:
                mod = dict(row)
                mods_by_item.setdefault(mod["pedido_item_id"], []).append(mod)

        # 4) Todo el historial de estados en 1 query
        historial_rows = conn.execute(f"""
            SELECT pedido_id, estado, cambiado_en, cambiado_por, detalle
            FROM pedido_estado_historial
            WHERE pedido_id IN ({placeholders})
            ORDER BY cambiado_en ASC, id ASC
        """, pedido_ids).fetchall()

        historial_by_pedido: dict[int, list[dict]] = {}
        for row in historial_rows:
            h = dict(row)
            historial_by_pedido.setdefault(h["pedido_id"], []).append(h)

    # 5) Ensamblar resultado
    for p in pedidos:
        pid = p["id"]
        p_items = items_by_pedido.get(pid, [])
        for item in p_items:
            item["modificaciones"] = mods_by_item.get(item["id"], [])
            # Limpiar campo auxiliar
            item.pop("pedido_id", None)
        p["items"] = p_items
        p["historial_estados"] = historial_by_pedido.get(pid, [])

    return pedidos


def obtener_pedido(pedido_id: int) -> dict | None:
    """Obtiene un pedido con sus items y modificaciones."""
    with get_connection() as conn:
        pedido = conn.execute("""
            SELECT p.id, p.mesa_id, m.numero as mesa_numero, m.nombre as mesa_nombre,
                   p.mesero, p.estado, p.fecha, p.hora, p.hora_pagado, p.creado_en,
                   p.pagado_en, p.pagado_por, p.metodo_pago, p.monto_recibido,
                   p.cambio, p.notas, p.total
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

        historial = conn.execute("""
            SELECT estado, cambiado_en, cambiado_por, detalle
            FROM pedido_estado_historial
            WHERE pedido_id = ?
            ORDER BY cambiado_en ASC, id ASC
        """, (pedido_id,)).fetchall()

    result = dict(pedido)
    result["items"] = items_list
    result["historial_estados"] = [dict(h) for h in historial]
    return result


def obtener_pedidos_con_detalle(fecha: str | None = None, estado: str | None = None,
                                mesa_id: int | None = None) -> list[dict]:
    """Obtiene pedidos con items, modificaciones e historial en queries eficientes (sin N+1)."""
    fecha = fecha or datetime.now().strftime("%Y-%m-%d")
    with get_connection() as conn:
        query = """
            SELECT p.id, p.mesa_id, m.numero as mesa_numero, m.nombre as mesa_nombre,
                   p.mesero, p.estado, p.fecha, p.hora, p.hora_pagado, p.creado_en,
                   p.pagado_en, p.pagado_por, p.metodo_pago, p.monto_recibido,
                   p.cambio, p.notas, p.total
            FROM pedidos p
            LEFT JOIN mesas m ON p.mesa_id = m.id
            WHERE p.fecha = ?
        """
        params: list = [fecha]
        if estado:
            query += " AND p.estado = ?"
            params.append(estado)
        if mesa_id:
            query += " AND p.mesa_id = ?"
            params.append(mesa_id)
        query += " ORDER BY p.hora DESC"

        pedidos = [dict(r) for r in conn.execute(query, params).fetchall()]
        if not pedidos:
            return []

        pedido_ids = [p["id"] for p in pedidos]
        ph = ",".join("?" * len(pedido_ids))

        items_rows = conn.execute(
            f"SELECT id, pedido_id, producto, cantidad, precio_unitario, subtotal, notas "
            f"FROM pedido_items WHERE pedido_id IN ({ph}) ORDER BY pedido_id, id",
            pedido_ids
        ).fetchall()

        item_ids = [r["id"] for r in items_rows]
        mods_by_item: dict[int, list] = {}
        if item_ids:
            ph2 = ",".join("?" * len(item_ids))
            for m in conn.execute(
                f"SELECT pedido_item_id, id, tipo, descripcion, cantidad, precio_extra "
                f"FROM pedido_item_modificaciones WHERE pedido_item_id IN ({ph2}) "
                f"ORDER BY pedido_item_id, tipo, id",
                item_ids
            ).fetchall():
                mods_by_item.setdefault(m["pedido_item_id"], []).append(dict(m))

        items_by_pedido: dict[int, list] = {}
        for row in items_rows:
            item = dict(row)
            item["modificaciones"] = mods_by_item.get(item["id"], [])
            items_by_pedido.setdefault(item["pedido_id"], []).append(item)

        hist_by_pedido: dict[int, list] = {}
        for h in conn.execute(
            f"SELECT pedido_id, estado, cambiado_en, cambiado_por, detalle "
            f"FROM pedido_estado_historial WHERE pedido_id IN ({ph}) "
            f"ORDER BY cambiado_en ASC, id ASC",
            pedido_ids
        ).fetchall():
            hist_by_pedido.setdefault(h["pedido_id"], []).append(dict(h))

        for p in pedidos:
            p["items"] = items_by_pedido.get(p["id"], [])
            p["historial_estados"] = hist_by_pedido.get(p["id"], [])

    return pedidos


def cambiar_estado_pedido(pedido_id: int, nuevo_estado: str,
                          cambiado_por: str = "") -> bool:
    """Cambia el estado de un pedido."""
    try:
        with get_connection() as conn:
            pedido = conn.execute(
                "SELECT estado FROM pedidos WHERE id = ?",
                (pedido_id,)
            ).fetchone()
            if not pedido:
                return False
            if pedido["estado"] == nuevo_estado:
                return True

            cambiado_en = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            detalle = f"Estado actualizado a {nuevo_estado.replace('_', ' ')}"
            if nuevo_estado == "pagado":
                hora_pagado = cambiado_en[11:19]
                conn.execute(
                    "UPDATE pedidos SET estado = ?, hora_pagado = ?, pagado_en = ?, pagado_por = ? WHERE id = ?",
                    (nuevo_estado, hora_pagado, cambiado_en, cambiado_por, pedido_id)
                )
            else:
                conn.execute(
                    "UPDATE pedidos SET estado = ? WHERE id = ?",
                    (nuevo_estado, pedido_id)
                )
            _registrar_historial_estado_pedido(
                conn,
                pedido_id,
                nuevo_estado,
                cambiado_por=cambiado_por,
                detalle=detalle,
                cambiado_en=cambiado_en,
            )
            conn.commit()
        return True
    except Exception as e:
        print(f"[ERROR] cambiar_estado_pedido: {e}")
        return False


def pagar_pedido(pedido_id: int, registrado_por: str = "",
                 metodo_pago: str = "efectivo",
                 monto_recibido: float | None = None) -> dict:
    """Marca pedido como pagado, registra ventas y descuenta inventario."""
    try:
        pedido = obtener_pedido(pedido_id)
        if not pedido:
            return {"ok": False, "error": "Pedido no encontrado"}
        if pedido["estado"] == "pagado":
            return {"ok": False, "error": "El pedido ya fue pagado"}

        ahora = datetime.now()
        fecha_cobro = ahora.strftime("%Y-%m-%d")
        hora_pagado = ahora.strftime("%H:%M:%S")
        pagado_en = ahora.strftime("%Y-%m-%d %H:%M:%S")
        metodo_pago = _metodo_pago_normalizado(metodo_pago)

        arqueo = obtener_arqueo_caja_activo(fecha_cobro)
        if not arqueo:
            return {
                "ok": False,
                "error": "Debes abrir el arqueo de caja antes de cobrar pedidos",
            }

        total_pedido = round(float(pedido["total"] or 0), 2)
        if metodo_pago == "transferencia":
            monto_recibido_final = total_pedido
            cambio = 0.0
        else:
            monto_recibido_final = float(monto_recibido if monto_recibido is not None else total_pedido)
            if monto_recibido_final + 1e-9 < total_pedido:
                return {
                    "ok": False,
                    "error": "El monto recibido no alcanza para cubrir el pedido",
                }
            cambio = round(monto_recibido_final - total_pedido, 2)

        with get_connection() as conn:
            venta_grupo = f"pedido-{pedido_id}-{uuid4().hex[:10]}"

            for item in pedido["items"]:
                subtotal = round(float(item["subtotal"] or 0), 2)
                precio_unitario_venta = round(
                    subtotal / item["cantidad"], 2
                ) if int(item["cantidad"] or 0) > 0 else 0.0
                conn.execute("""
                    INSERT INTO ventas (
                        fecha, hora, producto, cantidad, precio_unitario, total,
                        registrado_por, venta_grupo, metodo_pago, monto_recibido,
                        cambio, referencia_tipo, referencia_id
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pedido', ?)
                """, (
                    fecha_cobro,
                    hora_pagado,
                    item["producto"],
                    item["cantidad"],
                    precio_unitario_venta,
                    subtotal,
                    registrado_por,
                    venta_grupo,
                    metodo_pago,
                    monto_recibido_final,
                    cambio,
                    pedido_id,
                ))

                # Descontar inventario por composicion base del producto
                consumo_producto = _consumo_producto(
                    conn, item["producto"], item["cantidad"], incluir_panaderia=False
                )
                for insumo_id, datos in consumo_producto.items():
                    conn.execute(
                        "UPDATE insumos SET stock = MAX(0, stock - ?) WHERE id = ?",
                        (datos["cantidad"], insumo_id)
                    )

                # Descontar inventario por adicionales
                for mod in item.get("modificaciones", []):
                    if mod["tipo"] == "adicional":
                        consumo_adicional = {}
                        _acumular_consumo_modificacion(
                            conn,
                            mod["descripcion"],
                            float(mod.get("cantidad", 1) or 1),
                            consumo_adicional,
                            incluir_panaderia=False,
                        )
                        for insumo_id, datos in consumo_adicional.items():
                            conn.execute(
                                "UPDATE insumos SET stock = MAX(0, stock - ?) WHERE id = ?",
                                (datos["cantidad"], insumo_id)
                            )

            # Marcar como pagado
            conn.execute(
                """
                UPDATE pedidos
                SET estado = 'pagado',
                    hora_pagado = ?,
                    pagado_en = ?,
                    pagado_por = ?,
                    metodo_pago = ?,
                    monto_recibido = ?,
                    cambio = ?
                WHERE id = ?
                """,
                (hora_pagado, pagado_en, registrado_por, metodo_pago, monto_recibido_final, cambio, pedido_id)
            )
            _registrar_historial_estado_pedido(
                conn,
                pedido_id,
                "pagado",
                cambiado_por=registrado_por,
                detalle=f"Cobro registrado por {metodo_pago}",
                cambiado_en=pagado_en,
            )
            conn.commit()
        return {
            "ok": True,
            "pedido_id": pedido_id,
            "venta_grupo": venta_grupo,
            "fecha": fecha_cobro,
            "hora": hora_pagado,
            "metodo_pago": metodo_pago,
            "monto_recibido": round(monto_recibido_final, 2),
            "cambio": cambio,
            "total": total_pedido,
        }
    except Exception as e:
        print(f"[ERROR] pagar_pedido: {e}")
        return {"ok": False, "error": str(e)}


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
        resultado = []
        for row in rows:
            adicional_id = row["id"]
            insumos = conn.execute("""
                SELECT ai.insumo_id, ai.cantidad, ai.unidad_config,
                       i.nombre as insumo_nombre, i.unidad as unidad_inventario
                FROM adicional_insumos ai
                JOIN insumos i ON i.id = ai.insumo_id
                WHERE ai.adicional_id = ?
                ORDER BY i.nombre
            """, (adicional_id,)).fetchall()
            componentes = conn.execute("""
                SELECT componente_producto, cantidad
                FROM adicional_componentes
                WHERE adicional_id = ?
                ORDER BY componente_producto
            """, (adicional_id,)).fetchall()

            adicional = dict(row)
            adicional["insumos"] = [dict(i) for i in insumos]
            adicional["componentes"] = [dict(c) for c in componentes]
            adicional["tiene_configuracion"] = bool(adicional["insumos"] or adicional["componentes"])
            resultado.append(adicional)
    return resultado


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


def actualizar_adicional_detalle(adicional_id: int, nombre: str, precio: float) -> bool:
    nombre = str(nombre or "").strip()
    if adicional_id <= 0 or not nombre:
        return False

    try:
        with get_connection() as conn:
            existe = conn.execute(
                "SELECT id FROM adicionales WHERE id = ?",
                (adicional_id,)
            ).fetchone()
            if not existe:
                return False

            conn.execute(
                "UPDATE adicionales SET nombre = ?, precio = ? WHERE id = ?",
                (nombre, float(precio), adicional_id)
            )
            conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    except Exception:
        return False


def actualizar_adicional(adicional_id: int, precio: float) -> bool:
    try:
        with get_connection() as conn:
            cur = conn.execute(
                "UPDATE adicionales SET precio = ? WHERE id = ?",
                (precio, adicional_id)
            )
            conn.commit()
        return cur.rowcount > 0
    except Exception:
        return False


def eliminar_adicional(adicional_id: int) -> bool:
    try:
        with get_connection() as conn:
            cur = conn.execute("UPDATE adicionales SET activo = 0 WHERE id = ?", (adicional_id,))
            conn.commit()
        return cur.rowcount > 0
    except Exception:
        return False


def guardar_configuracion_adicional(adicional_id: int, insumos: list[dict] | None = None,
                                    componentes: list[dict] | None = None) -> bool:
    insumos = insumos or []
    componentes = componentes or []
    try:
        with get_connection() as conn:
            conn.execute("DELETE FROM adicional_insumos WHERE adicional_id = ?", (adicional_id,))
            conn.execute("DELETE FROM adicional_componentes WHERE adicional_id = ?", (adicional_id,))

            usados_insumo: set[int] = set()
            for item in insumos:
                insumo_id = int(item["insumo_id"])
                cantidad = float(item.get("cantidad", 0) or 0)
                if cantidad <= 0:
                    continue
                if insumo_id in usados_insumo:
                    continue
                usados_insumo.add(insumo_id)
                conn.execute("""
                    INSERT INTO adicional_insumos (adicional_id, insumo_id, cantidad, unidad_config)
                    VALUES (?, ?, ?, ?)
                """, (
                    adicional_id,
                    insumo_id,
                    cantidad,
                    str(item.get("unidad_config", "unidad") or "unidad").strip(),
                ))

            usados_componente: set[str] = set()
            for item in componentes:
                componente = str(item.get("componente_producto", "") or "").strip()
                cantidad = float(item.get("cantidad", 0) or 0)
                if not componente or componente in usados_componente:
                    continue
                if cantidad <= 0:
                    continue
                usados_componente.add(componente)
                conn.execute("""
                    INSERT INTO adicional_componentes (adicional_id, componente_producto, cantidad)
                    VALUES (?, ?, ?)
                """, (
                    adicional_id,
                    componente,
                    cantidad,
                ))

            conn.commit()
        return True
    except Exception as e:
        print(f"[ERROR] guardar_configuracion_adicional: {e}")
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

def _categoria_producto_conn(conn, producto: str) -> str:
    row = conn.execute(
        "SELECT categoria FROM productos WHERE nombre = ?",
        (producto,)
    ).fetchone()
    return row["categoria"] if row else ""


def _resolver_adicional_conn(conn, descripcion: str) -> dict | None:
    descripcion = str(descripcion or "").strip()
    if not descripcion:
        return None

    producto = conn.execute("""
        SELECT id, nombre, precio, categoria
        FROM productos
        WHERE nombre = ? AND activo = 1 AND es_adicional = 1
    """, (descripcion,)).fetchone()
    if producto:
        return {
            "tipo": "producto",
            "id": producto["id"],
            "nombre": producto["nombre"],
            "precio": float(producto["precio"] or 0),
            "categoria": producto["categoria"],
        }

    adicional = conn.execute("""
        SELECT id, nombre, precio
        FROM adicionales
        WHERE nombre = ? AND activo = 1
    """, (descripcion,)).fetchone()
    if adicional:
        return {
            "tipo": "catalogo",
            "id": adicional["id"],
            "nombre": adicional["nombre"],
            "precio": float(adicional["precio"] or 0),
        }
    return None


def _acumular_requerimiento_panaderia_producto(conn, producto: str, cantidad: float,
                                               requeridos: dict[str, float],
                                               ruta: tuple[str, ...] = ()) -> None:
    if cantidad <= 0:
        return
    if producto in ruta:
        raise ValueError(f"Ciclo detectado en la composicion del producto: {' > '.join(ruta + (producto,))}")

    categoria = _categoria_producto_conn(conn, producto)
    if categoria == "Panaderia":
        requeridos[producto] = requeridos.get(producto, 0.0) + float(cantidad)
        return

    componentes = conn.execute("""
        SELECT componente_producto, cantidad
        FROM producto_componentes
        WHERE producto = ?
        ORDER BY componente_producto
    """, (producto,)).fetchall()

    for componente in componentes:
        _acumular_requerimiento_panaderia_producto(
            conn,
            componente["componente_producto"],
            float(componente["cantidad"] or 0) * float(cantidad),
            requeridos,
            ruta + (producto,),
        )


def _acumular_requerimiento_panaderia_adicional(conn, adicional_id: int, cantidad: float,
                                                requeridos: dict[str, float]) -> None:
    if cantidad <= 0:
        return

    componentes = conn.execute("""
        SELECT componente_producto, cantidad
        FROM adicional_componentes
        WHERE adicional_id = ?
        ORDER BY componente_producto
    """, (adicional_id,)).fetchall()

    for componente in componentes:
        _acumular_requerimiento_panaderia_producto(
            conn,
            componente["componente_producto"],
            float(componente["cantidad"] or 0) * float(cantidad),
            requeridos,
        )


def _acumular_requerimiento_panaderia_modificacion(conn, descripcion: str, cantidad: float,
                                                   requeridos: dict[str, float]) -> None:
    adicional = _resolver_adicional_conn(conn, descripcion)
    if not adicional or cantidad <= 0:
        return
    if adicional["tipo"] == "producto":
        _acumular_requerimiento_panaderia_producto(
            conn, adicional["nombre"], cantidad, requeridos
        )
        return
    _acumular_requerimiento_panaderia_adicional(
        conn, int(adicional["id"]), cantidad, requeridos
    )


def _requerimiento_panaderia_items_conn(conn, items: list[dict]) -> dict[str, float]:
    requeridos: dict[str, float] = {}

    for item in items:
        producto = str(item.get("producto", "") or "").strip()
        cantidad = float(item.get("cantidad", 0) or 0)
        if producto and cantidad > 0:
            _acumular_requerimiento_panaderia_producto(conn, producto, cantidad, requeridos)

        for mod in item.get("modificaciones", []):
            if mod.get("tipo") != "adicional":
                continue
            descripcion = str(mod.get("descripcion", "") or "").strip()
            cantidad_mod = float(mod.get("cantidad", 0) or 0)
            if not descripcion or cantidad_mod <= 0:
                continue
            _acumular_requerimiento_panaderia_modificacion(
                conn, descripcion, cantidad_mod, requeridos
            )

    return requeridos


def _pedidos_comprometidos_panaderia_conn(conn, fecha: str,
                                          excluir_pedido_id: int | None = None) -> dict[str, float]:
    query = """
        SELECT id
        FROM pedidos
        WHERE fecha = ? AND estado != 'cancelado'
    """
    params: list = [fecha]
    if excluir_pedido_id is not None:
        query += " AND id != ?"
        params.append(excluir_pedido_id)

    pedido_ids = [row["id"] for row in conn.execute(query, tuple(params)).fetchall()]
    comprometidos: dict[str, float] = {}

    for pedido_id in pedido_ids:
        items = conn.execute("""
            SELECT id, producto, cantidad
            FROM pedido_items
            WHERE pedido_id = ?
            ORDER BY id
        """, (pedido_id,)).fetchall()

        items_payload = []
        for item in items:
            mods = conn.execute("""
                SELECT tipo, descripcion, cantidad
                FROM pedido_item_modificaciones
                WHERE pedido_item_id = ?
                ORDER BY id
            """, (item["id"],)).fetchall()
            items_payload.append({
                "producto": item["producto"],
                "cantidad": item["cantidad"],
                "modificaciones": [dict(mod) for mod in mods],
            })

        requeridos = _requerimiento_panaderia_items_conn(conn, items_payload)
        for producto, cantidad in requeridos.items():
            comprometidos[producto] = comprometidos.get(producto, 0.0) + float(cantidad)

    return comprometidos


def validar_items_contra_produccion_panaderia(items: list[dict], fecha: str | None = None,
                                              excluir_pedido_id: int | None = None) -> dict:
    fecha = fecha or datetime.now().strftime("%Y-%m-%d")

    with get_connection() as conn:
        requeridos = _requerimiento_panaderia_items_conn(conn, items)
        if not requeridos:
            return {"ok": True, "faltantes": [], "requeridos": {}, "fecha": fecha}

        produccion_rows = conn.execute("""
            SELECT rd.producto, SUM(rd.producido) as producido
            FROM registros_diarios rd
            JOIN productos p ON p.nombre = rd.producto
            WHERE rd.fecha = ? AND p.categoria = 'Panaderia'
            GROUP BY rd.producto
        """, (fecha,)).fetchall()
        produccion = {row["producto"]: float(row["producido"] or 0) for row in produccion_rows}

        comprometidos = _pedidos_comprometidos_panaderia_conn(
            conn, fecha, excluir_pedido_id=excluir_pedido_id
        )

        faltantes = []
        for producto, requerido in requeridos.items():
            producido = float(produccion.get(producto, 0) or 0)
            comprometido = float(comprometidos.get(producto, 0) or 0)
            disponible = max(producido - comprometido, 0.0)
            if disponible + 1e-9 < requerido:
                faltantes.append({
                    "producto": producto,
                    "requerido": requerido,
                    "producido": producido,
                    "comprometido": comprometido,
                    "disponible": disponible,
                    "faltante": requerido - disponible,
                })

    if not faltantes:
        return {"ok": True, "faltantes": [], "requeridos": requeridos, "fecha": fecha}

    detalles = ", ".join(
        f"{f['producto']} (disponible: {int(round(f['disponible']))}, requerido: {int(round(f['requerido']))})"
        for f in faltantes
    )
    return {
        "ok": False,
        "faltantes": faltantes,
        "requeridos": requeridos,
        "fecha": fecha,
        "error": f"No hay produccion suficiente registrada hoy para: {detalles}. Registra primero la produccion del dia.",
    }


def obtener_stock_disponible_hoy(fecha: str | None = None) -> dict[str, int]:
    """
    Retorna el stock disponible REAL por producto para la fecha indicada.
    Usa la misma fuente que el inventario: registros_diarios.vendido (ingresado manualmente
    por el panadero), y descuenta además los pedidos activos aún no cobrados.

    Fórmula: producido - vendido_manual - comprometido_pedidos_activos
    - vendido_manual: registros_diarios.vendido (mismo valor que muestra el inventario)
    - comprometido_activos: pedidos en estado pendiente/en_preparacion/listo

    Solo incluye productos con registro de producción para hoy.
    Productos sin registro = sin límite (no se validan).
    """
    fecha = fecha or datetime.now().strftime("%Y-%m-%d")
    with get_connection() as conn:
        # 1. Producción y vendido manual del día (fuente única, igual al inventario)
        prod_rows = conn.execute(
            "SELECT producto, SUM(producido) as producido, SUM(vendido) as vendido "
            "FROM registros_diarios WHERE fecha = ? GROUP BY producto",
            (fecha,)
        ).fetchall()
        if not prod_rows:
            return {}

        # 2. Pedidos activos (aún no cobrados): pendiente, en_preparacion, listo
        pedido_ids = [
            row["id"] for row in conn.execute(
                "SELECT id FROM pedidos WHERE fecha = ? AND estado IN ('pendiente', 'en_preparacion', 'listo')",
                (fecha,)
            ).fetchall()
        ]
        comprometidos: dict[str, int] = {}
        for pid in pedido_ids:
            for item in conn.execute(
                "SELECT producto, cantidad FROM pedido_items WHERE pedido_id = ?", (pid,)
            ).fetchall():
                p = item["producto"]
                comprometidos[p] = comprometidos.get(p, 0) + int(item["cantidad"] or 0)

        # 3. Calcular disponible = producido - vendido_manual - comprometido_activos
        disponibles: dict[str, int] = {}
        for r in prod_rows:
            producto = r["producto"]
            producido = int(r["producido"] or 0)
            vendido = int(r["vendido"] or 0)
            comprometido = comprometidos.get(producto, 0)
            disponibles[producto] = max(producido - vendido - comprometido, 0)

    return disponibles


def validar_stock_pedido(items: list[dict], fecha: str | None = None,
                         excluir_pedido_id: int | None = None) -> dict:
    """
    Valida que los items del pedido no superen el stock disponible real.
    Aplica a TODOS los productos con producción registrada hoy, sin importar categoría.
    Si un producto no tiene registro de producción, se permite (sin límite).
    """
    fecha = fecha or datetime.now().strftime("%Y-%m-%d")

    # Calcular requeridos del pedido (suma por producto)
    requeridos: dict[str, float] = {}
    for item in items:
        producto = str(item.get("producto", "") or "").strip()
        cantidad = float(item.get("cantidad", 0) or 0)
        if producto and cantidad > 0:
            requeridos[producto] = requeridos.get(producto, 0.0) + cantidad

    if not requeridos:
        return {"ok": True, "faltantes": []}

    # Obtener stock disponible (incluye productos de todas las categorías)
    disponibles = obtener_stock_disponible_hoy(fecha)

    # Si excluimos un pedido (edición), devolver sus items al disponible
    if excluir_pedido_id is not None:
        with get_connection() as conn:
            for row in conn.execute(
                "SELECT producto, cantidad FROM pedido_items WHERE pedido_id = ?",
                (excluir_pedido_id,)
            ).fetchall():
                p = row["producto"]
                if p in disponibles:
                    disponibles[p] = disponibles[p] + int(row["cantidad"] or 0)

    faltantes = []
    for producto, requerido in requeridos.items():
        if producto not in disponibles:
            # Sin registro de producción hoy → no validamos
            continue
        disponible = disponibles[producto]
        if disponible < requerido:
            faltantes.append({
                "producto": producto,
                "requerido": int(requerido),
                "disponible": disponible,
                "faltante": int(requerido - disponible),
            })

    if not faltantes:
        return {"ok": True, "faltantes": []}

    detalles = ", ".join(
        f"{f['producto']} (disponible: {f['disponible']}, solicitado: {f['requerido']})"
        for f in faltantes
    )
    return {
        "ok": False,
        "faltantes": faltantes,
        "error": f"Stock insuficiente: {detalles}",
    }


def _acumular_consumo_producto(conn, producto: str, cantidad: float,
                               consumo: dict, ruta: tuple[str, ...] = (),
                               incluir_panaderia: bool = False) -> None:
    if producto in ruta:
        raise ValueError(f"Ciclo detectado en la composicion del producto: {' > '.join(ruta + (producto,))}")

    categoria = _categoria_producto_conn(conn, producto)
    if categoria == "Panaderia" and not incluir_panaderia:
        return

    receta = conn.execute("""
        SELECT r.insumo_id, i.nombre, i.unidad, r.cantidad, r.unidad_receta
        FROM recetas r
        JOIN insumos i ON r.insumo_id = i.id
        WHERE r.producto = ?
    """, (producto,)).fetchall()

    for r in receta:
        key = r["insumo_id"]
        if key not in consumo:
            consumo[key] = {"nombre": r["nombre"], "unidad": r["unidad"], "cantidad": 0.0}
        consumo_base = convertir_cantidad(r["cantidad"], r["unidad_receta"], r["unidad"])
        consumo[key]["cantidad"] += consumo_base * cantidad

    componentes = conn.execute("""
        SELECT componente_producto, cantidad
        FROM producto_componentes
        WHERE producto = ?
        ORDER BY componente_producto
    """, (producto,)).fetchall()

    for componente in componentes:
        _acumular_consumo_producto(
            conn,
            componente["componente_producto"],
            float(componente["cantidad"] or 0) * cantidad,
            consumo,
            ruta + (producto,),
            incluir_panaderia=incluir_panaderia,
        )


def _consumo_producto(conn, producto: str, cantidad: float,
                      incluir_panaderia: bool = False) -> dict:
    consumo: dict[int, dict] = {}
    _acumular_consumo_producto(
        conn, producto, cantidad, consumo, incluir_panaderia=incluir_panaderia
    )
    return consumo


def _acumular_consumo_adicional(conn, adicional_id: int, cantidad: float,
                                consumo: dict,
                                incluir_panaderia: bool = False) -> None:
    insumos = conn.execute("""
        SELECT ai.insumo_id, ai.cantidad, ai.unidad_config,
               i.nombre, i.unidad
        FROM adicional_insumos ai
        JOIN insumos i ON i.id = ai.insumo_id
        WHERE ai.adicional_id = ?
        ORDER BY i.nombre
    """, (adicional_id,)).fetchall()

    for row in insumos:
        key = row["insumo_id"]
        if key not in consumo:
            consumo[key] = {"nombre": row["nombre"], "unidad": row["unidad"], "cantidad": 0.0}
        cantidad_base = convertir_cantidad(
            row["cantidad"], row["unidad_config"] or row["unidad"], row["unidad"]
        )
        consumo[key]["cantidad"] += cantidad_base * cantidad

    componentes = conn.execute("""
        SELECT componente_producto, cantidad
        FROM adicional_componentes
        WHERE adicional_id = ?
        ORDER BY componente_producto
    """, (adicional_id,)).fetchall()

    for componente in componentes:
        _acumular_consumo_producto(
            conn,
            componente["componente_producto"],
            float(componente["cantidad"] or 0) * cantidad,
            consumo,
            incluir_panaderia=incluir_panaderia,
        )


def _acumular_consumo_modificacion(conn, descripcion: str, cantidad: float,
                                   consumo: dict,
                                   incluir_panaderia: bool = False) -> None:
    adicional = _resolver_adicional_conn(conn, descripcion)
    if not adicional or cantidad <= 0:
        return
    if adicional["tipo"] == "producto":
        _acumular_consumo_producto(
            conn,
            adicional["nombre"],
            cantidad,
            consumo,
            incluir_panaderia=incluir_panaderia,
        )
        return
    _acumular_consumo_adicional(
        conn,
        int(adicional["id"]),
        cantidad,
        consumo,
        incluir_panaderia=incluir_panaderia,
    )


def _consumo_adicional(conn, adicional_id: int, cantidad: float,
                       incluir_panaderia: bool = False) -> dict:
    consumo: dict[int, dict] = {}
    _acumular_consumo_adicional(
        conn, adicional_id, cantidad, consumo, incluir_panaderia=incluir_panaderia
    )
    return consumo

def obtener_receta(producto: str) -> dict:
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT r.id, r.insumo_id, i.nombre as insumo_nombre,
                   i.unidad as unidad_inventario,
                   r.unidad_receta, r.cantidad
            FROM recetas r
            JOIN insumos i ON r.insumo_id = i.id
            WHERE r.producto = ?
            ORDER BY i.nombre
        """, (producto,)).fetchall()
        ficha = conn.execute("""
            SELECT producto, rendimiento_texto, tiempo_preparacion_min,
                   tiempo_amasado_min, tiempo_fermentacion_min,
                   tiempo_horneado_min, temperatura_horneado,
                   pasos, observaciones
            FROM receta_fichas
            WHERE producto = ?
        """, (producto,)).fetchone()
        componentes = conn.execute("""
            SELECT pc.id, pc.componente_producto, pc.cantidad,
                   p.categoria as componente_categoria
            FROM producto_componentes pc
            LEFT JOIN productos p ON p.nombre = pc.componente_producto
            WHERE pc.producto = ?
            ORDER BY pc.componente_producto
        """, (producto,)).fetchall()

    return {
        "ingredientes": [dict(r) for r in rows],
        "componentes": [dict(r) for r in componentes],
        "ficha": dict(ficha) if ficha else _ficha_receta_vacia(producto),
    }


def guardar_receta(producto: str, ingredientes: list[dict], ficha: dict | None = None,
                   componentes: list[dict] | None = None) -> bool:
    """Reemplaza la composicion de un producto y actualiza su ficha tecnica."""
    ficha = ficha or {}
    componentes = componentes or []
    try:
        with get_connection() as conn:
            conn.execute("DELETE FROM recetas WHERE producto = ?", (producto,))
            conn.execute("DELETE FROM producto_componentes WHERE producto = ?", (producto,))
            for ing in ingredientes:
                conn.execute(
                    "INSERT INTO recetas (producto, insumo_id, cantidad, unidad_receta) VALUES (?, ?, ?, ?)",
                    (
                        producto,
                        int(ing["insumo_id"]),
                        float(ing["cantidad"]),
                        (ing.get("unidad_receta") or "unidad").strip(),
                    )
                )
            for componente in componentes:
                componente_producto = str(componente.get("componente_producto", "") or "").strip()
                if not componente_producto or componente_producto == producto:
                    continue
                conn.execute(
                    "INSERT INTO producto_componentes (producto, componente_producto, cantidad) VALUES (?, ?, ?)",
                    (
                        producto,
                        componente_producto,
                        float(componente.get("cantidad", 0) or 0),
                    )
                )
            # Valida que la composicion no cree ciclos entre productos.
            _consumo_producto(conn, producto, 1, incluir_panaderia=True)
            conn.execute("""
                INSERT INTO receta_fichas (
                    producto, rendimiento_texto, tiempo_preparacion_min,
                    tiempo_amasado_min, tiempo_fermentacion_min,
                    tiempo_horneado_min, temperatura_horneado,
                    pasos, observaciones
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(producto) DO UPDATE SET
                    rendimiento_texto = excluded.rendimiento_texto,
                    tiempo_preparacion_min = excluded.tiempo_preparacion_min,
                    tiempo_amasado_min = excluded.tiempo_amasado_min,
                    tiempo_fermentacion_min = excluded.tiempo_fermentacion_min,
                    tiempo_horneado_min = excluded.tiempo_horneado_min,
                    temperatura_horneado = excluded.temperatura_horneado,
                    pasos = excluded.pasos,
                    observaciones = excluded.observaciones
            """, (
                producto,
                str(ficha.get("rendimiento_texto", "") or "").strip(),
                float(ficha.get("tiempo_preparacion_min", 0) or 0),
                float(ficha.get("tiempo_amasado_min", 0) or 0),
                float(ficha.get("tiempo_fermentacion_min", 0) or 0),
                float(ficha.get("tiempo_horneado_min", 0) or 0),
                float(ficha.get("temperatura_horneado", 0) or 0),
                str(ficha.get("pasos", "") or "").strip(),
                str(ficha.get("observaciones", "") or "").strip(),
            ))
            conn.commit()
        return True
    except Exception as e:
        print(f"[ERROR] guardar_receta: {e}")
        return False


def obtener_consumo_diario(fecha: str = None) -> list[dict]:
    """Calcula el consumo teorico del dia combinando produccion y pedidos pagados."""
    if fecha is None:
        fecha = datetime.now().strftime("%Y-%m-%d")

    consumo = {}
    with get_connection() as conn:
        produccion = conn.execute("""
            SELECT producto, producido
            FROM registros_diarios
            WHERE fecha = ? AND producido > 0
        """, (fecha,)).fetchall()

        for lote in produccion:
            consumo_producto = _consumo_producto(
                conn, lote["producto"], lote["producido"], incluir_panaderia=True
            )
            for key, datos in consumo_producto.items():
                if key not in consumo:
                    consumo[key] = {"nombre": datos["nombre"], "unidad": datos["unidad"], "cantidad": 0}
                consumo[key]["cantidad"] += datos["cantidad"]

        # Obtener items de pedidos pagados del dia
        items = conn.execute("""
            SELECT pi.producto, pi.cantidad, pi.id as item_id
            FROM pedido_items pi
            JOIN pedidos p ON pi.pedido_id = p.id
            WHERE p.fecha = ? AND p.estado = 'pagado'
        """, (fecha,)).fetchall()

        for item in items:
            # Consumo por composicion base del producto
            consumo_producto = _consumo_producto(
                conn, item["producto"], item["cantidad"], incluir_panaderia=False
            )
            for key, datos in consumo_producto.items():
                if key not in consumo:
                    consumo[key] = {"nombre": datos["nombre"], "unidad": datos["unidad"], "cantidad": 0}
                consumo[key]["cantidad"] += datos["cantidad"]

            # Consumo por adicionales
            mods = conn.execute("""
                SELECT m.tipo, m.descripcion, m.cantidad
                FROM pedido_item_modificaciones m
                WHERE m.pedido_item_id = ? AND m.tipo = 'adicional'
            """, (item["item_id"],)).fetchall()

            for mod in mods:
                consumo_adicional = {}
                _acumular_consumo_modificacion(
                    conn,
                    mod["descripcion"],
                    float(mod["cantidad"] or 0),
                    consumo_adicional,
                    incluir_panaderia=False,
                )
                for key, datos in consumo_adicional.items():
                    if key not in consumo:
                        consumo[key] = {"nombre": datos["nombre"], "unidad": datos["unidad"], "cantidad": 0}
                    consumo[key]["cantidad"] += datos["cantidad"]

    return sorted(consumo.values(), key=lambda x: x["nombre"])


def obtener_estadisticas_pedidos(fecha: str = None) -> dict:
    """Estadisticas de pedidos del dia."""
    if fecha is None:
        fecha = datetime.now().strftime("%Y-%m-%d")

    with get_connection() as conn:
        row = conn.execute("""
            SELECT
                COUNT(*) as total_pedidos,
                COALESCE(SUM(CASE WHEN estado = 'pendiente' THEN 1 ELSE 0 END), 0) as pendientes,
                COALESCE(SUM(CASE WHEN estado = 'en_preparacion' THEN 1 ELSE 0 END), 0) as en_preparacion,
                COALESCE(SUM(CASE WHEN estado = 'listo' THEN 1 ELSE 0 END), 0) as listos,
                COALESCE(SUM(CASE WHEN estado = 'pagado' THEN 1 ELSE 0 END), 0) as pagados,
                COALESCE(SUM(CASE WHEN estado = 'cancelado' THEN 1 ELSE 0 END), 0) as cancelados,
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


# ──────────────────────────────────────────────
# Audit Log
# ──────────────────────────────────────────────

def registrar_audit(
    usuario: str,
    accion: str,
    entidad: str = "",
    entidad_id: str = "",
    detalle: str = "",
    valor_antes: str = "",
    valor_nuevo: str = "",
) -> None:
    """Registra una acción crítica en el audit log."""
    ahora = datetime.now()
    fecha = ahora.strftime("%Y-%m-%d")
    creado_en = ahora.strftime("%Y-%m-%d %H:%M:%S")
    try:
        with get_connection() as conn:
            conn.execute("""
                INSERT INTO audit_log
                    (fecha, creado_en, usuario, accion, entidad, entidad_id, detalle, valor_antes, valor_nuevo)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (fecha, creado_en, str(usuario or ""), str(accion or ""),
                  str(entidad or ""), str(entidad_id or ""),
                  str(detalle or ""), str(valor_antes or ""), str(valor_nuevo or "")))
            conn.commit()
    except Exception as e:
        print(f"[AUDIT ERROR] {e}")


def obtener_audit_log(dias: int = 30, limite: int = 200) -> list[dict]:
    """Devuelve entradas recientes del audit log."""
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT id, fecha, creado_en, usuario, accion, entidad, entidad_id,
                   detalle, valor_antes, valor_nuevo
            FROM audit_log
            WHERE fecha >= date('now', ?)
            ORDER BY creado_en DESC, id DESC
            LIMIT ?
        """, (f"-{dias} days", limite)).fetchall()
    return [dict(r) for r in rows]


# ──────────────────────────────────────────────
# Top Productos del Día
# ──────────────────────────────────────────────

def obtener_top_productos_dia(fecha: str | None = None, limite: int = 3) -> list[dict]:
    """Top N productos más vendidos hoy (unidades vendidas)."""
    fecha = fecha or datetime.now().strftime("%Y-%m-%d")
    with get_connection() as conn:
        # Ventas del cajero
        rows_ventas = conn.execute("""
            SELECT producto,
                   COALESCE(SUM(cantidad), 0) as unidades,
                   COALESCE(SUM(total), 0.0) as ingresos
            FROM ventas
            WHERE fecha = ?
            GROUP BY producto
        """, (fecha,)).fetchall()

        # Ventas via pedidos de mesa (estado pagado)
        rows_pedidos = conn.execute("""
            SELECT pi.producto,
                   COALESCE(SUM(pi.cantidad), 0) as unidades,
                   COALESCE(SUM(pi.subtotal), 0.0) as ingresos
            FROM pedido_items pi
            JOIN pedidos p ON p.id = pi.pedido_id
            WHERE p.fecha = ? AND p.estado = 'pagado'
            GROUP BY pi.producto
        """, (fecha,)).fetchall()

    # Combinar ambas fuentes
    combinado: dict[str, dict] = {}
    for r in list(rows_ventas) + list(rows_pedidos):
        nombre = r["producto"]
        if nombre not in combinado:
            combinado[nombre] = {"producto": nombre, "unidades": 0, "ingresos": 0.0}
        combinado[nombre]["unidades"] += int(r["unidades"] or 0)
        combinado[nombre]["ingresos"] += float(r["ingresos"] or 0)

    resultado = sorted(combinado.values(), key=lambda x: x["unidades"], reverse=True)
    return resultado[:limite]


# ──────────────────────────────────────────────
# Alertas de Stock por Producto
# ──────────────────────────────────────────────

def obtener_alertas_stock_productos(fecha: str | None = None) -> list[dict]:
    """
    Devuelve estado de stock de productos de panadería del día.
    Estado: 'verde' (ok), 'amarillo' (pocas unidades), 'rojo' (agotado).
    """
    fecha = fecha or datetime.now().strftime("%Y-%m-%d")
    with get_connection() as conn:
        productos = conn.execute("""
            SELECT id, nombre, stock_minimo
            FROM productos
            WHERE activo = 1 AND categoria = 'Panaderia'
        """).fetchall()

        registros = conn.execute("""
            SELECT producto, producido, vendido,
                   COALESCE(producido - vendido, 0) as disponible
            FROM registros_diarios
            WHERE fecha = ?
        """, (fecha,)).fetchall()

    reg_por_prod = {r["producto"]: dict(r) for r in registros}

    resultado = []
    for p in productos:
        nombre = p["nombre"]
        stock_minimo = int(p["stock_minimo"] or 0)
        reg = reg_por_prod.get(nombre)

        if reg is None:
            # No hay registro del día = sin datos
            estado = "sin_datos"
            disponible = None
        else:
            disponible = max(int(reg["disponible"] or 0), 0)
            if disponible <= 0:
                estado = "rojo"
            elif stock_minimo > 0 and disponible <= stock_minimo:
                estado = "amarillo"
            else:
                estado = "verde"

        resultado.append({
            "producto": nombre,
            "disponible": disponible,
            "stock_minimo": stock_minimo,
            "estado": estado,
        })

    return resultado


def actualizar_stock_minimo_producto(producto_id: int, stock_minimo: int) -> bool:
    """Actualiza el stock mínimo de alerta de un producto."""
    try:
        with get_connection() as conn:
            conn.execute(
                "UPDATE productos SET stock_minimo = ? WHERE id = ?",
                (max(0, int(stock_minimo)), producto_id)
            )
            conn.commit()
        return True
    except Exception:
        return False


# ──────────────────────────────────────────────
# Ajustes de Pronóstico (ajuste manual del panadero)
# ──────────────────────────────────────────────

def guardar_ajuste_pronostico(
    fecha: str,
    producto: str,
    sugerido: int,
    ajustado: int,
    motivo: str = "",
    registrado_por: str = "",
) -> bool:
    """Guarda el ajuste manual del panadero al pronóstico del sistema."""
    creado_en = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with get_connection() as conn:
            conn.execute("""
                INSERT INTO ajustes_pronostico
                    (fecha, creado_en, producto, sugerido, ajustado, motivo, registrado_por)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(fecha, producto) DO UPDATE SET
                    ajustado = excluded.ajustado,
                    motivo = excluded.motivo,
                    registrado_por = excluded.registrado_por,
                    creado_en = excluded.creado_en
            """, (fecha, creado_en, producto, sugerido, ajustado, motivo, registrado_por))
            conn.commit()
        return True
    except Exception as e:
        print(f"[ERROR] guardar_ajuste_pronostico: {e}")
        return False


def obtener_ajuste_pronostico(fecha: str, producto: str) -> dict | None:
    """Devuelve el ajuste manual del panadero para un producto y fecha, si existe."""
    with get_connection() as conn:
        row = conn.execute("""
            SELECT fecha, producto, sugerido, ajustado, motivo, registrado_por, creado_en
            FROM ajustes_pronostico
            WHERE fecha = ? AND producto = ?
        """, (fecha, producto)).fetchone()
    return dict(row) if row else None


def obtener_historial_ajustes(producto: str, dias: int = 30) -> list[dict]:
    """Historial de ajustes manuales de un producto."""
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT fecha, producto, sugerido, ajustado, motivo, registrado_por, creado_en
            FROM ajustes_pronostico
            WHERE producto = ? AND fecha >= date('now', ?)
            ORDER BY fecha DESC
        """, (producto, f"-{dias} days")).fetchall()
    return [dict(r) for r in rows]


# ──────────────────────────────────────────────
# Merma / Desperdicio
# ──────────────────────────────────────────────

def registrar_merma(
    producto: str,
    cantidad: float,
    tipo: str = "sobrante",
    registrado_por: str = "",
    notas: str = "",
    fecha: str | None = None,
) -> bool:
    """Registra una merma/desperdicio de un producto."""
    ahora = datetime.now()
    fecha = fecha or ahora.strftime("%Y-%m-%d")
    creado_en = ahora.strftime("%Y-%m-%d %H:%M:%S")
    tipos_validos = {"sobrante", "vencido", "danado", "consumo_interno", "cortesia", "otro"}
    tipo = tipo if tipo in tipos_validos else "otro"
    try:
        with get_connection() as conn:
            conn.execute("""
                INSERT INTO mermas (fecha, creado_en, producto, cantidad, tipo, registrado_por, notas)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (fecha, creado_en, producto, float(cantidad), tipo, registrado_por, notas))
            conn.commit()
        return True
    except Exception as e:
        print(f"[ERROR] registrar_merma: {e}")
        return False


def obtener_mermas_dia(fecha: str | None = None) -> list[dict]:
    fecha = fecha or datetime.now().strftime("%Y-%m-%d")
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT id, fecha, creado_en, producto, cantidad, tipo, registrado_por, notas
            FROM mermas WHERE fecha = ?
            ORDER BY creado_en DESC
        """, (fecha,)).fetchall()
    return [dict(r) for r in rows]


def obtener_resumen_mermas(dias: int = 30) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT producto, tipo,
                   COALESCE(SUM(cantidad), 0) as total_unidades,
                   COUNT(*) as registros
            FROM mermas
            WHERE fecha >= date('now', ?)
            GROUP BY producto, tipo
            ORDER BY total_unidades DESC
        """, (f"-{dias} days",)).fetchall()
    return [dict(r) for r in rows]


# ──────────────────────────────────────────────
# Días Especiales / Festivos
# ──────────────────────────────────────────────

def obtener_dias_especiales(fecha_inicio: str | None = None, fecha_fin: str | None = None) -> list[dict]:
    """Devuelve días especiales en un rango de fechas."""
    with get_connection() as conn:
        if fecha_inicio and fecha_fin:
            rows = conn.execute("""
                SELECT id, fecha, descripcion, factor, tipo, activo
                FROM dias_especiales
                WHERE activo = 1 AND fecha BETWEEN ? AND ?
                ORDER BY fecha ASC
            """, (fecha_inicio, fecha_fin)).fetchall()
        else:
            rows = conn.execute("""
                SELECT id, fecha, descripcion, factor, tipo, activo
                FROM dias_especiales
                WHERE activo = 1
                ORDER BY fecha ASC
            """).fetchall()
    return [dict(r) for r in rows]


def obtener_factor_dia_especial(fecha: str) -> float:
    """Devuelve el factor multiplicador para una fecha especial (1.0 si no es especial)."""
    with get_connection() as conn:
        row = conn.execute("""
            SELECT factor FROM dias_especiales WHERE fecha = ? AND activo = 1
        """, (fecha,)).fetchone()
    return float(row["factor"]) if row else 1.0


def guardar_dia_especial(
    fecha: str,
    descripcion: str,
    factor: float = 1.0,
    tipo: str = "festivo",
) -> bool:
    try:
        with get_connection() as conn:
            conn.execute("""
                INSERT INTO dias_especiales (fecha, descripcion, factor, tipo)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(fecha) DO UPDATE SET
                    descripcion = excluded.descripcion,
                    factor = excluded.factor,
                    tipo = excluded.tipo,
                    activo = 1
            """, (fecha, descripcion, round(float(factor), 2), tipo))
            conn.commit()
        return True
    except Exception as e:
        print(f"[ERROR] guardar_dia_especial: {e}")
        return False


# ──────────────────────────────────────────────
# Dashboard de Cierre Diario
# ──────────────────────────────────────────────

def obtener_resumen_cierre_diario(fecha: str | None = None) -> dict:
    """
    Genera el resumen completo del cierre del día:
    ventas, ticket promedio, top producto, caja, merma, pronóstico mañana.
    """
    fecha = fecha or datetime.now().strftime("%Y-%m-%d")

    with get_connection() as conn:
        # ── Ventas del día ───────────────────────────────────────────────────
        ventas_row = conn.execute("""
            SELECT
                COUNT(DISTINCT COALESCE(NULLIF(venta_grupo, ''), CAST(id AS TEXT))) as transacciones,
                COALESCE(SUM(total), 0.0) as total_ventas,
                COALESCE(SUM(cantidad), 0) as unidades_vendidas
            FROM ventas WHERE fecha = ?
        """, (fecha,)).fetchone()

        # Ventas de pedidos de mesa (pagados)
        pedidos_row = conn.execute("""
            SELECT
                COUNT(*) as pedidos_pagados,
                COALESCE(SUM(total), 0.0) as total_pedidos
            FROM pedidos WHERE fecha = ? AND estado = 'pagado'
        """, (fecha,)).fetchone()

        # ── Caja ─────────────────────────────────────────────────────────────
        caja_row = conn.execute("""
            SELECT monto_apertura, monto_cierre, efectivo_esperado,
                   diferencia_cierre, estado, cerrado_por, cerrado_en,
                   abierto_por, abierto_en
            FROM arqueos_caja WHERE fecha = ?
            ORDER BY CASE estado WHEN 'cerrado' THEN 0 ELSE 1 END,
                     abierto_en DESC
            LIMIT 1
        """, (fecha,)).fetchone()

        # ── Top producto del día ──────────────────────────────────────────────
        top_row = conn.execute("""
            SELECT producto, COALESCE(SUM(cantidad), 0) as unidades
            FROM ventas WHERE fecha = ?
            GROUP BY producto ORDER BY unidades DESC LIMIT 1
        """, (fecha,)).fetchone()

        # ── Producto sin rotación ─────────────────────────────────────────────
        sin_rotacion = conn.execute("""
            SELECT rd.producto
            FROM registros_diarios rd
            WHERE rd.fecha = ? AND COALESCE(rd.vendido, 0) = 0
              AND COALESCE(rd.producido, 0) > 0
        """, (fecha,)).fetchall()

        # ── Merma del día ─────────────────────────────────────────────────────
        merma_row = conn.execute("""
            SELECT COALESCE(SUM(cantidad), 0) as total_merma
            FROM mermas WHERE fecha = ?
        """, (fecha,)).fetchone()

        # ── Producción del día ────────────────────────────────────────────────
        prod_row = conn.execute("""
            SELECT COALESCE(SUM(producido), 0) as total_producido,
                   COALESCE(SUM(vendido), 0) as total_vendido,
                   COALESCE(SUM(CASE WHEN producido > vendido THEN producido - vendido ELSE 0 END), 0) as sobrante
            FROM registros_diarios WHERE fecha = ?
        """, (fecha,)).fetchone()

    total_ventas = float((ventas_row["total_ventas"] or 0)) + float((pedidos_row["total_pedidos"] or 0))
    transacciones = int(ventas_row["transacciones"] or 0) + int(pedidos_row["pedidos_pagados"] or 0)
    ticket_promedio = round(total_ventas / transacciones, 2) if transacciones > 0 else 0.0

    return {
        "fecha": fecha,
        "total_ventas": round(total_ventas, 2),
        "transacciones": transacciones,
        "ticket_promedio": ticket_promedio,
        "top_producto": dict(top_row) if top_row else None,
        "productos_sin_rotacion": [r["producto"] for r in sin_rotacion],
        "caja": dict(caja_row) if caja_row else None,
        "total_merma": float(merma_row["total_merma"] or 0) if merma_row else 0.0,
        "produccion": {
            "total_producido": int(prod_row["total_producido"] or 0),
            "total_vendido": int(prod_row["total_vendido"] or 0),
            "sobrante": int(prod_row["sobrante"] or 0),
        } if prod_row else {},
    }


# ──────────────────────────────────────────────
# Exportación CSV
# ──────────────────────────────────────────────

def exportar_ventas_csv(dias: int = 30) -> list[dict]:
    """Retorna ventas del período listas para exportar a CSV."""
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT fecha, hora, producto, cantidad, precio_unitario, total,
                   COALESCE(metodo_pago, 'efectivo') as metodo_pago,
                   registrado_por
            FROM ventas
            WHERE fecha >= date('now', ?)
            ORDER BY fecha DESC, hora DESC
        """, (f"-{dias} days",)).fetchall()
    return [dict(r) for r in rows]


def exportar_inventario_csv() -> list[dict]:
    """Retorna inventario de insumos listo para exportar a CSV."""
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT nombre, unidad, stock, stock_minimo, activo
            FROM insumos ORDER BY nombre ASC
        """).fetchall()
    return [dict(r) for r in rows]
