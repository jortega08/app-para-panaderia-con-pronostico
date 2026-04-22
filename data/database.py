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
  - comandas: snapshots operativos imprimibles por pedido
  - comanda_items: lineas historicas impresas en cada comanda
  - documentos_emitidos: documentos comerciales/imprimibles historicos por venta/pedido/encargo
  - documento_envios: trazabilidad de envios por correo del documento
  - insumos: catalogo de ingredientes con stock
  - recetas: composicion producto → insumos
  - adicional_insumos: insumos consumidos por cada adicional
  - adicional_componentes: productos base consumidos por cada adicional
"""

import hashlib
import json
import logging
import math
import random
import sqlite3
import os
import re
import threading
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4
from werkzeug.security import check_password_hash, generate_password_hash

logger = logging.getLogger(__name__)

from data.db_adapter import get_connection as _get_connection, DB_TYPE
from app.security import PLATFORM_ADMIN_ROLE, TENANT_ADMIN_ROLE, VALID_ROLES, normalize_role

DB_PATH = Path(__file__).parent / "panaderia.db"
DEFAULT_TENANT_SLUG = "principal"
DEFAULT_TENANT_NAME = "Panaderia Principal"
DEFAULT_BRANCH_SLUG = "principal"
DEFAULT_BRANCH_NAME = "Sede Principal"
SURTIDO_TIPOS_VALIDOS = {"none", "sal", "dulce", "ambos"}

# Límites por plan de suscripción
PLAN_LIMITS: dict[str, dict[str, int]] = {
    "free":       {"max_sedes": 1,   "max_usuarios": 5,   "max_productos": 50},
    "starter":    {"max_sedes": 1,   "max_usuarios": 10,  "max_productos": 100},
    "pro":        {"max_sedes": 3,   "max_usuarios": 20,  "max_productos": 500},
    "enterprise": {"max_sedes": 999, "max_usuarios": 999, "max_productos": 999},
}

# ── Contexto de tenant/sede por hilo ──────────────────────────────────────────
# app.py llama set_query_context() en before_request con los IDs resueltos.
# Las funciones de DB llaman _tenant_scope() para obtener (panaderia_id, sede_id).
_query_ctx = threading.local()


def set_query_context(panaderia_id, sede_id) -> None:
    """Fija el tenant y sede activos para el hilo actual (llamar desde before_request)."""
    _query_ctx.panaderia_id = panaderia_id
    _query_ctx.sede_id = sede_id


def _tenant_scope() -> tuple:
    """Retorna (panaderia_id, sede_id) del contexto actual, o (None, None) si no está fijado."""
    return (
        getattr(_query_ctx, "panaderia_id", None),
        getattr(_query_ctx, "sede_id", None),
    )


def _apply_tenant_scope(filtros: list, params: list, include_sede: bool = True) -> None:
    """Agrega filtros de panaderia_id (y sede_id si include_sede=True) in-place."""
    panaderia_id, sede_id = _tenant_scope()
    if panaderia_id is not None:
        filtros.append("panaderia_id = ?")
        params.append(panaderia_id)
    if include_sede and sede_id is not None:
        filtros.append("sede_id = ?")
        params.append(sede_id)


def _normalizar_surtido_tipo(valor: str | None) -> str:
    surtido_tipo = str(valor or "").strip().lower()
    return surtido_tipo if surtido_tipo in SURTIDO_TIPOS_VALIDOS else "none"


def _tenant_insert_fields(include_sede: bool = True) -> tuple[str, tuple]:
    """Retorna (columnas, valores) para incluir en un INSERT con scope de tenant."""
    panaderia_id, sede_id = _tenant_scope()
    if include_sede:
        return "panaderia_id, sede_id", (panaderia_id, sede_id)
    return "panaderia_id", (panaderia_id,)

try:
    import psycopg2  # type: ignore
    _INTEGRITY_ERRORS = (sqlite3.IntegrityError, psycopg2.IntegrityError)
except Exception:
    _INTEGRITY_ERRORS = (sqlite3.IntegrityError,)


def _hash_pin(pin: str) -> str:
    return hashlib.sha256(str(pin).strip().encode('utf-8')).hexdigest()


def _hash_password(password: str) -> str:
    return generate_password_hash(str(password or "").strip())


def _pin_ya_esta_hasheado(pin: str) -> bool:
    pin_normalizado = str(pin or "").strip()
    return len(pin_normalizado) == 64 and all(c in "0123456789abcdefABCDEF" for c in pin_normalizado)


def _pin_lookup_digest(pin: str) -> str:
    """Digest determinístico para buscar PIN sin exponerlo ni hacerlo reversible."""
    return _hash_pin(pin)


def _pin_lookup_digest_desde_fila(pin_hash: str = "", pin: str = "") -> str:
    pin_hash_normalizado = str(pin_hash or "").strip()
    if pin_hash_normalizado:
        return pin_hash_normalizado
    pin_normalizado = str(pin or "").strip()
    if not pin_normalizado:
        return ""
    return pin_normalizado if _pin_ya_esta_hasheado(pin_normalizado) else _pin_lookup_digest(pin_normalizado)


def _texto_linea_canonico(value: str = "") -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _row_to_dict(row) -> dict:
    if row is None:
        return {}
    if isinstance(row, dict):
        return row
    try:
        return dict(row)
    except Exception:
        pass
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        try:
            return dict(mapping)
        except Exception:
            pass
    keys = getattr(row, "keys", None)
    if callable(keys):
        try:
            return {key: row[key] for key in keys()}
        except Exception:
            pass
    return {}


def _sanitize_pagination(page: int | None, size: int | None, default_size: int = 50, max_size: int = 100) -> tuple[int, int, int]:
    try:
        page_num = int(page or 1)
    except (TypeError, ValueError):
        page_num = 1
    try:
        size_num = int(size or default_size)
    except (TypeError, ValueError):
        size_num = default_size
    page_num = max(page_num, 1)
    size_num = max(1, min(size_num, max_size))
    return page_num, size_num, (page_num - 1) * size_num


def _build_pagination_meta(total_items: int, page: int, size: int, items_count: int) -> dict:
    total_num = max(int(total_items or 0), 0)
    page_num = max(int(page or 1), 1)
    size_num = max(int(size or 1), 1)
    total_pages = max(1, math.ceil(total_num / size_num)) if total_num else 1
    return {
        "page": page_num,
        "size": size_num,
        "offset": max((page_num - 1) * size_num, 0),
        "items_count": max(int(items_count or 0), 0),
        "total_items": total_num,
        "total_pages": total_pages,
        "has_prev": page_num > 1,
        "has_next": page_num < total_pages,
        "prev_page": page_num - 1 if page_num > 1 else None,
        "next_page": page_num + 1 if page_num < total_pages else None,
    }


def _normalizar_modificaciones_linea(modificaciones: list[dict] | None) -> list[dict]:
    adicionales: dict[tuple[str, float], dict] = {}
    exclusiones: dict[str, dict] = {}

    for mod in modificaciones or []:
        tipo = str(mod.get("tipo", "") or "").strip().lower()
        descripcion = _texto_linea_canonico(mod.get("descripcion", ""))
        if not descripcion or tipo not in {"adicional", "exclusion"}:
            continue
        if tipo == "adicional":
            try:
                cantidad = int(mod.get("cantidad", 1) or 0)
            except (TypeError, ValueError):
                cantidad = 0
            if cantidad <= 0:
                continue
            precio_extra = round(float(mod.get("precio_extra", 0) or 0), 2)
            key = (descripcion, precio_extra)
            bucket = adicionales.setdefault(
                key,
                {
                    "tipo": "adicional",
                    "descripcion": descripcion,
                    "cantidad": 0,
                    "precio_extra": precio_extra,
                },
            )
            bucket["cantidad"] += cantidad
            continue
        exclusiones.setdefault(
            descripcion.lower(),
            {
                "tipo": "exclusion",
                "descripcion": descripcion,
                "cantidad": 1,
                "precio_extra": 0.0,
            },
        )

    mods = list(adicionales.values()) + list(exclusiones.values())
    mods.sort(key=lambda item: (item["tipo"], item["descripcion"].lower(), float(item.get("precio_extra", 0) or 0)))
    return mods


def _modificaciones_json_para_snapshot(modificaciones: list[dict] | None) -> str:
    return json.dumps(
        _normalizar_modificaciones_linea(modificaciones),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def _modificaciones_desde_snapshot(raw_value: str | None) -> list[dict]:
    texto = str(raw_value or "").strip()
    if not texto:
        return []
    try:
        data = json.loads(texto)
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    return _normalizar_modificaciones_linea(data)


def _linea_operativa_key(item: dict, campos_precio: tuple[str, ...]) -> tuple:
    mods = _normalizar_modificaciones_linea(item.get("modificaciones"))
    mods_json = json.dumps(mods, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    precios = tuple(round(float(item.get(field, 0) or 0), 2) for field in campos_precio)
    return (
        int(item.get("producto_id", 0) or 0),
        _texto_linea_canonico(item.get("producto", "")).lower(),
        _texto_linea_canonico(item.get("notas", "")),
        _texto_linea_canonico(item.get("motivo_precio", "")),
        _texto_linea_canonico(item.get("autorizado_por", "")),
        _texto_linea_canonico(item.get("descuento_manual", "")),
        precios,
        mods_json,
    )


def normalizar_items_pedido(items: list[dict] | None) -> list[dict]:
    agrupados: dict[tuple, dict] = {}
    for raw in items or []:
        producto = _texto_linea_canonico(raw.get("producto", ""))
        if not producto:
            continue
        try:
            cantidad = int(raw.get("cantidad", 0) or 0)
        except (TypeError, ValueError):
            continue
        if cantidad <= 0:
            continue
        precio_unitario = round(float(raw.get("precio_unitario", raw.get("precio", 0)) or 0), 2)
        linea = {
            "producto_id": int(raw.get("producto_id", 0) or 0) or None,
            "producto": producto,
            "cantidad": cantidad,
            "precio_unitario": precio_unitario,
            "notas": _texto_linea_canonico(raw.get("notas", "")),
            "modificaciones": _normalizar_modificaciones_linea(raw.get("modificaciones")),
        }
        key = _linea_operativa_key(linea, ("precio_unitario",))
        if key in agrupados:
            agrupados[key]["cantidad"] += cantidad
        else:
            agrupados[key] = linea
    return list(agrupados.values())


def normalizar_items_venta(items: list[dict] | None) -> list[dict]:
    agrupados: dict[tuple, dict] = {}
    for raw in items or []:
        producto = _texto_linea_canonico(raw.get("producto", ""))
        if not producto:
            continue
        try:
            cantidad = int(raw.get("cantidad", 0) or 0)
        except (TypeError, ValueError):
            continue
        if cantidad <= 0:
            continue
        precio_base = round(float(raw.get("precio_base", raw.get("precio", 0)) or 0), 2)
        precio_aplicado = round(float(raw.get("precio_aplicado", precio_base) or precio_base), 2)
        linea = {
            "producto_id": int(raw.get("producto_id", 0) or 0) or None,
            "producto": producto,
            "cantidad": cantidad,
            "precio_base": precio_base,
            "precio_aplicado": precio_aplicado,
            "motivo_precio": _texto_linea_canonico(raw.get("motivo_precio", "")),
            "autorizado_por": _texto_linea_canonico(raw.get("autorizado_por", "")),
            "notas": _texto_linea_canonico(raw.get("notas", "")),
            "modificaciones": _normalizar_modificaciones_linea(raw.get("modificaciones")),
            "descuento_manual": _texto_linea_canonico(raw.get("descuento_manual", "")),
        }
        key = _linea_operativa_key(linea, ("precio_base", "precio_aplicado"))
        if key in agrupados:
            agrupados[key]["cantidad"] += cantidad
        else:
            agrupados[key] = linea
    return list(agrupados.values())


def _linea_adicional_unit_total(modificaciones: list[dict] | None) -> float:
    return round(sum(
        float(mod.get("cantidad", 1) or 0) * float(mod.get("precio_extra", 0) or 0)
        for mod in (modificaciones or [])
        if str(mod.get("tipo", "") or "").strip().lower() == "adicional"
    ), 2)


def _linea_subtotal(cantidad: int | float, precio_unitario: float, modificaciones: list[dict] | None = None) -> float:
    cantidad_num = max(float(cantidad or 0), 0.0)
    precio_num = round(float(precio_unitario or 0), 2)
    extras_unitarios = _linea_adicional_unit_total(modificaciones)
    return round((precio_num + extras_unitarios) * cantidad_num, 2)


def _normalizar_pines_usuarios(conn) -> None:
    schema = _schema_tabla_conn(conn, "usuarios").lower()
    has_pin_lookup_digest = "pin_lookup_digest" in schema if schema else True
    if has_pin_lookup_digest:
        rows = conn.execute("SELECT id, pin, pin_hash, pin_lookup_digest FROM usuarios").fetchall()
    else:
        rows = conn.execute("SELECT id, pin, pin_hash FROM usuarios").fetchall()
    for row in rows:
        pin_actual = str(row["pin"] or "").strip()
        pin_hash_actual = str(row["pin_hash"] or "").strip()
        pin_lookup_actual = str(row["pin_lookup_digest"] or "").strip() if has_pin_lookup_digest else ""
        if pin_actual and not _pin_ya_esta_hasheado(pin_actual):
            pin_hash_nuevo = _hash_pin(pin_actual)
            pin_lookup_nuevo = _pin_lookup_digest(pin_actual)
            if has_pin_lookup_digest:
                conn.execute(
                    "UPDATE usuarios SET pin = ?, pin_hash = ?, pin_lookup_digest = ? WHERE id = ?",
                    (pin_hash_nuevo, pin_hash_nuevo, pin_lookup_nuevo, row["id"])
                )
            else:
                conn.execute(
                    "UPDATE usuarios SET pin = ?, pin_hash = ? WHERE id = ?",
                    (pin_hash_nuevo, pin_hash_nuevo, row["id"])
                )
            continue
        if pin_actual and (not pin_hash_actual or not pin_lookup_actual):
            if has_pin_lookup_digest:
                conn.execute(
                    "UPDATE usuarios SET pin_hash = ?, pin_lookup_digest = ? WHERE id = ?",
                    (
                        pin_hash_actual or pin_actual,
                        pin_lookup_actual or _pin_lookup_digest_desde_fila(pin_hash_actual or pin_actual, pin_actual),
                        row["id"],
                    )
                )
            else:
                conn.execute(
                    "UPDATE usuarios SET pin_hash = ? WHERE id = ?",
                    (pin_hash_actual or pin_actual, row["id"])
                )


def _restablecer_transaccion_si_necesario(conn) -> None:
    if DB_TYPE != "postgresql":
        return
    try:
        conn.rollback()
    except Exception:
        pass


def _ejecutar_migracion_tolerante(conn, sql: str, params=()) -> bool:
    savepoint = f"sp_migracion_{uuid4().hex}"
    try:
        conn.execute(f"SAVEPOINT {savepoint}")
        conn.execute(sql, params)
        conn.execute(f"RELEASE SAVEPOINT {savepoint}")
        return True
    except Exception:
        try:
            conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        except Exception:
            # Solo como último recurso si el savepoint falla completamente
            _restablecer_transaccion_si_necesario(conn)
        return False


def _combinar_observaciones(base: str = "", extra: str = "") -> str:
    base_texto = str(base or "").strip()
    extra_texto = str(extra or "").strip()
    if not base_texto:
        return extra_texto
    if not extra_texto or extra_texto in base_texto:
        return base_texto
    return f"{base_texto} | {extra_texto}"


def _schema_tabla_conn(conn, nombre_tabla: str) -> str:
    if DB_TYPE != "sqlite":
        return ""
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
        (nombre_tabla,)
    ).fetchone()
    return str(row["sql"] or "") if row else ""


def _columnas_tabla_conn(conn, nombre_tabla: str) -> set[str]:
    if DB_TYPE == "sqlite":
        try:
            return {
                str(row[1])
                for row in conn.execute(f"PRAGMA table_info({nombre_tabla})").fetchall()
                if len(row) > 1
            }
        except Exception:
            return set()
    try:
        cursor = conn.execute(f"SELECT * FROM {nombre_tabla} LIMIT 0")
    except Exception:
        return set()
    return {str(desc[0]) for desc in (cursor.description or []) if desc and desc[0]}


def _tabla_existe_conn(conn, nombre_tabla: str) -> bool:
    return bool(_columnas_tabla_conn(conn, nombre_tabla))


def _quote_ident_pg(identifier: str) -> str:
    return '"' + str(identifier or "").replace('"', '""') + '"'


def _constraints_unicas_pg_conn(conn, nombre_tabla: str) -> dict[str, tuple[str, ...]]:
    if DB_TYPE != "postgresql":
        return {}
    rows = conn.execute(
        """
        SELECT
            con.conname AS constraint_name,
            string_agg(att.attname, ',' ORDER BY key.ordinality) AS columns
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS key(attnum, ordinality) ON TRUE
        JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = key.attnum
        WHERE con.contype = 'u'
          AND rel.relname = ?
        GROUP BY con.conname
        ORDER BY con.conname
        """,
        (nombre_tabla,),
    ).fetchall()
    constraints: dict[str, tuple[str, ...]] = {}
    for row in rows:
        constraint_name = str(row.get("constraint_name", "") or "").strip()
        columns = tuple(
            col.strip()
            for col in str(row.get("columns", "") or "").split(",")
            if col.strip()
        )
        if constraint_name and columns:
            constraints[constraint_name] = columns
    return constraints


def _constraint_pg_existe_conn(conn, nombre_tabla: str, constraint_name: str) -> bool:
    if DB_TYPE != "postgresql":
        return False
    row = conn.execute(
        """
        SELECT 1 AS existe
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        WHERE rel.relname = ?
          AND con.conname = ?
        LIMIT 1
        """,
        (nombre_tabla, constraint_name),
    ).fetchone()
    return bool(row)


def _reemplazar_unique_legacy_pg_conn(
    conn,
    nombre_tabla: str,
    columnas_legacy: tuple[str, ...],
    nueva_constraint: str,
    nuevas_columnas: tuple[str, ...],
) -> None:
    if DB_TYPE != "postgresql" or not _tabla_existe_conn(conn, nombre_tabla):
        return

    legacy = tuple(columnas_legacy)
    target = tuple(nuevas_columnas)
    table_sql = _quote_ident_pg(nombre_tabla)

    for constraint_name, columns in _constraints_unicas_pg_conn(conn, nombre_tabla).items():
        if columns == legacy:
            _ejecutar_migracion_tolerante(
                conn,
                f"ALTER TABLE {table_sql} DROP CONSTRAINT IF EXISTS {_quote_ident_pg(constraint_name)}",
            )

    if target in _constraints_unicas_pg_conn(conn, nombre_tabla).values():
        return

    columns_sql = ", ".join(_quote_ident_pg(col) for col in target)
    if not _ejecutar_migracion_tolerante(
        conn,
        f"ALTER TABLE {table_sql} ADD CONSTRAINT {_quote_ident_pg(nueva_constraint)} UNIQUE ({columns_sql})",
    ):
        logger.warning(
            "No se pudo asegurar UNIQUE %s(%s).",
            nombre_tabla,
            ", ".join(target),
        )
        return
    logger.info(
        "migracion: UNIQUE %s(%s)",
        nombre_tabla,
        ", ".join(target),
    )


def _asegurar_fk_panaderia_pg_conn(conn, nombre_tabla: str, constraint_name: str) -> None:
    if DB_TYPE != "postgresql" or not _tabla_existe_conn(conn, nombre_tabla):
        return
    if "panaderia_id" not in _columnas_tabla_conn(conn, nombre_tabla):
        return
    if _constraint_pg_existe_conn(conn, nombre_tabla, constraint_name):
        return

    table_sql = _quote_ident_pg(nombre_tabla)
    panaderias_sql = _quote_ident_pg("panaderias")
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS total
        FROM {table_sql} t
        LEFT JOIN {panaderias_sql} p ON p.id = t.panaderia_id
        WHERE t.panaderia_id IS NOT NULL AND p.id IS NULL
        """
    ).fetchone()
    total_huerfanas = int(row["total"] or 0) if row else 0
    if total_huerfanas:
        logger.warning(
            "Se omite FK %s.%s: hay %s filas con panaderia_id sin correspondencia en panaderias.",
            nombre_tabla,
            constraint_name,
            total_huerfanas,
        )
        return

    if not _ejecutar_migracion_tolerante(
        conn,
        f"ALTER TABLE {table_sql} "
        f"ADD CONSTRAINT {_quote_ident_pg(constraint_name)} "
        f"FOREIGN KEY ({_quote_ident_pg('panaderia_id')}) "
        f"REFERENCES {panaderias_sql} ({_quote_ident_pg('id')})",
    ):
        logger.warning("No se pudo asegurar FK panaderia_id en %s.", nombre_tabla)


def _reparar_foreign_keys_usuarios_legacy_postgres(conn) -> list[str]:
    """Reapunta FKs que queden enlazadas a usuarios_legacy despues de renombrar usuarios."""
    if DB_TYPE != "postgresql":
        return []
    if not _tabla_existe_conn(conn, "usuarios") or not _tabla_existe_conn(conn, "usuarios_legacy"):
        return []

    rows = conn.execute(
        """
        SELECT
            con.conname AS constraint_name,
            conrel.relname AS source_table,
            string_agg(src_att.attname, ',' ORDER BY src_key.ordinality) AS source_columns,
            string_agg(tgt_att.attname, ',' ORDER BY src_key.ordinality) AS target_columns,
            con.confdeltype AS delete_action
        FROM pg_constraint con
        JOIN pg_class conrel ON conrel.oid = con.conrelid
        JOIN pg_class confrel ON confrel.oid = con.confrelid
        JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS src_key(attnum, ordinality) ON TRUE
        JOIN LATERAL unnest(con.confkey) WITH ORDINALITY AS tgt_key(attnum, ordinality)
          ON tgt_key.ordinality = src_key.ordinality
        JOIN pg_attribute src_att ON src_att.attrelid = con.conrelid AND src_att.attnum = src_key.attnum
        JOIN pg_attribute tgt_att ON tgt_att.attrelid = con.confrelid AND tgt_att.attnum = tgt_key.attnum
        WHERE con.contype = 'f'
          AND confrel.relname = 'usuarios_legacy'
        GROUP BY con.conname, conrel.relname, con.confdeltype
        ORDER BY conrel.relname, con.conname
        """
    ).fetchall()

    delete_actions = {
        "a": "",
        "r": " ON DELETE RESTRICT",
        "c": " ON DELETE CASCADE",
        "n": " ON DELETE SET NULL",
        "d": " ON DELETE SET DEFAULT",
    }
    reparadas: list[str] = []

    for row in rows:
        source_table = str(row.get("source_table", "") or "").strip()
        constraint_name = str(row.get("constraint_name", "") or "").strip()
        source_columns = [col.strip() for col in str(row.get("source_columns", "") or "").split(",") if col.strip()]
        target_columns = [col.strip() for col in str(row.get("target_columns", "") or "").split(",") if col.strip()]
        if not source_table or not constraint_name or not source_columns or not target_columns:
            continue

        table_sql = _quote_ident_pg(source_table)
        constraint_sql = _quote_ident_pg(constraint_name)
        source_cols_sql = ", ".join(_quote_ident_pg(col) for col in source_columns)
        target_cols_sql = ", ".join(_quote_ident_pg(col) for col in target_columns)
        delete_clause = delete_actions.get(str(row.get("delete_action", "") or "").strip(), "")

        conn.execute(f"ALTER TABLE {table_sql} DROP CONSTRAINT IF EXISTS {constraint_sql}")
        conn.execute(
            f"ALTER TABLE {table_sql} ADD CONSTRAINT {constraint_sql} "
            f"FOREIGN KEY ({source_cols_sql}) REFERENCES {_quote_ident_pg('usuarios')} ({target_cols_sql}){delete_clause}"
        )
        reparadas.append(f"{source_table}.{constraint_name}")

    if reparadas:
        logger.warning(
            "Se reasignaron FKs desde usuarios_legacy hacia usuarios: %s",
            ", ".join(reparadas),
        )

    return reparadas


def _migrar_estado_encargos_postgres(conn) -> None:
    """Actualiza el CHECK de encargos.estado en PostgreSQL sin recrear la tabla."""
    if DB_TYPE != "postgresql" or not _tabla_existe_conn(conn, "encargos"):
        return

    conn.execute(
        """
        UPDATE encargos
        SET estado = CASE
            WHEN estado = 'pendiente' THEN 'confirmado'
            WHEN estado IN ('cotizacion', 'confirmado', 'con_anticipo', 'programado', 'listo', 'entregado', 'cancelado') THEN estado
            ELSE 'confirmado'
        END
        """
    )

    constraints = conn.execute(
        """
        SELECT con.conname AS constraint_name
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        WHERE con.contype = 'c'
          AND rel.relname = 'encargos'
          AND pg_get_constraintdef(con.oid) ILIKE '%estado%'
        ORDER BY con.conname
        """
    ).fetchall()

    for row in constraints:
        constraint_name = str(row.get("constraint_name", "") or "").strip()
        if constraint_name:
            conn.execute(
                f"ALTER TABLE {_quote_ident_pg('encargos')} DROP CONSTRAINT IF EXISTS {_quote_ident_pg(constraint_name)}"
            )

    conn.execute("ALTER TABLE encargos ALTER COLUMN estado SET DEFAULT 'confirmado'")
    conn.execute(
        """
        ALTER TABLE encargos
        ADD CONSTRAINT encargos_estado_check
        CHECK(estado IN ('cotizacion','confirmado','con_anticipo','programado','listo','entregado','cancelado'))
        """
    )


def _asegurar_surtido_tipo_productos_conn(conn) -> None:
    columnas = _columnas_tabla_conn(conn, "productos")
    if "surtido_tipo" in columnas:
        return
    if _ejecutar_migracion_tolerante(
        conn,
        "ALTER TABLE productos ADD COLUMN surtido_tipo TEXT NOT NULL DEFAULT 'none'",
    ):
        conn.execute("""
            UPDATE productos
            SET surtido_tipo = 'none'
            WHERE surtido_tipo IS NULL OR trim(surtido_tipo) = ''
        """)


def _asegurar_venta_item_modificaciones_schema_conn(conn) -> set[str]:
    columnas = _columnas_tabla_conn(conn, "venta_item_modificaciones")
    if not columnas:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS venta_item_modificaciones (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                venta_item_id   INTEGER NOT NULL,
                tipo            TEXT NOT NULL CHECK(tipo IN ('adicional', 'exclusion')),
                descripcion     TEXT NOT NULL,
                cantidad        INTEGER NOT NULL DEFAULT 1,
                precio_extra    REAL NOT NULL DEFAULT 0.0,
                FOREIGN KEY (venta_item_id) REFERENCES venta_items(id) ON DELETE CASCADE
            )
        """)
        _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_venta_item_mods_item ON venta_item_modificaciones(venta_item_id)")
        return _columnas_tabla_conn(conn, "venta_item_modificaciones")

    if "venta_item_id" not in columnas and "venta_id" in columnas:
        row_count_row = conn.execute(
            "SELECT COUNT(*) AS total FROM venta_item_modificaciones"
        ).fetchone()
        row_count = int((row_count_row["total"] if row_count_row else 0) or 0)
        if row_count == 0 and DB_TYPE == "sqlite":
            conn.execute("ALTER TABLE venta_item_modificaciones RENAME TO venta_item_modificaciones_legacy")
            conn.execute("""
                CREATE TABLE venta_item_modificaciones (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    venta_item_id   INTEGER NOT NULL,
                    tipo            TEXT NOT NULL CHECK(tipo IN ('adicional', 'exclusion')),
                    descripcion     TEXT NOT NULL,
                    cantidad        INTEGER NOT NULL DEFAULT 1,
                    precio_extra    REAL NOT NULL DEFAULT 0.0,
                    FOREIGN KEY (venta_item_id) REFERENCES venta_items(id) ON DELETE CASCADE
                )
            """)
            conn.execute("DROP TABLE venta_item_modificaciones_legacy")
        else:
            _ejecutar_migracion_tolerante(conn, "ALTER TABLE venta_item_modificaciones ADD COLUMN venta_item_id INTEGER")

    columnas = _columnas_tabla_conn(conn, "venta_item_modificaciones")
    if "venta_item_id" in columnas:
        _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_venta_item_mods_item ON venta_item_modificaciones(venta_item_id)")
    if "venta_id" in columnas:
        _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_venta_item_mods_venta ON venta_item_modificaciones(venta_id)")
    return columnas


def _eliminar_modificaciones_venta_items_conn(conn, venta_id: int, item_ids: list[int]) -> None:
    columnas = _asegurar_venta_item_modificaciones_schema_conn(conn)
    if "venta_item_id" in columnas and item_ids:
        placeholders = ",".join("?" * len(item_ids))
        conn.execute(
            f"DELETE FROM venta_item_modificaciones WHERE venta_item_id IN ({placeholders})",
            item_ids,
        )
    elif "venta_id" in columnas:
        conn.execute("DELETE FROM venta_item_modificaciones WHERE venta_id = ?", (venta_id,))


def _insertar_modificacion_venta_item_conn(conn, venta_id: int, venta_item_id: int, mod: dict) -> None:
    columnas = _asegurar_venta_item_modificaciones_schema_conn(conn)
    if "venta_item_id" in columnas and "venta_id" in columnas:
        conn.execute(
            """
            INSERT INTO venta_item_modificaciones
                (venta_item_id, venta_id, tipo, descripcion, cantidad, precio_extra)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                venta_item_id,
                venta_id,
                mod["tipo"],
                mod["descripcion"],
                mod.get("cantidad", 1),
                mod.get("precio_extra", 0),
            ),
        )
        return
    if "venta_item_id" in columnas:
        conn.execute(
            """
            INSERT INTO venta_item_modificaciones
                (venta_item_id, tipo, descripcion, cantidad, precio_extra)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                venta_item_id,
                mod["tipo"],
                mod["descripcion"],
                mod.get("cantidad", 1),
                mod.get("precio_extra", 0),
            ),
        )
        return
    conn.execute(
        """
        INSERT INTO venta_item_modificaciones
            (venta_id, tipo, descripcion, cantidad, precio_extra)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            venta_id,
            mod["tipo"],
            mod["descripcion"],
            mod.get("cantidad", 1),
            mod.get("precio_extra", 0),
        ),
    )


def _cargar_modificaciones_venta_items_conn(conn, venta_id: int, item_ids: list[int]) -> dict[int, list[dict]]:
    columnas = _asegurar_venta_item_modificaciones_schema_conn(conn)
    resultado: dict[int, list[dict]] = {}
    if not item_ids:
        return resultado

    if "venta_item_id" in columnas:
        select_cols = ["venta_item_id", "tipo", "descripcion", "cantidad", "precio_extra"]
        if "venta_id" in columnas:
            select_cols.append("venta_id")
            placeholders = ",".join("?" * len(item_ids))
            rows = conn.execute(
                f"""
                SELECT {', '.join(select_cols)}
                FROM venta_item_modificaciones
                WHERE venta_item_id IN ({placeholders})
                   OR (venta_id = ? AND venta_item_id IS NULL)
                ORDER BY tipo, id
                """,
                [*item_ids, venta_id],
            ).fetchall()
        else:
            placeholders = ",".join("?" * len(item_ids))
            rows = conn.execute(
                f"""
                SELECT {', '.join(select_cols)}
                FROM venta_item_modificaciones
                WHERE venta_item_id IN ({placeholders})
                ORDER BY tipo, id
                """,
                item_ids,
            ).fetchall()

        fallback_item_id = item_ids[0] if len(item_ids) == 1 else None
        for row in rows:
            mod = _row_to_dict(row)
            target_item_id = int(mod.get("venta_item_id") or 0) or fallback_item_id
            if not target_item_id:
                continue
            resultado.setdefault(target_item_id, []).append(mod)
        return resultado

    if "venta_id" in columnas:
        rows = conn.execute(
            """
            SELECT venta_id, tipo, descripcion, cantidad, precio_extra
            FROM venta_item_modificaciones
            WHERE venta_id = ?
            ORDER BY tipo, id
            """,
            (venta_id,),
        ).fetchall()
        if len(item_ids) == 1:
            resultado[item_ids[0]] = [_row_to_dict(row) for row in rows]
    return resultado


def _crear_username_base(nombre: str) -> str:
    base = _normalizar_texto_clave(nombre).replace(" ", ".")
    return base or f"usuario.{uuid4().hex[:8]}"


def _normalizar_username(username: str) -> str:
    return str(username or "").strip().lower()


def _email_local_para_username(username: str) -> str:
    username_norm = _normalizar_username(username) or f"usuario.{uuid4().hex[:8]}"
    return f"{username_norm}@local.invalid"


def _username_ya_asignado_conn(conn, username: str, exclude_usuario_id: int | None = None) -> bool:
    username_norm = _normalizar_username(username)
    if not username_norm:
        return False
    sql = "SELECT 1 FROM usuarios WHERE LOWER(COALESCE(username, '')) = ?"
    params: list = [username_norm]
    if exclude_usuario_id:
        sql += " AND id != ?"
        params.append(int(exclude_usuario_id))
    row = conn.execute(sql + " LIMIT 1", tuple(params)).fetchone()
    return bool(row)


def _crear_username_unico_conn(conn, nombre: str, username: str = "", exclude_usuario_id: int | None = None) -> str:
    base = _normalizar_username(username) or _crear_username_base(nombre)
    candidato = base
    suffix = 2
    while _username_ya_asignado_conn(conn, candidato, exclude_usuario_id=exclude_usuario_id):
        candidato = f"{base}.{suffix}"
        suffix += 1
    return candidato

CATEGORIAS_PREDETERMINADAS = [
    "Panaderia",
    "Bebidas Calientes",
    "Bebidas Frias",
    "Desayunos",
    "Almuerzos",
    "Acompañamientos",
    "Bebidas Frías",
    "Cacerola de Huevos",
    "Caldos",
    "Changua",
    "Clásicos de Queso",
    "De la Casa",
    "Dulcería",
    "Galletas",
    "Hojaldre",
    "Huevos Florentinos",
    "Huevos Rancheros",
    "Huevos Richs",
    "Omelettes",
    "Pastelería Casera",
    "Sándwiches",
    "Sándwiches - Croissant",
    "Sándwiches - Pan Saludable",
    "Típico",
]

ADICIONALES_LEGADO_PREDETERMINADOS = (
    "Huevo extra",
    "Queso extra",
    "Jamon extra",
    "Pan adicional",
    "Cafe adicional",
    "Mantequilla extra",
)

ORDEN_CATEGORIAS_PREFERIDO = [
    "Acompañamientos",
    "Caldos",
    "Changua",
    "Cacerola de Huevos",
    "Huevos Rancheros",
    "Huevos Florentinos",
    "Huevos Richs",
    "Omelettes",
    "Típico",
    "Sándwiches",
    "Sándwiches - Croissant",
    "Sándwiches - Pan Saludable",
    "De la Casa",
    "Bebidas Calientes",
    "Bebidas Frías",
    "Clásicos de Queso",
    "Hojaldre",
    "Dulcería",
    "Galletas",
    "Pastelería Casera",
    "Panaderia",
    "Bebidas Frias",
    "Desayunos",
    "Almuerzos",
]

MENUS_PRODUCTO_PREFERIDOS = ["Desayunos", "Tardes"]

CATEGORIAS_PANADERIA_PUBLICAS = {
    "acompanamientos",
    "clasicos de queso",
    "dulceria",
    "galletas",
    "hojaldre",
    "pasteleria casera",
}

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


def _normalizar_texto_clave(texto: str) -> str:
    base = unicodedata.normalize("NFKD", str(texto or ""))
    base = "".join(ch for ch in base if not unicodedata.combining(ch))
    base = base.strip().lower()
    return re.sub(r"[^a-z0-9]+", " ", base).strip()


def es_categoria_panaderia(categoria: str) -> bool:
    return _normalizar_texto_clave(categoria) in CATEGORIAS_PANADERIA_PUBLICAS


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


UMBRAL_CANTIDAD_RECETA_CORRUPTA = 10000.0
MAX_DIVISIONES_REPARACION_RECETA = 64


def _es_entero_aproximado(valor: float) -> bool:
    if not math.isfinite(valor):
        return False
    tolerancia = max(1e-9, abs(valor) * 1e-12)
    return abs(valor - round(valor)) <= tolerancia


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
                nombre TEXT NOT NULL,
                precio REAL NOT NULL DEFAULT 0.0,
                categoria TEXT NOT NULL DEFAULT 'Panaderia',
                menu TEXT NOT NULL DEFAULT '',
                descripcion TEXT NOT NULL DEFAULT '',
                es_panaderia INTEGER NOT NULL DEFAULT 0,
                activo INTEGER NOT NULL DEFAULT 1,
                es_adicional INTEGER NOT NULL DEFAULT 0,
                stock_minimo INTEGER NOT NULL DEFAULT 0,
                surtido_tipo TEXT NOT NULL DEFAULT 'none',
                UNIQUE(nombre, categoria)
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
                valor TEXT NOT NULL DEFAULT '',
                panaderia_id INTEGER
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS panaderias (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                slug          TEXT UNIQUE NOT NULL,
                nombre        TEXT NOT NULL,
                activa        INTEGER NOT NULL DEFAULT 1,
                dominio_custom TEXT NOT NULL DEFAULT '',
                created_at    TEXT NOT NULL
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS sedes (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                panaderia_id  INTEGER NOT NULL,
                slug          TEXT NOT NULL,
                nombre        TEXT NOT NULL,
                codigo        TEXT NOT NULL DEFAULT '',
                activa        INTEGER NOT NULL DEFAULT 1,
                created_at    TEXT NOT NULL,
                UNIQUE(panaderia_id, slug),
                FOREIGN KEY (panaderia_id) REFERENCES panaderias(id)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS tenant_subscriptions (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                panaderia_id      INTEGER NOT NULL UNIQUE,
                plan              TEXT NOT NULL DEFAULT 'free'
                                  CHECK(plan IN ('free','starter','pro','enterprise')),
                estado            TEXT NOT NULL DEFAULT 'activa'
                                  CHECK(estado IN ('activa','trial','vencida','cancelada','suspendida')),
                fecha_inicio      TEXT NOT NULL,
                fecha_vencimiento TEXT,
                max_sedes         INTEGER NOT NULL DEFAULT 1,
                max_usuarios      INTEGER NOT NULL DEFAULT 5,
                max_productos     INTEGER NOT NULL DEFAULT 50,
                notas             TEXT NOT NULL DEFAULT '',
                created_at        TEXT NOT NULL,
                updated_at        TEXT NOT NULL,
                FOREIGN KEY (panaderia_id) REFERENCES panaderias(id)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS terminales (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                panaderia_id INTEGER NOT NULL,
                sede_id      INTEGER NOT NULL,
                nombre       TEXT NOT NULL,
                codigo       TEXT NOT NULL,
                tipo         TEXT NOT NULL DEFAULT 'caja'
                             CHECK(tipo IN ('caja','mesero','kiosko','cocina')),
                activa       INTEGER NOT NULL DEFAULT 1,
                last_seen_at TEXT NOT NULL DEFAULT '',
                created_at   TEXT NOT NULL,
                updated_at   TEXT NOT NULL,
                UNIQUE(sede_id, codigo),
                FOREIGN KEY (panaderia_id) REFERENCES panaderias(id),
                FOREIGN KEY (sede_id)      REFERENCES sedes(id)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS tenant_branding (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                panaderia_id   INTEGER UNIQUE NOT NULL,
                brand_name     TEXT NOT NULL DEFAULT 'RICHS',
                legal_name     TEXT NOT NULL DEFAULT '',
                tagline        TEXT NOT NULL DEFAULT 'Panaderia artesanal',
                support_label  TEXT NOT NULL DEFAULT 'Delicias que nutren',
                logo_path      TEXT NOT NULL DEFAULT 'brand/richs-logo.svg',
                favicon_path   TEXT NOT NULL DEFAULT 'brand/richs-logo.svg',
                primary_color  TEXT NOT NULL DEFAULT '#8b5513',
                secondary_color TEXT NOT NULL DEFAULT '#d4722a',
                accent_color   TEXT NOT NULL DEFAULT '#e0a142',
                created_at     TEXT NOT NULL,
                updated_at     TEXT NOT NULL,
                FOREIGN KEY (panaderia_id) REFERENCES panaderias(id)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS tenant_assets (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                panaderia_id  INTEGER NOT NULL,
                tipo          TEXT NOT NULL,
                storage_key   TEXT NOT NULL,
                mime_type     TEXT NOT NULL DEFAULT '',
                checksum      TEXT NOT NULL DEFAULT '',
                created_at    TEXT NOT NULL,
                UNIQUE(panaderia_id, tipo, storage_key),
                FOREIGN KEY (panaderia_id) REFERENCES panaderias(id)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS login_attempts (
                scope_key     TEXT PRIMARY KEY,
                attempts      INTEGER NOT NULL DEFAULT 0,
                locked_until  TEXT NOT NULL DEFAULT '',
                updated_at    TEXT NOT NULL
            )
        """)

        # Usuarios con roles simples
        conn.execute("""
            CREATE TABLE IF NOT EXISTS usuarios (
                id     INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT NOT NULL,
                pin    TEXT NOT NULL,
                rol    TEXT NOT NULL CHECK(rol IN ('panadero', 'cajero', 'mesero', 'tenant_admin', 'platform_superadmin')),
                username TEXT,
                email TEXT,
                password_hash TEXT NOT NULL DEFAULT '',
                pin_hash TEXT NOT NULL DEFAULT '',
                pin_lookup_digest TEXT NOT NULL DEFAULT '',
                activo INTEGER NOT NULL DEFAULT 1,
                must_change_password INTEGER NOT NULL DEFAULT 1,
                failed_login_count INTEGER NOT NULL DEFAULT 0,
                locked_until TEXT NOT NULL DEFAULT '',
                last_login_at TEXT NOT NULL DEFAULT '',
                panaderia_id INTEGER,
                sede_id INTEGER
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS tenant_memberships (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_id   INTEGER NOT NULL,
                panaderia_id INTEGER NOT NULL,
                sede_id      INTEGER NOT NULL,
                rol          TEXT NOT NULL,
                activa       INTEGER NOT NULL DEFAULT 1,
                invited_by   INTEGER,
                created_at   TEXT NOT NULL,
                UNIQUE(usuario_id, panaderia_id, sede_id),
                FOREIGN KEY (usuario_id)   REFERENCES usuarios(id)   ON DELETE CASCADE,
                FOREIGN KEY (panaderia_id) REFERENCES panaderias(id),
                FOREIGN KEY (sede_id)      REFERENCES sedes(id)
            )
        """)

        # Ventas individuales (registradas por el cajero)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ventas (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha           TEXT NOT NULL,
                hora            TEXT NOT NULL,
                producto_id     INTEGER,
                producto        TEXT NOT NULL,
                cantidad        INTEGER NOT NULL,
                precio_unitario REAL NOT NULL,
                total           REAL NOT NULL,
                registrado_por  TEXT DEFAULT '',
                venta_grupo     TEXT DEFAULT '',
                metodo_pago     TEXT DEFAULT 'efectivo',
                monto_recibido  REAL NOT NULL DEFAULT 0.0,
                cambio          REAL NOT NULL DEFAULT 0.0,
                referencia_tipo TEXT DEFAULT 'pos',
                referencia_id   INTEGER
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS arqueos_caja (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha              TEXT NOT NULL,
                abierto_en         TEXT NOT NULL,
                abierto_por        TEXT NOT NULL DEFAULT '',
                monto_apertura     REAL NOT NULL DEFAULT 0.0,
                estado             TEXT NOT NULL DEFAULT 'abierto'
                                   CHECK(estado IN ('abierto', 'cerrado')),
                notas              TEXT DEFAULT '',
                cerrado_en         TEXT DEFAULT NULL,
                cerrado_por        TEXT DEFAULT '',
                monto_cierre       REAL DEFAULT NULL,
                efectivo_esperado  REAL DEFAULT NULL,
                diferencia_cierre  REAL DEFAULT NULL,
                notas_cierre       TEXT DEFAULT '',
                reabierto_en              TEXT DEFAULT '',
                reabierto_por             TEXT DEFAULT '',
                motivo_reapertura         TEXT DEFAULT '',
                reaperturas               INTEGER NOT NULL DEFAULT 0,
                monto_tarjeta_cierre      REAL DEFAULT NULL,
                monto_transferencia_cierre REAL DEFAULT NULL,
                diferencia_tarjeta        REAL DEFAULT NULL,
                diferencia_transferencia  REAL DEFAULT NULL
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
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha             TEXT NOT NULL,
                dia_semana        TEXT NOT NULL,
                producto          TEXT NOT NULL,
                producido         INTEGER NOT NULL,
                vendido           INTEGER NOT NULL,
                sobrante          INTEGER GENERATED ALWAYS AS (producido - vendido) VIRTUAL,
                sobrante_inicial  INTEGER NOT NULL DEFAULT 0,
                observaciones     TEXT DEFAULT '',
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
                activa INTEGER NOT NULL DEFAULT 1,
                eliminada INTEGER NOT NULL DEFAULT 0
            )
        """)

        # Pedidos con estado y trazabilidad
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pedidos (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                mesa_id        INTEGER,
                mesero         TEXT NOT NULL DEFAULT '',
                estado         TEXT NOT NULL DEFAULT 'pendiente'
                               CHECK(estado IN ('pendiente','en_preparacion','listo','pagado','cancelado')),
                fecha          TEXT NOT NULL,
                hora           TEXT NOT NULL,
                hora_pagado    TEXT DEFAULT NULL,
                notas          TEXT DEFAULT '',
                total          REAL NOT NULL DEFAULT 0.0,
                creado_en      TEXT,
                pagado_en      TEXT,
                pagado_por     TEXT DEFAULT '',
                metodo_pago    TEXT DEFAULT '',
                monto_recibido REAL NOT NULL DEFAULT 0.0,
                cambio         REAL NOT NULL DEFAULT 0.0,
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
                producto_id INTEGER,
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

        # Encargos (pre-pedidos con fecha de entrega)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS encargos (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha_entrega  TEXT NOT NULL,
                cliente        TEXT NOT NULL DEFAULT '',
                empresa        TEXT DEFAULT '',
                notas          TEXT DEFAULT '',
                estado         TEXT NOT NULL DEFAULT 'pendiente'
                               CHECK(estado IN ('pendiente','listo','entregado','cancelado')),
                registrado_por TEXT NOT NULL DEFAULT '',
                creado_en      TEXT NOT NULL,
                total          REAL NOT NULL DEFAULT 0.0
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS encargo_items (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                encargo_id      INTEGER NOT NULL,
                producto_id     INTEGER,
                producto        TEXT NOT NULL,
                cantidad        INTEGER NOT NULL DEFAULT 1,
                precio_unitario REAL NOT NULL DEFAULT 0.0,
                subtotal        REAL NOT NULL DEFAULT 0.0,
                notas           TEXT DEFAULT '',
                FOREIGN KEY (encargo_id) REFERENCES encargos(id) ON DELETE CASCADE
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
                activo  INTEGER NOT NULL DEFAULT 1,
                panaderia_id INTEGER
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS inventario_sede (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                sede_id      INTEGER NOT NULL,
                insumo_id     INTEGER NOT NULL,
                stock         REAL NOT NULL DEFAULT 0.0,
                stock_minimo  REAL NOT NULL DEFAULT 0.0,
                updated_at    TEXT NOT NULL,
                UNIQUE(sede_id, insumo_id),
                FOREIGN KEY (sede_id) REFERENCES sedes(id),
                FOREIGN KEY (insumo_id) REFERENCES insumos(id)
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
                panaderia_id INTEGER,
                UNIQUE(producto, insumo_id, panaderia_id),
                FOREIGN KEY (insumo_id) REFERENCES insumos(id),
                FOREIGN KEY (panaderia_id) REFERENCES panaderias(id)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS receta_fichas (
                id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                producto                TEXT NOT NULL,
                rendimiento_texto       TEXT DEFAULT '',
                tiempo_preparacion_min  REAL NOT NULL DEFAULT 0.0,
                tiempo_amasado_min      REAL NOT NULL DEFAULT 0.0,
                tiempo_fermentacion_min REAL NOT NULL DEFAULT 0.0,
                tiempo_horneado_min     REAL NOT NULL DEFAULT 0.0,
                temperatura_horneado    REAL NOT NULL DEFAULT 0.0,
                pasos                   TEXT DEFAULT '',
                observaciones           TEXT DEFAULT '',
                panaderia_id            INTEGER,
                UNIQUE(producto, panaderia_id),
                FOREIGN KEY (panaderia_id) REFERENCES panaderias(id)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS producto_componentes (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                producto            TEXT NOT NULL,
                componente_producto TEXT NOT NULL,
                cantidad            REAL NOT NULL DEFAULT 1.0,
                panaderia_id        INTEGER,
                UNIQUE(producto, componente_producto, panaderia_id),
                FOREIGN KEY (panaderia_id) REFERENCES panaderias(id)
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
                usuario_id   INTEGER,
                panaderia_id INTEGER,
                sede_id      INTEGER,
                ip           TEXT NOT NULL DEFAULT '',
                user_agent   TEXT NOT NULL DEFAULT '',
                request_id   TEXT NOT NULL DEFAULT '',
                accion       TEXT NOT NULL,
                resultado    TEXT NOT NULL DEFAULT 'ok',
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

        # ── Índices ──
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ventas_fecha ON ventas(fecha)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ventas_producto ON ventas(producto)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ventas_fecha_producto ON ventas(fecha, producto)")
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

        # Persistir tablas e índices antes de ejecutar migraciones.
        # En PostgreSQL, si una migración falla, el rollback no deshará las tablas.
        conn.commit()

        # Migrar tabla productos existente: agregar columnas si faltan
        _migrar_productos(conn)
        # Migrar tabla usuarios: agregar rol mesero al CHECK
        _migrar_usuarios(conn)
        _migrar_recetas(conn)
        _migrar_adicionales(conn)
        _reparar_cantidades_receta_infladas(conn)
        _migrar_ventas_pedidos_caja(conn)
        _migrar_plataforma_base(conn)
        _migrar_jornada(conn)
        _migrar_constraints_multitenant(conn)
        _reparar_foreign_keys_tablas_temporales(conn)
        _migrar_fase1(conn)
        _migrar_fase2(conn)
        _migrar_fase3(conn)
        _migrar_fase5(conn)
        _migrar_fase6(conn)
        _migrar_fase7(conn)
        _migrar_fase8(conn)
        _migrar_fase9(conn)
        _migrar_fase10(conn)
        _migrar_fase11(conn)
        _migrar_fase12(conn)
        _migrar_fase13(conn)
        _migrar_fase14(conn)
        _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_audit_panaderia_sede_fecha ON audit_log(panaderia_id, sede_id, fecha)")
        _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_login_attempts_updated_at ON login_attempts(updated_at)")
        _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_sedes_panaderia_slug ON sedes(panaderia_id, slug)")

        # Índice sobre columna que puede venir de migración (tabla vieja) o CREATE TABLE (tabla nueva)
        _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_ventas_venta_grupo ON ventas(venta_grupo)")

        tenant_base = obtener_panaderia_principal_conn(conn)
        sede_base = obtener_sede_principal_conn(conn, tenant_base["id"])
        _sembrar_categorias_producto(conn, tenant_base["id"])
        conn.execute("""
            INSERT OR IGNORE INTO configuracion_sistema (clave, valor)
            VALUES ('codigo_verificacion_caja', '2468')
        """)
        _desactivar_adicionales_legado(conn)

        # Productos iniciales con precios de ejemplo
        productos_iniciales = [
            ("Pan Frances", 5000.0, "Panaderia"),
            ("Pan Dulce", 600.0, "Panaderia"),
            ("Croissant", 4500.0, "Panaderia"),
            ("Integral", 2000.0, "Panaderia"),
        ]
        for nombre, precio, categoria in productos_iniciales:
            conn.execute(
                """
                INSERT OR IGNORE INTO productos (nombre, precio, categoria, es_panaderia, panaderia_id)
                VALUES (?, ?, ?, ?, ?)
                """,
                (nombre, precio, categoria, 1 if categoria == "Panaderia" else 0, tenant_base["id"])
            )

        # Usuario admin por defecto
        existe = conn.execute(
            "SELECT COUNT(*) as c FROM usuarios"
        ).fetchone()
        if existe["c"] == 0:
            if not _bootstrap_admin_desde_entorno(conn, tenant_base["id"], sede_base["id"]):
                if DB_TYPE != "postgresql":
                    conn.execute(
                        """
                        INSERT INTO usuarios
                            (nombre, pin, rol, username, email, password_hash, pin_hash, pin_lookup_digest,
                             activo, must_change_password, panaderia_id, sede_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            "Admin",
                            _hash_pin("1234"),
                            "panadero",
                            "admin",
                            "admin@local.invalid",
                            _hash_password("1234"),
                            _hash_pin("1234"),
                            _pin_lookup_digest("1234"),
                            1,
                            1,
                            tenant_base["id"],
                            sede_base["id"],
                        )
                    )
                    conn.execute(
                        """
                        INSERT INTO usuarios
                            (nombre, pin, rol, username, email, password_hash, pin_hash, pin_lookup_digest,
                             activo, must_change_password, panaderia_id, sede_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            "Cajero",
                            _hash_pin("0000"),
                            "cajero",
                            "cajero",
                            "cajero@local.invalid",
                            _hash_password("0000"),
                            _hash_pin("0000"),
                            _pin_lookup_digest("0000"),
                            1,
                            1,
                            tenant_base["id"],
                            sede_base["id"],
                        )
                    )
                    conn.execute(
                        """
                        INSERT INTO usuarios
                            (nombre, pin, rol, username, email, password_hash, pin_hash, pin_lookup_digest,
                             activo, must_change_password, panaderia_id, sede_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            "Mesero",
                            _hash_pin("1111"),
                            "mesero",
                            "mesero",
                            "mesero@local.invalid",
                            _hash_password("1111"),
                            _hash_pin("1111"),
                            _pin_lookup_digest("1111"),
                            1,
                            1,
                            tenant_base["id"],
                            sede_base["id"],
                        )
                    )
        _normalizar_pines_usuarios(conn)
        _normalizar_usuarios_plataforma(conn, tenant_base["id"], sede_base["id"])
        _sincronizar_inventario_sede_inicial(conn, sede_base["id"])
        _ejecutar_migracion_tolerante(conn, "CREATE UNIQUE INDEX IF NOT EXISTS idx_usuarios_username_unique ON usuarios(username)")
        _ejecutar_migracion_tolerante(conn, "CREATE UNIQUE INDEX IF NOT EXISTS idx_usuarios_email_unique ON usuarios(email)")

        # Mesas iniciales (5 mesas por defecto)
        for num in range(1, 6):
            conn.execute(
                "INSERT OR IGNORE INTO mesas (numero, nombre, panaderia_id, sede_id) VALUES (?, ?, ?, ?)",
                (num, f"Mesa {num}", tenant_base["id"], sede_base["id"])
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
                "INSERT OR IGNORE INTO insumos (nombre, unidad, stock, stock_minimo, panaderia_id) VALUES (?, ?, ?, ?, ?)",
                (nombre, unidad, stock, minimo, tenant_base["id"])
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
                    "SELECT id FROM insumos WHERE nombre = ? AND panaderia_id = ?",
                    (insumo_nombre, tenant_base["id"]),
                ).fetchone()
                if insumo:
                    conn.execute(
                        "INSERT OR IGNORE INTO recetas (producto, insumo_id, cantidad, unidad_receta, panaderia_id) VALUES (?, ?, ?, ?, ?)",
                        (producto, insumo["id"], cant, unidad_receta, tenant_base["id"]),
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
                    pasos, observaciones, panaderia_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                tenant_base["id"],
            ))

        conn.commit()


def _tabla_productos_requiere_reconstruccion(conn) -> bool:
    if DB_TYPE != "sqlite":
        return False
    schema = _schema_tabla_conn(conn, "productos")
    if not schema:
        return False

    schema_norm = "".join(schema.lower().split())
    if "nombretextunique" in schema_norm:
        return True
    return "unique(nombre,categoria)" not in schema_norm


def _reconstruir_tabla_productos(conn) -> None:
    rows = conn.execute("""
        SELECT
            id,
            nombre,
            COALESCE(precio, 0.0) AS precio,
            COALESCE(categoria, 'Panaderia') AS categoria,
            COALESCE(menu, '') AS menu,
            COALESCE(descripcion, '') AS descripcion,
            COALESCE(es_panaderia, CASE WHEN categoria = 'Panaderia' THEN 1 ELSE 0 END) AS es_panaderia,
            COALESCE(activo, 1) AS activo,
            COALESCE(es_adicional, 0) AS es_adicional,
            COALESCE(stock_minimo, 0) AS stock_minimo,
            COALESCE(surtido_tipo, 'none') AS surtido_tipo
        FROM productos
        ORDER BY id
    """).fetchall()

    conn.execute("ALTER TABLE productos RENAME TO productos_legacy")
    conn.execute("""
        CREATE TABLE productos (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre       TEXT NOT NULL,
            precio       REAL NOT NULL DEFAULT 0.0,
            categoria    TEXT NOT NULL DEFAULT 'Panaderia',
            menu         TEXT NOT NULL DEFAULT '',
            descripcion  TEXT NOT NULL DEFAULT '',
            es_panaderia INTEGER NOT NULL DEFAULT 0,
            activo       INTEGER NOT NULL DEFAULT 1,
            es_adicional INTEGER NOT NULL DEFAULT 0,
            stock_minimo INTEGER NOT NULL DEFAULT 0,
            surtido_tipo TEXT NOT NULL DEFAULT 'none',
            UNIQUE(nombre, categoria)
        )
    """)

    for row in rows:
        conn.execute("""
            INSERT OR IGNORE INTO productos (
                id, nombre, precio, categoria, menu, descripcion,
                es_panaderia, activo, es_adicional, stock_minimo, surtido_tipo
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row["id"],
            row["nombre"],
            float(row["precio"] or 0),
            row["categoria"],
            row["menu"],
            row["descripcion"],
            1 if int(row["es_panaderia"] or 0) else 0,
            1 if int(row["activo"] or 0) else 0,
            1 if int(row["es_adicional"] or 0) else 0,
            int(row["stock_minimo"] or 0),
            _normalizar_surtido_tipo(row["surtido_tipo"]),
        ))

    conn.execute("DROP TABLE productos_legacy")


def _migrar_productos(conn):
    """Agrega columnas de soporte y actualiza restricciones del catálogo."""
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE productos ADD COLUMN precio REAL NOT NULL DEFAULT 0.0")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE productos ADD COLUMN categoria TEXT NOT NULL DEFAULT 'Panaderia'")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE productos ADD COLUMN menu TEXT NOT NULL DEFAULT ''")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE productos ADD COLUMN descripcion TEXT NOT NULL DEFAULT ''")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE productos ADD COLUMN es_panaderia INTEGER NOT NULL DEFAULT 0")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE productos ADD COLUMN activo INTEGER NOT NULL DEFAULT 1")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE productos ADD COLUMN es_adicional INTEGER NOT NULL DEFAULT 0")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE productos ADD COLUMN stock_minimo INTEGER NOT NULL DEFAULT 0")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE productos ADD COLUMN surtido_tipo TEXT NOT NULL DEFAULT 'none'")
    conn.execute("""
        UPDATE productos
        SET es_panaderia = CASE
            WHEN es_panaderia IS NULL OR es_panaderia = 0 THEN
                CASE WHEN categoria = 'Panaderia' THEN 1 ELSE 0 END
            ELSE es_panaderia
        END
    """)
    conn.execute("""
        UPDATE productos
        SET surtido_tipo = CASE
            WHEN surtido_tipo IN ('none', 'sal', 'dulce', 'ambos') THEN surtido_tipo
            ELSE 'none'
        END
    """)
    if _tabla_productos_requiere_reconstruccion(conn):
        _reconstruir_tabla_productos(conn)


def _sembrar_categorias_producto(conn, panaderia_id: int | None = None):
    for categoria in CATEGORIAS_PREDETERMINADAS:
        if panaderia_id is not None:
            conn.execute(
                "INSERT OR IGNORE INTO categorias_producto (nombre, activa, panaderia_id) VALUES (?, 1, ?)",
                (categoria, panaderia_id),
            )
        else:
            conn.execute(
                "INSERT OR IGNORE INTO categorias_producto (nombre, activa) VALUES (?, 1)",
                (categoria,),
            )


def _desactivar_adicionales_legado(conn) -> int:
    """
    Desactiva adicionales heredados de instalaciones viejas para que
    en pedidos solo quede visible el catalogo definido por admin.
    """
    clave_migracion = "migracion_adicionales_legado_desactivados"
    row = conn.execute(
        "SELECT valor FROM configuracion_sistema WHERE clave = ?",
        (clave_migracion,),
    ).fetchone()
    if row and str(row["valor"] or "").strip() == "1":
        return 0

    desactivados = 0
    for nombre in ADICIONALES_LEGADO_PREDETERMINADOS:
        cur = conn.execute(
            "UPDATE adicionales SET activo = 0 WHERE nombre = ? AND activo = 1",
            (nombre,),
        )
        desactivados += int(cur.rowcount or 0)

    if row is None:
        conn.execute(
            "INSERT INTO configuracion_sistema (clave, valor) VALUES (?, ?)",
            (clave_migracion, "1"),
        )
    else:
        conn.execute(
            "UPDATE configuracion_sistema SET valor = ? WHERE clave = ?",
            ("1", clave_migracion),
        )

    if desactivados:
        logger.info(
            "Se desactivaron %s adicionales heredados para dejar solo el catalogo manual.",
            desactivados,
        )
    return desactivados


def _migrar_usuarios(conn):
    """Recrea la tabla usuarios con columnas y roles de plataforma si hace falta."""
    schema = _schema_tabla_conn(conn, "usuarios").lower()
    required_tokens = [
        "tenant_admin",
        "platform_superadmin",
        "username",
        "email",
        "password_hash",
        "pin_hash",
        "panaderia_id",
        "sede_id",
    ]
    if schema and all(token in schema for token in required_tokens):
        _reparar_foreign_keys_usuarios_legacy_postgres(conn)
        _ejecutar_migracion_tolerante(conn, "DROP TABLE IF EXISTS usuarios_legacy")
        _normalizar_pines_usuarios(conn)
        return

    rows = conn.execute("SELECT * FROM usuarios").fetchall()
    rows_dict = [dict(row) for row in rows]
    conn.execute("ALTER TABLE usuarios RENAME TO usuarios_legacy")
    conn.execute("""
        CREATE TABLE usuarios (
            id     INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre TEXT NOT NULL,
            pin    TEXT NOT NULL,
            rol    TEXT NOT NULL CHECK(rol IN ('panadero', 'cajero', 'mesero', 'tenant_admin', 'platform_superadmin')),
            username TEXT,
            email TEXT,
            password_hash TEXT NOT NULL DEFAULT '',
            pin_hash TEXT NOT NULL DEFAULT '',
            pin_lookup_digest TEXT NOT NULL DEFAULT '',
            activo INTEGER NOT NULL DEFAULT 1,
            must_change_password INTEGER NOT NULL DEFAULT 1,
            failed_login_count INTEGER NOT NULL DEFAULT 0,
            locked_until TEXT NOT NULL DEFAULT '',
            last_login_at TEXT NOT NULL DEFAULT '',
            panaderia_id INTEGER,
            sede_id INTEGER
        )
    """)
    for index, row in enumerate(rows_dict, start=1):
        nombre = str(row.get("nombre", "") or "").strip() or f"Usuario {index}"
        pin = str(row.get("pin", "") or "").strip()
        username = str(row.get("username", "") or "").strip() or _crear_username_base(nombre)
        email = str(row.get("email", "") or "").strip() or f"{username}@local.invalid"
        password_hash = str(row.get("password_hash", "") or "").strip()
        pin_hash = str(row.get("pin_hash", "") or "").strip()
        if not pin_hash and pin:
            pin_hash = pin if _pin_ya_esta_hasheado(pin) else _hash_pin(pin)
        pin_lookup_digest = str(row.get("pin_lookup_digest", "") or "").strip()
        if not pin_lookup_digest and pin:
            pin_lookup_digest = _pin_lookup_digest_desde_fila(pin_hash, pin)
        if not password_hash and pin:
            password_hash = _hash_password(pin[-6:])
        conn.execute(
            """
            INSERT INTO usuarios (
                id, nombre, pin, rol, username, email, password_hash, pin_hash, pin_lookup_digest,
                activo, must_change_password, failed_login_count, locked_until, last_login_at,
                panaderia_id, sede_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row.get("id"),
                nombre,
                pin_hash or pin,
                normalize_role(row.get("rol", "cajero"), fallback="cajero"),
                username,
                email,
                password_hash,
                pin_hash,
                pin_lookup_digest,
                int(row.get("activo", 1) or 1),
                int(row.get("must_change_password", 1) or 1),
                int(row.get("failed_login_count", 0) or 0),
                str(row.get("locked_until", "") or ""),
                str(row.get("last_login_at", "") or ""),
                row.get("panaderia_id"),
                row.get("sede_id"),
            )
        )
    _reparar_foreign_keys_usuarios_legacy_postgres(conn)
    conn.execute("DROP TABLE IF EXISTS usuarios_legacy")
    _normalizar_pines_usuarios(conn)


def _bootstrap_admin_desde_entorno(conn, panaderia_id: int, sede_id: int) -> bool:
    username = str(os.environ.get("BOOTSTRAP_ADMIN_USERNAME", "") or "").strip()
    password = str(os.environ.get("BOOTSTRAP_ADMIN_PASSWORD", "") or "").strip()
    pin = str(os.environ.get("BOOTSTRAP_ADMIN_PIN", "") or "").strip()
    nombre = str(os.environ.get("BOOTSTRAP_ADMIN_NAME", "") or "").strip() or "Administrador"
    email = str(os.environ.get("BOOTSTRAP_ADMIN_EMAIL", "") or "").strip() or f"{username}@local.invalid"
    role = normalize_role(os.environ.get("BOOTSTRAP_ADMIN_ROLE", TENANT_ADMIN_ROLE), fallback=TENANT_ADMIN_ROLE)

    if not username or not password or not pin:
        return False

    conn.execute(
        """
        INSERT INTO usuarios (
            nombre, pin, rol, username, email, password_hash, pin_hash, pin_lookup_digest,
            activo, must_change_password, panaderia_id, sede_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            nombre,
            _hash_pin(pin),
            role,
            username,
            email,
            _hash_password(password),
            _hash_pin(pin),
            _pin_lookup_digest(pin),
            1,
            1,
            panaderia_id,
            sede_id,
        )
    )
    return True


def obtener_panaderia_principal_conn(conn) -> dict:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row = conn.execute(
        "SELECT id, slug, nombre, activa, dominio_custom FROM panaderias WHERE slug = ?",
        (DEFAULT_TENANT_SLUG,),
    ).fetchone()
    if row:
        return dict(row)
    conn.execute(
        """
        INSERT INTO panaderias (slug, nombre, activa, dominio_custom, created_at)
        VALUES (?, ?, 1, '', ?)
        """,
        (DEFAULT_TENANT_SLUG, DEFAULT_TENANT_NAME, now),
    )
    row = conn.execute(
        "SELECT id, slug, nombre, activa, dominio_custom FROM panaderias WHERE slug = ?",
        (DEFAULT_TENANT_SLUG,),
    ).fetchone()
    return dict(row)


def obtener_sede_principal_conn(conn, panaderia_id: int) -> dict:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row = conn.execute(
        "SELECT id, panaderia_id, slug, nombre, codigo, activa FROM sedes WHERE panaderia_id = ? AND slug = ?",
        (panaderia_id, DEFAULT_BRANCH_SLUG),
    ).fetchone()
    if row:
        return dict(row)
    conn.execute(
        """
        INSERT INTO sedes (panaderia_id, slug, nombre, codigo, activa, created_at)
        VALUES (?, ?, ?, ?, 1, ?)
        """,
        (panaderia_id, DEFAULT_BRANCH_SLUG, DEFAULT_BRANCH_NAME, "PRINCIPAL", now),
    )
    row = conn.execute(
        "SELECT id, panaderia_id, slug, nombre, codigo, activa FROM sedes WHERE panaderia_id = ? AND slug = ?",
        (panaderia_id, DEFAULT_BRANCH_SLUG),
    ).fetchone()
    return dict(row)


def _normalizar_usuarios_plataforma(conn, panaderia_id: int, sede_id: int) -> None:
    rows = conn.execute(
        """
        SELECT id, nombre, pin, rol, username, email, password_hash, pin_hash, pin_lookup_digest, panaderia_id, sede_id
        FROM usuarios
        ORDER BY id ASC
        """
    ).fetchall()
    usados_usernames: set[str] = set()
    usados_emails: set[str] = set()
    for row in rows:
        username = str(row["username"] or "").strip()
        if not username or username in usados_usernames:
            base = _crear_username_base(row["nombre"])
            username = base
            suffix = 2
            while username in usados_usernames:
                username = f"{base}.{suffix}"
                suffix += 1
        usados_usernames.add(username)

        email = str(row["email"] or "").strip()
        if not email or email in usados_emails:
            base_email = f"{username}@local.invalid"
            email = base_email
            suffix = 2
            while email in usados_emails:
                email = f"{username}+{suffix}@local.invalid"
                suffix += 1
        usados_emails.add(email)

        pin_hash = str(row["pin_hash"] or "").strip()
        if not pin_hash:
            pin_actual = str(row["pin"] or "").strip()
            pin_hash = pin_actual if _pin_ya_esta_hasheado(pin_actual) else _hash_pin(pin_actual)
        pin_lookup_digest = str(row["pin_lookup_digest"] or "").strip()
        if not pin_lookup_digest:
            pin_lookup_digest = _pin_lookup_digest_desde_fila(pin_hash, row["pin"])
        password_hash = str(row["password_hash"] or "").strip()
        if not password_hash and row["pin"]:
            password_hash = _hash_password(str(row["pin"] or "").strip()[-6:])

        conn.execute(
            """
            UPDATE usuarios
            SET username = ?, email = ?, password_hash = ?, pin_hash = ?, pin_lookup_digest = ?,
                panaderia_id = COALESCE(panaderia_id, ?),
                sede_id = COALESCE(sede_id, ?)
            WHERE id = ?
            """,
            (username, email, password_hash, pin_hash, pin_lookup_digest, panaderia_id, sede_id, row["id"]),
        )


def _sincronizar_inventario_sede_inicial(conn, sede_id: int) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = conn.execute(
        "SELECT id, stock, stock_minimo FROM insumos ORDER BY id ASC"
    ).fetchall()
    for row in rows:
        conn.execute(
            """
            INSERT OR IGNORE INTO inventario_sede (sede_id, insumo_id, stock, stock_minimo, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (sede_id, row["id"], float(row["stock"] or 0), float(row["stock_minimo"] or 0), now),
        )


def _limpiar_duplicados_null_conn(conn, tabla: str, clave: str) -> None:
    """
    Elimina filas con panaderia_id=NULL cuya clave ya existe con panaderia_id asignado.
    Necesario cuando hubo seedings parciales antes de que se aplicara el constraint multitenant.
    """
    if DB_TYPE != "sqlite":
        return
    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute(
            f"DELETE FROM {tabla} WHERE panaderia_id IS NULL AND {clave} IN "
            f"(SELECT {clave} FROM {tabla} WHERE panaderia_id IS NOT NULL)",
        )
        conn.execute("PRAGMA foreign_keys = ON")
    except Exception:
        try:
            conn.execute("PRAGMA foreign_keys = ON")
        except Exception:
            pass


def _migrar_plataforma_base(conn) -> None:
    tenant = obtener_panaderia_principal_conn(conn)
    sede = obtener_sede_principal_conn(conn, tenant["id"])
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    columnas_panaderia = (
        "productos",
        "categorias_producto",
        "configuracion_sistema",
        "adicionales",
        "insumos",
        "dias_especiales",
    )
    for tabla in columnas_panaderia:
        _ejecutar_migracion_tolerante(conn, f"ALTER TABLE {tabla} ADD COLUMN panaderia_id INTEGER")

    columnas_tenant_sede = (
        "usuarios",
        "ventas",
        "arqueos_caja",
        "movimientos_caja",
        "registros_diarios",
        "mesas",
        "pedidos",
        "pedido_items",
        "pedido_estado_historial",
        "encargos",
        "audit_log",
        "mermas",
        "ajustes_pronostico",
        "alertas",
    )
    for tabla in columnas_tenant_sede:
        _ejecutar_migracion_tolerante(conn, f"ALTER TABLE {tabla} ADD COLUMN panaderia_id INTEGER")
        _ejecutar_migracion_tolerante(conn, f"ALTER TABLE {tabla} ADD COLUMN sede_id INTEGER")

    for sql in (
        "ALTER TABLE audit_log ADD COLUMN usuario_id INTEGER",
        "ALTER TABLE audit_log ADD COLUMN ip TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE audit_log ADD COLUMN user_agent TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE audit_log ADD COLUMN request_id TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE audit_log ADD COLUMN resultado TEXT NOT NULL DEFAULT 'ok'",
        "ALTER TABLE panaderias ADD COLUMN dominio_custom TEXT NOT NULL DEFAULT ''",
    ):
        _ejecutar_migracion_tolerante(conn, sql)

    # Antes de asignar panaderia_id, eliminar filas con panaderia_id=NULL que duplican
    # filas ya existentes con panaderia_id asignado (pueden haber quedado de migraciones parciales).
    _limpiar_duplicados_null_conn(conn, "insumos", "nombre")
    _limpiar_duplicados_null_conn(conn, "adicionales", "nombre")
    _limpiar_duplicados_null_conn(conn, "categorias_producto", "nombre")
    _limpiar_duplicados_null_conn(conn, "dias_especiales", "fecha")

    for tabla in columnas_panaderia:
        _ejecutar_migracion_tolerante(conn, f"UPDATE {tabla} SET panaderia_id = COALESCE(panaderia_id, ?)", (tenant["id"],))

    for tabla in columnas_tenant_sede:
        _ejecutar_migracion_tolerante(
            conn,
            f"UPDATE {tabla} SET panaderia_id = COALESCE(panaderia_id, ?), sede_id = COALESCE(sede_id, ?)",
            (tenant["id"], sede["id"]),
        )

    conn.execute(
        """
        INSERT OR IGNORE INTO tenant_branding (
            panaderia_id, brand_name, legal_name, tagline, support_label,
            logo_path, favicon_path, primary_color, secondary_color, accent_color,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            tenant["id"],
            "RICHS",
            tenant["nombre"],
            "Panaderia artesanal",
            "Delicias que nutren",
            "brand/richs-logo.svg",
            "brand/richs-logo.svg",
            "#8b5513",
            "#d4722a",
            "#e0a142",
            now,
            now,
        ),
    )


def _resolver_panaderia_legacy_recetas_conn(conn) -> int | None:
    if not _tabla_existe_conn(conn, "panaderias"):
        return None

    rows = conn.execute("SELECT id FROM panaderias ORDER BY id ASC").fetchall()
    if len(rows) == 1:
        return int(rows[0]["id"] or 0) or None
    if rows:
        return None

    try:
        tenant = obtener_panaderia_principal_conn(conn)
    except Exception:
        return None
    return int(tenant.get("id") or 0) or None


def _alinear_tablas_recetas_conn(conn) -> None:
    tenant_id = _resolver_panaderia_legacy_recetas_conn(conn)
    tablas = (
        ("recetas", "idx_recetas_tenant"),
        ("receta_fichas", "idx_receta_fichas_tenant"),
        ("producto_componentes", "idx_prod_comp_tenant"),
    )

    for nombre_tabla, index_name in tablas:
        if not _tabla_existe_conn(conn, nombre_tabla):
            continue
        _ejecutar_migracion_tolerante(conn, f"ALTER TABLE {nombre_tabla} ADD COLUMN panaderia_id INTEGER")
        if tenant_id is not None:
            conn.execute(
                f"UPDATE {nombre_tabla} SET panaderia_id = COALESCE(panaderia_id, ?)",
                (tenant_id,),
            )
        _ejecutar_migracion_tolerante(
            conn,
            f"CREATE INDEX IF NOT EXISTS {index_name} ON {nombre_tabla}(panaderia_id)",
        )

    if DB_TYPE != "postgresql":
        return

    _reemplazar_unique_legacy_pg_conn(
        conn,
        "recetas",
        ("producto", "insumo_id"),
        "uq_recetas_producto_insumo_panaderia",
        ("producto", "insumo_id", "panaderia_id"),
    )
    _reemplazar_unique_legacy_pg_conn(
        conn,
        "receta_fichas",
        ("producto",),
        "uq_receta_fichas_producto_panaderia",
        ("producto", "panaderia_id"),
    )
    _reemplazar_unique_legacy_pg_conn(
        conn,
        "producto_componentes",
        ("producto", "componente_producto"),
        "uq_prod_comp_producto_componente_panaderia",
        ("producto", "componente_producto", "panaderia_id"),
    )
    _asegurar_fk_panaderia_pg_conn(conn, "recetas", "fk_recetas_panaderia")
    _asegurar_fk_panaderia_pg_conn(conn, "receta_fichas", "fk_receta_fichas_panaderia")
    _asegurar_fk_panaderia_pg_conn(conn, "producto_componentes", "fk_prod_comp_panaderia")


def _migrar_recetas(conn):
    """Alinea tablas de recetas con multitenancy y soporte de unidades por producto."""
    _alinear_tablas_recetas_conn(conn)
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE recetas ADD COLUMN unidad_receta TEXT")

    rows = conn.execute("""
        SELECT r.id, r.cantidad, i.unidad
        FROM recetas r
        JOIN insumos i ON i.id = r.insumo_id
        WHERE r.unidad_receta IS NULL OR TRIM(r.unidad_receta) = ''
    """).fetchall()
    for row in rows:
        unidad_receta = unidad_receta_sugerida(row["unidad"])
        cantidad_receta = convertir_cantidad(row["cantidad"], row["unidad"], unidad_receta)
        conn.execute(
            "UPDATE recetas SET cantidad = ?, unidad_receta = ? WHERE id = ?",
            (cantidad_receta, unidad_receta, row["id"])
        )

    conn.execute("""
        UPDATE recetas
        SET unidad_receta = CASE
            WHEN unidad_receta IS NULL OR TRIM(unidad_receta) = '' THEN 'unidad'
            ELSE unidad_receta
        END
    """)


def _reparar_cantidades_receta_infladas(conn) -> int:
    """
    Corrige recetas infladas por una migracion antigua que reconvertia
    kg -> g o litro -> ml en cada arranque de la app.
    """
    rows = conn.execute("""
        SELECT r.id, r.producto, r.cantidad, r.unidad_receta,
               i.nombre AS insumo_nombre, i.unidad AS unidad_inventario
        FROM recetas r
        JOIN insumos i ON i.id = r.insumo_id
    """).fetchall()

    reparadas = 0
    for row in rows:
        cantidad_actual = float(row["cantidad"] or 0)
        if not math.isfinite(cantidad_actual) or cantidad_actual <= 0:
            continue

        factor = convertir_cantidad(1, row["unidad_inventario"], row["unidad_receta"])
        if factor <= 1:
            continue
        if cantidad_actual < UMBRAL_CANTIDAD_RECETA_CORRUPTA:
            continue

        cantidad_reparada = cantidad_actual
        divisiones = 0
        while math.isfinite(cantidad_reparada) and divisiones < MAX_DIVISIONES_REPARACION_RECETA:
            if divisiones == 0 and cantidad_reparada < UMBRAL_CANTIDAD_RECETA_CORRUPTA:
                break

            siguiente = cantidad_reparada / factor
            if not math.isfinite(siguiente) or siguiente <= 0:
                break
            if not _es_entero_aproximado(siguiente):
                break

            cantidad_reparada = siguiente
            divisiones += 1

        if not divisiones or not math.isfinite(cantidad_reparada):
            continue

        conn.execute(
            "UPDATE recetas SET cantidad = ? WHERE id = ?",
            (cantidad_reparada, row["id"]),
        )
        reparadas += 1

    if reparadas:
        logger.warning(
            "Se repararon %s cantidades de recetas infladas por conversion repetida.",
            reparadas,
        )
    return reparadas


def _migrar_adicionales(conn):
    """Agrega soporte de unidades configurables y componentes en adicionales."""
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE adicional_insumos ADD COLUMN unidad_config TEXT")

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
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE ventas ADD COLUMN venta_grupo TEXT DEFAULT ''")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE ventas ADD COLUMN metodo_pago TEXT DEFAULT 'efectivo'")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE ventas ADD COLUMN monto_recibido REAL NOT NULL DEFAULT 0.0")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE ventas ADD COLUMN cambio REAL NOT NULL DEFAULT 0.0")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE ventas ADD COLUMN referencia_tipo TEXT DEFAULT 'pos'")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE ventas ADD COLUMN referencia_id INTEGER")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE ventas ADD COLUMN producto_id INTEGER")

    _ejecutar_migracion_tolerante(conn, "ALTER TABLE pedidos ADD COLUMN creado_en TEXT")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE pedidos ADD COLUMN pagado_en TEXT")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE pedidos ADD COLUMN pagado_por TEXT DEFAULT ''")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE pedidos ADD COLUMN metodo_pago TEXT DEFAULT ''")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE pedidos ADD COLUMN monto_recibido REAL NOT NULL DEFAULT 0.0")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE pedidos ADD COLUMN cambio REAL NOT NULL DEFAULT 0.0")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE pedido_items ADD COLUMN producto_id INTEGER")

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

    _ejecutar_migracion_tolerante(conn, "ALTER TABLE arqueos_caja ADD COLUMN efectivo_esperado REAL DEFAULT NULL")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE arqueos_caja ADD COLUMN diferencia_cierre REAL DEFAULT NULL")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE arqueos_caja ADD COLUMN notas_cierre TEXT DEFAULT ''")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE arqueos_caja ADD COLUMN reabierto_en TEXT DEFAULT ''")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE arqueos_caja ADD COLUMN reabierto_por TEXT DEFAULT ''")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE arqueos_caja ADD COLUMN motivo_reapertura TEXT DEFAULT ''")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE arqueos_caja ADD COLUMN reaperturas INTEGER NOT NULL DEFAULT 0")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE arqueos_caja ADD COLUMN monto_tarjeta_cierre REAL DEFAULT NULL")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE arqueos_caja ADD COLUMN monto_transferencia_cierre REAL DEFAULT NULL")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE arqueos_caja ADD COLUMN diferencia_tarjeta REAL DEFAULT NULL")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE arqueos_caja ADD COLUMN diferencia_transferencia REAL DEFAULT NULL")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE registros_diarios ADD COLUMN sobrante_inicial INTEGER NOT NULL DEFAULT 0")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE ventas ADD COLUMN metodo_pago_2 TEXT DEFAULT NULL")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE ventas ADD COLUMN monto_pago_2 REAL DEFAULT NULL")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE pedidos ADD COLUMN metodo_pago_2 TEXT DEFAULT NULL")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE pedidos ADD COLUMN monto_pago_2 REAL DEFAULT NULL")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE pedidos ADD COLUMN unificado_en INTEGER DEFAULT NULL")


def _migrar_jornada(conn) -> None:
    """Agrega soporte de jornada laboral a usuarios y codigo corto a panaderias."""
    # usuarios: campos de jornada
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE usuarios ADD COLUMN jornada_activa INTEGER NOT NULL DEFAULT 0")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE usuarios ADD COLUMN jornada_activada_en TEXT NOT NULL DEFAULT ''")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE usuarios ADD COLUMN jornada_activada_por TEXT NOT NULL DEFAULT ''")
    # panaderias: codigo corto para login operativo (derivado del slug si no existe)
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE panaderias ADD COLUMN codigo TEXT NOT NULL DEFAULT ''")
    # Backfill: para panaderías existentes, derivar codigo del slug (mayúsculas, sin guiones)
    conn.execute("""
        UPDATE panaderias
        SET codigo = UPPER(REPLACE(REPLACE(slug, '-', ''), '_', ''))
        WHERE codigo = '' OR codigo IS NULL
    """)

    # Compatibilidad: usuarios operativos heredados de versiones sin jornada
    # deben conservar acceso al migrar.
    clave_migracion = "migracion_jornada_legacy_operativos"
    row = conn.execute(
        "SELECT valor FROM configuracion_sistema WHERE clave = ?",
        (clave_migracion,),
    ).fetchone()
    if not row or str(row["valor"] or "").strip() != "1":
        ahora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur = conn.execute(
            """
            UPDATE usuarios
            SET jornada_activa = 1,
                jornada_activada_en = CASE
                    WHEN COALESCE(jornada_activada_en, '') = '' THEN ?
                    ELSE jornada_activada_en
                END,
                jornada_activada_por = CASE
                    WHEN COALESCE(jornada_activada_por, '') = '' THEN 'migracion_legacy'
                    ELSE jornada_activada_por
                END
            WHERE activo = 1
              AND rol IN ('cajero', 'mesero')
              AND COALESCE(jornada_activa, 0) = 0
            """,
            (ahora,),
        )
        if row is None:
            conn.execute(
                "INSERT INTO configuracion_sistema (clave, valor) VALUES (?, ?)",
                (clave_migracion, "1"),
            )
        else:
            conn.execute(
                "UPDATE configuracion_sistema SET valor = ? WHERE clave = ?",
                ("1", clave_migracion),
            )
        if int(cur.rowcount or 0):
            logger.info(
                "migracion: se activaron %s jornadas operativas heredadas.",
                int(cur.rowcount or 0),
            )
    # Índices de jornada
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_usuarios_jornada ON usuarios(jornada_activa, panaderia_id, sede_id)")
    _ejecutar_migracion_tolerante(conn, "CREATE UNIQUE INDEX IF NOT EXISTS idx_panaderias_codigo ON panaderias(codigo)")


def _migrar_constraints_multitenant(conn) -> None:
    """
    Fase 0: reconstruye tablas cuyas UNIQUE no incluyen tenant.
    Sin esto, dos panaderías distintas colisionan en claves únicas.
    Solo aplica en SQLite; PostgreSQL requiere DDL diferente.
    """
    if DB_TYPE != "sqlite":
        return

    tenant = obtener_panaderia_principal_conn(conn)
    sede = obtener_sede_principal_conn(conn, tenant["id"])
    t_id = tenant["id"]
    s_id = sede["id"]

    def _sn(tabla: str) -> str:
        sql = _schema_tabla_conn(conn, tabla)
        return "".join(sql.lower().split()) if sql else ""

    # Deshabilitar FKs durante la reconstrucción (debe ser fuera de transacción activa)
    conn.commit()
    conn.execute("PRAGMA foreign_keys = OFF")

    # ── registros_diarios ──────────────────────────────────────────────────────
    if "unique(fecha,producto,panaderia_id,sede_id)" not in _sn("registros_diarios"):
        rows = conn.execute("""
            SELECT id, fecha, dia_semana, producto, producido, vendido,
                   COALESCE(sobrante_inicial, 0) AS sobrante_inicial,
                   COALESCE(observaciones, '') AS observaciones,
                   COALESCE(panaderia_id, ?) AS panaderia_id,
                   COALESCE(sede_id, ?) AS sede_id
            FROM registros_diarios ORDER BY id
        """, (t_id, s_id)).fetchall()
        conn.execute("ALTER TABLE registros_diarios RENAME TO _rd_old")
        conn.execute("""
            CREATE TABLE registros_diarios (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha            TEXT NOT NULL,
                dia_semana       TEXT NOT NULL,
                producto         TEXT NOT NULL,
                producido        INTEGER NOT NULL,
                vendido          INTEGER NOT NULL,
                sobrante         INTEGER GENERATED ALWAYS AS (producido - vendido) VIRTUAL,
                sobrante_inicial INTEGER NOT NULL DEFAULT 0,
                observaciones    TEXT DEFAULT '',
                panaderia_id     INTEGER,
                sede_id          INTEGER,
                UNIQUE(fecha, producto, panaderia_id, sede_id)
            )
        """)
        for r in rows:
            conn.execute("""
                INSERT OR IGNORE INTO registros_diarios
                    (id, fecha, dia_semana, producto, producido, vendido,
                     sobrante_inicial, observaciones, panaderia_id, sede_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (r["id"], r["fecha"], r["dia_semana"], r["producto"],
                  r["producido"], r["vendido"], r["sobrante_inicial"],
                  r["observaciones"], r["panaderia_id"], r["sede_id"]))
        conn.execute("DROP TABLE _rd_old")
        logger.info("migración: registros_diarios UNIQUE incluye panaderia_id, sede_id")

    # ── ajustes_pronostico ─────────────────────────────────────────────────────
    if "unique(fecha,producto,panaderia_id,sede_id)" not in _sn("ajustes_pronostico"):
        rows = conn.execute("""
            SELECT id, fecha, COALESCE(creado_en, '') AS creado_en, producto,
                   COALESCE(sugerido, 0) AS sugerido, COALESCE(ajustado, 0) AS ajustado,
                   COALESCE(motivo, '') AS motivo,
                   COALESCE(registrado_por, '') AS registrado_por,
                   COALESCE(panaderia_id, ?) AS panaderia_id,
                   COALESCE(sede_id, ?) AS sede_id
            FROM ajustes_pronostico ORDER BY id
        """, (t_id, s_id)).fetchall()
        conn.execute("ALTER TABLE ajustes_pronostico RENAME TO _ap_old")
        conn.execute("""
            CREATE TABLE ajustes_pronostico (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha          TEXT NOT NULL,
                creado_en      TEXT NOT NULL,
                producto       TEXT NOT NULL,
                sugerido       INTEGER NOT NULL DEFAULT 0,
                ajustado       INTEGER NOT NULL DEFAULT 0,
                motivo         TEXT NOT NULL DEFAULT '',
                registrado_por TEXT NOT NULL DEFAULT '',
                panaderia_id   INTEGER,
                sede_id        INTEGER,
                UNIQUE(fecha, producto, panaderia_id, sede_id)
            )
        """)
        for r in rows:
            conn.execute("""
                INSERT OR IGNORE INTO ajustes_pronostico
                    (id, fecha, creado_en, producto, sugerido, ajustado,
                     motivo, registrado_por, panaderia_id, sede_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (r["id"], r["fecha"], r["creado_en"], r["producto"],
                  r["sugerido"], r["ajustado"], r["motivo"],
                  r["registrado_por"], r["panaderia_id"], r["sede_id"]))
        conn.execute("DROP TABLE _ap_old")
        logger.info("migración: ajustes_pronostico UNIQUE incluye panaderia_id, sede_id")

    # ── mesas ──────────────────────────────────────────────────────────────────
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE mesas ADD COLUMN eliminada INTEGER NOT NULL DEFAULT 0")
    if "unique(numero,panaderia_id,sede_id)" not in _sn("mesas"):
        rows = conn.execute("""
            SELECT id, numero, COALESCE(nombre, '') AS nombre,
                   COALESCE(activa, 1) AS activa,
                   COALESCE(eliminada, 0) AS eliminada,
                   COALESCE(panaderia_id, ?) AS panaderia_id,
                   COALESCE(sede_id, ?) AS sede_id
            FROM mesas ORDER BY id
        """, (t_id, s_id)).fetchall()
        conn.execute("ALTER TABLE mesas RENAME TO _mesas_old")
        conn.execute("""
            CREATE TABLE mesas (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                numero       INTEGER NOT NULL,
                nombre       TEXT NOT NULL DEFAULT '',
                activa       INTEGER NOT NULL DEFAULT 1,
                eliminada    INTEGER NOT NULL DEFAULT 0,
                panaderia_id INTEGER,
                sede_id      INTEGER,
                UNIQUE(numero, panaderia_id, sede_id)
            )
        """)
        for r in rows:
            conn.execute("""
                INSERT OR IGNORE INTO mesas (id, numero, nombre, activa, eliminada, panaderia_id, sede_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (r["id"], r["numero"], r["nombre"], r["activa"], r["eliminada"],
                  r["panaderia_id"], r["sede_id"]))
        conn.execute("DROP TABLE _mesas_old")
        logger.info("migración: mesas UNIQUE incluye panaderia_id, sede_id")

    # ── adicionales ────────────────────────────────────────────────────────────
    if "unique(nombre,panaderia_id)" not in _sn("adicionales"):
        rows = conn.execute("""
            SELECT id, nombre, COALESCE(precio, 0.0) AS precio,
                   COALESCE(activo, 1) AS activo,
                   COALESCE(panaderia_id, ?) AS panaderia_id
            FROM adicionales ORDER BY id
        """, (t_id,)).fetchall()
        conn.execute("ALTER TABLE adicionales RENAME TO _adic_old")
        conn.execute("""
            CREATE TABLE adicionales (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre       TEXT NOT NULL,
                precio       REAL NOT NULL DEFAULT 0.0,
                activo       INTEGER NOT NULL DEFAULT 1,
                panaderia_id INTEGER,
                UNIQUE(nombre, panaderia_id)
            )
        """)
        for r in rows:
            conn.execute("""
                INSERT OR IGNORE INTO adicionales (id, nombre, precio, activo, panaderia_id)
                VALUES (?, ?, ?, ?, ?)
            """, (r["id"], r["nombre"], float(r["precio"] or 0),
                  int(r["activo"] or 1), r["panaderia_id"]))
        conn.execute("DROP TABLE _adic_old")
        logger.info("migración: adicionales UNIQUE incluye panaderia_id")

    # ── insumos ────────────────────────────────────────────────────────────────
    if "unique(nombre,panaderia_id)" not in _sn("insumos"):
        rows = conn.execute("""
            SELECT id, nombre, COALESCE(unidad, 'unidad') AS unidad,
                   COALESCE(stock, 0.0) AS stock,
                   COALESCE(stock_minimo, 0.0) AS stock_minimo,
                   COALESCE(activo, 1) AS activo,
                   COALESCE(panaderia_id, ?) AS panaderia_id
            FROM insumos ORDER BY id
        """, (t_id,)).fetchall()
        conn.execute("ALTER TABLE insumos RENAME TO _ins_old")
        conn.execute("""
            CREATE TABLE insumos (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre       TEXT NOT NULL,
                unidad       TEXT NOT NULL DEFAULT 'unidad',
                stock        REAL NOT NULL DEFAULT 0.0,
                stock_minimo REAL NOT NULL DEFAULT 0.0,
                activo       INTEGER NOT NULL DEFAULT 1,
                panaderia_id INTEGER,
                UNIQUE(nombre, panaderia_id)
            )
        """)
        for r in rows:
            conn.execute("""
                INSERT OR IGNORE INTO insumos
                    (id, nombre, unidad, stock, stock_minimo, activo, panaderia_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (r["id"], r["nombre"], r["unidad"], float(r["stock"] or 0),
                  float(r["stock_minimo"] or 0), int(r["activo"] or 1), r["panaderia_id"]))
        conn.execute("DROP TABLE _ins_old")
        logger.info("migración: insumos UNIQUE incluye panaderia_id")

    # ── categorias_producto ────────────────────────────────────────────────────
    if "unique(nombre,panaderia_id)" not in _sn("categorias_producto"):
        rows = conn.execute("""
            SELECT id, nombre, COALESCE(activa, 1) AS activa,
                   COALESCE(panaderia_id, ?) AS panaderia_id
            FROM categorias_producto ORDER BY id
        """, (t_id,)).fetchall()
        conn.execute("ALTER TABLE categorias_producto RENAME TO _cat_old")
        conn.execute("""
            CREATE TABLE categorias_producto (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre       TEXT NOT NULL,
                activa       INTEGER NOT NULL DEFAULT 1,
                panaderia_id INTEGER,
                UNIQUE(nombre, panaderia_id)
            )
        """)
        for r in rows:
            conn.execute("""
                INSERT OR IGNORE INTO categorias_producto (id, nombre, activa, panaderia_id)
                VALUES (?, ?, ?, ?)
            """, (r["id"], r["nombre"], int(r["activa"] or 1), r["panaderia_id"]))
        conn.execute("DROP TABLE _cat_old")
        logger.info("migración: categorias_producto UNIQUE incluye panaderia_id")

    # ── dias_especiales ────────────────────────────────────────────────────────
    if "unique(fecha,panaderia_id)" not in _sn("dias_especiales"):
        rows = conn.execute("""
            SELECT id, fecha, COALESCE(descripcion, '') AS descripcion,
                   COALESCE(factor, 1.0) AS factor,
                   COALESCE(tipo, 'festivo') AS tipo,
                   COALESCE(activo, 1) AS activo,
                   COALESCE(panaderia_id, ?) AS panaderia_id
            FROM dias_especiales ORDER BY id
        """, (t_id,)).fetchall()
        conn.execute("ALTER TABLE dias_especiales RENAME TO _de_old")
        conn.execute("""
            CREATE TABLE dias_especiales (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha        TEXT NOT NULL,
                descripcion  TEXT NOT NULL DEFAULT '',
                factor       REAL NOT NULL DEFAULT 1.0,
                tipo         TEXT NOT NULL DEFAULT 'festivo',
                activo       INTEGER NOT NULL DEFAULT 1,
                panaderia_id INTEGER,
                UNIQUE(fecha, panaderia_id)
            )
        """)
        for r in rows:
            conn.execute("""
                INSERT OR IGNORE INTO dias_especiales
                    (id, fecha, descripcion, factor, tipo, activo, panaderia_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (r["id"], r["fecha"], r["descripcion"], float(r["factor"] or 1.0),
                  r["tipo"], int(r["activo"] or 1), r["panaderia_id"]))
        conn.execute("DROP TABLE _de_old")
        logger.info("migración: dias_especiales UNIQUE incluye panaderia_id")

    # ── recetas ────────────────────────────────────────────────────────────────
    if "unique(producto,insumo_id,panaderia_id)" not in _sn("recetas"):
        _ejecutar_migracion_tolerante(conn, "ALTER TABLE recetas ADD COLUMN panaderia_id INTEGER")
        conn.execute("UPDATE recetas SET panaderia_id = COALESCE(panaderia_id, ?)", (t_id,))
        rows = conn.execute("""
            SELECT id, producto, insumo_id,
                   COALESCE(cantidad, 1.0) AS cantidad,
                   COALESCE(unidad_receta, 'unidad') AS unidad_receta,
                   COALESCE(panaderia_id, ?) AS panaderia_id
            FROM recetas ORDER BY id
        """, (t_id,)).fetchall()
        conn.execute("ALTER TABLE recetas RENAME TO _rec_old")
        conn.execute("""
            CREATE TABLE recetas (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                producto      TEXT NOT NULL,
                insumo_id     INTEGER NOT NULL,
                cantidad      REAL NOT NULL DEFAULT 1.0,
                unidad_receta TEXT NOT NULL DEFAULT 'unidad',
                panaderia_id  INTEGER,
                UNIQUE(producto, insumo_id, panaderia_id),
                FOREIGN KEY (insumo_id) REFERENCES insumos(id)
            )
        """)
        for r in rows:
            conn.execute("""
                INSERT OR IGNORE INTO recetas
                    (id, producto, insumo_id, cantidad, unidad_receta, panaderia_id)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (r["id"], r["producto"], r["insumo_id"], float(r["cantidad"] or 1.0),
                  r["unidad_receta"], r["panaderia_id"]))
        conn.execute("DROP TABLE _rec_old")
        logger.info("migración: recetas UNIQUE incluye panaderia_id")

    # ── receta_fichas ──────────────────────────────────────────────────────────
    if "unique(producto,panaderia_id)" not in _sn("receta_fichas"):
        _ejecutar_migracion_tolerante(conn, "ALTER TABLE receta_fichas ADD COLUMN panaderia_id INTEGER")
        conn.execute("UPDATE receta_fichas SET panaderia_id = COALESCE(panaderia_id, ?)", (t_id,))
        rows = conn.execute("""
            SELECT id, producto, COALESCE(rendimiento_texto, '') AS rendimiento_texto,
                   COALESCE(tiempo_preparacion_min, 0.0) AS tiempo_preparacion_min,
                   COALESCE(tiempo_amasado_min, 0.0) AS tiempo_amasado_min,
                   COALESCE(tiempo_fermentacion_min, 0.0) AS tiempo_fermentacion_min,
                   COALESCE(tiempo_horneado_min, 0.0) AS tiempo_horneado_min,
                   COALESCE(temperatura_horneado, 0.0) AS temperatura_horneado,
                   COALESCE(pasos, '') AS pasos,
                   COALESCE(observaciones, '') AS observaciones,
                   COALESCE(panaderia_id, ?) AS panaderia_id
            FROM receta_fichas ORDER BY id
        """, (t_id,)).fetchall()
        conn.execute("ALTER TABLE receta_fichas RENAME TO _rf_old")
        conn.execute("""
            CREATE TABLE receta_fichas (
                id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                producto                TEXT NOT NULL,
                rendimiento_texto       TEXT DEFAULT '',
                tiempo_preparacion_min  REAL NOT NULL DEFAULT 0.0,
                tiempo_amasado_min      REAL NOT NULL DEFAULT 0.0,
                tiempo_fermentacion_min REAL NOT NULL DEFAULT 0.0,
                tiempo_horneado_min     REAL NOT NULL DEFAULT 0.0,
                temperatura_horneado    REAL NOT NULL DEFAULT 0.0,
                pasos                   TEXT DEFAULT '',
                observaciones           TEXT DEFAULT '',
                panaderia_id            INTEGER,
                UNIQUE(producto, panaderia_id)
            )
        """)
        for r in rows:
            conn.execute("""
                INSERT OR IGNORE INTO receta_fichas
                    (id, producto, rendimiento_texto, tiempo_preparacion_min,
                     tiempo_amasado_min, tiempo_fermentacion_min, tiempo_horneado_min,
                     temperatura_horneado, pasos, observaciones, panaderia_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (r["id"], r["producto"], r["rendimiento_texto"],
                  float(r["tiempo_preparacion_min"] or 0),
                  float(r["tiempo_amasado_min"] or 0),
                  float(r["tiempo_fermentacion_min"] or 0),
                  float(r["tiempo_horneado_min"] or 0),
                  float(r["temperatura_horneado"] or 0),
                  r["pasos"], r["observaciones"], r["panaderia_id"]))
        conn.execute("DROP TABLE _rf_old")
        logger.info("migración: receta_fichas UNIQUE incluye panaderia_id")

    # ── producto_componentes ───────────────────────────────────────────────────
    if "unique(producto,componente_producto,panaderia_id)" not in _sn("producto_componentes"):
        _ejecutar_migracion_tolerante(conn, "ALTER TABLE producto_componentes ADD COLUMN panaderia_id INTEGER")
        conn.execute("UPDATE producto_componentes SET panaderia_id = COALESCE(panaderia_id, ?)", (t_id,))
        rows = conn.execute("""
            SELECT id, producto, componente_producto,
                   COALESCE(cantidad, 1.0) AS cantidad,
                   COALESCE(panaderia_id, ?) AS panaderia_id
            FROM producto_componentes ORDER BY id
        """, (t_id,)).fetchall()
        conn.execute("ALTER TABLE producto_componentes RENAME TO _pc_old")
        conn.execute("""
            CREATE TABLE producto_componentes (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                producto            TEXT NOT NULL,
                componente_producto TEXT NOT NULL,
                cantidad            REAL NOT NULL DEFAULT 1.0,
                panaderia_id        INTEGER,
                UNIQUE(producto, componente_producto, panaderia_id)
            )
        """)
        for r in rows:
            conn.execute("""
                INSERT OR IGNORE INTO producto_componentes
                    (id, producto, componente_producto, cantidad, panaderia_id)
                VALUES (?, ?, ?, ?, ?)
            """, (r["id"], r["producto"], r["componente_producto"],
                  float(r["cantidad"] or 1.0), r["panaderia_id"]))
        conn.execute("DROP TABLE _pc_old")
        logger.info("migración: producto_componentes UNIQUE incluye panaderia_id")

    # ── productos ──────────────────────────────────────────────────────────────
    if "unique(nombre,categoria,panaderia_id)" not in _sn("productos"):
        rows = conn.execute("""
            SELECT id, nombre, COALESCE(precio, 0.0) AS precio,
                   COALESCE(categoria, 'Panaderia') AS categoria,
                   COALESCE(menu, '') AS menu,
                   COALESCE(descripcion, '') AS descripcion,
                   COALESCE(es_panaderia, 0) AS es_panaderia,
                   COALESCE(activo, 1) AS activo,
                   COALESCE(es_adicional, 0) AS es_adicional,
                   COALESCE(stock_minimo, 0) AS stock_minimo,
                   COALESCE(panaderia_id, ?) AS panaderia_id
            FROM productos ORDER BY id
        """, (t_id,)).fetchall()
        conn.execute("ALTER TABLE productos RENAME TO _prod_old")
        conn.execute("""
            CREATE TABLE productos (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre       TEXT NOT NULL,
                precio       REAL NOT NULL DEFAULT 0.0,
                categoria    TEXT NOT NULL DEFAULT 'Panaderia',
                menu         TEXT NOT NULL DEFAULT '',
                descripcion  TEXT NOT NULL DEFAULT '',
                es_panaderia INTEGER NOT NULL DEFAULT 0,
                activo       INTEGER NOT NULL DEFAULT 1,
                es_adicional INTEGER NOT NULL DEFAULT 0,
                stock_minimo INTEGER NOT NULL DEFAULT 0,
                panaderia_id INTEGER,
                UNIQUE(nombre, categoria, panaderia_id)
            )
        """)
        for r in rows:
            conn.execute("""
                INSERT OR IGNORE INTO productos
                    (id, nombre, precio, categoria, menu, descripcion,
                     es_panaderia, activo, es_adicional, stock_minimo, panaderia_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (r["id"], r["nombre"], float(r["precio"] or 0), r["categoria"],
                  r["menu"], r["descripcion"], int(r["es_panaderia"] or 0),
                  int(r["activo"] or 1), int(r["es_adicional"] or 0),
                  int(r["stock_minimo"] or 0), r["panaderia_id"]))
        conn.execute("DROP TABLE _prod_old")
        logger.info("migración: productos UNIQUE incluye panaderia_id")

    # Rehabilitar FKs tras la reconstrucción
    conn.execute("PRAGMA foreign_keys = ON")

    # ── Índices de tenant para consultas por panaderia/sede ───────────────────
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_registros_tenant ON registros_diarios(panaderia_id, sede_id, fecha)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_ajustes_tenant ON ajustes_pronostico(panaderia_id, sede_id, fecha)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_mesas_tenant ON mesas(panaderia_id, sede_id)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_adicionales_tenant ON adicionales(panaderia_id)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_insumos_tenant ON insumos(panaderia_id)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_categorias_tenant ON categorias_producto(panaderia_id)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_dias_especiales_tenant ON dias_especiales(panaderia_id)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_recetas_tenant ON recetas(panaderia_id)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_receta_fichas_tenant ON receta_fichas(panaderia_id)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_prod_comp_tenant ON producto_componentes(panaderia_id)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_productos_tenant ON productos(panaderia_id)")


def _reparar_foreign_keys_tablas_temporales(conn) -> None:
    """Repara FKs que SQLite pudo dejar apuntando a tablas temporales renombradas."""
    if DB_TYPE != "sqlite":
        return

    def _sql_tabla(nombre: str) -> str:
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name = ?",
            (nombre,),
        ).fetchone()
        return str((row["sql"] if row else "") or "")

    reparaciones = []

    def _rebuild_table(nombre: str, create_sql: str, select_sql: str, insert_sql: str, values_builder) -> None:
        temp = f"__fix_{nombre}"
        rows = conn.execute(select_sql).fetchall()
        conn.execute(f"DROP TABLE IF EXISTS {temp}")
        create_sql_rendered = create_sql.format(table=temp)
        if create_sql_rendered == create_sql:
            create_sql_rendered = create_sql.replace(f"CREATE TABLE {nombre}", f"CREATE TABLE {temp}")
        conn.execute(create_sql_rendered)
        for row in rows:
            conn.execute(insert_sql.format(table=temp), values_builder(row))
        conn.execute(f"DROP TABLE {nombre}")
        conn.execute(f"ALTER TABLE {temp} RENAME TO {nombre}")
        reparaciones.append(nombre)

    try:
        conn.commit()
        conn.execute("PRAGMA foreign_keys = OFF")

        if '_mesas_old' in _sql_tabla("pedidos"):
            _rebuild_table(
                "pedidos",
                """
                CREATE TABLE pedidos (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    mesa_id        INTEGER,
                    mesero         TEXT NOT NULL DEFAULT '',
                    estado         TEXT NOT NULL DEFAULT 'pendiente'
                                   CHECK(estado IN ('pendiente','en_preparacion','listo','pagado','cancelado')),
                    fecha          TEXT NOT NULL,
                    hora           TEXT NOT NULL,
                    hora_pagado    TEXT DEFAULT NULL,
                    notas          TEXT DEFAULT '',
                    total          REAL NOT NULL DEFAULT 0.0,
                    creado_en      TEXT,
                    pagado_en      TEXT,
                    pagado_por     TEXT DEFAULT '',
                    metodo_pago    TEXT DEFAULT '',
                    monto_recibido REAL NOT NULL DEFAULT 0.0,
                    cambio         REAL NOT NULL DEFAULT 0.0,
                    metodo_pago_2  TEXT DEFAULT NULL,
                    monto_pago_2   REAL DEFAULT NULL,
                    unificado_en   INTEGER DEFAULT NULL,
                    panaderia_id   INTEGER,
                    sede_id        INTEGER,
                    FOREIGN KEY (mesa_id) REFERENCES mesas(id)
                )
                """,
                """
                SELECT id, mesa_id, mesero, estado, fecha, hora, hora_pagado, notas, total,
                       COALESCE(creado_en, '') AS creado_en,
                       COALESCE(pagado_en, '') AS pagado_en,
                       COALESCE(pagado_por, '') AS pagado_por,
                       COALESCE(metodo_pago, '') AS metodo_pago,
                       COALESCE(monto_recibido, 0.0) AS monto_recibido,
                       COALESCE(cambio, 0.0) AS cambio,
                       metodo_pago_2, monto_pago_2, unificado_en, panaderia_id, sede_id
                FROM pedidos
                ORDER BY id
                """,
                """
                INSERT INTO {table} (
                    id, mesa_id, mesero, estado, fecha, hora, hora_pagado, notas, total,
                    creado_en, pagado_en, pagado_por, metodo_pago, monto_recibido, cambio,
                    metodo_pago_2, monto_pago_2, unificado_en, panaderia_id, sede_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                lambda row: (
                    row["id"], row["mesa_id"], row["mesero"], row["estado"], row["fecha"], row["hora"],
                    row["hora_pagado"], row["notas"], row["total"], row["creado_en"], row["pagado_en"],
                    row["pagado_por"], row["metodo_pago"], row["monto_recibido"], row["cambio"],
                    row["metodo_pago_2"], row["monto_pago_2"], row["unificado_en"],
                    row["panaderia_id"], row["sede_id"],
                ),
            )

        if '_pedidos_fk_old' in _sql_tabla("pedido_estado_historial"):
            _rebuild_table(
                "pedido_estado_historial",
                """
                CREATE TABLE pedido_estado_historial (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    pedido_id    INTEGER NOT NULL,
                    estado       TEXT NOT NULL,
                    cambiado_en  TEXT NOT NULL,
                    cambiado_por TEXT NOT NULL DEFAULT '',
                    detalle      TEXT DEFAULT '',
                    panaderia_id INTEGER,
                    sede_id      INTEGER,
                    FOREIGN KEY (pedido_id) REFERENCES pedidos(id) ON DELETE CASCADE
                )
                """,
                """
                SELECT id, pedido_id, estado, cambiado_en, cambiado_por, detalle, panaderia_id, sede_id
                FROM pedido_estado_historial
                ORDER BY id
                """,
                """
                INSERT INTO {table} (
                    id, pedido_id, estado, cambiado_en, cambiado_por, detalle, panaderia_id, sede_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                lambda row: (
                    row["id"], row["pedido_id"], row["estado"], row["cambiado_en"],
                    row["cambiado_por"], row["detalle"], row["panaderia_id"], row["sede_id"],
                ),
            )

        if '_pedidos_fk_old' in _sql_tabla("pedido_items"):
            _rebuild_table(
                "pedido_items",
                """
                CREATE TABLE pedido_items (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    pedido_id      INTEGER NOT NULL,
                    producto       TEXT NOT NULL,
                    cantidad       INTEGER NOT NULL DEFAULT 1,
                    precio_unitario REAL NOT NULL DEFAULT 0.0,
                    subtotal       REAL NOT NULL DEFAULT 0.0,
                    notas          TEXT DEFAULT '',
                    producto_id    INTEGER,
                    panaderia_id   INTEGER,
                    sede_id        INTEGER,
                    FOREIGN KEY (pedido_id) REFERENCES pedidos(id) ON DELETE CASCADE
                )
                """,
                """
                SELECT id, pedido_id, producto, cantidad, precio_unitario, subtotal, notas,
                       producto_id, panaderia_id, sede_id
                FROM pedido_items
                ORDER BY id
                """,
                """
                INSERT INTO {table} (
                    id, pedido_id, producto, cantidad, precio_unitario, subtotal,
                    notas, producto_id, panaderia_id, sede_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                lambda row: (
                    row["id"], row["pedido_id"], row["producto"], row["cantidad"],
                    row["precio_unitario"], row["subtotal"], row["notas"], row["producto_id"],
                    row["panaderia_id"], row["sede_id"],
                ),
            )

        if '_pedido_items_fk_old' in _sql_tabla("pedido_item_modificaciones"):
            _rebuild_table(
                "pedido_item_modificaciones",
                """
                CREATE TABLE {table} (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    pedido_item_id  INTEGER NOT NULL,
                    tipo            TEXT NOT NULL CHECK(tipo IN ('adicional', 'exclusion')),
                    descripcion     TEXT NOT NULL,
                    cantidad        INTEGER NOT NULL DEFAULT 1,
                    precio_extra    REAL NOT NULL DEFAULT 0.0,
                    FOREIGN KEY (pedido_item_id) REFERENCES pedido_items(id) ON DELETE CASCADE
                )
                """,
                """
                SELECT id, pedido_item_id, tipo, descripcion, cantidad, precio_extra
                FROM pedido_item_modificaciones
                ORDER BY id
                """,
                """
                INSERT INTO {table} (
                    id, pedido_item_id, tipo, descripcion, cantidad, precio_extra
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                lambda row: (
                    row["id"], row["pedido_item_id"], row["tipo"], row["descripcion"],
                    row["cantidad"], row["precio_extra"],
                ),
            )

        sql_comandas = _sql_tabla("comandas")
        if '_pedidos_fk_old' in sql_comandas or '_mesas_old' in sql_comandas:
            _rebuild_table(
                "comandas",
                """
                CREATE TABLE comandas (
                    id                         INTEGER PRIMARY KEY AUTOINCREMENT,
                    pedido_id                  INTEGER NOT NULL,
                    panaderia_id               INTEGER,
                    sede_id                    INTEGER,
                    mesa_id                    INTEGER,
                    creada_por_usuario_id      INTEGER,
                    creada_por_nombre_snapshot TEXT NOT NULL DEFAULT '',
                    estado                     TEXT NOT NULL DEFAULT 'generada'
                                               CHECK(estado IN ('generada','impresa','reimpresa','cancelada')),
                    es_incremental             INTEGER NOT NULL DEFAULT 0,
                    comanda_origen_id          INTEGER,
                    nota_general               TEXT DEFAULT '',
                    created_at                 TEXT NOT NULL,
                    updated_at                 TEXT NOT NULL,
                    FOREIGN KEY (pedido_id) REFERENCES pedidos(id) ON DELETE CASCADE,
                    FOREIGN KEY (mesa_id) REFERENCES mesas(id),
                    FOREIGN KEY (comanda_origen_id) REFERENCES comandas(id)
                )
                """,
                """
                SELECT id, pedido_id, panaderia_id, sede_id, mesa_id, creada_por_usuario_id,
                       creada_por_nombre_snapshot, estado, es_incremental, comanda_origen_id,
                       nota_general, created_at, updated_at
                FROM comandas
                ORDER BY id
                """,
                """
                INSERT INTO {table} (
                    id, pedido_id, panaderia_id, sede_id, mesa_id, creada_por_usuario_id,
                    creada_por_nombre_snapshot, estado, es_incremental, comanda_origen_id,
                    nota_general, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                lambda row: (
                    row["id"], row["pedido_id"], row["panaderia_id"], row["sede_id"], row["mesa_id"],
                    row["creada_por_usuario_id"], row["creada_por_nombre_snapshot"], row["estado"],
                    row["es_incremental"], row["comanda_origen_id"], row["nota_general"],
                    row["created_at"], row["updated_at"],
                ),
            )

        sql_comanda_items = _sql_tabla("comanda_items")
        if '_pedido_items_fk_old' in sql_comanda_items or '_comandas_fk_old' in sql_comanda_items:
            _rebuild_table(
                "comanda_items",
                """
                CREATE TABLE comanda_items (
                    id                        INTEGER PRIMARY KEY AUTOINCREMENT,
                    comanda_id                INTEGER NOT NULL,
                    pedido_item_id            INTEGER,
                    producto_nombre_snapshot  TEXT NOT NULL DEFAULT '',
                    cantidad                  INTEGER NOT NULL DEFAULT 1,
                    observacion               TEXT DEFAULT '',
                    modificadores_json        TEXT DEFAULT '',
                    created_at                TEXT NOT NULL,
                    FOREIGN KEY (comanda_id) REFERENCES comandas(id) ON DELETE CASCADE,
                    FOREIGN KEY (pedido_item_id) REFERENCES pedido_items(id) ON DELETE SET NULL
                )
                """,
                """
                SELECT id, comanda_id, pedido_item_id, producto_nombre_snapshot, cantidad,
                       observacion, modificadores_json, created_at
                FROM comanda_items
                ORDER BY id
                """,
                """
                INSERT INTO {table} (
                    id, comanda_id, pedido_item_id, producto_nombre_snapshot, cantidad,
                    observacion, modificadores_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                lambda row: (
                    row["id"], row["comanda_id"], row["pedido_item_id"], row["producto_nombre_snapshot"],
                    row["cantidad"], row["observacion"], row["modificadores_json"], row["created_at"],
                ),
            )

        if '_ins_old' in _sql_tabla("inventario_sede"):
            _rebuild_table(
                "inventario_sede",
                """
                CREATE TABLE inventario_sede (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    sede_id      INTEGER NOT NULL,
                    insumo_id    INTEGER NOT NULL,
                    stock        REAL NOT NULL DEFAULT 0.0,
                    stock_minimo REAL NOT NULL DEFAULT 0.0,
                    updated_at   TEXT NOT NULL,
                    UNIQUE(sede_id, insumo_id),
                    FOREIGN KEY (sede_id) REFERENCES sedes(id),
                    FOREIGN KEY (insumo_id) REFERENCES insumos(id)
                )
                """,
                """
                SELECT id, sede_id, insumo_id, stock, stock_minimo, updated_at
                FROM inventario_sede
                ORDER BY id
                """,
                """
                INSERT INTO {table} (id, sede_id, insumo_id, stock, stock_minimo, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                lambda row: (
                    row["id"], row["sede_id"], row["insumo_id"], row["stock"],
                    row["stock_minimo"], row["updated_at"],
                ),
            )

        sql_adicional_insumos = _sql_tabla("adicional_insumos")
        if '_ins_old' in sql_adicional_insumos or '_adic_old' in sql_adicional_insumos:
            _rebuild_table(
                "adicional_insumos",
                """
                CREATE TABLE adicional_insumos (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    adicional_id  INTEGER NOT NULL,
                    insumo_id     INTEGER NOT NULL,
                    cantidad      REAL NOT NULL DEFAULT 1.0,
                    unidad_config TEXT NOT NULL DEFAULT 'unidad',
                    UNIQUE(adicional_id, insumo_id),
                    FOREIGN KEY (adicional_id) REFERENCES adicionales(id),
                    FOREIGN KEY (insumo_id) REFERENCES insumos(id)
                )
                """,
                """
                SELECT id, adicional_id, insumo_id, cantidad, COALESCE(unidad_config, 'unidad') AS unidad_config
                FROM adicional_insumos
                ORDER BY id
                """,
                """
                INSERT INTO {table} (id, adicional_id, insumo_id, cantidad, unidad_config)
                VALUES (?, ?, ?, ?, ?)
                """,
                lambda row: (
                    row["id"], row["adicional_id"], row["insumo_id"], row["cantidad"], row["unidad_config"],
                ),
            )

        if '_adic_old' in _sql_tabla("adicional_componentes"):
            _rebuild_table(
                "adicional_componentes",
                """
                CREATE TABLE adicional_componentes (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    adicional_id        INTEGER NOT NULL,
                    componente_producto TEXT NOT NULL,
                    cantidad            REAL NOT NULL DEFAULT 1.0,
                    UNIQUE(adicional_id, componente_producto),
                    FOREIGN KEY (adicional_id) REFERENCES adicionales(id)
                )
                """,
                """
                SELECT id, adicional_id, componente_producto, cantidad
                FROM adicional_componentes
                ORDER BY id
                """,
                """
                INSERT INTO {table} (id, adicional_id, componente_producto, cantidad)
                VALUES (?, ?, ?, ?)
                """,
                lambda row: (
                    row["id"], row["adicional_id"], row["componente_producto"], row["cantidad"],
                ),
            )
        conn.commit()
    finally:
        conn.execute("PRAGMA foreign_keys = ON")

    if reparaciones:
        logger.warning("Se repararon FKs apuntando a tablas temporales: %s", ", ".join(reparaciones))


def _migrar_fase1(conn) -> None:
    """Fase 1: formalizar el tenant como organización."""
    # panaderias: estado_operativo y created_by
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE panaderias ADD COLUMN estado_operativo TEXT NOT NULL DEFAULT 'activa'")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE panaderias ADD COLUMN created_by TEXT NOT NULL DEFAULT ''")
    # Backfill: panaderías existentes quedan en estado activa
    conn.execute("UPDATE panaderias SET estado_operativo = 'activa' WHERE estado_operativo = '' OR estado_operativo IS NULL")
    # sedes: índice único (panaderia_id, codigo) solo para códigos no vacíos
    _ejecutar_migracion_tolerante(conn, "CREATE UNIQUE INDEX IF NOT EXISTS idx_sedes_codigo ON sedes(panaderia_id, codigo) WHERE codigo != ''")


def _migrar_fase2(conn) -> None:
    """Fase 2: separar identidad de membresía en tenant_memberships."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Crear tabla si fue creada antes del esquema base (DB nueva la tiene; DB existente no)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tenant_memberships (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario_id   INTEGER NOT NULL,
            panaderia_id INTEGER NOT NULL,
            sede_id      INTEGER NOT NULL,
            rol          TEXT NOT NULL,
            activa       INTEGER NOT NULL DEFAULT 1,
            invited_by   INTEGER,
            created_at   TEXT NOT NULL,
            UNIQUE(usuario_id, panaderia_id, sede_id),
            FOREIGN KEY (usuario_id)   REFERENCES usuarios(id)   ON DELETE CASCADE,
            FOREIGN KEY (panaderia_id) REFERENCES panaderias(id),
            FOREIGN KEY (sede_id)      REFERENCES sedes(id)
        )
    """)
    # Seed: una membresía por usuario que ya tenga panaderia_id asignado
    conn.execute(
        """
        INSERT OR IGNORE INTO tenant_memberships
            (usuario_id, panaderia_id, sede_id, rol, activa, created_at)
        SELECT
            u.id,
            u.panaderia_id,
            COALESCE(
                u.sede_id,
                (SELECT MIN(s.id) FROM sedes s WHERE s.panaderia_id = u.panaderia_id)
            ),
            u.rol,
            u.activo,
            ?
        FROM usuarios u
        WHERE u.panaderia_id IS NOT NULL
          AND u.rol NOT IN ('platform_superadmin')
        """,
        (now,),
    )
    # Índices
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_tm_usuario ON tenant_memberships(usuario_id, activa)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_tm_panaderia ON tenant_memberships(panaderia_id, activa)")


def _migrar_fase3(conn) -> None:
    """Fase 3: membresía comercial — tabla tenant_subscriptions."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    today = datetime.now().strftime("%Y-%m-%d")
    # Crear tabla si no existe (DB antigua no la tiene)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tenant_subscriptions (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            panaderia_id      INTEGER NOT NULL UNIQUE,
            plan              TEXT NOT NULL DEFAULT 'free'
                              CHECK(plan IN ('free','starter','pro','enterprise')),
            estado            TEXT NOT NULL DEFAULT 'activa'
                              CHECK(estado IN ('activa','trial','vencida','cancelada','suspendida')),
            fecha_inicio      TEXT NOT NULL,
            fecha_vencimiento TEXT,
            max_sedes         INTEGER NOT NULL DEFAULT 1,
            max_usuarios      INTEGER NOT NULL DEFAULT 5,
            max_productos     INTEGER NOT NULL DEFAULT 50,
            notas             TEXT NOT NULL DEFAULT '',
            created_at        TEXT NOT NULL,
            updated_at        TEXT NOT NULL,
            FOREIGN KEY (panaderia_id) REFERENCES panaderias(id)
        )
    """)
    # Seed: un registro 'free' por panadería existente que no tenga suscripción
    limites = PLAN_LIMITS["free"]
    conn.execute(
        """
        INSERT OR IGNORE INTO tenant_subscriptions
            (panaderia_id, plan, estado, fecha_inicio, fecha_vencimiento,
             max_sedes, max_usuarios, max_productos, created_at, updated_at)
        SELECT p.id, 'free', 'activa', ?, NULL,
               ?, ?, ?,
               ?, ?
        FROM panaderias p
        WHERE p.activa = 1
        """,
        (today,
         limites["max_sedes"], limites["max_usuarios"], limites["max_productos"],
         now, now),
    )
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_ts_panaderia ON tenant_subscriptions(panaderia_id)")


def _migrar_fase5(conn) -> None:
    """Fase 5: terminales confiables por sede."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS terminales (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            panaderia_id INTEGER NOT NULL,
            sede_id      INTEGER NOT NULL,
            nombre       TEXT NOT NULL,
            codigo       TEXT NOT NULL,
            tipo         TEXT NOT NULL DEFAULT 'caja'
                         CHECK(tipo IN ('caja','mesero','kiosko','cocina')),
            activa       INTEGER NOT NULL DEFAULT 1,
            last_seen_at TEXT NOT NULL DEFAULT '',
            created_at   TEXT NOT NULL,
            updated_at   TEXT NOT NULL,
            UNIQUE(sede_id, codigo),
            FOREIGN KEY (panaderia_id) REFERENCES panaderias(id),
            FOREIGN KEY (sede_id)      REFERENCES sedes(id)
        )
    """)
    # Seed: un terminal 'CAJA-01' por sede activa existente
    conn.execute(
        """
        INSERT OR IGNORE INTO terminales
            (panaderia_id, sede_id, nombre, codigo, tipo, activa, created_at, updated_at)
        SELECT s.panaderia_id, s.id, 'Caja Principal', 'CAJA-01', 'caja', 1, ?, ?
        FROM sedes s
        WHERE s.activa = 1
        """,
        (now, now),
    )
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_terminales_sede ON terminales(sede_id, activa)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_terminales_codigo ON terminales(sede_id, codigo)")


def _migrar_fase6(conn) -> None:
    """Fase 6: session_version para invalidación server-side de sesiones."""
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE usuarios ADD COLUMN session_version INTEGER NOT NULL DEFAULT 0")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_usuarios_session_version ON usuarios(id, session_version)")


def _migrar_fase7(conn) -> None:
    """Fase 7: modelo transaccional POS — venta_headers / venta_items / venta_pagos."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ── venta_headers ─────────────────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS venta_headers (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha            TEXT NOT NULL,
            hora             TEXT NOT NULL,
            creado_en        TEXT NOT NULL,
            updated_at       TEXT NOT NULL,
            cajero           TEXT NOT NULL DEFAULT '',
            cajero_id        INTEGER,
            sede_id          INTEGER,
            panaderia_id     INTEGER,
            terminal_id      INTEGER,
            tipo_venta       TEXT NOT NULL DEFAULT 'rapida'
                             CHECK(tipo_venta IN ('rapida','con_documento')),
            estado           TEXT NOT NULL DEFAULT 'activa'
                             CHECK(estado IN ('activa','suspendida','pagada','anulada')),
            subtotal         REAL NOT NULL DEFAULT 0.0,
            descuento        REAL NOT NULL DEFAULT 0.0,
            total            REAL NOT NULL DEFAULT 0.0,
            estado_pago      TEXT NOT NULL DEFAULT 'pendiente'
                             CHECK(estado_pago IN ('pendiente','parcial','pagado','credito')),
            monto_pagado     REAL NOT NULL DEFAULT 0.0,
            saldo_pendiente  REAL NOT NULL DEFAULT 0.0,
            nombre_comprador TEXT NOT NULL DEFAULT '',
            tipo_doc         TEXT NOT NULL DEFAULT '',
            numero_doc       TEXT NOT NULL DEFAULT '',
            email_comprador  TEXT NOT NULL DEFAULT '',
            anulada_en       TEXT NOT NULL DEFAULT '',
            anulada_por      TEXT NOT NULL DEFAULT '',
            motivo_anulacion TEXT NOT NULL DEFAULT '',
            nota_suspension  TEXT NOT NULL DEFAULT '',
            suspendida_en    TEXT NOT NULL DEFAULT '',
            encargo_id       INTEGER,
            venta_grupo      TEXT NOT NULL DEFAULT '',
            FOREIGN KEY (sede_id)      REFERENCES sedes(id),
            FOREIGN KEY (panaderia_id) REFERENCES panaderias(id),
            FOREIGN KEY (terminal_id)  REFERENCES terminales(id)
        )
    """)

    # ── venta_items ───────────────────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS venta_items (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            venta_id        INTEGER NOT NULL,
            producto_id     INTEGER,
            producto        TEXT NOT NULL,
            cantidad        INTEGER NOT NULL DEFAULT 1,
            precio_base     REAL NOT NULL DEFAULT 0.0,
            precio_aplicado REAL NOT NULL DEFAULT 0.0,
            subtotal        REAL NOT NULL DEFAULT 0.0,
            motivo_precio   TEXT NOT NULL DEFAULT '',
            autorizado_por  TEXT NOT NULL DEFAULT '',
            notas           TEXT NOT NULL DEFAULT '',
            FOREIGN KEY (venta_id) REFERENCES venta_headers(id) ON DELETE CASCADE
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS venta_item_modificaciones (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            venta_item_id   INTEGER NOT NULL,
            tipo            TEXT NOT NULL CHECK(tipo IN ('adicional', 'exclusion')),
            descripcion     TEXT NOT NULL,
            cantidad        INTEGER NOT NULL DEFAULT 1,
            precio_extra    REAL NOT NULL DEFAULT 0.0,
            FOREIGN KEY (venta_item_id) REFERENCES venta_items(id) ON DELETE CASCADE
        )
    """)

    # ── venta_pagos ───────────────────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS venta_pagos (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            venta_id        INTEGER NOT NULL,
            metodo          TEXT NOT NULL DEFAULT 'efectivo'
                            CHECK(metodo IN ('efectivo','transferencia','tarjeta','credito')),
            monto           REAL NOT NULL DEFAULT 0.0,
            referencia      TEXT NOT NULL DEFAULT '',
            recibido        REAL NOT NULL DEFAULT 0.0,
            cambio          REAL NOT NULL DEFAULT 0.0,
            registrado_en   TEXT NOT NULL,
            registrado_por  TEXT NOT NULL DEFAULT '',
            FOREIGN KEY (venta_id) REFERENCES venta_headers(id) ON DELETE CASCADE
        )
    """)

    # ── Índices ───────────────────────────────────────────────────────────────
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_vh_fecha       ON venta_headers(fecha, panaderia_id, sede_id)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_vh_estado      ON venta_headers(estado, sede_id)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_vh_cajero      ON venta_headers(cajero_id, fecha)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_vi_venta       ON venta_items(venta_id)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_vim_venta_item ON venta_item_modificaciones(venta_item_id)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_vp_venta       ON venta_pagos(venta_id)")


# ──────────────────────────────────────────────────────────────────────────────
# Fase 8 — Clientes maestros + Encargos extendidos + Encargo_pagos
# ──────────────────────────────────────────────────────────────────────────────

_ESTADOS_ENCARGO_VALIDOS = (
    "cotizacion", "confirmado", "con_anticipo",
    "programado", "listo", "entregado", "cancelado",
)

_ORIGENES_CXC_VALIDOS = {"venta", "pedido", "encargo"}
_ESTADOS_CXC_VALIDOS = {"abierta", "parcial", "pagada", "vencida", "cancelada"}


def _migrar_fase8(conn) -> None:
    """Fase 8: tabla clientes, encargos con estados extendidos y encargo_pagos."""

    # ── clientes ──────────────────────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS clientes (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre       TEXT NOT NULL,
            telefono     TEXT NOT NULL DEFAULT '',
            email        TEXT NOT NULL DEFAULT '',
            tipo_doc     TEXT NOT NULL DEFAULT '',
            numero_doc   TEXT NOT NULL DEFAULT '',
            empresa      TEXT NOT NULL DEFAULT '',
            direccion    TEXT NOT NULL DEFAULT '',
            notas        TEXT NOT NULL DEFAULT '',
            panaderia_id INTEGER,
            sede_id      INTEGER,
            creado_en    TEXT NOT NULL,
            activo       INTEGER NOT NULL DEFAULT 1
        )
    """)
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_clientes_panaderia ON clientes(panaderia_id, activo)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_clientes_doc ON clientes(tipo_doc, numero_doc)")

    # ── encargos: recrear si el estado no admite aún 'cotizacion' ─────────────
    if DB_TYPE == "sqlite":
        row = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='encargos'").fetchone()
    else:
        row = None

    if row and "cotizacion" not in (row["sql"] or ""):
        conn.execute("""
            CREATE TABLE IF NOT EXISTS encargos_r3 (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha_entrega        TEXT NOT NULL,
                hora_entrega         TEXT NOT NULL DEFAULT '',
                cliente              TEXT NOT NULL DEFAULT '',
                cliente_id           INTEGER,
                empresa              TEXT NOT NULL DEFAULT '',
                telefono             TEXT NOT NULL DEFAULT '',
                notas                TEXT NOT NULL DEFAULT '',
                estado               TEXT NOT NULL DEFAULT 'confirmado'
                                     CHECK(estado IN ('cotizacion','confirmado','con_anticipo',
                                                      'programado','listo','entregado','cancelado')),
                registrado_por       TEXT NOT NULL DEFAULT '',
                creado_en            TEXT NOT NULL,
                total                REAL NOT NULL DEFAULT 0.0,
                anticipo             REAL NOT NULL DEFAULT 0.0,
                saldo_pendiente      REAL NOT NULL DEFAULT 0.0,
                canal_venta          TEXT NOT NULL DEFAULT 'tienda',
                tipo_encargo         TEXT NOT NULL DEFAULT 'orden',
                direccion_entrega    TEXT NOT NULL DEFAULT '',
                recordatorio_enviado INTEGER NOT NULL DEFAULT 0,
                panaderia_id         INTEGER,
                sede_id              INTEGER,
                FOREIGN KEY (cliente_id) REFERENCES clientes(id)
            )
        """)
        conn.execute("""
            INSERT INTO encargos_r3 (
                id, fecha_entrega, hora_entrega, cliente, cliente_id, empresa, telefono,
                notas, estado, registrado_por, creado_en, total, anticipo, saldo_pendiente,
                canal_venta, tipo_encargo, direccion_entrega, recordatorio_enviado,
                panaderia_id, sede_id
            )
            SELECT
                id, fecha_entrega, '', cliente, NULL,
                COALESCE(empresa, ''), '',
                COALESCE(notas, ''),
                CASE estado
                    WHEN 'pendiente'  THEN 'confirmado'
                    WHEN 'listo'      THEN 'listo'
                    WHEN 'entregado'  THEN 'entregado'
                    WHEN 'cancelado'  THEN 'cancelado'
                    ELSE 'confirmado'
                END,
                COALESCE(registrado_por, ''), creado_en, COALESCE(total, 0),
                0, COALESCE(total, 0),
                'tienda', 'orden', '', 0,
                panaderia_id, sede_id
            FROM encargos
        """)
        conn.execute("DROP TABLE encargos")
        conn.execute("ALTER TABLE encargos_r3 RENAME TO encargos")
    elif DB_TYPE == "postgresql":
        _migrar_estado_encargos_postgres(conn)

    # Columnas nuevas en encargos (si se creó sin ellas por otra ruta)
    for stmt in [
        "ALTER TABLE encargos ADD COLUMN hora_entrega TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE encargos ADD COLUMN cliente_id INTEGER",
        "ALTER TABLE encargos ADD COLUMN telefono TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE encargos ADD COLUMN anticipo REAL NOT NULL DEFAULT 0.0",
        "ALTER TABLE encargos ADD COLUMN saldo_pendiente REAL NOT NULL DEFAULT 0.0",
        "ALTER TABLE encargos ADD COLUMN canal_venta TEXT NOT NULL DEFAULT 'tienda'",
        "ALTER TABLE encargos ADD COLUMN tipo_encargo TEXT NOT NULL DEFAULT 'orden'",
        "ALTER TABLE encargos ADD COLUMN direccion_entrega TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE encargos ADD COLUMN recordatorio_enviado INTEGER NOT NULL DEFAULT 0",
    ]:
        _ejecutar_migracion_tolerante(conn, stmt)

    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_encargos_estado ON encargos(estado, panaderia_id)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_encargos_fecha ON encargos(fecha_entrega, panaderia_id)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_encargos_cliente ON encargos(cliente_id)")

    # ── encargo_pagos ─────────────────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS encargo_pagos (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            encargo_id     INTEGER NOT NULL,
            metodo         TEXT NOT NULL DEFAULT 'efectivo',
            monto          REAL NOT NULL DEFAULT 0.0,
            referencia     TEXT NOT NULL DEFAULT '',
            notas          TEXT NOT NULL DEFAULT '',
            registrado_por TEXT NOT NULL DEFAULT '',
            registrado_en  TEXT NOT NULL,
            FOREIGN KEY (encargo_id) REFERENCES encargos(id) ON DELETE CASCADE
        )
    """)
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_encargo_pagos_encargo ON encargo_pagos(encargo_id)")


def _migrar_fase9(conn) -> None:
    """Fase 9: digest determinístico para lookup de PIN operativo."""
    _ejecutar_migracion_tolerante(
        conn,
        "ALTER TABLE usuarios ADD COLUMN pin_lookup_digest TEXT NOT NULL DEFAULT ''",
    )
    rows = conn.execute(
        "SELECT id, pin, pin_hash, pin_lookup_digest FROM usuarios"
    ).fetchall()
    for row in rows:
        digest_actual = str(row["pin_lookup_digest"] or "").strip()
        if digest_actual:
            continue
        digest = _pin_lookup_digest_desde_fila(row["pin_hash"], row["pin"])
        if not digest:
            continue
        conn.execute(
            "UPDATE usuarios SET pin_lookup_digest = ? WHERE id = ?",
            (digest, row["id"]),
        )
    _ejecutar_migracion_tolerante(
        conn,
        "CREATE INDEX IF NOT EXISTS idx_usuarios_pin_lookup_digest "
        "ON usuarios(pin_lookup_digest, panaderia_id, sede_id, activo)",
    )


# ──────────────────────────────────────────────
# Clientes maestros
# ──────────────────────────────────────────────

def crear_cliente(nombre: str, telefono: str = "", email: str = "",
                  tipo_doc: str = "", numero_doc: str = "", empresa: str = "",
                  direccion: str = "", notas: str = "") -> dict:
    if not nombre.strip():
        return {"ok": False, "error": "El nombre es obligatorio"}
    panaderia_id, sede_id = _tenant_scope()
    creado_en = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with get_connection() as conn:
            cur = conn.execute("""
                INSERT INTO clientes (nombre, telefono, email, tipo_doc, numero_doc,
                    empresa, direccion, notas, panaderia_id, sede_id, creado_en, activo)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """, (nombre.strip(), telefono.strip(), email.strip(),
                  tipo_doc.strip(), numero_doc.strip(), empresa.strip(),
                  direccion.strip(), notas.strip(), panaderia_id, sede_id, creado_en))
            conn.commit()
        return {"ok": True, "cliente_id": cur.lastrowid}
    except Exception as e:
        logger.error(f"crear_cliente: {e}")
        return {"ok": False, "error": str(e)}


def obtener_clientes(busqueda: str = "", solo_activos: bool = True) -> list[dict]:
    panaderia_id, sede_id = _tenant_scope()
    filtros = []
    params: list = []
    if solo_activos:
        filtros.append("activo = 1")
    if panaderia_id is not None:
        filtros.append("panaderia_id = ?")
        params.append(panaderia_id)
    if busqueda:
        filtros.append("(nombre LIKE ? OR telefono LIKE ? OR numero_doc LIKE ? OR empresa LIKE ?)")
        b = f"%{busqueda}%"
        params.extend([b, b, b, b])
    where = f"WHERE {' AND '.join(filtros)}" if filtros else ""
    with get_connection() as conn:
        rows = conn.execute(
            f"SELECT * FROM clientes {where} ORDER BY nombre LIMIT 100",
            tuple(params)
        ).fetchall()
    return [dict(r) for r in rows]


def obtener_cliente(cliente_id: int) -> dict | None:
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM clientes WHERE id = ?", (cliente_id,)).fetchone()
    return _row_to_dict(row) or None


def actualizar_cliente(cliente_id: int, nombre: str, telefono: str = "",
                       email: str = "", tipo_doc: str = "", numero_doc: str = "",
                       empresa: str = "", direccion: str = "", notas: str = "") -> dict:
    if not nombre.strip():
        return {"ok": False, "error": "El nombre es obligatorio"}
    try:
        with get_connection() as conn:
            affected = conn.execute("""
                UPDATE clientes SET nombre=?, telefono=?, email=?, tipo_doc=?, numero_doc=?,
                    empresa=?, direccion=?, notas=?
                WHERE id = ?
            """, (nombre.strip(), telefono.strip(), email.strip(), tipo_doc.strip(),
                  numero_doc.strip(), empresa.strip(), direccion.strip(),
                  notas.strip(), cliente_id)).rowcount
            conn.commit()
        return {"ok": affected > 0}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ──────────────────────────────────────────────
# Encargos — funciones extendidas (Release 3)
# ──────────────────────────────────────────────

def _fecha_hora_actual_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _fecha_hoy_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _normalizar_fecha_cxc(fecha: str | None) -> str:
    texto = str(fecha or "").strip()
    return texto[:10] if texto else ""


def _estado_cuenta_cobrar(
    saldo_pendiente: float,
    monto_original: float,
    fecha_vencimiento: str = "",
    abonos_count: int = 0,
    estado_guardado: str = "",
) -> str:
    estado_base = str(estado_guardado or "").strip().lower()
    if estado_base == "cancelada":
        return "cancelada"
    saldo = round(float(saldo_pendiente or 0), 2)
    monto = round(float(monto_original or 0), 2)
    fecha_v = _normalizar_fecha_cxc(fecha_vencimiento)
    if saldo <= 0.005:
        return "pagada"
    if fecha_v and fecha_v < _fecha_hoy_str():
        return "vencida"
    if abonos_count > 0 or saldo + 0.005 < monto:
        return "parcial"
    return "abierta"


def _dias_vencida(fecha_vencimiento: str) -> int:
    fecha_v = _normalizar_fecha_cxc(fecha_vencimiento)
    if not fecha_v:
        return 0
    try:
        fecha_dt = datetime.strptime(fecha_v, "%Y-%m-%d").date()
    except ValueError:
        return 0
    return max((datetime.now().date() - fecha_dt).days, 0)


def _parse_cuenta_row(row) -> dict | None:
    if not row:
        return None
    cuenta = _row_to_dict(row)
    cuenta["monto_original"] = round(float(cuenta.get("monto_original", 0) or 0), 2)
    cuenta["saldo_pendiente"] = round(float(cuenta.get("saldo_pendiente", 0) or 0), 2)
    cuenta["abonos_count"] = int(cuenta.get("abonos_count", 0) or 0)
    cuenta["estado_actual"] = _estado_cuenta_cobrar(
        cuenta.get("saldo_pendiente", 0),
        cuenta.get("monto_original", 0),
        cuenta.get("fecha_vencimiento", ""),
        cuenta.get("abonos_count", 0),
        cuenta.get("estado", ""),
    )
    cuenta["vencida"] = cuenta["estado_actual"] == "vencida"
    cuenta["dias_vencida"] = _dias_vencida(cuenta.get("fecha_vencimiento", "")) if cuenta["vencida"] else 0
    return cuenta


def _resolver_origen_cartera_conn(conn, origen_tipo: str, origen_id: int):
    origen = str(origen_tipo or "").strip().lower()
    if origen == "venta":
        return _row_to_dict(conn.execute(
            "SELECT id, panaderia_id, sede_id, cliente_id, fecha AS fecha_origen FROM venta_headers WHERE id = ?",
            (int(origen_id),),
        ).fetchone())
    if origen == "pedido":
        return _row_to_dict(conn.execute(
            "SELECT id, panaderia_id, sede_id, cliente_id, fecha AS fecha_origen FROM pedidos WHERE id = ?",
            (int(origen_id),),
        ).fetchone())
    if origen == "encargo":
        return _row_to_dict(conn.execute(
            "SELECT id, panaderia_id, sede_id, cliente_id, fecha_entrega AS fecha_origen FROM encargos WHERE id = ?",
            (int(origen_id),),
        ).fetchone())
    return None


def _obtener_documento_origen_conn(conn, origen_tipo: str, origen_id: int) -> int | None:
    row = conn.execute(
        """
        SELECT id
        FROM documentos_emitidos
        WHERE origen_tipo = ? AND origen_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (str(origen_tipo or "").strip().lower(), int(origen_id)),
    ).fetchone()
    return (int(row["id"] or 0) or None) if row else None


def _obtener_cuenta_por_origen_conn(conn, origen_tipo: str, origen_id: int):
    return _row_to_dict(conn.execute(
        """
        SELECT *
        FROM cuentas_por_cobrar
        WHERE origen_tipo = ? AND origen_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (str(origen_tipo or "").strip().lower(), int(origen_id)),
    ).fetchone())


def _query_cuentas_por_cobrar_conn(conn, filtros_sql: list[str], params: list) -> list[dict]:
    where = f"WHERE {' AND '.join(filtros_sql)}" if filtros_sql else ""
    rows = conn.execute(
        f"""
        SELECT cxc.*,
               c.nombre AS cliente_nombre,
               c.telefono AS cliente_telefono,
               c.email AS cliente_email,
               c.numero_doc AS cliente_numero_doc,
               d.consecutivo AS documento_consecutivo,
               d.tipo_documento AS documento_tipo,
               COALESCE(cc.abonos_count, 0) AS abonos_count
        FROM cuentas_por_cobrar cxc
        LEFT JOIN clientes c ON c.id = cxc.cliente_id
        LEFT JOIN documentos_emitidos d ON d.id = cxc.documento_id
        LEFT JOIN (
            SELECT cuenta_id, COUNT(*) AS abonos_count
            FROM cuenta_cobros
            GROUP BY cuenta_id
        ) cc ON cc.cuenta_id = cxc.id
        {where}
        ORDER BY cxc.created_at DESC, cxc.id DESC
        """,
        tuple(params),
    ).fetchall()
    return [_parse_cuenta_row(row) for row in rows if row]


def _resolver_usuario_audit_conn(conn, usuario_id: int | None = None, usuario_nombre: str = "") -> tuple[int | None, str]:
    resuelto_id, nombre = _resolver_usuario_snapshot_conn(conn, usuario_id)
    return resuelto_id, str(nombre or usuario_nombre or "").strip()


def _recalcular_estado_cuenta_conn(conn, cuenta_id: int) -> dict | None:
    rows = _query_cuentas_por_cobrar_conn(conn, ["cxc.id = ?"], [int(cuenta_id)])
    if not rows:
        return None
    cuenta = rows[0]
    now_str = _fecha_hora_actual_str()
    conn.execute(
        "UPDATE cuentas_por_cobrar SET estado = ?, updated_at = ? WHERE id = ?",
        (cuenta["estado_actual"], now_str, int(cuenta_id)),
    )
    cuenta["estado"] = cuenta["estado_actual"]
    cuenta["updated_at"] = now_str
    return cuenta


def _crear_cuenta_por_cobrar_conn(
    conn,
    cliente_id,
    origen_tipo,
    origen_id,
    monto,
    fecha_vencimiento=None,
    documento_id=None,
    aprobado_por_usuario_id=None,
    observacion=None,
    *,
    usuario_id: int | None = None,
    usuario_nombre: str = "",
) -> dict:
    cliente_id = int(cliente_id or 0)
    origen_id = int(origen_id or 0)
    if cliente_id <= 0:
        raise ValueError("No se puede crear credito sin cliente")
    origen = str(origen_tipo or "").strip().lower()
    if origen not in _ORIGENES_CXC_VALIDOS:
        raise ValueError("Origen de cuenta por cobrar invalido")
    monto_original = round(float(monto or 0), 2)
    if monto_original <= 0:
        raise ValueError("El monto del credito debe ser mayor a cero")

    origen_row = _resolver_origen_cartera_conn(conn, origen, origen_id)
    if not origen_row:
        raise ValueError("No se encontro el origen para crear la cuenta")
    existente = _obtener_cuenta_por_origen_conn(conn, origen, origen_id)
    if existente:
        return {"ok": True, "cuenta_id": int(existente["id"]), "existente": True}

    now_str = _fecha_hora_actual_str()
    fecha_emision = _normalizar_fecha_cxc(origen_row["fecha_origen"]) or _fecha_hoy_str()
    fecha_venc = _normalizar_fecha_cxc(fecha_vencimiento) or ""
    documento_rel = int(documento_id or 0) or _obtener_documento_origen_conn(conn, origen, origen_id)
    estado_inicial = _estado_cuenta_cobrar(monto_original, monto_original, fecha_venc)

    cur = conn.execute(
        """
        INSERT INTO cuentas_por_cobrar (
            panaderia_id, sede_id, cliente_id, origen_tipo, origen_id, documento_id,
            estado, monto_original, saldo_pendiente, fecha_emision, fecha_vencimiento,
            aprobado_por_usuario_id, observacion, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            origen_row["panaderia_id"],
            origen_row["sede_id"],
            cliente_id,
            origen,
            origen_id,
            documento_rel,
            estado_inicial,
            monto_original,
            monto_original,
            fecha_emision,
            fecha_venc,
            int(aprobado_por_usuario_id or 0) or None,
            str(observacion or "").strip(),
            now_str,
            now_str,
        ),
    )
    cuenta_id = cur.lastrowid
    audit_user_id, audit_user_name = _resolver_usuario_audit_conn(conn, usuario_id, usuario_nombre)
    _registrar_audit_conn(
        conn,
        usuario=audit_user_name,
        usuario_id=audit_user_id,
        panaderia_id=origen_row["panaderia_id"],
        sede_id=origen_row["sede_id"],
        accion="crear_cuenta_por_cobrar",
        entidad="cuenta_por_cobrar",
        entidad_id=str(cuenta_id),
        detalle=f"Cuenta creada desde {origen} #{origen_id}",
        valor_nuevo=json.dumps(
            {
                "cliente_id": cliente_id,
                "origen_tipo": origen,
                "origen_id": origen_id,
                "monto_original": monto_original,
                "fecha_vencimiento": fecha_venc,
            },
            ensure_ascii=False,
        ),
    )
    return {"ok": True, "cuenta_id": cuenta_id, "existente": False}


def crear_cuenta_por_cobrar(
    cliente_id,
    origen_tipo,
    origen_id,
    monto,
    fecha_vencimiento=None,
    documento_id=None,
    aprobado_por_usuario_id=None,
    observacion=None,
    usuario_id: int | None = None,
    usuario_nombre: str = "",
) -> dict:
    try:
        with get_connection() as conn:
            resultado = _crear_cuenta_por_cobrar_conn(
                conn,
                cliente_id=cliente_id,
                origen_tipo=origen_tipo,
                origen_id=origen_id,
                monto=monto,
                fecha_vencimiento=fecha_vencimiento,
                documento_id=documento_id,
                aprobado_por_usuario_id=aprobado_por_usuario_id,
                observacion=observacion,
                usuario_id=usuario_id,
                usuario_nombre=usuario_nombre,
            )
            conn.commit()
            return resultado
    except Exception as e:
        logger.error(f"crear_cuenta_por_cobrar: {e}")
        return {"ok": False, "error": str(e)}


def _registrar_abono_cuenta_conn(
    conn,
    cuenta_id: int,
    monto: float,
    metodo_pago: str,
    referencia: str = "",
    nota: str = "",
    usuario_id: int | None = None,
    usuario_nombre: str = "",
) -> dict:
    metodos_validos = {"efectivo", "transferencia", "tarjeta"}
    metodo = str(metodo_pago or "").strip().lower()
    if metodo not in metodos_validos:
        raise ValueError("Metodo de pago invalido para el abono")
    monto_abono = round(float(monto or 0), 2)
    if monto_abono <= 0:
        raise ValueError("El abono debe ser mayor a cero")

    cuenta_row = conn.execute("SELECT * FROM cuentas_por_cobrar WHERE id = ?", (int(cuenta_id),)).fetchone()
    cuenta = _parse_cuenta_row(cuenta_row)
    if not cuenta:
        raise ValueError("Cuenta por cobrar no encontrada")
    if cuenta["estado_actual"] == "cancelada":
        raise ValueError("La cuenta esta cancelada")
    if cuenta["saldo_pendiente"] <= 0.005:
        raise ValueError("La cuenta ya esta pagada")
    if monto_abono - cuenta["saldo_pendiente"] > 0.005:
        raise ValueError("El abono no puede ser mayor al saldo pendiente")

    audit_user_id, audit_user_name = _resolver_usuario_audit_conn(conn, usuario_id, usuario_nombre)
    now_str = _fecha_hora_actual_str()
    conn.execute(
        """
        INSERT INTO cuenta_cobros (
            cuenta_id, monto, metodo_pago, referencia, nota,
            registrado_por_usuario_id, registrado_por_nombre_snapshot, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(cuenta_id),
            monto_abono,
            metodo,
            str(referencia or "").strip(),
            str(nota or "").strip(),
            audit_user_id,
            audit_user_name,
            now_str,
        ),
    )
    nuevo_saldo = round(max(float(cuenta["saldo_pendiente"]) - monto_abono, 0.0), 2)
    conn.execute(
        "UPDATE cuentas_por_cobrar SET saldo_pendiente = ?, updated_at = ? WHERE id = ?",
        (nuevo_saldo, now_str, int(cuenta_id)),
    )
    cuenta_actualizada = _recalcular_estado_cuenta_conn(conn, cuenta_id)
    _registrar_audit_conn(
        conn,
        usuario=audit_user_name,
        usuario_id=audit_user_id,
        panaderia_id=cuenta.get("panaderia_id"),
        sede_id=cuenta.get("sede_id"),
        accion="registrar_abono_cartera",
        entidad="cuenta_por_cobrar",
        entidad_id=str(cuenta_id),
        detalle=f"Abono de ${monto_abono:,.2f} por {metodo}",
        valor_antes=str(cuenta.get("saldo_pendiente", 0)),
        valor_nuevo=str(nuevo_saldo),
    )
    if cuenta_actualizada and cuenta_actualizada["estado_actual"] == "pagada":
        _registrar_audit_conn(
            conn,
            usuario=audit_user_name,
            usuario_id=audit_user_id,
            panaderia_id=cuenta.get("panaderia_id"),
            sede_id=cuenta.get("sede_id"),
            accion="pagar_cuenta_por_cobrar",
            entidad="cuenta_por_cobrar",
            entidad_id=str(cuenta_id),
            detalle=f"Cuenta saldada con abono final de ${monto_abono:,.2f}",
            valor_nuevo="pagada",
        )
    return {
        "ok": True,
        "cuenta_id": int(cuenta_id),
        "saldo_pendiente": nuevo_saldo,
        "estado": (cuenta_actualizada or {}).get("estado_actual", cuenta.get("estado_actual")),
    }


def registrar_abono_cuenta(
    cuenta_id: int,
    monto: float,
    metodo_pago: str,
    referencia: str = "",
    nota: str = "",
    usuario_id: int | None = None,
    usuario_nombre: str = "",
) -> dict:
    try:
        with get_connection() as conn:
            resultado = _registrar_abono_cuenta_conn(
                conn,
                cuenta_id=cuenta_id,
                monto=monto,
                metodo_pago=metodo_pago,
                referencia=referencia,
                nota=nota,
                usuario_id=usuario_id,
                usuario_nombre=usuario_nombre,
            )
            conn.commit()
            return resultado
    except Exception as e:
        logger.error(f"registrar_abono_cuenta: {e}")
        return {"ok": False, "error": str(e)}


def recalcular_estado_cuenta(cuenta_id: int) -> dict:
    try:
        with get_connection() as conn:
            cuenta = _recalcular_estado_cuenta_conn(conn, cuenta_id)
            conn.commit()
            if not cuenta:
                return {"ok": False, "error": "Cuenta por cobrar no encontrada"}
            return {"ok": True, "cuenta": cuenta}
    except Exception as e:
        logger.error(f"recalcular_estado_cuenta: {e}")
        return {"ok": False, "error": str(e)}


def obtener_abonos_cuenta(cuenta_id: int) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT cc.*,
                   u.nombre AS usuario_nombre
            FROM cuenta_cobros cc
            LEFT JOIN usuarios u ON u.id = cc.registrado_por_usuario_id
            WHERE cc.cuenta_id = ?
            ORDER BY cc.created_at ASC, cc.id ASC
            """,
            (int(cuenta_id),),
        ).fetchall()
    return [dict(row) for row in rows]


def obtener_cuenta_por_cobrar(cuenta_id: int) -> dict | None:
    with get_connection() as conn:
        cuentas = _query_cuentas_por_cobrar_conn(conn, ["cxc.id = ?"], [int(cuenta_id)])
    if not cuentas:
        return None
    cuenta = cuentas[0]
    cuenta["abonos"] = obtener_abonos_cuenta(cuenta_id)
    return cuenta


def obtener_cuentas_por_cliente(cliente_id: int) -> list[dict]:
    with get_connection() as conn:
        return _query_cuentas_por_cobrar_conn(conn, ["cxc.cliente_id = ?"], [int(cliente_id)])


def obtener_cuentas_por_cobrar(
    estado: str | None = None,
    cliente_id: int | None = None,
    busqueda_cliente: str = "",
    fecha_desde: str | None = None,
    fecha_hasta: str | None = None,
    origen_tipo: str | None = None,
) -> list[dict]:
    filtros = []
    params: list = []
    panaderia_id, sede_id = _tenant_scope()
    if panaderia_id is not None:
        filtros.append("cxc.panaderia_id = ?")
        params.append(panaderia_id)
    if sede_id is not None:
        filtros.append("cxc.sede_id = ?")
        params.append(sede_id)
    if cliente_id:
        filtros.append("cxc.cliente_id = ?")
        params.append(int(cliente_id))
    if origen_tipo and str(origen_tipo).strip().lower() in _ORIGENES_CXC_VALIDOS:
        filtros.append("cxc.origen_tipo = ?")
        params.append(str(origen_tipo).strip().lower())
    if fecha_desde:
        filtros.append("cxc.fecha_emision >= ?")
        params.append(_normalizar_fecha_cxc(fecha_desde))
    if fecha_hasta:
        filtros.append("cxc.fecha_emision <= ?")
        params.append(_normalizar_fecha_cxc(fecha_hasta))
    if busqueda_cliente:
        like = f"%{str(busqueda_cliente).strip()}%"
        filtros.append("(c.nombre LIKE ? OR c.numero_doc LIKE ? OR c.telefono LIKE ?)")
        params.extend([like, like, like])
    with get_connection() as conn:
        cuentas = _query_cuentas_por_cobrar_conn(conn, filtros, params)
    estado_filtro = str(estado or "").strip().lower()
    if estado_filtro:
        cuentas = [cuenta for cuenta in cuentas if cuenta.get("estado_actual") == estado_filtro]
    return cuentas


def obtener_cuentas_por_cobrar_paginadas(
    estado: str | None = None,
    cliente_id: int | None = None,
    busqueda_cliente: str = "",
    fecha_desde: str | None = None,
    fecha_hasta: str | None = None,
    origen_tipo: str | None = None,
    page: int | None = 1,
    size: int | None = 50,
) -> dict:
    cuentas = obtener_cuentas_por_cobrar(
        estado=estado,
        cliente_id=cliente_id,
        busqueda_cliente=busqueda_cliente,
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
        origen_tipo=origen_tipo,
    )
    page_num, size_num, offset = _sanitize_pagination(page, size)
    items = cuentas[offset:offset + size_num]
    return {
        "items": items,
        "pagination": _build_pagination_meta(len(cuentas), page_num, size_num, len(items)),
    }


def obtener_resumen_cartera() -> dict:
    cuentas = obtener_cuentas_por_cobrar()
    total_abierta = 0.0
    total_vencida = 0.0
    total_parcial = 0.0
    total_pagada = 0.0
    clientes_con_saldo: set[int] = set()
    deuda_por_cliente: dict[tuple[int, str], float] = {}
    for cuenta in cuentas:
        saldo = round(float(cuenta.get("saldo_pendiente", 0) or 0), 2)
        estado = str(cuenta.get("estado_actual") or "").strip().lower()
        if estado in {"abierta", "parcial", "vencida"} and saldo > 0.005:
            cliente = int(cuenta.get("cliente_id") or 0)
            if cliente > 0:
                clientes_con_saldo.add(cliente)
                key = (cliente, str(cuenta.get("cliente_nombre") or "Cliente"))
                deuda_por_cliente[key] = round(deuda_por_cliente.get(key, 0.0) + saldo, 2)
        if estado == "abierta":
            total_abierta += saldo
        elif estado == "parcial":
            total_parcial += saldo
        elif estado == "vencida":
            total_vencida += saldo
        elif estado == "pagada":
            total_pagada += round(float(cuenta.get("monto_original", 0) or 0), 2)
    top_clientes = [
        {"cliente_id": cliente_id, "cliente": nombre, "saldo_pendiente": saldo}
        for (cliente_id, nombre), saldo in sorted(deuda_por_cliente.items(), key=lambda item: item[1], reverse=True)[:5]
    ]
    return {
        "total_abierta": round(total_abierta, 2),
        "total_vencida": round(total_vencida, 2),
        "total_parcial": round(total_parcial, 2),
        "total_pagada": round(total_pagada, 2),
        "clientes_con_saldo": len(clientes_con_saldo),
        "total_cuentas": len(cuentas),
        "top_clientes": top_clientes,
    }


def actualizar_cliente_venta(venta_id: int, cliente_id: int | None = None, cliente_nombre_snapshot: str = "") -> bool:
    with get_connection() as conn:
        cur = conn.execute(
            """
            UPDATE venta_headers
            SET cliente_id = ?, cliente_nombre_snapshot = ?, updated_at = ?
            WHERE id = ? AND estado IN ('activa','suspendida','pagada')
            """,
            (
                int(cliente_id or 0) or None,
                str(cliente_nombre_snapshot or "").strip(),
                _fecha_hora_actual_str(),
                int(venta_id),
            ),
        )
        conn.commit()
    return cur.rowcount > 0


def actualizar_cliente_pedido(pedido_id: int, cliente_id: int | None = None, cliente_nombre_snapshot: str = "") -> bool:
    with get_connection() as conn:
        cur = conn.execute(
            "UPDATE pedidos SET cliente_id = ?, cliente_nombre_snapshot = ? WHERE id = ?",
            (int(cliente_id or 0) or None, str(cliente_nombre_snapshot or "").strip(), int(pedido_id)),
        )
        conn.commit()
    return cur.rowcount > 0


def _vincular_documento_cartera_por_origen_conn(conn, origen_tipo: str, origen_id: int, documento_id: int) -> None:
    conn.execute(
        """
        UPDATE cuentas_por_cobrar
        SET documento_id = ?, updated_at = ?
        WHERE origen_tipo = ? AND origen_id = ?
        """,
        (
            int(documento_id),
            _fecha_hora_actual_str(),
            str(origen_tipo or "").strip().lower(),
            int(origen_id),
        ),
    )


def crear_credito_desde_venta(
    venta_id: int,
    cliente_id: int,
    saldo: float,
    fecha_vencimiento: str | None = None,
    documento_id: int | None = None,
    aprobado_por_usuario_id: int | None = None,
    observacion: str | None = None,
    usuario_id: int | None = None,
    usuario_nombre: str = "",
) -> dict:
    return crear_cuenta_por_cobrar(
        cliente_id=cliente_id,
        origen_tipo="venta",
        origen_id=venta_id,
        monto=saldo,
        fecha_vencimiento=fecha_vencimiento,
        documento_id=documento_id,
        aprobado_por_usuario_id=aprobado_por_usuario_id,
        observacion=observacion or f"Credito generado desde venta #{venta_id}",
        usuario_id=usuario_id,
        usuario_nombre=usuario_nombre,
    )


def crear_credito_desde_pedido(
    pedido_id: int,
    cliente_id: int,
    saldo: float,
    fecha_vencimiento: str | None = None,
    documento_id: int | None = None,
    aprobado_por_usuario_id: int | None = None,
    observacion: str | None = None,
    usuario_id: int | None = None,
    usuario_nombre: str = "",
) -> dict:
    return crear_cuenta_por_cobrar(
        cliente_id=cliente_id,
        origen_tipo="pedido",
        origen_id=pedido_id,
        monto=saldo,
        fecha_vencimiento=fecha_vencimiento,
        documento_id=documento_id,
        aprobado_por_usuario_id=aprobado_por_usuario_id,
        observacion=observacion or f"Credito generado desde pedido #{pedido_id}",
        usuario_id=usuario_id,
        usuario_nombre=usuario_nombre,
    )


def crear_credito_desde_encargo(
    encargo_id: int,
    cliente_id: int,
    saldo: float,
    fecha_vencimiento: str | None = None,
    documento_id: int | None = None,
    aprobado_por_usuario_id: int | None = None,
    observacion: str | None = None,
    usuario_id: int | None = None,
    usuario_nombre: str = "",
) -> dict:
    return crear_cuenta_por_cobrar(
        cliente_id=cliente_id,
        origen_tipo="encargo",
        origen_id=encargo_id,
        monto=saldo,
        fecha_vencimiento=fecha_vencimiento,
        documento_id=documento_id,
        aprobado_por_usuario_id=aprobado_por_usuario_id,
        observacion=observacion or f"Saldo pendiente del encargo #{encargo_id}",
        usuario_id=usuario_id,
        usuario_nombre=usuario_nombre,
    )


def _sincronizar_credito_encargo_conn(conn, encargo_id: int, usuario_id: int | None = None, usuario_nombre: str = "") -> dict:
    encargo = conn.execute(
        """
        SELECT id, panaderia_id, sede_id, cliente_id, saldo_pendiente, fecha_entrega
        FROM encargos
        WHERE id = ?
        """,
        (int(encargo_id),),
    ).fetchone()
    if not encargo:
        raise ValueError("Encargo no encontrado")
    saldo = round(float(encargo["saldo_pendiente"] or 0), 2)
    cuenta_existente = _obtener_cuenta_por_origen_conn(conn, "encargo", encargo_id)
    if saldo <= 0.005:
        if cuenta_existente:
            _recalcular_estado_cuenta_conn(conn, int(cuenta_existente["id"]))
        return {"ok": True, "cuenta_id": int(cuenta_existente["id"]) if cuenta_existente else None}

    cliente_id = int(encargo["cliente_id"] or 0) or None
    if not cliente_id:
        raise ValueError("No se puede dejar saldo pendiente en un encargo sin cliente asociado")

    documento_id = _obtener_documento_origen_conn(conn, "encargo", encargo_id)
    if not cuenta_existente:
        return _crear_cuenta_por_cobrar_conn(
            conn,
            cliente_id=cliente_id,
            origen_tipo="encargo",
            origen_id=encargo_id,
            monto=saldo,
            fecha_vencimiento=_normalizar_fecha_cxc(encargo["fecha_entrega"]),
            documento_id=documento_id,
            observacion=f"Saldo pendiente del encargo #{encargo_id}",
            usuario_id=usuario_id,
            usuario_nombre=usuario_nombre,
        )

    abonos_totales = conn.execute(
        "SELECT COALESCE(SUM(monto), 0) AS total FROM cuenta_cobros WHERE cuenta_id = ?",
        (int(cuenta_existente["id"]),),
    ).fetchone()
    now_str = _fecha_hora_actual_str()
    monto_original = round(float(abonos_totales["total"] or 0) + saldo, 2)
    conn.execute(
        """
        UPDATE cuentas_por_cobrar
        SET cliente_id = ?, documento_id = ?, monto_original = ?, saldo_pendiente = ?,
            fecha_vencimiento = ?, observacion = ?, updated_at = ?
        WHERE id = ?
        """,
        (
            cliente_id,
            documento_id,
            monto_original,
            saldo,
            _normalizar_fecha_cxc(encargo["fecha_entrega"]),
            f"Saldo pendiente del encargo #{encargo_id}",
            now_str,
            int(cuenta_existente["id"]),
        ),
    )
    _recalcular_estado_cuenta_conn(conn, int(cuenta_existente["id"]))
    return {"ok": True, "cuenta_id": int(cuenta_existente["id"]), "existente": True}


def obtener_historial_cliente(cliente_id: int, page: int | None = 1, size: int | None = 50) -> dict:
    cliente = obtener_cliente(int(cliente_id))
    if not cliente:
        return {"ok": False, "error": "Cliente no encontrado"}
    page_num, size_num, offset = _sanitize_pagination(page, size)
    with get_connection() as conn:
        ventas_total = int(conn.execute(
            "SELECT COUNT(*) AS total FROM venta_headers WHERE cliente_id = ?",
            (int(cliente_id),),
        ).fetchone()["total"] or 0)
        ventas = [dict(row) for row in conn.execute(
            """
            SELECT id, fecha, hora, creado_en, estado, tipo_venta, total,
                   saldo_pendiente, venta_grupo, nombre_comprador
            FROM venta_headers
            WHERE cliente_id = ?
            ORDER BY creado_en DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            (int(cliente_id), size_num, offset),
        ).fetchall()]
        pedidos_total = int(conn.execute(
            "SELECT COUNT(*) AS total FROM pedidos WHERE cliente_id = ?",
            (int(cliente_id),),
        ).fetchone()["total"] or 0)
        pedidos = [dict(row) for row in conn.execute(
            """
            SELECT p.id, p.fecha, p.hora, p.estado, p.total, p.pagado_en,
                   p.metodo_pago, p.metodo_pago_2, p.monto_pago_2,
                   m.numero AS mesa_numero, m.nombre AS mesa_nombre
            FROM pedidos p
            LEFT JOIN mesas m ON m.id = p.mesa_id
            WHERE p.cliente_id = ?
            ORDER BY COALESCE(p.pagado_en, p.creado_en, p.fecha) DESC, p.id DESC
            LIMIT ? OFFSET ?
            """,
            (int(cliente_id), size_num, offset),
        ).fetchall()]
        encargos_total = int(conn.execute(
            "SELECT COUNT(*) AS total FROM encargos WHERE cliente_id = ?",
            (int(cliente_id),),
        ).fetchone()["total"] or 0)
        encargos = [dict(row) for row in conn.execute(
            """
            SELECT id, fecha_entrega, hora_entrega, estado, total, anticipo,
                   saldo_pendiente, canal_venta, creado_en
            FROM encargos
            WHERE cliente_id = ?
            ORDER BY creado_en DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            (int(cliente_id), size_num, offset),
        ).fetchall()]
        documentos_total = int(conn.execute(
            "SELECT COUNT(*) AS total FROM documentos_emitidos WHERE cliente_id = ?",
            (int(cliente_id),),
        ).fetchone()["total"] or 0)
        documentos = [dict(row) for row in conn.execute(
            """
            SELECT id, origen_tipo, origen_id, consecutivo, tipo_documento, estado,
                   total, created_at
            FROM documentos_emitidos
            WHERE cliente_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            (int(cliente_id), size_num, offset),
        ).fetchall()]
        abonos_total = int(conn.execute(
            """
            SELECT COUNT(*) AS total
            FROM cuenta_cobros cc
            JOIN cuentas_por_cobrar cxc ON cxc.id = cc.cuenta_id
            WHERE cxc.cliente_id = ?
            """,
            (int(cliente_id),),
        ).fetchone()["total"] or 0)
        abonos = [dict(row) for row in conn.execute(
            """
            SELECT cc.id, cc.cuenta_id, cc.monto, cc.metodo_pago, cc.referencia, cc.nota,
                   cc.registrado_por_nombre_snapshot, cc.created_at,
                   cxc.origen_tipo, cxc.origen_id
            FROM cuenta_cobros cc
            JOIN cuentas_por_cobrar cxc ON cxc.id = cc.cuenta_id
            WHERE cxc.cliente_id = ?
            ORDER BY cc.created_at DESC, cc.id DESC
            LIMIT ? OFFSET ?
            """,
            (int(cliente_id), size_num, offset),
        ).fetchall()]
    cuentas_data = obtener_cuentas_por_cobrar_paginadas(
        cliente_id=int(cliente_id),
        page=page_num,
        size=size_num,
    )
    cuentas = cuentas_data["items"]
    cuentas_total = int(cuentas_data["pagination"]["total_items"] or 0)
    cuentas_todas = obtener_cuentas_por_cliente(int(cliente_id))
    pagination = {
        "ventas": _build_pagination_meta(ventas_total, page_num, size_num, len(ventas)),
        "pedidos": _build_pagination_meta(pedidos_total, page_num, size_num, len(pedidos)),
        "encargos": _build_pagination_meta(encargos_total, page_num, size_num, len(encargos)),
        "documentos": _build_pagination_meta(documentos_total, page_num, size_num, len(documentos)),
        "abonos": _build_pagination_meta(abonos_total, page_num, size_num, len(abonos)),
        "cuentas": _build_pagination_meta(cuentas_total, page_num, size_num, len(cuentas)),
    }
    max_total = max(
        ventas_total,
        pedidos_total,
        encargos_total,
        documentos_total,
        abonos_total,
        cuentas_total,
        0,
    )
    pagination["global"] = _build_pagination_meta(max_total, page_num, size_num, 0)
    saldo_total = round(sum(float(cuenta.get("saldo_pendiente", 0) or 0) for cuenta in cuentas_todas), 2)
    ultimo_movimiento = ""
    for value in (
        [venta.get("creado_en", "") for venta in ventas]
        + [pedido.get("pagado_en", "") or pedido.get("fecha", "") for pedido in pedidos]
        + [encargo.get("creado_en", "") for encargo in encargos]
        + [doc.get("created_at", "") for doc in documentos]
        + [abono.get("created_at", "") for abono in abonos]
    ):
        if value and value > ultimo_movimiento:
            ultimo_movimiento = value
    return {
        "ok": True,
        "cliente": cliente,
        "ventas": ventas,
        "pedidos": pedidos,
        "encargos": encargos,
        "documentos": documentos,
        "cuentas": cuentas,
        "abonos": abonos,
        "pagination": pagination,
        "resumen": {
            "saldo_total_pendiente": saldo_total,
            "total_ventas": ventas_total,
            "total_pedidos": pedidos_total,
            "total_encargos": encargos_total,
            "total_documentos": documentos_total,
            "total_cuentas": cuentas_total,
            "total_abonos": abonos_total,
            "ultimo_movimiento": ultimo_movimiento,
        },
    }


def crear_encargo_v2(
    fecha_entrega: str, cliente: str, items: list[dict],
    empresa: str = "", notas: str = "", registrado_por: str = "",
    hora_entrega: str = "", telefono: str = "", anticipo: float = 0.0,
    canal_venta: str = "tienda", tipo_encargo: str = "orden",
    direccion_entrega: str = "", cliente_id: int | None = None,
    estado_inicial: str = "confirmado",
) -> dict:
    if not fecha_entrega or not cliente.strip():
        return {"ok": False, "error": "Fecha de entrega y cliente son obligatorios"}
    if not items:
        return {"ok": False, "error": "Debe incluir al menos un producto"}
    if estado_inicial not in _ESTADOS_ENCARGO_VALIDOS:
        estado_inicial = "confirmado"

    creado_en = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total = round(sum(
        float(it.get("precio_unitario", 0) or 0) * int(it.get("cantidad", 1) or 1)
        for it in items
    ), 2)
    anticipo = max(0.0, round(float(anticipo or 0), 2))
    saldo = round(total - anticipo, 2)
    if saldo > 0.005 and not int(cliente_id or 0):
        return {"ok": False, "error": "Debes asociar un cliente para dejar saldo pendiente en cartera"}
    panaderia_id, sede_id = _tenant_scope()

    try:
        with get_connection() as conn:
            cur = conn.execute("""
                INSERT INTO encargos (
                    fecha_entrega, hora_entrega, cliente, cliente_id, empresa, telefono,
                    notas, estado, registrado_por, creado_en, total, anticipo, saldo_pendiente,
                    canal_venta, tipo_encargo, direccion_entrega, recordatorio_enviado,
                    panaderia_id, sede_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
            """, (
                fecha_entrega, hora_entrega.strip(), cliente.strip(),
                int(cliente_id) if cliente_id else None,
                empresa.strip(), telefono.strip(), notas.strip(),
                estado_inicial, registrado_por.strip(), creado_en,
                total, anticipo, saldo,
                canal_venta, tipo_encargo, direccion_entrega.strip(),
                panaderia_id, sede_id,
            ))
            encargo_id = cur.lastrowid

            for it in items:
                cantidad = int(it.get("cantidad", 0) or 0)
                precio = float(it.get("precio_unitario", 0) or 0)
                if cantidad <= 0:
                    continue
                conn.execute("""
                    INSERT INTO encargo_items
                        (encargo_id, producto_id, producto, cantidad, precio_unitario, subtotal, notas)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    encargo_id,
                    int(it.get("producto_id", 0) or 0) or None,
                    str(it.get("producto", "") or "").strip(),
                    cantidad, precio, round(precio * cantidad, 2),
                    str(it.get("notas", "") or "").strip(),
                ))

            if anticipo > 0:
                metodo_anticipo = str(items[0].get("metodo_anticipo", "efectivo") or "efectivo") if items else "efectivo"
                conn.execute("""
                    INSERT INTO encargo_pagos (encargo_id, metodo, monto, registrado_por, registrado_en, notas)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (encargo_id, metodo_anticipo, anticipo, registrado_por.strip(), creado_en, "Anticipo inicial"))
                if anticipo >= total:
                    conn.execute("UPDATE encargos SET estado='con_anticipo' WHERE id=?", (encargo_id,))

            if saldo > 0.005:
                _sincronizar_credito_encargo_conn(conn, encargo_id, usuario_nombre=registrado_por.strip())
            conn.commit()
        return {"ok": True, "encargo_id": encargo_id, "total": total, "anticipo": anticipo, "saldo": saldo}
    except Exception as e:
        logger.error(f"crear_encargo_v2: {e}")
        return {"ok": False, "error": str(e)}


def actualizar_encargo(
    encargo_id: int, fecha_entrega: str, cliente: str, items: list[dict],
    empresa: str = "", notas: str = "", hora_entrega: str = "",
    telefono: str = "", canal_venta: str = "tienda", tipo_encargo: str = "orden",
    direccion_entrega: str = "", cliente_id: int | None = None,
) -> dict:
    if not fecha_entrega or not cliente.strip():
        return {"ok": False, "error": "Fecha de entrega y cliente son obligatorios"}
    total = round(sum(
        float(it.get("precio_unitario", 0) or 0) * int(it.get("cantidad", 1) or 1)
        for it in (items or [])
    ), 2)
    try:
        cliente_id_resuelto = int(cliente_id or 0) or None
    except (TypeError, ValueError):
        cliente_id_resuelto = None
    try:
        with get_connection() as conn:
            encargo_actual = conn.execute(
                "SELECT anticipo FROM encargos WHERE id=?",
                (encargo_id,),
            ).fetchone()
            if not encargo_actual:
                return {"ok": False, "error": "Encargo no encontrado"}
            saldo_estimado = round(max(total - float(encargo_actual["anticipo"] or 0), 0), 2)
            if saldo_estimado > 0.005 and not cliente_id_resuelto:
                return {"ok": False, "error": "Debes asociar un cliente para dejar saldo pendiente en cartera"}
            conn.execute("""
                UPDATE encargos SET
                    fecha_entrega=?, hora_entrega=?, cliente=?, cliente_id=?, empresa=?,
                    telefono=?, notas=?, total=?, saldo_pendiente=total - anticipo,
                    canal_venta=?, tipo_encargo=?, direccion_entrega=?
                WHERE id=?
            """, (
                fecha_entrega, hora_entrega.strip(), cliente.strip(),
                cliente_id_resuelto,
                empresa.strip(), telefono.strip(), notas.strip(), total,
                canal_venta, tipo_encargo, direccion_entrega.strip(),
                encargo_id,
            ))
            conn.execute("DELETE FROM encargo_items WHERE encargo_id=?", (encargo_id,))
            for it in (items or []):
                cantidad = int(it.get("cantidad", 0) or 0)
                precio = float(it.get("precio_unitario", 0) or 0)
                if cantidad <= 0:
                    continue
                conn.execute("""
                    INSERT INTO encargo_items
                        (encargo_id, producto_id, producto, cantidad, precio_unitario, subtotal, notas)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    encargo_id,
                    int(it.get("producto_id", 0) or 0) or None,
                    str(it.get("producto", "") or "").strip(),
                    cantidad, precio, round(precio * cantidad, 2),
                    str(it.get("notas", "") or "").strip(),
                ))
            _sincronizar_credito_encargo_conn(conn, encargo_id)
            conn.commit()
        return {"ok": True, "total": total}
    except Exception as e:
        logger.error(f"actualizar_encargo: {e}")
        return {"ok": False, "error": str(e)}


def actualizar_estado_encargo_v2(encargo_id: int, estado: str, usuario: str = "") -> dict:
    if estado not in _ESTADOS_ENCARGO_VALIDOS:
        return {"ok": False, "error": f"Estado invalido: {estado}. Validos: {', '.join(_ESTADOS_ENCARGO_VALIDOS)}"}
    try:
        with get_connection() as conn:
            affected = conn.execute(
                "UPDATE encargos SET estado=? WHERE id=?", (estado, encargo_id)
            ).rowcount
            conn.commit()
        return {"ok": affected > 0, "error": "Encargo no encontrado" if affected == 0 else None}
    except Exception as e:
        logger.error(f"actualizar_estado_encargo_v2: {e}")
        return {"ok": False, "error": str(e)}


def registrar_pago_encargo(encargo_id: int, metodo: str, monto: float,
                            registrado_por: str = "", referencia: str = "",
                            notas: str = "", usuario_id: int | None = None) -> dict:
    if monto <= 0:
        return {"ok": False, "error": "El monto debe ser mayor a cero"}
    metodos_validos = {"efectivo", "transferencia", "tarjeta"}
    if metodo not in metodos_validos:
        metodo = "efectivo"
    registrado_en = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with get_connection() as conn:
            encargo = conn.execute(
                "SELECT total, anticipo, saldo_pendiente, estado FROM encargos WHERE id=?",
                (encargo_id,)
            ).fetchone()
            if not encargo:
                return {"ok": False, "error": "Encargo no encontrado"}
            if monto - float(encargo["saldo_pendiente"] or 0) > 0.005:
                return {"ok": False, "error": "El pago no puede exceder el saldo pendiente del encargo"}

            conn.execute("""
                INSERT INTO encargo_pagos (encargo_id, metodo, monto, referencia, notas,
                    registrado_por, registrado_en)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (encargo_id, metodo, round(monto, 2), referencia.strip(),
                  notas.strip(), registrado_por.strip(), registrado_en))

            pagado_total = conn.execute(
                "SELECT COALESCE(SUM(monto), 0) AS total_pagado FROM encargo_pagos WHERE encargo_id=?",
                (encargo_id,)
            ).fetchone()
            pagado_total = float((pagado_total["total_pagado"] if pagado_total else 0) or 0)

            nuevo_anticipo = round(pagado_total, 2)
            nuevo_saldo = round(max(float(encargo["total"]) - nuevo_anticipo, 0), 2)
            nuevo_estado = encargo["estado"]
            if nuevo_anticipo >= float(encargo["total"]):
                nuevo_estado = "listo" if encargo["estado"] in ("con_anticipo", "programado") else encargo["estado"]
            elif nuevo_anticipo > 0 and encargo["estado"] in ("confirmado", "cotizacion"):
                nuevo_estado = "con_anticipo"

            conn.execute("""
                UPDATE encargos SET anticipo=?, saldo_pendiente=?, estado=? WHERE id=?
            """, (nuevo_anticipo, nuevo_saldo, nuevo_estado, encargo_id))
            cuenta_existente = _obtener_cuenta_por_origen_conn(conn, "encargo", encargo_id)
            if cuenta_existente and float(cuenta_existente["saldo_pendiente"] or 0) > 0.005:
                _registrar_abono_cuenta_conn(
                    conn,
                    cuenta_id=int(cuenta_existente["id"]),
                    monto=monto,
                    metodo_pago=metodo,
                    referencia=referencia,
                    nota=notas or f"Pago aplicado al encargo #{encargo_id}",
                    usuario_id=usuario_id,
                    usuario_nombre=registrado_por,
                )
            else:
                _sincronizar_credito_encargo_conn(
                    conn,
                    encargo_id,
                    usuario_id=usuario_id,
                    usuario_nombre=registrado_por,
                )
            conn.commit()

        return {"ok": True, "anticipo_total": nuevo_anticipo, "saldo": nuevo_saldo, "estado": nuevo_estado}
    except Exception as e:
        logger.error(f"registrar_pago_encargo: {e}")
        return {"ok": False, "error": str(e)}


def obtener_pagos_encargo(encargo_id: int) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM encargo_pagos WHERE encargo_id=? ORDER BY registrado_en ASC",
            (encargo_id,)
        ).fetchall()
    return [dict(r) for r in rows]


def obtener_encargos_v2(estado: str | None = None, fecha_entrega: str | None = None,
                        dias: int = 30) -> list[dict]:
    filtros: list = []
    params: list = []
    if estado and estado in _ESTADOS_ENCARGO_VALIDOS:
        filtros.append("e.estado = ?")
        params.append(estado)
    if fecha_entrega:
        filtros.append("e.fecha_entrega = ?")
        params.append(fecha_entrega)
    else:
        desde = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")
        filtros.append("e.fecha_entrega >= ?")
        params.append(desde)
    panaderia_id, sede_id = _tenant_scope()
    if panaderia_id is not None:
        filtros.append("e.panaderia_id = ?")
        params.append(panaderia_id)
    if sede_id is not None:
        filtros.append("e.sede_id = ?")
        params.append(sede_id)

    where = f"WHERE {' AND '.join(filtros)}" if filtros else ""
    with get_connection() as conn:
        encargos = conn.execute(f"""
            SELECT e.id, e.panaderia_id, e.sede_id, e.fecha_entrega, e.hora_entrega, e.cliente, e.cliente_id,
                   e.empresa, e.telefono, e.notas, e.estado, e.registrado_por,
                   e.creado_en, e.total, e.anticipo, e.saldo_pendiente,
                   e.canal_venta, e.tipo_encargo, e.direccion_entrega, e.recordatorio_enviado
            FROM encargos e
            {where}
            ORDER BY e.fecha_entrega ASC, e.creado_en DESC
        """, tuple(params)).fetchall()

        if not encargos:
            return []

        ids = [r["id"] for r in encargos]
        placeholders = ",".join("?" * len(ids))
        items = conn.execute(f"""
            SELECT id, encargo_id, producto_id, producto, cantidad,
                   precio_unitario, subtotal, notas
            FROM encargo_items WHERE encargo_id IN ({placeholders}) ORDER BY id ASC
        """, ids).fetchall()
        pagos = conn.execute(f"""
            SELECT encargo_id, metodo, monto, registrado_en
            FROM encargo_pagos WHERE encargo_id IN ({placeholders}) ORDER BY registrado_en ASC
        """, ids).fetchall()

    items_by_id: dict[int, list] = {}
    for it in items:
        items_by_id.setdefault(it["encargo_id"], []).append(dict(it))
    pagos_by_id: dict[int, list] = {}
    for p in pagos:
        pagos_by_id.setdefault(p["encargo_id"], []).append(dict(p))

    result = []
    for r in encargos:
        d = dict(r)
        d["items"] = items_by_id.get(r["id"], [])
        d["pagos"] = pagos_by_id.get(r["id"], [])
        result.append(d)
    return result


def obtener_demanda_comprometida_encargos(
    producto: str,
    fecha_entrega: str,
    estados: tuple[str, ...] = ("confirmado", "programado"),
) -> int:
    """Suma encargos comprometidos para producto y fecha dentro del tenant actual."""
    nombre_producto = str(producto or "").strip().lower()
    fecha = str(fecha_entrega or "").strip()
    estados_validos = tuple(estado for estado in estados if estado in _ESTADOS_ENCARGO_VALIDOS)
    if not nombre_producto or not fecha or not estados_validos:
        return 0

    panaderia_id, sede_id = _tenant_scope()
    placeholders_estados = ",".join("?" * len(estados_validos))
    filtros = [
        "e.fecha_entrega = ?",
        f"e.estado IN ({placeholders_estados})",
        "LOWER(TRIM(ei.producto)) = ?",
    ]
    params: list = [fecha, *estados_validos, nombre_producto]

    if panaderia_id is not None:
        filtros.append("e.panaderia_id = ?")
        params.append(panaderia_id)
    if sede_id is not None:
        filtros.append("e.sede_id = ?")
        params.append(sede_id)

    query = f"""
        SELECT COALESCE(SUM(ei.cantidad), 0) AS total
        FROM encargos e
        JOIN encargo_items ei ON ei.encargo_id = e.id
        WHERE {' AND '.join(filtros)}
    """
    with get_connection() as conn:
        row = conn.execute(query, tuple(params)).fetchone()
    return int((row["total"] if row else 0) or 0)


def obtener_encargo_v2(encargo_id: int) -> dict | None:
    with get_connection() as conn:
        row = conn.execute("""
            SELECT id, panaderia_id, sede_id, fecha_entrega, hora_entrega, cliente, cliente_id, empresa, telefono,
                   notas, estado, registrado_por, creado_en, total, anticipo,
                   saldo_pendiente, canal_venta, tipo_encargo, direccion_entrega,
                   recordatorio_enviado
            FROM encargos WHERE id=?
        """, (encargo_id,)).fetchone()
        if not row:
            return None
        items = conn.execute("""
            SELECT id, encargo_id, producto_id, producto, cantidad, precio_unitario, subtotal, notas
            FROM encargo_items WHERE encargo_id=? ORDER BY id ASC
        """, (encargo_id,)).fetchall()
        pagos = conn.execute("""
            SELECT * FROM encargo_pagos WHERE encargo_id=? ORDER BY registrado_en ASC
        """, (encargo_id,)).fetchall()
    d = _row_to_dict(row)
    d["items"] = [_row_to_dict(it) for it in items]
    d["pagos"] = [_row_to_dict(p) for p in pagos]
    return d


# ──────────────────────────────────────────────
# Terminales
# ──────────────────────────────────────────────

def obtener_terminal_por_id(terminal_id: int) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM terminales WHERE id = ?",
            (terminal_id,),
        ).fetchone()
    return dict(row) if row else None


def obtener_terminal_por_codigo(sede_id: int, codigo: str) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM terminales WHERE sede_id = ? AND UPPER(codigo) = UPPER(?)",
            (sede_id, codigo),
        ).fetchone()
    return dict(row) if row else None


def obtener_terminales_sede(sede_id: int) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM terminales WHERE sede_id = ? ORDER BY nombre",
            (sede_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def registrar_terminal(panaderia_id: int, sede_id: int, nombre: str, codigo: str, tipo: str = "caja") -> dict | None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with get_connection() as conn:
            conn.execute(
                """
                INSERT INTO terminales (panaderia_id, sede_id, nombre, codigo, tipo, activa, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (panaderia_id, sede_id, nombre, codigo, tipo, now, now),
            )
            conn.commit()
            row = conn.execute(
                "SELECT * FROM terminales WHERE sede_id = ? AND UPPER(codigo) = UPPER(?)",
                (sede_id, codigo),
            ).fetchone()
        return dict(row) if row else None
    except _INTEGRITY_ERRORS:
        return None


def actualizar_last_seen_terminal(terminal_id: int) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_connection() as conn:
        conn.execute(
            "UPDATE terminales SET last_seen_at = ?, updated_at = ? WHERE id = ?",
            (now, now, terminal_id),
        )
        conn.commit()


# ──────────────────────────────────────────────
# Productos
# ──────────────────────────────────────────────

def _insertar_categoria_conn(conn, nombre: str) -> None:
    """INSERT OR IGNORE de categoría incluyendo panaderia_id del contexto actual."""
    panaderia_id, _ = _tenant_scope()
    conn.execute(
        "INSERT OR IGNORE INTO categorias_producto (nombre, activa, panaderia_id) VALUES (?, 1, ?)",
        (nombre, panaderia_id),
    )


def obtener_categorias_producto() -> list[str]:
    filtros = ["activa = 1"]
    params: list = []
    _apply_tenant_scope(filtros, params, include_sede=False)
    with get_connection() as conn:
        rows = conn.execute(
            f"SELECT nombre FROM categorias_producto WHERE {' AND '.join(filtros)}",
            tuple(params),
        ).fetchall()

    orden = {
        _normalizar_texto_clave(nombre): indice
        for indice, nombre in enumerate(ORDEN_CATEGORIAS_PREFERIDO, start=1)
    }
    categorias = [str(r["nombre"] or "") for r in rows]
    categorias.sort(key=lambda nombre: (orden.get(_normalizar_texto_clave(nombre), 999), nombre))
    return categorias


def agregar_categoria_producto(nombre: str) -> bool:
    panaderia_id, _ = _tenant_scope()
    try:
        with get_connection() as conn:
            conn.execute(
                "INSERT INTO categorias_producto (nombre, activa, panaderia_id) VALUES (?, 1, ?)",
                (nombre, panaderia_id),
            )
            conn.commit()
        return True
    except _INTEGRITY_ERRORS:
        return False


def eliminar_categoria_producto(nombre: str) -> dict:
    if nombre == "Panaderia":
        return {"ok": False, "error": "No se puede eliminar la categoria por defecto."}
    panaderia_id, _ = _tenant_scope()
    try:
        with get_connection() as conn:
            count_row = conn.execute(
                "SELECT COUNT(*) AS total FROM productos WHERE categoria = ? AND activo = 1 AND panaderia_id = ?",
                (nombre, panaderia_id),
            ).fetchone()
            count = int((count_row["total"] if count_row else 0) or 0)
            if count > 0:
                return {"ok": False, "error": f"No se puede eliminar porque hay {count} productos usandola."}
            conn.execute(
                "DELETE FROM categorias_producto WHERE nombre = ? AND panaderia_id = ?",
                (nombre, panaderia_id),
            )
            conn.execute(
                "UPDATE productos SET categoria = 'Panaderia' WHERE categoria = ? AND panaderia_id = ?",
                (nombre, panaderia_id),
            )
            conn.commit()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def obtener_productos(categoria: str = None) -> list[str]:
    filtros = ["activo = 1"]
    params: list = []
    _apply_tenant_scope(filtros, params, include_sede=False)
    if categoria:
        filtros.append("categoria = ?")
        params.append(categoria)
    query = f"SELECT nombre FROM productos WHERE {' AND '.join(filtros)} ORDER BY nombre"
    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [r["nombre"] for r in rows]


def obtener_productos_panaderia() -> list[str]:
    filtros = ["activo = 1", "es_panaderia = 1"]
    params: list = []
    _apply_tenant_scope(filtros, params, include_sede=False)
    with get_connection() as conn:
        rows = conn.execute(
            f"SELECT DISTINCT nombre FROM productos WHERE {' AND '.join(filtros)} ORDER BY nombre",
            tuple(params),
        ).fetchall()
    return [r["nombre"] for r in rows]


def obtener_productos_con_precio(categoria: str = None) -> list[dict]:
    filtros = ["activo = 1"]
    params: list = []
    _apply_tenant_scope(filtros, params, include_sede=False)
    if categoria:
        filtros.append("categoria = ?")
        params.append(categoria)
    with get_connection() as conn:
        _asegurar_surtido_tipo_productos_conn(conn)
        query = f"""
            SELECT id, nombre, precio, categoria, menu, descripcion,
                   es_panaderia, es_adicional, stock_minimo, surtido_tipo
            FROM productos
            WHERE {' AND '.join(filtros)}
            ORDER BY categoria, nombre
        """
        rows = conn.execute(query, tuple(params)).fetchall()
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
            """
            SELECT categoria
            FROM productos
            WHERE nombre = ? AND activo = 1
            ORDER BY es_panaderia DESC, id ASC
            LIMIT 1
            """,
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


def obtener_producto_por_id(producto_id: int) -> dict | None:
    if int(producto_id or 0) <= 0:
        return None
    with get_connection() as conn:
        _asegurar_surtido_tipo_productos_conn(conn)
        row = conn.execute("""
            SELECT id, nombre, precio, categoria, menu, descripcion,
                   es_panaderia, es_adicional, stock_minimo, surtido_tipo, activo
            FROM productos
            WHERE id = ?
        """, (int(producto_id),)).fetchone()
    return dict(row) if row else None


def _obtener_producto_por_nombre_categoria_conn(conn, nombre: str, categoria: str) -> dict | None:
    nombre = str(nombre or "").strip()
    categoria = str(categoria or "").strip() or "Panaderia"
    if not nombre:
        return None
    _asegurar_surtido_tipo_productos_conn(conn)
    row = conn.execute("""
        SELECT id, nombre, precio, categoria, menu, descripcion,
               es_panaderia, es_adicional, surtido_tipo, activo
        FROM productos
        WHERE nombre = ? AND categoria = ?
        ORDER BY activo DESC, id ASC
        LIMIT 1
    """, (nombre, categoria)).fetchone()
    return dict(row) if row else None


def agregar_producto(nombre: str, precio: float = 0.0, categoria: str = "Panaderia",
                     es_adicional: bool = False, menu: str = "",
                     descripcion: str = "", es_panaderia: bool | None = None,
                     surtido_tipo: str = "none") -> bool:
    nombre = str(nombre or "").strip()
    categoria = str(categoria or "").strip() or "Panaderia"
    menu = str(menu or "").strip()
    descripcion = str(descripcion or "").strip()
    es_panaderia_final = es_categoria_panaderia(categoria) if es_panaderia is None else bool(es_panaderia)
    surtido_tipo = _normalizar_surtido_tipo(surtido_tipo)
    if not nombre:
        return False
    try:
        with get_connection() as conn:
            _asegurar_surtido_tipo_productos_conn(conn)
            _insertar_categoria_conn(conn, categoria)
            conn.execute(
                "UPDATE categorias_producto SET activa = 1 WHERE nombre = ?",
                (categoria,)
            )

            existente = _obtener_producto_por_nombre_categoria_conn(conn, nombre, categoria)
            if existente:
                if int(existente.get("activo", 0) or 0) == 1:
                    return False
                conn.execute("""
                    UPDATE productos
                    SET precio = ?, menu = ?, descripcion = ?, es_panaderia = ?, es_adicional = ?,
                        surtido_tipo = ?, activo = 1
                    WHERE id = ?
                """, (
                    float(precio),
                    menu,
                    descripcion,
                    1 if es_panaderia_final else 0,
                    1 if es_adicional else 0,
                    surtido_tipo,
                    int(existente["id"]),
                ))
                conn.commit()
                return True

            conn.execute(
                """
                INSERT INTO productos (
                    nombre, precio, categoria, menu, descripcion,
                    es_panaderia, es_adicional, surtido_tipo
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    nombre,
                    precio,
                    categoria,
                    menu,
                    descripcion,
                    1 if es_panaderia_final else 0,
                    1 if es_adicional else 0,
                    surtido_tipo,
                )
            )
            conn.commit()
        return True
    except _INTEGRITY_ERRORS:
        return False


def _seleccionar_producto_existente_importacion(registros: list[dict], nombre: str, categoria: str) -> dict | None:
    nombre = str(nombre or "").strip()
    categoria = str(categoria or "").strip() or "Panaderia"
    clave_objetivo = (
        _normalizar_texto_clave(nombre),
        _normalizar_texto_clave(categoria),
    )
    nombre_lower = nombre.lower()
    categoria_lower = categoria.lower()

    coincidencias = [
        row for row in registros
        if (
            _normalizar_texto_clave(row.get("nombre", "")),
            _normalizar_texto_clave(row.get("categoria", "")),
        ) == clave_objetivo
    ]
    if not coincidencias:
        return None

    def prioridad(row: dict) -> tuple[int, int, int]:
        coincide_exacto = (
            str(row.get("nombre", "") or "").strip().lower() == nombre_lower
            and str(row.get("categoria", "") or "").strip().lower() == categoria_lower
        )
        esta_activo = bool(row.get("activo"))
        return (
            0 if coincide_exacto else 1,
            0 if esta_activo else 1,
            int(row.get("id", 0) or 0),
        )

    return min(coincidencias, key=prioridad)


def guardar_catalogo_productos(productos: list[dict], sincronizar: bool = False) -> dict:
    resultado = {
        "creados": 0,
        "actualizados": 0,
        "desactivados": 0,
    }

    with get_connection() as conn:
        _asegurar_surtido_tipo_productos_conn(conn)
        categorias_activas: set[str] = set()
        ids_sincronizados: set[int] = set()
        registros_existentes = [
            dict(row)
            for row in conn.execute("""
                SELECT id, nombre, categoria, es_adicional, menu, descripcion, es_panaderia, surtido_tipo, activo
                FROM productos
            """).fetchall()
        ]
        for producto in productos:
            nombre = producto["nombre"].strip()
            precio = float(producto["precio"])
            categoria = (producto.get("categoria") or "").strip()
            categoria_busqueda = categoria or "Panaderia"
            menu = str(producto.get("menu", "") or "").strip()
            descripcion = str(producto.get("descripcion", "") or "").strip()
            es_adicional = 1 if bool(producto.get("es_adicional")) else 0
            es_panaderia = 1 if bool(producto.get("es_panaderia", es_categoria_panaderia(categoria))) else 0
            surtido_tipo = _normalizar_surtido_tipo(producto.get("surtido_tipo"))
            categorias_activas.add(categoria_busqueda)

            existente = _seleccionar_producto_existente_importacion(
                registros_existentes,
                nombre,
                categoria_busqueda,
            )

            if existente:
                categoria_final = categoria or existente.get("categoria") or "Panaderia"
                es_adicional_final = es_adicional if "es_adicional" in producto else int(existente["es_adicional"] or 0)
                menu_final = menu or str(existente.get("menu") or "")
                descripcion_final = descripcion or str(existente.get("descripcion") or "")
                es_panaderia_final = es_panaderia if "es_panaderia" in producto or categoria else int(existente.get("es_panaderia") or 0)
                surtido_tipo_final = surtido_tipo if "surtido_tipo" in producto else _normalizar_surtido_tipo(existente.get("surtido_tipo"))
                _insertar_categoria_conn(conn, categoria_final)
                conn.execute("""
                    UPDATE productos
                    SET nombre = ?, precio = ?, categoria = ?, menu = ?, descripcion = ?,
                        es_panaderia = ?, es_adicional = ?, surtido_tipo = ?, activo = 1
                    WHERE id = ?
                """, (
                    nombre,
                    precio,
                    categoria_final,
                    menu_final,
                    descripcion_final,
                    es_panaderia_final,
                    es_adicional_final,
                    surtido_tipo_final,
                    existente["id"],
                ))
                existente.update({
                    "nombre": nombre,
                    "categoria": categoria_final,
                    "menu": menu_final,
                    "descripcion": descripcion_final,
                    "es_panaderia": es_panaderia_final,
                    "es_adicional": es_adicional_final,
                    "surtido_tipo": surtido_tipo_final,
                    "activo": 1,
                })
                ids_sincronizados.add(int(existente["id"]))
                resultado["actualizados"] += 1
            else:
                categoria_final = categoria_busqueda
                _insertar_categoria_conn(conn, categoria_final)
                cursor = conn.execute(
                    """
                    INSERT INTO productos (
                        nombre, precio, categoria, menu, descripcion,
                        es_panaderia, es_adicional, surtido_tipo, activo
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
                    """,
                    (
                        nombre,
                        precio,
                        categoria_final,
                        menu,
                        descripcion,
                        es_panaderia,
                        es_adicional,
                        surtido_tipo,
                    )
                )
                nuevo_id = int(cursor.lastrowid or 0)
                registros_existentes.append({
                    "id": nuevo_id,
                    "nombre": nombre,
                    "categoria": categoria_final,
                    "menu": menu,
                    "descripcion": descripcion,
                    "es_panaderia": es_panaderia,
                    "es_adicional": es_adicional,
                    "surtido_tipo": surtido_tipo,
                    "activo": 1,
                })
                if nuevo_id > 0:
                    ids_sincronizados.add(nuevo_id)
                resultado["creados"] += 1

        if sincronizar:
            activos = conn.execute("""
                SELECT id
                FROM productos
                WHERE activo = 1
            """).fetchall()
            ids_desactivar = [
                row["id"]
                for row in activos
                if int(row["id"] or 0) not in ids_sincronizados
            ]
            for producto_id in ids_desactivar:
                conn.execute("UPDATE productos SET activo = 0 WHERE id = ?", (producto_id,))
            resultado["desactivados"] = len(ids_desactivar)

            conn.execute("UPDATE categorias_producto SET activa = 0")
            for categoria_activa in sorted(categorias_activas):
                _insertar_categoria_conn(conn, categoria_activa)
                conn.execute(
                    "UPDATE categorias_producto SET activa = 1 WHERE nombre = ?",
                    (categoria_activa,)
                )

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
                                 categoria: str, es_adicional: bool,
                                 es_panaderia: bool | None = None,
                                 surtido_tipo: str = "none") -> bool:
    nombre = str(nombre or "").strip()
    categoria = str(categoria or "").strip() or "Panaderia"
    es_panaderia_final = es_categoria_panaderia(categoria) if es_panaderia is None else bool(es_panaderia)
    surtido_tipo = _normalizar_surtido_tipo(surtido_tipo)
    if producto_id <= 0 or not nombre:
        return False

    try:
        with get_connection() as conn:
            _asegurar_surtido_tipo_productos_conn(conn)
            actual = conn.execute(
                "SELECT nombre FROM productos WHERE id = ?",
                (producto_id,)
            ).fetchone()
            if not actual:
                return False

            nombre_anterior = str(actual["nombre"] or "")
            _insertar_categoria_conn(conn, categoria)
            conn.execute("""
                UPDATE productos
                SET nombre = ?, precio = ?, categoria = ?, es_panaderia = ?, es_adicional = ?, surtido_tipo = ?
                WHERE id = ?
            """, (
                nombre,
                float(precio),
                categoria,
                1 if es_panaderia_final else 0,
                1 if es_adicional else 0,
                surtido_tipo,
                producto_id,
            ))

            _renombrar_producto_referencias_conn(conn, nombre_anterior, nombre)
            conn.commit()
            return True
    except _INTEGRITY_ERRORS:
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
            _insertar_categoria_conn(conn, nueva_categoria)
            cur = conn.execute(
                "UPDATE productos SET categoria = ?, es_panaderia = ? WHERE nombre = ?",
                (nueva_categoria, 1 if es_categoria_panaderia(nueva_categoria) else 0, producto)
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

def obtener_panaderia_principal() -> dict:
    with get_connection() as conn:
        return obtener_panaderia_principal_conn(conn)


def obtener_sede_principal(panaderia_id: int | None = None) -> dict:
    with get_connection() as conn:
        tenant = obtener_panaderia_principal_conn(conn)
        return obtener_sede_principal_conn(conn, int(panaderia_id or tenant["id"]))


def obtener_branding_panaderia(panaderia_id: int | None = None) -> dict:
    with get_connection() as conn:
        tenant = obtener_panaderia_principal_conn(conn)
        tenant_id = int(panaderia_id or tenant["id"])
        row = conn.execute(
            """
            SELECT panaderia_id, brand_name, legal_name, tagline, support_label,
                   logo_path, favicon_path, primary_color, secondary_color, accent_color,
                   tax_label, tax_id, invoice_footer
            FROM tenant_branding
            WHERE panaderia_id = ?
            """,
            (tenant_id,),
        ).fetchone()
        if row:
            return dict(row)
    return {
        "panaderia_id": panaderia_id,
        "brand_name": "RICHS",
        "legal_name": "",
        "tagline": "Panaderia artesanal",
        "support_label": "Delicias que nutren",
        "logo_path": "brand/richs-logo.svg",
        "favicon_path": "brand/richs-logo.svg",
        "primary_color": "#8b5513",
        "secondary_color": "#d4722a",
        "accent_color": "#e0a142",
        "tax_label": "NIT",
        "tax_id": "",
        "invoice_footer": "",
    }


# ── Resolución de tenant / sede (Fase 1) ──────────────────────────────────────

def obtener_panaderia_por_id(panaderia_id: int) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id, slug, nombre, activa, dominio_custom, estado_operativo, created_by FROM panaderias WHERE id = ?",
            (panaderia_id,),
        ).fetchone()
    return dict(row) if row else None


def obtener_panaderia_por_slug(slug: str) -> dict | None:
    slug = str(slug or "").strip().lower()
    if not slug:
        return None
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id, slug, nombre, activa, dominio_custom, estado_operativo, created_by FROM panaderias WHERE slug = ?",
            (slug,),
        ).fetchone()
    return dict(row) if row else None


def obtener_panaderia_por_dominio(dominio: str) -> dict | None:
    dominio = str(dominio or "").strip().lower()
    if not dominio:
        return None
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT id, slug, nombre, activa, dominio_custom, estado_operativo, created_by
            FROM panaderias
            WHERE LOWER(dominio_custom) = ? AND dominio_custom != ''
            """,
            (dominio,),
        ).fetchone()
    return dict(row) if row else None


def obtener_sede_por_id(sede_id: int) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id, panaderia_id, slug, nombre, codigo, activa FROM sedes WHERE id = ?",
            (sede_id,),
        ).fetchone()
    return dict(row) if row else None


def obtener_sede_por_codigo(panaderia_id: int, codigo: str) -> dict | None:
    codigo = str(codigo or "").strip().upper()
    if not codigo:
        return None
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id, panaderia_id, slug, nombre, codigo, activa FROM sedes WHERE panaderia_id = ? AND UPPER(codigo) = ?",
            (panaderia_id, codigo),
        ).fetchone()
    return dict(row) if row else None


def obtener_sede_por_panaderia_y_slug(panaderia_id: int, slug: str) -> dict | None:
    slug = str(slug or "").strip().lower()
    if not slug:
        return None
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id, panaderia_id, slug, nombre, codigo, activa FROM sedes WHERE panaderia_id = ? AND slug = ?",
            (panaderia_id, slug),
        ).fetchone()
    return dict(row) if row else None


def obtener_sedes_de_panaderia(panaderia_id: int) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT id, panaderia_id, slug, nombre, codigo, activa FROM sedes WHERE panaderia_id = ? AND activa = 1",
            (panaderia_id,),
        ).fetchall()
    return [dict(r) for r in rows]


# ── Suscripciones comerciales (Fase 3) ───────────────────────────────────────

def obtener_suscripcion_panaderia(panaderia_id: int) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT id, panaderia_id, plan, estado, fecha_inicio, fecha_vencimiento,
                   max_sedes, max_usuarios, max_productos, notas, created_at, updated_at
            FROM tenant_subscriptions
            WHERE panaderia_id = ?
            """,
            (panaderia_id,),
        ).fetchone()
    return dict(row) if row else None


def crear_suscripcion(
    panaderia_id: int,
    plan: str = "free",
    estado: str = "activa",
    fecha_vencimiento: str | None = None,
    notas: str = "",
) -> bool:
    plan = plan if plan in PLAN_LIMITS else "free"
    limites = PLAN_LIMITS[plan]
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        with get_connection() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO tenant_subscriptions
                    (panaderia_id, plan, estado, fecha_inicio, fecha_vencimiento,
                     max_sedes, max_usuarios, max_productos, notas, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    panaderia_id, plan, estado, today, fecha_vencimiento,
                    limites["max_sedes"], limites["max_usuarios"], limites["max_productos"],
                    notas, now, now,
                ),
            )
            conn.commit()
        return True
    except Exception:
        return False


def actualizar_plan_suscripcion(
    panaderia_id: int,
    plan: str,
    estado: str = "activa",
    fecha_vencimiento: str | None = None,
    notas: str = "",
) -> bool:
    plan = plan if plan in PLAN_LIMITS else "free"
    limites = PLAN_LIMITS[plan]
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with get_connection() as conn:
            conn.execute(
                """
                UPDATE tenant_subscriptions
                SET plan = ?, estado = ?, fecha_vencimiento = ?,
                    max_sedes = ?, max_usuarios = ?, max_productos = ?,
                    notas = ?, updated_at = ?
                WHERE panaderia_id = ?
                """,
                (
                    plan, estado, fecha_vencimiento,
                    limites["max_sedes"], limites["max_usuarios"], limites["max_productos"],
                    notas, now, panaderia_id,
                ),
            )
            conn.commit()
        return True
    except Exception:
        return False


def verificar_limite_sedes(panaderia_id: int) -> dict:
    """Retorna cuántas sedes hay y el máximo permitido."""
    with get_connection() as conn:
        actual_row = conn.execute(
            "SELECT COUNT(*) AS total FROM sedes WHERE panaderia_id = ? AND activa = 1",
            (panaderia_id,),
        ).fetchone()
        sub = conn.execute(
            "SELECT max_sedes FROM tenant_subscriptions WHERE panaderia_id = ?",
            (panaderia_id,),
        ).fetchone()
    actual = int((actual_row["total"] if actual_row else 0) or 0)
    maximo = int(sub["max_sedes"]) if sub else PLAN_LIMITS["free"]["max_sedes"]
    return {"actual": actual, "maximo": maximo, "puede_agregar": actual < maximo}


def verificar_limite_usuarios(panaderia_id: int) -> dict:
    """Retorna cuántos usuarios activos hay y el máximo permitido."""
    with get_connection() as conn:
        actual_row = conn.execute(
            "SELECT COUNT(*) AS total FROM usuarios WHERE panaderia_id = ? AND activo = 1",
            (panaderia_id,),
        ).fetchone()
        sub = conn.execute(
            "SELECT max_usuarios FROM tenant_subscriptions WHERE panaderia_id = ?",
            (panaderia_id,),
        ).fetchone()
    actual = int((actual_row["total"] if actual_row else 0) or 0)
    maximo = int(sub["max_usuarios"]) if sub else PLAN_LIMITS["free"]["max_usuarios"]
    return {"actual": actual, "maximo": maximo, "puede_agregar": actual < maximo}


def verificar_limite_productos(panaderia_id: int) -> dict:
    """Retorna cuántos productos activos hay y el máximo permitido."""
    with get_connection() as conn:
        actual_row = conn.execute(
            "SELECT COUNT(*) AS total FROM productos WHERE panaderia_id = ? AND activo = 1",
            (panaderia_id,),
        ).fetchone()
        sub = conn.execute(
            "SELECT max_productos FROM tenant_subscriptions WHERE panaderia_id = ?",
            (panaderia_id,),
        ).fetchone()
    actual = int((actual_row["total"] if actual_row else 0) or 0)
    maximo = int(sub["max_productos"]) if sub else PLAN_LIMITS["free"]["max_productos"]
    return {"actual": actual, "maximo": maximo, "puede_agregar": actual < maximo}


def obtener_estado_login_attempts(scope_key: str) -> dict:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT scope_key, attempts, locked_until, updated_at FROM login_attempts WHERE scope_key = ?",
            (scope_key,),
        ).fetchone()
    return dict(row) if row else {"scope_key": scope_key, "attempts": 0, "locked_until": "", "updated_at": ""}


def limpiar_login_attempts(scope_keys: list[str]) -> None:
    keys = [str(key or "").strip() for key in scope_keys if str(key or "").strip()]
    if not keys:
        return
    placeholders = ", ".join(["?"] * len(keys))
    with get_connection() as conn:
        conn.execute(f"DELETE FROM login_attempts WHERE scope_key IN ({placeholders})", tuple(keys))
        conn.commit()


def registrar_login_attempts_fallido(scope_key: str, max_attempts: int, lockout_minutes: int) -> dict:
    scope_key = str(scope_key or "").strip()
    if not scope_key:
        return {"attempts": 0, "locked_until": ""}
    now = datetime.now()
    now_txt = now.strftime("%Y-%m-%d %H:%M:%S")
    with get_connection() as conn:
        row = conn.execute(
            "SELECT attempts, locked_until FROM login_attempts WHERE scope_key = ?",
            (scope_key,),
        ).fetchone()
        attempts = int((row["attempts"] or 0) if row else 0) + 1
        locked_until = str((row["locked_until"] or "") if row else "")
        if attempts >= max(1, int(max_attempts or 1)):
            locked_until = (now + timedelta(minutes=max(1, int(lockout_minutes or 1)))).strftime("%Y-%m-%d %H:%M:%S")
            attempts = 0
        conn.execute("DELETE FROM login_attempts WHERE scope_key = ?", (scope_key,))
        conn.execute(
            """
            INSERT INTO login_attempts (scope_key, attempts, locked_until, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (scope_key, attempts, locked_until, now_txt),
        )
        conn.commit()
    return {"attempts": attempts, "locked_until": locked_until}


# ── Membresías (Fase 2) ───────────────────────────────────────────────────────

def _obtener_membresia_conn(conn, usuario_id: int, panaderia_id: int) -> dict | None:
    row = conn.execute(
        """
        SELECT id, usuario_id, panaderia_id, sede_id, rol, activa, invited_by, created_at
        FROM tenant_memberships
        WHERE usuario_id = ? AND panaderia_id = ? AND activa = 1
        """,
        (usuario_id, panaderia_id),
    ).fetchone()
    return dict(row) if row else None


def obtener_membresias_usuario(usuario_id: int) -> list[dict]:
    """Lista todas las membresías activas de un usuario (puede ser en varios tenants)."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT tm.id, tm.usuario_id, tm.panaderia_id, tm.sede_id, tm.rol, tm.activa,
                   p.nombre AS panaderia_nombre, p.slug AS panaderia_slug,
                   s.nombre AS sede_nombre, s.codigo AS sede_codigo
            FROM tenant_memberships tm
            JOIN panaderias p ON p.id = tm.panaderia_id
            JOIN sedes s      ON s.id = tm.sede_id
            WHERE tm.usuario_id = ? AND tm.activa = 1
            ORDER BY tm.panaderia_id, tm.sede_id
            """,
            (usuario_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def crear_membresia(
    usuario_id: int,
    panaderia_id: int,
    sede_id: int,
    rol: str,
    invited_by: int | None = None,
) -> bool:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rol = normalize_role(rol, fallback="cajero")
    try:
        with get_connection() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO tenant_memberships
                    (usuario_id, panaderia_id, sede_id, rol, activa, invited_by, created_at)
                VALUES (?, ?, ?, ?, 1, ?, ?)
                """,
                (usuario_id, panaderia_id, sede_id, rol, invited_by, now),
            )
            conn.commit()
        return True
    except Exception:
        return False


def actualizar_membresia_rol(usuario_id: int, panaderia_id: int, sede_id: int, rol: str) -> bool:
    """Actualiza el rol en la membresía existente o la crea si no existe."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rol = normalize_role(rol, fallback="cajero")
    try:
        with get_connection() as conn:
            conn.execute(
                """
                INSERT INTO tenant_memberships
                    (usuario_id, panaderia_id, sede_id, rol, activa, created_at)
                VALUES (?, ?, ?, ?, 1, ?)
                ON CONFLICT(usuario_id, panaderia_id, sede_id) DO UPDATE SET
                    rol    = excluded.rol,
                    activa = 1
                """,
                (usuario_id, panaderia_id, sede_id, rol, now),
            )
            conn.commit()
        return True
    except Exception:
        return False


def desactivar_membresia(usuario_id: int, panaderia_id: int) -> bool:
    try:
        with get_connection() as conn:
            conn.execute(
                "UPDATE tenant_memberships SET activa = 0 WHERE usuario_id = ? AND panaderia_id = ?",
                (usuario_id, panaderia_id),
            )
            # Invalidar sesiones activas del usuario en este tenant
            conn.execute(
                "UPDATE usuarios SET session_version = COALESCE(session_version, 0) + 1 WHERE id = ? AND panaderia_id = ?",
                (usuario_id, panaderia_id),
            )
            conn.commit()
        return True
    except Exception:
        return False


_USUARIO_SELECT = """
    SELECT id, nombre, rol, username, email, panaderia_id, sede_id,
           activo, locked_until, must_change_password,
           jornada_activa, jornada_activada_en, jornada_activada_por,
           COALESCE(session_version, 0) AS session_version
    FROM usuarios
"""


def _check_locked(row) -> bool:
    """Retorna True si el usuario está bloqueado."""
    locked_until = str(row["locked_until"] or "").strip()
    if locked_until:
        try:
            if datetime.strptime(locked_until, "%Y-%m-%d %H:%M:%S") > datetime.now():
                return True
        except ValueError:
            pass
    return False


def _enriquecer_con_membresia(conn, usuario: dict) -> dict:
    """Adjunta datos de membresía sin convertirla en la fuente principal del login."""
    usuario_id = usuario.get("id")
    panaderia_id = usuario.get("panaderia_id")
    if not usuario_id or not panaderia_id:
        return usuario
    membresia = _obtener_membresia_conn(conn, int(usuario_id), int(panaderia_id))
    if not membresia:
        return usuario
    resultado = dict(usuario)
    resultado["membership_id"] = membresia["id"]
    resultado["membership_rol"] = membresia["rol"]
    resultado["membership_panaderia_id"] = membresia["panaderia_id"]
    resultado["membership_sede_id"] = membresia["sede_id"]
    return resultado


def verificar_pin(pin: str) -> dict | None:
    """Verifica un PIN y retorna el usuario si es valido."""
    pin_normalizado = str(pin or "").strip()
    pin_hash = _hash_pin(pin_normalizado)
    pin_double_hash = _hash_pin(pin_hash)
    with get_connection() as conn:
        row = conn.execute(
            _USUARIO_SELECT + "WHERE activo = 1 AND (pin_hash = ? OR pin = ?)",
            (pin_hash, pin_hash)
        ).fetchone()
        if row:
            if _check_locked(row):
                return None
            return dict(row)

        row_legacy = conn.execute(
            _USUARIO_SELECT + "WHERE activo = 1 AND pin = ?",
            (pin_normalizado,)
        ).fetchone()
        if row_legacy:
            conn.execute(
                "UPDATE usuarios SET pin = ?, pin_hash = ? WHERE id = ?",
                (pin_hash, pin_hash, row_legacy["id"])
            )
            return dict(row_legacy)

        row_double = conn.execute(
            _USUARIO_SELECT + "WHERE activo = 1 AND (pin_hash = ? OR pin = ?)",
            (pin_double_hash, pin_double_hash)
        ).fetchone()
        if row_double:
            conn.execute(
                "UPDATE usuarios SET pin = ?, pin_hash = ? WHERE id = ?",
                (pin_hash, pin_hash, row_double["id"])
            )
            return dict(row_double)
    return None


def verificar_password(username_or_email: str, password: str) -> dict | None:
    """Verifica username/email + contraseña. Retorna el usuario si es valido."""
    identifier = str(username_or_email or "").strip().lower()
    pwd = str(password or "")
    if not identifier or not pwd:
        return None
    with get_connection() as conn:
        # Obtener hash por separado (no está en _USUARIO_SELECT por seguridad)
        hash_row = conn.execute(
            """
            SELECT id, password_hash FROM usuarios
            WHERE activo = 1
              AND password_hash IS NOT NULL AND password_hash != ''
              AND (LOWER(username) = ? OR LOWER(COALESCE(email,'')) = ?)
            """,
            (identifier, identifier)
        ).fetchone()
        if not hash_row:
            rows_by_name = conn.execute(
                """
                SELECT id, password_hash FROM usuarios
                WHERE activo = 1
                  AND password_hash IS NOT NULL AND password_hash != ''
                  AND LOWER(COALESCE(nombre, '')) = ?
                ORDER BY id ASC
                """,
                (identifier,),
            ).fetchall()
            if len(rows_by_name) == 1:
                hash_row = rows_by_name[0]
        if not hash_row:
            return None
        if not check_password_hash(hash_row["password_hash"], pwd):
            return None
        # Cargar el resto del perfil con el SELECT estándar
        row = conn.execute(
            _USUARIO_SELECT + "WHERE id = ?",
            (hash_row["id"],)
        ).fetchone()
        if not row:
            return None
        if _check_locked(row):
            return None
        return _enriquecer_con_membresia(conn, dict(row))


def registrar_login_exitoso(usuario_id: int, terminal_id: int | None = None) -> None:
    ahora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE usuarios
               SET last_login_at = ?,
                   ultimo_acceso_en = ?,
                   ultimo_acceso_terminal_id = ?,
                   failed_login_count = 0,
                   locked_until = ''
             WHERE id = ?
            """,
            (ahora, ahora, terminal_id, int(usuario_id)),
        )
        conn.commit()


def obtener_configuracion_login_operativo(panaderia_id: int | None = None) -> dict:
    with get_connection() as conn:
        tenant = obtener_panaderia_principal_conn(conn)
        tenant_id = int(panaderia_id or tenant["id"])
        sedes_activas_row = conn.execute(
            "SELECT COUNT(*) AS total FROM sedes WHERE panaderia_id = ? AND activa = 1",
            (tenant_id,),
        ).fetchone()
        sedes_activas = int((sedes_activas_row["total"] if sedes_activas_row else 0) or 0)
    return {
        "panaderia_id": tenant_id,
        "sedes_activas": sedes_activas,
        "sede_unica": sedes_activas <= 1,
        "requiere_username": sedes_activas > 1,
    }


_OPERATIVO_LOGIN_SELECT = """
    SELECT u.id, u.nombre, u.rol, u.username, u.email,
           u.panaderia_id, u.sede_id, u.activo, u.locked_until,
           u.must_change_password, u.jornada_activa,
           u.jornada_activada_en, u.jornada_activada_por,
           COALESCE(u.session_version, 0) AS session_version
    FROM usuarios u
"""


def listar_operativos_activos_por_pin(panaderia_id: int, pin: str) -> list[dict]:
    pin_norm = str(pin or "").strip()
    if not panaderia_id or not pin_norm:
        return []
    pin_hash = _hash_pin(pin_norm)
    pin_lookup_digest = _pin_lookup_digest(pin_norm)
    with get_connection() as conn:
        rows = conn.execute(
            _OPERATIVO_LOGIN_SELECT + """
            WHERE u.activo = 1
              AND u.jornada_activa = 1
              AND u.rol IN ('cajero', 'mesero')
              AND u.panaderia_id = ?
              AND (
                    u.pin_lookup_digest = ?
                    OR (COALESCE(u.pin_lookup_digest, '') = '' AND (u.pin_hash = ? OR u.pin = ?))
              )
            ORDER BY u.id ASC
            """,
            (int(panaderia_id), pin_lookup_digest, pin_hash, pin_hash),
        ).fetchall()
        resultados: list[dict] = []
        for row in rows:
            if _check_locked(row):
                continue
            resultados.append(_enriquecer_con_membresia(conn, dict(row)))
        return resultados


def verificar_usuario_operativo_local(panaderia_id: int, username: str, pin: str) -> dict | None:
    username_norm = str(username or "").strip().lower()
    pin_norm = str(pin or "").strip()
    if not panaderia_id or not username_norm or not pin_norm:
        return None
    pin_hash = _hash_pin(pin_norm)
    pin_lookup_digest = _pin_lookup_digest(pin_norm)
    with get_connection() as conn:
        rows = conn.execute(
            _OPERATIVO_LOGIN_SELECT + """
            WHERE u.activo = 1
              AND u.jornada_activa = 1
              AND u.rol IN ('cajero', 'mesero')
              AND u.panaderia_id = ?
              AND (
                    LOWER(COALESCE(u.username, '')) = ?
                    OR LOWER(COALESCE(u.nombre, '')) = ?
              )
              AND (
                    u.pin_lookup_digest = ?
                    OR (COALESCE(u.pin_lookup_digest, '') = '' AND (u.pin_hash = ? OR u.pin = ?))
              )
            ORDER BY u.id ASC
            """,
            (int(panaderia_id), username_norm, username_norm, pin_lookup_digest, pin_hash, pin_hash),
        ).fetchall()
        usuarios = [
            _enriquecer_con_membresia(conn, dict(row))
            for row in rows
            if not _check_locked(row)
        ]
        if len(usuarios) != 1:
            return None
        return usuarios[0]


def diagnosticar_login_operativo_local(
    panaderia_id: int,
    pin: str,
    username: str = "",
    requiere_username: bool = False,
) -> dict:
    username_norm = _normalizar_username(username)
    pin_norm = str(pin or "").strip()
    if not panaderia_id or not pin_norm:
        return {"status": "invalid"}

    pin_hash = _hash_pin(pin_norm)
    pin_lookup_digest = _pin_lookup_digest(pin_norm)
    filtros = [
        "u.activo = 1",
        "u.rol IN ('cajero', 'mesero')",
        "u.panaderia_id = ?",
        "(u.pin_lookup_digest = ? OR (COALESCE(u.pin_lookup_digest, '') = '' AND (u.pin_hash = ? OR u.pin = ?)))",
    ]
    params: list = [int(panaderia_id), pin_lookup_digest, pin_hash, pin_hash]
    if requiere_username:
        filtros.append("(LOWER(COALESCE(u.username, '')) = ? OR LOWER(COALESCE(u.nombre, '')) = ?)")
        params.extend([username_norm, username_norm])
    with get_connection() as conn:
        rows = conn.execute(
            _OPERATIVO_LOGIN_SELECT + f"""
            WHERE {' AND '.join(filtros)}
            ORDER BY u.id ASC
            """,
            tuple(params),
        ).fetchall()
        usuarios = [
            _enriquecer_con_membresia(conn, dict(row))
            for row in rows
            if not _check_locked(row)
        ]

    if not usuarios:
        return {"status": "invalid"}
    if requiere_username and len(usuarios) > 1:
        return {"status": "identificador_duplicado", "usuarios": usuarios}
    if not requiere_username and len(usuarios) > 1:
        activos = [u for u in usuarios if int(u.get("jornada_activa", 0) or 0) == 1]
        if len(activos) > 1:
            return {"status": "pin_duplicado", "usuarios": activos}
        if len(activos) == 1:
            return {"status": "ok", "usuario": activos[0]}
        return {"status": "jornada_cerrada", "usuarios": usuarios}

    usuario = usuarios[0]
    if int(usuario.get("jornada_activa", 0) or 0) != 1:
        return {"status": "jornada_cerrada", "usuario": usuario}
    return {"status": "ok", "usuario": usuario}


def listar_panaderias_plataforma() -> list[dict]:
    """Lista todas las panaderías con métricas de suscripción para el panel de plataforma."""
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT
                p.id, p.slug, p.nombre, p.activa, p.estado_operativo, p.codigo, p.created_at,
                COALESCE(ts.plan, 'free')     AS plan,
                COALESCE(ts.estado, 'activa') AS estado_suscripcion,
                ts.fecha_vencimiento,
                COALESCE(ts.max_sedes,     1) AS max_sedes,
                COALESCE(ts.max_usuarios,  5) AS max_usuarios,
                COALESCE(ts.max_productos,50) AS max_productos,
                (SELECT COUNT(*) FROM sedes s WHERE s.panaderia_id = p.id AND s.activa = 1)   AS total_sedes,
                (SELECT COUNT(*) FROM usuarios u WHERE u.panaderia_id = p.id AND u.activo = 1) AS total_usuarios,
                (SELECT COUNT(*) FROM productos pr WHERE pr.panaderia_id = p.id AND pr.activo = 1) AS total_productos
            FROM panaderias p
            LEFT JOIN tenant_subscriptions ts ON ts.panaderia_id = p.id
            ORDER BY p.nombre
        """).fetchall()
    return [dict(r) for r in rows]


def obtener_panaderia_por_codigo(codigo: str) -> dict | None:
    """Retorna datos básicos de una panadería por su código corto (para el login operativo)."""
    codigo_norm = str(codigo or "").strip().upper()
    if not codigo_norm:
        return None
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id, slug, nombre, activa, codigo FROM panaderias WHERE UPPER(codigo) = ? AND activa = 1",
            (codigo_norm,)
        ).fetchone()
    return dict(row) if row else None


def verificar_pin_operativo(codigo_panaderia: str, username: str, pin: str) -> dict | None:
    """Login operativo para cajero/mesero: codigo de panaderia + username + PIN 4 digitos.

    Retorna el usuario si:
      - La panaderia existe y esta activa
      - El usuario pertenece a esa panaderia, esta activo y tiene jornada_activa = 1
      - El PIN coincide
      - El usuario tiene rol cajero o mesero
    """
    codigo_norm = str(codigo_panaderia or "").strip().upper()
    username_norm = str(username or "").strip().lower()
    pin_norm = str(pin or "").strip()

    if not codigo_norm or not username_norm or not pin_norm:
        return None

    pin_hash = _hash_pin(pin_norm)

    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT u.id, u.nombre, u.rol, u.username, u.email,
                   u.panaderia_id, u.sede_id, u.activo, u.locked_until,
                   u.must_change_password, u.jornada_activa,
                   u.jornada_activada_en, u.jornada_activada_por
            FROM usuarios u
            JOIN panaderias p ON p.id = u.panaderia_id
            WHERE u.activo = 1
              AND u.jornada_activa = 1
              AND u.rol IN ('cajero', 'mesero')
              AND UPPER(p.codigo) = ?
              AND LOWER(u.username) = ?
              AND (u.pin_hash = ? OR u.pin = ?)
              AND p.activa = 1
            """,
            (codigo_norm, username_norm, pin_hash, pin_hash)
        ).fetchone()
        if not row:
            return None
        if _check_locked(row):
            return None
        return _enriquecer_con_membresia(conn, dict(row))


def verificar_pin_en_terminal(terminal_codigo: str, username: str, pin: str) -> dict | None:
    """Login operativo usando la terminal como contexto (sin código de panadería).

    Flujo: terminal_codigo → panaderia_id/sede_id → autenticar username+PIN.
    Requiere que el usuario tenga jornada_activa y membresía activa en ese tenant.
    """
    codigo_norm = str(terminal_codigo or "").strip().upper()
    username_norm = str(username or "").strip().lower()
    pin_norm = str(pin or "").strip()
    if not codigo_norm or not username_norm or not pin_norm:
        return None

    pin_hash = _hash_pin(pin_norm)

    with get_connection() as conn:
        # Primero buscar la terminal por código (puede haber varias con el mismo código en sedes distintas,
        # pero el código está globalmente en localStorage, así que buscamos la terminal activa que tiene
        # al usuario en su panaderia/sede)
        terminal_row = conn.execute(
            """
            SELECT t.id AS terminal_id, t.panaderia_id, t.sede_id, t.nombre AS terminal_nombre,
                   t.codigo AS terminal_codigo, t.tipo,
                   s.nombre AS sede_nombre, p.nombre AS panaderia_nombre, p.codigo AS panaderia_codigo
            FROM terminales t
            JOIN sedes s      ON s.id = t.sede_id
            JOIN panaderias p ON p.id = t.panaderia_id
            WHERE UPPER(t.codigo) = ?
              AND t.activa = 1
              AND p.activa = 1
            LIMIT 1
            """,
            (codigo_norm,),
        ).fetchone()
        if not terminal_row:
            return None

        panaderia_id = terminal_row["panaderia_id"]
        sede_id = terminal_row["sede_id"]

        row = conn.execute(
            """
            SELECT u.id, u.nombre, u.rol, u.username, u.email,
                   u.panaderia_id, u.sede_id, u.activo, u.locked_until,
                   u.must_change_password, u.jornada_activa,
                   u.jornada_activada_en, u.jornada_activada_por,
                   COALESCE(u.session_version, 0) AS session_version
            FROM usuarios u
            WHERE u.activo = 1
              AND u.jornada_activa = 1
              AND u.rol IN ('cajero', 'mesero')
              AND u.panaderia_id = ?
              AND u.sede_id = ?
              AND LOWER(u.username) = ?
              AND (u.pin_hash = ? OR u.pin = ?)
            """,
            (panaderia_id, sede_id, username_norm, pin_hash, pin_hash),
        ).fetchone()
        if not row:
            return None
        if _check_locked(row):
            return None

        result = _enriquecer_con_membresia(conn, dict(row))

        # Enriquecer con datos de la terminal
        result["terminal_id"] = terminal_row["terminal_id"]
        result["terminal_codigo"] = terminal_row["terminal_codigo"]
        result["terminal_nombre"] = terminal_row["terminal_nombre"]
        result["sede_nombre"] = terminal_row["sede_nombre"]
        result["panaderia_nombre"] = terminal_row["panaderia_nombre"]
        return result


def obtener_terminal_lookup(codigo: str) -> dict | None:
    """Retorna información pública de una terminal para el lookup del login."""
    codigo_norm = str(codigo or "").strip().upper()
    if not codigo_norm:
        return None
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT t.id, t.codigo, t.nombre AS terminal_nombre, t.tipo,
                   s.id AS sede_id, s.nombre AS sede_nombre,
                   p.id AS panaderia_id, p.nombre AS panaderia_nombre, p.codigo AS panaderia_codigo
            FROM terminales t
            JOIN sedes s      ON s.id = t.sede_id
            JOIN panaderias p ON p.id = t.panaderia_id
            WHERE UPPER(t.codigo) = ?
              AND t.activa = 1
              AND p.activa = 1
            LIMIT 1
            """,
            (codigo_norm,),
        ).fetchone()
    return dict(row) if row else None


# ── Jornada laboral ───────────────────────────────────────────────────────────

def obtener_usuarios_jornada(panaderia_id: int, sede_id: int | None = None) -> list[dict]:
    """Lista cajeros y meseros de una panadería/sede con su estado de jornada."""
    filtros = ["u.rol IN ('cajero', 'mesero')", "u.activo = 1", "u.panaderia_id = ?"]
    params: list = [panaderia_id]
    if sede_id is not None:
        filtros.append("u.sede_id = ?")
        params.append(sede_id)
    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT u.id, u.nombre, u.username, u.rol, u.sede_id,
                   u.jornada_activa, u.jornada_activada_en, u.jornada_activada_por,
                   s.nombre AS sede_nombre
            FROM usuarios u
            LEFT JOIN sedes s ON s.id = u.sede_id
            WHERE {' AND '.join(filtros)}
            ORDER BY u.rol, u.nombre
            """,
            tuple(params)
        ).fetchall()
    return [dict(r) for r in rows]


def activar_jornada_usuario(usuario_id: int, activado_por: str, panaderia_id: int) -> bool:
    """Activa la jornada de un usuario específico."""
    ahora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE usuarios
               SET jornada_activa = 1,
                   jornada_activada_en = ?,
                   jornada_activada_por = ?
             WHERE id = ?
               AND panaderia_id = ?
               AND rol IN ('cajero', 'mesero')
            """,
            (ahora, str(activado_por or ""), usuario_id, panaderia_id)
        )
        conn.commit()
    return True


def desactivar_jornada_usuario(usuario_id: int, panaderia_id: int) -> bool:
    """Desactiva la jornada de un usuario específico."""
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE usuarios
               SET jornada_activa = 0,
                   jornada_activada_en = '',
                   jornada_activada_por = '',
                   session_version = COALESCE(session_version, 0) + 1
             WHERE id = ?
               AND panaderia_id = ?
               AND rol IN ('cajero', 'mesero')
            """,
            (usuario_id, panaderia_id)
        )
        conn.commit()
    return True


def abrir_jornada_sede(panaderia_id: int, sede_id: int, activado_por: str) -> int:
    """Activa la jornada de todos los cajeros/meseros de una sede. Retorna cantidad activados."""
    ahora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_connection() as conn:
        cur = conn.execute(
            """
            UPDATE usuarios
               SET jornada_activa = 1,
                   jornada_activada_en = ?,
                   jornada_activada_por = ?
             WHERE panaderia_id = ?
               AND sede_id = ?
               AND rol IN ('cajero', 'mesero')
               AND activo = 1
            """,
            (ahora, str(activado_por or ""), panaderia_id, sede_id)
        )
        conn.commit()
        return cur.rowcount if hasattr(cur, "rowcount") else 0


def cerrar_jornada_sede(panaderia_id: int, sede_id: int) -> int:
    """Desactiva la jornada de todos los cajeros/meseros de una sede. Retorna cantidad cerrados."""
    with get_connection() as conn:
        cur = conn.execute(
            """
            UPDATE usuarios
               SET jornada_activa = 0,
                   jornada_activada_en = '',
                   jornada_activada_por = '',
                   session_version = COALESCE(session_version, 0) + 1
             WHERE panaderia_id = ?
               AND sede_id = ?
               AND rol IN ('cajero', 'mesero')
            """,
            (panaderia_id, sede_id)
        )
        conn.commit()
        return cur.rowcount if hasattr(cur, "rowcount") else 0


def cambiar_password_usuario(usuario_id: int, nueva_password: str) -> bool:
    """Cambia la contraseña de un usuario y limpia must_change_password."""
    nueva_password = str(nueva_password or "").strip()
    if not nueva_password or len(nueva_password) < 8:
        return False
    nuevo_hash = _hash_password(nueva_password)
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE usuarios
               SET password_hash = ?, must_change_password = 0
             WHERE id = ?
            """,
            (nuevo_hash, int(usuario_id))
        )
        conn.commit()
    return True


def obtener_usuarios() -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, nombre, rol, username, email, activo, panaderia_id, sede_id
            FROM usuarios
            ORDER BY rol, nombre
            """
        ).fetchall()
    return [dict(r) for r in rows]


def obtener_usuarios_panaderia(panaderia_id: int) -> list[dict]:
    """Lista todos los usuarios de la panadería local con sus datos operativos."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT u.id, u.nombre, u.rol, u.username, u.email,
                   u.activo, u.jornada_activa,
                   u.panaderia_id, u.sede_id,
                   u.last_login_at,
                   COALESCE(u.ultimo_acceso_en, u.last_login_at, '') AS ultimo_acceso_en,
                   u.ultimo_acceso_terminal_id,
                   s.nombre AS sede_nombre,
                   t.nombre AS ultimo_acceso_terminal_nombre
            FROM usuarios u
            LEFT JOIN sedes s ON s.id = u.sede_id
            LEFT JOIN terminales t ON t.id = u.ultimo_acceso_terminal_id
            WHERE u.panaderia_id = ?
            ORDER BY u.activo DESC, u.rol, u.nombre
            """,
            (panaderia_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def obtener_terminales_panaderia(panaderia_id: int) -> list[dict]:
    """Lista todas las terminales de una panadería con info de sede."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT t.id, t.codigo, t.nombre, t.tipo, t.activa,
                   t.sede_id, s.nombre AS sede_nombre, t.last_seen_at
            FROM terminales t
            JOIN sedes s ON s.id = t.sede_id
            WHERE t.panaderia_id = ?
            ORDER BY s.nombre, t.nombre
            """,
            (panaderia_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def _pin_operativo_ya_asignado_conn(
    conn,
    panaderia_id: int,
    pin_lookup_digest: str,
    exclude_usuario_id: int | None = None,
) -> bool:
    if not panaderia_id or not pin_lookup_digest:
        return False
    sql = """
        SELECT 1
        FROM usuarios
        WHERE activo = 1
          AND panaderia_id = ?
          AND rol IN ('cajero', 'mesero')
          AND pin_lookup_digest = ?
    """
    params: list = [panaderia_id, pin_lookup_digest]
    if exclude_usuario_id:
        sql += " AND id != ?"
        params.append(int(exclude_usuario_id))
    row = conn.execute(sql + " LIMIT 1", tuple(params)).fetchone()
    return bool(row)


def _resolver_sede_usuario_conn(conn, panaderia_id: int, sede_id: int | None = None) -> dict:
    sede = None
    if sede_id:
        sede = conn.execute(
            "SELECT id, nombre FROM sedes WHERE id = ? AND panaderia_id = ?",
            (int(sede_id), int(panaderia_id)),
        ).fetchone()
    if sede is None:
        sede = obtener_sede_principal_conn(conn, panaderia_id)
    if not sede:
        raise ValueError("No se encontro una sede valida para el usuario")
    return dict(sede)


def _verificar_autorizador_precio_conn(
    conn,
    panaderia_id: int,
    autorizado_por: str,
    pin: str,
) -> dict | None:
    autorizado_norm = _normalizar_username(autorizado_por)
    pin_norm = str(pin or "").strip()
    if not autorizado_norm or not pin_norm:
        return None
    pin_hash = _hash_pin(pin_norm)
    pin_lookup_digest = _pin_lookup_digest(pin_norm)
    row = conn.execute(
        """
        SELECT id, nombre, username, rol
        FROM usuarios
        WHERE activo = 1
          AND panaderia_id = ?
          AND rol IN ('tenant_admin', 'panadero', 'platform_superadmin')
          AND (
                LOWER(COALESCE(username, '')) = ?
                OR LOWER(COALESCE(nombre, '')) = ?
          )
          AND (
                pin_lookup_digest = ?
                OR (COALESCE(pin_lookup_digest, '') = '' AND (pin_hash = ? OR pin = ?))
          )
        LIMIT 1
        """,
        (int(panaderia_id), autorizado_norm, autorizado_norm, pin_lookup_digest, pin_hash, pin_hash),
    ).fetchone()
    return _row_to_dict(row) or None


def agregar_usuario(
    nombre: str,
    pin: str,
    rol: str,
    username: str = "",
    sede_id: int | None = None,
) -> dict:
    nombre = str(nombre or "").strip()
    rol = normalize_role(rol, fallback="cajero")
    username = str(username or "").strip()
    if not nombre or not pin:
        return {"ok": False, "error": "Nombre y PIN son obligatorios"}
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pin_hash = _hash_pin(pin)
    pin_lookup_digest = _pin_lookup_digest(pin)
    try:
        with get_connection() as conn:
            tenant = obtener_panaderia_principal_conn(conn)
            sede = _resolver_sede_usuario_conn(conn, tenant["id"], sede_id=sede_id)
            if rol in ("cajero", "mesero") and _pin_operativo_ya_asignado_conn(conn, tenant["id"], pin_lookup_digest):
                return {
                    "ok": False,
                    "error": "Ese PIN ya está en uso por otro usuario operativo activo. Usa uno distinto.",
                }
            username_final = _crear_username_unico_conn(conn, nombre, username=username)
            if username and username_final != _normalizar_username(username):
                return {"ok": False, "error": "Ese username ya existe. Usa uno distinto."}
            conn.execute(
                """
                INSERT INTO usuarios (
                    nombre, pin, rol, username, email, password_hash, pin_hash, pin_lookup_digest,
                    activo, must_change_password, panaderia_id, sede_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 1, ?, ?)
                """,
                (
                    nombre,
                    pin_hash,
                    rol,
                    username_final,
                    _email_local_para_username(username_final),
                    _hash_password(pin),
                    pin_hash,
                    pin_lookup_digest,
                    tenant["id"],
                    sede["id"],
                )
            )
            # Crear membresía para el nuevo usuario
            usuario_id = conn.execute(
                "SELECT id FROM usuarios WHERE username = ? AND panaderia_id = ?",
                (username_final, tenant["id"]),
            ).fetchone()
            if usuario_id:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO tenant_memberships
                        (usuario_id, panaderia_id, sede_id, rol, activa, created_at)
                    VALUES (?, ?, ?, ?, 1, ?)
                    """,
                    (usuario_id["id"], tenant["id"], sede["id"], rol, now),
                )
            conn.commit()
        return {"ok": True, "username": username_final, "sede_id": sede["id"]}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def actualizar_usuario(
    usuario_id: int,
    nombre: str,
    rol: str,
    pin: str = "",
    username: str = "",
    sede_id: int | None = None,
) -> dict:
    nombre = str(nombre or "").strip()
    rol = normalize_role(rol, fallback="cajero")
    pin = str(pin or "").strip()
    username = str(username or "").strip()
    if not nombre or not username or rol not in VALID_ROLES:
        return {"ok": False, "error": "Datos del usuario inválidos"}
    if pin and len(pin) < 4:
        return {"ok": False, "error": "El PIN debe tener al menos 4 dígitos"}

    try:
        with get_connection() as conn:
            prev = conn.execute(
                """
                SELECT rol, panaderia_id, pin_lookup_digest, pin_hash, pin, sede_id, username
                FROM usuarios
                WHERE id = ?
                """,
                (usuario_id,),
            ).fetchone()
            if not prev:
                return {"ok": False, "error": "Usuario no encontrado"}
            rol_cambio = str(prev["rol"] or "") != rol
            panaderia_id = int(prev["panaderia_id"] or 0)
            sede = _resolver_sede_usuario_conn(conn, panaderia_id, sede_id=sede_id)
            username_norm = _normalizar_username(username)
            if _username_ya_asignado_conn(conn, username_norm, exclude_usuario_id=usuario_id):
                return {"ok": False, "error": "Ese username ya existe. Usa uno distinto."}
            pin_lookup_digest = (
                _pin_lookup_digest(pin)
                if pin
                else _pin_lookup_digest_desde_fila(prev["pin_hash"], prev["pin"])
            )
            if rol in ("cajero", "mesero") and _pin_operativo_ya_asignado_conn(
                conn,
                panaderia_id,
                pin_lookup_digest,
                exclude_usuario_id=usuario_id,
            ):
                return {
                    "ok": False,
                    "error": "Ese PIN ya está en uso por otro usuario operativo activo. Usa uno distinto.",
                }
            invalida_sesion = bool(
                pin
                or rol_cambio
                or int(prev["sede_id"] or 0) != int(sede["id"] or 0)
                or _normalizar_username(prev["username"] or "") != username_norm
            )

            if pin:
                pin_hash = _hash_pin(pin)
                conn.execute(
                    """
                    UPDATE usuarios
                    SET nombre = ?, rol = ?, username = ?, email = ?, sede_id = ?,
                        pin = ?, pin_hash = ?, pin_lookup_digest = ?,
                        password_hash = ?, must_change_password = 1,
                        session_version = COALESCE(session_version, 0) + 1
                    WHERE id = ?
                    """,
                    (
                        nombre,
                        rol,
                        username_norm,
                        _email_local_para_username(username_norm),
                        sede["id"],
                        pin_hash,
                        pin_hash,
                        pin_lookup_digest,
                        _hash_password(pin),
                        usuario_id,
                    ),
                )
            elif invalida_sesion:
                conn.execute(
                    """
                    UPDATE usuarios
                       SET nombre = ?, rol = ?, username = ?, email = ?, sede_id = ?, pin_lookup_digest = ?,
                           session_version = COALESCE(session_version, 0) + 1
                     WHERE id = ?
                    """,
                    (
                        nombre,
                        rol,
                        username_norm,
                        _email_local_para_username(username_norm),
                        sede["id"],
                        pin_lookup_digest,
                        usuario_id,
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE usuarios
                    SET nombre = ?, rol = ?, username = ?, email = ?, sede_id = ?, pin_lookup_digest = ?
                    WHERE id = ?
                    """,
                    (
                        nombre,
                        rol,
                        username_norm,
                        _email_local_para_username(username_norm),
                        sede["id"],
                        pin_lookup_digest,
                        usuario_id,
                    ),
                )
            # Compatibilidad: sincronizar el rol en la membresía activa si existe.
            conn.execute(
                "UPDATE tenant_memberships SET rol = ?, sede_id = ? WHERE usuario_id = ? AND activa = 1",
                (rol, sede["id"], usuario_id),
            )
            conn.commit()
        return {"ok": True, "username": username_norm, "sede_id": sede["id"]}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def resetear_pin_usuario(
    usuario_id: int,
    panaderia_id: int,
    nuevo_pin: str = "",
) -> dict:
    pin_final = str(nuevo_pin or "").strip()
    generado = False
    if not pin_final:
        pin_final = f"{secrets.randbelow(10000):04d}"
        generado = True
    if len(pin_final) < 4:
        return {"ok": False, "error": "El PIN debe tener al menos 4 dígitos"}

    pin_hash = _hash_pin(pin_final)
    pin_lookup_digest = _pin_lookup_digest(pin_final)

    try:
        with get_connection() as conn:
            usuario = conn.execute(
                """
                SELECT id, rol
                FROM usuarios
                WHERE id = ? AND panaderia_id = ?
                """,
                (int(usuario_id), int(panaderia_id)),
            ).fetchone()
            if not usuario:
                return {"ok": False, "error": "Usuario no encontrado"}
            if str(usuario["rol"] or "") in ("cajero", "mesero") and _pin_operativo_ya_asignado_conn(
                conn,
                int(panaderia_id),
                pin_lookup_digest,
                exclude_usuario_id=int(usuario_id),
            ):
                return {
                    "ok": False,
                    "error": "Ese PIN ya está en uso por otro usuario operativo activo. Usa uno distinto.",
                }
            conn.execute(
                """
                UPDATE usuarios
                   SET pin = ?,
                       pin_hash = ?,
                       pin_lookup_digest = ?,
                       password_hash = ?,
                       must_change_password = 1,
                       session_version = COALESCE(session_version, 0) + 1
                 WHERE id = ?
                """,
                (pin_hash, pin_hash, pin_lookup_digest, _hash_password(pin_final), int(usuario_id)),
            )
            conn.commit()
        return {"ok": True, "pin_temporal": pin_final if generado else "", "generado": generado}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def _migrar_fase10(conn) -> None:
    """Fase 10: actividad operativa ampliada y modificaciones de venta por línea."""
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE usuarios ADD COLUMN ultimo_acceso_en TEXT NOT NULL DEFAULT ''")
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE usuarios ADD COLUMN ultimo_acceso_terminal_id INTEGER")
    _ejecutar_migracion_tolerante(conn, "UPDATE usuarios SET ultimo_acceso_en = last_login_at WHERE COALESCE(ultimo_acceso_en, '') = ''")
    _asegurar_venta_item_modificaciones_schema_conn(conn)
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_usuarios_ultimo_acceso ON usuarios(ultimo_acceso_en)")


def _migrar_fase11(conn) -> None:
    """Fase 11: comandas imprimibles para pedidos de mesas."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS comandas (
            id                        INTEGER PRIMARY KEY AUTOINCREMENT,
            pedido_id                 INTEGER NOT NULL,
            panaderia_id              INTEGER,
            sede_id                   INTEGER,
            mesa_id                   INTEGER,
            creada_por_usuario_id     INTEGER,
            creada_por_nombre_snapshot TEXT NOT NULL DEFAULT '',
            estado                    TEXT NOT NULL DEFAULT 'generada'
                                      CHECK(estado IN ('generada','impresa','reimpresa','cancelada')),
            es_incremental            INTEGER NOT NULL DEFAULT 0,
            comanda_origen_id         INTEGER,
            nota_general              TEXT DEFAULT '',
            created_at                TEXT NOT NULL,
            updated_at                TEXT NOT NULL,
            FOREIGN KEY (pedido_id) REFERENCES pedidos(id) ON DELETE CASCADE,
            FOREIGN KEY (mesa_id) REFERENCES mesas(id),
            FOREIGN KEY (comanda_origen_id) REFERENCES comandas(id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS comanda_items (
            id                        INTEGER PRIMARY KEY AUTOINCREMENT,
            comanda_id                INTEGER NOT NULL,
            pedido_item_id            INTEGER,
            producto_nombre_snapshot  TEXT NOT NULL DEFAULT '',
            cantidad                  INTEGER NOT NULL DEFAULT 1,
            observacion               TEXT DEFAULT '',
            modificadores_json        TEXT DEFAULT '',
            created_at                TEXT NOT NULL,
            FOREIGN KEY (comanda_id) REFERENCES comandas(id) ON DELETE CASCADE,
            FOREIGN KEY (pedido_item_id) REFERENCES pedido_items(id) ON DELETE SET NULL
        )
    """)
    for stmt in (
        "ALTER TABLE comandas ADD COLUMN panaderia_id INTEGER",
        "ALTER TABLE comandas ADD COLUMN sede_id INTEGER",
        "ALTER TABLE comandas ADD COLUMN mesa_id INTEGER",
        "ALTER TABLE comandas ADD COLUMN creada_por_usuario_id INTEGER",
        "ALTER TABLE comandas ADD COLUMN creada_por_nombre_snapshot TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE comandas ADD COLUMN estado TEXT NOT NULL DEFAULT 'generada'",
        "ALTER TABLE comandas ADD COLUMN es_incremental INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE comandas ADD COLUMN comanda_origen_id INTEGER",
        "ALTER TABLE comandas ADD COLUMN nota_general TEXT DEFAULT ''",
        "ALTER TABLE comandas ADD COLUMN created_at TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE comandas ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE comanda_items ADD COLUMN pedido_item_id INTEGER",
        "ALTER TABLE comanda_items ADD COLUMN producto_nombre_snapshot TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE comanda_items ADD COLUMN cantidad INTEGER NOT NULL DEFAULT 1",
        "ALTER TABLE comanda_items ADD COLUMN observacion TEXT DEFAULT ''",
        "ALTER TABLE comanda_items ADD COLUMN modificadores_json TEXT DEFAULT ''",
        "ALTER TABLE comanda_items ADD COLUMN created_at TEXT NOT NULL DEFAULT ''",
    ):
        _ejecutar_migracion_tolerante(conn, stmt)

    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_comandas_pedido ON comandas(pedido_id, created_at DESC)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_comandas_tenant ON comandas(panaderia_id, sede_id, created_at DESC)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_comanda_items_comanda ON comanda_items(comanda_id, id)")


def _migrar_fase12(conn) -> None:
    """Fase 12: documentos comerciales imprimibles y envios por correo."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS documentos_emitidos (
            id                           INTEGER PRIMARY KEY AUTOINCREMENT,
            origen_tipo                  TEXT NOT NULL
                                         CHECK(origen_tipo IN ('venta','pedido','encargo')),
            origen_id                    INTEGER NOT NULL,
            panaderia_id                 INTEGER,
            sede_id                      INTEGER,
            cliente_id                   INTEGER,
            tipo_documento               TEXT NOT NULL DEFAULT 'factura',
            consecutivo                  TEXT NOT NULL,
            consecutivo_numero           INTEGER NOT NULL DEFAULT 0,
            estado                       TEXT NOT NULL DEFAULT 'generado'
                                         CHECK(estado IN ('generado','emitido','enviado','anulado')),
            cliente_nombre_snapshot      TEXT NOT NULL DEFAULT '',
            cliente_tipo_doc_snapshot    TEXT NOT NULL DEFAULT '',
            cliente_numero_doc_snapshot  TEXT NOT NULL DEFAULT '',
            cliente_email_snapshot       TEXT NOT NULL DEFAULT '',
            cliente_empresa_snapshot     TEXT NOT NULL DEFAULT '',
            cliente_direccion_snapshot   TEXT NOT NULL DEFAULT '',
            payload_json                 TEXT NOT NULL DEFAULT '{}',
            subtotal                     REAL NOT NULL DEFAULT 0.0,
            impuestos                    REAL NOT NULL DEFAULT 0.0,
            total                        REAL NOT NULL DEFAULT 0.0,
            metodo_pago_snapshot         TEXT NOT NULL DEFAULT '',
            emitido_por_usuario_id       INTEGER,
            emitido_por_nombre_snapshot  TEXT NOT NULL DEFAULT '',
            created_at                   TEXT NOT NULL,
            updated_at                   TEXT NOT NULL,
            FOREIGN KEY (cliente_id) REFERENCES clientes(id),
            FOREIGN KEY (sede_id) REFERENCES sedes(id),
            FOREIGN KEY (panaderia_id) REFERENCES panaderias(id),
            FOREIGN KEY (emitido_por_usuario_id) REFERENCES usuarios(id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS documento_envios (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            documento_id          INTEGER NOT NULL,
            email_destino         TEXT NOT NULL DEFAULT '',
            estado                TEXT NOT NULL DEFAULT 'pendiente'
                                  CHECK(estado IN ('pendiente','enviado','error')),
            intentos              INTEGER NOT NULL DEFAULT 1,
            ultimo_error          TEXT NOT NULL DEFAULT '',
            enviado_por_usuario_id INTEGER,
            created_at            TEXT NOT NULL,
            sent_at               TEXT NOT NULL DEFAULT '',
            FOREIGN KEY (documento_id) REFERENCES documentos_emitidos(id) ON DELETE CASCADE,
            FOREIGN KEY (enviado_por_usuario_id) REFERENCES usuarios(id)
        )
    """)
    for stmt in (
        "ALTER TABLE tenant_branding ADD COLUMN tax_label TEXT NOT NULL DEFAULT 'NIT'",
        "ALTER TABLE tenant_branding ADD COLUMN tax_id TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE tenant_branding ADD COLUMN invoice_footer TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE documentos_emitidos ADD COLUMN panaderia_id INTEGER",
        "ALTER TABLE documentos_emitidos ADD COLUMN sede_id INTEGER",
        "ALTER TABLE documentos_emitidos ADD COLUMN cliente_id INTEGER",
        "ALTER TABLE documentos_emitidos ADD COLUMN consecutivo_numero INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE documentos_emitidos ADD COLUMN payload_json TEXT NOT NULL DEFAULT '{}'",
        "ALTER TABLE documentos_emitidos ADD COLUMN subtotal REAL NOT NULL DEFAULT 0.0",
        "ALTER TABLE documentos_emitidos ADD COLUMN impuestos REAL NOT NULL DEFAULT 0.0",
        "ALTER TABLE documentos_emitidos ADD COLUMN total REAL NOT NULL DEFAULT 0.0",
        "ALTER TABLE documentos_emitidos ADD COLUMN metodo_pago_snapshot TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE documentos_emitidos ADD COLUMN emitido_por_usuario_id INTEGER",
        "ALTER TABLE documentos_emitidos ADD COLUMN emitido_por_nombre_snapshot TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE documentos_emitidos ADD COLUMN created_at TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE documentos_emitidos ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE documento_envios ADD COLUMN ultimo_error TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE documento_envios ADD COLUMN enviado_por_usuario_id INTEGER",
        "ALTER TABLE documento_envios ADD COLUMN created_at TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE documento_envios ADD COLUMN sent_at TEXT NOT NULL DEFAULT ''",
    ):
        _ejecutar_migracion_tolerante(conn, stmt)

    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_documentos_origen ON documentos_emitidos(origen_tipo, origen_id, created_at DESC)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_documentos_sede_tipo ON documentos_emitidos(sede_id, tipo_documento, consecutivo_numero DESC)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_documentos_estado ON documentos_emitidos(estado, created_at DESC)")
    _ejecutar_migracion_tolerante(conn, "CREATE UNIQUE INDEX IF NOT EXISTS idx_documentos_consecutivo_sede ON documentos_emitidos(sede_id, tipo_documento, consecutivo_numero)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_documento_envios_documento ON documento_envios(documento_id, created_at DESC)")


def _migrar_fase13(conn) -> None:
    """Fase 13: cartera, credito y relacion comercial por cliente."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cuentas_por_cobrar (
            id                        INTEGER PRIMARY KEY AUTOINCREMENT,
            panaderia_id              INTEGER,
            sede_id                   INTEGER,
            cliente_id                INTEGER NOT NULL,
            origen_tipo               TEXT NOT NULL
                                      CHECK(origen_tipo IN ('venta','pedido','encargo')),
            origen_id                 INTEGER NOT NULL,
            documento_id              INTEGER,
            estado                    TEXT NOT NULL DEFAULT 'abierta'
                                      CHECK(estado IN ('abierta','parcial','pagada','vencida','cancelada')),
            monto_original            REAL NOT NULL DEFAULT 0.0,
            saldo_pendiente           REAL NOT NULL DEFAULT 0.0,
            fecha_emision             TEXT NOT NULL,
            fecha_vencimiento         TEXT NOT NULL DEFAULT '',
            aprobado_por_usuario_id   INTEGER,
            observacion               TEXT NOT NULL DEFAULT '',
            created_at                TEXT NOT NULL,
            updated_at                TEXT NOT NULL,
            FOREIGN KEY (cliente_id) REFERENCES clientes(id),
            FOREIGN KEY (documento_id) REFERENCES documentos_emitidos(id),
            FOREIGN KEY (aprobado_por_usuario_id) REFERENCES usuarios(id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cuenta_cobros (
            id                             INTEGER PRIMARY KEY AUTOINCREMENT,
            cuenta_id                      INTEGER NOT NULL,
            monto                          REAL NOT NULL DEFAULT 0.0,
            metodo_pago                    TEXT NOT NULL DEFAULT 'efectivo',
            referencia                     TEXT NOT NULL DEFAULT '',
            nota                           TEXT NOT NULL DEFAULT '',
            registrado_por_usuario_id      INTEGER,
            registrado_por_nombre_snapshot TEXT NOT NULL DEFAULT '',
            created_at                     TEXT NOT NULL,
            FOREIGN KEY (cuenta_id) REFERENCES cuentas_por_cobrar(id) ON DELETE CASCADE,
            FOREIGN KEY (registrado_por_usuario_id) REFERENCES usuarios(id)
        )
    """)
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_cxc_cliente_estado ON cuentas_por_cobrar(cliente_id, estado)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_cxc_fecha_emision ON cuentas_por_cobrar(fecha_emision, panaderia_id, sede_id)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_cxc_fecha_vencimiento ON cuentas_por_cobrar(fecha_vencimiento, estado)")
    _ejecutar_migracion_tolerante(conn, "CREATE UNIQUE INDEX IF NOT EXISTS idx_cxc_origen_unico ON cuentas_por_cobrar(panaderia_id, origen_tipo, origen_id)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_cuenta_cobros_cuenta ON cuenta_cobros(cuenta_id, created_at)")

    for stmt in (
        "ALTER TABLE venta_headers ADD COLUMN cliente_id INTEGER",
        "ALTER TABLE venta_headers ADD COLUMN cliente_nombre_snapshot TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE venta_headers ADD COLUMN empresa_comprador TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE venta_headers ADD COLUMN direccion_comprador TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE pedidos ADD COLUMN cliente_id INTEGER",
        "ALTER TABLE pedidos ADD COLUMN cliente_nombre_snapshot TEXT NOT NULL DEFAULT ''",
    ):
        _ejecutar_migracion_tolerante(conn, stmt)

    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_vh_cliente ON venta_headers(cliente_id, fecha)")
    _ejecutar_migracion_tolerante(conn, "CREATE INDEX IF NOT EXISTS idx_pedidos_cliente ON pedidos(cliente_id, fecha)")

    rows_ventas = conn.execute(
        """
        SELECT id, nombre_comprador
        FROM venta_headers
        WHERE COALESCE(cliente_nombre_snapshot, '') = ''
          AND COALESCE(nombre_comprador, '') <> ''
        """
    ).fetchall()
    for row in rows_ventas:
        conn.execute(
            "UPDATE venta_headers SET cliente_nombre_snapshot = ? WHERE id = ?",
            (str(row["nombre_comprador"] or "").strip(), row["id"]),
        )


def _migrar_fase14(conn) -> None:
    """Fase 14: borrado logico de mesas para mantener integridad historica."""
    _ejecutar_migracion_tolerante(conn, "ALTER TABLE mesas ADD COLUMN eliminada INTEGER NOT NULL DEFAULT 0")
    _ejecutar_migracion_tolerante(conn, "UPDATE mesas SET eliminada = 0 WHERE eliminada IS NULL")


def eliminar_usuario(usuario_id: int) -> bool:
    try:
        with get_connection() as conn:
            conn.execute("DELETE FROM usuarios WHERE id = ?", (usuario_id,))
            conn.commit()
        return True
    except Exception:
        return False


# ── Session version (invalidación server-side) ───────────────────────────────

def obtener_session_version_usuario(usuario_id: int) -> int:
    """Retorna la session_version actual del usuario, o -1 si no existe."""
    try:
        with get_connection() as conn:
            row = conn.execute(
                "SELECT COALESCE(session_version, 0) AS v FROM usuarios WHERE id = ?",
                (usuario_id,)
            ).fetchone()
        return int(row["v"]) if row else -1
    except Exception:
        return -1


def incrementar_session_version_usuario(usuario_id: int) -> None:
    """Incrementa session_version atómicamente, invalidando todas las sesiones activas."""
    try:
        with get_connection() as conn:
            conn.execute(
                "UPDATE usuarios SET session_version = COALESCE(session_version, 0) + 1 WHERE id = ?",
                (usuario_id,)
            )
            conn.commit()
    except Exception:
        pass


def set_usuario_activo(usuario_id: int, activo: bool) -> bool:
    """Activa o desactiva un usuario. Desactivar invalida todas sus sesiones."""
    try:
        with get_connection() as conn:
            activo_int = 1 if activo else 0
            conn.execute(
                "UPDATE usuarios SET activo = ? WHERE id = ?",
                (activo_int, usuario_id)
            )
            if not activo:
                conn.execute(
                    "UPDATE usuarios SET session_version = COALESCE(session_version, 0) + 1 WHERE id = ?",
                    (usuario_id,)
                )
                # Desactivar también la jornada
                conn.execute(
                    "UPDATE usuarios SET jornada_activa = 0 WHERE id = ?",
                    (usuario_id,)
                )
            conn.commit()
        return True
    except Exception:
        return False


def _metodo_pago_normalizado(metodo_pago: str) -> str:
    metodo = str(metodo_pago or "").strip().lower()
    return metodo if metodo in ("efectivo", "transferencia", "tarjeta") else "efectivo"


def _registrar_historial_estado_pedido(conn, pedido_id: int, estado: str,
                                       cambiado_por: str = "", detalle: str = "",
                                       cambiado_en: str | None = None) -> None:
    cambiado_en = cambiado_en or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute("""
        INSERT INTO pedido_estado_historial (pedido_id, estado, cambiado_en, cambiado_por, detalle)
        VALUES (?, ?, ?, ?, ?)
    """, (pedido_id, estado, cambiado_en, cambiado_por, detalle))


def obtener_arqueo_caja_activo(fecha: str | None = None) -> dict | None:
    """Retorna el arqueo abierto más reciente.

    Si se pasa `fecha` explícita, filtra por esa fecha (usado al cerrar un día
    específico). Si no se pasa, busca cualquier arqueo abierto sin importar la
    fecha — así funciona correctamente pasada la medianoche sin requerir
    cierre y reapertura manual.
    """
    _select = """SELECT id, fecha, abierto_en, abierto_por, monto_apertura, estado, notas,
                        cerrado_en, cerrado_por, monto_cierre, efectivo_esperado,
                        diferencia_cierre, notas_cierre, reabierto_en, reabierto_por,
                        motivo_reapertura, reaperturas
                 FROM arqueos_caja"""
    with get_connection() as conn:
        if fecha:
            filtros = ["fecha = ?", "estado = 'abierto'"]
            params: list = [fecha]
            _apply_tenant_scope(filtros, params)
            row = conn.execute(
                f"{_select} WHERE {' AND '.join(filtros)} ORDER BY abierto_en DESC LIMIT 1",
                tuple(params),
            ).fetchone()
        else:
            filtros = ["estado = 'abierto'"]
            params = []
            _apply_tenant_scope(filtros, params)
            row = conn.execute(
                f"{_select} WHERE {' AND '.join(filtros)} ORDER BY abierto_en DESC LIMIT 1",
                tuple(params),
            ).fetchone()
    return dict(row) if row else None


def obtener_arqueo_caja_dia(fecha: str | None = None) -> dict | None:
    fecha = fecha or (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")
    filtros = ["fecha = ?"]
    params: list = [fecha]
    _apply_tenant_scope(filtros, params)
    with get_connection() as conn:
        row = conn.execute(f"""
            SELECT id, fecha, abierto_en, abierto_por, monto_apertura, estado, notas,
                   cerrado_en, cerrado_por, monto_cierre, efectivo_esperado,
                   diferencia_cierre, notas_cierre, reabierto_en, reabierto_por,
                   motivo_reapertura, reaperturas
            FROM arqueos_caja
            WHERE {' AND '.join(filtros)}
            ORDER BY
                CASE estado WHEN 'abierto' THEN 0 ELSE 1 END,
                abierto_en DESC
            LIMIT 1
        """, tuple(params)).fetchone()
    return dict(row) if row else None


def _limites_arqueo(arqueo: dict | None) -> tuple[str | None, str | None]:
    if not arqueo:
        return None, None
    abierto_en = str(arqueo.get("abierto_en", "") or "").strip() or None
    cerrado_en = str(arqueo.get("cerrado_en", "") or "").strip() or None
    return abierto_en, cerrado_en


def abrir_arqueo_caja(abierto_por: str, monto_apertura: float, notas: str = "",
                      fecha: str | None = None) -> dict:
    fecha = fecha or (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")
    if monto_apertura < 0:
        return {"ok": False, "error": "El monto de apertura no puede ser negativo"}

    panaderia_id, sede_id = _tenant_scope()
    with get_connection() as conn:
        filtros = ["fecha = ?", "estado = 'abierto'"]
        scope_params: list = [fecha]
        _apply_tenant_scope(filtros, scope_params)
        existente = conn.execute(
            f"SELECT id, abierto_en, abierto_por, monto_apertura FROM arqueos_caja "
            f"WHERE {' AND '.join(filtros)} ORDER BY abierto_en DESC LIMIT 1",
            tuple(scope_params),
        ).fetchone()
        if existente:
            return {
                "ok": False,
                "error": "Ya hay un arqueo abierto para hoy",
                "arqueo": dict(existente),
            }

        abierto_en = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur = conn.execute("""
            INSERT INTO arqueos_caja (fecha, abierto_en, abierto_por, monto_apertura, estado, notas, panaderia_id, sede_id)
            VALUES (?, ?, ?, ?, 'abierto', ?, ?, ?)
        """, (fecha, abierto_en, abierto_por, float(monto_apertura), notas.strip(), panaderia_id, sede_id))
        conn.commit()

    arqueo = obtener_arqueo_caja_activo(fecha)
    return {"ok": True, "arqueo_id": cur.lastrowid, "arqueo": arqueo}


def obtener_historial_arqueos(limite: int = 15) -> list[dict]:
    filtros: list[str] = []
    params: list = []
    _apply_tenant_scope(filtros, params)
    where = f"WHERE {' AND '.join(filtros)}" if filtros else ""
    with get_connection() as conn:
        rows = conn.execute(f"""
            SELECT id, fecha, abierto_en, abierto_por, monto_apertura, estado,
                   notas, cerrado_en, cerrado_por, monto_cierre,
                   efectivo_esperado, diferencia_cierre, notas_cierre,
                   reabierto_en, reabierto_por, motivo_reapertura, reaperturas
            FROM arqueos_caja
            {where}
            ORDER BY abierto_en DESC
            LIMIT ?
        """, tuple(params) + (limite,)).fetchall()
    return [dict(r) for r in rows]


def obtener_movimientos_caja(fecha: str | None = None, limite: int | None = None) -> list[dict]:
    fecha = fecha or (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")
    filtros = ["fecha = ?"]
    params: list = [fecha]
    _apply_tenant_scope(filtros, params)
    query = f"""
        SELECT id, arqueo_id, fecha, creado_en, tipo, concepto, monto, registrado_por, notas
        FROM movimientos_caja
        WHERE {' AND '.join(filtros)}
        ORDER BY creado_en DESC, id DESC
    """
    if limite is not None:
        query += " LIMIT ?"
        params.append(limite)
    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def registrar_movimiento_caja(tipo: str, concepto: str, monto: float,
                              registrado_por: str = "", notas: str = "",
                              fecha: str | None = None) -> dict:
    fecha = fecha or (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")
    tipo = str(tipo or "").strip().lower()
    if tipo not in ("ingreso", "egreso"):
        return {"ok": False, "error": "Tipo de movimiento invalido"}
    if float(monto or 0) <= 0:
        return {"ok": False, "error": "El monto debe ser mayor a cero"}
    if not str(concepto or "").strip():
        return {"ok": False, "error": "El concepto es obligatorio"}

    arqueo = obtener_arqueo_caja_activo()  # sin filtro de fecha
    if not arqueo:
        return {"ok": False, "error": "Debes tener una caja abierta para registrar movimientos"}

    creado_en = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    panaderia_id, sede_id = _tenant_scope()
    try:
        with get_connection() as conn:
            cur = conn.execute("""
                INSERT INTO movimientos_caja (
                    arqueo_id, fecha, creado_en, tipo, concepto, monto, registrado_por, notas,
                    panaderia_id, sede_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                arqueo["id"],
                fecha,
                creado_en,
                tipo,
                str(concepto).strip(),
                round(float(monto), 2),
                registrado_por,
                str(notas or "").strip(),
                panaderia_id,
                sede_id,
            ))
            conn.commit()
        return {"ok": True, "movimiento_id": cur.lastrowid}
    except Exception as e:
        logger.error(f"registrar_movimiento_caja: {e}")
        return {"ok": False, "error": str(e)}


def cerrar_arqueo_caja(cerrado_por: str, monto_cierre: float,
                       notas_cierre: str = "", codigo_verificacion: str = "",
                       monto_tarjeta_cierre: float | None = None,
                       monto_transferencia_cierre: float | None = None,
                       fecha: str | None = None) -> dict:
    fecha = fecha or (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")
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

    ventas_tarjeta = float(resumen.get("ventas_tarjeta", 0) or 0)
    ventas_transferencia = float(resumen.get("ventas_transferencia", 0) or 0)

    tarjeta_val = round(float(monto_tarjeta_cierre), 2) if monto_tarjeta_cierre is not None else None
    transferencia_val = round(float(monto_transferencia_cierre), 2) if monto_transferencia_cierre is not None else None
    dif_tarjeta = round(tarjeta_val - ventas_tarjeta, 2) if tarjeta_val is not None else None
    dif_transferencia = round(transferencia_val - ventas_transferencia, 2) if transferencia_val is not None else None

    cerrado_en = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        with get_connection() as conn:
            # Paso 1: cierre base (columnas que siempre existen)
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

            # Paso 2: columnas de reconciliacion (pueden no existir en DB antiguo)
            _ejecutar_migracion_tolerante(conn, "ALTER TABLE arqueos_caja ADD COLUMN monto_tarjeta_cierre REAL DEFAULT NULL")
            _ejecutar_migracion_tolerante(conn, "ALTER TABLE arqueos_caja ADD COLUMN monto_transferencia_cierre REAL DEFAULT NULL")
            _ejecutar_migracion_tolerante(conn, "ALTER TABLE arqueos_caja ADD COLUMN diferencia_tarjeta REAL DEFAULT NULL")
            _ejecutar_migracion_tolerante(conn, "ALTER TABLE arqueos_caja ADD COLUMN diferencia_transferencia REAL DEFAULT NULL")
            try:
                conn.execute("""
                    UPDATE arqueos_caja
                    SET monto_tarjeta_cierre = ?,
                        monto_transferencia_cierre = ?,
                        diferencia_tarjeta = ?,
                        diferencia_transferencia = ?
                    WHERE id = ?
                """, (tarjeta_val, transferencia_val, dif_tarjeta, dif_transferencia, arqueo["id"]))
                conn.commit()
            except Exception:
                pass  # Si las columnas aun no existen, ignorar

        arqueo_final = obtener_arqueo_caja_dia(fecha)
        return {
            "ok": True,
            "arqueo": arqueo_final,
            "efectivo_esperado": efectivo_esperado,
            "monto_cierre": monto_cierre,
            "diferencia": diferencia,
            "ventas_tarjeta": ventas_tarjeta,
            "ventas_transferencia": ventas_transferencia,
            "monto_tarjeta_cierre": tarjeta_val,
            "monto_transferencia_cierre": transferencia_val,
            "diferencia_tarjeta": dif_tarjeta,
            "diferencia_transferencia": dif_transferencia,
        }
    except Exception as e:
        logger.error(f"cerrar_arqueo_caja: {e}")
        return {"ok": False, "error": str(e)}


def obtener_resumen_caja_dia(fecha: str | None = None) -> dict:
    fecha = fecha or (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")
    arqueo = _row_to_dict(obtener_arqueo_caja_dia(fecha))
    abierto_en, cerrado_en = _limites_arqueo(arqueo)

    ventas_where = "1 = 0"
    ventas_params: list = []
    if abierto_en:
        ventas_filtros = ["fecha = ?", "(fecha || ' ' || hora) >= ?"]
        ventas_params = [fecha, abierto_en]
        if cerrado_en:
            ventas_filtros.append("(fecha || ' ' || hora) <= ?")
            ventas_params.append(cerrado_en)
        _apply_tenant_scope(ventas_filtros, ventas_params)
        ventas_where = " AND ".join(ventas_filtros)

    with get_connection() as conn:
        row = conn.execute(f"""
            SELECT
                -- Efectivo: pago simple O parte efectivo en pago mixto (primario) O secundario
                COALESCE(SUM(CASE
                    WHEN metodo_pago = 'efectivo' AND metodo_pago_2 IS NULL THEN total_grupo
                    WHEN metodo_pago = 'efectivo' AND metodo_pago_2 IS NOT NULL THEN monto_recibido_grupo - COALESCE(cambio_grupo, 0)
                    WHEN metodo_pago_2 = 'efectivo' THEN monto_pago_2_grupo
                    ELSE 0 END), 0.0) AS ventas_efectivo,
                -- Transferencia
                COALESCE(SUM(CASE
                    WHEN metodo_pago = 'transferencia' AND metodo_pago_2 IS NULL THEN total_grupo
                    WHEN metodo_pago = 'transferencia' AND metodo_pago_2 IS NOT NULL THEN monto_recibido_grupo
                    WHEN metodo_pago_2 = 'transferencia' THEN monto_pago_2_grupo
                    ELSE 0 END), 0.0) AS ventas_transferencia,
                -- Tarjeta
                COALESCE(SUM(CASE
                    WHEN metodo_pago = 'tarjeta' AND metodo_pago_2 IS NULL THEN total_grupo
                    WHEN metodo_pago = 'tarjeta' AND metodo_pago_2 IS NOT NULL THEN monto_recibido_grupo
                    WHEN metodo_pago_2 = 'tarjeta' THEN monto_pago_2_grupo
                    ELSE 0 END), 0.0) AS ventas_tarjeta,
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
                    SUM(total) AS total_grupo,
                    MAX(metodo_pago_2) AS metodo_pago_2,
                    MAX(monto_pago_2) AS monto_pago_2_grupo
                FROM ventas
                WHERE {ventas_where}
                GROUP BY COALESCE(NULLIF(venta_grupo, ''), 'legacy-' || id)
            ) base
        """, tuple(ventas_params)).fetchone()

        if arqueo:
            movimientos = conn.execute("""
                SELECT
                    COALESCE(SUM(CASE WHEN tipo = 'ingreso' THEN monto ELSE 0 END), 0.0) AS ingresos,
                    COALESCE(SUM(CASE WHEN tipo = 'egreso' THEN monto ELSE 0 END), 0.0) AS egresos,
                    COUNT(*) AS total_movimientos
                FROM movimientos_caja
                WHERE arqueo_id = ?
            """, (arqueo["id"],)).fetchone()
        else:
            movimientos = conn.execute("""
                SELECT 0.0 AS ingresos, 0.0 AS egresos, 0 AS total_movimientos
            """).fetchone()

    ventas_efectivo = float(row["ventas_efectivo"] or 0.0)
    ventas_transferencia = float(row["ventas_transferencia"] or 0.0)
    ventas_tarjeta = float(row["ventas_tarjeta"] or 0.0)
    monto_apertura = float(arqueo.get("monto_apertura", 0.0) or 0.0)
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
        "ventas_tarjeta": round(ventas_tarjeta, 2),
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
            {"metodo": "Tarjeta", "total": round(ventas_tarjeta, 2)},
        ],
        "cierre": {
            "monto_cierre": round(float(arqueo.get("monto_cierre", 0.0) or 0.0), 2),
            "efectivo_esperado": round(float(arqueo.get("efectivo_esperado", efectivo_esperado) or 0.0), 2),
            "diferencia": round(float(arqueo.get("diferencia_cierre", 0.0) or 0.0), 2),
            "cerrado_en": arqueo.get("cerrado_en"),
            "cerrado_por": arqueo.get("cerrado_por", ""),
            "notas_cierre": arqueo.get("notas_cierre", ""),
            "reabierto_en": arqueo.get("reabierto_en", ""),
            "reabierto_por": arqueo.get("reabierto_por", ""),
            "motivo_reapertura": arqueo.get("motivo_reapertura", ""),
            "reaperturas": int(arqueo.get("reaperturas", 0) or 0),
            "monto_tarjeta_cierre": arqueo.get("monto_tarjeta_cierre"),
            "monto_transferencia_cierre": arqueo.get("monto_transferencia_cierre"),
            "diferencia_tarjeta": arqueo.get("diferencia_tarjeta"),
            "diferencia_transferencia": arqueo.get("diferencia_transferencia"),
            "ventas_tarjeta_sistema": round(ventas_tarjeta, 2),
            "ventas_transferencia_sistema": round(ventas_transferencia, 2),
        },
    }


# ──────────────────────────────────────────────
# Modelo transaccional POS (venta_headers / venta_items / venta_pagos)
# ──────────────────────────────────────────────

_METODOS_PAGO_VALIDOS = {"efectivo", "transferencia", "tarjeta", "credito"}


def _recalcular_totales_venta(conn, venta_id: int) -> None:
    """Recalcula subtotal/total/monto_pagado/saldo_pendiente en venta_headers."""
    items = conn.execute(
        "SELECT COALESCE(SUM(subtotal), 0.0) AS t FROM venta_items WHERE venta_id = ?",
        (venta_id,),
    ).fetchone()
    subtotal = round(float(items["t"]), 2)

    pagos = conn.execute(
        "SELECT COALESCE(SUM(monto), 0.0) AS p FROM venta_pagos WHERE venta_id = ?",
        (venta_id,),
    ).fetchone()
    monto_pagado = round(float(pagos["p"]), 2)

    header = _row_to_dict(
        conn.execute("SELECT descuento FROM venta_headers WHERE id = ?", (venta_id,)).fetchone()
    )
    descuento = round(float(header.get("descuento", 0) or 0), 2)
    total = round(subtotal - descuento, 2)
    saldo = round(max(total - monto_pagado, 0.0), 2)

    if monto_pagado >= total - 0.005:
        estado_pago = "pagado"
    elif monto_pagado > 0:
        estado_pago = "parcial"
    else:
        estado_pago = "pendiente"

    conn.execute(
        """
        UPDATE venta_headers
        SET subtotal = ?, total = ?, monto_pagado = ?, saldo_pendiente = ?,
            estado_pago = ?, updated_at = ?
        WHERE id = ?
        """,
        (subtotal, total, monto_pagado, saldo, estado_pago,
         datetime.now().strftime("%Y-%m-%d %H:%M:%S"), venta_id),
    )


def crear_venta_header(
    cajero: str,
    cajero_id: int | None = None,
    sede_id: int | None = None,
    panaderia_id: int | None = None,
    terminal_id: int | None = None,
    tipo_venta: str = "rapida",
) -> int:
    """Crea una cabecera de venta en estado 'activa'. Retorna el venta_id."""
    ahora = datetime.now()
    fecha_op = (ahora - timedelta(hours=4)).strftime("%Y-%m-%d")
    hora_op = ahora.strftime("%H:%M:%S")
    now_str = ahora.strftime("%Y-%m-%d %H:%M:%S")
    tipo_venta = tipo_venta if tipo_venta in ("rapida", "con_documento") else "rapida"
    with get_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO venta_headers
                (fecha, hora, creado_en, updated_at, cajero, cajero_id, sede_id,
                 panaderia_id, terminal_id, tipo_venta, estado, estado_pago)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'activa', 'pendiente')
            """,
            (fecha_op, hora_op, now_str, now_str,
             cajero, cajero_id, sede_id, panaderia_id, terminal_id, tipo_venta),
        )
        conn.commit()
        return cur.lastrowid


def actualizar_items_venta(
    venta_id: int,
    items: list[dict],
    actor_role: str = "",
    actor_name: str = "",
    panaderia_id: int | None = None,
) -> dict:
    """Reemplaza los items de una venta activa/suspendida y recalcula totales."""
    items_norm = normalizar_items_venta(items)
    if not items_norm:
        return {"ok": False, "error": "No hay items validos para guardar"}

    actor_role = normalize_role(actor_role, fallback="")
    actor_name = _texto_linea_canonico(actor_name)
    panaderia_id = int(panaderia_id or 0)

    with get_connection() as conn:
        vh = conn.execute(
            "SELECT estado, panaderia_id FROM venta_headers WHERE id = ?",
            (venta_id,),
        ).fetchone()
        if not vh or vh["estado"] not in ("activa", "suspendida"):
            return {"ok": False, "error": "La venta no admite cambios"}
        if not panaderia_id:
            panaderia_id = int(vh["panaderia_id"] or 0)

        item_ids = [
            row["id"]
            for row in conn.execute("SELECT id FROM venta_items WHERE venta_id = ?", (venta_id,)).fetchall()
        ]
        _eliminar_modificaciones_venta_items_conn(conn, venta_id, item_ids)
        conn.execute("DELETE FROM venta_items WHERE venta_id = ?", (venta_id,))

        manuales: list[dict] = []
        for item in items_norm:
            producto = str(item.get("producto", "") or "").strip()
            cantidad = max(1, int(item.get("cantidad", 1) or 1))
            precio_base = round(float(item.get("precio_base", item.get("precio", 0)) or 0), 2)
            precio_aplicado = round(float(item.get("precio_aplicado", precio_base) or precio_base), 2)
            motivo_precio = _texto_linea_canonico(item.get("motivo_precio", ""))
            autorizado_por = _texto_linea_canonico(item.get("autorizado_por", ""))
            autorizado_pin = str(item.get("autorizado_pin", "") or "").strip()
            modificaciones = _normalizar_modificaciones_linea(item.get("modificaciones"))

            if not producto:
                continue

            if precio_aplicado != precio_base:
                if not motivo_precio:
                    return {"ok": False, "error": f"El precio manual de {producto} requiere motivo"}

                autorizador = None
                if actor_role in ("cajero", "mesero"):
                    autorizador = _verificar_autorizador_precio_conn(
                        conn,
                        panaderia_id,
                        autorizado_por,
                        autorizado_pin,
                    )
                    if not autorizador:
                        return {
                            "ok": False,
                            "error": f"{producto}: se requiere autorizacion valida para cambiar el precio",
                        }
                    autorizado_por = str(autorizador.get("nombre") or autorizador.get("username") or autorizado_por)
                else:
                    autorizado_por = autorizado_por or actor_name

                manuales.append(
                    {
                        "producto": producto,
                        "precio_base": precio_base,
                        "precio_aplicado": precio_aplicado,
                        "motivo_precio": motivo_precio,
                        "autorizado_por": autorizado_por,
                    }
                )
            else:
                autorizado_por = ""

            subtotal = _linea_subtotal(cantidad, precio_aplicado, modificaciones)
            cur_item = conn.execute(
                """
                INSERT INTO venta_items
                    (venta_id, producto_id, producto, cantidad, precio_base,
                     precio_aplicado, subtotal, motivo_precio, autorizado_por, notas)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    venta_id,
                    item.get("producto_id"),
                    producto,
                    cantidad,
                    precio_base,
                    precio_aplicado,
                    subtotal,
                    motivo_precio,
                    autorizado_por,
                    str(item.get("notas", "") or ""),
                ),
            )
            venta_item_id = cur_item.lastrowid
            for mod in modificaciones:
                _insertar_modificacion_venta_item_conn(conn, venta_id, venta_item_id, mod)
        _recalcular_totales_venta(conn, venta_id)
        conn.commit()
    return {"ok": True, "manuales": manuales, "items": items_norm}


def actualizar_comprador_venta(
    venta_id: int,
    nombre_comprador: str = "",
    tipo_doc: str = "",
    numero_doc: str = "",
    email_comprador: str = "",
    empresa_comprador: str = "",
    direccion_comprador: str = "",
    cliente_id: int | None = None,
) -> bool:
    """Guarda datos del comprador para ventas 'con_documento'."""
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE venta_headers
            SET tipo_venta = 'con_documento',
                nombre_comprador = ?, tipo_doc = ?, numero_doc = ?,
                email_comprador = ?, empresa_comprador = ?, direccion_comprador = ?,
                cliente_id = ?, cliente_nombre_snapshot = ?, updated_at = ?
            WHERE id = ? AND estado IN ('activa','suspendida')
            """,
            (
                nombre_comprador.strip(),
                tipo_doc.strip(),
                numero_doc.strip(),
                email_comprador.strip(),
                empresa_comprador.strip(),
                direccion_comprador.strip(),
                int(cliente_id or 0) or None,
                nombre_comprador.strip(),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                venta_id,
            ),
        )
        conn.commit()
    return True


def registrar_pago_venta(
    venta_id: int,
    metodo: str,
    monto: float,
    registrado_por: str = "",
    referencia: str = "",
    recibido: float | None = None,
) -> dict:
    """Registra un pago en venta_pagos y recalcula el saldo. Devuelve el estado del pago."""
    metodo = metodo if metodo in _METODOS_PAGO_VALIDOS else "efectivo"
    monto = round(float(monto or 0), 2)
    if monto <= 0:
        return {"ok": False, "error": "El monto debe ser mayor a cero"}

    recibido_real = round(float(recibido if recibido is not None else monto), 2)
    cambio = round(max(recibido_real - monto, 0.0), 2) if metodo == "efectivo" else 0.0
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with get_connection() as conn:
        vh = conn.execute(
            "SELECT estado, total, monto_pagado FROM venta_headers WHERE id = ?",
            (venta_id,),
        ).fetchone()
        if not vh:
            return {"ok": False, "error": "Venta no encontrada"}
        if vh["estado"] not in ("activa", "suspendida"):
            return {"ok": False, "error": f"No se puede pagar una venta en estado '{vh['estado']}'"}

        conn.execute(
            """
            INSERT INTO venta_pagos
                (venta_id, metodo, monto, referencia, recibido, cambio,
                 registrado_en, registrado_por)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (venta_id, metodo, monto, referencia.strip(), recibido_real, cambio,
             now_str, registrado_por),
        )
        _recalcular_totales_venta(conn, venta_id)
        vh_updated = conn.execute(
            "SELECT total, monto_pagado, saldo_pendiente, estado_pago FROM venta_headers WHERE id = ?",
            (venta_id,),
        ).fetchone()
        conn.commit()

    return {
        "ok": True,
        "cambio": cambio,
        "total": float(vh_updated["total"]),
        "monto_pagado": float(vh_updated["monto_pagado"]),
        "saldo_pendiente": float(vh_updated["saldo_pendiente"]),
        "estado_pago": vh_updated["estado_pago"],
        "pagado": vh_updated["estado_pago"] == "pagado",
    }


def cerrar_venta(
    venta_id: int,
    usuario_id: int | None = None,
    usuario_nombre: str = "",
    fecha_vencimiento_credito: str | None = None,
) -> dict:
    """Finaliza la venta: requiere saldo = 0. Escribe en legacy ventas para retrocompat."""
    with get_connection() as conn:
        vh = conn.execute("SELECT * FROM venta_headers WHERE id = ?", (venta_id,)).fetchone()
        if not vh:
            return {"ok": False, "error": "Venta no encontrada"}
        if vh["estado"] == "pagada":
            return {"ok": True, "venta_grupo": vh["venta_grupo"]}
        if vh["estado"] != "activa":
            return {"ok": False, "error": f"No se puede cerrar una venta en estado '{vh['estado']}'"}
        if float(vh["saldo_pendiente"] or 0) > 0.005:
            return {"ok": False, "error": f"Saldo pendiente: ${vh['saldo_pendiente']:.0f}"}

        items = conn.execute(
            "SELECT * FROM venta_items WHERE venta_id = ?", (venta_id,)
        ).fetchall()
        pagos = conn.execute(
            "SELECT * FROM venta_pagos WHERE venta_id = ?", (venta_id,)
        ).fetchall()
        credito_total = round(
            sum(float(pago["monto"] or 0) for pago in pagos if str(pago["metodo"] or "").strip().lower() == "credito"),
            2,
        )
        if credito_total > 0.005 and not int(vh["cliente_id"] or 0):
            return {"ok": False, "error": "Debes asociar un cliente antes de cerrar una venta a credito"}

        # ── Generar venta_grupo ───────────────────────────────────────────────
        import uuid
        venta_grupo = f"vh-{venta_id}-{uuid.uuid4().hex[:8]}"
        metodo_principal = "efectivo"
        monto_recibido = float(vh["total"])
        cambio = 0.0
        metodo_2 = None
        monto_2 = None

        if pagos:
            metodo_principal = pagos[0]["metodo"]
            monto_recibido   = float(pagos[0]["recibido"] or pagos[0]["monto"])
            cambio           = float(pagos[0]["cambio"] or 0)
            if len(pagos) >= 2:
                metodo_2 = pagos[1]["metodo"]
                monto_2  = float(pagos[1]["monto"])

        # ── Escribir en legacy ventas ─────────────────────────────────────────
        for item in items:
            conn.execute(
                """
                INSERT INTO ventas
                    (fecha, hora, producto_id, producto, cantidad, precio_unitario, total,
                     registrado_por, venta_grupo, metodo_pago, monto_recibido, cambio,
                     referencia_tipo, referencia_id,
                     metodo_pago_2, monto_pago_2,
                     panaderia_id, sede_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pos', NULL, ?, ?, ?, ?)
                """,
                (vh["fecha"], vh["hora"],
                 item["producto_id"], item["producto"],
                 item["cantidad"], item["precio_aplicado"], item["subtotal"],
                 vh["cajero"], venta_grupo,
                 metodo_principal, monto_recibido, cambio,
                 metodo_2, monto_2,
                 vh["panaderia_id"], vh["sede_id"]),
            )

        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            """
            UPDATE venta_headers
            SET estado = 'pagada', estado_pago = 'pagado',
                venta_grupo = ?, updated_at = ?
            WHERE id = ?
            """,
            (venta_grupo, now_str, venta_id),
        )
        cuenta_credito = None
        if credito_total > 0.005:
            cuenta_credito = _crear_cuenta_por_cobrar_conn(
                conn,
                cliente_id=int(vh["cliente_id"]),
                origen_tipo="venta",
                origen_id=venta_id,
                monto=credito_total,
                fecha_vencimiento=fecha_vencimiento_credito,
                observacion=f"Credito generado desde venta #{venta_id}",
                usuario_id=usuario_id,
                usuario_nombre=usuario_nombre or str(vh["cajero"] or ""),
            )
        conn.commit()
    return {
        "ok": True,
        "venta_grupo": venta_grupo,
        "cuenta_por_cobrar_id": (cuenta_credito or {}).get("cuenta_id"),
        "credito_total": credito_total,
    }


def suspender_venta(venta_id: int, nota: str = "", suspendida_por: str = "") -> bool:
    """Pone una venta en estado 'suspendida' para reanudar después."""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_connection() as conn:
        cur = conn.execute(
            """
            UPDATE venta_headers
            SET estado = 'suspendida', nota_suspension = ?,
                suspendida_en = ?, updated_at = ?
            WHERE id = ? AND estado = 'activa'
            """,
            (nota.strip(), now_str, now_str, venta_id),
        )
        conn.commit()
    return cur.rowcount > 0


def reanudar_venta(venta_id: int) -> bool:
    """Reactiva una venta suspendida."""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_connection() as conn:
        cur = conn.execute(
            """
            UPDATE venta_headers
            SET estado = 'activa', nota_suspension = '', suspendida_en = '', updated_at = ?
            WHERE id = ? AND estado = 'suspendida'
            """,
            (now_str, venta_id),
        )
        conn.commit()
    return cur.rowcount > 0


def anular_venta(venta_id: int, motivo: str, anulada_por: str) -> bool:
    """Anula una venta. No escribe en legacy ventas."""
    if not motivo or not anulada_por:
        return False
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_connection() as conn:
        cur = conn.execute(
            """
            UPDATE venta_headers
            SET estado = 'anulada', motivo_anulacion = ?,
                anulada_por = ?, anulada_en = ?, updated_at = ?
            WHERE id = ? AND estado IN ('activa','suspendida')
            """,
            (motivo.strip(), anulada_por.strip(), now_str, now_str, venta_id),
        )
        conn.commit()
    return cur.rowcount > 0


def obtener_ventas_suspendidas(panaderia_id: int | None = None, sede_id: int | None = None) -> list[dict]:
    """Lista ventas en estado 'suspendida' con items para el panel del POS."""
    filtros = ["vh.estado = 'suspendida'"]
    params: list = []
    if panaderia_id:
        filtros.append("vh.panaderia_id = ?")
        params.append(panaderia_id)
    if sede_id:
        filtros.append("vh.sede_id = ?")
        params.append(sede_id)
    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT vh.id, vh.fecha, vh.hora, vh.cajero, vh.total, vh.monto_pagado,
                   vh.saldo_pendiente, vh.nota_suspension, vh.suspendida_en,
                   vh.tipo_venta, vh.nombre_comprador
            FROM venta_headers vh
            WHERE {' AND '.join(filtros)}
            ORDER BY vh.suspendida_en DESC
            """,
            tuple(params),
        ).fetchall()
        resultado = []
        for row in rows:
            venta = _row_to_dict(row)
            items = conn.execute(
                "SELECT producto, cantidad, precio_aplicado, subtotal FROM venta_items WHERE venta_id = ?",
                (venta["id"],),
            ).fetchall()
            venta["items"] = [_row_to_dict(i) for i in items]
            resultado.append(venta)
    return resultado


def obtener_venta_header(venta_id: int) -> dict | None:
    """Retorna la cabecera de venta con items y pagos."""
    with get_connection() as conn:
        vh = conn.execute("SELECT * FROM venta_headers WHERE id = ?", (venta_id,)).fetchone()
        if not vh:
            return None
        resultado = _row_to_dict(vh)
        items = [
            _row_to_dict(r) for r in conn.execute(
                "SELECT * FROM venta_items WHERE venta_id = ? ORDER BY id", (venta_id,)
            ).fetchall()
        ]
        item_ids = [int(item.get("id", 0) or 0) for item in items if int(item.get("id", 0) or 0) > 0]
        mods_by_item = _cargar_modificaciones_venta_items_conn(conn, venta_id, item_ids)
        for item in items:
            item["modificaciones"] = mods_by_item.get(int(item.get("id", 0) or 0), [])
        resultado["items"] = items
        resultado["pagos"] = [
            _row_to_dict(r) for r in conn.execute(
                "SELECT * FROM venta_pagos WHERE venta_id = ? ORDER BY id", (venta_id,)
            ).fetchall()
        ]
    return resultado


# -----------------------------------------------------------------------------
# Documentos comerciales emitidos
# -----------------------------------------------------------------------------

_DOCUMENTO_TIPOS_VALIDOS = {"factura", "documento_venta", "remision"}
_DOCUMENTO_ORIGENES_VALIDOS = {"venta", "pedido", "encargo"}


def _tipo_documento_normalizado(tipo_documento: str | None) -> str:
    tipo = str(tipo_documento or "factura").strip().lower()
    return tipo if tipo in _DOCUMENTO_TIPOS_VALIDOS else "factura"


def _origen_documento_normalizado(origen_tipo: str | None) -> str:
    origen = str(origen_tipo or "").strip().lower()
    if origen not in _DOCUMENTO_ORIGENES_VALIDOS:
        raise ValueError("Origen de documento invalido")
    return origen


def _documento_prefijo_tipo(tipo_documento: str) -> str:
    return {
        "factura": "FAC",
        "documento_venta": "DV",
        "remision": "REM",
    }.get(tipo_documento, "DOC")


def _documento_fecha_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _resolver_snapshot_cliente(
    cliente_base: dict | None = None,
    datos_cliente: dict | None = None,
) -> dict:
    cliente_base = cliente_base or {}
    datos_cliente = datos_cliente or {}
    snapshot = {
        "cliente_id": datos_cliente.get("cliente_id") or cliente_base.get("id"),
        "nombre": str(datos_cliente.get("nombre") or cliente_base.get("nombre") or "").strip(),
        "tipo_doc": str(datos_cliente.get("tipo_doc") or cliente_base.get("tipo_doc") or "").strip(),
        "numero_doc": str(datos_cliente.get("numero_doc") or cliente_base.get("numero_doc") or "").strip(),
        "email": str(datos_cliente.get("email") or cliente_base.get("email") or "").strip(),
        "empresa": str(datos_cliente.get("empresa") or cliente_base.get("empresa") or "").strip(),
        "direccion": str(datos_cliente.get("direccion") or cliente_base.get("direccion") or "").strip(),
    }
    if not snapshot["nombre"] and snapshot["empresa"]:
        snapshot["nombre"] = snapshot["empresa"]
    return snapshot


def _resolver_usuario_snapshot_conn(conn, usuario_id: int | None) -> tuple[int | None, str]:
    if not usuario_id:
        return None, ""
    row = conn.execute(
        "SELECT id, nombre, username FROM usuarios WHERE id = ?",
        (int(usuario_id),),
    ).fetchone()
    if not row:
        return None, ""
    nombre = str(row["nombre"] or row["username"] or "").strip()
    return int(row["id"]), nombre


def _resumen_metodos_pago(pagos: list[dict]) -> str:
    if not pagos:
        return ""
    partes: list[str] = []
    for pago in pagos:
        metodo = str(pago.get("metodo") or pago.get("label") or "").strip()
        monto = round(float(pago.get("monto", 0) or 0), 2)
        if not metodo:
            continue
        if monto > 0:
            partes.append(f"{metodo.capitalize()} ${monto:,.2f}")
        else:
            partes.append(metodo.capitalize())
    return " + ".join(partes)


def _resolver_info_negocio_documento_conn(
    conn,
    panaderia_id: int | None,
    sede_id: int | None,
) -> dict:
    tenant = obtener_panaderia_principal_conn(conn)
    tenant_id = int(panaderia_id or tenant["id"])
    sede = _row_to_dict(
        obtener_sede_por_id(int(sede_id)) if sede_id else obtener_sede_principal_conn(conn, tenant_id)
    )
    branding = obtener_branding_panaderia(tenant_id)
    return {
        "panaderia_id": tenant_id,
        "sede_id": int(sede.get("id") or 0) or None,
        "brand_name": branding.get("brand_name") or tenant.get("nombre") or "Panaderia",
        "legal_name": branding.get("legal_name") or tenant.get("nombre") or "",
        "tax_label": branding.get("tax_label") or "NIT",
        "tax_id": branding.get("tax_id") or "",
        "logo_path": branding.get("logo_path") or "brand/richs-logo.svg",
        "invoice_footer": branding.get("invoice_footer") or "",
        "sede_nombre": sede.get("nombre") or "",
        "sede_codigo": sede.get("codigo") or "",
    }


def _normalizar_item_documento_base(item: dict) -> dict:
    return {
        "producto_id": item.get("producto_id"),
        "producto": str(item.get("producto") or item.get("producto_nombre_snapshot") or "").strip(),
        "cantidad": int(item.get("cantidad", 0) or 0),
        "precio_unitario": round(float(
            item.get("precio_unitario", item.get("precio_aplicado", item.get("precio_base", 0))) or 0
        ), 2),
        "subtotal": round(float(item.get("subtotal", 0) or 0), 2),
        "notas": str(item.get("notas") or item.get("observacion") or "").strip(),
        "modificaciones": [
            {
                "tipo": str(mod.get("tipo") or "").strip(),
                "descripcion": str(mod.get("descripcion") or "").strip(),
                "cantidad": int(mod.get("cantidad", 1) or 1),
                "precio_extra": round(float(mod.get("precio_extra", 0) or 0), 2),
            }
            for mod in (item.get("modificaciones") or [])
        ],
    }


def _generar_consecutivo_documento_conn(
    conn,
    sede_id: int | None,
    tipo_documento: str,
) -> tuple[int, str]:
    tipo_documento = _tipo_documento_normalizado(tipo_documento)
    sede = _row_to_dict(obtener_sede_por_id(int(sede_id)) if sede_id else None)
    prefijo = _documento_prefijo_tipo(tipo_documento)
    sede_codigo = str(sede.get("codigo") or "GEN").strip().upper() or "GEN"
    row = conn.execute(
        """
        SELECT COALESCE(MAX(consecutivo_numero), 0) AS ultimo
        FROM documentos_emitidos
        WHERE sede_id IS ? AND tipo_documento = ?
        """,
        (sede_id, tipo_documento),
    ).fetchone()
    consecutivo_numero = int((row["ultimo"] or 0) if row else 0) + 1
    consecutivo = f"{prefijo}-{sede_codigo}-{consecutivo_numero:06d}"
    return consecutivo_numero, consecutivo


def generar_consecutivo_documento(sede_id: int | None, tipo_documento: str = "factura") -> str:
    with get_connection() as conn:
        _, consecutivo = _generar_consecutivo_documento_conn(
            conn,
            sede_id=sede_id,
            tipo_documento=tipo_documento,
        )
    return consecutivo


def _build_documento_payload_desde_venta_conn(
    conn,
    venta_id: int,
    datos_cliente: dict | None = None,
    tipo_documento: str = "factura",
) -> dict:
    venta = obtener_venta_header(venta_id)
    if not venta:
        raise ValueError("Venta no encontrada")
    if str(venta.get("estado") or "") != "pagada":
        raise ValueError("La venta debe estar pagada para emitir el documento")

    cliente_base = obtener_cliente(int(venta.get("cliente_id"))) if venta.get("cliente_id") else None
    cliente = _resolver_snapshot_cliente(
        cliente_base={
            **(cliente_base or {}),
            "nombre": (cliente_base or {}).get("nombre") or venta.get("nombre_comprador") or venta.get("cliente_nombre_snapshot"),
            "tipo_doc": (cliente_base or {}).get("tipo_doc") or venta.get("tipo_doc"),
            "numero_doc": (cliente_base or {}).get("numero_doc") or venta.get("numero_doc"),
            "email": (cliente_base or {}).get("email") or venta.get("email_comprador"),
            "empresa": (cliente_base or {}).get("empresa") or venta.get("empresa_comprador"),
            "direccion": (cliente_base or {}).get("direccion") or venta.get("direccion_comprador"),
        },
        datos_cliente=datos_cliente,
    )
    negocio = _resolver_info_negocio_documento_conn(
        conn,
        venta.get("panaderia_id"),
        venta.get("sede_id"),
    )
    items = [_normalizar_item_documento_base(item) for item in (venta.get("items") or [])]
    pagos = [
        {
            "metodo": str(pago.get("metodo") or "").strip(),
            "monto": round(float(pago.get("monto", 0) or 0), 2),
            "recibido": round(float(pago.get("recibido", 0) or 0), 2),
            "cambio": round(float(pago.get("cambio", 0) or 0), 2),
            "referencia": str(pago.get("referencia") or "").strip(),
            "registrado_en": str(pago.get("registrado_en") or "").strip(),
        }
        for pago in (venta.get("pagos") or [])
    ]

    return {
        "origen_tipo": "venta",
        "origen_id": int(venta_id),
        "cliente_id": cliente.get("cliente_id"),
        "tipo_documento": _tipo_documento_normalizado(tipo_documento),
        "fecha_emision": _documento_fecha_str(),
        "fecha_operacion": str(venta.get("fecha") or ""),
        "hora_operacion": str(venta.get("hora") or ""),
        "negocio": negocio,
        "cliente": cliente,
        "origen": {
            "venta_id": int(venta.get("id") or venta_id),
            "tipo_venta": str(venta.get("tipo_venta") or ""),
            "venta_grupo": str(venta.get("venta_grupo") or ""),
            "estado": str(venta.get("estado") or ""),
        },
        "items": items,
        "pagos": pagos,
        "totales": {
            "subtotal": round(float(venta.get("subtotal", 0) or 0), 2),
            "impuestos": 0.0,
            "total": round(float(venta.get("total", 0) or 0), 2),
            "saldo_pendiente": round(float(venta.get("saldo_pendiente", 0) or 0), 2),
        },
        "metodo_pago_resumen": _resumen_metodos_pago(pagos),
        "observaciones": "",
    }


def build_documento_payload_desde_venta(
    venta_id: int,
    datos_cliente: dict | None = None,
    tipo_documento: str = "factura",
) -> dict:
    with get_connection() as conn:
        return _build_documento_payload_desde_venta_conn(
            conn,
            venta_id=venta_id,
            datos_cliente=datos_cliente,
            tipo_documento=tipo_documento,
        )


def _build_documento_payload_desde_pedido_conn(
    conn,
    pedido_id: int,
    datos_cliente: dict | None = None,
    tipo_documento: str = "factura",
) -> dict:
    pedido = _obtener_pedido_conn(conn, pedido_id)
    if not pedido:
        raise ValueError("Pedido no encontrado")
    if str(pedido.get("estado") or "") != "pagado":
        raise ValueError("El pedido debe estar pagado para emitir el documento")

    cliente_base = obtener_cliente(int(pedido.get("cliente_id"))) if pedido.get("cliente_id") else None
    cliente = _resolver_snapshot_cliente(
        cliente_base={
            **(cliente_base or {}),
            "nombre": (cliente_base or {}).get("nombre") or pedido.get("cliente_nombre_snapshot"),
        },
        datos_cliente=datos_cliente,
    )
    negocio = _resolver_info_negocio_documento_conn(
        conn,
        pedido.get("panaderia_id"),
        pedido.get("sede_id"),
    )
    items = [_normalizar_item_documento_base(item) for item in (pedido.get("items") or [])]
    pagos: list[dict] = []
    if pedido.get("metodo_pago"):
        pagos.append(
            {
                "metodo": str(pedido.get("metodo_pago") or "").strip(),
                "monto": round(float(pedido.get("total", 0) or 0) - float(pedido.get("monto_pago_2", 0) or 0), 2),
                "recibido": round(float(pedido.get("monto_recibido", 0) or 0), 2),
                "cambio": round(float(pedido.get("cambio", 0) or 0), 2),
                "registrado_en": str(pedido.get("pagado_en") or ""),
            }
        )
    if pedido.get("metodo_pago_2"):
        pagos.append(
            {
                "metodo": str(pedido.get("metodo_pago_2") or "").strip(),
                "monto": round(float(pedido.get("monto_pago_2", 0) or 0), 2),
                "recibido": round(float(pedido.get("monto_pago_2", 0) or 0), 2),
                "cambio": 0.0,
                "registrado_en": str(pedido.get("pagado_en") or ""),
            }
        )

    return {
        "origen_tipo": "pedido",
        "origen_id": int(pedido_id),
        "cliente_id": cliente.get("cliente_id"),
        "tipo_documento": _tipo_documento_normalizado(tipo_documento),
        "fecha_emision": _documento_fecha_str(),
        "fecha_operacion": str(pedido.get("fecha") or ""),
        "hora_operacion": str((pedido.get("hora_pagado") or pedido.get("hora") or "")),
        "negocio": negocio,
        "cliente": cliente,
        "origen": {
            "pedido_id": int(pedido.get("id") or pedido_id),
            "mesa_id": pedido.get("mesa_id"),
            "mesa_numero": pedido.get("mesa_numero"),
            "mesa_nombre": pedido.get("mesa_nombre"),
            "pagado_por": pedido.get("pagado_por") or "",
            "pagado_en": pedido.get("pagado_en") or "",
        },
        "items": items,
        "pagos": pagos,
        "totales": {
            "subtotal": round(float(pedido.get("total", 0) or 0), 2),
            "impuestos": 0.0,
            "total": round(float(pedido.get("total", 0) or 0), 2),
            "saldo_pendiente": 0.0,
        },
        "metodo_pago_resumen": _resumen_metodos_pago(pagos),
        "observaciones": str(pedido.get("notas") or "").strip(),
    }


def build_documento_payload_desde_pedido(
    pedido_id: int,
    datos_cliente: dict | None = None,
    tipo_documento: str = "factura",
) -> dict:
    with get_connection() as conn:
        return _build_documento_payload_desde_pedido_conn(
            conn,
            pedido_id=pedido_id,
            datos_cliente=datos_cliente,
            tipo_documento=tipo_documento,
        )


def _build_documento_payload_desde_encargo_conn(
    conn,
    encargo_id: int,
    datos_cliente: dict | None = None,
    tipo_documento: str = "factura",
) -> dict:
    encargo = obtener_encargo_v2(encargo_id)
    if not encargo:
        raise ValueError("Encargo no encontrado")

    cliente_base = obtener_cliente(int(encargo.get("cliente_id"))) if encargo.get("cliente_id") else None
    cliente = _resolver_snapshot_cliente(
        cliente_base={
            **(cliente_base or {}),
            "nombre": (cliente_base or {}).get("nombre") or encargo.get("cliente"),
            "empresa": (cliente_base or {}).get("empresa") or encargo.get("empresa"),
        },
        datos_cliente=datos_cliente,
    )
    negocio = _resolver_info_negocio_documento_conn(
        conn,
        encargo.get("panaderia_id"),
        encargo.get("sede_id"),
    )
    items = [_normalizar_item_documento_base(item) for item in (encargo.get("items") or [])]
    pagos = [
        {
            "metodo": str(pago.get("metodo") or "").strip(),
            "monto": round(float(pago.get("monto", 0) or 0), 2),
            "referencia": str(pago.get("referencia") or "").strip(),
            "notas": str(pago.get("notas") or "").strip(),
            "registrado_en": str(pago.get("registrado_en") or ""),
        }
        for pago in (encargo.get("pagos") or [])
    ]
    anticipo = round(float(encargo.get("anticipo", 0) or 0), 2)
    saldo_pendiente = round(float(encargo.get("saldo_pendiente", 0) or 0), 2)

    return {
        "origen_tipo": "encargo",
        "origen_id": int(encargo_id),
        "cliente_id": cliente.get("cliente_id"),
        "tipo_documento": _tipo_documento_normalizado(tipo_documento),
        "fecha_emision": _documento_fecha_str(),
        "fecha_operacion": str(encargo.get("fecha_entrega") or ""),
        "hora_operacion": str(encargo.get("hora_entrega") or ""),
        "negocio": negocio,
        "cliente": cliente,
        "origen": {
            "encargo_id": int(encargo.get("id") or encargo_id),
            "estado": str(encargo.get("estado") or ""),
            "canal_venta": str(encargo.get("canal_venta") or ""),
            "tipo_encargo": str(encargo.get("tipo_encargo") or ""),
            "fecha_entrega": str(encargo.get("fecha_entrega") or ""),
            "hora_entrega": str(encargo.get("hora_entrega") or ""),
            "direccion_entrega": str(encargo.get("direccion_entrega") or "").strip(),
            "telefono": str(encargo.get("telefono") or "").strip(),
            "registrado_por": str(encargo.get("registrado_por") or "").strip(),
        },
        "items": items,
        "pagos": pagos,
        "totales": {
            "subtotal": round(float(encargo.get("total", 0) or 0), 2),
            "impuestos": 0.0,
            "total": round(float(encargo.get("total", 0) or 0), 2),
            "anticipo": anticipo,
            "saldo_pendiente": saldo_pendiente,
        },
        "metodo_pago_resumen": _resumen_metodos_pago(pagos),
        "observaciones": str(encargo.get("notas") or "").strip(),
    }


def build_documento_payload_desde_encargo(
    encargo_id: int,
    datos_cliente: dict | None = None,
    tipo_documento: str = "factura",
) -> dict:
    with get_connection() as conn:
        return _build_documento_payload_desde_encargo_conn(
            conn,
            encargo_id=encargo_id,
            datos_cliente=datos_cliente,
            tipo_documento=tipo_documento,
        )


def _parse_documento_emitido_row(row) -> dict | None:
    if not row:
        return None
    doc = dict(row)
    try:
        doc["payload"] = json.loads(doc.get("payload_json") or "{}")
    except json.JSONDecodeError:
        doc["payload"] = {}
    return doc


def crear_documento_emitido(
    origen_tipo: str,
    origen_id: int,
    payload: dict,
    usuario_id: int | None = None,
) -> dict:
    origen_tipo = _origen_documento_normalizado(origen_tipo)
    if int(payload.get("origen_id") or origen_id) != int(origen_id):
        raise ValueError("El payload no coincide con el origen solicitado")

    with get_connection() as conn:
        usuario_resuelto_id, usuario_nombre = _resolver_usuario_snapshot_conn(conn, usuario_id)
        negocio = payload.get("negocio") or {}
        cliente = payload.get("cliente") or {}
        totales = payload.get("totales") or {}
        tipo_documento = _tipo_documento_normalizado(payload.get("tipo_documento"))
        sede_id = int(negocio.get("sede_id") or 0) or None
        panaderia_id = int(negocio.get("panaderia_id") or 0) or None
        consecutivo_numero, consecutivo = _generar_consecutivo_documento_conn(
            conn,
            sede_id=sede_id,
            tipo_documento=tipo_documento,
        )
        payload_norm = dict(payload)
        payload_norm["tipo_documento"] = tipo_documento
        payload_norm["consecutivo"] = consecutivo
        payload_norm["consecutivo_numero"] = consecutivo_numero
        payload_norm["estado"] = "generado"
        payload_norm["fecha_emision"] = payload_norm.get("fecha_emision") or _documento_fecha_str()

        now_str = _documento_fecha_str()
        cur = conn.execute(
            """
            INSERT INTO documentos_emitidos (
                origen_tipo, origen_id, panaderia_id, sede_id, cliente_id, tipo_documento,
                consecutivo, consecutivo_numero, estado,
                cliente_nombre_snapshot, cliente_tipo_doc_snapshot, cliente_numero_doc_snapshot,
                cliente_email_snapshot, cliente_empresa_snapshot, cliente_direccion_snapshot,
                payload_json, subtotal, impuestos, total, metodo_pago_snapshot,
                emitido_por_usuario_id, emitido_por_nombre_snapshot, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'generado', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                origen_tipo,
                int(origen_id),
                panaderia_id,
                sede_id,
                int(cliente.get("cliente_id") or 0) or None,
                tipo_documento,
                consecutivo,
                consecutivo_numero,
                str(cliente.get("nombre") or "").strip(),
                str(cliente.get("tipo_doc") or "").strip(),
                str(cliente.get("numero_doc") or "").strip(),
                str(cliente.get("email") or "").strip(),
                str(cliente.get("empresa") or "").strip(),
                str(cliente.get("direccion") or "").strip(),
                json.dumps(payload_norm, ensure_ascii=False),
                round(float(totales.get("subtotal", 0) or 0), 2),
                round(float(totales.get("impuestos", 0) or 0), 2),
                round(float(totales.get("total", 0) or 0), 2),
                str(payload_norm.get("metodo_pago_resumen") or "").strip(),
                usuario_resuelto_id,
                usuario_nombre,
                now_str,
                now_str,
            ),
        )
        documento_id = cur.lastrowid
        _vincular_documento_cartera_por_origen_conn(conn, origen_tipo, int(origen_id), documento_id)

        _registrar_audit_conn(
            conn,
            usuario=usuario_nombre,
            usuario_id=usuario_resuelto_id,
            panaderia_id=panaderia_id,
            sede_id=sede_id,
            accion="generar_documento",
            entidad="documento_emitido",
            entidad_id=str(documento_id),
            detalle=f"Documento {consecutivo} generado desde {origen_tipo} #{origen_id}",
            valor_nuevo=json.dumps(
                {
                    "consecutivo": consecutivo,
                    "origen_tipo": origen_tipo,
                    "origen_id": int(origen_id),
                    "tipo_documento": tipo_documento,
                },
                ensure_ascii=False,
            ),
        )
        conn.commit()
    return {
        "ok": True,
        "documento_id": documento_id,
        "consecutivo": consecutivo,
    }


def obtener_documento_emitido(documento_id: int) -> dict | None:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM documentos_emitidos WHERE id = ?",
            (documento_id,),
        ).fetchone()
        documento = _parse_documento_emitido_row(row)
        if not documento:
            return None
        documento["envios"] = [
            dict(envio)
            for envio in conn.execute(
                """
                SELECT id, documento_id, email_destino, estado, intentos, ultimo_error,
                       enviado_por_usuario_id, created_at, sent_at
                FROM documento_envios
                WHERE documento_id = ?
                ORDER BY id DESC
                """,
                (documento_id,),
            ).fetchall()
        ]
    return documento


def obtener_documentos_por_origen(origen_tipo: str, origen_id: int) -> list[dict]:
    origen_tipo = _origen_documento_normalizado(origen_tipo)
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM documentos_emitidos
            WHERE origen_tipo = ? AND origen_id = ?
            ORDER BY id DESC
            """,
            (origen_tipo, int(origen_id)),
        ).fetchall()
    return [_parse_documento_emitido_row(row) for row in rows if row]


def obtener_documentos_recientes(
    limite: int = 20,
    origen_tipo: str | None = None,
    estado: str | None = None,
    tipo_documento: str | None = None,
) -> list[dict]:
    filtros: list[str] = []
    params: list = []
    if origen_tipo:
        filtros.append("origen_tipo = ?")
        params.append(_origen_documento_normalizado(origen_tipo))
    if estado:
        filtros.append("estado = ?")
        params.append(str(estado).strip().lower())
    if tipo_documento:
        filtros.append("tipo_documento = ?")
        params.append(_tipo_documento_normalizado(tipo_documento))
    where = f"WHERE {' AND '.join(filtros)}" if filtros else ""
    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM documentos_emitidos
            {where}
            ORDER BY id DESC
            LIMIT ?
            """,
            tuple(params + [max(1, int(limite or 20))]),
        ).fetchall()
    return [_parse_documento_emitido_row(row) for row in rows if row]


def obtener_documentos_recientes_paginados(
    page: int | None = 1,
    size: int | None = 50,
    origen_tipo: str | None = None,
    estado: str | None = None,
    tipo_documento: str | None = None,
    cliente: str | None = None,
    fecha_desde: str | None = None,
    fecha_hasta: str | None = None,
    estado_envio: str | None = None,
) -> dict:
    filtros: list[str] = []
    params: list = []
    panaderia_id, sede_id = _tenant_scope()
    fecha_desde_norm = str(fecha_desde or "").strip()[:10]
    fecha_hasta_norm = str(fecha_hasta or "").strip()[:10]
    cliente_q = str(cliente or "").strip().lower()
    estado_envio_norm = str(estado_envio or "").strip().lower()
    if fecha_desde_norm:
        try:
            datetime.strptime(fecha_desde_norm, "%Y-%m-%d")
        except ValueError:
            fecha_desde_norm = ""
    if fecha_hasta_norm:
        try:
            datetime.strptime(fecha_hasta_norm, "%Y-%m-%d")
        except ValueError:
            fecha_hasta_norm = ""
    if estado_envio_norm not in {"", "no_enviado", "enviado", "error"}:
        estado_envio_norm = ""
    if panaderia_id is not None:
        filtros.append("d.panaderia_id = ?")
        params.append(panaderia_id)
    if sede_id is not None:
        filtros.append("d.sede_id = ?")
        params.append(sede_id)
    if origen_tipo:
        filtros.append("d.origen_tipo = ?")
        params.append(_origen_documento_normalizado(origen_tipo))
    if estado:
        filtros.append("d.estado = ?")
        params.append(str(estado).strip().lower())
    if tipo_documento:
        filtros.append("d.tipo_documento = ?")
        params.append(_tipo_documento_normalizado(tipo_documento))
    if cliente_q:
        filtros.append(
            """
            (
                LOWER(COALESCE(d.cliente_nombre_snapshot, '')) LIKE ?
                OR LOWER(COALESCE(d.cliente_numero_doc_snapshot, '')) LIKE ?
                OR LOWER(COALESCE(d.cliente_email_snapshot, '')) LIKE ?
                OR LOWER(COALESCE(d.cliente_empresa_snapshot, '')) LIKE ?
            )
            """
        )
        like = f"%{cliente_q}%"
        params.extend([like, like, like, like])
    if fecha_desde_norm:
        filtros.append("substr(COALESCE(d.created_at, ''), 1, 10) >= ?")
        params.append(fecha_desde_norm)
    if fecha_hasta_norm:
        filtros.append("substr(COALESCE(d.created_at, ''), 1, 10) <= ?")
        params.append(fecha_hasta_norm)
    if estado_envio_norm:
        filtros.append("COALESCE(env_latest.estado, 'no_enviado') = ?")
        params.append(estado_envio_norm)
    where = f"WHERE {' AND '.join(filtros)}" if filtros else ""
    page_num, size_num, offset = _sanitize_pagination(page, size)
    from_clause = """
        FROM documentos_emitidos d
        LEFT JOIN documento_envios env_latest ON env_latest.id = (
            SELECT de.id
            FROM documento_envios de
            WHERE de.documento_id = d.id
            ORDER BY de.id DESC
            LIMIT 1
        )
        LEFT JOIN (
            SELECT
                documento_id,
                COUNT(*) AS total_envios,
                SUM(CASE WHEN estado = 'enviado' THEN 1 ELSE 0 END) AS total_envios_exitosos,
                SUM(CASE WHEN estado = 'error' THEN 1 ELSE 0 END) AS total_envios_error
            FROM documento_envios
            GROUP BY documento_id
        ) env_stats ON env_stats.documento_id = d.id
    """
    with get_connection() as conn:
        total = int(conn.execute(
            f"SELECT COUNT(*) AS total {from_clause} {where}",
            tuple(params),
        ).fetchone()["total"] or 0)
        rows = conn.execute(
            f"""
            SELECT
                d.*,
                COALESCE(env_latest.estado, 'no_enviado') AS estado_envio,
                COALESCE(env_latest.email_destino, '') AS ultimo_email_destino,
                COALESCE(env_latest.ultimo_error, '') AS ultimo_error_envio,
                COALESCE(NULLIF(env_latest.sent_at, ''), env_latest.created_at, '') AS ultimo_envio_at,
                COALESCE(env_latest.intentos, 0) AS ultimo_intento_envio,
                COALESCE(env_stats.total_envios, 0) AS total_envios,
                COALESCE(env_stats.total_envios_exitosos, 0) AS total_envios_exitosos,
                COALESCE(env_stats.total_envios_error, 0) AS total_envios_error
            {from_clause}
            {where}
            ORDER BY d.id DESC
            LIMIT ? OFFSET ?
            """,
            tuple(params + [size_num, offset]),
        ).fetchall()
    items = [_parse_documento_emitido_row(row) for row in rows if row]
    return {
        "items": items,
        "pagination": _build_pagination_meta(total, page_num, size_num, len(items)),
    }


def obtener_envios_documento(documento_id: int) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, documento_id, email_destino, estado, intentos, ultimo_error,
                   enviado_por_usuario_id, created_at, sent_at
            FROM documento_envios
            WHERE documento_id = ?
            ORDER BY id DESC
            """,
            (documento_id,),
        ).fetchall()
    return [dict(row) for row in rows]


def registrar_envio_documento(
    documento_id: int,
    email_destino: str,
    estado: str,
    error: str | None = None,
    usuario_id: int | None = None,
) -> dict:
    estado_norm = str(estado or "").strip().lower()
    if estado_norm not in {"pendiente", "enviado", "error"}:
        raise ValueError("Estado de envio invalido")

    email_destino = str(email_destino or "").strip()
    if not email_destino:
        raise ValueError("Email destino requerido")

    with get_connection() as conn:
        documento = conn.execute(
            "SELECT id, panaderia_id, sede_id, consecutivo FROM documentos_emitidos WHERE id = ?",
            (documento_id,),
        ).fetchone()
        if not documento:
            raise ValueError("Documento no encontrado")

        usuario_resuelto_id, usuario_nombre = _resolver_usuario_snapshot_conn(conn, usuario_id)
        prev = conn.execute(
            """
            SELECT intentos
            FROM documento_envios
            WHERE documento_id = ? AND LOWER(email_destino) = LOWER(?)
            ORDER BY id DESC
            LIMIT 1
            """,
            (documento_id, email_destino),
        ).fetchone()
        intentos = int((prev["intentos"] or 0) if prev else 0) + 1
        now_str = _documento_fecha_str()
        sent_at = now_str if estado_norm == "enviado" else ""
        cur = conn.execute(
            """
            INSERT INTO documento_envios (
                documento_id, email_destino, estado, intentos, ultimo_error,
                enviado_por_usuario_id, created_at, sent_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(documento_id),
                email_destino,
                estado_norm,
                intentos,
                str(error or "").strip(),
                usuario_resuelto_id,
                now_str,
                sent_at,
            ),
        )
        if estado_norm == "enviado":
            conn.execute(
                "UPDATE documentos_emitidos SET estado = 'enviado', updated_at = ? WHERE id = ?",
                (now_str, int(documento_id)),
            )

        accion = "reenvio_documento" if intentos > 1 else "envio_documento"
        detalle = f"Documento {documento['consecutivo']} enviado a {email_destino}"
        if estado_norm == "error":
            detalle = f"Fallo envio documento {documento['consecutivo']} a {email_destino}: {str(error or '').strip()}"
        _registrar_audit_conn(
            conn,
            usuario=usuario_nombre,
            usuario_id=usuario_resuelto_id,
            panaderia_id=documento["panaderia_id"],
            sede_id=documento["sede_id"],
            accion=accion,
            entidad="documento_emitido",
            entidad_id=str(documento_id),
            detalle=detalle,
            resultado="ok" if estado_norm == "enviado" else "error",
        )
        conn.commit()
    return {
        "ok": True,
        "envio_id": cur.lastrowid,
        "intentos": intentos,
        "estado": estado_norm,
    }


def _marcar_documento_evento_impresion(
    documento_id: int,
    *,
    accion_audit: str,
    detalle_audit: str,
    usuario_id: int | None = None,
) -> bool:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id, panaderia_id, sede_id, estado, consecutivo FROM documentos_emitidos WHERE id = ?",
            (documento_id,),
        ).fetchone()
        if not row:
            return False
        usuario_resuelto_id, usuario_nombre = _resolver_usuario_snapshot_conn(conn, usuario_id)
        now_str = _documento_fecha_str()
        nuevo_estado = row["estado"]
        if accion_audit == "imprimir_documento" and str(row["estado"] or "") == "generado":
            nuevo_estado = "emitido"
        conn.execute(
            "UPDATE documentos_emitidos SET estado = ?, updated_at = ? WHERE id = ?",
            (nuevo_estado, now_str, int(documento_id)),
        )
        _registrar_audit_conn(
            conn,
            usuario=usuario_nombre,
            usuario_id=usuario_resuelto_id,
            panaderia_id=row["panaderia_id"],
            sede_id=row["sede_id"],
            accion=accion_audit,
            entidad="documento_emitido",
            entidad_id=str(documento_id),
            detalle=detalle_audit or f"Documento {row['consecutivo']} impreso",
            valor_antes=str(row["estado"] or ""),
            valor_nuevo=nuevo_estado,
        )
        conn.commit()
    return True


def marcar_documento_impreso(documento_id: int, usuario_id: int | None = None) -> bool:
    return _marcar_documento_evento_impresion(
        documento_id,
        accion_audit="imprimir_documento",
        detalle_audit=f"Documento #{documento_id} marcado como impreso",
        usuario_id=usuario_id,
    )


def marcar_documento_reimpreso(documento_id: int, usuario_id: int | None = None) -> bool:
    return _marcar_documento_evento_impresion(
        documento_id,
        accion_audit="reimprimir_documento",
        detalle_audit=f"Documento #{documento_id} reimpreso",
        usuario_id=usuario_id,
    )


# ──────────────────────────────────────────────
# Ventas (cajero)
# ──────────────────────────────────────────────

def registrar_venta_lote(items: list[dict], registrado_por: str = "",
                         metodo_pago: str = "efectivo", monto_recibido: float | None = None,
                         referencia_tipo: str = "pos", referencia_id: int | None = None,
                         fecha_hora: datetime | None = None,
                         metodo_pago_2: str | None = None,
                         monto_pago_2: float | None = None) -> dict:
    if not items:
        return {"ok": False, "error": "No hay items para registrar"}

    ahora = fecha_hora or datetime.now()
    fecha = (ahora - timedelta(hours=4)).strftime("%Y-%m-%d")
    hora = ahora.strftime("%H:%M:%S")
    metodo_pago = _metodo_pago_normalizado(metodo_pago)
    metodo_pago_2 = _metodo_pago_normalizado(metodo_pago_2) if metodo_pago_2 else None
    monto_pago_2 = round(float(monto_pago_2), 2) if monto_pago_2 is not None else None

    arqueo = obtener_arqueo_caja_activo()  # sin filtro de fecha: encuentra cualquier arqueo abierto
    if not arqueo:
        return {
            "ok": False,
            "error": "Debes abrir el arqueo de caja antes de registrar ventas",
        }

    total = round(sum(
        float(item.get("total", item.get("cantidad", 0) * item.get("precio", 0)) or 0)
        for item in items
    ), 2)

    if metodo_pago_2 and monto_pago_2 is not None:
        # Pago mixto: monto_recibido cubre la parte del metodo primario
        monto_primario = round(total - monto_pago_2, 2)
        if metodo_pago == "efectivo":
            monto_recibido_final = float(monto_recibido if monto_recibido is not None else monto_primario)
            if monto_recibido_final + 1e-9 < monto_primario:
                return {"ok": False, "error": "El efectivo no alcanza para cubrir su parte del total"}
            cambio = round(monto_recibido_final - monto_primario, 2)
        else:
            monto_recibido_final = monto_primario
            cambio = 0.0
    elif metodo_pago == "transferencia":
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
    panaderia_id, sede_id = _tenant_scope()

    try:
        with get_connection() as conn:
            for item in items:
                producto_id = int(item.get("producto_id", 0) or 0) or None
                producto = str(item.get("producto", "") or "").strip()
                cantidad = int(item.get("cantidad", 0) or 0)
                if not producto or cantidad <= 0:
                    continue

                precio_unitario = float(item.get("precio", 0) or 0)
                total_item = round(float(item.get("total", cantidad * precio_unitario) or 0), 2)

                conn.execute("""
                    INSERT INTO ventas (
                        fecha, hora, producto_id, producto, cantidad, precio_unitario, total,
                        registrado_por, venta_grupo, metodo_pago, monto_recibido,
                        cambio, referencia_tipo, referencia_id, metodo_pago_2, monto_pago_2,
                        panaderia_id, sede_id
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    fecha,
                    hora,
                    producto_id,
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
                    metodo_pago_2,
                    monto_pago_2,
                    panaderia_id,
                    sede_id,
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
            "metodo_pago_2": metodo_pago_2,
            "monto_pago_2": monto_pago_2,
        }
    except Exception as e:
        logger.error(f"registrar_venta_lote: {e}")
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
        fecha = (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")
    filtros = ["fecha = ?"]
    params = [fecha]
    _apply_tenant_scope(filtros, params)
    with get_connection() as conn:
        rows = conn.execute(f"""
            SELECT hora, producto, cantidad, precio_unitario, total, registrado_por,
                   venta_grupo, metodo_pago, monto_recibido, cambio,
                   referencia_tipo, referencia_id
            FROM ventas
            WHERE {' AND '.join(filtros)}
            ORDER BY hora DESC
        """, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def obtener_resumen_ventas_dia(fecha: str = None) -> list[dict]:
    """Resumen agrupado por producto para un dia."""
    if fecha is None:
        fecha = (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")
    filtros = ["fecha = ?"]
    params = [fecha]
    _apply_tenant_scope(filtros, params)
    with get_connection() as conn:
        rows = conn.execute(f"""
            SELECT producto,
                   SUM(cantidad) as total_cantidad,
                   SUM(total) as total_dinero,
                   COUNT(DISTINCT COALESCE(NULLIF(venta_grupo, ''), 'legacy-' || id)) as num_ventas
            FROM ventas
            WHERE {' AND '.join(filtros)}
            GROUP BY producto
            ORDER BY total_dinero DESC
        """, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def obtener_resumen_ventas_por_responsable(fecha: str = None) -> list[dict]:
    """Resume ventas del dia por responsable operativo.

    - Pedidos de mesa: se atribuyen al mesero del pedido.
    - POS directo: se agrupan como POS / caja.
    """
    if fecha is None:
        fecha = (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")
    filtros = ["v.fecha = ?"]
    params = [fecha]
    panaderia_id, sede_id = _tenant_scope()
    if panaderia_id is not None:
        filtros.append("v.panaderia_id = ?")
        params.append(panaderia_id)
    if sede_id is not None:
        filtros.append("v.sede_id = ?")
        params.append(sede_id)
    with get_connection() as conn:
        rows = conn.execute(f"""
            SELECT
                CASE
                    WHEN v.referencia_tipo = 'pedido' THEN COALESCE(NULLIF(p.mesero, ''), 'Sin mesero')
                    ELSE 'POS / caja'
                END AS responsable,
                COALESCE(SUM(v.cantidad), 0) AS unidades,
                COALESCE(SUM(v.total), 0.0) AS total,
                COUNT(DISTINCT COALESCE(NULLIF(v.venta_grupo, ''), 'legacy-' || v.id)) AS transacciones,
                COALESCE(SUM(CASE WHEN v.referencia_tipo = 'pedido' THEN v.total ELSE 0 END), 0.0) AS total_pedidos,
                COALESCE(SUM(CASE WHEN v.referencia_tipo != 'pedido' OR v.referencia_tipo IS NULL THEN v.total ELSE 0 END), 0.0) AS total_pos
            FROM ventas v
            LEFT JOIN pedidos p
              ON v.referencia_tipo = 'pedido' AND v.referencia_id = p.id
            WHERE {' AND '.join(filtros)}
            GROUP BY responsable
            ORDER BY total DESC, responsable ASC
        """, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def obtener_total_ventas_dia(fecha: str = None) -> dict:
    """Total general de ventas del dia."""
    if fecha is None:
        fecha = (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")
    filtros = ["fecha = ?"]
    params = [fecha]
    _apply_tenant_scope(filtros, params)
    with get_connection() as conn:
        row = conn.execute(f"""
            SELECT COALESCE(SUM(cantidad), 0) as panes,
                   COALESCE(SUM(total), 0.0) as dinero,
                   COUNT(DISTINCT COALESCE(NULLIF(venta_grupo, ''), 'legacy-' || id)) as transacciones
            FROM ventas
            WHERE {' AND '.join(filtros)}
        """, tuple(params)).fetchone()
    return dict(row)


def obtener_vendido_dia_producto(fecha: str, producto: str) -> int:
    """Cantidad vendida de un producto en un dia (desde tabla ventas)."""
    filtros = ["fecha = ?", "producto = ?"]
    params = [fecha, producto]
    _apply_tenant_scope(filtros, params)
    with get_connection() as conn:
        row = conn.execute(f"""
            SELECT COALESCE(SUM(cantidad), 0) as vendido
            FROM ventas
            WHERE {' AND '.join(filtros)}
        """, tuple(params)).fetchone()
    return row["vendido"]


def _build_fecha_range_filters(
    dias: int = 30,
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
    campo: str = "fecha",
    include_sede: bool = True,
) -> tuple[list[str], list]:
    filtros: list[str] = []
    params: list = []

    fecha_inicio_str = str(fecha_inicio or "").strip() or None
    fecha_fin_str = str(fecha_fin or "").strip() or None
    if fecha_inicio_str and fecha_fin_str and fecha_inicio_str > fecha_fin_str:
        fecha_inicio_str, fecha_fin_str = fecha_fin_str, fecha_inicio_str

    if fecha_inicio_str and fecha_fin_str:
        filtros.append(f"{campo} BETWEEN ? AND ?")
        params.extend([fecha_inicio_str, fecha_fin_str])
    elif fecha_inicio_str:
        filtros.append(f"{campo} >= ?")
        params.append(fecha_inicio_str)
    elif fecha_fin_str:
        filtros.append(f"{campo} <= ?")
        params.append(fecha_fin_str)
    else:
        filtros.append(f"{campo} >= date('now', ?)")
        params.append(f"-{max(int(dias or 30), 1)} days")

    _apply_tenant_scope(filtros, params, include_sede=include_sede)
    return filtros, params


def obtener_ventas_rango(
    dias: int = 30,
    producto: str | None = None,
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
    limite: int | None = None,
) -> list[dict]:
    """Ventas detalladas de un rango. Soporta fechas explicitas y limite opcional."""
    filtros, params = _build_fecha_range_filters(dias, fecha_inicio, fecha_fin)
    if producto:
        filtros.append("producto = ?")
        params.append(producto)

    query = f"""
        SELECT fecha, hora, producto, cantidad, precio_unitario, total, registrado_por,
               venta_grupo, metodo_pago, monto_recibido, cambio, referencia_tipo, referencia_id
        FROM ventas
        WHERE {' AND '.join(filtros)}
        ORDER BY fecha DESC, hora DESC
    """
    if limite is not None:
        query += "\n        LIMIT ?"
        params.append(max(int(limite or 1), 1))

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def obtener_totales_ventas_rango(
    dias: int = 30,
    producto: str | None = None,
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
) -> dict:
    """Totales agregados de ventas para un rango."""
    filtros, params = _build_fecha_range_filters(dias, fecha_inicio, fecha_fin)
    if producto:
        filtros.append("producto = ?")
        params.append(producto)

    query = f"""
        SELECT COALESCE(SUM(cantidad), 0) as panes,
               COALESCE(SUM(total), 0.0) as dinero,
               COUNT(DISTINCT COALESCE(NULLIF(venta_grupo, ''), 'legacy-' || id)) as transacciones
        FROM ventas
        WHERE {' AND '.join(filtros)}
    """

    with get_connection() as conn:
        row = conn.execute(query, tuple(params)).fetchone()
    return dict(row) if row else {"panes": 0, "dinero": 0.0, "transacciones": 0}


def obtener_serie_ventas_diarias(
    dias: int = 30,
    producto: str | None = None,
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
) -> list[dict]:
    """Serie diaria de panes, ingresos y transacciones."""
    filtros, params = _build_fecha_range_filters(dias, fecha_inicio, fecha_fin)
    if producto:
        filtros.append("producto = ?")
        params.append(producto)

    query = f"""
        SELECT fecha,
               COALESCE(SUM(cantidad), 0) as panes,
               COALESCE(SUM(total), 0.0) as dinero,
               COUNT(DISTINCT COALESCE(NULLIF(venta_grupo, ''), 'legacy-' || id)) as transacciones
        FROM ventas
        WHERE {' AND '.join(filtros)}
        GROUP BY fecha
        ORDER BY fecha ASC
    """

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def obtener_resumen_productos_rango(
    dias: int = 30,
    producto: str | None = None,
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
) -> list[dict]:
    """Ranking de productos por ingresos en un rango."""
    filtros, params = _build_fecha_range_filters(dias, fecha_inicio, fecha_fin)
    if producto:
        filtros.append("producto = ?")
        params.append(producto)

    query = f"""
        SELECT producto,
               COALESCE(SUM(cantidad), 0) as panes,
               COALESCE(SUM(total), 0.0) as dinero,
               COUNT(DISTINCT COALESCE(NULLIF(venta_grupo, ''), 'legacy-' || id)) as transacciones
        FROM ventas
        WHERE {' AND '.join(filtros)}
        GROUP BY producto
        ORDER BY dinero DESC
    """

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [dict(r) for r in rows]


# ──────────────────────────────────────────────
# Registros diarios (produccion - panadero)
# ──────────────────────────────────────────────

def obtener_resumen_medios_pago_rango(
    dias: int = 30,
    producto: str | None = None,
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
) -> list[dict]:
    """Totales por metodo de pago para un rango."""
    filtros, params = _build_fecha_range_filters(dias, fecha_inicio, fecha_fin)
    if producto:
        filtros.append("producto = ?")
        params.append(producto)

    query = f"""
        SELECT COALESCE(NULLIF(metodo_pago, ''), 'efectivo') as metodo,
               COALESCE(SUM(total), 0.0) as total,
               COUNT(DISTINCT COALESCE(NULLIF(venta_grupo, ''), 'legacy-' || id)) as transacciones
        FROM ventas
        WHERE {' AND '.join(filtros)}
        GROUP BY COALESCE(NULLIF(metodo_pago, ''), 'efectivo')
        ORDER BY total DESC
    """

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def obtener_serie_medios_pago_diaria_rango(
    dias: int = 30,
    producto: str | None = None,
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
) -> list[dict]:
    """Serie diaria agregada por metodo de pago y numero de transacciones."""
    filtros, params = _build_fecha_range_filters(dias, fecha_inicio, fecha_fin)
    if producto:
        filtros.append("producto = ?")
        params.append(producto)

    query = f"""
        SELECT fecha,
               COALESCE(SUM(CASE
                   WHEN LOWER(COALESCE(NULLIF(metodo_pago, ''), 'efectivo')) = 'transferencia'
                   THEN total ELSE 0 END), 0.0) as transferencia,
               COALESCE(SUM(CASE
                   WHEN LOWER(COALESCE(NULLIF(metodo_pago, ''), 'efectivo')) = 'transferencia'
                   THEN 0 ELSE total END), 0.0) as efectivo,
               COUNT(DISTINCT COALESCE(NULLIF(venta_grupo, ''), 'legacy-' || id)) as transacciones
        FROM ventas
        WHERE {' AND '.join(filtros)}
        GROUP BY fecha
        ORDER BY fecha ASC
    """

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def obtener_serie_ventas_horaria_rango(
    dias: int = 30,
    producto: str | None = None,
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
) -> list[dict]:
    """Serie horaria agregada de unidades vendidas."""
    filtros, params = _build_fecha_range_filters(dias, fecha_inicio, fecha_fin)
    if producto:
        filtros.append("producto = ?")
        params.append(producto)

    query = f"""
        SELECT SUBSTR(hora, 1, 2) as hora,
               COALESCE(SUM(cantidad), 0) as panes
        FROM ventas
        WHERE {' AND '.join(filtros)}
        GROUP BY SUBSTR(hora, 1, 2)
        ORDER BY SUBSTR(hora, 1, 2) ASC
    """

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def obtener_arqueos_rango(
    dias: int = 30,
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
) -> list[dict]:
    filtros, params = _build_fecha_range_filters(dias, fecha_inicio, fecha_fin)
    query = f"""
        SELECT id, fecha, abierto_en, abierto_por, monto_apertura, estado,
               notas, cerrado_en, cerrado_por, monto_cierre,
               efectivo_esperado, diferencia_cierre, notas_cierre,
               reabierto_en, reabierto_por, motivo_reapertura, reaperturas
        FROM arqueos_caja
        WHERE {' AND '.join(filtros)}
        ORDER BY fecha DESC, abierto_en DESC
    """
    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def obtener_movimientos_caja_rango(
    dias: int = 30,
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
) -> list[dict]:
    filtros, params = _build_fecha_range_filters(dias, fecha_inicio, fecha_fin)
    query = f"""
        SELECT id, arqueo_id, fecha, creado_en, tipo, concepto, monto, registrado_por, notas
        FROM movimientos_caja
        WHERE {' AND '.join(filtros)}
        ORDER BY fecha DESC, creado_en DESC, id DESC
    """
    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def guardar_registro(fecha: str, producto: str,
                     producido: int, vendido: int,
                     observaciones: str = "",
                     sobrante_inicial: int = 0) -> bool:
    dia_semana = datetime.strptime(fecha, "%Y-%m-%d").strftime("%A")
    dias_es = {
        "Monday": "Lunes", "Tuesday": "Martes", "Wednesday": "Miercoles",
        "Thursday": "Jueves", "Friday": "Viernes",
        "Saturday": "Sabado", "Sunday": "Domingo"
    }
    dia_semana = dias_es.get(dia_semana, dia_semana)

    panaderia_id, sede_id = _tenant_scope()
    try:
        with get_connection() as conn:
            scope_filtros = ["fecha = ?", "producto = ?"]
            scope_params: list = [fecha, producto]
            _apply_tenant_scope(scope_filtros, scope_params)
            previo = conn.execute(
                f"SELECT producido FROM registros_diarios WHERE {' AND '.join(scope_filtros)}",
                tuple(scope_params),
            ).fetchone()
            producido_anterior = int(previo["producido"] or 0) if previo else 0
            delta_producido = int(producido or 0) - producido_anterior

            conn.execute("""
                INSERT INTO registros_diarios
                    (fecha, dia_semana, producto, producido, vendido, observaciones,
                     sobrante_inicial, panaderia_id, sede_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(fecha, producto) DO UPDATE SET
                    producido     = excluded.producido,
                    vendido       = excluded.vendido,
                    observaciones = excluded.observaciones
            """, (fecha, dia_semana, producto, producido, vendido, observaciones,
                  int(sobrante_inicial or 0), panaderia_id, sede_id))

            if delta_producido != 0 and _es_producto_panaderia_conn(conn, producto):
                consumo_producto = _consumo_producto(
                    conn, producto, abs(delta_producido), incluir_panaderia=True
                )
                for insumo_id, datos in consumo_producto.items():
                    if delta_producido > 0:
                        conn.execute(
                            """
                            UPDATE insumos
                            SET stock = CASE
                                WHEN stock - ? < 0 THEN 0
                                ELSE stock - ?
                            END
                            WHERE id = ?
                            """,
                            (datos["cantidad"], datos["cantidad"], insumo_id)
                        )
                    else:
                        conn.execute(
                            "UPDATE insumos SET stock = stock + ? WHERE id = ?",
                            (datos["cantidad"], insumo_id)
                        )
            conn.commit()
        return True
    except Exception as e:
        logger.error(f"guardar_registro: {e}")
        return False


def descartar_stock_produccion(
    fecha: str,
    producto: str,
    cantidad: int,
    motivo: str,
    registrado_por: str = "",
    tipo_merma: str = "vencido",
) -> dict:
    fecha = str(fecha or "").strip()
    producto = str(producto or "").strip()
    motivo = str(motivo or "").strip()
    tipo_merma = str(tipo_merma or "vencido").strip().lower() or "vencido"

    try:
        cantidad_int = int(cantidad or 0)
    except (TypeError, ValueError):
        cantidad_int = 0

    if not fecha:
        return {"ok": False, "error": "Fecha requerida"}
    if not producto:
        return {"ok": False, "error": "Producto requerido"}
    if cantidad_int <= 0:
        return {"ok": False, "error": "La cantidad a descartar debe ser mayor a cero"}
    if not motivo:
        return {"ok": False, "error": "Debes indicar el motivo del descarte"}

    dia_semana = datetime.strptime(fecha, "%Y-%m-%d").strftime("%A")
    dias_es = {
        "Monday": "Lunes", "Tuesday": "Martes", "Wednesday": "Miercoles",
        "Thursday": "Jueves", "Friday": "Viernes",
        "Saturday": "Sabado", "Sunday": "Domingo"
    }
    dia_semana = dias_es.get(dia_semana, dia_semana)

    try:
        with get_connection() as conn:
            ventas_row = conn.execute("""
                SELECT COALESCE(SUM(cantidad), 0) AS vendido_real
                FROM ventas
                WHERE fecha = ? AND producto = ?
            """, (fecha, producto)).fetchone()
            vendido_real = int(ventas_row["vendido_real"] or 0) if ventas_row else 0

            comprometido = int(_pedidos_comprometidos_producto_conn(conn, fecha).get(producto, 0) or 0)

            registro = conn.execute("""
                SELECT fecha, dia_semana, producto, producido, vendido, sobrante_inicial, observaciones
                FROM registros_diarios
                WHERE fecha = ? AND producto = ?
            """, (fecha, producto)).fetchone()

            if registro:
                producido_actual = int(registro["producido"] or 0)
                vendido_actual = max(int(registro["vendido"] or 0), vendido_real)
                sobrante_inicial_actual = int(registro["sobrante_inicial"] or 0)
                observaciones_actuales = str(registro["observaciones"] or "").strip()
            else:
                previo = conn.execute("""
                    SELECT fecha, producido, vendido, sobrante_inicial
                    FROM registros_diarios
                    WHERE fecha < ? AND producto = ?
                    ORDER BY fecha DESC, id DESC
                    LIMIT 1
                """, (fecha, producto)).fetchone()
                sobrante_inicial_actual = 0
                if previo:
                    sobrante_inicial_actual = max(
                        int(previo["sobrante_inicial"] or 0)
                        + int(previo["producido"] or 0)
                        - int(previo["vendido"] or 0),
                        0,
                    )
                producido_actual = 0
                vendido_actual = vendido_real
                observaciones_actuales = ""

            stock_total = max(sobrante_inicial_actual + producido_actual - vendido_actual, 0)
            disponible_libre = max(stock_total - comprometido, 0)
            if cantidad_int > disponible_libre:
                return {
                    "ok": False,
                    "error": (
                        f"No puedes descartar {cantidad_int} unidad(es) de {producto}. "
                        f"Solo hay {disponible_libre} libres para retirar."
                    ),
                }

            desde_sobrante = min(cantidad_int, sobrante_inicial_actual)
            restante = cantidad_int - desde_sobrante
            nuevo_sobrante_inicial = sobrante_inicial_actual - desde_sobrante
            nuevo_producido = producido_actual - restante

            nota_descarte = f"Descarte {cantidad_int} und. Motivo: {motivo}"
            observaciones_finales = _combinar_observaciones(observaciones_actuales, nota_descarte)

            conn.execute("""
                INSERT INTO registros_diarios
                    (fecha, dia_semana, producto, producido, vendido, observaciones, sobrante_inicial)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(fecha, producto) DO UPDATE SET
                    producido = excluded.producido,
                    vendido = excluded.vendido,
                    observaciones = excluded.observaciones,
                    sobrante_inicial = excluded.sobrante_inicial
            """, (
                fecha,
                dia_semana,
                producto,
                max(nuevo_producido, 0),
                vendido_actual,
                observaciones_finales,
                max(nuevo_sobrante_inicial, 0),
            ))

            creado_en = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            conn.execute("""
                INSERT INTO mermas (fecha, creado_en, producto, cantidad, tipo, registrado_por, notas)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                fecha,
                creado_en,
                producto,
                float(cantidad_int),
                tipo_merma if tipo_merma in {"sobrante", "vencido", "danado", "consumo_interno", "cortesia", "otro"} else "otro",
                str(registrado_por or ""),
                motivo,
            ))
            conn.commit()

        return {
            "ok": True,
            "cantidad": cantidad_int,
            "producto": producto,
            "desde_sobrante_inicial": desde_sobrante,
            "desde_produccion_hoy": restante,
        }
    except Exception as e:
        logger.error(f"descartar_stock_produccion: {e}")
        return {"ok": False, "error": str(e)}


def obtener_registros(
    producto: str = None,
    dias: int = 30,
    fecha_inicio: str | None = None,
    fecha_fin: str | None = None,
) -> list[dict]:
    filtros, params = _build_fecha_range_filters(dias, fecha_inicio, fecha_fin)
    if producto:
        filtros.append("producto = ?")
        params.append(producto)

    query = f"""
        SELECT fecha, dia_semana, producto, producido, vendido,
               sobrante, sobrante_inicial,
               (sobrante_inicial + sobrante) AS sobrante_total,
               observaciones
        FROM registros_diarios
        WHERE {' AND '.join(filtros)}
        ORDER BY fecha DESC, producto ASC
    """

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def obtener_resumen_por_dia_semana(producto: str) -> dict:
    filtros = ["producto = ?"]
    params: list = [producto]
    _apply_tenant_scope(filtros, params)
    with get_connection() as conn:
        rows = conn.execute(f"""
            SELECT dia_semana,
                   ROUND(AVG(vendido), 1) AS promedio_vendido,
                   COUNT(*) AS muestras
            FROM registros_diarios
            WHERE {' AND '.join(filtros)}
            GROUP BY dia_semana
        """, tuple(params)).fetchall()
    return {r["dia_semana"]: {"promedio": float(r["promedio_vendido"] or 0),
                               "muestras": r["muestras"]} for r in rows}


def contar_registros(producto: str) -> int:
    filtros = ["producto = ?"]
    params: list = [producto]
    _apply_tenant_scope(filtros, params)
    with get_connection() as conn:
        result = conn.execute(
            f"SELECT COUNT(*) as total FROM registros_diarios WHERE {' AND '.join(filtros)}",
            tuple(params),
        ).fetchone()
    return result["total"] if result else 0


# ──────────────────────────────────────────────
# Mesas
# ──────────────────────────────────────────────

def _obtener_mesa_conn(conn, mesa_id: int, include_deleted: bool = False) -> dict | None:
    filtros = ["id = ?"]
    params: list = [mesa_id]
    if not include_deleted:
        filtros.append("COALESCE(eliminada, 0) = 0")
    _apply_tenant_scope(filtros, params)
    row = conn.execute(
        f"""
        SELECT id, numero, nombre, activa, COALESCE(eliminada, 0) AS eliminada,
               panaderia_id, sede_id
        FROM mesas
        WHERE {' AND '.join(filtros)}
        """,
        tuple(params),
    ).fetchone()
    return _row_to_dict(row)


def obtener_mesa(mesa_id: int, include_deleted: bool = False) -> dict | None:
    with get_connection() as conn:
        return _obtener_mesa_conn(conn, mesa_id, include_deleted=include_deleted)


def obtener_mesas(include_inactive: bool = False, include_deleted: bool = False) -> list[dict]:
    filtros = []
    params: list = []
    if not include_inactive:
        filtros.append("activa = 1")
    if not include_deleted:
        filtros.append("COALESCE(eliminada, 0) = 0")
    _apply_tenant_scope(filtros, params)
    where = " AND ".join(filtros) if filtros else "1 = 1"
    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT id, numero, nombre, activa, COALESCE(eliminada, 0) AS eliminada
            FROM mesas
            WHERE {where}
            ORDER BY numero
            """,
            tuple(params),
        ).fetchall()
    return [dict(r) for r in rows]


def _mesa_tiene_pedidos_abiertos_conn(conn, mesa_id: int) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*) AS total
        FROM pedidos
        WHERE mesa_id = ?
          AND estado NOT IN ('pagado', 'cancelado')
          AND unificado_en IS NULL
        """,
        (mesa_id,),
    ).fetchone()
    return bool(int(row["total"] or 0)) if row else False


def agregar_mesa(numero: int, nombre: str = "") -> dict:
    try:
        numero = int(numero or 0)
    except (TypeError, ValueError):
        return {"ok": False, "error": "Numero invalido"}
    if numero <= 0:
        return {"ok": False, "error": "Numero invalido"}

    nombre = str(nombre or "").strip() or f"Mesa {numero}"
    panaderia_id, sede_id = _tenant_scope()
    try:
        with get_connection() as conn:
            scope_filtros = ["numero = ?"]
            scope_params = [numero]
            _apply_tenant_scope(scope_filtros, scope_params)
            existente = conn.execute(
                f"""
                SELECT id, numero, nombre, activa, COALESCE(eliminada, 0) AS eliminada
                FROM mesas
                WHERE {' AND '.join(scope_filtros)}
                """,
                tuple(scope_params),
            ).fetchone()
            if existente and int(existente["activa"] or 0) == 1 and int(existente["eliminada"] or 0) == 0:
                return {"ok": False, "error": "La mesa ya existe y se encuentra activa"}

            if existente:
                conn.execute(
                    "UPDATE mesas SET numero = ?, nombre = ?, activa = 1, eliminada = 0 WHERE id = ?",
                    (numero, nombre, existente["id"]),
                )
                mesa_id = int(existente["id"])
                accion = "reactivada"
                mesa_antes = dict(existente)
            else:
                cursor = conn.execute(
                    """
                    INSERT INTO mesas (numero, nombre, activa, eliminada, panaderia_id, sede_id)
                    VALUES (?, ?, 1, 0, ?, ?)
                    """,
                    (numero, nombre, panaderia_id, sede_id),
                )
                mesa_id = int(cursor.lastrowid or 0)
                accion = "creada"
                mesa_antes = None
            conn.commit()
            mesa = _obtener_mesa_conn(conn, mesa_id, include_deleted=True)
        return {
            "ok": True,
            "accion": accion,
            "mesa": mesa,
            "mesa_antes": mesa_antes,
        }
    except _INTEGRITY_ERRORS:
        return {"ok": False, "error": "Ya existe otra mesa con ese numero"}
    except Exception as exc:
        logger.error(f"agregar_mesa: {exc}")
        return {"ok": False, "error": str(exc)}


def actualizar_mesa(mesa_id: int, numero: int, nombre: str = "") -> dict:
    try:
        mesa_id = int(mesa_id or 0)
        numero = int(numero or 0)
    except (TypeError, ValueError):
        return {"ok": False, "error": "Datos de mesa invalidos"}
    if mesa_id <= 0 or numero <= 0:
        return {"ok": False, "error": "Datos de mesa invalidos"}

    nombre = str(nombre or "").strip() or f"Mesa {numero}"
    try:
        with get_connection() as conn:
            mesa_actual = _obtener_mesa_conn(conn, mesa_id, include_deleted=False)
            if not mesa_actual:
                return {"ok": False, "error": "Mesa no encontrada"}

            duplicado_filtros = ["numero = ?", "id <> ?", "COALESCE(eliminada, 0) = 0"]
            duplicado_params: list = [numero, mesa_id]
            _apply_tenant_scope(duplicado_filtros, duplicado_params)
            duplicado = conn.execute(
                f"SELECT id FROM mesas WHERE {' AND '.join(duplicado_filtros)} LIMIT 1",
                tuple(duplicado_params),
            ).fetchone()
            if duplicado:
                return {"ok": False, "error": "Ya existe otra mesa con ese numero"}

            conn.execute(
                "UPDATE mesas SET numero = ?, nombre = ? WHERE id = ?",
                (numero, nombre, mesa_id),
            )
            conn.commit()
            mesa = _obtener_mesa_conn(conn, mesa_id, include_deleted=False)
        return {
            "ok": True,
            "accion": "actualizada",
            "mesa": mesa,
            "mesa_antes": mesa_actual,
        }
    except _INTEGRITY_ERRORS:
        return {"ok": False, "error": "Ya existe otra mesa con ese numero"}
    except Exception as exc:
        logger.error(f"actualizar_mesa: {exc}")
        return {"ok": False, "error": str(exc)}


def _cambiar_estado_mesa(mesa_id: int, activa: bool) -> dict:
    try:
        mesa_id = int(mesa_id or 0)
    except (TypeError, ValueError):
        return {"ok": False, "error": "Mesa invalida"}
    if mesa_id <= 0:
        return {"ok": False, "error": "Mesa invalida"}

    try:
        with get_connection() as conn:
            mesa_actual = _obtener_mesa_conn(conn, mesa_id, include_deleted=False)
            if not mesa_actual:
                return {"ok": False, "error": "Mesa no encontrada"}

            activa_int = 1 if activa else 0
            if int(mesa_actual.get("activa", 0) or 0) == activa_int:
                return {
                    "ok": True,
                    "accion": "activada" if activa else "desactivada",
                    "mesa": mesa_actual,
                    "mesa_antes": dict(mesa_actual),
                }

            conn.execute(
                "UPDATE mesas SET activa = ? WHERE id = ?",
                (activa_int, mesa_id),
            )
            conn.commit()
            mesa = _obtener_mesa_conn(conn, mesa_id, include_deleted=False)
        return {
            "ok": True,
            "accion": "activada" if activa else "desactivada",
            "mesa": mesa,
            "mesa_antes": mesa_actual,
        }
    except Exception as exc:
        logger.error(f"_cambiar_estado_mesa: {exc}")
        return {"ok": False, "error": str(exc)}


def activar_mesa(mesa_id: int) -> dict:
    return _cambiar_estado_mesa(mesa_id, True)


def desactivar_mesa(mesa_id: int) -> dict:
    return _cambiar_estado_mesa(mesa_id, False)


def eliminar_mesa(mesa_id: int) -> dict:
    try:
        mesa_id = int(mesa_id or 0)
    except (TypeError, ValueError):
        return {"ok": False, "error": "Mesa invalida"}
    if mesa_id <= 0:
        return {"ok": False, "error": "Mesa invalida"}

    try:
        with get_connection() as conn:
            mesa_actual = _obtener_mesa_conn(conn, mesa_id, include_deleted=True)
            if not mesa_actual or int(mesa_actual.get("eliminada", 0) or 0) == 1:
                return {"ok": False, "error": "Mesa no encontrada"}
            if _mesa_tiene_pedidos_abiertos_conn(conn, mesa_id):
                return {
                    "ok": False,
                    "error": "La mesa tiene pedidos abiertos. Desactívala en lugar de eliminarla.",
                    "codigo": "mesa_con_pedido_abierto",
                }

            conn.execute(
                "UPDATE mesas SET activa = 0, eliminada = 1 WHERE id = ?",
                (mesa_id,),
            )
            conn.commit()
        return {
            "ok": True,
            "accion": "eliminada",
            "mesa": {
                **mesa_actual,
                "activa": 0,
                "eliminada": 1,
            },
            "mesa_antes": mesa_actual,
        }
    except Exception as exc:
        logger.error(f"eliminar_mesa: {exc}")
        return {"ok": False, "error": str(exc)}


# ──────────────────────────────────────────────
# Pedidos
# ──────────────────────────────────────────────

def _calcular_total_items_pedido(items: list[dict]) -> float:
    total = 0.0
    for item in items:
        total += _linea_subtotal(
            int(item.get("cantidad", 0) or 0),
            float(item.get("precio_unitario", 0) or 0),
            item.get("modificaciones"),
        )
    return round(total, 2)


def _insertar_items_pedido_conn(conn, pedido_id: int, items: list[dict]) -> None:
    existentes = conn.execute(
        """
        SELECT id, producto_id, producto, cantidad, precio_unitario, notas
        FROM pedido_items
        WHERE pedido_id = ?
        ORDER BY id
        """,
        (pedido_id,),
    ).fetchall()
    existentes_por_id = {int(row["id"]): dict(row) for row in existentes}
    mods_rows = conn.execute(
        """
        SELECT pedido_item_id, tipo, descripcion, cantidad, precio_extra
        FROM pedido_item_modificaciones
        WHERE pedido_item_id IN (
            SELECT id FROM pedido_items WHERE pedido_id = ?
        )
        ORDER BY tipo, id
        """,
        (pedido_id,),
    ).fetchall()
    mods_por_item: dict[int, list[dict]] = {}
    for row in mods_rows:
        mod = dict(row)
        mods_por_item.setdefault(int(mod["pedido_item_id"]), []).append(mod)
    lineas_existentes: dict[tuple, dict] = {}
    for item_id, row in existentes_por_id.items():
        linea_db = {
            "producto_id": row.get("producto_id"),
            "producto": row.get("producto", ""),
            "cantidad": int(row.get("cantidad", 0) or 0),
            "precio_unitario": float(row.get("precio_unitario", 0) or 0),
            "notas": row.get("notas", ""),
            "modificaciones": mods_por_item.get(item_id, []),
        }
        lineas_existentes[_linea_operativa_key(linea_db, ("precio_unitario",))] = {
            "id": item_id,
            "cantidad": int(row.get("cantidad", 0) or 0),
        }

    for item in normalizar_items_pedido(items):
        modificaciones = _normalizar_modificaciones_linea(item.get("modificaciones"))
        line_key = _linea_operativa_key({**item, "modificaciones": modificaciones}, ("precio_unitario",))
        subtotal = _linea_subtotal(
            int(item.get("cantidad", 0) or 0),
            float(item.get("precio_unitario", 0) or 0),
            modificaciones,
        )
        existente = lineas_existentes.get(line_key)
        if existente:
            nueva_cantidad = int(existente["cantidad"] or 0) + int(item.get("cantidad", 0) or 0)
            nuevo_subtotal = _linea_subtotal(
                nueva_cantidad,
                float(item.get("precio_unitario", 0) or 0),
                modificaciones,
            )
            conn.execute(
                """
                UPDATE pedido_items
                SET cantidad = ?, subtotal = ?, notas = ?
                WHERE id = ?
                """,
                (nueva_cantidad, nuevo_subtotal, item.get("notas", ""), int(existente["id"])),
            )
            existente["cantidad"] = nueva_cantidad
            continue
        cur_item = conn.execute("""
            INSERT INTO pedido_items
                (pedido_id, producto_id, producto, cantidad, precio_unitario, subtotal, notas)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            pedido_id,
            int(item.get("producto_id", 0) or 0) or None,
            item["producto"],
            item["cantidad"],
            item["precio_unitario"],
            subtotal,
            item.get("notas", ""),
        ))
        item_id = cur_item.lastrowid
        for mod in modificaciones:
            conn.execute("""
                INSERT INTO pedido_item_modificaciones
                    (pedido_item_id, tipo, descripcion, cantidad, precio_extra)
                VALUES (?, ?, ?, ?, ?)
            """, (
                item_id,
                mod["tipo"],
                mod["descripcion"],
                mod.get("cantidad", 1),
                mod.get("precio_extra", 0),
            ))
        lineas_existentes[line_key] = {"id": cur_item.lastrowid, "cantidad": int(item.get("cantidad", 0) or 0)}


def _limpiar_items_pedido_conn(conn, pedido_id: int) -> None:
    item_rows = conn.execute(
        "SELECT id FROM pedido_items WHERE pedido_id = ?",
        (pedido_id,),
    ).fetchall()
    item_ids = [row["id"] for row in item_rows]
    if item_ids:
        placeholders = ",".join("?" * len(item_ids))
        conn.execute(
            f"DELETE FROM pedido_item_modificaciones WHERE pedido_item_id IN ({placeholders})",
            item_ids,
        )
    conn.execute("DELETE FROM pedido_items WHERE pedido_id = ?", (pedido_id,))


def _crear_pedido_conn(conn, mesa_id: int, mesero: str, items: list[dict],
                       notas: str = "", estado: str = "listo",
                       detalle_historial: str = "Pedido recibido y listo para cobrar",
                       cliente_id: int | None = None,
                       cliente_nombre_snapshot: str = "") -> int:
    items = normalizar_items_pedido(items)
    ahora = datetime.now()
    fecha = ahora.strftime("%Y-%m-%d")
    hora = ahora.strftime("%H:%M:%S")
    creado_en = ahora.strftime("%Y-%m-%d %H:%M:%S")
    total = _calcular_total_items_pedido(items)
    panaderia_id, sede_id = _tenant_scope()

    cursor = conn.execute("""
        INSERT INTO pedidos (
            mesa_id, mesero, estado, fecha, hora, creado_en, notas, total,
            panaderia_id, sede_id, cliente_id, cliente_nombre_snapshot
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        mesa_id, mesero, estado, fecha, hora, creado_en, notas, total,
        panaderia_id, sede_id, int(cliente_id or 0) or None, str(cliente_nombre_snapshot or "").strip(),
    ))
    pedido_id = cursor.lastrowid
    _registrar_historial_estado_pedido(
        conn,
        pedido_id,
        estado,
        cambiado_por=mesero,
        detalle=detalle_historial,
        cambiado_en=creado_en,
    )
    _insertar_items_pedido_conn(conn, pedido_id, items)
    return pedido_id


def _reemplazar_pedido_conn(conn, pedido_id: int, items: list[dict], notas: str = "",
                            estado: str = "listo",
                            cliente_id: int | None = None,
                            cliente_nombre_snapshot: str | None = None) -> float:
    items = normalizar_items_pedido(items)
    total = _calcular_total_items_pedido(items)
    _limpiar_items_pedido_conn(conn, pedido_id)
    _insertar_items_pedido_conn(conn, pedido_id, items)
    if cliente_nombre_snapshot is None:
        conn.execute("""
            UPDATE pedidos
            SET notas = ?, total = ?, estado = ?, cliente_id = COALESCE(cliente_id, ?)
            WHERE id = ?
        """, (notas, total, estado, int(cliente_id or 0) or None, pedido_id))
        return total
    conn.execute("""
        UPDATE pedidos
        SET notas = ?, total = ?, estado = ?, cliente_id = ?, cliente_nombre_snapshot = ?
        WHERE id = ?
    """, (notas, total, estado, int(cliente_id or 0) or None, str(cliente_nombre_snapshot or "").strip(), pedido_id))
    return total

def unir_cuentas_mesa(mesa_id: int) -> dict:
    """Une todos los pedidos activos (pendiente, en_preparacion, listo) de una mesa en un solo pedido."""
    try:
        if not mesa_id:
            return {"ok": False, "error": "Mesa invalida."}
            
        with get_connection() as conn:
            fecha_hoy = (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")
            # 1. Obtener todos los pedidos activos de la mesa HOY
            pedidos = conn.execute(
                "SELECT id, total FROM pedidos WHERE mesa_id = ? AND fecha = ? AND estado IN ('pendiente', 'en_preparacion', 'listo') ORDER BY id ASC",
                (mesa_id, fecha_hoy)
            ).fetchall()
            
            if len(pedidos) <= 1:
                return {"ok": False, "error": "La mesa no tiene multiples pedidos activos para unir."}
                
            # 2. Elegir el pedido principal (el mas antiguo)
            pedido_principal_id = pedidos[0]["id"]
            total_principal = float(pedidos[0]["total"] or 0)
            
            ahora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # 3. Trasladar items y dependencias de los demas pedidos al principal
            for p in pedidos[1:]:
                pid = p["id"]
                ptotal = float(p["total"] or 0)
                
                # Mover items al pedido principal
                conn.execute(
                    "UPDATE pedido_items SET pedido_id = ? WHERE pedido_id = ?",
                    (pedido_principal_id, pid)
                )
                
                # Mover historial de estado si hay algo util (en este caso el trigger)
                conn.execute(
                    "INSERT INTO pedido_estado_historial (pedido_id, estado, cambiado_por, detalle, cambiado_en) "
                    "VALUES (?, ?, 'Sistema', ?, ?)",
                    (pedido_principal_id, 'listo', f"Cuenta fusionada con el pedido #{pid}", ahora)
                )
                
                total_principal += ptotal
                
                # Eliminar los pedidos huérfanos que ya vaciamos
                conn.execute("DELETE FROM pedido_estado_historial WHERE pedido_id = ?", (pid,))
                conn.execute("DELETE FROM pedidos WHERE id = ?", (pid,))
                
            # Actualizar el total del pedido principal, y pasarlo a 'listo'
            conn.execute(
                "UPDATE pedidos SET total = ?, estado = 'listo' WHERE id = ?", 
                (round(total_principal, 2), pedido_principal_id)
            )
            conn.commit()
            return {"ok": True, "pedido_id": pedido_principal_id}
            
    except Exception as e:
        return {"ok": False, "error": str(e)}

def _obtener_pedido_conn(conn, pedido_id: int) -> dict | None:
    pedido = conn.execute("""
        SELECT p.id, p.panaderia_id, p.sede_id, p.mesa_id, m.numero as mesa_numero, m.nombre as mesa_nombre,
               p.mesero, p.estado, p.fecha, p.hora, p.hora_pagado, p.creado_en,
               p.pagado_en, p.pagado_por, p.metodo_pago, p.monto_recibido,
               p.cambio, p.notas, p.total, p.metodo_pago_2, p.monto_pago_2,
               p.cliente_id, p.cliente_nombre_snapshot
        FROM pedidos p
        LEFT JOIN mesas m ON p.mesa_id = m.id
        WHERE p.id = ?
    """, (pedido_id,)).fetchone()
    if not pedido:
        return None

    items = conn.execute("""
        SELECT id, producto_id, producto, cantidad, precio_unitario, subtotal, notas
        FROM pedido_items
        WHERE pedido_id = ?
        ORDER BY id
    """, (pedido_id,)).fetchall()

    items_list = []
    for item in items:
        item_dict = _row_to_dict(item)
        mods = conn.execute("""
            SELECT id, tipo, descripcion, cantidad, precio_extra
            FROM pedido_item_modificaciones
            WHERE pedido_item_id = ?
            ORDER BY tipo, id
        """, (item_dict["id"],)).fetchall()
        item_dict["modificaciones"] = [_row_to_dict(m) for m in mods]
        items_list.append(item_dict)

    historial = conn.execute("""
        SELECT estado, cambiado_en, cambiado_por, detalle
        FROM pedido_estado_historial
        WHERE pedido_id = ?
        ORDER BY cambiado_en ASC, id ASC
    """, (pedido_id,)).fetchall()

    result = _row_to_dict(pedido)
    result["items"] = items_list
    result["historial_estados"] = [_row_to_dict(h) for h in historial]
    return result


def _evento_historial_pedido(pedido: dict, historial: dict, index: int) -> dict:
    estado = str(historial.get("estado", "") or "").strip()
    detalle = str(historial.get("detalle", "") or "").strip()
    detalle_norm = _normalizar_texto_clave(detalle)
    creado_en = str(pedido.get("creado_en", "") or "").strip()
    es_creacion = index == 0 or (creado_en and str(historial.get("cambiado_en", "") or "").strip() == creado_en)

    event_type = "estado"
    label = f"Estado: {estado.replace('_', ' ').capitalize()}" if estado else "Actualizacion"
    if es_creacion:
        event_type = "creado"
        label = "Pedido creado"
    elif estado == "pagado":
        event_type = "cobrado"
        label = "Pedido cobrado"
    elif estado == "cancelado":
        event_type = "cancelado"
        label = "Pedido cancelado"
    elif "actualizacion" in detalle_norm or "agregado por" in detalle_norm:
        event_type = "actualizacion"
        label = "Pedido actualizado"
    elif estado == "listo":
        label = "Listo para caja"
    elif estado == "en_preparacion":
        label = "En preparacion"
    elif estado == "pendiente":
        label = "Pendiente"

    return {
        "tipo": event_type,
        "estado": estado,
        "titulo": label,
        "descripcion": detalle,
        "usuario": str(historial.get("cambiado_por", "") or "").strip(),
        "fecha_hora": str(historial.get("cambiado_en", "") or "").strip(),
        "source": "pedido_estado_historial",
    }


def _obtener_trazabilidad_pedido_conn(conn, pedido_id: int, pedido: dict | None = None) -> list[dict]:
    pedido = pedido or _obtener_pedido_conn(conn, pedido_id)
    if not pedido:
        return []

    eventos: list[dict] = []
    historial = pedido.get("historial_estados") or []
    for index, item in enumerate(historial):
        eventos.append(_evento_historial_pedido(pedido, item, index))

    audit_rows = conn.execute(
        """
        SELECT a.id, a.creado_en, a.usuario, a.accion, a.detalle, a.entidad_id
        FROM audit_log a
        JOIN comandas c
          ON a.entidad = 'comanda'
         AND a.entidad_id = CAST(c.id AS TEXT)
        WHERE c.pedido_id = ?
          AND a.accion IN ('crear_comanda', 'imprimir_comanda', 'reimprimir_comanda')
        ORDER BY a.creado_en ASC, a.id ASC
        """,
        (pedido_id,),
    ).fetchall()

    labels = {
        "crear_comanda": ("comanda_generada", "Comanda generada"),
        "imprimir_comanda": ("comanda_impresa", "Comanda impresa"),
        "reimprimir_comanda": ("comanda_reimpresa", "Comanda reimpresa"),
    }
    for row in audit_rows:
        tipo, titulo = labels.get(str(row["accion"] or "").strip(), ("evento", "Evento de comanda"))
        descripcion = str(row["detalle"] or "").strip()
        comanda_id = str(row["entidad_id"] or "").strip()
        if comanda_id:
            descripcion = f"Comanda #{comanda_id}" + (f" · {descripcion}" if descripcion else "")
        eventos.append({
            "tipo": tipo,
            "estado": "",
            "titulo": titulo,
            "descripcion": descripcion,
            "usuario": str(row["usuario"] or "").strip(),
            "fecha_hora": str(row["creado_en"] or "").strip(),
            "source": "audit_log",
        })

    eventos.sort(key=lambda item: (str(item.get("fecha_hora", "") or ""), str(item.get("titulo", "") or "")))
    return eventos


def _resolver_usuario_comanda_conn(conn, pedido: dict, usuario_id: int | None = None) -> tuple[int | None, str]:
    nombre_snapshot = str(pedido.get("mesero", "") or "").strip()
    if not nombre_snapshot and usuario_id:
        row = conn.execute(
            "SELECT nombre FROM usuarios WHERE id = ?",
            (usuario_id,),
        ).fetchone()
        if row:
            nombre_snapshot = str(row["nombre"] or "").strip()

    resolved_user_id = int(usuario_id or 0) or None
    if nombre_snapshot:
        row = conn.execute(
            """
            SELECT id
            FROM usuarios
            WHERE LOWER(TRIM(nombre)) = LOWER(TRIM(?))
              AND (? IS NULL OR panaderia_id = ?)
              AND (? IS NULL OR sede_id = ?)
            ORDER BY activo DESC, id ASC
            LIMIT 1
            """,
            (
                nombre_snapshot,
                pedido.get("panaderia_id"),
                pedido.get("panaderia_id"),
                pedido.get("sede_id"),
                pedido.get("sede_id"),
            ),
        ).fetchone()
        if row:
            resolved_user_id = int(row["id"] or 0) or resolved_user_id

    return resolved_user_id, (nombre_snapshot or "Sin asignar")


def _ultima_comanda_pedido_conn(conn, pedido_id: int) -> dict | None:
    row = conn.execute(
        """
        SELECT id, pedido_id, es_incremental, estado, created_at
        FROM comandas
        WHERE pedido_id = ?
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (pedido_id,),
    ).fetchone()
    return _row_to_dict(row) or None


def _obtener_items_comanda_conn(conn, comanda_id: int) -> list[dict]:
    rows = conn.execute(
        """
        SELECT id, comanda_id, pedido_item_id, producto_nombre_snapshot, cantidad,
               observacion, modificadores_json, created_at
        FROM comanda_items
        WHERE comanda_id = ?
        ORDER BY id ASC
        """,
        (comanda_id,),
    ).fetchall()
    items: list[dict] = []
    for row in rows:
        item = dict(row)
        item["modificaciones"] = _modificaciones_desde_snapshot(item.get("modificadores_json"))
        items.append(item)
    return items


def _obtener_comanda_conn(conn, comanda_id: int) -> dict | None:
    row = conn.execute(
        """
        SELECT c.id, c.pedido_id, c.panaderia_id, c.sede_id, c.mesa_id,
               c.creada_por_usuario_id, c.creada_por_nombre_snapshot, c.estado,
               c.es_incremental, c.comanda_origen_id, c.nota_general,
               c.created_at, c.updated_at,
               p.fecha AS pedido_fecha, p.hora AS pedido_hora, p.estado AS pedido_estado,
               p.mesero AS pedido_mesero, p.creado_en AS pedido_creado_en,
               m.numero AS mesa_numero, m.nombre AS mesa_nombre,
               s.nombre AS sede_nombre
        FROM comandas c
        JOIN pedidos p ON p.id = c.pedido_id
        LEFT JOIN mesas m ON m.id = c.mesa_id
        LEFT JOIN sedes s ON s.id = c.sede_id
        WHERE c.id = ?
        """,
        (comanda_id,),
    ).fetchone()
    if not row:
        return None
    comanda = dict(row)
    comanda["items"] = _obtener_items_comanda_conn(conn, comanda_id)
    return comanda


def _crear_comanda_desde_pedido_conn(conn, pedido_id: int, usuario_id: int | None = None,
                                     incremental: bool = False) -> dict:
    pedido = _obtener_pedido_conn(conn, pedido_id)
    if not pedido:
        return {"ok": False, "error": "Pedido no encontrado"}

    items = list(pedido.get("items") or [])
    if not items:
        return {"ok": False, "error": "El pedido no tiene items para comandar"}

    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    resolved_user_id, nombre_snapshot = _resolver_usuario_comanda_conn(conn, pedido, usuario_id)
    ultima = _ultima_comanda_pedido_conn(conn, pedido_id)
    comanda_origen_id = ultima["id"] if incremental and ultima else None
    panaderia_id = pedido.get("panaderia_id")
    sede_id = pedido.get("sede_id")
    cursor = conn.execute(
        """
        INSERT INTO comandas (
            pedido_id, panaderia_id, sede_id, mesa_id,
            creada_por_usuario_id, creada_por_nombre_snapshot,
            estado, es_incremental, comanda_origen_id, nota_general,
            created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, 'generada', ?, ?, ?, ?, ?)
        """,
        (
            pedido_id,
            panaderia_id,
            sede_id,
            pedido.get("mesa_id"),
            resolved_user_id,
            nombre_snapshot,
            1 if incremental else 0,
            comanda_origen_id,
            str(pedido.get("notas", "") or "").strip(),
            created_at,
            created_at,
        ),
    )
    comanda_id = cursor.lastrowid

    for item in items:
        conn.execute(
            """
            INSERT INTO comanda_items (
                comanda_id, pedido_item_id, producto_nombre_snapshot, cantidad,
                observacion, modificadores_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                comanda_id,
                item.get("id"),
                str(item.get("producto", "") or "").strip(),
                int(item.get("cantidad", 0) or 0),
                str(item.get("notas", "") or "").strip(),
                _modificaciones_json_para_snapshot(item.get("modificaciones")),
                created_at,
            ),
        )

    _registrar_audit_conn(
        conn,
        usuario=nombre_snapshot,
        usuario_id=resolved_user_id,
        panaderia_id=panaderia_id,
        sede_id=sede_id,
        accion="crear_comanda",
        entidad="comanda",
        entidad_id=str(comanda_id),
        detalle=f"Comanda generada para pedido #{pedido_id}",
        valor_nuevo=json.dumps({
            "pedido_id": pedido_id,
            "mesa_id": pedido.get("mesa_id"),
            "estado": "generada",
            "es_incremental": 1 if incremental else 0,
        }, ensure_ascii=True),
    )
    return {"ok": True, "comanda_id": comanda_id, "pedido_id": pedido_id}


def crear_comanda_desde_pedido(pedido_id: int, usuario_id: int | None = None,
                               incremental: bool = False) -> dict:
    try:
        with get_connection() as conn:
            resultado = _crear_comanda_desde_pedido_conn(
                conn,
                pedido_id=pedido_id,
                usuario_id=usuario_id,
                incremental=incremental,
            )
            if resultado.get("ok"):
                conn.commit()
            return resultado
    except Exception as e:
        logger.error(f"crear_comanda_desde_pedido: {e}")
        return {"ok": False, "error": str(e)}


def obtener_items_comanda(comanda_id: int) -> list[dict]:
    with get_connection() as conn:
        return _obtener_items_comanda_conn(conn, comanda_id)


def obtener_comanda(comanda_id: int) -> dict | None:
    with get_connection() as conn:
        return _obtener_comanda_conn(conn, comanda_id)


def obtener_comandas_por_pedido(pedido_id: int) -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT c.id, c.pedido_id, c.estado, c.es_incremental,
                   c.comanda_origen_id, c.created_at, c.updated_at,
                   COUNT(ci.id) AS total_items
            FROM comandas c
            LEFT JOIN comanda_items ci ON ci.comanda_id = c.id
            WHERE c.pedido_id = ?
            GROUP BY c.id, c.pedido_id, c.estado, c.es_incremental,
                     c.comanda_origen_id, c.created_at, c.updated_at
            ORDER BY c.created_at DESC, c.id DESC
            """,
            (pedido_id,),
        ).fetchall()
    return [dict(row) for row in rows]


def _actualizar_estado_comanda_conn(conn, comanda_id: int, nuevo_estado: str,
                                    accion_audit: str, detalle_audit: str,
                                    actor_nombre: str = "",
                                    actor_id: int | None = None) -> bool:
    comanda = _obtener_comanda_conn(conn, comanda_id)
    if not comanda:
        return False
    updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        "UPDATE comandas SET estado = ?, updated_at = ? WHERE id = ?",
        (nuevo_estado, updated_at, comanda_id),
    )
    _registrar_audit_conn(
        conn,
        usuario=str(actor_nombre or comanda.get("creada_por_nombre_snapshot", "") or ""),
        usuario_id=actor_id if actor_id is not None else comanda.get("creada_por_usuario_id"),
        panaderia_id=comanda.get("panaderia_id"),
        sede_id=comanda.get("sede_id"),
        accion=accion_audit,
        entidad="comanda",
        entidad_id=str(comanda_id),
        detalle=detalle_audit,
        valor_antes=str(comanda.get("estado", "") or ""),
        valor_nuevo=nuevo_estado,
    )
    return True


def marcar_comanda_impresa(comanda_id: int, actor_nombre: str = "", actor_id: int | None = None) -> bool:
    try:
        with get_connection() as conn:
            ok = _actualizar_estado_comanda_conn(
                conn,
                comanda_id,
                nuevo_estado="impresa",
                accion_audit="imprimir_comanda",
                detalle_audit=f"Comanda #{comanda_id} marcada como impresa",
                actor_nombre=actor_nombre,
                actor_id=actor_id,
            )
            if ok:
                conn.commit()
            return ok
    except Exception as e:
        logger.error(f"marcar_comanda_impresa: {e}")
        return False


def marcar_comanda_reimpresa(comanda_id: int, actor_nombre: str = "", actor_id: int | None = None) -> bool:
    try:
        with get_connection() as conn:
            ok = _actualizar_estado_comanda_conn(
                conn,
                comanda_id,
                nuevo_estado="reimpresa",
                accion_audit="reimprimir_comanda",
                detalle_audit=f"Comanda #{comanda_id} reimpresa",
                actor_nombre=actor_nombre,
                actor_id=actor_id,
            )
            if ok:
                conn.commit()
            return ok
    except Exception as e:
        logger.error(f"marcar_comanda_reimpresa: {e}")
        return False


def _cobrar_pedido_conn(conn, pedido: dict, registrado_por: str = "",
                        metodo_pago: str = "efectivo",
                        monto_recibido: float | None = None,
                        detalle_historial: str | None = None,
                        metodo_pago_2: str | None = None,
                        monto_pago_2: float | None = None,
                        cliente_id: int | None = None,
                        cliente_nombre_snapshot: str = "",
                        fecha_vencimiento_credito: str | None = None,
                        usuario_id: int | None = None) -> dict:
    if not pedido:
        raise ValueError("Pedido no encontrado")
    if pedido["estado"] == "pagado":
        raise ValueError("El pedido ya fue pagado")

    ahora = datetime.now()
    fecha_cobro = ahora.strftime("%Y-%m-%d")
    hora_pagado = ahora.strftime("%H:%M:%S")
    pagado_en = ahora.strftime("%Y-%m-%d %H:%M:%S")
    metodo_pago = _metodo_pago_normalizado(metodo_pago)
    metodo_pago_2 = _metodo_pago_normalizado(metodo_pago_2) if metodo_pago_2 else None
    monto_pago_2 = round(float(monto_pago_2), 2) if monto_pago_2 is not None else None

    arqueo = conn.execute("""
        SELECT id
        FROM arqueos_caja
        WHERE estado = 'abierto'
        ORDER BY abierto_en DESC
        LIMIT 1
    """).fetchone()
    if not arqueo:
        raise ValueError("Debes abrir el arqueo de caja antes de cobrar pedidos")

    total_pedido = round(float(pedido["total"] or 0), 2)
    cliente_id_resuelto = int(cliente_id or pedido.get("cliente_id") or 0) or None
    cliente_nombre_final = str(cliente_nombre_snapshot or pedido.get("cliente_nombre_snapshot") or "").strip()

    if metodo_pago_2 and monto_pago_2 is not None:
        monto_primario = round(total_pedido - monto_pago_2, 2)
        if metodo_pago == "efectivo":
            monto_recibido_final = float(monto_recibido if monto_recibido is not None else monto_primario)
            if monto_recibido_final + 1e-9 < monto_primario:
                raise ValueError("El efectivo no alcanza para cubrir su parte del pedido")
            cambio = round(monto_recibido_final - monto_primario, 2)
        else:
            monto_recibido_final = monto_primario
            cambio = 0.0
    elif metodo_pago in {"transferencia", "tarjeta", "credito"}:
        monto_recibido_final = total_pedido
        cambio = 0.0
    else:
        monto_recibido_final = float(monto_recibido if monto_recibido is not None else total_pedido)
        if monto_recibido_final + 1e-9 < total_pedido:
            raise ValueError("El monto recibido no alcanza para cubrir el pedido")
        cambio = round(monto_recibido_final - total_pedido, 2)

    credito_total = 0.0
    monto_primario_real = round(total_pedido - float(monto_pago_2 or 0), 2) if metodo_pago_2 and monto_pago_2 is not None else total_pedido
    if metodo_pago == "credito":
        credito_total += max(monto_primario_real, 0.0)
    if metodo_pago_2 == "credito":
        credito_total += round(float(monto_pago_2 or 0), 2)
    if credito_total > 0.005 and not cliente_id_resuelto:
        raise ValueError("Debes asociar un cliente antes de cobrar un pedido a credito")

    venta_grupo = f"pedido-{pedido['id']}-{uuid4().hex[:10]}"
    for item in pedido["items"]:
        subtotal = round(float(item["subtotal"] or 0), 2)
        cantidad_item = int(item["cantidad"] or 0)
        precio_unitario_venta = round(subtotal / cantidad_item, 2) if cantidad_item > 0 else 0.0
        conn.execute("""
            INSERT INTO ventas (
                fecha, hora, producto_id, producto, cantidad, precio_unitario, total,
                registrado_por, venta_grupo, metodo_pago, monto_recibido,
                cambio, referencia_tipo, referencia_id, metodo_pago_2, monto_pago_2
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pedido', ?, ?, ?)
        """, (
            fecha_cobro,
            hora_pagado,
            int(item.get("producto_id", 0) or 0) or None,
            item["producto"],
            cantidad_item,
            precio_unitario_venta,
            subtotal,
            registrado_por,
            venta_grupo,
            metodo_pago,
            monto_recibido_final,
            cambio,
            pedido["id"],
            metodo_pago_2,
            monto_pago_2,
        ))

        consumo_producto = _consumo_producto(
            conn, item["producto"], cantidad_item, incluir_panaderia=False
        )
        for insumo_id, datos in consumo_producto.items():
            conn.execute(
                """
                UPDATE insumos
                SET stock = CASE
                    WHEN stock - ? < 0 THEN 0
                    ELSE stock - ?
                END
                WHERE id = ?
                """,
                (datos["cantidad"], datos["cantidad"], insumo_id)
            )

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
                        """
                        UPDATE insumos
                        SET stock = CASE
                            WHEN stock - ? < 0 THEN 0
                            ELSE stock - ?
                        END
                        WHERE id = ?
                        """,
                        (datos["cantidad"], datos["cantidad"], insumo_id)
                    )

    conn.execute("""
        UPDATE pedidos
        SET estado = 'pagado',
            hora_pagado = ?,
            pagado_en = ?,
            pagado_por = ?,
            metodo_pago = ?,
            monto_recibido = ?,
            cambio = ?,
            metodo_pago_2 = ?,
            monto_pago_2 = ?,
            cliente_id = ?,
            cliente_nombre_snapshot = ?
        WHERE id = ?
    """, (
        hora_pagado, pagado_en, registrado_por, metodo_pago, monto_recibido_final, cambio,
        metodo_pago_2, monto_pago_2, cliente_id_resuelto, cliente_nombre_final, pedido["id"]
    ))
    detalle_pago = metodo_pago
    if metodo_pago_2:
        detalle_pago += f" + {metodo_pago_2}"
    _registrar_historial_estado_pedido(
        conn,
        pedido["id"],
        "pagado",
        cambiado_por=registrado_por,
        detalle=detalle_historial or f"Cobro registrado por {detalle_pago}",
        cambiado_en=pagado_en,
    )
    cuenta_credito = None
    if credito_total > 0.005:
        cuenta_credito = _crear_cuenta_por_cobrar_conn(
            conn,
            cliente_id=cliente_id_resuelto,
            origen_tipo="pedido",
            origen_id=pedido["id"],
            monto=credito_total,
            fecha_vencimiento=fecha_vencimiento_credito,
            observacion=f"Credito generado desde pedido #{pedido['id']}",
            usuario_id=usuario_id,
            usuario_nombre=registrado_por,
        )
    return {
        "ok": True,
        "pedido_id": pedido["id"],
        "venta_grupo": venta_grupo,
        "fecha": fecha_cobro,
        "hora": hora_pagado,
        "metodo_pago": metodo_pago,
        "monto_recibido": round(monto_recibido_final, 2),
        "cambio": cambio,
        "metodo_pago_2": metodo_pago_2,
        "monto_pago_2": monto_pago_2,
        "total": total_pedido,
        "cuenta_por_cobrar_id": (cuenta_credito or {}).get("cuenta_id"),
        "credito_total": round(credito_total, 2),
    }


def crear_pedido(mesa_id: int, mesero: str, items: list[dict],
                 notas: str = "") -> dict:
    """Crea un pedido o agrega items a uno activo existente de la misma mesa.

    Retorna {"ok": True, "pedido_id": int, "accion": "creado"|"agregado"} o {"ok": False, "error": str}.
    """
    try:
        items = normalizar_items_pedido(items)
        if not items:
            return {"ok": False, "error": "No hay items validos para el pedido"}
        with get_connection() as conn:
            mesa = _obtener_mesa_conn(conn, mesa_id, include_deleted=False)
            if not mesa:
                return {"ok": False, "error": "Mesa no encontrada"}
            if int(mesa.get("activa", 0) or 0) != 1:
                return {"ok": False, "error": "La mesa esta inactiva y no puede recibir pedidos nuevos"}
            fecha_hoy = (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")
            # Si ya hay un pedido activo para esta mesa HOY, agregar los items ahí
            scope_filtros = ["mesa_id = ?", "estado IN ('pendiente', 'en_preparacion', 'listo')",
                             "unificado_en IS NULL", "fecha = ?"]
            scope_params: list = [mesa_id, fecha_hoy]
            _apply_tenant_scope(scope_filtros, scope_params)
            activo = conn.execute(
                f"SELECT id FROM pedidos WHERE {' AND '.join(scope_filtros)} ORDER BY id ASC LIMIT 1",
                tuple(scope_params),
            ).fetchone()

            if activo:
                pedido_id = activo["id"]
                ahora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                _insertar_items_pedido_conn(conn, pedido_id, items)
                nuevo_total_row = conn.execute("""
                    SELECT COALESCE(SUM(subtotal), 0) AS total FROM pedido_items WHERE pedido_id = ?
                """, (pedido_id,)).fetchone()
                nuevo_total = float((nuevo_total_row["total"] if nuevo_total_row else 0) or 0)
                conn.execute(
                    "UPDATE pedidos SET total = ? WHERE id = ?",
                    (round(nuevo_total, 2), pedido_id),
                )
                _registrar_historial_estado_pedido(
                    conn, pedido_id, "listo",
                    cambiado_por=mesero,
                    detalle=f"Productos adicionales agregados por {mesero}",
                    cambiado_en=ahora,
                )
                resultado_comanda = _crear_comanda_desde_pedido_conn(conn, pedido_id)
                if not resultado_comanda.get("ok"):
                    raise ValueError(resultado_comanda.get("error") or "No se pudo generar la comanda")
                conn.commit()
                return {
                    "ok": True,
                    "pedido_id": pedido_id,
                    "accion": "agregado",
                    "comanda_id": resultado_comanda.get("comanda_id"),
                }

            pedido_id = _crear_pedido_conn(conn, mesa_id, mesero, items, notas)
            resultado_comanda = _crear_comanda_desde_pedido_conn(conn, pedido_id)
            if not resultado_comanda.get("ok"):
                raise ValueError(resultado_comanda.get("error") or "No se pudo generar la comanda")
            conn.commit()
        return {
            "ok": True,
            "pedido_id": pedido_id,
            "accion": "creado",
            "comanda_id": resultado_comanda.get("comanda_id"),
        }
    except Exception as e:
        logger.error(f"crear_pedido: {e}")
        return {"ok": False, "error": str(e)}


def _contar_actualizaciones_pedido_conn(conn, pedido_id: int) -> int:
    historial = conn.execute("""
        SELECT detalle
        FROM pedido_estado_historial
        WHERE pedido_id = ?
        ORDER BY id
    """, (pedido_id,)).fetchall()
    return sum(
        1
        for row in historial
        if "actualizacion" in _normalizar_texto_clave(row["detalle"] or "")
    )


def _etiqueta_ordinal_actualizacion(numero: int) -> str:
    ordinales = {
        1: "primera",
        2: "segunda",
        3: "tercera",
        4: "cuarta",
        5: "quinta",
        6: "sexta",
        7: "septima",
        8: "octava",
        9: "novena",
        10: "decima",
    }
    numero = max(1, int(numero or 1))
    return ordinales.get(numero, f"#{numero}")


def actualizar_pedido(pedido_id: int, actualizado_por: str, items: list[dict],
                      notas: str = "", motivo: str = "", rol: str = "") -> dict:
    """Actualiza un pedido existente. Solo caja puede hacerlo."""
    ahora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    actualizado_por = str(actualizado_por or "").strip() or "Caja"
    motivo = str(motivo or "").strip()
    rol = str(rol or "").strip().lower()
    items = normalizar_items_pedido(items)

    try:
        with get_connection() as conn:
            if rol != "cajero":
                return {"ok": False, "error": "Solo caja puede editar pedidos existentes", "status": 403}
            if len(motivo) < 10:
                return {"ok": False, "error": "Debes escribir una razon de al menos 10 caracteres", "status": 400}
            if not items:
                return {"ok": False, "error": "No hay items validos para el pedido", "status": 400}

            pedido = conn.execute("""
                SELECT id, mesa_id, mesero, estado, fecha
                FROM pedidos
                WHERE id = ?
            """, (pedido_id,)).fetchone()
            if not pedido:
                return {"ok": False, "error": "Pedido no encontrado", "status": 404}
            if pedido["estado"] in ("pagado", "cancelado"):
                return {"ok": False, "error": "Este pedido ya no se puede editar", "status": 400}

            numero_actualizacion = _contar_actualizaciones_pedido_conn(conn, pedido_id) + 1
            ordinal = _etiqueta_ordinal_actualizacion(numero_actualizacion)
            detalle_historial = f"Actualizacion {ordinal} por caja. Motivo: {motivo}"

            _reemplazar_pedido_conn(conn, pedido_id, items, notas, estado="listo")
            _registrar_historial_estado_pedido(
                conn,
                pedido_id,
                "listo",
                cambiado_por=actualizado_por,
                detalle=detalle_historial,
                cambiado_en=ahora,
            )
            resultado_comanda = _crear_comanda_desde_pedido_conn(conn, pedido_id)
            if not resultado_comanda.get("ok"):
                raise ValueError(resultado_comanda.get("error") or "No se pudo generar la comanda")
            conn.commit()
        return {
            "ok": True,
            "pedido_id": pedido_id,
            "motivo": motivo,
            "actualizacion_numero": numero_actualizacion,
            "comanda_id": resultado_comanda.get("comanda_id"),
        }
    except Exception as e:
        logger.error(f"actualizar_pedido: {e}")
        return {"ok": False, "error": str(e), "status": 500}


def obtener_pedido_activo_mesa_mesero(mesa_id: int, mesero: str,
                                      fecha: str | None = None) -> dict | None:
    """Retorna el pedido activo más reciente de una mesa para un mesero."""
    fecha = fecha or (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")
    filtros = ["mesa_id = ?", "fecha = ?", "mesero = ?", "estado NOT IN ('pagado', 'cancelado')"]
    params: list = [mesa_id, fecha, mesero]
    _apply_tenant_scope(filtros, params)
    with get_connection() as conn:
        row = conn.execute(
            f"SELECT id FROM pedidos WHERE {' AND '.join(filtros)} ORDER BY creado_en DESC, hora DESC, id DESC LIMIT 1",
            tuple(params),
        ).fetchone()
    if not row:
        return None
    return obtener_pedido(row["id"])


def obtener_pedidos(estado: str = None, mesa_id: int = None,
                    fecha: str = None, mesero: str | None = None) -> list[dict]:
    """Obtiene pedidos filtrados por estado, mesa y/o fecha."""
    if fecha is None:
        fecha = (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")

    filtros = ["p.fecha = ?"]
    params: list = [fecha]
    panaderia_id, sede_id = _tenant_scope()
    if panaderia_id is not None:
        filtros.append("p.panaderia_id = ?")
        params.append(panaderia_id)
    if sede_id is not None:
        filtros.append("p.sede_id = ?")
        params.append(sede_id)
    if estado:
        filtros.append("p.estado = ?")
        params.append(estado)
    if mesa_id:
        filtros.append("p.mesa_id = ?")
        params.append(mesa_id)
    if mesero:
        filtros.append("p.mesero = ?")
        params.append(mesero)

    query = f"""
        SELECT p.id, p.mesa_id, m.numero as mesa_numero, m.nombre as mesa_nombre,
               p.mesero, p.estado, p.fecha, p.hora, p.hora_pagado, p.creado_en,
               p.pagado_en, p.pagado_por, p.metodo_pago, p.monto_recibido,
               p.cambio, p.notas, p.total
        FROM pedidos p
        LEFT JOIN mesas m ON p.mesa_id = m.id
        WHERE {' AND '.join(filtros)}
        ORDER BY p.hora DESC
    """

    with get_connection() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def obtener_pedidos_con_detalle(estado: str = None, mesa_id: int = None,
                                fecha: str = None, mesero: str | None = None) -> list[dict]:
    """Obtiene pedidos con items, modificaciones e historial en pocas queries (sin N+1)."""
    if fecha is None:
        fecha = (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")

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
    if mesero:
        query += " AND p.mesero = ?"
        params.append(mesero)

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
            SELECT id, pedido_id, producto_id, producto, cantidad, precio_unitario, subtotal, notas
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
        pedido = _obtener_pedido_conn(conn, pedido_id)
        if pedido:
            pedido["trazabilidad"] = _obtener_trazabilidad_pedido_conn(conn, pedido_id, pedido=pedido)
        return pedido


def obtener_trazabilidad_pedido(pedido_id: int) -> list[dict]:
    with get_connection() as conn:
        return _obtener_trazabilidad_pedido_conn(conn, pedido_id)


def obtener_pedido_activo_mesa(mesa_id: int, fecha: str | None = None) -> dict | None:
    fecha = fecha or (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")
    filtros = [
        "mesa_id = ?",
        "fecha = ?",
        "estado NOT IN ('pagado', 'cancelado')",
        "unificado_en IS NULL",
    ]
    params: list = [mesa_id, fecha]
    _apply_tenant_scope(filtros, params)
    with get_connection() as conn:
        row = conn.execute(
            f"""
            SELECT id
            FROM pedidos
            WHERE {' AND '.join(filtros)}
            ORDER BY creado_en DESC, hora DESC, id DESC
            LIMIT 1
            """,
            tuple(params),
        ).fetchone()
        if not row:
            return None
        pedido = _obtener_pedido_conn(conn, int(row["id"] or 0))
        if pedido:
            pedido["trazabilidad"] = _obtener_trazabilidad_pedido_conn(conn, int(row["id"] or 0), pedido=pedido)
        return pedido


def obtener_pedidos_con_detalle(fecha: str | None = None, estado: str | None = None,
                                mesa_id: int | None = None, mesero: str | None = None,
                                limit: int | None = None, offset: int = 0) -> list[dict]:
    """Obtiene pedidos con items, modificaciones e historial en queries eficientes (sin N+1)."""
    fecha = fecha or (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")
    with get_connection() as conn:
        filtros = ["p.fecha = ?", "p.unificado_en IS NULL"]
        params: list = [fecha]
        panaderia_id, sede_id = _tenant_scope()
        if panaderia_id is not None:
            filtros.append("p.panaderia_id = ?")
            params.append(panaderia_id)
        if sede_id is not None:
            filtros.append("p.sede_id = ?")
            params.append(sede_id)
        if estado:
            filtros.append("p.estado = ?")
            params.append(estado)
        if mesa_id:
            filtros.append("p.mesa_id = ?")
            params.append(mesa_id)
        if mesero:
            filtros.append("p.mesero = ?")
            params.append(mesero)
        query = f"""
            SELECT p.id, p.panaderia_id, p.sede_id, p.mesa_id, m.numero as mesa_numero, m.nombre as mesa_nombre,
                   p.mesero, p.estado, p.fecha, p.hora, p.hora_pagado, p.creado_en,
                   p.pagado_en, p.pagado_por, p.metodo_pago, p.monto_recibido,
                   p.cambio, p.notas, p.total, p.unificado_en,
                   p.metodo_pago_2, p.monto_pago_2, p.cliente_id, p.cliente_nombre_snapshot
            FROM pedidos p
            LEFT JOIN mesas m ON p.mesa_id = m.id
            WHERE {' AND '.join(filtros)}
            ORDER BY p.hora DESC
        """
        if limit is not None:
            query += "\n            LIMIT ? OFFSET ?"
            params.extend([max(1, int(limit)), max(0, int(offset or 0))])

        pedidos = [dict(r) for r in conn.execute(query, params).fetchall()]
        if not pedidos:
            return []

        pedido_ids = [p["id"] for p in pedidos]
        ph = ",".join("?" * len(pedido_ids))

        items_rows = conn.execute(
            f"SELECT id, pedido_id, producto_id, producto, cantidad, precio_unitario, subtotal, notas "
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


def obtener_pedidos_con_detalle_paginados(
    fecha: str | None = None,
    estado: str | None = None,
    mesa_id: int | None = None,
    mesero: str | None = None,
    page: int | None = 1,
    size: int | None = 50,
) -> dict:
    fecha = fecha or (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")
    page_num, size_num, offset = _sanitize_pagination(page, size)
    with get_connection() as conn:
        filtros = ["p.fecha = ?", "p.unificado_en IS NULL"]
        params: list = [fecha]
        panaderia_id, sede_id = _tenant_scope()
        if panaderia_id is not None:
            filtros.append("p.panaderia_id = ?")
            params.append(panaderia_id)
        if sede_id is not None:
            filtros.append("p.sede_id = ?")
            params.append(sede_id)
        if estado:
            filtros.append("p.estado = ?")
            params.append(estado)
        if mesa_id:
            filtros.append("p.mesa_id = ?")
            params.append(mesa_id)
        if mesero:
            filtros.append("p.mesero = ?")
            params.append(mesero)
        total = int(conn.execute(
            f"SELECT COUNT(*) AS total FROM pedidos p WHERE {' AND '.join(filtros)}",
            tuple(params),
        ).fetchone()["total"] or 0)
    pedidos = obtener_pedidos_con_detalle(
        fecha=fecha,
        estado=estado,
        mesa_id=mesa_id,
        mesero=mesero,
        limit=size_num,
        offset=offset,
    )
    return {
        "items": pedidos,
        "pagination": _build_pagination_meta(total, page_num, size_num, len(pedidos)),
    }


def cambiar_estado_pedido(pedido_id: int, nuevo_estado: str,
                          cambiado_por: str = "",
                          detalle: str | None = None) -> bool:
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
            detalle_final = str(detalle or "").strip() or f"Estado actualizado a {nuevo_estado.replace('_', ' ')}"
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
                detalle=detalle_final,
                cambiado_en=cambiado_en,
            )
            if nuevo_estado == "listo":
                resultado_comanda = _crear_comanda_desde_pedido_conn(conn, pedido_id)
                if not resultado_comanda.get("ok"):
                    raise ValueError(resultado_comanda.get("error") or "No se pudo generar la comanda")
            conn.commit()
        return True
    except Exception as e:
        logger.error(f"cambiar_estado_pedido: {e}")
        return False


def pagar_pedido(pedido_id: int, registrado_por: str = "",
                 metodo_pago: str = "efectivo",
                 monto_recibido: float | None = None,
                 metodo_pago_2: str | None = None,
                 monto_pago_2: float | None = None,
                 cliente_id: int | None = None,
                 cliente_nombre_snapshot: str = "",
                 fecha_vencimiento_credito: str | None = None,
                 usuario_id: int | None = None) -> dict:
    """Marca pedido como pagado, registra ventas y descuenta inventario."""
    try:
        with get_connection() as conn:
            pedido = _obtener_pedido_conn(conn, pedido_id)
            resultado = _cobrar_pedido_conn(
                conn,
                pedido,
                registrado_por=registrado_por,
                metodo_pago=metodo_pago,
                monto_recibido=monto_recibido,
                metodo_pago_2=metodo_pago_2,
                monto_pago_2=monto_pago_2,
                cliente_id=cliente_id,
                cliente_nombre_snapshot=cliente_nombre_snapshot,
                fecha_vencimiento_credito=fecha_vencimiento_credito,
                usuario_id=usuario_id,
            )
            conn.commit()
        return resultado
    except ValueError as e:
        return {"ok": False, "error": str(e)}
    except Exception as e:
        logger.error(f"pagar_pedido: {e}")
        return {"ok": False, "error": str(e)}


def _normalizar_item_para_guardar(item: dict) -> dict:
    return {
        "producto": item["producto"],
        "cantidad": int(item["cantidad"] or 0),
        "precio_unitario": float(item["precio_unitario"] or 0),
        "subtotal": float(item.get("subtotal", 0) or 0),
        "notas": item.get("notas", ""),
        "modificaciones": [
            {
                "tipo": mod["tipo"],
                "descripcion": mod["descripcion"],
                "cantidad": int(mod.get("cantidad", 1) or 1),
                "precio_extra": float(mod.get("precio_extra", 0) or 0),
            }
            for mod in item.get("modificaciones", [])
        ],
    }


def _distribuir_cantidad_proporcional(total_mod: int, cantidad_base: int,
                                      cantidad_split: int) -> tuple[int, int]:
    total_mod = max(int(total_mod or 0), 0)
    cantidad_base = max(int(cantidad_base or 0), 0)
    cantidad_split = max(int(cantidad_split or 0), 0)
    if total_mod <= 0 or cantidad_base <= 0 or cantidad_split <= 0:
        return 0, total_mod
    if cantidad_split >= cantidad_base:
        return total_mod, 0

    asignada = int(round(total_mod * (cantidad_split / cantidad_base)))
    asignada = max(0, min(total_mod, asignada))
    if total_mod > 0 and asignada == 0:
        asignada = 1
    restante = max(total_mod - asignada, 0)
    return asignada, restante


def _separar_modificaciones_item(modificaciones: list[dict], cantidad_total: int,
                                 cantidad_split: int) -> tuple[list[dict], list[dict]]:
    mods_split: list[dict] = []
    mods_restantes: list[dict] = []

    for mod in modificaciones or []:
        base = {
            "tipo": mod["tipo"],
            "descripcion": mod["descripcion"],
            "precio_extra": float(mod.get("precio_extra", 0) or 0),
        }
        if mod["tipo"] == "adicional":
            asignada, restante = _distribuir_cantidad_proporcional(
                int(mod.get("cantidad", 1) or 1),
                cantidad_total,
                cantidad_split,
            )
            if asignada > 0:
                mods_split.append({**base, "cantidad": asignada})
            if restante > 0:
                mods_restantes.append({**base, "cantidad": restante})
        else:
            exclusion = {**base, "cantidad": 1}
            if cantidad_split >= cantidad_total:
                mods_split.append(exclusion)
            elif cantidad_split <= 0:
                mods_restantes.append(exclusion)
            else:
                mods_split.append(exclusion)
                mods_restantes.append(exclusion)

    return mods_split, mods_restantes


def _dividir_item_para_cuenta(item: dict, cantidad_split: int) -> tuple[dict, dict | None]:
    item_base = _normalizar_item_para_guardar(item)
    cantidad_total = max(int(item_base["cantidad"] or 0), 0)
    cantidad_split = max(int(cantidad_split or 0), 0)
    if cantidad_split <= 0 or cantidad_split > cantidad_total:
        raise ValueError("Cantidad invalida al dividir la cuenta")

    mods_split, mods_restantes = _separar_modificaciones_item(
        item_base.get("modificaciones", []),
        cantidad_total,
        cantidad_split,
    )
    item_split = {
        "producto": item_base["producto"],
        "cantidad": cantidad_split,
        "precio_unitario": item_base["precio_unitario"],
        "notas": item_base.get("notas", ""),
        "modificaciones": mods_split,
    }
    restante = cantidad_total - cantidad_split
    item_restante = None
    if restante > 0:
        item_restante = {
            "producto": item_base["producto"],
            "cantidad": restante,
            "precio_unitario": item_base["precio_unitario"],
            "notas": item_base.get("notas", ""),
            "modificaciones": mods_restantes,
        }
    return item_split, item_restante


def dividir_pedido_y_cobrar(pedido_id: int, selecciones: list[dict],
                            registrado_por: str = "",
                            metodo_pago: str = "efectivo",
                            monto_recibido: float | None = None,
                            metodo_pago_2: str | None = None,
                            monto_pago_2: float | None = None,
                            cliente_id: int | None = None,
                            cliente_nombre_snapshot: str = "",
                            fecha_vencimiento_credito: str | None = None,
                            usuario_id: int | None = None) -> dict:
    """Divide un pedido por items/cantidades y cobra solo la parte seleccionada."""
    try:
        seleccion_map: dict[int, int] = {}
        for seleccion in selecciones or []:
            try:
                item_id = int((seleccion or {}).get("item_id", 0) or 0)
                cantidad = int((seleccion or {}).get("cantidad", 0) or 0)
            except (TypeError, ValueError):
                continue
            if item_id > 0 and cantidad > 0:
                seleccion_map[item_id] = cantidad

        if not seleccion_map:
            return {"ok": False, "error": "Selecciona al menos un item para dividir"}

        with get_connection() as conn:
            pedido = _obtener_pedido_conn(conn, pedido_id)
            if not pedido:
                return {"ok": False, "error": "Pedido no encontrado"}
            if pedido["estado"] in ("pagado", "cancelado"):
                return {"ok": False, "error": "Este pedido ya no se puede dividir"}

            items_split: list[dict] = []
            items_restantes: list[dict] = []
            for item in pedido["items"]:
                item_normalizado = _normalizar_item_para_guardar(item)
                cantidad_total = int(item_normalizado["cantidad"] or 0)
                cantidad_seleccionada = min(
                    max(int(seleccion_map.get(int(item["id"]), 0) or 0), 0),
                    cantidad_total,
                )
                if cantidad_seleccionada <= 0:
                    items_restantes.append(item_normalizado)
                    continue
                item_split, item_restante = _dividir_item_para_cuenta(item_normalizado, cantidad_seleccionada)
                items_split.append(item_split)
                if item_restante:
                    items_restantes.append(item_restante)

            if not items_split:
                return {"ok": False, "error": "Selecciona una parte valida del pedido"}
            if not items_restantes:
                return {"ok": False, "error": "Seleccionaste todo el pedido. Usa el cobro normal."}

            pedido_dividido_id = _crear_pedido_conn(
                conn,
                int(pedido["mesa_id"] or 0),
                str(pedido.get("mesero", "") or ""),
                items_split,
                notas=str(pedido.get("notas", "") or ""),
                estado="listo",
                detalle_historial=f"Cuenta dividida desde pedido #{pedido_id}",
                cliente_id=cliente_id or pedido.get("cliente_id"),
                cliente_nombre_snapshot=cliente_nombre_snapshot or pedido.get("cliente_nombre_snapshot") or "",
            )
            pedido_dividido = _obtener_pedido_conn(conn, pedido_dividido_id)
            resultado_pago = _cobrar_pedido_conn(
                conn,
                pedido_dividido,
                registrado_por=registrado_por,
                metodo_pago=metodo_pago,
                monto_recibido=monto_recibido,
                metodo_pago_2=metodo_pago_2,
                monto_pago_2=monto_pago_2,
                detalle_historial=f"Cobro de cuenta dividida por {metodo_pago}",
                cliente_id=cliente_id,
                cliente_nombre_snapshot=cliente_nombre_snapshot,
                fecha_vencimiento_credito=fecha_vencimiento_credito,
                usuario_id=usuario_id,
            )

            restante_total = _reemplazar_pedido_conn(
                conn,
                pedido_id,
                items_restantes,
                notas=str(pedido.get("notas", "") or ""),
                estado="listo",
            )
            _registrar_historial_estado_pedido(
                conn,
                pedido_id,
                "listo",
                cambiado_por=registrado_por,
                detalle=f"Cuenta dividida: se cobraron ${resultado_pago['total']:.2f} en pedido #{pedido_dividido_id}",
            )
            pedido_restante = _obtener_pedido_conn(conn, pedido_id)
            pedido_cobrado = _obtener_pedido_conn(conn, pedido_dividido_id)
            conn.commit()

        resultado_pago["ok"] = True
        resultado_pago["pedido_origen_id"] = pedido_id
        resultado_pago["pedido_dividido_id"] = pedido_dividido_id
        resultado_pago["pedido_restante"] = pedido_restante
        resultado_pago["pedido_cobrado"] = pedido_cobrado
        resultado_pago["total_restante"] = round(float(restante_total or 0), 2)
        return resultado_pago
    except ValueError as e:
        return {"ok": False, "error": str(e)}
    except Exception as e:
        logger.error(f"dividir_pedido_y_cobrar: {e}")
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
        """, (mesa_id, (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d"))).fetchall()
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
        logger.error(f"guardar_configuracion_adicional: {e}")
        return False


# ──────────────────────────────────────────────
# Insumos (inventario)
# ──────────────────────────────────────────────

def obtener_insumos() -> list[dict]:
    filtros = ["activo = 1"]
    params: list = []
    _apply_tenant_scope(filtros, params, include_sede=False)
    with get_connection() as conn:
        rows = conn.execute(
            f"SELECT id, nombre, unidad, stock, stock_minimo, activo FROM insumos WHERE {' AND '.join(filtros)} ORDER BY nombre",
            tuple(params),
        ).fetchall()
    return [dict(r) for r in rows]


def agregar_insumo(nombre: str, unidad: str, stock: float = 0,
                   stock_minimo: float = 0) -> bool:
    panaderia_id, _ = _tenant_scope()
    try:
        with get_connection() as conn:
            conn.execute(
                "INSERT INTO insumos (nombre, unidad, stock, stock_minimo, panaderia_id) VALUES (?, ?, ?, ?, ?)",
                (nombre, unidad, stock, stock_minimo, panaderia_id),
            )
            conn.commit()
        return True
    except _INTEGRITY_ERRORS:
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
    filtros = ["activo = 1", "stock <= stock_minimo"]
    params: list = []
    _apply_tenant_scope(filtros, params, include_sede=False)
    with get_connection() as conn:
        rows = conn.execute(
            f"SELECT id, nombre, unidad, stock, stock_minimo FROM insumos WHERE {' AND '.join(filtros)} ORDER BY (stock / CASE WHEN stock_minimo > 0 THEN stock_minimo ELSE 1 END) ASC",
            tuple(params),
        ).fetchall()
    return [dict(r) for r in rows]


# ──────────────────────────────────────────────
# Recetas
# ──────────────────────────────────────────────

def _categoria_producto_conn(conn, producto: str) -> str:
    row = conn.execute(
        """
        SELECT categoria
        FROM productos
        WHERE nombre = ? AND activo = 1
        ORDER BY es_panaderia DESC, id ASC
        LIMIT 1
        """,
        (producto,)
    ).fetchone()
    return row["categoria"] if row else ""


def _es_producto_panaderia_conn(conn, producto: str) -> bool:
    row = conn.execute(
        """
        SELECT MAX(es_panaderia) AS es_panaderia
        FROM productos
        WHERE nombre = ? AND activo = 1
        """,
        (producto,)
    ).fetchone()
    return bool(int((row["es_panaderia"] or 0) if row else 0))


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

    if _es_producto_panaderia_conn(conn, producto):
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
            cantidad_total_mod = cantidad_mod * max(cantidad, 1.0)
            if not descripcion or cantidad_total_mod <= 0:
                continue
            _acumular_requerimiento_panaderia_modificacion(
                conn, descripcion, cantidad_total_mod, requeridos
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


def _pedidos_comprometidos_producto_conn(conn, fecha: str,
                                         excluir_pedido_id: int | None = None) -> dict[str, int]:
    query = """
        SELECT pi.producto, COALESCE(SUM(pi.cantidad), 0) AS cantidad
        FROM pedido_items pi
        JOIN pedidos p ON p.id = pi.pedido_id
        WHERE p.fecha = ?
          AND p.estado IN ('pendiente', 'en_preparacion', 'listo')
    """
    params: list[object] = [fecha]
    if excluir_pedido_id is not None:
        query += " AND p.id != ?"
        params.append(excluir_pedido_id)
    query += "\n        GROUP BY pi.producto"

    rows = conn.execute(query, tuple(params)).fetchall()
    return {
        str(row["producto"] or ""): int(row["cantidad"] or 0)
        for row in rows
        if str(row["producto"] or "").strip()
    }


def _stock_operativo_detalle_conn(conn, fecha: str,
                                  excluir_pedido_id: int | None = None) -> dict[str, dict]:
    registros = conn.execute("""
        SELECT fecha, producto, producido, vendido, sobrante_inicial, observaciones, id
        FROM registros_diarios
        WHERE fecha <= ?
        ORDER BY producto ASC, fecha DESC, id DESC
    """, (fecha,)).fetchall()

    detalle: dict[str, dict] = {}
    for row in registros:
        producto = str(row["producto"] or "").strip()
        if not producto or producto in detalle:
            continue

        fecha_registro = str(row["fecha"] or "").strip()
        producido = int(row["producido"] or 0)
        vendido_manual = int(row["vendido"] or 0)
        sobrante_inicial = int(row["sobrante_inicial"] or 0)
        tiene_registro_hoy = fecha_registro == fecha
        stock_base = max(sobrante_inicial + producido, 0) if tiene_registro_hoy else max(
            sobrante_inicial + producido - vendido_manual,
            0,
        )

        detalle[producto] = {
            "producto": producto,
            "fecha_registro": fecha_registro,
            "tiene_registro_hoy": tiene_registro_hoy,
            "sobrante_inicial_hoy": sobrante_inicial if tiene_registro_hoy else stock_base,
            "producido_hoy": producido if tiene_registro_hoy else 0,
            "vendido_manual_hoy": vendido_manual if tiene_registro_hoy else 0,
            "stock_base": stock_base,
            "observaciones": str(row["observaciones"] or "").strip(),
        }

    if not detalle:
        return {}

    ventas_rows = conn.execute("""
        SELECT producto, COALESCE(SUM(cantidad), 0) AS vendido_real
        FROM ventas
        WHERE fecha = ?
        GROUP BY producto
    """, (fecha,)).fetchall()
    ventas_totales = {
        str(row["producto"] or ""): int(row["vendido_real"] or 0)
        for row in ventas_rows
        if str(row["producto"] or "").strip()
    }
    comprometidos = _pedidos_comprometidos_producto_conn(conn, fecha, excluir_pedido_id=excluir_pedido_id)

    for producto, info in detalle.items():
        ventas_reales = ventas_totales.get(producto, 0)
        vendido_operativo = max(int(info["vendido_manual_hoy"] or 0), ventas_reales)
        comprometido = int(comprometidos.get(producto, 0) or 0)
        disponible_bruto = max(int(info["stock_base"] or 0) - vendido_operativo, 0)

        info["ventas_reales_hoy"] = ventas_reales
        info["vendido_operativo_hoy"] = vendido_operativo
        info["comprometido_hoy"] = comprometido
        info["disponible_bruto"] = disponible_bruto
        info["disponible_libre"] = max(disponible_bruto - comprometido, 0)

    return detalle


def validar_items_contra_produccion_panaderia(items: list[dict], fecha: str | None = None,
                                              excluir_pedido_id: int | None = None) -> dict:
    fecha = fecha or (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")

    with get_connection() as conn:
        requeridos = _requerimiento_panaderia_items_conn(conn, items)
        if not requeridos:
            return {"ok": True, "faltantes": [], "requeridos": {}, "fecha": fecha}

        stock_operativo = _stock_operativo_detalle_conn(conn, fecha, excluir_pedido_id=excluir_pedido_id)
        comprometidos = _pedidos_comprometidos_panaderia_conn(
            conn, fecha, excluir_pedido_id=excluir_pedido_id
        )

        faltantes = []
        for producto, requerido in requeridos.items():
            detalle = stock_operativo.get(producto, {})
            producido = float(detalle.get("stock_base", 0) or 0)
            comprometido = float(comprometidos.get(producto, 0) or 0)
            disponible = max(float(detalle.get("disponible_bruto", 0) or 0) - comprometido, 0.0)
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
    por el panadero), y descuenta además los pedidos activos y las ventas reales de caja.

    Fórmula: producido - max(vendido_manual, ventas_reales_caja) - comprometido_pedidos_activos
    - vendido_manual: registros_diarios.vendido (el panadero lo ajusta)
    - ventas_reales_caja: suma de las ventas reales POS y mesero ya cobradas (tabla ventas)
    - comprometido_activos: pedidos en estado pendiente/en_preparacion/listo

    Incluye también sobrante arrastrado desde el último registro disponible del producto.
    """
    fecha = fecha or (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")
    with get_connection() as conn:
        detalle = _stock_operativo_detalle_conn(conn, fecha)
    return {
        producto: int(info.get("disponible_libre", 0) or 0)
        for producto, info in detalle.items()
    }


def obtener_stock_operativo_detalle(
    fecha: str | None = None,
    excluir_pedido_id: int | None = None,
) -> dict[str, dict]:
    fecha = fecha or (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")
    with get_connection() as conn:
        detalle = _stock_operativo_detalle_conn(conn, fecha, excluir_pedido_id=excluir_pedido_id)
    return {
        producto: dict(info)
        for producto, info in detalle.items()
    }


def obtener_productos_surtido_disponibles(
    fecha: str | None = None,
    excluir_pedido_id: int | None = None,
) -> list[dict]:
    fecha = fecha or (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")
    with get_connection() as conn:
        _asegurar_surtido_tipo_productos_conn(conn)
        detalle_stock = _stock_operativo_detalle_conn(conn, fecha, excluir_pedido_id=excluir_pedido_id)
        filtros = ["activo = 1", "surtido_tipo != 'none'"]
        params: list[object] = []
        _apply_tenant_scope(filtros, params, include_sede=False)
        rows = conn.execute(f"""
            SELECT id, nombre, precio, categoria, menu, descripcion,
                   es_panaderia, es_adicional, stock_minimo, surtido_tipo
            FROM productos
            WHERE {' AND '.join(filtros)}
            ORDER BY precio ASC, nombre ASC
        """, tuple(params)).fetchall()

    productos: list[dict] = []
    for row in rows:
        nombre = str(row["nombre"] or "").strip()
        stock_info = detalle_stock.get(nombre)
        if not stock_info:
            continue
        disponible = max(int(stock_info.get("disponible_libre", 0) or 0), 0)
        if disponible <= 0:
            continue
        producto = dict(row)
        producto["surtido_tipo"] = _normalizar_surtido_tipo(producto.get("surtido_tipo"))
        producto["disponible"] = disponible
        producto["stock_operativo"] = dict(stock_info)
        productos.append(producto)
    return productos


def _surtido_categorias_producto(producto: dict) -> tuple[str, ...]:
    surtido_tipo = _normalizar_surtido_tipo(producto.get("surtido_tipo"))
    if surtido_tipo == "ambos":
        return ("sal", "dulce")
    if surtido_tipo in {"sal", "dulce"}:
        return (surtido_tipo,)
    return ()


def _seleccionar_candidato_surtido(
    candidatos: list[dict],
    conteos: dict[int, int],
    total_actual: float,
    objetivo: float,
    tolerancia_exceso: float,
    categoria_preferida: str | None = None,
) -> dict | None:
    restantes = []
    for producto in candidatos:
        producto_id = int(producto.get("id", 0) or 0)
        disponibles = int(producto.get("disponible", 0) or 0)
        usados = conteos.get(producto_id, 0)
        if disponibles - usados <= 0:
            continue
        if categoria_preferida and categoria_preferida not in _surtido_categorias_producto(producto):
            continue
        restantes.append(producto)

    if not restantes:
        return None

    restante_objetivo = max(objetivo - total_actual, 0.0)
    permitidos = [
        producto for producto in restantes
        if total_actual + float(producto.get("precio", 0) or 0) <= objetivo + tolerancia_exceso
    ] or restantes

    mejor: dict | None = None
    mejor_puntaje = None
    for producto in permitidos:
        producto_id = int(producto.get("id", 0) or 0)
        precio = float(producto.get("precio", 0) or 0)
        usados = conteos.get(producto_id, 0)
        disponible_restante = max(int(producto.get("disponible", 0) or 0) - usados, 0)
        subtotal_si_agrega = total_actual + precio
        delta_objetivo = abs(objetivo - subtotal_si_agrega)
        exceso = max(subtotal_si_agrega - objetivo, 0.0)
        diversidad_penalidad = usados * 0.32
        disponibilidad_bonus = min(disponible_restante, 4) * 0.025
        preferencia_bonus = 0.05 if categoria_preferida and categoria_preferida in _surtido_categorias_producto(producto) else 0.0
        ajuste_restante = 0.0
        if restante_objetivo > 0:
            ajuste_restante = abs(restante_objetivo - precio) / max(restante_objetivo, 1.0)
        puntaje = (
            (delta_objetivo / max(objetivo, 1.0))
            + (exceso / max(tolerancia_exceso, 1.0)) * 0.55
            + ajuste_restante * 0.35
            + diversidad_penalidad
            - disponibilidad_bonus
            - preferencia_bonus
            + random.random() * 0.08
        )
        if mejor_puntaje is None or puntaje < mejor_puntaje:
            mejor = producto
            mejor_puntaje = puntaje
    return mejor


def generar_surtido_por_valor(
    valor_objetivo: float,
    fecha: str | None = None,
    excluir_pedido_id: int | None = None,
    items_existentes: list[dict] | None = None,
) -> dict:
    objetivo = round(float(valor_objetivo or 0), 2)
    if objetivo <= 0:
        return {"ok": False, "error": "El valor objetivo debe ser mayor a cero"}

    candidatos = obtener_productos_surtido_disponibles(fecha=fecha, excluir_pedido_id=excluir_pedido_id)
    if not candidatos:
        return {"ok": False, "error": "No hay panes aptos para surtido con disponibilidad registrada hoy"}

    reservados_actuales: dict[str, int] = {}
    for item in normalizar_items_pedido(items_existentes):
        producto = str(item.get("producto", "") or "").strip()
        cantidad = int(item.get("cantidad", 0) or 0)
        if producto and cantidad > 0:
            reservados_actuales[producto] = reservados_actuales.get(producto, 0) + cantidad

    candidatos_ajustados: list[dict] = []
    for producto in candidatos:
        disponible = int(producto.get("disponible", 0) or 0) - reservados_actuales.get(str(producto.get("nombre", "") or ""), 0)
        if disponible <= 0:
            continue
        copia = dict(producto)
        copia["disponible"] = disponible
        candidatos_ajustados.append(copia)
    candidatos = candidatos_ajustados
    if not candidatos:
        return {"ok": False, "error": "El carrito actual ya consume toda la disponibilidad de los panes surtidos"}

    precios = [float(producto.get("precio", 0) or 0) for producto in candidatos if float(producto.get("precio", 0) or 0) > 0]
    if not precios:
        return {"ok": False, "error": "Los productos habilitados para surtido no tienen precio valido"}

    candidatos_sal = [producto for producto in candidatos if "sal" in _surtido_categorias_producto(producto)]
    candidatos_dulce = [producto for producto in candidatos if "dulce" in _surtido_categorias_producto(producto)]
    tiene_sal = bool(candidatos_sal)
    tiene_dulce = bool(candidatos_dulce)
    precio_minimo = min(precios)
    tolerancia_exceso = max(round(objetivo * 0.12, 2), precio_minimo * 0.5, 300.0)

    selecciones: list[dict] = []
    conteos: dict[int, int] = {}
    total = 0.0

    categorias_iniciales: list[str | None] = []
    if tiene_sal and tiene_dulce:
        minimo_mixto = min(float(candidatos_sal[0].get("precio", 0) or 0), precio_minimo) + min(float(candidatos_dulce[0].get("precio", 0) or 0), precio_minimo)
        if objetivo + tolerancia_exceso >= minimo_mixto:
            categorias_iniciales = ["sal", "dulce"]
            random.shuffle(categorias_iniciales)
    if not categorias_iniciales:
        categorias_iniciales = ["sal" if tiene_sal else "dulce" if tiene_dulce else None]

    for categoria in categorias_iniciales:
        candidato = _seleccionar_candidato_surtido(
            candidatos,
            conteos,
            total,
            objetivo,
            tolerancia_exceso,
            categoria_preferida=categoria,
        )
        if not candidato:
            continue
        producto_id = int(candidato.get("id", 0) or 0)
        conteos[producto_id] = conteos.get(producto_id, 0) + 1
        total = round(total + float(candidato.get("precio", 0) or 0), 2)
        selecciones.append({
            "producto_id": producto_id,
            "producto": candidato["nombre"],
            "precio_unitario": float(candidato.get("precio", 0) or 0),
            "categoria": candidato.get("categoria", ""),
            "cantidad": 1,
            "surtido_tipo": candidato.get("surtido_tipo", "none"),
            "categoria_surtido": categoria or next(iter(_surtido_categorias_producto(candidato)), "sal"),
            "disponible": int(candidato.get("disponible", 0) or 0),
        })

    max_iteraciones = max(4, min(16, int(math.ceil(objetivo / max(precio_minimo, 1.0))) + 3))
    while len(selecciones) < max_iteraciones:
        restante = objetivo - total
        if restante <= 0 and total >= objetivo - precio_minimo * 0.4:
            break
        categoria_preferida = None
        sal_actual = sum(1 for item in selecciones if item.get("categoria_surtido") == "sal")
        dulce_actual = sum(1 for item in selecciones if item.get("categoria_surtido") == "dulce")
        if tiene_sal and tiene_dulce:
            categoria_preferida = "sal" if sal_actual <= dulce_actual else "dulce"

        candidato = _seleccionar_candidato_surtido(
            candidatos,
            conteos,
            total,
            objetivo,
            tolerancia_exceso,
            categoria_preferida=categoria_preferida,
        )
        if not candidato:
            break

        precio = float(candidato.get("precio", 0) or 0)
        if total + precio > objetivo + tolerancia_exceso:
            break

        producto_id = int(candidato.get("id", 0) or 0)
        conteos[producto_id] = conteos.get(producto_id, 0) + 1
        total = round(total + precio, 2)
        categorias_producto = _surtido_categorias_producto(candidato)
        categoria_final = categoria_preferida if categoria_preferida in categorias_producto else (categorias_producto[0] if categorias_producto else "sal")
        selecciones.append({
            "producto_id": producto_id,
            "producto": candidato["nombre"],
            "precio_unitario": precio,
            "categoria": candidato.get("categoria", ""),
            "cantidad": 1,
            "surtido_tipo": candidato.get("surtido_tipo", "none"),
            "categoria_surtido": categoria_final,
            "disponible": int(candidato.get("disponible", 0) or 0),
        })

        if abs(objetivo - total) <= precio_minimo * 0.35:
            break

    if not selecciones:
        return {"ok": False, "error": "No se pudo construir un surtido valido con el valor indicado"}

    agrupados: dict[tuple[int, str], dict] = {}
    for item in selecciones:
        key = (int(item.get("producto_id", 0) or 0), str(item.get("categoria_surtido", "") or ""))
        if key not in agrupados:
            agrupados[key] = dict(item)
        else:
            agrupados[key]["cantidad"] += 1

    items = list(agrupados.values())
    total = round(sum(float(item["precio_unitario"]) * int(item["cantidad"]) for item in items), 2)
    diferencia = round(objetivo - total, 2)
    categorias_resumen = {
        "sal": sum(int(item["cantidad"]) for item in items if item.get("categoria_surtido") == "sal"),
        "dulce": sum(int(item["cantidad"]) for item in items if item.get("categoria_surtido") == "dulce"),
    }
    mezcla = "mixto"
    if categorias_resumen["sal"] and not categorias_resumen["dulce"]:
        mezcla = "sal"
    elif categorias_resumen["dulce"] and not categorias_resumen["sal"]:
        mezcla = "dulce"

    return {
        "ok": True,
        "fecha": fecha or (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d"),
        "objetivo": objetivo,
        "propuesto": total,
        "diferencia": diferencia,
        "tolerancia_exceso": round(tolerancia_exceso, 2),
        "mezcla": mezcla,
        "categorias": categorias_resumen,
        "total_lineas": len(items),
        "total_unidades": sum(int(item["cantidad"]) for item in items),
        "items": items,
    }


def validar_stock_pedido(items: list[dict], fecha: str | None = None,
                         excluir_pedido_id: int | None = None) -> dict:
    """
    Valida que los items del pedido no superen el stock disponible real.
    Aplica a TODOS los productos con producción registrada hoy, sin importar categoría.
    Si un producto no tiene registro de producción, se permite (sin límite).
    """
    fecha = fecha or (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")

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

    if _es_producto_panaderia_conn(conn, producto) and not incluir_panaderia:
        return

    panaderia_id, _ = _tenant_scope()
    receta = conn.execute("""
        SELECT r.insumo_id, i.nombre, i.unidad, r.cantidad, r.unidad_receta
        FROM recetas r
        JOIN insumos i ON r.insumo_id = i.id
        WHERE r.producto = ? AND r.panaderia_id = ?
    """, (producto, panaderia_id)).fetchall()

    for r in receta:
        key = r["insumo_id"]
        if key not in consumo:
            consumo[key] = {"nombre": r["nombre"], "unidad": r["unidad"], "cantidad": 0.0}
        consumo_base = convertir_cantidad(r["cantidad"], r["unidad_receta"], r["unidad"])
        consumo[key]["cantidad"] += consumo_base * cantidad

    componentes = conn.execute("""
        SELECT componente_producto, cantidad
        FROM producto_componentes
        WHERE producto = ? AND panaderia_id = ?
        ORDER BY componente_producto
    """, (producto, panaderia_id)).fetchall()

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
    panaderia_id, _ = _tenant_scope()
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT r.id, r.insumo_id, i.nombre as insumo_nombre,
                   i.unidad as unidad_inventario,
                   r.unidad_receta, r.cantidad
            FROM recetas r
            JOIN insumos i ON r.insumo_id = i.id
            WHERE r.producto = ? AND r.panaderia_id = ?
            ORDER BY i.nombre
        """, (producto, panaderia_id)).fetchall()
        ficha = conn.execute("""
            SELECT producto, rendimiento_texto, tiempo_preparacion_min,
                   tiempo_amasado_min, tiempo_fermentacion_min,
                   tiempo_horneado_min, temperatura_horneado,
                   pasos, observaciones
            FROM receta_fichas
            WHERE producto = ? AND panaderia_id = ?
        """, (producto, panaderia_id)).fetchone()
        componentes = conn.execute("""
            SELECT pc.id, pc.componente_producto, pc.cantidad,
                   p.categoria as componente_categoria
            FROM producto_componentes pc
            LEFT JOIN productos p ON p.nombre = pc.componente_producto
            WHERE pc.producto = ? AND pc.panaderia_id = ?
            ORDER BY pc.componente_producto
        """, (producto, panaderia_id)).fetchall()

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
    panaderia_id, _ = _tenant_scope()
    try:
        with get_connection() as conn:
            conn.execute(
                "DELETE FROM recetas WHERE producto = ? AND panaderia_id = ?",
                (producto, panaderia_id),
            )
            conn.execute(
                "DELETE FROM producto_componentes WHERE producto = ? AND panaderia_id = ?",
                (producto, panaderia_id),
            )
            for ing in ingredientes:
                conn.execute(
                    "INSERT INTO recetas (producto, insumo_id, cantidad, unidad_receta, panaderia_id) VALUES (?, ?, ?, ?, ?)",
                    (
                        producto,
                        int(ing["insumo_id"]),
                        float(ing["cantidad"]),
                        (ing.get("unidad_receta") or "unidad").strip(),
                        panaderia_id,
                    ),
                )
            for componente in componentes:
                componente_producto = str(componente.get("componente_producto", "") or "").strip()
                if not componente_producto or componente_producto == producto:
                    continue
                conn.execute(
                    "INSERT INTO producto_componentes (producto, componente_producto, cantidad, panaderia_id) VALUES (?, ?, ?, ?)",
                    (
                        producto,
                        componente_producto,
                        float(componente.get("cantidad", 0) or 0),
                        panaderia_id,
                    ),
                )
            # Valida que la composicion no cree ciclos entre productos.
            _consumo_producto(conn, producto, 1, incluir_panaderia=True)
            conn.execute("""
                INSERT INTO receta_fichas (
                    producto, rendimiento_texto, tiempo_preparacion_min,
                    tiempo_amasado_min, tiempo_fermentacion_min,
                    tiempo_horneado_min, temperatura_horneado,
                    pasos, observaciones, panaderia_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(producto, panaderia_id) DO UPDATE SET
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
                panaderia_id,
            ))
            conn.commit()
        return True
    except Exception as e:
        logger.error(f"guardar_receta: {e}")
        return False


def obtener_consumo_diario(fecha: str = None) -> list[dict]:
    """Calcula el consumo teorico del dia combinando produccion y pedidos pagados."""
    if fecha is None:
        fecha = (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")

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
        fecha = (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")

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


def obtener_resumen_mesas(include_inactive: bool = False) -> list[dict]:
    """Resumen de mesas con sus pedidos activos."""
    mesas = obtener_mesas(include_inactive=include_inactive)
    hoy = (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")
    resultado = []
    with get_connection() as conn:
        for mesa in mesas:
            pedidos = conn.execute("""
                SELECT COUNT(*) as num_pedidos,
                       COALESCE(SUM(total), 0) as total_mesa
                FROM pedidos
                WHERE mesa_id = ? AND estado NOT IN ('pagado', 'cancelado')
                  AND unificado_en IS NULL
                  AND fecha = ?
            """, (mesa["id"], hoy)).fetchone()
            mesa["num_pedidos"] = pedidos["num_pedidos"]
            mesa["total_mesa"] = pedidos["total_mesa"]
            # Estado de la mesa
            ultimo = conn.execute("""
                SELECT id, estado FROM pedidos
                WHERE mesa_id = ? AND estado NOT IN ('pagado', 'cancelado')
                  AND unificado_en IS NULL
                  AND fecha = ?
                ORDER BY creado_en DESC, hora DESC, id DESC LIMIT 1
            """, (mesa["id"], hoy)).fetchone()
            mesa["estado_mesa"] = ultimo["estado"] if ultimo else "libre"
            mesa["pedido_activo_id"] = int(ultimo["id"] or 0) if ultimo else None
            resultado.append(mesa)
    return resultado


# ──────────────────────────────────────────────
# Audit Log
# ──────────────────────────────────────────────

def _registrar_audit_conn(
    conn,
    usuario: str,
    accion: str,
    entidad: str = "",
    entidad_id: str = "",
    detalle: str = "",
    valor_antes: str = "",
    valor_nuevo: str = "",
    usuario_id: int | None = None,
    panaderia_id: int | None = None,
    sede_id: int | None = None,
    ip: str = "",
    user_agent: str = "",
    resultado: str = "ok",
    request_id: str = "",
) -> None:
    ahora = datetime.now()
    fecha = ahora.strftime("%Y-%m-%d")
    creado_en = ahora.strftime("%Y-%m-%d %H:%M:%S")
    conn.execute("""
        INSERT INTO audit_log
            (fecha, creado_en, usuario, usuario_id, panaderia_id, sede_id, ip, user_agent,
             request_id, accion, resultado, entidad, entidad_id, detalle, valor_antes, valor_nuevo)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        fecha,
        creado_en,
        str(usuario or ""),
        usuario_id,
        panaderia_id,
        sede_id,
        str(ip or ""),
        str(user_agent or ""),
        str(request_id or ""),
        str(accion or ""),
        str(resultado or "ok"),
        str(entidad or ""),
        str(entidad_id or ""),
        str(detalle or ""),
        str(valor_antes or ""),
        str(valor_nuevo or ""),
    ))


def registrar_audit(
    usuario: str,
    accion: str,
    entidad: str = "",
    entidad_id: str = "",
    detalle: str = "",
    valor_antes: str = "",
    valor_nuevo: str = "",
    usuario_id: int | None = None,
    panaderia_id: int | None = None,
    sede_id: int | None = None,
    ip: str = "",
    user_agent: str = "",
    resultado: str = "ok",
    request_id: str = "",
) -> None:
    """Registra una acción crítica en el audit log."""
    ahora = datetime.now()
    fecha = ahora.strftime("%Y-%m-%d")
    creado_en = ahora.strftime("%Y-%m-%d %H:%M:%S")
    try:
        with get_connection() as conn:
            _registrar_audit_conn(
                conn,
                usuario=usuario,
                accion=accion,
                entidad=entidad,
                entidad_id=entidad_id,
                detalle=detalle,
                valor_antes=valor_antes,
                valor_nuevo=valor_nuevo,
                usuario_id=usuario_id,
                panaderia_id=panaderia_id,
                sede_id=sede_id,
                ip=ip,
                user_agent=user_agent,
                resultado=resultado,
                request_id=request_id,
            )
            conn.commit()
    except Exception as e:
        logger.error(f"registrar_audit: {e}")


def obtener_audit_log(dias: int = 30, limite: int = 200) -> list[dict]:
    """Devuelve entradas recientes del audit log."""
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT id, fecha, creado_en, usuario, usuario_id, panaderia_id, sede_id,
                   ip, user_agent, request_id, accion, resultado, entidad, entidad_id,
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
    fecha = fecha or (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")
    v_filtros = ["fecha = ?"]
    v_params: list = [fecha]
    _apply_tenant_scope(v_filtros, v_params)
    panaderia_id, sede_id = _tenant_scope()
    with get_connection() as conn:
        # Ventas del cajero
        rows_ventas = conn.execute(f"""
            SELECT producto,
                   COALESCE(SUM(cantidad), 0) as unidades,
                   COALESCE(SUM(total), 0.0) as ingresos
            FROM ventas
            WHERE {' AND '.join(v_filtros)}
            GROUP BY producto
        """, tuple(v_params)).fetchall()

        # Ventas via pedidos de mesa (estado pagado)
        p_filtros = ["p.fecha = ?", "p.estado = 'pagado'"]
        p_params: list = [fecha]
        if panaderia_id is not None:
            p_filtros.append("p.panaderia_id = ?")
            p_params.append(panaderia_id)
        if sede_id is not None:
            p_filtros.append("p.sede_id = ?")
            p_params.append(sede_id)
        rows_pedidos = conn.execute(f"""
            SELECT pi.producto,
                   COALESCE(SUM(pi.cantidad), 0) as unidades,
                   COALESCE(SUM(pi.subtotal), 0.0) as ingresos
            FROM pedido_items pi
            JOIN pedidos p ON p.id = pi.pedido_id
            WHERE {' AND '.join(p_filtros)}
            GROUP BY pi.producto
        """, tuple(p_params)).fetchall()

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
    fecha = fecha or (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")
    with get_connection() as conn:
        productos = conn.execute("""
            SELECT MIN(id) AS id, nombre, MAX(stock_minimo) AS stock_minimo
            FROM productos
            WHERE activo = 1 AND es_panaderia = 1
            GROUP BY nombre
        """).fetchall()
        stock_operativo = _stock_operativo_detalle_conn(conn, fecha)

    resultado = []
    for p in productos:
        nombre = p["nombre"]
        stock_minimo = int(p["stock_minimo"] or 0)
        reg = stock_operativo.get(nombre)

        if reg is None:
            # No hay registro del día = sin datos
            estado = "sin_datos"
            disponible = None
        else:
            disponible = max(int(reg.get("disponible_libre", 0) or 0), 0)
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
        logger.error(f"guardar_ajuste_pronostico: {e}")
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
    panaderia_id, sede_id = _tenant_scope()
    try:
        with get_connection() as conn:
            conn.execute("""
                INSERT INTO mermas (fecha, creado_en, producto, cantidad, tipo, registrado_por, notas, panaderia_id, sede_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (fecha, creado_en, producto, float(cantidad), tipo, registrado_por, notas, panaderia_id, sede_id))
            conn.commit()
        return True
    except Exception as e:
        logger.error(f"registrar_merma: {e}")
        return False


def obtener_mermas_dia(fecha: str | None = None) -> list[dict]:
    fecha = fecha or (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")
    filtros = ["fecha = ?"]
    params: list = [fecha]
    _apply_tenant_scope(filtros, params)
    with get_connection() as conn:
        rows = conn.execute(f"""
            SELECT id, fecha, creado_en, producto, cantidad, tipo, registrado_por, notas
            FROM mermas WHERE {' AND '.join(filtros)}
            ORDER BY creado_en DESC
        """, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def obtener_resumen_mermas(dias: int = 30) -> list[dict]:
    filtros = [f"fecha >= date('now', ?)"]
    params: list = [f"-{dias} days"]
    _apply_tenant_scope(filtros, params)
    with get_connection() as conn:
        rows = conn.execute(f"""
            SELECT producto, tipo,
                   COALESCE(SUM(cantidad), 0) as total_unidades,
                   COUNT(*) as registros
            FROM mermas
            WHERE {' AND '.join(filtros)}
            GROUP BY producto, tipo
            ORDER BY total_unidades DESC
        """, tuple(params)).fetchall()
    return [dict(r) for r in rows]


# ──────────────────────────────────────────────
# Días Especiales / Festivos
# ──────────────────────────────────────────────

def obtener_dias_especiales(fecha_inicio: str | None = None, fecha_fin: str | None = None) -> list[dict]:
    """Devuelve días especiales en un rango de fechas."""
    filtros = ["activo = 1"]
    params: list = []
    _apply_tenant_scope(filtros, params, include_sede=False)
    if fecha_inicio and fecha_fin:
        filtros.append("fecha BETWEEN ? AND ?")
        params += [fecha_inicio, fecha_fin]
    with get_connection() as conn:
        rows = conn.execute(
            f"SELECT id, fecha, descripcion, factor, tipo, activo FROM dias_especiales WHERE {' AND '.join(filtros)} ORDER BY fecha ASC",
            tuple(params),
        ).fetchall()
    return [dict(r) for r in rows]


def obtener_factor_dia_especial(fecha: str) -> float:
    """Devuelve el factor multiplicador para una fecha especial (1.0 si no es especial)."""
    filtros = ["fecha = ?", "activo = 1"]
    params: list = [fecha]
    _apply_tenant_scope(filtros, params, include_sede=False)
    with get_connection() as conn:
        row = conn.execute(
            f"SELECT factor FROM dias_especiales WHERE {' AND '.join(filtros)}",
            tuple(params),
        ).fetchone()
    return float(row["factor"]) if row else 1.0


def guardar_dia_especial(
    fecha: str,
    descripcion: str,
    factor: float = 1.0,
    tipo: str = "festivo",
) -> bool:
    panaderia_id, _ = _tenant_scope()
    try:
        with get_connection() as conn:
            conn.execute("""
                INSERT INTO dias_especiales (fecha, descripcion, factor, tipo, panaderia_id)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(fecha, panaderia_id) DO UPDATE SET
                    descripcion = excluded.descripcion,
                    factor = excluded.factor,
                    tipo = excluded.tipo,
                    activo = 1
            """, (fecha, descripcion, round(float(factor), 2), tipo, panaderia_id))
            conn.commit()
        return True
    except Exception as e:
        logger.error(f"guardar_dia_especial: {e}")
        return False


# ──────────────────────────────────────────────
# Dashboard de Cierre Diario
# ──────────────────────────────────────────────

def obtener_resumen_cierre_diario(fecha: str | None = None) -> dict:
    """
    Genera el resumen completo del cierre del día:
    ventas, ticket promedio, top producto, caja, merma, pronóstico mañana.
    """
    fecha = fecha or (datetime.now() - timedelta(hours=4)).strftime("%Y-%m-%d")
    v_filtros = ["fecha = ?"]
    v_params: list = [fecha]
    _apply_tenant_scope(v_filtros, v_params)
    panaderia_id, sede_id = _tenant_scope()
    p_filtros = ["fecha = ?", "estado = 'pagado'"]
    p_params: list = [fecha]
    if panaderia_id is not None:
        p_filtros.append("panaderia_id = ?")
        p_params.append(panaderia_id)
    if sede_id is not None:
        p_filtros.append("sede_id = ?")
        p_params.append(sede_id)

    with get_connection() as conn:
        # ── Ventas del día ───────────────────────────────────────────────────
        ventas_row = conn.execute(f"""
            SELECT
                COUNT(DISTINCT COALESCE(NULLIF(venta_grupo, ''), CAST(id AS TEXT))) as transacciones,
                COALESCE(SUM(total), 0.0) as total_ventas,
                COALESCE(SUM(cantidad), 0) as unidades_vendidas
            FROM ventas WHERE {' AND '.join(v_filtros)}
        """, tuple(v_params)).fetchone()

        # Ventas de pedidos de mesa (pagados)
        pedidos_row = conn.execute(f"""
            SELECT
                COUNT(*) as pedidos_pagados,
                COALESCE(SUM(total), 0.0) as total_pedidos
            FROM pedidos WHERE {' AND '.join(p_filtros)}
        """, tuple(p_params)).fetchone()

        # ── Caja ─────────────────────────────────────────────────────────────
        caja_filtros = ["fecha = ?"]
        caja_params: list = [fecha]
        _apply_tenant_scope(caja_filtros, caja_params)
        caja_row = conn.execute(f"""
            SELECT monto_apertura, monto_cierre, efectivo_esperado,
                   diferencia_cierre, estado, cerrado_por, cerrado_en,
                   abierto_por, abierto_en
            FROM arqueos_caja WHERE {' AND '.join(caja_filtros)}
            ORDER BY CASE estado WHEN 'cerrado' THEN 0 ELSE 1 END,
                     abierto_en DESC
            LIMIT 1
        """, tuple(caja_params)).fetchone()

        # ── Top producto del día ──────────────────────────────────────────────
        top_row = conn.execute(f"""
            SELECT producto, COALESCE(SUM(cantidad), 0) as unidades
            FROM ventas WHERE {' AND '.join(v_filtros)}
            GROUP BY producto ORDER BY unidades DESC LIMIT 1
        """, tuple(v_params)).fetchone()

        # ── Producto sin rotación ─────────────────────────────────────────────
        rd_filtros = ["rd.fecha = ?", "COALESCE(rd.vendido, 0) = 0", "COALESCE(rd.producido, 0) > 0"]
        rd_params: list = [fecha]
        _apply_tenant_scope(rd_filtros, rd_params)
        sin_rotacion = conn.execute(f"""
            SELECT rd.producto
            FROM registros_diarios rd
            WHERE {' AND '.join(rd_filtros)}
        """, tuple(rd_params)).fetchall()

        # ── Merma del día ─────────────────────────────────────────────────────
        merma_row = conn.execute(f"""
            SELECT COALESCE(SUM(cantidad), 0) as total_merma
            FROM mermas WHERE {' AND '.join(v_filtros)}
        """, tuple(v_params)).fetchone()

        # ── Producción del día ────────────────────────────────────────────────
        prod_row = conn.execute(f"""
            SELECT COALESCE(SUM(producido), 0) as total_producido,
                   COALESCE(SUM(vendido), 0) as total_vendido,
                   COALESCE(SUM(CASE WHEN producido > vendido THEN producido - vendido ELSE 0 END), 0) as sobrante
            FROM registros_diarios WHERE {' AND '.join(rd_filtros)}
        """, tuple(rd_params)).fetchone()

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


def exportar_productos_sistema() -> list[dict]:
    """Retorna el catalogo de productos del sistema listo para exportacion."""
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT
                id,
                nombre,
                categoria,
                menu,
                descripcion,
                precio,
                activo,
                es_panaderia,
                es_adicional,
                stock_minimo
            FROM productos
            ORDER BY activo DESC, categoria ASC, nombre ASC, id ASC
        """).fetchall()
    return [dict(r) for r in rows]


# ──────────────────────────────────────────────
# Encargos (pre-pedidos con fecha de entrega)
# ──────────────────────────────────────────────

def crear_encargo(fecha_entrega: str, cliente: str, items: list[dict],
                  empresa: str = "", notas: str = "",
                  registrado_por: str = "") -> dict:
    if not fecha_entrega or not cliente.strip():
        return {"ok": False, "error": "Fecha de entrega y cliente son obligatorios"}
    if not items:
        return {"ok": False, "error": "Debe incluir al menos un producto"}

    creado_en = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total = round(sum(
        float(it.get("precio_unitario", 0) or 0) * int(it.get("cantidad", 0) or 0)
        for it in items
    ), 2)

    panaderia_id, sede_id = _tenant_scope()
    try:
        with get_connection() as conn:
            cur = conn.execute("""
                INSERT INTO encargos (fecha_entrega, cliente, empresa, notas, registrado_por, creado_en, total, panaderia_id, sede_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (fecha_entrega, cliente.strip(), empresa.strip(), notas.strip(),
                  registrado_por.strip(), creado_en, total, panaderia_id, sede_id))
            encargo_id = cur.lastrowid

            for it in items:
                cantidad = int(it.get("cantidad", 0) or 0)
                precio = float(it.get("precio_unitario", 0) or 0)
                if cantidad <= 0:
                    continue
                conn.execute("""
                    INSERT INTO encargo_items
                        (encargo_id, producto_id, producto, cantidad, precio_unitario, subtotal, notas)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    encargo_id,
                    int(it.get("producto_id", 0) or 0) or None,
                    str(it.get("producto", "") or "").strip(),
                    cantidad,
                    precio,
                    round(precio * cantidad, 2),
                    str(it.get("notas", "") or "").strip(),
                ))
            conn.commit()
        return {"ok": True, "encargo_id": encargo_id, "total": total}
    except Exception as e:
        logger.error(f"crear_encargo: {e}")
        return {"ok": False, "error": str(e)}


def obtener_encargos(estado: str | None = None, fecha_entrega: str | None = None,
                     dias: int = 30) -> list[dict]:
    filtros: list = []
    params: list = []
    if estado:
        filtros.append("e.estado = ?")
        params.append(estado)
    if fecha_entrega:
        filtros.append("e.fecha_entrega = ?")
        params.append(fecha_entrega)
    else:
        desde = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")
        filtros.append("e.fecha_entrega >= ?")
        params.append(desde)
    panaderia_id, sede_id = _tenant_scope()
    if panaderia_id is not None:
        filtros.append("e.panaderia_id = ?")
        params.append(panaderia_id)
    if sede_id is not None:
        filtros.append("e.sede_id = ?")
        params.append(sede_id)

    where = f"WHERE {' AND '.join(filtros)}" if filtros else ""
    with get_connection() as conn:
        encargos = conn.execute(f"""
            SELECT e.id, e.fecha_entrega, e.cliente, e.empresa, e.notas,
                   e.estado, e.registrado_por, e.creado_en, e.total
            FROM encargos e
            {where}
            ORDER BY e.fecha_entrega ASC, e.creado_en DESC
        """, tuple(params)).fetchall()

        if not encargos:
            return []

        ids = [r["id"] for r in encargos]
        placeholders = ",".join("?" * len(ids))
        items = conn.execute(f"""
            SELECT id, encargo_id, producto_id, producto, cantidad,
                   precio_unitario, subtotal, notas
            FROM encargo_items
            WHERE encargo_id IN ({placeholders})
            ORDER BY id ASC
        """, ids).fetchall()

    items_by_encargo: dict[int, list] = {}
    for it in items:
        items_by_encargo.setdefault(it["encargo_id"], []).append(dict(it))

    result = []
    for r in encargos:
        d = dict(r)
        d["items"] = items_by_encargo.get(r["id"], [])
        result.append(d)
    return result


def obtener_encargo(encargo_id: int) -> dict | None:
    with get_connection() as conn:
        row = conn.execute("""
            SELECT id, fecha_entrega, cliente, empresa, notas, estado,
                   registrado_por, creado_en, total
            FROM encargos WHERE id = ?
        """, (encargo_id,)).fetchone()
        if not row:
            return None
        items = conn.execute("""
            SELECT id, encargo_id, producto_id, producto, cantidad,
                   precio_unitario, subtotal, notas
            FROM encargo_items WHERE encargo_id = ?
            ORDER BY id ASC
        """, (encargo_id,)).fetchall()
    d = dict(row)
    d["items"] = [dict(it) for it in items]
    return d


def actualizar_estado_encargo(encargo_id: int, estado: str,
                               usuario: str = "") -> dict:
    estados_validos = ("pendiente", "listo", "entregado", "cancelado")
    if estado not in estados_validos:
        return {"ok": False, "error": f"Estado invalido: {estado}"}
    try:
        with get_connection() as conn:
            affected = conn.execute("""
                UPDATE encargos SET estado = ? WHERE id = ?
            """, (estado, encargo_id)).rowcount
            conn.commit()
        if affected == 0:
            return {"ok": False, "error": "Encargo no encontrado"}
        return {"ok": True}
    except Exception as e:
        logger.error(f"actualizar_estado_encargo: {e}")
        return {"ok": False, "error": str(e)}


def eliminar_encargo(encargo_id: int) -> dict:
    try:
        with get_connection() as conn:
            affected = conn.execute(
                "DELETE FROM encargos WHERE id = ? AND estado IN ('pendiente','cancelado')",
                (encargo_id,)
            ).rowcount
            conn.commit()
        if affected == 0:
            return {"ok": False, "error": "Solo se pueden eliminar encargos pendientes o cancelados"}
        return {"ok": True}
    except Exception as e:
        logger.error(f"eliminar_encargo: {e}")
        return {"ok": False, "error": str(e)}


# ──────────────────────────────────────────────
# Unificacion de pedidos
# ──────────────────────────────────────────────

def unificar_pedidos(pedido_ids: list[int], unificado_por: str = "") -> dict:
    """Fusiona varios pedidos activos en el primero de la lista.

    - Mueve todos los items de los pedidos secundarios al pedido principal.
    - Recalcula el total del pedido principal.
    - Marca los secundarios con unificado_en = id_principal.
    - Los pedidos deben estar en estado pendiente, en_preparacion o listo.
    """
    if len(pedido_ids) < 2:
        return {"ok": False, "error": "Se necesitan al menos dos pedidos para unificar"}

    pedido_ids = [int(p) for p in pedido_ids]
    principal_id = pedido_ids[0]
    secundarios = pedido_ids[1:]
    estados_activos = ("pendiente", "en_preparacion", "listo")

    try:
        with get_connection() as conn:
            # Validar que todos existen y están activos
            placeholders = ",".join("?" * len(pedido_ids))
            rows = conn.execute(f"""
                SELECT id, estado, mesa_id, total, unificado_en
                FROM pedidos WHERE id IN ({placeholders})
            """, pedido_ids).fetchall()

            if len(rows) != len(pedido_ids):
                return {"ok": False, "error": "Uno o mas pedidos no existen"}

            pedidos_map = {r["id"]: dict(r) for r in rows}
            for pid, p in pedidos_map.items():
                if p["estado"] not in estados_activos:
                    return {"ok": False, "error": f"El pedido #{pid} ya fue pagado o cancelado y no se puede unificar"}
                if p["unificado_en"] is not None:
                    return {"ok": False, "error": f"El pedido #{pid} ya fue unificado anteriormente"}

            # Mover items de secundarios al principal
            ahora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for sec_id in secundarios:
                conn.execute("""
                    UPDATE pedido_items SET pedido_id = ? WHERE pedido_id = ?
                """, (principal_id, sec_id))
                conn.execute("""
                    UPDATE pedidos
                    SET unificado_en = ?, estado = 'cancelado'
                    WHERE id = ?
                """, (principal_id, sec_id))
                conn.execute("""
                    INSERT INTO pedido_estado_historial
                        (pedido_id, estado, cambiado_en, cambiado_por, detalle)
                    VALUES (?, 'cancelado', ?, ?, ?)
                """, (sec_id, ahora, unificado_por,
                      f"Pedido unificado en #{principal_id} por {unificado_por}"))

            # Recalcular total del pedido principal
            nuevo_total_row = conn.execute("""
                SELECT COALESCE(SUM(subtotal), 0) AS total FROM pedido_items WHERE pedido_id = ?
            """, (principal_id,)).fetchone()
            nuevo_total = float((nuevo_total_row["total"] if nuevo_total_row else 0) or 0)
            conn.execute("""
                UPDATE pedidos SET total = ? WHERE id = ?
            """, (round(nuevo_total, 2), principal_id))
            conn.execute("""
                INSERT INTO pedido_estado_historial
                    (pedido_id, estado, cambiado_en, cambiado_por, detalle)
                VALUES (?, ?, ?, ?, ?)
            """, (principal_id, pedidos_map[principal_id]["estado"], ahora, unificado_por,
                  f"Pedidos unificados: {', '.join(f'#{s}' for s in secundarios)}"))
            conn.commit()

        return {"ok": True, "pedido_principal_id": principal_id, "unificados": secundarios}
    except Exception as e:
        logger.error(f"unificar_pedidos: {e}")
        return {"ok": False, "error": str(e)}
