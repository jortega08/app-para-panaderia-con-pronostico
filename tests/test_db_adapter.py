from __future__ import annotations

import sys
import sqlite3
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from data.db_adapter import _PGCursor
from data import database as db_module


class _FakeCursor:
    def __init__(self, *, fetchone_result=None):
        self.calls = []
        self.description = None
        self.rowcount = 0
        self._fetchone_result = fetchone_result

    def execute(self, *args):
        self.calls.append(args)

    def fetchone(self):
        return self._fetchone_result


class _FakeExecuteResult:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _CapturingConnection:
    def __init__(self, *, row=None):
        self.calls = []
        self._row = row if row is not None else {"ultimo": 0}

    def execute(self, sql, params=()):
        self.calls.append((sql, params))
        return _FakeExecuteResult(self._row)


class _FakeFetchAllResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _TopProductosConnection:
    def __init__(self):
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, sql, params=()):
        self.calls.append((" ".join(sql.split()), params))
        if "FROM ventas" in sql:
            return _FakeFetchAllResult([
                {"producto": "Pan Frances", "unidades": 2, "ingresos": 5000.0},
            ])
        if "FROM pedido_items" in sql:
            return _FakeFetchAllResult([
                {"producto": "Pan Frances", "unidades": 3, "ingresos": 7500.0},
                {"producto": "Croissant", "unidades": 1, "ingresos": 4500.0},
            ])
        return _FakeFetchAllResult([])


class PGCursorAdapterTestCase(unittest.TestCase):
    def test_execute_without_params_keeps_percent_literals_safe(self) -> None:
        cursor = _FakeCursor()

        wrapped = _PGCursor(cursor)
        wrapped.execute(
            """
            SELECT con.conname AS constraint_name
            FROM pg_constraint con
            WHERE pg_get_constraintdef(con.oid) ILIKE '%estado%'
            """
        )

        self.assertEqual(len(cursor.calls), 1)
        self.assertEqual(len(cursor.calls[0]), 1)
        self.assertIn("%estado%", cursor.calls[0][0])

    def test_execute_with_params_still_passes_translated_placeholders(self) -> None:
        cursor = _FakeCursor()

        wrapped = _PGCursor(cursor)
        wrapped.execute("SELECT * FROM usuarios WHERE id = ?", (123,))

        self.assertEqual(cursor.calls, [("SELECT * FROM usuarios WHERE id = %s", (123,))])

    def test_insert_returning_handles_empty_tuple_row(self) -> None:
        cursor = _FakeCursor(fetchone_result=())

        wrapped = _PGCursor(cursor)
        wrapped.execute("INSERT INTO productos (nombre) VALUES ('Pan Frances')")

        self.assertEqual(len(cursor.calls), 1)
        self.assertIn("RETURNING id", cursor.calls[0][0])
        self.assertIsNone(wrapped.lastrowid)

    def test_insert_returning_includes_pos_and_comanda_tables(self) -> None:
        for table_name in ("venta_headers", "venta_items", "comandas"):
            with self.subTest(table=table_name):
                cursor = _FakeCursor(fetchone_result=(321,))
                wrapped = _PGCursor(cursor)

                wrapped.execute(f"INSERT INTO {table_name} (created_at) VALUES ('2026-04-22 16:40:27')")

                self.assertEqual(len(cursor.calls), 1)
                self.assertIn("RETURNING id", cursor.calls[0][0])
                self.assertEqual(wrapped.lastrowid, 321)


class TopProductosDiaTestCase(unittest.TestCase):
    def test_uses_tenant_scope_for_pedido_filters(self) -> None:
        conn = _TopProductosConnection()
        db_module.set_query_context(169, 169)
        try:
            with patch.object(db_module, "get_connection", return_value=conn):
                top = db_module.obtener_top_productos_dia("2026-04-28", limite=5)
        finally:
            db_module.set_query_context(None, None)

        self.assertEqual(top[0], {"producto": "Pan Frances", "unidades": 5, "ingresos": 12500.0})
        self.assertEqual(top[1], {"producto": "Croissant", "unidades": 1, "ingresos": 4500.0})
        ventas_sql, ventas_params = conn.calls[0]
        pedidos_sql, pedidos_params = conn.calls[1]
        self.assertIn("panaderia_id = ?", ventas_sql)
        self.assertIn("sede_id = ?", ventas_sql)
        self.assertEqual(ventas_params, ("2026-04-28", 169, 169))
        self.assertIn("p.panaderia_id = ?", pedidos_sql)
        self.assertIn("p.sede_id = ?", pedidos_sql)
        self.assertEqual(pedidos_params, ("2026-04-28", 169, 169))


class DocumentoConsecutivoQueryTestCase(unittest.TestCase):
    def test_uses_equal_operator_for_non_null_sede_id(self) -> None:
        conn = _CapturingConnection(row={"ultimo": 4})

        with patch.object(db_module, "obtener_sede_por_id", return_value={"codigo": "NTE"}) as obtener_sede:
            consecutivo_numero, consecutivo = db_module._generar_consecutivo_documento_conn(
                conn,
                sede_id=169,
                tipo_documento="factura",
            )

        obtener_sede.assert_called_once_with(169)
        self.assertEqual(consecutivo_numero, 5)
        self.assertEqual(consecutivo, "FAC-NTE-000005")
        self.assertEqual(len(conn.calls), 1)
        sql, params = conn.calls[0]
        self.assertIn("WHERE sede_id = ? AND tipo_documento = ?", " ".join(sql.split()))
        self.assertEqual(params, (169, "factura"))

    def test_uses_is_null_only_when_sede_id_is_none(self) -> None:
        conn = _CapturingConnection(row={"ultimo": 0})

        with patch.object(db_module, "obtener_sede_por_id") as obtener_sede:
            consecutivo_numero, consecutivo = db_module._generar_consecutivo_documento_conn(
                conn,
                sede_id=None,
                tipo_documento="factura",
            )

        obtener_sede.assert_not_called()
        self.assertEqual(consecutivo_numero, 1)
        self.assertEqual(consecutivo, "FAC-GEN-000001")
        self.assertEqual(len(conn.calls), 1)
        sql, params = conn.calls[0]
        self.assertIn("WHERE sede_id IS NULL AND tipo_documento = ?", " ".join(sql.split()))
        self.assertEqual(params, ("factura",))


class SQLiteTemporaryForeignKeyRepairTestCase(unittest.TestCase):
    def test_repairs_recetas_reference_to_renamed_insumos_table(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute(
            """
            CREATE TABLE insumos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT NOT NULL
            )
            """
        )
        conn.execute("INSERT INTO insumos (id, nombre) VALUES (1, 'Harina')")
        conn.execute(
            """
            CREATE TABLE recetas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                producto TEXT NOT NULL,
                insumo_id INTEGER NOT NULL,
                cantidad REAL NOT NULL DEFAULT 1.0,
                unidad_receta TEXT NOT NULL DEFAULT 'unidad',
                panaderia_id INTEGER,
                UNIQUE(producto, insumo_id, panaderia_id),
                FOREIGN KEY (insumo_id) REFERENCES _ins_old(id)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO recetas (producto, insumo_id, cantidad, unidad_receta, panaderia_id)
            VALUES ('Pan Frances', 1, 150.0, 'g', 1)
            """
        )
        conn.commit()

        conn.execute("PRAGMA foreign_keys = ON")
        with self.assertRaises(sqlite3.OperationalError):
            conn.execute(
                """
                INSERT INTO recetas (producto, insumo_id, cantidad, unidad_receta, panaderia_id)
                VALUES ('Pan Dulce', 1, 120.0, 'g', 1)
                """
            )
        conn.rollback()

        with patch.object(db_module, "DB_TYPE", "sqlite"):
            db_module._reparar_foreign_keys_tablas_temporales(conn)

        schema = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='recetas'"
        ).fetchone()["sql"]
        self.assertNotIn("_ins_old", schema)
        fk_rows = conn.execute("PRAGMA foreign_key_list(recetas)").fetchall()
        self.assertEqual(fk_rows[0]["table"], "insumos")

        conn.execute(
            """
            INSERT INTO recetas (producto, insumo_id, cantidad, unidad_receta, panaderia_id)
            VALUES ('Pan Dulce', 1, 120.0, 'g', 1)
            """
        )


class Phase15MigrationTestCase(unittest.TestCase):
    def test_adds_columns_and_backfills_custom_price_fields(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE encargos (id INTEGER PRIMARY KEY AUTOINCREMENT)")
        conn.execute(
            """
            CREATE TABLE encargo_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                encargo_id INTEGER,
                precio_unitario REAL NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE pedido_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pedido_id INTEGER,
                cantidad INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE mesas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                numero INTEGER,
                nombre TEXT
            )
            """
        )
        conn.execute("INSERT INTO encargo_items (encargo_id, precio_unitario) VALUES (1, 7500)")
        conn.commit()

        db_module._migrar_fase15(conn)

        encargo_cols = {row["name"] for row in conn.execute("PRAGMA table_info(encargos)").fetchall()}
        self.assertIn("tipo_doc", encargo_cols)
        self.assertIn("fecha_produccion", encargo_cols)
        self.assertIn("recordatorio_entrega_en", encargo_cols)

        item = conn.execute("SELECT precio_base, precio_aplicado FROM encargo_items").fetchone()
        self.assertEqual(float(item["precio_base"]), 7500.0)
        self.assertEqual(float(item["precio_aplicado"]), 7500.0)

        pedido_cols = {row["name"] for row in conn.execute("PRAGMA table_info(pedido_items)").fetchall()}
        self.assertIn("cantidad_entregada", pedido_cols)
        self.assertIn("entregado_por", pedido_cols)

        mesa_cols = {row["name"] for row in conn.execute("PRAGMA table_info(mesas)").fetchall()}
        self.assertIn("pendiente_atencion", mesa_cols)
        self.assertIn("pendiente_atencion_motivo", mesa_cols)


if __name__ == "__main__":
    unittest.main()
