from __future__ import annotations

import sys
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


if __name__ == "__main__":
    unittest.main()
