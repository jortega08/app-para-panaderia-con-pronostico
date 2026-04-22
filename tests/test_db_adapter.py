from __future__ import annotations

import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from data.db_adapter import _PGCursor


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


if __name__ == "__main__":
    unittest.main()
