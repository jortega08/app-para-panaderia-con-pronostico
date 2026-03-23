"""
db_adapter.py
-------------
Adaptador de base de datos que soporta SQLite (desarrollo) y PostgreSQL (producción).

Uso:
    from data.db_adapter import get_connection, DB_TYPE

    with get_connection() as conn:
        rows = conn.execute("SELECT * FROM productos WHERE activo = ?", (1,)).fetchall()
        # rows son dict-like en ambos modos

Variables de entorno:
    DATABASE_URL: URL de conexión
        SQLite:     sqlite:///data/panaderia.db   (o ruta absoluta)
        PostgreSQL: postgresql://user:pass@host:5432/dbname
"""

from __future__ import annotations

import os
import re
import sqlite3
from pathlib import Path

# ── Detectar tipo de BD desde DATABASE_URL ────────────────────────────────────

_DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
_DEFAULT_SQLITE = Path(__file__).parent / "panaderia.db"

if _DATABASE_URL.startswith(("postgresql://", "postgres://")):
    DB_TYPE = "postgresql"
elif _DATABASE_URL.startswith("sqlite:///"):
    DB_TYPE = "sqlite"
    _SQLITE_PATH = Path(_DATABASE_URL.removeprefix("sqlite:///"))
    if not _SQLITE_PATH.is_absolute():
        _SQLITE_PATH = Path(__file__).parent.parent / _SQLITE_PATH
else:
    DB_TYPE = "sqlite"
    _SQLITE_PATH = _DEFAULT_SQLITE


# ── Adaptador SQLite (default) ────────────────────────────────────────────────

def _get_sqlite_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_SQLITE_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


# ── Adaptador PostgreSQL ───────────────────────────────────────────────────────

def _translate_sql_for_pg(sql: str) -> str:
    """Traduce SQL de estilo SQLite a PostgreSQL."""
    # 1. ? → %s (parámetros posicionales)
    result = sql.replace("?", "%s")

    # 2. date('now', '-N days') → CURRENT_DATE - INTERVAL 'N days'
    def _replace_date_now(m: re.Match) -> str:
        modifier = m.group(1).strip().strip("'\"")
        # e.g. '-7 days' → -7 days
        return f"(CURRENT_DATE + INTERVAL '{modifier}')"
    result = re.sub(r"date\s*\(\s*'now'\s*,\s*([^)]+)\)", _replace_date_now, result, flags=re.IGNORECASE)

    # 3. date('now') → CURRENT_DATE
    result = re.sub(r"date\s*\(\s*'now'\s*\)", "CURRENT_DATE", result, flags=re.IGNORECASE)

    # 4. datetime('now') → NOW()
    result = re.sub(r"datetime\s*\(\s*'now'\s*\)", "NOW()", result, flags=re.IGNORECASE)

    # 5. INTEGER PRIMARY KEY AUTOINCREMENT → SERIAL PRIMARY KEY
    result = re.sub(
        r"INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT",
        "SERIAL PRIMARY KEY",
        result,
        flags=re.IGNORECASE,
    )

    # 6. PRAGMA statements → vacío (se ignorarán)
    if re.match(r"^\s*PRAGMA\b", result, re.IGNORECASE):
        return ""

    # 7. INSERT OR IGNORE → INSERT ... ON CONFLICT DO NOTHING
    result = re.sub(r"\bINSERT\s+OR\s+IGNORE\b", "INSERT", result, flags=re.IGNORECASE)
    # Agregar ON CONFLICT al final si fue un INSERT OR IGNORE
    if "INSERT OR IGNORE" in sql.upper():
        result = result.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"

    # 8. INSERT OR REPLACE → INSERT ... ON CONFLICT DO UPDATE
    # (más complejo, se maneja por tabla específica si es necesario)

    return result


class _PGRow(dict):
    """Fila de resultado PostgreSQL con acceso dict-like (compatible con sqlite3.Row)."""

    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self.values())[key]
        return super().__getitem__(key)


class _PGCursor:
    """Cursor PostgreSQL que imita el comportamiento de sqlite3 Connection.execute()."""

    def __init__(self, pg_cursor):
        self._cur = pg_cursor
        self.lastrowid = None
        self.rowcount = 0

    def execute(self, sql: str, params=()) -> "_PGCursor":
        pg_sql = _translate_sql_for_pg(sql)
        if not pg_sql.strip():
            return self  # PRAGMA vacío, ignorar

        is_insert = pg_sql.strip().upper().startswith("INSERT")
        if is_insert and "RETURNING" not in pg_sql.upper() and "ON CONFLICT DO NOTHING" not in pg_sql.upper():
            # Agregar RETURNING id para obtener lastrowid
            pg_sql_returning = pg_sql.rstrip().rstrip(";") + " RETURNING id"
            try:
                self._cur.execute(pg_sql_returning, params or ())
                row = self._cur.fetchone()
                self.lastrowid = row[0] if row else None
            except Exception:
                # Si RETURNING falla (ej. tabla sin 'id'), ejecutar sin RETURNING
                self._cur.execute(pg_sql, params or ())
                try:
                    self._cur.execute("SELECT LASTVAL()")
                    r = self._cur.fetchone()
                    self.lastrowid = r[0] if r else None
                except Exception:
                    self.lastrowid = None
        else:
            self._cur.execute(pg_sql, params or ())

        self.rowcount = self._cur.rowcount
        return self

    def fetchone(self):
        row = self._cur.fetchone()
        if row is None:
            return None
        if self._cur.description:
            cols = [d[0] for d in self._cur.description]
            return _PGRow(zip(cols, row))
        return row

    def fetchall(self):
        rows = self._cur.fetchall()
        if not rows:
            return []
        if self._cur.description:
            cols = [d[0] for d in self._cur.description]
            return [_PGRow(zip(cols, row)) for row in rows]
        return rows

    def __iter__(self):
        return iter(self.fetchall())


class _PGConnection:
    """Conexión PostgreSQL que imita la API de sqlite3.Connection."""

    def __init__(self, pg_conn):
        self._conn = pg_conn

    def execute(self, sql: str, params=()) -> _PGCursor:
        cursor = self._conn.cursor()
        pg_cursor = _PGCursor(cursor)
        pg_cursor.execute(sql, params)
        return pg_cursor

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        else:
            self.commit()
        # No cerrar aquí para compatibilidad con pool/reuse; dejar que GC lo haga
        return False


def _get_pg_connection() -> _PGConnection:
    import psycopg2  # type: ignore

    pg_url = _DATABASE_URL
    # Heroku/Railway usan 'postgres://', psycopg2 necesita 'postgresql://'
    if pg_url.startswith("postgres://"):
        pg_url = "postgresql://" + pg_url[len("postgres://"):]

    conn = psycopg2.connect(pg_url)
    conn.autocommit = False
    return _PGConnection(conn)


# ── Función pública ────────────────────────────────────────────────────────────

def get_connection():
    """
    Retorna una conexión activa a la base de datos.
    Transparente para SQLite y PostgreSQL.

    Uso:
        with get_connection() as conn:
            rows = conn.execute("SELECT ...", params).fetchall()
    """
    if DB_TYPE == "postgresql":
        return _get_pg_connection()
    return _get_sqlite_connection()
