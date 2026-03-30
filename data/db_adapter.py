"""
db_adapter.py
-------------
Database adapter that supports SQLite for local development and PostgreSQL for
production deployments.
"""

from __future__ import annotations

import os
import re
import sqlite3
from pathlib import Path


_DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
_DEFAULT_SQLITE = Path(__file__).parent / "panaderia.db"
_FLASK_ENV = str(os.environ.get("FLASK_ENV", "") or "").strip().lower()
_IS_RAILWAY = any(
    str(os.environ.get(key, "") or "").strip()
    for key in ("RAILWAY_ENVIRONMENT", "RAILWAY_PROJECT_ID", "RAILWAY_SERVICE_ID")
)
_REQUIRE_POSTGRES = _IS_RAILWAY or str(os.environ.get("REQUIRE_POSTGRES", "") or "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}


def _resolve_sqlite_path(database_url: str) -> Path:
    sqlite_path = Path(database_url.removeprefix("sqlite:///"))
    if not sqlite_path.is_absolute():
        sqlite_path = Path(__file__).parent.parent / sqlite_path
    return sqlite_path


if _DATABASE_URL.startswith(("postgresql://", "postgres://")):
    DB_TYPE = "postgresql"
    _SQLITE_PATH = None
elif _DATABASE_URL.startswith("sqlite:///"):
    DB_TYPE = "sqlite"
    _SQLITE_PATH = _resolve_sqlite_path(_DATABASE_URL)
else:
    if _REQUIRE_POSTGRES:
        raise RuntimeError(
            "DATABASE_URL no configurada o invalida. En Railway/produccion debe ser PostgreSQL."
        )
    DB_TYPE = "sqlite"
    _SQLITE_PATH = _DEFAULT_SQLITE

if DB_TYPE == "sqlite" and _REQUIRE_POSTGRES:
    raise RuntimeError(
        "En Railway/produccion DATABASE_URL debe apuntar a PostgreSQL, no a SQLite."
    )


def _mask_database_url(url: str) -> str:
    if not url:
        return ""
    return re.sub(r":([^:@/]+)@", ":***@", url, count=1)


def get_database_info() -> dict:
    info = {
        "type": DB_TYPE,
        "database_url": _mask_database_url(_DATABASE_URL),
        "flask_env": _FLASK_ENV,
        "is_railway": _IS_RAILWAY,
        "require_postgres": _REQUIRE_POSTGRES,
        "supports_app_file_backup": DB_TYPE == "sqlite",
    }
    if DB_TYPE == "sqlite" and _SQLITE_PATH is not None:
        info["sqlite_path"] = str(_SQLITE_PATH)
    return info


def _get_sqlite_connection() -> sqlite3.Connection:
    if _SQLITE_PATH is None:
        raise RuntimeError("SQLite no esta configurado para este entorno.")
    conn = sqlite3.connect(str(_SQLITE_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def _translate_sql_for_pg(sql: str) -> str:
    """Translate SQLite-style SQL to PostgreSQL."""
    result = sql.replace("?", "%s")

    def _replace_date_now(match: re.Match) -> str:
        modifier = match.group(1).strip().strip("'\"")
        return f"(CURRENT_DATE + INTERVAL '{modifier}')"

    result = re.sub(
        r"date\s*\(\s*'now'\s*,\s*([^)]+)\)",
        _replace_date_now,
        result,
        flags=re.IGNORECASE,
    )
    result = re.sub(r"date\s*\(\s*'now'\s*\)", "CURRENT_DATE", result, flags=re.IGNORECASE)
    result = re.sub(r"datetime\s*\(\s*'now'\s*\)", "NOW()", result, flags=re.IGNORECASE)
    result = re.sub(
        r"INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT",
        "SERIAL PRIMARY KEY",
        result,
        flags=re.IGNORECASE,
    )
    result = re.sub(
        r"(GENERATED\s+ALWAYS\s+AS\s*\([^)]+\))\s+VIRTUAL",
        r"\1 STORED",
        result,
        flags=re.IGNORECASE,
    )

    if re.match(r"^\s*PRAGMA\b", result, re.IGNORECASE):
        return ""

    result = re.sub(r"\bINSERT\s+OR\s+IGNORE\b", "INSERT", result, flags=re.IGNORECASE)
    if "INSERT OR IGNORE" in sql.upper():
        result = result.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
    return result


class _PGRow(dict):
    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self.values())[key]
        return super().__getitem__(key)


class _PGCursor:
    def __init__(self, pg_cursor):
        self._cur = pg_cursor
        self.lastrowid = None
        self.rowcount = 0

    def execute(self, sql: str, params=()) -> "_PGCursor":
        pg_sql = _translate_sql_for_pg(sql)
        if not pg_sql.strip():
            return self

        self._cur.execute(pg_sql, params or ())
        self.rowcount = self._cur.rowcount
        self.lastrowid = None
        return self

    def fetchone(self):
        row = self._cur.fetchone()
        if row is None:
            return None
        if self._cur.description:
            cols = [desc[0] for desc in self._cur.description]
            return _PGRow(zip(cols, row))
        return row

    def fetchall(self):
        rows = self._cur.fetchall()
        if not rows:
            return []
        if self._cur.description:
            cols = [desc[0] for desc in self._cur.description]
            return [_PGRow(zip(cols, row)) for row in rows]
        return rows

    def __iter__(self):
        return iter(self.fetchall())


class _PGConnection:
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
        return False


def _get_pg_connection() -> _PGConnection:
    import psycopg2  # type: ignore

    pg_url = _DATABASE_URL
    if pg_url.startswith("postgres://"):
        pg_url = "postgresql://" + pg_url[len("postgres://"):]

    conn = psycopg2.connect(pg_url)
    conn.autocommit = False
    return _PGConnection(conn)


def get_connection():
    if DB_TYPE == "postgresql":
        return _get_pg_connection()
    return _get_sqlite_connection()