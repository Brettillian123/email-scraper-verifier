# src/db.py
from __future__ import annotations

import logging
import os
import re
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

try:  # Required for Postgres. Kept as optional to avoid hard import errors in tooling.
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
except Exception:  # pragma: no cover
    psycopg2 = None  # type: ignore


# ---------------------------------------------------------------------------
# Configuration / env
# ---------------------------------------------------------------------------

_db_log = logging.getLogger(__name__)

# SQLite is no longer a production backend. The only supported system of record is Postgres.
# SQLite support is available only as an explicit dev escape hatch for legacy workflows.
DEFAULT_SQLITE_PATH = "data/dev.db"
DEFAULT_TENANT_ID = "dev"

ALLOW_SQLITE_DEV = (os.getenv("ALLOW_SQLITE_DEV") or "").strip().lower() in {"1", "true", "yes"}


def _database_url() -> str:
    """
    Prefer DATABASE_URL, but keep DB_URL as backward compatible fallback.

    Target state: Postgres is the only system of record. If neither is set, this
    function raises (to prevent accidental SQLite usage in production).

    For legacy/dev-only SQLite runs, you may set ALLOW_SQLITE_DEV=1 and supply a
    sqlite:///... URL or a local .db path via DATABASE_URL / DB_URL / DATABASE_PATH.
    """
    url = (os.getenv("DATABASE_URL") or "").strip() or (os.getenv("DB_URL") or "").strip()
    if not url:
        # Historical: some scripts used DATABASE_PATH (SQLite-only).
        # Preserve as dev escape hatch only.
        url = (os.getenv("DATABASE_PATH") or "").strip()

    if not url:
        raise RuntimeError(
            "DATABASE_URL (or DB_URL) is required and must be a Postgres connection string "
            "(postgresql://...)."
        )

    if _is_postgres_url(url):
        return url

    # Optional legacy escape hatch
    if ALLOW_SQLITE_DEV:
        return url

    raise RuntimeError(
        "This project is Postgres-only. DATABASE_URL/DB_URL must start with postgresql:// "
        "(or postgres://). "
        "If you intentionally need legacy SQLite for local dev, set ALLOW_SQLITE_DEV=1 explicitly."
    )


def _is_postgres_url(url: str) -> bool:
    u = (url or "").strip().lower()
    return u.startswith("postgres://") or u.startswith("postgresql://")


def _is_sqlite_url(url: str) -> bool:
    u = (url or "").strip().lower()
    return u.startswith("sqlite:///")


def _sqlite_path_from_url(url: str) -> str:
    u = (url or "").strip()
    if _is_sqlite_url(u):
        return u[len("sqlite:///") :]
    return u


def _db_path() -> str:
    """
    SQLite dev-only path helper.

    The project is Postgres-first. This helper exists only for legacy components
    that have not yet been migrated (e.g., any remaining SQLite-only search/FTS).

    It is disabled unless ALLOW_SQLITE_DEV=1.
    """
    if not ALLOW_SQLITE_DEV:
        raise RuntimeError(
            "_db_path() is disabled (Postgres-only). Set ALLOW_SQLITE_DEV=1 only for legacy "
            "local-dev workflows."
        )

    url = _database_url()
    if _is_postgres_url(url):
        raise RuntimeError("_db_path() is SQLite-only; DATABASE_URL/DB_URL points at Postgres.")
    return _sqlite_path_from_url(url) or DEFAULT_SQLITE_PATH


def _now_iso_z(dt: datetime | None = None) -> str:
    d = dt or datetime.now(UTC)
    if d.tzinfo is None:
        d = d.replace(tzinfo=UTC)
    else:
        d = d.astimezone(UTC)
    return d.strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# SQL compatibility helpers (SQLite -> Postgres)
# ---------------------------------------------------------------------------


def _qmark_to_percent(sql: str) -> str:
    """
    Convert SQLite qmark placeholders (?) to psycopg2 (%s).

    Best-effort: skips ? inside single-quoted string literals.
    """
    out: list[str] = []
    in_str = False
    i = 0
    while i < len(sql):
        ch = sql[i]
        if ch == "'":
            out.append(ch)
            # Handle escaped '' inside a string
            if in_str and i + 1 < len(sql) and sql[i + 1] == "'":
                out.append("'")
                i += 2
                continue
            in_str = not in_str
            i += 1
            continue
        if ch == "?" and not in_str:
            out.append("%s")
            i += 1
            continue
        out.append(ch)
        i += 1
    return "".join(out)


_RX_INSERT_OR_IGNORE = re.compile(r"^\s*insert\s+or\s+ignore\s+into\s+", re.IGNORECASE)
_RX_PRAGMA_TABLE_INFO = re.compile(
    r"^\s*pragma\s+table_info\s*\(\s*([^)]+?)\s*\)\s*;?\s*$", re.IGNORECASE
)
_RX_PRAGMA_FK_LIST = re.compile(
    r"^\s*pragma\s+foreign_key_list\s*\(\s*([^)]+?)\s*\)\s*;?\s*$", re.IGNORECASE
)
_RX_PRAGMA_FK_ENFORCE = re.compile(r"^\s*pragma\s+foreign_keys\s*(=|$)", re.IGNORECASE)
_RX_SQLITE_MASTER = re.compile(r"\bsqlite_master\b", re.IGNORECASE)
_RX_INSTR = re.compile(r"\binstr\s*\(", re.IGNORECASE)


def _normalize_ident(raw: str) -> str:
    """
    Normalize a table identifier possibly containing quotes or schema prefixes.
    """
    s = (raw or "").strip().strip(";").strip()
    if (s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"')):
        s = s[1:-1]
    if "." in s:
        s = s.split(".")[-1]
        s = s.strip('"').strip("'")
    return s


# ---------------------------------------------------------------------------
# Compat cursor/connection wrappers
# ---------------------------------------------------------------------------


class CompatCursor:
    """
    Cursor wrapper:
      - On Postgres: converts SQLite `?` placeholders to `%s`
      - Translates a few SQLite idioms used throughout the codebase
      - Emulates selected SQLite introspection queries:
          * PRAGMA table_info(...)
          * PRAGMA foreign_key_list(...)
          * sqlite_master existence checks
    """

    def __init__(self, parent: CompatConnection, cursor: Any, is_pg: bool):
        self._parent = parent
        self._cursor = cursor
        self._is_pg = is_pg

        self.lastrowid: int | None = None
        self.rowcount: int = -1

        # Emulation buffers (for PRAGMA/sqlite_master)
        self._emulated_rows: list[tuple[Any, ...]] | None = None
        self._emulated_idx: int = 0

        # Prefetched rows buffer (used when we auto-append RETURNING id)
        self._prefetch: list[Any] | None = None
        self._prefetch_idx: int = 0

    @property
    def description(self):
        if self._emulated_rows is not None:
            return None
        return getattr(self._cursor, "description", None)

    def _set_emulated(self, rows: list[tuple[Any, ...]]) -> None:
        self._emulated_rows = rows
        self._emulated_idx = 0

    def _fetchone_emulated(self) -> tuple[Any, ...] | None:
        if self._emulated_rows is None:
            return None
        if self._emulated_idx >= len(self._emulated_rows):
            return None
        row = self._emulated_rows[self._emulated_idx]
        self._emulated_idx += 1
        return row

    def _fetchall_emulated(self) -> list[tuple[Any, ...]]:
        if self._emulated_rows is None:
            return []
        if self._emulated_idx <= 0:
            return list(self._emulated_rows)
        return list(self._emulated_rows[self._emulated_idx :])

    def _maybe_pragma_emulation(self, sql: str, params: Sequence[Any]) -> bool:
        if not self._is_pg:
            return False

        s = sql.strip()

        # PRAGMA foreign_keys=ON -> no-op on Postgres
        if _RX_PRAGMA_FK_ENFORCE.match(s):
            self._set_emulated([])
            return True

        m = _RX_PRAGMA_TABLE_INFO.match(s)
        if m:
            table = _normalize_ident(m.group(1))
            rows = self._parent._pg_pragma_table_info(table)
            self._set_emulated(rows)
            return True

        m = _RX_PRAGMA_FK_LIST.match(s)
        if m:
            table = _normalize_ident(m.group(1))
            rows = self._parent._pg_pragma_foreign_key_list(table)
            self._set_emulated(rows)
            return True

        # sqlite_master existence checks (tasks/demo code uses this frequently)
        if _RX_SQLITE_MASTER.search(s):
            rows = self._parent._pg_emulate_sqlite_master_query(s, params)
            self._set_emulated(rows)
            return True

        return False

    def _translate_sql(self, sql: str) -> str:
        s = sql

        # datetime('now') -> NOW()
        s = s.replace("datetime('now')", "NOW()").replace('datetime("now")', "NOW()")

        # Common: datetime('now','utc') (best-effort)
        s = s.replace("datetime('now','utc')", "NOW()").replace("datetime('now', 'utc')", "NOW()")

        # instr(a,b) -> strpos(a,b)
        s = _RX_INSTR.sub("strpos(", s)

        # INSERT OR IGNORE -> INSERT ... ON CONFLICT DO NOTHING
        if _RX_INSERT_OR_IGNORE.match(s) and "on conflict" not in s.lower():
            s = _RX_INSERT_OR_IGNORE.sub("INSERT INTO ", s)
            s = s.rstrip().rstrip(";")
            s = f"{s} ON CONFLICT DO NOTHING"

        # Placeholders
        s = _qmark_to_percent(s)

        return s

    def _maybe_append_returning_id(self, sql: str) -> str:
        """
        For Postgres only: if query looks like an INSERT into a table with an `id`
        column and doesn't already RETURNING, append `RETURNING id` so callers can
        read lastrowid.
        """
        s = sql.strip()
        if not self._is_pg:
            return sql
        if "returning" in s.lower():
            return sql
        if not re.match(r"^\s*insert\b", s, re.IGNORECASE):
            return sql

        # Try to detect target table name: INSERT INTO table_name ...
        m = re.match(r"^\s*insert\s+into\s+([a-zA-Z0-9_\"\.]+)\s*", s, re.IGNORECASE)
        if not m:
            return sql
        raw = m.group(1)
        table = _normalize_ident(raw)
        if not table:
            return sql

        try:
            if not self._parent._pg_table_has_column(table, "id"):
                return sql
        except Exception:
            return sql

        # Don't append if it's a multi-row insert with SELECT ... (still OK), but keep simple.
        return s.rstrip().rstrip(";") + " RETURNING id"

    def execute(self, sql: str, params: Sequence[Any] | None = None):
        params = params or ()
        self.lastrowid = None
        self.rowcount = -1
        self._emulated_rows = None
        self._emulated_idx = 0
        self._prefetch = None
        self._prefetch_idx = 0

        if self._maybe_pragma_emulation(sql, params):
            self.rowcount = 0
            return self

        q = sql
        if self._is_pg:
            q = self._translate_sql(q)
            q = self._maybe_append_returning_id(q)

        self._cursor.execute(q, params)

        try:
            self.rowcount = int(getattr(self._cursor, "rowcount", -1))
        except Exception:
            self.rowcount = -1

        # For RETURNING id (auto-appended or caller-provided), capture it for lastrowid.
        #
        # Important: do not coerce rows to tuples here. When using DictCursor, callers may rely on
        # row['col'] access. We only derive lastrowid from the first column when possible.
        try:
            if self._is_pg and "returning" in q.lower():
                rows = self._cursor.fetchall() or []
                self._prefetch = list(rows)
                self._prefetch_idx = 0

                if rows:
                    try:
                        self.lastrowid = int(rows[0][0])  # type: ignore[index]
                    except Exception:
                        try:
                            self.lastrowid = int(
                                getattr(rows[0], "get", lambda *_: None)("id")  # type: ignore[misc]
                            )
                        except Exception:
                            self.lastrowid = None
        except Exception:
            # Best-effort only; leave lastrowid unset if anything goes wrong.
            self.lastrowid = None
        return self

    def fetchone(self):
        if self._emulated_rows is not None:
            return self._fetchone_emulated()

        if self._prefetch is not None:
            if self._prefetch_idx >= len(self._prefetch):
                return None
            row = self._prefetch[self._prefetch_idx]
            self._prefetch_idx += 1
            return row

        return self._cursor.fetchone()

    def fetchall(self):
        if self._emulated_rows is not None:
            return self._fetchall_emulated()

        if self._prefetch is not None:
            if self._prefetch_idx <= 0:
                return list(self._prefetch)
            return list(self._prefetch[self._prefetch_idx :])

        return self._cursor.fetchall()

    def __iter__(self):
        while True:
            row = self.fetchone()
            if row is None:
                break
            yield row

    def close(self):
        try:
            return self._cursor.close()
        except Exception:
            _db_log.debug("CompatCursor.close() failed", exc_info=True)
            return None


class CompatConnection:
    """
    Connection wrapper:
      - Provides .execute() convenience like sqlite3.Connection
      - Returns CompatCursor on .cursor()
      - Supports context manager commit/rollback
      - On Postgres: adds helpers to emulate a few SQLite introspection operations
    """

    def __init__(self, conn: Any, is_pg: bool):
        self._conn = conn
        self._is_pg = is_pg

    @property
    def is_postgres(self) -> bool:
        return bool(self._is_pg)

    def cursor(self) -> CompatCursor:
        cur = self._conn.cursor()
        return CompatCursor(self, cur, self._is_pg)

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> CompatCursor:
        cur = self.cursor()
        cur.execute(sql, params or ())
        return cur

    def commit(self) -> None:
        try:
            self._conn.commit()
        except Exception:
            _db_log.debug("CompatConnection.commit() failed", exc_info=True)

    def rollback(self) -> None:
        try:
            self._conn.rollback()
        except Exception:
            _db_log.debug("CompatConnection.rollback() failed", exc_info=True)

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            _db_log.debug("CompatConnection.close() failed", exc_info=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            try:
                self.commit()
            except Exception:
                _db_log.debug("CompatConnection.__exit__ commit failed", exc_info=True)
        else:
            try:
                self.rollback()
            except Exception:
                _db_log.debug("CompatConnection.__exit__ rollback failed", exc_info=True)
        try:
            self.close()
        except Exception:
            _db_log.debug("CompatConnection.__exit__ close failed", exc_info=True)
        return False

    # ---------------- Postgres introspection emulation ----------------

    def _pg_table_has_column(self, table: str, col: str) -> bool:
        t = _normalize_ident(table)
        c = (col or "").strip()
        if not t or not c:
            return False
        q = """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema='public'
              AND table_name=%s
              AND column_name=%s
            LIMIT 1
        """
        cur = self._conn.cursor()
        cur.execute(q, (t, c))
        return cur.fetchone() is not None

    def _pg_pragma_table_info(self, table: str) -> list[tuple[Any, ...]]:
        """
        Emulate SQLite: PRAGMA table_info(table)
        Shape in SQLite:
          (cid, name, type, notnull, dflt_value, pk)
        """
        t = _normalize_ident(table)
        if not t:
            return []

        q = """
            SELECT
              ordinal_position - 1 AS cid,
              c.column_name AS name,
              data_type AS type,
              CASE WHEN is_nullable = 'NO' THEN 1 ELSE 0 END AS notnull,
              column_default AS dflt_value,
              CASE WHEN tc.constraint_type IS NOT NULL THEN 1 ELSE 0 END AS pk
            FROM information_schema.columns c
            LEFT JOIN (
              SELECT
                kcu.table_name,
                kcu.column_name,
                tc.constraint_type
              FROM information_schema.table_constraints tc
              JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
               AND tc.table_schema = kcu.table_schema
              WHERE tc.table_schema='public' AND tc.constraint_type='PRIMARY KEY'
            ) tc
              ON tc.table_name = c.table_name AND tc.column_name = c.column_name
            WHERE c.table_schema='public' AND c.table_name=%s
            ORDER BY ordinal_position
        """
        cur = self._conn.cursor()
        cur.execute(q, (t,))
        rows = cur.fetchall() or []
        out: list[tuple[Any, ...]] = []
        for cid, name, ctype, notnull, dflt_value, pk in rows:
            out.append((cid, name, ctype, notnull, dflt_value, pk))
        return out

    def _pg_pragma_foreign_key_list(self, table: str) -> list[tuple[Any, ...]]:
        """
        Emulate SQLite: PRAGMA foreign_key_list(table)

        SQLite shape:
          (id, seq, table, from, to, on_update, on_delete, match)
        """
        t = _normalize_ident(table)
        if not t:
            return []

        def _act(code: str) -> str:
            # Postgres: a=NO ACTION, r=RESTRICT, c=CASCADE, n=SET NULL, d=SET DEFAULT
            return {
                "a": "NO ACTION",
                "r": "RESTRICT",
                "c": "CASCADE",
                "n": "SET NULL",
                "d": "SET DEFAULT",
            }.get(code, "NO ACTION")

        out: list[tuple[Any, ...]] = []
        try:
            q = """
                SELECT
                  con.oid AS id,
                  ck.ordinality - 1 AS seq,
                  con.confrelid::regclass::text AS ref_table,
                  a.attname AS from_col,
                  af.attname AS to_col,
                  con.confupdtype AS upd,
                  con.confdeltype AS del
                FROM pg_constraint con
                JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS ck(attnum, ordinality) ON true
                JOIN LATERAL unnest(con.confkey) WITH ORDINALITY AS fk(attnum, ordinality)
                  ON fk.ordinality = ck.ordinality
                JOIN pg_attribute a
                  ON a.attrelid = con.conrelid AND a.attnum = ck.attnum AND NOT a.attisdropped
                JOIN pg_attribute af
                  ON af.attrelid = con.confrelid AND af.attnum = fk.attnum AND NOT af.attisdropped
                WHERE con.contype = 'f'
                  AND con.conrelid = %s::regclass
                ORDER BY con.oid, ck.ordinality
            """
            cur = self._conn.cursor()
            cur.execute(q, (t,))
            rows = cur.fetchall() or []
            for oid, seq, ref_table, from_col, to_col, up, de in rows:
                out.append(
                    (
                        int(oid),
                        int(seq),
                        str(ref_table),
                        str(from_col),
                        str(to_col),
                        _act(str(up)),
                        _act(str(de)),
                        "NONE",
                    )
                )
        except Exception:
            return []

        return out

    def _pg_emulate_sqlite_master_query(
        self, sql: str, params: Sequence[Any]
    ) -> list[tuple[Any, ...]]:
        """
        Emulate simple `sqlite_master` checks used in tasks/scripts.

        Supported shapes (best-effort):
          - SELECT name FROM sqlite_master WHERE type='table' AND name = ?
          - SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?
          - type IN ('table','view') variants
          - name='literal' variants (no param)
        """
        s = sql.strip()
        select_one = bool(re.match(r"^\s*select\s+1\b", s, re.IGNORECASE))

        name: str | None = None
        if params:
            try:
                name = str(params[0]) if params[0] is not None else None
            except Exception:
                name = None

        if name is None:
            m = re.search(r"\bname\s*=\s*'([^']+)'\s*", s, re.IGNORECASE)
            if m:
                name = m.group(1)

        if not name:
            return []

        want_views = bool(
            re.search(r"type\s+in\s*\(\s*'table'\s*,\s*'view'\s*\)", s, re.IGNORECASE)
        )
        want_views = want_views or bool(re.search(r"type\s*=\s*'view'", s, re.IGNORECASE))

        exists = False
        try:
            if want_views:
                q = """
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema='public' AND table_name=%s
                    UNION ALL
                    SELECT 1
                    FROM information_schema.views
                    WHERE table_schema='public' AND table_name=%s
                    LIMIT 1
                """
                cur = self._conn.cursor()
                cur.execute(q, (name, name))
                exists = cur.fetchone() is not None
            else:
                q = """
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema='public' AND table_name=%s
                    LIMIT 1
                """
                cur = self._conn.cursor()
                cur.execute(q, (name,))
                exists = cur.fetchone() is not None
        except Exception:
            exists = False

        if not exists:
            return []

        if select_one:
            return [(1,)]
        return [(name,)]


# ---------------------------------------------------------------------------
# Connection selection
# ---------------------------------------------------------------------------


def get_conn() -> CompatConnection:
    """
    Primary DB access point.

    Target state:
      - Postgres is the only supported system of record.
      - All code paths (API, workers, scripts) must go through this function so the
        compatibility layer can smooth over any remaining SQLite-isms during the
        migration window (qmark placeholders, INSERT OR IGNORE, a few PRAGMAs).

    Dev escape hatch:
      - If ALLOW_SQLITE_DEV=1 is set, SQLite can be used for legacy local workflows.
        This is not supported for production.
    """
    url = _database_url()

    if _is_postgres_url(url):
        if psycopg2 is None:  # pragma: no cover
            raise RuntimeError(
                "DATABASE_URL/DB_URL points at Postgres but psycopg2 is not installed. "
                "Install psycopg2-binary (or psycopg2) to enable Postgres."
            )

        conn = psycopg2.connect(  # type: ignore[call-arg]
            url,
            cursor_factory=psycopg2.extras.DictCursor,
        )
        try:
            conn.autocommit = False
        except Exception:
            pass

        # Best-effort: enforce UTC session timezone to reduce surprises
        try:
            cur = conn.cursor()
            cur.execute("SET TIME ZONE 'UTC'")
        except Exception:
            pass

        return CompatConnection(conn, is_pg=True)

    if not ALLOW_SQLITE_DEV:
        raise RuntimeError(
            "Postgres is required. Set DATABASE_URL/DB_URL to a postgresql:// connection string. "
            "If you intentionally need legacy SQLite for local dev only, set ALLOW_SQLITE_DEV=1."
        )

    # Legacy SQLite dev-only fallback (not supported for production).
    import sqlite3  # local import by design

    path = _sqlite_path_from_url(url) or DEFAULT_SQLITE_PATH
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    try:
        con.execute("PRAGMA foreign_keys=ON")
    except Exception:
        pass
    return CompatConnection(con, is_pg=False)


def get_connection(db_path: str | None = None) -> Any:
    """
    Legacy SQLite-only connection helper (dev-only).

    Prefer get_conn() everywhere. This helper remains only to keep older
    scripts/modules importable while the last SQLite-only components are retired.

    Disabled unless ALLOW_SQLITE_DEV=1.
    """
    if not ALLOW_SQLITE_DEV:
        raise RuntimeError(
            "get_connection() is disabled (Postgres-only). Use get_conn(). "
            "Set ALLOW_SQLITE_DEV=1 only for legacy local-dev workflows."
        )

    import sqlite3  # local import by design

    if db_path is None:
        url = _database_url()
        if _is_postgres_url(url):
            raise RuntimeError(
                "get_connection() is SQLite-only; DATABASE_URL/DB_URL points at Postgres."
            )
        path = _sqlite_path_from_url(url) or DEFAULT_SQLITE_PATH
    else:
        if _is_postgres_url(db_path):
            raise RuntimeError("get_connection() is SQLite-only; a Postgres URL was provided.")
        path = _sqlite_path_from_url(db_path)

    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    try:
        con.execute("PRAGMA foreign_keys=ON")
    except Exception:
        pass
    return con


# ---------------------------------------------------------------------------
# Introspection helpers
# ---------------------------------------------------------------------------


def _table_columns(conn: Any, table: str) -> dict[str, dict[str, Any]]:
    """
    Returns a dict mapping column_name -> metadata via PRAGMA table_info(table).

    On Postgres, this is emulated by CompatCursor/CompatConnection.
    """
    meta: dict[str, dict[str, Any]] = {}
    try:
        cur = conn.execute(f"PRAGMA table_info({table})")
        rows = cur.fetchall() or []
    except Exception:
        return meta

    for row in rows:
        try:
            cid, name, ctype, notnull, dflt_value, pk = row[:6]
        except Exception:
            continue
        if not name:
            continue
        meta[str(name)] = {
            "name": str(name),
            "type": (str(ctype) if ctype is not None else "").upper(),
            "notnull": bool(int(notnull) if notnull is not None else 0),
            "default": dflt_value,
            "pk": bool(int(pk) if pk is not None else 0),
        }
    return meta


def _fk_map(conn: Any, table: str) -> dict[str, tuple[str, str]]:
    """
    Maps local_col -> (ref_table, ref_col) for foreign keys in table.

    Works for:
      - SQLite via PRAGMA foreign_key_list
      - Postgres via CompatCursor PRAGMA emulation
    """
    m: dict[str, tuple[str, str]] = {}
    try:
        cur = conn.execute(f"PRAGMA foreign_key_list({table})")
        rows = cur.fetchall() or []
    except Exception:
        return m

    for row in rows:
        try:
            _id, _seq, ref_table, from_col, to_col = row[:5]
        except Exception:
            continue
        if from_col and ref_table and to_col:
            m[str(from_col)] = (str(ref_table), str(to_col))
    return m


# ---------------------------------------------------------------------------
# Tenant resolution helpers
# ---------------------------------------------------------------------------


def _env_tenant_id() -> str:
    return (os.getenv("TENANT_ID") or "").strip() or DEFAULT_TENANT_ID


def _infer_tenant_from_row(conn: Any, table: str, row_id: int) -> str | None:
    cols = _table_columns(conn, table)
    if "tenant_id" not in cols:
        return None
    try:
        cur = conn.execute(f"SELECT tenant_id FROM {table} WHERE id = ? LIMIT 1", (int(row_id),))
        row = cur.fetchone()
        if row and row[0]:
            return str(row[0])
    except Exception:
        return None
    return None


def _resolve_tenant_id(
    conn: Any,
    *,
    tenant_id: str | None = None,
    company_id: int | None = None,
    person_id: int | None = None,
    email: str | None = None,
) -> str:
    """
    Resolve tenant_id using:
      1) explicit tenant_id parameter
      2) tenant inferred from existing rows (companies/people/emails) when possible
      3) TENANT_ID env var
      4) DEFAULT_TENANT_ID ('dev')
    """
    if tenant_id and str(tenant_id).strip():
        return str(tenant_id).strip()

    if company_id:
        t = _infer_tenant_from_row(conn, "companies", int(company_id))
        if t:
            return t
    if person_id:
        t = _infer_tenant_from_row(conn, "people", int(person_id))
        if t:
            return t

    if email:
        cols = _table_columns(conn, "emails")
        if "tenant_id" in cols:
            try:
                cur = conn.execute(
                    "SELECT tenant_id FROM emails WHERE email = ? LIMIT 1",
                    ((email or "").strip().lower(),),
                )
                row = cur.fetchone()
                if row and row[0]:
                    return str(row[0])
            except Exception:
                pass

    return _env_tenant_id()


# ---------------------------------------------------------------------------
# Minimal entity ensure helpers (schema-aware)
# ---------------------------------------------------------------------------


def _derive_name_from_email(email: str) -> tuple[str, str, str]:
    local = (email or "").split("@", 1)[0]
    parts = [p for p in re.split(r"[._\-\s]+", local) if p]
    if not parts:
        return ("Unknown", "Unknown", "")
    first = parts[0].capitalize()
    last = " ".join(p.capitalize() for p in parts[1:]) if len(parts) > 1 else ""
    full = f"{first} {last}".strip()
    return (full, first, last)


def _ensure_company(conn: Any, tenant_id: str, domain: str) -> int:
    domain_norm = (domain or "").strip().lower()
    if not domain_norm:
        raise ValueError("_ensure_company: domain required")

    cols = _table_columns(conn, "companies")
    where = "domain = ?"
    params: list[Any] = [domain_norm]
    if "tenant_id" in cols:
        where = "tenant_id = ? AND domain = ?"
        params = [tenant_id, domain_norm]

    try:
        row = conn.execute(
            f"SELECT id FROM companies WHERE {where} LIMIT 1", tuple(params)
        ).fetchone()
        if row and row[0] is not None:
            return int(row[0])
    except Exception:
        pass

    insert_cols: list[str] = []
    insert_vals: list[Any] = []

    def add(c: str, v: Any) -> None:
        if c in cols:
            insert_cols.append(c)
            insert_vals.append(v)

    add("tenant_id", tenant_id)
    add("name", domain_norm)
    add("domain", domain_norm)
    add("user_supplied_domain", domain_norm)

    if not insert_cols:
        insert_cols = ["name", "domain"]
        insert_vals = [domain_norm, domain_norm]

    ph = ", ".join(["?"] * len(insert_cols))
    cols_sql = ", ".join(insert_cols)

    try:
        cur = conn.execute(f"INSERT INTO companies ({cols_sql}) VALUES ({ph})", tuple(insert_vals))
        try:
            if getattr(cur, "lastrowid", None) is not None:
                return int(cur.lastrowid)  # type: ignore[attr-defined]
        except Exception:
            pass
    except Exception:
        # If Postgres raised an integrity error, the transaction needs rollback
        # before the next query.
        try:
            conn.rollback()
        except Exception:
            pass

    try:
        row = conn.execute(
            f"SELECT id FROM companies WHERE {where} LIMIT 1", tuple(params)
        ).fetchone()
        if row and row[0] is not None:
            return int(row[0])
    except Exception:
        pass

    raise RuntimeError("Failed to ensure companies row")


def _ensure_person(conn: Any, tenant_id: str, *, email: str, company_id: int) -> int | None:
    emails_cols = _table_columns(conn, "emails")
    if "person_id" not in emails_cols:
        return None

    fk = _fk_map(conn, "emails").get("person_id")
    if fk:
        person_table, person_pk = fk
    else:
        person_table, person_pk = ("people", "id")

    pcols = _table_columns(conn, person_table)
    if not pcols:
        return None

    full, first, last = _derive_name_from_email(email)
    where_parts: list[str] = []
    params: list[Any] = []
    if "tenant_id" in pcols:
        where_parts.append("tenant_id = ?")
        params.append(tenant_id)
    if "company_id" in pcols:
        where_parts.append("company_id = ?")
        params.append(int(company_id))
    if "full_name" in pcols:
        where_parts.append("full_name = ?")
        params.append(full)

    if where_parts:
        try:
            row = conn.execute(
                f"SELECT {person_pk} FROM {person_table} WHERE "
                + " AND ".join(where_parts)
                + " LIMIT 1",
                tuple(params),
            ).fetchone()
            if row and row[0] is not None:
                return int(row[0])
        except Exception:
            pass

    insert_cols: list[str] = []
    insert_vals: list[Any] = []

    def add(c: str, v: Any) -> None:
        if c in pcols:
            insert_cols.append(c)
            insert_vals.append(v)

    add("tenant_id", tenant_id)
    add("company_id", int(company_id))
    add("full_name", full)
    add("first_name", first)
    add("last_name", last)
    add("email", email)
    add("created_at", _now_iso_z())
    add("updated_at", _now_iso_z())

    if not insert_cols:
        return None

    ph = ", ".join(["?"] * len(insert_cols))
    cols_sql = ", ".join(insert_cols)

    try:
        cur = conn.execute(
            f"INSERT INTO {person_table} ({cols_sql}) VALUES ({ph})", tuple(insert_vals)
        )
        try:
            if getattr(cur, "lastrowid", None) is not None:
                return int(cur.lastrowid)  # type: ignore[attr-defined]
        except Exception:
            pass
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass

    if where_parts:
        try:
            row = conn.execute(
                f"SELECT {person_pk} FROM {person_table} WHERE "
                + " AND ".join(where_parts)
                + " LIMIT 1",
                tuple(params),
            ).fetchone()
            if row and row[0] is not None:
                return int(row[0])
        except Exception:
            pass

    return None


def _select_email_id(conn: Any, tenant_id: str, email: str) -> int | None:
    cols = _table_columns(conn, "emails")
    if not cols:
        return None

    where = "email = ?"
    params: list[Any] = [(email or "").strip().lower()]
    if "tenant_id" in cols:
        where = "tenant_id = ? AND email = ?"
        params = [tenant_id, (email or "").strip().lower()]

    try:
        row = conn.execute(f"SELECT id FROM emails WHERE {where} LIMIT 1", tuple(params)).fetchone()
        if row and row[0] is not None:
            return int(row[0])
    except Exception:
        return None
    return None


# ---------------------------------------------------------------------------
# Optional: Redis enqueue helper (kept here to avoid circular imports)
# ---------------------------------------------------------------------------


def enqueue_probe_email(email_id: int, email: str, domain: str, force: bool = False) -> None:
    """
    Enqueue an SMTP probe task by importing queueing lazily.

    This keeps db.py usable in scripts that don't have Redis installed/configured.
    """
    try:
        from rq import Queue  # local import

        from src.queueing.redis import get_redis  # local import
        from src.queueing.tasks import task_probe_email  # local import

        queue_name = (os.getenv("SMTP_QUEUE_NAME") or "").strip() or "smtp"
        q = Queue(queue_name, connection=get_redis())
        q.enqueue(
            task_probe_email,
            email_id=int(email_id),
            email=str(email),
            domain=str(domain),
            force=bool(force),
            job_timeout=20,
            retry=None,
        )
    except Exception:
        return


# ---------------------------------------------------------------------------
# Core write paths (tenant-aware + schema-aware)
# ---------------------------------------------------------------------------


def upsert_generated_email(
    conn: Any,
    person_id: int | None,
    email: str,
    domain: str,
    source_note: str | None = None,
    *,
    enqueue_probe: bool = False,
    force_probe: bool = False,
    tenant_id: str | None = None,
) -> int | None:
    """
    Insert a generated email candidate into `emails` (idempotent).

    Uses INSERT OR IGNORE for SQLite portability; translated to ON CONFLICT DO NOTHING on Postgres.
    """
    email_norm = (email or "").strip().lower()
    dom_norm = (domain or "").strip().lower()
    if not email_norm or "@" not in email_norm:
        raise ValueError("upsert_generated_email expects a full email address with '@'")

    t = _resolve_tenant_id(conn, tenant_id=tenant_id, person_id=person_id, email=email_norm)

    company_id: int | None = None
    try:
        pcols = _table_columns(conn, "people")
        if person_id and pcols and "company_id" in pcols:
            row = conn.execute(
                "SELECT company_id FROM people WHERE id = ? LIMIT 1", (int(person_id),)
            ).fetchone()
            if row and row[0] is not None:
                company_id = int(row[0])
    except Exception:
        company_id = None

    if company_id is None:
        company_id = _ensure_company(conn, t, dom_norm)

    person_final: int | None = person_id
    try:
        emails_cols = _table_columns(conn, "emails")
        if "person_id" in emails_cols:
            if person_final is None:
                person_final = _ensure_person(conn, t, email=email_norm, company_id=int(company_id))
            else:
                fk = _fk_map(conn, "emails").get("person_id")
                if fk:
                    ptable, ppk = fk
                    r = conn.execute(
                        f"SELECT 1 FROM {ptable} WHERE {ppk} = ? LIMIT 1", (int(person_final),)
                    ).fetchone()
                    if not r:
                        person_final = _ensure_person(
                            conn, t, email=email_norm, company_id=int(company_id)
                        )
    except Exception:
        pass

    emails_cols = _table_columns(conn, "emails")
    insert_cols: list[str] = []
    insert_vals: list[Any] = []

    def add(c: str, v: Any) -> None:
        if c in emails_cols:
            insert_cols.append(c)
            insert_vals.append(v)

    add("tenant_id", t)
    add("person_id", person_final)
    add("company_id", int(company_id))
    add("email", email_norm)
    add("source_url", None)
    add("is_published", 0)
    add("icp_score", None)
    add("created_at", _now_iso_z())
    add("updated_at", _now_iso_z())

    add("domain", dom_norm)
    add("source_note", source_note)
    add("source", "generated")

    if not insert_cols:
        insert_cols = ["email"]
        insert_vals = [email_norm]

    ph = ", ".join(["?"] * len(insert_cols))
    cols_sql = ", ".join(insert_cols)

    conn.execute(f"INSERT OR IGNORE INTO emails ({cols_sql}) VALUES ({ph})", tuple(insert_vals))
    email_id = _select_email_id(conn, t, email_norm)

    if email_id is not None and enqueue_probe:
        enqueue_probe_email(int(email_id), email_norm, dom_norm, force=bool(force_probe))

    return email_id


def upsert_verification_result(
    email_id: int | None,
    email: str,
    domain: str,
    verify_status: str | None,
    reason: str | None,
    mx_host: str | None = None,
    verified_at: datetime | str | None = None,
    *,
    tenant_id: str | None = None,
    company_id: int | None = None,
    person_id: int | None = None,
) -> None:
    """
    Persist verification result in a schema-aware way.

    Prefers writing to `verification_results` if present; otherwise updates legacy `emails` columns.
    """
    with get_conn() as conn:
        email_norm = (email or "").strip().lower()
        dom_norm = (domain or "").strip().lower()
        if not email_norm or "@" not in email_norm:
            raise ValueError("upsert_verification_result expects a full email address with '@'")

        t = _resolve_tenant_id(
            conn, tenant_id=tenant_id, company_id=company_id, person_id=person_id, email=email_norm
        )

        # Ensure email row exists if email_id not provided
        if email_id is None:
            emails_cols = _table_columns(conn, "emails")
            if emails_cols:
                insert_cols: list[str] = []
                insert_vals: list[Any] = []

                def add_e(c: str, v: Any) -> None:
                    if c in emails_cols:
                        insert_cols.append(c)
                        insert_vals.append(v)

                comp_id = company_id
                if comp_id is None:
                    try:
                        comp_id = _ensure_company(conn, t, dom_norm)
                    except Exception:
                        comp_id = None

                person_final = person_id
                try:
                    if "person_id" in emails_cols and person_final is None and comp_id is not None:
                        person_final = _ensure_person(
                            conn, t, email=email_norm, company_id=int(comp_id)
                        )
                except Exception:
                    pass

                add_e("tenant_id", t)
                add_e("company_id", int(comp_id) if comp_id is not None else None)
                add_e("person_id", person_final)
                add_e("email", email_norm)
                add_e("created_at", _now_iso_z())
                add_e("updated_at", _now_iso_z())

                # If emails.company_id exists and is NOT NULL, avoid inserting an invalid row.
                if "company_id" in emails_cols and comp_id is None:
                    pass
                else:
                    ph = ", ".join(["?"] * len(insert_cols))
                    cols_sql = ", ".join(insert_cols)
                    conn.execute(
                        f"INSERT OR IGNORE INTO emails ({cols_sql}) VALUES ({ph})",
                        tuple(insert_vals),
                    )

            email_id = _select_email_id(conn, t, email_norm)

        ver_cols = _table_columns(conn, "verification_results")
        if ver_cols and email_id is not None:
            checked_at = _now_iso_z()
            v_at = None
            if isinstance(verified_at, datetime):
                v_at = _now_iso_z(verified_at)
            elif isinstance(verified_at, str) and verified_at.strip():
                v_at = verified_at.strip()

            insert_cols: list[str] = []
            insert_vals: list[Any] = []

            def add_v(c: str, v: Any) -> None:
                if c in ver_cols:
                    insert_cols.append(c)
                    insert_vals.append(v)

            add_v("tenant_id", t)
            add_v("email_id", int(email_id))
            add_v("mx_host", (mx_host or "").strip().lower() or None)
            add_v("status", (verify_status or "").strip().lower() or None)
            add_v("reason", reason)
            add_v("checked_at", checked_at)

            add_v("verify_status", (verify_status or "").strip().lower() or None)
            add_v("verify_reason", reason)
            add_v("verified_mx", (mx_host or "").strip().lower() or None)
            add_v("verified_at", v_at)

            if insert_cols:
                ph = ", ".join(["?"] * len(insert_cols))
                cols_sql = ", ".join(insert_cols)
                conn.execute(
                    f"INSERT INTO verification_results ({cols_sql}) VALUES ({ph})",
                    tuple(insert_vals),
                )
            return

        # Legacy fallback: update emails columns if they exist (older schemas)
        emails_cols = _table_columns(conn, "emails")
        if emails_cols:
            updates: list[str] = []
            vals: list[Any] = []

            def set_if(c: str, expr: str, v: Any) -> None:
                if c in emails_cols:
                    updates.append(f"{c} = {expr}")
                    vals.append(v)

            set_if("verify_status", "?", (verify_status or "").strip().lower() or None)
            set_if("reason", "?", reason)
            set_if("mx_host", "?", (mx_host or "").strip().lower() or None)

            if "verified_at" in emails_cols:
                if isinstance(verified_at, datetime):
                    set_if("verified_at", "?", _now_iso_z(verified_at))
                elif isinstance(verified_at, str) and verified_at.strip():
                    set_if("verified_at", "?", verified_at.strip())
                else:
                    set_if("verified_at", "?", _now_iso_z())

            if not updates:
                return

            if email_id is not None:
                if "tenant_id" in emails_cols:
                    vals.extend([t, int(email_id)])
                    conn.execute(
                        f"UPDATE emails SET {', '.join(updates)} WHERE tenant_id = ? AND id = ?",
                        tuple(vals),
                    )
                else:
                    vals.append(int(email_id))
                    conn.execute(
                        f"UPDATE emails SET {', '.join(updates)} WHERE id = ?",
                        tuple(vals),
                    )
                return

            # Update by email string if we couldn't resolve id
            if "tenant_id" in emails_cols:
                vals.extend([t, email_norm])
                conn.execute(
                    f"UPDATE emails SET {', '.join(updates)} WHERE tenant_id = ? AND email = ?",
                    tuple(vals),
                )
            else:
                vals.append(email_norm)
                conn.execute(f"UPDATE emails SET {', '.join(updates)} WHERE email = ?", tuple(vals))


def write_domain_resolution(
    company_id: int,
    company_name: str | None,
    user_hint: str | None,
    chosen_domain: str | None,
    method: str | None,
    confidence: float | None,
    reason: str | None,
    resolver_version: str | None = None,
    *,
    tenant_id: str | None = None,
) -> None:
    """
    Record a domain resolution choice (R10/R11 style) if the table exists.
    Also mirrors onto companies.{official_domain,...} if those columns exist.
    """
    with get_conn() as conn:
        chosen = (chosen_domain or "").strip().lower() or None
        t = _resolve_tenant_id(conn, tenant_id=tenant_id, company_id=int(company_id))

        dr_cols = _table_columns(conn, "domain_resolutions")
        if dr_cols:
            insert_cols: list[str] = []
            insert_vals: list[Any] = []

            def add(c: str, v: Any) -> None:
                if c in dr_cols:
                    insert_cols.append(c)
                    insert_vals.append(v)

            add("tenant_id", t)
            add("company_id", int(company_id))
            add("company_name", company_name)
            add("user_hint", user_hint)
            add("chosen_domain", chosen)
            add("method", method or "unknown")
            add("confidence", confidence)
            add("reason", reason)
            add("resolver_version", resolver_version)

            if insert_cols:
                ph = ", ".join(["?"] * len(insert_cols))
                cols_sql = ", ".join(insert_cols)
                conn.execute(
                    f"INSERT INTO domain_resolutions ({cols_sql}) VALUES ({ph})", tuple(insert_vals)
                )

        if chosen:
            c_cols = _table_columns(conn, "companies")
            if not c_cols:
                return

            sets: list[str] = []
            vals: list[Any] = []

            def set_if(col: str, val: Any) -> None:
                if col in c_cols:
                    sets.append(f"{col} = ?")
                    vals.append(val)

            set_if("official_domain", chosen)
            set_if("official_domain_confidence", confidence)
            set_if("official_domain_source", method)
            set_if("official_domain_checked_at", _now_iso_z())

            set_if("domain_official", chosen)
            set_if("domain_official_confidence", confidence)
            set_if("domain_official_source", method)
            set_if("domain_official_checked_at", _now_iso_z())

            if not sets:
                return

            if "tenant_id" in c_cols:
                vals.extend([t, int(company_id)])
                conn.execute(
                    f"UPDATE companies SET {', '.join(sets)} WHERE tenant_id = ? AND id = ?",
                    tuple(vals),
                )
            else:
                vals.append(int(company_id))
                conn.execute(f"UPDATE companies SET {', '.join(sets)} WHERE id = ?", tuple(vals))
