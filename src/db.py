# src/db.py
import os
import sqlite3
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse, unquote

try:
    import psycopg2
    _HAS_PSYCOPG2 = True
except ImportError:
    psycopg2 = None
    _HAS_PSYCOPG2 = False


def _get_db_url() -> str:
    url = os.environ.get("DATABASE_URL", "").strip()
    if url:
        return url
    url = os.environ.get("DB_URL", "").strip()
    if url:
        return url
    path = os.environ.get("DATABASE_PATH", "").strip()
    if path:
        return path
    return "dev.db"


def _is_postgres(url: str) -> bool:
    return url.startswith("postgresql://") or url.startswith("postgres://")


def _db_path() -> str:
    """
    SQLite-only path helper. Do NOT use when DATABASE_URL is PostgreSQL.
    """
    url = _get_db_url()
    if _is_postgres(url):
        raise RuntimeError("PostgreSQL detected. Use get_conn() instead of _db_path().")
    if url.startswith("sqlite:///"):
        return url[len("sqlite:///") :]
    return url


class CompatCursor:
    """Cursor wrapper that normalizes PostgreSQL paramstyle to look like SQLite ('?')."""

    def __init__(self, cursor, is_pg: bool):
        self._cursor = cursor
        self._is_pg = is_pg
        self.lastrowid = None
        self.rowcount = 0

    def execute(self, sql, params=()):
        if self._is_pg:
            sql = sql.replace("?", "%s")
            sql = sql.replace("datetime('now')", "NOW()")
        self._cursor.execute(sql, params)
        self.rowcount = self._cursor.rowcount
        if hasattr(self._cursor, "lastrowid"):
            self.lastrowid = self._cursor.lastrowid
        return self

    def fetchone(self):
        row = self._cursor.fetchone()
        if row is None:
            return None
        return row

    def fetchall(self):
        return self._cursor.fetchall()

    def __iter__(self):
        return iter(self.fetchall())


class CompatConnection:
    """Connection wrapper for SQLite/PostgreSQL compatibility."""

    def __init__(self, conn, is_pg: bool):
        self._conn = conn
        self._is_pg = is_pg
        self.row_factory = None

    @property
    def is_pg(self) -> bool:
        return bool(self._is_pg)

    def execute(self, sql, params=()):
        cur = self.cursor()
        cur.execute(sql, params)
        return cur

    def cursor(self):
        return CompatCursor(self._conn.cursor(), self._is_pg)

    def commit(self):
        return self._conn.commit()

    def rollback(self):
        return self._conn.rollback()

    def close(self):
        return self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._conn.close()


def get_conn() -> CompatConnection:
    url = _get_db_url()
    if _is_postgres(url):
        if not _HAS_PSYCOPG2:
            raise RuntimeError("PostgreSQL URL but psycopg2 not installed")
        parsed = urlparse(url)
        conn = psycopg2.connect(
            host=parsed.hostname or "localhost",
            port=parsed.port or 5432,
            user=parsed.username,
            password=unquote(parsed.password) if parsed.password else None,
            dbname=(parsed.path or "/email_scraper").lstrip("/"),
        )
        conn.autocommit = False
        return CompatConnection(conn, is_pg=True)

    # SQLite fallback
    path = url.replace("sqlite:///", "") if url.startswith("sqlite:///") else url
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys=ON")
    return CompatConnection(conn, is_pg=False)


def get_connection(db_path: str | None = None) -> CompatConnection:
    """
    Back-compat alias.

    IMPORTANT: always returns a CompatConnection (not a raw psycopg2/sqlite connection).
    """
    if db_path is None:
        return get_conn()

    if _is_postgres(db_path):
        if not _HAS_PSYCOPG2:
            raise RuntimeError("PostgreSQL URL but psycopg2 not installed")
        parsed = urlparse(db_path)
        conn = psycopg2.connect(
            host=parsed.hostname or "localhost",
            port=parsed.port or 5432,
            user=parsed.username,
            password=unquote(parsed.password) if parsed.password else None,
            dbname=(parsed.path or "/email_scraper").lstrip("/"),
        )
        conn.autocommit = False
        return CompatConnection(conn, is_pg=True)

    path = db_path.replace("sqlite:///", "") if db_path.startswith("sqlite:///") else db_path
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys=ON")
    return CompatConnection(conn, is_pg=False)


def _ts_iso8601_z(value: datetime | str | None) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        else:
            value = value.astimezone(UTC)
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sqlite_table_exists(cur: sqlite3.Cursor, table: str) -> bool:
    row = cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return bool(row)


def _table_columns_any(cur: Any, table: str, *, is_pg: bool) -> set[str]:
    """
    Return a set of column names for `table`.
    Works for:
      - SQLite (PRAGMA table_info)
      - Postgres (information_schema)
    """
    try:
        if not is_pg:
            cols = set()
            for _cid, name, _ctype, _notnull, _dflt, _pk in cur.execute(f"PRAGMA table_info({table})"):
                cols.add(str(name))
            return cols

        rows = cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = ?
            """,
            (table,),
        ).fetchall()
        return {str(r[0]) for r in rows}
    except Exception:
        return set()


def _ensure_email_id_via_conn(con: CompatConnection, *, email: str, domain: str | None = None) -> int | None:
    """
    Ensure an emails row exists for `email` and return emails.id.
    Best-effort and backend-agnostic.
    """
    email_norm = (email or "").strip().lower()
    dom = (domain or "").strip().lower() if domain else None
    if not email_norm:
        return None

    cur = con.cursor()

    # Fast path: select existing
    try:
        row = cur.execute("SELECT id FROM emails WHERE LOWER(email) = ? LIMIT 1", (email_norm,)).fetchone()
        if row and row[0] is not None:
            return int(row[0])
    except Exception:
        return None

    # Try insert/upsert
    if con.is_pg:
        # Prefer ON CONFLICT(email) if a unique constraint exists; otherwise this will error and we fall back.
        try:
            row = cur.execute(
                "INSERT INTO emails (email) VALUES (?) "
                "ON CONFLICT (email) DO UPDATE SET email = EXCLUDED.email "
                "RETURNING id",
                (email_norm,),
            ).fetchone()
            if row and row[0] is not None:
                return int(row[0])
        except Exception:
            # Fallback: plain insert then re-select
            try:
                if dom is not None:
                    cur.execute("INSERT INTO emails (email, domain) VALUES (?, ?)", (email_norm, dom))
                else:
                    cur.execute("INSERT INTO emails (email) VALUES (?)", (email_norm,))
            except Exception:
                pass
    else:
        # SQLite: ensure uniqueness then INSERT OR IGNORE
        try:
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_emails_email ON emails(email)")
        except Exception:
            pass
        try:
            cur.execute("INSERT OR IGNORE INTO emails (email) VALUES (?)", (email_norm,))
        except Exception:
            # Some schemas include a domain column; try it if available
            try:
                cur.execute("INSERT OR IGNORE INTO emails (email, domain) VALUES (?, ?)", (email_norm, dom))
            except Exception:
                pass

    # Re-select
    try:
        row = cur.execute("SELECT id FROM emails WHERE LOWER(email) = ? LIMIT 1", (email_norm,)).fetchone()
        if row and row[0] is not None:
            return int(row[0])
    except Exception:
        return None

    return None


def _write_verification_results_row(
    con: CompatConnection,
    *,
    email_id: int | None,
    email: str,
    domain: str,
    verify_status: str,
    verify_reason: str | None,
    verified_mx: str | None,
    mx_host: str | None,
    status: str | None,
    reason: str | None,
    verified_at: datetime | str | None,
    checked_at: datetime | str | None,
    fallback_status: str | None,
    fallback_raw: str | None,
    catch_all_status: str | None,
    fallback_checked_at: datetime | str | None,
) -> None:
    cur = con.cursor()
    cols = _table_columns_any(cur, "verification_results", is_pg=con.is_pg)

    if not cols:
        # If we can't introspect, try a minimal insert and let it fail silently upstream.
        cols = {
            "email_id",
            "email",
            "domain",
            "verify_status",
            "verify_reason",
            "verified_mx",
            "verified_at",
            "checked_at",
        }

    now_dt = datetime.now(UTC)
    v_at = verified_at or now_dt
    c_at = checked_at or now_dt

    insert_cols: list[str] = []
    insert_vals: list[Any] = []

    def add(col: str, val: Any) -> None:
        if col in cols:
            insert_cols.append(col)
            insert_vals.append(val)

    add("email_id", email_id)
    add("email", (email or "").strip().lower())
    add("domain", (domain or "").strip().lower())
    add("verify_status", verify_status)
    add("verify_reason", verify_reason)
    add("verified_mx", verified_mx)
    add("mx_host", mx_host)
    add("status", status)
    add("reason", reason)
    add("verified_at", v_at)
    add("checked_at", c_at)
    add("fallback_status", fallback_status)
    add("fallback_raw", fallback_raw)
    add("catch_all_status", catch_all_status)
    add("fallback_checked_at", fallback_checked_at)

    if not insert_cols:
        return

    placeholders = ", ".join("?" for _ in insert_cols)
    cols_sql = ", ".join(insert_cols)
    sql = f"INSERT INTO verification_results ({cols_sql}) VALUES ({placeholders})"
    cur.execute(sql, tuple(insert_vals))


def _best_effort_update_emails_status(
    con: CompatConnection,
    *,
    email_id: int | None,
    email: str,
    verify_status: str,
    reason: str | None,
    mx_host: str | None,
    verified_at: datetime | str | None,
) -> None:
    """
    Best-effort convenience update: if emails has verify_status/reason/mx_host/verified_at columns, update them.
    Never raises.
    """
    if email_id is None:
        return

    cur = con.cursor()
    cols = _table_columns_any(cur, "emails", is_pg=con.is_pg)
    if not cols:
        return

    sets: list[str] = []
    vals: list[Any] = []

    def set_col(col: str, val: Any) -> None:
        if col in cols:
            sets.append(f"{col} = ?")
            vals.append(val)

    set_col("verify_status", verify_status)
    # Different schemas may store "reason" or "verify_reason"
    set_col("reason", reason)
    set_col("verify_reason", reason)
    set_col("mx_host", mx_host)
    set_col("verified_at", verified_at or datetime.now(UTC))

    if not sets:
        return

    vals.append(int(email_id))
    sql = f"UPDATE emails SET {', '.join(sets)} WHERE id = ?"
    try:
        cur.execute(sql, tuple(vals))
    except Exception:
        # Some schemas key by email, not id
        try:
            vals2 = vals[:-1] + [(email or "").strip().lower()]
            cur.execute(
                f"UPDATE emails SET {', '.join(sets)} WHERE LOWER(email) = ?",
                tuple(vals2),
            )
        except Exception:
            return


# ---------------- ensure company/person (SQLite-only legacy helpers) ----------------


def _table_columns(cur, table: str) -> dict[str, dict]:
    """
    SQLite-only helper.
    Returns {col_name: {name,type,notnull,default,pk}} for table.
    """
    meta = {}
    for _cid, name, ctype, notnull, dflt_value, pk in cur.execute(f"PRAGMA table_info({table})"):
        meta[name] = {
            "name": name,
            "type": (ctype or "").upper(),
            "notnull": bool(notnull),
            "default": dflt_value,
            "pk": bool(pk),
        }
    return meta


def _fk_map(cur, table: str) -> dict[str, tuple[str, str]]:
    """
    SQLite-only helper.
    Maps local_col -> (ref_table, ref_col) for FKs in table.
    """
    m = {}
    for _id, _seq, ref_table, from_col, to_col, *_ in cur.execute(f"PRAGMA foreign_key_list({table})"):
        m[from_col] = (ref_table, to_col)
    return m


def _emails_pk_col(cur) -> str | None:
    """
    SQLite-only helper.
    Return the primary key column name for the emails table, or None if none found.
    """
    cols = _table_columns(cur, "emails")
    for name, meta in cols.items():
        if meta.get("pk"):
            return name
    if "id" in cols:
        return "id"
    return None


def _select_email_id(cur, email: str) -> int | None:
    """
    SQLite-only helper.
    Look up the primary key of an email row by its address.
    Returns None if the table has no PK or the row does not exist.
    """
    pk = _emails_pk_col(cur)
    if not pk:
        return None
    row = cur.execute(f"SELECT {pk} FROM emails WHERE email = ? LIMIT 1", (email,)).fetchone()
    return int(row[0]) if row else None


def _derive_name_from_email(email: str) -> tuple[str, str, str]:
    """
    'john.smith@example.com' -> ('John Smith', 'John', 'Smith')
    """
    local = email.split("@", 1)[0]
    parts = [p for p in local.replace(".", " ").replace("_", " ").replace("-", " ").split() if p]
    if not parts:
        return ("Unknown", "Unknown", "")
    first = parts[0].capitalize()
    last = " ".join(p.capitalize() for p in parts[1:]) if len(parts) > 1 else ""
    full = f"{first} {last}".strip()
    return (full, first, last)


def _ensure_company(cur, domain: str) -> int:
    row = cur.execute("SELECT id FROM companies WHERE domain = ?", (domain,)).fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO companies(name, domain) VALUES(?, ?)", (domain, domain))
    return cur.lastrowid


def _ensure_person(cur, *, email: str, company_id: int | None) -> int | None:
    """
    SQLite-only helper.
    Create or find a person that emails.person_id points to.
    """
    emails_cols = _table_columns(cur, "emails")
    if "person_id" not in emails_cols:
        return None

    fks = _fk_map(cur, "emails")
    if "person_id" not in fks:
        return None

    person_table, person_pk_col = fks["person_id"]
    pcols = _table_columns(cur, person_table)

    email_col = "email" if "email" in pcols else None
    if email_col:
        row = cur.execute(
            f"SELECT {person_pk_col} FROM {person_table} WHERE {email_col} = ?",
            (email,),
        ).fetchone()
        if row:
            return row[0]

    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    full, first, last = _derive_name_from_email(email)

    insert_cols: list[str] = []
    insert_vals: list[object] = []

    def add(col: str, val: object) -> None:
        insert_cols.append(col)
        insert_vals.append(val)

    if email_col:
        add(email_col, email)
    if "company_id" in pcols:
        add("company_id", company_id)
    if "name" in pcols:
        add("name", full)
    if "full_name" in pcols and "name" not in pcols:
        add("full_name", full)
    if "first_name" in pcols:
        add("first_name", first)
    if "last_name" in pcols:
        add("last_name", last)
    if "created_at" in pcols:
        add("created_at", now)
    if "updated_at" in pcols:
        add("updated_at", now)

    already = set(insert_cols)
    for c in pcols.values():
        if c["pk"] or not c["notnull"] or c["default"] is not None or c["name"] in already:
            continue
        ctype = c["type"]
        if "INT" in ctype:
            add(c["name"], 0)
        elif any(k in ctype for k in ("REAL", "FLOA", "DOUB")):
            add(c["name"], 0.0)
        else:
            add(c["name"], "")

    cols_sql = ", ".join(insert_cols)
    ph = ", ".join("?" for _ in insert_vals)
    cur.execute(f"INSERT INTO {person_table} ({cols_sql}) VALUES ({ph})", insert_vals)
    return cur.lastrowid


def _upsert_verification_result_legacy_sqlite_emails(
    *,
    email: str,
    verify_status: str,
    reason: str | None = None,
    mx_host: str | None = None,
    verified_at: datetime | str | None = None,
    company_id: int | None = None,
    person_id: int | None = None,
    source_url: str | None = None,
    icp_score: int | None = None,
) -> None:
    """
    Legacy SQLite implementation that upserts into the emails table (not verification_results).
    Preserved for older schemas.
    """
    db = _db_path()
    domain = email.split("@")[-1].lower().strip()
    ts = _ts_iso8601_z(verified_at)

    with sqlite3.connect(db) as con:
        con.execute("PRAGMA foreign_keys=ON")
        cur = con.cursor()

        comp_id = company_id or _ensure_company(cur, domain)

        emails_cols = _table_columns(cur, "emails")
        if "person_id" in emails_cols:
            if person_id is None:
                person_id = _ensure_person(cur, email=email, company_id=comp_id)
            else:
                fks = _fk_map(cur, "emails")
                if "person_id" in fks:
                    ptable, ppk = fks["person_id"]
                    exists = cur.execute(
                        f"SELECT 1 FROM {ptable} WHERE {ppk} = ? LIMIT 1",
                        (person_id,),
                    ).fetchone()
                    if not exists:
                        person_id = _ensure_person(cur, email=email, company_id=comp_id)

        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_emails_email ON emails(email)")

        cols = _table_columns(cur, "emails")
        insert_cols = ["email", "company_id", "verify_status", "reason", "mx_host", "verified_at"]
        insert_vals: list[Any] = [email, comp_id, verify_status, reason, mx_host, ts]

        if "person_id" in cols:
            insert_cols.append("person_id")
            insert_vals.append(person_id)
        if "source_url" in cols:
            insert_cols.append("source_url")
            insert_vals.append(source_url)
        if "icp_score" in cols:
            insert_cols.append("icp_score")
            insert_vals.append(icp_score)

        placeholders = ", ".join("?" for _ in insert_cols)
        insert_cols_sql = ", ".join(insert_cols)

        update_parts = [
            "company_id    = COALESCE(excluded.company_id, emails.company_id)",
            "verify_status = excluded.verify_status",
            "reason        = excluded.reason",
            "mx_host       = excluded.mx_host",
            "verified_at   = excluded.verified_at",
        ]
        if "person_id" in cols:
            update_parts.append("person_id = COALESCE(excluded.person_id, emails.person_id)")
        if "source_url" in cols:
            update_parts.append("source_url = COALESCE(excluded.source_url, emails.source_url)")
        if "icp_score" in cols:
            update_parts.append("icp_score = COALESCE(excluded.icp_score, emails.icp_score)")

        update_sql = ", ".join(update_parts)

        sql = f"""
            INSERT INTO emails ({insert_cols_sql})
            VALUES ({placeholders})
            ON CONFLICT(email) DO UPDATE SET
              {update_sql}
        """
        cur.execute(sql, insert_vals)
        con.commit()


# ---------------- main upserts ----------------


def upsert_verification_result(
    *,
    email: str,
    verify_status: str,
    domain: str | None = None,
    email_id: int | None = None,
    # "reason" historically meant verify_reason; we support both.
    reason: str | None = None,
    verify_reason: str | None = None,
    # mx host fields
    mx_host: str | None = None,
    verified_mx: str | None = None,
    # rcpt classification fields (if available)
    status: str | None = None,
    # timing
    verified_at: datetime | str | None = None,
    checked_at: datetime | str | None = None,
    fallback_checked_at: datetime | str | None = None,
    # escalation/catch-all
    fallback_status: str | None = None,
    fallback_raw: str | None = None,
    catch_all_status: str | None = None,
    # legacy args (ignored for verification_results path)
    company_id: int | None = None,
    person_id: int | None = None,
    source_url: str | None = None,
    icp_score: int | None = None,
    **_ignored: Any,
) -> None:
    """
    Unified persistence for verification outcomes.

    If verification_results exists (Postgres or SQLite), this:
      - ensures an emails row exists (so email_id is populated)
      - best-effort updates emails.* status columns (if present)
      - inserts a row into verification_results (append-only)

    If verification_results does not exist (older SQLite schemas), falls back to the
    legacy behavior: upsert into emails.* columns.
    """
    email_norm = (email or "").strip().lower()
    if not email_norm or "@" not in email_norm:
        raise ValueError("upsert_verification_result: expected a full email address")

    dom = (domain or email_norm.split("@", 1)[1]).strip().lower()
    v_reason = verify_reason if verify_reason is not None else reason
    v_mx = verified_mx if verified_mx is not None else mx_host

    # SQLite legacy schema detection: if not PG and verification_results table is absent,
    # keep the previous behavior that writes to emails table.
    url = _get_db_url()
    if not _is_postgres(url):
        try:
            db = _db_path()
            with sqlite3.connect(db) as scon:
                cur = scon.cursor()
                if not _sqlite_table_exists(cur, "verification_results"):
                    _upsert_verification_result_legacy_sqlite_emails(
                        email=email_norm,
                        verify_status=verify_status,
                        reason=v_reason,
                        mx_host=v_mx,
                        verified_at=verified_at,
                        company_id=company_id,
                        person_id=person_id,
                        source_url=source_url,
                        icp_score=icp_score,
                    )
                    return
        except Exception:
            # If SQLite probing fails, continue to unified path using get_conn()
            pass

    con = get_conn()
    try:
        # Ensure emails row exists; NEVER pass placeholder 0 through.
        if email_id is None or int(email_id) <= 0:
            email_id = _ensure_email_id_via_conn(con, email=email_norm, domain=dom)

        # Best-effort: update convenience columns on emails
        _best_effort_update_emails_status(
            con,
            email_id=email_id,
            email=email_norm,
            verify_status=verify_status,
            reason=v_reason,
            mx_host=v_mx,
            verified_at=verified_at,
        )

        # Insert into verification_results
        _write_verification_results_row(
            con,
            email_id=email_id,
            email=email_norm,
            domain=dom,
            verify_status=verify_status,
            verify_reason=v_reason,
            verified_mx=v_mx,
            mx_host=mx_host,
            status=status,
            reason=reason,
            verified_at=verified_at,
            checked_at=checked_at,
            fallback_status=fallback_status,
            fallback_raw=fallback_raw,
            catch_all_status=catch_all_status,
            fallback_checked_at=fallback_checked_at,
        )

        con.commit()
    except Exception:
        try:
            con.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            con.close()
        except Exception:
            pass


# ---------------- R16: verify/probe enqueue helper ----------------


def enqueue_probe_email(
    email_id: int, email: str, domain: str | None, *, force: bool = False
) -> None:
    """
    Best-effort enqueue for the R16 SMTP RCPT probe task (task_probe_email).

    Tries the ingest enqueue shim first (which tests observe), then falls back to
    real RQ enqueue on the 'verify' queue. Never raises.
    """
    try:
        try:
            from src.ingest.normalize import norm_domain  # type: ignore
        except Exception:
            norm_domain = None  # type: ignore

        canon_dom = domain
        if norm_domain:
            try:
                canon_dom = norm_domain(domain) if domain else None  # type: ignore[arg-type]
            except Exception:
                canon_dom = domain

        if not canon_dom:
            try:
                dom_raw = email.split("@", 1)[1]
                canon_dom = norm_domain(dom_raw) if norm_domain else dom_raw  # type: ignore[arg-type]
            except Exception:
                return

        try:
            from src.ingest import enqueue as ingest_enqueue  # type: ignore

            ingest_enqueue(
                "task_probe_email",
                {
                    "email_id": int(email_id),
                    "email": str(email),
                    "domain": str(canon_dom),
                    "force": bool(force),
                },
            )
        except Exception:
            pass

        try:
            from rq import Queue  # type: ignore

            from src.queueing.redis_conn import get_redis  # type: ignore
            from src.queueing.tasks import task_probe_email  # type: ignore
        except Exception:
            return

        try:
            q = Queue("verify", connection=get_redis())
            q.enqueue(
                task_probe_email,
                email_id=email_id,
                email=email,
                domain=canon_dom,
                force=force,
                job_timeout=20,
                retry=None,
            )
        except Exception:
            return
    except Exception:
        return


# ---------------- R12: generated emails upsert (SQLite-only) ----------------


def upsert_generated_email(
    con: sqlite3.Connection,
    person_id: int | None,
    email: str,
    domain: str,
    source_note: str | None = None,
    *,
    enqueue_probe: bool = False,
    force_probe: bool = False,
) -> int | None:
    """
    Insert a 'generated' email candidate for later verification.

    SQLite-only helper (expects a sqlite3.Connection).
    """
    con.execute("PRAGMA foreign_keys=ON")
    cur = con.cursor()

    email = (email or "").lower().strip()
    domain = (domain or "").lower().strip()
    if not email or "@" not in email:
        raise ValueError("upsert_generated_email: expected full email address with '@'")

    company_id = _ensure_company(cur, domain)

    emails_cols = _table_columns(cur, "emails")
    if "person_id" in emails_cols:
        if person_id is None:
            person_id = _ensure_person(cur, email=email, company_id=company_id)
        else:
            fks = _fk_map(cur, "emails")
            if "person_id" in fks:
                ptable, ppk = fks["person_id"]
                exists = cur.execute(
                    f"SELECT 1 FROM {ptable} WHERE {ppk} = ? LIMIT 1", (person_id,)
                ).fetchone()
                if not exists:
                    person_id = _ensure_person(cur, email=email, company_id=company_id)

    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_emails_email ON emails(email)")

    cols_meta = _table_columns(cur, "emails")

    insert_cols: list[str] = ["email", "company_id"]
    insert_vals: list[Any] = [email, company_id]

    def maybe(col: str, val: Any) -> None:
        if col in cols_meta:
            insert_cols.append(col)
            insert_vals.append(val)

    maybe("person_id", person_id)
    maybe("domain", domain)
    maybe("source", "generated")
    maybe("source_note", source_note)
    maybe("verify_status", None)
    if "created_at" in cols_meta:
        insert_cols.append("created_at")
        insert_vals.append(_ts_iso8601_z(None))

    placeholders = ", ".join("?" for _ in insert_cols)
    insert_cols_sql = ", ".join(insert_cols)

    sql = f"""
        INSERT INTO emails ({insert_cols_sql})
        VALUES ({placeholders})
        ON CONFLICT(email) DO NOTHING
    """
    cur.execute(sql, insert_vals)

    email_id = _select_email_id(cur, email)

    if enqueue_probe and email_id is not None:
        try:
            enqueue_probe_email(email_id, email, domain, force=force_probe)
        except Exception:
            pass

    return email_id


# ---------------- R08: DB integration helpers (unchanged; expect SQLite conn) ----------------


def set_user_hint_and_enqueue(
    conn: sqlite3.Connection, company_id: int, user_hint: str | None
) -> None:
    with conn:
        conn.execute(
            "UPDATE companies SET user_supplied_domain = ? WHERE id = ?",
            (user_hint, company_id),
        )


def write_domain_resolution(
    conn: sqlite3.Connection,
    company_id: int,
    company_name: str,
    decision: Any,
    user_hint: str | None,
) -> None:
    resolver_version = (
        getattr(decision, "resolver_version", None) or getattr(decision, "version", None) or "r08.3"
    )
    chosen = getattr(decision, "chosen", None)
    method = getattr(decision, "method", None)
    confidence = int(getattr(decision, "confidence", 0) or 0)
    reason = getattr(decision, "reason", None)

    with conn:
        conn.execute(
            """
            INSERT INTO domain_resolutions (
                company_id,
                company_name,
                user_hint,
                chosen_domain,
                method,
                confidence,
                reason,
                resolver_version
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                company_id,
                company_name,
                user_hint,
                chosen,
                method,
                confidence,
                reason,
                resolver_version,
            ),
        )

        if chosen:
            now = _ts_iso8601_z(None)
            conn.execute(
                """
                UPDATE companies
                   SET official_domain              = ?,
                       official_domain_confidence   = ?,
                       official_domain_source       = ?,
                       official_domain_checked_at   = ?
                 WHERE id = ?
                """,
                (chosen, confidence, method, now, company_id),
            )
