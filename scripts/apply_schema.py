#!/usr/bin/env python
# scripts/apply_schema.py
from __future__ import annotations

import os
import re
import sqlite3
import sys
from collections.abc import Iterable
from pathlib import Path
from urllib.parse import unquote, urlparse

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_FILE = ROOT / "db" / "schema.sql"


def _is_windows_drive(p: str) -> bool:
    # e.g., "C:/path" or "D:\path"
    return len(p) >= 3 and p[1] == ":" and (p[2] == "/" or p[2] == "\\")


def resolve_sqlite_path() -> Path:
    """
    Accepts:
      - no env var  -> defaults to <repo>/dev.db
      - DATABASE_URL like sqlite:///relative/dev.db
      - DATABASE_URL like sqlite:////absolute/posix/dev.db
      - DATABASE_URL like sqlite:///C:/Users/You/email-scraper/dev.db (Windows absolute)
      - DATABASE_URL like sqlite:////C:/Users/You/email-scraper/dev.db (Windows absolute w/ extra slash)
    """
    url = (os.getenv("DATABASE_URL") or "").strip()
    if not url:
        return ROOT / "dev.db"

    parsed = urlparse(url)
    if parsed.scheme != "sqlite":
        sys.exit("ERROR: These setup scripts only support SQLite (sqlite:///...).")

    raw_path = unquote(parsed.path or "")
    # On Windows, urlparse gives "/C:/..." for sqlite:///C:/...
    if os.name == "nt" and raw_path.startswith("/") and _is_windows_drive(raw_path[1:]):
        raw_path = raw_path[1:]

    if not raw_path:
        return ROOT / "dev.db"

    # Windows absolute "C:/..." or "C:\..."
    if _is_windows_drive(raw_path):
        return Path(raw_path)

    # POSIX absolute "/..."
    if raw_path.startswith("/"):
        return Path(raw_path)

    # Otherwise treat as relative to repo root
    return (ROOT / raw_path).resolve()


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    # WAL improves concurrency; safe to attempt here.
    try:
        conn.execute("PRAGMA journal_mode = WAL;")
    except sqlite3.OperationalError:
        pass
    return conn


def load_schema_text() -> str:
    if not SCHEMA_FILE.exists():
        sys.exit(f"ERROR: Missing schema file: {SCHEMA_FILE}")
    return SCHEMA_FILE.read_text(encoding="utf-8")


def iter_statements(sql: str) -> Iterable[str]:
    """
    Split on semicolons without being too clever; good enough for our schema.
    Also upgrades CREATE INDEX/UNIQUE INDEX to IF NOT EXISTS to be idempotent.
    We do NOT modify CREATE TABLE/VIEW to avoid surprises; we catch 'already exists' instead.
    """
    for raw in sql.split(";"):
        stmt = raw.strip()
        if not stmt:
            continue

        # Make indexes idempotent.
        stmt = re.sub(
            r"^\s*CREATE\s+UNIQUE\s+INDEX\s+(?!IF\s+NOT\s+EXISTS)",
            "CREATE UNIQUE INDEX IF NOT EXISTS ",
            stmt,
            flags=re.IGNORECASE,
        )
        stmt = re.sub(
            r"^\s*CREATE\s+INDEX\s+(?!IF\s+NOT\s+EXISTS)",
            "CREATE INDEX IF NOT EXISTS ",
            stmt,
            flags=re.IGNORECASE,
        )
        yield stmt + ";"


def table_columns(conn: sqlite3.Connection, table: str) -> dict:
    cols = {}
    for r in conn.execute(f"PRAGMA table_info({table});"):
        cols[r["name"]] = dict(
            cid=r["cid"],
            type=r["type"],
            not_null=bool(r["notnull"]),
            default=r["dflt_value"],
            pk=bool(r["pk"]),
        )
    return cols


def ensure_email_columns(conn: sqlite3.Connection) -> None:
    desired: tuple[tuple[str, str], ...] = (
        ("verify_status", "TEXT"),
        ("reason", "TEXT"),
        ("mx_host", "TEXT"),
        ("verified_at", "TEXT"),  # ISO8601
    )
    cols = table_columns(conn, "emails")
    for col, coltype in desired:
        if col not in cols:
            conn.execute(f"ALTER TABLE emails ADD COLUMN {col} {coltype};")


def ensure_unique_index(conn: sqlite3.Connection) -> None:
    # Idempotency guard: one row per email.
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_emails_email ON emails(email);")


def apply_schema(conn: sqlite3.Connection, schema_text: str) -> None:
    for stmt in iter_statements(schema_text):
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            # Ignore benign idempotency errors if the object already exists
            if "already exists" in msg:
                continue
            # Allow 'duplicate column name' if re-running ALTERs are present in schema
            if "duplicate column name" in msg:
                continue
            raise


def verify_integrity(conn: sqlite3.Connection) -> None:
    row = conn.execute("PRAGMA integrity_check;").fetchone()
    if not row or row[0] != "ok":
        sys.exit(f"ERROR: integrity_check failed: {row[0] if row else 'unknown'}")


def ensure_v_emails_latest(conn: sqlite3.Connection) -> None:
    # One row per email, preferring most recent verified_at, then highest id.
    conn.executescript(
        """
        CREATE VIEW IF NOT EXISTS v_emails_latest AS
        WITH ranked AS (
          SELECT
            e.*,
            ROW_NUMBER() OVER (
              PARTITION BY e.email
              ORDER BY
                (e.verified_at IS NULL) ASC,  -- push NULLs last
                e.verified_at DESC,
                e.id DESC
            ) AS rn
          FROM emails e
        )
        SELECT
          id, email, person_id, company_id, is_published, source_url, icp_score,
          verify_status, reason, mx_host, verified_at
        FROM ranked
        WHERE rn = 1;
        """
    )


def _object_exists(conn: sqlite3.Connection, name: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE (type='table' OR type='view' OR type='index') AND name=?;",
            (name,),
        ).fetchone()
        is not None
    )


def main() -> None:
    db_path = resolve_sqlite_path()
    print(f"→ Using SQLite at: {db_path}")

    with connect(db_path) as conn:
        schema_text = load_schema_text()
        apply_schema(conn, schema_text)

        # Make sure our idempotency contract is satisfied even if schema predates it:
        ensure_email_columns(conn)
        ensure_unique_index(conn)
        ensure_v_emails_latest(conn)
        conn.commit()

        verify_integrity(conn)

        # Summarize key tables/views
        for t in ("companies", "people", "emails", "verification_results"):
            exists = "yes" if _object_exists(conn, t) else "no"
            print(f"· {t:24} exists: {exists}")
        print(
            f"· v_emails_latest         exists: {'yes' if _object_exists(conn, 'v_emails_latest') else 'no'}"
        )
        print(
            f"· ux_emails_email (index) exists: {'yes' if _object_exists(conn, 'ux_emails_email') else 'no'}"
        )

    print("✔ Schema applied (idempotent), index ensured, integrity OK.")


if __name__ == "__main__":
    main()
