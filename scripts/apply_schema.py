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

# Columns that must NEVER be uniquely indexed (multi-brand rule)
_OFFICIAL_COLS = ("official_domain", "domain_official")


def _is_windows_drive(p: str) -> bool:
    # e.g., "C:/path" or "D:\\path"
    return len(p) >= 3 and p[1] == ":" and (p[2] == "/" or p[2] == "\\")


def resolve_sqlite_path() -> Path:
    """
    Accepts:
      - no env var  -> defaults to <repo>/dev.db
      - DATABASE_URL like sqlite:///relative/dev.db
      - DATABASE_URL like sqlite:////absolute/posix/dev.db
      - DATABASE_URL like sqlite:///C:/Users/You/email-scraper/dev.db (Windows)
      - DATABASE_URL like sqlite:////C:/Users/You/email-scraper/dev.db (Windows + extra slash)
    """
    url = (os.getenv("DATABASE_URL") or "").strip()
    if not url:
        return ROOT / "data" / "dev.db" if (ROOT / "data").exists() else ROOT / "dev.db"

    parsed = urlparse(url)
    if parsed.scheme != "sqlite":
        sys.exit("ERROR: These setup scripts only support SQLite (sqlite:///...).")

    raw_path = unquote(parsed.path or "")
    # On Windows, urlparse gives "/C:/..." for sqlite:///C:/...
    if os.name == "nt" and raw_path.startswith("/") and _is_windows_drive(raw_path[1:]):
        raw_path = raw_path[1:]

    if not raw_path:
        return ROOT / "dev.db"

    # Windows absolute "C:/..." or "C:\\..."
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
    Split on semicolons (simple splitter). Also upgrades CREATE INDEX/UNIQUE INDEX
    to IF NOT EXISTS to be idempotent. We don't touch CREATE TABLE/VIEW text beyond that.
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


# ---------- Multi-brand safety helpers ----------


def _is_official_unique_stmt(stmt: str) -> bool:
    """
    True if the SQL creates a UNIQUE index on companies(official_domain|domain_official).
    Robust to case, whitespace, quotes/brackets.
    """
    s = stmt.strip()
    if not re.match(r"(?is)^CREATE\s+UNIQUE\s+INDEX\b", s):
        return False
    pat = r'(?is)\bON\s+["\[]?companies["\]]?\s*\(\s*["\[]?(official_domain|domain_official)["\]]?\s*\)'
    return re.search(pat, s) is not None


def _drop_unique_official_if_present(conn: sqlite3.Connection) -> None:
    """
    If a UNIQUE index exists on companies.(official_domain|domain_official), drop it.
    This enforces "many companies → one domain" regardless of existing state.
    """
    rows = conn.execute("PRAGMA index_list('companies')").fetchall()
    for r in rows:
        # PRAGMA index_list returns: seq, name, unique, origin, partial
        if int(r["unique"]) != 1:
            continue
        name = r["name"]
        cols = [ri["name"] for ri in conn.execute(f"PRAGMA index_info('{name}')")]
        if any(c in _OFFICIAL_COLS for c in cols):
            conn.execute(f'DROP INDEX "{name}"')


# ---------- Existence helpers & guarded execution ----------


def _object_exists(conn: sqlite3.Connection, name: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE (type='table' OR type='view' OR type='index') AND name=?;",
            (name,),
        ).fetchone()
        is not None
    )


def _extract_create_target(stmt: str) -> tuple[str | None, str | None]:
    """
    Try to pull object type and name from a CREATE statement.
    Returns (obj_type, obj_name) with obj_type in {'table','view','index','unique index'} or (None, None).
    """
    s = stmt.strip().rstrip(";")
    m = re.match(
        r"(?is)^\s*CREATE\s+(?:(TEMP|TEMPORARY)\s+)?"
        r"(?P<kind>TABLE|VIEW|UNIQUE\s+INDEX|INDEX)\s+"
        r"(?:IF\s+NOT\s+EXISTS\s+)?"
        r'(?P<name>["`\[]?[A-Za-z_][\w$]*["`\]]?)',
        s,
    )
    if not m:
        return (None, None)
    kind = m.group("kind").lower()
    name = m.group("name")
    # strip quotes/brackets
    name = re.sub(r'^[`"\[]|[`"\]]$', "", name)
    return (kind, name)


def apply_schema(conn: sqlite3.Connection, schema_text: str) -> None:
    for stmt in iter_statements(schema_text):
        # Skip any forbidden UNIQUE index on official-domain (multi-brand rule)
        if _is_official_unique_stmt(stmt):
            print(
                "· Skipping UNIQUE index on companies.(official_domain|domain_official) per multi-brand rule"
            )
            continue

        # If the object already exists, skip CREATE to avoid re-executing broken legacy SQL
        kind, name = _extract_create_target(stmt)
        if (
            kind in {"table", "view", "index", "unique index"}
            and name
            and _object_exists(conn, name)
        ):
            # Let ALTER statements still run later if present; we only skip CREATEs here.
            # This is especially helpful if schema.sql has old CREATE TABLE text that would now error.
            print(f"· Skipping CREATE {kind} {name} (already exists)")
            continue

        try:
            conn.execute(stmt)
        except sqlite3.OperationalError as e:
            # Print the failing statement to help diagnose issues like "near 'domain': syntax error"
            print("\n--- FAILED SQL STATEMENT ---")
            print(stmt.strip())
            print("--- END FAILED SQL STATEMENT ---\n")
            msg = str(e).lower()
            # Benign idempotency errors (kept for completeness)
            if "already exists" in msg or "duplicate column name" in msg:
                continue
            raise
    # After applying schema, enforce multi-brand guard
    _drop_unique_official_if_present(conn)


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


def main() -> None:
    db_path = resolve_sqlite_path()
    print(f"→ Using SQLite at: {db_path}")

    with connect(db_path) as conn:
        schema_text = load_schema_text()
        apply_schema(conn, schema_text)

        # Make sure our idempotency contract is satisfied even if schema predates it:
        ensure_email_columns(conn)
        ensure_unique_index(
            conn
        )  # emails(email) only; multi-brand forbids uniqueness on official-domain
        ensure_v_emails_latest(conn)
        conn.commit()

        verify_integrity(conn)

        # Summaries
        for t in ("companies", "people", "emails", "verification_results"):
            exists = "yes" if _object_exists(conn, t) else "no"
            print(f"· {t:24} exists: {exists}")
        print(
            f"· v_emails_latest         exists: {'yes' if _object_exists(conn, 'v_emails_latest') else 'no'}"
        )
        print(
            f"· ux_emails_email (index) exists: {'yes' if _object_exists(conn, 'ux_emails_email') else 'no'}"
        )

    print("✔ Schema applied (idempotent), multi-brand guard enforced, integrity OK.")


if __name__ == "__main__":
    main()
