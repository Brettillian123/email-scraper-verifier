# ruff: noqa: E402
# tests/conftest.py
"""
Pytest configuration and fixtures for the Email Scraper project.

Target state: Postgres is the primary test database.
Legacy SQLite support is available only when ALLOW_SQLITE_DEV=1 is set.

Test Database Setup:
  - Set DATABASE_URL/DB_URL to a PostgreSQL connection string
  - For CI, use a service container (see ci.yml)
  - For local dev, use a local Postgres instance or Docker

Example:
  DATABASE_URL=postgresql://postgres:postgres@localhost:5432/email_scraper_test pytest
"""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path
from typing import Any

import pytest

# Ensure project root importable
ROOT = Path(__file__).resolve().parents[1] if Path(__file__).resolve().parents else Path.cwd()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _is_postgres_url(url: str) -> bool:
    """Check if a URL is a PostgreSQL connection string."""
    u = (url or "").strip().lower()
    return u.startswith("postgres://") or u.startswith("postgresql://")


def _get_db_url() -> str:
    """Get the database URL from environment."""
    return (os.getenv("DATABASE_URL") or os.getenv("DB_URL") or "").strip()


def _ensure_postgres_schema(conn: Any) -> None:
    """
    Create minimal schema for tests on PostgreSQL.

    Uses the compat layer to run schema creation SQL.
    """
    # Minimal schema for tests (companies, people tables)
    # The full schema should be applied via apply_schema.py before tests
    schema_sql = """
        CREATE TABLE IF NOT EXISTS tenants (
            id TEXT PRIMARY KEY,
            name TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );

        INSERT INTO tenants (id, name)
        VALUES ('dev', 'Development')
        ON CONFLICT (id) DO NOTHING;

        CREATE TABLE IF NOT EXISTS companies (
            id BIGSERIAL PRIMARY KEY,
            tenant_id TEXT NOT NULL DEFAULT 'dev' REFERENCES tenants(id) ON DELETE CASCADE,
            name TEXT,
            domain TEXT,
            user_supplied_domain TEXT,
            source_url TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS people (
            id BIGSERIAL PRIMARY KEY,
            tenant_id TEXT NOT NULL DEFAULT 'dev' REFERENCES tenants(id) ON DELETE CASCADE,
            company_id BIGINT REFERENCES companies(id) ON DELETE SET NULL,
            first_name TEXT,
            last_name TEXT,
            full_name TEXT,
            title TEXT,
            role TEXT,
            source_url TEXT,
            notes TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS emails (
            id BIGSERIAL PRIMARY KEY,
            tenant_id TEXT NOT NULL DEFAULT 'dev' REFERENCES tenants(id) ON DELETE CASCADE,
            person_id BIGINT REFERENCES people(id) ON DELETE SET NULL,
            company_id BIGINT REFERENCES companies(id) ON DELETE CASCADE,
            email TEXT NOT NULL,
            is_published INTEGER DEFAULT 0,
            source_url TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE UNIQUE INDEX IF NOT EXISTS ux_emails_tenant_email
            ON emails(tenant_id, email);
    """

    # Split and execute statements
    for stmt in schema_sql.split(";"):
        stmt = stmt.strip()
        if stmt:
            try:
                conn.execute(stmt + ";")
            except Exception as e:
                # Ignore errors for IF NOT EXISTS statements
                if "already exists" not in str(e).lower():
                    print(f"Schema warning: {e}")

    try:
        conn.commit()
    except Exception:
        pass


def _ensure_sqlite_schema(db_path: Path) -> None:
    """
    Create minimal schema for tests on SQLite (legacy dev mode only).
    """
    import sqlite3

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS companies (
              id INTEGER PRIMARY KEY,
              name TEXT,
              domain TEXT,
              user_supplied_domain TEXT,
              source_url TEXT,
              created_at TEXT DEFAULT CURRENT_TIMESTAMP,
              updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS people (
              id INTEGER PRIMARY KEY,
              company_id INTEGER,
              first_name TEXT,
              last_name TEXT,
              full_name TEXT,
              title TEXT,
              role TEXT,
              source_url TEXT,
              notes TEXT,
              created_at TEXT DEFAULT CURRENT_TIMESTAMP,
              updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
              FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS emails (
              id INTEGER PRIMARY KEY,
              person_id INTEGER,
              company_id INTEGER,
              email TEXT NOT NULL,
              is_published INTEGER DEFAULT 0,
              source_url TEXT,
              created_at TEXT DEFAULT CURRENT_TIMESTAMP,
              updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
              FOREIGN KEY(person_id) REFERENCES people(id) ON DELETE SET NULL,
              FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
            );
            """
        )
        conn.commit()
    finally:
        conn.close()


@pytest.fixture(scope="session")
def postgres_available() -> bool:
    """Check if PostgreSQL is configured and available."""
    url = _get_db_url()
    if not _is_postgres_url(url):
        return False

    try:
        from src.db import get_conn

        with get_conn() as conn:
            conn.execute("SELECT 1")
            return True
    except Exception:
        return False


@pytest.fixture(autouse=True)
def _wire_db_env(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """
    Autouse fixture: Configure database for tests.

    For PostgreSQL:
      - Uses DATABASE_URL from environment
      - Ensures minimal schema exists
      - Cleans test data between test modules

    For SQLite (legacy dev mode, ALLOW_SQLITE_DEV=1):
      - Creates a temporary SQLite database
      - Ensures minimal schema exists
    """
    url = _get_db_url()

    if _is_postgres_url(url):
        # PostgreSQL mode
        try:
            from src.db import get_conn

            with get_conn() as conn:
                _ensure_postgres_schema(conn)
        except Exception as e:
            pytest.skip(f"PostgreSQL not available: {e}")

        # Ensure DATABASE_URL is set
        monkeypatch.setenv("DATABASE_URL", url)
        monkeypatch.setenv("DB_URL", url)

        print("PYTEST DB WIRED → PostgreSQL")

    elif os.getenv("ALLOW_SQLITE_DEV", "").strip().lower() in {"1", "true", "yes"}:
        # Legacy SQLite dev mode
        if "temp_db" in request.fixturenames:
            db_path: Path = request.getfixturevalue("temp_db")
        else:
            session_tmp = tmp_path_factory.mktemp("db_session")
            db_path = session_tmp / "fallback.db"

        _ensure_sqlite_schema(db_path)

        sqlite_url = f"sqlite:///{db_path}"
        monkeypatch.setenv("DATABASE_URL", sqlite_url)
        monkeypatch.setenv("DB_URL", sqlite_url)
        monkeypatch.setenv("TEST_DB_PATH", str(db_path))
        monkeypatch.setenv("ALLOW_SQLITE_DEV", "1")

        print(f"PYTEST DB WIRED → SQLite (dev mode): {db_path}")

    else:
        # No database configured
        pytest.skip(
            "No database configured. Set DATABASE_URL to PostgreSQL connection string, "
            "or set ALLOW_SQLITE_DEV=1 for legacy SQLite mode."
        )

    # Reload ingest module if loaded
    if "src.ingest" in sys.modules:
        try:
            importlib.reload(sys.modules["src.ingest"])
        except Exception:
            pass


@pytest.fixture
def temp_db(tmp_path: Path) -> Path:
    """
    Fixture providing a temporary SQLite database path (legacy dev mode).

    Only available when ALLOW_SQLITE_DEV=1.
    """
    if os.getenv("ALLOW_SQLITE_DEV", "").strip().lower() not in {"1", "true", "yes"}:
        pytest.skip("temp_db fixture requires ALLOW_SQLITE_DEV=1")

    db_path = tmp_path / "test.db"
    _ensure_sqlite_schema(db_path)
    return db_path


@pytest.fixture
def db_conn():
    """
    Fixture providing a database connection.

    Uses src.db.get_conn() which works for both PostgreSQL and SQLite.
    """
    from src.db import get_conn

    conn = get_conn()
    yield conn
    try:
        conn.close()
    except Exception:
        pass


@pytest.fixture
def clean_db(db_conn):
    """
    Fixture that cleans test data from the database after each test.

    This ensures test isolation by removing data created during the test.
    """
    yield db_conn

    # Clean up test data (order matters due to foreign keys)
    tables = ["emails", "people", "companies"]
    for table in tables:
        try:
            # Delete non-essential test data (preserve tenants)
            db_conn.execute(f"DELETE FROM {table} WHERE tenant_id = 'dev'")
        except Exception:
            pass

    try:
        db_conn.commit()
    except Exception:
        pass


@pytest.fixture(scope="session")
def schema_applied():
    """
    Session-scoped fixture ensuring the full schema is applied.

    Call apply_schema.py to set up all tables, indexes, and views.
    """
    url = _get_db_url()
    if not _is_postgres_url(url):
        return  # SQLite uses minimal schema from _ensure_sqlite_schema

    try:
        import subprocess

        result = subprocess.run(
            [sys.executable, str(ROOT / "apply_schema.py")],
            capture_output=True,
            text=True,
            env={**os.environ, "DATABASE_URL": url},
        )
        if result.returncode != 0:
            print(f"Schema apply warning: {result.stderr}")
    except Exception as e:
        print(f"Schema apply error: {e}")
