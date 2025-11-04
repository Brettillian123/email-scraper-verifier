# ruff: noqa: E402
# tests/conftest.py
from __future__ import annotations

import importlib
import sqlite3
import sys
from pathlib import Path

import pytest

# Ensure project root importable
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _ensure_schema(db_path: Path) -> None:
    """Create the minimal schema used by tests if it doesn't exist."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS companies (
              id INTEGER PRIMARY KEY,
              name TEXT,
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
            """
        )
        conn.commit()
    finally:
        conn.close()


@pytest.fixture(autouse=True)
def _wire_ingest_sqlite_env(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """
    Autouse: make sure src.ingest writes to the same SQLite DB the tests read,
    and that tables exist. Also force best-effort persistence.
    """
    if "temp_db" in request.fixturenames:
        db_path: Path = request.getfixturevalue("temp_db")
    else:
        session_tmp = tmp_path_factory.mktemp("db_session")
        db_path = session_tmp / "fallback.db"

    # Ensure schema exists where we'll write
    _ensure_schema(db_path)

    # Point ingest at this DB (URL + plain-path fallback)
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("TEST_DB_PATH", str(db_path))

    # Reload ingest so import-time lookups see env
    if "src.ingest" in sys.modules:
        importlib.reload(sys.modules["src.ingest"])
    else:
        import src.ingest  # noqa: F401

    # After reload, grab the live module and *force* its helpers to use our DB
    ingest_mod = sys.modules["src.ingest"]
    # Always resolve to our temp DB path
    monkeypatch.setattr(ingest_mod, "_sqlite_path_from_env", lambda: str(db_path), raising=False)
    # Always persist during tests
    monkeypatch.setattr(ingest_mod, "_force_best_effort", lambda: True, raising=False)

    print(f"PYTEST DB WIRED → {db_path}")
