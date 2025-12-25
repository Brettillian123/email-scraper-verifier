# tests/test_r08_integration.py
"""
R08 Integration Test - Domain Resolution

Tests that resolve_company_domain correctly:
1. Writes company data to the companies table
2. Creates an audit trail in domain_resolutions
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

# Get repo root for schema path
REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = REPO_ROOT / "db" / "schema.sql"


def test_resolver_writes_company_and_audit(tmp_path, monkeypatch):
    """Test that domain resolution writes to companies and domain_resolutions tables."""
    import src.db as db_mod
    import src.queueing.tasks as tasks_mod
    from src.queueing.tasks import resolve_company_domain

    # Create an isolated DB and load schema into it
    db_path = tmp_path / "t.db"

    with open(SCHEMA_PATH, encoding="utf-8") as f:
        schema_sql = f.read()

    con = sqlite3.connect(db_path)
    con.executescript(schema_sql)
    con.close()

    # CRITICAL: Patch at multiple levels to override conftest.py fixtures
    def _test_conn():
        return sqlite3.connect(str(db_path))

    # Patch the tasks module's _conn helper
    monkeypatch.setattr(tasks_mod, "_conn", _test_conn)

    # Also patch the main db module's get_conn (conftest may patch this)
    monkeypatch.setattr(db_mod, "get_conn", _test_conn)

    # Also set the env var for any code that reads it directly
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    # Seed a company row (minimal insert; other fields default NULL)
    with sqlite3.connect(str(db_path)) as con:
        cur = con.execute("INSERT INTO companies(name) VALUES (?)", ("Bücher GmbH",))
        company_id = cur.lastrowid

    # Fake the network: only the punycode domain "works"
    import src.resolve.domain as mod

    monkeypatch.setattr(mod, "_dns_any", lambda h: h == "xn--bcher-kva.de")
    monkeypatch.setattr(mod, "_http_head_ok", lambda h: (h == "xn--bcher-kva.de", None))

    # Execute the job function exactly as the worker would
    res = resolve_company_domain(company_id, "Bücher GmbH", "bücher.de")
    assert res["chosen"] == "xn--bcher-kva.de"
    assert res["confidence"] >= 80

    # Verify company row was updated with the official domain & confidence
    with sqlite3.connect(str(db_path)) as con:
        row = con.execute(
            "SELECT official_domain, official_domain_confidence FROM companies WHERE id = ?",
            (company_id,),
        ).fetchone()
        assert row is not None
        assert row[0] == "xn--bcher-kva.de"
        assert row[1] >= 80

        # And an audit row exists with the decision details
        audit = con.execute(
            "SELECT chosen_domain, method, confidence, resolver_version "
            "FROM domain_resolutions WHERE company_id = ?",
            (company_id,),
        ).fetchall()
        assert len(audit) == 1
        chosen_domain, method, confidence, resolver_version = audit[0]
        assert chosen_domain == "xn--bcher-kva.de"
        assert confidence >= 80
        assert isinstance(method, str) and method
        assert isinstance(resolver_version, str) and resolver_version
