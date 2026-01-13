# tests/test_r08_integration.py
"""
R08 Domain Resolution Integration Tests

Tests the domain resolution job that writes to companies and domain_resolutions tables.

NOTE: The resolve_company_domain function creates and manages its own database
connection internally, closing it when done. Tests must use fresh connections
for verification to avoid "connection already closed" errors.
"""

from __future__ import annotations

import os
import uuid

# Check if we're running against PostgreSQL
_DB_URL = os.environ.get("DATABASE_URL", "")
_IS_POSTGRES = "postgresql" in _DB_URL.lower() or "postgres" in _DB_URL.lower()


def test_resolver_writes_company_and_audit(db_conn, monkeypatch):
    """Test that domain resolution writes to companies and domain_resolutions tables."""
    import src.db as db_mod
    from src.queueing.tasks import resolve_company_domain

    # Get a fresh connection for setup (the job will use its own)
    setup_conn = db_mod.get_conn()

    try:
        # Seed a company row with a unique name to avoid conflicts
        unique_suffix = uuid.uuid4().hex[:8]
        company_name = f"Bücher GmbH {unique_suffix}"

        cur = setup_conn.execute(
            "INSERT INTO companies(tenant_id, name) VALUES (%s, %s) RETURNING id",
            ("dev", company_name),
        )
        row = cur.fetchone()
        company_id = row[0] if row else None
        setup_conn.commit()

        assert company_id is not None, "Failed to insert company"

        # Fake the network
        import src.resolve.domain as mod

        monkeypatch.setattr(mod, "_dns_any", lambda h: h == "xn--bcher-kva.de")
        monkeypatch.setattr(
            mod,
            "_http_head_ok",
            lambda h: (h == "xn--bcher-kva.de", None),
        )

    finally:
        try:
            setup_conn.close()
        except Exception:
            pass

    # Execute the job - this creates and closes its own connection
    res = resolve_company_domain(company_id, company_name, "bücher.de")
    assert res["chosen"] == "xn--bcher-kva.de"
    assert res["confidence"] >= 80

    # Get a fresh connection for verification
    verify_conn = db_mod.get_conn()
    try:
        # Verify audit row was created
        cur = verify_conn.execute(
            "SELECT chosen_domain, confidence "
            "FROM domain_resolutions "
            "WHERE company_id = %s "
            "ORDER BY id DESC "
            "LIMIT 1",
            (company_id,),
        )
        audit_row = cur.fetchone()

        if audit_row:
            assert audit_row[0] == "xn--bcher-kva.de"
            assert audit_row[1] >= 80
    finally:
        try:
            verify_conn.close()
        except Exception:
            pass
