"""Tests for GET /admin/export-domains.txt."""

from __future__ import annotations

from starlette.testclient import TestClient

import src.api.admin as admin_module
import src.api.deps as deps_module
from src.api.app import app

# ---------------------------------------------------------------------------
# Helpers (follow test_r24_admin_ui.py patterns)
# ---------------------------------------------------------------------------


def _install_dev_auth(monkeypatch) -> None:
    """Ensure AUTH_MODE is 'dev' so require_admin is a no-op."""
    import src.api.app as app_mod

    monkeypatch.setattr(deps_module, "AUTH_MODE", "dev")
    monkeypatch.setattr(app_mod, "AUTH_MODE", "dev")


def _install_fake_superuser(monkeypatch) -> None:
    """Monkeypatch _require_superuser to return a fake User without a session."""
    from dataclasses import dataclass

    @dataclass
    class _FakeUser:
        id: str = "test-admin"
        tenant_id: str = "dev"
        email: str = "admin@test.local"
        display_name: str | None = "Test Admin"
        is_active: bool = True
        is_verified: bool = True
        is_superuser: bool = True
        is_approved: bool = True
        created_at: str = "2025-01-01T00:00:00Z"
        last_login_at: str | None = None

    monkeypatch.setattr(admin_module, "_require_superuser", lambda request: _FakeUser())


def _stub_audit(monkeypatch) -> None:
    """Silence audit logging so tests don't need the audit table."""
    monkeypatch.setattr(admin_module, "log_admin_action", lambda **kw: None)


class _FakeConn:
    """Minimal DB connection stub with configurable rows."""

    def __init__(self, rows: list[tuple]):
        self._rows = rows

    def execute(self, query, params=None):  # noqa: ARG002
        return self

    def fetchall(self):
        return self._rows

    def close(self):
        pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_export_domains_txt_happy_path(monkeypatch) -> None:
    """GET /admin/export-domains.txt returns sorted, deduplicated domains."""
    _install_dev_auth(monkeypatch)
    _install_fake_superuser(monkeypatch)
    _stub_audit(monkeypatch)

    fake_rows = [("alpha.com",), ("beta.io",), ("gamma.org",)]
    monkeypatch.setattr(
        "src.api.admin.get_conn",
        lambda: _FakeConn(fake_rows),
    )

    client = TestClient(app)
    resp = client.get("/admin/export-domains.txt")

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/plain")
    assert "attachment" in resp.headers.get("content-disposition", "")
    assert "company_domains.txt" in resp.headers.get("content-disposition", "")

    lines = resp.text.strip().splitlines()
    assert lines == ["alpha.com", "beta.io", "gamma.org"]


def test_export_domains_txt_empty_db(monkeypatch) -> None:
    """Empty companies table returns 200 with an empty body."""
    _install_dev_auth(monkeypatch)
    _install_fake_superuser(monkeypatch)
    _stub_audit(monkeypatch)

    monkeypatch.setattr(
        "src.api.admin.get_conn",
        lambda: _FakeConn([]),
    )

    client = TestClient(app)
    resp = client.get("/admin/export-domains.txt")

    assert resp.status_code == 200
    assert resp.text.strip() == ""


def test_export_domains_txt_filters_none_rows(monkeypatch) -> None:
    """Rows where COALESCE resolves to None are excluded from output."""
    _install_dev_auth(monkeypatch)
    _install_fake_superuser(monkeypatch)
    _stub_audit(monkeypatch)

    fake_rows = [("alpha.com",), (None,), ("gamma.org",)]
    monkeypatch.setattr(
        "src.api.admin.get_conn",
        lambda: _FakeConn(fake_rows),
    )

    client = TestClient(app)
    resp = client.get("/admin/export-domains.txt")

    lines = resp.text.strip().splitlines()
    assert lines == ["alpha.com", "gamma.org"]


def test_export_domains_txt_requires_superuser(monkeypatch) -> None:
    """Non-superuser requests get 403."""
    _install_dev_auth(monkeypatch)
    _stub_audit(monkeypatch)

    # Do NOT install the fake superuser — let _require_superuser raise 403
    monkeypatch.setattr(
        admin_module,
        "_get_current_user_from_session",
        lambda request: None,
    )

    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/admin/export-domains.txt")

    assert resp.status_code == 403
