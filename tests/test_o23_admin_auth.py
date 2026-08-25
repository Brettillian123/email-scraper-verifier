# tests/test_o23_admin_auth.py
from __future__ import annotations

from fastapi.testclient import TestClient

import src.api.admin as admin_module
import src.api.deps as deps_module
from src.api.app import app


def _sample_admin_summary() -> dict:
    """
    Deterministic payload used to isolate /admin/metrics from Redis/SQLite.

    For O23 auth tests we only care about status codes, but providing a small
    stable payload keeps behaviour predictable if the handler is invoked.
    """
    return {
        "queues": [],
        "workers": [],
        "verification": {
            "total_emails": 0,
            "by_status": {},
            "valid_rate": 0.0,
        },
        "costs": {
            "smtp_probes": 0,
            "catchall_checks": 0,
            "domains_resolved": 0,
            "pages_crawled": 0,
        },
    }


class _DummySettings:
    """
    Minimal stand-in for src.config.settings for auth tests.

    Only ADMIN_API_KEY and ADMIN_ALLOWED_IPS are needed by require_admin.
    """

    def __init__(self, api_key: str, allowed_ips: list[str]) -> None:
        self.ADMIN_API_KEY = api_key
        self.ADMIN_ALLOWED_IPS = allowed_ips


def _install_fake_summary(monkeypatch) -> None:
    def fake_get_admin_summary() -> dict:
        return _sample_admin_summary()

    monkeypatch.setattr(admin_module, "get_admin_summary", fake_get_admin_summary)


def _install_session_user(monkeypatch, *, is_superuser: bool):
    """
    Give admin._require_superuser a session user without a real cookie.

    Only the session lookup is stubbed, so the real _require_superuser gate
    still runs and is what decides 200 vs 403.
    """
    from src.auth.core import User

    user = User(
        id="test-admin",
        tenant_id="dev",
        email="admin@test.local",
        display_name="Test Admin",
        is_active=True,
        is_verified=True,
        is_superuser=is_superuser,
        is_approved=True,
        created_at="2025-01-01T00:00:00Z",
        last_login_at=None,
    )
    monkeypatch.setattr(
        admin_module, "_get_current_user_from_session", lambda request: user
    )
    return user


def test_admin_requires_api_key_when_configured(monkeypatch) -> None:
    """
    When ADMIN_API_KEY is set, require_admin must reject missing/invalid keys
    with 401. A valid key clears require_admin but does NOT by itself grant
    access: /admin/metrics is additionally superuser-only, so a keyed caller
    with no superuser session is refused with 403.
    """
    _install_fake_summary(monkeypatch)

    # Configure a dummy API key and no IP allow-list.
    dummy_settings = _DummySettings(api_key="secret-key", allowed_ips=[])
    monkeypatch.setattr(deps_module, "settings", dummy_settings)

    # AUTH_MODE must NOT be "dev"/"none" (skip key checks) or "session".
    monkeypatch.setattr(deps_module, "AUTH_MODE", "apikey")

    client = TestClient(app)

    # No header -> 401
    resp = client.get("/admin/metrics")
    assert resp.status_code == 401

    # Wrong key -> 401
    resp = client.get("/admin/metrics", headers={"x-admin-api-key": "wrong"})
    assert resp.status_code == 401

    # Correct key clears the key check, but the superuser gate still refuses.
    resp = client.get("/admin/metrics", headers={"x-admin-api-key": "secret-key"})
    assert resp.status_code == 403

    # Correct key + superuser session -> 200, and the handler really ran.
    _install_session_user(monkeypatch, is_superuser=True)
    resp = client.get("/admin/metrics", headers={"x-admin-api-key": "secret-key"})
    assert resp.status_code == 200
    assert resp.json() == _sample_admin_summary()


def test_admin_refuses_non_superuser_with_valid_api_key(monkeypatch) -> None:
    """
    A valid admin API key plus a verified/approved but non-superuser session
    must still be refused: /admin/metrics exposes cross-tenant data.
    """
    _install_fake_summary(monkeypatch)
    monkeypatch.setattr(
        deps_module, "settings", _DummySettings(api_key="secret-key", allowed_ips=[])
    )
    monkeypatch.setattr(deps_module, "AUTH_MODE", "apikey")
    _install_session_user(monkeypatch, is_superuser=False)

    client = TestClient(app)

    resp = client.get("/admin/metrics", headers={"x-admin-api-key": "secret-key"})
    assert resp.status_code == 403


def test_admin_rejects_disallowed_ip_even_with_valid_key(monkeypatch) -> None:
    """
    The O23 IP allow-list is enforced ahead of the key check: a caller whose IP
    is not listed gets 403 even with the correct key and a superuser session.
    """
    _install_fake_summary(monkeypatch)
    monkeypatch.setattr(
        deps_module,
        "settings",
        _DummySettings(api_key="secret-key", allowed_ips=["10.0.0.1"]),
    )
    monkeypatch.setattr(deps_module, "AUTH_MODE", "apikey")
    _install_session_user(monkeypatch, is_superuser=True)

    client = TestClient(app)  # TestClient reports client IP 127.0.0.1

    resp = client.get("/admin/metrics", headers={"x-admin-api-key": "secret-key"})
    assert resp.status_code == 403


def test_is_ip_allowed_empty_allowlist_allows_any(monkeypatch) -> None:
    """
    When ADMIN_ALLOWED_IPS is empty, any client IP should be allowed.
    """
    dummy_settings = _DummySettings(api_key="", allowed_ips=[])
    monkeypatch.setattr(deps_module, "settings", dummy_settings)

    # Import inside test to ensure we see the monkeypatched settings.
    from src.api.deps import _is_ip_allowed

    assert _is_ip_allowed("127.0.0.1") is True
    assert _is_ip_allowed("10.0.0.1") is True
    assert _is_ip_allowed(None) is True


def test_is_ip_allowed_enforces_allowlist(monkeypatch) -> None:
    """
    When ADMIN_ALLOWED_IPS is non-empty, only listed IPs are allowed.
    """
    dummy_settings = _DummySettings(api_key="", allowed_ips=["10.0.0.1"])
    monkeypatch.setattr(deps_module, "settings", dummy_settings)

    from src.api.deps import _is_ip_allowed

    assert _is_ip_allowed("10.0.0.1") is True
    assert _is_ip_allowed("127.0.0.1") is False
    assert _is_ip_allowed(None) is False
