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


def test_admin_requires_api_key_when_configured(monkeypatch) -> None:
    """
    When ADMIN_API_KEY is set, /admin/metrics must reject missing/invalid keys
    with 401 and accept a matching x-admin-api-key header.
    """
    _install_fake_summary(monkeypatch)

    # Configure a dummy API key and no IP allow-list.
    dummy_settings = _DummySettings(api_key="secret-key", allowed_ips=[])
    monkeypatch.setattr(deps_module, "settings", dummy_settings)

    client = TestClient(app)

    # No header → 401
    resp = client.get("/admin/metrics")
    assert resp.status_code == 401

    # Wrong key → 401
    resp = client.get("/admin/metrics", headers={"x-admin-api-key": "wrong"})
    assert resp.status_code == 401

    # Correct key → 200
    resp = client.get("/admin/metrics", headers={"x-admin-api-key": "secret-key"})
    assert resp.status_code == 200


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
