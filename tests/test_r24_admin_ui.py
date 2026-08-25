# tests/test_r24_admin_ui.py
from __future__ import annotations

from fastapi.testclient import TestClient

import src.api.admin as admin_module
import src.api.deps as deps_module
from src.api.app import app


def _sample_admin_summary() -> dict:
    """
    Small, deterministic payload used to isolate the /admin/metrics endpoint
    from Redis/SQLite in this unit test.

    The acceptance script and manual testing will exercise the real wiring.
    """
    return {
        "queues": [
            {
                "name": "ingest",
                "queued": 3,
                "started": 10,
                "failed": 1,
            },
            {
                "name": "smtp",
                "queued": 0,
                "started": 25,
                "failed": 0,
            },
        ],
        "workers": [
            {
                "name": "worker-1",
                "queues": ["ingest", "smtp"],
                "state": "busy",
                "last_heartbeat": None,
            }
        ],
        "verification": {
            "total_emails": 42,
            "by_status": {
                "valid": 30,
                "invalid": 10,
                "risky_catch_all": 2,
            },
            "valid_rate": 30 / 42,
        },
        "costs": {
            "smtp_probes": 42,
            "catchall_checks": 5,
            "domains_resolved": 12,
            "pages_crawled": 7,
        },
    }


def _sample_analytics_summary() -> dict:
    """
    Deterministic payload used to isolate the /admin/analytics endpoint
    from real SQLite in this unit test.
    """
    return {
        "verification_time_series": [
            {
                "date": "2025-01-01",
                "total": 10,
                "valid": 7,
                "invalid": 2,
                "risky_catch_all": 1,
                "valid_rate": 0.7,
            },
            {
                "date": "2025-01-02",
                "total": 5,
                "valid": 3,
                "invalid": 1,
                "risky_catch_all": 1,
                "valid_rate": 0.6,
            },
        ],
        "domain_breakdown": [
            {
                "domain": "example.com",
                "total": 12,
                "valid": 9,
                "invalid": 2,
                "risky_catch_all": 1,
                "valid_rate": 0.75,
            },
            {
                "domain": "example.org",
                "total": 3,
                "valid": 1,
                "invalid": 1,
                "risky_catch_all": 1,
                "valid_rate": 1 / 3,
            },
        ],
        "error_breakdown": {
            "mx_4xx": 5,
            "timeout": 2,
        },
    }


def _install_fake_summary(monkeypatch) -> None:
    """
    Monkeypatch src.api.admin.get_admin_summary to avoid hitting real Redis/DB.
    """

    def fake_get_admin_summary() -> dict:
        return _sample_admin_summary()

    monkeypatch.setattr(admin_module, "get_admin_summary", fake_get_admin_summary)


def _install_fake_analytics(monkeypatch) -> None:
    """
    Monkeypatch src.api.admin.get_analytics_summary to avoid hitting real DB.
    """

    def fake_get_analytics_summary(
        window_days: int = 30,
        top_domains: int = 20,
        top_errors: int = 20,
    ) -> dict:
        # Ignore arguments; return a fixed, small payload.
        return _sample_analytics_summary()

    monkeypatch.setattr(
        admin_module,
        "get_analytics_summary",
        fake_get_analytics_summary,
    )


def _install_session_user(monkeypatch, *, is_superuser: bool = True):
    """
    Give admin._require_superuser a session user without a real session cookie.

    Only the session lookup is stubbed; the real _require_superuser gate still
    runs, so these tests exercise the actual authorisation decision.
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


def _install_dev_auth_mode(monkeypatch) -> None:
    """
    Ensure AUTH_MODE is 'dev' regardless of environment.

    The project .env sets AUTH_MODE=session, which causes require_admin
    (router-level dependency) to redirect unauthenticated requests to the
    login page.  In tests we always want the no-auth dev path.
    """
    import src.api.app as app_mod

    monkeypatch.setattr(deps_module, "AUTH_MODE", "dev")
    monkeypatch.setattr(app_mod, "AUTH_MODE", "dev")


def test_admin_metrics_shape(monkeypatch) -> None:
    """
    /admin/metrics returns the expected top-level keys and nested structure.

    This is the primary R24 JSON status surface. It is superuser-only: the
    payload is system-wide and crosses tenants, so an unauthorised caller must
    be refused even when AUTH_MODE is the permissive dev mode.
    """
    _install_fake_summary(monkeypatch)
    _install_dev_auth_mode(monkeypatch)
    client = TestClient(app)

    # No session at all -> refused, dev auth mode notwithstanding.
    assert client.get("/admin/metrics").status_code == 403

    # Signed in, but not a superuser -> still refused.
    _install_session_user(monkeypatch, is_superuser=False)
    assert client.get("/admin/metrics").status_code == 403

    _install_session_user(monkeypatch, is_superuser=True)
    resp = client.get("/admin/metrics")
    assert resp.status_code == 200

    data = resp.json()
    # Top-level keys
    assert "queues" in data
    assert "workers" in data
    assert "verification" in data
    assert "costs" in data

    # Queues
    queues = data["queues"]
    assert isinstance(queues, list)
    assert len(queues) >= 1
    q0 = queues[0]
    assert "name" in q0
    assert "queued" in q0
    assert "started" in q0
    assert "failed" in q0

    # Workers
    workers = data["workers"]
    assert isinstance(workers, list)
    if workers:
        w0 = workers[0]
        assert "name" in w0
        assert "queues" in w0
        assert "state" in w0

    # Verification stats
    verification = data["verification"]
    assert isinstance(verification, dict)
    assert "total_emails" in verification
    assert "by_status" in verification
    assert "valid_rate" in verification
    assert isinstance(verification["by_status"], dict)

    # Cost counters
    costs = data["costs"]
    assert isinstance(costs, dict)
    for key in ("smtp_probes", "catchall_checks", "domains_resolved", "pages_crawled"):
        assert key in costs


def test_admin_analytics_shape(monkeypatch) -> None:
    """
    /admin/analytics returns expected keys and basic nested structure (O17),
    and like /admin/metrics it is superuser-only.
    """
    _install_fake_analytics(monkeypatch)
    _install_dev_auth_mode(monkeypatch)
    client = TestClient(app)
    path = "/admin/analytics?window_days=7&top_domains=5&top_errors=3"

    # No session at all -> refused, dev auth mode notwithstanding.
    assert client.get(path).status_code == 403

    # Signed in, but not a superuser -> still refused.
    _install_session_user(monkeypatch, is_superuser=False)
    assert client.get(path).status_code == 403

    _install_session_user(monkeypatch, is_superuser=True)
    resp = client.get(path)
    assert resp.status_code == 200

    data = resp.json()
    assert "verification_time_series" in data
    assert "domain_breakdown" in data
    assert "error_breakdown" in data

    ts = data["verification_time_series"]
    assert isinstance(ts, list)
    if ts:
        p0 = ts[0]
        for key in ("date", "total", "valid", "invalid", "risky_catch_all", "valid_rate"):
            assert key in p0

    domains = data["domain_breakdown"]
    assert isinstance(domains, list)
    if domains:
        d0 = domains[0]
        for key in ("domain", "total", "valid", "invalid", "risky_catch_all", "valid_rate"):
            assert key in d0

    errors = data["error_breakdown"]
    assert isinstance(errors, dict)


def test_admin_html_page_renders(monkeypatch) -> None:
    """
    /admin/ returns an HTML dashboard shell that references both
    /admin/metrics and /admin/analytics, and is superuser-only.
    """
    _install_fake_summary(monkeypatch)
    _install_fake_analytics(monkeypatch)
    _install_dev_auth_mode(monkeypatch)
    client = TestClient(app)

    # No session at all -> refused, dev auth mode notwithstanding.
    assert client.get("/admin/").status_code == 403

    # Signed in, but not a superuser -> still refused.
    _install_session_user(monkeypatch, is_superuser=False)
    assert client.get("/admin/").status_code == 403

    _install_session_user(monkeypatch, is_superuser=True)
    resp = client.get("/admin/")
    assert resp.status_code == 200
    content_type = resp.headers.get("content-type", "")
    assert content_type.startswith("text/html")
    body = resp.text
    assert "/admin/metrics" in body
    assert "/admin/analytics" in body
