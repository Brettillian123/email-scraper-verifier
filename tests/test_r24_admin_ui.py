# tests/test_r24_admin_ui.py
from __future__ import annotations

from fastapi.testclient import TestClient

import src.api.admin as admin_module
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


def test_admin_metrics_shape(monkeypatch) -> None:
    """
    /admin/metrics returns the expected top-level keys and nested structure.

    This is the primary R24 JSON status surface.
    """
    _install_fake_summary(monkeypatch)
    client = TestClient(app)

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
    /admin/analytics returns expected keys and basic nested structure (O17).
    """
    _install_fake_analytics(monkeypatch)
    client = TestClient(app)

    resp = client.get("/admin/analytics?window_days=7&top_domains=5&top_errors=3")
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
    /admin/metrics and /admin/analytics.
    """
    _install_fake_summary(monkeypatch)
    _install_fake_analytics(monkeypatch)
    client = TestClient(app)

    resp = client.get("/admin/")
    assert resp.status_code == 200
    content_type = resp.headers.get("content-type", "")
    assert content_type.startswith("text/html")
    body = resp.text
    assert "Email Scraper – Admin" in body
    assert "/admin/metrics" in body
    assert "/admin/analytics" in body
