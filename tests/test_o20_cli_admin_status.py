# tests/test_o20_cli_admin_status.py
from __future__ import annotations

import json

import src.cli as cli_module
from src.cli import main as cli_main


def _sample_admin_summary() -> dict:
    """
    Deterministic admin summary used to isolate the CLI from real Redis/DB.
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


def _sample_analytics() -> dict:
    """
    Deterministic analytics summary used to isolate the CLI from real SQLite.
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
            }
        ],
        "domain_breakdown": [
            {
                "domain": "example.com",
                "total": 12,
                "valid": 9,
                "invalid": 2,
                "risky_catch_all": 1,
                "valid_rate": 0.75,
            }
        ],
        "error_breakdown": {
            "mx_4xx": 5,
            "timeout": 2,
        },
    }


def _install_fake_admin(monkeypatch) -> None:
    """
    Monkeypatch src.cli.get_admin_summary / get_analytics_summary so the CLI
    does not touch real infrastructure in tests.
    """

    def fake_get_admin_summary() -> dict:
        return _sample_admin_summary()

    def fake_get_analytics_summary(
        window_days: int = 30,
        top_domains: int = 20,
        top_errors: int = 20,
    ) -> dict:
        # Ignore args; return fixed payload.
        return _sample_analytics()

    monkeypatch.setattr(cli_module, "get_admin_summary", fake_get_admin_summary)
    monkeypatch.setattr(
        cli_module,
        "get_analytics_summary",
        fake_get_analytics_summary,
    )


def test_admin_status_human(monkeypatch, capsys) -> None:
    """
    `email-scraper admin status` prints a human-readable summary and exits 0.
    """
    _install_fake_admin(monkeypatch)

    rc = cli_main(["admin", "status", "--window-days", "7", "--top-domains", "5"])
    assert rc == 0

    captured = capsys.readouterr()
    stdout = captured.out

    # Basic headers from the human renderer
    assert "=== Queues ===" in stdout
    assert "Total queued:" in stdout
    assert "=== Workers ===" in stdout
    assert "=== Verification summary ===" in stdout
    assert "=== Cost proxies ===" in stdout
    assert "=== Verification time series ===" in stdout
    assert "=== Top domains ===" in stdout
    assert "=== Top errors ===" in stdout

    # A couple of specific values from the fake payload
    assert "ingest" in stdout
    assert "smtp" in stdout
    assert "example.com" in stdout
    assert "mx_4xx" in stdout


def test_admin_status_json(monkeypatch, capsys) -> None:
    """
    `email-scraper admin status --json` emits combined JSON with summary and
    analytics sections.
    """
    _install_fake_admin(monkeypatch)

    rc = cli_main(["admin", "status", "--json"])
    assert rc == 0

    captured = capsys.readouterr()
    stdout = captured.out

    payload = json.loads(stdout)
    assert "summary" in payload
    assert "analytics" in payload

    summary = payload["summary"]
    analytics = payload["analytics"]

    # Summary shape
    assert "queues" in summary
    assert "workers" in summary
    assert "verification" in summary
    assert "costs" in summary

    # Analytics shape
    assert "verification_time_series" in analytics
    assert "domain_breakdown" in analytics
    assert "error_breakdown" in analytics
