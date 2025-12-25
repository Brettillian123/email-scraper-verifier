from __future__ import annotations

import sqlite3
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest

import src.verify.catchall as catchall_mod
from src.queueing import tasks as qtasks

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def memory_db(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[sqlite3.Connection, Callable[..., None]]:
    """
    In-memory domain_resolutions table wired into src.verify.catchall.get_connection().
    Minimal schema that matches what catchall.py expects.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE domain_resolutions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER,
            company_name TEXT,
            chosen_domain TEXT,
            domain TEXT,
            lowest_mx TEXT,
            resolved_at TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            catch_all_status TEXT,
            catch_all_checked_at TEXT,
            catch_all_localpart TEXT,
            catch_all_smtp_code INTEGER,
            catch_all_smtp_msg TEXT
        )
        """
    )

    def seed(domain: str, lowest_mx: str | None = "mx.test.local") -> None:
        conn.execute(
            """
            INSERT INTO domain_resolutions (domain, chosen_domain, lowest_mx, resolved_at)
            VALUES (?, ?, ?, ?)
            """,
            (domain, domain, lowest_mx, "2025-01-01T00:00:00Z"),
        )
        conn.commit()

    # All catchall operations talk to this connection
    monkeypatch.setattr(catchall_mod, "get_connection", lambda: conn)
    return conn, seed


# ---------------------------------------------------------------------------
# Happy-path classification tests
# ---------------------------------------------------------------------------


def _stub_mx(monkeypatch: pytest.MonkeyPatch, domain: str, mx_host: str | None) -> None:
    """
    Patch get_or_resolve_mx() to return a simple dict with lowest_mx/failure.
    """

    def fake_get_or_resolve(d: str, *_, **__) -> dict[str, Any]:
        assert d == domain
        if mx_host is None:
            return {"failure": "no_mx"}
        return {"lowest_mx": mx_host, "failure": None}

    monkeypatch.setattr(catchall_mod, "get_or_resolve_mx", fake_get_or_resolve)


def test_catchall_probe_250_yields_catch_all(memory_db, monkeypatch: pytest.MonkeyPatch) -> None:
    conn, seed = memory_db
    domain = "example.com"
    mx_host = "mx.test.local"
    seed(domain, mx_host)
    _stub_mx(monkeypatch, domain, mx_host)

    seen: dict[str, Any] = {}

    def fake_probe(mx: str, dom: str, localpart: str):
        seen["mx"] = mx
        seen["domain"] = dom
        seen["localpart"] = localpart
        return 250, b"OK", 7.5, None

    monkeypatch.setattr(catchall_mod, "_smtp_probe_random_address", fake_probe)

    res = catchall_mod.check_catchall_for_domain(domain)

    assert res.domain == domain
    assert res.status == "catch_all"
    assert res.cached is False
    assert res.mx_host == mx_host
    assert res.rcpt_code == 250
    assert res.localpart is not None
    assert seen["mx"] == mx_host
    assert seen["domain"] == domain
    assert seen["localpart"] == res.localpart

    row = conn.execute(
        """
        SELECT catch_all_status, catch_all_checked_at, catch_all_localpart, catch_all_smtp_code
          FROM domain_resolutions
         WHERE domain = ?
        """,
        (domain,),
    ).fetchone()
    assert row is not None
    assert row["catch_all_status"] == "catch_all"
    assert row["catch_all_checked_at"] is not None
    assert row["catch_all_localpart"] == res.localpart
    assert row["catch_all_smtp_code"] == 250


def test_catchall_probe_550_yields_not_catch_all(
    memory_db,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn, seed = memory_db
    domain = "example.net"
    mx_host = "mx.test.local"
    seed(domain, mx_host)
    _stub_mx(monkeypatch, domain, mx_host)

    def fake_probe(mx: str, dom: str, localpart: str):
        return 550, b"user unknown", 10.0, None

    monkeypatch.setattr(catchall_mod, "_smtp_probe_random_address", fake_probe)

    res = catchall_mod.check_catchall_for_domain(domain)

    assert res.status == "not_catch_all"
    assert res.cached is False
    assert res.rcpt_code == 550

    row = conn.execute(
        """
        SELECT catch_all_status, catch_all_smtp_code
          FROM domain_resolutions
         WHERE domain = ?
        """,
        (domain,),
    ).fetchone()
    assert row is not None
    assert row["catch_all_status"] == "not_catch_all"
    assert row["catch_all_smtp_code"] == 550


def test_catchall_probe_450_yields_tempfail(memory_db, monkeypatch: pytest.MonkeyPatch) -> None:
    conn, seed = memory_db
    domain = "tempfail.test"
    mx_host = "mx.temp.local"
    seed(domain, mx_host)
    _stub_mx(monkeypatch, domain, mx_host)

    def fake_probe(mx: str, dom: str, localpart: str):
        return 450, b"try again later", 20.0, "temporary failure"

    monkeypatch.setattr(catchall_mod, "_smtp_probe_random_address", fake_probe)

    res = catchall_mod.check_catchall_for_domain(domain)

    assert res.status == "tempfail"
    assert res.rcpt_code == 450
    assert res.error is not None

    row = conn.execute(
        """
        SELECT catch_all_status, catch_all_smtp_code
          FROM domain_resolutions
         WHERE domain = ?
        """,
        (domain,),
    ).fetchone()
    assert row is not None
    assert row["catch_all_status"] == "tempfail"
    assert row["catch_all_smtp_code"] == 450


def test_catchall_timeout_without_code_yields_tempfail(
    memory_db,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn, seed = memory_db
    domain = "timeout.test"
    mx_host = "mx.timeout.local"
    seed(domain, mx_host)
    _stub_mx(monkeypatch, domain, mx_host)

    def fake_probe(mx: str, dom: str, localpart: str):
        return None, None, 5000.0, "timeout"

    monkeypatch.setattr(catchall_mod, "_smtp_probe_random_address", fake_probe)

    res = catchall_mod.check_catchall_for_domain(domain)

    assert res.status == "tempfail"
    assert res.rcpt_code is None
    assert res.error is not None

    row = conn.execute(
        "SELECT catch_all_status FROM domain_resolutions WHERE domain = ?",
        (domain,),
    ).fetchone()
    assert row is not None
    assert row["catch_all_status"] == "tempfail"


def test_catchall_no_mx_yields_no_mx_and_skips_smtp(
    memory_db,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn, seed = memory_db
    domain = "nomx.test"
    # Seed a row so UPDATE has something to touch, even if MX resolution fails
    seed(domain, lowest_mx=None)
    _stub_mx(monkeypatch, domain, mx_host=None)

    called = {"probe": False}

    def fake_probe(*args, **kwargs):
        _ = (args, kwargs)
        called["probe"] = True
        return 250, b"OK", 1.0, None

    # If SMTP probe is called, the test should fail
    monkeypatch.setattr(catchall_mod, "_smtp_probe_random_address", fake_probe)

    res = catchall_mod.check_catchall_for_domain(domain)

    assert called["probe"] is False
    assert res.status == "no_mx"
    assert res.mx_host is None
    assert res.rcpt_code is None

    row = conn.execute(
        """
        SELECT catch_all_status, catch_all_smtp_code
          FROM domain_resolutions
         WHERE domain = ?
        """,
        (domain,),
    ).fetchone()
    assert row is not None
    assert row["catch_all_status"] == "no_mx"
    assert row["catch_all_smtp_code"] is None


# ---------------------------------------------------------------------------
# Caching behaviour tests
# ---------------------------------------------------------------------------


def test_catchall_uses_cached_verdict_within_ttl(
    memory_db,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn, seed = memory_db
    domain = "cached.test"
    mx_host = "mx.cached.local"
    seed(domain, mx_host)

    gomx_calls = {"count": 0}

    def fake_get_or_resolve(d: str, *_, **__):
        gomx_calls["count"] += 1
        assert d == domain
        return {"lowest_mx": mx_host, "failure": None}

    monkeypatch.setattr(catchall_mod, "get_or_resolve_mx", fake_get_or_resolve)

    probe_calls = {"count": 0}

    def fake_probe(mx: str, dom: str, localpart: str):
        probe_calls["count"] += 1
        return 550, b"user unknown", 15.0, None

    monkeypatch.setattr(catchall_mod, "_smtp_probe_random_address", fake_probe)

    # First call → fresh probe, populates cache
    res1 = catchall_mod.check_catchall_for_domain(domain)
    assert res1.status == "not_catch_all"
    assert res1.cached is False

    # Second call within TTL → should be cached (no new MX/probe)
    res2 = catchall_mod.check_catchall_for_domain(domain)
    assert res2.status == "not_catch_all"
    assert res2.cached is True
    assert res2.rcpt_code == 550

    # MX resolution and SMTP probe should have run once total.
    assert gomx_calls["count"] == 1
    assert probe_calls["count"] == 1

    row = conn.execute(
        "SELECT catch_all_status, catch_all_checked_at FROM domain_resolutions WHERE domain = ?",
        (domain,),
    ).fetchone()
    assert row is not None
    assert row["catch_all_status"] == "not_catch_all"
    assert row["catch_all_checked_at"] is not None


def test_catchall_force_bypasses_cache(memory_db, monkeypatch: pytest.MonkeyPatch) -> None:
    conn, seed = memory_db
    domain = "forcecached.test"
    mx_host = "mx.force.local"
    seed(domain, mx_host)

    gomx_calls = {"count": 0}

    def fake_get_or_resolve(d: str, *_, **__):
        gomx_calls["count"] += 1
        assert d == domain
        return {"lowest_mx": mx_host, "failure": None}

    monkeypatch.setattr(catchall_mod, "get_or_resolve_mx", fake_get_or_resolve)

    probe_calls = {"count": 0}

    def fake_probe(mx: str, dom: str, localpart: str):
        probe_calls["count"] += 1
        code = 550 if probe_calls["count"] == 1 else 250
        return code, b"mock", 5.0, None

    monkeypatch.setattr(catchall_mod, "_smtp_probe_random_address", fake_probe)

    # First call: populate cache (not_catch_all)
    res1 = catchall_mod.check_catchall_for_domain(domain)
    assert res1.status == "not_catch_all"
    assert res1.cached is False
    assert res1.rcpt_code == 550

    # Second call with force=True: must bypass cache and probe again
    res2 = catchall_mod.check_catchall_for_domain(domain, force=True)
    assert res2.cached is False
    assert res2.rcpt_code == 250

    assert gomx_calls["count"] == 2
    assert probe_calls["count"] == 2

    row = conn.execute(
        "SELECT catch_all_status, catch_all_smtp_code FROM domain_resolutions WHERE domain = ?",
        (domain,),
    ).fetchone()
    assert row is not None
    # Last write should reflect the forced probe
    assert row["catch_all_smtp_code"] == 250


# ---------------------------------------------------------------------------
# Task wiring tests (task_check_catchall)
# ---------------------------------------------------------------------------


def test_task_check_catchall_success_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    # Patch the TCP25 preflight to succeed
    def fake_preflight(mx_host, *, timeout_s=1.5, redis=None, ttl_s=300):
        return {"ok": True, "mx_host": mx_host, "cached": False, "error": None}

    monkeypatch.setattr(qtasks, "_smtp_tcp25_preflight_mx", fake_preflight)

    def fake_check(domain: str, *, force: bool = False):
        assert domain == "example.com"
        assert force is False
        return SimpleNamespace(
            domain="example.com",
            status="catch_all",
            mx_host="mx.task.local",
            rcpt_code=250,
            cached=False,
            localpart="_ca_test",
            elapsed_ms=12.5,
            error=None,
        )

    monkeypatch.setattr(qtasks, "check_catchall_for_domain", fake_check)

    fn = qtasks.task_check_catchall.__wrapped__  # type: ignore[attr-defined]
    out = fn("example.com", force=False)

    assert out["ok"] is True
    assert out["domain"] == "example.com"
    assert out["status"] == "catch_all"
    assert out["rcpt_code"] == 250
    assert out["cached"] is False
    assert out["mx_host"] == "mx.task.local"
    assert isinstance(out["elapsed_ms"], int)


def test_task_check_catchall_empty_domain_rejected() -> None:
    fn = qtasks.task_check_catchall.__wrapped__  # type: ignore[attr-defined]
    out = fn("   ", force=False)
    assert out["ok"] is False
    assert out["error"] == "empty_domain"
