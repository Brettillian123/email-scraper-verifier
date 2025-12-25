# tests/test_r16_smtp.py
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import src.queueing.tasks as qtasks
import src.resolve.mx as mx_mod
import src.verify.smtp as smtp_mod

# Get repo root for schema path
REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = REPO_ROOT / "db" / "schema.sql"

# -----------------------
# Fakes / test primitives
# -----------------------


def _bypass_preflight(monkeypatch):
    """Patch preflight to always return success in smtp module."""

    def _fake_preflight(mx_host):
        return (True, None)  # (success, no error)

    # Try different possible function names
    monkeypatch.setattr(smtp_mod, "_preflight_port25", _fake_preflight, raising=False)
    monkeypatch.setattr(smtp_mod, "preflight_port25", _fake_preflight, raising=False)
    monkeypatch.setattr(smtp_mod, "_tcp25_preflight", _fake_preflight, raising=False)


def _bypass_tcp25_preflight(monkeypatch):
    """Patch TCP25 preflight in tasks module to always return success."""

    def _fake_preflight(mx_host, *, timeout_s=1.5, redis=None, ttl_s=300):
        return {"ok": True, "mx_host": mx_host, "cached": False, "error": None}

    monkeypatch.setattr(qtasks, "_smtp_tcp25_preflight_mx", _fake_preflight, raising=False)


def _bypass_mx_resolution(monkeypatch):
    """Patch MX resolution to return a fake MX host for tests."""

    def _fake_mx_info(domain, force, db_path):
        _ = (domain, force, db_path)
        return ("mx.example.com", None)

    # Patch _mx_info which is what task_probe_email actually uses
    monkeypatch.setattr(qtasks, "_mx_info", _fake_mx_info, raising=False)


class _FakeSMTP:
    """
    Minimal fake smtplib.SMTP that lets us control RCPT responses.
    """

    def __init__(
        self,
        host: str,
        port: int,
        local_hostname: str | None = None,
        timeout: float | None = None,
    ):
        self.host = host
        self.port = port
        self.local_hostname = local_hostname
        self.timeout = timeout
        self.rcpt_code = 250
        self.rcpt_msg = b"OK"
        self.raise_on_rcpt: BaseException | None = None
        self._started_tls = False
        self.mail_from: str | None = None
        self.rcpt_to: str | None = None

    def ehlo(self):
        return (250, b"ehlo-ok")

    def starttls(self):
        self._started_tls = True
        return (220, b"tls-ok")

    def mail(self, sender: str):
        self.mail_from = sender
        return (250, b"mail-from-ok")

    def rcpt(self, recipient: str):
        self.rcpt_to = recipient
        if self.raise_on_rcpt:
            raise self.raise_on_rcpt
        return (self.rcpt_code, self.rcpt_msg)

    def quit(self):
        return (221, b"bye")


def _patch_smtp(
    monkeypatch,
    *,
    code: int = 250,
    msg: bytes = b"OK",
    exc: BaseException | None = None,
):
    """
    Patch smtplib.SMTP inside src.verify.smtp to use _FakeSMTP with desired behavior.
    """

    def _factory(host, port, local_hostname=None, timeout=None):
        fake = _FakeSMTP(host, port, local_hostname, timeout)
        fake.rcpt_code = code
        fake.rcpt_msg = msg
        fake.raise_on_rcpt = exc
        return fake

    monkeypatch.setattr(smtp_mod.smtplib, "SMTP", _factory)


def _capture_behavior_calls(monkeypatch):
    """
    Replace the MX behavior-recording hook with a collector.
    """
    calls: list[dict[str, Any]] = []

    def _rec(*args, **kwargs):
        mx_host = kwargs.get("mx_host")
        code = kwargs.get("code")
        elapsed = kwargs.get("elapsed") or kwargs.get("elapsed_ms") or kwargs.get("latency_ms")
        error_kind = kwargs.get("error_kind")

        if mx_host is None and len(args) >= 2:
            mx_host = args[1]
        if code is None and len(args) >= 3:
            code = args[2]
        if elapsed is None and len(args) >= 4:
            elapsed = args[3]

        calls.append(
            {
                "mx_host": mx_host,
                "code": code,
                "elapsed": elapsed,
                "error_kind": error_kind,
            }
        )
        return None

    candidate_names = [
        "record_mx_probe",
        "record_mx_behavior",
        "note_mx_behavior",
        "update_mx_behavior",
    ]

    for name in candidate_names:
        monkeypatch.setattr(smtp_mod, name, _rec, raising=False)

    for name in candidate_names:
        monkeypatch.setattr(mx_mod, name, _rec, raising=False)

    return calls


# -----------------------
# Tests
# -----------------------


def test_accept_code_maps_to_accept(monkeypatch):
    _patch_smtp(monkeypatch, code=250, msg=b"2.1.5 OK")
    _bypass_preflight(monkeypatch)
    calls = _capture_behavior_calls(monkeypatch)

    res = smtp_mod.probe_rcpt(
        email="alice@example.com",
        mx_host="mx.example.com",
        helo_domain="verifier.test",
        mail_from="bounce@verifier.test",
    )

    assert res["ok"] is True
    assert res["category"] == "accept"
    assert res["code"] == 250
    assert res["mx_host"] == "mx.example.com"
    assert isinstance(res["elapsed_ms"], int)
    assert len(calls) == 1
    assert calls[0]["mx_host"] == "mx.example.com"
    assert calls[0]["code"] == 250
    assert calls[0]["error_kind"] is None


def test_5xx_maps_to_hard_fail(monkeypatch):
    _patch_smtp(monkeypatch, code=550, msg=b"5.1.1 User unknown")
    _bypass_preflight(monkeypatch)
    calls = _capture_behavior_calls(monkeypatch)

    res = smtp_mod.probe_rcpt(
        email="rejected@example.com",
        mx_host="mx.example.com",
        helo_domain="verifier.test",
        mail_from="bounce@verifier.test",
    )

    assert res["ok"] is False
    assert res["category"] == "hard_fail"
    assert res["code"] == 550
    assert len(calls) == 1
    assert calls[0]["code"] == 550


def test_4xx_maps_to_temp_fail(monkeypatch):
    _patch_smtp(monkeypatch, code=450, msg=b"4.2.0 Try again later")
    _bypass_preflight(monkeypatch)
    calls = _capture_behavior_calls(monkeypatch)

    res = smtp_mod.probe_rcpt(
        email="greylisted@example.com",
        mx_host="mx.example.com",
        helo_domain="verifier.test",
        mail_from="bounce@verifier.test",
    )

    assert res["ok"] is False
    assert res["category"] == "temp_fail"
    assert res["code"] == 450
    assert len(calls) == 1
    assert calls[0]["code"] == 450


def test_exception_maps_to_unknown_and_sets_error(monkeypatch):
    _patch_smtp(monkeypatch, exc=TimeoutError("socket timeout"))
    _bypass_preflight(monkeypatch)
    _capture_behavior_calls(monkeypatch)

    res = smtp_mod.probe_rcpt(
        email="timeout@example.com",
        mx_host="mx.example.com",
        helo_domain="verifier.test",
        mail_from="bounce@verifier.test",
    )

    assert res["ok"] is False
    assert res["category"] == "unknown"
    assert res["error"] is not None
    assert "timeout" in res["error"].lower()


def test_task_probe_email_returns_expected_shape(tmp_path, monkeypatch):
    from src.queueing.tasks import task_probe_email

    db_path = tmp_path / "test.db"
    with open(SCHEMA_PATH, encoding="utf-8") as f:
        con = sqlite3.connect(db_path)
        con.executescript(f.read())
        con.close()

    import src.queueing.tasks as tasks_mod

    def _test_conn():
        return sqlite3.connect(str(db_path))

    monkeypatch.setattr(tasks_mod, "_conn", _test_conn)

    # Seed
    with sqlite3.connect(str(db_path)) as con:
        cur = con.execute(
            "INSERT INTO companies(name,domain) VALUES (?,?)",
            ("Test Co", "example.com"),
        )
        company_id = cur.lastrowid
        cur = con.execute(
            "INSERT INTO emails(company_id,email) VALUES (?,?)",
            (company_id, "test@example.com"),
        )
        email_id = cur.lastrowid

        # Add domain_resolutions row with NOT catch-all status
        con.execute(
            """
            INSERT INTO domain_resolutions (
                company_id, company_name, chosen_domain, method, confidence,
                reason, resolver_version, catch_all_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                company_id,
                "Test Co",
                "example.com",
                "test",
                100,
                "test domain",
                "test",
                "not_catch_all",
            ),
        )

    _patch_smtp(monkeypatch, code=250, msg=b"OK")
    _bypass_preflight(monkeypatch)
    _bypass_tcp25_preflight(monkeypatch)
    _bypass_mx_resolution(monkeypatch)
    _capture_behavior_calls(monkeypatch)

    ret = task_probe_email(email_id, "test@example.com", "example.com")

    assert ret["email"] == "test@example.com"
    assert ret["ok"] is True
    assert ret["category"] == "accept"


def test_task_probe_email_handles_bad_input(monkeypatch):
    from src.queueing.tasks import task_probe_email

    _bypass_preflight(monkeypatch)
    _bypass_tcp25_preflight(monkeypatch)
    _bypass_mx_resolution(monkeypatch)

    # Test with empty/invalid email
    ret = task_probe_email(-1, "", "example.com")

    # Should return error for bad input
    assert ret["ok"] is False


def test_task_probe_email_propagates_probe_error_as_unknown(tmp_path, monkeypatch):
    from src.queueing.tasks import task_probe_email

    db_path = tmp_path / "test.db"
    with open(SCHEMA_PATH, encoding="utf-8") as f:
        con = sqlite3.connect(db_path)
        con.executescript(f.read())
        con.close()

    import src.queueing.tasks as tasks_mod

    def _test_conn():
        return sqlite3.connect(str(db_path))

    monkeypatch.setattr(tasks_mod, "_conn", _test_conn)

    with sqlite3.connect(str(db_path)) as con:
        cur = con.execute(
            "INSERT INTO companies(name,domain) VALUES (?,?)",
            ("Test Co", "example.com"),
        )
        company_id = cur.lastrowid
        cur = con.execute(
            "INSERT INTO emails(company_id,email) VALUES (?,?)",
            (company_id, "error@example.com"),
        )
        email_id = cur.lastrowid

        con.execute(
            """
            INSERT INTO domain_resolutions (
                company_id, company_name, chosen_domain, method, confidence,
                reason, resolver_version, catch_all_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                company_id,
                "Test Co",
                "example.com",
                "test",
                100,
                "test domain",
                "test",
                "not_catch_all",
            ),
        )

    _patch_smtp(monkeypatch, exc=RuntimeError("network boom"))
    _bypass_preflight(monkeypatch)
    _bypass_tcp25_preflight(monkeypatch)
    _bypass_mx_resolution(monkeypatch)
    _capture_behavior_calls(monkeypatch)

    ret = task_probe_email(email_id, "error@example.com", "example.com")
    assert ret["ok"] is False
    assert ret["category"] == "unknown"
    assert ret["error"] is not None
