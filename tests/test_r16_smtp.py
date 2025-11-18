# tests/test_r16_smtp.py
from __future__ import annotations

from typing import Any

import src.resolve.mx as mx_mod

# Unit-under-test modules
import src.verify.smtp as smtp_mod
from src.queueing import tasks as qtasks

# -----------------------
# Fakes / test primitives
# -----------------------


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
        # Default scripted behavior can be overridden per test
        self.rcpt_code = 250
        self.rcpt_msg = b"OK"
        self.raise_on_rcpt: BaseException | None = None
        self._started_tls = False
        self.mail_from: str | None = None
        self.rcpt_to: str | None = None

    # SMTP handshake bits used by our implementation
    def ehlo(self):
        return (250, b"ehlo-ok")

    def starttls(self):
        # Our implementation may or may not call this; support both ways
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


class _FakeRedis:
    def ping(self):  # used only as a liveness check
        return True


# -----------------------
# smtp.probe_rcpt mapping
# -----------------------


def _patch_smtp(
    monkeypatch,
    *,
    code: int = 250,
    msg: bytes = b"OK",
    exc: BaseException | None = None,
):
    """
    Patch smtplib.SMTP inside src.verify.smtp to use _FakeSMTP with desired behavior.
    Returns the fake class so the test can inspect attributes.
    """

    def _factory(host, port, local_hostname=None, timeout=None):
        fake = _FakeSMTP(host, port, local_hostname, timeout)
        fake.rcpt_code = code
        fake.rcpt_msg = msg
        fake.raise_on_rcpt = exc
        return fake

    # Patch constructor to return a fresh fake instance each call
    monkeypatch.setattr(smtp_mod.smtplib, "SMTP", _factory)


def _capture_behavior_calls(monkeypatch):
    """
    Replace the MX behavior-recording hook with a collector.

    This is tolerant of implementation changes:
      - It patches several likely hook names on both smtp_mod and mx_mod.
      - It accepts arbitrary positional/keyword arguments and tries to
        extract mx_host, code, elapsed, and error_kind in a best-effort way.
    """
    calls: list[dict[str, Any]] = []

    def _rec(*args, **kwargs):
        # Prefer explicit keyword args if present
        mx_host = kwargs.get("mx_host")
        code = kwargs.get("code")
        elapsed = kwargs.get("elapsed") or kwargs.get("elapsed_ms") or kwargs.get("latency_ms")
        error_kind = kwargs.get("error_kind")

        # Fallback for common positional calling pattern:
        #   (domain, mx_host, code, elapsed, ...)
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

        # We intentionally do not call the real implementation; for unit
        # tests we only care that the hook is invoked, not that it writes
        # to domain_resolutions or similar.
        return None

    # Candidate hook names (covering R15/R16/O06 variants)
    candidate_names = [
        "record_mx_probe",
        "record_mx_behavior",
        "note_mx_behavior",
        "update_mx_behavior",
    ]

    # Ensure smtp_mod has these attributes patched; if they didn't exist,
    # raising=False will create them so calls via smtp_mod.<name>(...) are
    # still intercepted.
    for name in candidate_names:
        monkeypatch.setattr(smtp_mod, name, _rec, raising=False)

    # Also patch the MX module, but only for attributes that already exist
    # there; this lets us intercept calls like src.resolve.mx.record_mx_behavior(...)
    for name in candidate_names:
        if hasattr(mx_mod, name):
            monkeypatch.setattr(mx_mod, name, _rec, raising=False)

    return calls


def test_accept_code_maps_to_accept(monkeypatch):
    _patch_smtp(monkeypatch, code=250, msg=b"2.1.5 OK")
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
    # behavior cache hook was invoked
    assert len(calls) == 1
    assert calls[0]["mx_host"] == "mx.example.com"
    assert calls[0]["code"] == 250
    assert calls[0]["error_kind"] is None


def test_5xx_maps_to_hard_fail(monkeypatch):
    _patch_smtp(monkeypatch, code=550, msg=b"5.1.1 User unknown")
    _capture_behavior_calls(monkeypatch)

    res = smtp_mod.probe_rcpt(
        email="bob@example.com",
        mx_host="mx.example.com",
        helo_domain="verifier.test",
        mail_from="bounce@verifier.test",
    )

    assert res["ok"] is True
    assert res["category"] == "hard_fail"
    assert res["code"] == 550


def test_4xx_maps_to_temp_fail(monkeypatch):
    _patch_smtp(monkeypatch, code=450, msg=b"4.2.0 Try again later")
    _capture_behavior_calls(monkeypatch)

    res = smtp_mod.probe_rcpt(
        email="carol@example.com",
        mx_host="mx.example.com",
        helo_domain="verifier.test",
        mail_from="bounce@verifier.test",
    )

    assert res["ok"] is True
    assert res["category"] == "temp_fail"
    assert res["code"] == 450


def test_exception_maps_to_unknown_and_sets_error(monkeypatch):
    _patch_smtp(monkeypatch, exc=TimeoutError("socket timeout"))
    calls = _capture_behavior_calls(monkeypatch)

    res = smtp_mod.probe_rcpt(
        email="dave@example.com",
        mx_host="mx.example.com",
        helo_domain="verifier.test",
        mail_from="bounce@verifier.test",
    )

    assert res["ok"] is False
    assert res["category"] == "unknown"  # our impl classifies socket errors as unknown
    assert res["code"] is None
    assert isinstance(res["error"], str)
    # behavior cache still recorded with error_kind
    assert len(calls) == 1
    assert calls[0]["error_kind"] is not None


# -----------------------
# queue task: task_probe_email
# -----------------------


def test_task_probe_email_returns_expected_shape(monkeypatch):
    """
    Ensure the queue task resolves MX, applies throttling hooks, calls probe_rcpt,
    and returns the expected keys without raising.
    """
    # Force _mx_info to return a stable (mx_host, behavior_hint)
    monkeypatch.setattr(qtasks, "_mx_info", lambda d, force, db_path: ("mx.test", {"hint": "ok"}))

    # Bypass all rate-limiters
    monkeypatch.setattr(qtasks, "try_acquire", lambda redis, key, cap: True)
    monkeypatch.setattr(qtasks, "can_consume_rps", lambda *a, **k: True)
    monkeypatch.setattr(qtasks, "release", lambda *a, **k: None)

    # Fake Redis conn that pings
    monkeypatch.setattr(qtasks, "get_redis", lambda: _FakeRedis())

    # Stub out the core probe to a deterministic value
    def _fake_probe(email, mx_host, **kw):
        return {
            "ok": True,
            "category": "accept",
            "code": 250,
            "message": "OK",
            "mx_host": mx_host,
            "helo_domain": kw.get("helo_domain", "verifier.test"),
            "elapsed_ms": 12,
            "error": None,
        }

    monkeypatch.setattr(qtasks, "probe_rcpt", _fake_probe)

    res = qtasks.task_probe_email(
        email_id=42,
        email="eve@example.com",
        domain="example.com",
        force=False,
    )

    # Shape & content
    assert res["ok"] is True
    assert res["category"] == "accept"
    assert res["code"] == 250
    assert res["mx_host"] == "mx.test"
    assert res["domain"] == "example.com"
    assert res["email_id"] == 42
    assert res["email"] == "eve@example.com"
    assert isinstance(res["elapsed_ms"], int)
    assert res["error"] is None


def test_task_probe_email_handles_bad_input(monkeypatch):
    res = qtasks.task_probe_email(email_id=1, email="not-an-email", domain="", force=False)
    assert res["ok"] is False
    assert res["category"] == "unknown"
    assert res["code"] is None
    assert res["mx_host"] is None


def test_task_probe_email_propagates_probe_error_as_unknown(monkeypatch):
    # Resolve ok
    monkeypatch.setattr(qtasks, "_mx_info", lambda d, force, db_path: ("mx.test", None))
    # Throttling passes
    monkeypatch.setattr(qtasks, "try_acquire", lambda redis, key, cap: True)
    monkeypatch.setattr(qtasks, "can_consume_rps", lambda *a, **k: True)
    monkeypatch.setattr(qtasks, "release", lambda *a, **k: None)
    monkeypatch.setattr(qtasks, "get_redis", lambda: _FakeRedis())

    # Make the probe raise an exception
    def _boom(*a, **kw):
        raise RuntimeError("network exploded")

    monkeypatch.setattr(qtasks, "probe_rcpt", _boom)

    res = qtasks.task_probe_email(
        email_id=7,
        email="zoe@example.com",
        domain="example.com",
        force=False,
    )
    assert res["ok"] is False
    assert res["category"] == "unknown"
    assert res["code"] is None
    assert res["mx_host"] == "mx.test"
    assert "RuntimeError" in res["error"]
