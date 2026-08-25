"""The deferred requeue that replaced the inline sleep, and the batched session.

Production spent 64,351 probes to store 3,188 results. 20,443 of those probes
(32%) were temp_fails abandoned after sleeping 1s and then 2s, while greylisting
needs 300s+ -- so the sleep could only ever burn worker time. These tests pin
the replacement: classify, requeue, walk on.

tests/test_greylist_deferral_gate.py guards the *gate* (only a real 4xx may
enter the schedule). This file guards what happens once it does.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from src.queueing import tasks


def _reply(category, code, message, *, attempted=True):
    return {
        "ok": category == "accept",
        "category": category,
        "code": code,
        "message": message,
        "mx_host": "mx.acme.com",
        "helo_domain": "verifier.example",
        "elapsed_ms": 3,
        "error": None if category == "accept" else f"smtp_response:{code}",
        "attempted": attempted,
        "defer_seconds": None,
    }


@pytest.fixture
def sessions(monkeypatch):
    """Record each probe_rcpt_batch call; one call == one SMTP session."""
    calls: list[list[str]] = []
    table: dict[str, dict] = {}

    def fake_batch(emails, mx_host, **kw):
        calls.append(list(emails))
        return [table.get(e, _reply("hard_fail", 550, "5.1.1 no such user")) for e in emails]

    monkeypatch.setattr(tasks, "probe_rcpt_batch", fake_batch)
    return calls, table


def test_server_4xx_becomes_deferred_not_temp_fail(sessions):
    calls, table = sessions
    table["ada@acme.com"] = _reply("temp_fail", 450, "4.7.1 greylisted, try again later")

    (res,) = tasks._probe_candidates(["ada@acme.com"], "mx.acme.com", "not_catch_all")

    assert res["status"] == "deferred"
    assert res["reason"] == "greylist_deferred"


def test_connection_level_temp_fail_is_not_deferred(sessions):
    """A timeout has no code. Waiting 15 minutes for a dead socket buys nothing."""
    calls, table = sessions
    table["ada@acme.com"] = _reply("temp_fail", None, "")

    (res,) = tasks._probe_candidates(["ada@acme.com"], "mx.acme.com", "not_catch_all")

    assert res["status"] == "temp_fail"


def test_one_session_carries_the_whole_probe_plan(sessions):
    """Lane A's multi-RCPT session: three candidates, one connection."""
    calls, table = sessions
    plan = ["alovelace@acme.com", "ada.lovelace@acme.com", "ada@acme.com"]
    table[plan[0]] = _reply("accept", 250, "2.1.5 OK")

    results = tasks._probe_candidates(plan, "mx.acme.com", None)

    assert calls == [plan], "each probe_rcpt_batch call is one SMTP session"
    assert [r["status"] for r in results] == ["valid", "invalid", "invalid"]


def test_accept_beside_a_hard_fail_rules_out_catch_all(sessions):
    """The same session rejected one plausible local-part and accepted another.

    That is exactly what the separate catch-all sanity connection measured, so
    the accept grades `valid` even though the domain's status was unknown --
    and the caller can skip that extra connection.
    """
    calls, table = sessions
    plan = ["alovelace@acme.com", "ada.lovelace@acme.com"]
    table[plan[0]] = _reply("accept", 250, "2.1.5 OK")

    results = tasks._probe_candidates(plan, "mx.acme.com", None)

    assert results[0]["status"] == "valid"
    assert all(r["catchall_ruled_out"] for r in results)


def test_a_lone_accept_stays_unconfirmed(sessions):
    """Nothing was rejected, so nothing was proved. Do not invent a verdict."""
    calls, table = sessions
    table["ada@acme.com"] = _reply("accept", 250, "2.1.5 OK")

    (res,) = tasks._probe_candidates(["ada@acme.com"], "mx.acme.com", None)

    assert res["status"] == "risky_catch_all"
    assert res["catchall_ruled_out"] is False


class _RecordingQueue:
    scheduled: list[tuple[str, timedelta, dict]] = []

    def __init__(self, name=None, connection=None):
        self.name = name

    def enqueue_in(self, delta, func, **kwargs):
        _RecordingQueue.scheduled.append((self.name, delta, kwargs))
        return object()


@pytest.fixture
def scheduled(monkeypatch):
    _RecordingQueue.scheduled = []
    monkeypatch.setattr(tasks, "Queue", _RecordingQueue)
    monkeypatch.setattr(tasks, "get_redis", lambda: object())
    monkeypatch.setattr(tasks, "_persist_probe_result_r18", lambda **kw: (None,) * 5)
    return _RecordingQueue.scheduled


def test_requeue_walks_the_configured_schedule(scheduled):
    schedule = tasks._cfg.retry_timeout.greylist_retry_schedule
    assert schedule == [60, 300, 900], "revived from config._parse_intervals' defaults"

    for attempt, expected in enumerate(schedule):
        assert tasks._maybe_defer_greylist_retry(
            email_id=7,
            email="ada@acme.com",
            domain="acme.com",
            mx_host="mx.acme.com",
            code=450,
            message="4.7.1 greylisted",
            defer_attempt=attempt,
        )
        queue, delta, kwargs = scheduled[-1]
        assert queue == "verify"
        assert delta == timedelta(seconds=expected)
        assert kwargs["defer_attempt"] == attempt + 1

    # Budget spent: the caller must now persist whatever it has.
    assert not tasks._maybe_defer_greylist_retry(
        email_id=7,
        email="ada@acme.com",
        domain="acme.com",
        mx_host="mx.acme.com",
        code=450,
        message="4.7.1 greylisted",
        defer_attempt=len(schedule),
    )


def test_a_failed_enqueue_is_reported_as_not_scheduled(monkeypatch, scheduled):
    """Redis down must not leave the address believing a retry is coming."""

    def _boom(*a, **k):
        raise RuntimeError("redis unavailable")

    monkeypatch.setattr(tasks, "get_redis", _boom)

    assert not tasks._maybe_defer_greylist_retry(
        email_id=7,
        email="ada@acme.com",
        domain="acme.com",
        mx_host="mx.acme.com",
        code=450,
        message="4.7.1 greylisted",
        defer_attempt=0,
    )
