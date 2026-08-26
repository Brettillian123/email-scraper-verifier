"""Two guards added after the 2026-08-25 outage.

A backfill issued ~8,700 RCPT probes in one run, ~6,400 of them at a single
provider. Every individual second was inside the RPS budget, so nothing objected.
The provider began throttling (921 x 450) and the HOST blocked outbound port 25
outright -- 674 probes recorded `port25_unreachable`. Verification stopped for a
day and needed the provider to lift the block by hand.

In the same run, confirmed addresses fell 1,847 -> 1,564 and companies holding at
least one valid address fell 355 -> 286, because `CLEANUP_INVALID_GENERATED`
deletes an address the moment a probe returns `invalid` -- and a throttling
provider rejects rather than defers, which is indistinguishable from "no such
user" at the SMTP layer. How much of that loss was genuine churn cannot now be
established, because the rows were deleted.

Hence:
  1. a rolling daily ceiling on probe VOLUME, global and per MX host, and
  2. an address that has ever verified valid is never deleted by a later 550.
"""

from __future__ import annotations

import pytest

from src.queueing import rate_limit, tasks

pytestmark = pytest.mark.no_db


class _FakeRedis:
    """Minimal INCRBY/EXPIRE/GET surface, which is all the budget uses."""

    def __init__(self) -> None:
        self.store: dict[str, int] = {}

    def incrby(self, key, n):
        self.store[key] = self.store.get(key, 0) + int(n)
        return self.store[key]

    def expire(self, key, ttl):
        return True

    def get(self, key):
        return self.store.get(key)


class _ExplodingRedis:
    def incrby(self, *a, **k):
        raise RuntimeError("redis is down")

    def expire(self, *a, **k):
        raise RuntimeError("redis is down")

    def get(self, *a, **k):
        raise RuntimeError("redis is down")


# --------------------------------------------------------------------------
# 1. Daily volume ceiling
# --------------------------------------------------------------------------


def test_global_cap_stops_probing(monkeypatch):
    """The ceiling the outage needed. Burst limits alone did not provide it."""
    monkeypatch.setattr(rate_limit, "SMTP_DAILY_PROBE_CAP", 100)
    monkeypatch.setattr(rate_limit, "SMTP_DAILY_PROBE_CAP_PER_MX", 1000)
    r = _FakeRedis()

    ok, _ = rate_limit.daily_probe_budget(r, "mx.example.com", count=100)
    assert ok, "the first 100 probes are inside the cap"

    ok, why = rate_limit.daily_probe_budget(r, "mx.example.com", count=1)
    assert not ok
    assert "daily_global_cap" in why


def test_per_mx_cap_stops_hammering_one_provider(monkeypatch):
    """~6,400 probes went to ONE provider. A global cap alone would not have
    stopped that if the global ceiling were generous."""
    monkeypatch.setattr(rate_limit, "SMTP_DAILY_PROBE_CAP", 100_000)
    monkeypatch.setattr(rate_limit, "SMTP_DAILY_PROBE_CAP_PER_MX", 50)
    r = _FakeRedis()

    ok, _ = rate_limit.daily_probe_budget(r, "aspmx.l.google.com", count=50)
    assert ok
    ok, why = rate_limit.daily_probe_budget(r, "aspmx.l.google.com", count=1)
    assert not ok
    assert "daily_mx_cap" in why

    # A different provider still has its own budget.
    ok, _ = rate_limit.daily_probe_budget(r, "mx.other.com", count=10)
    assert ok, "the per-MX cap must not be global by accident"


def test_budget_fails_open_when_redis_is_down():
    """Losing the ceiling is bad; halting all verification is worse, and the
    host-level block is the backstop either way."""
    ok, why = rate_limit.daily_probe_budget(_ExplodingRedis(), "mx.example.com")
    assert ok
    assert why == "budget_unavailable"


def test_refused_probe_is_unknown_never_invalid(monkeypatch):
    """The critical detail. An address we DECLINED to test is untested, not dead.

    Returning `invalid` here would feed straight into the cleanup path and delete
    good rows -- turning a rate-limit event into permanent data loss, which is
    precisely the shape of the 2026-08-25 damage.
    """
    monkeypatch.setattr(tasks, "daily_probe_budget", lambda *a, **k: (False, "daily_global_cap:0"))
    monkeypatch.setattr(tasks, "get_redis", lambda: _FakeRedis())

    def _boom(*a, **k):
        raise AssertionError("probe_rcpt_batch must not be called once the cap is hit")

    monkeypatch.setattr(tasks, "probe_rcpt_batch", _boom)

    out = tasks._probe_candidates(["a@x.com", "b@x.com"], "mx.x.com", None)

    assert {r["status"] for r in out} == {"unknown"}
    assert all("probe_budget" in r["reason"] for r in out)
    assert all(r["status"] != "invalid" for r in out)


# --------------------------------------------------------------------------
# 2. A confirmed address is never deleted by a later 550
# --------------------------------------------------------------------------


class _Conn:
    def __init__(self, first_valid_at):
        self.first_valid_at = first_valid_at

    def execute(self, sql, params=None):
        self._row = (self.first_valid_at,)
        return self

    def fetchone(self):
        return self._row


def test_previously_confirmed_address_is_protected():
    assert tasks._was_ever_valid(_Conn("2026-08-20T10:00:00Z"), 1) is True


def test_never_confirmed_address_is_deletable():
    assert tasks._was_ever_valid(_Conn(None), 1) is False


def test_guard_fails_closed_when_the_column_is_missing():
    """Pre-migration, or any query error: keep the address.

    Keeping a dead address costs a stale row. Deleting a live one costs the
    result outright, and there is no way to get it back.
    """

    class _Broken:
        def execute(self, *a, **k):
            raise RuntimeError("column does not exist")

    assert tasks._was_ever_valid(_Broken(), 1) is True
