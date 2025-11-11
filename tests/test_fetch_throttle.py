# tests/test_fetch_throttle.py
from __future__ import annotations

import contextlib
import types
from collections.abc import Iterator

import pytest

throttle = pytest.importorskip("src.fetch.throttle")


# -------------------------------- test utilities --------------------------------------


@contextlib.contextmanager
def fake_clock(monkeypatch) -> Iterator[types.SimpleNamespace]:
    """
    Freeze time and capture sleeps.

    - Overrides time.monotonic()/perf_counter()/time.time so throttle's _now() sees our clock.
    - Overrides time.sleep(dt) to *advance* the frozen clock by dt and accumulate total slept time.

    Exposes:
      now() -> float            current monotonic time
      advance(dt)               manually advance without calling sleep()
      slept() -> float          total seconds 'slept'
      reset_slept()             zero the sleep accumulator
    """
    t = {"now": 1_000_000.0, "slept": 0.0}
    epoch0 = 1_700_000_000.0

    def monotonic():
        return t["now"]

    def perf_counter():
        return t["now"]

    def time_time():
        # derive a wall-clock from our monotonic base
        return epoch0 + (t["now"] - 1_000_000.0)

    def sleep(dt):
        dt = float(dt)
        if dt <= 0:
            return
        t["slept"] += dt
        # advance our monotonic clock as real sleep would
        t["now"] += dt

    monkeypatch.setattr("time.monotonic", monotonic)
    monkeypatch.setattr("time.perf_counter", perf_counter)
    monkeypatch.setattr("time.time", time_time)
    monkeypatch.setattr("time.sleep", sleep)

    ns = types.SimpleNamespace(
        now=lambda: t["now"],
        advance=lambda dt: t.__setitem__("now", t["now"] + float(dt)),
        slept=lambda: t["slept"],
        reset_slept=lambda: t.__setitem__("slept", 0.0),
    )
    yield ns


@pytest.fixture(autouse=True)
def _reset_state():
    throttle.clear()
    yield
    throttle.clear()


# ------------------------------ basic politeness gap ----------------------------------


def test_basic_politeness_gap(monkeypatch):
    # Robots crawl-delay will be used when marking OK; choose 1.5s for easy asserts
    monkeypatch.setattr(throttle.robots, "get_crawl_delay", lambda host: 1.5, raising=False)

    with fake_clock(monkeypatch) as clk:
        host = "example.test"

        # Initially no wait
        slept = throttle.wait_for_turn(host)
        assert slept == pytest.approx(0.0)

        # After a successful request, schedule crawl-delay
        delay = throttle.mark_ok(host)  # uses robots.get_crawl_delay(host)
        assert delay == pytest.approx(1.5)

        # Next request should sleep until the scheduled time
        clk.reset_slept()
        slept = throttle.wait_for_turn(host)
        assert slept == pytest.approx(1.5)
        assert clk.slept() == pytest.approx(1.5)

        # Time has advanced due to our fake sleep; immediate next call should not sleep again
        slept2 = throttle.wait_for_turn(host)
        assert slept2 == pytest.approx(0.0)


# ------------------------------ WAF exponential cool-off -------------------------------


@pytest.mark.parametrize("status,expected_first_cooloff", [(429, 6.0), (403, 6.0)])
def test_exponential_cooloff_on_waf(monkeypatch, status, expected_first_cooloff):
    # Deterministic backoff settings
    monkeypatch.setattr(throttle, "BASE_BACKOFF_S", 3.0, raising=False)
    monkeypatch.setattr(throttle, "MAX_BACKOFF_S", 60.0, raising=False)
    # Crawl-delay shouldn't interfere in this test
    monkeypatch.setattr(throttle.robots, "get_crawl_delay", lambda host: 0.0, raising=False)

    with fake_clock(monkeypatch) as clk:
        host = "waf.test"

        # First strike
        d1 = throttle.after_response(host, status)
        assert d1 == pytest.approx(expected_first_cooloff)

        clk.reset_slept()
        slept1 = throttle.wait_for_turn(host)
        assert slept1 == pytest.approx(expected_first_cooloff)
        assert clk.slept() == pytest.approx(expected_first_cooloff)

        # Advance to the time after first cool-off (already advanced by fake sleep)
        # Second strike → double again
        d2 = throttle.after_response(host, status)
        assert d2 == pytest.approx(12.0)

        clk.reset_slept()
        slept2 = throttle.wait_for_turn(host)
        assert slept2 == pytest.approx(12.0)
        assert clk.slept() == pytest.approx(12.0)

        # Many strikes should cap at MAX_BACKOFF_S
        last = d2
        for _ in range(10):
            last = throttle.after_response(host, status)
            # Advance by sleeping out the schedule each time
            throttle.wait_for_turn(host)
        assert last <= 60.0 + 1e-9
        assert last == pytest.approx(60.0)


# ------------------------------ mixed status behavior ---------------------------------


def test_non_waf_non_success_still_respects_crawl_delay(monkeypatch):
    """
    For statuses other than success and WAF (e.g., 404 or 302), we still apply the crawl-delay gap.
    """
    monkeypatch.setattr(throttle.robots, "get_crawl_delay", lambda host: 0.75, raising=False)

    with fake_clock(monkeypatch) as clk:
        host = "mixed.test"

        # Simulate a 404
        d = throttle.after_response(host, 404)
        assert d == pytest.approx(0.75)

        clk.reset_slept()
        slept = throttle.wait_for_turn(host)
        assert slept == pytest.approx(0.75)
        assert clk.slept() == pytest.approx(0.75)
