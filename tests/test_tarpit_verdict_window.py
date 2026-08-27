"""A tarpit verdict must be re-earnable, or it is permanent by construction.

The tarpit gate skips a host before a socket opens. That is correct -- Mimecast
really does defer 95% of our probes -- but it means the flag PREVENTS the
evidence that would overturn it. If the verdict is computed over a long history,
a host can never get out.

Measured 2026-08-27. Mimecast carried a 94.7% temp-fail rate over 1,955 probes
inside the 180-day behaviour window and was skipped on every attempt. **Every one
of those probes predated August**, and therefore predated the greylist deferral
(60/300/900s) built specifically for hosts that defer. The rate that condemned
them was measured under retry timings greylisting always rejects; they had never
been tried under the current behaviour and, because of the skip, never could be.
An overnight pass enqueued 500 people, issued 45 probes and found 2 addresses.

The fix splits the windows. Latency keeps the long one -- a host that was slow six
months ago is still slow, and the only use of that number is widening a timeout.
The verdict gets a short one, so "is this host deferring us NOW?" is answered
with recent evidence, and a host we have not probed lately is not currently known
to be anything.

This is the same shape as the person-scoped pattern bug: a conclusion drawn once,
never revisited, that suppresses the observations which would revise it.
"""

from __future__ import annotations

import pytest

from src.resolve import mx as mx_mod

pytestmark = pytest.mark.no_db

TARPIT_ROWS = 60


def _rows(days_ago: int, n: int = TARPIT_ROWS, category: str = "temp_fail"):
    """n probes, all `category`, all stamped `days_ago` days in the past."""
    from datetime import UTC, datetime, timedelta

    ts = (datetime.now(UTC) - timedelta(days=days_ago)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return [(451, category, 300, ts) for _ in range(n)]


class _Conn:
    def __init__(self, rows):
        self._rows = rows

    def execute(self, *a, **k):
        return self

    def fetchall(self):
        return self._rows

    def commit(self):
        pass

    def close(self):
        pass


@pytest.fixture
def stub_db(monkeypatch):
    def _install(rows):
        monkeypatch.setattr(mx_mod, "get_conn", lambda: _Conn(rows))
        monkeypatch.setattr(mx_mod, "_ensure_behavior_schema", lambda conn: None)

    return _install


def test_recent_deferrals_still_flag_a_tarpit(stub_db):
    """The feature must keep working. A host deferring us NOW is a tarpit."""
    stub_db(_rows(days_ago=2))
    hint = mx_mod.get_mx_behavior_hint("mx.test", window_days=180, tarpit_window_days=14)
    assert hint is not None
    assert hint["tarpit"] is True
    assert hint["tarpit_n"] == TARPIT_ROWS


def test_stale_deferrals_do_not_flag_a_tarpit(stub_db):
    """The regression. Old evidence must not condemn a host forever.

    These rows are inside the 180-day latency window and outside the 14-day
    verdict window -- exactly Mimecast's situation on 2026-08-27, where every
    condemning sample predated the fix designed to handle it.
    """
    stub_db(_rows(days_ago=90))
    hint = mx_mod.get_mx_behavior_hint("mx.test", window_days=180, tarpit_window_days=14)
    assert hint is not None
    assert hint["tarpit"] is False, (
        "a host was called a tarpit on evidence too old to reflect current "
        "behaviour; the flag suppresses the probes that would refresh it, so "
        "this verdict would never be revisited"
    )
    assert hint["tarpit_n"] == 0


def test_latency_still_uses_the_long_window(stub_db):
    """Splitting the windows must not throw away the latency signal.

    Old latency is still useful -- it feeds timeout widening, which is the right
    response to a slow host. Only the VERDICT needed shortening.
    """
    stub_db(_rows(days_ago=90))
    hint = mx_mod.get_mx_behavior_hint("mx.test", window_days=180, tarpit_window_days=14)
    assert hint["n"] == TARPIT_ROWS, "latency sample was discarded with the verdict"
    assert hint["latency_p50_ms"] == 300


def test_too_few_recent_samples_is_not_a_tarpit(stub_db):
    """TARPIT_MIN_SAMPLES still applies inside the shorter window.

    Otherwise the first two deferrals after a quiet spell would condemn a host,
    which is how a coverage push would blacklist every MX it has just met.
    """
    stub_db(_rows(days_ago=1, n=mx_mod.TARPIT_MIN_SAMPLES - 1))
    hint = mx_mod.get_mx_behavior_hint("mx.test", window_days=180, tarpit_window_days=14)
    assert hint["tarpit"] is False
    assert hint["tarpit_n"] == mx_mod.TARPIT_MIN_SAMPLES - 1


def test_recent_healthy_traffic_clears_a_host(stub_db):
    """A host that answers cleanly now is not a tarpit, whatever it did before."""
    stub_db(_rows(days_ago=1, category="hard_fail"))
    hint = mx_mod.get_mx_behavior_hint("mx.test", window_days=180, tarpit_window_days=14)
    assert hint["tarpit"] is False
    assert hint["tarpit_temp_fail_rate"] == 0.0
