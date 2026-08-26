"""A run must not report a terminal status while its work is still queued.

This defect failed in BOTH directions inside one week, which is why it gets its
own module:

  * `a661ca92` reported **succeeded with 655 generate jobs still queued**.
    Verification kept writing rows for roughly an hour afterwards, and measuring
    that run by its terminal status undercounted it as 20 companies against a
    true 87.
  * `c19269bd` sat **running for 18 hours after its queues had fully drained**
    and had to be marked terminal by hand.

The cause of the first: `task_generate_company_emails` is the "terminal stage",
but a fan-out only ENQUEUES per-person jobs and returns in seconds. The probing
happens later, in those jobs.

The fix moves the company-complete signal to the LAST person job. These tests use
the real Redis from docker-compose.test.yml -- the counter is the whole mechanism,
so faking it would test nothing.

Note the deliberate asymmetry in `company_pending_done`: a MISSING counter
returns None, meaning "do not complete from here". A lost key can therefore only
delay a terminal status, never fabricate one. Completing early is the failure
that corrupts measurements; completing late is visible and harmless.
"""

from __future__ import annotations

import pytest

from src.queueing import pipeline_v2 as pv2

TENANT = "dev"
RUN = "test-run-completion"


@pytest.fixture
def clean_counter():
    """Drop the counter before and after, so reruns cannot pass on stale state."""

    def _drop():
        try:
            r = pv2._get_redis()
            r.delete(pv2._company_pending_key(TENANT, RUN, 1))
        except Exception:
            pass

    _drop()
    yield
    _drop()


def test_counter_tracks_outstanding_people(clean_counter):
    """Three people enqueued, three finishing, and only the last one reports 0."""
    pv2.company_pending_set(tenant_id=TENANT, run_id=RUN, company_id=1, n=3)

    assert pv2.company_pending_done(tenant_id=TENANT, run_id=RUN, company_id=1) == 2
    assert pv2.company_pending_done(tenant_id=TENANT, run_id=RUN, company_id=1) == 1
    assert pv2.company_pending_done(tenant_id=TENANT, run_id=RUN, company_id=1) == 0


def test_missing_counter_reports_none_not_zero(clean_counter):
    """The safe direction.

    None means "do not complete from here". Returning 0 would let any job with a
    lost key declare the company finished -- reintroducing the exact bug, but
    intermittently, which is worse.
    """
    assert pv2.company_pending_done(tenant_id=TENANT, run_id=RUN, company_id=1) is None


def test_zero_people_leaves_no_counter(clean_counter):
    """A company with nobody to probe must not strand the run.

    company_pending_set is a no-op at n=0, so the fan-out's own completion path
    stays in charge -- there is no person job coming to take over.
    """
    pv2.company_pending_set(tenant_id=TENANT, run_id=RUN, company_id=1, n=0)
    assert pv2.company_pending_done(tenant_id=TENANT, run_id=RUN, company_id=1) is None


def test_counter_does_not_go_negative(clean_counter):
    """A retried or duplicated person job must not push the count below zero.

    RQ retries exist; a negative counter would mean the company could never
    report 0 again and the run would hang -- trading an early completion for a
    permanent one.
    """
    pv2.company_pending_set(tenant_id=TENANT, run_id=RUN, company_id=1, n=1)
    assert pv2.company_pending_done(tenant_id=TENANT, run_id=RUN, company_id=1) == 0
    # counter is gone now; further calls report None, never a negative number
    assert pv2.company_pending_done(tenant_id=TENANT, run_id=RUN, company_id=1) is None


def test_fanout_does_not_complete_a_company_with_people_pending(monkeypatch, clean_counter):
    """The regression itself, at the decision point.

    With people outstanding, _maybe_mark_run_complete_from_job() must heartbeat
    and return WITHOUT calling _check_run_completion. That call is what marked
    a661ca92 succeeded while 655 jobs were still queued.
    """
    pv2.company_pending_set(tenant_id=TENANT, run_id=RUN, company_id=1, n=5)

    completed: list[tuple] = []
    beats: list[str] = []

    class _Job:
        meta = {
            "run_id": RUN,
            "tenant_id": TENANT,
            "company_id": 1,
            "total_companies": 1,
            "is_terminal_stage": True,
            "stage": "generate_company_fanout",
        }

    # _maybe_mark_run_complete_from_job does `from rq import get_current_job`
    # INSIDE the function, so the patch has to land on rq itself.
    import rq

    monkeypatch.setattr(rq, "get_current_job", lambda: _Job())
    import src.queueing.tasks as tasks_mod

    monkeypatch.setattr(tasks_mod, "_touch_run_heartbeat", lambda **k: beats.append("beat"))
    monkeypatch.setattr(tasks_mod, "_check_run_completion", lambda **k: completed.append(k))

    pv2._maybe_mark_run_complete_from_job()

    assert beats, "the run heartbeat must still fire; the reaper depends on it"
    assert completed == [], (
        "the fan-out completed a company that still had 5 people queued -- "
        "this is the a661ca92 defect"
    )


def test_fanout_still_completes_when_nothing_is_pending(monkeypatch, clean_counter):
    """The other direction: with no counter, completion must proceed.

    c19269bd sat `running` for 18 hours after draining. A gate that never opens
    is not an improvement over one that opens too early.
    """
    completed: list[dict] = []

    class _Job:
        meta = {
            "run_id": RUN,
            "tenant_id": TENANT,
            "company_id": 1,
            "total_companies": 1,
            "is_terminal_stage": True,
            "stage": "generate_company_fanout",
        }

    # _maybe_mark_run_complete_from_job does `from rq import get_current_job`
    # INSIDE the function, so the patch has to land on rq itself.
    import rq

    monkeypatch.setattr(rq, "get_current_job", lambda: _Job())
    import src.queueing.tasks as tasks_mod

    monkeypatch.setattr(tasks_mod, "_touch_run_heartbeat", lambda **k: None)
    monkeypatch.setattr(tasks_mod, "_check_run_completion", lambda **k: completed.append(k))

    pv2._maybe_mark_run_complete_from_job()

    assert completed, "a company with no people pending must still complete"
    assert completed[0]["company_id"] == 1
