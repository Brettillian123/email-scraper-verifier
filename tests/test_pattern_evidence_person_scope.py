"""A hard bounce is evidence about the PERSON until that person is shown to exist.

Background, measured in production 2026-08-25. `_teach_unprocessed()` banked every
550 from a probe session as evidence against the FORMAT. Extraction legitimately
picks up non-employees -- advisors, brand ambassadors, testimonial names, people
who have left -- and each of them bounces on every format we try. At
REFUTED_MIN_FAILURES=2, two such people refuted a company's entire vocabulary.
Every remaining employee then ranked to an empty plan and was never probed.

State when this was found: 73 companies with every pattern refuted and zero
confirmed, 521 people at them, 460 of those never probed. Spot-probing those same
companies with the full vocabulary still returned valid addresses on `flast` and
`first.last` -- the formats their own evidence claimed were dead. caremessage.org
had 26 people, was refuted after 3, and ccorral@caremessage.org resolves.

The discriminating signal is an ACCEPT alongside the rejections: it proves the
person has a mailbox AND that the MX distinguishes local-parts, which is what
makes the rejections mean something about format. With no accept anywhere, the
probe set is the signature of an absent person and must teach nothing.
"""

from __future__ import annotations

import pytest

from src.queueing import tasks

pytestmark = pytest.mark.no_db


class _FakeConn:
    def commit(self):
        pass

    def rollback(self):
        pass


@pytest.fixture
def taught(monkeypatch):
    """Capture every (pattern, ok) the generator commits to the evidence store."""
    recorded: list[tuple[str, bool]] = []

    def _record(con, *, company_id, domain, email, first, last, ok, pattern):
        recorded.append((pattern, bool(ok)))

    monkeypatch.setattr(tasks, "_record_pattern_outcome_safe", _record)
    monkeypatch.setattr(tasks, "_mx_info", lambda d, force=False, db_path=None: ("mx.test", None))
    monkeypatch.setattr(
        tasks, "_load_catchall_status_for_domain", lambda *a, **k: "not_catch_all"
    )
    monkeypatch.setattr(tasks, "_infer_tenant_id", lambda **k: "dev")
    monkeypatch.setattr(tasks, "get_current_job", lambda: None)
    monkeypatch.setattr(tasks, "upsert_generated_email", lambda **k: 1)
    monkeypatch.setattr(tasks, "_persist_sequential_verification_result", lambda **k: None)
    monkeypatch.setattr(tasks, "_set_email_source_note", lambda *a, **k: None)
    monkeypatch.setattr(tasks, "_cleanup_invalid_generated_email", lambda *a, **k: True)
    monkeypatch.setattr(tasks, "_maybe_defer_greylist_retry", lambda *a, **k: False)
    return recorded


def _run(monkeypatch, outcomes: dict[str, str], candidates: list[tuple[str, str]]):
    """Drive the sequential generator with a scripted probe outcome per address."""

    def _probe(emails, mx_host, catch_all_status):
        return [
            {
                "email": e,
                "status": outcomes.get(e, "invalid"),
                "reason": "scripted",
                "code": 250 if outcomes.get(e) == "valid" else 550,
                "retries": 1,
            }
            for e in emails
        ]

    monkeypatch.setattr(tasks, "_probe_candidates", _probe)
    return tasks._generate_emails_sequential(
        con=_FakeConn(),
        person_id=1,
        domain="example.com",
        ranked_candidates=candidates,
        effective_pattern=None,
        company_pattern=None,
        evidence=[],
        max_probes=6,
        nf="kelly",
        nl="leveque",
        company_id=42,
    )


CANDIDATES = [
    ("kleveque@example.com", "flast"),
    ("kelly.leveque@example.com", "first.last"),
    ("kelly@example.com", "first"),
]


def test_person_who_bounces_on_everything_teaches_nothing(monkeypatch, taught):
    """The absent-person signature. This is the regression that mattered.

    Before the fix these three 550s became one failure against each of flast,
    first.last and first. A second such person tipped all three past
    REFUTED_MIN_FAILURES and the company was finished -- no employee after them
    was ever probed.
    """
    result = _run(monkeypatch, {}, CANDIDATES)

    assert result["status"] == "all_invalid"
    assert taught == [], (
        f"an all-bounce person taught the evidence store {taught}; "
        "that is a claim about the company's formats drawn from a person "
        "who is not at the company"
    )


def test_accept_alongside_rejections_teaches_both(monkeypatch, taught):
    """The informative case must still be recorded, or the fix would cost us the
    negative evidence that makes ranking work."""
    result = _run(
        monkeypatch,
        {"kelly.leveque@example.com": "valid"},
        CANDIDATES,
    )

    assert result["status"] == "valid_found"
    assert result["valid_pattern"] == "first.last"

    by_pattern = dict(taught)
    assert by_pattern.get("first.last") is True, "the confirmed pattern was not recorded"
    assert by_pattern.get("flast") is False, (
        "a rejection sitting beside an accept IS real format evidence and must "
        "still be banked"
    )


def test_first_candidate_accepting_still_banks_the_rest(monkeypatch, taught):
    """`_teach_unprocessed()` exists so the RCPTs already paid for are not wasted.

    The loop returns at the first accept, so the later rejections in the same
    batch are only reachable through that path. An accept at rank 1 must still
    bank them.
    """
    _run(monkeypatch, {"kleveque@example.com": "valid"}, CANDIDATES)

    by_pattern = dict(taught)
    assert by_pattern.get("flast") is True
    assert by_pattern.get("first.last") is False
    assert by_pattern.get("first") is False
