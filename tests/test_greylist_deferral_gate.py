"""Only a real server-issued 4xx may enter the greylist retry schedule.

Background: 20,443 of 64,351 production probes (32%) were temp_fails abandoned
after a 1s and a 2s sleep, while greylisting needs 300s+. Mimecast returned 69
accepts from 6,671 probes because of it. The fix requeues those probes on a
60/300/900s schedule instead.

The trap this test guards: `_classify_exc()` buckets socket timeouts AND every
unexpected exception as `temp_fail` with `code=None`. Gating the requeue on the
category alone would put a timeout -- or a TypeError in our own code -- on a
15-minute retry loop. Those 319 timeout probes already consumed 16.5% of all
SMTP wall-time at one attempt each.

No server reply means no code, which means nothing to come back to. See the gate
in src/queueing/tasks.py (`category == "temp_fail" and code is not None and
400 <= code < 500`).
"""

from __future__ import annotations

import smtplib

import pytest

from src.verify.smtp import _classify_exc, is_greylist_deferral


def _eligible(category: str, code: int | None) -> bool:
    """Mirror of the gate in tasks.py. Kept in lockstep by the tests below."""
    return category == "temp_fail" and code is not None and 400 <= code < 500


@pytest.mark.parametrize(
    "exc",
    [
        TimeoutError("socket timeout"),
        smtplib.SMTPServerDisconnected("connection dropped mid-session"),
        TypeError("a bug in our own code"),
        AttributeError("another bug in our own code"),
        OSError("network unreachable"),
    ],
)
def test_no_server_reply_is_never_greylist_deferred(exc):
    """A timeout is not a deferral, and neither is a crash in our own code.

    Retrying these on the greylist schedule multiplies the single most expensive
    failure mode in the system and, because get_mx_behavior_hint() keys on
    recent_temp_fail_rate, would also mislabel healthy hosts as tarpits.
    """
    category, code, _msg, _err = _classify_exc(exc)
    assert code is None, f"{type(exc).__name__} unexpectedly carried an SMTP code"
    assert not _eligible(category, code), (
        f"{type(exc).__name__} would be requeued on the greylist schedule"
    )


@pytest.mark.parametrize("code", [421, 450, 451, 452])
def test_server_issued_4xx_is_greylist_deferred(code):
    """4xx is the server saying 'come back later'. That is the whole feature."""
    exc = smtplib.SMTPResponseException(code, b"4.7.1 try again later")
    category, parsed, _msg, _err = _classify_exc(exc)
    assert parsed == code
    assert _eligible(category, parsed), f"{code} should be requeued, was not"


@pytest.mark.parametrize("code", [550, 551, 553, 554])
def test_permanent_rejection_is_never_deferred(code):
    """5xx is a verdict. Requeuing it would re-probe a known-dead address."""
    exc = smtplib.SMTPResponseException(code, b"5.1.1 user unknown")
    category, parsed, _msg, _err = _classify_exc(exc)
    assert category == "hard_fail"
    assert not _eligible(category, parsed)


def test_is_greylist_deferral_labels_but_does_not_gate():
    """`is_greylist_deferral` names WHY we deferred; it must not be the gate.

    A bare `450 mailbox busy` is not greylisting, but it is still a legitimate
    'come back later' that we do want to retry. Narrowing the gate to this
    predicate would silently drop those.
    """
    assert is_greylist_deferral(451, "4.7.1 greylisted, try again later") is True
    assert is_greylist_deferral(450, "mailbox busy") is False
    assert is_greylist_deferral(None, "") is False
    assert is_greylist_deferral(550, "user unknown") is False

    # ...yet a bare 450 must still reach the retry schedule.
    assert _eligible("temp_fail", 450)


def test_gate_matches_the_source_of_truth():
    """Fail loudly if tasks.py stops gating on a real 4xx.

    This test is the reason the gate exists; a future edit that reverts it to
    `category == "temp_fail"` should not pass silently.
    """
    from pathlib import Path

    src = Path(__file__).resolve().parents[1] / "src" / "queueing" / "tasks.py"
    text = src.read_text(encoding="utf-8", errors="replace")
    assert 'category == "temp_fail" and code is not None and 400 <= code < 500' in text, (
        "the greylist gate in tasks.py no longer requires a server-issued 4xx"
    )
