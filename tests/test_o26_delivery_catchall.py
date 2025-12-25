from __future__ import annotations

from src.verify.delivery_catchall import (
    DomainDeliveryEvidence,
    classify_domain_delivery_catchall,
    is_user_unknown_hard_bounce,
    should_count_as_bad_invalid_mailbox,
    should_count_as_good_real_mailbox,
    should_upgrade_risky_to_valid,
)

"""
O26 — Unit tests for delivery-time catch-all helpers.

These tests cover:

  - DomainDeliveryEvidence → DeliveryCatchallStatus classification.
  - Heuristic "user unknown" detection from bounce_code / bounce_reason.
  - Per-address good/bad mailbox classification.
  - The upgrade rule for risky_catch_all → valid based on domain evidence.
"""


def test_classify_domain_delivery_catchall_not_catchall_proven() -> None:
    """
    When we have at least one good real mailbox AND at least one bad invalid
    mailbox on a domain, we should mark it as not_catchall_proven.
    """
    evidence = DomainDeliveryEvidence(has_good_real=True, has_bad_invalid=True)
    status = classify_domain_delivery_catchall(evidence)
    assert status == "not_catchall_proven"


def test_classify_domain_delivery_catchall_unknown_when_evidence_incomplete() -> None:
    """
    Any missing side of the A/B pattern should leave the status as unknown.
    """
    assert (
        classify_domain_delivery_catchall(
            DomainDeliveryEvidence(has_good_real=False, has_bad_invalid=False)
        )
        == "unknown"
    )
    assert (
        classify_domain_delivery_catchall(
            DomainDeliveryEvidence(has_good_real=True, has_bad_invalid=False)
        )
        == "unknown"
    )
    assert (
        classify_domain_delivery_catchall(
            DomainDeliveryEvidence(has_good_real=False, has_bad_invalid=True)
        )
        == "unknown"
    )


def test_is_user_unknown_hard_bounce_by_code() -> None:
    """
    Bounce codes with a 5.1.x prefix should be treated as user-unknown style
    hard bounces.
    """
    assert is_user_unknown_hard_bounce("5.1.1", None) is True
    assert is_user_unknown_hard_bounce("5.1.10", "irrelevant") is True
    # Other 5.x codes without 5.1.x prefix should not match here.
    assert is_user_unknown_hard_bounce("5.2.1", "mailbox full") is False
    # Non-5.x codes should not match.
    assert is_user_unknown_hard_bounce("4.2.1", "temporary failure") is False


def test_is_user_unknown_hard_bounce_by_reason() -> None:
    """
    When no bounce_code is present, we fall back to inspecting the free-text
    reason for common "user unknown" phrasings.
    """
    assert is_user_unknown_hard_bounce(None, "User unknown for this domain")
    assert is_user_unknown_hard_bounce(None, "no such user here")
    assert is_user_unknown_hard_bounce(None, "delivery failed: recipient not found")
    # Non-user-unknown reasons should not match.
    assert is_user_unknown_hard_bounce(None, "mailbox full, try again later") is False


def test_should_count_as_good_real_mailbox() -> None:
    """
    A "good real" mailbox must have a test-send that completed (sent or
    delivered_assumed) and no user-unknown style hard bounce.
    """
    # Sent / delivered_assumed with no bounce → good.
    assert should_count_as_good_real_mailbox("sent", None, None)
    assert should_count_as_good_real_mailbox("delivered_assumed", None, "some transient error")

    # No test-send → not good.
    assert not should_count_as_good_real_mailbox(None, None, None)
    assert not should_count_as_good_real_mailbox("queued", None, None)

    # Explicit user-unknown bounce should override any apparent success.
    assert not should_count_as_good_real_mailbox("sent", "5.1.1", "user unknown")


def test_should_count_as_bad_invalid_mailbox() -> None:
    """
    A "bad invalid" mailbox must have a hard bounce AND it must be clearly
    user-unknown style.
    """
    assert should_count_as_bad_invalid_mailbox("bounce_hard", "5.1.1", "user unknown")

    # Hard bounce but not user-unknown → not counted here.
    assert not should_count_as_bad_invalid_mailbox("bounce_hard", "5.2.1", "mailbox full")

    # Non-hard bounces should not be considered here.
    assert not should_count_as_bad_invalid_mailbox("bounce_soft", "5.1.1", "user unknown")
    assert not should_count_as_bad_invalid_mailbox("sent", "5.1.1", "user unknown")


def test_should_upgrade_risky_to_valid_happy_path() -> None:
    """
    When all the O26 conditions hold, we should upgrade risky_catch_all →
    valid (no_bounce_after_test_send).
    """
    assert should_upgrade_risky_to_valid(
        verify_status="risky_catch_all",
        domain_delivery_catchall_status="not_catchall_proven",
        test_send_status="sent",
        bounce_code=None,
        bounce_reason=None,
    )


def test_should_not_upgrade_when_domain_not_proven() -> None:
    """
    Without domain-level not_catchall_proven evidence, we should not upgrade,
    even if the individual address looks good.
    """
    assert not should_upgrade_risky_to_valid(
        verify_status="risky_catch_all",
        domain_delivery_catchall_status="unknown",
        test_send_status="sent",
        bounce_code=None,
        bounce_reason=None,
    )
    assert not should_upgrade_risky_to_valid(
        verify_status="risky_catch_all",
        domain_delivery_catchall_status=None,
        test_send_status="sent",
        bounce_code=None,
        bounce_reason=None,
    )


def test_should_not_upgrade_when_not_risky_or_no_test_send() -> None:
    """
    Sanity checks: we only ever consider upgrading rows that are currently
    risky_catch_all and that have a completed test-send.
    """
    # Wrong verify_status.
    assert not should_upgrade_risky_to_valid(
        verify_status="valid",
        domain_delivery_catchall_status="not_catchall_proven",
        test_send_status="sent",
        bounce_code=None,
        bounce_reason=None,
    )

    # No or incomplete test-send.
    for ts in (None, "queued", "requested"):
        assert not should_upgrade_risky_to_valid(
            verify_status="risky_catch_all",
            domain_delivery_catchall_status="not_catchall_proven",
            test_send_status=ts,
            bounce_code=None,
            bounce_reason=None,
        )


def test_should_not_upgrade_when_user_unknown_bounce_present() -> None:
    """
    Even if the domain is not_catchall_proven, a clear user-unknown bounce
    for this address should prevent any upgrade.
    """
    assert not should_upgrade_risky_to_valid(
        verify_status="risky_catch_all",
        domain_delivery_catchall_status="not_catchall_proven",
        test_send_status="sent",
        bounce_code="5.1.1",
        bounce_reason="user unknown",
    )
