from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

"""
O26 — Delivery-time catch-all refinement helpers.

This module encodes the domain-level A/B pattern you described:

  - A = real mailbox on domain D with a test-send that did NOT hard-bounce.
  - B = obviously invalid mailbox on domain D with a hard 5.1.x "user unknown" bounce.

If, for a given domain D, we see both:

  - at least one A (good real mailbox), and
  - at least one B (bad invalid mailbox with 5.1.x "user unknown"),

then we can treat D as *not* a true catch-all at DELIVERY time, even if RCPT-based
R17 logic previously flagged it as catch-all.

This module does NOT talk to the database directly. It’s meant to be used by:

  - O26 backfill scripts that scan verification_results / emails / companies and
    compute domain-level delivery_catchall_status, and
  - any reclassification logic that wants to upgrade risky_catch_all → valid
    when delivery-time evidence is strong enough.
"""

DeliveryCatchallStatus = Literal["unknown", "not_catchall_proven", "catchall_consistent"]


@dataclass
class DomainDeliveryEvidence:
    """
    Minimal domain-level evidence derived from verification_results.

    has_good_real:
        True if we have at least one *real* mailbox A on this domain that:
          - had a test-send, and
          - did NOT hard-bounce as "user unknown".

    has_bad_invalid:
        True if we have at least one *invalid* mailbox B on this domain that:
          - had a test-send, and
          - DID hard-bounce as "user unknown" (5.1.x code or equivalent reason).
    """

    has_good_real: bool = False
    has_bad_invalid: bool = False


def classify_domain_delivery_catchall(evidence: DomainDeliveryEvidence) -> DeliveryCatchallStatus:
    """
    Given aggregate evidence for a domain, emit a delivery_catchall_status.

    Current policy (can be extended later):

      - If we have both:
            has_good_real = True
            has_bad_invalid = True
        then:
            "not_catchall_proven"

      - Otherwise:
            "unknown"

    We leave room for a future "catchall_consistent" state if you later decide
    to encode the negative pattern (e.g., no 5.1.x bounces for clearly invalid
    addresses after many test-sends).
    """
    if evidence.has_good_real and evidence.has_bad_invalid:
        return "not_catchall_proven"

    return "unknown"


def is_user_unknown_hard_bounce(
    bounce_code: str | None,
    bounce_reason: str | None,
) -> bool:
    """
    Heuristic for "5.1.x user unknown" style hard bounces.

    We lean on the bounce_code first (preferred if present), then fall back
    to a simple substring search in the free-text reason.

    This is intentionally conservative. We only want to mark B when the ESP is
    clearly saying "this mailbox does not exist".
    """
    if bounce_code:
        # Most SES-style "user unknown" codes are 5.1.x (e.g. 5.1.1, 5.1.10).
        # We keep the prefix match simple and transparent.
        if bounce_code.startswith("5.1."):
            return True

    if bounce_reason:
        lowered = bounce_reason.lower()
        # Cover common phrasings:
        #   "user unknown", "recipient not found", "no such user", etc.
        for needle in (
            "user unknown",
            "unknown user",
            "no such user",
            "recipient not found",
            "mailbox unavailable",
            "recipient address rejected",
        ):
            if needle in lowered:
                return True

    return False


def should_count_as_good_real_mailbox(
    test_send_status: str | None,
    bounce_code: str | None,
    bounce_reason: str | None,
) -> bool:
    """
    Decide whether a single verification_result row should contribute to
    `has_good_real` for its domain.

    We count it as "good real" when:

      - The test-send actually happened (sent or delivered_assumed).
      - There is NO clear "user unknown" hard bounce.

    This is intentionally simple: it does not look at verify_status at all.
    In practice you will typically filter to rows where verify_status is
    'valid' or 'risky_catch_all' before calling this helper.
    """
    if test_send_status not in ("sent", "delivered_assumed"):
        return False

    if is_user_unknown_hard_bounce(bounce_code, bounce_reason):
        return False

    return True


def should_count_as_bad_invalid_mailbox(
    test_send_status: str | None,
    bounce_code: str | None,
    bounce_reason: str | None,
) -> bool:
    """
    Decide whether a verification_result row should contribute to
    `has_bad_invalid` for its domain.

    We count it as "bad invalid" when:

      - The test-send status reflects a hard bounce, and
      - The bounce is clearly "user unknown".

    You can optionally layer additional filters (e.g. verify_status == 'invalid')
    in the caller if you want to be stricter about what qualifies as B.
    """
    if test_send_status != "bounce_hard":
        return False

    return is_user_unknown_hard_bounce(bounce_code, bounce_reason)


def should_upgrade_risky_to_valid(
    verify_status: str,
    domain_delivery_catchall_status: DeliveryCatchallStatus | None,
    test_send_status: str | None,
    bounce_code: str | None,
    bounce_reason: str | None,
) -> bool:
    """
    Core O26 upgrade rule at the single-address level.

    Given the current row-level status + domain-level status, decide whether
    we should upgrade:

        risky_catch_all  →  valid (no_bounce_after_test_send)

    Conditions:

      - The current verify_status is exactly 'risky_catch_all'.
      - The domain's delivery_catchall_status is 'not_catchall_proven'.
      - This address has had a successful test-send:
            test_send_status in ('sent', 'delivered_assumed')
        AND no clear "user unknown" hard bounce.

    This function is deliberately boolean and side-effect free. A DB-focused
    script can use it as:

        if should_upgrade_risky_to_valid(...):
            UPDATE verification_results
            SET verify_status = 'valid',
                verify_reason = 'no_bounce_after_test_send'
            WHERE id = ?;
    """
    if verify_status != "risky_catch_all":
        return False

    if domain_delivery_catchall_status != "not_catchall_proven":
        return False

    if test_send_status not in ("sent", "delivered_assumed"):
        return False

    if is_user_unknown_hard_bounce(bounce_code, bounce_reason):
        # If we somehow have a 5.1.x bounce but still think the test_send_status
        # is "sent" or "delivered_assumed", play it safe and do NOT upgrade.
        return False

    return True
