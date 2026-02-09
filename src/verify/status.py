from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal

"""
R18 â€” Canonical verification status classifier.

Takes low-level signals from:
  - R16 SMTP RCPT probe (rcpt_category / code / msg)
  - R17 catch-all probe (domain-level catch_all_status)
  - O07 fallback vendor (fallback_status, raw payload)
and emits a single, canonical verify_status + verify_reason.

Intended usage:
  - Build a VerificationSignals instance from DB fields.
  - Make sure catch_all_status is derived from R17, e.g.:
        - check_catchall_for_domain(domain).status, or
        - the latest domain_resolutions.catch_all_status
        - normalised into: "catch_all" | "not_catch_all" | "unknown" | None
  - Call classify(signals, now=datetime.utcnow()).
  - Persist verify_status / verify_reason / verified_mx / verified_at
    back onto verification_results.

O26 â€” Bounce-based escalation helper.

Additional helper:

  - should_escalate_to_test_send(...)

which decides, based on the R18 outcome + provider behavior, whether a
verification_result should be escalated to a test-send / bounce-based
verification path.
"""

VerifyStatus = Literal["valid", "risky_catch_all", "invalid", "unknown_timeout"]


@dataclass
class VerificationSignals:
    """
    Inputs for R18 verification classification.

    All fields are intentionally simple primitives so this type can be
    used directly with rows from verification_results + domain_resolutions.

    Notes:
      - rcpt_category should come from R16 (probe_rcpt["category"]) and then
        normalised via _norm_rcpt_category().
      - catch_all_status must ultimately come from R17:
            "catch_all" | "not_catch_all" | "unknown" | None
        Do NOT pass raw "tempfail"/"no_mx"/"error" here; those should be
        normalised to "unknown" by your orchestration layer.
    """

    # Normalised RCPT outcome:
    # "deliverable" | "undeliverable" | "tempfail" | "timeout"
    # | "blocked" | "unknown" | None
    rcpt_category: str | None
    rcpt_code: int | None  # 250, 550, 421, etc.
    rcpt_msg: bytes | None  # raw/decoded SMTP message (optional)

    # Domain-level catch-all status (from R17), normalised to:
    # "catch_all" | "not_catch_all" | "unknown" | None
    catch_all_status: str | None

    # "deliverable" | "undeliverable" | "unknown" | None
    fallback_status: str | None

    # MX host used for the probe (gmail-smtp-in.l.google.com, ...)
    mx_host: str | None
    verified_at: str | None  # ISO-8601 UTC timestamp of last probe/classification


def _norm(s: str | None) -> str | None:
    if s is None:
        return None
    s2 = s.strip().lower()
    return s2 or None


def _norm_rcpt_category(cat: str | None) -> str | None:
    """
    Normalise upstream RCPT categories into the vocabulary expected by R18.

    Accepts synonyms from:
      - R16 _classify() ("accept", "hard_fail", "temp_fail", "unknown", ...)
      - Vendor / orchestration layers ("deliverable", "undeliverable", "timeout", ...)
    """
    c = _norm(cat)
    if c is None:
        return None

    if c in {"deliverable", "accept", "ok", "success"}:
        return "deliverable"
    if c in {"undeliverable", "hard_fail", "hardfail", "invalid"}:
        return "undeliverable"
    if c in {"tempfail", "temp_fail", "softfail", "soft_fail"}:
        return "tempfail"
    if c in {"timeout", "timed_out", "timedout"}:
        return "timeout"
    if c in {"blocked", "blocked_by_provider"}:
        return "blocked"
    if c in {"unknown"}:
        return "unknown"

    # Unknown label â€“ leave as-is so we can still distinguish it.
    return c


def _norm_fallback_status(status: str | None) -> str | None:
    """
    Normalise fallback/vendor status into simple "deliverable"/"undeliverable"/"unknown".
    """
    s = _norm(status)
    if s is None:
        return None

    if s in {"deliverable", "valid", "ok"}:
        return "deliverable"
    if s in {"undeliverable", "invalid", "bounce"}:
        return "undeliverable"
    if s in {"unknown", "neutral"}:
        return "unknown"

    return s


def _norm_catch_all_status(status: str | None) -> str | None:
    """
    Normalise domain-level catch-all status into the vocabulary R18 expects.

    Inputs may be:
      - Directly from R17: "catch_all" | "not_catch_all" | "tempfail" | "no_mx" | "error"
      - Already-normalised: "catch_all" | "not_catch_all" | "unknown"
      - None

    Outputs:
      - "catch_all" | "not_catch_all" | "unknown" | None
    """
    s = _norm(status)
    if s is None:
        return None
    if s in {"catch_all", "not_catch_all"}:
        return s
    if s in {"tempfail", "no_mx", "error"}:
        return "unknown"
    # If orchestration passed some future enum, keep it but it will not be
    # treated as a confirmed catch-all.
    return s


def _parse_iso8601(ts: str | None) -> datetime | None:
    """
    Best-effort ISO-8601 parser for the simple UTC formats we use, e.g.:

      2025-11-20T19:45:03Z
      2025-11-20T19:45:03+00:00

    Returns naive UTC datetime or None on failure.
    """
    if not ts:
        return None

    txt = ts.strip()
    try:
        if txt.endswith("Z"):
            # datetime.fromisoformat doesn't understand bare "Z"
            return datetime.fromisoformat(txt[:-1] + "+00:00").replace(tzinfo=None)
        # Either already has offset or is naive.
        dt = datetime.fromisoformat(txt)
        # If it has tzinfo, normalise to naive UTC for comparisons.
        if dt.tzinfo is not None:
            return dt.astimezone(tz=None).replace(tzinfo=None)
        return dt
    except Exception:
        # Be defensive: if parsing fails, treat as if no timestamp.
        return None


def _check_ttl(
    verified_at: str | None,
    *,
    now: datetime,
    ttl_days: int,
) -> tuple[VerifyStatus, str] | None:
    """
    Return a stale-result classification if TTL exceeded, else None.
    """
    ts = _parse_iso8601(verified_at)
    if ts is None:
        return None
    if now - ts > timedelta(days=ttl_days):
        return "unknown_timeout", "stale_result_ttl_exceeded"
    return None


def _compute_rcpt_flags(
    *,
    rcpt_category: str | None,
    rcpt_code: int | None,
) -> tuple[bool, bool, bool, bool, bool]:
    """
    Derive common SMTP RCPT flags from category/code.

    rcpt_category is expected to be normalised via _norm_rcpt_category().
    """
    is_5xx = rcpt_code is not None and 500 <= rcpt_code < 600
    is_2xx = rcpt_code is not None and 200 <= rcpt_code < 300
    is_4xx = rcpt_code is not None and 400 <= rcpt_code < 500

    good_rcpt = (rcpt_category == "deliverable") or is_2xx
    soft_fail = rcpt_category in {"tempfail", "timeout", "blocked"} or is_4xx
    return is_5xx, is_2xx, is_4xx, good_rcpt, soft_fail


def _classify_hard_invalid(
    *,
    rcpt_category: str | None,
    is_5xx: bool,
    fallback_status: str | None,
) -> tuple[VerifyStatus, str] | None:
    """
    Handle hard invalids (5xx / undeliverable), optionally overridden by fallback.
    """
    if rcpt_category != "undeliverable" and not is_5xx:
        return None

    if fallback_status == "deliverable":
        return "valid", "fallback_valid_overrides_rcpt_5xx"
    return "invalid", "rcpt_5xx_user_unknown"


def _classify_good_rcpt(
    *,
    good_rcpt: bool,
    catch_all_status: str | None,
    fallback_status: str | None,
) -> tuple[VerifyStatus, str] | None:
    """
    Handle good (2xx / deliverable) RCPT paths, including catch-all logic.

    Policy:

      - Confirmed catch-all domains ("catch_all") â†’ risky_catch_all
        (or invalid if fallback says undeliverable).
      - Non-catch-all ("not_catch_all") AND unknown/None catch-all status:
        treat 2xx as valid by default.
      - Fallback "deliverable" can upgrade the reason string but not downgrade
        confirmed catch-all to valid.
    """
    if not good_rcpt:
        return None

    # Explicit non-catch-all: 2xx is fully trusted.
    if catch_all_status == "not_catch_all":
        if fallback_status == "deliverable":
            return "valid", "rcpt_2xx_non_catchall_fallback_valid"
        return "valid", "rcpt_2xx_non_catchall"

    # Confirmed catch-all: structurally risky.
    if catch_all_status == "catch_all":
        if fallback_status == "undeliverable":
            return "invalid", "rcpt_2xx_catchall_fallback_invalid"
        # Keep as risky even if vendor says deliverable; domain-level
        # catch-all is a structural risk.
        return "risky_catch_all", "rcpt_2xx_catchall"

    # Unknown / None catch-all status:
    # Be conservative - treat as risky since we couldn't verify the domain isn't catch-all.
    # This prevents false "valid" labels on domains where catch-all detection failed.
    if fallback_status == "deliverable":
        # Vendor says deliverable, but we couldn't check catch-all status.
        # Still risky because the domain might accept everything.
        return "risky_catch_all", "rcpt_2xx_catchall_unknown_fallback_deliverable"
    return "risky_catch_all", "rcpt_2xx_catchall_unknown"


def _classify_soft_fail(
    *,
    soft_fail: bool,
    fallback_status: str | None,
    catch_all_status: str | None = None,
) -> tuple[VerifyStatus, str] | None:
    """
    Handle tempfail / timeout / blocked outcomes combined with fallback.

    Strategy:
      - If a vendor has a strong opinion, trust it.
      - Otherwise, if the domain is known catch-all, treat as risky.
      - Otherwise, we're stuck with an unknown timeout-style outcome.
    """
    if not soft_fail:
        return None

    if fallback_status == "deliverable":
        return "valid", "fallback_valid_after_tempfail"
    if fallback_status == "undeliverable":
        return "invalid", "fallback_invalid_after_tempfail"

    # No vendor opinion. If we *do* know the domain is catch-all, that
    # structural property is still meaningful: it's a risky target even
    # if this specific RCPT timed out.
    if catch_all_status == "catch_all":
        return "risky_catch_all", "catchall_softfail_no_fallback"

    # Otherwise we're genuinely in the dark: the mailbox might exist,
    # might not, and we only saw tempfail/timeout/blocked.
    return "unknown_timeout", "tempfail_or_timeout"


def _classify_fallback_only(
    *,
    rcpt_category: str | None,
    rcpt_code: int | None,
    fallback_status: str | None,
) -> tuple[VerifyStatus, str] | None:
    """
    Handle cases with no SMTP RCPT, but where a vendor has an opinion.
    """
    if rcpt_code is not None or rcpt_category is not None:
        return None

    if fallback_status == "deliverable":
        return "valid", "fallback_valid_no_smtp"
    if fallback_status == "undeliverable":
        return "invalid", "fallback_invalid_no_smtp"
    return None


def classify(
    signals: VerificationSignals,
    *,
    now: datetime,
    ttl_days: int = 90,
) -> tuple[VerifyStatus, str]:
    """
    Classify an email verification outcome into a canonical VerifyStatus + reason.

    Order of operations (roughly):

      1. TTL staleness check.
      2. Hard invalids (5xx / undeliverable), unless overridden by fallback.
      3. 2xx / deliverable RCPT:
         - confirmed catch-all â†’ risky
         - non-catch-all or unknown â†’ valid (with distinct reasons)
           or risky/invalid with fallback.
      4. Tempfail / timeout / blocked, combined with fallback + catch-all.
      5. Fallback-only classifications.
      6. No strong signals â†’ unknown_timeout / no_verification_attempt.
    """
    rcpt_category_raw = _norm(signals.rcpt_category)
    rcpt_category = _norm_rcpt_category(rcpt_category_raw)
    catch_all_status = _norm_catch_all_status(signals.catch_all_status)
    fallback_status = _norm_fallback_status(signals.fallback_status)
    rcpt_code = signals.rcpt_code

    # ------------------------------------------------------------------
    # 1) TTL / staleness guard
    # ------------------------------------------------------------------
    ttl_result = _check_ttl(signals.verified_at, now=now, ttl_days=ttl_days)
    if ttl_result is not None:
        return ttl_result

    # ------------------------------------------------------------------
    # 2) Derive RCPT flags (2xx/4xx/5xx, good_rcpt, soft_fail)
    # ------------------------------------------------------------------
    is_5xx, _is_2xx, is_4xx, good_rcpt, soft_fail = _compute_rcpt_flags(
        rcpt_category=rcpt_category,
        rcpt_code=rcpt_code,
    )

    # ------------------------------------------------------------------
    # 3) Hard invalids â€” 5xx / undeliverable
    # ------------------------------------------------------------------
    result = _classify_hard_invalid(
        rcpt_category=rcpt_category,
        is_5xx=is_5xx,
        fallback_status=fallback_status,
    )
    if result is not None:
        return result

    # ------------------------------------------------------------------
    # 4) Good RCPT paths (2xx / deliverable)
    # ------------------------------------------------------------------
    result = _classify_good_rcpt(
        good_rcpt=good_rcpt,
        catch_all_status=catch_all_status,
        fallback_status=fallback_status,
    )
    if result is not None:
        return result

    # ------------------------------------------------------------------
    # 5) Tempfail / timeout / blocked paths
    # ------------------------------------------------------------------
    soft_fail = soft_fail or is_4xx
    result = _classify_soft_fail(
        soft_fail=soft_fail,
        fallback_status=fallback_status,
        catch_all_status=catch_all_status,
    )
    if result is not None:
        return result

    # ------------------------------------------------------------------
    # 6) Fallback-only paths (no SMTP RCPT, but vendor has an opinion)
    # ------------------------------------------------------------------
    result = _classify_fallback_only(
        rcpt_category=rcpt_category,
        rcpt_code=rcpt_code,
        fallback_status=fallback_status,
    )
    if result is not None:
        return result

    # ------------------------------------------------------------------
    # 7) No strong signal at all
    # ------------------------------------------------------------------
    # We either never tried to verify, or got an ambiguous outcome with
    # no vendor help.
    return "unknown_timeout", "no_verification_attempt"


# ---------------------------------------------------------------------------
# O26: Helper for deciding when to escalate to test-send / bounce verification
# ---------------------------------------------------------------------------


def should_escalate_to_test_send(
    signals: VerificationSignals,
    *,
    verify_status: VerifyStatus,
    probe_hostile: bool,
    test_send_status: str | None,
) -> bool:
    """
    Decide whether this verification_result should be escalated to a
    bounce-based test-send path.

    Intended usage (pseudocode from your verification pipeline):

        verify_status, verify_reason = classify(signals, now=now)

        probe_hostile = mx_behavior.probing_hostile  # from O06-style stats
        test_send_status = row["test_send_status"]   # from verification_results

        if should_escalate_to_test_send(
            signals,
            verify_status=verify_status,
            probe_hostile=probe_hostile,
            test_send_status=test_send_status,
        ):
            token = test_send.request_test_send(conn, verification_result_id)
            enqueue_test_send_job(token, ...)

    Escalation rules:

      - Only escalate when the canonical outcome is "unknown_timeout".
      - Only for MX hosts marked probe-hostile (never returning 2xx/5xx).
      - Only when the RCPT outcome was tempfail / timeout / blocked / 4xx.
      - Only when no test-send has been requested yet:
            test_send_status in {None, "not_requested"}.

    This keeps escalation focused on providers that refuse to give a
    definitive RCPT answer but also avoids hammering the same mailbox.
    """
    if not probe_hostile:
        return False

    if verify_status != "unknown_timeout":
        return False

    # Avoid re-escalating rows where we've already queued/sent a test email
    # or processed a bounce.
    if test_send_status not in (None, "not_requested"):
        return False

    # Re-derive normalised RCPT category and flags so we can check that the
    # outcome really was a tempfail/timeout/blocked-style soft failure.
    rcpt_category = _norm_rcpt_category(_norm(signals.rcpt_category))
    rcpt_code = signals.rcpt_code

    _is_5xx, _is_2xx, is_4xx, _good_rcpt, soft_fail = _compute_rcpt_flags(
        rcpt_category=rcpt_category,
        rcpt_code=rcpt_code,
    )

    # We only escalate when the low-level path was a soft failure or 4xx.
    if not (soft_fail or is_4xx):
        return False

    return True
