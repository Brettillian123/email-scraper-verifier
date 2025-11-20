"""
R18 — Canonical verification status classifier.

Takes low-level signals from:
  - R16 SMTP RCPT probe (rcpt_category / code / msg)
  - R17 catch-all probe (domain-level catch_all_status)
  - O07 fallback vendor (fallback_status, raw payload)
and emits a single, canonical verify_status + verify_reason.

Intended usage:
  - Build a VerificationSignals instance from DB fields.
  - Call classify(signals, now=datetime.utcnow()).
  - Persist verify_status / verify_reason / verified_mx / verified_at
    back onto verification_results.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal

VerifyStatus = Literal["valid", "risky_catch_all", "invalid", "unknown_timeout"]


@dataclass
class VerificationSignals:
    """
    Inputs for R18 verification classification.

    All fields are intentionally simple primitives so this type can be
    used directly with rows from verification_results + domain_resolutions.
    """

    # "deliverable" | "undeliverable" | "tempfail" | "timeout"
    # | "blocked" | ...
    rcpt_category: str | None
    rcpt_code: int | None  # 250, 550, 421, etc.
    rcpt_msg: bytes | None  # raw/decoded SMTP message (optional)

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
    """
    if not good_rcpt:
        return None

    if catch_all_status == "not_catch_all":
        return "valid", "rcpt_2xx_non_catchall"

    if catch_all_status == "catch_all":
        if fallback_status == "undeliverable":
            return "invalid", "rcpt_2xx_catchall_fallback_invalid"
        # Keep as risky even if vendor says deliverable; domain-level
        # catch-all is a structural risk.
        return "risky_catch_all", "rcpt_2xx_catchall"

    if catch_all_status in {None, "unknown"}:
        if fallback_status == "deliverable":
            return "valid", "rcpt_2xx_unknown_catchall_fallback_valid"
        return "risky_catch_all", "rcpt_2xx_unknown_catchall"

    return None


def _classify_soft_fail(
    *,
    soft_fail: bool,
    fallback_status: str | None,
) -> tuple[VerifyStatus, str] | None:
    """
    Handle tempfail / timeout / blocked outcomes combined with fallback.
    """
    if not soft_fail:
        return None

    if fallback_status == "deliverable":
        return "valid", "fallback_valid_after_tempfail"
    if fallback_status == "undeliverable":
        return "invalid", "fallback_invalid_after_tempfail"
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
         - non-catch-all → valid
         - catch-all → risky
         - unknown → risky or valid with fallback.
      4. Tempfail / timeout / blocked, combined with fallback.
      5. Fallback-only classifications.
      6. No strong signals → unknown_timeout / no_verification_attempt.
    """
    rcpt_category = _norm(signals.rcpt_category)
    catch_all_status = _norm(signals.catch_all_status)
    fallback_status = _norm(signals.fallback_status)
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
    # 3) Hard invalids — 5xx / undeliverable
    # ------------------------------------------------------------------
    result = _classify_hard_invalid(
        rcpt_category=rcpt_category,
        is_5xx=is_5xx,
        fallback_status=fallback_status,
    )
    if result is not None:
        return result

    # ------------------------------------------------------------------
    # 4) Good RCPT paths (2xx)
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
