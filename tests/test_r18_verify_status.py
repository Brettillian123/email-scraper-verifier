from __future__ import annotations

from datetime import datetime, timedelta

from src.verify.status import VerificationSignals, classify


def _iso(dt: datetime) -> str:
    """Helper to emit the same simple ISO-8601 UTC format used in the app."""
    return dt.replace(microsecond=0).isoformat() + "Z"


def test_non_catchall_2xx_is_valid() -> None:
    """
    Non-catch-all domain + 2xx RCPT → valid / rcpt_2xx_non_catchall.
    """
    now = datetime(2025, 1, 1, 12, 0, 0)

    signals = VerificationSignals(
        rcpt_category="deliverable",
        rcpt_code=250,
        rcpt_msg=None,
        catch_all_status="not_catch_all",
        fallback_status=None,
        mx_host="mx.example.test",
        verified_at=_iso(now),
    )

    status, reason = classify(signals, now=now)

    assert status == "valid"
    assert reason == "rcpt_2xx_non_catchall"


def test_catchall_2xx_is_risky_catch_all() -> None:
    """
    Catch-all domain + 2xx RCPT → risky_catch_all / rcpt_2xx_catchall.
    """
    now = datetime(2025, 1, 1, 12, 0, 0)

    signals = VerificationSignals(
        rcpt_category="deliverable",
        rcpt_code=250,
        rcpt_msg=None,
        catch_all_status="catch_all",
        fallback_status=None,
        mx_host="mx.catchall.test",
        verified_at=_iso(now),
    )

    status, reason = classify(signals, now=now)

    assert status == "risky_catch_all"
    assert reason == "rcpt_2xx_catchall"


def test_550_user_unknown_is_invalid() -> None:
    """
    rcpt_category='undeliverable' / 550 → invalid / rcpt_5xx_user_unknown.
    """
    now = datetime(2025, 1, 1, 12, 0, 0)

    signals = VerificationSignals(
        rcpt_category="undeliverable",
        rcpt_code=550,
        rcpt_msg=None,
        catch_all_status="not_catch_all",
        fallback_status=None,
        mx_host="mx.invalid.test",
        verified_at=_iso(now),
    )

    status, reason = classify(signals, now=now)

    assert status == "invalid"
    assert reason == "rcpt_5xx_user_unknown"


def test_timeout_without_fallback_is_unknown_timeout() -> None:
    """
    Timeout / tempfail with no fallback → unknown_timeout / tempfail_or_timeout.
    """
    now = datetime(2025, 1, 1, 12, 0, 0)

    signals = VerificationSignals(
        rcpt_category="timeout",
        rcpt_code=421,
        rcpt_msg=None,
        catch_all_status="unknown",
        fallback_status=None,
        mx_host="mx.slow.test",
        verified_at=_iso(now),
    )

    status, reason = classify(signals, now=now)

    assert status == "unknown_timeout"
    assert reason == "tempfail_or_timeout"


def test_tempfail_with_fallback_valid_is_valid() -> None:
    """
    Tempfail + fallback_status='deliverable' → valid / fallback_valid_after_tempfail.
    """
    now = datetime(2025, 1, 1, 12, 0, 0)

    signals = VerificationSignals(
        rcpt_category="tempfail",
        rcpt_code=421,
        rcpt_msg=None,
        catch_all_status="unknown",
        fallback_status="deliverable",
        mx_host="mx.temp.test",
        verified_at=_iso(now),
    )

    status, reason = classify(signals, now=now)

    assert status == "valid"
    assert reason == "fallback_valid_after_tempfail"


def test_stale_result_ttl_exceeded_yields_unknown_timeout() -> None:
    """
    verified_at older than ttl_days → unknown_timeout / stale_result_ttl_exceeded
    even if the original RCPT looked good.
    """
    now = datetime(2025, 1, 1, 12, 0, 0)
    stale_ts = now - timedelta(days=181)  # > 90-day TTL

    signals = VerificationSignals(
        rcpt_category="deliverable",
        rcpt_code=250,
        rcpt_msg=None,
        catch_all_status="not_catch_all",
        fallback_status=None,
        mx_host="mx.stale.test",
        verified_at=_iso(stale_ts),
    )

    status, reason = classify(signals, now=now, ttl_days=90)

    assert status == "unknown_timeout"
    assert reason == "stale_result_ttl_exceeded"


def test_fallback_only_valid_no_smtp() -> None:
    """
    No RCPT response but fallback says deliverable → valid / fallback_valid_no_smtp.
    """
    now = datetime(2025, 1, 1, 12, 0, 0)

    signals = VerificationSignals(
        rcpt_category=None,
        rcpt_code=None,
        rcpt_msg=None,
        catch_all_status="unknown",
        fallback_status="deliverable",
        mx_host="mx.vendor-only.test",
        verified_at=_iso(now),
    )

    status, reason = classify(signals, now=now)

    assert status == "valid"
    assert reason == "fallback_valid_no_smtp"
