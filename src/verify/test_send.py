# src/verify/test_send.py
from __future__ import annotations

import datetime as dt
import secrets
import sqlite3
from dataclasses import dataclass
from typing import Literal

from src.generate.patterns import PATTERN_PRIORITY
from src.generate.patterns import PATTERNS as CANON_PATTERNS
from src.ingest.normalize import normalize_split_parts

"""
Bounce-based verification helpers (O26).

This module centralizes all logic for:
  - tracking test-send status on verification_results
  - generating and resolving test-send tokens
  - applying bounce outcomes (hard/soft)
  - upgrading stale "sent" test-sends to "delivered_assumed"

It is intentionally DB-focused and does NOT send email itself.
Your workers / CLI scripts should:

  1) Call request_test_send() when you want to escalate a verification_result
     to a real test-send.
  2) Actually send the email using your SMTP/ESP, using an envelope sender like:
         bounce+{token}@yourdomain.com
  3) Call mark_test_send_sent() after the send succeeds, so we have a timestamp.
  4) When a bounce is processed (e.g. via scripts/import_bounces.py),
     call apply_bounce() with the parsed status.
  5) Periodically call assume_delivered_for_stale_test_sends() from a cron/worker
     to mark old "sent" rows as "delivered_assumed".
"""

TestSendStatus = Literal[
    "not_requested",
    "pending",
    "sent",
    "bounce_hard",
    "bounce_soft",
    "delivered_assumed",
]

HARD_BOUNCE_REASON_DEFAULT = "hard_bounce"
SOFT_BOUNCE_REASON_DEFAULT = "soft_bounce"
DELIVERED_ASSUMED_REASON = "no_bounce_after_test_send"


@dataclass(frozen=True)
class TestSendConfig:
    """
    Config for test-send behavior.

    This is kept intentionally small; if you need per-tenant settings later,
    you can thread a config instance down into the helpers.
    """

    # How long we wait before assuming "delivered" when no bounce has been seen.
    delivered_after: dt.timedelta = dt.timedelta(hours=24)

    # Whether we should upgrade verification_results.verify_status when we
    # conclude things from bounces / non-bounces.
    update_verify_status: bool = True


@dataclass(frozen=True)
class NextTestSendCandidate:
    """
    Description of the "next best" email candidate for test-send escalation
    for a given person + domain.

    This is intentionally small and DB-agnostic so it can be used from
    scripts/import_bounces.py and any future workers.
    """

    verification_result_id: int
    email_id: int
    email: str
    pattern: str | None


def _now_utc() -> dt.datetime:
    # Keep consistent with the rest of the project (UTC stored as ISO8601).
    return dt.datetime.utcnow().replace(microsecond=0)


def _iso(dt_value: dt.datetime | None) -> str | None:
    if dt_value is None:
        return None
    return dt_value.isoformat(timespec="seconds")


def _fetch_verification_id_by_token(
    conn: sqlite3.Connection,
    token: str,
) -> int | None:
    cur = conn.execute(
        """
        SELECT id
        FROM verification_results
        WHERE test_send_token = ?
        """,
        (token,),
    )
    row = cur.fetchone()
    return int(row[0]) if row else None


def _infer_pattern_for_email(
    local: str,
    first: str | None,
    last: str | None,
) -> str | None:
    """
    Best-effort guess: which canonical pattern generated this local-part?

    Uses the same normalization as R12 (normalize_split_parts) and tries
    PATTERN_PRIORITY first, then any remaining patterns from CANON_PATTERNS.
    """
    nf, nl = normalize_split_parts(first or "", last or "")
    if not (nf or nl):
        return None

    # Try known priority order first, then the remaining patterns
    ordered_keys: list[str] = list(PATTERN_PRIORITY) + [
        key for key in CANON_PATTERNS.keys() if key not in PATTERN_PRIORITY
    ]

    for key in ordered_keys:
        fn = CANON_PATTERNS.get(key)
        if fn is None:
            continue
        try:
            candidate_local = fn(nf, nl)
        except Exception:
            # Be defensive: if any legacy pattern raises, just skip it.
            continue
        if candidate_local == local:
            return key

    return None


def choose_next_test_send_candidate(
    conn: sqlite3.Connection,
    *,
    email_id: int,
) -> NextTestSendCandidate | None:
    """
    Given the email_id that just had a test-send bounce, look at all
    addresses for the same person + domain and pick the next best
    candidate that:

      - has latest verify_status in {"risky_catch_all", "unknown_timeout"}
      - has test_send_status in {NULL, "not_requested"}
      - is ranked by PATTERN_PRIORITY (then tie-break by local-part/email)

    Returns None if there is nothing left to try.

    This is DB-read-only; it does not modify any rows or send emails.
    """
    # 1) Find the person + domain for the bounced email_id.
    cur = conn.execute(
        """
        SELECT e.id AS email_id,
               e.email AS email,
               e.person_id AS person_id
        FROM emails AS e
        WHERE e.id = ?
        """,
        (int(email_id),),
    )
    row = cur.fetchone()
    if row is None:
        return None

    bounced_email_id, bounced_email, person_id = row
    if person_id is None or not bounced_email:
        return None

    email_str = str(bounced_email)
    if "@" not in email_str:
        return None
    dom = email_str.split("@", 1)[1].lower().strip()
    if not dom:
        return None

    # 2) Pull latest verification_result row for each email for this person+domain.
    cur2 = conn.execute(
        """
        WITH latest AS (
          SELECT email_id, MAX(id) AS id
          FROM verification_results
          GROUP BY email_id
        )
        SELECT
          e.id             AS email_id,
          e.email          AS email,
          p.first_name     AS first_name,
          p.last_name      AS last_name,
          vr.id            AS vr_id,
          vr.verify_status AS verify_status,
          vr.test_send_status AS test_send_status
        FROM emails AS e
        JOIN people AS p
          ON p.id = e.person_id
        JOIN latest
          ON latest.email_id = e.id
        JOIN verification_results AS vr
          ON vr.email_id = latest.email_id
         AND vr.id = latest.id
        WHERE p.id = ?
          AND lower(substr(e.email, instr(e.email, '@') + 1)) = ?
        """,
        (int(person_id), dom),
    )
    rows = cur2.fetchall()

    candidates: list[tuple[int, str, NextTestSendCandidate]] = []

    for r in rows:
        (
            eid,
            em,
            first_name,
            last_name,
            vr_id,
            verify_status,
            test_send_status,
        ) = r

        vs = (verify_status or "").strip().lower()
        ts = (test_send_status or "").strip().lower()

        # Only consider weak / ambiguous candidates.
        if vs not in {"risky_catch_all", "unknown_timeout"}:
            continue

        # Skip anything that already has a test-send lifecycle beyond "not_requested".
        # (pending / sent / bounced / delivered_assumed are all excluded here.)
        if ts and ts != "not_requested":
            continue

        em_str = str(em)
        if "@" not in em_str:
            continue

        local = em_str.split("@", 1)[0].lower().strip()
        if not local:
            continue

        pat = _infer_pattern_for_email(local, first_name, last_name)

        if pat in PATTERN_PRIORITY:
            prio = PATTERN_PRIORITY.index(pat)  # lower index = higher priority
        else:
            # Unknown or unclassified pattern → push to the back.
            prio = len(PATTERN_PRIORITY) + 10

        cand = NextTestSendCandidate(
            verification_result_id=int(vr_id),
            email_id=int(eid),
            email=em_str,
            pattern=pat,
        )
        # Sort by pattern priority, then local-part for determinism.
        candidates.append((prio, local, cand))

    if not candidates:
        return None

    candidates.sort(key=lambda t: (t[0], t[1]))
    return candidates[0][2]


def generate_test_send_token(verification_result_id: int) -> str:
    """
    Generate a unique, opaque token for a given verification_result_id.

    The token is safe to embed in an email address, e.g.:

        bounce+{token}@yourdomain.com
    """
    random_part = secrets.token_urlsafe(16)
    return f"vr{verification_result_id}-{random_part}"


def request_test_send(
    conn: sqlite3.Connection,
    verification_result_id: int,
) -> str:
    """
    Mark a verification_result as pending test-send and assign a token.

    Returns the generated token, which the caller should embed into the
    bounce address (bounce+{token}@yourdomain.com).

    This is idempotent-ish: if a token already exists, we reuse it but
    ensure status is "pending".
    """
    cur = conn.execute(
        """
        SELECT test_send_token, test_send_status
        FROM verification_results
        WHERE id = ?
        """,
        (verification_result_id,),
    )
    row = cur.fetchone()
    if row is None:
        raise ValueError(f"verification_results id={verification_result_id} not found")

    existing_token, existing_status = row

    if existing_token:
        token = str(existing_token)
    else:
        token = generate_test_send_token(verification_result_id)

    conn.execute(
        """
        UPDATE verification_results
        SET test_send_token = ?,
            test_send_status = 'pending',
            test_send_at = NULL
        WHERE id = ?
        """,
        (token, verification_result_id),
    )
    conn.commit()
    return token


def mark_test_send_sent(
    conn: sqlite3.Connection,
    token: str,
    sent_at: dt.datetime | None = None,
) -> None:
    """
    Mark a test-send as actually sent (after SMTP/ESP succeeds).

    This sets:
        test_send_status = 'sent'
        test_send_at     = sent_at (or now)
    """
    if sent_at is None:
        sent_at = _now_utc()

    verification_id = _fetch_verification_id_by_token(conn, token)
    if verification_id is None:
        # For robustness, treat this as a no-op (callers can log the miss).
        return

    conn.execute(
        """
        UPDATE verification_results
        SET test_send_status = 'sent',
            test_send_at = ?
        WHERE id = ?
        """,
        (_iso(sent_at), verification_id),
    )
    conn.commit()


def apply_bounce(
    conn: sqlite3.Connection,
    token: str,
    status_code: str | None,
    reason: str | None,
    *,
    is_hard: bool,
    cfg: TestSendConfig | None = None,
) -> None:
    """
    Apply a bounce outcome (hard or soft) for the given test-send token.

    Typical usage from scripts/import_bounces.py:

        apply_bounce(
            conn,
            token=parsed_token,
            status_code=dsn_status,
            reason=parsed_reason,
            is_hard=is_hard_bounce,
        )
    """
    if cfg is None:
        cfg = TestSendConfig()

    verification_id = _fetch_verification_id_by_token(conn, token)
    if verification_id is None:
        # Unknown token: ignore; caller can log if desired.
        return

    bounce_reason = reason or (
        HARD_BOUNCE_REASON_DEFAULT if is_hard else SOFT_BOUNCE_REASON_DEFAULT
    )
    bounce_code = status_code or ""

    new_status: TestSendStatus = "bounce_hard" if is_hard else "bounce_soft"

    # Update test-send / bounce fields.
    conn.execute(
        """
        UPDATE verification_results
        SET test_send_status = ?,
            bounce_code = ?,
            bounce_reason = ?
        WHERE id = ?
        """,
        (new_status, bounce_code, bounce_reason, verification_id),
    )

    if cfg.update_verify_status:
        if is_hard:
            # Hard bounce → this mailbox is almost certainly invalid.
            conn.execute(
                """
                UPDATE verification_results
                SET verify_status = 'invalid',
                    verify_reason = 'hard_bounce_user_unknown'
                WHERE id = ?
                """,
                (verification_id,),
            )
        else:
            # Soft bounce: we *do not* upgrade to invalid. We keep whatever
            # verify_status was (likely unknown_timeout or risky_catch_all).
            # You could refine this later by reason category.
            pass

    conn.commit()


def assume_delivered_for_stale_test_sends(
    conn: sqlite3.Connection,
    *,
    cfg: TestSendConfig | None = None,
    now: dt.datetime | None = None,
) -> int:
    """
    Mark "sent" test-sends older than cfg.delivered_after as delivered_assumed.

    This sets:
        test_send_status = 'delivered_assumed'
        (optionally) verify_status / verify_reason:

        - If verify_status is NULL, 'unknown_timeout', or 'risky_catch_all',
          we upgrade to 'valid' with verify_reason = DELIVERED_ASSUMED_REASON.
        - Otherwise we leave verify_status as-is (e.g. already valid/invalid).
    """
    if cfg is None:
        cfg = TestSendConfig()
    if now is None:
        now = _now_utc()

    cutoff = now - cfg.delivered_after
    cutoff_iso = _iso(cutoff)

    # Find candidate rows up front so we can count updates accurately.
    cur = conn.execute(
        """
        SELECT id, verify_status
        FROM verification_results
        WHERE test_send_status = 'sent'
          AND test_send_at IS NOT NULL
          AND test_send_at <= ?
        """,
        (cutoff_iso,),
    )
    rows = cur.fetchall()
    if not rows:
        return 0

    updated = 0
    for verif_id, verify_status in rows:
        verif_id_int = int(verif_id)

        # Always update the test_send_status.
        conn.execute(
            """
            UPDATE verification_results
            SET test_send_status = 'delivered_assumed'
            WHERE id = ?
            """,
            (verif_id_int,),
        )

        if cfg.update_verify_status:
            # Upgrade when we previously had no strong negative signal.
            # This now includes risky_catch_all, because a real test-send
            # that never bounces is strong evidence the mailbox is valid.
            if (
                verify_status is None
                or verify_status == "unknown_timeout"
                or verify_status == "risky_catch_all"
            ):
                conn.execute(
                    """
                    UPDATE verification_results
                    SET verify_status = 'valid',
                        verify_reason = ?
                    WHERE id = ?
                    """,
                    (DELIVERED_ASSUMED_REASON, verif_id_int),
                )

        updated += 1

    conn.commit()
    return updated
