from __future__ import annotations

"""
O26 — Backfill delivery-time catch-all status per domain.

This script:

  1) Scans all verification_results rows that have a non-null test_send_status,
     joined with emails/companies to get the domain.
  2) Aggregates DomainDeliveryEvidence per domain using the O26 policy helpers.
  3) Writes delivery_catchall_status + delivery_catchall_checked_at into
     domain_resolutions.

It is idempotent: running it multiple times will simply recompute the same
statuses from the current verification_results/bounce data.

NOTE: We deliberately consider *all* historical verification_results rows with
test-sends, not just the latest per email. Domain-level delivery behavior is
a property of the domain, and any past A/B evidence (good real + bad invalid)
is enough to prove "not_catchall_proven" for that domain.
"""

import argparse
import datetime as dt
import sqlite3
from pathlib import Path

from src.verify.delivery_catchall import (
    DomainDeliveryEvidence,
    classify_domain_delivery_catchall,
    should_count_as_bad_invalid_mailbox,
    should_count_as_good_real_mailbox,
)


def _ensure_db_exists(db_path: str) -> None:
    p = Path(db_path)
    if not p.exists():
        raise SystemExit(f"Database not found: {db_path}")


def _load_domain_evidence(conn: sqlite3.Connection) -> dict[str, DomainDeliveryEvidence]:
    """
    Build a DomainDeliveryEvidence instance for each domain that has at least one
    verification_result row with a non-null test_send_status.

    We intentionally scan *all* such rows (no MAX(id) per email) so that older
    test-sends still contribute evidence at the domain level.
    """
    sql = """
        SELECT
            c.domain,
            vr.verify_status,
            vr.verify_reason,
            vr.test_send_status,
            vr.bounce_code,
            vr.bounce_reason
        FROM verification_results AS vr
        JOIN emails e    ON e.id = vr.email_id
        JOIN companies c ON c.id = e.company_id
        WHERE vr.test_send_status IS NOT NULL
    """

    evidence_by_domain: dict[str, DomainDeliveryEvidence] = {}

    cur = conn.execute(sql)
    for (
        domain,
        verify_status,
        verify_reason,
        test_send_status,
        bounce_code,
        bounce_reason,
    ) in cur.fetchall():
        ev = evidence_by_domain.setdefault(domain, DomainDeliveryEvidence())

        # A-side: "good real" mailboxes.
        if should_count_as_good_real_mailbox(test_send_status, bounce_code, bounce_reason):
            ev.has_good_real = True

        # B-side: "bad invalid" mailboxes.
        bad_via_bounce = should_count_as_bad_invalid_mailbox(
            test_send_status, bounce_code, bounce_reason
        )

        # Some rows (like your crestwell fake) may have no bounce_code /
        # bounce_reason but *do* encode the user-unknown outcome in verify_reason.
        bad_via_reason = (
            verify_status == "invalid"
            and verify_reason == "hard_bounce_user_unknown"
            and test_send_status == "bounce_hard"
        )

        if bad_via_bounce or bad_via_reason:
            ev.has_bad_invalid = True

    return evidence_by_domain


def backfill_delivery_catchall_status(conn: sqlite3.Connection) -> int:
    """
    Compute and persist delivery_catchall_status for all domains where we have
    any test-send evidence.

    Returns the number of domain_resolutions rows updated.
    """
    evidence_by_domain = _load_domain_evidence(conn)
    if not evidence_by_domain:
        return 0

    now = dt.datetime.utcnow().isoformat(timespec="seconds")

    updated_rows = 0
    for domain, evidence in evidence_by_domain.items():
        status = classify_domain_delivery_catchall(evidence)

        # Only touch domain_resolutions rows that actually exist for this domain.
        cur = conn.execute(
            """
            UPDATE domain_resolutions
            SET
                delivery_catchall_status = ?,
                delivery_catchall_checked_at = ?
            WHERE domain = ?
            """,
            (status, now, domain),
        )
        updated_rows += cur.rowcount

    conn.commit()
    return updated_rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "O26 backfill: compute domain-level delivery_catchall_status based on "
            "test-send (bounce) evidence."
        )
    )
    parser.add_argument(
        "--db",
        dest="db_path",
        default="data/dev.db",
        help="Path to the SQLite database (default: data/dev.db).",
    )
    args = parser.parse_args()

    _ensure_db_exists(args.db_path)

    conn = sqlite3.connect(args.db_path)
    try:
        updated = backfill_delivery_catchall_status(conn)
    finally:
        conn.close()

    print(f"O26: updated delivery_catchall_status for {updated} domain_resolutions rows.")


if __name__ == "__main__":
    main()
