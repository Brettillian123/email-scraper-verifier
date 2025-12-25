from __future__ import annotations

"""
O26 — Upgrade risky_catch_all → valid based on delivery-time evidence.

This script uses the domain-level delivery_catchall_status computed by
backfill_o26_delivery_catchall.py together with per-address test-send data
to upgrade rows like:

    verify_status = 'risky_catch_all'
    test_send_status IN ('sent', 'delivered_assumed')
    (no 5.1.x "user unknown" bounce)

to:

    verify_status = 'valid'
    verify_reason = 'no_bounce_after_test_send'

but ONLY when the domain has:

    delivery_catchall_status = 'not_catchall_proven'

It is idempotent: once a row has been upgraded to 'valid', subsequent runs
will skip it because verify_status != 'risky_catch_all'.

Important: we intentionally scan *all* verification_results rows that are
currently risky_catch_all (with a non-null test_send_status), not just the
latest per email. Any such row is eligible for upgrade if the policy says so.
"""

import argparse
import sqlite3
from pathlib import Path

from src.verify.delivery_catchall import (
    DeliveryCatchallStatus,
    should_upgrade_risky_to_valid,
)


def _ensure_db_exists(db_path: str) -> None:
    p = Path(db_path)
    if not p.exists():
        raise SystemExit(f"Database not found: {db_path}")


def _iter_upgrade_candidates(conn: sqlite3.Connection):
    """
    Yield verification_results rows that are candidates for O26 upgrade.

    We include any row where:

      - verify_status = 'risky_catch_all'
      - test_send_status IS NOT NULL

    and join to domain_resolutions to see the domain's
    delivery_catchall_status.

    We do NOT restrict to the latest row per email: historical rows with
    test-sends are still meaningful, and upgrading them from risky_catch_all
    to valid is safe and idempotent.
    """
    sql = """
        SELECT
            vr.id,
            vr.verify_status,
            vr.verify_reason,
            vr.test_send_status,
            vr.bounce_code,
            vr.bounce_reason,
            dr.delivery_catchall_status
        FROM verification_results AS vr
        JOIN emails e    ON e.id = vr.email_id
        JOIN companies c ON c.id = e.company_id
        JOIN domain_resolutions dr ON dr.domain = c.domain
        WHERE vr.verify_status = 'risky_catch_all'
          AND vr.test_send_status IS NOT NULL
    """

    cur = conn.execute(sql)
    for row in cur.fetchall():
        (
            vr_id,
            verify_status,
            verify_reason,
            test_send_status,
            bounce_code,
            bounce_reason,
            domain_delivery_catchall_status,
        ) = row

        # Normalize domain_delivery_catchall_status to the literal type.
        status: DeliveryCatchallStatus | None
        if domain_delivery_catchall_status is None:
            status = None
        else:
            status = domain_delivery_catchall_status  # type: ignore[assignment]

        yield (
            vr_id,
            verify_status,
            verify_reason,
            test_send_status,
            bounce_code,
            bounce_reason,
            status,
        )


def backfill_upgrade_risky_to_valid(conn: sqlite3.Connection) -> int:
    """
    Apply the O26 upgrade policy to all eligible rows.

    Returns the number of verification_results rows updated.
    """
    updated = 0

    for (
        vr_id,
        verify_status,
        _verify_reason,
        test_send_status,
        bounce_code,
        bounce_reason,
        domain_delivery_catchall_status,
    ) in _iter_upgrade_candidates(conn):
        if should_upgrade_risky_to_valid(
            verify_status=verify_status,
            domain_delivery_catchall_status=domain_delivery_catchall_status,
            test_send_status=test_send_status,
            bounce_code=bounce_code,
            bounce_reason=bounce_reason,
        ):
            conn.execute(
                """
                UPDATE verification_results
                SET
                    verify_status = ?,
                    verify_reason = ?
                WHERE id = ?
                """,
                ("valid", "no_bounce_after_test_send", vr_id),
            )
            updated += 1

    conn.commit()
    return updated


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "O26 backfill: upgrade risky_catch_all → valid based on "
            "delivery-time catch-all evidence and test-send results."
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
        updated = backfill_upgrade_risky_to_valid(conn)
    finally:
        conn.close()

    print(f"O26: upgraded {updated} verification_results rows from risky_catch_all to valid.")


if __name__ == "__main__":
    main()
