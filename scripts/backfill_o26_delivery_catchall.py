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
import os
from contextlib import closing
from typing import Any

from src.db import get_conn
from src.verify.delivery_catchall import (
    DomainDeliveryEvidence,
    classify_domain_delivery_catchall,
    should_count_as_bad_invalid_mailbox,
    should_count_as_good_real_mailbox,
)


def _iso_utc_now() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _has_column(conn, table: str, column: str) -> bool:
    sql = """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = %s
          AND column_name = %s
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (table, column))
        return cur.fetchone() is not None


def _pick_column(conn, table: str, candidates: tuple[str, ...]) -> str | None:
    for c in candidates:
        if _has_column(conn, table, c):
            return c
    return None


def _relation_exists(conn, name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (name,))
        row = cur.fetchone()
    return bool(row and row[0] is not None)


def _load_domain_evidence(
    conn, tenant_id: str | None
) -> dict[tuple[str, str], DomainDeliveryEvidence]:
    """
    Build a DomainDeliveryEvidence instance for each (tenant_id, domain) that has
    at least one verification_result row with a non-null test_send_status.

    We intentionally scan *all* such rows (no MAX(id) per email) so that older
    test-sends still contribute evidence at the domain level.
    """
    if not _relation_exists(conn, "verification_results"):
        raise SystemExit("verification_results table not found.")
    if not _relation_exists(conn, "emails"):
        raise SystemExit("emails table not found.")
    if not _relation_exists(conn, "companies"):
        raise SystemExit("companies table not found.")

    company_domain_col = _pick_column(
        conn, "companies", ("domain", "domain_official", "official_domain")
    )
    if not company_domain_col:
        raise SystemExit("companies has no domain column (domain/domain_official/official_domain).")

    vr_has_tenant = _has_column(conn, "verification_results", "tenant_id")
    e_has_tenant = _has_column(conn, "emails", "tenant_id")
    c_has_tenant = _has_column(conn, "companies", "tenant_id")

    # Choose the best tenant source available for stable grouping.
    tenant_expr = "COALESCE(vr.tenant_id, e.tenant_id, c.tenant_id, 'dev')"
    if not (vr_has_tenant or e_has_tenant or c_has_tenant):
        tenant_expr = "'dev'"

    # Build join predicates with tenant safety when possible.
    join_e = "e.id = vr.email_id"
    if vr_has_tenant and e_has_tenant:
        join_e += " AND e.tenant_id = vr.tenant_id"

    join_c = "c.id = e.company_id"
    if e_has_tenant and c_has_tenant:
        join_c += " AND c.tenant_id = e.tenant_id"

    where_parts = ["vr.test_send_status IS NOT NULL"]
    params: list[Any] = []

    if tenant_id:
        # Only apply tenant filter if we can meaningfully scope it.
        if vr_has_tenant:
            where_parts.append("vr.tenant_id = %s")
            params.append(tenant_id)
        elif e_has_tenant:
            where_parts.append("e.tenant_id = %s")
            params.append(tenant_id)
        elif c_has_tenant:
            where_parts.append("c.tenant_id = %s")
            params.append(tenant_id)

    sql = f"""
        SELECT
            c.{company_domain_col} AS domain,
            vr.verify_status,
            vr.verify_reason,
            vr.test_send_status,
            vr.bounce_code,
            vr.bounce_reason,
            {tenant_expr} AS tenant_id
        FROM verification_results AS vr
        JOIN emails e    ON {join_e}
        JOIN companies c ON {join_c}
        WHERE {" AND ".join(where_parts)}
    """

    evidence_by_key: dict[tuple[str, str], DomainDeliveryEvidence] = {}

    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        for (
            domain,
            verify_status,
            verify_reason,
            test_send_status,
            bounce_code,
            bounce_reason,
            tid,
        ) in cur.fetchall():
            if not domain:
                continue
            tid_s = str(tid or "dev")
            dom_s = str(domain)

            ev = evidence_by_key.setdefault((tid_s, dom_s), DomainDeliveryEvidence())

            # A-side: "good real" mailboxes.
            if should_count_as_good_real_mailbox(test_send_status, bounce_code, bounce_reason):
                ev.has_good_real = True

            # B-side: "bad invalid" mailboxes.
            bad_via_bounce = should_count_as_bad_invalid_mailbox(
                test_send_status, bounce_code, bounce_reason
            )

            # Some rows may have no bounce_code/bounce_reason but encode the outcome in verify_reason.
            bad_via_reason = (
                verify_status == "invalid"
                and verify_reason == "hard_bounce_user_unknown"
                and test_send_status == "bounce_hard"
            )

            if bad_via_bounce or bad_via_reason:
                ev.has_bad_invalid = True

    return evidence_by_key


def backfill_delivery_catchall_status(conn, tenant_id: str | None) -> int:
    """
    Compute and persist delivery_catchall_status for all domains where we have
    any test-send evidence.

    Returns the number of domain_resolutions rows updated.
    """
    if not _relation_exists(conn, "domain_resolutions"):
        raise SystemExit("domain_resolutions table not found.")

    if not _has_column(conn, "domain_resolutions", "delivery_catchall_status"):
        raise SystemExit("domain_resolutions.delivery_catchall_status column not found.")
    if not _has_column(conn, "domain_resolutions", "delivery_catchall_checked_at"):
        raise SystemExit("domain_resolutions.delivery_catchall_checked_at column not found.")

    dr_has_tenant = _has_column(conn, "domain_resolutions", "tenant_id")

    evidence_by_key = _load_domain_evidence(conn, tenant_id=tenant_id)
    if not evidence_by_key:
        return 0

    now = _iso_utc_now()

    updated_rows = 0
    with conn:
        with conn.cursor() as cur:
            for (tid, domain), evidence in evidence_by_key.items():
                status = classify_domain_delivery_catchall(evidence)

                if dr_has_tenant:
                    if tenant_id and tid != tenant_id:
                        # If we were able to filter at source but still have mixed tid (edge cases),
                        # do not write outside the requested tenant.
                        continue

                    cur.execute(
                        """
                        UPDATE domain_resolutions
                        SET
                            delivery_catchall_status = %s,
                            delivery_catchall_checked_at = %s
                        WHERE tenant_id = %s AND domain = %s
                        """,
                        (status, now, tid, domain),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE domain_resolutions
                        SET
                            delivery_catchall_status = %s,
                            delivery_catchall_checked_at = %s
                        WHERE domain = %s
                        """,
                        (status, now, domain),
                    )

                updated_rows += cur.rowcount or 0

    return updated_rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "O26 backfill: compute domain-level delivery_catchall_status based on "
            "test-send (bounce) evidence."
        )
    )
    parser.add_argument(
        "--tenant-id",
        dest="tenant_id",
        default=None,
        help="Optional tenant_id to scope the backfill to one tenant. Default: all tenants.",
    )
    parser.add_argument(
        "--dsn",
        dest="dsn",
        default=None,
        help="Optional Postgres DSN/URL override. If provided, sets DATABASE_URL for this run.",
    )
    args = parser.parse_args()

    if args.dsn:
        os.environ["DATABASE_URL"] = args.dsn

    with closing(get_conn()) as conn:
        updated = backfill_delivery_catchall_status(conn, tenant_id=args.tenant_id)

    print(f"O26: updated delivery_catchall_status for {updated} domain_resolutions rows.")


if __name__ == "__main__":
    main()
