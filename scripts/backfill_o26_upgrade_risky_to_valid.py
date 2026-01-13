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
import os
from collections.abc import Iterator
from contextlib import closing
from dataclasses import dataclass
from typing import Any

from src.db import get_conn
from src.verify.delivery_catchall import DeliveryCatchallStatus, should_upgrade_risky_to_valid


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


def _require_relations(conn, names: tuple[str, ...]) -> None:
    missing = [name for name in names if not _relation_exists(conn, name)]
    if not missing:
        return
    if len(missing) == 1:
        raise SystemExit(f"{missing[0]} table not found.")
    raise SystemExit(f"Missing required tables: {', '.join(missing)}")


def _require_company_domain_col(conn) -> str:
    company_domain_col = _pick_column(
        conn, "companies", ("domain", "domain_official", "official_domain")
    )
    if company_domain_col:
        return company_domain_col
    raise SystemExit("companies has no domain column (domain/domain_official/official_domain).")


@dataclass(frozen=True)
class _TenantFlags:
    vr: bool
    emails: bool
    companies: bool
    domain_resolutions: bool


def _tenant_flags(conn) -> _TenantFlags:
    return _TenantFlags(
        vr=_has_column(conn, "verification_results", "tenant_id"),
        emails=_has_column(conn, "emails", "tenant_id"),
        companies=_has_column(conn, "companies", "tenant_id"),
        domain_resolutions=_has_column(conn, "domain_resolutions", "tenant_id"),
    )


def _build_join_clauses(flags: _TenantFlags, company_domain_col: str) -> tuple[str, str, str]:
    join_e = "e.id = vr.email_id"
    if flags.vr and flags.emails:
        join_e += " AND e.tenant_id = vr.tenant_id"

    join_c = "c.id = e.company_id"
    if flags.emails and flags.companies:
        join_c += " AND c.tenant_id = e.tenant_id"

    join_dr = f"dr.domain = c.{company_domain_col}"
    if flags.domain_resolutions:
        if flags.companies:
            join_dr += " AND dr.tenant_id = c.tenant_id"
        elif flags.emails:
            join_dr += " AND dr.tenant_id = e.tenant_id"
        elif flags.vr:
            join_dr += " AND dr.tenant_id = vr.tenant_id"

    return join_e, join_c, join_dr


def _build_where_clause(tenant_id: str | None, flags: _TenantFlags) -> tuple[str, tuple[Any, ...]]:
    where_parts = [
        "vr.verify_status = 'risky_catch_all'",
        "vr.test_send_status IS NOT NULL",
    ]
    params: list[Any] = []

    if tenant_id:
        if flags.vr:
            where_parts.append("vr.tenant_id = %s")
            params.append(tenant_id)
        elif flags.emails:
            where_parts.append("e.tenant_id = %s")
            params.append(tenant_id)
        elif flags.companies:
            where_parts.append("c.tenant_id = %s")
            params.append(tenant_id)
        elif flags.domain_resolutions:
            where_parts.append("dr.tenant_id = %s")
            params.append(tenant_id)

    return " AND ".join(where_parts), tuple(params)


def _candidate_select_sql(vr_has_tenant: bool) -> str:
    tenant_col = "vr.tenant_id," if vr_has_tenant else ""
    return f"""
        SELECT
            vr.id,
            {tenant_col}
            vr.verify_status,
            vr.verify_reason,
            vr.test_send_status,
            vr.bounce_code,
            vr.bounce_reason,
            dr.delivery_catchall_status
    """


def _iter_upgrade_candidates(conn, tenant_id: str | None) -> Iterator[tuple[Any, ...]]:
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
    _require_relations(conn, ("verification_results", "emails", "companies", "domain_resolutions"))
    company_domain_col = _require_company_domain_col(conn)

    flags = _tenant_flags(conn)
    join_e, join_c, join_dr = _build_join_clauses(flags, company_domain_col)
    where_sql, params = _build_where_clause(tenant_id, flags)

    sql = (
        _candidate_select_sql(flags.vr)
        + f"""
        FROM verification_results AS vr
        JOIN emails e    ON {join_e}
        JOIN companies c ON {join_c}
        JOIN domain_resolutions dr ON {join_dr}
        WHERE {where_sql}
        """
    )

    with conn.cursor() as cur:
        cur.execute(sql, params)
        yield from cur.fetchall()


def backfill_upgrade_risky_to_valid(conn, tenant_id: str | None) -> int:
    """
    Apply the O26 upgrade policy to all eligible rows.

    Returns the number of verification_results rows updated.
    """
    vr_has_tenant = _has_column(conn, "verification_results", "tenant_id")

    updated = 0
    with conn:
        with conn.cursor() as cur:
            for row in _iter_upgrade_candidates(conn, tenant_id=tenant_id):
                if vr_has_tenant:
                    (
                        vr_id,
                        vr_tid,
                        verify_status,
                        _verify_reason,
                        test_send_status,
                        bounce_code,
                        bounce_reason,
                        domain_delivery_catchall_status,
                    ) = row
                else:
                    (
                        vr_id,
                        verify_status,
                        _verify_reason,
                        test_send_status,
                        bounce_code,
                        bounce_reason,
                        domain_delivery_catchall_status,
                    ) = row
                    vr_tid = None

                status: DeliveryCatchallStatus | None
                if domain_delivery_catchall_status is None:
                    status = None
                else:
                    status = domain_delivery_catchall_status  # type: ignore[assignment]

                if should_upgrade_risky_to_valid(
                    verify_status=verify_status,
                    domain_delivery_catchall_status=status,
                    test_send_status=test_send_status,
                    bounce_code=bounce_code,
                    bounce_reason=bounce_reason,
                ):
                    if vr_has_tenant:
                        cur.execute(
                            """
                            UPDATE verification_results
                            SET
                                verify_status = %s,
                                verify_reason = %s
                            WHERE id = %s AND tenant_id = %s
                            """,
                            ("valid", "no_bounce_after_test_send", vr_id, vr_tid),
                        )
                    else:
                        cur.execute(
                            """
                            UPDATE verification_results
                            SET
                                verify_status = %s,
                                verify_reason = %s
                            WHERE id = %s
                            """,
                            ("valid", "no_bounce_after_test_send", vr_id),
                        )
                    updated += cur.rowcount or 0

    return updated


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "O26 backfill: upgrade risky_catch_all → valid based on "
            "delivery-time catch-all evidence and test-send results."
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
        updated = backfill_upgrade_risky_to_valid(conn, tenant_id=args.tenant_id)

    print(f"O26: upgraded {updated} verification_results rows from risky_catch_all to valid.")


if __name__ == "__main__":
    main()
