from __future__ import annotations

"""
O26 demo — queue test-sends for "unclear" domains.

This script is intended as a *demo* driver on top of the O26 test-send
infrastructure you already have:

  - It looks for "unclear" domains where the latest verification_results
    per email are still ambiguous (risky_catch_all / unknown_timeout)
    and have NOT yet had a test-send requested.

  - For each such domain, it selects a small number of "real" addresses
    to escalate to a real test-send by marking their latest
    verification_results rows with a test_send_token, test_send_at,
    and test_send_status = 'requested'.

  - Your existing SES sender / worker is then responsible for:
        1) Picking up rows where test_send_status = 'requested'
        2) Actually sending emails using bounce+{token}@iqverifier.xyz
        3) Marking test_send_status = 'sent' when successful
        4) Letting import_bounces.py + O26 backfills handle the rest

This script does NOT send any email itself. It is purely DB-facing.
"""

import argparse
import secrets
import sqlite3
from dataclasses import dataclass
from pathlib import Path


@dataclass
class RealCandidate:
    verif_id: int
    email: str
    domain: str


def _ensure_db_exists(db_path: str) -> None:
    p = Path(db_path)
    if not p.exists():
        raise SystemExit(f"Database not found: {db_path}")


def _fetch_unclear_domains(
    conn: sqlite3.Connection,
    max_domains: int | None = None,
    only_domain: str | None = None,
) -> list[str]:
    """
    Return a list of domains that have at least one "unclear" latest
    verification_results row:

      - latest.verify_status IN ('risky_catch_all', 'unknown_timeout')
      - latest.test_send_status IS NULL or 'not_requested'

    If only_domain is provided, we restrict to that domain only (and
    return [] if it has no unclear rows).
    """
    params: list[object] = []
    limit_clause = ""
    domain_clause = ""

    if only_domain:
        domain_clause = "AND c.domain = ?"
        params.append(only_domain)

    if max_domains is not None and not only_domain:
        limit_clause = "LIMIT ?"
        params.append(max_domains)

    sql = f"""
        WITH latest AS (
            SELECT vr.*
            FROM verification_results AS vr
            JOIN (
                SELECT email_id, MAX(id) AS max_id
                FROM verification_results
                GROUP BY email_id
            ) AS lv ON lv.max_id = vr.id
        ),
        unclear_emails AS (
            SELECT
                e.email,
                c.domain,
                latest.verify_status,
                latest.verify_reason,
                latest.test_send_status
            FROM latest
            JOIN emails     AS e ON e.id = latest.email_id
            JOIN companies  AS c ON c.id = e.company_id
            WHERE latest.verify_status IN ('risky_catch_all', 'unknown_timeout')
              AND (latest.test_send_status IS NULL
                   OR latest.test_send_status = 'not_requested')
              {domain_clause}
        )
        SELECT DISTINCT domain
        FROM unclear_emails
        ORDER BY domain
        {limit_clause}
    """

    cur = conn.execute(sql, params)
    rows = cur.fetchall()
    return [row[0] for row in rows]


def _fetch_real_candidates_for_domain(
    conn: sqlite3.Connection,
    domain: str,
    per_domain_real: int,
) -> list[RealCandidate]:
    """
    For a given domain, return up to per_domain_real "real" email
    candidates whose latest verification_results rows are still unclear
    and have not yet had a test-send requested.

    We *prioritize* risky_catch_all rows, because O26's upgrade logic
    is specifically designed to turn those into valid based on
    delivery-time evidence. If there are no risky_catch_all rows, we
    fall back to unknown_timeout rows so the demo can still drive
    test-sends for flaky domains.

    We filter out obviously synthetic / bad locals (bounce, noreply,
    test, example, simulator) to bias towards real leads.
    """

    base_sql = """
        WITH latest AS (
            SELECT vr.*
            FROM verification_results AS vr
            JOIN (
                SELECT email_id, MAX(id) AS max_id
                FROM verification_results
                GROUP BY email_id
            ) AS lv ON lv.max_id = vr.id
        )
        SELECT
            latest.id      AS verif_id,
            e.email        AS email,
            c.domain       AS domain
        FROM latest
        JOIN emails     AS e ON e.id = latest.email_id
        JOIN companies  AS c ON c.id = e.company_id
        WHERE c.domain = ?
          AND latest.verify_status = ?
          AND (latest.test_send_status IS NULL
               OR latest.test_send_status = 'not_requested')
          AND e.email NOT LIKE '%bounce%'
          AND e.email NOT LIKE '%no-reply%'
          AND e.email NOT LIKE '%noreply%'
          AND e.email NOT LIKE '%test%'
          AND e.email NOT LIKE '%example%'
          AND e.email NOT LIKE '%simulator%'
        ORDER BY latest.id ASC
        LIMIT ?
    """

    # 1) Try to get risky_catch_all rows first (these are the ones O26 upgrades).
    cur = conn.execute(base_sql, (domain, "risky_catch_all", per_domain_real))
    rows = cur.fetchall()

    # 2) If none, fall back to unknown_timeout so the demo can still drive
    #    test-sends for flaky domains, even though O26 won't "upgrade" them.
    if not rows:
        cur = conn.execute(base_sql, (domain, "unknown_timeout", per_domain_real))
        rows = cur.fetchall()

    candidates: list[RealCandidate] = []
    for verif_id, email, dom in rows:
        candidates.append(RealCandidate(verif_id=verif_id, email=email, domain=dom))
    return candidates


def _generate_test_send_token(verif_id: int) -> str:
    """
    Generate a unique, bounce-safe token for a given verification_results.id.

    The token will typically be embedded in the envelope sender, e.g.:

        bounce+{token}@iqverifier.xyz
    """
    suffix = secrets.token_urlsafe(8)
    return f"vr{verif_id}-{suffix}"


def _queue_test_send_for_verif(
    conn: sqlite3.Connection,
    verif_id: int,
    dry_run: bool = False,
) -> str:
    """
    Mark a single verification_results row as needing a test-send.

    Concretely:

      - test_send_status = 'requested'
      - test_send_token  = generated token
      - test_send_at     = current UTC time (SQLite)

    Returns the token that will be used in the bounce address.
    """
    token = _generate_test_send_token(verif_id)

    if dry_run:
        return token

    conn.execute(
        """
        UPDATE verification_results
        SET
            test_send_status = 'requested',
            test_send_token  = ?,
            test_send_at     = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
        WHERE id = ?
        """,
        (token, verif_id),
    )
    return token


def demo_o26_unclear_domains(
    conn: sqlite3.Connection,
    max_domains: int | None,
    per_domain_real: int,
    only_domain: str | None,
    dry_run: bool,
) -> None:
    """
    Main driver: find unclear domains, pick real candidates per domain,
    and queue test-sends for their latest verification_results rows.
    """
    domains = _fetch_unclear_domains(conn, max_domains=max_domains, only_domain=only_domain)
    if not domains:
        if only_domain:
            print(f"O26 demo: no unclear emails found for domain={only_domain!r}. Nothing to do.")
        else:
            print("O26 demo: no unclear domains found. Nothing to do.")
        return

    print(
        f"O26 demo: found {len(domains)} unclear domain(s){' (restricted)' if only_domain else ''}:"
    )

    total_queued = 0
    for domain in domains:
        print(f"\n→ Domain: {domain}")
        real_candidates = _fetch_real_candidates_for_domain(conn, domain, per_domain_real)

        if not real_candidates:
            print(
                "  · No eligible 'real' candidates (all already test-sent or filtered out). Skipping."
            )
            continue

        print(
            f"  · Selected {len(real_candidates)} real candidate(s) for test-send (limit={per_domain_real}):"
        )

        for cand in real_candidates:
            token = _queue_test_send_for_verif(conn, cand.verif_id, dry_run=dry_run)
            total_queued += 1
            print(
                f"    - vr_id={cand.verif_id} email={cand.email} "
                f"→ test_send_status='requested' token={token}"
                f"{' (dry-run, not written)' if dry_run else ''}"
            )

    if not dry_run:
        conn.commit()

    print(
        f"\nO26 demo: {'would queue' if dry_run else 'queued'} "
        f"{total_queued} test-send(s) across {len(domains)} domain(s)."
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "O26 demo: automatically queue test-sends for 'unclear' domains "
            "(risky_catch_all / unknown_timeout without prior test-send)."
        )
    )
    parser.add_argument(
        "--db",
        dest="db_path",
        default="data/dev.db",
        help="Path to the SQLite database (default: data/dev.db).",
    )
    parser.add_argument(
        "--domain",
        dest="only_domain",
        default=None,
        help="If set, only consider this single domain (e.g. crestwellpartners.com).",
    )
    parser.add_argument(
        "--max-domains",
        dest="max_domains",
        type=int,
        default=3,
        help=(
            "Maximum number of unclear domains to process (ignored if --domain is set). Default: 3."
        ),
    )
    parser.add_argument(
        "--per-domain-real",
        dest="per_domain_real",
        type=int,
        default=2,
        help="Maximum number of 'real' addresses per domain to queue for test-send (default: 2).",
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        help="If set, do not write any changes; just print what would be queued.",
    )

    args = parser.parse_args()

    _ensure_db_exists(args.db_path)

    conn = sqlite3.connect(args.db_path)
    try:
        demo_o26_unclear_domains(
            conn=conn,
            max_domains=args.max_domains if not args.only_domain else None,
            per_domain_real=args.per_domain_real,
            only_domain=args.only_domain,
            dry_run=args.dry_run,
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
