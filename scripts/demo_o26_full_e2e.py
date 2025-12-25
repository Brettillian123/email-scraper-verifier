from __future__ import annotations

"""
O26 – Full end-to-end demo for a single domain.

This script orchestrates the full bounce-based verification flow:

  1) Crawl + extract + generate + verify + ICP:
       scripts/demo_autodiscovery.py

  2) Cascade test-sends for this domain:
       - In each round:
           * scripts/demo_o26_unclear_domains.py
             (top up queued test-sends for this domain)
           * scripts/send_test_sends_ses.py
           * scripts/import_test_sends_from_sqs.py
           * assume_delivered_for_stale_test_sends(...)

       - Stop when:
           * there are no more queued test-sends for this domain
             (no rows with test_send_status='requested'), or
           * a safety max_rounds is hit.

  3) Domain-level delivery_catchall + upgrade risky -> valid:
       scripts/backfill_o26_delivery_catchall.py
       scripts/backfill_o26_upgrade_risky_to_valid.py

  4) Show verification_results snapshot for the domain.

  5) Show search-backend snapshot (SqliteFtsBackend / LeadSearchParams).

Prereqs:
  - DB schema + migrations already applied (e.g. via scripts/accept_r25.ps1).
  - AWS SES + SNS+SQS integration configured (same as your existing setup).
  - .env / environment configured for:
        AWS_REGION
        AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (if needed)
        TEST_SEND_FROM, TEST_SEND_REPLY_TO, TEST_SEND_MAIL_FROM_DOMAIN, etc.
        TEST_SEND_STALE_AFTER_SECONDS (optionally small in dev).

Usage example (from repo root, venv active):

    python scripts/demo_o26_full_e2e.py \
        --db data/dev.db \
        --company "Crestwell Partners" \
        --domain crestwellpartners.com \
        --website-url "https://crestwellpartners.com" \
        --max-test-sends 20 \
        --max-sqs-messages 20
"""

import argparse
import sqlite3
import subprocess
import sys
from pathlib import Path

from src.search.backend import SqliteFtsBackend
from src.search.indexing import LeadSearchParams
from src.verify.test_send import assume_delivered_for_stale_test_sends


def _run(cmd: list[str]) -> None:
    """
    Print and run a subprocess, failing fast on non-zero exit codes.
    """
    print()
    print("→ Running:", " ".join(cmd))
    subprocess.run(cmd, check=True)


def _ensure_db_exists(db_path: Path) -> None:
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")


def _verification_snapshot(db_path: Path, domain: str) -> None:
    print()
    print("=" * 80)
    print(f"Verification snapshot for domain: {domain}")
    print("=" * 80)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(
            """
            SELECT
              vr.id,
              e.email,
              vr.verify_status,
              vr.verify_reason,
              vr.test_send_status,
              vr.test_send_token,
              vr.test_send_at,
              vr.bounce_code,
              vr.bounce_reason
            FROM verification_results AS vr
            JOIN emails     AS e ON e.id = vr.email_id
            JOIN companies  AS c ON c.id = e.company_id
            WHERE c.domain = ?
            ORDER BY vr.id;
            """,
            (domain,),
        )
        rows = cur.fetchall()
        if not rows:
            print("(no rows found for this domain)")
            return

        for r in rows:
            print(
                f"vr_id={r['id']:3d}  "
                f"email={r['email']:<40}  "
                f"vs={r['verify_status'] or 'None':<16}  "
                f"reason={r['verify_reason'] or 'None':<24}  "
                f"test={r['test_send_status'] or 'None':<14}  "
                f"code={r['bounce_code'] or 'None'}"
            )
    finally:
        conn.close()


def _search_snapshot(db_path: Path, query: str) -> None:
    print()
    print("=" * 80)
    print(f"Search backend snapshot for query: {query!r}")
    print("=" * 80)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        backend = SqliteFtsBackend(conn)
        params = LeadSearchParams(
            query=query,
            sort="icp_desc",
            limit=50,
        )
        rows = backend.search_leads(params)
        if not rows:
            print("(no rows returned from search backend)")
            return

        for row in rows:
            email = row["email"]
            vs = row.get("verify_status")
            label = row.get("verify_label")
            is_primary = row.get("is_primary_for_person")
            print(
                f"{email:<40}  "
                f"vs={vs or 'None':<16}  "
                f"label={label or 'None':<20}  "
                f"is_primary={is_primary!r}"
            )
    finally:
        conn.close()


def _assume_delivered(db_path: Path) -> None:
    print()
    print("→ Applying assume_delivered_for_stale_test_sends() ...")
    conn = sqlite3.connect(db_path)
    try:
        updated = assume_delivered_for_stale_test_sends(conn)
        conn.commit()
    finally:
        conn.close()
    print(f"   Updated rows: {updated}")


def _domain_has_requested_test_sends(db_path: Path, domain: str) -> bool:
    """
    Return True if there are any test-sends for this domain that are
    queued / ready to send: status == 'requested'.

    This is deliberately aligned with what send_test_sends_ses.py treats
    as "sendable". We do NOT treat 'pending' or 'sent' as queued here,
    to avoid spinning when there is nothing more to send.
    """
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM verification_results AS vr
            JOIN emails     AS e ON e.id = vr.email_id
            JOIN companies  AS c ON c.id = e.company_id
            WHERE c.domain = ?
              AND vr.test_send_status = 'requested'
            LIMIT 1;
            """,
            (domain,),
        )
        return cur.fetchone() is not None
    finally:
        conn.close()


def _seed_test_sends_for_domain(
    py: str,
    repo_root: Path,
    db_path: Path,
    domain: str,
    per_domain_real: int,
) -> None:
    """
    Call demo_o26_unclear_domains.py to top up test-sends for this domain.

    This guarantees that in each cascade round we try to request additional
    test-sends for the domain (subject to whatever safeguards
    demo_o26_unclear_domains.py applies).
    """
    _run(
        [
            py,
            str(repo_root / "scripts" / "demo_o26_unclear_domains.py"),
            "--db",
            str(db_path),
            "--domain",
            domain,
            "--per-domain-real",
            str(per_domain_real),
        ]
    )


def _run_o26_round(
    py: str,
    repo_root: Path,
    db_path: Path,
    max_test_sends: int,
    max_sqs_messages: int,
    dry_run_ses: bool,
) -> None:
    """
    One O26 "round":
      - send queued test-sends
      - import bounces from SQS
      - apply assume_delivered_for_stale_test_sends
    """
    send_cmd = [
        py,
        str(repo_root / "scripts" / "send_test_sends_ses.py"),
        "--db",
        str(db_path),
        "--max",
        str(max_test_sends),
    ]
    if dry_run_ses:
        send_cmd.append("--dry-run")
    _run(send_cmd)

    _run(
        [
            py,
            str(repo_root / "scripts" / "import_test_sends_from_sqs.py"),
            "--db",
            str(db_path),
            "--max-messages",
            str(max_sqs_messages),
        ]
    )

    _assume_delivered(db_path)


def _run_o26_cascade(
    py: str,
    repo_root: Path,
    db_path: Path,
    domain: str,
    per_domain_real: int,
    max_test_sends: int,
    max_sqs_messages: int,
    max_rounds: int = 10,
) -> None:
    """
    Drive the O26 cascade for a single domain until:

      - There are no more queued test-sends for that domain
        (no rows with test_send_status='requested'), OR
      - max_rounds rounds have been executed.

    In each round we:

      - Re-run demo_o26_unclear_domains.py for *this* domain to request
        additional test-sends if the domain is still unclear and there are
        eligible candidates.
      - Then send those test-sends and import bounces, which may enqueue
        follow-up permutations via the O26 bounce logic.
    """
    round_no = 0

    while True:
        # Top up queued test-sends for this domain (if any are eligible).
        _seed_test_sends_for_domain(
            py=py,
            repo_root=repo_root,
            db_path=db_path,
            domain=domain,
            per_domain_real=per_domain_real,
        )

        if not _domain_has_requested_test_sends(db_path, domain):
            print(
                f"\n✔ No queued test-sends (status='requested') remaining for "
                f"domain {domain}; stopping cascade."
            )
            return

        if round_no >= max_rounds:
            print(f"\n⚠ Reached max_rounds={max_rounds} for domain {domain}; stopping cascade.")
            return

        round_no += 1
        print(f"\n=== O26 cascade round {round_no} for {domain} ===")

        _run_o26_round(
            py=py,
            repo_root=repo_root,
            db_path=db_path,
            max_test_sends=max_test_sends,
            max_sqs_messages=max_sqs_messages,
            dry_run_ses=False,
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Full O26 end-to-end demo for a single domain.",
    )
    parser.add_argument(
        "--db",
        default="data/dev.db",
        help="Path to SQLite database (default: data/dev.db).",
    )
    parser.add_argument(
        "--company",
        required=True,
        help="Company name for display/normalization, e.g. 'Crestwell Partners'.",
    )
    parser.add_argument(
        "--domain",
        required=True,
        help="Primary domain to resolve/crawl, e.g. 'crestwellpartners.com'.",
    )
    parser.add_argument(
        "--website-url",
        required=True,
        help="Canonical website URL, e.g. 'https://crestwellpartners.com'.",
    )
    parser.add_argument(
        "--per-domain-real",
        type=int,
        default=2,
        help="How many 'real' test-sends to queue per unclear domain per round (default: 2).",
    )
    parser.add_argument(
        "--max-test-sends",
        type=int,
        default=20,
        help="Maximum number of SES test-sends to send per cascade round (default: 20).",
    )
    parser.add_argument(
        "--max-sqs-messages",
        type=int,
        default=20,
        help="Maximum number of SQS bounce messages to process per round (default: 20).",
    )
    parser.add_argument(
        "--dry-run-ses",
        action="store_true",
        help="Do not actually send SES emails; just print what would be sent.",
    )

    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parent.parent
    db_path = (repo_root / args.db).resolve()
    _ensure_db_exists(db_path)

    py = sys.executable

    # 1) demo_autodiscovery: crawl + extract + generate + verify + ICP
    _run(
        [
            py,
            str(repo_root / "scripts" / "demo_autodiscovery.py"),
            "--db",
            str(db_path),
            "--company",
            args.company,
            "--domain",
            args.domain,
            "--website-url",
            args.website_url,
            "--run-icp",
        ]
    )

    # 2) O26 cascade:
    #    - In dry-run mode, just show one seed+round.
    #    - In normal mode, cascade until there are no queued test-sends.
    if args.dry_run_ses:
        print("\n(dry-run) Seeding test-sends and executing a single O26 round ...")
        _seed_test_sends_for_domain(
            py=py,
            repo_root=repo_root,
            db_path=db_path,
            domain=args.domain,
            per_domain_real=args.per_domain_real,
        )
        _run_o26_round(
            py=py,
            repo_root=repo_root,
            db_path=db_path,
            max_test_sends=args.max_test_sends,
            max_sqs_messages=args.max_sqs_messages,
            dry_run_ses=True,
        )
    else:
        _run_o26_cascade(
            py=py,
            repo_root=repo_root,
            db_path=db_path,
            domain=args.domain,
            per_domain_real=args.per_domain_real,
            max_test_sends=args.max_test_sends,
            max_sqs_messages=args.max_sqs_messages,
            max_rounds=10,
        )

    # 3) backfill O26 delivery_catchall + upgrade risky_catch_all -> valid
    _run(
        [
            py,
            str(repo_root / "scripts" / "backfill_o26_delivery_catchall.py"),
            "--db",
            str(db_path),
        ]
    )
    _run(
        [
            py,
            str(repo_root / "scripts" / "backfill_o26_upgrade_risky_to_valid.py"),
            "--db",
            str(db_path),
        ]
    )

    # 4) Snapshot: verification_results for this domain
    _verification_snapshot(db_path, args.domain)

    # 5) Snapshot: search backend for this domain token
    _search_snapshot(db_path, query=args.domain.split(".", 1)[0])

    print()
    print("✔ Full O26 end-to-end demo completed.")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
