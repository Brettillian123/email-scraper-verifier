#!/usr/bin/env python
# scripts/backfill_company_email_patterns.py
"""
Seed company_email_pattern from the verification results already in the DB.

This is what turns the 311 companies that already have a confirmed-valid
address into usable pattern evidence, instead of re-deriving it one SMTP probe
at a time.

What it reads
-------------
The latest verification per address (highest verification_results.id), joined
to the person it belongs to. Rows with verify_status 'valid' become
confirmed_count; 'invalid' rows that are still present become failed_count.
Catch-all results ('risky_catch_all') are ignored on purpose -- an SMTP RCPT
probe cannot decide a catch-all domain, so counting it either way would be
inventing evidence.

Counting
--------
Counts are DISTINCT PEOPLE per pattern, not addresses, because the confidence
rule ("trusted at >= 2 independent confirmed people") is about people.

Idempotence and resumability
----------------------------
confirmed_count is written absolutely -- it is fully derivable from the valid
addresses still in the database, so a re-run converges. failed_count is only
ever raised, never lowered: invalid address rows are deleted by design
(CLEANUP_INVALID_GENERATED=1), so the counter accumulated at runtime is the
only record of a miss and must survive a re-run.

A row whose stored counts already match is not written at all, so a second run
writes nothing. Each (company, pattern) row is committed on its own: a
verification row that will not read, or a write the database rejects, is
counted and skipped and cannot undo the rows already committed, so an
interrupted run resumes by simply being re-run.

Usage
-----
    python scripts/backfill_company_email_patterns.py --dry-run
    python scripts/backfill_company_email_patterns.py

Requires db/migrations/2026_08_19_company_email_pattern.sql to be applied
first (scripts/apply_schema.py).
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.db import get_conn  # noqa: E402
from src.generate.patterns import (  # noqa: E402
    classify_pattern,
    domain_of,
    is_role_localpart,
    localpart_of,
    upsert_pattern_counts,
)

# Latest verification per address, with the person behind it. MAX(id) is
# chronological because the column is a BIGSERIAL. Plain grouped SQL rather
# than DISTINCT ON so the same statement runs on the sqlite dev DB, which is
# the only place this script can be rehearsed before it touches Postgres.
SELECT_OUTCOMES = """
    WITH latest AS (
        SELECT vr.email_id, vr.verify_status
        FROM verification_results AS vr
        JOIN (
            SELECT email_id, MAX(id) AS id
            FROM verification_results
            GROUP BY email_id
        ) AS newest ON newest.email_id = vr.email_id AND newest.id = vr.id
    )
    SELECT p.company_id, e.email, p.first_name, p.last_name, p.id, latest.verify_status
    FROM emails AS e
    JOIN latest ON latest.email_id = e.id
    JOIN people AS p ON p.id = e.person_id
    WHERE latest.verify_status IN ('valid', 'invalid')
      AND (
          COALESCE(p.first_name, '') <> ''
          OR COALESCE(p.last_name, '') <> ''
      )
"""

# Read with the qmark shim so this runs on Postgres and on a sqlite dev DB
# alike. patterns.get_domain_patterns() is Postgres-only.
SELECT_EXISTING = """
    SELECT company_id, pattern, confirmed_count, failed_count
    FROM company_email_pattern
"""

#: Per-row failures printed in full before the summary just counts them.
_MAX_ERRORS_SHOWN = 10


def _rollback(conn: Any) -> None:
    """Postgres aborts the transaction on a failed statement; clear it.

    Same guard as src/queueing/reaper.py:_has_retry_count. Without it every
    statement after the first failure fails too, which is how one bad row ends
    a whole backfill.
    """
    try:
        conn.rollback()
    except Exception:
        pass


def _existing_counts(conn) -> dict[tuple[int, str], tuple[int, int]]:
    """(company_id, pattern) -> (confirmed_count, failed_count) already stored."""
    try:
        rows = conn.execute(SELECT_EXISTING, ()).fetchall() or []
    except Exception:
        # Table not migrated yet: treat every row as new.
        _rollback(conn)
        return {}
    return {(int(r[0]), str(r[1])): (int(r[2] or 0), int(r[3] or 0)) for r in rows}


def _fetch_outcomes(conn) -> list[tuple]:
    cur = conn.cursor()
    try:
        cur.execute(SELECT_OUTCOMES, ())
        return [tuple(row) for row in (cur.fetchall() or [])]
    finally:
        try:
            cur.close()
        except Exception:
            pass


def run(*, dry_run: bool, verbose: bool) -> int:
    conn = get_conn()
    try:
        rows = _fetch_outcomes(conn)

        # (company_id, pattern) -> {"confirmed": {person_id}, "failed": {person_id}}
        confirmed: dict[tuple[int, str], set[int]] = {}
        failed: dict[tuple[int, str], set[int]] = {}
        domains: dict[tuple[int, str], str] = {}

        skipped_role = 0
        bad_rows = 0
        for row in rows:
            # One unreadable verification row is counted and skipped; it must
            # never cost the other companies their evidence.
            try:
                company_id, email, first_name, last_name, person_id, verify_status = row
                local = localpart_of(email)
                if not local or is_role_localpart(local):
                    skipped_role += 1
                    continue
                pattern = classify_pattern(local, first_name or "", last_name or "")
                key = (int(company_id), pattern)
                bucket = confirmed if str(verify_status) == "valid" else failed
                bucket.setdefault(key, set()).add(int(person_id))
                dom = domain_of(email)
                if dom:
                    domains[key] = dom
            except Exception as exc:
                bad_rows += 1
                if bad_rows <= _MAX_ERRORS_SHOWN:
                    print(f"  ! row {row!r}: {type(exc).__name__}: {exc}")

        # Plan only the rows whose stored counts would actually change.
        # upsert_pattern_counts() SETs confirmed_count and raises failed_count,
        # so a row already holding those values has nothing to write and a
        # second run plans nothing.
        stored = _existing_counts(conn)
        todo: list[tuple[tuple[int, str], int, int]] = []
        unchanged = 0
        for key in sorted(set(confirmed) | set(failed)):
            n_confirmed = len(confirmed.get(key, ()))
            n_failed = len(failed.get(key, ()))
            was_confirmed, was_failed = stored.get(key, (-1, -1))
            if n_confirmed == was_confirmed and n_failed <= was_failed:
                unchanged += 1
                continue
            todo.append((key, n_confirmed, n_failed))

        pattern_hist: Counter[str] = Counter(pattern for (_, pattern), _c, _f in todo)

        print(f"verification rows considered : {len(rows)}")
        print(f"skipped (role/empty local)   : {skipped_role}")
        print(f"skipped (unreadable row)     : {bad_rows}")
        print(f"rows already up to date      : {unchanged} (nothing to write)")
        print(f"companies with evidence      : {len({c for (c, _p), _c, _f in todo})}")
        print(f"(company, pattern) rows      : {len(todo)}")
        print("pattern distribution across companies:")
        for pattern, count in pattern_hist.most_common():
            print(f"  {pattern:<12} {count}")

        if dry_run:
            print("\nDRY RUN - nothing written.")
            return 0

        # One row per transaction: a row the database rejects is rolled back
        # and skipped, and cannot undo the rows already committed.
        written = errors = 0
        for key, n_confirmed, n_failed in todo:
            company_id, pattern = key
            try:
                upsert_pattern_counts(
                    conn,
                    company_id=company_id,
                    domain=domains.get(key),
                    pattern=pattern,
                    confirmed_count=n_confirmed,
                    failed_count=n_failed,
                )
                conn.commit()
                written += 1
                if verbose:
                    print(
                        f"  company={company_id} pattern={pattern} "
                        f"confirmed={n_confirmed} failed={n_failed}"
                    )
            except Exception as exc:
                _rollback(conn)
                errors += 1
                if errors <= _MAX_ERRORS_SHOWN:
                    print(
                        f"  ! company={company_id} pattern={pattern}: {type(exc).__name__}: {exc}"
                    )

        print(
            f"\nprocessed {len(rows)} verification rows"
            f" | written {written} (company, pattern) rows"
            f" | skipped {unchanged} already up to date + {skipped_role} role/empty"
            f" | errors {errors} writes + {bad_rows} rows"
        )
        return 1 if errors else 0
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill company_email_pattern from existing verification results.",
    )
    parser.add_argument(
        "--dsn",
        "--db",
        dest="dsn",
        default=None,
        help="Postgres DSN (optional; overrides DATABASE_URL for this run).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be written without touching the database.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print every (company, pattern) row as it is written.",
    )
    args = parser.parse_args()

    if args.dsn:
        os.environ["DATABASE_URL"] = args.dsn

    return run(dry_run=args.dry_run, verbose=args.verbose)


if __name__ == "__main__":
    raise SystemExit(main())
