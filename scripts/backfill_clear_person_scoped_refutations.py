"""Clear pattern refutations that were drawn from people who were never there.

Until 2026-08-25 the sequential generator recorded every 550 from a probe
session as evidence against the FORMAT. A person who is not at the company --
an advisor, a brand ambassador, a testimonial name, someone who has left --
bounces on every format, so each one banked a failure against the whole
vocabulary. At REFUTED_MIN_FAILURES=2, two such people refuted every pattern a
company had, and every remaining employee then ranked to an empty candidate
plan and was never probed.

The code fix holds failures until some candidate for that person ACCEPTS. This
repairs the evidence the old path already wrote.

WHAT IS CLEARED, AND WHY ONLY THIS
    failed_count is reset ONLY at companies with no confirmed pattern anywhere.
    Those companies never saw an accept, so every failure they hold came from
    the absent-person path and none of it is trustworthy.

    Companies that DO have a confirmed pattern are left alone: their failures
    sat alongside an accept on the same session, which is the case the new code
    still records. Clearing those would throw away the negative evidence that
    makes ranking work.

    inferred_count is never touched -- that comes from the address harvester,
    not from probing.

Dry-run by default. Pass --apply to write.
"""

from __future__ import annotations

import argparse

from src.db import get_conn

COUNT_SQL = """
SELECT
  count(*) FILTER (WHERE cep.failed_count > 0)                       AS rows_to_clear,
  count(DISTINCT cep.company_id) FILTER (WHERE cep.failed_count > 0) AS companies,
  coalesce(sum(cep.failed_count), 0)                                 AS failures_dropped
  FROM company_email_pattern cep
 WHERE NOT EXISTS (
   SELECT 1 FROM company_email_pattern c2
    WHERE c2.company_id = cep.company_id AND c2.confirmed_count > 0
 )
"""

UNPROBED_SQL = """
SELECT count(*)
  FROM people p
 WHERE p.company_id IN (
   SELECT DISTINCT cep.company_id FROM company_email_pattern cep
    WHERE cep.failed_count > 0
      AND NOT EXISTS (
        SELECT 1 FROM company_email_pattern c2
         WHERE c2.company_id = cep.company_id AND c2.confirmed_count > 0)
 )
   AND NOT EXISTS (SELECT 1 FROM emails e WHERE e.person_id = p.id)
"""

UPDATE_SQL = """
UPDATE company_email_pattern cep
   SET failed_count = 0
 WHERE cep.failed_count > 0
   AND NOT EXISTS (
     SELECT 1 FROM company_email_pattern c2
      WHERE c2.company_id = cep.company_id AND c2.confirmed_count > 0
   )
"""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="write the change (default: dry run)")
    args = ap.parse_args()

    con = get_conn()
    try:
        rows, companies, failures = con.execute(COUNT_SQL).fetchone()
        (unprobed,) = con.execute(UNPROBED_SQL).fetchone()

        print(f"pattern rows holding untrustworthy failures : {rows}")
        print(f"companies affected                          : {companies}")
        print(f"failure counts to be dropped                : {failures}")
        print(f"people at those companies never probed      : {unprobed}")

        if not args.apply:
            print("\nDRY RUN -- nothing written. Re-run with --apply.")
            return 0

        cur = con.execute(UPDATE_SQL)
        con.commit()
        print(f"\ncleared. rows updated: {getattr(cur, 'rowcount', 'n/a')}")
        print("Those companies now rank the full base-rate vocabulary again.")
        return 0
    finally:
        try:
            con.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
