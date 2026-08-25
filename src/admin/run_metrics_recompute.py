# src/admin/run_metrics_recompute.py
"""
Recompute a run's run_metrics row from what the run actually wrote to the DB.

Why: all 13 existing run_metrics rows have every timing field at 0, and the
funnel counters are 0 too. Two independent causes, both verified by reading the
code end to end:

  1. run_completion_callback() aggregates `autodiscovery_results`, but its only
     caller (_track_company_completion) never passes that argument, so it always
     aggregates an empty list.
  2. Nothing anywhere measures stage duration. AutodiscoveryResult has no timing
     fields, so crawl/extract/generate/verify_time_s could only ever be 0.

Also verified: `run_id` is written to `companies` only (via _link_company_to_run).
No code path ever writes run_id on sources/people/emails/verification_results,
so the existing run-scoped query in _enrich_metrics_with_verification
(`WHERE e.run_id = ?`) matches nothing. Everything here therefore joins through
`companies.run_id`, which is the one linkage that is actually populated.

Timings are derived, not instrumented: each stage's duration is the wall-clock
span between the first and last row that stage wrote for this run's companies.
For fanned-out stages that IS the stage's elapsed time. Two honest caveats:
  - a stage that wrote a single row reports ~0s;
  - `companies.run_id` is set only while still NULL, so a company first seen by
    an earlier run stays attributed to that earlier run, and a re-run of the
    same domain under-counts. That is a pre-existing data-model gap, not one
    introduced here; fixing it means writing run_id on every artifact insert.

Usage (this is also the backfill for the 13 existing rows; dry-run by default):
    python -m src.admin.run_metrics_recompute --run-id <id> --tenant dev
    python -m src.admin.run_metrics_recompute --all
    python -m src.admin.run_metrics_recompute --all --save
"""

from __future__ import annotations

import argparse
import logging
from typing import Any

from src.admin.run_metrics import RunMetricsSummary, save_run_metrics
from src.db import get_conn
from src.queueing.pipeline_v2 import _parse_utc_dt  # handles every TEXT timestamp format in use

log = logging.getLogger(__name__)


def _span_s(lo: Any, hi: Any) -> float:
    """Seconds between two DB timestamps (0.0 if either is missing/unparseable)."""
    a, b = _parse_utc_dt(lo), _parse_utc_dt(hi)
    if a is None or b is None:
        return 0.0
    return max(0.0, (b - a).total_seconds())


def _one(con: Any, sql: str, params: tuple) -> tuple:
    """Run an aggregate query, returning a row of zeros/None if the table is absent."""
    try:
        row = con.execute(sql, params).fetchone()
        return tuple(row) if row else ()
    except Exception:
        try:
            con.rollback()  # Postgres aborts the tx on a failed statement
        except Exception:
            pass
        log.debug("run_metrics_recompute: query failed", exc_info=True)
        return ()


def _val(row: tuple, i: int, default: Any = 0) -> Any:
    return row[i] if len(row) > i and row[i] is not None else default


def recompute_run_metrics(
    run_id: str,
    tenant_id: str,
    *,
    conn: Any = None,
    save: bool = False,
) -> RunMetricsSummary:
    """Derive a RunMetricsSummary for one run from companies/sources/people/emails/
    verification_results, optionally persisting it via save_run_metrics()."""
    close = conn is None
    con = conn or get_conn()
    scope = (tenant_id, run_id)
    try:
        s = RunMetricsSummary(run_id=run_id, tenant_id=tenant_id)

        s.total_companies = int(
            _val(
                _one(
                    con,
                    "SELECT COUNT(*) FROM companies WHERE tenant_id = ? AND run_id = ?",
                    scope,
                ),
                0,
            )
        )

        # crawl: pages fetched
        r = _one(
            con,
            """
            SELECT COUNT(*), COUNT(DISTINCT s.company_id), MIN(s.fetched_at), MAX(s.fetched_at)
              FROM sources s JOIN companies c ON c.id = s.company_id
             WHERE c.tenant_id = ? AND c.run_id = ?
            """,
            scope,
        )
        s.companies_with_pages = int(_val(r, 1))
        s.companies_zero_pages = max(0, s.total_companies - s.companies_with_pages)
        s.crawl_time_s = _span_s(_val(r, 2, None), _val(r, 3, None))

        # extract: people upserted
        r = _one(
            con,
            """
            SELECT COUNT(*), COUNT(DISTINCT p.company_id), MIN(p.created_at), MAX(p.created_at)
              FROM people p JOIN companies c ON c.id = p.company_id
             WHERE c.tenant_id = ? AND c.run_id = ?
            """,
            scope,
        )
        s.people_upserted = int(_val(r, 0))
        s.total_candidates_extracted = s.people_upserted  # no separate candidates table
        s.companies_with_candidates = int(_val(r, 1))
        s.companies_zero_candidates = max(0, s.total_companies - s.companies_with_candidates)
        s.extract_time_s = _span_s(_val(r, 2, None), _val(r, 3, None))

        # generate: emails written
        r = _one(
            con,
            """
            SELECT COUNT(*), COUNT(DISTINCT e.person_id), MIN(e.created_at), MAX(e.created_at)
              FROM emails e JOIN companies c ON c.id = e.company_id
             WHERE c.tenant_id = ? AND c.run_id = ?
            """,
            scope,
        )
        s.emails_generated = int(_val(r, 0))
        s.candidates_with_email = int(_val(r, 1))
        s.candidates_no_email = max(0, s.people_upserted - s.candidates_with_email)
        s.generate_time_s = _span_s(_val(r, 2, None), _val(r, 3, None))

        # verify: SMTP outcomes
        r = _one(
            con,
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE vr.verify_status = 'valid'),
                   COUNT(*) FILTER (WHERE vr.verify_status = 'invalid'),
                   COUNT(*) FILTER (WHERE vr.verify_status = 'risky_catch_all'),
                   COUNT(*) FILTER (WHERE vr.verify_status = 'unknown_timeout'),
                   MIN(vr.checked_at), MAX(vr.checked_at)
              FROM verification_results vr
              JOIN emails e ON e.id = vr.email_id
              JOIN companies c ON c.id = e.company_id
             WHERE c.tenant_id = ? AND c.run_id = ?
            """,
            scope,
        )
        s.emails_verified = int(_val(r, 0))
        s.emails_valid = int(_val(r, 1))
        s.emails_invalid = int(_val(r, 2))
        s.emails_risky_catch_all = int(_val(r, 3))
        s.emails_unknown_timeout = int(_val(r, 4))
        s.verify_time_s = _span_s(_val(r, 5, None), _val(r, 6, None))

        # total: the run row's own clock
        r = _one(
            con,
            """
            SELECT started_at, finished_at, created_at, updated_at
              FROM runs WHERE tenant_id = ? AND id = ?
            """,
            scope,
        )
        s.total_time_s = _span_s(
            _val(r, 0, None) or _val(r, 2, None),
            _val(r, 1, None) or _val(r, 3, None),
        )

        if save:
            save_run_metrics(s, conn=con)
        return s
    finally:
        if close:
            try:
                con.close()
            except Exception:
                pass


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Recompute run_metrics from the DB (dry-run by default)."
    )
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--tenant", default=None, help="tenant_id (looked up from runs when omitted)")
    ap.add_argument("--all", action="store_true", help="every run in the runs table")
    ap.add_argument(
        "--save", action="store_true", help="write the run_metrics rows (default: print only)"
    )
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    con = get_conn()
    try:
        if args.all:
            targets = [
                (str(r[0]), str(r[1]))
                for r in con.execute(
                    "SELECT id, tenant_id FROM runs ORDER BY created_at"
                ).fetchall()
            ]
        elif args.run_id:
            tenant = args.tenant
            if not tenant:
                row = con.execute(
                    "SELECT tenant_id FROM runs WHERE id = ?", (args.run_id,)
                ).fetchone()
                if not row:
                    print(f"no such run: {args.run_id}")
                    return 1
                tenant = str(row[0])
            targets = [(args.run_id, tenant)]
        else:
            ap.error("pass --run-id or --all")

        hdr = f"{'run_id':<38} {'cos':>4} {'people':>7} {'emails':>7} {'verif':>6} " \
              f"{'crawl':>8} {'extract':>8} {'generate':>9} {'verify':>8} {'total':>9}"
        print(hdr)
        print("-" * len(hdr))
        for run_id, tenant in targets:
            s = recompute_run_metrics(run_id, tenant, conn=con, save=args.save)
            print(
                f"{run_id:<38} {s.total_companies:>4} {s.people_upserted:>7} "
                f"{s.emails_generated:>7} {s.emails_verified:>6} "
                f"{s.crawl_time_s:>8.1f} {s.extract_time_s:>8.1f} {s.generate_time_s:>9.1f} "
                f"{s.verify_time_s:>8.1f} {s.total_time_s:>9.1f}"
            )
        outcome = "WRITTEN to run_metrics" if args.save else "dry-run, nothing written"
        print(f"\n{len(targets)} run(s); {outcome}")
        return 0
    finally:
        try:
            con.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
