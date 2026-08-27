"""Daily operational audit: surface the things that will need a decision.

This exists because every serious problem in this system so far was SILENT. A
component that is disabled reports the same thing as one with nothing to do:

  * `POST /runs` returned 501 for four months and nothing said so.
  * SMTP probing was off entirely after the Contabo migration; runs still
    reported `succeeded`.
  * `company_email_pattern` did not exist, so pattern learning wrote nowhere.
  * A backfill got outbound port 25 blocked, and the first anyone knew was that
    verification had stopped.

Each check below is a thing that once went unnoticed, or a decision that will
fall due on a date nobody has written down. Run daily from cron; the report goes
to the log and to `GET /api/browser/ops-audit` for the admin UI.

Exit codes:  0 = nothing needed   1 = action needed   2 = the audit itself broke
"""

from __future__ import annotations

import json
import os
import socket
import sys
from datetime import UTC, datetime
from typing import Any

from src.db import get_conn

REPORT_PATH = os.getenv("OPS_AUDIT_REPORT", "/app/logs/ops-audit.json")

ACTION = "ACTION"
WATCH = "watch"
OK = "ok"


class Audit:
    def __init__(self) -> None:
        self.findings: list[dict[str, Any]] = []

    def add(self, level: str, check: str, message: str, detail: str = "") -> None:
        self.findings.append(
            {"level": level, "check": check, "message": message, "detail": detail}
        )

    @property
    def needs_action(self) -> bool:
        return any(f["level"] == ACTION for f in self.findings)


def check_port25(a: Audit) -> None:
    """The failure that stops everything, and gives no other signal."""
    try:
        with socket.create_connection(("aspmx.l.google.com", 25), timeout=8) as s:
            ok = s.recv(128).startswith(b"2")
    except Exception as exc:
        a.add(ACTION, "port25", "Outbound SMTP is BLOCKED — no verification can run", str(exc))
        return
    if ok:
        a.add(OK, "port25", "Outbound SMTP reachable")
    else:
        a.add(ACTION, "port25", "Port 25 answered but not with a 2xx banner")


def check_stuck_runs(con, a: Audit) -> None:
    """Run status has been wrong in both directions. Trust the clock, not the row."""
    rows = con.execute(
        "SELECT id, label, started_at FROM runs WHERE status = 'running'"
        " AND started_at::timestamptz < now() - interval '6 hours'"
    ).fetchall()
    if rows:
        ids = ", ".join(str(r[0])[:8] for r in rows[:5])
        a.add(
            ACTION,
            "stuck_runs",
            f"{len(rows)} run(s) have been 'running' for over 6 hours",
            f"{ids} — check queues are empty, then mark terminal by hand",
        )
    else:
        a.add(OK, "stuck_runs", "No long-running runs")


def check_probe_budget(con, a: Audit) -> None:
    """Hitting the ceiling is the system working. Hitting it EVERY day is a plan
    that does not fit inside its own limits."""
    try:
        from src.queueing.rate_limit import SMTP_DAILY_PROBE_CAP, daily_probe_usage
        from src.queueing.redis_conn import get_redis

        u = daily_probe_usage(get_redis())
        used = int(u.get("global_used") or 0)
        pct = 100.0 * used / max(1, SMTP_DAILY_PROBE_CAP)
        if pct >= 100:
            a.add(
                WATCH,
                "probe_budget",
                f"Daily probe cap reached ({used}/{SMTP_DAILY_PROBE_CAP})",
                "Expected while a backfill is draining. Persistent = spread the work further.",
            )
        else:
            a.add(OK, "probe_budget", f"Probe budget {used}/{SMTP_DAILY_PROBE_CAP} ({pct:.0f}%)")
    except Exception as exc:
        a.add(WATCH, "probe_budget", "Could not read probe budget", str(exc))


def check_recovery_backlog(con, a: Audit) -> None:
    """The nightly job should be removed once it has nothing left to do."""
    row = con.execute(
        "SELECT count(*) FROM people p"
        " WHERE p.first_name <> '' AND p.last_name <> ''"
        "   AND (p.generation_status IS NULL"
        "        OR p.generation_status IN ('deferred','exhausted'))"
        "   AND NOT EXISTS (SELECT 1 FROM emails e WHERE e.person_id = p.id)"
        "   AND EXISTS (SELECT 1 FROM companies c"
        "               WHERE c.id = p.company_id AND c.domain <> '')"
    ).fetchone()
    remaining = int(row[0]) if row else 0
    if remaining == 0:
        a.add(
            ACTION,
            "recovery_backlog",
            "Nightly probe pass has nothing left to do — REMOVE THE CRON",
            "crontab -e, delete the cw-iq-recovery.sh line. It is now a no-op that "
            "wakes five times a night for nothing.",
        )
    else:
        a.add(OK, "recovery_backlog", f"{remaining} people still worth probing")


def check_tarpit_blocked(con, a: Audit) -> None:
    """People we cannot reach because their mail host tarpits us.

    Not a bug and not fixable by probing harder: Mimecast sits at 95% temp-fail
    across 3,930 samples, Zoho at 93.6%. Those hosts are skipped before a socket
    opens, so these people are UNTESTED rather than answered.

    They are excluded from the nightly pass on purpose -- nothing changes about a
    tarpit between one night and the next, and re-selecting them burned budget
    for 45 real probes out of 500 people. Surfaced here instead so the number
    stays visible and can be revisited if a provider's behaviour changes.
    """
    row = con.execute(
        "SELECT count(*) FROM people WHERE generation_status = 'not_attempted'"
    ).fetchone()
    n = int(row[0]) if row and row[0] is not None else 0
    if n:
        a.add(
            WATCH,
            "tarpit_blocked",
            f"{n} people sit behind mail hosts that tarpit us — untested, not answered",
            "Excluded from the nightly pass by design. Revisit only if a provider's "
            "temp-fail rate drops; see the tarpit thresholds in src/resolve/mx.py.",
        )
    else:
        a.add(OK, "tarpit_blocked", "No people blocked behind tarpit hosts")


def check_address_freshness(con, a: Audit) -> None:
    """Addresses decay. Measured: ~64% of Feb-April confirmations were dead by
    late August. Nobody was tracking that, which is why it surfaced as an outage."""
    row = con.execute(
        "SELECT count(*) FROM emails"
        " WHERE first_valid_at IS NOT NULL"
        "   AND first_valid_at::timestamptz < now() - interval '120 days'"
    ).fetchone()
    stale = int(row[0]) if row else 0
    total = con.execute(
        "SELECT count(*) FROM emails WHERE first_valid_at IS NOT NULL"
    ).fetchone()
    total_n = int(total[0]) if total else 0
    if stale and total_n:
        pct = 100.0 * stale / total_n
        level = ACTION if pct >= 50 else WATCH
        a.add(
            level,
            "address_freshness",
            f"{stale} of {total_n} confirmed addresses ({pct:.0f}%) are over 120 days old",
            "Re-verify in PACED batches. The last unpaced re-verification got port "
            "25 blocked. ~250 people per provider per day.",
        )
    else:
        a.add(OK, "address_freshness", f"{total_n} confirmed addresses, none over 120 days")


def check_unapplied_migrations(con, a: Audit) -> None:
    """`company_email_pattern` never existing is the precedent here: written,
    committed, never applied, and silently inert for months."""
    checks = {
        "company_email_pattern": "SELECT to_regclass('public.company_email_pattern')",
        "people.generation_status": (
            "SELECT 1 FROM information_schema.columns"
            " WHERE table_name='people' AND column_name='generation_status'"
        ),
        "emails.first_valid_at": (
            "SELECT 1 FROM information_schema.columns"
            " WHERE table_name='emails' AND column_name='first_valid_at'"
        ),
    }
    missing = []
    for name, sql in checks.items():
        try:
            row = con.execute(sql).fetchone()
            if not row or row[0] is None:
                missing.append(name)
        except Exception:
            missing.append(name)
    if missing:
        a.add(
            ACTION,
            "migrations",
            f"Schema objects missing: {', '.join(missing)}",
            "Apply the matching file from db/migrations/. Code that depends on these "
            "fails SILENTLY, not loudly.",
        )
    else:
        a.add(OK, "migrations", "Expected schema objects present")


def check_reaper_migration(con, a: Audit) -> None:
    """Deliberately unapplied. This check exists so that stays a DECISION rather
    than something nobody remembers."""
    row = con.execute("SELECT to_regclass('public.run_reaper_state')").fetchone()
    if row and row[0] is not None:
        a.add(
            ACTION,
            "reaper",
            "run_reaper_state exists — the reaper migration has been applied",
            "It defaults to requeue=True and will re-enqueue live runs, duplicating "
            "outbound SMTP. Confirm run completion is trustworthy before leaving it on.",
        )
    else:
        a.add(OK, "reaper", "Reaper migration still unapplied (intended)")


def check_catchall_share(con, a: Audit) -> None:
    """A rising catch-all share means the ceiling is moving, not the code."""
    row = con.execute(
        "SELECT count(*) FILTER (WHERE verify_status='risky_catch_all'), count(*)"
        " FROM verification_results"
        " WHERE checked_at::timestamptz > now() - interval '30 days'"
    ).fetchone()
    ca, total = (int(row[0] or 0), int(row[1] or 0)) if row else (0, 0)
    if total >= 100:
        pct = 100.0 * ca / total
        a.add(
            WATCH if pct >= 45 else OK,
            "catch_all_share",
            f"Catch-all is {pct:.0f}% of the last 30 days' verifications ({ca}/{total})",
            "Catch-all domains are undecidable by RCPT probing; a rising share moves "
            "the ceiling regardless of code quality.",
        )
    else:
        a.add(OK, "catch_all_share", f"Only {total} verifications in 30 days")


def main() -> int:
    a = Audit()
    con = None
    try:
        check_port25(a)
        con = get_conn()
        for fn in (
            check_stuck_runs,
            check_probe_budget,
            check_recovery_backlog,
            check_tarpit_blocked,
            check_address_freshness,
            check_unapplied_migrations,
            check_reaper_migration,
            check_catchall_share,
        ):
            try:
                fn(con, a)
            except Exception as exc:
                a.add(WATCH, fn.__name__, "check failed to run", str(exc))
    except Exception as exc:
        print(f"ops_audit: FATAL {exc}", file=sys.stderr)
        return 2
    finally:
        try:
            if con is not None:
                con.close()
        except Exception:
            pass

    stamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    actions = [f for f in a.findings if f["level"] == ACTION]
    watches = [f for f in a.findings if f["level"] == WATCH]

    print(f"=== CrestwellIQ ops audit — {stamp} ===")
    if actions:
        print(f"\n*** {len(actions)} THING(S) NEED YOUR DECISION ***")
        for f in actions:
            print(f"\n  [{f['check']}] {f['message']}")
            if f["detail"]:
                print(f"      -> {f['detail']}")
    if watches:
        print(f"\n--- {len(watches)} worth watching ---")
        for f in watches:
            print(f"  [{f['check']}] {f['message']}")
            if f["detail"]:
                print(f"      -> {f['detail']}")
    if not actions and not watches:
        print("\nNothing needs attention.")
    print(f"\n({len(a.findings)} checks run)")

    # Persist to the DATABASE, not to a file. The audit runs in cw-iq-worker-1
    # and the admin API serves from cw-iq-api; those containers share no mounts,
    # so a file on disk is invisible to the reader. Rows accumulate, because
    # "when did this start failing?" is the question that gets asked.
    payload = json.dumps(a.findings)
    try:
        con2 = get_conn()
        try:
            con2.execute(
                "INSERT INTO ops_audit_report (needs_action, findings_json) VALUES (?, ?)",
                (bool(a.needs_action), payload),
            )
            con2.commit()
        finally:
            try:
                con2.close()
            except Exception:
                pass
    except Exception as exc:
        print(f"(could not record audit to the database: {exc})")

    # A local copy is still handy when shelling into the worker directly.
    try:
        os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
        with open(REPORT_PATH, "w", encoding="utf-8") as fh:
            json.dump(
                {"generated_at": stamp, "needs_action": a.needs_action, "findings": a.findings},
                fh,
                indent=2,
            )
    except Exception:
        pass

    return 1 if a.needs_action else 0


if __name__ == "__main__":
    sys.exit(main())
