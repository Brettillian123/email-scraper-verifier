# scripts/probe_smtp.py
from __future__ import annotations

r"""
R16/R18 CLI — SMTP RCPT TO probe + R18 status debug

This script supports two modes:

1) Default (remote-safe): enqueue to the VPS "mx" worker and wait/poll for DB result
   - Does NOT attempt TCP/25 locally.
   - Requires Redis + DB to point to the same shared backend (e.g., via SSH tunnels).

2) Direct (VPS-only debugging): run probe in-process (TCP/25)
   - Requires SMTP_PROBES_ENABLED=1 (and any host allowlist rules) or it fails fast.

Usage examples (PowerShell):
  # Default: enqueue to mx queue and wait for verification_results to show up
  #   $PyExe .\scripts\probe_smtp.py --email "someone@example.com"

  # Increase wait time:
  #   $PyExe .\scripts\probe_smtp.py --email "someone@example.com" --wait-seconds 120

  # Force re-resolve MX (if task supports it):
  #   $PyExe .\scripts\probe_smtp.py --email "someone@example.com" --force-resolve

  # Direct (VPS only; will error fast if SMTP probing is disabled on this host):
  #   $PyExe .\scripts\probe_smtp.py --email "someone@example.com" --direct

Behavior:
  - ENQUEUE mode:
      * Enqueues src.queueing.tasks.task_probe_email to the "mx" queue.
      * Polls the DB for the latest verification_results row for the email.
      * Prints the R18 canonical fields when available.

  - DIRECT mode:
      * Resolves MX via src.resolve.mx.get_or_resolve_mx() when available.
      * Calls src.verify.smtp.probe_rcpt() directly and prints RCPT outcome.
      * Then best-effort looks up verification_results for R18 (if your direct path persists).
"""

import argparse
import os
import time
from datetime import UTC, datetime, timedelta
from inspect import signature
from types import SimpleNamespace
from typing import Any

from src.config import (
    SMTP_COMMAND_TIMEOUT,
    SMTP_CONNECT_TIMEOUT,
    SMTP_HELO_DOMAIN,
    SMTP_MAIL_FROM,
)

try:  # pragma: no cover
    from redis import Redis  # type: ignore
    from rq import Queue  # type: ignore
except Exception:  # pragma: no cover
    Redis = None  # type: ignore
    Queue = None  # type: ignore

try:  # pragma: no cover
    from src.verify.preflight import SmtpProbingDisabledError  # type: ignore
except Exception:  # pragma: no cover
    SmtpProbingDisabledError = RuntimeError  # type: ignore

try:  # pragma: no cover
    from src.verify.smtp import probe_rcpt  # type: ignore
except Exception:  # pragma: no cover
    probe_rcpt = None  # type: ignore


# ---------------------------------------------------------------------------
# Env helpers
# ---------------------------------------------------------------------------


def _get_database_url() -> str | None:
    """
    Prefer DATABASE_URL, but support DB_URL for backward compatibility.
    """
    db_url = (os.getenv("DATABASE_URL") or "").strip()
    if db_url:
        return db_url
    legacy = (os.getenv("DB_URL") or "").strip()
    return legacy or None


def _get_redis_url() -> str:
    """
    Prefer RQ_REDIS_URL, but support REDIS_URL for compatibility.
    """
    return (
        (os.getenv("RQ_REDIS_URL") or "").strip()
        or (os.getenv("REDIS_URL") or "").strip()
        or "redis://127.0.0.1:6379/0"
    )


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _parse_dt(v: Any) -> datetime | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.astimezone(UTC) if v.tzinfo else v.replace(tzinfo=UTC)
    s = str(v).strip()
    if not s:
        return None
    try:
        # Accept ISO strings with 'Z'
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        return dt.astimezone(UTC) if dt.tzinfo else dt.replace(tzinfo=UTC)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# MX resolver (used only for DIRECT mode / printing)
# ---------------------------------------------------------------------------


def _get_or_resolve_mx(domain: str, *, force: bool, db_path: str | None) -> Any:
    """
    Prefer a helper from src.resolve.mx; fall back to resolve_mx().
    Returns an object with attributes:
      - lowest_mx: str | None
      - behavior or mx_behavior: dict | None
    """
    try:  # pragma: no cover
        from src.resolve.mx import get_or_resolve_mx as _gomx  # type: ignore

        return _gomx(domain, force=force, db_path=db_path)
    except Exception:
        pass

    try:  # pragma: no cover
        from src.resolve.mx import resolve_mx as _resolve_mx  # type: ignore

        res = _resolve_mx(company_id=0, domain=domain, force=force, db_path=db_path)
        return res
    except Exception:
        return SimpleNamespace(lowest_mx=domain, behavior=None, mx_behavior=None)


# ---------------------------------------------------------------------------
# DB querying (portable: use src.db.get_conn if available)
# ---------------------------------------------------------------------------


def _connect_db() -> Any:
    """
    Return a DB-API connection using the project's src.db.get_conn when possible.
    Falls back to sqlite3 if DATABASE_URL/DB_URL is sqlite.
    """
    db_url = _get_database_url()

    # Prefer project DB helper if present.
    try:  # pragma: no cover
        from src.db import get_conn  # type: ignore

        try:
            return get_conn()  # type: ignore[misc]
        except TypeError:
            # Try common variants
            if db_url:
                try:
                    return get_conn(db_url)  # type: ignore[misc]
                except Exception:
                    pass
            return get_conn(database_url=db_url)  # type: ignore[misc]
    except Exception:
        pass

    # Fallback: sqlite3 for older/local setups
    if db_url and db_url.startswith("sqlite:///"):
        import sqlite3

        path = db_url[len("sqlite:///") :]
        return sqlite3.connect(path)

    raise RuntimeError(
        "Unable to connect to DB. Ensure DATABASE_URL/DB_URL is set, "
        "or that src.db.get_conn() is available."
    )


def _param(con: Any) -> str:
    mod = (con.__class__.__module__ or "").lower()
    if "sqlite3" in mod:
        return "?"
    return "%s"


def _is_sqlite(con: Any) -> bool:
    mod = (con.__class__.__module__ or "").lower()
    return "sqlite3" in mod


def _q(sql: str, ph: str) -> str:
    return sql.replace("{p}", ph)


def _db_now_utc_best_effort() -> datetime:
    """
    Use DB server time as the anchor for "this run" to avoid local clock skew.
    Falls back to local UTC on failure.
    """
    try:
        con = _connect_db()
    except Exception:
        return _now_utc()

    try:
        cur = con.cursor()
        if _is_sqlite(con):
            cur.execute("SELECT datetime('now')")
            row = cur.fetchone()
            dt = _parse_dt(row[0] if row else None)
            return dt or _now_utc()

        # Postgres / other: NOW() is authoritative for subsequent writes
        cur.execute("SELECT NOW()")
        row = cur.fetchone()
        dt = _parse_dt(row[0] if row else None)
        return dt or _now_utc()
    except Exception:
        return _now_utc()
    finally:
        try:
            con.close()
        except Exception:
            pass


def _try_select_email_id(cur: Any, *, ph: str, email_norm: str) -> int | None:
    try:
        cur.execute(_q("SELECT id FROM emails WHERE email = {p} LIMIT 1", ph), (email_norm,))
        row = cur.fetchone()
        if row and row[0] is not None:
            return int(row[0])
    except Exception:
        return None
    return None


def _try_insert_email_sqlite(cur: Any, con: Any, *, ph: str, email_norm: str) -> None:
    try:
        cur.execute(_q("INSERT OR IGNORE INTO emails (email) VALUES ({p})", ph), (email_norm,))
        try:
            con.commit()
        except Exception:
            pass
    except Exception:
        # Best-effort only
        return


def _try_upsert_email_postgres(
    cur: Any,
    con: Any,
    *,
    ph: str,
    email_norm: str,
    dom: str,
) -> int | None:
    # Prefer ON CONFLICT(email) if unique constraint exists (typical: ux_emails_email).
    try:
        cur.execute(
            _q(
                "INSERT INTO emails (email) VALUES ({p}) "
                "ON CONFLICT (email) DO UPDATE SET email = EXCLUDED.email "
                "RETURNING id",
                ph,
            ),
            (email_norm,),
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            try:
                con.commit()
            except Exception:
                pass
            return int(row[0])
    except Exception:
        pass

    # Fallback: try inserting domain too if present (ignore if it fails).
    try:
        cur.execute(
            _q(
                "INSERT INTO emails (email, domain) VALUES ({p}, {p}) "
                "ON CONFLICT (email) DO UPDATE SET email = EXCLUDED.email "
                "RETURNING id",
                ph,
            ),
            (email_norm, dom),
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            try:
                con.commit()
            except Exception:
                pass
            return int(row[0])
    except Exception:
        return None
    return None


def _ensure_email_row_best_effort(email: str, domain: str) -> int | None:
    """
    Best-effort: ensure emails(email) exists and return its id.

    This is intentionally defensive:
      - If the schema differs or constraints are missing, we do not fail the CLI.
      - We always fall back to "SELECT id ..." after any insert attempt.

    Returns:
      - email_id (int) on success
      - None if we cannot create/find a row safely
    """
    email_norm = (email or "").strip().lower()
    dom = (domain or "").strip().lower()
    if not email_norm:
        return None

    try:
        con = _connect_db()
    except Exception:
        return None

    ph = _param(con)

    try:
        cur = con.cursor()

        found = _try_select_email_id(cur, ph=ph, email_norm=email_norm)
        if found is not None:
            return found

        if _is_sqlite(con):
            _try_insert_email_sqlite(cur, con, ph=ph, email_norm=email_norm)
        else:
            inserted = _try_upsert_email_postgres(
                cur,
                con,
                ph=ph,
                email_norm=email_norm,
                dom=dom,
            )
            if inserted is not None:
                return inserted

        return _try_select_email_id(cur, ph=ph, email_norm=email_norm)
    finally:
        try:
            con.close()
        except Exception:
            pass


def _fetchone_best_effort(cur: Any, sql: str, params: tuple[Any, ...]) -> tuple[Any, ...] | None:
    try:
        cur.execute(sql, params)
        return cur.fetchone()
    except Exception:
        return None


def _score_vr_row(vrow: tuple[Any, ...]) -> tuple[datetime, int]:
    # Prefer verified_at/checked_at, else treat as very old; then id desc.
    ts = _parse_dt(vrow[4]) or _parse_dt(vrow[5]) or datetime(1970, 1, 1, tzinfo=UTC)
    try:
        rid = int(vrow[0] or 0)
    except Exception:
        rid = 0
    return (ts, rid)


def _try_load_vr_by_email(cur: Any, *, ph: str, email_norm: str) -> tuple[Any, ...] | None:
    return _fetchone_best_effort(
        cur,
        _q(
            """
            SELECT
              id,
              verify_status,
              verify_reason,
              verified_mx,
              verified_at,
              checked_at,
              fallback_status
            FROM verification_results
            WHERE LOWER(email) = {p}
            ORDER BY COALESCE(verified_at, checked_at) DESC, id DESC
            LIMIT 1
            """,
            ph,
        ),
        (email_norm,),
    )


def _try_load_vr_by_email_id(cur: Any, *, ph: str, email_id: int) -> tuple[Any, ...] | None:
    return _fetchone_best_effort(
        cur,
        _q(
            """
            SELECT
              id,
              verify_status,
              verify_reason,
              verified_mx,
              verified_at,
              checked_at,
              fallback_status
            FROM verification_results
            WHERE email_id = {p}
            ORDER BY COALESCE(verified_at, checked_at) DESC, id DESC
            LIMIT 1
            """,
            ph,
        ),
        (email_id,),
    )


def _try_load_vr_join(cur: Any, *, ph: str, email_norm: str) -> tuple[Any, ...] | None:
    return _fetchone_best_effort(
        cur,
        _q(
            """
            SELECT
              vr.id,
              vr.verify_status,
              vr.verify_reason,
              vr.verified_mx,
              vr.verified_at,
              vr.checked_at,
              vr.fallback_status
            FROM verification_results vr
            JOIN emails e ON e.id = vr.email_id
            WHERE e.email = {p}
            ORDER BY COALESCE(vr.verified_at, vr.checked_at) DESC, vr.id DESC
            LIMIT 1
            """,
            ph,
        ),
        (email_norm,),
    )


def _try_load_catch_all_status(cur: Any, *, ph: str, dom: str) -> Any:
    row = _fetchone_best_effort(
        cur,
        _q(
            """
            SELECT catch_all_status
            FROM domain_resolutions
            WHERE chosen_domain = {p} OR user_hint = {p} OR domain = {p}
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            ph,
        ),
        (dom, dom, dom),
    )
    return row[0] if row else None


def _load_latest_verification(
    email: str,
    domain: str,
) -> dict[str, Any] | None:
    """
    Best-effort helper: load the latest verification_results row for this email.

    Key robustness requirement:
      - Must work even if verification_results.email_id is NULL (legacy/buggy worker write path).
      - Must work even if emails table has no row.

    Returns a small dict with:
      - verify_status, verify_reason, verified_mx, verified_at, checked_at
      - fallback_status
      - catch_all_status
    or None if nothing is found / schema not present.
    """
    email_norm = (email or "").strip().lower()
    dom = (domain or "").strip().lower()
    if not email_norm:
        return None

    try:
        con = _connect_db()
    except Exception:
        return None

    ph = _param(con)

    try:
        cur = con.cursor()

        candidates: list[tuple[Any, ...]] = []
        v_by_email = _try_load_vr_by_email(cur, ph=ph, email_norm=email_norm)
        if v_by_email:
            candidates.append(v_by_email)

        email_id = _try_select_email_id(cur, ph=ph, email_norm=email_norm)
        if email_id is not None:
            v_by_email_id = _try_load_vr_by_email_id(cur, ph=ph, email_id=email_id)
            if v_by_email_id:
                candidates.append(v_by_email_id)

        vrow: tuple[Any, ...] | None
        if candidates:
            vrow = max(candidates, key=_score_vr_row)
        else:
            vrow = _try_load_vr_join(cur, ph=ph, email_norm=email_norm)

        if not vrow:
            return None

        catch_all_status = _try_load_catch_all_status(cur, ph=ph, dom=dom)

        return {
            "id": vrow[0],
            "verify_status": vrow[1],
            "verify_reason": vrow[2],
            "verified_mx": vrow[3],
            "verified_at": vrow[4],
            "checked_at": vrow[5],
            "fallback_status": vrow[6],
            "catch_all_status": catch_all_status,
        }
    except Exception:
        return None
    finally:
        try:
            con.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Enqueue
# ---------------------------------------------------------------------------


def _import_task_probe_email() -> Any:
    """
    Import the worker task that performs SMTP probing and persists verification_results.
    """
    try:  # pragma: no cover
        from src.queueing.tasks import task_probe_email  # type: ignore

        return task_probe_email
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "Could not import src.queueing.tasks.task_probe_email. "
            "Ensure the worker task exists and is importable."
        ) from exc


def _enqueue_probe(
    *,
    email: str,
    mx_host: str | None,
    force_resolve: bool,
    queue_name: str,
    wait_seconds_hint: int,
) -> str:
    if Redis is None or Queue is None:
        raise RuntimeError("redis/rq not installed or not importable in this environment.")

    redis_url = _get_redis_url()
    r = Redis.from_url(redis_url)
    q = Queue(queue_name, connection=r)

    task = _import_task_probe_email()
    task_sig = signature(task)

    # Derive domain from email
    domain = email.split("@", 1)[1].strip().lower() if "@" in email else ""

    # Best-effort: ensure emails row exists so task (and other code) can use email_id.
    # If this fails, we still enqueue and rely on verification_results.email polling.
    email_id = _ensure_email_row_best_effort(email=email, domain=domain)

    kwargs: dict[str, Any] = {}
    # Populate only parameters the task actually accepts.
    if "email_id" in task_sig.parameters and email_id is not None:
        kwargs["email_id"] = int(email_id)
    if "email" in task_sig.parameters:
        kwargs["email"] = email
    if "domain" in task_sig.parameters:
        kwargs["domain"] = domain
    if "force_resolve" in task_sig.parameters:
        kwargs["force_resolve"] = bool(force_resolve)
    if "force" in task_sig.parameters:
        kwargs["force"] = bool(force_resolve)
    if "mx_host" in task_sig.parameters and mx_host:
        kwargs["mx_host"] = mx_host
    if "database_url" in task_sig.parameters:
        kwargs["database_url"] = _get_database_url()
    if "db_url" in task_sig.parameters:
        kwargs["db_url"] = _get_database_url()
    if "db_path" in task_sig.parameters:
        kwargs["db_path"] = (os.getenv("DATABASE_PATH") or "").strip() or None

    # Queue.enqueue signature differs slightly across rq versions; only pass ttl args if accepted.
    enqueue_sig = signature(q.enqueue)
    enqueue_extra: dict[str, Any] = {"job_timeout": 300}

    # Keep job metadata long enough to debug; harmless if your worker also deletes aggressively.
    # (If your infra forces immediate cleanup, Job.fetch will still fail; DB polling remains the source of truth.)
    desired_result_ttl = max(300, int(wait_seconds_hint) + 60)
    if "result_ttl" in enqueue_sig.parameters:
        enqueue_extra["result_ttl"] = desired_result_ttl
    if "failure_ttl" in enqueue_sig.parameters:
        enqueue_extra["failure_ttl"] = desired_result_ttl

    # If task doesn't have an "email" kwarg, try positional first-arg.
    if "email" not in kwargs:
        job = q.enqueue(task, email, **kwargs, **enqueue_extra)
    else:
        job = q.enqueue(task, **kwargs, **enqueue_extra)

    return job.id


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="probe_smtp.py",
        description=(
            "R16/R18: Probe an email via RCPT TO (direct) or enqueue to verify worker (default) "
            "and show R18 verify_status from the DB."
        ),
    )
    p.add_argument(
        "--email",
        required=True,
        help="Target email address to probe (e.g., someone@example.com).",
    )
    p.add_argument(
        "--mx-host",
        default=None,
        help="Optional MX host. In enqueue mode, only passed if the worker task accepts it.",
    )
    p.add_argument(
        "--force-resolve",
        action="store_true",
        help="Ask the resolver/worker to refresh cached MX before probing (if supported).",
    )

    mode = p.add_mutually_exclusive_group()
    mode.add_argument(
        "--enqueue",
        action="store_true",
        help="Enqueue the probe to the verify queue and wait/poll for DB results (default).",
    )
    mode.add_argument(
        "--direct",
        action="store_true",
        help="Run the probe in-process (TCP/25). Requires SMTP_PROBES_ENABLED=1 on this host.",
    )

    p.add_argument(
        "--run-id", default=None, help="Optional run_id to attach to verification_results.")
    p.add_argument(
        "--queue",
        default="verify",
        help='RQ queue name for enqueue mode (default: "verify").',
    )
    p.add_argument(
        "--wait-seconds",
        type=int,
        default=60,
        help="How long to wait for the worker to write verification_results (enqueue mode).",
    )
    p.add_argument(
        "--poll-ms",
        type=int,
        default=500,
        help="Polling interval in milliseconds (enqueue mode).",
    )

    return p.parse_args()


def _print_r18(vr: dict[str, Any]) -> None:
    print()
    print("R18 classification (verification_results):")
    print(f"  Verify status : {vr.get('verify_status')}")
    print(f"  Reason        : {vr.get('verify_reason')}")
    print(f"  MX host       : {vr.get('verified_mx')}")
    print(f"  Verified at   : {vr.get('verified_at')}")
    print(f"  Checked at    : {vr.get('checked_at')}")
    print()
    print(f"  Catch-all     : {vr.get('catch_all_status') or '(unknown)'}")
    fb = vr.get("fallback_status") or "(none)"
    print(f"  Fallback      : {fb}")


def main() -> None:
    args = _parse_args()

    # Derive domain from the email (simple split; upstream validation happens in probe_rcpt/worker)
    try:
        domain = args.email.split("@", 1)[1].strip().lower()
    except Exception as err:
        print("Error: --email must contain a single '@' with a domain part.")
        raise SystemExit(2) from err

    # Default behavior: enqueue unless --direct is explicitly requested
    mode = "direct" if args.direct else "enqueue"

    if mode == "enqueue":
        # Anchor to DB time (reduces skew vs worker timestamps)
        started_at = _db_now_utc_best_effort()

        job_id = _enqueue_probe(
            email=args.email,
            mx_host=args.mx_host.strip() if args.mx_host else None,
            force_resolve=bool(args.force_resolve),
            queue_name=str(args.queue or "mx").strip() or "mx",
            wait_seconds_hint=int(args.wait_seconds),
        )

        print(f"Email:          {args.email}")
        print(f"Domain:         {domain}")
        print("Mode:           enqueue")
        print(f"Queue:          {args.queue}")
        print(f"Job id:         {job_id}")
        print(f"Redis:          {_get_redis_url()}")
        print(f"Database URL:   {(_get_database_url() or '(unset)')}")
        print()

        deadline = time.time() + max(1, int(args.wait_seconds))
        poll_s = max(0.05, int(args.poll_ms) / 1000.0)

        last_seen_vr: dict[str, Any] | None = None
        last_seen_id: Any = None

        # Allow a small negative skew window (DB NOW() vs worker write timestamps can be within same second)
        accept_skew = timedelta(seconds=2)

        while time.time() < deadline:
            vr = _load_latest_verification(args.email, domain)
            if vr is not None:
                last_seen_vr = vr
                last_seen_id = vr.get("id")

                # Avoid returning an obviously stale row (best-effort).
                ts = _parse_dt(vr.get("verified_at")) or _parse_dt(vr.get("checked_at"))
                if ts is None:
                    # If no timestamps exist, treat the row as usable for debugging rather than timing out.
                    _print_r18(vr)
                    return

                if ts + accept_skew >= started_at:
                    _print_r18(vr)
                    return

            time.sleep(poll_s)

        print("Timed out waiting for verification_results to update.")
        if last_seen_id is not None:
            print(
                f"Note: last seen verification_results id={last_seen_id} looked older than this run."
            )
            # Still print the most recent row we saw to avoid a "silent" failure when the worker did write.
            if last_seen_vr is not None:
                _print_r18(last_seen_vr)
        raise SystemExit(4)

    # -------------------------
    # DIRECT mode
    # -------------------------
    if probe_rcpt is None:
        print("Error: src.verify.smtp.probe_rcpt is not importable.")
        raise SystemExit(2)

    # Use DB path convention for the legacy resolver path (if needed)
    db_path = (os.getenv("DATABASE_PATH") or "").strip() or None

    if args.mx_host:
        mx_host = args.mx_host.strip()
        behavior_hint = None
    else:
        mx_info = _get_or_resolve_mx(domain, force=bool(args.force_resolve), db_path=db_path)
        mx_host = getattr(mx_info, "lowest_mx", None) or domain
        behavior_hint = getattr(mx_info, "behavior", None) or getattr(mx_info, "mx_behavior", None)

    try:
        result = probe_rcpt(
            args.email,
            mx_host,
            helo_domain=SMTP_HELO_DOMAIN,
            mail_from=SMTP_MAIL_FROM,
            connect_timeout=SMTP_CONNECT_TIMEOUT,
            command_timeout=SMTP_COMMAND_TIMEOUT,
            behavior_hint=behavior_hint,
        )
    except SmtpProbingDisabledError as exc:
        print("SMTP probing is disabled on this host (direct mode refused).")
        print(f"Error: {exc}")
        raise SystemExit(3) from exc

    category = result.get("category")
    code = result.get("code")
    err = result.get("error")
    msg = result.get("message")

    print(f"Email:          {args.email}")
    print(f"Domain:         {domain}")
    print("Mode:           direct")
    print(f"MX host:        {result.get('mx_host') or mx_host}")
    print(f"HELO:           {result.get('helo_domain') or SMTP_HELO_DOMAIN}")
    print(f"RCPT category:  {category}")
    if code is not None or msg:
        msg_part = (msg or "").strip()
        print(f"RCPT code/msg:  {code} {msg_part}".rstrip())
    if err:
        print(f"RCPT error:     {err}")
    print(f"Elapsed:        {int(result.get('elapsed_ms') or 0)} ms")

    vr = _load_latest_verification(args.email, domain)
    if vr is None:
        print()
        print("R18: no verification_results row found for this email in DB.")
    else:
        _print_r18(vr)


if __name__ == "__main__":
    main()
