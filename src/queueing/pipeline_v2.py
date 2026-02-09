# src/queueing/pipeline_v2.py
"""
Enhanced pipeline orchestration for web-app operation.


Features:
  - Company limit enforcement (default 1000)
  - Mode selection (autodiscovery, generate, verify, full)
  - Run-level metrics aggregation
  - Link companies/emails to run_id for result tracking
  - Completion callbacks for metrics finalization


Adjusted behavior (per your requirement):
  - generate-only runs:
      * enqueue company-level fanout job per domain
      * that job enqueues per-person task_generate_emails(person_id, first, last, domain)
  - full runs:
      * enqueue autodiscovery (crawl/extract) per domain
      * enqueue generation fanout per domain (depends_on autodiscovery)
      * enqueue verification per domain
          - when generation enqueues probes (MAX_PROBES_PER_PERSON > 0), verification sweeps
            only "sourced" emails (source_url present) to avoid duplicating the generated probes
          - when generation does NOT enqueue probes (MAX_PROBES_PER_PERSON == 0), verification
            sweeps all emails after generation
"""


from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from rq import Queue

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


DEFAULT_COMPANY_LIMIT = 1000
HARD_COMPANY_LIMIT_24H = 1000
HARD_COMPANY_LIMIT_WINDOW_HOURS = 24
DEFAULT_DISCOVERY_QUEUE = "crawl"
DEFAULT_VERIFY_QUEUE = "verify"
DEFAULT_GENERATE_QUEUE = "generate"
DEFAULT_JOB_TIMEOUT = 1800  # 30 minutes




# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------




def _utc_now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_utc_dt(value: Any) -> datetime | None:
    """Best-effort parse for DB timestamps."""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    if isinstance(value, (int, float)):
        # Unix timestamp seconds (best-effort)
        try:
            return datetime.fromtimestamp(float(value), tz=UTC)
        except Exception:
            return None
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        # Normalize common formats.
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt.astimezone(UTC)
        except Exception:
            # SQLite often stores "YYYY-MM-DD HH:MM:SS"
            try:
                dt = datetime.fromisoformat(s.replace(" ", "T"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC)
                return dt.astimezone(UTC)
            except Exception:
                return None
    return None


def _sum_domains_from_meta_rows(
    rows: list[tuple],
    since_dt: datetime,
) -> int:
    """Sum domains_count values from user_activity metadata rows within the time window."""
    total = 0
    for r in rows:
        if not r:
            continue
        meta_raw = r[0]
        ts_raw = r[1] if len(r) > 1 else None
        ts = _parse_utc_dt(ts_raw)
        if ts is None or ts < since_dt:
            continue
        try:
            meta = json.loads(meta_raw) if isinstance(meta_raw, str) else (meta_raw or {})
        except Exception:
            meta = {}
        try:
            total += int(
                meta.get("domains_count")
                or meta.get("effective_domain_count") or 0
            )
        except Exception:
            continue
    return total


def _count_via_user_activity(
    con,
    *,
    tenant_id: str,
    exclude_run_id: str | None,
    since_dt: datetime,
    since_iso: str,
) -> tuple[int | None, str]:
    """Try counting companies via user_activity table. Returns (count, method) or (None, reason)."""
    if not _has_table(con, "user_activity"):
        return None, "no_user_activity"

    cols = _table_cols(con, "user_activity")
    has_tenant = "tenant_id" in cols
    action_col = "action" if "action" in cols else None
    created_col = "created_at" if "created_at" in cols else ("ts" if "ts" in cols else None)
    meta_col = next((c for c in ("metadata_json", "metadata", "meta_json") if c in cols), None)
    res_id_col = next((c for c in ("resource_id", "run_id") if c in cols), None)

    if not (action_col and created_col and meta_col and has_tenant):
        return None, "user_activity_missing_columns"

    where = [f"{action_col} IS NOT NULL", f"{created_col} IS NOT NULL", "tenant_id = ?"]
    params: list[Any] = [tenant_id]

    where.append(f"LOWER({action_col}) LIKE ?")
    params.append("%run%started%")

    if exclude_run_id and res_id_col:
        where.append(f"{res_id_col} <> ?")
        params.append(exclude_run_id)

    sql = f"""
    SELECT {meta_col}, {created_col}
    FROM user_activity
    WHERE {' AND '.join(where)}
    """

    rows: list[tuple[Any, Any]] = []
    try:
        cur = con.execute(sql + f" AND {created_col} >= ?", tuple(params + [since_iso]))
        rows = cur.fetchall() or []
    except Exception:
        try:
            cur = con.execute(sql, tuple(params))
            rows = cur.fetchall() or []
        except Exception:
            rows = []

    return _sum_domains_from_meta_rows(rows, since_dt), "user_activity"


def _sum_domains_from_run_rows(
    rows: list[tuple],
    since_dt: datetime,
) -> int:
    """Sum domain list lengths from runs.domains_json rows within the time window."""
    total = 0
    for dj, ts_raw in rows:
        ts = _parse_utc_dt(ts_raw)
        if ts is None or ts < since_dt:
            continue
        try:
            domains_val = json.loads(dj) if isinstance(dj, str) else (dj or [])
        except Exception:
            domains_val = []
        if isinstance(domains_val, list):
            total += len(domains_val)
    return total


def _count_via_runs(
    con,
    *,
    tenant_id: str,
    exclude_run_id: str | None,
    since_dt: datetime,
    since_iso: str,
) -> tuple[int | None, str]:
    """Try counting companies via runs table. Returns (count, method) or (None, reason)."""
    if not _has_table(con, "runs"):
        return None, "no_tables"

    run_cols = _table_cols(con, "runs")
    if "domains_json" not in run_cols:
        return None, "runs_missing_domains_json"

    time_col = next(
        (c for c in ("started_at", "created_at", "updated_at")
         if c in run_cols),
        None,
    )
    if not time_col:
        return None, "runs_missing_timestamp"

    has_tenant = "tenant_id" in run_cols
    where = [f"{time_col} IS NOT NULL"]
    params: list[Any] = []

    if has_tenant:
        where.append("tenant_id = ?")
        params.append(tenant_id)

    if exclude_run_id and "id" in run_cols:
        where.append("id <> ?")
        params.append(exclude_run_id)

    base_sql = f"""
    SELECT domains_json, {time_col}
    FROM runs
    WHERE {' AND '.join(where)}
    """

    rows: list[tuple[Any, Any]] = []
    try:
        cur = con.execute(base_sql + f" AND {time_col} >= ?", tuple(params + [since_iso]))
        rows = cur.fetchall() or []
    except Exception:
        try:
            cur = con.execute(base_sql, tuple(params))
            rows = cur.fetchall() or []
        except Exception:
            rows = []

    return _sum_domains_from_run_rows(rows, since_dt), f"runs.{time_col}"


def _count_companies_last_24h(
    con,
    *,
    tenant_id: str,
    exclude_run_id: str | None = None,
    now_dt: datetime | None = None,
) -> tuple[int | None, str]:
    """
    Return (count, method).


    count is the total number of companies (domains) started in the last 24 hours
    for the tenant, best-effort across schema variants.


    If the count cannot be determined (no suitable tables/columns), returns (None, reason).
    """
    now_dt = now_dt or datetime.now(UTC)
    since_dt = now_dt - timedelta(hours=HARD_COMPANY_LIMIT_WINDOW_HOURS)
    since_iso = since_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Preferred: user_activity table where run starts are logged.
    result, method = _count_via_user_activity(
        con,
        tenant_id=tenant_id,
        exclude_run_id=exclude_run_id,
        since_dt=since_dt,
        since_iso=since_iso,
    )
    if result is not None:
        return result, method

    # Fallback: sum run.domains_json lengths over recent runs.
    return _count_via_runs(
        con,
        tenant_id=tenant_id,
        exclude_run_id=exclude_run_id,
        since_dt=since_dt,
        since_iso=since_iso,
    )


def _tokenize_mode_input(modes: Any) -> list[str]:
    """Parse raw mode input (str, list, tuple, etc.) into a flat list of lowercase tokens."""
    if isinstance(modes, str):
        s = modes.strip().lower()
        if not s:
            return []
        for ch in ("+", ","):
            s = s.replace(ch, " ")
        return [p for p in s.split() if p]

    if isinstance(modes, (list, tuple, set)):
        raw: list[str] = []
        for item in modes:
            if item is None:
                continue
            if isinstance(item, str):
                s2 = item.strip().lower()
                if not s2:
                    continue
                for ch in ("+", ","):
                    s2 = s2.replace(ch, " ")
                raw.extend([p for p in s2.split() if p])
            else:
                raw.append(str(item).strip().lower())
        return raw

    return [str(modes).strip().lower()]


_MODE_ALIASES: dict[str, list[str]] = {
    "all": ["full"], "full": ["full"], "everything": ["full"],
    "autodiscovery": ["autodiscovery"], "discovery": ["autodiscovery"], "crawl": ["autodiscovery"],
    "generate": ["generate"], "generation": ["generate"], "gen": ["generate"],
    "verify": ["verify"], "verification": ["verify"], "verif": ["verify"],
    "genverify": ["generate", "verify"],
    "generateverify": ["generate", "verify"],
    "generate_verify": ["generate", "verify"],
    "generation_verify": ["generate", "verify"],
}


def _normalize_modes(modes: Any) -> list[str]:
    """Normalize pipeline mode selection to canonical stage names."""
    if modes is None:
        return ["full"]

    raw = _tokenize_mode_input(modes)
    if not raw:
        return ["full"]

    mapped: list[str] = []
    for m in raw:
        canonical = _MODE_ALIASES.get(m)
        if canonical and canonical == ["full"]:
            return ["full"]
        if canonical:
            mapped.extend(canonical)
        else:
            mapped.append(m)

    # De-duplicate while preserving order.
    seen: set[str] = set()
    out: list[str] = []
    for m in mapped:
        if m not in seen:
            seen.add(m)
            out.append(m)

    return out or ["full"]




def _get_conn():
    """Get database connection."""
    from src.db import get_conn


    return get_conn()




def _get_redis():
    """Get Redis connection."""
    from src.queueing.redis_conn import get_redis


    return get_redis()




def _has_table(con, table: str) -> bool:
    """Check if table exists (best-effort)."""
    try:
        con.execute(f"SELECT 1 FROM {table} LIMIT 1")
        return True
    except Exception:
        return False




def _table_cols(con, table: str) -> set[str]:
    """
    Column discovery.


    Primary path: Postgres information_schema.
    Dev fallback: SQLite PRAGMA (if compat/dev uses SQLite).
    """
    try:
        cur = con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = ?
            """,
            (table,),
        )
        rows = cur.fetchall() or []
        cols = {str(r[0]) for r in rows if r and r[0]}
        if cols:
            return cols
    except Exception:
        pass


    # Dev/test fallback
    try:
        cur = con.execute(f"PRAGMA table_info({table})")
        return {row[1] for row in (cur.fetchall() or []) if row and len(row) > 1}
    except Exception:
        return set()




def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _as_bool(x: Any, default: bool = False) -> bool:
    if isinstance(x, bool):
        return x
    if x is None:
        return default
    if isinstance(x, (int, float)):
        return bool(x)
    if isinstance(x, str):
        v = x.strip().lower()
        if v in {"1", "true", "t", "yes", "y", "on"}:
            return True
        if v in {"0", "false", "f", "no", "n", "off"}:
            return False
    return default




def _max_probes_per_person_env() -> int:
    """
    Must match behavior in task_generate_emails (default 6).
    If >0, generation will enqueue probes itself.
    """
    return max(0, _safe_int(os.getenv("MAX_PROBES_PER_PERSON", "6"), default=6))




def _update_run_row(
    con,
    *,
    tenant_id: str,
    run_id: str,
    status: str | None = None,
    progress: dict | None = None,
    error: str | None = None,
    started_at: str | None = None,
    finished_at: str | None = None,
) -> None:
    """Update run row with given fields (best-effort across schema variants)."""
    cols = _table_cols(con, "runs")
    updates: list[str] = []
    params: list[Any] = []


    if status and "status" in cols:
        updates.append("status = ?")
        params.append(status)


    if progress is not None and "progress_json" in cols:
        updates.append("progress_json = ?")
        params.append(json.dumps(progress, separators=(",", ":")))


    if error is not None and "error" in cols:
        updates.append("error = ?")
        params.append(error)


    if started_at and "started_at" in cols:
        updates.append("started_at = ?")
        params.append(started_at)


    if finished_at and "finished_at" in cols:
        updates.append("finished_at = ?")
        params.append(finished_at)


    if "updated_at" in cols:
        updates.append("updated_at = ?")
        params.append(_utc_now_iso())


    if not updates:
        return


    if "tenant_id" in cols:
        params.extend([tenant_id, run_id])
        con.execute(
            f"UPDATE runs SET {', '.join(updates)} WHERE tenant_id = ? AND id = ?",
            tuple(params),
        )
    else:
        params.append(run_id)
        con.execute(
            f"UPDATE runs SET {', '.join(updates)} WHERE id = ?",
            tuple(params),
        )
    con.commit()




def _link_company_to_run(
    con,
    *,
    company_id: int,
    run_id: str,
    tenant_id: str,
    has_tenant: bool,
) -> None:
    """Best-effort: set run_id on a company row if it's still NULL."""
    try:
        if has_tenant:
            con.execute(
                """
                UPDATE companies SET run_id = ?
                WHERE tenant_id = ? AND id = ? AND run_id IS NULL
                """,
                (run_id, tenant_id, company_id),
            )
        else:
            con.execute(
                """
                UPDATE companies SET run_id = ?
                WHERE id = ? AND run_id IS NULL
                """,
                (run_id, company_id),
            )
        con.commit()
    except Exception:
        pass


def _insert_company_row(
    con,
    *,
    insert_cols: list[str],
    insert_vals: list[Any],
) -> int:
    """Insert a company row, returning the new ID (0 on failure)."""
    placeholders = ", ".join(["?"] * len(insert_vals))
    try:
        row = con.execute(
            f"""
            INSERT INTO companies ({', '.join(insert_cols)})
            VALUES ({placeholders})
            RETURNING id
            """,
            tuple(insert_vals),
        ).fetchone()
        con.commit()
        if row and row[0]:
            return int(row[0])
    except Exception:
        try:
            cur = con.execute(
                f"INSERT INTO companies ({', '.join(insert_cols)}) VALUES ({placeholders})",
                tuple(insert_vals),
            )
            con.commit()
            return int(getattr(cur, "lastrowid", 0) or 0)
        except Exception:
            pass
    return 0


def _select_company_id(con, *, tenant_id: str, domain: str, has_tenant: bool) -> int:
    """Look up an existing company ID by domain (0 if not found)."""
    if has_tenant:
        row = con.execute(
            "SELECT id FROM companies WHERE tenant_id = ? AND domain = ?",
            (tenant_id, domain),
        ).fetchone()
    else:
        row = con.execute(
            "SELECT id FROM companies WHERE domain = ?",
            (domain,),
        ).fetchone()
    return int(row[0]) if row else 0


def _ensure_company_for_domain(
    con,
    *,
    tenant_id: str,
    domain: str,
    run_id: str | None = None,
) -> tuple[int, str]:
    """
    Ensure a company row exists for the given domain.


    Returns (company_id, company_name).
    Links company to run_id if provided and the schema supports it.
    """
    cols = _table_cols(con, "companies")
    has_tenant = "tenant_id" in cols
    has_run_id = "run_id" in cols

    if has_tenant:
        row = con.execute(
            "SELECT id, name FROM companies WHERE tenant_id = ? AND domain = ?",
            (tenant_id, domain),
        ).fetchone()
    else:
        row = con.execute(
            "SELECT id, name FROM companies WHERE domain = ?",
            (domain,),
        ).fetchone()

    if row:
        company_id = int(row[0])
        company_name = row[1] or domain
        if has_run_id and run_id:
            _link_company_to_run(
                con, company_id=company_id, run_id=run_id,
                tenant_id=tenant_id, has_tenant=has_tenant,
            )
        return company_id, company_name

    # Insert new company.
    company_name = domain
    insert_cols: list[str] = ["name", "domain", "official_domain"]
    insert_vals: list[Any] = [company_name, domain, domain]

    if has_tenant:
        insert_cols.append("tenant_id")
        insert_vals.append(tenant_id)
    if has_run_id and run_id:
        insert_cols.append("run_id")
        insert_vals.append(run_id)

    company_id = _insert_company_row(
        con, insert_cols=insert_cols, insert_vals=insert_vals,
    )

    if not company_id:
        company_id = _select_company_id(
            con, tenant_id=tenant_id, domain=domain, has_tenant=has_tenant,
        )

    return company_id, company_name




def _parse_options(options: dict) -> dict[str, Any]:
    """Parse and normalize run options."""
    return {
        "modes": _normalize_modes(options.get("modes", ["full"])),
        "company_limit": int(options.get("company_limit", DEFAULT_COMPANY_LIMIT)),
        "skip_verified": _as_bool(options.get("skip_verified", True), default=True),
        "skip_catch_all": _as_bool(options.get("skip_catch_all", False), default=False),
        "timeout_per_company_s": int(options.get("timeout_per_company_s", 300)),
        "ai_enabled": _as_bool(options.get("ai_enabled", True), default=True),
        "force_discovery": _as_bool(options.get("force_discovery", False), default=False),
        "discovery_queue": options.get("discovery_queue") or DEFAULT_DISCOVERY_QUEUE,
        "verify_queue": options.get("verify_queue") or DEFAULT_VERIFY_QUEUE,
        "generate_queue": options.get("generate_queue") or DEFAULT_GENERATE_QUEUE,
        "job_timeout": int(options.get("job_timeout", DEFAULT_JOB_TIMEOUT)),
        # Used only if MAX_PROBES_PER_PERSON==0 and we must sweep after generation.
        "verify_sweep_delay_s": int(options.get("verify_sweep_delay_s", 15)),
    }




def _should_run_stage(modes: list[str], stage: str) -> bool:
    """Check if a pipeline stage should run based on mode selection."""
    if "full" in modes or "all" in modes:
        return True
    return stage in modes




def _pick_people_table(con) -> str | None:
    if _has_table(con, "people"):
        return "people"
    if _has_table(con, "persons"):
        return "persons"
    return None




def _query_people_table(
    con,
    *,
    table: str,
    select_cols: list[str],
    has_tenant: bool,
    tenant_id: str,
    company_id: int,
) -> list[tuple]:
    """Run a SELECT on the people table with optional tenant filter."""
    if has_tenant:
        cur = con.execute(
            f"""
            SELECT {', '.join(select_cols)}
            FROM {table}
            WHERE tenant_id = ? AND company_id = ?
            """,
            (tenant_id, company_id),
        )
    else:
        cur = con.execute(
            f"""
            SELECT {', '.join(select_cols)}
            FROM {table}
            WHERE company_id = ?
            """,
            (company_id,),
        )
    return cur.fetchall() or []


def _parse_first_last_rows(rows: list[tuple]) -> list[tuple[int, str, str]]:
    """Parse (id, first_name, last_name) rows into people tuples."""
    people: list[tuple[int, str, str]] = []
    for r in rows:
        try:
            pid = int(r[0])
            first = (r[1] or "").strip()
            last = (r[2] or "").strip()
            if first or last:
                people.append((pid, first, last))
        except Exception:
            continue
    return people


def _parse_full_name_rows(rows: list[tuple]) -> list[tuple[int, str, str]]:
    """Parse (id, full_name) rows, splitting name into first/last."""
    people: list[tuple[int, str, str]] = []
    for r in rows:
        try:
            pid = int(r[0])
            nm = (r[1] or "").strip()
            parts = [p for p in nm.split() if p]
            if not parts:
                continue
            first = parts[0]
            last = parts[-1] if len(parts) > 1 else ""
            if first or last:
                people.append((pid, first, last))
        except Exception:
            continue
    return people


def _load_people_for_company(
    con,
    *,
    tenant_id: str,
    company_id: int,
) -> list[tuple[int, str, str]]:
    """
    Return (person_id, first, last) tuples for the given company.


    Tolerant of schema drift and naming differences.
    """
    table = _pick_people_table(con)
    if not table:
        return []

    cols = _table_cols(con, table)
    if "company_id" not in cols:
        return []

    has_tenant = "tenant_id" in cols
    id_col = "id" if "id" in cols else ("person_id" if "person_id" in cols else None)
    if not id_col:
        return []

    first_col = next((c for c in ("first_name", "first", "given_name") if c in cols), None)
    last_col = next((c for c in ("last_name", "last", "family_name", "surname") if c in cols), None)
    name_col = next((c for c in ("full_name", "name") if c in cols), None)

    if first_col and last_col:
        rows = _query_people_table(
            con, table=table, select_cols=[id_col, first_col, last_col],
            has_tenant=has_tenant, tenant_id=tenant_id, company_id=company_id,
        )
        return _parse_first_last_rows(rows)

    if name_col:
        rows = _query_people_table(
            con, table=table, select_cols=[id_col, name_col],
            has_tenant=has_tenant, tenant_id=tenant_id, company_id=company_id,
        )
        return _parse_full_name_rows(rows)

    return []




def _enqueue(
    q: Queue,
    func: Callable[..., Any],
    *,
    depends_on: Any | None = None,
    **kwargs: Any,
):
    """Enqueue with optional depends_on without passing depends_on=None."""
    if depends_on is not None:
        return q.enqueue(func, depends_on=depends_on, **kwargs)
    return q.enqueue(func, **kwargs)




# ---------------------------------------------------------------------------
# Generation fanout task
# ---------------------------------------------------------------------------




def task_generate_company_emails(  # noqa: C901
    *,
    tenant_id: str,
    run_id: str,
    company_id: int,
    domain: str,
    generate_queue: str = DEFAULT_GENERATE_QUEUE,
    job_timeout: int = DEFAULT_JOB_TIMEOUT,
) -> dict[str, Any]:
    """
    Company-level generation fanout.


    Enqueues per-person generation jobs:
        task_generate_emails(person_id, first, last, domain)
    """
    from rq import get_current_job


    con = _get_conn()
    dom = (domain or "").strip().lower()


    try:
        if not dom:
            return {
                "ok": True,
                "company_id": company_id,
                "domain": dom,
                "people_found": 0,
                "people_enqueued": 0,
                "queue": generate_queue,
            }


        # Read options from job meta (passed from pipeline orchestrator)
        job = get_current_job()
        meta = job.meta if job is not None else {}


        people = _load_people_for_company(con, tenant_id=tenant_id, company_id=company_id)
        redis = _get_redis()
        q = Queue(name=generate_queue, connection=redis)


        # IMPORTANT: signature is task_generate_emails(person_id, first, last, domain)
        from src.queueing.tasks import task_generate_emails


        enqueued = 0
        for (person_id, first, last) in people:
            try:
                q.enqueue(
                    task_generate_emails,
                    person_id=person_id,
                    first=first,
                    last=last,
                    domain=dom,
                    job_timeout=job_timeout,
                    meta={
                        "run_id": run_id,
                        "tenant_id": tenant_id,
                        "domain": dom,
                        "company_id": company_id,
                        "person_id": person_id,
                        "stage": "generate_person_emails",
                        # Propagate options from parent job meta
                        "ai_enabled": meta.get("ai_enabled", True),
                        "skip_verified": meta.get("skip_verified", True),
                        "skip_catch_all": meta.get("skip_catch_all", False),
                    },
                )
                enqueued += 1
            except Exception:
                log.debug(
                    "task_generate_company_emails: failed to enqueue person generation",
                    exc_info=True,
                    extra={
                        "run_id": run_id,
                        "tenant_id": tenant_id,
                        "company_id": company_id,
                        "person_id": person_id,
                        "domain": dom,
                    },
                )


        return {
            "ok": True,
            "company_id": company_id,
            "domain": dom,
            "people_found": len(people),
            "people_enqueued": enqueued,
            "queue": generate_queue,
        }


    finally:
        try:
            con.close()
        except Exception:
            pass




# ---------------------------------------------------------------------------
# Verification sweep helper
# ---------------------------------------------------------------------------




def verify_company_emails(  # noqa: C901
    company_id: int,
    *,
    tenant_id: str | None = None,
    run_id: str | None = None,
    verify_queue: str = DEFAULT_VERIFY_QUEUE,
    only_with_source_url: bool = False,
    **_kwargs: Any,
) -> dict[str, Any]:
    """
    Verify all unverified emails for a company.


    Important (based on your emails schema):
      - emails has: tenant_id, run_id, person_id, company_id, email, source_url
      - there is NO pattern_used/pattern_rank marker to distinguish generated emails


    So, when generation is enabled and it already enqueues probes, we avoid duplicate
    probing by verifying only emails that appear to be discovered from pages:
      - source_url IS NOT NULL and non-empty


    This is a pragmatic heuristic to keep full runs from double-probing generated emails.
    """
    from src.queueing.tasks import verify_email_task


    con = _get_conn()
    enqueued = 0


    try:
        email_cols = _table_cols(con, "emails")
        has_tenant = "tenant_id" in email_cols
        has_person = "person_id" in email_cols
        has_source_url = "source_url" in email_cols


        select_cols = ["e.id", "e.email"]
        if has_person:
            select_cols.append("e.person_id")


        where: list[str] = ["e.company_id = ?"]
        params: list[Any] = [company_id]


        if tenant_id and has_tenant:
            where.append("e.tenant_id = ?")
            params.append(tenant_id)


        if only_with_source_url and has_source_url:
            where.append("e.source_url IS NOT NULL")
            where.append("BTRIM(e.source_url) <> ''")


        sql = f"""
        SELECT {", ".join(select_cols)}
        FROM emails e
        LEFT JOIN verification_results vr ON vr.email_id = e.id
        WHERE {" AND ".join(where)}
          AND vr.id IS NULL
        """


        cur = con.execute(sql, tuple(params))


        redis = _get_redis()
        q = Queue(name=verify_queue, connection=redis)


        for row in cur.fetchall() or []:
            try:
                email_addr = (row[1] or "").strip()
                if not email_addr:
                    continue


                person_id = None
                if has_person and len(row) >= 3:
                    try:
                        person_id = int(row[2]) if row[2] is not None else None
                    except Exception:
                        person_id = None


                q.enqueue(
                    verify_email_task,
                    email=email_addr,
                    company_id=company_id,
                    person_id=person_id,
                    meta={
                        "run_id": run_id,
                        "tenant_id": tenant_id,
                        "company_id": company_id,
                        "stage": "verify_email_task",
                        "only_with_source_url": only_with_source_url,
                    },
                )
                enqueued += 1
            except Exception:
                pass


        return {
            "ok": True,
            "company_id": company_id,
            "emails_enqueued": enqueued,
            "queue": verify_queue,
            "only_with_source_url": only_with_source_url,
        }


    finally:
        try:
            con.close()
        except Exception:
            pass




# ---------------------------------------------------------------------------
# Main Pipeline Function
# ---------------------------------------------------------------------------




def _apply_domain_limits(
    con,
    *,
    domains: list[str],
    tenant_id: str,
    run_id: str,
    company_limit: int,
) -> tuple[list[str], dict[str, Any]]:
    """
    Apply per-run company limit and 24h hard limit to the domain list.

    Returns (trimmed_domains, limit_info_dict).
    """
    original_count = len(domains)
    company_limit_applied = False
    if len(domains) > company_limit:
        log.info(
            "Applying per-run company limit: %s -> %s",
            original_count,
            company_limit,
            extra={"run_id": run_id, "tenant_id": tenant_id},
        )
        domains = domains[:company_limit]
        company_limit_applied = True

    used_24h, used_24h_method = _count_companies_last_24h(
        con,
        tenant_id=tenant_id,
        exclude_run_id=run_id,
    )
    hard_24h_enforced = used_24h is not None
    hard_24h_applied = False
    remaining_24h: int | None = None

    if used_24h is not None:
        try:
            remaining_24h = max(0, HARD_COMPANY_LIMIT_24H - int(used_24h))
        except Exception:
            remaining_24h = None

        if remaining_24h is not None and remaining_24h <= 0:
            raise RuntimeError(
                f"24h company limit exceeded: "
                f"limit={HARD_COMPANY_LIMIT_24H} "
                f"per {HARD_COMPANY_LIMIT_WINDOW_HOURS}h "
                f"(used={used_24h}, method={used_24h_method})"
            )

        if remaining_24h is not None and len(domains) > remaining_24h:
            log.info(
                "Applying 24h company hard limit: %s -> %s (used=%s, method=%s)",
                len(domains),
                remaining_24h,
                used_24h,
                used_24h_method,
                extra={"run_id": run_id, "tenant_id": tenant_id},
            )
            domains = domains[:remaining_24h]
            hard_24h_applied = True
    else:
        log.debug(
            "24h company limit not enforced (method=%s)",
            used_24h_method,
            extra={"run_id": run_id, "tenant_id": tenant_id},
        )

    info = {
        "original_count": original_count,
        "company_limit_applied": company_limit_applied,
        "hard_24h_enforced": hard_24h_enforced,
        "hard_24h_applied": hard_24h_applied,
        "used_24h": used_24h,
        "used_24h_method": used_24h_method,
        "remaining_24h": remaining_24h,
    }
    return domains, info


def _enqueue_domain_jobs(
    con,
    *,
    domain: str,
    tenant_id: str,
    run_id: str,
    company_id: int,
    options: dict[str, Any],
    run_autodiscovery: bool,
    run_generate: bool,
    run_verify: bool,
    max_probes: int,
    total_companies: int,
    discovery_q: Queue,
    generate_q: Queue,
    verify_q: Queue,
    job_timeout: int,
) -> dict[str, Any]:
    """Enqueue autodiscovery/generate/verify jobs for a single domain."""
    job_info: dict[str, Any] = {"domain": domain, "company_id": company_id, "jobs": []}
    autod_job = None
    gen_job = None

    # 1) Autodiscovery
    if run_autodiscovery:
        try:
            from src.queueing.tasks import autodiscover_company

            autod_job = discovery_q.enqueue(
                autodiscover_company,
                company_id=company_id,
                job_timeout=job_timeout,
                meta={
                    "run_id": run_id,
                    "tenant_id": tenant_id,
                    "domain": domain,
                    "company_id": company_id,
                    "stage": "autodiscovery",
                    "total_companies": total_companies,
                    "ai_enabled": options.get("ai_enabled", True),
                    "force_discovery": options.get("force_discovery", False),
                    "timeout_per_company_s": options.get("timeout_per_company_s", 300),
                    "skip_verified": options.get("skip_verified", True),
                    "skip_catch_all": options.get("skip_catch_all", False),
                },
            )
            job_info["jobs"].append({
                "stage": "autodiscovery",
                "job_id": autod_job.id,
                "queue": options["discovery_queue"],
            })
        except Exception as exc:
            log.warning("Failed to enqueue autodiscovery for %s: %s", domain, exc)

    # 2) Generation (company fanout) - depends on autodiscovery when present
    if run_generate:
        try:
            gen_job = _enqueue(
                generate_q,
                task_generate_company_emails,
                depends_on=autod_job,
                tenant_id=tenant_id,
                run_id=run_id,
                company_id=company_id,
                domain=domain,
                generate_queue=options["generate_queue"],
                job_timeout=job_timeout,
                meta={
                    "run_id": run_id,
                    "tenant_id": tenant_id,
                    "domain": domain,
                    "company_id": company_id,
                    "stage": "generate_company_fanout",
                    "ai_enabled": options.get("ai_enabled", True),
                    "skip_verified": options.get("skip_verified", True),
                    "skip_catch_all": options.get("skip_catch_all", False),
                },
            )
            job_info["jobs"].append({
                "stage": "generate",
                "job_id": gen_job.id,
                "queue": options["generate_queue"],
                "depends_on": getattr(autod_job, "id", None),
            })
        except Exception as exc:
            log.warning("Failed to enqueue generate fanout for %s: %s", domain, exc)

    # 3) Verification
    if run_verify:
        depends = gen_job if (run_generate and gen_job is not None) else autod_job
        try:
            vjob = _enqueue(
                verify_q,
                verify_company_emails,
                depends_on=depends,
                company_id=company_id,
                tenant_id=tenant_id,
                run_id=run_id,
                verify_queue=options["verify_queue"],
                only_with_source_url=False,
                job_timeout=job_timeout,
                meta={
                    "run_id": run_id,
                    "tenant_id": tenant_id,
                    "domain": domain,
                    "company_id": company_id,
                    "stage": (
                        "verify_company_sweep_post_generate"
                        if run_generate
                        else "verify_company_sweep"
                    ),
                    "only_with_source_url": False,
                    "max_probes_per_person_env": max_probes,
                },
            )
            job_info["jobs"].append({
                "stage": "verify",
                "job_id": vjob.id,
                "queue": options["verify_queue"],
                "depends_on": getattr(depends, "id", None),
                "only_with_source_url": False,
            })
        except Exception as exc:
            log.warning("Failed to enqueue verify sweep for %s: %s", domain, exc)

    return job_info


def _write_user_supplied_resolution(
    con,
    *,
    company_id: int,
    company_name: str,
    domain: str,
    tenant_id: str,
) -> None:
    """Best-effort domain resolution write for user-supplied domains."""
    try:
        from src.db import write_domain_resolution

        class UserSuppliedDecision:
            def __init__(self, d: str):
                self.chosen_domain = d
                self.method = "user_supplied"
                self.confidence = 100.0
                self.reason = "User-provided domain"

        write_domain_resolution(
            con,
            company_id=company_id,
            company_name=company_name,
            decision=UserSuppliedDecision(domain),
            user_hint=domain,
            tenant_id=tenant_id,
        )
        con.commit()
    except Exception:
        log.debug(
            "pipeline_start_v2: write_domain_resolution failed",
            exc_info=True,
            extra={"domain": domain},
        )


def _log_run_started_activity(
    *,
    tenant_id: str,
    run_id: str,
    domain_count: int,
    modes: list[str],
    elapsed: float,
) -> None:
    """Log run-started activity (best-effort)."""
    try:
        from src.admin.user_activity import ACTION_RUN_STARTED, log_user_activity

        log_user_activity(
            tenant_id=tenant_id,
            user_id="system",
            action=ACTION_RUN_STARTED,
            resource_type="run",
            resource_id=run_id,
            metadata={
                "domains_count": domain_count,
                "modes": modes,
                "fanout_time_s": round(elapsed, 2),
            },
        )
    except Exception:
        pass


def _load_run_config(
    con,
    *,
    tenant_id: str,
    run_id: str,
) -> tuple[list[str], dict[str, Any]]:
    """Load domains and parsed options for a run. Raises on missing/invalid data."""
    if not _has_table(con, "runs"):
        raise RuntimeError("runs table not found")

    run_cols = _table_cols(con, "runs")
    if "tenant_id" in run_cols:
        row = con.execute(
            "SELECT domains_json, options_json FROM runs WHERE tenant_id = ? AND id = ?",
            (tenant_id, run_id),
        ).fetchone()
    else:
        row = con.execute(
            "SELECT domains_json, options_json FROM runs WHERE id = ?",
            (run_id,),
        ).fetchone()

    if not row:
        raise ValueError(f"Run not found: {run_id}")

    domains_json = row[0] or "[]"
    options_json = row[1] or "{}"

    domains = json.loads(domains_json) if isinstance(domains_json, str) else domains_json
    if not isinstance(domains, list):
        raise ValueError("runs.domains_json is not a list")

    raw_options = json.loads(options_json) if isinstance(options_json, str) else options_json
    options = _parse_options(raw_options or {})

    return domains, options


def _build_initial_progress(
    *,
    now: str,
    options: dict[str, Any],
    limit_info: dict[str, Any],
    modes: list[str],
    domain_count: int,
) -> dict[str, Any]:
    """Build the initial progress dict for a pipeline run."""
    return {
        "phase": "starting",
        "started_at": now,
        "options": options,
        "original_domain_count": limit_info["original_count"],
        "effective_domain_count": domain_count,
        "company_limit_applied": limit_info["company_limit_applied"],
        "hard_24h_limit": HARD_COMPANY_LIMIT_24H,
        "hard_24h_limit_window_h": HARD_COMPANY_LIMIT_WINDOW_HOURS,
        "hard_24h_limit_enforced": limit_info["hard_24h_enforced"],
        "hard_24h_limit_applied": limit_info["hard_24h_applied"],
        "hard_24h_limit_method": limit_info["used_24h_method"],
        "recent_24h_used": limit_info["used_24h"],
        "recent_24h_remaining": limit_info["remaining_24h"],
        "modes": modes,
        "domains": [],
        "metrics": {
            "total_companies": domain_count,
            "companies_enqueued": 0,
            "autodiscovery_jobs_enqueued": 0,
            "generate_jobs_enqueued": 0,
            "verify_jobs_enqueued": 0,
            "companies_completed": 0,
            "companies_failed": 0,
        },
    }


_STAGE_METRIC_KEYS = {
    "autodiscovery": "autodiscovery_jobs_enqueued",
    "generate": "generate_jobs_enqueued",
    "verify": "verify_jobs_enqueued",
}


def _update_progress_for_domain(
    progress: dict[str, Any],
    *,
    job_info: dict[str, Any],
    domain: str,
    company_id: int,
    enqueued_count: int,
) -> None:
    """Update progress dict after enqueueing jobs for one domain."""
    for j in job_info["jobs"]:
        metric_key = _STAGE_METRIC_KEYS.get(j.get("stage", ""))
        if metric_key:
            progress["metrics"][metric_key] += 1

    progress["domains"].append({
        "domain": domain,
        "company_id": company_id,
        "state": "enqueued",
        "jobs": job_info["jobs"],
    })
    progress["metrics"]["companies_enqueued"] = enqueued_count


def pipeline_start_v2(*, run_id: str, tenant_id: str) -> dict[str, Any]:
    """
    Enhanced pipeline orchestrator for web-app operation.


    Full run behavior:
      - autodiscovery per domain
      - generation fanout per domain (depends_on autodiscovery)
      - verification per domain


    Verification strategy for full runs:
      - If generation will enqueue probes (MAX_PROBES_PER_PERSON > 0):
          verify only "sourced" emails (source_url present) to avoid duplicate probing
          of generated emails.
      - If generation will NOT enqueue probes (MAX_PROBES_PER_PERSON == 0):
          verify all emails after generation (no source_url filter).
    """
    start_time = time.time()
    con = _get_conn()
    now = _utc_now_iso()


    try:
        domains, options = _load_run_config(con, tenant_id=tenant_id, run_id=run_id)

        company_limit = max(0, int(options["company_limit"]))

        domains, limit_info = _apply_domain_limits(
            con,
            domains=domains,
            tenant_id=tenant_id,
            run_id=run_id,
            company_limit=company_limit,
        )

        modes = options["modes"]
        run_autodiscovery = _should_run_stage(modes, "autodiscovery")
        run_generate = _should_run_stage(modes, "generate")
        run_verify = _should_run_stage(modes, "verify")

        max_probes = _max_probes_per_person_env()

        log.info(
            "Pipeline starting: run_id=%s domains=%d modes=%s ai_enabled=%s force_discovery=%s",
            run_id, len(domains), modes,
            options.get("ai_enabled", True), options.get("force_discovery", False),
            extra={"run_id": run_id, "tenant_id": tenant_id},
        )

        progress = _build_initial_progress(
            now=now, options=options, limit_info=limit_info,
            modes=modes, domain_count=len(domains),
        )

        _update_run_row(
            con, tenant_id=tenant_id, run_id=run_id,
            status="running", started_at=now, progress=progress,
        )

        redis = _get_redis()
        discovery_q = Queue(name=options["discovery_queue"], connection=redis)
        generate_q = Queue(name=options["generate_queue"], connection=redis)
        verify_q = Queue(name=options["verify_queue"], connection=redis)

        total_companies = len(domains)
        job_timeout = options["job_timeout"]
        enqueued: list[dict[str, Any]] = []

        for i, d in enumerate(domains):
            dom = str(d or "").strip().lower()
            if not dom:
                continue

            company_id, company_name = _ensure_company_for_domain(
                con, tenant_id=tenant_id, domain=dom, run_id=run_id,
            )

            _write_user_supplied_resolution(
                con, company_id=company_id, company_name=company_name,
                domain=dom, tenant_id=tenant_id,
            )

            job_info = _enqueue_domain_jobs(
                con, domain=dom, tenant_id=tenant_id, run_id=run_id,
                company_id=company_id, options=options,
                run_autodiscovery=run_autodiscovery,
                run_generate=run_generate, run_verify=run_verify,
                max_probes=max_probes, total_companies=total_companies,
                discovery_q=discovery_q, generate_q=generate_q,
                verify_q=verify_q, job_timeout=job_timeout,
            )
            enqueued.append(job_info)

            _update_progress_for_domain(
                progress, job_info=job_info, domain=dom,
                company_id=company_id, enqueued_count=len(enqueued),
            )

            if i % 10 == 0 or i == len(domains) - 1:
                _update_run_row(con, tenant_id=tenant_id, run_id=run_id, progress=progress)


        elapsed = time.time() - start_time
        progress["phase"] = "fanout_complete"
        progress["fanout_time_s"] = round(elapsed, 2)
        progress["metrics"]["companies_enqueued"] = len(enqueued)
        progress["metrics"]["max_probes_per_person_env"] = max_probes


        _update_run_row(con, tenant_id=tenant_id, run_id=run_id, progress=progress)


        _log_run_started_activity(
            tenant_id=tenant_id,
            run_id=run_id,
            domain_count=len(domains),
            modes=modes,
            elapsed=elapsed,
        )


        return {
            "ok": True,
            "run_id": run_id,
            "tenant_id": tenant_id,
            "modes": modes,
            "domains_processed": len(domains),
            "company_limit": company_limit,
            "hard_24h_limit": HARD_COMPANY_LIMIT_24H,
            "recent_24h_used": limit_info["used_24h"],
            "recent_24h_remaining": limit_info["remaining_24h"],
            "hard_24h_limit_enforced": limit_info["hard_24h_enforced"],
            "hard_24h_limit_applied": limit_info["hard_24h_applied"],
            "hard_24h_limit_method": limit_info["used_24h_method"],
            "enqueued": enqueued,
            "fanout_time_s": round(elapsed, 2),
            "max_probes_per_person_env": max_probes,
        }


    except Exception as exc:
        log.exception("pipeline_start_v2 failed", extra={"run_id": run_id, "tenant_id": tenant_id})
        try:
            _update_run_row(
                con,
                tenant_id=tenant_id,
                run_id=run_id,
                status="failed",
                error=f"{type(exc).__name__}: {exc}",
                finished_at=_utc_now_iso(),
            )
        except Exception:
            pass
        raise


    finally:
        try:
            con.close()
        except Exception:
            pass






def _build_permutation_predicate(
    email_cols: set[str],
) -> str | None:
    """Build SQL predicate to identify generated-permutation rows. Returns None if not possible."""
    has_source_note = "source_note" in email_cols
    has_source_url = "source_url" in email_cols

    if has_source_note:
        perm_pred = (
            "(e.source_note LIKE 'generated:%'"
            " OR e.source_note LIKE 'sequential_candidate:%'"
            " OR e.source_note LIKE 'unverified:%'"
            " OR e.source_note LIKE 'sequential_%')"
        )
        if has_source_url:
            perm_pred = (
                f"({perm_pred} AND "
                f"(e.source_url IS NULL OR TRIM(e.source_url) = ''))"
            )
        return perm_pred

    if has_source_url:
        return "(e.source_url IS NULL OR TRIM(e.source_url) = '')"

    return None


def _build_cleanup_status_predicate(
    vr_cols: set[str],
    order_expr: str,
    delete_untested: bool,
) -> str | None:
    """
    Build SQL predicate for cleanup eligibility based on verification status.

    Returns None if cleanup cannot proceed safely.
    """
    has_verify_status = "verify_status" in vr_cols
    untested_pred = (
        "NOT EXISTS ("
        "SELECT 1 FROM verification_results vr0 WHERE vr0.email_id = e.id"
        ")"
    )

    if has_verify_status:
        latest_status_subq = (
            f"(SELECT vr.verify_status FROM verification_results vr "
            f"WHERE vr.email_id = e.id ORDER BY {order_expr} LIMIT 1)"
        )
        invalid_pred = f"{latest_status_subq} = 'invalid'"
        if delete_untested:
            return f"({invalid_pred} OR {untested_pred})"
        return f"({invalid_pred})"

    # Without verify_status, only allow untested deletion if explicitly enabled.
    if delete_untested:
        return f"({untested_pred})"

    return None


def _batch_delete_email_ids(
    con,
    ids: list[int],
) -> dict[str, Any]:
    """Delete emails and their verification_results by ID in batches."""
    def _chunks(seq: list[int], n: int = 500):
        for i in range(0, len(seq), n):
            yield seq[i : i + n]

    def _delete_in(table: str, col: str, vals: list[int]) -> int:
        if not vals:
            return 0
        ph = ", ".join(["?"] * len(vals))
        con.execute(
            f"DELETE FROM {table} WHERE {col} IN ({ph})",
            tuple(vals),
        )
        return len(vals)

    vr_deleted = 0
    try:
        for ch in _chunks(ids, 500):
            vr_deleted += _delete_in("verification_results", "email_id", ch)
    except Exception:
        # FK may be ON DELETE CASCADE; continue.
        pass

    email_deleted = 0
    for ch in _chunks(ids, 500):
        email_deleted += _delete_in("emails", "id", ch)

    try:
        con.commit()
    except Exception:
        pass

    return {
        "ok": True,
        "emails_deleted": email_deleted,
        "verification_rows_deleted": vr_deleted,
    }


def _build_vr_order_expr(vr_cols: set[str]) -> str:
    """Build ORDER BY expression for latest verification result."""
    order_terms: list[str] = []
    for c in ("verified_at", "checked_at", "created_at"):
        if c in vr_cols:
            order_terms.append(f"vr.{c} DESC")
    if "id" in vr_cols:
        order_terms.append("vr.id DESC")
    return ", ".join(order_terms) if order_terms else "vr.email_id DESC"


def _cleanup_generated_permutations_for_run(
    con,
    *,
    tenant_id: str,
    run_id: str,
) -> dict[str, Any]:
    """
    Best-effort cleanup to reduce DB clutter from generated permutations.

    Default behavior (safe):
      - Deletes *permutation* emails for the given run ONLY when the *latest*
        verification result indicates invalid (verify_status = 'invalid').

    Optional behavior (disabled by default):
      - If PIPELINE_DELETE_UNTESTED_PERMS is set to true/1/yes, also deletes
        permutation emails that are "untested" (no verification_results rows yet).

    Guardrails:
      - Requires emails.run_id.
      - Only targets rows that look like generated permutations (source_note markers when
        available, otherwise only rows with blank source_url).
      - Uses the *latest* verification_results row per email_id (best-effort) so a later
        valid/risky/unknown result will prevent deletion.
    """
    if not _has_table(con, "emails"):
        return {"ok": False, "reason": "emails_table_missing"}

    email_cols = _table_cols(con, "emails")
    if "run_id" not in email_cols:
        return {"ok": False, "reason": "emails_missing_run_id"}

    perm_pred = _build_permutation_predicate(email_cols)
    if perm_pred is None:
        return {"ok": False, "reason": "cannot_identify_permutations"}

    if not _has_table(con, "verification_results"):
        return {"ok": False, "reason": "verification_results_missing"}

    vr_cols = _table_cols(con, "verification_results")
    if "email_id" not in vr_cols:
        return {"ok": False, "reason": "verification_results_missing_email_id"}

    order_expr = _build_vr_order_expr(vr_cols)

    delete_untested = (
        (os.getenv("PIPELINE_DELETE_UNTESTED_PERMS") or "")
        .strip().lower() in {"1", "true", "yes"}
    )

    status_pred = _build_cleanup_status_predicate(
        vr_cols, order_expr, delete_untested,
    )
    if status_pred is None:
        return {
            "ok": True, "emails_deleted": 0,
            "verification_rows_deleted": 0,
            "reason": "verify_status_missing",
        }

    where: list[str] = ["e.run_id = ?"]
    params: list[Any] = [run_id]
    if "tenant_id" in email_cols:
        where.append("e.tenant_id = ?")
        params.append(tenant_id)

    sql = f"""
    SELECT e.id
    FROM emails e
    WHERE {' AND '.join(where)}
      AND {perm_pred}
      AND {status_pred}
    """

    ids: list[int] = []
    try:
        cur = con.execute(sql, tuple(params))
        ids = [int(r[0]) for r in (cur.fetchall() or []) if r]
    except Exception:
        return {"ok": False, "reason": "select_failed"}

    if not ids:
        return {"ok": True, "emails_deleted": 0, "verification_rows_deleted": 0}

    return _batch_delete_email_ids(con, ids)


# ---------------------------------------------------------------------------
# Completion callback (kept; best-effort)
# ---------------------------------------------------------------------------




def _aggregate_autodiscovery_metrics(
    autodiscovery_results: list[dict[str, Any]],
) -> dict[str, Any]:
    """Aggregate metrics from autodiscovery results into a summary dict."""
    metrics: dict[str, Any] = {
        "total_companies": 0,
        "companies_with_candidates": 0,
        "companies_zero_candidates": 0,
        "companies_with_pages": 0,
        "companies_zero_pages": 0,
        "companies_403_blocked": 0,
        "companies_robots_blocked": 0,
        "total_candidates": 0,
        "people_upserted": 0,
        "emails_upserted": 0,
        "ai_approved": 0,
        "ai_rejected": 0,
        "errors": [],
    }

    for r in autodiscovery_results:
        metrics["total_companies"] += 1
        pages = r.get("pages_fetched", 0) or 0
        metrics["companies_with_pages"] += 1 if pages > 0 else 0
        metrics["companies_zero_pages"] += 1 if pages <= 0 else 0

        cand = (
            (r.get("candidates_with_email", 0) or 0)
            + (r.get("candidates_without_email", 0) or 0)
        )
        metrics["companies_with_candidates"] += 1 if cand > 0 else 0
        metrics["companies_zero_candidates"] += 1 if cand <= 0 else 0

        metrics["total_candidates"] += cand
        metrics["people_upserted"] += r.get("people_upserted", 0) or 0
        metrics["emails_upserted"] += r.get("emails_upserted", 0) or 0

        if (r.get("pages_403", 0) or 0) > 0:
            metrics["companies_403_blocked"] += 1
        if (r.get("pages_skipped_robots", 0) or 0) > 0:
            metrics["companies_robots_blocked"] += 1

        metrics["ai_approved"] += r.get("ai_approved_people", 0) or 0

        for err in r.get("errors", []) or []:
            if err and len(metrics["errors"]) < 50:
                metrics["errors"].append(err)

    return metrics


def _enrich_metrics_with_verification(
    con,
    metrics: dict[str, Any],
    *,
    run_id: str,
    tenant_id: str,
) -> None:
    """Add verification summary counts to metrics dict (best-effort, in-place)."""
    try:
        verify_stats = con.execute(
            """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE vr.verify_status = 'valid') AS valid,
                COUNT(*) FILTER (WHERE vr.verify_status = 'invalid') AS invalid,
                COUNT(*) FILTER (WHERE vr.verify_status = 'risky_catch_all') AS risky,
                COUNT(*) FILTER (WHERE vr.verify_status = 'unknown_timeout') AS timeout
            FROM verification_results vr
            JOIN emails e ON e.id = vr.email_id
            WHERE e.run_id = ?
              AND e.tenant_id = ?
            """,
            (run_id, tenant_id),
        ).fetchone()
        if verify_stats:
            metrics["emails_verified"] = verify_stats[0] or 0
            metrics["emails_valid"] = verify_stats[1] or 0
            metrics["emails_invalid"] = verify_stats[2] or 0
            metrics["emails_risky_catch_all"] = verify_stats[3] or 0
            metrics["emails_unknown_timeout"] = verify_stats[4] or 0
    except Exception:
        pass


def _save_run_metrics_summary(
    con,
    metrics: dict[str, Any],
    *,
    run_id: str,
    tenant_id: str,
) -> None:
    """Persist run metrics summary (best-effort)."""
    try:
        from src.admin.run_metrics import RunMetricsSummary, save_run_metrics

        summary = RunMetricsSummary(
            run_id=run_id,
            tenant_id=tenant_id,
            **{k: v for k, v in metrics.items() if k != "errors"},
        )
        summary.total_errors = len(metrics.get("errors", []))
        save_run_metrics(summary, conn=con)
    except Exception:
        pass


def _run_optional_cleanup(
    con,
    *,
    tenant_id: str,
    run_id: str,
) -> dict[str, Any]:
    """Run permutation cleanup if enabled via env var, else return skip marker."""
    cleanup_enabled = (
        (os.getenv("PIPELINE_PERMUTATION_CLEANUP") or "")
        .strip().lower() in {"1", "true", "yes"}
    )
    if not cleanup_enabled:
        return {"ok": True, "skipped": True, "reason": "cleanup_disabled"}
    try:
        return _cleanup_generated_permutations_for_run(
            con, tenant_id=tenant_id, run_id=run_id,
        )
    except Exception:
        return {"ok": False, "reason": "cleanup_exception"}


def _log_run_completed_activity(
    *,
    tenant_id: str,
    run_id: str,
    status: str,
    metrics: dict[str, Any],
) -> None:
    """Log run-completed activity (best-effort)."""
    try:
        from src.admin.user_activity import ACTION_RUN_COMPLETED, log_user_activity

        log_user_activity(
            tenant_id=tenant_id,
            user_id="system",
            action=ACTION_RUN_COMPLETED,
            resource_type="run",
            resource_id=run_id,
            metadata={
                "status": status,
                "total_companies": metrics["total_companies"],
                "emails_valid": metrics.get("emails_valid", 0),
            },
        )
    except Exception:
        pass


def run_completion_callback(
    *,
    run_id: str,
    tenant_id: str,
    autodiscovery_results: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Called when all jobs for a run have completed.


    Aggregates metrics and updates run status to succeeded/failed.
    """
    con = _get_conn()
    now = _utc_now_iso()

    try:
        metrics = _aggregate_autodiscovery_metrics(autodiscovery_results or [])
        _enrich_metrics_with_verification(con, metrics, run_id=run_id, tenant_id=tenant_id)
        _save_run_metrics_summary(con, metrics, run_id=run_id, tenant_id=tenant_id)

        cleanup_result = _run_optional_cleanup(con, tenant_id=tenant_id, run_id=run_id)
        status = "succeeded" if not metrics.get("errors") else "completed_with_errors"

        # Load + update progress (tenant-scoped)
        try:
            row = con.execute(
                "SELECT progress_json FROM runs WHERE tenant_id = ? AND id = ?",
                (tenant_id, run_id),
            ).fetchone()
            progress = json.loads(row[0]) if row and row[0] else {}
        except Exception:
            progress = {}

        progress["permutation_cleanup"] = cleanup_result
        progress["phase"] = "completed"
        progress["completed_at"] = now
        progress["metrics"] = metrics

        _update_run_row(
            con, tenant_id=tenant_id, run_id=run_id,
            status=status, progress=progress, finished_at=now,
        )

        _log_run_completed_activity(
            tenant_id=tenant_id, run_id=run_id, status=status, metrics=metrics,
        )

        return {"ok": True, "run_id": run_id, "status": status, "metrics": metrics}

    except Exception as exc:
        log.exception(
            "run_completion_callback failed",
            extra={"run_id": run_id, "tenant_id": tenant_id},
        )
        return {"ok": False, "run_id": run_id, "error": str(exc)}

    finally:
        try:
            con.close()
        except Exception:
            pass




# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------


pipeline_start = pipeline_start_v2


__all__ = [
    "pipeline_start_v2",
    "pipeline_start",
    "run_completion_callback",
    "verify_company_emails",
    "task_generate_company_emails",
    "DEFAULT_COMPANY_LIMIT",
]
