# src/ingest/persist.py
"""
R13-adjusted persistence layer.

Goals:
- Every write path can accept *raw* rows and normalize them first.
- Preserve provenance: never drop/overwrite source_url during normalization/persist.
- Store company normalization outputs (companies.name_norm, companies.norm_key).
- Store people title fields (people.title_raw, people.title_norm) without
  clobbering original title.
- Do NOT auto-merge companies by norm_key — we still key on domain (then name).

Queueing policy (best-effort):
- Always write to the DB first.
- Attempt to enqueue after DB writes; if queue is unavailable, log and continue.

New in R16:
- Add a best-effort helper to enqueue SMTP RCPT probes (`task_probe_email`) with
  {email_id, email, domain}. This *does not* skip freemail domains — acceptance
  uses gmail.com and similar for manual smoke tests.

DATABASE SUPPORT:
- Supports both SQLite and PostgreSQL via DATABASE_URL environment variable
- SQLite: DATABASE_URL=sqlite:///path/to/db.sqlite
- PostgreSQL: DATABASE_URL=postgresql://user:pass@host:port/dbname
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Iterable
from contextlib import contextmanager
from datetime import datetime
from typing import Any

from src.config import load_icp_config
from src.ingest.normalize import norm_domain, normalize_row
from src.scoring.icp import compute_icp

logger = logging.getLogger(__name__)

# Load ICP config once (R14)
ICPCFG: dict[str, Any] = load_icp_config() or {}

# Import lazily / resiliently so tests without src.db don't explode on import.
try:  # pragma: no cover
    from src.db import set_user_hint_and_enqueue  # type: ignore
except Exception:  # pragma: no cover

    def set_user_hint_and_enqueue(*_args, **_kwargs) -> None:  # type: ignore
        return None


# Optional freemail detector (project-level), fallback to a local set if unavailable.
try:  # pragma: no cover
    from src.ingest.freemail import is_freemail as _is_freemail  # type: ignore
except Exception:  # pragma: no cover

    def _is_freemail(domain: str) -> bool:
        d = (domain or "").lower().strip()
        # Minimal but practical default list; project module (if present) takes precedence.
        return d in {
            "gmail.com",
            "googlemail.com",
            "yahoo.com",
            "yahoo.co.uk",
            "ymail.com",
            "outlook.com",
            "hotmail.com",
            "live.com",
            "msn.com",
            "aol.com",
            "icloud.com",
            "me.com",
            "mac.com",
            "gmx.com",
            "proton.me",
            "protonmail.com",
            "yandex.com",
            "zoho.com",
            "mail.com",
            "hey.com",
        }


# ---------------------------------------------------------------------------
# Database connection helpers (SQLite + PostgreSQL support)
# ---------------------------------------------------------------------------


def _get_database_url() -> str:
    """Get DATABASE_URL from environment with sensible default."""
    return os.getenv("DATABASE_URL", "").strip()


def _is_postgresql() -> bool:
    """Check if DATABASE_URL points to PostgreSQL."""
    url = _get_database_url()
    return url.startswith("postgresql://") or url.startswith("postgres://")


def _is_sqlite() -> bool:
    """Check if DATABASE_URL points to SQLite."""
    url = _get_database_url()
    return url.startswith("sqlite:///") or not url


def _param_placeholder(is_pg: bool) -> str:
    return "%s" if is_pg else "?"


def _placeholders(is_pg: bool, n: int) -> str:
    return ",".join([_param_placeholder(is_pg)] * n)


@contextmanager
def _get_connection():
    """
    Get a database connection (SQLite or PostgreSQL).

    Yields a connection object that supports:
    - execute(sql, params)
    - commit()
    - row_factory for dict-like row access (SQLite)
    """
    url = _get_database_url()

    if _is_postgresql():
        # Use src.db.get_conn() for PostgreSQL (handles connection pooling)
        try:
            from src.db import get_conn

            conn = get_conn()
            yield conn
            # Note: get_conn() connections handle their own lifecycle
            return
        except ImportError as err:
            raise RuntimeError(
                "PostgreSQL support requires src.db module. "
                "Ensure DATABASE_URL is correct and dependencies are installed."
            ) from err

    # SQLite fallback
    import sqlite3

    if url.startswith("sqlite:///"):
        db_path = url[len("sqlite:///") :]
    elif not url:
        # Default to dev.db in project root
        from pathlib import Path

        db_path = str(Path(__file__).resolve().parents[2] / "dev.db")
    else:
        raise RuntimeError(f"Unsupported DATABASE_URL format: {url!r}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        yield conn
    finally:
        conn.close()


def _table_columns(con: Any, table: str) -> set[str]:
    """
    Get column names for a table (works with SQLite and PostgreSQL).
    """
    if _is_postgresql():
        # PostgreSQL: use information_schema
        try:
            cur = con.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                """,
                (table,),
            )
            return {row[0] for row in cur.fetchall()}
        except Exception:
            # Try PRAGMA emulation (CompatConnection may support it)
            pass

    # SQLite or fallback: use PRAGMA
    try:
        cur = con.execute(f"PRAGMA table_info({table})")
        return {row[1] for row in cur.fetchall()}  # column name is at index 1
    except Exception:
        return set()


def _extract_company_norm_fields(
    normalized: dict[str, Any],
) -> tuple[str | None, str | None]:
    """
    Support both legacy and R13 field names from normalize_row():
      - display name: company_name_norm (legacy) or company_norm (R13)
      - key: company_norm_key (legacy) or company_key (R13)
    """
    name_norm = (
        (normalized.get("company_name_norm") or normalized.get("company_norm") or "").strip()
        or None
    )
    norm_key = (
        (normalized.get("company_norm_key") or normalized.get("company_key") or "").strip()
        or None
    )
    return name_norm, norm_key


# ---------------------------------------------------------------------------
# Company upsert (no auto-merge by norm_key)
# ---------------------------------------------------------------------------


def _company_find_id_by_domain(con: Any, domain: str, *, is_pg: bool) -> int | None:
    sql = f"SELECT id FROM companies WHERE domain = {_param_placeholder(is_pg)}"
    cur = con.execute(sql, (domain,))
    row = cur.fetchone()
    return int(row[0]) if row else None


def _company_find_id_by_name(con: Any, name: str, *, is_pg: bool) -> int | None:
    sql = f"SELECT id FROM companies WHERE name = {_param_placeholder(is_pg)}"
    cur = con.execute(sql, (name,))
    row = cur.fetchone()
    return int(row[0]) if row else None


def _company_fill_if_empty(
    con: Any,
    *,
    company_id: int,
    cols: set[str],
    is_pg: bool,
    name: str | None,
    name_norm: str | None,
    norm_key: str | None,
) -> None:
    """
    Only fill empty (NULL/'') fields using COALESCE(NULLIF(col,''), ?).
    """
    updates: list[tuple[str, Any]] = []
    if name and "name" in cols:
        updates.append(("name", name))
    if name_norm and "name_norm" in cols:
        updates.append(("name_norm", name_norm))
    if norm_key and "norm_key" in cols:
        updates.append(("norm_key", norm_key))

    if not updates:
        return

    ph = _param_placeholder(is_pg)
    for field, value in updates:
        sql = (
            f"UPDATE companies SET {field} = COALESCE(NULLIF({field},''), {ph}) "
            f"WHERE id = {ph}"
        )
        con.execute(sql, (value, company_id))


def _company_insert(
    con: Any,
    *,
    cols: set[str],
    is_pg: bool,
    name: str | None,
    domain: str | None,
    name_norm: str | None,
    norm_key: str | None,
) -> int:
    insert_cols: list[str] = []
    vals: list[Any] = []

    if "name" in cols:
        insert_cols.append("name")
        vals.append(name)
    if "domain" in cols:
        insert_cols.append("domain")
        vals.append(domain)
    if "name_norm" in cols:
        insert_cols.append("name_norm")
        vals.append(name_norm)
    if "norm_key" in cols:
        insert_cols.append("norm_key")
        vals.append(norm_key)

    if not insert_cols:
        if is_pg:
            cur = con.execute("INSERT INTO companies DEFAULT VALUES RETURNING id")
            row = cur.fetchone()
            con.commit()
            return int(row[0]) if row else 0
        cur = con.execute("INSERT INTO companies DEFAULT VALUES")
        con.commit()
        return int(cur.lastrowid)

    cols_sql = ",".join(insert_cols)
    phs = _placeholders(is_pg, len(insert_cols))

    if is_pg:
        sql = f"INSERT INTO companies ({cols_sql}) VALUES ({phs}) RETURNING id"
        cur = con.execute(sql, vals)
        row = cur.fetchone()
        con.commit()
        return int(row[0]) if row else 0

    sql = f"INSERT INTO companies ({cols_sql}) VALUES ({phs})"
    cur = con.execute(sql, vals)
    con.commit()
    return int(cur.lastrowid)


def _upsert_company(
    con: Any,
    name: str | None,
    domain: str | None,
    name_norm: str | None,
    norm_key: str | None,
) -> int:
    """
    Upsert a company record keyed by domain, then by *exact* name.
    - On INSERT, set name/domain plus R13 fields (name_norm, norm_key) when available.
    - On UPDATE, only fill empty (NULL/'') fields using COALESCE(NULLIF(col,''), ?).

    Works with both SQLite and PostgreSQL.
    """
    cols = _table_columns(con, "companies")
    is_pg = _is_postgresql()

    if domain:
        company_id = _company_find_id_by_domain(con, domain, is_pg=is_pg)
        if company_id is not None:
            _company_fill_if_empty(
                con,
                company_id=company_id,
                cols=cols,
                is_pg=is_pg,
                name=name,
                name_norm=name_norm,
                norm_key=norm_key,
            )
            con.commit()
            return company_id
        return _company_insert(
            con,
            cols=cols,
            is_pg=is_pg,
            name=name,
            domain=domain,
            name_norm=name_norm,
            norm_key=norm_key,
        )

    if name:
        company_id = _company_find_id_by_name(con, name, is_pg=is_pg)
        if company_id is not None:
            _company_fill_if_empty(
                con,
                company_id=company_id,
                cols=cols,
                is_pg=is_pg,
                name=None,
                name_norm=name_norm,
                norm_key=norm_key,
            )
            con.commit()
            return company_id
        return _company_insert(
            con,
            cols=cols,
            is_pg=is_pg,
            name=name,
            domain=None,
            name_norm=name_norm,
            norm_key=norm_key,
        )

    # Shouldn't happen (ingest guarantees name or domain), but be safe:
    return _company_insert(
        con,
        cols=cols,
        is_pg=is_pg,
        name=None,
        domain=None,
        name_norm=name_norm,
        norm_key=norm_key,
    )


# ---------------------------------------------------------------------------
# R08: enqueue async domain resolver; never set official domain here
# ---------------------------------------------------------------------------


def _enqueue_domain_resolution(
    con: Any,
    company_id: int,
    company_name: str,
    normalized_hint: str | None,
) -> None:
    """
    Store the user hint (domain/website) and enqueue the async resolver job.

    Best-effort policy:
    - Persist the hint if possible.
    - Attempt to enqueue; on queue/redis errors, log and return without raising.
    Important: do NOT write official domain here — only the resolver task does that.
    """
    # Persist the (possibly normalized) hint on the company record (best-effort).
    try:
        set_user_hint_and_enqueue(con, company_id, normalized_hint)
    except Exception as e:
        logger.debug("Skipping set_user_hint_and_enqueue (best-effort): %s", e)

    # 1) Preferred path: go through the ingest enqueue shim (what tests spy on)
    try:
        from src.ingest import enqueue as ingest_enqueue

        ingest_enqueue(
            "resolve_company_domain",
            {
                "company_id": company_id,
                "company_name": company_name,
                "user_supplied_domain": normalized_hint,  # exact key expected by tests
            },
        )
        return  # tests observe this via enqueue_spy
    except Exception as e:
        logger.debug("ingest.enqueue unavailable, falling back to RQ: %s", e)

    # 2) Fallback path: direct RQ (best-effort; swallow Redis outages)
    try:
        from rq import Queue  # type: ignore

        from src.queueing.redis_conn import get_redis  # type: ignore
        from src.queueing.tasks import (
            resolve_company_domain as _resolve_company_domain,  # type: ignore
        )
    except Exception:
        return  # environment without RQ/Redis installed

    try:
        q = Queue("default", connection=get_redis())
        q.enqueue(
            _resolve_company_domain,
            company_id,
            company_name,
            normalized_hint,
            job_timeout=30,
            retry=None,
        )
    except (ConnectionError, TimeoutError, OSError) as e:
        logger.warning("Queue degraded (domain resolution not enqueued): %s", e)
    except Exception as e:
        logger.warning("Queue degraded (unexpected): %s", e)


# ---------------------------------------------------------------------------
# R15: enqueue MX resolver (idempotent; skip freemail)
# ---------------------------------------------------------------------------


def _enqueue_mx_resolution(
    company_id: int,
    domain: str | None,
    *,
    force: bool = False,
) -> None:
    """
    Enqueue the R15 MX resolver for a concrete company domain.

    Rules:
    - Only runs best-effort; never raises.
    - Skips freemail domains.
    - Calls the ingest enqueue shim (for tests) **and** then attempts real RQ enqueue.
      MX writes are idempotent, so double-enqueue is safe if the shim also enqueues.
    - Idempotent behavior is handled inside src.resolve.mx.resolve_mx (DB-upsert).
    """
    try:
        canon = norm_domain(domain) if domain else None
        if not canon:
            return
        if _is_freemail(canon):
            logger.info("R15 MX enqueue skipped (freemail): %s", canon)
            return

        # 1) Preferred: ingest enqueue shim (observed in tests)
        try:
            from src.ingest import enqueue as ingest_enqueue  # type: ignore

            ingest_enqueue(
                "task_resolve_mx",
                {
                    "company_id": int(company_id),
                    "domain": canon,
                    "force": bool(force),
                },
            )
            logger.info(
                "R15 MX enqueue via ingest shim: company_id=%s domain=%s",
                company_id,
                canon,
            )
            # NOTE: Do NOT return here; fall through to RQ to ensure real enqueue in runtime.
        except Exception as e:
            logger.debug("ingest.enqueue unavailable for MX; will attempt RQ: %s", e)

        # 2) Also attempt direct RQ on 'mx' queue (idempotent)
        try:
            from rq import Queue  # type: ignore

            from src.queueing.redis_conn import get_redis  # type: ignore
            from src.queueing.tasks import task_resolve_mx  # type: ignore
        except Exception:
            return  # environment without RQ/Redis installed

        try:
            q = Queue("mx", connection=get_redis())
            q.enqueue(
                task_resolve_mx,
                company_id=company_id,
                domain=canon,
                force=force,
                job_timeout=10,
                retry=None,
            )
            logger.info("R15 MX enqueue via RQ: company_id=%s domain=%s", company_id, canon)
        except (ConnectionError, TimeoutError, OSError) as e:
            logger.warning("Queue degraded (MX resolution not enqueued): %s", e)
        except Exception as e:
            logger.warning("Queue degraded (MX unexpected): %s", e)
    except Exception as e:
        # Belt-and-suspenders: never break ingest on enqueue errors.
        logger.debug("Ignoring MX enqueue error (best-effort): %s", e)


# ---------------------------------------------------------------------------
# R16: enqueue SMTP RCPT probe (best-effort; DOES NOT skip freemail)
# ---------------------------------------------------------------------------


def _enqueue_probe_email(
    email_id: int,
    email: str,
    domain: str | None,
    *,
    force: bool = False,
) -> None:
    """
    Enqueue the R16 SMTP RCPT probe for a specific email row.

    Behavior:
    - Best-effort; never raises.
    - DOES NOT skip freemail domains (acceptance probes frequently target large ISPs).
    - Uses the ingest enqueue shim (what tests spy on) and *also* attempts real RQ enqueue
      on the 'verify' queue.
    - Idempotency: task_probe_email itself should be idempotent/safe; duplicate enqueues are OK.
    """
    try:
        canon_dom = norm_domain(domain) if domain else None
        if not canon_dom:
            # Derive from the email if possible
            try:
                canon_dom = norm_domain(email.split("@", 1)[1])
            except Exception:
                logger.info("R16 probe enqueue skipped (no domain/email): %r", email)
                return

        # 1) Preferred: ingest enqueue shim
        try:
            from src.ingest import enqueue as ingest_enqueue  # type: ignore

            ingest_enqueue(
                "task_probe_email",
                {
                    "email_id": int(email_id),
                    "email": str(email),
                    "domain": str(canon_dom),
                    "force": bool(force),
                },
            )
            logger.info(
                "R16 probe enqueue via ingest shim: email_id=%s email=%s domain=%s",
                email_id,
                email,
                canon_dom,
            )
            # Do not early return; also try real RQ in runtime environments.
        except Exception as e:
            logger.debug("ingest.enqueue unavailable for probe; will attempt RQ: %s", e)

        # 2) Real RQ enqueue on 'verify'
        try:
            from rq import Queue  # type: ignore

            from src.queueing.redis_conn import get_redis  # type: ignore
            from src.queueing.tasks import task_probe_email  # type: ignore
        except Exception:
            return  # environment without RQ/Redis installed

        try:
            q = Queue("verify", connection=get_redis())
            q.enqueue(
                task_probe_email,
                email_id=email_id,
                email=email,
                domain=canon_dom,
                force=force,
                job_timeout=20,
                retry=None,
            )
            logger.info(
                "R16 probe enqueue via RQ: email_id=%s email=%s domain=%s",
                email_id,
                email,
                canon_dom,
            )
        except (ConnectionError, TimeoutError, OSError) as e:
            logger.warning("Queue degraded (probe not enqueued): %s", e)
        except Exception as e:
            logger.warning("Queue degraded (probe unexpected): %s", e)

    except Exception as e:
        # Never block ingest on enqueue errors.
        logger.debug("Ignoring R16 probe enqueue error (best-effort): %s", e)


# ---------------------------------------------------------------------------
# People/lead persistence
# ---------------------------------------------------------------------------


def _insert_person(con: Any, company_id: int, normalized: dict[str, Any]) -> None:
    """
    Insert a person row. Honors new R13/R14 fields if present in schema.
    Never drops provenance: source_url is passed through as provided.

    Works with both SQLite and PostgreSQL.
    """
    people_cols = _table_columns(con, "people")
    is_pg = _is_postgresql()

    desired: dict[str, Any] = {
        "company_id": company_id,
        "first_name": normalized.get("first_name") or "",
        "last_name": normalized.get("last_name") or "",
        "full_name": normalized.get("full_name") or "",
        "title": normalized.get("title") or "",
        "title_raw": (normalized.get("title_raw") or normalized.get("title") or ""),
        "title_norm": normalized.get("title_norm") or "",
        "role": normalized.get("role") or "",
        "role_family": normalized.get("role_family") or "",
        "seniority": normalized.get("seniority") or "",
        "source_url": normalized.get("source_url") or "",
        "notes": normalized.get("notes") or "",
        "errors": normalized.get("errors") or "",
    }

    payload = {k: v for k, v in desired.items() if k in people_cols}

    # R14: inline ICP scoring for new rows (null-safe, config-aware)
    needed = {"icp_score", "icp_reasons", "last_scored_at"}
    if ICPCFG and needed.issubset(people_cols):
        person_for_icp = {
            "domain": normalized.get("domain"),
            "role_family": normalized.get("role_family"),
            "seniority": normalized.get("seniority"),
        }
        try:
            res = compute_icp(person_for_icp, None, ICPCFG)
            payload["icp_score"] = int(res.score)
            payload["icp_reasons"] = json.dumps(res.reasons, ensure_ascii=False)
            payload["last_scored_at"] = (
                datetime.utcnow().isoformat(timespec="seconds") + "Z"
            )
        except Exception as e:  # best-effort; do not break ingest on scoring errors
            logger.warning("ICP scoring failed during insert; continuing without score: %s", e)

    cols = list(payload.keys())
    if not cols:
        con.execute("INSERT INTO people DEFAULT VALUES")
        return

    cols_sql = ",".join(cols)
    phs = _placeholders(is_pg, len(cols))
    sql = f"INSERT INTO people ({cols_sql}) VALUES ({phs})"
    con.execute(sql, [payload[c] for c in cols])


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _inject_user_supplied_from_raw(normalized: dict[str, Any], raw: dict[str, Any]) -> None:
    """
    If the normalizer dropped `user_supplied_domain`, restore it from the raw row
    so downstream enqueue can normalize and forward it.
    """
    if not normalized.get("user_supplied_domain"):
        usd = (raw.get("user_supplied_domain") or "").strip()
        if usd:
            normalized["user_supplied_domain"] = usd


def _compute_normalized_hint(normalized: dict[str, Any]) -> str | None:
    """
    Pull a domain/website hint from normalized input and normalize it for queuing.
    Preference order: user_supplied_domain -> domain -> website.
    """
    raw_hint = (
        (normalized.get("user_supplied_domain") or "").strip()
        or (normalized.get("domain") or "").strip()
        or (normalized.get("website") or "").strip()
    )
    raw_hint = raw_hint or None
    return norm_domain(raw_hint) if raw_hint else None


def persist_best_effort(normalized: dict[str, Any]) -> None:
    """
    Back-compat entrypoint that assumes the input dict is already normalized.
    (Used by earlier scripts/tests.) For new code, prefer upsert_row()/persist_rows()."

    Works with both SQLite and PostgreSQL.
    """
    with _get_connection() as con:
        company = (normalized.get("company") or "").strip() or None
        domain = (normalized.get("domain") or "").strip() or None

        # Company normalized fields (support legacy & R13 names)
        name_norm, norm_key = _extract_company_norm_fields(normalized)

        # Upsert company first (R07) without auto-merging by norm_key
        company_id = _upsert_company(con, company, domain, name_norm, norm_key)

        # Insert person row BEFORE any queueing (DB-first policy)
        _insert_person(con, company_id, normalized)

        # R08: enqueue resolver as best-effort after DB writes
        normalized_hint = _compute_normalized_hint(normalized)
        _enqueue_domain_resolution(con, company_id, (company or ""), normalized_hint)

        # R15: enqueue MX resolver if we already have a concrete non-freemail domain
        if domain:
            _enqueue_mx_resolution(company_id, domain, force=False)

        # NOTE (R16): We do NOT enqueue probe jobs here because this module doesn't
        # create emails. Probes should be enqueued where email rows are created
        # (e.g., R12 generation or extractors) using `_enqueue_probe_email(...)`.

        con.commit()


def upsert_row(raw: dict[str, Any]) -> None:
    """
    Normalize a single raw row and persist it (people + company).
    This is the primary function used by the ingest CLI when bulk API is unavailable.
    """
    normalized, _errs = normalize_row(raw)
    # Ensure user_supplied_domain survives normalization for enqueue
    _inject_user_supplied_from_raw(normalized, raw)
    persist_best_effort(normalized)


def persist_rows(rows: Iterable[dict[str, Any]]) -> int:
    """
    Normalize and persist an iterable of raw rows.
    Returns the number of rows persisted.

    Works with both SQLite and PostgreSQL.
    """
    n = 0
    with _get_connection() as con:
        for raw in rows:
            if not isinstance(raw, dict):
                continue
            normalized, _errs = normalize_row(raw)

            # Ensure user_supplied_domain survives normalization for enqueue
            _inject_user_supplied_from_raw(normalized, raw)

            company = (normalized.get("company") or "").strip() or None
            domain = (normalized.get("domain") or "").strip() or None
            name_norm, norm_key = _extract_company_norm_fields(normalized)

            company_id = _upsert_company(con, company, domain, name_norm, norm_key)

            # Insert person BEFORE attempting to enqueue
            _insert_person(con, company_id, normalized)
            n += 1

            # Best-effort enqueue after DB write
            normalized_hint = _compute_normalized_hint(normalized)
            _enqueue_domain_resolution(con, company_id, (company or ""), normalized_hint)

            # R15: enqueue MX resolver for concrete non-freemail domains
            if domain:
                _enqueue_mx_resolution(company_id, domain, force=False)

            # NOTE (R16): As above, this file doesn't create emails; call
            # _enqueue_probe_email(...) at the point where an email row is created.

        con.commit()
        return n
