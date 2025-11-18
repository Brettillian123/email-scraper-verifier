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
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from collections.abc import Iterable
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


def _sqlite_path_from_env() -> str:
    url = os.getenv("DATABASE_URL", "").strip()
    if not url.startswith("sqlite:///"):
        raise RuntimeError(f"DATABASE_URL must be sqlite:///...; got {url!r}")
    # works for Windows paths like C:/... and POSIX /...
    return url[len("sqlite:///") :]


def _table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    cur = con.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}  # column name is at index 1


def _extract_company_norm_fields(normalized: dict[str, Any]) -> tuple[str | None, str | None]:
    """
    Support both legacy and R13 field names from normalize_row():
      - display name: company_name_norm (legacy) or company_norm (R13)
      - key: company_norm_key (legacy) or company_key (R13)
    """
    name_norm = (
        normalized.get("company_name_norm") or normalized.get("company_norm") or ""
    ).strip() or None
    norm_key = (
        normalized.get("company_norm_key") or normalized.get("company_key") or ""
    ).strip() or None
    return name_norm, norm_key


# ---------------------------------------------------------------------------
# Company upsert (no auto-merge by norm_key)
# ---------------------------------------------------------------------------


def _upsert_company(
    con: sqlite3.Connection,
    name: str | None,
    domain: str | None,
    name_norm: str | None,
    norm_key: str | None,
) -> int:
    """
    Upsert a company record keyed by domain, then by *exact* name.
    - On INSERT, set name/domain plus R13 fields (name_norm, norm_key) when available.
    - On UPDATE, only fill empty (NULL/'') fields using COALESCE(NULLIF(col,''), ?).
    """
    cur = con.cursor()
    cols = _table_columns(con, "companies")

    def _insert(n: str | None, d: str | None, nn: str | None, nk: str | None) -> int:
        insert_cols: list[str] = []
        vals: list[Any] = []
        if "name" in cols:
            insert_cols.append("name")
            vals.append(n)
        if "domain" in cols:
            insert_cols.append("domain")
            vals.append(d)
        if "name_norm" in cols:
            insert_cols.append("name_norm")
            vals.append(nn)
        if "norm_key" in cols:
            insert_cols.append("norm_key")
            vals.append(nk)
        placeholders = ",".join("?" for _ in insert_cols) or "DEFAULT VALUES"
        if insert_cols:
            cur.execute(
                f"INSERT INTO companies ({','.join(insert_cols)}) VALUES ({placeholders})",
                vals,
            )
        else:
            cur.execute("INSERT INTO companies DEFAULT VALUES")
        return int(cur.lastrowid)

    # Prefer domain key
    if domain:
        row = cur.execute(
            "SELECT id FROM companies WHERE domain = ?",
            (domain,),
        ).fetchone()
        if row:
            company_id = int(row[0])
            # Fill missing display name if we learned one
            if name and "name" in cols:
                cur.execute(
                    "UPDATE companies SET name = COALESCE(NULLIF(name,''), ?) WHERE id = ?",
                    (name, company_id),
                )
            # Fill R13 normalized fields if columns exist
            if "name_norm" in cols and name_norm:
                cur.execute(
                    "UPDATE companies "
                    "SET name_norm = COALESCE(NULLIF(name_norm,''), ?) "
                    "WHERE id = ?",
                    (name_norm, company_id),
                )
            if "norm_key" in cols and norm_key:
                cur.execute(
                    "UPDATE companies SET norm_key = COALESCE(NULLIF(norm_key,''), ?) WHERE id = ?",
                    (norm_key, company_id),
                )
            return company_id
        # No domain match → insert
        return _insert(name, domain, name_norm, norm_key)

    # No domain; fall back to exact name
    if name:
        row = cur.execute(
            "SELECT id FROM companies WHERE name = ?",
            (name,),
        ).fetchone()
        if row:
            company_id = int(row[0])
            # Fill normalized fields if available
            if "name_norm" in cols and name_norm:
                cur.execute(
                    "UPDATE companies "
                    "SET name_norm = COALESCE(NULLIF(name_norm,''), ?) "
                    "WHERE id = ?",
                    (name_norm, company_id),
                )
            if "norm_key" in cols and norm_key:
                cur.execute(
                    "UPDATE companies SET norm_key = COALESCE(NULLIF(norm_key,''), ?) WHERE id = ?",
                    (norm_key, company_id),
                )
            return company_id
        return _insert(name, None, name_norm, norm_key)

    # Shouldn’t happen (ingest guarantees name or domain), but be safe:
    return _insert(None, None, name_norm, norm_key)


# ---------------------------------------------------------------------------
# R08: enqueue async domain resolver; never set official domain here
# ---------------------------------------------------------------------------


def _enqueue_domain_resolution(
    con: sqlite3.Connection,
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
                "R15 MX enqueue via ingest shim: company_id=%s domain=%s", company_id, canon
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
            logger.info(
                "R15 MX enqueue via ingest shim: company_id=%s domain=%s",
                company_id,
                canon,
            )

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


def _insert_person(con: sqlite3.Connection, company_id: int, normalized: dict[str, Any]) -> None:
    """
    Insert a person row. Honors new R13/R14 fields if present in schema.
    Never drops provenance: source_url is passed through as provided.
    """
    people_cols = _table_columns(con, "people")
    payload: dict[str, Any] = {}

    # Foreign key
    if "company_id" in people_cols:
        payload["company_id"] = company_id

    # Core person fields (display names were normalized in normalize_row)
    if "first_name" in people_cols:
        payload["first_name"] = normalized.get("first_name") or ""
    if "last_name" in people_cols:
        payload["last_name"] = normalized.get("last_name") or ""
    if "full_name" in people_cols:
        payload["full_name"] = normalized.get("full_name") or ""

    # Title fields (original + normalized)
    if "title" in people_cols:
        payload["title"] = normalized.get("title") or ""
    if "title_raw" in people_cols:
        payload["title_raw"] = normalized.get("title_raw") or normalized.get("title") or ""
    if "title_norm" in people_cols:
        payload["title_norm"] = normalized.get("title_norm") or ""

    # Role (legacy), plus O02-derived role_family/seniority if present
    if "role" in people_cols:
        payload["role"] = normalized.get("role") or ""
    if "role_family" in people_cols:
        payload["role_family"] = normalized.get("role_family") or ""
    if "seniority" in people_cols:
        payload["seniority"] = normalized.get("seniority") or ""

    # Provenance
    if "source_url" in people_cols:
        payload["source_url"] = normalized.get("source_url") or ""

    # Notes
    if "notes" in people_cols:
        payload["notes"] = normalized.get("notes") or ""

    # Optional errors snapshot (if schema has it)
    if "errors" in people_cols:
        payload["errors"] = normalized.get("errors") or ""

    # R14: inline ICP scoring for new rows (null-safe, config-aware)
    # Only attempt if config is present and columns exist.
    if (
        ICPCFG
        and "icp_score" in people_cols
        and "icp_reasons" in people_cols
        and "last_scored_at" in people_cols
    ):
        person_for_icp = {
            "domain": normalized.get("domain"),
            "role_family": normalized.get("role_family"),
            "seniority": normalized.get("seniority"),
        }
        try:
            res = compute_icp(person_for_icp, None, ICPCFG)
            payload["icp_score"] = int(res.score)
            payload["icp_reasons"] = json.dumps(res.reasons, ensure_ascii=False)
            payload["last_scored_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        except Exception as e:  # best-effort; do not break ingest on scoring errors
            logger.warning("ICP scoring failed during insert; continuing without score: %s", e)

    cols = list(payload.keys())
    placeholders = ",".join("?" for _ in cols)
    con.execute(
        f"INSERT INTO people ({','.join(cols)}) VALUES ({placeholders})",
        [payload[c] for c in cols],
    )


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
    """
    db_path = _sqlite_path_from_env()
    con = sqlite3.connect(db_path)
    try:
        con.execute("PRAGMA foreign_keys = ON")
        con.row_factory = sqlite3.Row

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
    finally:
        con.close()


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
    """
    db_path = _sqlite_path_from_env()
    con = sqlite3.connect(db_path)
    n = 0
    try:
        con.execute("PRAGMA foreign_keys = ON")
        con.row_factory = sqlite3.Row

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

            # NOTE (R16): As above, this file doesn’t create emails; call
            # _enqueue_probe_email(...) at the point where an email row is created.

        con.commit()
        return n
    finally:
        con.close()
