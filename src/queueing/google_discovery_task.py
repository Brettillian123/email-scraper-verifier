# src/queueing/google_discovery_task.py
"""
Google Discovery RQ task.

Finds companies needing more contacts, searches Google CSE for
C-suite LinkedIn profiles, inserts discovered people, and enqueues
email generation via the existing task_generate_emails pipeline.

Follows the same patterns as manual_candidates.py:
  - Lazy _get_conn() import
  - ? placeholders (CompatConnection translates for Postgres)
  - _utc_now_iso() helper
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

log = logging.getLogger(__name__)

GOOGLE_DISCOVERY_SOURCE = "google_cse:linkedin"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utc_now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _get_conn():
    from src.db import get_conn

    return get_conn()


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------


def _load_config(con: Any, tenant_id: str) -> dict[str, Any]:
    """Load google_discovery_config for tenant, returning defaults if no row."""
    row = con.execute(
        "SELECT enabled, companies_per_day, min_people_threshold, "
        "target_roles, daily_query_budget "
        "FROM google_discovery_config WHERE tenant_id = ?",
        (tenant_id,),
    ).fetchone()

    if not row:
        return {
            "enabled": False,
            "companies_per_day": 20,
            "min_people_threshold": 2,
            "target_roles": ["CEO", "CFO", "COO", "CTO", "CIO", "CHRO", "CMO"],
            "daily_query_budget": 140,
        }

    return {
        "enabled": bool(row[0]),
        "companies_per_day": int(row[1]),
        "min_people_threshold": int(row[2]),
        "target_roles": [r.strip() for r in (row[3] or "").split(",") if r.strip()],
        "daily_query_budget": int(row[4]),
    }


# ---------------------------------------------------------------------------
# DB queries
# ---------------------------------------------------------------------------


def _find_companies_needing_contacts(
    con: Any,
    tenant_id: str,
    threshold: int,
    limit: int,
) -> list[dict[str, Any]]:
    """
    Find companies with fewer than `threshold` people.
    Ordered by least people first (most starved companies get priority).
    """
    rows = con.execute(
        """
        SELECT c.id, c.name, COALESCE(NULLIF(c.official_domain, ''), c.domain) AS domain,
               COUNT(p.id) AS people_count
        FROM companies c
        LEFT JOIN people p ON p.company_id = c.id
        WHERE c.tenant_id = ?
          AND COALESCE(NULLIF(c.official_domain, ''), c.domain) IS NOT NULL
          AND COALESCE(NULLIF(c.official_domain, ''), c.domain) != ''
        GROUP BY c.id, c.name, COALESCE(NULLIF(c.official_domain, ''), c.domain)
        HAVING COUNT(p.id) < ?
        ORDER BY COUNT(p.id) ASC, c.id ASC
        LIMIT ?
        """,
        (tenant_id, threshold, limit),
    ).fetchall()

    return [{"id": r[0], "name": r[1] or "", "domain": r[2], "people_count": r[3]} for r in rows]


def _person_already_exists(
    con: Any,
    tenant_id: str,
    company_id: int,
    first_name: str,
    last_name: str,
) -> bool:
    """Check if a person with this name already exists at this company."""
    row = con.execute(
        "SELECT 1 FROM people WHERE tenant_id = ? AND company_id = ? "
        "AND LOWER(first_name) = ? AND LOWER(last_name) = ? LIMIT 1",
        (tenant_id, company_id, first_name.lower(), last_name.lower()),
    ).fetchone()
    return row is not None


def _insert_discovered_person(
    con: Any,
    tenant_id: str,
    company_id: int,
    first_name: str,
    last_name: str,
    title: str,
    source_url: str,
) -> int | None:
    """Insert a person row. Returns person_id or None on failure."""
    now = _utc_now_iso()
    full_name = f"{first_name} {last_name}"
    try:
        cur = con.execute(
            "INSERT INTO people (tenant_id, company_id, first_name, last_name, "
            "full_name, title_raw, source_url, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id",
            (
                tenant_id,
                company_id,
                first_name,
                last_name,
                full_name,
                title,
                source_url,
                now,
                now,
            ),
        )
        row = cur.fetchone()
        con.commit()
        return int(row[0]) if row else None
    except Exception:
        log.debug("Failed to insert person %s %s", first_name, last_name, exc_info=True)
        try:
            con.rollback()
        except Exception:
            pass
        return None


def _create_run_record(
    con: Any,
    tenant_id: str,
    trigger_type: str,
) -> int | None:
    """Create a discovery run record. Returns run ID."""
    now = _utc_now_iso()
    try:
        cur = con.execute(
            "INSERT INTO google_discovery_runs "
            "(tenant_id, status, trigger_type, started_at) "
            "VALUES (?, 'running', ?, ?) RETURNING id",
            (tenant_id, trigger_type, now),
        )
        row = cur.fetchone()
        con.commit()
        return int(row[0]) if row else None
    except Exception:
        log.debug("Failed to create discovery run record", exc_info=True)
        try:
            con.rollback()
        except Exception:
            pass
        return None


def _update_run_record(
    con: Any,
    run_id: int,
    *,
    status: str,
    companies_queried: int,
    queries_used: int,
    people_found: int,
    people_inserted: int,
    emails_generated: int,
    errors: list[str],
    details: list[dict],
) -> None:
    """Update the discovery run record with final results."""
    now = _utc_now_iso()
    try:
        con.execute(
            "UPDATE google_discovery_runs SET "
            "status = ?, companies_queried = ?, queries_used = ?, "
            "people_found = ?, people_inserted = ?, emails_generated = ?, "
            "errors = ?, finished_at = ?, details_json = ? "
            "WHERE id = ?",
            (
                status,
                companies_queried,
                queries_used,
                people_found,
                people_inserted,
                emails_generated,
                json.dumps(errors) if errors else None,
                now,
                json.dumps(details) if details else None,
                run_id,
            ),
        )
        con.commit()
    except Exception:
        log.debug("Failed to update discovery run %s", run_id, exc_info=True)
        try:
            con.rollback()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Per-company processing (extracted for complexity)
# ---------------------------------------------------------------------------


def _process_person(
    con: Any,
    tenant_id: str,
    company: dict[str, Any],
    person: Any,
    email_gen_fn: Any,
    errors: list[str],
) -> tuple[int, int, int]:
    """Process a single discovered person. Returns (found, inserted, emails)."""
    if person.confidence == "low":
        return (0, 0, 0)

    if _person_already_exists(con, tenant_id, company["id"], person.first_name, person.last_name):
        log.debug(
            "Skipping duplicate: %s %s at %s",
            person.first_name,
            person.last_name,
            company["domain"],
        )
        return (1, 0, 0)

    person_id = _insert_discovered_person(
        con,
        tenant_id,
        company["id"],
        person.first_name,
        person.last_name,
        person.title,
        person.source_url,
    )
    if not person_id:
        return (1, 0, 0)

    emails = _generate_email_for_person(email_gen_fn, person_id, person, company, errors)
    return (1, 1, emails)


def _generate_email_for_person(
    email_gen_fn: Any,
    person_id: int,
    person: Any,
    company: dict[str, Any],
    errors: list[str],
) -> int:
    """Try to generate emails for one person. Returns 1 on success, 0 otherwise."""
    if email_gen_fn is None:
        return 0
    try:
        result = email_gen_fn(
            person_id=person_id,
            first=person.first_name,
            last=person.last_name,
            domain=company["domain"],
        )
        if result and result.get("valid_email"):
            return 1
    except Exception as exc:
        errors.append(
            f"Email gen for {person.first_name} {person.last_name} at {company['domain']}: {exc}"
        )
    return 0


def _process_company(
    con: Any,
    tenant_id: str,
    company: dict[str, Any],
    roles: list[str],
    email_gen_fn: Any,
    errors: list[str],
) -> dict[str, Any]:
    """Run discovery for a single company. Returns detail dict + updates errors."""
    from src.search.google_discovery import discover_people_for_company

    detail: dict[str, Any] = {
        "company_id": company["id"],
        "company_name": company["name"],
        "domain": company["domain"],
        "queries_used": 0,
        "people_found": 0,
        "people_inserted": 0,
        "emails_generated": 0,
    }

    try:
        discovery = discover_people_for_company(
            company_name=company["name"],
            domain=company["domain"],
            roles=roles,
        )
        detail["queries_used"] = discovery.queries_used
        if discovery.errors:
            errors.extend(discovery.errors)

        for person in discovery.people:
            found, inserted, emails = _process_person(
                con, tenant_id, company, person, email_gen_fn, errors
            )
            detail["people_found"] += found
            detail["people_inserted"] += inserted
            detail["emails_generated"] += emails
    except Exception as exc:
        errors.append(f"Company {company['name']} ({company['domain']}): {exc}")
        log.exception("Discovery failed for company %s", company["name"])

    return detail


def _load_email_gen_fn() -> Any:
    """Import task_generate_emails, returning None if unavailable."""
    try:
        from src.queueing.tasks import task_generate_emails

        return task_generate_emails
    except ImportError:
        log.warning("Could not import task_generate_emails, emails won't be generated")
        return None


def _empty_run_result() -> dict[str, int]:
    return {
        "companies_queried": 0,
        "queries_used": 0,
        "people_found": 0,
        "people_inserted": 0,
        "emails_generated": 0,
    }


# ---------------------------------------------------------------------------
# Main task
# ---------------------------------------------------------------------------


def task_google_discovery(
    *,
    tenant_id: str = "dev",
    trigger_type: str = "manual",
) -> dict[str, Any]:
    """
    Main RQ task: discover C-suite contacts via Google CSE.

    Can be called directly (by cron script) or enqueued as an RQ job.
    """
    con = _get_conn()
    run_id = None

    try:
        config = _load_config(con, tenant_id)

        if trigger_type == "cron" and not config["enabled"]:
            log.info("Google discovery disabled for tenant=%s", tenant_id)
            return {"ok": True, "skipped": True, "reason": "disabled"}

        run_id = _create_run_record(con, tenant_id, trigger_type)

        companies = _find_companies_needing_contacts(
            con,
            tenant_id,
            threshold=config["min_people_threshold"],
            limit=config["companies_per_day"],
        )

        if not companies:
            log.info("No companies need more contacts")
            if run_id:
                _update_run_record(
                    con,
                    run_id,
                    status="succeeded",
                    errors=[],
                    details=[],
                    **_empty_run_result(),
                )
            return {"ok": True, **_empty_run_result()}

        log.info("Found %d companies needing contacts", len(companies))

        email_gen_fn = _load_email_gen_fn()
        errors: list[str] = []
        details: list[dict] = []
        total_queries = 0

        for company in companies:
            if total_queries >= config["daily_query_budget"]:
                log.info("Query budget exhausted (%d)", total_queries)
                break

            detail = _process_company(
                con,
                tenant_id,
                company,
                config["target_roles"],
                email_gen_fn,
                errors,
            )
            total_queries += detail["queries_used"]
            details.append(detail)

        totals = {
            "companies_queried": len(details),
            "queries_used": total_queries,
            "people_found": sum(d["people_found"] for d in details),
            "people_inserted": sum(d["people_inserted"] for d in details),
            "emails_generated": sum(d["emails_generated"] for d in details),
        }

        if run_id:
            _update_run_record(
                con,
                run_id,
                status="succeeded",
                errors=errors,
                details=details,
                **totals,
            )

        log.info("Google discovery complete: %s", totals)
        return {
            "ok": True,
            "run_id": run_id,
            "trigger_type": trigger_type,
            "errors": errors[:20],
            **totals,
        }

    except Exception as exc:
        log.exception("Google discovery task failed")
        if run_id:
            try:
                _update_run_record(
                    con,
                    run_id,
                    status="failed",
                    errors=[str(exc)],
                    details=[],
                    **_empty_run_result(),
                )
            except Exception:
                pass
        return {"ok": False, "error": str(exc)}
    finally:
        try:
            con.close()
        except Exception:
            pass
