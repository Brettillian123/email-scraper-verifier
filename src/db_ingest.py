# src/db_ingest.py
"""
Database ingestion helpers for R07 and related flows.

This module provides CRUD operations for companies, people, and emails
during the ingestion pipeline. All operations are tenant-aware and use
the unified get_conn() from src.db which supports PostgreSQL.

Target state: Postgres is the only system of record.
"""

from __future__ import annotations

import os
from typing import Any

from rq import Queue

from src.db import get_conn
from src.queueing.redis_conn import get_redis


def _env_tenant_id() -> str:
    """Get the default tenant ID from environment or use 'dev'."""
    return (os.getenv("TENANT_ID") or "").strip() or "dev"


# ----- 1) Upsert company (by normalized_company, normalized_domain?) -----
def upsert_company(
    normalized_company: str | None,
    normalized_domain: str | None,
    *,
    tenant_id: str | None = None,
) -> int:
    """
    Returns company_id.

    If only company provided (no domain), store with domain=NULL and
    domain_status='pending_resolution' (R08 will handle resolution).
    """
    t = tenant_id or _env_tenant_id()

    with get_conn() as conn:
        if normalized_domain:
            # Prefer domain match when present
            cur = conn.execute(
                "SELECT id FROM companies WHERE tenant_id = ? AND domain = ?",
                (t, normalized_domain),
            )
            row = cur.fetchone()
            if row:
                return int(row[0] if isinstance(row, tuple) else row["id"])

            # Insert with domain known
            cur = conn.execute(
                "INSERT INTO companies (tenant_id, name, domain) VALUES (?, ?, ?)",
                (t, normalized_company, normalized_domain),
            )
            conn.commit()
            if cur.lastrowid:
                return int(cur.lastrowid)

            # Fallback: re-select (handles race conditions)
            cur = conn.execute(
                "SELECT id FROM companies WHERE tenant_id = ? AND domain = ?",
                (t, normalized_domain),
            )
            row = cur.fetchone()
            if row:
                return int(row[0] if isinstance(row, tuple) else row["id"])
            raise RuntimeError("Failed to upsert company with domain")

        # No domain → pending_resolution
        cur = conn.execute(
            "SELECT id FROM companies WHERE tenant_id = ? AND name = ? AND domain IS NULL",
            (t, normalized_company),
        )
        row = cur.fetchone()
        if row:
            return int(row[0] if isinstance(row, tuple) else row["id"])

        # Check if schema has domain_status column
        cols_cur = conn.execute("PRAGMA table_info(companies)")
        cols = {r[1] for r in cols_cur.fetchall()}

        if "domain_status" in cols:
            cur = conn.execute(
                (
                    "INSERT INTO companies (tenant_id, name, domain, domain_status) "
                    "VALUES (?, ?, NULL, ?)"
                ),
                (t, normalized_company, "pending_resolution"),
            )
        else:
            cur = conn.execute(
                "INSERT INTO companies (tenant_id, name, domain) VALUES (?, ?, NULL)",
                (t, normalized_company),
            )

        conn.commit()
        if cur.lastrowid:
            return int(cur.lastrowid)

        # Fallback: re-select
        cur = conn.execute(
            "SELECT id FROM companies WHERE tenant_id = ? AND name = ? AND domain IS NULL",
            (t, normalized_company),
        )
        row = cur.fetchone()
        if row:
            return int(row[0] if isinstance(row, tuple) else row["id"])
        raise RuntimeError("Failed to upsert company without domain")


# ----- 2) Upsert person under company_id -----
def upsert_person(
    company_id: int,
    first_name: str | None,
    last_name: str | None,
    title: str | None,
    role: str | None,
    *,
    tenant_id: str | None = None,
) -> int:
    """
    Returns person_id.

    Match on (tenant_id, company_id, first_name, last_name). Update title/role if provided.
    """
    t = tenant_id or _env_tenant_id()

    with get_conn() as conn:
        # Build WHERE that handles NULLs correctly
        where_parts = ["tenant_id = ?", "company_id = ?"]
        params: list[Any] = [t, company_id]

        if first_name is None:
            where_parts.append("first_name IS NULL")
        else:
            where_parts.append("first_name = ?")
            params.append(first_name)

        if last_name is None:
            where_parts.append("last_name IS NULL")
        else:
            where_parts.append("last_name = ?")
            params.append(last_name)

        where_clause = " AND ".join(where_parts)
        cur = conn.execute(
            f"SELECT id, title, role FROM people WHERE {where_clause}",
            tuple(params),
        )
        row = cur.fetchone()

        if row:
            person_id = int(row[0] if isinstance(row, tuple) else row["id"])
            existing_title = row[1] if isinstance(row, tuple) else row["title"]
            existing_role = row[2] if isinstance(row, tuple) else row["role"]

            # Update title/role if provided and different
            updates: list[str] = []
            vals: list[Any] = []
            if title is not None and title != existing_title:
                updates.append("title = ?")
                vals.append(title)
            if role is not None and role != existing_role:
                updates.append("role = ?")
                vals.append(role)

            if updates:
                vals.append(person_id)
                conn.execute(f"UPDATE people SET {', '.join(updates)} WHERE id = ?", tuple(vals))
                conn.commit()

            return person_id

        # Insert new person
        cur = conn.execute(
            "INSERT INTO people (tenant_id, company_id, first_name, last_name, title, role) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (t, company_id, first_name, last_name, title, role),
        )
        conn.commit()

        if cur.lastrowid:
            return int(cur.lastrowid)

        # Fallback: re-select
        cur = conn.execute(
            f"SELECT id FROM people WHERE {where_clause}",
            tuple(params),
        )
        row = cur.fetchone()
        if row:
            return int(row[0] if isinstance(row, tuple) else row["id"])
        raise RuntimeError("Failed to upsert person")


# ----- 3) Upsert email placeholder (optional in R07) -----
def upsert_email_placeholder(
    company_id: int,
    person_id: int,
    email: str | None,
    *,
    tenant_id: str | None = None,
) -> int | None:
    """
    If an explicit email address is present, ensure an emails row exists
    and return email_id. Otherwise return None (R07 allows skipping).
    """
    if not email:
        return None

    t = tenant_id or _env_tenant_id()
    email_norm = email.strip().lower()

    with get_conn() as conn:
        # Check for existing email (tenant-scoped unique index)
        cur = conn.execute(
            "SELECT id FROM emails WHERE tenant_id = ? AND email = ?",
            (t, email_norm),
        )
        row = cur.fetchone()
        if row:
            return int(row[0] if isinstance(row, tuple) else row["id"])

        # Insert new email
        cur = conn.execute(
            "INSERT INTO emails (tenant_id, email, person_id, company_id) VALUES (?, ?, ?, ?)",
            (t, email_norm, person_id, company_id),
        )
        conn.commit()

        if cur.lastrowid:
            return int(cur.lastrowid)

        # Fallback: re-select (handles race conditions with ON CONFLICT DO NOTHING)
        cur = conn.execute(
            "SELECT id FROM emails WHERE tenant_id = ? AND email = ?",
            (t, email_norm),
        )
        row = cur.fetchone()
        if row:
            return int(row[0] if isinstance(row, tuple) else row["id"])

        return None


# ----- 4) Enqueue follow-ups (R08–R12 hooks; reuse R06 infrastructure) -----
def enqueue_followups(
    company_id: int,
    domain_present: bool,
    person_id: int,
    has_first_last: bool,
    email_id: int | None,
) -> None:
    """
    - If domain missing → enqueue resolve_company_domain(company_id) (R08).
    - If domain present and we have first/last but no email →
      enqueue generate_permutations(person_id, company_id) (R12).
    - If explicit email present → enqueue verify_email(email_id) (R06).
    """
    q = Queue("verify", connection=get_redis())

    if not domain_present:
        # Defer implementation to R08; enqueue by import path (no import needed here)
        q.enqueue("src.queueing.tasks.resolve_company_domain", company_id)

    if domain_present and has_first_last and not email_id:
        # Defer implementation to R12
        q.enqueue("src.queueing.tasks.generate_permutations", person_id, company_id)

    if email_id is not None:
        # Reuse R06 verification path
        q.enqueue("src.queueing.tasks.verify_email", email_id)


# ----- 5) Bulk upsert helpers (for batch ingestion) -----
def bulk_upsert_companies(
    companies: list[dict[str, Any]],
    *,
    tenant_id: str | None = None,
) -> dict[str, int]:
    """
    Bulk upsert companies and return a mapping of domain -> company_id.

    Each dict in companies should have:
      - name: str | None
      - domain: str | None (normalized)

    Returns: {domain: company_id, ...} for companies with domains
    """
    t = tenant_id or _env_tenant_id()
    result: dict[str, int] = {}

    with get_conn() as conn:
        for company in companies:
            name = company.get("name")
            domain = company.get("domain")

            if domain:
                domain_norm = domain.strip().lower()
                cur = conn.execute(
                    "SELECT id FROM companies WHERE tenant_id = ? AND domain = ?",
                    (t, domain_norm),
                )
                row = cur.fetchone()
                if row:
                    result[domain_norm] = int(row[0] if isinstance(row, tuple) else row["id"])
                    continue

                # Insert new
                cur = conn.execute(
                    "INSERT INTO companies (tenant_id, name, domain) VALUES (?, ?, ?)",
                    (t, name, domain_norm),
                )
                if cur.lastrowid:
                    result[domain_norm] = int(cur.lastrowid)
            else:
                # No domain - skip for bulk operations (or handle separately)
                pass

        conn.commit()

    return result
