# src/db_ingest.py
from __future__ import annotations

import sqlite3

from rq import Queue

from src.db import _db_path
from src.queueing.redis_conn import get_redis


# ----- DB connection (lightweight helper) -----
def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(_db_path())
    con.row_factory = sqlite3.Row
    return con


# ----- 1) Upsert company (by normalized_company, normalized_domain?) -----
def upsert_company(
    normalized_company: str | None,
    normalized_domain: str | None,
) -> int:
    """
    Returns company_id.

    If only company provided (no domain), store with domain=NULL and
    domain_status='pending_resolution' (R08 will handle resolution).
    """
    con = _connect()
    cur = con.cursor()

    if normalized_domain:
        # Prefer domain match when present
        cur.execute(
            "SELECT id FROM companies WHERE domain = ?",
            (normalized_domain,),
        )
        row = cur.fetchone()
        if row:
            company_id = int(row["id"])
            # (No other updates required for R07)
            con.close()
            return company_id

        # Insert with domain known
        cur.execute(
            "INSERT INTO companies (name, domain) VALUES (?, ?)",
            (normalized_company, normalized_domain),
        )
        company_id = int(cur.lastrowid)
        con.commit()
        con.close()
        return company_id

    # No domain → pending_resolution
    cur.execute(
        "SELECT id FROM companies WHERE name = ? AND domain IS NULL",
        (normalized_company,),
    )
    row = cur.fetchone()
    if row:
        company_id = int(row["id"])
        con.close()
        return company_id

    cur.execute(
        "INSERT INTO companies (name, domain, domain_status) VALUES (?, NULL, ?)",
        (normalized_company, "pending_resolution"),
    )
    company_id = int(cur.lastrowid)
    con.commit()
    con.close()
    return company_id


# ----- 2) Upsert person under company_id -----
def upsert_person(
    company_id: int,
    first_name: str | None,
    last_name: str | None,
    title: str | None,
    role: str | None,
) -> int:
    """
    Returns person_id.

    Match on (company_id, first_name, last_name). Update title/role if provided.
    """
    con = _connect()
    cur = con.cursor()

    # Build WHERE that handles NULLs correctly
    where_fn = "first_name IS NULL" if first_name is None else "first_name = ?"
    where_ln = "last_name IS NULL" if last_name is None else "last_name = ?"

    params: list = [company_id]
    if first_name is not None:
        params.append(first_name)
    if last_name is not None:
        params.append(last_name)

    query = f"SELECT id, title, role FROM people WHERE company_id = ? AND {where_fn} AND {where_ln}"
    cur.execute(query, params)
    row = cur.fetchone()
    if row:
        person_id = int(row["id"])
        # Update title/role if provided (minimal upsert)
        updates: list[str] = []
        vals: list = []
        if title is not None and title != row["title"]:
            updates.append("title = ?")
            vals.append(title)
        if role is not None and role != row["role"]:
            updates.append("role = ?")
            vals.append(role)
        if updates:
            vals.append(person_id)
            cur.execute(f"UPDATE people SET {', '.join(updates)} WHERE id = ?", vals)
            con.commit()
        con.close()
        return person_id

    # Insert new person
    cur.execute(
        "INSERT INTO people (company_id, first_name, last_name, title, role) "
        "VALUES (?, ?, ?, ?, ?)",
        (company_id, first_name, last_name, title, role),
    )
    person_id = int(cur.lastrowid)
    con.commit()
    con.close()
    return person_id


# ----- 3) Upsert email placeholder (optional in R07) -----
def upsert_email_placeholder(
    company_id: int,
    person_id: int,
    email: str | None,
) -> int | None:
    """
    If an explicit email address is present, ensure an emails row exists
    and return email_id. Otherwise return None (R07 allows skipping).
    """
    if not email:
        return None

    con = _connect()
    cur = con.cursor()

    # Reuse unique index on emails.email (ux_emails_email)
    cur.execute("SELECT id FROM emails WHERE email = ?", (email,))
    row = cur.fetchone()
    if row:
        email_id = int(row["id"])
        con.close()
        return email_id

    cur.execute(
        "INSERT INTO emails (email, person_id, company_id) VALUES (?, ?, ?)",
        (email, person_id, company_id),
    )
    email_id = int(cur.lastrowid)
    con.commit()
    con.close()
    return email_id


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
