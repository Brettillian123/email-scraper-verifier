# src/db_suppression.py
"""
Suppression list helpers for email verification.

This module provides functions to check and manage email/domain suppression
lists. Suppressed addresses are skipped during verification to avoid
unnecessary probes and comply with legal/business requirements.

Target state: Postgres is the only system of record.
All functions accept a connection from src.db.get_conn() (CompatConnection).
"""

from __future__ import annotations

import hashlib
import os
from typing import Any


def _normalize_email(email: str) -> str:
    """
    Normalize email for suppression lookups.

    - Strip surrounding whitespace
    - Lowercase
    """
    return email.strip().lower()


def _normalize_domain(domain: str) -> str:
    return domain.strip().lower()


def _env_tenant_id() -> str:
    """Get the default tenant ID from environment or use 'dev'."""
    return (os.getenv("TENANT_ID") or "").strip() or "dev"


def hash_email(email: str) -> str:
    """
    One-way hash for email addresses used in suppression, if the schema
    stores email hashes instead of plaintext.

    The exact algorithm is SHA-256 over the normalized (strip/lowercased)
    address. If your project already defines a canonical hashing helper,
    prefer that and keep this implementation in sync.
    """
    normalized = _normalize_email(email)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _suppression_columns(conn: Any) -> set[str]:
    """
    Introspect the suppression table and return its column names.

    This keeps the helpers resilient to minor schema variations
    (email vs email_hash, optional expires_at, etc.).

    Works with both SQLite and Postgres via the compat layer's PRAGMA emulation.
    """
    cur = conn.execute("PRAGMA table_info(suppression)")
    cols: set[str] = set()
    for row in cur.fetchall():
        # row[1] is the column name for PRAGMA table_info (both SQLite and emulated)
        if isinstance(row, tuple):
            cols.add(str(row[1]))
        else:
            cols.add(str(row["name"]))
    return cols


def _active_clause(cols: set[str]) -> str:
    """
    Return the SQL snippet that filters out expired suppressions, if the
    schema has an expires_at column.
    """
    if "expires_at" in cols:
        return " AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)"
    return ""


def is_email_suppressed(
    conn: Any,
    email: str,
    *,
    tenant_id: str | None = None,
) -> bool:
    """
    True if the specific email OR its domain is suppressed according to
    the suppression table.

    This function combines both address-level and domain-level suppression
    so callers don't have to remember to check both.

    It works with both plaintext (email) and hashed (email_hash) schemas:
    whichever column exists will be used.
    """
    t = tenant_id or _env_tenant_id()
    normalized = _normalize_email(email)
    _, _, domain_part = normalized.partition("@")
    domain = domain_part or ""

    cols = _suppression_columns(conn)
    has_tenant = "tenant_id" in cols

    conditions: list[str] = []
    params: list[Any] = []

    # Build tenant filter
    tenant_clause = ""
    if has_tenant:
        tenant_clause = "tenant_id = ? AND "
        params.append(t)

    if "email" in cols:
        conditions.append(f"({tenant_clause}email = ?)")
        params.append(normalized)

    if "email_hash" in cols:
        conditions.append(f"({tenant_clause}email_hash = ?)")
        params.append(hash_email(normalized))

    if domain and "domain" in cols:
        conditions.append(f"({tenant_clause}domain = ?)")
        params.append(_normalize_domain(domain))

    # If the suppression table doesn't have any of the expected columns,
    # treat everything as unsuppressed rather than failing hard.
    if not conditions:
        return False

    where_clause = " OR ".join(conditions)
    sql = f"SELECT 1 FROM suppression WHERE ({where_clause}){_active_clause(cols)} LIMIT 1"

    cur = conn.execute(sql, tuple(params))
    return cur.fetchone() is not None


def is_domain_suppressed(
    conn: Any,
    domain: str,
    *,
    tenant_id: str | None = None,
) -> bool:
    """
    True if this entire domain is suppressed (e.g., bounced too much,
    legal request, etc.).
    """
    t = tenant_id or _env_tenant_id()
    normalized_domain = _normalize_domain(domain)
    cols = _suppression_columns(conn)

    if "domain" not in cols:
        return False

    has_tenant = "tenant_id" in cols
    active = _active_clause(cols)

    if has_tenant:
        sql = f"SELECT 1 FROM suppression WHERE tenant_id = ? AND domain = ?{active} LIMIT 1"
        params: tuple[Any, ...] = (t, normalized_domain)
    else:
        sql = f"SELECT 1 FROM suppression WHERE domain = ?{active} LIMIT 1"
        params = (normalized_domain,)

    cur = conn.execute(sql, params)
    return cur.fetchone() is not None


def upsert_suppression(
    conn: Any,
    *,
    email: str,
    reason: str,
    source: str,
    tenant_id: str | None = None,
) -> None:
    """
    Insert or update a suppression row for the given email.

    The key column is chosen based on the suppression schema:
      - If email exists, we use it.
      - Else if email_hash exists, we hash the normalized email and use that.

    For Postgres, uses ON CONFLICT ... DO UPDATE (upsert pattern).
    For SQLite (legacy dev mode), uses INSERT OR REPLACE.

    The suppression table typically has:
        UNIQUE(tenant_id, email, domain)

    This upserts the row keyed by (tenant_id, email, NULL domain) — domain-based
    suppressions (rows with only domain set) are unaffected.
    """
    t = tenant_id or _env_tenant_id()
    normalized = _normalize_email(email)
    cols = _suppression_columns(conn)

    key_col: str | None = None
    key_value: str | None = None

    if "email" in cols:
        key_col = "email"
        key_value = normalized
    elif "email_hash" in cols:
        key_col = "email_hash"
        key_value = hash_email(normalized)

    if key_col is None or key_value is None:
        raise RuntimeError("suppression table must have either an 'email' or 'email_hash' column")

    has_tenant = "tenant_id" in cols
    has_domain = "domain" in cols
    has_created_at = "created_at" in cols
    is_postgres = getattr(conn, "is_postgres", False)

    if is_postgres:
        # Use Postgres ON CONFLICT upsert
        if has_tenant and has_domain:
            # Schema has UNIQUE(tenant_id, email, domain)
            if has_created_at:
                sql = f"""
                    INSERT INTO suppression (
                      tenant_id, {key_col}, domain, reason, source, created_at
                    )
                    VALUES (?, ?, NULL, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT (tenant_id, {key_col}, domain)
                    DO UPDATE SET reason = EXCLUDED.reason, source = EXCLUDED.source
                """
                conn.execute(sql, (t, key_value, reason, source))
            else:
                sql = f"""
                    INSERT INTO suppression (tenant_id, {key_col}, domain, reason, source)
                    VALUES (?, ?, NULL, ?, ?)
                    ON CONFLICT (tenant_id, {key_col}, domain)
                    DO UPDATE SET reason = EXCLUDED.reason, source = EXCLUDED.source
                """
                conn.execute(sql, (t, key_value, reason, source))
        elif has_tenant:
            # Simpler unique constraint
            if has_created_at:
                sql = f"""
                    INSERT INTO suppression (
                      tenant_id, {key_col}, reason, source, created_at
                    )
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT (tenant_id, {key_col})
                    DO UPDATE SET reason = EXCLUDED.reason, source = EXCLUDED.source
                """
                conn.execute(sql, (t, key_value, reason, source))
            else:
                sql = f"""
                    INSERT INTO suppression (tenant_id, {key_col}, reason, source)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT (tenant_id, {key_col})
                    DO UPDATE SET reason = EXCLUDED.reason, source = EXCLUDED.source
                """
                conn.execute(sql, (t, key_value, reason, source))
        else:
            # No tenant_id column (legacy schema)
            if has_created_at:
                sql = f"""
                    INSERT INTO suppression ({key_col}, reason, source, created_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT ({key_col})
                    DO UPDATE SET reason = EXCLUDED.reason, source = EXCLUDED.source
                """
                conn.execute(sql, (key_value, reason, source))
            else:
                sql = f"""
                    INSERT INTO suppression ({key_col}, reason, source)
                    VALUES (?, ?, ?)
                    ON CONFLICT ({key_col})
                    DO UPDATE SET reason = EXCLUDED.reason, source = EXCLUDED.source
                """
                conn.execute(sql, (key_value, reason, source))
    else:
        # Legacy SQLite fallback (dev mode only)
        if has_tenant:
            if has_created_at:
                sql = f"""
                    INSERT OR REPLACE INTO suppression (
                      tenant_id, {key_col}, reason, source, created_at
                    )
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                """
                conn.execute(sql, (t, key_value, reason, source))
            else:
                sql = f"""
                    INSERT OR REPLACE INTO suppression (tenant_id, {key_col}, reason, source)
                    VALUES (?, ?, ?, ?)
                """
                conn.execute(sql, (t, key_value, reason, source))
        else:
            if has_created_at:
                sql = f"""
                    INSERT OR REPLACE INTO suppression ({key_col}, reason, source, created_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                """
                conn.execute(sql, (key_value, reason, source))
            else:
                sql = f"""
                    INSERT OR REPLACE INTO suppression ({key_col}, reason, source)
                    VALUES (?, ?, ?)
                """
                conn.execute(sql, (key_value, reason, source))


def delete_suppression(
    conn: Any,
    *,
    email: str | None = None,
    domain: str | None = None,
    tenant_id: str | None = None,
) -> int:
    """
    Remove suppression entries matching the given email and/or domain.

    Returns the number of rows deleted.
    """
    if not email and not domain:
        return 0

    t = tenant_id or _env_tenant_id()
    cols = _suppression_columns(conn)
    has_tenant = "tenant_id" in cols

    conditions: list[str] = []
    params: list[Any] = []

    if has_tenant:
        conditions.append("tenant_id = ?")
        params.append(t)

    if email and "email" in cols:
        conditions.append("email = ?")
        params.append(_normalize_email(email))
    elif email and "email_hash" in cols:
        conditions.append("email_hash = ?")
        params.append(hash_email(email))

    if domain and "domain" in cols:
        conditions.append("domain = ?")
        params.append(_normalize_domain(domain))

    if not conditions:
        return 0

    sql = f"DELETE FROM suppression WHERE {' AND '.join(conditions)}"
    cur = conn.execute(sql, tuple(params))

    return cur.rowcount if cur.rowcount >= 0 else 0


def list_suppressions(
    conn: Any,
    *,
    tenant_id: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """
    List suppression entries for a tenant.

    Returns a list of dicts with the suppression row data.
    """
    t = tenant_id or _env_tenant_id()
    cols = _suppression_columns(conn)
    has_tenant = "tenant_id" in cols

    if has_tenant:
        sql = "SELECT * FROM suppression WHERE tenant_id = ? ORDER BY id DESC LIMIT ? OFFSET ?"
        params: tuple[Any, ...] = (t, limit, offset)
    else:
        sql = "SELECT * FROM suppression ORDER BY id DESC LIMIT ? OFFSET ?"
        params = (limit, offset)

    cur = conn.execute(sql, params)
    rows = cur.fetchall() or []

    result: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, tuple):
            # Convert tuple to dict using column names
            desc = cur.description or []
            col_names = [d[0] for d in desc] if desc else list(range(len(row)))
            result.append(dict(zip(col_names, row, strict=False)))
        else:
            # Already dict-like
            result.append(dict(row))

    return result
