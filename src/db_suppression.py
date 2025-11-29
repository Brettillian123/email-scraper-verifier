from __future__ import annotations

import hashlib
import sqlite3


def _normalize_email(email: str) -> str:
    """
    Normalize email for suppression lookups.

    - Strip surrounding whitespace
    - Lowercase
    """
    return email.strip().lower()


def _normalize_domain(domain: str) -> str:
    return domain.strip().lower()


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


def _suppression_columns(conn: sqlite3.Connection) -> set[str]:
    """
    Introspect the suppression table and return its column names.

    This keeps the helpers resilient to minor schema variations
    (email vs email_hash, optional expires_at, etc.).
    """
    cur = conn.execute("PRAGMA table_info(suppression)")
    cols: set[str] = set()
    for row in cur.fetchall():
        # row[1] is the column name for PRAGMA table_info
        cols.add(row[1])
    return cols


def _active_clause(cols: set[str]) -> str:
    """
    Return the SQL snippet that filters out expired suppressions, if the
    schema has an expires_at column.
    """
    if "expires_at" in cols:
        return " AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)"
    return ""


def is_email_suppressed(conn: sqlite3.Connection, email: str) -> bool:
    """
    True if the specific email OR its domain is suppressed according to
    the suppression table.

    This function combines both address-level and domain-level suppression
    so callers don't have to remember to check both.

    It works with both plaintext (email) and hashed (email_hash) schemas:
    whichever column exists will be used.
    """
    normalized = _normalize_email(email)
    _, _, domain_part = normalized.partition("@")
    domain = domain_part or ""

    cols = _suppression_columns(conn)
    conditions: list[str] = []
    params: list[str] = []

    if "email" in cols:
        conditions.append("email = ?")
        params.append(normalized)

    if "email_hash" in cols:
        conditions.append("email_hash = ?")
        params.append(hash_email(normalized))

    if domain and "domain" in cols:
        conditions.append("domain = ?")
        params.append(_normalize_domain(domain))

    # If the suppression table doesn't have any of the expected columns,
    # treat everything as unsuppressed rather than failing hard.
    if not conditions:
        return False

    where_clause = " OR ".join(conditions)
    sql = f"SELECT 1 FROM suppression WHERE ({where_clause}){_active_clause(cols)} LIMIT 1"

    cur = conn.execute(sql, params)
    return cur.fetchone() is not None


def is_domain_suppressed(conn: sqlite3.Connection, domain: str) -> bool:
    """
    True if this entire domain is suppressed (e.g., bounced too much,
    legal request, etc.).
    """
    normalized_domain = _normalize_domain(domain)
    cols = _suppression_columns(conn)

    if "domain" not in cols:
        return False

    sql = f"SELECT 1 FROM suppression WHERE domain = ?{_active_clause(cols)} LIMIT 1"
    cur = conn.execute(sql, (normalized_domain,))
    return cur.fetchone() is not None


def upsert_suppression(
    conn: sqlite3.Connection,
    *,
    email: str,
    reason: str,
    source: str,
) -> None:
    """
    Insert or update a suppression row for the given email.

    The key column is chosen based on the suppression schema:
      - If email exists, we use it.
      - Else if email_hash exists, we hash the normalized email and use that.

    Implementation detail:
      - Uses SQLite's INSERT OR REPLACE, which will honor whichever UNIQUE
        constraint exists (e.g. UNIQUE(email, domain) or UNIQUE(email_hash)).
      - For plaintext schema like:

            CREATE TABLE suppression (
              id INTEGER PRIMARY KEY,
              email TEXT,
              domain TEXT,
              reason TEXT,
              source TEXT,
              created_at TEXT DEFAULT (datetime('now')),
              UNIQUE(email, domain)
            );

        this will upsert the row keyed by (email, NULL) — domain-based
        suppressions (rows with only domain set) are unaffected.
    """
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
        # Schema doesn't have an obvious key column; better to fail loud
        # here so it can be fixed alongside a migration.
        raise RuntimeError("suppression table must have either an 'email' or 'email_hash' column")

    has_created_at = "created_at" in cols

    if has_created_at:
        sql = f"""
            INSERT OR REPLACE INTO suppression ({key_col}, reason, source, created_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        """
        params = (key_value, reason, source)
    else:
        sql = f"""
            INSERT OR REPLACE INTO suppression ({key_col}, reason, source)
            VALUES (?, ?, ?)
        """
        params = (key_value, reason, source)

    conn.execute(sql, params)
