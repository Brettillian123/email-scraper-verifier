# scripts/migrate_r15_add_domain_resolutions.py
from __future__ import annotations

"""
R15 — DNS/MX Lookup Service: Migration

Creates or amends the `domain_resolutions` table to support deterministic MX
resolution with caching.

Columns (exact names required by R15):
- id              INTEGER PK
- company_id      INTEGER FK (companies.id)
- domain          TEXT          canonical domain (lower(), NFKC)
- mx_hosts        TEXT          JSON list of MX hosts sorted by pref
- preference_map  TEXT          JSON map {mx_host: pref}
- lowest_mx       TEXT          lowest-preference host (string)
- resolved_at     TEXT          ISO8601 timestamp (UTC)
- ttl             INTEGER       TTL seconds; default 86400
- failure         TEXT          null or message

(Strongly recommended optional O06 column — added if missing)
- mx_behavior     TEXT          JSON summary (avg_latency_ms, timeout_rate, last_seen_codes, tarpit_flag)

This migration is:
- idempotent
- safe to run repeatedly
- styled like prior migrations (uses PRAGMA table_info checks)
"""

import argparse
import sqlite3
from collections.abc import Iterable

REQUIRED_COLUMNS: tuple[tuple[str, str, str], ...] = (
    # (name, type, default_sql)
    ("id", "INTEGER", None),  # PK handled on CREATE TABLE
    ("company_id", "INTEGER", None),
    ("domain", "TEXT", None),
    ("mx_hosts", "TEXT", None),
    ("preference_map", "TEXT", None),
    ("lowest_mx", "TEXT", None),
    ("resolved_at", "TEXT", None),
    ("ttl", "INTEGER", "86400"),
    ("failure", "TEXT", None),
)

# Optional (O06)
OPTIONAL_COLUMNS: tuple[tuple[str, str, str], ...] = (("mx_behavior", "TEXT", None),)


def table_exists(cur: sqlite3.Cursor, table: str) -> bool:
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? COLLATE NOCASE",
        (table,),
    )
    return cur.fetchone() is not None


def col_exists(cur: sqlite3.Cursor, table: str, col: str) -> bool:
    cur.execute(f"PRAGMA table_info({table})")
    return any(r[1] == col for r in cur.fetchall())


def existing_columns(cur: sqlite3.Cursor, table: str) -> Iterable[str]:
    cur.execute(f"PRAGMA table_info({table})")
    return [r[1] for r in cur.fetchall()]


def create_table(cur: sqlite3.Cursor) -> None:
    # Create with the full, current schema
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS domain_resolutions (
            id INTEGER PRIMARY KEY,
            company_id INTEGER,
            domain TEXT,
            mx_hosts TEXT,
            preference_map TEXT,
            lowest_mx TEXT,
            resolved_at TEXT,
            ttl INTEGER DEFAULT 86400,
            failure TEXT
        )
        """
    )
    # Optional column created separately for clarity/idempotency
    # (see ensure_optional_columns)


def ensure_required_columns(cur: sqlite3.Cursor) -> None:
    # Add any missing required columns one by one (idempotent)
    for name, typ, default_sql in REQUIRED_COLUMNS:
        if not col_exists(cur, "domain_resolutions", name):
            if default_sql is None:
                cur.execute(f"ALTER TABLE domain_resolutions ADD COLUMN {name} {typ}")
            else:
                cur.execute(
                    f"ALTER TABLE domain_resolutions ADD COLUMN {name} {typ} DEFAULT {default_sql}"
                )


def ensure_optional_columns(cur: sqlite3.Cursor) -> None:
    for name, typ, default_sql in OPTIONAL_COLUMNS:
        if not col_exists(cur, "domain_resolutions", name):
            if default_sql is None:
                cur.execute(f"ALTER TABLE domain_resolutions ADD COLUMN {name} {typ}")
            else:
                cur.execute(
                    f"ALTER TABLE domain_resolutions ADD COLUMN {name} {typ} DEFAULT {default_sql}"
                )


def ensure_indices(cur: sqlite3.Cursor) -> None:
    # Helpful indices (idempotent). Names chosen to be stable if they already exist.
    # Company lookup
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_domain_resolutions_company_id "
        "ON domain_resolutions(company_id)"
    )
    # Domain lookup (unique per domain if you want one row per domain; we DO NOT enforce uniqueness
    # to preserve idempotency with legacy data. If you later want uniqueness, do it in app code via upsert.)
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_domain_resolutions_domain ON domain_resolutions(domain)"
    )


def print_header(db_path: str) -> None:
    print(f"Using DB: {db_path}")
    print("==> Applying R15 migration: domain_resolutions schema")


def print_status(cur: sqlite3.Cursor) -> None:
    exists = table_exists(cur, "domain_resolutions")
    print(f"· domain_resolutions                exists: {'yes' if exists else 'no'}")
    if exists:
        cols = ", ".join(sorted(existing_columns(cur, "domain_resolutions")))
        print(f"· Columns: {cols}")


def run(db_path: str) -> None:
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys = ON")  # harmless if FKs not declared
    try:
        cur = con.cursor()
        print_header(db_path)
        # Pre-status
        print_status(cur)

        # Ensure table and columns
        create_table(cur)
        ensure_required_columns(cur)
        ensure_optional_columns(cur)
        ensure_indices(cur)

        con.commit()

        # Post-status
        print("✔ R15 migration completed.")
        print_status(cur)
        print("✔ Indices ensured (idempotent).")
        print("✔ Schema applied (safe & repeatable).")
    finally:
        con.close()


def main() -> None:
    ap = argparse.ArgumentParser(
        description="R15 migration: create/ensure domain_resolutions schema."
    )
    ap.add_argument(
        "--db",
        default="data/dev.db",
        help="Path to SQLite database (default: data/dev.db)",
    )
    args = ap.parse_args()
    run(args.db)


if __name__ == "__main__":
    main()
