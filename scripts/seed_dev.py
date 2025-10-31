#!/usr/bin/env python
# scripts/seed_dev.py
from __future__ import annotations

import os
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]


def resolve_sqlite_path() -> Path:
    url = (os.getenv("DATABASE_URL") or "").strip()
    if not url:
        return ROOT / "dev.db"
    if not url.startswith("sqlite:"):
        raise SystemExit("ERROR: seed_dev.py only supports SQLite (sqlite:///...).")
    path = url[len("sqlite:") :]
    while path.startswith("/"):
        path = path[1:]
    p = Path(path)
    if p.drive or p.is_absolute():
        return p
    return (ROOT / p).resolve()


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def table_columns(conn: sqlite3.Connection, table: str) -> dict[str, sqlite3.Row]:
    cols = {}
    for r in conn.execute(f"PRAGMA table_info({table});"):
        cols[r["name"]] = r
    return cols


def ensure_company(conn: sqlite3.Connection, name: str, domain: str) -> int:
    cols = table_columns(conn, "companies")
    # Prefer a `domain` or `primary_domain` column if present
    domain_col = (
        "domain" if "domain" in cols else ("primary_domain" if "primary_domain" in cols else None)
    )

    # Try to find by domain if we have a domain column, else fallback to name.
    if domain_col:
        row = conn.execute(
            f"SELECT id FROM companies WHERE {domain_col} = ? LIMIT 1;",
            (domain,),
        ).fetchone()
        if row:
            return int(row["id"])

    row = conn.execute(
        "SELECT id FROM companies WHERE name = ? LIMIT 1;",
        (name,),
    ).fetchone()
    if row:
        cid = int(row["id"])
    else:
        if domain_col:
            conn.execute(
                f"INSERT INTO companies (name, {domain_col}) VALUES (?, ?);",
                (name, domain),
            )
        else:
            conn.execute("INSERT INTO companies (name) VALUES (?);", (name,))
        cid = int(conn.execute("SELECT last_insert_rowid();").fetchone()[0])

    # If there is a company_domains mapping table, ensure a mapping too.
    meta = conn.execute(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='company_domains';"
    ).fetchone()[0]
    if meta:
        conn.execute(
            "INSERT OR IGNORE INTO company_domains (company_id, domain) VALUES (?, ?);",
            (cid, domain),
        )
    return cid


def insert_email(
    conn: sqlite3.Connection,
    company_id: int,
    email: str,
    is_published: int = 1,
    source_url: str = "seed://dev",
    icp_score: int = 0,
    verify_status: str | None = "valid",
    reason: str | None = "seed",
    mx_host: str | None = "mx.example.com",
) -> None:
    cols = table_columns(conn, "emails")

    now = datetime.now(UTC).isoformat(timespec="seconds")
    payload: dict[str, Any] = {"email": email, "company_id": company_id}

    if "is_published" in cols:
        payload["is_published"] = is_published
    if "source_url" in cols:
        payload["source_url"] = source_url
    if "icp_score" in cols:
        payload["icp_score"] = icp_score
    if "verify_status" in cols:
        payload["verify_status"] = verify_status
    if "reason" in cols:
        payload["reason"] = reason
    if "mx_host" in cols:
        payload["mx_host"] = mx_host
    if "verified_at" in cols:
        payload["verified_at"] = now

    keys = ", ".join(payload.keys())
    qmarks = ", ".join(["?"] * len(payload))
    values: list[Any] = list(payload.values())

    # Idempotent for SQLite
    sql = f"INSERT OR IGNORE INTO emails ({keys}) VALUES ({qmarks});"
    conn.execute(sql, values)


def main() -> None:
    db_path = resolve_sqlite_path()
    print(f"→ Seeding SQLite at: {db_path}")

    with connect(db_path) as conn:
        # Minimal sanity checks
        need = ["companies", "emails"]
        for t in need:
            exists = conn.execute(
                "SELECT count(*) FROM sqlite_master WHERE type IN ('table','view') AND name = ?;",
                (t,),
            ).fetchone()[0]
            if not exists:
                raise SystemExit(f"ERROR: '{t}' is missing. Run scripts/apply_schema.py first.")

        # Ensure two companies
        ex_id = ensure_company(conn, "Example Inc.", "example.com")
        cr_id = ensure_company(conn, "Crestwell Partners", "crestwellpartners.com")

        # Two emails (safe to re-run)
        insert_email(conn, ex_id, "alice@example.com")
        insert_email(conn, cr_id, "banderson@crestwellpartners.com")

        conn.commit()

        # Quick peek
        sample = conn.execute(
            "SELECT id, email, company_id, verify_status, verified_at FROM emails ORDER BY id LIMIT 10;"
        ).fetchall()
        print("Seeded emails (up to 10 shown):")
        for r in sample:
            print(
                f"· #{r['id']}: {r['email']} (company_id={r['company_id']} vs={r['verify_status']} t={r['verified_at']})"
            )

    print("✔ Seed complete (idempotent).")


if __name__ == "__main__":
    main()
