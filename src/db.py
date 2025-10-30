# src/db.py
import os
import sqlite3
from datetime import UTC, datetime


def _db_path():
    url = os.environ["DATABASE_URL"]
    if not url.startswith("sqlite:///"):
        raise RuntimeError(f"Only sqlite supported in dev; got {url}")
    return url.removeprefix("sqlite:///")


def upsert_verification_result(
    *, email: str, verify_status: str, reason: str, mx_host: str
) -> None:
    db = _db_path()
    domain = email.split("@")[-1].lower().strip()
    ts = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    with sqlite3.connect(db) as con:
        con.execute("PRAGMA foreign_keys=ON")
        cur = con.cursor()

        # 1) Ensure there is a company for this domain (companies(domain) schema)
        cid_row = cur.execute(
            "SELECT id FROM companies WHERE domain = ?",
            (domain,),
        ).fetchone()
        if cid_row:
            company_id = cid_row[0]
        else:
            # minimal company; name can equal domain if unknown
            cur.execute(
                "INSERT INTO companies(name, domain) VALUES(?, ?)",
                (domain, domain),
            )
            company_id = cur.lastrowid

        # 2) Idempotency guard on emails.email
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_emails_email ON emails(email)")

        # 3) UPSERT by email
        cur.execute(
            """
            INSERT INTO emails (email, company_id, verify_status, reason, mx_host, verified_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(email) DO UPDATE SET
              company_id   = excluded.company_id,
              verify_status= excluded.verify_status,
              reason       = excluded.reason,
              mx_host      = excluded.mx_host,
              verified_at  = excluded.verified_at
            """,
            (email, company_id, verify_status, reason, mx_host, ts),
        )
        con.commit()
