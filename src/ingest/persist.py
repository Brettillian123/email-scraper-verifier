# src/ingest/persist.py
from __future__ import annotations

import os
import sqlite3
from typing import Any


def _sqlite_path_from_env() -> str:
    url = os.getenv("DATABASE_URL", "").strip()
    if not url.startswith("sqlite:///"):
        raise RuntimeError(f"DATABASE_URL must be sqlite:///...; got {url!r}")
    return url[len("sqlite:///") :]  # works for Windows paths like C:/... and POSIX /...


def _table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    cur = con.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}  # column name is at index 1


def _upsert_company(con: sqlite3.Connection, name: str | None, domain: str | None) -> int:
    cur = con.cursor()
    cols = _table_columns(con, "companies")

    def _insert(n: str | None, d: str | None) -> int:
        insert_cols: list[str] = []
        vals: list[Any] = []
        if "name" in cols:
            insert_cols.append("name")
            vals.append(n)
        if "domain" in cols:
            insert_cols.append("domain")
            vals.append(d)
        placeholders = ",".join("?" for _ in insert_cols)
        cur.execute(
            f"INSERT INTO companies ({','.join(insert_cols)}) VALUES ({placeholders})",
            vals,
        )
        return int(cur.lastrowid)

    if domain:
        row = cur.execute("SELECT id FROM companies WHERE domain = ?", (domain,)).fetchone()
        if row:
            company_id = int(row[0])
            if name and "name" in cols:
                cur.execute(
                    "UPDATE companies SET name = COALESCE(NULLIF(name,''), ?) WHERE id = ?",
                    (name, company_id),
                )
            return company_id
        return _insert(name, domain)

    if name:
        row = cur.execute("SELECT id FROM companies WHERE name = ?", (name,)).fetchone()
        if row:
            return int(row[0])
        return _insert(name, None)

    # Shouldn’t happen (ingest guarantees name or domain), but be safe:
    return _insert(None, None)


def persist_best_effort(normalized: dict[str, Any]) -> None:
    db_path = _sqlite_path_from_env()
    con = sqlite3.connect(db_path)
    try:
        con.execute("PRAGMA foreign_keys = ON")
        con.row_factory = sqlite3.Row

        company = (normalized.get("company") or "").strip() or None
        domain = (normalized.get("domain") or "").strip() or None
        company_id = _upsert_company(con, company, domain)

        people_cols = _table_columns(con, "people")
        payload: dict[str, Any] = {}
        if "company_id" in people_cols:
            payload["company_id"] = company_id
        if "first_name" in people_cols:
            payload["first_name"] = normalized.get("first_name") or ""
        if "last_name" in people_cols:
            payload["last_name"] = normalized.get("last_name") or ""
        if "full_name" in people_cols:
            payload["full_name"] = normalized.get("full_name") or ""
        if "title" in people_cols:
            payload["title"] = normalized.get("title") or ""
        if "role" in people_cols:
            payload["role"] = normalized.get("role") or ""
        if "source_url" in people_cols:
            payload["source_url"] = normalized.get("source_url") or ""
        if "notes" in people_cols:
            payload["notes"] = normalized.get("notes") or ""

        cols = list(payload.keys())
        placeholders = ",".join("?" for _ in cols)
        con.execute(
            f"INSERT INTO people ({','.join(cols)}) VALUES ({placeholders})",
            [payload[c] for c in cols],
        )
        con.commit()
    finally:
        con.close()
