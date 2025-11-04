# src/db.py
import os
import sqlite3
from datetime import UTC, datetime

# -------------------- basics --------------------


def _db_path():
    url = os.environ["DATABASE_URL"]
    if not url.startswith("sqlite:///"):
        raise RuntimeError(f"Only sqlite supported in dev; got {url}")
    return url.removeprefix("sqlite:///")


def _table_columns(cur, table: str) -> dict[str, dict]:
    """
    Returns {col_name: {name,type,notnull,default,pk}} for table.
    """
    meta = {}
    return meta


def _fk_map(cur, table: str) -> dict[str, tuple[str, str]]:
    """
    Maps local_col -> (ref_table, ref_col) for FKs in table.
    """
    m = {}
    for _id, _seq, ref_table, from_col, to_col, *_ in cur.execute(
        f"PRAGMA foreign_key_list({table})"
    ):
        m[from_col] = (ref_table, to_col)
    return m


def _ts_iso8601_z(value: datetime | str | None) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        else:
            value = value.astimezone(UTC)
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _derive_name_from_email(email: str) -> tuple[str, str, str]:
    """
    'john.smith@example.com' -> ('John Smith', 'John', 'Smith')
    """
    local = email.split("@", 1)[0]
    parts = [p for p in local.replace(".", " ").replace("_", " ").replace("-", " ").split() if p]
    if not parts:
        return ("Unknown", "Unknown", "")
    first = parts[0].capitalize()
    last = " ".join(p.capitalize() for p in parts[1:]) if len(parts) > 1 else ""
    full = f"{first} {last}".strip()
    return (full, first, last)


# ---------------- ensure company/person ----------------


def _ensure_company(cur, domain: str) -> int:
    row = cur.execute("SELECT id FROM companies WHERE domain = ?", (domain,)).fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO companies(name, domain) VALUES(?, ?)", (domain, domain))
    return cur.lastrowid


def _ensure_person(cur, *, email: str, company_id: int | None) -> int | None:
    """
    Create or find a person that emails.person_id points to.
    - Detects referenced table/PK via PRAGMA.
    - If person table has 'email', upsert/find by that.
    - Inserts a minimal row satisfying NOT NULLs if needed.
    Returns the person's PK or None if emails.person_id isn't present/linked.
    """
    emails_cols = _table_columns(cur, "emails")
    if "person_id" not in emails_cols:
        return None

    fks = _fk_map(cur, "emails")
    if "person_id" not in fks:
        return None

    person_table, person_pk_col = fks["person_id"]
    pcols = _table_columns(cur, person_table)

    # Try to find by email if the person table has that column
    email_col = "email" if "email" in pcols else None
    if email_col:
        row = cur.execute(
            f"SELECT {person_pk_col} FROM {person_table} WHERE {email_col} = ?",
            (email,),
        ).fetchone()
        if row:
            return row[0]

    # Insert minimal person row
    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    full, first, last = _derive_name_from_email(email)

    insert_cols: list[str] = []
    insert_vals: list[object] = []

    def add(col: str, val):
        insert_cols.append(col)
        insert_vals.append(val)

    # Prefer semantic fields if present
    if email_col:
        add(email_col, email)
    if "company_id" in pcols:
        add("company_id", company_id)
    if "name" in pcols:
        add("name", full)
    if "full_name" in pcols and "name" not in pcols:
        add("full_name", full)
    if "first_name" in pcols:
        add("first_name", first)
    if "last_name" in pcols:
        add("last_name", last)
    if "created_at" in pcols:
        add("created_at", now)
    if "updated_at" in pcols:
        add("updated_at", now)

    # Satisfy NOT NULL columns without defaults
    already = set(insert_cols)
    for c in pcols.values():
        if c["pk"] or not c["notnull"] or c["default"] is not None or c["name"] in already:
            continue
        ctype = c["type"]
        if "INT" in ctype:
            add(c["name"], 0)
        elif any(k in ctype for k in ("REAL", "FLOA", "DOUB")):
            add(c["name"], 0.0)
        else:
            add(c["name"], "")

    cols_sql = ", ".join(insert_cols)
    ph = ", ".join("?" for _ in insert_vals)
    cur.execute(f"INSERT INTO {person_table} ({cols_sql}) VALUES ({ph})", insert_vals)
    return cur.lastrowid


# ---------------- main upsert ----------------


def upsert_verification_result(
    *,
    email: str,
    verify_status: str,
    reason: str | None = None,
    mx_host: str | None = None,
    verified_at: datetime | str | None = None,
    company_id: int | None = None,
    person_id: int | None = None,
    source_url: str | None = None,
    icp_score: int | None = None,
    **_ignored,
) -> None:
    """
    Idempotent write keyed by emails.email.
    Ensures company & person, then UPSERTs email.
    Always updates: verify_status, reason, mx_host, verified_at.
    Optionally updates: person_id, source_url, icp_score if those columns exist.
    """
    db = _db_path()
    domain = email.split("@")[-1].lower().strip()
    ts = _ts_iso8601_z(verified_at)

    with sqlite3.connect(db) as con:
        con.execute("PRAGMA foreign_keys=ON")
        cur = con.cursor()

        # Ensure company
        comp_id = company_id or _ensure_company(cur, domain)

        # Ensure/validate person if the FK exists
        emails_cols = _table_columns(cur, "emails")
        if "person_id" in emails_cols:
            if person_id is None:
                person_id = _ensure_person(cur, email=email, company_id=comp_id)
            else:
                # validate caller-supplied person_id
                fks = _fk_map(cur, "emails")
                if "person_id" in fks:
                    ptable, ppk = fks["person_id"]
                    exists = cur.execute(
                        f"SELECT 1 FROM {ptable} WHERE {ppk} = ? LIMIT 1",
                        (person_id,),
                    ).fetchone()
                    if not exists:
                        person_id = _ensure_person(cur, email=email, company_id=comp_id)

        # Idempotency guard
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_emails_email ON emails(email)")

        # Build INSERT dynamically to match available columns
        cols = _table_columns(cur, "emails")
        insert_cols = ["email", "company_id", "verify_status", "reason", "mx_host", "verified_at"]
        insert_vals = [email, comp_id, verify_status, reason, mx_host, ts]

        if "person_id" in cols:
            insert_cols.append("person_id")
            insert_vals.append(person_id)
        if "source_url" in cols:
            insert_cols.append("source_url")
            insert_vals.append(source_url)
        if "icp_score" in cols:
            insert_cols.append("icp_score")
            insert_vals.append(icp_score)

        placeholders = ", ".join("?" for _ in insert_cols)
        insert_cols_sql = ", ".join(insert_cols)

        update_parts = [
            "company_id    = COALESCE(excluded.company_id, emails.company_id)",
            "verify_status = excluded.verify_status",
            "reason        = excluded.reason",
            "mx_host       = excluded.mx_host",
            "verified_at   = excluded.verified_at",
        ]
        if "person_id" in cols:
            update_parts.append("person_id = COALESCE(excluded.person_id, emails.person_id)")
        if "source_url" in cols:
            update_parts.append("source_url = COALESCE(excluded.source_url, emails.source_url)")
        if "icp_score" in cols:
            update_parts.append("icp_score = COALESCE(excluded.icp_score, emails.icp_score)")

        update_sql = ", ".join(update_parts)

        sql = f"""
            INSERT INTO emails ({insert_cols_sql})
            VALUES ({placeholders})
            ON CONFLICT(email) DO UPDATE SET
              {update_sql}
        """
        cur.execute(sql, insert_vals)
        con.commit()


def bulk_insert_ingest_items(rows: list[dict]) -> int:
    cols = [
        "company",
        "domain",
        "role",
        "first_name",
        "last_name",
        "full_name",
        "title",
        "source_url",
        "notes",
        "norm_domain",
        "norm_company",
        "norm_role",
        "errors",
    ]
    placeholders = ",".join(["?"] * len(cols))
    values = [[r.get(c) for c in cols] for r in rows]
    with sqlite3.connect(_db_path()) as conn:
        cur = conn.cursor()
        cur.executemany(
            f"INSERT INTO ingest_items ({','.join(cols)}) VALUES ({placeholders})",
            values,
        )
        conn.commit()
        return cur.rowcount
