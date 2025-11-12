# scripts/extract_candidates.py
from __future__ import annotations

r"""
R11 CLI: Extract person/email candidates from HTML saved by R10 and persist
them to the database with provenance.

Usage examples (PowerShell-friendly):
  python .\scripts\extract_candidates.py --domain crestwellpartners.com --db dev.db
  python .\scripts\extract_candidates.py --source-url https://www.example.com/team --db dev.db
  # Optional override if needed:
  python .\scripts\extract_candidates.py --source-url https://sub.example.com/contact --official-domain example.com
"""

import argparse
import datetime as _dt
import sqlite3
import sys
from collections.abc import Iterable, Sequence
from pathlib import Path
from urllib.parse import urlparse

# Ensure "src/" is importable when running as a script
_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT = _THIS_FILE.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.extract import Candidate, extract_candidates  # noqa: E402


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="R11: extract people/emails from saved sources")
    p.add_argument("--db", default="dev.db", help="Path to SQLite database (default: dev.db)")

    target = p.add_mutually_exclusive_group(required=True)
    target.add_argument(
        "--domain", help="Restrict to sources under this domain (e.g., example.com)"
    )
    target.add_argument("--source-url", help="Extract from a single source URL")

    p.add_argument(
        "--official-domain",
        help="Official domain used to filter emails (defaults to --domain or derived from --source-url)",
    )
    return p.parse_args(argv)


# ----------------------------- DB helpers ------------------------------------


def _connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    # Reasonable safety defaults
    con.execute("PRAGMA foreign_keys = ON;")
    con.execute("PRAGMA journal_mode = WAL;")
    con.execute("PRAGMA synchronous = NORMAL;")
    return con


def _table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    cur = con.execute(f"PRAGMA table_info({table})")
    cols: set[str] = set()
    for row in cur.fetchall():
        # Row may be sqlite3.Row (dict-like) or a plain tuple
        name = None
        try:
            name = row["name"]  # type: ignore[index]
        except Exception:
            # PRAGMA table_info columns: (cid, name, type, notnull, dflt_value, pk)
            if isinstance(row, (tuple, list)) and len(row) > 1:
                name = row[1]
        if name:
            cols.add(str(name).lower())
    return cols


def _select_sources_by_domain(con: sqlite3.Connection, domain: str) -> list[sqlite3.Row]:
    """
    Select candidate source rows whose URLs appear to live under the given domain.
    We match common forms:
      http(s)://example.com/...
      http(s)://www.example.com/...
      http(s)://<sub>.example.com/...
    """
    domain = domain.strip().lstrip(".").lower()
    patterns = [
        f"http://{domain}/%",
        f"https://{domain}/%",
        f"http://www.{domain}/%",
        f"https://www.{domain}/%",
        f"http://%.{domain}/%",
        f"https://%.{domain}/%",
    ]
    # Deduplicate patterns (in case domain already includes 'www.')
    patterns = list(dict.fromkeys(patterns))

    # Build OR chain
    ors = " OR ".join(["source_url LIKE ?"] * len(patterns))
    sql = f"SELECT id, source_url, html FROM sources WHERE {ors}"
    cur = con.execute(sql, patterns)
    return cur.fetchall()


def _select_source_by_url(con: sqlite3.Connection, url: str) -> list[sqlite3.Row]:
    cur = con.execute(
        "SELECT id, source_url, html FROM sources WHERE source_url = ? LIMIT 1",
        (url,),
    )
    rows = cur.fetchall()
    return rows


def _normalized_domain_from_url(url: str) -> str:
    try:
        host = (urlparse(url).netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


def _find_or_create_person_id(
    con: sqlite3.Connection,
    first_name: str | None,
    last_name: str | None,
) -> int | None:
    """
    Conservative: only create a person when we have at least a first or last name.
    Try to use (first_name,last_name) columns if present; else fallback to 'full_name' or 'name' if available.
    """
    if not first_name and not last_name:
        return None

    people_cols = _table_columns(con, "people")
    now_iso = _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")

    # Strategy A: (first_name, last_name)
    if {"first_name", "last_name"}.issubset(people_cols):
        cur = con.execute(
            "SELECT id FROM people WHERE first_name IS ? AND last_name IS ?",
            (first_name, last_name),
        )
        row = cur.fetchone()
        if row:
            return int(row["id"])

        cols = ["first_name", "last_name"]
        vals = [first_name, last_name]
        if "created_at" in people_cols:
            cols.append("created_at")
            vals.append(now_iso)
        if "updated_at" in people_cols:
            cols.append("updated_at")
            vals.append(now_iso)
        sql = f"INSERT INTO people ({', '.join(cols)}) VALUES ({', '.join(['?'] * len(vals))})"
        cur = con.execute(sql, vals)
        return int(cur.lastrowid)

    # Strategy B: single full_name/name column
    full_name_col = (
        "full_name" if "full_name" in people_cols else ("name" if "name" in people_cols else None)
    )
    if full_name_col:
        full_name = " ".join([p for p in (first_name or "", last_name or "") if p]).strip()
        if not full_name:
            return None
        cur = con.execute(f"SELECT id FROM people WHERE {full_name_col} = ?", (full_name,))
        row = cur.fetchone()
        if row:
            return int(row["id"])

        cols = [full_name_col]
        vals = [full_name]
        if "created_at" in people_cols:
            cols.append("created_at")
            vals.append(now_iso)
        if "updated_at" in people_cols:
            cols.append("updated_at")
            vals.append(now_iso)
        sql = f"INSERT INTO people ({', '.join(cols)}) VALUES ({', '.join(['?'] * len(vals))})"
        cur = con.execute(sql, vals)
        return int(cur.lastrowid)

    # If schema is unknown, skip creating people; email will still be recorded.
    return None


def _upsert_email(
    con: sqlite3.Connection,
    email: str,
    source_url: str,
    person_id: int | None,
    extracted_at_iso: str,
) -> None:
    """
    Upsert into emails, preserving existing richer data:
      - Do not overwrite person_id if it is already set.
      - Only set source_url/extracted_at if they are NULL.
    Requires a UNIQUE constraint on emails.email.
    """
    email_cols = _table_columns(con, "emails")

    # Must have 'email' column to proceed
    if "email" not in email_cols:
        raise RuntimeError("emails table must have an 'email' column")

    insert_cols = ["email"]
    insert_vals = [email]

    if "source_url" in email_cols:
        insert_cols.append("source_url")
        insert_vals.append(source_url)
    if "person_id" in email_cols:
        insert_cols.append("person_id")
        insert_vals.append(person_id)
    if "extracted_at" in email_cols:
        insert_cols.append("extracted_at")
        insert_vals.append(extracted_at_iso)

    placeholders = ", ".join(["?"] * len(insert_vals))
    insert_list = ", ".join(insert_cols)

    # Build ON CONFLICT update set preserving existing values (COALESCE)
    updates: list[str] = []
    if "person_id" in email_cols:
        updates.append("person_id = COALESCE(emails.person_id, excluded.person_id)")
    if "source_url" in email_cols:
        updates.append("source_url = COALESCE(emails.source_url, excluded.source_url)")
    if "extracted_at" in email_cols:
        updates.append("extracted_at = COALESCE(emails.extracted_at, excluded.extracted_at)")

    if updates:
        sql = (
            f"INSERT INTO emails ({insert_list}) VALUES ({placeholders}) "
            f"ON CONFLICT(email) DO UPDATE SET {', '.join(updates)}"
        )
    else:
        # Fallback if there is no column to update beyond email
        sql = f"INSERT OR IGNORE INTO emails ({insert_list}) VALUES ({placeholders})"

    con.execute(sql, insert_vals)


def _persist_candidates(
    con: sqlite3.Connection,
    cands: Iterable[Candidate],
) -> tuple[int, int]:
    """
    Persist candidates to DB. Returns (n_people_inserted_or_found, n_emails_upserted).
    """
    n_people = 0
    n_emails = 0
    now_iso = _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")

    with con:  # transaction
        for cand in cands:
            person_id: int | None = None
            if cand.first_name or cand.last_name:
                person_id = _find_or_create_person_id(con, cand.first_name, cand.last_name)
                if person_id:
                    n_people += 1  # counts "found or created" logically

            _upsert_email(
                con,
                email=cand.email,
                source_url=cand.source_url,
                person_id=person_id,
                extracted_at_iso=now_iso,
            )
            n_emails += 1

    return n_people, n_emails


# ------------------------------ Main flow ------------------------------------


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    db_path = args.db

    if not Path(db_path).exists():
        print(
            f"[warn] Database not found at {db_path!r}. Proceeding to create/connect anyway.",
            file=sys.stderr,
        )

    con = _connect(db_path)

    if args.domain:
        official_domain = (args.official_domain or args.domain).lower()
        src_rows = _select_sources_by_domain(con, args.domain)
        target_desc = f"domain={args.domain}"
    else:
        # args.source_url path
        official_domain = (
            args.official_domain or _normalized_domain_from_url(args.source_url)
        ).lower()
        src_rows = _select_source_by_url(con, args.source_url)
        target_desc = f"source_url={args.source_url}"

    if not src_rows:
        print(
            f"[info] No matching sources found for {target_desc}. Nothing to do.", file=sys.stderr
        )
        return 0

    total_pages = len(src_rows)
    all_candidates: list[Candidate] = []

    for row in src_rows:
        source_url = row["source_url"]
        html = row["html"] or ""
        cands = extract_candidates(
            html=html, source_url=source_url, official_domain=official_domain
        )
        all_candidates.extend(cands)

    if not all_candidates:
        print(f"[info] No candidates extracted from {total_pages} page(s) under {target_desc}.")
        return 0

    n_people, n_emails = _persist_candidates(con, all_candidates)

    # Small summary
    unique_emails = len({c.email for c in all_candidates})
    print(
        f"[ok] Processed {total_pages} page(s) for {target_desc}. "
        f"Extracted {len(all_candidates)} candidates ({unique_emails} unique emails). "
        f"Upserted {n_emails} email rows; matched/created ~{n_people} people."
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
