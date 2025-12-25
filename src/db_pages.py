# src/db_pages.py

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

from src.crawl.runner import Page  # keep / adjust this import if you already have it


def _sources_columns(conn: sqlite3.Connection) -> set[str]:
    """
    Return the set of column names on the `sources` table.

    We use this to stay robust against schema drift (extra columns, renamed
    columns, optional company_id, etc.).
    """
    cur = conn.execute("PRAGMA table_info(sources)")
    return {row[1] for row in cur.fetchall()}


def _page_html(page: Page) -> bytes | None:
    """
    Best-effort extraction of the HTML payload from a Page-like object.

    We try common attribute names in order: html, body, content, text.
    """
    for attr in ("html", "body", "content", "text"):
        val = getattr(page, attr, None)
        if val is None:
            continue
        if isinstance(val, (bytes, bytearray)):
            return bytes(val)
        # Treat everything else as text and encode to UTF-8
        try:
            return str(val).encode("utf-8")
        except Exception:
            return str(val).encode("utf-8", "ignore")
    return None


def save_pages(
    conn: sqlite3.Connection,
    pages: Iterable[Page],
    company_id: int | None = None,
) -> int:
    """
    Persist crawled pages into the `sources` table.

    Behaviour:

      * Inserts one row per Page.
      * Always writes `source_url` and `html` when those columns exist.
      * If `company_id` column exists and a value is provided, it is stored.
      * Uses an UPSERT on `source_url` when possible; otherwise falls back to
        INSERT OR IGNORE.

    Returns the number of rows written/updated (best-effort).
    """
    cols = _sources_columns(conn)

    # If the table is missing the expected core columns, there is nothing
    # sensible we can do.
    if "source_url" not in cols or "html" not in cols:
        return 0

    has_company_id = "company_id" in cols
    has_status_code = "status_code" in cols or "http_status" in cols
    status_col = (
        "status_code"
        if "status_code" in cols
        else ("http_status" if "http_status" in cols else None)
    )
    has_content_type = "content_type" in cols
    has_fetched_at = "fetched_at" in cols

    now = datetime.now(UTC).replace(microsecond=0).isoformat()
    written = 0

    for page in pages:
        # URL: prefer `url`, fall back to `source_url`.
        url = getattr(page, "url", None) or getattr(page, "source_url", None)
        if not url:
            continue

        html_blob = _page_html(page)
        if html_blob is None:
            # No HTML payload → nothing useful to store for this page.
            continue

        params: dict[str, Any] = {
            "source_url": url,
            "html": html_blob,
        }
        col_list: list[str] = ["source_url", "html"]

        # Optional company_id
        if has_company_id and company_id is not None:
            params["company_id"] = company_id
            col_list.append("company_id")

        # Optional status code
        if status_col and has_status_code:
            status_val = (
                getattr(page, "status", None)
                or getattr(page, "status_code", None)
                or getattr(page, "code", None)
            )
            if status_val is not None:
                params[status_col] = int(status_val)
                col_list.append(status_col)

        # Optional content_type
        if has_content_type:
            ctype = (
                getattr(page, "content_type", None)
                or getattr(page, "mime_type", None)
                or (
                    getattr(page, "headers", {}).get("Content-Type")
                    if hasattr(page, "headers")
                    else None
                )
            )
            if ctype is not None:
                params["content_type"] = str(ctype)
                col_list.append("content_type")

        # Optional fetched_at
        if has_fetched_at:
            params["fetched_at"] = now
            col_list.append("fetched_at")

        placeholders = ", ".join(f":{c}" for c in col_list)
        cols_sql = ", ".join(col_list)

        # Try a proper ON CONFLICT(source_url) UPSERT first; if the schema does
        # not define a UNIQUE/PK on source_url, fall back to INSERT OR IGNORE.
        upsert_sql = f"""
            INSERT INTO sources ({cols_sql})
            VALUES ({placeholders})
            ON CONFLICT(source_url) DO UPDATE SET
              html = excluded.html
              {", company_id = excluded.company_id" if has_company_id else ""}
        """

        try:
            conn.execute(upsert_sql, params)
        except sqlite3.OperationalError:
            # Older schemas / different constraints: degrade gracefully.
            fallback_sql = f"INSERT OR IGNORE INTO sources ({cols_sql}) VALUES ({placeholders})"
            conn.execute(fallback_sql, params)

        written += 1

    conn.commit()
    return written
