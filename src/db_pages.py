# src/db_pages.py

from __future__ import annotations

import os
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any


def _env_tenant_id() -> str:
    return os.environ.get("TENANT_ID") or os.environ.get("TENANT") or "dev"


def _sources_columns(conn: Any) -> set[str]:
    """
    Return the set of column names on the `sources` table.

    Uses PRAGMA table_info(...) which is natively supported by SQLite and is
    emulated by src/db.py's Postgres compatibility layer.
    """
    cur = conn.execute("PRAGMA table_info(sources)")
    return {row[1] for row in cur.fetchall()}


def _page_url(page: Any) -> str | None:
    for attr in ("url", "source_url", "page_url"):
        val = getattr(page, attr, None)
        if val:
            return str(val)
    return None


def _page_html_text(page: Any) -> str | None:
    """
    Best-effort extraction of an HTML/text payload from a Page-like object.

    We try common attribute names in order: html, body, content, text.
    For bytes payloads we decode as UTF-8 with errors ignored.
    """
    for attr in ("html", "body", "content", "text"):
        val = getattr(page, attr, None)
        if val is None:
            continue
        if isinstance(val, (bytes, bytearray)):
            try:
                return bytes(val).decode("utf-8", "ignore")
            except Exception:
                return None
        try:
            return str(val)
        except Exception:
            return None
    return None


def _page_status_code(page: Any) -> int | None:
    raw = (
        getattr(page, "status", None)
        or getattr(page, "status_code", None)
        or getattr(page, "code", None)
    )
    if raw is None:
        return None
    try:
        return int(raw)
    except Exception:
        return None


def _page_content_type(page: Any) -> str | None:
    val = getattr(page, "content_type", None) or getattr(page, "mime_type", None)
    if val:
        return str(val)

    headers = getattr(page, "headers", None)
    if isinstance(headers, dict):
        ct = headers.get("Content-Type") or headers.get("content-type")
        if ct:
            return str(ct)

    return None


def _status_column(cols: set[str]) -> str | None:
    if "status_code" in cols:
        return "status_code"
    if "http_status" in cols:
        return "http_status"
    return None


def _effective_tenant(has_tenant_id: bool, tenant_id: str | None) -> str | None:
    if not has_tenant_id:
        return None
    return tenant_id or _env_tenant_id()


def _select_existing_source_id(
    conn: Any,
    *,
    has_id: bool,
    has_tenant_id: bool,
    url: str,
    effective_tenant: str | None,
) -> Any:
    if not has_id:
        return None

    where = "source_url = ?"
    params: list[Any] = [url]
    if has_tenant_id and effective_tenant is not None:
        where += " AND tenant_id = ?"
        params.append(effective_tenant)

    cur = conn.execute(
        f"SELECT id FROM sources WHERE {where} ORDER BY id DESC LIMIT 1",
        params,
    )
    row = cur.fetchone()
    if not row:
        return None
    return row[0]


def _build_insert_payload(
    *,
    cols: set[str],
    url: str,
    html_text: str,
    effective_tenant: str | None,
    company_id: int | None,
    status_col: str | None,
    status_code: int | None,
    content_type: str | None,
    now: str,
) -> tuple[list[str], list[Any]]:
    insert_cols: list[str] = ["source_url", "html"]
    insert_vals: list[Any] = [url, html_text]

    if "tenant_id" in cols and effective_tenant is not None:
        insert_cols.append("tenant_id")
        insert_vals.append(effective_tenant)

    if "company_id" in cols and company_id is not None:
        insert_cols.append("company_id")
        insert_vals.append(company_id)

    if status_col and status_code is not None and status_col in cols:
        insert_cols.append(status_col)
        insert_vals.append(status_code)

    if "content_type" in cols and content_type is not None:
        insert_cols.append("content_type")
        insert_vals.append(content_type)

    if "fetched_at" in cols:
        insert_cols.append("fetched_at")
        insert_vals.append(now)

    return insert_cols, insert_vals


def _update_existing_source(
    conn: Any,
    *,
    cols: set[str],
    existing_id: Any,
    html_text: str,
    company_id: int | None,
    status_col: str | None,
    status_code: int | None,
    content_type: str | None,
    now: str,
) -> None:
    set_parts: list[str] = ["html = ?"]
    vals: list[Any] = [html_text]

    if "company_id" in cols and company_id is not None:
        set_parts.append("company_id = ?")
        vals.append(company_id)

    if status_col and status_code is not None and status_col in cols:
        set_parts.append(f"{status_col} = ?")
        vals.append(status_code)

    if "content_type" in cols and content_type is not None:
        set_parts.append("content_type = ?")
        vals.append(content_type)

    if "fetched_at" in cols:
        set_parts.append("fetched_at = ?")
        vals.append(now)

    vals.append(existing_id)
    conn.execute(
        f"UPDATE sources SET {', '.join(set_parts)} WHERE id = ?",
        vals,
    )


def _insert_new_source(conn: Any, *, insert_cols: list[str], insert_vals: list[Any]) -> None:
    placeholders = ", ".join("?" for _ in insert_cols)
    cols_sql = ", ".join(insert_cols)
    conn.execute(
        f"INSERT INTO sources ({cols_sql}) VALUES ({placeholders})",
        insert_vals,
    )


def save_pages(
    conn: Any,
    pages: Iterable[Any],
    company_id: int | None = None,
    tenant_id: str | None = None,
) -> int:
    """
    Persist crawled pages into the `sources` table.

    Goals:
      * Work on both SQLite and Postgres via src/db.py compatibility wrappers.
      * Stay robust to schema drift (extra/optional columns).
      * Avoid uncontrolled duplication even when the table does not have a
        UNIQUE constraint on source_url by performing a manual upsert.

    Returns a best-effort count of pages processed (inserted or updated).
    """
    cols = _sources_columns(conn)
    if "source_url" not in cols or "html" not in cols:
        return 0

    has_id = "id" in cols
    has_tenant_id = "tenant_id" in cols
    status_col = _status_column(cols)
    effective_tenant = _effective_tenant(has_tenant_id, tenant_id)

    now = datetime.now(UTC).replace(microsecond=0).isoformat()
    written = 0

    for page in pages:
        url = _page_url(page)
        if not url:
            continue

        html_text = _page_html_text(page)
        if html_text is None:
            continue

        status_code = _page_status_code(page) if status_col else None
        content_type = _page_content_type(page)

        existing_id = _select_existing_source_id(
            conn,
            has_id=has_id,
            has_tenant_id=has_tenant_id,
            url=url,
            effective_tenant=effective_tenant,
        )

        if existing_id is not None:
            _update_existing_source(
                conn,
                cols=cols,
                existing_id=existing_id,
                html_text=html_text,
                company_id=company_id,
                status_col=status_col,
                status_code=status_code,
                content_type=content_type,
                now=now,
            )
            written += 1
            continue

        insert_cols, insert_vals = _build_insert_payload(
            cols=cols,
            url=url,
            html_text=html_text,
            effective_tenant=effective_tenant,
            company_id=company_id,
            status_col=status_col,
            status_code=status_code,
            content_type=content_type,
            now=now,
        )
        _insert_new_source(conn, insert_cols=insert_cols, insert_vals=insert_vals)
        written += 1

    try:
        conn.commit()
    except Exception:
        # Some callers may pass in a managed/readonly connection; best-effort.
        pass

    return written
