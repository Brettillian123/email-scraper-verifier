# src/db_pages.py
from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # For type hints only; avoids runtime dependency/cycles.
    from src.crawl.runner import Page


__all__ = ["save_pages"]


def save_pages(conn: sqlite3.Connection, pages: list[Page]) -> None:
    """
    Persist crawled pages into the 'sources' table.

    Expects a table with (at least) the following schema:

        CREATE TABLE IF NOT EXISTS sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_url TEXT UNIQUE NOT NULL,
            html BLOB NOT NULL,
            fetched_at INTEGER NOT NULL
        );

    Note: The migration that creates this table should live under scripts/
    (see R10 instructions). This helper assumes the table already exists.
    """
    if not pages:
        return

    cur = conn.cursor()
    cur.executemany(
        "INSERT OR REPLACE INTO sources (source_url, html, fetched_at) VALUES (?,?,?)",
        [(p.url, p.html, int(p.fetched_at)) for p in pages],
    )
    conn.commit()
