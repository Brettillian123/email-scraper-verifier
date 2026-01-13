# src/admin/audit.py
from __future__ import annotations

import datetime as dt
import json
import sqlite3
from typing import Any

from src.db import get_conn


def log_admin_action(
    action: str,
    user_id: str | None,
    remote_ip: str | None,
    metadata: dict[str, Any] | None = None,
    conn: sqlite3.Connection | None = None,
) -> None:
    """
    O23: Best-effort audit log for admin actions.

    Writes a row to the `admin_audit` table with:
      - ts:        UTC timestamp (ISO 8601, seconds precision)
      - action:    short string like "view_metrics", "view_analytics"
      - user_id:   optional identifier derived from API key or header
      - remote_ip: client IP address as seen by FastAPI
      - metadata:  JSON-encoded dict with any extra context

    This function is intentionally tolerant of failures:
      - If the table does not exist, or the INSERT fails, the exception is
        swallowed so admin endpoints do not break.
    """
    if conn is None:
        conn = get_conn()

    payload = metadata or {}
    try:
        conn.execute(
            """
            INSERT INTO admin_audit (ts, action, user_id, remote_ip, metadata)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                dt.datetime.utcnow().isoformat(timespec="seconds"),
                action,
                user_id,
                remote_ip,
                json.dumps(payload, separators=(",", ":")),
            ),
        )
        conn.commit()
    except sqlite3.Error:
        # Best-effort only; do not propagate to callers.
        return


def get_recent_admin_actions(
    limit: int = 100,
    conn: sqlite3.Connection | None = None,
) -> list[dict[str, Any]]:
    """
    Small helper to inspect the most recent admin actions.

    Primarily intended for debugging and potential future UI/CLI endpoints.
    Returns at most `limit` rows ordered by ts DESC.

    If the table does not exist or a query error occurs, returns an empty list.
    """
    if limit < 1:
        limit = 1

    if conn is None:
        conn = get_conn()

    try:
        cur = conn.execute(
            """
            SELECT id, ts, action, user_id, remote_ip, metadata
            FROM admin_audit
            ORDER BY ts DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        )
    except sqlite3.Error:
        return []

    rows = cur.fetchall()
    results: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, sqlite3.Row):
            item = {
                "id": row["id"],
                "ts": row["ts"],
                "action": row["action"],
                "user_id": row["user_id"],
                "remote_ip": row["remote_ip"],
                "metadata": row["metadata"],
            }
        else:
            # Fallback positional mapping
            item = {
                "id": row[0],
                "ts": row[1],
                "action": row[2],
                "user_id": row[3],
                "remote_ip": row[4],
                "metadata": row[5],
            }
        results.append(item)
    return results
