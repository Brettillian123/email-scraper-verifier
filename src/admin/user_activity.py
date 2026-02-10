# src/admin/user_activity.py
"""
User activity tracking for usage monitoring.

This module provides functions to:
  - Log user actions (run creation, exports, searches, etc.)
  - Query user activity for billing/analytics
  - Aggregate usage statistics per user

Used by:
  - API endpoints (via middleware or explicit calls)
  - Admin dashboard user overview
  - Usage/billing reports
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from src.db import get_conn

log = logging.getLogger(__name__)


# Common action types
ACTION_RUN_CREATED = "run_created"
ACTION_RUN_STARTED = "run_started"
ACTION_RUN_COMPLETED = "run_completed"
ACTION_RUN_FAILED = "run_failed"
ACTION_RUN_CANCELLED = "run_cancelled"
ACTION_EXPORT = "export"
ACTION_SEARCH = "search"
ACTION_VERIFY = "verify"
ACTION_LOGIN = "login"
ACTION_API_CALL = "api_call"


@dataclass
class UserActivityEntry:
    """A single user activity record."""

    id: int | None = None
    tenant_id: str = ""
    user_id: str = ""
    action: str = ""
    resource_type: str | None = None
    resource_id: str | None = None
    ip_address: str | None = None
    user_agent: str | None = None
    metadata: dict[str, Any] | None = None
    created_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "tenant_id": self.tenant_id,
            "user_id": self.user_id,
            "action": self.action,
            "resource_type": self.resource_type,
            "resource_id": self.resource_id,
            "ip_address": self.ip_address,
            "user_agent": self.user_agent,
            "metadata": self.metadata,
            "created_at": self.created_at,
        }


@dataclass
class UserUsageSummary:
    """Aggregated usage statistics for a user."""

    tenant_id: str
    user_id: str
    runs_created: int = 0
    runs_completed: int = 0
    runs_failed: int = 0
    exports: int = 0
    searches: int = 0
    verifications: int = 0
    total_actions: int = 0
    first_activity: str | None = None
    last_activity: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "user_id": self.user_id,
            "runs_created": self.runs_created,
            "runs_completed": self.runs_completed,
            "runs_failed": self.runs_failed,
            "exports": self.exports,
            "searches": self.searches,
            "verifications": self.verifications,
            "total_actions": self.total_actions,
            "first_activity": self.first_activity,
            "last_activity": self.last_activity,
        }


def _utc_now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _table_exists(conn, table: str) -> bool:
    """Check if a table exists."""
    try:
        conn.execute(f"SELECT 1 FROM {table} LIMIT 1")
        return True
    except Exception:
        return False


def log_user_activity(
    *,
    tenant_id: str,
    user_id: str,
    action: str,
    resource_type: str | None = None,
    resource_id: str | None = None,
    ip_address: str | None = None,
    user_agent: str | None = None,
    metadata: dict[str, Any] | None = None,
    conn: Any = None,
) -> bool:
    """
    Log a user activity event.

    Args:
        tenant_id: The tenant ID
        user_id: The user ID
        action: Action type (use ACTION_* constants)
        resource_type: Optional type of resource (run, company, email, etc.)
        resource_id: Optional ID of the resource
        ip_address: Optional client IP address
        user_agent: Optional client user agent
        metadata: Optional additional context as dict
        conn: Optional database connection

    Returns:
        True if logged successfully, False otherwise
    """
    if not tenant_id or not user_id or not action:
        log.warning("log_user_activity called with missing required fields")
        return False

    close_conn = conn is None
    if conn is None:
        conn = get_conn()

    try:
        if not _table_exists(conn, "user_activity"):
            log.debug("user_activity table does not exist; skipping log")
            return False

        metadata_json = json.dumps(metadata) if metadata else None
        now = _utc_now_iso()

        conn.execute(
            """
            INSERT INTO user_activity (
                tenant_id, user_id, action,
                resource_type, resource_id,
                ip_address, user_agent,
                metadata, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tenant_id,
                user_id,
                action,
                resource_type,
                resource_id,
                ip_address,
                user_agent,
                metadata_json,
                now,
            ),
        )
        conn.commit()
        return True

    except Exception as exc:
        log.debug(
            "Failed to log user activity",
            extra={"user_id": user_id, "action": action, "error": str(exc)},
        )
        return False
    finally:
        if close_conn:
            try:
                conn.close()
            except Exception:
                pass


def get_user_activity(
    tenant_id: str,
    user_id: str,
    *,
    limit: int = 100,
    offset: int = 0,
    action_filter: str | None = None,
    since: str | None = None,
    conn: Any = None,
) -> list[UserActivityEntry]:
    """
    Query user activity history.

    Args:
        tenant_id: The tenant ID
        user_id: The user ID
        limit: Maximum results (default 100)
        offset: Pagination offset
        action_filter: Optional filter by action type
        since: Optional ISO timestamp to filter from
        conn: Optional database connection

    Returns:
        List of UserActivityEntry objects
    """
    close_conn = conn is None
    if conn is None:
        conn = get_conn()

    results: list[UserActivityEntry] = []

    try:
        if not _table_exists(conn, "user_activity"):
            return results

        # Build query
        where_parts = ["tenant_id = ?", "user_id = ?"]
        params: list[Any] = [tenant_id, user_id]

        if action_filter:
            where_parts.append("action = ?")
            params.append(action_filter)

        if since:
            where_parts.append("created_at >= ?")
            params.append(since)

        where_clause = " AND ".join(where_parts)
        params.extend([limit, offset])

        cur = conn.execute(
            f"""
            SELECT id, tenant_id, user_id, action, resource_type, resource_id,
                   ip_address, user_agent, metadata, created_at
            FROM user_activity
            WHERE {where_clause}
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
            """,
            tuple(params),
        )

        for row in cur.fetchall():
            # Handle both tuple and dict-like rows
            if hasattr(row, "_asdict"):
                data = row._asdict()
            elif hasattr(row, "keys"):
                data = dict(row)
            else:
                cols = [d[0] for d in cur.description]
                data = dict(zip(cols, row, strict=False))

            # Parse metadata JSON
            meta = data.get("metadata")
            if meta and isinstance(meta, str):
                try:
                    meta = json.loads(meta)
                except Exception:
                    pass

            results.append(
                UserActivityEntry(
                    id=data.get("id"),
                    tenant_id=data.get("tenant_id", ""),
                    user_id=data.get("user_id", ""),
                    action=data.get("action", ""),
                    resource_type=data.get("resource_type"),
                    resource_id=data.get("resource_id"),
                    ip_address=data.get("ip_address"),
                    user_agent=data.get("user_agent"),
                    metadata=meta if isinstance(meta, dict) else None,
                    created_at=data.get("created_at"),
                )
            )

        return results

    except Exception:
        log.exception("Failed to query user activity", extra={"user_id": user_id})
        return results
    finally:
        if close_conn:
            try:
                conn.close()
            except Exception:
                pass


def get_user_usage_summary(
    tenant_id: str,
    user_id: str,
    *,
    since_days: int | None = None,
    conn: Any = None,
) -> UserUsageSummary:
    """
    Get aggregated usage statistics for a user.

    Args:
        tenant_id: The tenant ID
        user_id: The user ID
        since_days: Optional limit to last N days
        conn: Optional database connection

    Returns:
        UserUsageSummary with aggregated counts
    """
    close_conn = conn is None
    if conn is None:
        conn = get_conn()

    summary = UserUsageSummary(tenant_id=tenant_id, user_id=user_id)

    try:
        if not _table_exists(conn, "user_activity"):
            return summary

        # Build query
        where_parts = ["tenant_id = ?", "user_id = ?"]
        params: list[Any] = [tenant_id, user_id]

        if since_days:
            cutoff = (datetime.now(UTC) - timedelta(days=since_days)).strftime("%Y-%m-%dT%H:%M:%SZ")
            where_parts.append("created_at >= ?")
            params.append(cutoff)

        where_clause = " AND ".join(where_parts)

        cur = conn.execute(
            f"""
            SELECT
                COUNT(*) FILTER (WHERE action = '{ACTION_RUN_CREATED}') AS runs_created,
                COUNT(*) FILTER (WHERE action = '{ACTION_RUN_COMPLETED}') AS runs_completed,
                COUNT(*) FILTER (WHERE action = '{ACTION_RUN_FAILED}') AS runs_failed,
                COUNT(*) FILTER (WHERE action = '{ACTION_EXPORT}') AS exports,
                COUNT(*) FILTER (WHERE action = '{ACTION_SEARCH}') AS searches,
                COUNT(*) FILTER (WHERE action = '{ACTION_VERIFY}') AS verifications,
                COUNT(*) AS total_actions,
                MIN(created_at) AS first_activity,
                MAX(created_at) AS last_activity
            FROM user_activity
            WHERE {where_clause}
            """,
            tuple(params),
        )

        row = cur.fetchone()
        if row:
            if hasattr(row, "_asdict"):
                data = row._asdict()
            elif hasattr(row, "keys"):
                data = dict(row)
            else:
                cols = [d[0] for d in cur.description]
                data = dict(zip(cols, row, strict=False))

            summary.runs_created = data.get("runs_created") or 0
            summary.runs_completed = data.get("runs_completed") or 0
            summary.runs_failed = data.get("runs_failed") or 0
            summary.exports = data.get("exports") or 0
            summary.searches = data.get("searches") or 0
            summary.verifications = data.get("verifications") or 0
            summary.total_actions = data.get("total_actions") or 0
            summary.first_activity = data.get("first_activity")
            summary.last_activity = data.get("last_activity")

        return summary

    except Exception:
        log.exception("Failed to get user usage summary", extra={"user_id": user_id})
        return summary
    finally:
        if close_conn:
            try:
                conn.close()
            except Exception:
                pass


def get_tenant_usage_summary(
    tenant_id: str,
    *,
    since_days: int | None = None,
    conn: Any = None,
) -> list[UserUsageSummary]:
    """
    Get usage summary for all users in a tenant.

    Returns list of UserUsageSummary, one per user.
    """
    close_conn = conn is None
    if conn is None:
        conn = get_conn()

    results: list[UserUsageSummary] = []

    try:
        if not _table_exists(conn, "user_activity"):
            return results

        # Build query
        where_parts = ["tenant_id = ?"]
        params: list[Any] = [tenant_id]

        if since_days:
            cutoff = (datetime.now(UTC) - timedelta(days=since_days)).strftime("%Y-%m-%dT%H:%M:%SZ")
            where_parts.append("created_at >= ?")
            params.append(cutoff)

        where_clause = " AND ".join(where_parts)

        cur = conn.execute(
            f"""
            SELECT
                user_id,
                COUNT(*) FILTER (WHERE action = '{ACTION_RUN_CREATED}') AS runs_created,
                COUNT(*) FILTER (WHERE action = '{ACTION_RUN_COMPLETED}') AS runs_completed,
                COUNT(*) FILTER (WHERE action = '{ACTION_RUN_FAILED}') AS runs_failed,
                COUNT(*) FILTER (WHERE action = '{ACTION_EXPORT}') AS exports,
                COUNT(*) FILTER (WHERE action = '{ACTION_SEARCH}') AS searches,
                COUNT(*) FILTER (WHERE action = '{ACTION_VERIFY}') AS verifications,
                COUNT(*) AS total_actions,
                MIN(created_at) AS first_activity,
                MAX(created_at) AS last_activity
            FROM user_activity
            WHERE {where_clause}
            GROUP BY user_id
            ORDER BY total_actions DESC
            """,
            tuple(params),
        )

        for row in cur.fetchall():
            if hasattr(row, "_asdict"):
                data = row._asdict()
            elif hasattr(row, "keys"):
                data = dict(row)
            else:
                cols = [d[0] for d in cur.description]
                data = dict(zip(cols, row, strict=False))

            results.append(
                UserUsageSummary(
                    tenant_id=tenant_id,
                    user_id=data.get("user_id", ""),
                    runs_created=data.get("runs_created") or 0,
                    runs_completed=data.get("runs_completed") or 0,
                    runs_failed=data.get("runs_failed") or 0,
                    exports=data.get("exports") or 0,
                    searches=data.get("searches") or 0,
                    verifications=data.get("verifications") or 0,
                    total_actions=data.get("total_actions") or 0,
                    first_activity=data.get("first_activity"),
                    last_activity=data.get("last_activity"),
                )
            )

        return results

    except Exception:
        log.exception("Failed to get tenant usage summary", extra={"tenant_id": tenant_id})
        return results
    finally:
        if close_conn:
            try:
                conn.close()
            except Exception:
                pass


__all__ = [
    # Constants
    "ACTION_RUN_CREATED",
    "ACTION_RUN_STARTED",
    "ACTION_RUN_COMPLETED",
    "ACTION_RUN_FAILED",
    "ACTION_RUN_CANCELLED",
    "ACTION_EXPORT",
    "ACTION_SEARCH",
    "ACTION_VERIFY",
    "ACTION_LOGIN",
    "ACTION_API_CALL",
    # Types
    "UserActivityEntry",
    "UserUsageSummary",
    # Functions
    "log_user_activity",
    "get_user_activity",
    "get_user_usage_summary",
    "get_tenant_usage_summary",
]
