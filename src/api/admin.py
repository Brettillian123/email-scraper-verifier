# src/api/admin.py
from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from src.admin.audit import log_admin_action
from src.admin.metrics import get_admin_summary, get_analytics_summary
from src.api.deps import require_admin

if TYPE_CHECKING:
    from src.auth.core import User

router = APIRouter(
    prefix="/admin",
    tags=["admin"],
    dependencies=[Depends(require_admin)],
)

# Templates for the minimal HTML admin dashboard.
# The directory is relative to the project root when running `uvicorn src.api.app:app`.
templates = Jinja2Templates(directory="src/api/templates")


# ---------------------------------------------------------------------------
# User Management
# ---------------------------------------------------------------------------


def _get_current_user_from_session(request: Request) -> User | None:
    """Get the current user from session for superuser check."""
    try:
        from src.auth.core import SESSION_COOKIE_NAME, get_session

        session_id = request.cookies.get(SESSION_COOKIE_NAME)
        if not session_id:
            return None

        session, user = get_session(session_id)
        return user
    except Exception:
        return None


def _require_superuser(request: Request) -> User:
    """Verify the current user is a superuser."""
    user = _get_current_user_from_session(request)
    if not user or not user.is_superuser:
        raise HTTPException(status_code=403, detail="Superuser access required")
    return user


class UserActionRequest(BaseModel):
    email: str


@router.get("/users", response_class=HTMLResponse)
def admin_users_page(request: Request) -> HTMLResponse:
    """User management page (superuser only)."""
    user = _require_superuser(request)

    remote_ip = request.client.host if request.client else None
    log_admin_action(
        action="view_users_page",
        user_id=user.id,
        remote_ip=remote_ip,
        metadata={"path": "/admin/users"},
    )

    return templates.TemplateResponse("admin_users.html", {"request": request})


@router.get("/users/list")
def admin_users_list(request: Request, pending_only: bool = False) -> dict:
    """API endpoint to list users."""
    _require_superuser(request)

    from src.db import get_conn

    conn = get_conn()
    try:
        if pending_only:
            cur = conn.execute(
                """
                SELECT id, email, tenant_id, display_name, is_active, is_superuser,
                       is_approved, is_verified, created_at, last_login_at
                FROM users
                WHERE is_approved = FALSE
                ORDER BY created_at DESC
                """
            )
        else:
            cur = conn.execute(
                """
                SELECT id, email, tenant_id, display_name, is_active, is_superuser,
                       is_approved, is_verified, created_at, last_login_at
                FROM users
                ORDER BY created_at DESC
                """
            )
        rows = cur.fetchall()

        users = []
        for row in rows:
            users.append(
                {
                    "id": row["id"],
                    "email": row["email"],
                    "tenant_id": row["tenant_id"],
                    "display_name": row.get("display_name"),
                    "is_active": bool(row["is_active"]),
                    "is_superuser": bool(row["is_superuser"]),
                    "is_approved": bool(row.get("is_approved")),
                    "is_verified": bool(row.get("is_verified")),
                    "created_at": row["created_at"],
                    "last_login_at": row.get("last_login_at"),
                }
            )

        return {"users": users, "count": len(users)}
    finally:
        conn.close()


@router.post("/users/approve")
def admin_approve_user(request: Request, body: UserActionRequest) -> dict:
    """Approve a pending user."""
    admin_user = _require_superuser(request)

    from src.auth.core import get_user_by_email
    from src.db import get_conn

    user = get_user_by_email(body.email)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if user.is_approved:
        return {"status": "already_approved", "email": user.email}

    conn = get_conn()
    try:
        conn.execute(
            "UPDATE users SET is_approved = TRUE WHERE id = %s",
            (user.id,),
        )
        conn.commit()

        remote_ip = request.client.host if request.client else None
        log_admin_action(
            action="approve_user",
            user_id=admin_user.id,
            remote_ip=remote_ip,
            metadata={"approved_email": user.email, "approved_user_id": user.id},
        )

        return {"status": "approved", "email": user.email}
    finally:
        conn.close()


@router.post("/users/reject")
def admin_reject_user(request: Request, body: UserActionRequest) -> dict:
    """Reject and delete a pending user."""
    admin_user = _require_superuser(request)

    from src.auth.core import delete_user_sessions, get_user_by_email
    from src.db import get_conn

    user = get_user_by_email(body.email)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if user.is_approved:
        raise HTTPException(status_code=400, detail="Cannot reject an already approved user")

    if user.is_superuser:
        raise HTTPException(status_code=400, detail="Cannot reject a superuser")

    conn = get_conn()
    try:
        # Delete sessions
        delete_user_sessions(user.id)

        # Delete user limits and user
        conn.execute("DELETE FROM user_limits WHERE user_id = %s", (user.id,))
        conn.execute("DELETE FROM users WHERE id = %s", (user.id,))
        conn.commit()

        remote_ip = request.client.host if request.client else None
        log_admin_action(
            action="reject_user",
            user_id=admin_user.id,
            remote_ip=remote_ip,
            metadata={"rejected_email": user.email, "rejected_user_id": user.id},
        )

        return {"status": "rejected", "email": user.email}
    finally:
        conn.close()


@router.post("/users/toggle-active")
def admin_toggle_user_active(request: Request, body: UserActionRequest) -> dict:
    """Enable or disable a user."""
    admin_user = _require_superuser(request)

    from src.auth.core import delete_user_sessions, get_user_by_email
    from src.db import get_conn

    user = get_user_by_email(body.email)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if user.id == admin_user.id:
        raise HTTPException(status_code=400, detail="Cannot disable yourself")

    new_status = not user.is_active

    conn = get_conn()
    try:
        conn.execute(
            "UPDATE users SET is_active = %s WHERE id = %s",
            (new_status, user.id),
        )
        conn.commit()

        # If disabling, clear their sessions
        if not new_status:
            delete_user_sessions(user.id)

        remote_ip = request.client.host if request.client else None
        log_admin_action(
            action="toggle_user_active",
            user_id=admin_user.id,
            remote_ip=remote_ip,
            metadata={"target_email": user.email, "new_status": new_status},
        )

        return {"status": "updated", "email": user.email, "is_active": new_status}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Metrics & Analytics Routes
# ---------------------------------------------------------------------------


@router.get("/metrics")
def admin_metrics(request: Request) -> dict[str, object]:
    """
    JSON endpoint exposing high-level admin metrics.

    This is the primary R24 surface; it is also reused by the CLI (O20) and
    richer analytics endpoints (O17) as a quick summary.
    """
    summary = get_admin_summary()

    # Best-effort audit log; failures are swallowed inside log_admin_action.
    remote_ip = request.client.host if request.client else None
    log_admin_action(
        action="view_metrics",
        user_id=None,  # could be derived from API key in a future enhancement
        remote_ip=remote_ip,
        metadata={"path": "/admin/metrics"},
    )

    return summary


@router.get("/analytics")
def admin_analytics(
    request: Request,
    window_days: int = 30,
    top_domains: int = 20,
    top_errors: int = 20,
) -> dict[str, object]:
    """
    O17: JSON endpoint exposing deeper admin analytics.

    Query parameters:
        window_days:   number of days of verification history to include
                       (rolling window, default 30).
        top_domains:   number of domains to return in the breakdown (default 20).
        top_errors:    number of error keys to return in the breakdown (default 20).

    Response shape:
        {
          "verification_time_series": [...],
          "domain_breakdown": [...],
          "error_breakdown": { ... }
        }
    """
    analytics = get_analytics_summary(
        window_days=window_days,
        top_domains=top_domains,
        top_errors=top_errors,
    )

    remote_ip = request.client.host if request.client else None
    log_admin_action(
        action="view_analytics",
        user_id=None,
        remote_ip=remote_ip,
        metadata={
            "path": "/admin/analytics",
            "window_days": window_days,
            "top_domains": top_domains,
            "top_errors": top_errors,
        },
    )

    return analytics


# ---------------------------------------------------------------------------
# Dashboard Routes (Superuser Only)
# ---------------------------------------------------------------------------


@router.get("/", response_class=HTMLResponse)
def admin_page(request: Request) -> HTMLResponse:
    """
    Minimal HTML admin dashboard (superuser only).

    The page itself is a thin shell; it fetches /admin/metrics via JS and
    renders the queues, workers, verification stats, and cost counters.

    With O17 enabled, the frontend can also call /admin/analytics to render
    time-series charts and domain/error breakdowns.
    """
    user = _require_superuser(request)

    remote_ip = request.client.host if request.client else None
    log_admin_action(
        action="view_admin_html",
        user_id=user.id,
        remote_ip=remote_ip,
        metadata={"path": "/admin/"},
    )

    return templates.TemplateResponse(
        "admin.html",
        {
            "request": request,
            "user": user,
        },
    )


@router.get("/dashboard", response_class=HTMLResponse)
def admin_dashboard_alias(request: Request) -> HTMLResponse:
    """Alias for /admin/ - serves the same dashboard (superuser only)."""
    return admin_page(request)
