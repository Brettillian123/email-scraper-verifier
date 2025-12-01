# src/api/admin.py
from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from src.admin.audit import log_admin_action
from src.admin.metrics import get_admin_summary, get_analytics_summary
from src.api.deps import require_admin

router = APIRouter(
    prefix="/admin",
    tags=["admin"],
    dependencies=[Depends(require_admin)],
)

# Templates for the minimal HTML admin dashboard.
# The directory is relative to the project root when running `uvicorn src.api.app:app`.
templates = Jinja2Templates(directory="src/api/templates")


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


@router.get("/", response_class=HTMLResponse)
def admin_page(request: Request) -> HTMLResponse:
    """
    Minimal HTML admin dashboard.

    The page itself is a thin shell; it fetches /admin/metrics via JS and
    renders the queues, workers, verification stats, and cost counters.

    With O17 enabled, the frontend can also call /admin/analytics to render
    time-series charts and domain/error breakdowns.
    """
    remote_ip = request.client.host if request.client else None
    log_admin_action(
        action="view_admin_html",
        user_id=None,
        remote_ip=remote_ip,
        metadata={"path": "/admin/"},
    )
    return templates.TemplateResponse("admin.html", {"request": request})
