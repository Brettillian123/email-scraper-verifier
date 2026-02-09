# src/api/deps.py
from __future__ import annotations

import os
from urllib.parse import quote

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import APIKeyHeader

from src.config import settings

# Header used for admin/API-key auth on /admin endpoints.
# Example:  x-admin-api-key: supersecret
api_key_header = APIKeyHeader(name="x-admin-api-key", auto_error=False)

# Auth mode from environment
AUTH_MODE = os.getenv("AUTH_MODE", "dev").strip().lower()


def _is_ip_allowed(client_ip: str | None) -> bool:
    """
    O23: Optional IP allow-list for admin endpoints.

    If ADMIN_ALLOWED_IPS is empty, all IPs are allowed.
    If it is non-empty, only those IPs may access /admin routes.
    """
    allowed = settings.ADMIN_ALLOWED_IPS
    if not allowed:
        # No allow-list configured - allow any IP.
        return True
    if not client_ip:
        return False
    return client_ip in allowed


def _check_session_auth(request: Request) -> bool:
    """
    Check if request has valid session authentication.
    
    Returns True if authenticated, False otherwise.
    """
    from src.auth.core import SESSION_COOKIE_NAME, get_session
    
    session_id = request.cookies.get(SESSION_COOKIE_NAME)
    if not session_id:
        return False
    
    session, user = get_session(session_id)
    return session is not None and user is not None


def require_admin(
    request: Request,
    api_key: str | None = Depends(api_key_header),
) -> None:
    """
    Dependency enforcing authentication for admin routes.

    Behaviour based on AUTH_MODE:
      - AUTH_MODE=session: requires valid session cookie, redirects to login if missing
      - AUTH_MODE=none/dev: allows access (for local development)
      - Otherwise: requires ADMIN_API_KEY header if configured
      
    Additionally:
      - If ADMIN_ALLOWED_IPS is non-empty, the caller's client IP must be
        present in that list or receive 403.

    Attach this as a router-level dependency:

        router = APIRouter(
            prefix="/admin",
            tags=["admin"],
            dependencies=[Depends(require_admin)],
        )
    """
    # IP allow-list check (always enforced when configured)
    client_ip = request.client.host if request.client else None
    if not _is_ip_allowed(client_ip):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access is not allowed from this IP address",
        )
    
    # Session-based auth mode
    if AUTH_MODE == "session":
        if not _check_session_auth(request):
            # Redirect to login with return URL
            next_url = quote(str(request.url.path), safe="")
            raise HTTPException(
                status_code=status.HTTP_307_TEMPORARY_REDIRECT,
                headers={"Location": f"/auth/login?next={next_url}"},
            )
        return
    
    # Dev/none modes - no auth required
    if AUTH_MODE in ("none", "dev"):
        return
    
    # API key auth mode (legacy)
    configured_key = settings.ADMIN_API_KEY
    if configured_key:
        if not api_key or api_key != configured_key:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing admin API key",
            )
