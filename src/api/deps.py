# src/api/deps.py
from __future__ import annotations

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import APIKeyHeader

from src.config import settings

# Header used for admin/API-key auth on /admin endpoints.
# Example:  x-admin-api-key: supersecret
api_key_header = APIKeyHeader(name="x-admin-api-key", auto_error=False)


def _is_ip_allowed(client_ip: str | None) -> bool:
    """
    O23: Optional IP allow-list for admin endpoints.

    If ADMIN_ALLOWED_IPS is empty, all IPs are allowed.
    If it is non-empty, only those IPs may access /admin routes.
    """
    allowed = settings.ADMIN_ALLOWED_IPS
    if not allowed:
        # No allow-list configured → allow any IP.
        return True
    if not client_ip:
        return False
    return client_ip in allowed


def require_admin(
    request: Request,
    api_key: str | None = Depends(api_key_header),
) -> None:
    """
    O23: Dependency enforcing API key + optional IP allow-list for admin routes.

    Behaviour:
      - If ADMIN_API_KEY is unset/empty, auth is effectively disabled
        (useful for local dev).
      - If ADMIN_API_KEY is set, the caller must supply a matching
        x-admin-api-key header or receive 401.
      - If ADMIN_ALLOWED_IPS is non-empty, the caller's client IP must be
        present in that list or receive 403.

    Attach this as a router-level dependency:

        router = APIRouter(
            prefix="/admin",
            tags=["admin"],
            dependencies=[Depends(require_admin)],
        )
    """
    configured_key = settings.ADMIN_API_KEY

    # API key check (only enforced when configured).
    if configured_key:
        if not api_key or api_key != configured_key:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing admin API key",
            )

    # IP allow-list (only enforced when configured).
    client_ip = request.client.host if request.client else None
    if not _is_ip_allowed(client_ip):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access is not allowed from this IP address",
        )
