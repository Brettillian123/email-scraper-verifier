# src/api/deps.py
"""Shared API dependencies: admin auth and the single AuthContext provider.

This module is the ONE place that decides who a caller is. Routers must import
`AuthContext` / `get_auth_context` from here directly - never behind a
try/except that can silently degrade to a permissive fallback.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
from typing import Any
from urllib.parse import quote

from fastapi import Depends, Header, HTTPException, Request, status
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field

from src.config import settings

logger = logging.getLogger(__name__)

# Header used for admin/API-key auth on /admin endpoints.
# Example:  x-admin-api-key: supersecret
api_key_header = APIKeyHeader(name="x-admin-api-key", auto_error=False)

# Auth mode from environment. Anything unrecognised - including unset - fails closed.
#   - "session":    session cookie, or X-IQ-Key service key (production)
#   - "hs256":      HS256 bearer token
#   - "dev"/"none": unverified header identity (local development only)
AUTH_MODE = os.getenv("AUTH_MODE", "").strip().lower()

# Identity used by dev/none modes and by service-key callers.
DEV_TENANT_ID = os.getenv("DEV_TENANT_ID", "dev").strip() or "dev"
DEV_USER_ID = os.getenv("DEV_USER_ID", "user_dev").strip() or "user_dev"

# Service-to-service key, compared against the X-IQ-Key header in session mode.
IQ_API_KEY = os.getenv("IQ_API_KEY", "").strip()

# HS256 JWT settings (AUTH_MODE=hs256).
AUTH_HS256_SECRET = os.getenv("AUTH_HS256_SECRET", "").strip()
AUTH_JWT_ISSUER = os.getenv("AUTH_JWT_ISSUER", "").strip() or None
AUTH_JWT_AUDIENCE = os.getenv("AUTH_JWT_AUDIENCE", "").strip() or None

if AUTH_MODE in ("none", "dev"):
    logger.warning(
        "AUTH_MODE=%r: API authentication is DISABLED - any caller can read and "
        "write any tenant's data. Local development only; production must run "
        "with AUTH_MODE=session.",
        AUTH_MODE,
    )


class AuthContext(BaseModel):
    """Resolved caller identity, used for tenant scoping."""

    tenant_id: str
    user_id: str
    email: str | None = None
    roles: list[str] = Field(default_factory=list)


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


def _session_user(request: Request):
    """Return the User behind this request's session cookie, or None."""
    from src.auth.core import SESSION_COOKIE_NAME, get_session

    session_id = request.cookies.get(SESSION_COOKIE_NAME)
    if not session_id:
        return None

    session, user = get_session(session_id)
    if not session or not user:
        return None
    return user


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
        # A session alone is NOT enough. /admin/* is excluded from
        # RequireAuthMiddleware, which is what normally enforces the
        # verified -> approved pipeline, so this dependency has to apply those
        # gates itself. Without them, self-registration (REGISTRATION_ENABLED=1)
        # plus email verification was sufficient to reach the admin routes that
        # do not separately call _require_superuser().
        user = _session_user(request)
        if user is None or not user.is_verified or not user.is_approved:
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


# --------------------------------------------------------------------------------------
# AuthContext resolution
# --------------------------------------------------------------------------------------


def _b64url_decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode((data + padding).encode("utf-8"))


def _jwt_hs256_verify(token: str) -> dict[str, Any]:
    """
    Minimal HS256 JWT verifier using the Python stdlib.

    This is intentionally minimal for an MVP without adding new deps.
    In production, prefer a hosted auth provider + RS256/JWKS validation.
    """
    if not AUTH_HS256_SECRET:
        raise HTTPException(
            status_code=500,
            detail="AUTH_MODE=hs256 requires AUTH_HS256_SECRET to be set",
        )

    parts = token.split(".")
    if len(parts) != 3:
        raise HTTPException(status_code=401, detail="Invalid bearer token format")

    header_b64, payload_b64, sig_b64 = parts
    try:
        header = json.loads(_b64url_decode(header_b64).decode("utf-8"))
        payload = json.loads(_b64url_decode(payload_b64).decode("utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=401, detail="Invalid bearer token encoding") from exc

    if (header.get("alg") or "").upper() != "HS256":
        raise HTTPException(status_code=401, detail="Unsupported JWT alg (expected HS256)")

    signing_input = f"{header_b64}.{payload_b64}".encode()
    expected_sig = hmac.new(
        AUTH_HS256_SECRET.encode("utf-8"),
        signing_input,
        hashlib.sha256,
    ).digest()
    actual_sig = _b64url_decode(sig_b64)

    if not hmac.compare_digest(expected_sig, actual_sig):
        raise HTTPException(status_code=401, detail="Invalid token signature")

    # Optional issuer/audience checks
    if AUTH_JWT_ISSUER and payload.get("iss") != AUTH_JWT_ISSUER:
        raise HTTPException(status_code=401, detail="Invalid token issuer")
    if AUTH_JWT_AUDIENCE:
        aud = payload.get("aud")
        if isinstance(aud, str):
            ok = aud == AUTH_JWT_AUDIENCE
        elif isinstance(aud, list):
            ok = AUTH_JWT_AUDIENCE in aud
        else:
            ok = False
        if not ok:
            raise HTTPException(status_code=401, detail="Invalid token audience")

    return payload


def _hs256_context(authorization: str | None) -> AuthContext:
    """AUTH_MODE=hs256: map verified bearer-token claims onto an AuthContext."""
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    claims = _jwt_hs256_verify(authorization.split(" ", 1)[1].strip())

    # Map common claim names to your tenant model.
    # Adjust these once you choose Clerk/Auth0/Supabase Auth, etc.
    tenant = (
        claims.get("tenant_id") or claims.get("org_id") or claims.get("organization_id") or ""
    ).strip()
    user = (claims.get("sub") or claims.get("user_id") or "").strip()
    email = (claims.get("email") or "").strip() or None

    if not tenant or not user:
        raise HTTPException(status_code=401, detail="Token missing tenant_id and/or sub")

    roles: list[str] = []
    raw_roles = claims.get("roles") or claims.get("role") or []
    if isinstance(raw_roles, str):
        roles = [raw_roles]
    elif isinstance(raw_roles, list):
        roles = [str(r) for r in raw_roles if r]

    return AuthContext(tenant_id=tenant, user_id=user, email=email, roles=roles)


def _is_service_key(presented: str | None) -> bool:
    """Constant-time X-IQ-Key check. Never true when no key is configured."""
    if not IQ_API_KEY or not presented:
        return False
    return hmac.compare_digest(presented.strip(), IQ_API_KEY)


def get_auth_context(
    request: Request,
    authorization: str | None = Header(default=None),
    x_iq_key: str | None = Header(default=None),
    x_tenant_id: str | None = Header(default=None),
    x_user_id: str | None = Header(default=None),
    x_user_email: str | None = Header(default=None),
) -> AuthContext:
    """
    Resolve (tenant_id, user_id) for tenant scoping. Fails closed.

    - AUTH_MODE=session: authenticated session cookie (user must be verified and
      approved), else a valid X-IQ-Key service key, else 401.
    - AUTH_MODE=hs256: Authorization: Bearer <jwt>, claims mapped to tenant/user.
    - AUTH_MODE=dev/none: unverified X-Tenant-Id / X-User-Id headers with DEV_*
      fallbacks. INSECURE - local development only.
    - Anything else, including unset: 500. Never falls through to permissive.
    """
    if AUTH_MODE == "session":
        user = _session_user(request)
        # Same gates RequireAuthMiddleware applies to HTML routes.
        if user is not None and user.is_verified and user.is_approved:
            return AuthContext(
                tenant_id=user.tenant_id,
                user_id=user.id,
                email=user.email,
                roles=["admin"] if user.is_superuser else [],
            )
        if _is_service_key(x_iq_key):
            return AuthContext(
                tenant_id=(x_tenant_id or "").strip() or DEV_TENANT_ID,
                user_id=(x_user_id or "").strip() or DEV_USER_ID,
                roles=["service"],
            )
        raise HTTPException(status_code=401, detail="Authentication required")

    if AUTH_MODE == "hs256":
        return _hs256_context(authorization)

    if AUTH_MODE in ("none", "dev"):
        return AuthContext(
            tenant_id=(x_tenant_id or "").strip() or DEV_TENANT_ID,
            user_id=(x_user_id or "").strip() or DEV_USER_ID,
            email=(x_user_email or "").strip() or None,
        )

    raise HTTPException(status_code=500, detail=f"Unsupported AUTH_MODE={AUTH_MODE!r}")
