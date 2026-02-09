# src/auth/middleware.py
"""
Authentication middleware for protecting routes.

Provides:
- Session validation middleware
- Route protection decorators
- AuthContext injection from sessions
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Annotated

from fastapi import Depends, HTTPException, Request
from fastapi.responses import RedirectResponse

from src.auth.core import (
    SESSION_COOKIE_NAME,
    User,
    UserLimits,
    check_usage_limit,
    get_session,
    get_user_limits,
)

if TYPE_CHECKING:
    from src.api.app import AuthContext

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Session-Based Auth Dependencies
# ---------------------------------------------------------------------------

async def get_current_user_optional(request: Request) -> User | None:
    """
    Get the current user from session cookie (optional).
    
    Returns None if not authenticated.
    """
    session_id = request.cookies.get(SESSION_COOKIE_NAME)
    if not session_id:
        return None
    
    session, user = get_session(session_id)
    if not session or not user:
        return None
    
    return user


async def get_current_user(request: Request) -> User:
    """
    Get the current user from session cookie (required).
    
    Raises 401 if not authenticated.
    """
    user = await get_current_user_optional(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


async def get_current_user_or_redirect(request: Request) -> User:
    """
    Get the current user, redirecting to login if not authenticated.
    
    For HTML pages that should redirect to login.
    """
    user = await get_current_user_optional(request)
    if not user:
        # Store the current URL to redirect back after login
        next_url = str(request.url)
        raise HTTPException(
            status_code=302,
            headers={"Location": f"/auth/login?next={next_url}"},
        )
    return user


# ---------------------------------------------------------------------------
# AuthContext Bridge
# ---------------------------------------------------------------------------

def create_auth_context_from_user(user: User) -> AuthContext:
    """
    Create an AuthContext from a User object.
    
    This bridges the session-based auth to the existing JWT-based AuthContext
    used throughout the API.
    """
    from src.api.app import AuthContext
    
    return AuthContext(
        tenant_id=user.tenant_id,
        user_id=user.id,
        email=user.email,
        roles=["admin"] if user.is_superuser else [],
    )


async def get_auth_context_from_session(request: Request) -> AuthContext:
    """
    Get AuthContext from session cookie.
    
    This can be used as a drop-in replacement for the existing JWT-based
    get_auth_context dependency when AUTH_MODE is set to use sessions.
    """
    user = await get_current_user(request)
    return create_auth_context_from_user(user)


# ---------------------------------------------------------------------------
# Limit Checking Dependencies
# ---------------------------------------------------------------------------

class LimitChecker:
    """
    Dependency for checking user limits before an operation.
    
    Usage:
        @app.post("/runs")
        async def create_run(
            user: User = Depends(get_current_user),
            _check: None = Depends(LimitChecker("runs", "daily")),
        ):
            ...
    """
    
    def __init__(self, counter_type: str, period_type: str):
        self.counter_type = counter_type
        self.period_type = period_type
    
    async def __call__(self, request: Request) -> None:
        user = await get_current_user(request)
        
        current, limit = check_usage_limit(
            user.id,
            user.tenant_id,
            self.counter_type,
            self.period_type,
        )
        
        if limit is not None and current >= limit:
            raise HTTPException(
                status_code=429,
                detail=(
                    f"Rate limit exceeded: {current}/{limit}"
                    f" {self.counter_type} per {self.period_type}"
                ),
            )


async def get_user_limits_dep(request: Request) -> UserLimits:
    """Get the current user's limits."""
    user = await get_current_user(request)
    return get_user_limits(user.id, user.tenant_id)


# ---------------------------------------------------------------------------
# Role-Based Access Control
# ---------------------------------------------------------------------------

def require_superuser(
    user: Annotated[User, Depends(get_current_user)],
) -> User:
    """Require the current user to be a superuser."""
    if not user.is_superuser:
        raise HTTPException(
            status_code=403,
            detail="This action requires superuser privileges",
        )
    return user


def require_verified(
    user: Annotated[User, Depends(get_current_user)],
) -> User:
    """Require the current user's email to be verified."""
    if not user.is_verified:
        raise HTTPException(
            status_code=403,
            detail="Please verify your email address to access this feature",
        )
    return user


def require_feature(feature_name: str) -> Callable:
    """
    Create a dependency that requires a specific feature to be enabled.
    
    Usage:
        @app.post("/ai-extract")
        async def ai_extract(
            _: None = Depends(require_feature("can_use_ai_extraction")),
        ):
            ...
    """
    async def _checker(request: Request) -> None:
        user = await get_current_user(request)
        limits = get_user_limits(user.id, user.tenant_id)
        
        if not getattr(limits, feature_name, False):
            raise HTTPException(
                status_code=403,
                detail=f"Feature '{feature_name}' is not enabled for your account",
            )
    
    return _checker


# ---------------------------------------------------------------------------
# HTML Page Protection Middleware
# ---------------------------------------------------------------------------

class RequireAuthMiddleware:
    """
    ASGI middleware that redirects unauthenticated requests to login.
    
    Also enforces the verification â†’ approval pipeline:
      1. Not logged in   â†’ /auth/login
      2. Not verified    â†’ /auth/verify-email
      3. Not approved    â†’ /auth/pending
      4. All good        â†’ continue to requested page
    
    Apply this to protect entire route groups.
    """
    
    def __init__(
        self,
        app,
        exclude_paths: list[str] | None = None,
        login_url: str = "/auth/login",
        pending_url: str = "/auth/pending",
        verify_url: str = "/auth/verify-email",
    ):
        self.app = app
        self.exclude_paths = exclude_paths or [
            "/auth/",
            "/static/",
            "/health",
            "/favicon.ico",
        ]
        self.login_url = login_url
        self.pending_url = pending_url
        self.verify_url = verify_url
    
    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        
        path = scope.get("path", "")
        
        # Check if path is excluded
        for excluded in self.exclude_paths:
            if path.startswith(excluded):
                return await self.app(scope, receive, send)
        
        # Check session
        headers = dict(scope.get("headers", []))
        cookie_header = headers.get(b"cookie", b"").decode()
        
        session_id = None
        for part in cookie_header.split(";"):
            part = part.strip()
            if part.startswith(f"{SESSION_COOKIE_NAME}="):
                session_id = part.split("=", 1)[1]
                break
        
        if session_id:
            session, user = get_session(session_id)
            if session and user:
                # Gate 1: Email must be verified
                if not user.is_verified:
                    response = RedirectResponse(url=self.verify_url, status_code=302)
                    await response(scope, receive, send)
                    return

                # Gate 2: Account must be approved
                if not user.is_approved:
                    response = RedirectResponse(url=self.pending_url, status_code=302)
                    await response(scope, receive, send)
                    return
                
                # Authenticated, verified, and approved â€” continue
                return await self.app(scope, receive, send)
        
        # Not authenticated, redirect to login
        query_string = scope.get("query_string", b"").decode()
        next_url = f"{path}?{query_string}" if query_string else path
        redirect_url = f"{self.login_url}?next={next_url}"
        
        response = RedirectResponse(url=redirect_url, status_code=302)
        await response(scope, receive, send)
