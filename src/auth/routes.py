# src/auth/routes.py
"""
Authentication routes for web UI.

Provides:
- Login/logout
- User registration
- Email verification (6-digit code via SES)
- Password reset flow
- Session-based auth for browser clients
"""

from __future__ import annotations

import logging
import os
from typing import Annotated

from fastapi import APIRouter, Form, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, EmailStr

from src.auth.core import (
    SESSION_COOKIE_HTTPONLY,
    SESSION_COOKIE_NAME,
    SESSION_COOKIE_SAMESITE,
    SESSION_COOKIE_SECURE,
    User,
    authenticate,
    create_email_verification_code,
    create_password_reset_token,
    create_session,
    create_user,
    delete_session,
    get_session,
    get_user_by_email,
    mark_user_verified,
    use_password_reset_token,
    validate_email_verification_code,
    validate_password_reset_token,
)
from src.auth.ses import send_password_reset, send_verification_code

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["auth"])

# Templates directory
templates = Jinja2Templates(directory="src/auth/templates")

# Configuration
APP_NAME = os.getenv("APP_NAME", "CrestwellIQ")
APP_URL = os.getenv("APP_URL", "http://localhost:8000")
REGISTRATION_ENABLED = os.getenv("REGISTRATION_ENABLED", "true").lower() == "true"
DEFAULT_TENANT_ID = os.getenv("DEFAULT_TENANT_ID", os.getenv("DEV_TENANT_ID", "dev"))
POST_LOGIN_REDIRECT = os.getenv("POST_LOGIN_REDIRECT", "/admin/dashboard")
VERIFICATION_CODE_EXPIRY_MINUTES = int(os.getenv("VERIFICATION_CODE_EXPIRY_MINUTES", "15"))


# ---------------------------------------------------------------------------
# Request/Response Models
# ---------------------------------------------------------------------------


class LoginRequest(BaseModel):
    email: EmailStr
    password: str
    remember_me: bool = False


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str
    password_confirm: str
    display_name: str | None = None


class ForgotPasswordRequest(BaseModel):
    email: EmailStr


class ResetPasswordRequest(BaseModel):
    token: str
    password: str
    password_confirm: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_client_ip(request: Request) -> str | None:
    """Get client IP, respecting X-Forwarded-For from reverse proxy."""
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else None


def _set_session_cookie(response: Response, session_id: str, persistent: bool = False) -> None:
    """Set the session cookie on a response."""
    max_age = 30 * 24 * 60 * 60 if persistent else None  # 30 days or session
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=session_id,
        max_age=max_age,
        httponly=SESSION_COOKIE_HTTPONLY,
        secure=SESSION_COOKIE_SECURE,
        samesite=SESSION_COOKIE_SAMESITE,
    )


def _clear_session_cookie(response: Response) -> None:
    """Clear the session cookie."""
    response.delete_cookie(
        key=SESSION_COOKIE_NAME,
        httponly=SESSION_COOKIE_HTTPONLY,
        secure=SESSION_COOKIE_SECURE,
        samesite=SESSION_COOKIE_SAMESITE,
    )


def _render_template(
    request: Request,
    template_name: str,
    context: dict | None = None,
    status_code: int = 200,
) -> HTMLResponse:
    """Render a Jinja2 template with common context."""
    ctx = {
        "request": request,
        "app_name": APP_NAME,
        "registration_enabled": REGISTRATION_ENABLED,
        **(context or {}),
    }
    return templates.TemplateResponse(template_name, ctx, status_code=status_code)


def _get_session_user(request: Request) -> tuple[str | None, User | None]:
    """Get the session user from the cookie. Returns (session_id, user)."""
    session_id = request.cookies.get(SESSION_COOKIE_NAME)
    if not session_id:
        return None, None
    session, user = get_session(session_id)
    if session and user:
        return session_id, user
    return None, None


def _send_verification_email(user_id: str, email: str) -> tuple[bool, str | None]:
    """
    Generate a verification code and send it via SES.

    Returns (success, error_message).
    """
    code, error = create_email_verification_code(user_id, email)
    if not code:
        return False, error

    sent = send_verification_code(email, code, expiry_minutes=VERIFICATION_CODE_EXPIRY_MINUTES)
    if not sent:
        return False, "Failed to send verification email. Please try again."

    return True, None


# ---------------------------------------------------------------------------
# Login Routes
# ---------------------------------------------------------------------------


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, next: str | None = None, error: str | None = None):
    """Render login page."""
    session_id = request.cookies.get(SESSION_COOKIE_NAME)
    if session_id:
        session, user = get_session(session_id)
        if session and user:
            return RedirectResponse(url=next or POST_LOGIN_REDIRECT, status_code=302)

    return _render_template(
        request,
        "login.html",
        {
            "next_url": next or POST_LOGIN_REDIRECT,
            "error": error,
        },
    )


@router.post("/login")
async def login_submit(
    request: Request,
    email: Annotated[str, Form()],
    password: Annotated[str, Form()],
    remember_me: Annotated[bool, Form()] = False,
    next_url: Annotated[str, Form()] = "",
):
    """Process login form submission."""
    redirect_to = next_url or POST_LOGIN_REDIRECT

    user, error = authenticate(email, password)

    if not user:
        return _render_template(
            request,
            "login.html",
            {
                "next_url": redirect_to,
                "error": error,
                "email": email,
            },
            status_code=401,
        )

    # Create session
    session = create_session(
        user_id=user.id,
        tenant_id=user.tenant_id,
        ip_address=_get_client_ip(request),
        user_agent=request.headers.get("user-agent"),
        persistent=remember_me,
    )

    # Redirect based on user state:
    #  1. Unverified â†’ verify-email page
    #  2. Unapproved â†’ pending page
    #  3. Otherwise  â†’ dashboard
    if not user.is_verified:
        dest = "/auth/verify-email"
    elif not user.is_approved:
        dest = "/auth/pending"
    else:
        dest = redirect_to

    response = RedirectResponse(url=dest, status_code=302)
    _set_session_cookie(response, session.id, persistent=remember_me)

    logger.info(f"User logged in: {user.email} (tenant: {user.tenant_id})")
    return response


# ---------------------------------------------------------------------------
# Logout Route
# ---------------------------------------------------------------------------


@router.get("/logout")
async def logout(request: Request):
    """Log out and clear session."""
    session_id = request.cookies.get(SESSION_COOKIE_NAME)
    if session_id:
        delete_session(session_id)

    response = RedirectResponse(url="/auth/login", status_code=302)
    _clear_session_cookie(response)
    return response


# ---------------------------------------------------------------------------
# Email Verification Routes
# ---------------------------------------------------------------------------


@router.get("/verify-email", response_class=HTMLResponse)
async def verify_email_page(
    request: Request,
    error: str | None = None,
    success: str | None = None,
):
    """Render the email verification code entry page."""
    _, user = _get_session_user(request)

    if not user:
        return RedirectResponse(url="/auth/login", status_code=302)

    # Already verified â†’ move on
    if user.is_verified:
        if user.is_approved:
            return RedirectResponse(url=POST_LOGIN_REDIRECT, status_code=302)
        return RedirectResponse(url="/auth/pending", status_code=302)

    return _render_template(
        request,
        "verify_email.html",
        {
            "user_email": user.email,
            "error": error,
            "success": success,
        },
    )


@router.post("/verify-email")
async def verify_email_submit(
    request: Request,
    code: Annotated[str, Form()],
):
    """Validate the 6-digit verification code."""
    _, user = _get_session_user(request)

    if not user:
        return RedirectResponse(url="/auth/login", status_code=302)

    if user.is_verified:
        return RedirectResponse(url=POST_LOGIN_REDIRECT, status_code=302)

    # Strip whitespace in case user copies with spaces
    code = code.strip()

    success, error = validate_email_verification_code(user.id, code)
    if not success:
        return _render_template(
            request,
            "verify_email.html",
            {
                "user_email": user.email,
                "error": error,
            },
            status_code=400,
        )

    # Mark user as verified
    mark_user_verified(user.id)
    logger.info(f"Email verified: {user.email}")

    # Redirect based on approval status
    if user.is_approved:
        return RedirectResponse(url=POST_LOGIN_REDIRECT, status_code=302)
    return RedirectResponse(url="/auth/pending", status_code=302)


@router.post("/verify-email/resend")
async def verify_email_resend(request: Request):
    """Resend a new verification code."""
    _, user = _get_session_user(request)

    if not user:
        return RedirectResponse(url="/auth/login", status_code=302)

    if user.is_verified:
        return RedirectResponse(url=POST_LOGIN_REDIRECT, status_code=302)

    sent, error = _send_verification_email(user.id, user.email)
    if not sent:
        return _render_template(
            request,
            "verify_email.html",
            {
                "user_email": user.email,
                "error": error,
            },
        )

    return _render_template(
        request,
        "verify_email.html",
        {
            "user_email": user.email,
            "success": "A new verification code has been sent to your email.",
        },
    )


# ---------------------------------------------------------------------------
# Pending Approval Route
# ---------------------------------------------------------------------------


@router.get("/pending", response_class=HTMLResponse)
async def pending_page(request: Request):
    """Show pending approval page for unapproved users."""
    _, user = _get_session_user(request)

    if not user:
        return RedirectResponse(url="/auth/login", status_code=302)

    # Unverified â†’ send to verify first
    if not user.is_verified:
        return RedirectResponse(url="/auth/verify-email", status_code=302)

    if user.is_approved:
        return RedirectResponse(url=POST_LOGIN_REDIRECT, status_code=302)

    return _render_template(
        request,
        "pending.html",
        {
            "user_email": user.email,
        },
    )


# ---------------------------------------------------------------------------
# Registration Routes
# ---------------------------------------------------------------------------


@router.get("/register", response_class=HTMLResponse)
async def register_page(request: Request, error: str | None = None):
    """Render registration page."""
    if not REGISTRATION_ENABLED:
        raise HTTPException(status_code=403, detail="Registration is disabled")

    return _render_template(
        request,
        "register.html",
        {
            "error": error,
        },
    )


@router.post("/register")
async def register_submit(
    request: Request,
    email: Annotated[str, Form()],
    password: Annotated[str, Form()],
    password_confirm: Annotated[str, Form()],
    display_name: Annotated[str | None, Form()] = None,
):
    """Process registration form submission."""
    if not REGISTRATION_ENABLED:
        raise HTTPException(status_code=403, detail="Registration is disabled")

    # Validate passwords match
    if password != password_confirm:
        return _render_template(
            request,
            "register.html",
            {
                "error": "Passwords do not match",
                "email": email,
                "display_name": display_name,
            },
            status_code=400,
        )

    tenant_id = DEFAULT_TENANT_ID

    # Create user â€” NOT verified, NOT approved
    user, error = create_user(
        email=email,
        password=password,
        tenant_id=tenant_id,
        display_name=display_name,
        is_verified=False,
        is_approved=False,
    )

    if not user:
        return _render_template(
            request,
            "register.html",
            {
                "error": error,
                "email": email,
                "display_name": display_name,
            },
            status_code=400,
        )

    # Send verification code via SES
    sent, ses_error = _send_verification_email(user.id, user.email)
    if not sent:
        logger.error(f"Failed to send verification email during registration: {ses_error}")
        # Don't block registration â€” user can resend from the verify page

    # Auto-login after registration
    session = create_session(
        user_id=user.id,
        tenant_id=user.tenant_id,
        ip_address=_get_client_ip(request),
        user_agent=request.headers.get("user-agent"),
    )

    # Redirect to email verification page
    response = RedirectResponse(url="/auth/verify-email", status_code=302)
    _set_session_cookie(response, session.id)

    logger.info(
        "New user registered (pending verification): %s (tenant: %s)",
        user.email,
        user.tenant_id,
    )
    return response


# ---------------------------------------------------------------------------
# Password Reset Routes
# ---------------------------------------------------------------------------


@router.get("/forgot-password", response_class=HTMLResponse)
async def forgot_password_page(request: Request, success: bool = False):
    """Render forgot password page."""
    return _render_template(
        request,
        "forgot_password.html",
        {
            "success": success,
        },
    )


@router.post("/forgot-password")
async def forgot_password_submit(
    request: Request,
    email: Annotated[str, Form()],
):
    """Process forgot password form."""
    user = get_user_by_email(email)

    # Always show success to prevent email enumeration
    if user:
        token = create_password_reset_token(user.id, _get_client_ip(request))
        reset_url = f"{APP_URL}/auth/reset-password?token={token}"

        sent = send_password_reset(user.email, reset_url)
        if not sent:
            logger.error(f"Failed to send password reset email to {email}")

    return _render_template(
        request,
        "forgot_password.html",
        {
            "success": True,
        },
    )


@router.get("/reset-password", response_class=HTMLResponse)
async def reset_password_page(request: Request, token: str, error: str | None = None):
    """Render password reset page."""
    user = validate_password_reset_token(token)
    if not user:
        return _render_template(
            request,
            "reset_password.html",
            {
                "error": "Invalid or expired reset link. Please request a new one.",
                "token": None,
            },
        )

    return _render_template(
        request,
        "reset_password.html",
        {
            "token": token,
            "error": error,
        },
    )


@router.post("/reset-password")
async def reset_password_submit(
    request: Request,
    token: Annotated[str, Form()],
    password: Annotated[str, Form()],
    password_confirm: Annotated[str, Form()],
):
    """Process password reset form."""
    if password != password_confirm:
        return _render_template(
            request,
            "reset_password.html",
            {
                "error": "Passwords do not match",
                "token": token,
            },
            status_code=400,
        )

    success, error = use_password_reset_token(token, password)

    if not success:
        return _render_template(
            request,
            "reset_password.html",
            {
                "error": error,
                "token": token if "expired" not in (error or "").lower() else None,
            },
            status_code=400,
        )

    return RedirectResponse(
        url="/auth/login?error=Password+reset+successful.+Please+log+in.",
        status_code=302,
    )


# ---------------------------------------------------------------------------
# API Endpoints (JSON responses for SPA/API clients)
# ---------------------------------------------------------------------------


@router.post("/api/login")
async def api_login(request: Request, body: LoginRequest):
    """API endpoint for login (returns JSON)."""
    user, error = authenticate(body.email, body.password)

    if not user:
        raise HTTPException(status_code=401, detail=error)

    session = create_session(
        user_id=user.id,
        tenant_id=user.tenant_id,
        ip_address=_get_client_ip(request),
        user_agent=request.headers.get("user-agent"),
        persistent=body.remember_me,
    )

    return {
        "session_id": session.id,
        "user": {
            "id": user.id,
            "email": user.email,
            "display_name": user.display_name,
            "tenant_id": user.tenant_id,
            "is_verified": user.is_verified,
        },
        "expires_at": session.expires_at,
    }


@router.post("/api/register")
async def api_register(request: Request, body: RegisterRequest):
    """API endpoint for registration (returns JSON)."""
    if not REGISTRATION_ENABLED:
        raise HTTPException(status_code=403, detail="Registration is disabled")

    if body.password != body.password_confirm:
        raise HTTPException(status_code=400, detail="Passwords do not match")

    user, error = create_user(
        email=body.email,
        password=body.password,
        tenant_id=DEFAULT_TENANT_ID,
        display_name=body.display_name,
        is_verified=False,
        ip_address=_get_client_ip(request),
    )

    if not user:
        raise HTTPException(status_code=400, detail=error)

    # Send verification code
    _send_verification_email(user.id, user.email)

    session = create_session(
        user_id=user.id,
        tenant_id=user.tenant_id,
        ip_address=_get_client_ip(request),
        user_agent=request.headers.get("user-agent"),
    )

    return {
        "session_id": session.id,
        "user": {
            "id": user.id,
            "email": user.email,
            "display_name": user.display_name,
            "tenant_id": user.tenant_id,
            "is_verified": False,
        },
        "verify_email_required": True,
    }


@router.post("/api/verify-email")
async def api_verify_email(request: Request, code: str):
    """API endpoint for email verification (returns JSON)."""
    _, user = _get_session_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")

    success, error = validate_email_verification_code(user.id, code.strip())
    if not success:
        raise HTTPException(status_code=400, detail=error)

    mark_user_verified(user.id)
    return {"verified": True}


@router.post("/api/verify-email/resend")
async def api_resend_verification(request: Request):
    """API endpoint to resend verification code."""
    _, user = _get_session_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")

    if user.is_verified:
        return {"already_verified": True}

    sent, error = _send_verification_email(user.id, user.email)
    if not sent:
        raise HTTPException(status_code=500, detail=error or "Failed to send email")

    return {"sent": True}


@router.get("/api/me")
async def api_me(request: Request):
    """Get current user info from session."""
    session_id = request.cookies.get(SESSION_COOKIE_NAME)
    if not session_id:
        raise HTTPException(status_code=401, detail="Not authenticated")

    session, user = get_session(session_id)
    if not session or not user:
        raise HTTPException(status_code=401, detail="Session expired")

    return {
        "user": {
            "id": user.id,
            "email": user.email,
            "display_name": user.display_name,
            "tenant_id": user.tenant_id,
            "is_superuser": user.is_superuser,
            "is_verified": user.is_verified,
        },
        "session": {
            "created_at": session.created_at,
            "expires_at": session.expires_at,
        },
    }
