# src/auth/core.py
"""
Core authentication module.

Provides:
- Password hashing (bcrypt)
- User registration and management
- Session-based authentication
- Password reset tokens
- Email verification codes (6-digit via SES)
- User limit enforcement
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import secrets
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from typing import Literal

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Session configuration
SESSION_DURATION_HOURS = int(os.getenv("SESSION_DURATION_HOURS", "24"))
SESSION_DURATION_PERSISTENT_DAYS = int(os.getenv("SESSION_DURATION_PERSISTENT_DAYS", "30"))
SESSION_COOKIE_NAME = os.getenv("SESSION_COOKIE_NAME", "session_id")
SESSION_COOKIE_SECURE = os.getenv("SESSION_COOKIE_SECURE", "true").lower() == "true"
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE: Literal["lax", "strict", "none"] = "lax"

# Password reset
PASSWORD_RESET_EXPIRY_HOURS = int(os.getenv("PASSWORD_RESET_EXPIRY_HOURS", "1"))

# Email verification
EMAIL_VERIFY_EXPIRY_HOURS = int(os.getenv("EMAIL_VERIFY_EXPIRY_HOURS", "48"))

# Email verification codes (6-digit)
VERIFICATION_CODE_EXPIRY_MINUTES = int(os.getenv("VERIFICATION_CODE_EXPIRY_MINUTES", "15"))
VERIFICATION_CODE_MAX_ATTEMPTS = int(os.getenv("VERIFICATION_CODE_MAX_ATTEMPTS", "5"))
VERIFICATION_CODE_MAX_ACTIVE = int(os.getenv("VERIFICATION_CODE_MAX_ACTIVE", "5"))

# Account lockout
MAX_FAILED_LOGIN_ATTEMPTS = int(os.getenv("MAX_FAILED_LOGIN_ATTEMPTS", "5"))
LOCKOUT_DURATION_MINUTES = int(os.getenv("LOCKOUT_DURATION_MINUTES", "15"))

# Password requirements
MIN_PASSWORD_LENGTH = int(os.getenv("MIN_PASSWORD_LENGTH", "8"))


# ---------------------------------------------------------------------------
# Password Hashing (bcrypt via hashlib fallback if bcrypt unavailable)
# ---------------------------------------------------------------------------

try:
    import bcrypt
    _BCRYPT_AVAILABLE = True
except ImportError:
    _BCRYPT_AVAILABLE = False
    logger.warning("bcrypt not available, using PBKDF2 fallback (less secure)")


def hash_password(password: str) -> str:
    """Hash a password for storage."""
    if _BCRYPT_AVAILABLE:
        salt = bcrypt.gensalt(rounds=12)
        hashed = bcrypt.hashpw(password.encode("utf-8"), salt)
        return hashed.decode("utf-8")
    else:
        # PBKDF2 fallback
        salt = secrets.token_hex(16)
        dk = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            salt.encode("utf-8"),
            iterations=100000,
        )
        return f"pbkdf2:sha256:100000${salt}${dk.hex()}"


def verify_password(password: str, password_hash: str) -> bool:
    """Verify a password against its hash."""
    if not password_hash:
        return False
    
    if password_hash.startswith("pbkdf2:"):
        # PBKDF2 format: pbkdf2:sha256:iterations$salt$hash
        try:
            _, algo_info, rest = password_hash.split(":", 2)
            algo, iterations_str = algo_info.split(":")
            iterations = int(iterations_str)
            salt, stored_hash = rest.split("$", 1)
            dk = hashlib.pbkdf2_hmac(
                algo,
                password.encode("utf-8"),
                salt.encode("utf-8"),
                iterations=iterations,
            )
            return hmac.compare_digest(dk.hex(), stored_hash)
        except Exception:
            return False
    elif _BCRYPT_AVAILABLE:
        try:
            return bcrypt.checkpw(
                password.encode("utf-8"),
                password_hash.encode("utf-8"),
            )
        except Exception:
            return False
    
    return False


def validate_password_strength(password: str) -> tuple[bool, str | None]:
    """
    Validate password meets requirements.
    
    Returns (is_valid, error_message).
    """
    if len(password) < MIN_PASSWORD_LENGTH:
        return False, f"Password must be at least {MIN_PASSWORD_LENGTH} characters"
    
    # Basic complexity checks
    has_upper = any(c.isupper() for c in password)
    has_lower = any(c.islower() for c in password)
    has_digit = any(c.isdigit() for c in password)
    
    if not (has_upper and has_lower and has_digit):
        return False, "Password must contain uppercase, lowercase, and a number"
    
    return True, None


# ---------------------------------------------------------------------------
# Data Classes
# ---------------------------------------------------------------------------

@dataclass
class User:
    """User model."""
    id: str
    tenant_id: str
    email: str
    display_name: str | None
    is_active: bool
    is_verified: bool
    is_superuser: bool
    is_approved: bool
    created_at: str
    last_login_at: str | None
    
    @classmethod
    def from_row(cls, row: dict[str, Any]) -> User:
        return cls(
            id=row["id"],
            tenant_id=row["tenant_id"],
            email=row["email"],
            display_name=row.get("display_name"),
            is_active=bool(row.get("is_active", True)),
            is_verified=bool(row.get("is_verified", False)),
            is_superuser=bool(row.get("is_superuser", False)),
            is_approved=bool(row.get("is_approved", False)),
            created_at=row.get("created_at", ""),
            last_login_at=row.get("last_login_at"),
        )


@dataclass
class Session:
    """Session model."""
    id: str
    user_id: str
    tenant_id: str
    created_at: str
    expires_at: str
    is_persistent: bool


@dataclass 
class UserLimits:
    """User limits/quotas."""
    max_runs_per_day: int | None
    max_domains_per_run: int | None
    max_concurrent_runs: int | None
    max_verifications_per_day: int | None
    max_verifications_per_month: int | None
    max_exports_per_day: int | None
    max_export_rows: int | None
    can_use_ai_extraction: bool
    can_use_smtp_verify: bool
    can_access_admin: bool


# ---------------------------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------------------------

def _utc_now() -> datetime:
    return datetime.now(UTC)


def _utc_now_iso() -> str:
    return _utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")


def _generate_token() -> str:
    """Generate a secure random token."""
    return secrets.token_urlsafe(32)


def _generate_user_id() -> str:
    """Generate a user ID."""
    return f"user_{uuid.uuid4().hex[:12]}"


def _generate_verification_code() -> str:
    """Generate a cryptographically random 6-digit numeric code."""
    return f"{secrets.randbelow(1_000_000):06d}"


# ---------------------------------------------------------------------------
# Database Operations
# ---------------------------------------------------------------------------

def _get_conn():
    """Get database connection."""
    from src.db import get_conn
    return get_conn()


def get_user_by_email(email: str) -> User | None:
    """Look up user by email address."""
    email = email.lower().strip()
    conn = _get_conn()
    try:
        cur = conn.execute(
            "SELECT * FROM users WHERE LOWER(email) = %s AND is_active = TRUE",
            (email,),
        )
        row = cur.fetchone()
        if row:
            return User.from_row(dict(row))
        return None
    finally:
        conn.close()


def get_user_by_id(user_id: str) -> User | None:
    """Look up user by ID."""
    conn = _get_conn()
    try:
        cur = conn.execute(
            "SELECT * FROM users WHERE id = %s",
            (user_id,),
        )
        row = cur.fetchone()
        if row:
            return User.from_row(dict(row))
        return None
    finally:
        conn.close()


def get_user_password_hash(user_id: str) -> str | None:
    """Get password hash for a user (separate query for security)."""
    conn = _get_conn()
    try:
        cur = conn.execute(
            "SELECT password_hash FROM users WHERE id = %s",
            (user_id,),
        )
        row = cur.fetchone()
        return row["password_hash"] if row else None
    finally:
        conn.close()


def create_user(
    *,
    email: str,
    password: str,
    tenant_id: str,
    display_name: str | None = None,
    is_verified: bool = False,
    is_approved: bool = False,
) -> tuple[User | None, str | None]:
    """
    Create a new user.
    
    Args:
        email: User's email address
        password: User's password (will be hashed)
        tenant_id: Tenant to associate user with
        display_name: Optional display name
        is_verified: Whether email is verified (default False)
        is_approved: Whether user is approved to access the app (default False)
    
    Returns (user, error_message).
    """
    email = email.lower().strip()
    
    # Validate email format
    if not email or "@" not in email:
        return None, "Invalid email address"
    
    # Validate password
    is_valid, error = validate_password_strength(password)
    if not is_valid:
        return None, error
    
    # Check if user exists
    existing = get_user_by_email(email)
    if existing:
        return None, "An account with this email already exists"
    
    user_id = _generate_user_id()
    password_hash = hash_password(password)
    now = _utc_now_iso()
    
    conn = _get_conn()
    try:
        # Ensure tenant exists
        conn.execute(
            """
            INSERT INTO tenants (id, name, created_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """,
            (tenant_id, tenant_id, now),
        )
        
        conn.execute(
            """
            INSERT INTO users (
                id, tenant_id, email, password_hash, display_name,
                is_active, is_verified, is_superuser, is_approved, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, TRUE, %s, FALSE, %s, %s, %s)
            """,
            (
                user_id, tenant_id, email, password_hash,
                display_name, is_verified, is_approved, now, now,
            ),
        )
        
        # Create default user limits
        conn.execute(
            """
            INSERT INTO user_limits (user_id, tenant_id, created_at, updated_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id) DO NOTHING
            """,
            (user_id, tenant_id, now, now),
        )
        
        conn.commit()
        
        user = get_user_by_id(user_id)
        return user, None
        
    except Exception as e:
        conn.rollback()
        logger.exception("Failed to create user")
        return None, f"Failed to create account: {e}"
    finally:
        conn.close()


def update_last_login(user_id: str) -> None:
    """Update user's last login timestamp."""
    conn = _get_conn()
    try:
        conn.execute(
            "UPDATE users SET last_login_at = %s WHERE id = %s",
            (_utc_now_iso(), user_id),
        )
        conn.commit()
    finally:
        conn.close()


def check_account_locked(user_id: str) -> tuple[bool, str | None]:
    """
    Check if account is locked due to failed login attempts.
    
    Returns (is_locked, locked_until).
    """
    conn = _get_conn()
    try:
        cur = conn.execute(
            "SELECT failed_login_attempts, locked_until FROM users WHERE id = %s",
            (user_id,),
        )
        row = cur.fetchone()
        if not row:
            return False, None
        
        locked_until = row.get("locked_until")
        if locked_until:
            locked_dt = datetime.fromisoformat(locked_until.replace("Z", "+00:00"))
            if locked_dt > _utc_now():
                return True, locked_until
            # Lock expired, clear it
            conn.execute(
                "UPDATE users SET locked_until = NULL, failed_login_attempts = 0 WHERE id = %s",
                (user_id,),
            )
            conn.commit()
        
        return False, None
    finally:
        conn.close()


def record_failed_login(user_id: str) -> None:
    """Record a failed login attempt, potentially locking the account."""
    conn = _get_conn()
    try:
        cur = conn.execute(
            "SELECT failed_login_attempts FROM users WHERE id = %s",
            (user_id,),
        )
        row = cur.fetchone()
        attempts = (row.get("failed_login_attempts") or 0) + 1 if row else 1
        
        locked_until = None
        if attempts >= MAX_FAILED_LOGIN_ATTEMPTS:
            locked_until = (_utc_now() + timedelta(minutes=LOCKOUT_DURATION_MINUTES)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        
        conn.execute(
            "UPDATE users SET failed_login_attempts = %s, locked_until = %s WHERE id = %s",
            (attempts, locked_until, user_id),
        )
        conn.commit()
    finally:
        conn.close()


def clear_failed_logins(user_id: str) -> None:
    """Clear failed login counter on successful login."""
    conn = _get_conn()
    try:
        conn.execute(
            "UPDATE users SET failed_login_attempts = 0, locked_until = NULL WHERE id = %s",
            (user_id,),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Session Management
# ---------------------------------------------------------------------------

def create_session(
    user_id: str,
    tenant_id: str,
    *,
    ip_address: str | None = None,
    user_agent: str | None = None,
    persistent: bool = False,
) -> Session:
    """Create a new session for a user."""
    session_id = _generate_token()
    now = _utc_now()
    now_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    if persistent:
        expires = now + timedelta(days=SESSION_DURATION_PERSISTENT_DAYS)
    else:
        expires = now + timedelta(hours=SESSION_DURATION_HOURS)
    
    expires_iso = expires.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    conn = _get_conn()
    try:
        conn.execute(
            """
            INSERT INTO sessions (
                id, user_id, tenant_id, created_at, expires_at,
                last_activity_at, ip_address, user_agent, is_persistent
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                session_id, user_id, tenant_id, now_iso, expires_iso,
                now_iso, ip_address, user_agent, persistent,
            ),
        )
        conn.commit()
        
        return Session(
            id=session_id,
            user_id=user_id,
            tenant_id=tenant_id,
            created_at=now_iso,
            expires_at=expires_iso,
            is_persistent=persistent,
        )
    finally:
        conn.close()


def get_session(session_id: str) -> tuple[Session | None, User | None]:
    """
    Get session and associated user.
    
    Returns (session, user) or (None, None) if invalid/expired.
    """
    if not session_id:
        return None, None
    
    conn = _get_conn()
    try:
        cur = conn.execute(
            """
            SELECT s.*, u.* FROM sessions s
            JOIN users u ON u.id = s.user_id
            WHERE s.id = %s AND u.is_active = TRUE
            """,
            (session_id,),
        )
        row = cur.fetchone()
        if not row:
            return None, None
        
        # Check expiry
        expires_at = row.get("expires_at")
        if expires_at:
            expires_dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
            if expires_dt < _utc_now():
                # Session expired, clean it up
                delete_session(session_id)
                return None, None
        
        session = Session(
            id=row["id"],
            user_id=row["user_id"],
            tenant_id=row["tenant_id"],
            created_at=row["created_at"],
            expires_at=row["expires_at"],
            is_persistent=bool(row.get("is_persistent")),
        )
        
        user = User.from_row(dict(row))
        
        # Update last activity
        conn.execute(
            "UPDATE sessions SET last_activity_at = %s WHERE id = %s",
            (_utc_now_iso(), session_id),
        )
        conn.commit()
        
        return session, user
        
    finally:
        conn.close()


def delete_session(session_id: str) -> None:
    """Delete a session (logout)."""
    conn = _get_conn()
    try:
        conn.execute("DELETE FROM sessions WHERE id = %s", (session_id,))
        conn.commit()
    finally:
        conn.close()


def delete_user_sessions(user_id: str) -> None:
    """Delete all sessions for a user (force logout everywhere)."""
    conn = _get_conn()
    try:
        conn.execute("DELETE FROM sessions WHERE user_id = %s", (user_id,))
        conn.commit()
    finally:
        conn.close()


def cleanup_expired_sessions() -> int:
    """Remove expired sessions. Returns count deleted."""
    conn = _get_conn()
    try:
        cur = conn.execute(
            "DELETE FROM sessions WHERE expires_at < %s",
            (_utc_now_iso(),),
        )
        conn.commit()
        return cur.rowcount or 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Password Reset
# ---------------------------------------------------------------------------

def create_password_reset_token(user_id: str, ip_address: str | None = None) -> str:
    """Create a password reset token."""
    token = _generate_token()
    now = _utc_now()
    expires = now + timedelta(hours=PASSWORD_RESET_EXPIRY_HOURS)
    
    conn = _get_conn()
    try:
        conn.execute(
            """
            INSERT INTO password_reset_tokens (id, user_id, created_at, expires_at, ip_address)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                token,
                user_id,
                now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
                ip_address,
            ),
        )
        conn.commit()
        return token
    finally:
        conn.close()


def validate_password_reset_token(token: str) -> User | None:
    """
    Validate a password reset token.
    
    Returns the user if valid, None otherwise.
    """
    conn = _get_conn()
    try:
        cur = conn.execute(
            """
            SELECT t.*, u.* FROM password_reset_tokens t
            JOIN users u ON u.id = t.user_id
            WHERE t.id = %s AND t.used_at IS NULL AND u.is_active = TRUE
            """,
            (token,),
        )
        row = cur.fetchone()
        if not row:
            return None
        
        # Check expiry
        expires_at = row.get("expires_at")
        if expires_at:
            expires_dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
            if expires_dt < _utc_now():
                return None
        
        return User.from_row(dict(row))
    finally:
        conn.close()


def use_password_reset_token(token: str, new_password: str) -> tuple[bool, str | None]:
    """
    Use a password reset token to set a new password.
    
    Returns (success, error_message).
    """
    # Validate password
    is_valid, error = validate_password_strength(new_password)
    if not is_valid:
        return False, error
    
    user = validate_password_reset_token(token)
    if not user:
        return False, "Invalid or expired reset token"
    
    password_hash = hash_password(new_password)
    now = _utc_now_iso()
    
    conn = _get_conn()
    try:
        # Update password
        conn.execute(
            "UPDATE users SET password_hash = %s, updated_at = %s WHERE id = %s",
            (password_hash, now, user.id),
        )
        
        # Mark token as used
        conn.execute(
            "UPDATE password_reset_tokens SET used_at = %s WHERE id = %s",
            (now, token),
        )
        
        # Invalidate all existing sessions (security measure)
        conn.execute("DELETE FROM sessions WHERE user_id = %s", (user.id,))
        
        conn.commit()
        return True, None
    except Exception as e:
        conn.rollback()
        logger.exception("Failed to reset password")
        return False, f"Failed to reset password: {e}"
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Email Verification Codes (6-digit via SES)
# ---------------------------------------------------------------------------

def create_email_verification_code(
    user_id: str,
    email: str,
) -> tuple[str | None, str | None]:
    """
    Create a new email verification code for a user.

    Invalidates any previous unused codes for this user, then inserts a new
    one with a short expiry window.

    Returns (code, error_message).
    """
    email = email.lower().strip()
    code = _generate_verification_code()
    token_id = _generate_token()
    now = _utc_now()
    expires = now + timedelta(minutes=VERIFICATION_CODE_EXPIRY_MINUTES)
    now_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    expires_iso = expires.strftime("%Y-%m-%dT%H:%M:%SZ")

    conn = _get_conn()
    try:
        # Rate-limit: count codes created in the last hour for this user
        one_hour_ago = (now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        cur = conn.execute(
            """
            SELECT COUNT(*) AS cnt FROM email_verification_tokens
            WHERE user_id = %s AND created_at > %s
            """,
            (user_id, one_hour_ago),
        )
        row = cur.fetchone()
        recent_count = (row["cnt"] if row else 0)
        if recent_count >= VERIFICATION_CODE_MAX_ACTIVE:
            return None, "Too many verification codes requested. Please wait before trying again."

        # Invalidate previous unused codes (set expired timestamp in the past)
        conn.execute(
            """
            UPDATE email_verification_tokens
            SET expires_at = %s
            WHERE user_id = %s AND verified_at IS NULL
            """,
            (now_iso, user_id),
        )

        # Insert new code
        conn.execute(
            """
            INSERT INTO email_verification_tokens
                (id, user_id, email, code, created_at, expires_at, attempts)
            VALUES (%s, %s, %s, %s, %s, %s, 0)
            """,
            (token_id, user_id, email, code, now_iso, expires_iso),
        )
        conn.commit()

        logger.info(
            "Verification code created",
            extra={"user_id": user_id, "email": email, "token_id": token_id},
        )
        return code, None

    except Exception as e:
        conn.rollback()
        logger.exception("Failed to create verification code")
        return None, f"Failed to generate verification code: {e}"
    finally:
        conn.close()


def validate_email_verification_code(
    user_id: str,
    code: str,
) -> tuple[bool, str | None]:
    """
    Validate a 6-digit verification code for a user.

    Checks:
      - Code matches an active (not expired, not used) token for the user
      - Attempt count has not exceeded the brute-force threshold

    On success, marks the token as verified.
    On failure, increments the attempt counter.

    Returns (success, error_message).
    """
    code = code.strip()
    now_iso = _utc_now_iso()

    conn = _get_conn()
    try:
        # Find the latest active token for this user
        cur = conn.execute(
            """
            SELECT id, code, attempts, expires_at
            FROM email_verification_tokens
            WHERE user_id = %s AND verified_at IS NULL
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (user_id,),
        )
        row = cur.fetchone()

        if not row:
            return False, "No active verification code found. Please request a new one."

        token_id = row["id"]
        stored_code = row["code"]
        attempts = row["attempts"] or 0
        expires_at = row["expires_at"]

        # Check expiry
        if expires_at:
            expires_dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
            if expires_dt < _utc_now():
                return False, "Verification code has expired. Please request a new one."

        # Check attempt limit
        if attempts >= VERIFICATION_CODE_MAX_ATTEMPTS:
            return False, "Too many failed attempts. Please request a new code."

        # Constant-time comparison to prevent timing attacks
        if not hmac.compare_digest(code, stored_code or ""):
            # Increment attempt counter
            conn.execute(
                "UPDATE email_verification_tokens SET attempts = attempts + 1 WHERE id = %s",
                (token_id,),
            )
            conn.commit()
            remaining = VERIFICATION_CODE_MAX_ATTEMPTS - attempts - 1
            if remaining <= 0:
                return False, "Too many failed attempts. Please request a new code."
            suffix = "s" if remaining != 1 else ""
            return False, f"Invalid code. {remaining} attempt{suffix} remaining."

        # Code is valid â€” mark as verified
        conn.execute(
            "UPDATE email_verification_tokens SET verified_at = %s WHERE id = %s",
            (now_iso, token_id),
        )
        conn.commit()

        logger.info("Verification code validated", extra={"user_id": user_id, "token_id": token_id})
        return True, None

    except Exception as e:
        conn.rollback()
        logger.exception("Failed to validate verification code")
        return False, f"Verification failed: {e}"
    finally:
        conn.close()


def mark_user_verified(user_id: str) -> None:
    """Set is_verified = TRUE on the users table."""
    conn = _get_conn()
    try:
        conn.execute(
            "UPDATE users SET is_verified = TRUE, updated_at = %s WHERE id = %s",
            (_utc_now_iso(), user_id),
        )
        conn.commit()
        logger.info("User marked as verified", extra={"user_id": user_id})
    except Exception:
        conn.rollback()
        logger.exception("Failed to mark user as verified")
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# User Limits
# ---------------------------------------------------------------------------

def get_user_limits(user_id: str, tenant_id: str) -> UserLimits:
    """
    Get effective limits for a user.
    
    User-specific limits override tenant defaults (NULL = use tenant default).
    """
    conn = _get_conn()
    try:
        # Get user limits
        cur = conn.execute(
            "SELECT * FROM user_limits WHERE user_id = %s",
            (user_id,),
        )
        user_row = cur.fetchone()
        
        # Get tenant limits (fallback)
        cur = conn.execute(
            "SELECT * FROM tenant_limits WHERE tenant_id = %s",
            (tenant_id,),
        )
        tenant_row = cur.fetchone()
        
        def _get_limit(field: str, default: Any = None) -> Any:
            # User override takes precedence
            if user_row and user_row.get(field) is not None:
                return user_row[field]
            # Fall back to tenant default
            if tenant_row and tenant_row.get(field) is not None:
                return tenant_row[field]
            return default
        
        return UserLimits(
            max_runs_per_day=_get_limit("max_runs_per_day"),
            max_domains_per_run=_get_limit("max_domains_per_run"),
            max_concurrent_runs=_get_limit("max_concurrent_runs", 2),
            max_verifications_per_day=_get_limit("max_verifications_per_day"),
            max_verifications_per_month=_get_limit("max_verifications_per_month"),
            max_exports_per_day=_get_limit("max_exports_per_day"),
            max_export_rows=_get_limit("max_export_rows", 10000),
            can_use_ai_extraction=_get_limit("can_use_ai_extraction", True),
            can_use_smtp_verify=_get_limit("can_use_smtp_verify", True),
            can_access_admin=bool(user_row.get("can_access_admin")) if user_row else False,
        )
    finally:
        conn.close()


def check_usage_limit(
    user_id: str,
    tenant_id: str,
    counter_type: str,
    period_type: str,
) -> tuple[int, int | None]:
    """
    Check current usage against limit.
    
    Returns (current_count, limit) where limit=None means unlimited.
    """
    limits = get_user_limits(user_id, tenant_id)
    
    # Map counter type to limit field
    limit_map = {
        ("runs", "daily"): limits.max_runs_per_day,
        ("verifications", "daily"): limits.max_verifications_per_day,
        ("verifications", "monthly"): limits.max_verifications_per_month,
        ("exports", "daily"): limits.max_exports_per_day,
    }
    
    limit = limit_map.get((counter_type, period_type))
    
    # Get current count
    now = _utc_now()
    if period_type == "daily":
        period_start = now.strftime("%Y-%m-%d")
    else:  # monthly
        period_start = now.strftime("%Y-%m")
    
    conn = _get_conn()
    try:
        cur = conn.execute(
            """
            SELECT count FROM usage_counters
            WHERE tenant_id = %s AND user_id = %s 
              AND counter_type = %s AND period_start = %s AND period_type = %s
            """,
            (tenant_id, user_id, counter_type, period_start, period_type),
        )
        row = cur.fetchone()
        current_count = row["count"] if row else 0
        
        return current_count, limit
    finally:
        conn.close()


def increment_usage_counter(
    user_id: str,
    tenant_id: str,
    counter_type: str,
    period_type: str,
    amount: int = 1,
) -> int:
    """
    Increment a usage counter.
    
    Returns new count.
    """
    now = _utc_now()
    if period_type == "daily":
        period_start = now.strftime("%Y-%m-%d")
    else:  # monthly
        period_start = now.strftime("%Y-%m")
    
    now_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    conn = _get_conn()
    try:
        # Upsert counter
        conn.execute(
            """
            INSERT INTO usage_counters (
                tenant_id, user_id, counter_type,
                period_start, period_type, count, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (tenant_id, user_id, counter_type, period_start, period_type)
            DO UPDATE SET count = usage_counters.count + %s, updated_at = %s
            """,
            (
                tenant_id, user_id, counter_type,
                period_start, period_type, amount, now_iso,
                amount, now_iso,
            ),
        )
        conn.commit()
        
        # Return new count
        cur = conn.execute(
            """
            SELECT count FROM usage_counters
            WHERE tenant_id = %s AND user_id = %s 
              AND counter_type = %s AND period_start = %s AND period_type = %s
            """,
            (tenant_id, user_id, counter_type, period_start, period_type),
        )
        row = cur.fetchone()
        return row["count"] if row else amount
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# High-Level Auth Operations
# ---------------------------------------------------------------------------

def authenticate(email: str, password: str) -> tuple[User | None, str | None]:
    """
    Authenticate a user by email and password.
    
    Returns (user, error_message).
    """
    user = get_user_by_email(email)
    if not user:
        # Don't reveal whether email exists
        return None, "Invalid email or password"
    
    # Check if account is locked
    is_locked, locked_until = check_account_locked(user.id)
    if is_locked:
        return None, f"Account locked. Try again after {locked_until}"
    
    # Verify password
    password_hash = get_user_password_hash(user.id)
    if not verify_password(password, password_hash):
        record_failed_login(user.id)
        return None, "Invalid email or password"
    
    # Check if account is active
    if not user.is_active:
        return None, "Account is disabled"
    
    # Success
    clear_failed_logins(user.id)
    update_last_login(user.id)
    
    return user, None
