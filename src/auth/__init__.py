# src/auth/__init__.py
"""Authentication module."""

from src.auth.core import (
    SESSION_COOKIE_HTTPONLY,
    SESSION_COOKIE_NAME,
    SESSION_COOKIE_SAMESITE,
    SESSION_COOKIE_SECURE,
    Session,
    User,
    UserLimits,
    authenticate,
    check_usage_limit,
    create_session,
    create_user,
    delete_session,
    get_session,
    get_user_limits,
    increment_usage_counter,
)

__all__ = [
    "User",
    "Session",
    "UserLimits",
    "authenticate",
    "create_user",
    "create_session",
    "get_session",
    "delete_session",
    "get_user_limits",
    "check_usage_limit",
    "increment_usage_counter",
    "SESSION_COOKIE_NAME",
    "SESSION_COOKIE_SECURE",
    "SESSION_COOKIE_HTTPONLY",
    "SESSION_COOKIE_SAMESITE",
]
