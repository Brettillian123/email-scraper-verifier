# src/utils.py
"""
Shared utility functions used across the codebase.

This module centralizes common helpers to avoid duplication.
"""
from __future__ import annotations

from datetime import UTC, datetime


def utc_now_iso() -> str:
    """
    Return the current UTC time as an ISO 8601 string with 'Z' suffix.
    
    Example: "2025-01-15T14:30:00Z"
    """
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_now_iso_z(dt: datetime | None = None) -> str:
    """
    Return a UTC ISO 8601 string with 'Z' suffix.
    
    If dt is provided, converts it to UTC first.
    If dt is None, uses current UTC time.
    
    Example: "2025-01-15T14:30:00Z"
    """
    d = dt or datetime.now(UTC)
    if d.tzinfo is None:
        d = d.replace(tzinfo=UTC)
    else:
        d = d.astimezone(UTC)
    return d.strftime("%Y-%m-%dT%H:%M:%SZ")


def is_postgres_url(url: str) -> bool:
    """
    Check if a database URL is a PostgreSQL connection string.
    """
    u = (url or "").strip().lower()
    return u.startswith("postgres://") or u.startswith("postgresql://")


def is_sqlite_url(url: str) -> bool:
    """
    Check if a database URL is a SQLite connection string.
    """
    u = (url or "").strip().lower()
    return u.startswith("sqlite:///")


__all__ = [
    "utc_now_iso",
    "utc_now_iso_z",
    "is_postgres_url",
    "is_sqlite_url",
]
