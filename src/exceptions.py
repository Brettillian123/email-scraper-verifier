# src/exceptions.py
"""
Shared exception classes used across the codebase.

This module centralizes common exceptions to avoid duplication
and ensure consistent error handling.
"""

from __future__ import annotations


class TemporarySMTPError(Exception):
    """
    Raised when an SMTP operation fails with a temporary/retriable error.

    Examples:
        - 4xx SMTP response codes
        - Greylisting responses (450)
        - Rate limiting
        - Temporary connection failures
    """

    pass


class PermanentSMTPError(Exception):
    """
    Raised when an SMTP operation fails with a permanent/non-retriable error.

    Examples:
        - 5xx SMTP response codes
        - User unknown (550)
        - Mailbox unavailable
        - Domain does not exist
    """

    pass


__all__ = [
    "TemporarySMTPError",
    "PermanentSMTPError",
]
