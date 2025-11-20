"""
O07 — Third-party verifier fallback

Provides a small, optional wrapper around an external verification API.

Public API:

    FallbackStatus = Literal["valid", "invalid", "catch_all", "unknown"]

    @dataclass
    class FallbackResult:
        email: str
        status: FallbackStatus
        raw: dict[str, Any]

    def verify_with_fallback(email: str) -> FallbackResult

Behavior:
  - If THIRD_PARTY_VERIFY_ENABLED is False OR URL/API key are missing:
      -> returns status="unknown" and does NOT perform any network I/O.
  - Otherwise, performs a single HTTP call to the configured provider,
    parses its status, maps it into our FallbackStatus, and returns the
    full JSON payload in `raw` for later persistence/inspection.

This module is intentionally conservative: failures to call the provider
(network errors, bad JSON, etc.) never raise; they simply return
status="unknown" with an error note in `raw`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from src.config import (
    THIRD_PARTY_VERIFY_API_KEY,
    THIRD_PARTY_VERIFY_ENABLED,
    THIRD_PARTY_VERIFY_URL,
)

try:  # pragma: no cover - exercised via tests with monkeypatch
    import httpx  # type: ignore
except Exception:  # pragma: no cover
    httpx = None  # type: ignore


FallbackStatus = Literal["valid", "invalid", "catch_all", "unknown"]


@dataclass
class FallbackResult:
    email: str
    status: FallbackStatus
    raw: dict[str, Any]


def _map_provider_status(raw_status: str | None) -> FallbackStatus:
    """
    Map provider-specific status strings to our internal FallbackStatus.

    This is intentionally permissive; unknown or missing values collapse
    to "unknown".
    """
    if not raw_status:
        return "unknown"

    s = raw_status.strip().lower()

    if s in {"valid", "deliverable", "ok", "success"}:
        return "valid"
    if s in {"invalid", "undeliverable", "bad", "hard_bounce"}:
        return "invalid"
    if s in {"catch_all", "catchall"}:
        return "catch_all"

    return "unknown"


def _normalize_email(email: str) -> str:
    return (email or "").strip()


def verify_with_fallback(email: str) -> FallbackResult:
    """
    Call the configured third-party verifier for `email`, if enabled.

    Returns:
      FallbackResult(email, status, raw_json)

    No exceptions are propagated to callers; failures are represented as
    status="unknown" with an error note inside `raw`.
    """
    e = _normalize_email(email)
    if not e:
        return FallbackResult(
            email=email,
            status="unknown",
            raw={"skipped": True, "reason": "empty_email"},
        )

    # Guard: feature toggled off or misconfigured -> skip network.
    if (
        not THIRD_PARTY_VERIFY_ENABLED
        or not THIRD_PARTY_VERIFY_URL
        or not THIRD_PARTY_VERIFY_API_KEY
    ):
        return FallbackResult(
            email=e,
            status="unknown",
            raw={
                "skipped": True,
                "reason": "fallback_disabled_or_unconfigured",
            },
        )

    # Guard: httpx not installed (defensive).
    if httpx is None:  # pragma: no cover - exercised only if dependency missing
        return FallbackResult(
            email=e,
            status="unknown",
            raw={
                "skipped": True,
                "reason": "httpx_not_available",
            },
        )

    # Perform a single HTTP request to the provider.
    try:
        headers = {
            "Authorization": f"Bearer {THIRD_PARTY_VERIFY_API_KEY}",
            "Accept": "application/json",
        }
        payload = {"email": e}

        # Keep this simple; callers do not supply custom timeouts.
        with httpx.Client(timeout=10.0) as client:
            resp = client.post(THIRD_PARTY_VERIFY_URL, json=payload, headers=headers)
        resp.raise_for_status()

        data = resp.json()
        if not isinstance(data, dict):
            # Ensure `raw` is a dict even if the provider returns a list/str/etc.
            data = {"raw": data}

        raw_status = data.get("status") or data.get("result") or data.get("state")
        status = _map_provider_status(str(raw_status) if raw_status is not None else None)

        return FallbackResult(
            email=e,
            status=status,
            raw=data,
        )

    except Exception as exc:
        # Swallow all errors; represent as unknown with error note.
        return FallbackResult(
            email=e,
            status="unknown",
            raw={
                "error": f"{type(exc).__name__}: {exc}",
            },
        )


__all__ = [
    "FallbackStatus",
    "FallbackResult",
    "verify_with_fallback",
]
