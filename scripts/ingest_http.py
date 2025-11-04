# scripts/ingest_http.py
from __future__ import annotations

import asyncio
import hmac
import logging
import os
import time
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse

from src.ingest import ingest_row  # reuse your CSV/JSONL pipeline

# -------------------- config --------------------

MAX_BATCH = 5000
try:
    INGEST_TOKEN = os.environ["INGEST_TOKEN"].strip()
    if not INGEST_TOKEN:
        raise KeyError("INGEST_TOKEN empty")
except KeyError as e:
    raise RuntimeError(
        "INGEST_TOKEN must be set in the environment before starting the server."
    ) from e

log = logging.getLogger(__name__)
# Helpful when comparing server/client windows locally (prints on startup)
log.info("INGEST_TOKEN prefix: %s...", INGEST_TOKEN[:8])

# -------------------- rate limiting (token bucket) --------------------


class TokenBucket:
    """
    Simple in-process token bucket. Good enough for local R07 use.
    rate: tokens per second; capacity: max burst size.
    """

    def __init__(self, rate: float, capacity: int) -> None:
        self.rate = float(rate)
        self.capacity = int(capacity)
        self.tokens = float(capacity)
        self.updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def allow(self) -> bool:
        async with self._lock:
            now = time.monotonic()
            delta = now - self.updated
            self.updated = now
            # Refill
            self.tokens = min(self.capacity, self.tokens + delta * self.rate)
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return True
            return False


bucket = TokenBucket(rate=5.0, capacity=5)


async def rate_limit_dependency() -> None:
    ok = await bucket.allow()
    if not ok:
        # “Retry-After: 1” is a hint; real wait depends on traffic
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="rate limit exceeded",
            headers={"Retry-After": "1"},
        )


# -------------------- auth --------------------


def _get_bearer_token(auth_header: str | None) -> str | None:
    """
    Parse "Authorization: Bearer <token>" in a tolerant way:
    - Case-insensitive scheme
    - Trims surrounding spaces
    - Returns the raw token string or None
    """
    if not auth_header:
        return None
    parts = auth_header.strip().split(None, 1)
    if len(parts) != 2:
        return None
    scheme, value = parts[0], parts[1].strip()
    if scheme.lower() != "bearer" or not value:
        return None
    return value


def bearer_auth(request: Request) -> str:
    supplied = _get_bearer_token(request.headers.get("authorization"))
    expected = INGEST_TOKEN
    if not supplied or not hmac.compare_digest(supplied, expected):
        # Include WWW-Authenticate so callers know it’s bearer-protected
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid or missing bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return supplied  # keeps dependency semantics


# -------------------- app & routes --------------------

app = FastAPI(
    title="Email Scraper – Local Ingest API",
    docs_url=None,
    redoc_url=None,
)


@app.post("/ingest")
async def ingest_endpoint(
    payload: list[dict[str, Any]],
    _auth: str = Depends(bearer_auth),
    _rl: None = Depends(rate_limit_dependency),
):
    """
    Body: JSON array of lead objects (max 5,000).
    Returns: {accepted, rejected, rejects: [{index, error}]}
    """
    if not isinstance(payload, list):
        raise HTTPException(status_code=400, detail="body must be a JSON array")

    if len(payload) > MAX_BATCH:
        raise HTTPException(
            status_code=400,
            detail=f"batch too large; max {MAX_BATCH} items",
        )

    accepted = 0
    rejects: list[dict[str, Any]] = []

    for idx, row in enumerate(payload):
        try:
            if not isinstance(row, dict):
                raise ValueError("each item must be an object")
            ingest_row(row)  # does normalization + persistence
            accepted += 1
        except Exception as e:  # noqa: BLE001 – we want to catch and report per-row
            rejects.append({"index": idx, "error": f"{type(e).__name__}: {e}"})

    return JSONResponse(
        {
            "accepted": accepted,
            "rejected": len(rejects),
            "rejects": rejects,
        }
    )
