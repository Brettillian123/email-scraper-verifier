# src/api/middleware/activity_logging.py
"""
Activity logging middleware for FastAPI.

Automatically logs user activity for key endpoints:
  - POST /runs -> run_created
  - GET /runs/{id}/export -> export
  - GET /leads/search -> search
  - POST /verify -> verify

Usage:
    from src.api.middleware.activity_logging import ActivityLoggingMiddleware

    app.add_middleware(ActivityLoggingMiddleware)
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

log = logging.getLogger(__name__)


# Endpoints to track and their action names
TRACKED_ENDPOINTS = {
    ("POST", "/runs"): "run_created",
    ("POST", "/api/v2/runs"): "run_created",
    ("GET", "/runs/{run_id}/export"): "export",
    ("GET", "/api/v2/runs/{run_id}/export"): "export",
    ("GET", "/leads/search"): "search",
    ("GET", "/api/v2/leads/search"): "search",
    ("POST", "/verify"): "verify",
    ("POST", "/api/v2/verify"): "verify",
}

# Path patterns to match (for dynamic routes)
PATH_PATTERNS = [
    (r"^/runs/[^/]+/export$", "export", "run"),
    (r"^/api/v2/runs/[^/]+/export$", "export", "run"),
    (r"^/runs/[^/]+/results$", "view_results", "run"),
    (r"^/api/v2/runs/[^/]+/results$", "view_results", "run"),
    (r"^/runs/[^/]+/metrics$", "view_metrics", "run"),
    (r"^/api/v2/runs/[^/]+/metrics$", "view_metrics", "run"),
]


class ActivityLoggingMiddleware(BaseHTTPMiddleware):
    """
    Middleware that logs user activity for tracked endpoints.

    Extracts user info from request headers or auth context and logs
    to the user_activity table.
    """

    def __init__(self, app, *, exclude_paths: list[str] | None = None):
        super().__init__(app)
        self.exclude_paths = set(exclude_paths or [])
        self.exclude_paths.update({"/health", "/healthz", "/metrics", "/favicon.ico"})

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Skip excluded paths
        path = request.url.path
        if path in self.exclude_paths:
            return await call_next(request)

        # Skip non-tracked methods
        method = request.method
        if method in {"OPTIONS", "HEAD"}:
            return await call_next(request)

        # Track timing
        start_time = time.time()

        # Call the actual endpoint
        response = await call_next(request)

        # Log activity (async, non-blocking)
        try:
            await self._log_activity(request, response, start_time)
        except Exception:
            pass

        return response

    async def _log_activity(
        self,
        request: Request,
        response: Response,
        start_time: float,
    ) -> None:
        """Log activity for the request."""
        import re

        path = request.url.path
        method = request.method

        # Determine action from path
        action = None
        resource_type = None
        resource_id = None

        # Check exact matches
        key = (method, path)
        if key in TRACKED_ENDPOINTS:
            action = TRACKED_ENDPOINTS[key]

        # Check pattern matches
        if not action:
            for pattern, act, res_type in PATH_PATTERNS:
                if re.match(pattern, path):
                    action = act
                    resource_type = res_type
                    # Extract resource ID from path
                    parts = path.split("/")
                    if len(parts) >= 3:
                        resource_id = parts[2]  # e.g., /runs/{run_id}/...
                    break

        # Skip if no action matched
        if not action:
            return

        # Only log successful requests
        if response.status_code >= 400:
            return

        # Extract user info
        tenant_id = self._get_header(request, "x-tenant-id") or "dev"
        user_id = self._get_header(request, "x-user-id") or "anonymous"

        # Try to get from auth context if available
        try:
            if hasattr(request.state, "auth"):
                auth = request.state.auth
                tenant_id = getattr(auth, "tenant_id", tenant_id)
                user_id = getattr(auth, "user_id", user_id)
        except Exception:
            pass

        # Extract resource info from response body for certain actions
        if action == "run_created":
            try:
                body = getattr(response, "_body", None)
                if body:
                    data = json.loads(body)
                    resource_id = data.get("run_id")
                    resource_type = "run"
            except Exception:
                pass

        # Build metadata
        elapsed_ms = int((time.time() - start_time) * 1000)
        metadata = {
            "method": method,
            "path": path,
            "status_code": response.status_code,
            "elapsed_ms": elapsed_ms,
        }

        # Add query params for search
        if action == "search":
            metadata["query"] = request.query_params.get("q")

        # Log the activity
        try:
            from src.admin.user_activity import log_user_activity

            log_user_activity(
                tenant_id=tenant_id,
                user_id=user_id,
                action=action,
                resource_type=resource_type,
                resource_id=resource_id,
                ip_address=self._get_client_ip(request),
                user_agent=self._get_header(request, "user-agent"),
                metadata=metadata,
            )
        except ImportError:
            log.debug("user_activity module not available")
        except Exception:
            log.debug("Failed to log user activity", exc_info=True)

    def _get_header(self, request: Request, name: str) -> str | None:
        """Get header value, case-insensitive."""
        return request.headers.get(name)

    def _get_client_ip(self, request: Request) -> str | None:
        """Get client IP, checking X-Forwarded-For first."""
        xff = self._get_header(request, "x-forwarded-for")
        if xff:
            return xff.split(",")[0].strip()
        if request.client:
            return request.client.host
        return None


# Simpler function-based approach for manual logging
def log_endpoint_activity(
    request: Request,
    action: str,
    resource_type: str | None = None,
    resource_id: str | None = None,
    metadata: dict | None = None,
) -> None:
    """
    Manually log activity for an endpoint.

    Use this in endpoint functions when you need more control:

        @app.post("/custom-action")
        async def custom_action(request: Request):
            # ... do work ...
            log_endpoint_activity(
                request,
                action="custom_action",
                resource_type="widget",
                resource_id=widget_id,
                metadata={"count": 42},
            )
    """
    try:
        from src.admin.user_activity import log_user_activity

        # Extract user info
        tenant_id = request.headers.get("x-tenant-id") or "dev"
        user_id = request.headers.get("x-user-id") or "anonymous"

        try:
            if hasattr(request.state, "auth"):
                auth = request.state.auth
                tenant_id = getattr(auth, "tenant_id", tenant_id)
                user_id = getattr(auth, "user_id", user_id)
        except Exception:
            pass

        ip = None
        xff = request.headers.get("x-forwarded-for")
        if xff:
            ip = xff.split(",")[0].strip()
        elif request.client:
            ip = request.client.host

        log_user_activity(
            tenant_id=tenant_id,
            user_id=user_id,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            ip_address=ip,
            user_agent=request.headers.get("user-agent"),
            metadata=metadata,
        )
    except Exception:
        pass


__all__ = [
    "ActivityLoggingMiddleware",
    "log_endpoint_activity",
]
