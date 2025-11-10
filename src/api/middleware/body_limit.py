from __future__ import annotations

from starlette.responses import PlainTextResponse
from starlette.types import ASGIApp, Receive, Scope, Send


class BodySizeLimitMiddleware:
    """
    Enforces a maximum total HTTP request body size.

    - Fast path: if Content-Length exists and exceeds the limit, respond 413 immediately.
    - Streaming path: wrap `receive()` and sum chunk lengths; if the running total
      exceeds the limit, respond 413 and simulate a client disconnect.
    """

    def __init__(self, app: ASGIApp, max_bytes: int, header_check: bool = True) -> None:
        self.app = app
        self.max_bytes = int(max_bytes)
        self.header_check = bool(header_check)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        if self.header_check:
            try:
                headers = {
                    k.decode("latin1").lower(): v.decode("latin1")
                    for k, v in scope.get("headers", [])
                }
                cl = headers.get("content-length")
                if cl is not None and cl.isdigit() and int(cl) > self.max_bytes:
                    await self._send_413(scope, receive, send, int(cl))
                    return
            except Exception:
                pass

        total = 0
        responded = False

        async def limited_receive() -> dict:
            nonlocal total, responded
            if responded:
                return {"type": "http.disconnect"}

            message = await receive()

            if message["type"] == "http.request":
                body = message.get("body", b"") or b""
                total += len(body)
                if total > self.max_bytes:
                    responded = True
                    await self._send_413(scope, receive, send, total)
                    return {"type": "http.disconnect"}

            return message

        await self.app(scope, limited_receive, send)

    async def _send_413(self, scope: Scope, receive: Receive, send: Send, total_bytes: int) -> None:
        msg = (
            f"Payload too large: {total_bytes} bytes > limit {self.max_bytes} bytes. "
            "Please upload a smaller file or compress it."
        )
        resp = PlainTextResponse(msg, status_code=413)
        await resp(scope, receive, send)
