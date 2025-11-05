from __future__ import annotations

import os

from fastapi import FastAPI, Request

from src.api.middleware.body_limit import BodySizeLimitMiddleware

# Configurable via env; default 5 MiB
BODY_LIMIT_BYTES = int(os.getenv("BODY_LIMIT_BYTES", str(5 * 1024 * 1024)))

app = FastAPI(title="Email Scraper API")

# Register early so limits apply to all routes
app.add_middleware(BodySizeLimitMiddleware, max_bytes=BODY_LIMIT_BYTES)


@app.get("/health")
async def health():
    return {"ok": True}


# Example endpoint that reads the raw body (works for CSV/JSONL uploads)
@app.post("/ingest")
async def ingest(request: Request):
    data = await request.body()  # middleware will cap size before this
    # TODO: pass `data` to your existing ingest pipeline if/when you wire it up
    return {"ok": True, "received_bytes": len(data)}
