from __future__ import annotations

import json
import os
from typing import Annotated

from fastapi import FastAPI, Header, HTTPException, Query, Request

from src.db import bulk_insert_ingest_items
from src.ingest.normalize import normalize_row
from src.ingest.validators import validate_domain_sanity, validate_minimum_fields

MAX_ITEMS = int(os.getenv("INGEST_HTTP_MAX_ITEMS", "5000"))
TOKEN = os.getenv("INGEST_HTTP_TOKEN")

app = FastAPI(title="R07 Ingest (local-only)")


def _auth(authorization: str | None):
    if not TOKEN:
        raise HTTPException(status_code=500, detail="Server misconfigured: no token")
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    if authorization.split(" ", 1)[1] != TOKEN:
        raise HTTPException(status_code=401, detail="Invalid token")


def _process_items(items: list[dict], dry_run: bool):
    ok_rows, errs = [], []
    for idx, obj in enumerate(items, start=1):
        good, emsg = validate_minimum_fields(obj)
        if not good:
            errs.append(f"Item {idx}: {emsg}")
            continue
        if not validate_domain_sanity(obj.get("domain") or ""):
            errs.append(f"Item {idx}: invalid domain")
            continue
        row, _ = normalize_row(obj)
        ok_rows.append(row)
    if not dry_run and ok_rows:
        bulk_insert_ingest_items(ok_rows)
    return {"accepted": len(ok_rows), "rejected": len(errs), "errors": errs[:50]}


@app.post("/ingest")
async def ingest(
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
    dry_run: bool = Query(default=False),
):
    _auth(authorization)

    ctype = request.headers.get("content-type", "")
    if "application/json" in ctype:
        payload = await request.json()
        if not isinstance(payload, list):
            raise HTTPException(status_code=422, detail="Expected JSON array")
        if len(payload) > MAX_ITEMS:
            raise HTTPException(status_code=413, detail=f"Too many items (> {MAX_ITEMS})")
        return _process_items(payload, dry_run)

    if "application/x-ndjson" in ctype or "application/ndjson" in ctype:
        raw = (await request.body()).decode("utf-8")
        lines = [x for x in (ln.strip() for ln in raw.splitlines()) if x]
        if len(lines) > MAX_ITEMS:
            raise HTTPException(status_code=413, detail=f"Too many items (> {MAX_ITEMS})")
        items = []
        for i, ln in enumerate(lines, start=1):
            try:
                items.append(json.loads(ln))
            except json.JSONDecodeError as e:
                raise HTTPException(status_code=422, detail=f"Line {i}: {e}") from e
        return _process_items(items, dry_run)

    raise HTTPException(status_code=415, detail="Unsupported Content-Type")
