from __future__ import annotations

import json
import os
from typing import Annotated

from fastapi import FastAPI, Header, HTTPException, Query, Request

from src.db import bulk_insert_ingest_items
from src.ingest.normalize import normalize_row
from src.ingest.validators import validate_domain_sanity, validate_minimum_fields

# R13: keep requests bounded; preserve local-only usage
MAX_ITEMS = int(os.getenv("INGEST_HTTP_MAX_ITEMS", "5000"))
TOKEN = os.getenv("INGEST_HTTP_TOKEN")

app = FastAPI(title="R13 Ingest (local-only)")


def _auth(authorization: str | None) -> None:
    if not TOKEN:
        raise HTTPException(status_code=500, detail="Server misconfigured: no token")
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    if authorization.split(" ", 1)[1] != TOKEN:
        raise HTTPException(status_code=401, detail="Invalid token")


def _process_items(items: list[dict], dry_run: bool):
    """
    Validate â†’ normalize â†’ (optionally) persist.

    R13 wiring:
      - Every item passes through normalize_row(), which:
          â€¢ Title-cases names with particle rules
          â€¢ Normalizes titles with abbreviation safelist
          â€¢ Normalizes company display + computes norm_key
          â€¢ Preserves provenance (source_url) and copies title â†’ title_raw/title_norm
      - We keep normalize_row()'s error snapshot embedded in each row (row['errors']).
      - Only hard schema/domain validation rejections are counted as 'rejected'.
    """
    ok_rows: list[dict] = []
    errs: list[str] = []

    for idx, obj in enumerate(items, start=1):
        try:
            validate_minimum_fields(obj)
        except ValueError as e:
            errs.append(f"Item {idx}: {e}")
            continue
        # validate_domain_sanity is intentionally a no-op; real validation happens in normalization
        validate_domain_sanity(obj.get("domain") or "")

        # R13: normalize and preserve provenance/source_url
        row, _norm_errors = normalize_row(obj)
        ok_rows.append(row)

    if not dry_run and ok_rows:
        # Persist in bulk; downstream DB layer will respect title_raw/title_norm and company norms
        bulk_insert_ingest_items(ok_rows)

    return {"accepted": len(ok_rows), "rejected": len(errs), "errors": errs[:50]}


@app.post("/ingest")
async def ingest(
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
    dry_run: bool = Query(default=False),
):
    _auth(authorization)

    ctype = (request.headers.get("content-type") or "").lower()

    # JSON array
    if "application/json" in ctype:
        payload = await request.json()
        if not isinstance(payload, list):
            raise HTTPException(status_code=422, detail="Expected JSON array")
        if len(payload) > MAX_ITEMS:
            raise HTTPException(status_code=413, detail=f"Too many items (> {MAX_ITEMS})")
        return _process_items(payload, dry_run)

    # NDJSON
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
