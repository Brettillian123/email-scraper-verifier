from __future__ import annotations

"""
O27 — CLI to run AI-assisted people extraction for a single source row.

This is a lightweight debug/backfill tool that lets you point the AI extractor
at one HTML page (sources.id) and inspect the resulting Candidate objects
without running the full auto-discovery pipeline.

Usage (from repo root, venv active):

  (.venv) python scripts/extract_candidates_ai.py \
      --db data/dev.db \
      --source-id 123

It will:

  - Load the HTML + URL + company domain for sources.id = 123.
  - Run src.extract.ai_candidates.extract_ai_candidates().
  - Print the resulting candidates to stdout.

AI_PEOPLE_ENABLED must be enabled via env, and your OpenAI API configuration
must be valid for the underlying client.
"""

import argparse
import os
import sqlite3
from pathlib import Path
from typing import Any

from src.extract.ai_candidates import AI_PEOPLE_ENABLED, extract_ai_candidates


def _decode_html(blob: Any) -> str:
    """Best-effort decode for HTML BLOB/text stored in SQLite."""
    if blob is None:
        return ""
    if isinstance(blob, (bytes, bytearray)):
        for enc in ("utf-8", "latin-1"):
            try:
                return blob.decode(enc)
            except Exception:
                continue
        return blob.decode("utf-8", errors="ignore")
    return str(blob)


def _load_source_row(db_path: str, source_id: int) -> sqlite3.Row | None:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        row = con.execute(
            """
            SELECT
              s.id,
              s.company_id,
              s.source_url,
              s.html,
              COALESCE(c.official_domain, c.domain) AS domain
            FROM sources AS s
            LEFT JOIN companies AS c
              ON c.id = s.company_id
            WHERE s.id = ?
            """,
            (source_id,),
        ).fetchone()
        return row
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run AI people extraction for a single source row."
    )
    parser.add_argument(
        "--db",
        help="Path to SQLite DB (defaults to $DATABASE_PATH or data/dev.db)",
    )
    parser.add_argument(
        "--source-id",
        type=int,
        required=True,
        help="sources.id to process",
    )
    args = parser.parse_args()

    db_path_raw = args.db or os.getenv("DATABASE_PATH") or "data/dev.db"
    db_path = str(Path(db_path_raw).resolve())

    if not AI_PEOPLE_ENABLED:
        print(
            "WARNING: AI_PEOPLE_ENABLED is not set to '1'; "
            "extract_ai_candidates() will return no results.\n"
            "Set AI_PEOPLE_ENABLED=1 and ensure your OpenAI API key is configured.",
        )

    row = _load_source_row(db_path, args.source_id)
    if not row:
        print(f"source id={args.source_id} not found in {db_path}")
        return

    html_str = _decode_html(row["html"])
    source_url = row["source_url"] or ""
    domain = row["domain"] or ""

    print(
        f"Running AI extraction for source_id={row['id']} "
        f"(company_id={row['company_id']}, domain={domain!r})",
    )
    print(f"URL: {source_url}\n")

    candidates = extract_ai_candidates(html_str, source_url, domain or None)

    if not candidates:
        print("No AI candidates returned.")
        return

    for idx, cand in enumerate(candidates, start=1):
        name_parts = [p for p in (cand.first_name, cand.last_name) if p]
        name_line = " ".join(name_parts) if name_parts else "(no parsed first/last)"
        raw_name = cand.raw_name or "(no raw_name)"
        email = cand.email or "(no email)"

        print(f"{idx}. {name_line}")
        print(f"    raw_name: {raw_name}")
        print(f"    email   : {email}")
        print(f"    source  : {cand.source_url}")
        print()

    print(f"Total AI candidates: {len(candidates)}")


if __name__ == "__main__":
    main()
