from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from pathlib import Path

# --- make "src" importable when run from scripts/ ---
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.db import bulk_insert_ingest_items  # noqa: E402
from src.ingest.normalize import normalize_row  # noqa: E402
from src.ingest.validators import (  # noqa: E402
    MAX_ROWS_DEFAULT,
    enforce_row_cap,
    validate_domain_sanity,
    validate_header_csv,
    validate_minimum_fields,
)


def _read_csv(path: Path, delim: str, strict: bool, max_rows: int):
    count = 0
    ok_rows, errs = [], []

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=delim)
        ok, msg = validate_header_csv(reader.fieldnames or [])
        if not ok:
            return [], [f"Header error: {msg}"]
        for i, row in enumerate(reader, start=2):  # data starts at line 2
            count += 1
            if count % 10000 == 0:
                enforce_row_cap(count, max_rows)
            good, emsg = validate_minimum_fields(row)
            if not good:
                if strict:
                    return [], [f"Line {i}: {emsg}"]
                errs.append(f"Line {i}: {emsg}")
                continue
            if not validate_domain_sanity(row.get("domain") or ""):
                if strict:
                    return [], [f"Line {i}: invalid domain"]
                errs.append(f"Line {i}: invalid domain")
                continue
            dbrow, _ = normalize_row(row)
            ok_rows.append(dbrow)

    enforce_row_cap(count, max_rows)
    return ok_rows, errs


def _read_jsonl(path: Path, strict: bool, max_rows: int):
    count = 0
    ok_rows, errs = [], []
    with path.open("r", encoding="utf-8", newline="") as f:
        for i, line in enumerate(f, start=1):
            s = line.strip()
            if not s:
                continue
            count += 1
            if count % 10000 == 0:
                enforce_row_cap(count, max_rows)
            try:
                obj = json.loads(s)
            except json.JSONDecodeError as e:
                if strict:
                    return [], [f"Line {i}: invalid JSON: {e}"]
                errs.append(f"Line {i}: invalid JSON: {e}")
                continue
            good, emsg = validate_minimum_fields(obj)
            if not good:
                if strict:
                    return [], [f"Line {i}: {emsg}"]
                errs.append(f"Line {i}: {emsg}")
                continue
            if not validate_domain_sanity(obj.get("domain") or ""):
                if strict:
                    return [], [f"Line {i}: invalid domain"]
                errs.append(f"Line {i}: invalid domain")
                continue
            dbrow, _ = normalize_row(obj)
            ok_rows.append(dbrow)

    enforce_row_cap(count, max_rows)
    return ok_rows, errs


def main():
    ap = argparse.ArgumentParser(description="R07 ingest (CSV/TSV/JSONL)")
    ap.add_argument("path", help="Input file (.csv | .tsv | .jsonl)")
    ap.add_argument("--format", choices=["csv", "tsv", "jsonl"], help="Override format detection")
    ap.add_argument(
        "--max-rows", type=int, default=int(os.getenv("INGEST_MAX_ROWS", MAX_ROWS_DEFAULT))
    )
    ap.add_argument("--strict", action="store_true", help="Fail fast on first error")
    ap.add_argument("--dry-run", action="store_true", help="Validate only; do not write to DB")
    args = ap.parse_args()

    p = Path(args.path)
    if not p.exists():
        print(f"File not found: {p}", file=sys.stderr)
        sys.exit(2)

    fmt = args.format or p.suffix.lower().lstrip(".")
    if fmt not in {"csv", "tsv", "jsonl"}:
        print("Cannot detect format (use --format).", file=sys.stderr)
        sys.exit(2)

    if fmt in {"csv", "tsv"}:
        delim = "," if fmt == "csv" else "\t"
        ok_rows, errs = _read_csv(p, delim, args.strict, args.max_rows)
    else:
        ok_rows, errs = _read_jsonl(p, args.strict, args.max_rows)

    if errs:
        print("Validation errors:", file=sys.stderr)
        for e in errs[:50]:
            print(f"  - {e}", file=sys.stderr)
        if len(errs) > 50:
            print(f"  ... and {len(errs) - 50} more", file=sys.stderr)
        sys.exit(1)

    if args.dry_run:
        print(f"Validated {len(ok_rows)} rows. (dry-run; no DB writes)")
        sys.exit(0)

    inserted = bulk_insert_ingest_items(ok_rows) if ok_rows else 0
    print(f"Inserted {inserted} rows into ingest_items.")
    sys.exit(0)


if __name__ == "__main__":
    main()
