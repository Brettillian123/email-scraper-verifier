from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

# --- make "src" importable when run from scripts/ ---
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.normalize import normalize_row  # noqa: E402

from src.db import bulk_insert_ingest_items  # noqa: E402
from src.ingest.rejects import (  # noqa: E402
    current_rejects_file,
    log_reject,
)
from src.ingest.validators import (  # noqa: E402
    MAX_ROWS_DEFAULT,
    TooManyRowsError,
    enforce_row_cap,
    validate_domain_sanity,
    validate_header_csv,
    validate_minimum_fields,
)


def build_parser() -> argparse.ArgumentParser:
    """
    CLI for ingestion with a hard cap on input size.
    """
    p = argparse.ArgumentParser(description="R07 ingest (CSV/TSV/JSONL)")
    p.add_argument("path", help="CSV or JSONL to ingest")
    p.add_argument(
        "--max-rows",
        type=int,
        default=MAX_ROWS_DEFAULT,
        help=f"Hard cap on rows (default: {MAX_ROWS_DEFAULT})",
    )
    p.add_argument(
        "--format",
        choices=["csv", "tsv", "jsonl"],
        help="Override format detection",
    )
    p.add_argument(
        "--strict",
        action="store_true",
        help="Fail fast on first error (TSV legacy path)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate only; do not write to DB",
    )
    return p


# ---------------- CSV (BOM-safe) with per-row rejections ----------------
def iter_csv_rows(path: str, max_rows: int | None) -> list[dict[str, str]]:
    """
    Read CSV with UTF-8 BOM handling. Invalid rows are logged to rejects with line numbers.
    Enforces a strict row cap using enforce_row_cap to fail early on oversized inputs.
    """
    rows: list[dict[str, str]] = []
    with open(path, newline="", encoding="utf-8-sig") as f:  # BOM-safe
        rdr = csv.DictReader(f)
        # Header must exist
        validate_header_csv(rdr.fieldnames)

        for line_no, row in enumerate(rdr, start=2):  # header is line 1
            try:
                validate_minimum_fields(row)
                dom = (row.get("domain") or "").strip()
                validate_domain_sanity(dom)

                rows.append(row)
                # Fail early if we exceed the user-specified cap
                enforce_row_cap(len(rows), max_rows or MAX_ROWS_DEFAULT)
            except Exception as e:
                log_reject(line_no, str(e), row)
                continue

    print(f"Rejects file (if any): {current_rejects_file()}")
    return rows


# ---------------- JSONL (per-row rejects, mirrored from CSV) ----------------
def iter_jsonl_rows(path: str, max_rows: int | None) -> list[dict[str, str]]:
    """
    Read JSONL one object per line. Invalid rows are logged to rejects with line numbers.
    Enforces a strict row cap using enforce_row_cap to fail early on oversized inputs.
    """
    rows: list[dict[str, str]] = []
    with open(path, encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):  # keep 1-based line numbers
            s = line.strip()
            if not s:
                continue

            try:
                obj = json.loads(s)
            except json.JSONDecodeError as e:
                log_reject(line_no, f"json: {e}", {"_raw": s})
                continue

            try:
                validate_minimum_fields(obj)
                dom = (obj.get("domain") or "").strip()
                validate_domain_sanity(dom)

                rows.append(obj)
                enforce_row_cap(len(rows), max_rows or MAX_ROWS_DEFAULT)
            except Exception as e:
                log_reject(line_no, str(e), obj)
                continue

    print(f"Rejects file (if any): {current_rejects_file()}")
    return rows


# ---------------- TSV reader (legacy delimiter-based path) ----------------
def _read_delim(path: Path, delim: str, strict: bool, max_rows: int | None):
    """
    Legacy TSV/CSV path that performs validation during iteration.
    Maintained for compatibility; mirrors the row-cap guardrail.
    """
    count = 0
    ok_rows: list[dict[str, object]] = []
    errs: list[str] = []

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=delim)
        try:
            validate_header_csv(reader.fieldnames)
        except ValueError as e:
            return [], [f"Header error: {e}"]

        for i, row in enumerate(reader, start=2):  # data starts at line 2
            count += 1
            try:
                enforce_row_cap(count, max_rows or MAX_ROWS_DEFAULT)
            except TooManyRowsError:
                # Propagate to caller for unified handling
                raise

            try:
                validate_minimum_fields(row)
                validate_domain_sanity(row.get("domain"))
            except Exception as e:
                if strict:
                    return [], [f"Line {i}: {e}"]
                errs.append(f"Line {i}: {e}")
                continue

            dbrow, _ = normalize_row(row)
            ok_rows.append(dbrow)

    return ok_rows, errs


def _normalize_and_fill(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    """
    Run normalize_row and ensure required fields for DB writes (e.g., role).
    """
    normalized = [normalize_row(r)[0] for r in rows]
    for r in normalized:
        # Safety net: ensure role is present and non-empty for NOT NULL constraint.
        if "role" not in r or r["role"] is None or str(r["role"]).strip() == "":
            r["role"] = "other"
    return normalized


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    p = Path(args.path)
    if not p.exists():
        print(f"File not found: {p}", file=sys.stderr)
        sys.exit(2)

    fmt = args.format or p.suffix.lower().lstrip(".")
    if fmt not in {"csv", "tsv", "jsonl"}:
        print("Cannot detect format (use --format).", file=sys.stderr)
        sys.exit(2)

    errs: list[str] = []
    ok_rows: list[dict[str, object]] = []

    try:
        if fmt == "csv":
            # CSV: BOM-safe + per-row reject logging
            raw_rows = iter_csv_rows(str(p), args.max_rows)
            ok_rows = _normalize_and_fill(raw_rows)
        elif fmt == "jsonl":
            # JSONL: per-row reject logging (mirrors CSV)
            raw_rows = iter_jsonl_rows(str(p), args.max_rows)
            ok_rows = _normalize_and_fill(raw_rows)
        else:  # tsv
            ok_rows, errs = _read_delim(p, "\t", args.strict, args.max_rows)
    except TooManyRowsError as e:
        # Ensure a clear, actionable message for oversized payloads.
        # Example: "Input contains 12,431 rows but --max-rows=10,000. Lower your file size or raise the cap."
        print(str(e), file=sys.stderr)
        print("Lower your file size or raise --max-rows.", file=sys.stderr)
        sys.exit(3)
    except ValueError as e:
        # Header-level or other early fatal validation
        print(str(e), file=sys.stderr)
        sys.exit(2)

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
