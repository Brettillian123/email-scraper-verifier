#!/usr/bin/env python
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from pathlib import Path
from typing import Any

# --- make "src" importable when run from scripts/ ---
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Local imports (after sys.path tweak)
from src.ingest import ingest_row, normalize_company  # noqa: E402
from src.ingest.rejects import (  # noqa: E402
    current_rejects_file,
    log_reject,
)
from src.ingest.validators import (  # noqa: E402
    MAX_ROWS_DEFAULT,
    TooManyRowsError,
    enforce_row_cap,
    validate_header_csv,
)


def build_parser() -> argparse.ArgumentParser:
    """
    CLI for ingestion with a hard cap on input size.
    """
    p = argparse.ArgumentParser(description="R07 ingest (CSV/TSV/JSONL)")
    p.add_argument("path", help="CSV, TSV, or JSONL to ingest")
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
        help="(TSV only) Fail fast on first error",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate only; do not write to DB",
    )
    return p


# ---------------- Helpers (R07 preflight) ----------------
def _needs_name(row: dict[str, Any]) -> bool:
    full = normalize_company(row.get("full_name"))
    first = normalize_company(row.get("first_name"))
    last = normalize_company(row.get("last_name"))
    return not (full or (first and last))


def _preflight_errors(row: dict[str, Any]) -> list[str]:
    """
    R07: company required; must have either full_name or first+last.
    role is optional; user_supplied_domain is optional (no strict validation here).
    """
    errs: list[str] = []
    company = normalize_company(row.get("company"))
    if not company:
        errs.append("company is required")
    if _needs_name(row):
        errs.append("full_name (or first+last) is required")
    return errs


def _normalize_legacy_fields(row: dict[str, Any]) -> None:
    """
    Accept legacy `domain` by mapping it to `user_supplied_domain`.
    Do not validate the domain here; R08 will resolve/verify later.
    """
    if "domain" in row and "user_supplied_domain" not in row:
        row["user_supplied_domain"] = row.get("domain")


# ---------------- CSV (BOM-safe) with per-row rejections ----------------
def iter_csv_rows(path: str, max_rows: int | None) -> list[dict[str, Any]]:
    """
    Read CSV with UTF-8 BOM handling. Invalid rows are logged to rejects with line numbers.
    Enforces a strict row cap to fail early on oversized inputs.
    Returns only rows that pass R07 preflight (company+name).
    """
    rows: list[dict[str, Any]] = []
    with open(path, newline="", encoding="utf-8-sig") as f:  # BOM-safe
        rdr = csv.DictReader(f)
        validate_header_csv(rdr.fieldnames)

        for line_no, row in enumerate(rdr, start=2):  # header is line 1
            _normalize_legacy_fields(row)
            errs = _preflight_errors(row)
            if errs:
                log_reject(line_no, "; ".join(errs), row)
                continue

            rows.append(row)
            enforce_row_cap(len(rows), max_rows or MAX_ROWS_DEFAULT)

    print(f"Rejects file (if any): {current_rejects_file()}")
    return rows


# ---------------- JSONL (per-row rejects, mirrored from CSV) ----------------
def iter_jsonl_rows(path: str, max_rows: int | None) -> list[dict[str, Any]]:
    """
    Read JSONL one object per line. Invalid rows are logged to rejects with line numbers.
    Enforces a strict row cap to fail early on oversized inputs.
    Returns only rows that pass R07 preflight (company+name).
    """
    rows: list[dict[str, Any]] = []
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

            if not isinstance(obj, dict):
                log_reject(line_no, "json object is not a dict", {"_raw": s})
                continue

            _normalize_legacy_fields(obj)
            errs = _preflight_errors(obj)
            if errs:
                log_reject(line_no, "; ".join(errs), obj)
                continue

            rows.append(obj)
            enforce_row_cap(len(rows), max_rows or MAX_ROWS_DEFAULT)

    print(f"Rejects file (if any): {current_rejects_file()}")
    return rows


# ---------------- TSV reader (legacy delimiter-based path) ----------------
def _read_delim(path: Path, delim: str, strict: bool, max_rows: int | None):
    """
    Legacy TSV/CSV path that performs validation during iteration.
    Mirrors the row-cap guardrail and R07 preflight rules.
    Returns (ok_rows, errs).
    """
    ok_rows: list[dict[str, Any]] = []
    errs: list[str] = []

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=delim)
        try:
            validate_header_csv(reader.fieldnames)
        except ValueError as e:
            return [], [f"Header error: {e}"]

        for line_no, row in enumerate(reader, start=2):  # data starts at line 2
            try:
                enforce_row_cap(len(ok_rows) + 1, max_rows or MAX_ROWS_DEFAULT)
            except TooManyRowsError:
                raise

            _normalize_legacy_fields(row)
            row_errs = _preflight_errors(row)
            if row_errs:
                msg = f"Line {line_no}: {'; '.join(row_errs)}"
                if strict:
                    return [], [msg]
                errs.append(msg)
                continue

            ok_rows.append(row)

    return ok_rows, errs


# ---------------- Small helpers to keep main() simple ----------------
def _detect_format(p: Path, override: str | None) -> str:
    fmt = (override or p.suffix.lower().lstrip(".")).lower()
    if fmt not in {"csv", "tsv", "jsonl"}:
        raise ValueError("Cannot detect format (use --format).")
    return fmt


def _ingest_file(fmt: str, p: Path, max_rows: int, strict: bool) -> tuple[int, list[str]]:
    """
    Run ingestion for a given file format and return (accepted_count, errors).
    """
    accepted = 0
    errs: list[str] = []

    if fmt == "csv":
        for r in iter_csv_rows(str(p), max_rows):
            if ingest_row(r):
                accepted += 1
    elif fmt == "jsonl":
        for r in iter_jsonl_rows(str(p), max_rows):
            if ingest_row(r):
                accepted += 1
    else:  # tsv
        ok_rows, errs = _read_delim(p, "\t", strict, max_rows)
        if not errs:
            for r in ok_rows:
                if ingest_row(r):
                    accepted += 1

    return accepted, errs


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    p = Path(args.path)
    if not p.exists():
        print(f"File not found: {p}", file=sys.stderr)
        sys.exit(2)

    try:
        fmt = _detect_format(p, args.format)
    except ValueError as e:
        print(str(e), file=sys.stderr)
        sys.exit(2)

    # Respect dry-run by telling ingest_row to skip persistence.
    if args.dry_run:
        os.environ["INGEST_SKIP_PERSIST"] = "1"

    try:
        accepted, errs = _ingest_file(fmt, p, args.max_rows, args.strict)
    except TooManyRowsError as e:
        print(str(e), file=sys.stderr)
        print("Lower your file size or raise --max-rows.", file=sys.stderr)
        sys.exit(3)
    except ValueError as e:
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
        print(f"Validated {accepted} rows. (dry-run; no DB writes)")
    else:
        print(f"Inserted {accepted} rows into ingest_items.")
    sys.exit(0)


if __name__ == "__main__":
    main()
