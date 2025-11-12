# src/ingest/cli.py
"""
R13 Ingest CLI
Ensures every inbound row passes through normalize_row() before DB writes.

Usage examples
--------------
# Auto-detect format from extension and persist to DB
python -m src.ingest.cli samples/leads.csv

# JSONL input, dry-run (print normalized JSONL to stdout)
python -m src.ingest.cli --format jsonl --dry-run samples/leads.jsonl

# CSV input, write normalized output to a file AND persist to DB
python -m src.ingest.cli samples/leads.csv --out normalized.jsonl
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections.abc import Iterable, Iterator
from itertools import tee
from pathlib import Path

from .normalize import normalize_row

# ---- Persist adapter --------------------------------------------------------


def _load_persist_adapter():
    """
    Best-effort import of a persistence function.

    We support either:
      - persist.persist_rows(rows: Iterable[dict]) -> int
      - persist.upsert_row(row: dict) -> None

    If neither exists, we'll operate in dry-run mode unless user requested DB write,
    in which case we raise a helpful error.
    """
    try:
        from . import persist as _persist  # type: ignore
    except Exception:  # pragma: no cover
        return None, None

    persist_rows = getattr(_persist, "persist_rows", None)
    upsert_row = getattr(_persist, "upsert_row", None)
    return persist_rows, upsert_row


# ---- IO helpers -------------------------------------------------------------


def _read_csv(path: Path) -> Iterator[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert None values from missing columns to empty strings
            yield {k: (v if v is not None else "") for k, v in row.items()}


def _read_jsonl(path: Path) -> Iterator[dict[str, object]]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _iter_rows(paths: list[Path], fmt: str) -> Iterator[dict[str, object]]:
    for p in paths:
        if fmt == "csv":
            yield from _read_csv(p)
        elif fmt == "jsonl":
            yield from _read_jsonl(p)
        elif fmt == "auto":
            if p.suffix.lower() in {".csv"}:
                yield from _read_csv(p)
            elif p.suffix.lower() in {".jsonl", ".ndjson"}:
                yield from _read_jsonl(p)
            else:
                raise SystemExit(f"Cannot auto-detect format for: {p}")
        else:
            raise SystemExit(f"Unknown format: {fmt}")


def _normalize_stream(rows: Iterable[dict[str, object]]) -> Iterator[dict[str, object]]:
    for raw in rows:
        if not isinstance(raw, dict):
            continue  # skip malformed
        norm, _errs = normalize_row(raw)
        # Never drop provenance; normalize_row already passes source_url through.
        yield norm


def _write_jsonl(rows: Iterable[dict[str, object]], out_path: Path | None) -> None:
    out_f = sys.stdout if out_path is None else out_path.open("w", encoding="utf-8")
    close = out_f is not sys.stdout
    try:
        for r in rows:
            out_f.write(json.dumps(r, ensure_ascii=False) + "\n")
    finally:
        if close:
            out_f.close()


def _persist(rows: Iterable[dict[str, object]], batch_size: int) -> int:
    persist_rows, upsert_row = _load_persist_adapter()
    if persist_rows is not None:
        # Delegate to bulk persist API
        return int(persist_rows(rows))  # type: ignore[call-arg]
    if upsert_row is not None:
        # Stream per-row upserts
        n = 0
        batch: list[dict[str, object]] = []
        for r in rows:
            batch.append(r)
            if len(batch) >= batch_size:
                for br in batch:
                    upsert_row(br)  # type: ignore[misc]
                n += len(batch)
                batch.clear()
        if batch:
            for br in batch:
                upsert_row(br)  # type: ignore[misc]
            n += len(batch)
        return n
    # No persist adapter available
    raise RuntimeError(
        "No persistence adapter found. Implement one of:\n"
        " - src/ingest/persist.persist_rows(rows: Iterable[dict]) -> int\n"
        " - src/ingest/persist.upsert_row(row: dict) -> None\n"
        "Or run with --dry-run to only emit normalized JSONL."
    )


# ---- CLI --------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Ingest leads with R13 normalization.")
    p.add_argument("inputs", nargs="+", help="Input files (.csv, .jsonl)")
    p.add_argument(
        "--format",
        choices=["auto", "csv", "jsonl"],
        default="auto",
        help="Input format (default: auto by extension)",
    )
    p.add_argument(
        "--out",
        metavar="PATH",
        help="Write normalized JSONL to PATH (also persists to DB unless --dry-run). "
        "Use '-' or omit to write to stdout.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write to DB; only emit normalized JSONL.",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Batch size for per-row upserts when bulk API is unavailable (default: 500).",
    )
    args = p.parse_args(argv)

    paths = [Path(s) for s in args.inputs]
    for pth in paths:
        if not pth.exists():
            raise SystemExit(f"Input not found: {pth}")

    # Read → normalize
    raw_iter = _iter_rows(paths, args.format)
    norm_iter = _normalize_stream(raw_iter)

    # If we need to both write to file/stdout AND persist, we must tee the iterator
    out_path = None if (args.out in (None, "-", "")) else Path(args.out)

    if args.dry_run:
        _write_jsonl(norm_iter, out_path)
        return

    # Not a dry-run: tee the stream if writing to a file/stdout as well
    if out_path is None and args.out is None:
        # Persist only
        count = _persist(norm_iter, args.batch_size)
        print(f"✔ Ingested {count} normalized row(s).")
    else:
        a, b = tee(norm_iter)
        _write_jsonl(a, out_path)
        count = _persist(b, args.batch_size)
        dest = "stdout" if out_path is None else str(out_path)
        print(f"✔ Wrote normalized JSONL to {dest} and ingested {count} row(s).")


if __name__ == "__main__":
    main()
