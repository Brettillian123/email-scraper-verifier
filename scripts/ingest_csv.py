# scripts/ingest_csv.py
from __future__ import annotations

"""
CSV ingestor (R13-ready)

- Reads one or more CSV files
- Normalizes each row with src.ingest.normalize.normalize_row (name/title/company)
- Preserves provenance (source_url)
- Persists via src.ingest.persist.persist_best_effort (per-row) so we can log rejects
- Optional: emit normalized JSONL (--out PATH or "-" for stdout)
- Supports both legacy --path and positional file args
- POLISH: After successful ingest (not --dry-run), auto-backfill O02 role/seniority
  (if helper exists) and run R14 scoring against the DB.
"""

import argparse
import csv
import json
import subprocess
import sys
from collections.abc import Iterable, Iterator
from datetime import datetime
from pathlib import Path

# --- make "src" importable when run from scripts/ ---
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest.normalize import normalize_row  # noqa: E402
from src.ingest.persist import persist_best_effort  # noqa: E402

CANONICAL_FIELDS = {
    "company",
    "domain",
    "role",
    "first_name",
    "last_name",
    "full_name",
    "title",
    "source_url",
    "notes",
}


def _validate_headers(fieldnames: list[str]) -> None:
    """
    Validate the header row:
      - Must include 'role'
      - Must include at least one of {'domain','company'}
      - Other fields are allowed; order does not matter
    """
    cols = {c.strip().lower() for c in fieldnames if c}
    if "role" not in cols:
        raise SystemExit("CSV header missing required column: role")
    if "domain" not in cols and "company" not in cols:
        raise SystemExit("CSV header must include domain or company")
    # Optional: warn if canonical fields are missing (not fatal)
    missing = sorted(CANONICAL_FIELDS - cols)
    if missing:
        print(f"[warn] CSV missing optional columns: {', '.join(missing)}")


def _read_csv(path: Path, delimiter: str) -> Iterator[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh, delimiter=delimiter)
        if not reader.fieldnames:
            raise SystemExit(f"CSV has no header row: {path}")
        _validate_headers(reader.fieldnames)
        for row in reader:
            # Normalize keys to lowercase for robustness
            yield {str(k).lower(): (v if v is not None else "") for k, v in row.items()}


def _iter_inputs(paths: list[Path], delimiter: str) -> Iterator[dict[str, str]]:
    for p in paths:
        if not p.exists():
            raise SystemExit(f"File not found: {p}")
        yield from _read_csv(p, delimiter=delimiter)


def _write_jsonl(rows: Iterable[dict[str, object]], out_path: Path | None) -> None:
    out_f = sys.stdout if out_path is None else out_path.open("w", encoding="utf-8")
    close = out_f is not sys.stdout
    try:
        for r in rows:
            out_f.write(json.dumps(r, ensure_ascii=False) + "\n")
    finally:
        if close:
            out_f.close()


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Ingest leads from CSV with R13 normalization.")
    ap.add_argument("inputs", nargs="*", help="CSV files to ingest (positional form)")
    ap.add_argument("--path", help="(Deprecated) single CSV file path (use positional instead)")
    ap.add_argument(
        "--delimiter",
        default=",",
        help="Field delimiter (default: ,)",
    )
    ap.add_argument(
        "--max-rows",
        type=int,
        default=100_000,
        help="Row cap (default: 100000)",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write to DB; only emit normalized JSONL if --out is set.",
    )
    ap.add_argument(
        "--out",
        metavar="PATH",
        help='Write normalized JSONL to PATH (use "-" for stdout).',
    )
    # POLISH: DB path & ability to skip post-ingest scoring
    ap.add_argument(
        "--db",
        default="data/dev.db",
        help="SQLite DB path used by migrators (default: data/dev.db)",
    )
    ap.add_argument(
        "--no-score",
        action="store_true",
        help="Skip post-ingest O02 backfill and R14 scoring.",
    )
    args = ap.parse_args(argv)

    # Resolve inputs (support legacy --path)
    paths: list[Path] = []
    if args.path:
        paths.append(Path(args.path))
    paths.extend(Path(p) for p in args.inputs)
    if not paths:
        ap.error("Provide at least one input CSV (positional) or --path FILE")

    # Prepare output sink if requested
    out_path: Path | None = None
    if args.out is not None:
        out_path = None if args.out == "-" else Path(args.out)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    rej_path = Path(f"ingest_csv.rejects.{ts}.log") if not args.dry_run else None
    rejects = 0
    ok = 0
    seen = 0

    # Iterate rows → normalize → (optional) write normalized → (optional) persist
    norm_buffer: list[dict[str, object]] = []

    for raw in _iter_inputs(paths, delimiter=args.delimiter):
        seen += 1
        if seen > args.max_rows:
            raise SystemExit(f"Row limit exceeded: {args.max_rows}")

        try:
            norm, _errs = normalize_row(raw)
            # Optionally collect normalized output
            if out_path is not None:
                norm_buffer.append(norm)
                # Flush occasionally to limit memory
                if len(norm_buffer) >= 1000:
                    _write_jsonl(norm_buffer, out_path)
                    norm_buffer.clear()

            if not args.dry_run:
                # Persist the already-normalized row (best-effort)
                persist_best_effort(norm)

            ok += 1

        except Exception as e:
            rejects += 1
            if rej_path is not None:
                with rej_path.open("a", encoding="utf-8") as rej:
                    rej.write(
                        json.dumps(
                            {
                                "row_number": seen,
                                "error": str(e),
                                "row": raw,
                            },
                            ensure_ascii=False,
                        )
                        + "\n"
                    )

    # Flush any remaining normalized rows to output
    if norm_buffer and out_path is not None:
        _write_jsonl(norm_buffer, out_path)

    # Epilogue
    print(
        f"CSV ingest complete: ok={ok} rejects={rejects} "
        f"max_rows={args.max_rows} files={', '.join(str(p) for p in paths)}"
    )
    if rejects and rej_path is not None:
        print(f"Rejected rows logged to: {rej_path}")
    if out_path is None and args.dry_run:
        # If user requested dry-run without --out, default to stdout for visibility
        print("[note] --dry-run without --out: no normalized output was written.")

    # POLISH: After a real ingest, ensure O02 fields exist then score (R14).
    if not args.dry_run and not args.no_score:
        scripts_dir = ROOT / "scripts"

        # 1) Populate role_family/seniority if helper exists
        bf_o02 = scripts_dir / "backfill_o02_roles.py"
        if bf_o02.exists():
            print("Backfilling O02 role_family/seniority…")
            subprocess.run(
                [sys.executable, str(bf_o02), "--db", args.db],
                check=True,
            )
        else:
            print(
                "[warn] backfill_o02_roles.py not found; assuming ingest populated role/seniority."
            )

        # 2) Score ICP (R14)
        print("Scoring (R14): backfilling icp_score/icp_reasons…")
        subprocess.run(
            [
                sys.executable,
                str(scripts_dir / "migrate_r14_add_icp.py"),
                "--db",
                args.db,
                "-v",
            ],
            check=True,
        )


if __name__ == "__main__":
    main()
