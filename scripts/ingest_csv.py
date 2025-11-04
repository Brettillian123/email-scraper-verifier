# scripts/ingest_csv.py
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime
from pathlib import Path

# --- make "src" importable when run from scripts/ ---
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest import enforce_row_limit, ingest_row  # noqa: E402

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


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest leads from CSV.")
    ap.add_argument("--path", required=True, help="Path to CSV file")
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
    args = ap.parse_args()

    path = Path(args.path)
    if not path.exists():
        raise SystemExit(f"File not found: {path}")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    rej_path = Path(f"ingest_csv.rejects.{ts}.log")
    rejects = 0
    ok = 0
    seen = 0

    with (
        path.open("r", encoding="utf-8-sig", newline="") as fh,
        rej_path.open("w", encoding="utf-8") as rej,
    ):
        reader = csv.DictReader(fh, delimiter=args.delimiter)
        if not reader.fieldnames:
            raise SystemExit("CSV has no header row")
        _validate_headers(reader.fieldnames)

        for row in reader:
            seen += 1
            if seen > args.max_rows:
                enforce_row_limit(seen, channel="file", file_limit=args.max_rows)
            try:
                # Pass the raw row dict; src.ingest.ingest_row will normalize,
                # write to DB, and enqueue follow-ups.
                ingest_row({k.lower(): v for k, v in row.items()})
                ok += 1
            except Exception as e:
                rejects += 1
                rej.write(
                    json.dumps(
                        {
                            "row_number": seen,
                            "error": str(e),
                            "row": {k: v for k, v in row.items()},
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )

    print(f"CSV ingest complete: ok={ok} rejects={rejects} max_rows={args.max_rows} file={path}")
    if rejects:
        print(f"Rejected rows logged to: {rej_path}")


if __name__ == "__main__":
    main()
