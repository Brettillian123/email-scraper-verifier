# scripts/ingest_jsonl.py
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

# --- make "src" importable when run from scripts/ ---
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest import enforce_row_limit, ingest_row  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest leads from JSONL.")
    ap.add_argument("--path", required=True, help="Path to JSONL file")
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
    rej_path = Path(f"ingest_jsonl.rejects.{ts}.log")
    rejects = 0
    ok = 0
    seen = 0

    with path.open("r", encoding="utf-8-sig") as fh, rej_path.open("w", encoding="utf-8") as rej:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            seen += 1
            if seen > args.max_rows:
                enforce_row_limit(seen, channel="file", file_limit=args.max_rows)

            try:
                obj = json.loads(line)
                if not isinstance(obj, dict):
                    raise ValueError("JSONL line is not an object")
                # Keys are passed through as-is; ingest_row handles normalization,
                # DB writes, and follow-up enqueues.
                ingest_row({str(k).lower(): v for k, v in obj.items()})
                ok += 1
            except Exception as e:
                rejects += 1
                rej.write(
                    json.dumps(
                        {"row_number": seen, "error": str(e), "line": line},
                        ensure_ascii=False,
                    )
                    + "\n"
                )

    print(f"JSONL ingest complete: ok={ok} rejects={rejects} max_rows={args.max_rows} file={path}")
    if rejects:
        print(f"Rejected rows logged to: {rej_path}")


if __name__ == "__main__":
    main()
