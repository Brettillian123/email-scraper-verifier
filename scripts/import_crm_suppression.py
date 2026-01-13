from __future__ import annotations

import argparse
import csv
from pathlib import Path

from src.db import get_conn
from src.db_suppression import upsert_suppression


def import_csv(csv_path: str, source: str) -> None:
    """
    Import CRM suppression data from a CSV file into the suppression table.

    Expected CSV columns (header row required):

        email       # email address to suppress
        reason      # optional, e.g. "bounced", "complaint", "unsubscribed"

    Notes:
        - Only rows with a non-empty "email" field are imported.
        - If "reason" is missing/empty, the provided source is still applied
          and the reason is set to "crm_sync" by default.
        - For each email, we call upsert_suppression(), which normalizes the
          email and uses CURRENT_TIMESTAMP as created_at.

    Example:

        email,reason
        o11test@example.com,bounced
        unsub@example.com,unsubscribed
    """
    csv_path_resolved = Path(csv_path).resolve()

    conn = get_conn()

    try:
        with csv_path_resolved.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                raw_email = (row.get("email") or "").strip()
                if not raw_email:
                    # Skip rows without an email.
                    continue

                reason = (row.get("reason") or "").strip() or "crm_sync"
                upsert_suppression(conn, email=raw_email, reason=reason, source=source)

        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import CRM suppression CSV into the local suppression table."
    )
    parser.add_argument(
        "--source",
        default="crm_sync",
        help="Source label to record on suppression rows (default: crm_sync)",
    )
    parser.add_argument(
        "csv_path",
        help="Path to CRM suppression CSV file",
    )

    args = parser.parse_args()
    import_csv(args.csv_path, args.source)


if __name__ == "__main__":
    main()
