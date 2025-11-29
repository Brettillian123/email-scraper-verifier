from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path

from src.db_suppression import upsert_suppression


def import_csv(db_path: str, csv_path: str, source: str) -> None:
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
    db_path_resolved = Path(db_path).resolve()
    csv_path_resolved = Path(csv_path).resolve()

    conn = sqlite3.connect(str(db_path_resolved))
    conn.row_factory = sqlite3.Row

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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import CRM suppression CSV into the local suppression table."
    )
    parser.add_argument(
        "--db",
        default="data/dev.db",
        help="Path to SQLite database (default: data/dev.db)",
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
    import_csv(args.db, args.csv_path, args.source)


if __name__ == "__main__":
    main()
