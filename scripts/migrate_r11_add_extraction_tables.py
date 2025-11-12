from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

DDL = """
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS people (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT
);

CREATE TABLE IF NOT EXISTS emails (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  person_id INTEGER REFERENCES people(id) ON DELETE SET NULL,
  email TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS email_provenance (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email_id INTEGER NOT NULL REFERENCES emails(id) ON DELETE CASCADE,
  source_url TEXT NOT NULL,
  discovered_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(email_id, source_url)
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS ix_emails_person_id ON emails(person_id);
CREATE INDEX IF NOT EXISTS ix_prov_email_id ON email_provenance(email_id);
"""


def migrate(db_path: str) -> None:
    p = Path(db_path)
    con = sqlite3.connect(str(p))
    try:
        con.executescript(DDL)
        con.commit()
        print(f"Migration complete. Ensured people/emails/email_provenance in {p}")
    finally:
        con.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="dev.db")
    args = ap.parse_args()
    migrate(args.db)
