from __future__ import annotations

import argparse
import sqlite3
import sys

DDL = """
CREATE TABLE IF NOT EXISTS mx_probe_stats (
  id INTEGER PRIMARY KEY,
  mx_host    TEXT NOT NULL,
  ts         TEXT NOT NULL DEFAULT (datetime('now')),
  code       INTEGER,               -- SMTP RCPT code or NULL on transport error
  category   TEXT,                  -- 'accept' | 'hard_fail' | 'temp_fail' | 'unknown'
  error_kind TEXT,                  -- 'timeout'|'disconnected'|'smtp_response'|... or NULL
  elapsed_ms INTEGER                -- probe latency in ms
);
CREATE INDEX IF NOT EXISTS idx_mx_probe_host_ts ON mx_probe_stats(mx_host, ts);
"""


def main(db: str) -> None:
    con = sqlite3.connect(db)
    with con:
        for stmt in DDL.strip().split(";"):
            s = stmt.strip()
            if s:
                con.execute(s)
    print(f"✔ O06: mx_probe_stats ensured in {db}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    args = ap.parse_args()
    try:
        main(args.db)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
