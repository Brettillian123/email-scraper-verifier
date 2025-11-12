import sqlite3
import sys

con = sqlite3.connect(sys.argv[1] if len(sys.argv) > 1 else "dev.db")
con.executescript("""
CREATE TABLE IF NOT EXISTS domain_patterns (
  domain TEXT PRIMARY KEY,
  pattern TEXT,
  confidence REAL NOT NULL,
  samples INTEGER NOT NULL,
  inferred_at TEXT NOT NULL DEFAULT (datetime('now'))
);
""")
con.commit()
