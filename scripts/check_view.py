import pathlib
import sqlite3

root = pathlib.Path(__file__).resolve().parents[1]
db_path = root / "dev.db"

conn = sqlite3.connect(db_path)
conn.execute("PRAGMA foreign_keys=ON;")
row = conn.execute("SELECT email, status, reason, checked_at FROM v_emails_latest").fetchone()
conn.close()

print(row)  # Expect: ('avery.nguyen@acme.test', 'valid', 'accepts RCPT', <timestamp>)
