import sqlite3

from src.db import upsert_verification_result

# two writes to the same email — should UPDATE, not INSERT
upsert_verification_result(email="alice@example.com", verify_status="valid",   reason="t1", mx_host="mx1")
upsert_verification_result(email="alice@example.com", verify_status="invalid", reason="t2", mx_host="mx2")

# verify: exactly 1 row in base table for that email
conn = sqlite3.connect("dev.db"); conn.row_factory = sqlite3.Row
c = conn.cursor()
print("rows in emails for alice:", c.execute("SELECT COUNT(*) c FROM emails WHERE email=?", ("alice@example.com",)).fetchone()["c"])
print("latest in view:", dict(c.execute(
    "SELECT email, verify_status, reason, mx_host FROM v_emails_latest WHERE email=?",
    ("alice@example.com",)
).fetchone()))
