import os
import sqlite3
from datetime import UTC, datetime

from src.db import upsert_verification_result

upsert_verification_result(
    email='check@example.com',
    verify_status='valid',
    reason='smoke',
    mx_host='mx.example.com',
    verified_at=datetime.now(UTC),
    person_id=123,          # will be used only if emails.person_id exists
    source_url='https://example.com',  # used only if column exists
    icp_score=42,           # used only if column exists
)

# Print what we wrote
db = os.environ['DATABASE_URL'].removeprefix('sqlite:///')
con = sqlite3.connect(db)
cur = con.cursor()
cur.execute("SELECT email, verify_status, reason, mx_host, verified_at FROM emails WHERE email=?", ('check@example.com',))
print('Row:', cur.fetchone())
