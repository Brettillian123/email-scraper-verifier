import os
import sqlite3
from datetime import UTC, datetime

from src.db import upsert_verification_result

email = 'check@example.com'
upsert_verification_result(
    email=email,
    verify_status='valid',
    reason='smoke',
    mx_host='mx.example.com',
    verified_at=datetime.now(UTC),
    # NOTE: no person_id passed in — function will ensure/create and set it
)

db = os.environ['DATABASE_URL'].removeprefix('sqlite:///')
con = sqlite3.connect(db); cur = con.cursor()
cols = [r[1] for r in cur.execute('PRAGMA table_info(emails)')]
if 'person_id' in cols:
    row = cur.execute('SELECT email, person_id FROM emails WHERE email=?', (email,)).fetchone()
    print('Email row:', row)
else:
    print('emails.person_id not in schema; nothing to set.')
