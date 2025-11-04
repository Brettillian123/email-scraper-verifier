import sqlite3

conn = sqlite3.connect("dev.db"); conn.row_factory=None
print(conn.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='ux_emails_email'").fetchall())
