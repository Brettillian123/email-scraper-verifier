import sqlite3

conn = sqlite3.connect("dev.db"); conn.row_factory = sqlite3.Row
rows = conn.execute("PRAGMA index_list('emails');").fetchall()
print([dict(r) for r in rows])
