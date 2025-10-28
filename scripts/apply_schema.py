import sqlite3, pathlib

root = pathlib.Path(__file__).resolve().parents[1]
db_path = root / "dev.db"
schema_path = root / "db" / "schema.sql"

sql = schema_path.read_text(encoding="utf-8")

conn = sqlite3.connect(db_path)
try:
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.executescript(sql)
    # Create the "latest verification" view
    conn.executescript("""
    CREATE VIEW IF NOT EXISTS v_emails_latest AS
    SELECT e.id AS email_id, e.email, e.company_id, e.person_id, e.icp_score,
           vr.status, vr.reason, vr.checked_at
    FROM emails e
    LEFT JOIN (
      SELECT vr1.email_id, vr1.status, vr1.reason, vr1.checked_at
      FROM verification_results vr1
      WHERE vr1.checked_at = (
        SELECT MAX(vr2.checked_at) FROM verification_results vr2
        WHERE vr2.email_id = vr1.email_id
      )
    ) vr ON vr.email_id = e.id;
    """)
    conn.commit()
    print(f"Initialized {db_path} and created view v_emails_latest.")
finally:
    conn.close()
