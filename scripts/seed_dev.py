import sqlite3, datetime, pathlib
root = pathlib.Path(__file__).resolve().parents[1]
db_path = root / "dev.db"

now = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S")

conn = sqlite3.connect(db_path)
conn.execute("PRAGMA foreign_keys=ON;")
cur = conn.cursor()

# Company
cur.execute("INSERT OR IGNORE INTO companies (name, domain, website_url) VALUES (?,?,?)",
            ("Acme Widgets", "acme.test", "https://acme.test"))

company_id = cur.lastrowid or cur.execute("SELECT id FROM companies WHERE domain=?", ("acme.test",)).fetchone()[0]

# Person
cur.execute("""INSERT INTO people (company_id, first_name, last_name, full_name, title, source_url)
              VALUES (?,?,?,?,?,?)""",
            (company_id, "Avery", "Nguyen", "Avery Nguyen", "VP, Sales", "https://acme.test/team"))
person_id = cur.lastrowid

# Email
cur.execute("""INSERT INTO emails (person_id, company_id, email, is_published, source_url, icp_score)
              VALUES (?,?,?,?,?,?)""",
            (person_id, company_id, "avery.nguyen@acme.test", 1, "https://acme.test/team", 0.82))
email_id = cur.lastrowid

# Verification results (two snapshots)
cur.execute("""INSERT INTO verification_results (email_id, mx_host, status, reason, checked_at)
              VALUES (?,?,?,?,?)""",
            (email_id, "mx.acme.test", "unknown_timeout", "initial timeout", now))

later = (datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")

cur.execute("""INSERT INTO verification_results (email_id, mx_host, status, reason, checked_at)
              VALUES (?,?,?,?,?)""",
            (email_id, "mx.acme.test", "valid", "accepts RCPT", later))


        conn.commit()

        # Quick peek
        sample = conn.execute(
            "SELECT id, email, company_id, verify_status, verified_at FROM emails ORDER BY id LIMIT 10;"
        ).fetchall()
        print("Seeded emails (up to 10 shown):")
        for r in sample:
            print(
                f"· #{r['id']}: {r['email']} (company_id={r['company_id']} vs={r['verify_status']} t={r['verified_at']})"
            )

    print("✔ Seed complete (idempotent).")


if __name__ == "__main__":
    main()
