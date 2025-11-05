import argparse
import sqlite3


def col_exists(conn, table, col):
    return any(r[1] == col for r in conn.execute(f"PRAGMA table_info({table})"))


def add_col(conn, table, col, ddl, dry):
    if not col_exists(conn, table, col):
        sql = f"ALTER TABLE {table} ADD COLUMN {col} {ddl}"
        print(f"[APPLY] {sql}")
        if not dry:
            conn.execute(sql)
    else:
        print(f"[SKIP]  {table}.{col} already exists")


def ensure_domain_resolutions(conn, dry):
    create_sql = """
    CREATE TABLE IF NOT EXISTS domain_resolutions (
      id               INTEGER PRIMARY KEY,
      company_id       INTEGER NOT NULL,
      company_name     TEXT NOT NULL,
      user_hint        TEXT,          -- from ingest row (may be NULL)
      chosen_domain    TEXT,          -- punycode ascii
      method           TEXT NOT NULL, -- 'hint_validated' | 'dns_valid' | 'http_redirect' | 'fallback' | ...
      confidence       INTEGER NOT NULL,  -- 0..100
      reason           TEXT,          -- brief decision note
      resolver_version TEXT NOT NULL, -- e.g. 'r08.1'
      created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
    )
    """
    idx1 = "CREATE INDEX IF NOT EXISTS idx_domain_resolutions_company_id ON domain_resolutions(company_id)"
    print("[APPLY] create domain_resolutions (if missing)")
    if not dry:
        conn.execute(create_sql)
        conn.execute(idx1)


def main():
    ap = argparse.ArgumentParser(description="Finalize R08 DB pieces safely")
    ap.add_argument("db", nargs="?", default="dev.db")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    with conn:
        # You already have official_domain* columns. Keep them.
        # Add a place to store the *raw* hint from ingest if missing.
        add_col(conn, "companies", "user_supplied_domain", "TEXT", args.dry_run)

        # (Optional but handy) keep per-row resolution notes on ingest staging
        add_col(conn, "ingest_items", "resolved_domain", "TEXT", args.dry_run)
        add_col(conn, "ingest_items", "resolved_domain_source", "TEXT", args.dry_run)
        add_col(conn, "ingest_items", "resolved_domain_confidence", "INTEGER", args.dry_run)

        # Create the audit log table for R08 decisions
        ensure_domain_resolutions(conn, args.dry_run)

        # Helpful lookup index
        idx = "CREATE INDEX IF NOT EXISTS idx_companies_user_supplied_domain ON companies(user_supplied_domain)"
        print(f"[APPLY] {idx}")
        if not args.dry_run:
            conn.execute(idx)

    print("Done.")


if __name__ == "__main__":
    main()
