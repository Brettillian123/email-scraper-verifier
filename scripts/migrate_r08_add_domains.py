# scripts/migrate_r08_add_domains.py
import argparse
import sqlite3


def column_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r[1] == col for r in rows)  # r[1] is the column name


def add_column_if_missing(conn: sqlite3.Connection, table: str, col: str, type_sql: str, dry: bool):
    if not column_exists(conn, table, col):
        sql = f"ALTER TABLE {table} ADD COLUMN {col} {type_sql}"
        print(f"[APPLY] {sql}")
        if not dry:
            conn.execute(sql)
    else:
        print(f"[SKIP]  {table}.{col} already exists")


def main():
    p = argparse.ArgumentParser(description="R08 migration: add domain-resolution columns")
    p.add_argument("db_path", nargs="?", default="dev.db")
    p.add_argument("--dry-run", action="store_true", help="show actions without writing changes")
    args = p.parse_args()

    conn = sqlite3.connect(args.db_path)
    try:
        # Keep it atomic
        with conn:
            # --- companies: where the official, resolved domain lives ---
            add_column_if_missing(conn, "companies", "official_domain", "TEXT", args.dry_run)
            add_column_if_missing(
                conn, "companies", "official_domain_source", "TEXT", args.dry_run
            )  # e.g., 'home_page', 'whois', 'search'
            add_column_if_missing(
                conn, "companies", "official_domain_confidence", "REAL", args.dry_run
            )  # 0.0–1.0
            add_column_if_missing(
                conn, "companies", "official_domain_checked_at", "TEXT", args.dry_run
            )  # ISO8601

            # Helpful lookup speed-up (non-unique, partial uniqueness can come later if needed)
            idx_sql = "CREATE INDEX IF NOT EXISTS ix_companies_official_domain ON companies(official_domain)"
            print(f"[APPLY] {idx_sql}")
            if not args.dry_run:
                conn.execute(idx_sql)

            # --- ingest_items: keep what we attempted/resolved during intake ---
            add_column_if_missing(conn, "ingest_items", "resolved_domain", "TEXT", args.dry_run)
            add_column_if_missing(
                conn, "ingest_items", "resolved_domain_source", "TEXT", args.dry_run
            )
            add_column_if_missing(
                conn, "ingest_items", "resolved_domain_confidence", "REAL", args.dry_run
            )

        print("Done.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
