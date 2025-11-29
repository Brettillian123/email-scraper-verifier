# scripts/migrate_r21_search_indexing.py
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


def get_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    # Ensure foreign keys are enforced; consistent with other scripts.
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def create_fts_tables(conn: sqlite3.Connection) -> None:
    """
    Create FTS5 virtual tables for people and companies.

    These are content-less FTS tables that mirror the current people/companies
    tables via triggers. The rowid of each FTS table is kept equal to the
    corresponding base table's primary key (id).
    """
    conn.executescript(
        """
        CREATE VIRTUAL TABLE IF NOT EXISTS people_fts
        USING fts5(
            company_id UNINDEXED,
            full_name,
            first_name,
            last_name,
            title_norm,
            role_family,
            seniority,
            company_name,
            company_domain,
            attrs_text
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS companies_fts
        USING fts5(
            name_norm,
            domain,
            attrs_text
        );
        """
    )


def create_people_fts_triggers(conn: sqlite3.Connection) -> None:
    """
    Triggers to keep people_fts in sync with people + companies.

    Notes:
      * rowid of people_fts == people.id
      * company_name / company_domain come from companies.*
      * attrs_text is currently a placeholder; can be backfilled later
        from companies.attrs JSON.
    """
    conn.executescript(
        """
        -- Insert new people into people_fts
        CREATE TRIGGER IF NOT EXISTS people_fts_ai
        AFTER INSERT ON people
        BEGIN
          INSERT INTO people_fts(
            rowid,
            company_id,
            full_name,
            first_name,
            last_name,
            title_norm,
            role_family,
            seniority,
            company_name,
            company_domain,
            attrs_text
          )
          SELECT
            NEW.id,
            NEW.company_id,
            NEW.full_name,
            NEW.first_name,
            NEW.last_name,
            NEW.title_norm,
            NEW.role_family,
            NEW.seniority,
            COALESCE(c.name_norm, c.name),
            COALESCE(c.official_domain, c.domain),
            ''
          FROM companies AS c
          WHERE c.id = NEW.company_id;
        END;

        -- Refresh people_fts when a person is updated
        CREATE TRIGGER IF NOT EXISTS people_fts_au
        AFTER UPDATE ON people
        BEGIN
          DELETE FROM people_fts WHERE rowid = OLD.id;

          INSERT INTO people_fts(
            rowid,
            company_id,
            full_name,
            first_name,
            last_name,
            title_norm,
            role_family,
            seniority,
            company_name,
            company_domain,
            attrs_text
          )
          SELECT
            NEW.id,
            NEW.company_id,
            NEW.full_name,
            NEW.first_name,
            NEW.last_name,
            NEW.title_norm,
            NEW.role_family,
            NEW.seniority,
            COALESCE(c.name_norm, c.name),
            COALESCE(c.official_domain, c.domain),
            ''
          FROM companies AS c
          WHERE c.id = NEW.company_id;
        END;

        -- Remove deleted people from people_fts
        CREATE TRIGGER IF NOT EXISTS people_fts_ad
        AFTER DELETE ON people
        BEGIN
          DELETE FROM people_fts WHERE rowid = OLD.id;
        END;
        """
    )


def create_companies_fts_triggers(conn: sqlite3.Connection) -> None:
    """
    Triggers to keep companies_fts in sync with companies, and to keep
    company_name/company_domain in people_fts fresh when companies change.
    """
    conn.executescript(
        """
        -- Insert new companies into companies_fts
        CREATE TRIGGER IF NOT EXISTS companies_fts_ai
        AFTER INSERT ON companies
        BEGIN
          INSERT INTO companies_fts(
            rowid,
            name_norm,
            domain,
            attrs_text
          )
          VALUES(
            NEW.id,
            COALESCE(NEW.name_norm, NEW.name),
            COALESCE(NEW.official_domain, NEW.domain),
            ''
          );
        END;

        -- Refresh company data in both companies_fts and people_fts when a company is updated
        CREATE TRIGGER IF NOT EXISTS companies_fts_au
        AFTER UPDATE ON companies
        BEGIN
          DELETE FROM companies_fts WHERE rowid = OLD.id;

          INSERT INTO companies_fts(
            rowid,
            name_norm,
            domain,
            attrs_text
          )
          VALUES(
            NEW.id,
            COALESCE(NEW.name_norm, NEW.name),
            COALESCE(NEW.official_domain, NEW.domain),
            ''
          );

          -- Keep company fields in people_fts in sync
          UPDATE people_fts
          SET
            company_name = COALESCE(NEW.name_norm, NEW.name),
            company_domain = COALESCE(NEW.official_domain, NEW.domain)
          WHERE company_id = NEW.id;
        END;

        -- Remove deleted companies from companies_fts
        CREATE TRIGGER IF NOT EXISTS companies_fts_ad
        AFTER DELETE ON companies
        BEGIN
          DELETE FROM companies_fts WHERE rowid = OLD.id;
        END;
        """
    )


def backfill_people_fts(conn: sqlite3.Connection) -> None:
    """
    Backfill people_fts from existing people + companies rows.

    This only inserts rows for people that do not yet exist in people_fts.
    """
    conn.execute(
        """
        INSERT INTO people_fts(
            rowid,
            company_id,
            full_name,
            first_name,
            last_name,
            title_norm,
            role_family,
            seniority,
            company_name,
            company_domain,
            attrs_text
        )
        SELECT
            p.id,
            p.company_id,
            p.full_name,
            p.first_name,
            p.last_name,
            p.title_norm,
            p.role_family,
            p.seniority,
            COALESCE(c.name_norm, c.name),
            COALESCE(c.official_domain, c.domain),
            ''
        FROM people AS p
        JOIN companies AS c
          ON c.id = p.company_id
        WHERE p.id NOT IN (
            SELECT rowid FROM people_fts
        );
        """
    )


def backfill_companies_fts(conn: sqlite3.Connection) -> None:
    """
    Backfill companies_fts from existing companies rows.

    This only inserts rows for companies that do not yet exist in companies_fts.
    """
    conn.execute(
        """
        INSERT INTO companies_fts(
            rowid,
            name_norm,
            domain,
            attrs_text
        )
        SELECT
            c.id,
            COALESCE(c.name_norm, c.name),
            COALESCE(c.official_domain, c.domain),
            ''
        FROM companies AS c
        WHERE c.id NOT IN (
            SELECT rowid FROM companies_fts
        );
        """
    )


def run_migration(db_path: str) -> None:
    db_file = Path(db_path)
    print(f"[R21] Using SQLite database at: {db_file}")

    conn = get_connection(str(db_file))
    try:
        print("[R21] Creating FTS5 virtual tables (people_fts, companies_fts) if missing ...")
        create_fts_tables(conn)

        print("[R21] Creating triggers to keep FTS indexes in sync ...")
        create_people_fts_triggers(conn)
        create_companies_fts_triggers(conn)

        print("[R21] Backfilling existing people into people_fts ...")
        backfill_people_fts(conn)

        print("[R21] Backfilling existing companies into companies_fts ...")
        backfill_companies_fts(conn)

        conn.commit()
        print("✔ R21 search indexing migration applied successfully.")
    finally:
        conn.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="R21 migration: create FTS5 search indexes and triggers for people/companies."
    )
    parser.add_argument(
        "--db",
        dest="db_path",
        default="data/dev.db",
        help="Path to SQLite database file (default: data/dev.db)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    run_migration(args.db_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
