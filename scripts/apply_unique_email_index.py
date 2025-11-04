#!/usr/bin/env python
# (no typing imports needed; use builtins)

import argparse
import os
import shutil
import sqlite3
import sys
import time

SQL_DUP_CHECK = """
SELECT email, COUNT(*) AS c
FROM emails
GROUP BY email
HAVING c > 1;
"""

SQL_DUP_ROWS = """
SELECT id, company_id, email, created_at
FROM emails
WHERE email = ?
ORDER BY id DESC;
"""

SQL_DROP_NONUNIQUE = "DROP INDEX IF EXISTS idx_emails_email;"
SQL_CREATE_UNIQUE = "CREATE UNIQUE INDEX IF NOT EXISTS ux_emails_email ON emails(email);"


def resolve_db_from_env() -> str:
    url = os.environ.get("DATABASE_URL", "")
    if not url.startswith("sqlite:///"):
        raise RuntimeError(f"DATABASE_URL must start with sqlite:/// (got: {url or 'unset'})")
    return url.removeprefix("sqlite:///")


def backup_db(db_path: str) -> str:
    ts = time.strftime("%Y%m%d_%H%M%S")
    bak = f"{db_path}.{ts}.bak"
    shutil.copyfile(db_path, bak)
    return bak


def find_duplicates(cur: sqlite3.Cursor) -> list[tuple[str, int]]:
    return list(cur.execute(SQL_DUP_CHECK).fetchall())


def auto_fix_duplicates(cur: sqlite3.Cursor, dups: list[tuple[str, int]]) -> int:
    """
    Keep the highest id per email, delete the rest.
    Returns number of rows deleted.
    """
    deleted = 0
    for email, _ in dups:
        rows = cur.execute(SQL_DUP_ROWS, (email,)).fetchall()  # id DESC
        _keep_id = rows[0][0]  # highest id (documented, not used)
        to_delete = [r[0] for r in rows[1:]]
        if to_delete:
            cur.executemany("DELETE FROM emails WHERE id=?", [(i,) for i in to_delete])
            deleted += len(to_delete)
            # Optional: you could merge/repair FK references here if needed.
        # NOTE: If you want a different policy (e.g., keep latest created_at), change ORDER BY above.
    return deleted


def ensure_unique_index(cur: sqlite3.Cursor) -> None:
    cur.execute(SQL_DROP_NONUNIQUE)
    cur.execute(SQL_CREATE_UNIQUE)


def print_index_list(cur: sqlite3.Cursor) -> None:
    rows = cur.execute("PRAGMA index_list(emails);").fetchall()
    print("PRAGMA index_list(emails):")
    for r in rows:
        # r: (seq, name, unique, origin, partial)
        print("  ", r)


def main():
    ap = argparse.ArgumentParser(
        description="Apply unique index ux_emails_email on emails(email) with safety checks."
    )
    ap.add_argument(
        "--db",
        help="Path to sqlite DB file (defaults to DATABASE_URL=sqlite:///...)",
        default=None,
    )
    ap.add_argument(
        "--auto-fix-duplicates",
        action="store_true",
        help="Automatically keep the highest id per duplicate email and delete others.",
    )
    ap.add_argument(
        "--no-backup",
        action="store_true",
        help="Do not create a .bak backup (not recommended).",
    )
    args = ap.parse_args()

    try:
        db_path = args.db or resolve_db_from_env()
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(2)

    if not os.path.exists(db_path):
        print(f"[ERROR] DB file not found: {db_path}", file=sys.stderr)
        sys.exit(2)

    if not args.no_backup:
        bak = backup_db(db_path)
        print(f"[INFO] Backup created: {bak}")

    try:
        con = sqlite3.connect(db_path)
        cur = con.cursor()

        # sanity: emails table exists?
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='emails';")
        if not cur.fetchone():
            print("[ERROR] Table 'emails' not found in DB.", file=sys.stderr)
            sys.exit(2)

        # 1) check duplicates
        dups = find_duplicates(cur)
        if dups:
            print("[WARN] Duplicate email values detected:")
            for email, count in dups:
                print(f"  {email}  (count={count})")

            if args.auto_fix_duplicates:
                deleted = auto_fix_duplicates(cur, dups)
                print(f"[INFO] Auto-fixed duplicates; deleted {deleted} row(s).")
            else:
                print(
                    "[ERROR] Aborting because duplicates exist. Re-run with --auto-fix-duplicates to resolve automatically, or fix manually and re-run."
                )
                sys.exit(1)

        # 2) apply index changes
        ensure_unique_index(cur)
        con.commit()
        print("[OK] Applied unique index: ux_emails_email ON emails(email)")

        # 3) verify
        print_index_list(cur)

        # quick constraint check: try a duplicate within a transaction and roll back
        cur.execute("BEGIN")
        try:
            cur.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM companies")
            next_co_id = cur.fetchone()[0]
            cur.execute(
                "INSERT OR IGNORE INTO companies (id, domain) VALUES (?, ?)",
                (next_co_id, "tmp.local"),
            )
            cur.execute(
                "INSERT INTO emails (company_id, email) VALUES (?, ?)",
                (next_co_id, "unique_test@example.com"),
            )
            try:
                cur.execute(
                    "INSERT INTO emails (company_id, email) VALUES (?, ?)",
                    (next_co_id, "unique_test@example.com"),
                )
                print("[ERROR] Duplicate insert unexpectedly succeeded (index not enforcing).")
            except sqlite3.IntegrityError:
                print("[OK] Duplicate insert correctly failed with IntegrityError.")
        finally:
            cur.execute("ROLLBACK")

        con.close()
        sys.exit(0)

    except sqlite3.Error as e:
        print(f"[ERROR] SQLite error: {e}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
