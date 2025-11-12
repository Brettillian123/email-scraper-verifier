# scripts/backfill_o02_roles.py
from __future__ import annotations

import os
import re
import sqlite3
import sys

SENIORITY_MAP: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\b(ceo|cfo|coo|cto|cmo|cio|cpo|cso|cho|cdo)\b", re.I), "C"),
    (re.compile(r"\bchief\b", re.I), "C"),
    (re.compile(r"\b(executive\s+)?(senior|sr\.?)\s+vice\s+president\b", re.I), "VP"),
    (re.compile(r"\b(vp|vice\s+president)\b", re.I), "VP"),
    (re.compile(r"\b(dir|director)\b", re.I), "Director"),
    (re.compile(r"\b(head\s+of)\b", re.I), "Director"),
    (re.compile(r"\b(manager|mgr)\b", re.I), "Manager"),
    (re.compile(r"\b(lead|team\s+lead)\b", re.I), "Manager"),
    (re.compile(r"\b(principal|staff|senior|sr\.?)\b", re.I), "IC"),
]

ROLE_MAP: list[tuple[re.Pattern[str], str]] = [
    (
        re.compile(r"\b(sales|account\s+exec|account\s+manager|bd|business\s+development)\b", re.I),
        "Sales",
    ),
    (
        re.compile(r"\b(marketing|demand\s+gen|growth|brand|content|seo|sem|paid\s+media)\b", re.I),
        "Marketing",
    ),
    (
        re.compile(r"\b(engineer|engineering|developer|dev(ops)?|software|sre|qa|test)\b", re.I),
        "Engineering",
    ),
    (re.compile(r"\b(product\s+(manager|owner)|product\s+management|pm)\b", re.I), "Product"),
    (re.compile(r"\b(design|ux|ui|creative)\b", re.I), "Design"),
    (
        re.compile(
            r"\b(data\s+science|data\s+analyst|analytics|ml|ai|bi|business\s+intelligence)\b", re.I
        ),
        "Data",
    ),
    (re.compile(r"\b(finance|accounting|controller|fp&a|cpa)\b", re.I), "Finance"),
    (re.compile(r"\b(hr|people\s+ops|talent|recruit(ing|er)?)\b", re.I), "HR"),
    (re.compile(r"\b(operations|ops|supply\s+chain|logistics)\b", re.I), "Operations"),
    (re.compile(r"\b(customer\s+(success|support)|cs|cx|help\s*desk)\b", re.I), "Customer Success"),
    (re.compile(r"\b(it|systems?|sysadmin|security|infosec)\b", re.I), "IT"),
    (re.compile(r"\b(legal|counsel|attorney|compliance)\b", re.I), "Legal"),
]


def infer_seniority(title: str) -> str:
    for pat, label in SENIORITY_MAP:
        if pat.search(title):
            return label
    return "IC"


def infer_role_family(title: str) -> str | None:
    for pat, label in ROLE_MAP:
        if pat.search(title):
            return label
    return None


def main() -> int:
    # Accept explicit --db path or use DATABASE_URL
    db_path: str | None = None
    if "--db" in sys.argv:
        idx = sys.argv.index("--db")
        if idx + 1 < len(sys.argv):
            db_path = sys.argv[idx + 1]

    if not db_path:
        db_url = os.environ.get("DATABASE_URL", "")
        if not db_url.startswith("sqlite:///"):
            print("DATABASE_URL missing or not sqlite:///…", file=sys.stderr)
            return 2
        db_path = db_url.replace("sqlite:///", "")

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Choose title_norm if present/filled, else title_raw, else title
    # Only update rows where either field is NULL
    def existing_cols(table: str) -> set[str]:
        return {r[1] for r in cur.execute(f"PRAGMA table_info({table})")}

    cols = existing_cols("people")
    if not {"role_family", "seniority"} <= cols:
        print("people.role_family/seniority not found; did you run O02 migration?", file=sys.stderr)
        return 3

    title_expr = "COALESCE(NULLIF(title_norm,''), NULLIF(title_raw,''), NULLIF(title,''))"
    q = f"""
        SELECT id, {title_expr} AS t
        FROM people
        WHERE (role_family IS NULL OR seniority IS NULL)
          AND {title_expr} IS NOT NULL
    """
    rows = list(cur.execute(q))

    updates = 0
    for pid, title in rows:
        rf = infer_role_family(title or "")
        sr = infer_seniority(title or "")
        cur.execute(
            "UPDATE people SET role_family = COALESCE(role_family, ?), seniority = COALESCE(seniority, ?) WHERE id = ?",
            (rf, sr, pid),
        )
        updates += 1

    conn.commit()
    print(f"O02 backfill complete: updated_rows={updates} db={db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
