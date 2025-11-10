import sqlite3
import sys

db = sys.argv[1] if len(sys.argv) > 1 else "dev_ingest.db"
conn = sqlite3.connect(db)
conn.row_factory = sqlite3.Row


def cols(table):
    try:
        return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.OperationalError:
        return set()


pc = cols("people")
cc = cols("companies")
ic = cols("ingest_items")

selects = []
joins = []
# full_name
if "full_name" in pc:
    selects.append("p.full_name AS full_name")
elif {"first_name", "last_name"} <= pc:
    selects.append("TRIM(p.first_name || ' ' || p.last_name) AS full_name")
elif "first_name" in pc:
    selects.append("p.first_name AS full_name")
else:
    selects.append("NULL AS full_name")

# company name
if "company" in pc:
    selects.append("p.company AS company")
elif "company_id" in pc and {"id", "name"} <= cc:
    selects.append("c.name AS company")
    joins.append("LEFT JOIN companies c ON p.company_id = c.id")
else:
    selects.append("NULL AS company")

# user_supplied_domain
usd = "'' AS user_supplied_domain"
if "user_supplied_domain" in pc:
    usd = "p.user_supplied_domain AS user_supplied_domain"
elif "user_supplied_domain" in ic:
    if "person_id" in ic:
        joins.append("LEFT JOIN ingest_items ii ON ii.person_id = p.id")
        usd = "ii.user_supplied_domain AS user_supplied_domain"
    elif "people_id" in ic:
        joins.append("LEFT JOIN ingest_items ii ON ii.people_id = p.id")
        usd = "ii.user_supplied_domain AS user_supplied_domain"
    elif (
        {"full_name"} <= ic
        and "full_name" in pc
        and ("company" in ic or {"company_id", "id", "name"} <= (pc | cc))
    ):
        # best-effort match on name + company if available
        if any(j.startswith("LEFT JOIN companies") for j in joins) and "company" in ic:
            joins.append(
                "LEFT JOIN ingest_items ii ON ii.full_name = p.full_name AND ii.company = c.name"
            )
            usd = "ii.user_supplied_domain AS user_supplied_domain"
        elif "company" in ic and "company" in pc:
            joins.append(
                "LEFT JOIN ingest_items ii ON ii.full_name = p.full_name AND ii.company = p.company"
            )
            usd = "ii.user_supplied_domain AS user_supplied_domain"
        else:
            joins.append("LEFT JOIN ingest_items ii ON ii.full_name = p.full_name")
            usd = "ii.user_supplied_domain AS user_supplied_domain"
selects.append(usd)

# Show quick counts
for t in ("companies", "people", "ingest_items"):
    try:
        n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"{t} =>", n)
    except Exception as e:
        print(f"{t} => missing ({e})")

sql = f"SELECT {', '.join(selects)} FROM people p {' '.join(joins)} LIMIT 10"
print("\nQUERY:\n", sql, "\n")
try:
    rows = conn.execute(sql).fetchall()
    for r in rows:
        print(dict(r))
except Exception as e:
    print("query failed:", e)

conn.close()
