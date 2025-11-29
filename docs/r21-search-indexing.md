<!-- docs/r21-search-indexing.md -->

# R21 — Search Indexing Prep (SQLite FTS5)

Goal: prepare the database for fast, flexible lead search (R22) by defining:

- what text we index,
- how we keep the index fresh,
- and how callers (R22) will query it.

This document describes the SQLite implementation that mirrors the future
Postgres `tsvector + GIN` design.

---

## 1. Search document model (per-person lead)

Each *lead* in search is essentially “a person + their company + their email”.

### Text fields

These are used for full-text search (SQLite FTS5):

**People**

- `people.first_name`
- `people.last_name`
- `people.full_name`
- `people.title_norm` (fallback: `people.title`)
- `people.role_family`
- `people.seniority`

**Company (via person’s company)**

- `companies.name_norm` (fallback: `companies.name`)
- `companies.official_domain` (fallback: `companies.domain`)
- `companies.attrs` flattened into a string:
  - industry / industry label
  - size bucket
  - tech keywords (e.g. `"tech:salesforce tech:hubspot"`)

We currently store the flattened attrs in `people_fts.attrs_text`
(placeholder is `''` for now; can be backfilled later).

### Filter fields (non full-text)

These are used as structured filters on top of the text search:

- `emails.verify_status` (R18)
- `people.icp_score` (R14)
- `people.role_family`, `people.seniority`
- company attributes (from `companies.attrs` JSON):
  - size (e.g. `size_bucket`),
  - industry,
  - tech keywords
- recency:
  - `emails.verified_at` (or later `people.last_scored_at`)

---

## 2. SQLite FTS schema

R21 introduces two FTS5 virtual tables:

```sql
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
Notes:

people_fts.rowid == people.id

companies_fts.rowid == companies.id

company_name / company_domain are denormalized from companies

attrs_text is a free-form flattened text representation of attributes
(currently ''; can be populated from companies.attrs)

3. Triggers for keeping FTS fresh
R21 uses triggers to keep FTS in sync with base tables.

People → people_fts
AFTER INSERT ON people

Insert into people_fts with:

rowid = NEW.id

company_id = NEW.company_id

name + title fields from people

company name/domain via join on companies

AFTER UPDATE ON people

DELETE FROM people_fts WHERE rowid = OLD.id

Insert the updated row (same shape as insert trigger)

AFTER DELETE ON people

DELETE FROM people_fts WHERE rowid = OLD.id

Companies → companies_fts + people_fts
AFTER INSERT ON companies

Insert into companies_fts with:

rowid = NEW.id

name_norm / name

official_domain / domain

AFTER UPDATE ON companies

DELETE FROM companies_fts WHERE rowid = OLD.id

Insert updated row into companies_fts

UPDATE people_fts to refresh:

company_name

company_domain
for any rows with company_id = NEW.id

AFTER DELETE ON companies

DELETE FROM companies_fts WHERE rowid = OLD.id

All of the above are created in:

scripts/migrate_r21_search_indexing.py

That script also backfills existing data.

4. Backfill strategy
migrate_r21_search_indexing.py performs one-time backfill:

people_fts
sql
Copy code
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
JOIN companies AS c ON c.id = p.company_id
WHERE p.id NOT IN (SELECT rowid FROM people_fts);
companies_fts
sql
Copy code
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
WHERE c.id NOT IN (SELECT rowid FROM companies_fts);
5. Query helper: search_people_leads
The main search entrypoint (for R22) is:

python
Copy code
from src.search import LeadSearchParams, search_people_leads
Parameters
LeadSearchParams:

query: str — required FTS5 text query

verify_status: Sequence[str] | None — optional allowed statuses

icp_min: int | None — optional minimum ICP score

limit: int — maximum number of rows (default 50)

Behavior
search_people_leads(conn, params):

Executes FTS search:

sql
Copy code
FROM people_fts
JOIN people      ON people.id      = people_fts.rowid
JOIN v_emails_latest ve ON ve.person_id = people.id
JOIN companies   ON companies.id   = people.company_id
WHERE people_fts MATCH :query
[AND p.icp_score >= :icp_min]
[AND ve.verify_status IN (...)]
ORDER BY bm25(people_fts) ASC, email ASC
LIMIT :limit
Returns list of dicts with keys:

email

first_name, last_name, full_name

title

company

domain

source_url

verify_status

verified_at

icp_score

rank (bm25 score; lower is better)

R22’s /leads/search will call into this helper (or the SqliteFtsBackend
wrapper) instead of writing SQL directly.

6. Fuzzy matching (O21)
O21 adds a simple fuzzy company lookup on top of companies_fts:

src/search/indexing.py:

python
Copy code
def simple_similarity(a: str, b: str) -> float:
    # difflib.SequenceMatcher based similarity in [0, 1]

def fuzzy_company_lookup(conn, name: str, limit: int = 10) -> list[dict]:
    # 1) candidates from companies_fts MATCH (or LIKE fallback)
    # 2) compute similarity(query, candidate_name)
    # 3) sort by similarity desc, return top N
This simulates what a Postgres pg_trgm index will do later, and is used both
for search UX and future dedupe helpers.

7. Search backend abstraction (O13 prep)
To keep R22’s HTTP layer agnostic of the underlying search implementation, we
define:

src/search/backend.py:

python
Copy code
class SearchBackend(Protocol):
    def search(self, params: LeadSearchParams) -> list[dict]: ...
    def index_batch(self, docs: Iterable[dict]) -> None: ...

class SqliteFtsBackend(SearchBackend):
    def __init__(self, conn: sqlite3.Connection): ...
    def search(self, params: LeadSearchParams) -> list[dict]:
        return search_people_leads(conn, params)
    def index_batch(self, docs: Iterable[dict]) -> None:
        # no-op for SQLite FTS
R22 can depend on SearchBackend and use SqliteFtsBackend in dev. A future
MeilisearchBackend / OpenSearchBackend can satisfy the same protocol.

8. Dev DB workflow
To get a dev DB ready for R22 search:

powershell
Copy code
$PyExe = ".\.venv\Scripts\python.exe"
if (!(Test-Path $PyExe)) { $PyExe = "python" }

& $PyExe scripts\apply_schema.py
& $PyExe scripts\migrate_r21_search_indexing.py --db data\dev.db
R21 is considered “good” when:

FTS tables + triggers exist and backfill succeeds.

tests/test_r21_search_indexing.py passes:

basic FTS match

company name search

ICP + verify_status filters

trigger update/delete behavior

scripts/accept_r21.ps1 runs cleanly (and R20 export still passes).
