<!-- docs/r21-search-mirror-schema.md -->

# R21 / O13 — Search Mirror Document Schema

This document defines the JSON document shape used when exporting leads for an
external search engine (Meilisearch / OpenSearch / etc.).

R21 delivers:

- SQLite FTS-based search for dev (and small prod).
- A JSONL export (`scripts/export_for_search.py`) that mirrors what a
  future search engine index will store.

O13 later wires this up to a real search service, but the schema is defined
**now** so R22/R23 can depend on it.

---

## 1. High-level idea

Each document represents a single **lead**, i.e.:

> one email address + its person + its company

The export script pulls from:

- `v_emails_latest` (email + verify status + recency)
- `people` (name, title, ICP, role, seniority)
- `companies` (name, domain, attrs JSON)

The document is intentionally close to what `/leads/search` will want to
return as a result row.

---

## 2. Document shape

### 2.1 Top-level JSON

Each line in the JSONL output is a single document with this shape:

```json
{
  "id": "email:<emails.id>",
  "email": "alice.anderson@crestwellpartners.com",

  "first_name": "Alice",
  "last_name": "Anderson",
  "full_name": "Alice Anderson",

  "title": "VP of Sales",
  "role_family": "sales",
  "seniority": "vp",

  "company": "Crestwell Partners",
  "domain": "crestwellpartners.com",

  "verify_status": "valid",
  "icp_score": 92,

  "industry": "B2B SaaS",
  "company_size": "50-200",
  "tech_keywords": ["salesforce", "hubspot"],

  "verified_at": "2025-11-28T20:00:00Z",
  "source_url": "https://example.com/source"
}
2.2 Field-by-field spec
Field	Type	Required	Source	Notes
id	string	yes	"email:" + emails.id	Stable unique identifier.
email	string	yes	v_emails_latest.email	Primary contact email.
first_name	string or null	no	people.first_name	May be null / empty.
last_name	string or null	no	people.last_name	May be null / empty.
full_name	string or null	no	people.full_name or derived	Fallback: "first last".
title	string or null	no	people.title_norm or people.title	Prefer normalized title if present.
role_family	string or null	no	people.role_family	From O02 normalization.
seniority	string or null	no	people.seniority	From O02 normalization.
company	string or null	no	companies.name_norm or companies.name	Display name for company.
domain	string or null	no	companies.official_domain or companies.domain	Canonical domain if available.
verify_status	string or null	no	v_emails_latest.verify_status	R18 canonical verify status.
icp_score	integer or null	no	people.icp_score	R14 ICP score (0–100 etc.).
industry	string or null	no	companies.attrs.industry or industry_label	Derived from JSON attrs.
company_size	string or null	no	companies.attrs.size_bucket or company_size	E.g. "1-10", "11-50", "51-200".
tech_keywords	array of string	no	companies.attrs.tech_keywords / tech_stack	Normalized to list of strings.
verified_at	string or null	no	v_emails_latest.verified_at	ISO 8601 timestamp (string).
source_url	string or null	no	prefer email → person → company	Chosen in that priority order.

Nullability rules

For optional enrichment fields (industry, size, tech, ICP, etc.), it is
acceptable to emit null if the underlying data is missing.

email and id should always be present.

3. Mapping from SQL (current implementation)
scripts/export_for_search.py currently uses:

sql
Copy code
SELECT
  ve.id             AS email_id,
  ve.email          AS email,
  ve.person_id      AS person_id,
  ve.verify_status  AS verify_status,
  ve.verified_at    AS verified_at,
  ve.source_url     AS email_source_url,

  p.first_name      AS first_name,
  p.last_name       AS last_name,
  p.full_name       AS full_name,
  p.title           AS title,
  p.title_norm      AS title_norm,
  p.role_family     AS role_family,
  p.seniority       AS seniority,
  p.icp_score       AS icp_score,
  p.source_url      AS person_source_url,

  c.id              AS company_id,
  c.name            AS company_name_raw,
  c.name_norm       AS company_name_norm,
  c.domain          AS company_domain_raw,
  c.official_domain AS company_domain_official,
  c.website_url     AS company_website_url,
  c.attrs           AS company_attrs
FROM v_emails_latest AS ve
JOIN people AS p   ON p.id = ve.person_id
JOIN companies AS c ON c.id = p.company_id;
Then it builds the document as:

id = "email:" + email_id

email = email

full_name:

if p.full_name is non-empty, use it

else build from first_name + last_name if available

title:

p.title_norm if non-null

else p.title

company:

company_name_norm if non-null

else company_name_raw

domain:

company_domain_official if non-null

else company_domain_raw

source_url:

email-level source_url if present

else person-level

else company website URL

3.1 company_attrs → derived fields
c.attrs is expected to be JSON (TEXT in SQLite):

json
Copy code
{
  "industry": "B2B SaaS",
  "size_bucket": "50-200",
  "tech_keywords": ["salesforce", "hubspot"]
}
The export script attempts:

industry:

attrs["industry"] or attrs["industry_label"]

company_size:

attrs["size_bucket"] or attrs["company_size"]

tech_keywords:

attrs["tech_keywords"] or attrs["tech_stack"]

coerced to list[str] if it’s a list

If JSON parsing fails or keys are missing, the derived fields remain null/[].

4. Intended index configuration (Meilisearch / OpenSearch)
This section is advisory for future O13 work; it doesn’t affect R21.

4.1 Meilisearch (example)
Index name: leads

Primary key: id

Searchable attributes (full-text):

full_name

email

title

company

domain

tech_keywords

(optionally) industry

Filterable attributes:

verify_status

icp_score

role_family

seniority

industry

company_size

tech_keywords

domain

Sortable attributes:

icp_score

verified_at

Meilisearch provides built-in typo tolerance / fuzzy search, so O21 trigram
logic is primarily for SQLite/Postgres dev flows.

4.2 OpenSearch / Elasticsearch (example)
Index name: leads

ID: id field

Use keyword vs text fields appropriately:

email, domain, verify_status, role_family, seniority,
industry, company_size → keyword

full_name, title, company → text (with analyzers)

tech_keywords → keyword (multi-valued)

icp_score → integer

verified_at → date

Optional: add trigram/phonetic analyzers on company / full_name to match
the behavior of fuzzy_company_lookup.

5. Example JSONL export
Running:

bash
Copy code
python scripts/export_for_search.py --db data/dev.db --out tmp/search_docs.jsonl
produces lines like:

json
Copy code
{"id":"email:1","email":"alice.anderson@crestwellpartners.com","first_name":"Alice","last_name":"Anderson","full_name":"Alice Anderson","title":"VP of Sales","role_family":"sales","seniority":"vp","company":"Crestwell Partners","domain":"crestwellpartners.com","verify_status":"valid","icp_score":92,"industry":"B2B SaaS","company_size":"50-200","tech_keywords":["salesforce","hubspot"],"verified_at":"2025-11-28T20:00:00Z","source_url":"https://example.com/profile"}
{"id":"email:2","email":"bob.brown@otherco.com","first_name":"Bob","last_name":"Brown","full_name":"Bob Brown","title":"Head of Marketing","role_family":"marketing","seniority":"director","company":"OtherCo","domain":"otherco.com","verify_status":"valid","icp_score":45,"industry":null,"company_size":null,"tech_keywords":[],"verified_at":"2025-11-27T16:10:00Z","source_url":"https://otherco.com"}
This file can be:

bulk-imported into Meilisearch/OpenSearch, or

used as a snapshot for local search experiments.

6. Change management
Because /leads/search and any external search engine will depend on this
schema:

Additive fields (new keys) are safe as long as they have sensible defaults.

Renaming or removing fields must be treated as a breaking change:

bump a search schema version in code/config,

or introduce new fields while deprecating old ones gradually.

The current R21/O13 implementation treats this document shape as v1 of the
search mirror schema.
