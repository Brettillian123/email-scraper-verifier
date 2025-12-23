# /leads/search API (R22 + R23 + O26)

Lead search HTTP endpoint backed by the SQLite FTS + ICP/verification stack.

- **Checklist item:** R22 – Lead Search API (/leads/search)
- **Checklist item:** R23 – Faceting & counts
- **Optional:** O15 – Query/cache layer for repeated searches (15–60 min TTL)
- **Optional:** O14 – Materialized view for fast facets
- **Optional:** O26 – Verification labels + primary/alternate selection
- **Status:** R22/R23 contract for current SQLite-based implementation; designed to be stable enough for future Postgres or search-engine backends.

---

## 1. Overview

`/leads/search` returns a filtered, ranked list of leads (people + email + company context) based on:

- Free-text keyword search over names, titles, and company names.
- ICP & verification signals:
  - `icp_score`
  - `verify_status`
  - role / seniority
  - company size / industry / tech keywords
- Recency of verification.
- Sort order with **keyset pagination** (opaque cursor).
- **Facet counts (R23)** over the filtered universe of leads:
  - `verify_status`
  - `icp_bucket` (0–39, 40–59, 60–79, 80–100)
  - `role_family`
  - `seniority`
  - `company_size_bucket`
  - `company_industry`
  - (optionally `tech_keyword` in future extensions)
- **Verification labels (O26)**:
  - `verify_label` – second-dimension label on top of `verify_status` (native vs catch-all tested, primary vs alternate).
  - `is_primary_for_person` – marks the canonical “primary” email for a person when one exists.

**Non-goals for R22/R23:**

- No total hit count.
- No multi-tenant auth yet (will be handled later with R23/R24/R27).

Clients should treat the API contract in this document as stable and avoid depending on any additional fields not documented here.

---

## 2. Endpoint summary

- **Method:** `GET`
- **Path:** `/leads/search`
- **Content type:** `application/json`
- **Auth:** Same as existing internal API endpoints in `src/api/app.py` (currently minimal / internal-only).

---

## 3. Query parameters

All parameters are **optional** unless noted otherwise. List-type parameters are passed as comma-separated strings and parsed server-side.

### 3.1 Parameter reference

| Name            | Type                     | Required | Default    | Example                                                                                      | Notes |
|-----------------|--------------------------|----------|------------|----------------------------------------------------------------------------------------------|-------|
| `q`             | string                   | no       | `""`       | `q=sales+operations`                                                                         | Free-text query over person name, title, company name, and any additional indexed text. |
| `verify_status` | comma-separated strings  | no       | (any)      | `verify_status=valid,risky_catch_all`                                                       | Filters by canonical R18 statuses. Unknown values are rejected with 400. |
| `icp_min`       | integer                  | no       | `70`       | `icp_min=80`                                                                                 | Minimum ICP score. If omitted, API uses a recommended default (e.g. 70). |
| `roles`         | comma-separated strings  | no       | (any)      | `roles=sales,marketing,revops`                                                               | Filters by canonical `role_family` values (from title normalization / O02). |
| `seniority`     | comma-separated strings  | no       | (any)      | `seniority=director,vp,cxo`                                                                 | Filters by canonical `seniority` values. |
| `industries`    | comma-separated strings  | no       | (any)      | `industries=B2B%20SaaS,Fintech`                                                              | Filters by company industry tags (from `companies.attrs`). |
| `sizes`         | comma-separated strings  | no       | (any)      | `sizes=1-10,11-50,51-200`                                                                    | Filters by company size buckets (from `companies.attrs`, e.g. `51-200`). |
| `tech`          | comma-separated strings  | no       | (any)      | `tech=salesforce,hubspot`                                                                    | Filters by detected tech keywords (from `companies.attrs.tech_keywords`). |
| `source`        | comma-separated strings  | no       | (any)      | `source=published,generated`                                                                 | Filters by lead source: typically `published` (R11) vs `generated` (R12). |
| `recency_days`  | integer                  | no       | (no limit) | `recency_days=30`                                                                            | Only include leads verified/seen in the last N days. Uses `verified_at` primarily. |
| `sort`          | string                   | no       | `icp_desc` | `sort=icp_desc` or `sort=verified_desc`                                                     | Sort order; **R22 supports only `icp_desc` (default) and `verified_desc`**. Others are rejected with 400. |
| `limit`         | integer                  | no       | `50`       | `limit=25`                                                                                   | Page size. Clamped to `1 ≤ limit ≤ 100`. |
| `cursor`        | string (opaque)          | no       | —          | `cursor=cursor_example_token`                                                               | Keyset pagination cursor returned from a previous page. Treated as opaque by clients. |
| `facets`        | comma-separated strings  | no       | (none)     | `facets=verify_status,icp_bucket,role_family`                                               | **R23:** which facet dimensions to compute counts for under the current filters. Unknown facet names are ignored. |

### 3.2 Notes on list parameters

All list-like parameters are provided as comma-separated strings:

- `verify_status=valid,risky_catch_all`
- `roles=sales,revops`
- `seniority=director,vp`
- `industries=B2B%20SaaS,Fintech`
- `sizes=51-200,201-500`
- `tech=salesforce,hubspot`
- `source=generated,published`
- `facets=verify_status,icp_bucket,role_family`

The API:

- Splits on `,`.
- Trims surrounding whitespace for each value.
- Rejects empty values (e.g. `roles=,sales,` is invalid).

If any value in a filter is invalid (e.g. unknown `verify_status` or unsupported `sort`), the request returns `400 Bad Request` with a structured error payload (see §6). Unknown `facets` values are simply ignored; they do not cause an error.

---

## 4. Sorting and keyset pagination

### 4.1 Supported sort orders (R22)

R22 supports two sort modes:

- `icp_desc` (**default**)
  - Primary key: `icp_score` descending.
  - Tie-breaker: `people.id` ascending.
- `verified_desc`
  - Primary key: `verified_at` descending (newest first).
  - Tie-breaker: `people.id` ascending.

If an unsupported `sort` value is provided (e.g. `icp_asc`, `created_desc`), the API returns **400** with `error="invalid_sort"`.

### 4.2 Keyset strategy

To avoid gaps/dupes and keep queries efficient, `/leads/search` uses **keyset pagination** instead of offset-based pagination.

For each sort mode, the order and cursor are:

- **`icp_desc`**
  - ORDER BY: `icp_score DESC, person_id ASC`
  - Cursor fields: `icp_score`, `person_id`
- **`verified_desc`**
  - ORDER BY: `verified_at DESC, person_id ASC`
  - Cursor fields: `verified_at`, `person_id`

The server enforces a keyset predicate on subsequent pages using the cursor. Example for `icp_desc`:

```sql
AND (
  people.icp_score < :cursor_icp
  OR (people.icp_score = :cursor_icp AND people.id > :cursor_person_id)
)
ORDER BY people.icp_score DESC, people.id ASC
LIMIT :limit
Clients never see the raw SQL; they just pass the opaque cursor string.

4.3 Cursor encoding format
The cursor is a URL-safe base64-encoded JSON object. Clients MUST treat it as opaque; the structure below is documented only for debugging and future compatibility.

For sort=icp_desc:

jsonc
Copy code
{
  "sort": "icp_desc",
  "icp_score": 87,
  "person_id": 123
}
For sort=verified_desc:

jsonc
Copy code
{
  "sort": "verified_desc",
  "verified_at": "2025-11-01T15:24:16Z",
  "person_id": 123
}
This JSON is encoded via base64.urlsafe_b64encode. The server will restore padding if missing.

Server behavior:

If cursor is absent:

Returns the first page.

May be eligible for caching (O15).

If cursor is present:

Decodes the cursor.

Validates that sort in the cursor matches the requested sort (or default icp_desc).

Applies the appropriate keyset predicate.

Cursor pages are not cached in O15 to keep cache behavior simple.

If the cursor is malformed or inconsistent with the sort order, the API returns 400 with error="invalid_cursor".

5. Response schema
5.1 Top-level shape
jsonc
Copy code
{
  "results": [
    {
      "email": "alice@example.com",
      "first_name": "Alice",
      "last_name": "Nguyen",
      "full_name": "Alice Nguyen",
      "title": "VP Sales",
      "role_family": "sales",
      "seniority": "vp",
      "company": "Crestwell Partners",
      "company_id": 123,
      "company_domain": "crestwellpartners.com",
      "industry": "B2B SaaS",
      "company_size": "51-200",
      "tech": ["salesforce", "hubspot"],
      "icp_score": 87,
      "verify_status": "valid",
      "verify_label": "valid_native_primary",
      "is_primary_for_person": true,
      "verified_at": "2025-11-01T15:24:16Z",
      "source": "generated",
      "source_url": "https://example.com/team/alice"
    }
  ],
  "limit": 50,
  "sort": "icp_desc",
  "next_cursor": "cursor_example_token",
  "facets": {
    "verify_status": [
      { "value": "valid", "count": 10 },
      { "value": "risky_catch_all", "count": 3 },
      { "value": "invalid", "count": 1 }
    ],
    "icp_bucket": [
      { "value": "80-100", "count": 8 },
      { "value": "60-79", "count": 5 },
      { "value": "40-59", "count": 1 }
    ]
  }
}
Notes:

results: array of lead objects (possibly empty).

limit: the effective page size used for this response.

sort: the effective sort used (icp_desc or verified_desc).

next_cursor:

String cursor for the next page, if more results are available.

null if there are no more pages.

The presence of a non-null next_cursor is the only indicator that another page exists. There is no total count.

facets (R23):

Object keyed by facet name.

Each value is a list of { "value": <bucket>, "count": <integer> }.

Omitted or {} if no facets were requested (facets parameter not provided).

5.2 Lead object fields
Field	Type	Description
email	string	Primary email address for the lead.
first_name	string | null	Normalized first name (from R13/O09).
last_name	string | null	Normalized last name.
full_name	string | null	Concatenated full name, if available.
title	string | null	Normalized job title (user-facing, from title_norm when available).
role_family	string | null	Canonical role bucket (e.g. sales, marketing, revops, cs, ops).
seniority	string | null	Canonical seniority (e.g. ic, manager, director, vp, cxo).
company	string | null	Company name (normalized display form).
company_id	integer | null	Internal company ID. Included for debugging/deep-linking; stable but internal.
company_domain	string | null	Canonical company domain (usually official_domain or derived from email).
industry	string | null	Main industry label (from companies.attrs.industry).
company_size	string | null	Size bucket (e.g. 1-10, 11-50, 51-200, 201-500, 500+).
tech	string[]	List of tech keywords for the company (from companies.attrs.tech_keywords).
icp_score	integer | null	ICP score (0–100) from R14.
verify_status	string | null	Canonical verify status from R18 (valid, risky_catch_all, invalid, unknown_timeout, etc.).
verify_label	string | null	O26 verification label. Common values: valid_native_primary, valid_native_alternate, valid_catchall_tested_primary, valid_catchall_tested_alternate, plus non-valid labels (invalid, risky_catch_all, unknown_timeout, unknown). See §5.4.
is_primary_for_person	boolean | null	O26 primary/alternate flag. true for the canonical primary valid email for a person, false for other emails for that person, null/absent when no valid primary exists for that person.
verified_at	string | null	ISO 8601 timestamp when verification last ran for this email (UTC, ...Z).
source	string | null	Lead origin, typically published (R11 extraction) or generated (R12 permutations).
source_url	string | null	Provenance URL (page where the email was found, or a canonical company/person URL).

Additional fields can be added in future versions, but existing fields and semantics should remain stable.

5.3 Facets object (R23)
The facets object is present when the client provides a non-empty facets parameter.

Shape:

jsonc
Copy code
{
  "facets": {
    "verify_status": [
      { "value": "valid", "count": 123 },
      { "value": "risky_catch_all", "count": 45 },
      { "value": "invalid", "count": 17 }
    ],
    "icp_bucket": [
      { "value": "80-100", "count": 90 },
      { "value": "60-79", "count": 55 },
      { "value": "40-59", "count": 8 },
      { "value": "0-39", "count": 4 }
    ],
    "role_family": [
      { "value": "sales", "count": 70 },
      { "value": "marketing", "count": 20 }
    ]
  }
}
Each facet key corresponds to one of the allowed facet names:

verify_status

icp_bucket

role_family

seniority

company_size_bucket

company_industry

For icp_bucket, the server uses fixed buckets:

0-39, 40-59, 60-79, 80-100.

Values with count = 0 are omitted. Null values are omitted (e.g. leads missing that attribute).

5.4 Verification labels (O26)
verify_label is a derived, second-dimension label on top of verify_status. It lets clients distinguish:

Native vs catch-all-tested valids.

Primary vs alternate emails for the same person.

Common values:

For valid emails:

valid_native_primary – native valid, chosen as the canonical primary email for this person.

valid_native_alternate – native valid, but secondary/alternate for this person.

valid_catchall_tested_primary – valid via O26 test-send on a catch-all domain, chosen as primary.

valid_catchall_tested_alternate – valid via O26 test-send, but alternate for this person.

For non-valid statuses:

invalid

risky_catch_all

unknown_timeout

unknown (fallback for any unrecognized combination; should be rare).

is_primary_for_person works in tandem:

When there is at least one valid email for a person:

Exactly one row per person is marked with is_primary_for_person = true.

Other rows for that person have is_primary_for_person = false.

When there are no valid emails for a person:

is_primary_for_person is omitted / null, and verify_label reflects the coarse status (e.g. invalid, risky_catch_all).

Clients that only want “best guess” primary emails can filter on:

verify_status = "valid" AND is_primary_for_person = true.

6. Error responses
Errors are returned as JSON with an HTTP 4xx/5xx status.

6.1 400 Bad Request
Used for invalid query parameters (type errors, unsupported values, malformed cursor, etc.).

json
Copy code
{
  "error": "invalid_sort",
  "detail": "sort must be one of: icp_desc, verified_desc"
}
Common error codes:

invalid_sort – sort not in the supported list.

invalid_limit – limit not parseable as integer or out of bounds.

invalid_icp_min – icp_min not parseable as integer.

invalid_recency_days – recency_days not parseable as integer.

invalid_cursor – cursor cannot be decoded or does not match requested sort.

invalid_verify_status – unknown verify_status value.

invalid_roles / invalid_seniority – unknown role/seniority.

invalid_param – generic parameter error; detail will specify which param and why.

Unknown facets values are ignored rather than causing an error.

6.2 500 Internal Server Error
Unexpected server-side errors (DB issues, etc.):

json
Copy code
{
  "error": "internal_error",
  "detail": "unexpected error while processing search"
}
The server logs more detail internally. Clients should treat this as retryable in many cases.

7. Example requests
7.1 Basic high-ICP valid leads
http
Copy code
GET /leads/search?q=sales&verify_status=valid&icp_min=80&limit=25 HTTP/1.1
Accept: application/json
Returns at most 25 leads:

All have verify_status="valid" and icp_score >= 80.

Sorted by icp_desc (default).

Each lead includes verify_label and (for valid leads) is_primary_for_person.

7.2 SaaS sales leadership, recent verification, with facets
http
Copy code
GET /leads/search?\
q=account%20executive&\
verify_status=valid,risky_catch_all&\
icp_min=70&\
roles=sales,revops&\
seniority=director,vp,cxo&\
industries=B2B%20SaaS&\
sizes=51-200,201-500&\
tech=salesforce,hubspot&\
source=generated,published&\
recency_days=30&\
sort=icp_desc&\
facets=verify_status,icp_bucket,role_family&\
limit=50 HTTP/1.1
Accept: application/json
Example use case: “Give me SaaS GTM leadership at mid-sized companies using Salesforce/HubSpot, recently verified, and show me how they break down by verify_status, ICP bucket, and role family.”

7.3 Pagination with cursor
First page:

http
Copy code
GET /leads/search?q=marketing&icp_min=70&limit=20 HTTP/1.1
Accept: application/json
Response (truncated):

jsonc
Copy code
{
  "results": [ /* 20 leads */ ],
  "limit": 20,
  "sort": "icp_desc",
  "next_cursor": "cursor_example_token",
  "facets": { "verify_status": [ /* counts for page-1 universe */ ] }
}
Second page:

http
Copy code
GET /leads/search?q=marketing&icp_min=70&limit=20&cursor=cursor_example_token HTTP/1.1
Accept: application/json
Server uses the cursor to continue after the last lead from page 1:

page1 and page2 IDs will not overlap.

By design, facets are typically computed only on the first page; subsequent pages may omit facets or return {}.

When next_cursor becomes null, there are no further pages.

8. O15: Query/cache layer behavior (summary)
R22 pairs with O15 to avoid hammering the DB for common queries.

High-level cache rules:

Only first pages are cached (cursor is absent and no keyset fields set).

The cache key is derived from a normalized representation of:

text
Copy code
q, verify_status, icp_min, roles, seniority,
industries, sizes, tech, source, recency_days,
sort, limit, facets
The normalized key is JSON-encoded with sorted keys and then hashed (e.g. SHA-256), producing something like:

text
Copy code
leads_search:<hash>
TTL: ~15 minutes to start (e.g. 900 seconds).

On cache hit:

Returns the cached SearchResult (leads + facets + next_cursor) directly.

On cache miss:

Runs the search against the backend.

Writes the result object into cache with TTL.

Returns the result.

Cursor pages (cursor present) always bypass the cache.

Implementation details live in src/search/cache.py, but this document defines the observable behavior: callers do not need to know whether a specific response was cached.

9. Future extensions
Planned but not part of current R22/R23 scope:

Additional sort orders:

icp_asc, created_desc, etc.

Additional / higher-cardinality facets:

e.g. tech_keyword, geo/region facets, etc.

Total hit count:

Either approximate or exact count.

Per-tenant auth and API keys (R23/R24/R27).

Richer materialized views and/or external search engines:

O14 introduces a simple lead_search_docs materialized table for facets;

future versions may expand this or mirror into Postgres/Search-as-a-service.

When these are introduced, this document will be extended but existing fields/behavior will remain backward compatible wherever possible.
