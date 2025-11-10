# R08 — Domain Resolution

## Algorithm overview
- Inputs: `company_name` (required) and optional `user_supplied_domain` hint.
- Normalize: Unicode NFKC → lowercase → trim; convert domains to IDNA/punycode for all checks; store ASCII form.
- Generate candidates from name: slugify → `{slug}.com`, `{slug}.io`, `{slug}.co`, `{slug}.net`, `{slug}.org`; dedupe.
- Validate hint (if present):
  - Reject if in denylist (see Guardrails).
  - Otherwise probe via DNS (A/AAAA/MX) and HTTP (follow up to 3 redirects).
- Probe candidates:
  - **DNS-valid** if any A/AAAA/MX resolves for apex.
  - **HTTP OK** if `http://{candidate}` (or HTTPS upgrade) returns 2xx on the same registrable domain.
  - **HTTP redirect-validated** if clean 301/302/308 ends at the same registrable domain.
- Score & decide (see rubric). Choose highest score; tie-breakers: `http_ok` > `http_redirect` > `dns_valid` > name-only, then shorter registrable domain, then lexicographic.
- Persist idempotently to `companies.official_*` fields only. Multi-brand allowed (same `official_domain` across companies).
- Versioning: record resolver version in `official_domain_source` (e.g., `r08.4:http_ok`).

## Confidence rubric
- **http_ok:** 100
- **http_redirect:** 90
- **hint_validated:** 85
- **dns_valid:** 70
- **fallback:** 0  (no evidence → do not persist)
- Accept if score ≥ 80; otherwise leave unresolved.

## Guardrails
- **Denylist:** common freemail/consumer/hosts (e.g., `gmail.com`, `yahoo.com`, `outlook.com`, `hotmail.com`, `icloud.com`, `aol.com`, `proton.me`, `protonmail.com`, `zoho.com`, `yandex.com`).
- **Timeouts:** DNS ≤ 2s; HTTP total ≤ ~3s; ≤ 3 redirects; use bundled PSL only (no network fetch).
  - Note: during local dev you may temporarily bump timeouts for flaky networks; defaults remain tight.
- **IDN policy:** normalize to punycode for all network ops; store ASCII punycode; reject mixed-script lookalikes.
- **Redirect safety:** treat redirects as validated only if the final **registrable domain** matches the candidate’s registrable domain.

## Schema changes and fields written
- `companies.official_domain` (TEXT) — chosen canonical domain.
- `companies.official_domain_source` (TEXT) — `${RESOLVER_VERSION}:${method}` (e.g., `r08.4:http_ok`, `r08.4:http_redirect`, `r08.4:dns_valid`, `r08.4:hint_validated`).
- `companies.official_domain_confidence` (INTEGER 0–100).
- `companies.official_domain_checked_at` (TEXT, UTC ISO8601).
- `companies.user_supplied_domain` (TEXT) — preserved as hint; never overwritten.
- Keep the index on `user_supplied_domain`; do **not** make `official_domain` unique.

## Ops runbook (drain backlog with scripts/resolve_domains.py)
- Run from the repo root (module form) or ensure `PYTHONPATH` points to the repo root.
- Dry run (no writes):
  - `python -m scripts.resolve_domains --dry-run --limit 100 --db data/dev.db`
- Process everything pending:
  - `python -m scripts.resolve_domains --all --db data/dev.db --busy-timeout-ms 5000`
- Chunked passes (repeat until empty):
  - `python -m scripts.resolve_domains --limit 500 --db data/dev.db --busy-timeout-ms 5000`
- Tip: you can also set `DATABASE_PATH` to target a specific DB.
