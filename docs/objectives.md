# Email Scraper/Verifier — Objectives & Guardrails (v1)

## Objectives (what success looks like)
- Build a low-cost MVP that produces a verified lead list for our ICP from allowed web sources.
- Automate verification + scoring to reduce manual cleanup to near-zero.

## Compliance & Scope
- Robots/ToS: Crawl only pages allowed by robots.txt; never access login/gated content.
- Identification: User-Agent = "EmailScraperBot/0.1 (+mailto:banderson@crestwellpartners.com)"
- Contact for site owners: banderson@crestwellpartners.com
- Allowed sources: /about, /team, /contact, newsroom/press, legal notice pages.
- Suppression policy:
  - Suppress role/distribution addresses (e.g., admin@, info@, sales@, support@, help@, hello@, hr@).
  - Suppress any email/domain in explicit opt-out records.
  - Never re-verify suppressed emails/domains.

## Success Metrics (initial targets)
- Valid rate ≥ 70% of verified emails.
- Cost per 1k verifications ≤ $5.00.
- Throughput ≥ 1,000 emails/week.

## Out of scope (MVP)
- Paid enrichment, CRM sync, deep JS-rendered crawling.
