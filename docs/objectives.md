# Email Scraper/Verifier — Objectives & Guardrails (v0)

## Objectives (what success looks like)
- Build an MVP that produces a verified lead list for [your ICP] from **allowed** web sources.

## Compliance & Scope
- **Robots/ToS**: Only crawl pages allowed by robots.txt; no login/gated content; identify as a polite bot with contact email.
- **Allowed sources**: e.g., `/about`, `/team`, `/contact`, newsroom posts, legal notice pages.
- **Suppression policy**: Maintain global do-not-contact for role/distribution addresses and opt-outs; never re-verify suppressed emails.

## Success Metrics (initial targets)
- **Valid rate**: ≥ 70% of verified as “valid”
- **Cost per 1k verifications**: ≤ $X (set a number)
- **Throughput**: ≥ 1,000 emails/week

## Out of scope (for MVP)
- Paid data providers, deep JS rendering, CRM sync.
