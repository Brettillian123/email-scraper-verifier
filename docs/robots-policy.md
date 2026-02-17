# docs/robots-policy.md
# Robots & Fetch Policy (R09)

- We will not fetch pages disallowed by robots.txt.
- We honor Crawl-delay per robots for our UA; if absent, default **3s per host**.
- We identify with a UA that includes product + version + contact email.
- We avoid gated/auth pages and respect site ToS (no form submission, no login; only public pages).
- We cache robots.txt and page responses; cache TTLs described below.
- We throttle on 403/429 and back off progressively (prevents WAF blocks).
- Logs never store page bodies containing PII; only metadata.

## Cache TTLs

> **Note:** These values reflect the actual code defaults in `src/fetch/robots.py`
> and `src/fetch/client.py`. If you change them in code, update this document.

- **robots.txt:** soft TTL **1 hour** (`ROBOTS_TTL_SECONDS=3600`); revalidate with ETag/Last-Modified; on 5xx, deny-cache for **5 minutes** (`ROBOTS_DENY_TTL_SECONDS=300`); on 404, allow all (no robots restrictions).
- **HTML pages:** soft TTL determined by Cache-Control headers, default **15 minutes** (`FETCH_CACHE_TTL_SEC=900`); allow stale-while-revalidate.
- **Error/backoff signals:** on 403/429, cache a per-host "slowdown" token 15m, doubling on repeats up to 24h, with 10–20% jitter.

### Environment overrides

| Variable | Default | Description |
|---|---|---|
| `ROBOTS_TTL_SECONDS` | `3600` (1h) | How long to cache a successfully fetched robots.txt |
| `ROBOTS_DENY_TTL_SECONDS` | `300` (5min) | How long to deny-cache after a 5xx response from robots.txt |
| `ROBOTS_DEFAULT_DELAY_SECONDS` | `3.0` | Default crawl delay when robots.txt has no Crawl-delay directive |
| `FETCH_CACHE_TTL_SEC` | `900` (15min) | Default cache TTL for HTML page responses |
| `ROBOTS_CACHE_TTL_SEC` | `86400` (24h) | Cache TTL for robots.txt (used by config.py; client.py uses `ROBOTS_TTL_SECONDS`) |

### Behavioral notes

- **5xx on robots.txt → deny all:** If the robots.txt endpoint returns a server error, we conservatively deny all crawling for that host for the deny-TTL window. This prevents crawling sites whose robots.txt we cannot verify.
- **404 on robots.txt → allow all:** A missing robots.txt (404) is treated as "no restrictions" per the standard.
- **Deny-on-5xx is time-limited:** After `ROBOTS_DENY_TTL_SECONDS` expires, the next request will re-fetch robots.txt and apply the new result.

_This aligns with the "User-agent, crawl delay, robots.txt enforcement, cache" scope for R09 and mitigates risks of robots-disallowed paths, WAF blocks, and cache stampede._
