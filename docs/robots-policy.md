# Robots & Fetch Policy (R09)

- We will not fetch pages disallowed by robots.txt.
- We honor Crawl-delay per robots for our UA; if absent, default **3s per host**.
- We identify with a UA that includes product + version + contact email.
- We avoid gated/auth pages and respect site ToS (no form submission, no login; only public pages).
- We cache robots.txt and page responses; cache TTLs described below.
- We throttle on 403/429 and back off progressively (prevents WAF blocks).
- Logs never store page bodies containing PII; only metadata.

## Cache TTLs
- **robots.txt:** soft TTL 24h; revalidate with ETag/Last-Modified; on 404/timeout, negative-cache 1h.
- **HTML pages:** soft TTL 6h (or shorter if Cache-Control is more restrictive); allow stale-while-revalidate.
- **Error/backoff signals:** on 403/429, cache a per-host “slowdown” token 15m, doubling on repeats up to 24h, with 10–20% jitter.

_This aligns with the “User-agent, crawl delay, robots.txt enforcement, cache” scope for R09 and mitigates risks of robots-disallowed paths, WAF blocks, and cache stampede._
