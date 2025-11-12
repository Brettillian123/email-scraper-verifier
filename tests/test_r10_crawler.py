# tests/test_r10_crawler.py
from __future__ import annotations


def test_crawl_domain_seeds_follow_depth_and_limits(monkeypatch):
    """
    R10 crawler acceptance (focused, single test):

    - Stubs fetch_url to serve tiny HTML for /about, /team, /contact, /news,
      plus one irrelevant page and a couple of followable pages.
    - Validates only same-host URLs are fetched.
    - Validates keyword-guided links get followed up to CRAWL_MAX_DEPTH.
    - Validates CRAWL_MAX_PAGES_PER_DOMAIN is enforced.
    - Ensures the result contains (url, html) pairs for the four core seeds.
    """
    # Import the runner module once, then override its imported config symbols.
    import src.crawl.runner as runner

    # Configure seeds, follow hints, depth, page cap, and size limit so the
    # test can exercise breadth-first discovery and a single follow hop.
    monkeypatch.setattr(runner, "CRAWL_SEED_PATHS", "/about,/team,/contact,/news", raising=False)
    monkeypatch.setattr(runner, "CRAWL_FOLLOW_KEYWORDS", "about,team,contact,news", raising=False)
    monkeypatch.setattr(runner, "CRAWL_MAX_DEPTH", 1, raising=False)
    # Cap at 5 so we fetch the four seeds + one followed page, then stop.
    monkeypatch.setattr(runner, "CRAWL_MAX_PAGES_PER_DOMAIN", 5, raising=False)
    monkeypatch.setattr(runner, "CRAWL_HTML_MAX_BYTES", 65536, raising=False)

    base = "https://example.com"

    def U(path: str) -> str:
        return f"{base}{path if path.startswith('/') else '/' + path}"

    # Tiny HTML fixtures. The regex in runner finds href="..."/href='...'
    # without fragments or queries, so keep links simple.
    pages_html: dict[str, bytes] = {
        # Seeds
        U("/about"): (
            b"<html><body>"
            b"<a href='/news/article1'>news-article</a>"  # relevant, depth 1
            b"<a href='/random'>random</a>"  # irrelevant; should not follow
            b"<a href='https://external.com/team'>ext</a>"  # external; must be ignored
            b"</body></html>"
        ),
        U("/team"): (
            b"<html><body>"
            b"<a href='/contact-details'>contact-details</a>"  # relevant, depth 1
            b"</body></html>"
        ),
        U("/contact"): b"<html><body>contact page</body></html>",
        U("/news"): b"<html><body>news hub</body></html>",
        # Followed from /about (depth 1)
        U("/news/article1"): (
            b"<html><body>"
            b"<a href='/team/leadership'>leadership</a>"  # depth 2 (should NOT be followed)
            b"</body></html>"
        ),
        # Candidate discovered from /team (also depth 1) — we won't reach it
        # due to the 5-page cap, but include to ensure it *could* be followed.
        U("/contact-details"): b"<html><body>contact details</body></html>",
        # Irrelevant page (not hinted)
        U("/random"): b"<html><body>random</body></html>",
        # Depth-2 (should never be fetched because max_depth=1)
        U("/team/leadership"): b"<html><body>leadership</body></html>",
    }

    fetch_calls: list[str] = []

    class DummyResp:
        def __init__(self, status_code: int, content: bytes | None):
            self.status_code = status_code
            self.content = content

    def fake_fetch(url: str) -> DummyResp:
        fetch_calls.append(url)
        html = pages_html.get(url)
        if html is not None:
            return DummyResp(200, html)
        return DummyResp(404, b"")

    # Patch the runner's bound fetch_url so crawl_domain() uses our stub.
    monkeypatch.setattr(runner, "fetch_url", fake_fetch, raising=True)

    # Execute crawl.
    pages = runner.crawl_domain("example.com")

    # Basic shape: (url, html) pairs returned.
    assert all(hasattr(p, "url") and hasattr(p, "html") for p in pages)

    urls = [p.url for p in pages]
    html_by_url = {p.url: p.html for p in pages}

    # 1) only same-host URLs are fetched
    assert all(u.startswith(f"{base}/") for u in urls)
    assert not any(u.startswith("https://external.com") for u in urls)
    # Also ensure our stub was never even *asked* for external URLs.
    assert not any(c.startswith("https://external.com") for c in fetch_calls)

    # 2) keyword-guided links get followed up to CRAWL_MAX_DEPTH
    # We should have followed one hinted internal link at depth 1.
    assert U("/news/article1") in urls
    # And we should NOT have followed depth-2 from that page (max_depth=1).
    assert U("/team/leadership") not in urls

    # 3) CRAWL_MAX_PAGES_PER_DOMAIN is enforced (4 seeds + 1 follow = 5)
    assert len(pages) == 5

    # 4) the four core seeds are present
    expected_seeds = {U("/about"), U("/team"), U("/contact"), U("/news")}
    assert expected_seeds.issubset(set(urls))

    # 5) irrelevant internal page was not followed
    assert U("/random") not in urls

    # Sanity: ensure we actually got HTML bytes for seed pages.
    for seed in expected_seeds:
        assert isinstance(html_by_url[seed], (bytes, bytearray))
        assert b"<html" in html_by_url[seed].lower()
