# src/crawl/headless.py
"""
Headless browser fallback for JavaScript-rendered (SPA) pages.

When the standard httpx client fetches an HTML shell (React/Next.js/Vue/etc.),
this module can re-render the page with a real browser to get the full DOM.

Design:
  - Playwright is an OPTIONAL dependency; all functions degrade gracefully.
  - A single browser instance is reused across renders within a crawl session.
  - Strict timeouts prevent runaway renders from blocking the pipeline.
  - Only used when SPA shell detection triggers (not for every page).

Usage:
    from src.crawl.headless import is_spa_shell, render_page, HeadlessBrowser

    if is_spa_shell(body):
        with HeadlessBrowser() as browser:
            rendered = browser.render(url, timeout_ms=15000)
            if rendered:
                body = rendered
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Conditional Playwright import
# ---------------------------------------------------------------------------

try:
    from playwright.sync_api import sync_playwright

    _HAS_PLAYWRIGHT = True
except ImportError:
    _HAS_PLAYWRIGHT = False
    sync_playwright = None  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

HEADLESS_ENABLED: bool = os.getenv("HEADLESS_BROWSER_ENABLED", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
HEADLESS_TIMEOUT_MS: int = int(os.getenv("HEADLESS_TIMEOUT_MS", "15000"))
HEADLESS_WAIT_UNTIL: str = os.getenv("HEADLESS_WAIT_UNTIL", "networkidle")
HEADLESS_MAX_RENDERS_PER_CRAWL: int = int(os.getenv("HEADLESS_MAX_RENDERS_PER_CRAWL", "8"))

# ---------------------------------------------------------------------------
# SPA Shell Detection
# ---------------------------------------------------------------------------

# Common SPA framework root container IDs
_SPA_CONTAINER_IDS = frozenset(
    {
        "__next",  # Next.js
        "__nuxt",  # Nuxt.js
        "__gatsby",  # Gatsby
        "app",  # Vue.js / generic
        "root",  # React (create-react-app)
        "__svelte",  # SvelteKit
        "svelte",  # Svelte
        "ember-app",  # Ember
        "main-app",  # Generic
        "react-root",  # React variant
        "vue-app",  # Vue variant
    }
)

# Regex to detect SPA container divs with minimal or no children content
_SPA_CONTAINER_RE = re.compile(
    r'<div\s+id\s*=\s*["\']('
    + "|".join(re.escape(c) for c in _SPA_CONTAINER_IDS)
    + r')["\'][^>]*>\s*</div>',
    re.IGNORECASE,
)

# More lenient: detect SPA containers that have only whitespace or script tags inside
_SPA_CONTAINER_EMPTY_RE = re.compile(
    r'<div\s+id\s*=\s*["\']('
    + "|".join(re.escape(c) for c in _SPA_CONTAINER_IDS)
    + r')["\'][^>]*>(?:\s|<script[^>]*>.*?</script>)*</div>',
    re.IGNORECASE | re.DOTALL,
)

# Script bundle indicators (common in SPAs)
_JS_BUNDLE_RE = re.compile(
    r'<script\s[^>]*src\s*=\s*["\'][^"\']*'
    r"(?:chunks?/|bundle|_next/|_nuxt/|__gatsby|dist/|assets/js/|app\.[a-f0-9]+\.js)",
    re.IGNORECASE,
)

# Minimum visible text length — if the page body has very little text, it's likely a shell
_MIN_VISIBLE_TEXT_CHARS = 200


def is_spa_shell(body: bytes, *, strict: bool = False) -> bool:
    """
    Detect whether an HTML response is a JavaScript SPA shell (empty container
    that needs client-side rendering).

    Args:
        body: Raw HTML bytes from httpx
        strict: If True, require multiple signals; if False, a single strong
                signal is enough

    Returns:
        True if the page appears to be a JS shell that won't yield useful
        content without a browser render.
    """
    if not body:
        return False

    # Quick length check — very small HTML with scripts is suspicious
    if len(body) > 500_000:
        # Very large pages are unlikely to be empty shells
        return False

    text = body.decode("utf-8", "ignore")
    text_lower = text.lower()

    signals = 0

    # Signal 1: SPA container div that is empty or near-empty
    if _SPA_CONTAINER_RE.search(text):
        signals += 2  # Strong signal
        log.debug("SPA detection: empty SPA container found")
    elif _SPA_CONTAINER_EMPTY_RE.search(text):
        signals += 2
        log.debug("SPA detection: SPA container with only scripts found")

    # Signal 2: JS bundle references (framework-specific chunks)
    bundle_matches = _JS_BUNDLE_RE.findall(text)
    if len(bundle_matches) >= 2:
        signals += 1
        log.debug("SPA detection: %d JS bundle references found", len(bundle_matches))

    # Signal 3: Very little visible text content
    # Strip all tags, then check remaining text length
    stripped = re.sub(r"<[^>]+>", " ", text)
    stripped = re.sub(r"\s+", " ", stripped).strip()
    # Also remove common boilerplate (noscript messages, etc.)
    stripped_clean = re.sub(
        r"(?:you need to enable javascript|please enable javascript|"
        r"this app works best with javascript|loading\.\.\.)",
        "",
        stripped,
        flags=re.IGNORECASE,
    )
    if len(stripped_clean) < _MIN_VISIBLE_TEXT_CHARS:
        signals += 1
        log.debug("SPA detection: very little visible text (%d chars)", len(stripped_clean))

    # Signal 4: "noscript" tag with fallback message (strong SPA indicator)
    if "<noscript" in text_lower and (
        "enable javascript" in text_lower
        or "requires javascript" in text_lower
        or "you need to" in text_lower
    ):
        signals += 1
        log.debug("SPA detection: noscript fallback message found")

    threshold = 3 if strict else 2
    is_shell = signals >= threshold

    if is_shell:
        log.info(
            "SPA shell detected (signals=%d, threshold=%d)",
            signals,
            threshold,
        )

    return is_shell


# ---------------------------------------------------------------------------
# Headless Browser Wrapper
# ---------------------------------------------------------------------------


class HeadlessBrowser:
    """
    Context-managed headless browser for rendering SPA pages.

    Usage:
        with HeadlessBrowser() as browser:
            html_bytes = browser.render("https://example.com/team")

    The browser instance is reused for multiple renders within the context.
    """

    def __init__(
        self,
        *,
        timeout_ms: int | None = None,
        wait_until: str | None = None,
        user_agent: str | None = None,
    ) -> None:
        self._timeout_ms = timeout_ms or HEADLESS_TIMEOUT_MS
        self._wait_until = wait_until or HEADLESS_WAIT_UNTIL
        self._user_agent = user_agent
        self._pw: Any = None
        self._browser: Any = None
        self._render_count = 0

    @property
    def available(self) -> bool:
        """True if Playwright is installed and headless rendering is enabled."""
        return _HAS_PLAYWRIGHT and HEADLESS_ENABLED

    def __enter__(self) -> HeadlessBrowser:
        if not self.available:
            return self
        try:
            self._pw = sync_playwright().__enter__()  # type: ignore[union-attr]
            launch_args = {
                "headless": True,
                "args": [
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--single-process",
                ],
            }
            self._browser = self._pw.chromium.launch(**launch_args)
            log.info("Headless browser launched for SPA rendering")
        except Exception as exc:
            log.warning("Failed to launch headless browser: %s", exc)
            self._pw = None
            self._browser = None
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._browser is not None:
            try:
                self._browser.close()
            except Exception:
                pass
        if self._pw is not None:
            try:
                self._pw.__exit__(None, None, None)
            except Exception:
                pass
        self._browser = None
        self._pw = None

    def render(self, url: str, *, timeout_ms: int | None = None) -> bytes | None:
        """
        Render a URL with a headless browser and return the fully rendered HTML.

        Args:
            url: The URL to render
            timeout_ms: Override default timeout for this render

        Returns:
            Rendered HTML as bytes, or None if rendering failed or is unavailable.
        """
        if self._browser is None:
            return None

        if self._render_count >= HEADLESS_MAX_RENDERS_PER_CRAWL:
            log.debug(
                "Headless render limit reached (%d/%d), skipping %s",
                self._render_count,
                HEADLESS_MAX_RENDERS_PER_CRAWL,
                url,
            )
            return None

        effective_timeout = timeout_ms or self._timeout_ms

        try:
            context_opts: dict[str, Any] = {}
            if self._user_agent:
                context_opts["user_agent"] = self._user_agent

            context = self._browser.new_context(**context_opts)
            page = context.new_page()

            try:
                page.goto(
                    url,
                    wait_until=self._wait_until,
                    timeout=effective_timeout,
                )

                # Extra wait for lazy-loaded team sections
                page.wait_for_timeout(2000)

                content = page.content()
                rendered_bytes = content.encode("utf-8")

                self._render_count += 1
                log.info(
                    "Headless render succeeded: url=%s size=%d render_count=%d",
                    url,
                    len(rendered_bytes),
                    self._render_count,
                )
                return rendered_bytes

            finally:
                try:
                    page.close()
                except Exception:
                    pass
                try:
                    context.close()
                except Exception:
                    pass

        except Exception as exc:
            log.warning("Headless render failed for %s: %s", url, exc)
            return None


# ---------------------------------------------------------------------------
# Module-level convenience
# ---------------------------------------------------------------------------


def render_page(url: str, *, user_agent: str | None = None) -> bytes | None:
    """
    One-shot convenience: launch browser, render a single page, close.

    For batch rendering (multiple pages in one crawl), use HeadlessBrowser
    as a context manager instead.
    """
    with HeadlessBrowser(user_agent=user_agent) as browser:
        return browser.render(url)


def is_headless_available() -> bool:
    """Check if headless rendering is both installed and enabled."""
    return _HAS_PLAYWRIGHT and HEADLESS_ENABLED


__all__ = [
    "is_spa_shell",
    "is_headless_available",
    "render_page",
    "HeadlessBrowser",
    "HEADLESS_ENABLED",
]
