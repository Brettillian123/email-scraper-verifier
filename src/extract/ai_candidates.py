"""
O27 - AI-assisted people extraction/refinement.

Given a broad list of Candidate objects (from heuristic extraction),
use an LLM to filter and normalize to real people only.

IMPORTANT CONTRACT (for ai_candidates_wrapper.py):
- If AI is enabled and the call succeeds but the model returns no people,
  this function MUST return [] (do not fallback here).
- Fallback and quality gates are handled exclusively by ai_candidates_wrapper.py.
"""

from __future__ import annotations

import importlib
import json
import logging
import os
import re
from collections.abc import Sequence
from typing import Any

from src.config import settings
from src.extract.candidates import Candidate

try:
    # New-style OpenAI client (openai>=1.0)
    from openai import OpenAI  # type: ignore[import]
except ImportError:  # pragma: no cover - optional dependency
    OpenAI = None  # type: ignore[assignment]

log = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Config / env wiring
# -----------------------------------------------------------------------------

# Prefer src.config.settings (which already reads env), but keep env compatibility.
OPENAI_API_KEY = (
    os.getenv("OPENAI_API_KEY") or (getattr(settings, "openai_api_key", "") or "")
).strip() or None

# Prefer AI_PEOPLE_MODEL; support legacy OPENAI_MODEL.
OPENAI_MODEL = (
    os.getenv("AI_PEOPLE_MODEL")
    or os.getenv("OPENAI_MODEL")
    or (getattr(settings, "ai_people_model", None) or "").strip()
    or "gpt-5-nano"
)

# Optional base URL override (compatible with both new and legacy libraries).
OPENAI_API_BASE = (os.getenv("OPENAI_API_BASE") or "").strip() or None

# Exported feature flag used by wrapper glue.
# AI is enabled by default when OPENAI_API_KEY is set, unless AI_PEOPLE_ENABLED=false
AI_PEOPLE_ENABLED: bool = bool(OPENAI_API_KEY) and bool(
    getattr(settings, "ai_people_enabled", True)  # Default True when API key present
)

_HAS_NEW_OPENAI = OpenAI is not None

# =============================================================================
# ROLE EMAIL DETECTION (used only for prompt context / candidate fields)
# =============================================================================

_ROLE_PREFIXES = frozenset(
    {
        "info",
        "contact",
        "hello",
        "support",
        "sales",
        "team",
        "admin",
        "help",
        "office",
        "careers",
        "hr",
        "jobs",
        "press",
        "media",
        "marketing",
        "billing",
        "accounts",
        "service",
        "enquiries",
        "inquiries",
        "general",
        "feedback",
        "webmaster",
        "postmaster",
        "abuse",
        "security",
        "privacy",
        "legal",
        "compliance",
        "pr",
        "noreply",
        "no-reply",
        "donotreply",
        "do-not-reply",
        "mailer-daemon",
        "example",
        "test",
        "demo",
        "sample",
    }
)


def _is_role_email(email: str | None) -> bool:
    """Check if email looks like a role/shared inbox address."""
    if not email or "@" not in email:
        return False
    local = email.split("@", 1)[0].lower().strip()
    return local in _ROLE_PREFIXES


def _get_client():
    """
    Build an OpenAI client compatible with either the new or legacy library.

    Returns None when API key is missing or OpenAI is not installed.
    """
    if not OPENAI_API_KEY:
        return None

    if _HAS_NEW_OPENAI:
        return OpenAI(  # type: ignore[misc]
            api_key=OPENAI_API_KEY,
            base_url=OPENAI_API_BASE or None,
        )

    # Legacy openai<1.0 client (import lazily to avoid hard dependency)
    try:
        legacy = importlib.import_module("openai")
    except Exception:  # pragma: no cover
        return None

    legacy.api_key = OPENAI_API_KEY  # type: ignore[attr-defined]
    if OPENAI_API_BASE:
        try:
            legacy.api_base = OPENAI_API_BASE  # type: ignore[attr-defined]
        except Exception:  # pragma: no cover
            pass
    return legacy


# =============================================================================
# AI SYSTEM PROMPT
# =============================================================================

_SYSTEM_PROMPT = """\
You are a B2B contact extraction engine.
You receive noisy candidate entries that may or may not be real people.
Your job is to return only real individual people who work or worked for the given company.

RULES FOR IDENTIFYING PEOPLE:
- raw_name is the PRIMARY clue. If raw_name looks like a human name (e.g. 'Abbey Shenberg',
  'Brittany Brandt', 'John Smith'), treat it as a real person.
- Do NOT return teams, departments, roles, or non-people like:
  'Sales Team', 'Support', 'Customer Success', 'HR Department', 'AI Teammates',
  'Infrastructure Monitoring', 'Bahasa Indonesia', or navigation labels.

CRITICAL RULE FOR ROLE/SHARED EMAILS:
- Role emails are shared company inboxes like: info@, office@, contact@, hello@, support@,
  sales@, team@, careers@, hr@, billing@, example@, admin@, general@, enquiries@, help@
- If a candidate has BOTH a role email AND a real person name:
  -> Return the PERSON with email set to null or empty string
  -> Do NOT attribute the role email to that specific person
  -> The role email is a shared inbox, not their personal email
- If a candidate has ONLY a role email with no real person name:
  -> Do NOT include it in the output (it's not a person)
- If a candidate has a personal-looking email (first.last@, flast@, firstl@, jsmith@, etc.):
  -> Return the person WITH that email

OUTPUT FORMAT:
For each real person, return:
- id: MUST match the input candidate's id (integer)
- full_name: full human name, properly cased
- first_name: first name only
- last_name: last name only
- email: personal email if available, null if only role email or no email
- title: concise job title (strip marketing fluff), null if unknown
- page_url: the source page URL

Output JSON with top-level key "people" containing an array.
Each element must have: id, full_name, first_name, last_name, email, title, page_url.

MERGE RULE:
If multiple candidates describe the SAME person (same normalized full_name), merge into
ONE output entry. When merging:
- Choose the best PERSONAL email (ignore role emails)
- Choose the most specific title
- Use the most relevant page_url
"""


def _dedup_and_compact_candidates(
    candidates: Sequence[Candidate],
) -> tuple[list[dict[str, Any]], int]:
    """
    Convert Candidate objects into a compact, de-duplicated JSON structure for the model.

    Returns:
        (json_candidates, original_len)
    """
    json_candidates: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, bool]] = set()

    for idx, c in enumerate(candidates):
        email = getattr(c, "email", None)
        first_name = getattr(c, "first_name", None)
        last_name = getattr(c, "last_name", None)
        raw_name = getattr(c, "raw_name", None)
        title = getattr(c, "title", None)
        source_url = getattr(c, "source_url", None)
        source_type = getattr(c, "source_type", None)
        context_snippet = getattr(c, "context_snippet", None)

        # Compute role guess deterministically if not provided by upstream.
        is_role_address_guess = bool(getattr(c, "is_role_address_guess", False))
        if not is_role_address_guess:
            is_role_address_guess = _is_role_email(email)

        # If no explicit raw_name, synthesize from first/last.
        if not raw_name and (first_name or last_name):
            parts = [p for p in (first_name, last_name) if p]
            raw_name = " ".join(parts) if parts else None

        # Light pruning: if we have neither a name nor an email, AI cannot help.
        if not raw_name and not email:
            continue

        # Normalize for dedup key
        email_lc = (email or "").strip().lower()
        name_norm = (raw_name or "").strip().lower()
        url_norm = (source_url or "").strip().lower()

        dedup_key = (email_lc, name_norm, url_norm, is_role_address_guess)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        json_candidates.append(
            {
                # IMPORTANT: id maps back to the original candidates index.
                "id": idx,
                "page_url": source_url,
                "source_type": source_type,
                "raw_name": raw_name,
                "email": email,
                "raw_title": title,
                "context_snippet": context_snippet,
                "is_role_address_guess": is_role_address_guess,
            }
        )

    return json_candidates, len(candidates)


def _build_payload(
    company_name: str,
    domain: str,
    candidates: Sequence[Candidate],
) -> dict[str, Any]:
    json_candidates, original_len = _dedup_and_compact_candidates(candidates)

    if not json_candidates:
        log.info("AI candidate payload empty after compaction (original_len=%s)", original_len)

    return {
        "company_name": company_name,
        "domain": domain,
        "candidates": json_candidates,
    }


def _make_messages(company_name: str, domain: str, payload: dict[str, Any]) -> list[dict[str, str]]:
    user_content = (
        f"Company name: {company_name}\n"
        f"Domain: {domain}\n\n"
        "Here is the JSON payload with raw candidates (already deduplicated). "
        "Filter and normalize them per the rules and return JSON only.\n\n"
        + json.dumps(payload, ensure_ascii=False)
    )

    return [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": user_content},
    ]


def _call_refiner(client, messages: list[dict[str, str]]) -> str:
    if _HAS_NEW_OPENAI:
        completion = client.chat.completions.create(
            model=OPENAI_MODEL,
            response_format={"type": "json_object"},
            messages=messages,
        )
        return completion.choices[0].message.content or "{}"

    completion = client.ChatCompletion.create(  # type: ignore[attr-defined]
        model=OPENAI_MODEL,
        response_format={"type": "json_object"},
        messages=messages,
    )
    return completion["choices"][0]["message"]["content"] or "{}"  # type: ignore[index]


def _parse_people_list(data: dict[str, Any]) -> list[dict[str, Any]] | None:
    people = data.get("people", [])
    if not isinstance(people, list):
        return None
    return [p for p in people if isinstance(p, dict)]


def _apply_ai_people(candidates: list[Candidate], people: list[dict[str, Any]]) -> list[Candidate]:
    refined: list[Candidate] = []

    for person in people:
        idx = person.get("id")
        if not isinstance(idx, int) or idx < 0 or idx >= len(candidates):
            log.debug("Skipping AI person with invalid id=%r", idx)
            continue

        base = candidates[idx]

        full_name = person.get("full_name") or None
        first_name = person.get("first_name") or None
        last_name = person.get("last_name") or None
        email = person.get("email") or None
        title = person.get("title") or None
        page_url = person.get("page_url") or None

        if isinstance(email, str) and not email.strip():
            email = None

        if first_name:
            base.first_name = str(first_name)
        if last_name:
            base.last_name = str(last_name)
        if full_name:
            base.raw_name = str(full_name)
            if hasattr(base, "full_name"):
                try:
                    base.full_name = str(full_name)  # type: ignore[attr-defined]
                except Exception:
                    pass

        base.email = str(email) if isinstance(email, str) else None

        if title:
            base.title = str(title)
        if page_url and hasattr(base, "source_url"):
            base.source_url = str(page_url)

        refined.append(base)

    return refined


def extract_ai_candidates(
    *,
    company_name: str,
    domain: str,
    raw_candidates: Sequence[Candidate],
    source_url: str | None = None,  # kept for backwards-compat logging, unused
) -> list[Candidate]:
    """
    AI refiner for person candidates.

    CONTRACT:
      - If AI is disabled/unavailable, returns raw_candidates unchanged.
      - If AI call succeeds but returns 0 people, returns [] (no fallback here).
        Fallback + quality gates happen in ai_candidates_wrapper.py.
    """
    candidates = list(raw_candidates)
    if not candidates:
        return candidates

    if not AI_PEOPLE_ENABLED:
        log.info("AI people refiner disabled; returning raw candidates unchanged")
        return candidates

    client = _get_client()
    if client is None:
        raise RuntimeError("OpenAI client unavailable (missing key/import/base_url config)")

    payload = _build_payload(company_name, domain, candidates)
    if not payload.get("candidates"):
        # No usable inputs for the model; wrapper owns fallback/quality gates.
        return []

    messages = _make_messages(company_name, domain, payload)

    try:
        content = _call_refiner(client, messages)
        data = json.loads(content)
        if not isinstance(data, dict):
            raise ValueError("AI response JSON is not an object")
    except Exception as exc:
        log.exception(
            "AI refinement call failed",
            extra={"company_name": company_name, "domain": domain, "exc": str(exc)},
        )
        raise

    people = _parse_people_list(data)
    if people is None:
        raise ValueError("AI response missing 'people' list")

    refined = _apply_ai_people(candidates, people)
    log.info("AI refinement complete: %d raw -> %d people", len(candidates), len(refined))

    # CRITICAL: do NOT fallback here. Wrapper will apply quality gates & fallback.
    return refined


__all__ = ["AI_PEOPLE_ENABLED", "extract_ai_candidates", "extract_people_from_html"]


# =============================================================================
# AI DIRECT HTML EXTRACTION (zero-candidate fallback)
# =============================================================================
#
# When the heuristic extractor finds ZERO candidates (common for SPA shells or
# pages with unconventional markup), we can still ask the AI to extract people
# directly from the raw HTML.  This is a separate flow from the refiner above.

_HTML_EXTRACT_SYSTEM_PROMPT = """\
You are a B2B contact extraction engine.
You receive structured text extracted from a company's team/about page.
The text is organized into sections with headings, image alt text, email links,
and optionally JSON-LD data.
Your job is to find all real individual people who work for the given company.

RULES:
- Extract real human names and their job titles.
- Image alt text often contains "Firstname Lastname" or "Name, Title" — use these.
- [Email links] sections contain mailto: addresses paired with display names.
- [Section] blocks contain visible page text with heading structure preserved.
- Do NOT return teams, departments, navigation labels, or non-people.
- Do NOT return customers, partners, or testimonial authors.
- If you find email addresses that look personal (first.last@, flast@, etc.),
  include them. Ignore role/shared emails (info@, support@, etc.).

OUTPUT FORMAT:
Return JSON with top-level key "people" containing an array.
Each element must have:
- full_name: full human name, properly cased
- first_name: first name only
- last_name: last name only
- email: personal email if found, null otherwise
- title: job title if found, null otherwise

If no real people are found, return {"people": []}.
"""

# Maximum extracted text chars to send to the AI.
# After smart extraction this represents ~3-8K tokens instead of 15-30K.
_HTML_EXTRACT_MAX_CHARS = 12_000


def _strip_html_regex(html: str) -> str:
    """Crude fallback when BeautifulSoup is unavailable."""
    text = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()[:_HTML_EXTRACT_MAX_CHARS]


def _cleanup_soup(soup) -> None:
    """Remove noise elements that tend to add tokens but not people signals."""
    # Remove noise elements entirely
    for tag_name in ("script", "style", "svg", "noscript", "iframe", "video", "audio"):
        for el in soup.find_all(tag_name):
            el.decompose()

    # Remove HTML comments
    for comment in soup.find_all(string=lambda s: isinstance(s, soup.Comment)):  # type: ignore[attr-defined]
        comment.extract()

    # Remove nav and footer (unlikely to contain team members)
    for el in soup.find_all(["nav", "footer"]):
        el.decompose()


def _extract_json_ld_parts(soup) -> list[str]:
    parts: list[str] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        text = script.get_text(strip=True)
        if text and ('"person"' in text.lower() or '"employee"' in text.lower()):
            parts.append(f"[JSON-LD] {text[:3000]}")
    return parts


def _extract_image_alt_parts(soup) -> list[str]:
    alt_texts: list[str] = []
    for img in soup.find_all("img"):
        alt = (img.get("alt") or "").strip()
        if not alt or len(alt) <= 3 or len(alt) >= 120:
            continue

        alt_lower = alt.lower()
        if any(
            skip in alt_lower
            for skip in (
                "logo",
                "icon",
                "banner",
                "background",
                "decoration",
                "spacer",
                "arrow",
                "button",
            )
        ):
            continue

        alt_texts.append(alt)

    if not alt_texts:
        return []
    return ["[Image alt text]\n" + "\n".join(alt_texts)]


def _extract_mailto_parts(soup) -> list[str]:
    mailto_emails: list[str] = []
    for a_tag in soup.find_all("a", href=True):
        href = a_tag.get("href", "")
        if not href.lower().startswith("mailto:"):
            continue

        email = href[7:].split("?")[0].strip()
        if not email or "@" not in email:
            continue

        link_text = a_tag.get_text(strip=True)
        entry = email
        if link_text and link_text.lower() != email.lower():
            entry = f"{link_text} <{email}>"
        mailto_emails.append(entry)

    if not mailto_emails:
        return []
    return ["[Email links]\n" + "\n".join(mailto_emails)]


def _section_depth(section) -> int:
    depth = 0
    parent = section.parent
    while parent and depth < 3:
        if parent.name in ("main", "article", "section"):
            break
        parent = parent.parent
        depth += 1
    return depth


def _section_has_headings(section) -> bool:
    return bool(section.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]))


def _render_section_text(section) -> str | None:
    headings = section.find_all(["h1", "h2", "h3", "h4", "h5", "h6"])
    if not headings:
        return None

    section_text_parts: list[str] = []
    for heading in headings:
        h_text = heading.get_text(strip=True)
        if h_text:
            section_text_parts.append(f"## {h_text}")

    body_text = section.get_text(separator="\n", strip=True)
    if body_text:
        body_text = re.sub(r"\n{3,}", "\n\n", body_text)
        section_text_parts.append(body_text)

    combined = "\n".join(section_text_parts).strip()
    if combined and len(combined) > 20:
        return combined
    return None


def _extract_section_parts(soup) -> tuple[list[str], int]:
    section_texts: list[str] = []
    for section in soup.find_all(["main", "article", "section", "div"]):
        if section.name == "div" and _section_depth(section) >= 3:
            continue
        if not _section_has_headings(section):
            continue

        rendered = _render_section_text(section)
        if rendered:
            section_texts.append(rendered)

    # Deduplicate overlapping sections (parent/child can repeat text)
    parts: list[str] = []
    seen_text: set[str] = set()
    section_chars = 0
    for st in section_texts:
        key = st[:100]
        if key in seen_text:
            continue
        seen_text.add(key)
        parts.append(f"[Section]\n{st}")
        section_chars += len(st)

    return parts, section_chars


def _extract_full_page_fallback_part(soup) -> str | None:
    all_text = soup.get_text(separator="\n", strip=True)
    if not all_text:
        return None
    all_text = re.sub(r"\n{3,}", "\n\n", all_text)
    if len(all_text) > 4000:
        all_text = all_text[:4000]
    return f"[Full page text]\n{all_text}"


def _extract_text_for_ai(html: str) -> str:
    """
    Extract a compact, structured text representation from HTML for the AI.

    Instead of sending raw HTML (which wastes tokens on tags, CSS classes,
    data attributes, SVG paths, etc.), this extracts only the signals the AI
    needs to identify people:
      - Visible text content (with section structure preserved)
      - Image alt text (headshots often have "Jane Doe, VP Engineering")
      - mailto: links (personal emails)
      - JSON-LD structured data (if present)

    A typical team page goes from ~50-200KB of raw HTML down to ~2-8KB of
    structured text, reducing token cost by 10-20x.
    """
    try:
        from bs4 import BeautifulSoup, Comment  # type: ignore[import]

        soup = BeautifulSoup(html, "html.parser")
        # Stash Comment on soup so _cleanup_soup can reference it without re-importing.
        soup.Comment = Comment  # type: ignore[attr-defined]
    except ImportError:
        return _strip_html_regex(html)

    _cleanup_soup(soup)

    parts: list[str] = []
    parts.extend(_extract_json_ld_parts(soup))
    parts.extend(_extract_image_alt_parts(soup))
    parts.extend(_extract_mailto_parts(soup))

    section_parts, section_chars = _extract_section_parts(soup)
    parts.extend(section_parts)

    # Fallback: if headed sections yielded very little, get body text.
    if section_chars < 200:
        fallback = _extract_full_page_fallback_part(soup)
        if fallback:
            parts.append(fallback)

    result = "\n\n".join(parts)

    # Final size guard
    if len(result) > _HTML_EXTRACT_MAX_CHARS:
        result = result[:_HTML_EXTRACT_MAX_CHARS]

    return result


def extract_people_from_html(
    *,
    html: str,
    source_url: str,
    company_name: str,
    domain: str,
) -> list[Candidate]:
    """
    AI-powered direct people extraction from raw HTML.

    Unlike extract_ai_candidates (which refines existing candidates), this
    function sends the raw HTML to the AI and asks it to find people directly.
    Used as a fallback when heuristic extraction finds zero candidates.

    Returns a list of Candidate objects, or [] if AI is unavailable or finds
    no people.
    """
    if not AI_PEOPLE_ENABLED:
        log.debug("AI HTML extraction skipped: AI_PEOPLE_ENABLED is False")
        return []

    client = _get_client()
    if client is None:
        log.debug("AI HTML extraction skipped: no OpenAI client")
        return []

    # Extract structured text (not raw HTML) to minimize token usage.
    # This typically reduces a 50-200KB page to 2-8KB of text.
    extracted_text = _extract_text_for_ai(html)
    if len(extracted_text) < 50:
        log.debug(
            "AI HTML extraction skipped: too little text after extraction (%d chars)",
            len(extracted_text),
        )
        return []

    user_content = (
        f"Company name: {company_name}\n"
        f"Domain: {domain}\n"
        f"Page URL: {source_url}\n\n"
        "Extract all real people from this page content. Return JSON only.\n\n" + extracted_text
    )

    messages = [
        {"role": "system", "content": _HTML_EXTRACT_SYSTEM_PROMPT},
        {"role": "user", "content": user_content},
    ]

    try:
        log.info(
            "Calling AI HTML extractor: url=%s company=%s text_chars=%d",
            source_url,
            company_name,
            len(extracted_text),
        )
        content = _call_refiner(client, messages)
        data = json.loads(content)
        if not isinstance(data, dict):
            raise ValueError("AI response JSON is not an object")
    except Exception as exc:
        log.warning(
            "AI HTML extraction failed: url=%s err=%s",
            source_url,
            exc,
        )
        return []

    people = _parse_people_list(data)
    if not people:
        log.info("AI HTML extraction returned 0 people for %s", source_url)
        return []

    # Convert AI response to Candidate objects
    candidates: list[Candidate] = []
    for person in people:
        full_name = (person.get("full_name") or "").strip()
        first_name = (person.get("first_name") or "").strip() or None
        last_name = (person.get("last_name") or "").strip() or None
        email = (person.get("email") or "").strip() or None
        title = (person.get("title") or "").strip() or None

        if not full_name and not first_name and not last_name:
            continue

        # Basic name validation: must have at least 2 characters
        if full_name and len(full_name) < 2:
            continue

        candidates.append(
            Candidate(
                email=email.lower() if email and "@" in email else None,
                source_url=source_url,
                first_name=first_name,
                last_name=last_name,
                raw_name=full_name or None,
                title=title,
                source_type="ai_html_extraction",
                context_snippet=f"AI extracted from {source_url}",
                is_role_address_guess=_is_role_email(email) if email else False,
            )
        )

    log.info(
        "AI HTML extraction complete: %d people from %s",
        len(candidates),
        source_url,
    )
    return candidates
