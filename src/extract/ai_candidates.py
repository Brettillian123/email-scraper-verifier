"""
O27 — AI-assisted people extraction/refinement.

Given a broad list of Candidate objects (from heuristic extraction),
use an LLM to filter and normalize to real people only.
"""

from __future__ import annotations

import importlib
import json
import logging
import os
from collections.abc import Sequence
from typing import Any

from src.config import load_settings
from src.extract.candidates import Candidate

try:
    # New-style OpenAI client (openai>=1.0)
    from openai import OpenAI  # type: ignore[import]
except ImportError:  # pragma: no cover - optional dependency
    OpenAI = None  # type: ignore[assignment]

log = logging.getLogger(__name__)
_cfg = load_settings()

# Basic config / env wiring
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY") or getattr(_cfg, "OPENAI_API_KEY", None)
OPENAI_MODEL = os.getenv("OPENAI_MODEL") or getattr(_cfg, "OPENAI_MODEL", "gpt-4.1-mini")
OPENAI_API_BASE = os.getenv("OPENAI_API_BASE") or getattr(_cfg, "OPENAI_API_BASE", None)

# Exported feature flag used by auto-discovery glue
AI_PEOPLE_ENABLED: bool = bool(
    OPENAI_API_KEY and os.getenv("AI_PEOPLE_ENABLED", "1").strip().lower() not in {"0", "false"}
)

_HAS_NEW_OPENAI = OpenAI is not None

# =============================================================================
# ROLE EMAIL DETECTION (for fallback filtering)
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


def _has_real_name(cand: Candidate) -> bool:
    """Check if candidate has a plausible human name."""
    raw_name = getattr(cand, "raw_name", None)
    first_name = getattr(cand, "first_name", None)
    last_name = getattr(cand, "last_name", None)

    # Check raw_name: must be at least 3 chars and contain a space (two parts)
    if raw_name:
        name = raw_name.strip()
        if len(name) >= 3 and " " in name:
            return True

    # Check first + last name
    if first_name and last_name:
        if len(first_name.strip()) >= 2 and len(last_name.strip()) >= 2:
            return True

    return False


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
  → Return the PERSON with email set to null or empty string
  → Do NOT attribute the role email to that specific person
  → The role email is a shared inbox, not their personal email
- If a candidate has ONLY a role email with no real person name:
  → Do NOT include it in the output (it's not a person)
- If a candidate has a personal-looking email (first.last@, flast@, firstl@, jsmith@, etc.):
  → Return the person WITH that email

EXAMPLES:
Input: {raw_name: "Abbey Shenberg", email: "office@company.com", is_role_address_guess: true}
CORRECT: {full_name: "Abbey Shenberg", first_name: "Abbey", last_name: "Shenberg", email: null}
WRONG: {full_name: "Abbey Shenberg", email: "office@company.com"}

Input: {raw_name: "Abbey Shenberg", email: "abbey.shenberg@company.com",
        is_role_address_guess: false}
CORRECT: {full_name: "Abbey Shenberg", email: "abbey.shenberg@company.com"}

Input: {raw_name: null, email: "info@company.com", is_role_address_guess: true}
CORRECT: (do not include in output - no person identified)

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
- Use the most relevant page_url"""


def _get_client():
    """Build an OpenAI client compatible with either the new or legacy library.

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


def _dedup_and_compact_candidates(
    candidates: Sequence[Candidate],
) -> tuple[list[dict[str, Any]], int]:
    """Convert Candidate objects into a compact, de-duplicated JSON structure for the model.

    Returns:
        (json_candidates, original_len)
        - json_candidates: list of dicts suitable for payload["candidates"]
        - original_len: len(candidates) before dedup
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
        is_role_address_guess = bool(getattr(c, "is_role_address_guess", False))

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
                # IMPORTANT: id is the index into the *original* candidates list
                # so we can map back when applying AI refinements.
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
    """Convert Candidate objects into a compact JSON structure for the model.

    Includes candidates that have no email but do have a plausible name.
    """
    json_candidates, original_len = _dedup_and_compact_candidates(candidates)

    if not json_candidates:
        log.info(
            "AI candidate payload is empty after compaction (original_len=%s)",
            original_len,
        )

    return {
        "company_name": company_name,
        "domain": domain,
        "candidates": json_candidates,
    }


def _make_messages(
    company_name: str,
    domain: str,
    payload: dict[str, Any],
) -> list[dict[str, str]]:
    """Build the chat messages for the AI refinement call."""
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


def _apply_ai_people(
    candidates: list[Candidate],
    people: list[dict[str, Any]],
) -> list[Candidate]:
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
            base.first_name = first_name
        if last_name:
            base.last_name = last_name
        if full_name:
            base.raw_name = full_name
            if hasattr(base, "full_name"):
                try:
                    base.full_name = full_name  # type: ignore[attr-defined]
                except Exception:
                    pass

        base.email = email

        if title:
            base.title = title
        if page_url and hasattr(base, "source_url"):
            base.source_url = page_url

        refined.append(base)

    return refined


def _fallback_filter_role_only(candidates: list[Candidate]) -> list[Candidate]:
    return [
        c for c in candidates if _has_real_name(c) or not _is_role_email(getattr(c, "email", None))
    ]


def extract_ai_candidates(
    *,
    company_name: str,
    domain: str,
    raw_candidates: Sequence[Candidate],
    source_url: str | None = None,  # kept for backwards-compat logging, unused
) -> list[Candidate]:
    """AI refiner for person candidates.

    Input:
        - A broad, heuristically generated list of Candidate objects (raw_candidates)
        - Company name + domain for context

    Output:
        - A filtered + normalized list of Candidate objects representing
          real people associated with this company.

    Behavior:
        - If AI is disabled or unavailable, returns raw_candidates unchanged.
        - Uses the model to DROP non-person entries (teams, products, nav labels, etc.)
          and CLEANUP names/titles for the remaining ones.
        - Role emails (info@, office@, etc.) are stripped from person output;
          only personal emails are preserved.
        - If AI returns nothing, filters out role-only emails before falling back.
    """
    candidates = list(raw_candidates)
    if not candidates:
        return candidates

    if not AI_PEOPLE_ENABLED:
        log.info("AI people refiner disabled via AI_PEOPLE_ENABLED flag; returning raw candidates")
        return candidates

    client = _get_client()
    if client is None:
        log.warning("OpenAI client unavailable (missing key or import); returning raw candidates")
        return candidates

    payload = _build_payload(company_name, domain, candidates)
    if not payload.get("candidates"):
        return candidates

    messages = _make_messages(company_name, domain, payload)

    try:
        content = _call_refiner(client, messages)
        data = json.loads(content)
        if not isinstance(data, dict):
            raise ValueError("AI response JSON is not an object")
    except Exception as exc:  # pragma: no cover - defensive
        log.exception(
            "AI refinement call failed; returning raw candidates",
            extra={"company_name": company_name, "domain": domain, "exc": str(exc)},
        )
        return candidates

    people = _parse_people_list(data)
    if people is None:
        log.warning("AI response missing 'people' list; returning raw candidates")
        return candidates

    refined = _apply_ai_people(candidates, people)
    if refined:
        log.info("AI refinement complete: %d raw → %d people", len(candidates), len(refined))
        return refined

    filtered = _fallback_filter_role_only(candidates)
    if filtered:
        removed_count = len(candidates) - len(filtered)
        log.info(
            "AI refinement returned no people; falling back to %d filtered candidates "
            "(removed %d role-only emails)",
            len(filtered),
            removed_count,
        )
        return filtered

    log.info(
        (
            "AI refinement returned no people and all %d candidates were role-only; "
            "returning empty list"
        ),
        len(candidates),
    )
    return []
