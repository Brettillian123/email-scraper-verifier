"""
O27 — AI-assisted people extraction/refinement.

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
OPENAI_API_KEY = (os.getenv("OPENAI_API_KEY") or (settings.openai_api_key or "")).strip() or None

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
AI_PEOPLE_ENABLED: bool = bool(OPENAI_API_KEY) and bool(
    getattr(settings, "ai_people_enabled", False)
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
  → Return the PERSON with email set to null or empty string
  → Do NOT attribute the role email to that specific person
  → The role email is a shared inbox, not their personal email
- If a candidate has ONLY a role email with no real person name:
  → Do NOT include it in the output (it's not a person)
- If a candidate has a personal-looking email (first.last@, flast@, firstl@, jsmith@, etc.):
  → Return the person WITH that email

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
    log.info("AI refinement complete: %d raw → %d people", len(candidates), len(refined))

    # CRITICAL: do NOT fallback here. Wrapper will apply quality gates & fallback.
    return refined


__all__ = ["AI_PEOPLE_ENABLED", "extract_ai_candidates"]
