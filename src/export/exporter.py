# src/export/exporter.py
"""
R20 — Export pipeline.

Builds a clean final list for sales/CRM from v_emails_latest:

  email, first_name, last_name, title, company, domain, source_url,
  icp_score, verify_status, verified_at

Key decisions:

- Title: prefer title_norm if present, then title_raw.
- Company: use the normalized display name from v_emails_latest.company_name.
- Domain: use the email's domain (company_domain from the view), not just companies.domain.
- verify_status / verified_at: use R18 canonical fields from verification_results.
- verified_at is treated as ISO-8601 UTC (YYYY-MM-DDTHH:MM:SSZ).
- Row order: high ICP first, then company, last_name, first_name, email.

This module layers on top of:
  - src.export.policy.ExportPolicy (O10)
  - src.db_suppression.is_email_suppressed (R19/O11)
  - v_emails_latest DB view (R18 wiring)
"""

from __future__ import annotations

import inspect
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import yaml

from src.db_suppression import is_email_suppressed
from src.export.policy import ExportPolicy


@dataclass
class ExportLead:
    email: str
    first_name: str | None
    last_name: str | None
    title: str | None
    company: str | None
    domain: str | None
    source_url: str | None
    icp_score: float | None
    verify_status: str | None
    verified_at: str | None  # ISO 8601 string from DB


def iter_candidate_rows(conn: sqlite3.Connection) -> Iterable[sqlite3.Row]:
    """
    Raw candidate rows from v_emails_latest, with the columns we need for export.

    We rely on db/schema.sql (or the in-memory test schema) to expose:
      - first_name, last_name
      - title_norm, title_raw
      - company_name, company_domain
      - source_url (coalesced email/person/source URL)
      - icp_score, verify_status, verified_at
    """
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        """
        SELECT
            email,
            first_name,
            last_name,
            COALESCE(title_norm, title_raw) AS title,
            company_name,
            company_domain,
            source_url,
            icp_score,
            verify_status,
            verified_at
        FROM v_emails_latest
        ORDER BY
            (icp_score IS NULL) ASC,   -- non-null scores first
            icp_score DESC,
            company_name,
            last_name,
            first_name,
            email
        """
    )
    yield from cur


def _load_export_policy(policy_name: str) -> ExportPolicy:
    """
    Load an ExportPolicy (or compatible stub) from docs/icp-schema.yaml.

    - Reads docs/icp-schema.yaml relative to the repo root.
    - Looks for export_policies[policy_name], falling back to "default".
    - Calls ExportPolicy.from_config(...) with a call shape that matches
      the actual from_config signature.

    Supported from_config APIs:

      1) Legacy / stub API (used in R20 tests):
         @classmethod
         def from_config(cls, cfg: Mapping[str, Any]) -> ExportPolicy: ...

      2) O10 real API:
         @classmethod
         def from_config(cls, name: str, cfg: Mapping[str, Any]) -> ExportPolicy: ...

    This works both for the real ExportPolicy (O10) and for the FakePolicy stub
    used in tests, as long as they expose one of the two APIs above.
    """
    # repo root: .../src/export/exporter.py → parents[2]
    root = Path(__file__).resolve().parents[2]
    cfg_path = root / "docs" / "icp-schema.yaml"

    try:
        text = cfg_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RuntimeError(
            f"Unable to read export policy config at {cfg_path}",
        ) from exc

    data = yaml.safe_load(text) or {}
    policies = data.get("export_policies") or {}  # type: ignore[assignment]

    # Try the requested policy first
    selected_name = policy_name
    cfg = policies.get(selected_name)

    # Fall back to "default" if the requested one isn't present
    if cfg is None:
        selected_name = "default"
        cfg = policies.get(selected_name)

    if cfg is None:
        raise RuntimeError(
            f"Export policy {policy_name!r} not found in export_policies "
            "and no 'default' policy is defined.",
        )

    # Introspect the *current* ExportPolicy.from_config implementation.
    # This might be the real ExportPolicy class or a FakePolicy stub from tests.
    raw_attr = inspect.getattr_static(ExportPolicy, "from_config")

    if isinstance(raw_attr, classmethod):
        func = raw_attr.__func__
    elif isinstance(raw_attr, staticmethod):
        func = raw_attr.__func__
    else:
        func = raw_attr  # type: ignore[assignment]

    sig = inspect.signature(func)
    params = list(sig.parameters.values())
    num_params = len(params)

    # Parameters include the implicit "cls"/"self" for methods defined on the class.
    # We support:
    #   (cls, cfg)                → legacy/stub API
    #   (cls, name, cfg)         → real O10 API
    if num_params == 2:
        # (cls, cfg) — legacy / stub API
        return ExportPolicy.from_config(cfg)  # type: ignore[arg-type]
    elif num_params >= 3:
        # (cls, name, cfg, *rest) — pass name + cfg, ignore any extras with defaults
        return ExportPolicy.from_config(selected_name, cfg)  # type: ignore[arg-type]
    else:
        raise TypeError(
            f"ExportPolicy.from_config has unsupported signature {sig}; "
            "expected either (cls, cfg) or (cls, name, cfg).",
        )


def iter_exportable_leads(
    conn: sqlite3.Connection,
    policy_name: str = "default",
) -> Iterable[ExportLead]:
    """
    Yield ExportLead objects that pass:
      - suppression (R19 + O11),
      - export policy (O10, via ExportPolicy),
      - basic sanity around verify_status/icp_score.

    The ExportPolicy instance is loaded from docs/icp-schema.yaml via
    _load_export_policy(policy_name).
    """
    policy = _load_export_policy(policy_name)

    for row in iter_candidate_rows(conn):
        email = row["email"]
        if not email:
            continue

        # 1) Hard suppression (global + CRM)
        if is_email_suppressed(conn, email):
            continue

        # 2) Export policy gates (verify_status + icp_score + role rules, etc.).
        ok, _reason = policy.is_exportable_row(
            email=email,
            verify_status=row["verify_status"],
            icp_score=row["icp_score"],
            # Extra fields are made available to the policy; it can ignore them
            # or use them (e.g., role_family, seniority, industry, is_role_address, etc.).
            extra=row,
        )
        if not ok:
            continue

        lead = ExportLead(
            email=email,
            first_name=row["first_name"],
            last_name=row["last_name"],
            title=row["title"],
            company=row["company_name"],
            domain=row["company_domain"],
            source_url=row["source_url"],
            icp_score=row["icp_score"],
            verify_status=row["verify_status"],
            verified_at=row["verified_at"],
        )

        yield _sanitize_for_csv(lead)


def _escape_cell(value: str | None) -> str | None:
    """
    Guard against Excel/Sheets "formula injection" (CSV Injection / DDE attacks).

    Prefixes any cell starting with a dangerous character with a single quote.
    Dangerous characters per OWASP CSV Injection guidance:
      =  +  -  @  \t (tab)  \r (carriage return)

    Reference: https://owasp.org/www-community/attacks/CSV_Injection
    """
    if value is None:
        return None
    if value and value[0] in ("=", "+", "-", "@", "\t", "\r"):
        return "'" + value
    return value


def _sanitize_for_csv(lead: ExportLead) -> ExportLead:
    """
    Apply _escape_cell to text fields that might be interpreted as formulas
    by spreadsheet software. Numeric fields (icp_score) are left untouched.
    """
    return ExportLead(
        email=_escape_cell(lead.email) or "",  # email is required
        first_name=_escape_cell(lead.first_name),
        last_name=_escape_cell(lead.last_name),
        title=_escape_cell(lead.title),
        company=_escape_cell(lead.company),
        domain=_escape_cell(lead.domain),
        source_url=_escape_cell(lead.source_url),
        icp_score=lead.icp_score,
        verify_status=lead.verify_status,
        verified_at=lead.verified_at,
    )
