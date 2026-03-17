# src/queueing/manual_candidates.py
"""
Manual candidate verification task.

Processes a batch of user-submitted candidates for a company:
  - Candidates WITH an email  → verify directly via SMTP
  - Candidates WITHOUT an email → generate permutations, then verify sequentially
  - Valid   → keep person + email rows, update audit row
  - Invalid → delete person + email rows, update audit row (audit survives)

Reuses existing sequential verification machinery from tasks.py.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from typing import Any

log = logging.getLogger(__name__)

MANUAL_SOURCE_URL = "manual:user_added"


def _utc_now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _get_conn():
    from src.db import get_conn

    return get_conn()


# ---------------------------------------------------------------------------
# DB helpers  (use ? placeholders — CompatConnection translates for Postgres)
# ---------------------------------------------------------------------------


def _insert_email_for_manual(
    con: Any,
    *,
    tenant_id: str,
    company_id: int,
    person_id: int,
    email: str,
) -> int | None:
    """Insert a manual email row.  Returns email_id or None on failure."""
    now = _utc_now_iso()
    email_norm = email.strip().lower()
    try:
        # CompatConnection translates INSERT OR IGNORE → ON CONFLICT DO NOTHING
        con.execute(
            "INSERT OR IGNORE INTO emails"
            " (tenant_id, company_id, person_id, email, source_url,"
            "  is_published, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, 0, ?, ?)",
            (tenant_id, company_id, person_id, email_norm, MANUAL_SOURCE_URL, now, now),
        )
        con.commit()

        row = con.execute(
            "SELECT id FROM emails WHERE tenant_id = ? AND email = ? LIMIT 1",
            (tenant_id, email_norm),
        ).fetchone()
        if row:
            # Ensure person_id is linked even on conflict (upsert semantics)
            con.execute(
                "UPDATE emails SET person_id = ?, updated_at = ? WHERE id = ?",
                (person_id, now, row[0]),
            )
            con.commit()
            return int(row[0])
        return None
    except Exception:
        log.debug("Failed to insert manual email %s", email_norm, exc_info=True)
        try:
            con.rollback()
        except Exception:
            pass
        return None


def _update_attempt_outcome(
    con: Any,
    attempt_id: int,
    *,
    outcome: str,
    verified_email: str | None = None,
    verify_status: str | None = None,
    verify_reason: str | None = None,
    error_detail: str | None = None,
    person_id: int | None = None,
    email_id: int | None = None,
) -> None:
    """Update the audit row with the verification outcome."""
    con.execute(
        "UPDATE manual_candidate_attempts"
        " SET outcome = ?, verified_email = ?, verify_status = ?,"
        "     verify_reason = ?, error_detail = ?, person_id = ?,"
        "     email_id = ?, processed_at = ?"
        " WHERE id = ?",
        (
            outcome,
            verified_email,
            verify_status,
            verify_reason,
            error_detail,
            person_id,
            email_id,
            _utc_now_iso(),
            attempt_id,
        ),
    )
    con.commit()


def _delete_person_and_emails(con: Any, person_id: int) -> None:
    """Delete a person and all their emails.

    verification_results cascade via FK on emails.
    """
    try:
        con.execute("DELETE FROM emails WHERE person_id = ?", (person_id,))
        con.execute("DELETE FROM people WHERE id = ?", (person_id,))
        con.commit()
        log.info("Cleaned up invalid manual candidate: person_id=%s", person_id)
    except Exception:
        log.warning("Failed to clean up person_id=%s", person_id, exc_info=True)
        try:
            con.rollback()
        except Exception:
            pass


def _get_company_domain(con: Any, company_id: int, tenant_id: str) -> str | None:
    """Resolve the canonical domain for a company."""
    row = con.execute(
        "SELECT COALESCE(official_domain, domain, user_supplied_domain)"
        " FROM companies"
        " WHERE id = ? AND tenant_id = ?",
        (company_id, tenant_id),
    ).fetchone()
    return row[0].strip().lower() if row and row[0] else None


# ---------------------------------------------------------------------------
# Per-candidate verification logic
# ---------------------------------------------------------------------------


def _verify_submitted_email(
    con: Any,
    *,
    email: str,
    person_id: int,
    company_id: int,
    tenant_id: str,
    domain: str,
) -> dict[str, Any]:
    """
    Verify a user-provided email address directly via SMTP.

    Reuses _verify_permutation_with_retry and _persist_sequential_verification_result
    from the existing tasks.py sequential pipeline.
    """
    from src.queueing.tasks import (
        _load_catchall_status_for_domain,
        _mx_info,
        _persist_sequential_verification_result,
        _verify_permutation_with_retry,
    )

    db_path = os.getenv("DATABASE_PATH") or "data/dev.db"
    email_norm = email.strip().lower()

    # Resolve MX
    mx_host, _ = _mx_info(domain, force=False, db_path=db_path)
    if not mx_host:
        return {"status": "no_mx", "reason": "no_mx_host", "email": email_norm, "email_id": None}

    # Check catch-all status
    catch_all_status = _load_catchall_status_for_domain(db_path, domain, tenant_id=tenant_id)

    # Insert the email row
    email_id = _insert_email_for_manual(
        con,
        tenant_id=tenant_id,
        company_id=company_id,
        person_id=person_id,
        email=email_norm,
    )

    # Verify with retries
    result = _verify_permutation_with_retry(
        email_addr=email_norm,
        mx_host=mx_host,
        catch_all_status=catch_all_status,
        max_retries=3,
    )

    status = result.get("status", "unknown")
    reason = result.get("reason", "")
    code = result.get("code")

    # Persist verification result
    if email_id is not None:
        try:
            _persist_sequential_verification_result(
                con=con,
                email_id=email_id,
                email=email_norm,
                domain=domain,
                mx_host=mx_host,
                status=status,
                reason=reason,
                code=code,
                catch_all_status=catch_all_status,
                company_id=company_id,
                person_id=person_id,
            )
            con.commit()
        except Exception:
            log.debug("Failed to persist verification result for manual email", exc_info=True)
            try:
                con.rollback()
            except Exception:
                pass

    return {
        "status": status,
        "reason": reason,
        "code": code,
        "email": email_norm,
        "email_id": email_id,
        "mx_host": mx_host,
    }


def _generate_and_verify_for_person(
    *,
    person_id: int,
    first_name: str,
    last_name: str,
    domain: str,
) -> dict[str, Any]:
    """
    Generate email permutations and verify sequentially for a name-only candidate.

    Delegates to task_generate_emails which already handles sequential
    generation + verification + cleanup of invalid permutations.
    """
    from src.queueing.tasks import task_generate_emails

    result = task_generate_emails(
        person_id=person_id,
        first=first_name or "",
        last=last_name or "",
        domain=domain,
    )

    valid_email = result.get("valid_email")
    status = result.get("status", "exhausted")

    return {
        "status": "valid" if valid_email else "invalid",
        "reason": status,
        "email": valid_email,
        "attempts": result.get("attempts", []),
        "total_probes": result.get("verified", 0),
    }


# ---------------------------------------------------------------------------
# Main RQ task
# ---------------------------------------------------------------------------


def task_verify_manual_candidates(  # noqa: C901
    *,
    tenant_id: str,
    company_id: int,
    batch_id: str,
) -> dict[str, Any]:
    """
    RQ task: process all pending manual_candidate_attempts for a batch.

    For each candidate in the batch:
      1. If submitted_email → verify directly
      2. If no email → generate permutations + sequential verify
      3. Valid   → update audit row, keep person + email
      4. Invalid → update audit row, delete person + email
    """
    con = _get_conn()
    results: list[dict[str, Any]] = []
    valid_count = 0
    invalid_count = 0
    error_count = 0

    try:
        domain = _get_company_domain(con, company_id, tenant_id)
        if not domain:
            log.error(
                "Manual candidates: no domain for company_id=%s",
                company_id,
            )
            con.execute(
                "UPDATE manual_candidate_attempts"
                " SET outcome = 'error',"
                "     error_detail = 'No domain found for company',"
                "     processed_at = ?"
                " WHERE batch_id = ? AND outcome = 'pending'",
                (_utc_now_iso(), batch_id),
            )
            con.commit()
            return {"ok": False, "error": "no_domain", "batch_id": batch_id}

        rows = con.execute(
            "SELECT id, first_name, last_name, full_name, title,"
            "       submitted_email, person_id"
            " FROM manual_candidate_attempts"
            " WHERE batch_id = ? AND tenant_id = ? AND outcome = 'pending'"
            " ORDER BY id",
            (batch_id, tenant_id),
        ).fetchall()

        total = len(rows)
        log.info(
            "Manual candidates: processing batch %s (%d candidates) for company %s domain %s",
            batch_id,
            total,
            company_id,
            domain,
        )

        for row in rows:
            attempt_id = row[0]
            first_name = row[1]
            last_name = row[2]
            full_name = row[3]
            title = row[4]
            submitted_email = row[5]
            person_id = row[6]

            entry: dict[str, Any] = {
                "attempt_id": attempt_id,
                "name": full_name or f"{first_name or ''} {last_name or ''}".strip(),
                "title": title,
                "submitted_email": submitted_email,
            }

            try:
                if submitted_email:
                    # ---- Mode A: user provided an email → verify directly ----
                    vr = _verify_submitted_email(
                        con,
                        email=submitted_email,
                        person_id=person_id,
                        company_id=company_id,
                        tenant_id=tenant_id,
                        domain=domain,
                    )

                    is_valid = vr["status"] in ("valid", "risky_catch_all")
                    outcome = (
                        "valid" if is_valid else ("no_mx" if vr["status"] == "no_mx" else "invalid")
                    )

                    _update_attempt_outcome(
                        con,
                        attempt_id,
                        outcome=outcome,
                        verified_email=submitted_email if is_valid else None,
                        verify_status=vr["status"],
                        verify_reason=vr["reason"],
                        person_id=person_id if is_valid else None,
                        email_id=vr.get("email_id") if is_valid else None,
                    )

                    if is_valid:
                        valid_count += 1
                        entry["outcome"] = "valid"
                        entry["verified_email"] = submitted_email
                    else:
                        invalid_count += 1
                        entry["outcome"] = "invalid"
                        entry["reason"] = vr["reason"]
                        _delete_person_and_emails(con, person_id)

                else:
                    # ---- Mode B: name only → generate + verify ----
                    gv = _generate_and_verify_for_person(
                        person_id=person_id,
                        first_name=first_name or "",
                        last_name=last_name or "",
                        domain=domain,
                    )

                    is_valid = gv["status"] == "valid" and gv.get("email")
                    outcome = "valid" if is_valid else "invalid"

                    _update_attempt_outcome(
                        con,
                        attempt_id,
                        outcome=outcome,
                        verified_email=gv.get("email") if is_valid else None,
                        verify_status="valid" if is_valid else "invalid",
                        verify_reason=gv.get("reason", ""),
                        person_id=person_id if is_valid else None,
                    )

                    if is_valid:
                        valid_count += 1
                        entry["outcome"] = "valid"
                        entry["verified_email"] = gv["email"]
                    else:
                        invalid_count += 1
                        entry["outcome"] = "invalid"
                        entry["reason"] = gv.get("reason", "no_valid_permutation")
                        _delete_person_and_emails(con, person_id)

            except Exception as exc:
                error_count += 1
                entry["outcome"] = "error"
                entry["error"] = str(exc)
                log.exception(
                    "Manual candidate verification error: attempt_id=%s",
                    attempt_id,
                )
                _update_attempt_outcome(
                    con,
                    attempt_id,
                    outcome="error",
                    error_detail=str(exc)[:500],
                )
                if person_id:
                    _delete_person_and_emails(con, person_id)

            results.append(entry)

        log.info(
            "Manual candidates batch %s complete: %d total, %d valid, %d invalid, %d errors",
            batch_id,
            total,
            valid_count,
            invalid_count,
            error_count,
        )

        return {
            "ok": True,
            "batch_id": batch_id,
            "company_id": company_id,
            "domain": domain,
            "total": total,
            "valid": valid_count,
            "invalid": invalid_count,
            "errors": error_count,
            "results": results,
        }

    except Exception as exc:
        log.exception("Manual candidates batch %s failed", batch_id)
        return {"ok": False, "batch_id": batch_id, "error": str(exc)}

    finally:
        try:
            con.close()
        except Exception:
            pass
