# src/api/pipeline.py - Sales Pipeline Tracker API
#
# Tracks deals from lead discovery through placement.
# Wired into existing companies/people/emails tables.

from __future__ import annotations

import csv
import io
import json
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from src.api.deps import AuthContext, get_auth_context

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_conn():
    from src.db import get_conn
    return get_conn()


def _utc_now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


VALID_STAGES = [
    "new_lead", "qualified", "discovery_scheduled", "discovery_completed",
    "exec_intro", "profiles_delivered", "placement_in_progress",
    "closed_won", "closed_lost",
]

VALID_SOURCES = [
    "crestwell_iq", "email_outreach", "inbound_contact", "inbound_calendly",
    "inbound_quiz", "referral", "manual",
]

VALID_ACTIVITY_TYPES = [
    "email_sent", "email_reply", "call_scheduled", "call_completed",
    "stage_change", "note", "import",
]


def _row_to_deal(row: tuple, columns: list[str]) -> dict[str, Any]:
    """Convert a database row tuple to a deal dict."""
    d: dict[str, Any] = {}
    for i, col in enumerate(columns):
        val = row[i] if i < len(row) else None
        if col in ("created_at", "updated_at", "close_date") and val is not None:
            val = str(val)
        d[col] = val
    return d


def _row_to_activity(row: tuple, columns: list[str]) -> dict[str, Any]:
    d: dict[str, Any] = {}
    for i, col in enumerate(columns):
        val = row[i] if i < len(row) else None
        if col == "metadata" and isinstance(val, str):
            try:
                val = json.loads(val)
            except Exception:
                pass
        if col == "created_at" and val is not None:
            val = str(val)
        d[col] = val
    return d


DEAL_COLUMNS = [
    "id", "tenant_id", "company_name", "company_domain",
    "contact_name", "contact_email", "contact_title", "contact_role",
    "stage", "deal_value", "source", "source_detail",
    "next_action_date", "next_action_desc",
    "close_reason", "close_date",
    "iq_company_id", "iq_email_id",
    "created_at", "updated_at",
]

ACTIVITY_COLUMNS = [
    "id", "tenant_id", "deal_id", "type", "description",
    "touch_number", "metadata", "created_at",
]


# ---------------------------------------------------------------------------
# Request Models
# ---------------------------------------------------------------------------

class CreateDealRequest(BaseModel):
    company_name: str
    company_domain: str | None = None
    contact_name: str
    contact_email: str | None = None
    contact_title: str | None = None
    contact_role: str | None = None
    stage: str = "new_lead"
    deal_value: float = 0
    source: str = "manual"
    source_detail: str | None = None
    next_action_date: str | None = None
    next_action_desc: str | None = None
    iq_company_id: int | None = None
    iq_email_id: int | None = None


class UpdateDealRequest(BaseModel):
    company_name: str | None = None
    company_domain: str | None = None
    contact_name: str | None = None
    contact_email: str | None = None
    contact_title: str | None = None
    contact_role: str | None = None
    stage: str | None = None
    deal_value: float | None = None
    source: str | None = None
    source_detail: str | None = None
    next_action_date: str | None = None
    next_action_desc: str | None = None
    close_reason: str | None = None


class CreateActivityRequest(BaseModel):
    type: str
    description: str | None = None
    touch_number: int | None = None
    metadata: dict | None = None


class ImportIQRequest(BaseModel):
    min_icp_score: int = 0
    limit: int = 100
    contact_ids: list[int] | None = None  # specific email IDs to import


class ImportCSVRequest(BaseModel):
    contacts: list[dict[str, Any]]


# ---------------------------------------------------------------------------
# DEALS CRUD
# ---------------------------------------------------------------------------

@router.get("/deals")
def list_deals(
    auth: AuthContext = Depends(get_auth_context),
    stage: str | None = Query(None),
    source: str | None = Query(None),
    search: str | None = Query(None),
    overdue: bool = Query(False),
    sort: str = Query("updated_at"),
    order: str = Query("desc"),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
) -> dict:
    """List deals with filtering, search, and pagination."""
    con = _get_conn()

    conditions = ["d.tenant_id = %s"]
    params: list[Any] = [auth.tenant_id]

    if stage:
        conditions.append("d.stage = %s")
        params.append(stage)

    if source:
        conditions.append("d.source = %s")
        params.append(source)

    if search:
        conditions.append("(d.company_name ILIKE %s OR d.contact_name ILIKE %s OR d.contact_email ILIKE %s)")
        pattern = f"%{search}%"
        params.extend([pattern, pattern, pattern])

    if overdue:
        conditions.append("d.next_action_date IS NOT NULL")
        conditions.append("d.next_action_date < CURRENT_DATE::TEXT")
        conditions.append("d.stage NOT IN ('closed_won', 'closed_lost')")

    where_clause = " AND ".join(conditions)

    # Validate sort column
    allowed_sorts = {"created_at", "updated_at", "company_name", "deal_value", "stage", "next_action_date"}
    if sort not in allowed_sorts:
        sort = "updated_at"
    order_dir = "DESC" if order.lower() == "desc" else "ASC"

    # Count total
    count_row = con.execute(
        f"SELECT COUNT(*) FROM pipeline_deals d WHERE {where_clause}",
        params,
    ).fetchone()
    total = count_row[0] if count_row else 0

    # Fetch page
    offset = (page - 1) * limit
    cols = ", ".join(f"d.{c}" for c in DEAL_COLUMNS)
    rows = con.execute(
        f"SELECT {cols} FROM pipeline_deals d WHERE {where_clause} "
        f"ORDER BY d.{sort} {order_dir} LIMIT %s OFFSET %s",
        params + [limit, offset],
    ).fetchall()

    deals = [_row_to_deal(r, DEAL_COLUMNS) for r in rows]

    return {
        "deals": deals,
        "total": total,
        "page": page,
        "limit": limit,
        "has_next": (page * limit) < total,
    }


@router.post("/deals")
def create_deal(
    body: CreateDealRequest,
    auth: AuthContext = Depends(get_auth_context),
) -> dict:
    """Create a new deal."""
    con = _get_conn()

    if body.stage and body.stage not in VALID_STAGES:
        raise HTTPException(400, detail=f"Invalid stage: {body.stage}")
    if body.source and body.source not in VALID_SOURCES:
        raise HTTPException(400, detail=f"Invalid source: {body.source}")

    now = _utc_now_iso()

    row = con.execute(
        """INSERT INTO pipeline_deals
           (tenant_id, company_name, company_domain,
            contact_name, contact_email, contact_title, contact_role,
            stage, deal_value, source, source_detail,
            next_action_date, next_action_desc,
            iq_company_id, iq_email_id, created_at, updated_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
           RETURNING id""",
        (
            auth.tenant_id, body.company_name, body.company_domain,
            body.contact_name, body.contact_email, body.contact_title, body.contact_role,
            body.stage, body.deal_value, body.source, body.source_detail,
            body.next_action_date, body.next_action_desc,
            body.iq_company_id, body.iq_email_id, now, now,
        ),
    ).fetchone()
    deal_id = row[0]

    # Record initial stage in history
    con.execute(
        """INSERT INTO pipeline_stage_history (deal_id, tenant_id, from_stage, to_stage, entered_at)
           VALUES (%s, %s, NULL, %s, %s)""",
        (deal_id, auth.tenant_id, body.stage, now),
    )

    # Log creation activity
    con.execute(
        """INSERT INTO pipeline_activities (tenant_id, deal_id, type, description, created_at)
           VALUES (%s, %s, 'note', 'Deal created', %s)""",
        (auth.tenant_id, deal_id, now),
    )

    con.commit()

    return {"id": deal_id, "stage": body.stage, "created_at": now}


@router.get("/deals/{deal_id}")
def get_deal(
    deal_id: int,
    auth: AuthContext = Depends(get_auth_context),
) -> dict:
    """Get a single deal with its activities and stage history."""
    con = _get_conn()

    cols = ", ".join(DEAL_COLUMNS)
    row = con.execute(
        f"SELECT {cols} FROM pipeline_deals WHERE id = %s AND tenant_id = %s",
        (deal_id, auth.tenant_id),
    ).fetchone()

    if not row:
        raise HTTPException(404, detail="Deal not found")

    deal = _row_to_deal(row, DEAL_COLUMNS)

    # Fetch activities
    act_cols = ", ".join(ACTIVITY_COLUMNS)
    act_rows = con.execute(
        f"SELECT {act_cols} FROM pipeline_activities WHERE deal_id = %s AND tenant_id = %s ORDER BY created_at DESC LIMIT 50",
        (deal_id, auth.tenant_id),
    ).fetchall()
    activities = [_row_to_activity(r, ACTIVITY_COLUMNS) for r in act_rows]

    # Fetch stage history
    sh_rows = con.execute(
        "SELECT id, deal_id, from_stage, to_stage, entered_at FROM pipeline_stage_history WHERE deal_id = %s ORDER BY entered_at ASC",
        (deal_id,),
    ).fetchall()
    stage_history = [
        {"id": r[0], "deal_id": r[1], "from_stage": r[2], "to_stage": r[3], "entered_at": str(r[4])}
        for r in sh_rows
    ]

    deal["activities"] = activities
    deal["stage_history"] = stage_history

    return deal


@router.patch("/deals/{deal_id}")
def update_deal(
    deal_id: int,
    body: UpdateDealRequest,
    auth: AuthContext = Depends(get_auth_context),
) -> dict:
    """Update a deal. Auto-logs stage changes."""
    con = _get_conn()

    # Fetch current deal
    current = con.execute(
        "SELECT stage FROM pipeline_deals WHERE id = %s AND tenant_id = %s",
        (deal_id, auth.tenant_id),
    ).fetchone()

    if not current:
        raise HTTPException(404, detail="Deal not found")

    current_stage = current[0]
    now = _utc_now_iso()

    # Build SET clause dynamically from non-None fields
    updates: list[str] = []
    params: list[Any] = []
    fields = body.model_dump(exclude_none=True)

    if not fields:
        raise HTTPException(400, detail="No fields to update")

    # Validate stage if changing
    if "stage" in fields and fields["stage"] not in VALID_STAGES:
        raise HTTPException(400, detail=f"Invalid stage: {fields['stage']}")

    # Require close_reason for closing stages
    new_stage = fields.get("stage")
    if new_stage in ("closed_won", "closed_lost"):
        if not fields.get("close_reason") and not body.close_reason:
            raise HTTPException(400, detail="close_reason is required when closing a deal")
        if "close_date" not in fields:
            fields["close_date"] = now[:10]  # YYYY-MM-DD

    for key, val in fields.items():
        updates.append(f"{key} = %s")
        params.append(val)

    updates.append("updated_at = %s")
    params.append(now)

    params.extend([deal_id, auth.tenant_id])

    con.execute(
        f"UPDATE pipeline_deals SET {', '.join(updates)} WHERE id = %s AND tenant_id = %s",
        params,
    )

    # If stage changed, log it
    if new_stage and new_stage != current_stage:
        con.execute(
            """INSERT INTO pipeline_stage_history (deal_id, tenant_id, from_stage, to_stage, entered_at)
               VALUES (%s, %s, %s, %s, %s)""",
            (deal_id, auth.tenant_id, current_stage, new_stage, now),
        )
        con.execute(
            """INSERT INTO pipeline_activities (tenant_id, deal_id, type, description, created_at)
               VALUES (%s, %s, 'stage_change', %s, %s)""",
            (auth.tenant_id, deal_id, f"Stage changed: {current_stage} → {new_stage}", now),
        )

    con.commit()

    return {"id": deal_id, "updated_at": now, "stage": new_stage or current_stage}


@router.delete("/deals/{deal_id}")
def delete_deal(
    deal_id: int,
    auth: AuthContext = Depends(get_auth_context),
) -> dict:
    """Delete a deal and all related records (cascading)."""
    con = _get_conn()

    row = con.execute(
        "SELECT id FROM pipeline_deals WHERE id = %s AND tenant_id = %s",
        (deal_id, auth.tenant_id),
    ).fetchone()

    if not row:
        raise HTTPException(404, detail="Deal not found")

    con.execute("DELETE FROM pipeline_deals WHERE id = %s AND tenant_id = %s", (deal_id, auth.tenant_id))
    con.commit()

    return {"deleted": True, "id": deal_id}


# ---------------------------------------------------------------------------
# ACTIVITIES
# ---------------------------------------------------------------------------

@router.get("/deals/{deal_id}/activities")
def list_activities(
    deal_id: int,
    auth: AuthContext = Depends(get_auth_context),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
) -> dict:
    """List activities for a deal."""
    con = _get_conn()

    # Verify deal ownership
    d = con.execute(
        "SELECT id FROM pipeline_deals WHERE id = %s AND tenant_id = %s",
        (deal_id, auth.tenant_id),
    ).fetchone()
    if not d:
        raise HTTPException(404, detail="Deal not found")

    offset = (page - 1) * limit
    cols = ", ".join(ACTIVITY_COLUMNS)
    rows = con.execute(
        f"SELECT {cols} FROM pipeline_activities WHERE deal_id = %s AND tenant_id = %s ORDER BY created_at DESC LIMIT %s OFFSET %s",
        (deal_id, auth.tenant_id, limit, offset),
    ).fetchall()

    return {
        "activities": [_row_to_activity(r, ACTIVITY_COLUMNS) for r in rows],
        "page": page,
        "has_next": len(rows) >= limit,
    }


@router.post("/deals/{deal_id}/activities")
def create_activity(
    deal_id: int,
    body: CreateActivityRequest,
    auth: AuthContext = Depends(get_auth_context),
) -> dict:
    """Add an activity to a deal."""
    con = _get_conn()

    if body.type not in VALID_ACTIVITY_TYPES:
        raise HTTPException(400, detail=f"Invalid activity type: {body.type}")

    # Verify deal ownership
    d = con.execute(
        "SELECT id FROM pipeline_deals WHERE id = %s AND tenant_id = %s",
        (deal_id, auth.tenant_id),
    ).fetchone()
    if not d:
        raise HTTPException(404, detail="Deal not found")

    now = _utc_now_iso()
    metadata_json = json.dumps(body.metadata) if body.metadata else None

    row = con.execute(
        """INSERT INTO pipeline_activities (tenant_id, deal_id, type, description, touch_number, metadata, created_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id""",
        (auth.tenant_id, deal_id, body.type, body.description, body.touch_number, metadata_json, now),
    ).fetchone()

    # Update deal's updated_at timestamp
    con.execute(
        "UPDATE pipeline_deals SET updated_at = %s WHERE id = %s",
        (now, deal_id),
    )

    con.commit()

    return {"id": row[0], "type": body.type, "created_at": now}


# ---------------------------------------------------------------------------
# METRICS
# ---------------------------------------------------------------------------

@router.get("/metrics")
def get_metrics(
    auth: AuthContext = Depends(get_auth_context),
) -> dict:
    """Dashboard metrics: counts by stage, pipeline value, overdue, win rate."""
    con = _get_conn()
    tid = auth.tenant_id

    # Deals by stage (excluding closed)
    stage_rows = con.execute(
        """SELECT stage, COUNT(*), COALESCE(SUM(deal_value), 0)
           FROM pipeline_deals
           WHERE tenant_id = %s AND stage NOT IN ('closed_won', 'closed_lost')
           GROUP BY stage ORDER BY stage""",
        (tid,),
    ).fetchall()

    stages = [{"stage": r[0], "count": r[1], "value": float(r[2])} for r in stage_rows]

    # Total pipeline value (open deals)
    total_row = con.execute(
        "SELECT COALESCE(SUM(deal_value), 0), COUNT(*) FROM pipeline_deals WHERE tenant_id = %s AND stage NOT IN ('closed_won', 'closed_lost')",
        (tid,),
    ).fetchone()
    total_value = float(total_row[0]) if total_row else 0
    open_count = total_row[1] if total_row else 0

    # Overdue follow-ups
    overdue_row = con.execute(
        """SELECT COUNT(*) FROM pipeline_deals
           WHERE tenant_id = %s AND next_action_date IS NOT NULL
           AND next_action_date < CURRENT_DATE::TEXT
           AND stage NOT IN ('closed_won', 'closed_lost')""",
        (tid,),
    ).fetchone()
    overdue_count = overdue_row[0] if overdue_row else 0

    # Overdue deals list (top 10)
    overdue_deals = con.execute(
        """SELECT id, company_name, contact_name, next_action_date, next_action_desc, stage
           FROM pipeline_deals
           WHERE tenant_id = %s AND next_action_date IS NOT NULL
           AND next_action_date < CURRENT_DATE::TEXT
           AND stage NOT IN ('closed_won', 'closed_lost')
           ORDER BY next_action_date ASC LIMIT 10""",
        (tid,),
    ).fetchall()
    overdue_list = [
        {"id": r[0], "company_name": r[1], "contact_name": r[2],
         "next_action_date": r[3], "next_action_desc": r[4], "stage": r[5]}
        for r in overdue_deals
    ]

    # Win/loss counts
    won_row = con.execute(
        "SELECT COUNT(*) FROM pipeline_deals WHERE tenant_id = %s AND stage = 'closed_won'",
        (tid,),
    ).fetchone()
    lost_row = con.execute(
        "SELECT COUNT(*) FROM pipeline_deals WHERE tenant_id = %s AND stage = 'closed_lost'",
        (tid,),
    ).fetchone()
    won = won_row[0] if won_row else 0
    lost = lost_row[0] if lost_row else 0
    win_rate = round(won / (won + lost) * 100, 1) if (won + lost) > 0 else 0

    # Recent activities (across all deals)
    recent_rows = con.execute(
        """SELECT a.id, a.deal_id, a.type, a.description, a.touch_number, a.created_at,
                  d.company_name, d.contact_name
           FROM pipeline_activities a
           JOIN pipeline_deals d ON d.id = a.deal_id
           WHERE a.tenant_id = %s
           ORDER BY a.created_at DESC LIMIT 15""",
        (tid,),
    ).fetchall()
    recent_activities = [
        {"id": r[0], "deal_id": r[1], "type": r[2], "description": r[3],
         "touch_number": r[4], "created_at": str(r[5]),
         "company_name": r[6], "contact_name": r[7]}
        for r in recent_rows
    ]

    # Average days per stage (from stage history)
    avg_rows = con.execute(
        """SELECT sh1.to_stage,
                  AVG(EXTRACT(EPOCH FROM (sh2.entered_at - sh1.entered_at)) / 86400)
           FROM pipeline_stage_history sh1
           JOIN pipeline_stage_history sh2
             ON sh2.deal_id = sh1.deal_id
             AND sh2.entered_at > sh1.entered_at
             AND sh2.id = (
               SELECT MIN(sh3.id) FROM pipeline_stage_history sh3
               WHERE sh3.deal_id = sh1.deal_id AND sh3.entered_at > sh1.entered_at
             )
           WHERE sh1.tenant_id = %s
           GROUP BY sh1.to_stage""",
        (tid,),
    ).fetchall()
    avg_days_per_stage = {r[0]: round(float(r[1]), 1) if r[1] else 0 for r in avg_rows}

    return {
        "stages": stages,
        "total_pipeline_value": total_value,
        "open_deals": open_count,
        "overdue_count": overdue_count,
        "overdue_deals": overdue_list,
        "won": won,
        "lost": lost,
        "win_rate": win_rate,
        "recent_activities": recent_activities,
        "avg_days_per_stage": avg_days_per_stage,
    }


# ---------------------------------------------------------------------------
# IMPORT: From CrestwellIQ (direct DB query)
# ---------------------------------------------------------------------------

@router.post("/import/iq")
def import_from_iq(
    body: ImportIQRequest,
    auth: AuthContext = Depends(get_auth_context),
) -> dict:
    """Import verified contacts from CrestwellIQ into the pipeline."""
    con = _get_conn()
    tid = auth.tenant_id
    now = _utc_now_iso()

    # Build query to fetch contacts from emails table
    conditions = [
        "e.tenant_id = %s",
        "vr.verify_status = 'valid'",
    ]
    params: list[Any] = [tid]

    if body.min_icp_score > 0:
        conditions.append("e.icp_score >= %s")
        params.append(body.min_icp_score)

    if body.contact_ids:
        placeholders = ", ".join(["%s"] * len(body.contact_ids))
        conditions.append(f"e.id IN ({placeholders})")
        params.extend(body.contact_ids)

    where = " AND ".join(conditions)
    params.append(body.limit)

    rows = con.execute(
        f"""SELECT e.id, e.email, p.full_name, p.title, p.first_name, p.last_name,
                   c.name AS company_name, c.domain AS company_domain,
                   e.icp_score, c.id AS company_id
            FROM emails e
            LEFT JOIN people p ON p.id = e.person_id AND p.tenant_id = e.tenant_id
            LEFT JOIN companies c ON c.id = e.company_id AND c.tenant_id = e.tenant_id
            LEFT JOIN LATERAL (
                SELECT verify_status FROM verification_results
                WHERE email_id = e.id AND tenant_id = e.tenant_id
                ORDER BY verified_at DESC NULLS LAST LIMIT 1
            ) vr ON TRUE
            WHERE {where}
            ORDER BY e.icp_score DESC NULLS LAST, e.created_at DESC
            LIMIT %s""",
        params,
    ).fetchall()

    if not rows:
        return {"imported": 0, "skipped": 0, "message": "No matching contacts found"}

    # Check which emails already exist in import log
    emails_in_rows = [r[1] for r in rows if r[1]]
    if emails_in_rows:
        placeholders = ", ".join(["%s"] * len(emails_in_rows))
        existing = con.execute(
            f"SELECT source_identifier FROM pipeline_import_log WHERE tenant_id = %s AND source = 'crestwell_iq' AND source_identifier IN ({placeholders})",
            [tid] + emails_in_rows,
        ).fetchall()
        existing_emails = {r[0] for r in existing}
    else:
        existing_emails = set()

    imported = 0
    skipped = 0

    for row in rows:
        email_id, email_addr, full_name, title, first_name, last_name, company_name, company_domain, icp_score, company_id = row

        if not email_addr or email_addr in existing_emails:
            skipped += 1
            continue

        contact_name = full_name or f"{first_name or ''} {last_name or ''}".strip() or "Unknown"

        deal_row = con.execute(
            """INSERT INTO pipeline_deals
               (tenant_id, company_name, company_domain,
                contact_name, contact_email, contact_title,
                stage, source, source_detail,
                iq_company_id, iq_email_id, created_at, updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,'new_lead','crestwell_iq',%s,%s,%s,%s,%s)
               RETURNING id""",
            (
                tid, company_name or "Unknown", company_domain,
                contact_name, email_addr, title,
                f"ICP score: {icp_score}" if icp_score else None,
                company_id, email_id, now, now,
            ),
        ).fetchone()
        deal_id = deal_row[0]

        # Stage history
        con.execute(
            "INSERT INTO pipeline_stage_history (deal_id, tenant_id, from_stage, to_stage, entered_at) VALUES (%s,%s,NULL,'new_lead',%s)",
            (deal_id, tid, now),
        )

        # Activity
        con.execute(
            "INSERT INTO pipeline_activities (tenant_id, deal_id, type, description, created_at) VALUES (%s,%s,'import',%s,%s)",
            (tid, deal_id, f"Imported from CrestwellIQ (ICP: {icp_score or 'N/A'})", now),
        )

        # Import log
        con.execute(
            "INSERT INTO pipeline_import_log (tenant_id, source, source_identifier, deal_id, imported_at) VALUES (%s,'crestwell_iq',%s,%s,%s)",
            (tid, email_addr, deal_id, now),
        )

        imported += 1

    con.commit()

    return {"imported": imported, "skipped": skipped}


# ---------------------------------------------------------------------------
# IMPORT: From Email Outreach CSV
# ---------------------------------------------------------------------------

@router.post("/import/csv")
def import_from_csv(
    body: ImportCSVRequest,
    auth: AuthContext = Depends(get_auth_context),
) -> dict:
    """Import contacts from email automation CSV export."""
    con = _get_conn()
    tid = auth.tenant_id
    now = _utc_now_iso()

    if not body.contacts:
        raise HTTPException(400, detail="No contacts provided")

    # Group contacts by email to merge touches
    by_email: dict[str, list[dict]] = {}
    for c in body.contacts:
        email = (c.get("email") or "").strip().lower()
        if email:
            by_email.setdefault(email, []).append(c)

    # Check existing imports
    all_emails = list(by_email.keys())
    if all_emails:
        placeholders = ", ".join(["%s"] * len(all_emails))
        existing = con.execute(
            f"SELECT source_identifier, deal_id FROM pipeline_import_log WHERE tenant_id = %s AND source = 'email_outreach' AND source_identifier IN ({placeholders})",
            [tid] + all_emails,
        ).fetchall()
        existing_map = {r[0]: r[1] for r in existing}
    else:
        existing_map = {}

    deals_created = 0
    deals_updated = 0
    touches_logged = 0

    for email_addr, contacts in by_email.items():
        first_contact = contacts[0]
        company_name = first_contact.get("company_name") or first_contact.get("full_name", "Unknown")
        company_domain = first_contact.get("company_domain", "")

        if email_addr in existing_map:
            # Deal exists, just log new touches
            deal_id = existing_map[email_addr]
            deals_updated += 1
        else:
            # Create new deal
            deal_row = con.execute(
                """INSERT INTO pipeline_deals
                   (tenant_id, company_name, company_domain,
                    contact_name, contact_email, contact_title, contact_role,
                    stage, source, created_at, updated_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,'new_lead','email_outreach',%s,%s)
                   RETURNING id""",
                (
                    tid,
                    company_name, company_domain,
                    first_contact.get("full_name", "Unknown"),
                    email_addr,
                    first_contact.get("title"),
                    first_contact.get("role"),
                    now, now,
                ),
            ).fetchone()
            deal_id = deal_row[0]

            con.execute(
                "INSERT INTO pipeline_stage_history (deal_id, tenant_id, from_stage, to_stage, entered_at) VALUES (%s,%s,NULL,'new_lead',%s)",
                (deal_id, tid, now),
            )
            con.execute(
                "INSERT INTO pipeline_import_log (tenant_id, source, source_identifier, deal_id, imported_at) VALUES (%s,'email_outreach',%s,%s,%s)",
                (tid, email_addr, deal_id, now),
            )
            deals_created += 1

        # Log each touch as an activity
        for c in contacts:
            touch = c.get("touch")
            touch_num = int(touch) if touch else None
            date = c.get("date", now[:10])

            # Check if this touch already logged
            existing_touch = con.execute(
                "SELECT id FROM pipeline_activities WHERE deal_id = %s AND type = 'email_sent' AND touch_number = %s",
                (deal_id, touch_num),
            ).fetchone()

            if not existing_touch and touch_num:
                con.execute(
                    """INSERT INTO pipeline_activities (tenant_id, deal_id, type, description, touch_number, metadata, created_at)
                       VALUES (%s,%s,'email_sent',%s,%s,%s,%s)""",
                    (
                        tid, deal_id,
                        f"Touch {touch_num} sent ({c.get('role', 'unknown')} template)",
                        touch_num,
                        json.dumps({"role": c.get("role"), "date": date}),
                        now,
                    ),
                )
                touches_logged += 1

    con.commit()

    return {
        "deals_created": deals_created,
        "deals_updated": deals_updated,
        "touches_logged": touches_logged,
    }


# ---------------------------------------------------------------------------
# EXPORT
# ---------------------------------------------------------------------------

@router.post("/export")
def export_deals(
    auth: AuthContext = Depends(get_auth_context),
    stage: str | None = Query(None),
) -> StreamingResponse:
    """Export deals as CSV."""
    con = _get_conn()

    conditions = ["tenant_id = %s"]
    params: list[Any] = [auth.tenant_id]

    if stage:
        conditions.append("stage = %s")
        params.append(stage)

    where = " AND ".join(conditions)
    export_cols = [
        "id", "company_name", "company_domain",
        "contact_name", "contact_email", "contact_title", "contact_role",
        "stage", "deal_value", "source", "source_detail",
        "next_action_date", "next_action_desc",
        "close_reason", "close_date",
        "created_at", "updated_at",
    ]

    rows = con.execute(
        f"SELECT {', '.join(export_cols)} FROM pipeline_deals WHERE {where} ORDER BY created_at DESC",
        params,
    ).fetchall()

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(export_cols)
    for row in rows:
        writer.writerow([str(v) if v is not None else "" for v in row])

    buf.seek(0)
    filename = f"pipeline_deals_{datetime.now(UTC).strftime('%Y-%m-%d')}.csv"

    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
