#!/bin/bash
# setup_dashboard.sh
# Complete setup script for Email Scraper Dashboard
# Run with: bash setup_dashboard.sh

set -e

echo "=============================================="
echo "Email Scraper Dashboard Setup"
echo "=============================================="

cd /opt/email-scraper

# 1. Backup existing files
echo ""
echo "[1/6] Backing up existing files..."
mkdir -p backups
cp src/queueing/pipeline_v2.py backups/pipeline_v2.py.bak 2>/dev/null || true
cp src/api/browser.py backups/browser.py.bak 2>/dev/null || true

# 2. Fix official_domain in pipeline_v2.py
echo ""
echo "[2/6] Patching pipeline_v2.py to set official_domain..."

# Check if already patched
if grep -q '"official_domain"' src/queueing/pipeline_v2.py; then
    echo "  Already patched - skipping"
else
    # Patch INSERT statement to include official_domain
    sed -i 's/insert_cols: list\[str\] = \["name", "domain"\]/insert_cols: list[str] = ["name", "domain", "official_domain"]/' src/queueing/pipeline_v2.py
    sed -i 's/insert_vals: list\[Any\] = \[company_name, domain\]/insert_vals: list[Any] = [company_name, domain, domain]/' src/queueing/pipeline_v2.py
    echo "  Patched INSERT statement"
fi

# 3. Copy browser.py (PostgreSQL version)
echo ""
echo "[3/6] Installing browser.py API routes..."
cat > src/api/browser.py << 'BROWSER_EOF'
# src/api/browser.py - PostgreSQL version
from __future__ import annotations
import json, os, uuid
from datetime import UTC, datetime
from typing import Any
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

RQ_REDIS_URL = (os.getenv("RQ_REDIS_URL") or os.getenv("REDIS_URL") or "redis://127.0.0.1:6379/0").strip()
router = APIRouter(prefix="/api/browser", tags=["browser"])

def _get_conn():
    from src.db import get_conn
    return get_conn()

def _utc_now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

class RunCreateRequest(BaseModel):
    domains: list[str] = Field(..., min_length=1, max_length=5000)
    ai_enabled: bool = Field(default=True)
    force_discovery: bool = Field(default=False)
    modes: list[str] = Field(default=["full"])
    company_limit: int = Field(default=1000, ge=1, le=5000)

class PaginatedResponse(BaseModel):
    items: list[dict[str, Any]]
    total: int
    page: int
    page_size: int
    total_pages: int

@router.get("/stats")
def get_stats() -> dict[str, Any]:
    con = _get_conn()
    try:
        stats = {}
        for table, key in [("companies","companies"),("people","people"),("emails","emails"),("sources","sources"),("runs","runs"),("verification_results","verifications")]:
            try:
                stats[key] = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            except: stats[key] = 0
        try:
            rows = con.execute("SELECT verify_status, COUNT(*) FROM verification_results WHERE verify_status IS NOT NULL GROUP BY verify_status").fetchall()
            stats["verification_breakdown"] = {r[0]: r[1] for r in rows}
        except: stats["verification_breakdown"] = {}
        try:
            rows = con.execute("SELECT status, COUNT(*) FROM runs GROUP BY status").fetchall()
            stats["runs_by_status"] = {r[0]: r[1] for r in rows}
        except: stats["runs_by_status"] = {}
        return stats
    finally:
        try: con.close()
        except: pass

@router.get("/companies")
def list_companies(page: int = Query(1, ge=1), page_size: int = Query(50, ge=1, le=500), search: str = Query(None)) -> PaginatedResponse:
    con = _get_conn()
    try:
        offset = (page - 1) * page_size
        where_clause, params = "", []
        if search:
            st = f"%{search.lower()}%"
            where_clause = "WHERE LOWER(domain) LIKE %s OR LOWER(name) LIKE %s OR LOWER(official_domain) LIKE %s"
            params = [st, st, st]
        total = con.execute(f"SELECT COUNT(*) FROM companies {where_clause}", tuple(params)).fetchone()[0]
        sql = f"""SELECT c.id, c.name, c.domain, c.official_domain, c.attrs, c.created_at,
            (SELECT COUNT(*) FROM people WHERE company_id = c.id),
            (SELECT COUNT(*) FROM emails WHERE company_id = c.id),
            (SELECT COUNT(*) FROM sources WHERE company_id = c.id)
            FROM companies c {where_clause} ORDER BY c.id DESC LIMIT %s OFFSET %s"""
        params.extend([page_size, offset])
        rows = con.execute(sql, tuple(params)).fetchall()
        items = []
        for r in rows:
            attrs = {}
            if r[4]:
                try: attrs = json.loads(r[4]) if isinstance(r[4], str) else r[4]
                except: pass
            items.append({"id": r[0], "name": r[1], "domain": r[2], "official_domain": r[3],
                "ai_extracted": attrs.get("ai_people_extracted", False), "created_at": r[5],
                "people_count": r[6], "emails_count": r[7], "pages_count": r[8]})
        return PaginatedResponse(items=items, total=total, page=page, page_size=page_size, total_pages=max(1,(total+page_size-1)//page_size))
    finally:
        try: con.close()
        except: pass

@router.get("/companies/{company_id}")
def get_company(company_id: int) -> dict[str, Any]:
    con = _get_conn()
    try:
        row = con.execute("SELECT id, name, domain, official_domain, website_url, attrs, created_at, updated_at FROM companies WHERE id = %s", (company_id,)).fetchone()
        if not row: raise HTTPException(status_code=404, detail="Company not found")
        attrs = {}
        if row[5]:
            try: attrs = json.loads(row[5]) if isinstance(row[5], str) else row[5]
            except: pass
        people = con.execute("SELECT id, first_name, last_name, full_name, title, source_url FROM people WHERE company_id = %s ORDER BY id", (company_id,)).fetchall()
        emails = con.execute("SELECT e.id, e.email, e.source_url, e.person_id, vr.verify_status, vr.verify_reason FROM emails e LEFT JOIN verification_results vr ON vr.email_id = e.id WHERE e.company_id = %s ORDER BY e.id", (company_id,)).fetchall()
        pages = con.execute("SELECT id, source_url, LENGTH(html), fetched_at FROM sources WHERE company_id = %s ORDER BY fetched_at DESC", (company_id,)).fetchall()
        return {"id": row[0], "name": row[1], "domain": row[2], "official_domain": row[3], "website_url": row[4], "attrs": attrs, "created_at": row[6], "updated_at": row[7],
            "people": [{"id": p[0], "first_name": p[1], "last_name": p[2], "full_name": p[3], "title": p[4], "source_url": p[5]} for p in people],
            "emails": [{"id": e[0], "email": e[1], "source_url": e[2], "person_id": e[3], "verify_status": e[4], "verify_reason": e[5]} for e in emails],
            "pages": [{"id": pg[0], "source_url": pg[1], "html_size": pg[2], "fetched_at": pg[3]} for pg in pages]}
    finally:
        try: con.close()
        except: pass

@router.get("/people")
def list_people(page: int = Query(1, ge=1), page_size: int = Query(50, ge=1, le=500), company_id: int = Query(None), search: str = Query(None)) -> PaginatedResponse:
    con = _get_conn()
    try:
        offset = (page - 1) * page_size
        where_parts, params = [], []
        if company_id: where_parts.append("p.company_id = %s"); params.append(company_id)
        if search:
            st = f"%{search.lower()}%"
            where_parts.append("(LOWER(p.first_name) LIKE %s OR LOWER(p.last_name) LIKE %s OR LOWER(p.full_name) LIKE %s OR LOWER(p.title) LIKE %s)")
            params.extend([st, st, st, st])
        where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""
        total = con.execute(f"SELECT COUNT(*) FROM people p {where_clause}", tuple(params)).fetchone()[0]
        sql = f"SELECT p.id, p.first_name, p.last_name, p.full_name, p.title, p.source_url, p.company_id, c.domain FROM people p LEFT JOIN companies c ON c.id = p.company_id {where_clause} ORDER BY p.id DESC LIMIT %s OFFSET %s"
        params.extend([page_size, offset])
        rows = con.execute(sql, tuple(params)).fetchall()
        items = [{"id": r[0], "first_name": r[1], "last_name": r[2], "full_name": r[3], "title": r[4], "source_url": r[5], "company_id": r[6], "company_domain": r[7]} for r in rows]
        return PaginatedResponse(items=items, total=total, page=page, page_size=page_size, total_pages=max(1,(total+page_size-1)//page_size))
    finally:
        try: con.close()
        except: pass

@router.get("/emails")
def list_emails(page: int = Query(1, ge=1), page_size: int = Query(50, ge=1, le=500), company_id: int = Query(None), status: str = Query(None), search: str = Query(None)) -> PaginatedResponse:
    con = _get_conn()
    try:
        offset = (page - 1) * page_size
        where_parts, params = [], []
        if company_id: where_parts.append("e.company_id = %s"); params.append(company_id)
        if status: where_parts.append("vr.verify_status = %s"); params.append(status)
        if search: where_parts.append("LOWER(e.email) LIKE %s"); params.append(f"%{search.lower()}%")
        where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""
        total = con.execute(f"SELECT COUNT(DISTINCT e.id) FROM emails e LEFT JOIN verification_results vr ON vr.email_id = e.id {where_clause}", tuple(params)).fetchone()[0]
        sql = f"SELECT DISTINCT ON (e.id) e.id, e.email, e.source_url, e.company_id, e.person_id, c.domain, p.first_name, p.last_name, vr.verify_status, vr.verify_reason, vr.verified_at FROM emails e LEFT JOIN companies c ON c.id = e.company_id LEFT JOIN people p ON p.id = e.person_id LEFT JOIN verification_results vr ON vr.email_id = e.id {where_clause} ORDER BY e.id DESC LIMIT %s OFFSET %s"
        params.extend([page_size, offset])
        rows = con.execute(sql, tuple(params)).fetchall()
        items = [{"id": r[0], "email": r[1], "source_url": r[2], "company_id": r[3], "person_id": r[4], "company_domain": r[5], "first_name": r[6], "last_name": r[7], "verify_status": r[8], "verify_reason": r[9], "verified_at": r[10]} for r in rows]
        return PaginatedResponse(items=items, total=total, page=page, page_size=page_size, total_pages=max(1,(total+page_size-1)//page_size))
    finally:
        try: con.close()
        except: pass

@router.get("/runs")
def list_runs(page: int = Query(1, ge=1), page_size: int = Query(20, ge=1, le=100)) -> PaginatedResponse:
    con = _get_conn()
    try:
        offset = (page - 1) * page_size
        total = con.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
        rows = con.execute("SELECT id, status, label, domains_json, options_json, progress_json, error, created_at, started_at, finished_at FROM runs ORDER BY created_at DESC LIMIT %s OFFSET %s", (page_size, offset)).fetchall()
        items = []
        for r in rows:
            domains, options, progress = [], {}, {}
            try: domains = json.loads(r[3]) if r[3] else []
            except: pass
            try: options = json.loads(r[4]) if r[4] else {}
            except: pass
            try: progress = json.loads(r[5]) if r[5] else {}
            except: pass
            items.append({"id": r[0], "status": r[1], "label": r[2], "domain_count": len(domains) if isinstance(domains, list) else 0, "domains": domains[:5] if isinstance(domains, list) else [], "options": options, "progress": progress, "error": r[6], "created_at": r[7], "started_at": r[8], "finished_at": r[9]})
        return PaginatedResponse(items=items, total=total, page=page, page_size=page_size, total_pages=max(1,(total+page_size-1)//page_size))
    finally:
        try: con.close()
        except: pass

@router.get("/runs/{run_id}")
def get_run(run_id: str) -> dict[str, Any]:
    con = _get_conn()
    try:
        row = con.execute("SELECT id, status, label, domains_json, options_json, progress_json, error, created_at, started_at, finished_at FROM runs WHERE id = %s", (run_id,)).fetchone()
        if not row: raise HTTPException(status_code=404, detail="Run not found")
        domains = json.loads(row[3]) if row[3] else []
        options = json.loads(row[4]) if row[4] else {}
        progress = json.loads(row[5]) if row[5] else {}
        return {"id": row[0], "status": row[1], "label": row[2], "domains": domains, "options": options, "progress": progress, "error": row[6], "created_at": row[7], "started_at": row[8], "finished_at": row[9]}
    finally:
        try: con.close()
        except: pass

@router.post("/runs")
def create_run(request: RunCreateRequest) -> dict[str, Any]:
    from redis import Redis
    from rq import Queue
    con = _get_conn()
    try:
        run_id, tenant_id, now = str(uuid.uuid4()), "dev", _utc_now_iso()
        domains = [d.strip().lower() for d in request.domains if d.strip()]
        if not domains: raise HTTPException(status_code=400, detail="No valid domains provided")
        options = {"modes": request.modes, "ai_enabled": request.ai_enabled, "force_discovery": request.force_discovery, "company_limit": request.company_limit}
        con.execute("INSERT INTO runs (id, tenant_id, status, domains_json, options_json, created_at, updated_at) VALUES (%s, %s, 'queued', %s, %s, %s, %s)", (run_id, tenant_id, json.dumps(domains), json.dumps(options), now, now))
        con.commit()
        try:
            redis = Redis.from_url(RQ_REDIS_URL)
            q = Queue(name="orchestrator", connection=redis)
            from src.queueing.pipeline_v2 import pipeline_start_v2
            job = q.enqueue(pipeline_start_v2, run_id=run_id, tenant_id=tenant_id, job_timeout=3600)
            return {"ok": True, "run_id": run_id, "job_id": job.id, "status": "queued", "domain_count": len(domains)}
        except Exception as e:
            con.execute("UPDATE runs SET status = 'failed', error = %s WHERE id = %s", (f"Failed to enqueue: {e}", run_id))
            con.commit()
            raise HTTPException(status_code=500, detail=f"Failed to enqueue run: {e}")
    finally:
        try: con.close()
        except: pass

@router.get("/search")
def search_all(q: str = Query(..., min_length=1), limit: int = Query(20, ge=1, le=100)) -> dict[str, Any]:
    con = _get_conn()
    try:
        st = f"%{q.lower()}%"
        companies = con.execute("SELECT id, name, domain, official_domain FROM companies WHERE LOWER(domain) LIKE %s OR LOWER(name) LIKE %s OR LOWER(official_domain) LIKE %s LIMIT %s", (st, st, st, limit)).fetchall()
        people = con.execute("SELECT p.id, p.first_name, p.last_name, p.full_name, p.title, c.domain FROM people p LEFT JOIN companies c ON c.id = p.company_id WHERE LOWER(p.first_name) LIKE %s OR LOWER(p.last_name) LIKE %s OR LOWER(p.full_name) LIKE %s LIMIT %s", (st, st, st, limit)).fetchall()
        emails = con.execute("SELECT e.id, e.email, c.domain, vr.verify_status FROM emails e LEFT JOIN companies c ON c.id = e.company_id LEFT JOIN verification_results vr ON vr.email_id = e.id WHERE LOWER(e.email) LIKE %s LIMIT %s", (st, limit)).fetchall()
        return {"query": q,
            "companies": [{"id": c[0], "name": c[1], "domain": c[2], "official_domain": c[3]} for c in companies],
            "people": [{"id": p[0], "first_name": p[1], "last_name": p[2], "full_name": p[3], "title": p[4], "company_domain": p[5]} for p in people],
            "emails": [{"id": e[0], "email": e[1], "company_domain": e[2], "verify_status": e[3]} for e in emails]}
    finally:
        try: con.close()
        except: pass
BROWSER_EOF
echo "  Installed browser.py"

# 4. Add browser routes to app.py if not already present
echo ""
echo "[4/6] Registering browser routes in app.py..."
if grep -q "browser_routes" src/api/app.py; then
    echo "  Already registered - skipping"
else
    # Add import after admin import
    sed -i '/from src.api import admin as admin_routes/a from src.api import browser as browser_routes' src/api/app.py
    # Add router after admin router
    sed -i '/app.include_router(admin_routes.router)/a app.include_router(browser_routes.router)' src/api/app.py
    echo "  Added browser routes to app.py"
fi

# 5. Add dashboard route to admin.py if not already present
echo ""
echo "[5/6] Adding dashboard route to admin.py..."
if grep -q "dashboard" src/api/admin.py; then
    echo "  Already present - skipping"
else
    cat >> src/api/admin.py << 'DASHBOARD_ROUTE'

@router.get("/dashboard", response_class=HTMLResponse)
def dashboard_page(request: Request) -> HTMLResponse:
    """Full-featured dashboard for pipeline management and database browsing."""
    return templates.TemplateResponse("dashboard.html", {"request": request})
DASHBOARD_ROUTE
    echo "  Added dashboard route"
fi

# 6. Fix existing companies in database
echo ""
echo "[6/6] Fixing existing companies (setting official_domain)..."
psql $DATABASE_URL -c "UPDATE companies SET official_domain = domain WHERE (official_domain IS NULL OR official_domain = '') AND domain IS NOT NULL;" 2>/dev/null || echo "  Database update skipped (run manually if needed)"

echo ""
echo "=============================================="
echo "Setup Complete!"
echo "=============================================="
echo ""
echo "Next steps:"
echo ""
echo "1. Copy the dashboard.html file to src/api/templates/:"
echo "   (You should already have this from earlier)"
echo ""
echo "2. Restart uvicorn:"
echo "   pkill -f uvicorn"
echo "   uvicorn src.api.app:app --host 0.0.0.0 --port 8000 --reload &"
echo ""
echo "3. Restart workers:"
echo "   pkill -f 'rq worker'"
echo "   rq worker orchestrator crawl generate verify &"
echo ""
echo "4. Access dashboard at:"
echo "   http://$(hostname -I | awk '{print $1}'):8000/admin/dashboard"
echo ""
