"""
Email Queue - Background rate-limited email sending via Gmail API.

Sends 1 email/minute, split into 2 blocks across send windows:
  - 8:00-11:00 AM CST
  - 2:00-4:00 PM CST

Usage:
  Add to your existing FastAPI app:
    from email_queue import router as email_queue_router, start_email_worker
    app.include_router(email_queue_router)
    start_email_worker()

Requires:
  pip install fastapi httpx pytz psycopg2-binary

Environment variables:
  DATABASE_URL          - PostgreSQL connection string
  GOOGLE_CLIENT_ID      - Google OAuth client ID
  GOOGLE_CLIENT_SECRET  - Google OAuth client secret
  EMAIL_QUEUE_CALLBACK  - OAuth callback URL (e.g. https://crestwelliq.com/api/email-queue/auth/callback)
"""

import json
import os
import re
import threading
import time
import uuid
import base64
import email.mime.multipart
import email.mime.text
import logging
from datetime import datetime
from typing import Optional
from urllib.parse import urlencode

import httpx
import psycopg2
import pytz
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

logger = logging.getLogger("email_queue")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [email_queue] %(levelname)s %(message)s"))
    logger.addHandler(handler)

# ── Config ───────────────────────────────────────────────────────────────────

CST = pytz.timezone("America/Chicago")
SEND_WINDOWS = [
    (8, 11),   # 8:00 AM - 11:00 AM CST
    (14, 16),  # 2:00 PM - 4:00 PM CST
]
SEND_INTERVAL_SECONDS = 60  # 1 email per minute

DATABASE_URL = os.environ.get("DATABASE_URL", "")
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
EMAIL_QUEUE_CALLBACK = os.environ.get("EMAIL_QUEUE_CALLBACK", "")

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"
GMAIL_SEND_URL = "https://gmail.googleapis.com/gmail/v1/users/me/messages/send"
GOOGLE_SCOPES = "https://www.googleapis.com/auth/gmail.send https://www.googleapis.com/auth/gmail.compose https://www.googleapis.com/auth/userinfo.email https://www.googleapis.com/auth/userinfo.profile"

router = APIRouter(prefix="/api/email-queue", tags=["email-queue"])

# ── Database ─────────────────────────────────────────────────────────────────

def get_db():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    return conn


def _dict_row(cursor, row):
    if row is None:
        return None
    return {col.name: val for col, val in zip(cursor.description, row)}


def fetchone_dict(cursor):
    row = cursor.fetchone()
    return _dict_row(cursor, row)


def fetchall_dict(cursor):
    return [_dict_row(cursor, r) for r in cursor.fetchall()]


def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS email_queue_google_auth (
            id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
            email TEXT NOT NULL,
            name TEXT NOT NULL DEFAULT '',
            access_token TEXT NOT NULL,
            refresh_token TEXT NOT NULL,
            expires_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS email_queue_jobs (
            id TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'pending',
            provider TEXT NOT NULL DEFAULT 'google',
            total INTEGER NOT NULL DEFAULT 0,
            sent INTEGER NOT NULL DEFAULT 0,
            failed INTEGER NOT NULL DEFAULT 0,
            errors_json TEXT NOT NULL DEFAULT '[]',
            block_number INTEGER NOT NULL DEFAULT 1,
            total_blocks INTEGER NOT NULL DEFAULT 2,
            block1_size INTEGER NOT NULL DEFAULT 0,
            block2_size INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS email_queue_items (
            id SERIAL PRIMARY KEY,
            job_id TEXT NOT NULL REFERENCES email_queue_jobs(id) ON DELETE CASCADE,
            to_email TEXT NOT NULL,
            full_name TEXT NOT NULL DEFAULT '',
            title TEXT NOT NULL DEFAULT '',
            role TEXT NOT NULL DEFAULT '',
            company_domain TEXT NOT NULL DEFAULT '',
            company_name TEXT NOT NULL DEFAULT '',
            subject TEXT NOT NULL,
            body_html TEXT NOT NULL,
            attachments_json TEXT NOT NULL DEFAULT '[]',
            headers_json TEXT NOT NULL DEFAULT '{}',
            status TEXT NOT NULL DEFAULT 'pending',
            error TEXT,
            sent_at TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_eq_items_job_status
            ON email_queue_items(job_id, status);
    """)
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Email queue tables initialized")


# ── Google OAuth (VPS-owned) ─────────────────────────────────────────────────

def _get_google_auth() -> dict | None:
    """Get the stored Google auth credentials."""
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM email_queue_google_auth WHERE id = 1")
        return fetchone_dict(cur)
    finally:
        cur.close()
        conn.close()


def _get_valid_access_token() -> str:
    """Get a valid access token, refreshing if expired."""
    auth = _get_google_auth()
    if not auth:
        raise RuntimeError("Gmail not connected. Visit /api/email-queue/auth/login to connect.")

    # Check if token is expired or about to expire (5 min buffer)
    if auth["expires_at"]:
        from datetime import timezone
        expires = auth["expires_at"]
        if hasattr(expires, 'tzinfo') and expires.tzinfo is None:
            expires = expires.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        if now >= expires:
            return _refresh_and_store(auth["refresh_token"])

    return auth["access_token"]


def _refresh_and_store(refresh_token: str) -> str:
    """Refresh the access token and update the database."""
    with httpx.Client(timeout=15) as client:
        resp = client.post(GOOGLE_TOKEN_URL, data={
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        })
        resp.raise_for_status()
        data = resp.json()

    access_token = data["access_token"]
    expires_in = data.get("expires_in", 3600)
    from datetime import timezone, timedelta
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in - 300)

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """UPDATE email_queue_google_auth
               SET access_token = %s, expires_at = %s, updated_at = NOW()
               WHERE id = 1""",
            (access_token, expires_at),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    logger.info("Google access token refreshed")
    return access_token


@router.get("/auth/login")
async def google_login():
    """Redirect to Google OAuth consent screen. Visit this once to connect Gmail."""
    if not GOOGLE_CLIENT_ID or not EMAIL_QUEUE_CALLBACK:
        raise HTTPException(500, "GOOGLE_CLIENT_ID and EMAIL_QUEUE_CALLBACK must be set")

    params = urlencode({
        "client_id": GOOGLE_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": EMAIL_QUEUE_CALLBACK,
        "scope": GOOGLE_SCOPES,
        "access_type": "offline",
        "prompt": "consent",
        "state": "email-queue-auth",
    })
    return RedirectResponse(f"{GOOGLE_AUTH_URL}?{params}")


@router.get("/auth/callback")
async def google_callback(code: str = "", error: str = ""):
    """Handle Google OAuth callback. Stores tokens in PostgreSQL."""
    if error:
        raise HTTPException(400, f"OAuth error: {error}")
    if not code:
        raise HTTPException(400, "No authorization code received")

    # Exchange code for tokens
    with httpx.Client(timeout=15) as client:
        resp = client.post(GOOGLE_TOKEN_URL, data={
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "code": code,
            "redirect_uri": EMAIL_QUEUE_CALLBACK,
            "grant_type": "authorization_code",
        })
        if not resp.is_success:
            raise HTTPException(400, f"Token exchange failed: {resp.text}")
        token_data = resp.json()

    access_token = token_data["access_token"]
    refresh_token = token_data.get("refresh_token", "")
    expires_in = token_data.get("expires_in", 3600)

    if not refresh_token:
        raise HTTPException(400, "No refresh token received. Make sure prompt=consent is set.")

    # Get user info
    with httpx.Client(timeout=10) as client:
        resp = client.get(GOOGLE_USERINFO_URL, headers={"Authorization": f"Bearer {access_token}"})
        user_info = resp.json() if resp.is_success else {}

    user_email = user_info.get("email", "unknown")
    user_name = user_info.get("name", user_email)

    from datetime import timezone, timedelta
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in - 300)

    # Upsert auth record (only 1 row, id=1)
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """INSERT INTO email_queue_google_auth (id, email, name, access_token, refresh_token, expires_at)
               VALUES (1, %s, %s, %s, %s, %s)
               ON CONFLICT (id) DO UPDATE SET
                 email = EXCLUDED.email,
                 name = EXCLUDED.name,
                 access_token = EXCLUDED.access_token,
                 refresh_token = EXCLUDED.refresh_token,
                 expires_at = EXCLUDED.expires_at,
                 updated_at = NOW()""",
            (user_email, user_name, access_token, refresh_token, expires_at),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    logger.info(f"Gmail connected: {user_email}")
    return {"ok": True, "message": f"Gmail connected as {user_email}. You can close this page."}


@router.get("/auth/status")
async def google_auth_status():
    """Check if Gmail is connected."""
    auth = _get_google_auth()
    if not auth:
        return {"connected": False}
    return {
        "connected": True,
        "email": auth["email"],
        "name": auth["name"],
        "updated_at": str(auth["updated_at"]),
    }


# ── Models ───────────────────────────────────────────────────────────────────

class EmailItem(BaseModel):
    to_email: str
    subject: str
    body_html: str
    full_name: str = ""
    title: str = ""
    role: str = ""
    company_domain: str = ""
    company_name: str = ""
    attachments: list[dict] = []
    headers: dict[str, str] = {}


class SubmitRequest(BaseModel):
    emails: list[EmailItem]
    provider: str = "google"


class SubmitResponse(BaseModel):
    job_id: str
    total: int
    status: str


class StatusResponse(BaseModel):
    job_id: str
    status: str
    total: int
    sent: int
    failed: int
    pending: int
    block_number: int
    total_blocks: int
    errors: list[dict]
    created_at: str
    updated_at: str
    recent_log: list[dict]
    sent_contacts: list[dict]


# ── Endpoints ────────────────────────────────────────────────────────────────

@router.post("/submit", response_model=SubmitResponse)
async def submit_job(req: SubmitRequest):
    if not req.emails:
        raise HTTPException(400, "No emails provided")
    if len(req.emails) > 200:
        raise HTTPException(400, "Maximum 200 emails per job")

    # Verify Gmail is connected
    auth = _get_google_auth()
    if not auth:
        raise HTTPException(401, "Gmail not connected. Visit /api/email-queue/auth/login first.")

    job_id = str(uuid.uuid4())[:12]
    total = len(req.emails)
    block1_size = (total + 1) // 2
    block2_size = total - block1_size

    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """INSERT INTO email_queue_jobs
               (id, status, provider, total, block1_size, block2_size, total_blocks)
               VALUES (%s, 'pending', %s, %s, %s, %s, %s)""",
            (job_id, req.provider, total, block1_size, block2_size, 2 if block2_size > 0 else 1),
        )

        for em in req.emails:
            cur.execute(
                """INSERT INTO email_queue_items
                   (job_id, to_email, full_name, title, role, company_domain, company_name,
                    subject, body_html, attachments_json, headers_json)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (job_id, em.to_email, em.full_name, em.title, em.role,
                 em.company_domain, em.company_name,
                 em.subject, em.body_html,
                 json.dumps(em.attachments), json.dumps(em.headers)),
            )

        conn.commit()
    finally:
        cur.close()
        conn.close()

    logger.info(f"Job {job_id} created: {total} emails queued")
    return SubmitResponse(job_id=job_id, total=total, status="pending")


@router.get("/status/{job_id}", response_model=StatusResponse)
async def get_status(job_id: str):
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM email_queue_jobs WHERE id = %s", (job_id,))
        job = fetchone_dict(cur)
        if not job:
            raise HTTPException(404, "Job not found")

        cur.execute(
            """SELECT to_email, status, error, sent_at FROM email_queue_items
               WHERE job_id = %s AND status != 'pending'
               ORDER BY id DESC LIMIT 10""",
            (job_id,),
        )
        recent = fetchall_dict(cur)
        recent_log = [
            {"to_email": r["to_email"], "success": r["status"] == "sent",
             "error": r["error"], "time": r["sent_at"] or ""}
            for r in recent
        ]

        cur.execute(
            """SELECT to_email, full_name, title, role, company_domain, company_name
               FROM email_queue_items
               WHERE job_id = %s AND status = 'sent'
               ORDER BY id ASC""",
            (job_id,),
        )
        sent_contacts = fetchall_dict(cur)

        errors = json.loads(job["errors_json"])
        pending = job["total"] - job["sent"] - job["failed"]

        return StatusResponse(
            job_id=job["id"], status=job["status"], total=job["total"],
            sent=job["sent"], failed=job["failed"], pending=pending,
            block_number=job["block_number"], total_blocks=job["total_blocks"],
            errors=errors, created_at=str(job["created_at"]),
            updated_at=str(job["updated_at"]),
            recent_log=recent_log, sent_contacts=sent_contacts,
        )
    finally:
        cur.close()
        conn.close()


@router.post("/cancel/{job_id}")
async def cancel_job(job_id: str):
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT status FROM email_queue_jobs WHERE id = %s", (job_id,))
        job = fetchone_dict(cur)
        if not job:
            raise HTTPException(404, "Job not found")
        if job["status"] in ("done", "cancelled"):
            return {"ok": True, "message": f"Job already {job['status']}"}

        cur.execute(
            "UPDATE email_queue_jobs SET status = 'cancelled', updated_at = NOW() WHERE id = %s",
            (job_id,),
        )
        conn.commit()
        logger.info(f"Job {job_id} cancelled")
        return {"ok": True, "message": "Job cancelled"}
    finally:
        cur.close()
        conn.close()


@router.get("/active")
async def get_active_job():
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """SELECT id, status, total, sent, failed, block_number, total_blocks
               FROM email_queue_jobs
               WHERE status IN ('pending', 'running', 'waiting')
               ORDER BY created_at DESC LIMIT 1"""
        )
        job = fetchone_dict(cur)
        if not job:
            return {"active": False}
        return {"active": True, "job_id": job["id"], **job}
    finally:
        cur.close()
        conn.close()


# ── Gmail Sending ────────────────────────────────────────────────────────────

def _build_mime_message(to_email: str, subject: str, body_html: str,
                        headers: dict[str, str] | None = None) -> str:
    msg = email.mime.multipart.MIMEMultipart("alternative")
    msg["To"] = to_email
    msg["Subject"] = subject
    if headers:
        for key, value in headers.items():
            msg[key] = value

    plain = re.sub(r"<br\s*/?>", "\n", body_html)
    plain = re.sub(r"</p>", "\n\n", plain)
    plain = re.sub(r"<[^>]+>", "", plain)
    plain = plain.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    plain = plain.replace("&quot;", '"').replace("&#39;", "'")
    plain = re.sub(r"\n{3,}", "\n\n", plain).strip()

    msg.attach(email.mime.text.MIMEText(plain, "plain", "utf-8"))
    msg.attach(email.mime.text.MIMEText(body_html, "html", "utf-8"))
    return msg.as_string()


def _base64url_encode(data: str) -> str:
    return base64.urlsafe_b64encode(data.encode("utf-8")).rstrip(b"=").decode("ascii")


def _send_gmail(access_token: str, to_email: str, subject: str,
                body_html: str, headers: dict[str, str] | None = None) -> str:
    mime = _build_mime_message(to_email, subject, body_html, headers)
    raw = _base64url_encode(mime)
    with httpx.Client(timeout=30) as client:
        resp = client.post(
            GMAIL_SEND_URL,
            headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
            json={"raw": raw},
        )
        if resp.status_code == 401:
            raise TokenExpiredError("Access token expired")
        resp.raise_for_status()
        return resp.json().get("id", "")


class TokenExpiredError(Exception):
    pass


# ── Background Worker ────────────────────────────────────────────────────────

def _is_in_send_window() -> bool:
    now = datetime.now(CST)
    return any(start <= now.hour < end for start, end in SEND_WINDOWS)


def _next_window_label() -> str:
    now = datetime.now(CST)
    for start, _end in SEND_WINDOWS:
        if now.hour < start:
            h = start % 12 or 12
            return f"{h}:00 {'AM' if start < 12 else 'PM'} CST"
    h = SEND_WINDOWS[0][0] % 12 or 12
    return f"{h}:00 AM CST (tomorrow)"


def _check_cancelled(job_id: str) -> bool:
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT status FROM email_queue_jobs WHERE id = %s", (job_id,))
        row = fetchone_dict(cur)
        return row is not None and row["status"] == "cancelled"
    finally:
        cur.close()
        conn.close()


def _update_job_status(job_id: str, status: str, **extra_fields):
    conn = get_db()
    cur = conn.cursor()
    try:
        sets = ["status = %s", "updated_at = NOW()"]
        vals: list = [status]
        for k, v in extra_fields.items():
            sets.append(f"{k} = %s")
            vals.append(v)
        vals.append(job_id)
        cur.execute(f"UPDATE email_queue_jobs SET {', '.join(sets)} WHERE id = %s", vals)
        conn.commit()
    finally:
        cur.close()
        conn.close()


def _record_failure(job_id: str, item: dict, block: int, time_str: str, error: str):
    conn = get_db()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE email_queue_items SET status = 'failed', error = %s, sent_at = %s WHERE id = %s",
            (error, time_str, item["id"]),
        )
        cur.execute("SELECT errors_json FROM email_queue_jobs WHERE id = %s", (job_id,))
        job = fetchone_dict(cur)
        errors = json.loads(job["errors_json"]) if job else []
        errors.append({"to_email": item["to_email"], "error": error})
        cur.execute(
            """UPDATE email_queue_jobs SET failed = failed + 1, errors_json = %s, block_number = %s,
               updated_at = NOW() WHERE id = %s""",
            (json.dumps(errors), block, job_id),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def _worker_loop():
    logger.info("Email queue worker started")

    while True:
        try:
            conn = get_db()
            cur = conn.cursor()
            cur.execute(
                """SELECT * FROM email_queue_jobs
                   WHERE status IN ('pending', 'running', 'waiting')
                   ORDER BY created_at ASC LIMIT 1"""
            )
            job = fetchone_dict(cur)
            cur.close()
            conn.close()

            if not job:
                time.sleep(30)
                continue

            job_id = job["id"]
            block1_size = job["block1_size"]

            _update_job_status(job_id, "running")

            # Get a valid access token from VPS-owned auth
            try:
                access_token = _get_valid_access_token()
            except Exception as e:
                logger.error(f"Job {job_id}: Cannot get Gmail token: {e}")
                _update_job_status(job_id, "failed")
                time.sleep(60)
                continue

            conn = get_db()
            cur = conn.cursor()
            cur.execute(
                """SELECT id, to_email, subject, body_html, headers_json
                   FROM email_queue_items
                   WHERE job_id = %s AND status = 'pending'
                   ORDER BY id ASC""",
                (job_id,),
            )
            items = fetchall_dict(cur)
            cur.close()
            conn.close()

            sent_so_far = job["sent"] + job["failed"]

            for idx, item in enumerate(items):
                if _check_cancelled(job_id):
                    logger.info(f"Job {job_id} was cancelled, stopping")
                    break

                global_idx = sent_so_far + idx
                current_block = 1 if global_idx < block1_size else 2

                # Block transition: wait for next window
                if current_block == 2 and global_idx == block1_size:
                    logger.info(f"Job {job_id}: Block 1 complete. Waiting for next send window...")
                    _update_job_status(job_id, "waiting", block_number=2)

                    while _is_in_send_window():
                        time.sleep(60)
                        if _check_cancelled(job_id):
                            break

                    while not _is_in_send_window():
                        time.sleep(60)
                        if _check_cancelled(job_id):
                            break

                    _update_job_status(job_id, "running")

                # Wait for send window
                if not _is_in_send_window():
                    logger.info(f"Job {job_id}: Outside send window. Waiting for {_next_window_label()}...")
                    _update_job_status(job_id, "waiting")

                    while not _is_in_send_window():
                        time.sleep(60)
                        if _check_cancelled(job_id):
                            break

                    _update_job_status(job_id, "running")

                if _check_cancelled(job_id):
                    break

                # Refresh token if needed before each send
                try:
                    access_token = _get_valid_access_token()
                except Exception as e:
                    logger.error(f"Job {job_id}: Token refresh failed: {e}")
                    _record_failure(job_id, item, current_block,
                                    datetime.now(CST).strftime("%I:%M %p"),
                                    f"Token error: {e}")
                    continue

                # Send the email
                headers = json.loads(item["headers_json"] or "{}")
                now_str = datetime.now(CST).strftime("%I:%M %p")

                try:
                    _send_gmail(access_token, item["to_email"], item["subject"],
                                item["body_html"], headers)

                    conn = get_db()
                    cur = conn.cursor()
                    cur.execute(
                        "UPDATE email_queue_items SET status = 'sent', sent_at = %s WHERE id = %s",
                        (now_str, item["id"]),
                    )
                    cur.execute(
                        """UPDATE email_queue_jobs SET sent = sent + 1, block_number = %s,
                           updated_at = NOW() WHERE id = %s""",
                        (current_block, job_id),
                    )
                    conn.commit()
                    cur.close()
                    conn.close()
                    logger.info(f"Job {job_id}: Sent to {item['to_email']} ({global_idx + 1}/{job['total']})")

                except TokenExpiredError:
                    # Force refresh and retry once
                    try:
                        auth = _get_google_auth()
                        if auth:
                            access_token = _refresh_and_store(auth["refresh_token"])
                            _send_gmail(access_token, item["to_email"], item["subject"],
                                        item["body_html"], headers)
                            conn = get_db()
                            cur = conn.cursor()
                            cur.execute(
                                "UPDATE email_queue_items SET status = 'sent', sent_at = %s WHERE id = %s",
                                (now_str, item["id"]),
                            )
                            cur.execute(
                                """UPDATE email_queue_jobs SET sent = sent + 1, block_number = %s,
                                   updated_at = NOW() WHERE id = %s""",
                                (current_block, job_id),
                            )
                            conn.commit()
                            cur.close()
                            conn.close()
                            logger.info(f"Job {job_id}: Sent to {item['to_email']} after token refresh")
                        else:
                            _record_failure(job_id, item, current_block, now_str,
                                            "Gmail not connected")
                    except Exception as e2:
                        _record_failure(job_id, item, current_block, now_str, str(e2)[:200])
                        logger.error(f"Job {job_id}: Failed {item['to_email']} after refresh: {e2}")

                except Exception as e:
                    _record_failure(job_id, item, current_block, now_str, str(e)[:200])
                    logger.error(f"Job {job_id}: Failed {item['to_email']}: {e}")

                # Wait 60 seconds before next email
                if idx < len(items) - 1:
                    time.sleep(SEND_INTERVAL_SECONDS)

            if not _check_cancelled(job_id):
                _update_job_status(job_id, "done")
                logger.info(f"Job {job_id} complete")

        except Exception as e:
            logger.error(f"Worker error: {e}")
            time.sleep(30)


# ── Startup ──────────────────────────────────────────────────────────────────

def start_email_worker():
    """Start the background email worker thread. Call this at app startup."""
    init_db()
    t = threading.Thread(target=_worker_loop, daemon=True, name="email-queue-worker")
    t.start()
    logger.info("Email queue worker thread launched")
