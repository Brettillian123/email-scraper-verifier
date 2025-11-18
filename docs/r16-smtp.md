# R16 — SMTP RCPT-TO Probe

**Scope:** Add a minimal, configurable SMTP verification probe that connects to a domain’s MX and issues `EHLO` → optional `STARTTLS` → `MAIL FROM` → `RCPT TO` (no `DATA`).
**Outcome:** A structured, loggable result per email address (`accept | hard_fail | temp_fail | unknown`) with timing and server code/message, wired into a queue job and a CLI for manual checks.

---

## Components added/changed

- **Core probe**
  - `src/verify/smtp.py` → `probe_rcpt(...) -> dict`
- **Queue task**
  - `src/queueing/tasks.py` → `@job("verify") task_probe_email(email_id, email, domain, force=False) -> dict`
- **CLI helper**
  - `scripts/probe_smtp.py`
- **Configuration**
  - `src/config.py` now exposes (env-overridable):
    ```python
    SMTP_HELO_DOMAIN     = os.getenv("SMTP_HELO_DOMAIN", "verifier.crestwellpartners.com")
    SMTP_MAIL_FROM       = os.getenv("SMTP_MAIL_FROM", f"bounce@{SMTP_HELO_DOMAIN}")
    SMTP_CONNECT_TIMEOUT = float(os.getenv("SMTP_CONNECT_TIMEOUT", "10"))
    SMTP_COMMAND_TIMEOUT = float(os.getenv("SMTP_COMMAND_TIMEOUT", "10"))
    ```
- **Tests**
  - `tests/test_r16_smtp.py` (pure unit; no live servers)
- **Acceptance**
  - `scripts/accept_r16.ps1` (end-to-end smoke using queue + CLI)

---

## Why RCPT-TO only?

- It’s widely supported for address existence checks without sending mail.
- Keeps the probe fast, low-impact, and simple to classify.
- We **do not** transmit content (`DATA`) or attempt delivery.

> Note: Some providers tarp/greylist or “accept-all”. R16 reports the server’s observable behavior; we refine persistence/heuristics in R18+.

---

## Probe API

### Signature

```python
from src.verify.smtp import probe_rcpt

def probe_rcpt(
    email: str,
    mx_host: str,
    *,
    helo_domain: str,
    mail_from: str,
    connect_timeout: float = 10.0,
    command_timeout: float = 10.0,
    behavior_hint: dict | None = None,
) -> dict:
    ...
Inputs
email: the candidate (local@domain), validated minimally.

mx_host: concrete MX target (host). Non-empty required.

helo_domain: name to present in EHLO (config-driven).

mail_from: bounce/return path used for MAIL FROM (config-driven).

connect_timeout / command_timeout: socket and command time budgets.

behavior_hint: optional dict (from O06 behavior cache) to adapt timeouts.

Behavior (high-level)
Normalize inputs (strip; preserve local-part case semantics).

Derive timeouts:

If behavior_hint signals tarpit/slow, shrink timeouts and/or reduce retries.

Else, use configured defaults.

Connect via smtplib.SMTP(mx_host, 25, local_hostname=helo_domain, timeout=connect_timeout).

ehlo(); optionally starttls() if supported; ehlo() again after TLS.

mail(mail_from), then rcpt(email); capture code, message, elapsed.

Classify:

python
Copy code
if 200 <= code < 300: category = "accept"
elif 500 <= code < 600: category = "hard_fail"
elif 400 <= code < 500: category = "temp_fail"
else: category = "unknown"
Socket/SMTP exceptions → ok=False, category="unknown", code=None, error="...".

Call O06 hook (if present) to record MX behavior (latency, error kind, code).

Return a dict:

python
Copy code
{
  "ok": True,                          # or False on exception
  "category": "accept|hard_fail|temp_fail|unknown",
  "code": 250,                         # or None
  "message": "2.1.5 OK",               # server text (short)
  "mx_host": "aspmx.l.google.com",
  "helo_domain": "verifier.crestwellpartners.com",
  "elapsed_ms": 123,
  "error": None,                       # string on error
}
Queue integration (R16)
task_probe_email(...)
Input: email_id, email, domain, force=False

Resolves lowest_mx using R15 helpers (get_or_resolve_mx or resolve_mx fallback).

Reads behavior_hint from mx_behavior (O06) when available.

Applies R06 throttling:

Global & per-MX concurrency semaphores.

Global & per-MX RPS tokens (1-sec buckets).

Calls probe_rcpt(...) with identity + timeouts from src/config.

Returns the probe’s shape; does not persist (R18 handles DB writes).

Enqueueing
src/db.py::upsert_generated_email(..., enqueue_probe=True) calls
enqueue_probe_email(email_id, email, domain) which:

Uses the ingest enqueue shim (observed by tests) and also attempts direct RQ enqueue on verify.

CLI usage
scripts/probe_smtp.py provides a handy manual check.

powershell
Copy code
# PowerShell examples (uses env-configured identity/timeouts)

# Resolve MX via R15 (cached or live), then probe:
$PyExe .\scripts\probe_smtp.py --email "someone@gmail.com"

# Force MX re-resolve:
$PyExe .\scripts\probe_smtp.py --email "user@example.com" --force-resolve

# Specify an explicit MX (skips resolver):
$PyExe .\scripts\probe_smtp.py --email "user@example.com" --mx-host "aspmx.l.google.com"
Output (example):

yaml
Copy code
Target : user@example.com
MX     : aspmx.l.google.com
HELO   : verifier.crestwellpartners.com
Result : accept (code=250, error=None)
Message: 2.1.5 OK
Elapsed: 85 ms
Configuration
Set via environment (all optional):

bash
Copy code
SMTP_HELO_DOMAIN=verifier.crestwellpartners.com
SMTP_MAIL_FROM=bounce@verifier.crestwellpartners.com
SMTP_CONNECT_TIMEOUT=10
SMTP_COMMAND_TIMEOUT=10
For queue/worker runs, ensure your usual R06/R15 env is present (Redis URL, throttle caps, DB path).

Tests
All tests are local, no network.

bash
Copy code
# Focused subset
pytest -k "r16" -q

# What’s covered:
# - RCPT code mapping (2xx → accept, 5xx → hard_fail, 4xx → temp_fail)
# - Exception mapping (socket/SMTP → unknown + error string)
# - Behavior-cache integration invoked
# - Queue task shape and throttle hooks (monkeypatched)
See: tests/test_r16_smtp.py

Acceptance
Run:

powershell
Copy code
scripts\accept_r16.ps1
The script:

Applies schema/migrations.

Runs pytest -k "r15 or r16".

Ingests samples and resolves MX (R15).

Starts a worker on mx,verify.

Enqueues a small batch for task_probe_email.

Runs the CLI probe once or twice.

Shows domain_resolutions rows with mx_behavior snippet (O06).

Success banner:

css
Copy code
✔ R16 SMTP RCPT TO probe acceptance passed.
Operational notes & guardrails
Ethics/Respect: Only connect and issue RCPT TO; do not send content. Respect backoff, throttling, and consider suppressing probes for sensitive or high-risk domains if needed.

Freemail: R15 enqueue already skips MX resolution for freemail; R16 probes may still run for explicit test targets via CLI—use sparingly.

Tarpits/Greylisting: Expect 4xx/timeouts; they map to temp_fail or unknown. O06 behavior cache helps adapt future timeouts.

STARTTLS: Attempted opportunistically; many servers accept without it. After TLS, issue a fresh EHLO.

Troubleshooting
bad_input from task_probe_email

Ensure email contains @ and domain argument is non-empty (or derivable).

global/MX concurrency cap reached

Lower load or increase caps:

ini
Copy code
GLOBAL_MAX_CONCURRENCY=12
PER_MX_MAX_CONCURRENCY_DEFAULT=2
GLOBAL_RPS=6
PER_MX_RPS_DEFAULT=1
CLI hangs/slow

Provider tarpit: try SMTP_CONNECT_TIMEOUT=5 SMTP_COMMAND_TIMEOUT=5.

No mx_behavior updates

O06 hook might be missing; ensure your record_mx_probe(...) is imported or shimmed in src.verify.smtp.

Future work (R18+)
Persist probe outcomes into verification_results with richer reason codes.

Add provider-specific heuristics (accept-all detection).

Fold behavior statistics into smarter timeouts and retry schedules.

Changelog
R16

Added src/verify/smtp.py (probe_rcpt).

Added task_probe_email on verify queue.

Added scripts/probe_smtp.py.

Added focused unit tests (tests/test_r16_smtp.py).

Updated src/config.py with SMTP identity + timeouts.

Added scripts/accept_r16.ps1 acceptance.

Quick commands (copy-paste)
powershell
Copy code
# Set DB path for local runs
$env:DATABASE_URL = "sqlite:///$((Resolve-Path 'data\dev.db').Path)"
$env:DATABASE_PATH = "$((Resolve-Path 'data\dev.db').Path)"

# Run focused tests
$PyExe -m pytest -k "r15 or r16" -q

# Resolve MX for domains present in DB (R15)
$PyExe .\scripts\resolve_mx.py --from-db

# Probe a specific email (R16 CLI)
$PyExe .\scripts\probe_smtp.py --email "someone@gmail.com"

# Acceptance
scripts\accept_r16.ps1
