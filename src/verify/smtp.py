"""
R16 â€” Core SMTP RCPT probe (+ O06 behavior-aware timeouts + fast-fail preflight)

Public function:

    probe_rcpt(
        email: str,
        mx_host: str,
        *,
        helo_domain: str,
        mail_from: str,
        connect_timeout: float = 20.0,
        command_timeout: float = 20.0,
        behavior_hint: dict | None = None,
    ) -> dict

Responsibilities:
- Light email normalization (trim, split, IDNA for domain; preserve local-part case).
- Require a non-empty MX host.
- Optional fast-fail TCP/25 preflight (circuit breaker) to avoid NÃ— timeout blowups.
- Open an SMTP connection with sane (and O06-tuned) timeouts.
- EHLO; opportunistic STARTTLS if offered; EHLO again afterward.
- Run MAIL FROM / RCPT TO and capture the RCPT reply code/message. A non-2xx
  MAIL FROM ends the session without issuing the RCPT (its reply is about our
  sender or the host's rate limit, never about the mailbox).
- Classify the response into: accept | hard_fail | temp_fail | unknown.
- Always record behavior stats (O06) via a behavior hook.
- Return a structured dict suitable for logging and later persistence (R18).

probe_rcpt() is a one-address wrapper over probe_rcpt_batch(), which is the real
engine: it reuses one SMTP session for several RCPTs, enforces a hard wall-clock
budget per session (so SMTP_MX_MAX_ADDRS can no longer multiply into 40s probes),
and skips or defers MX hosts the O06 hint marks as tarpits instead of merely
widening their timeouts.

Notes:
- Local-part is case-preserving; domain-part is lowercased and IDNA-encoded.
- On exceptions (timeouts, TLS/protocol errors), return ok=False, category="unknown".
- One session is bounded by wall-clock AND by bytes read, so a hostile MX that
  trickles or never answers cannot pin a worker.
- If no behavior_hint is supplied, we can look one up (preferring MX module).
"""

# src/verify/smtp.py
from __future__ import annotations

import os
import re
import smtplib
import socket
import sys
import time
from typing import Any

from src.config import (
    SMTP_MX_MAX_ADDRS,
    SMTP_PREFER_IPV4,
    SMTP_PREFLIGHT_CACHE_TTL_SECONDS,
    SMTP_PREFLIGHT_ENABLED,
    SMTP_PREFLIGHT_MAX_ADDRS,
    SMTP_PREFLIGHT_TIMEOUT_SECONDS,
)


def _env_num(name: str, default, cast):
    try:
        return cast(os.environ[name])
    except Exception:
        return default


# Session reuse / time budget / tarpit policy tunables.
SMTP_SESSION_MAX_RCPTS: int = _env_num("SMTP_SESSION_MAX_RCPTS", 4, int)
SMTP_SESSION_RATELIMIT_STOP_AFTER: int = _env_num("SMTP_SESSION_RATELIMIT_STOP_AFTER", 2, int)
SMTP_PROBE_TOTAL_BUDGET_SECONDS: float = _env_num("SMTP_PROBE_TOTAL_BUDGET_SECONDS", 12.0, float)
# 30d is falsified by this deployment: observed gaps between real events are
# 114/133/170 days (no run reaper), so a 30d window returns None for every host.
# That reasoning holds for LATENCY, which is all this window now governs.
SMTP_BEHAVIOR_WINDOW_DAYS: int = _env_num("SMTP_BEHAVIOR_WINDOW_DAYS", 180, int)
# The TARPIT verdict gets its own, much shorter window, because it is
# self-sealing: a flagged host is skipped before a socket opens, so no evidence
# that would overturn the flag can ever be gathered. On 2026-08-27 Mimecast was
# being skipped on a 94.7% temp-fail rate whose every sample predated August --
# and therefore predated the greylist deferral built to handle exactly that
# behaviour. The verdict was permanent by construction. With no probes inside
# this window the answer is "not currently a tarpit", so the host gets one fresh
# chance and re-earns the flag on present-day evidence if it deserves it.
SMTP_TARPIT_WINDOW_DAYS: int = _env_num("SMTP_TARPIT_WINDOW_DAYS", 14, int)
SMTP_TARPIT_SKIP_SUCCESS_RATE: float = _env_num("SMTP_TARPIT_SKIP_SUCCESS_RATE", 0.03, float)
SMTP_TARPIT_DEFER_SECONDS: int = _env_num("SMTP_TARPIT_DEFER_SECONDS", 600, int)
# B6 session ceilings. The wall-clock ceiling is the existing per-session budget
# (SMTP_PROBE_TOTAL_BUDGET_SECONDS); these bound how much the peer may make us
# read. smtplib's own limit is per LINE, so unlimited continuation lines - or one
# byte every command_timeout-1 seconds - can pin a worker for hours.
SMTP_SESSION_MAX_BYTES: int = _env_num("SMTP_SESSION_MAX_BYTES", 65536, int)
SMTP_MAX_REPLY_LINE_BYTES: int = _env_num("SMTP_MAX_REPLY_LINE_BYTES", 8192, int)

try:  # pragma: no cover
    from src.verify.preflight import (
        SmtpProbingDisabledError,
        assert_smtp_probing_allowed,
        check_port25,
    )
except Exception:  # pragma: no cover
    SmtpProbingDisabledError = RuntimeError  # type: ignore

    def assert_smtp_probing_allowed() -> None:  # type: ignore
        raise RuntimeError("SMTP preflight module unavailable; refusing to run SMTP probing.")

    check_port25 = None  # type: ignore

# --- Optional deps / helpers -------------------------------------------------

try:  # pragma: no cover - exercised in integration
    from src.ingest.normalize import norm_domain as _shared_norm_domain  # type: ignore
except Exception:  # pragma: no cover
    _shared_norm_domain = None  # type: ignore


def _idna_domain(d: str) -> str:
    d = (d or "").strip().lower()
    try:
        return d.encode("idna").decode("ascii")
    except Exception:
        return d


def _normalize_email(email: str) -> tuple[str, str, str]:
    s = (email or "").strip()
    if not s or "@" not in s:
        raise ValueError("invalid_email")
    local, domain = s.split("@", 1)
    if _shared_norm_domain:
        try:
            nd = _shared_norm_domain(domain) or domain
        except Exception:
            nd = _idna_domain(domain)
    else:
        nd = _idna_domain(domain)
    return local, nd, f"{local}@{nd}"


# --- O06 integrations (prefer src.resolve.mx; fallback to legacy) ------------

_mx_record = None
_mx_get_hint = None

try:
    # (mx_host, *, window_days=30, db_path=None) -> dict | None
    from src.resolve.mx import get_mx_behavior_hint as _mx_get_hint

    # (domain, mx_host, elapsed_ms, category, code, error_kind)
    from src.resolve.mx import record_behavior as _mx_record_behavior

    # (mx_host, code, elapsed_s, *, error_kind, category?, db_path?)
    from src.resolve.mx import record_mx_probe as _mx_record  # type: ignore
except Exception:
    _mx_record = None
    _mx_get_hint = None
    _mx_record_behavior = None  # type: ignore


def _record_probe_raw(
    mx_host: str,
    code: int | None,
    elapsed: float,
    *,
    error_kind: str | None,
    category: str | None,
) -> None:
    """Store a raw datapoint via the MX backend (no-op if unavailable)."""
    if not _mx_record:
        return
    db_path = os.getenv("DATABASE_PATH")
    try:
        _mx_record(
            mx_host,
            code,
            elapsed,
            error_kind=error_kind,
            category=category,
            db_path=db_path,
        )  # type: ignore[arg-type]
    except TypeError:
        try:
            _mx_record(mx_host, code, elapsed, error_kind=error_kind)  # type: ignore[misc]
        except Exception:
            pass
    except Exception:
        pass


def will_skip_as_tarpit(mx_host: str) -> bool:
    """Would probe_rcpt_batch() skip this host without opening a socket?

    Callers need to know BEFORE they spend anything on the attempt. The tarpit
    gate sits inside probe_rcpt_batch, so a caller that consumes a probe budget
    first pays full price for a probe that never happens.

    Measured 2026-08-27: an overnight pass enqueued 500 people, consumed 1,500
    from the daily budget and issued 45 actual probes. The rest were
    tarpit-skipped -- Mimecast sits at 95% temp-fail across 3,930 samples, Zoho
    at 93.6% -- so the budget was spent on nothing.
    """
    try:
        hint = _get_hint(mx_host)
    except Exception:
        return False
    if not isinstance(hint, dict) or not hint.get("tarpit"):
        return False
    try:
        return float(hint.get("success_rate") or 0.0) < SMTP_TARPIT_SKIP_SUCCESS_RATE
    except Exception:
        return True


def _get_hint(mx_host: str) -> dict[str, Any] | None:
    if not _mx_get_hint:
        return None
    db_path = os.getenv("DATABASE_PATH")
    try:
        return _mx_get_hint(  # type: ignore[call-arg]
            mx_host,
            window_days=SMTP_BEHAVIOR_WINDOW_DAYS,
            tarpit_window_days=SMTP_TARPIT_WINDOW_DAYS,
            db_path=db_path,
        )
    except TypeError:
        try:
            return _mx_get_hint(mx_host)  # type: ignore[misc]
        except Exception:
            return None
    except Exception:
        return None


# --- Behavior-driven timeout tuning (O06 consumer) ---------------------------


def _apply_hint_timeouts(
    connect_timeout: float,
    command_timeout: float,
    behavior_hint: dict | None,
) -> tuple[float, float]:
    if not isinstance(behavior_hint, dict):
        return connect_timeout, command_timeout
    ct = behavior_hint.get("connect_timeout", connect_timeout)
    mt = behavior_hint.get("command_timeout", command_timeout)
    try:
        return float(ct), float(mt)
    except Exception:
        return connect_timeout, command_timeout


# --- Classification -----------------------------------------------------------


def _classify(code: int | None) -> str:
    if code is None:
        return "unknown"
    if 200 <= code < 300:
        return "accept"
    if 500 <= code < 600:
        return "hard_fail"
    if 400 <= code < 500:
        return "temp_fail"
    return "unknown"


#: RFC 6647 asks greylisters to answer 4xx with enhanced status 4.7.1 so a
#: client can tell a deliberate defer from a broken server. Real MTAs are
#: inconsistent about emitting it, hence the wording fallback.
_GREYLIST_ENHANCED_RE = re.compile(r"\b4\.7\.\d{1,3}\b")
_GREYLIST_KEYWORDS = (
    "greylist",
    "graylist",
    "please try again",
    "try again later",
    "temporarily deferred",
    "policy rejection",
)


def is_greylist_deferral(code: int | None, message: str | None) -> bool:
    """
    True if a 4xx reply looks like greylisting rather than a broken server.

    Orthogonal to _classify(): the accept/hard_fail/temp_fail buckets are
    unchanged. This only tags an already-established temp_fail so the caller
    can label WHY it deferred. Both tags get the same retry schedule, so a
    misread costs a wrong reason string and nothing else.
    """
    if code is None or not (400 <= code < 500):
        return False
    msg = message or ""
    if _GREYLIST_ENHANCED_RE.search(msg):
        return True
    low = msg.lower()
    return any(kw in low for kw in _GREYLIST_KEYWORDS)


def _decode_msg(msg: bytes | str | None) -> str:
    if msg is None:
        return ""
    if isinstance(msg, bytes):
        try:
            return msg.decode("latin-1", errors="replace").strip()
        except Exception:
            return repr(msg)
    return str(msg).strip()


# --- R16-visible behavior hook (tests monkeypatch this) ----------------------


def record_behavior(
    *,
    domain: str,
    mx_host: str,
    elapsed_ms: int,
    category: str,
    code: int | None,
    error_kind: str | None,
) -> None:
    """
    Tests patch smtp.record_behavior and expect exactly one call per probe.
    Default: if src.resolve.mx.record_behavior exists, delegate to it.
    Otherwise, record a raw datapoint so O06 stats still work.
    """
    try:
        if _mx_record_behavior:
            _mx_record_behavior(
                domain=domain,
                mx_host=mx_host,
                elapsed_ms=elapsed_ms,
                category=category,
                code=code,
                error_kind=error_kind,
            )
            return
    except Exception:
        pass

    # Fallback: write a raw probe event
    try:
        _record_probe_raw(
            mx_host,
            code,
            elapsed_ms / 1000.0,
            error_kind=error_kind,
            category=category,
        )
    except Exception:
        pass


# --- Fast-fail preflight (process-local circuit breaker) ---------------------

# mx_host -> (expires_monotonic, ok, error_str)
_PREFLIGHT_CACHE: dict[str, tuple[float, bool, str | None]] = {}


def _preflight_port25(mx_host: str) -> tuple[bool, str | None]:
    """
    Returns (ok, error_str). Uses a small TTL cache to avoid repeating
    the same doomed connect attempts within a single run.

    Note: check_port25() itself is guarded by assert_smtp_probing_allowed().
    """
    if not SMTP_PREFLIGHT_ENABLED or check_port25 is None:
        return True, None

    now = time.monotonic()
    cached = _PREFLIGHT_CACHE.get(mx_host)
    if cached is not None:
        exp, ok, err = cached
        if now < exp:
            return ok, err

    r = check_port25(
        mx_host,
        timeout_s=float(SMTP_PREFLIGHT_TIMEOUT_SECONDS),
        max_addrs=int(SMTP_PREFLIGHT_MAX_ADDRS),
    )
    ok = bool(getattr(r, "ok", False))
    err = None if ok else (getattr(r, "error", None) or "port25_unreachable")
    ttl = max(1, int(SMTP_PREFLIGHT_CACHE_TTL_SECONDS))
    _PREFLIGHT_CACHE[mx_host] = (now + ttl, ok, err)
    return ok, err


def _resolve_mx_ips(mx_host: str, *, prefer_ipv4: bool, max_addrs: int) -> list[str]:
    """
    Resolve MX host into a bounded list of IPs we will attempt.
    """
    # For test scenarios with fake domains, return host directly
    mx_lower = mx_host.lower()
    test_patterns = ("example.com", "example.net", ".test", "fake", "mock")
    if any(pattern in mx_lower for pattern in test_patterns):
        return [mx_host]

    try:
        infos = socket.getaddrinfo(mx_host, 25, type=socket.SOCK_STREAM)
    except OSError:
        # If DNS fails, return the host directly
        return [mx_host]

    # Prefer IPv4 first to match common consumer-network behavior
    if prefer_ipv4:
        infos = sorted(infos, key=lambda a: 0 if a[0] == socket.AF_INET else 1)

    ips: list[str] = []
    seen: set[str] = set()
    for _family, _socktype, _proto, _canon, sockaddr in infos:
        ip = sockaddr[0]
        if ip in seen:
            continue
        seen.add(ip)
        ips.append(ip)
        if len(ips) >= max_addrs:
            break
    return ips


# --- Public API ---------------------------------------------------------------


def _result(
    mx_host: str,
    helo_domain: str,
    *,
    category: str,
    code: int | None = None,
    message: str = "",
    error: str | None = None,
    elapsed_ms: int = 0,
    attempted: bool = True,
    defer_seconds: int | None = None,
) -> dict:
    return {
        "ok": category == "accept" and error is None,
        "category": category,
        "code": code,
        "message": message,
        "mx_host": mx_host,
        "helo_domain": helo_domain,
        "elapsed_ms": elapsed_ms,
        "error": error,
        # Additive keys; pre-existing callers read via .get() and ignore them.
        "attempted": attempted,  # True only if a RCPT actually went out on the wire
        "defer_seconds": defer_seconds,  # set only on tarpit_deferred
    }


class SmtpSessionLimitError(Exception):
    """A session hit a B6 ceiling (wall-clock, total bytes, or one reply line).

    Deliberately not an OSError (nor smtplib.SMTPException, which IS an OSError):
    smtplib.getreply() folds every OSError into SMTPServerDisconnected, which
    would bury the reason. Raised straight through getreply() instead, so the
    caller can close the socket and label the temp_fail accurately.
    """


class _BoundedReader:
    """Read side of an SMTP session with hard ceilings (B6).

    smtplib only ever calls .readline()/.close() on SMTP.file, and every recv()
    restarts the socket timeout - so a peer that trickles bytes, or answers with
    endless continuation lines, is bounded by nothing. This shim enforces an
    ABSOLUTE deadline and a total byte ceiling across the whole session, and caps
    a single reply line (raising here instead of letting smtplib answer its own
    "500 Line too long", which _classify() would bucket hard_fail = invalid).
    """

    def __init__(self, sock, deadline: float, chunk_timeout: float, counter: list):
        self._sock = sock
        self._deadline = deadline
        self._chunk_timeout = max(0.05, float(chunk_timeout))
        self._counter = counter  # [bytes_read] - shared across the STARTTLS re-wrap
        self._buf = b""

    def readline(self, limit: int = -1) -> bytes:
        cap = SMTP_MAX_REPLY_LINE_BYTES
        if limit is not None and limit >= 0:
            cap = min(int(limit), cap)
        cap = max(1, cap)
        while True:
            nl = self._buf.find(b"\n", 0, cap)
            if nl >= 0:
                line, self._buf = self._buf[: nl + 1], self._buf[nl + 1 :]
                return line
            if len(self._buf) >= cap:
                raise SmtpSessionLimitError(f"reply_line_over_{cap}_bytes")
            left = self._deadline - time.monotonic()
            if left <= 0:
                raise SmtpSessionLimitError("session_wallclock_exceeded")
            self._sock.settimeout(min(self._chunk_timeout, left))
            chunk = self._sock.recv(4096)
            if not chunk:  # peer closed; hand back whatever is left
                line, self._buf = self._buf, b""
                return line
            self._counter[0] += len(chunk)
            if self._counter[0] > SMTP_SESSION_MAX_BYTES:
                raise SmtpSessionLimitError(f"session_over_{SMTP_SESSION_MAX_BYTES}_bytes")
            self._buf += chunk

    def close(self) -> None:
        self._buf = b""


class _MakefileProxy:
    """Socket stand-in whose makefile() hands back the bounded reader.

    _BoundedSMTP has to intercept makefile() because that is where smtplib gets
    the banner reader, and socket.socket defines __slots__ so the bound method
    cannot be patched onto the instance. Everything else forwards to the real
    socket; the proxy is dropped as soon as the banner is in.
    """

    __slots__ = ("_sock", "_reader")

    def __init__(self, sock, reader):
        self._sock = sock
        self._reader = reader

    def makefile(self, *_args, **_kwargs):
        return self._reader

    def __getattr__(self, name):
        return getattr(self._sock, name)


# Captured at import. Tests replace smtplib.SMTP with a plain function, and a
# function cannot be subclassed, so this identity check is how the probe tells
# "wrap the banner read" from "a double is standing in for the whole session".
_SMTP_BASE = smtplib.SMTP


class _BoundedSMTP(_SMTP_BASE):
    """smtplib.SMTP that reads the 220 banner under the session ceilings.

    _guard_reads() cannot cover the banner: it works by replacing smtp.file, and
    the constructor has already connected AND called getreply() by the time it
    runs. smtplib's readline restarts the socket timeout on every recv(), so an
    MX trickling one byte per (timeout - epsilon) and never sending a newline is
    bounded only by smtplib._MAXLINE -- the exact tarpit the B6 ceilings above
    exist to stop. Overriding _get_socket() puts the reader in place before the
    first getreply() instead.
    """

    def __init__(self, *args, deadline: float, chunk_timeout: float, counter: list, **kwargs):
        self._read_bounds = (deadline, chunk_timeout, counter)
        try:
            super().__init__(*args, **kwargs)
        except BaseException:
            # connect() leaves the socket open if getreply() raises, and a
            # SmtpSessionLimitError is exactly that case.
            try:
                self.close()
            except Exception:
                pass
            raise
        # The reader holds the real socket, so hand it back: starttls() feeds
        # self.sock to ssl.wrap_socket, which wants a genuine socket.
        self.sock = getattr(self.sock, "_sock", self.sock)

    def _get_socket(self, host, port, timeout):
        sock = super()._get_socket(host, port, timeout)
        deadline, chunk_timeout, counter = self._read_bounds
        return _MakefileProxy(sock, _BoundedReader(sock, deadline, chunk_timeout, counter))


def _guard_reads(smtp, deadline: float, command_timeout: float, counter: list) -> None:
    """Install/reinstall the bounded reader. smtplib drops .file in starttls()
    and recreates it lazily, so call this after connect and after STARTTLS.

    After connect the reader is already _BoundedSMTP's: keep it, or anything it
    buffered past the 220 banner is dropped on the floor."""
    try:
        sock = getattr(smtp, "sock", None)
        if sock is None or not hasattr(sock, "recv"):
            return  # test doubles that never touch a socket
        current = getattr(smtp, "file", None)
        if isinstance(current, _BoundedReader) and current._sock is sock:
            return
        smtp.file = _BoundedReader(sock, deadline, command_timeout, counter)
    except Exception:
        pass


def _classify_exc(exc: BaseException) -> tuple[str, int | None, str, str]:
    """(category, code, message, error) for a probe that raised.

    The dividing line is whether the SERVER ANSWERED. A reply carries a code and
    is classified by it. Everything else -- a socket timeout, a mid-session
    disconnect, a protocol error, a bug in our own code -- is ``unknown``: we
    learned nothing about the mailbox, which is not the same as the host
    deferring us.

    Keeping these out of ``temp_fail`` matters twice over:
      * ``recent_temp_fail_rate`` is now the ONLY input to the tarpit decision
        (see resolve/mx.py), and a tarpit is a host that defers on purpose. A
        network outage on our side must not get a healthy MX skipped.
      * The greylist requeue already refuses a probe with no code, so a timeout
        never reached the 60/300/900s schedule anyway. This makes the category
        agree with that gate instead of contradicting it.

    No verdict changes: verify_status_map sends both ``temp_fail`` and
    ``unknown`` to ``unknown_timeout``.
    """
    if isinstance(exc, SmtpSessionLimitError):
        # Our own session cap, not the server's doing: retry is legitimate.
        return "temp_fail", None, "", f"session_limit:{exc}"
    if isinstance(exc, smtplib.SMTPResponseException):
        code = int(getattr(exc, "smtp_code", None) or 0) or None
        return (
            _classify(code),
            code,
            _decode_msg(getattr(exc, "smtp_error", b"")),
            f"smtp_response:{code}",
        )
    if isinstance(exc, TimeoutError):
        return "unknown", None, "", f"timeout:{exc}"
    if isinstance(exc, smtplib.SMTPServerDisconnected):
        kind = "timeout" if "timed out" in (str(exc) or "").lower() else "disconnected"
        return "unknown", None, "", f"{kind}:{exc}"
    if isinstance(exc, smtplib.SMTPException):
        return "unknown", None, "", f"smtp_error:{exc}"
    return "unknown", None, "", f"error:{type(exc).__name__}:{exc}"


def _left(deadline: float) -> float:
    return max(0.0, deadline - time.monotonic())


def _hook(**kw) -> None:
    # Resolved through the live module attribute on purpose: tests monkeypatch
    # smtp.record_behavior and expect exactly one call per real probe.
    sys.modules[__name__].record_behavior(**kw)


def _close(smtp) -> None:
    """Drop the socket without a QUIT round trip (dead or hostile peer)."""
    try:
        if smtp is not None:
            smtp.close()
    except Exception:
        pass


def _quit(smtp) -> None:
    if smtp is None:
        return
    try:
        with socket_timeout_guard(1.0):
            smtp.quit()
    except Exception:
        pass
    try:
        smtp.close()  # quit() skips close() when QUIT itself raises
    except Exception:
        pass


def _flush_hooks(hooks: list[dict]) -> None:
    """B7: write buffered behaviour rows only AFTER the session is closed.

    record_behavior() opens up to 3 unpooled Postgres connections and scans ~5k
    rows. Running it between RCPTs charged that time to the session budget while
    the remote server sat waiting on an open socket. Same rows, later.
    """
    for kw in hooks:
        _hook(**kw)
    hooks.clear()


def _set_sock_timeout(smtp, command_timeout: float, deadline: float) -> None:
    try:
        if smtp.sock is not None:
            smtp.sock.settimeout(min(command_timeout, max(0.05, _left(deadline))))
    except Exception:
        pass


def _mark(results, items, mx_host, helo_domain, *, category, error, defer_seconds=None) -> None:
    """Fill results for addresses that never got a RCPT issued.

    These deliberately do NOT call record_behavior: nothing reached the wire, so
    logging them would inflate the temp_fail_rate / success_rate denominators that
    the tarpit hint is itself computed from. Do not "fix" this to log uniformly.
    """
    for idx, _norm, _dom in items:
        results[idx] = _result(
            mx_host,
            helo_domain,
            category=category,
            error=error,
            attempted=False,
            defer_seconds=defer_seconds,
        )


def probe_rcpt_batch(  # noqa: C901
    emails: list[str],
    mx_host: str,
    *,
    helo_domain: str,
    mail_from: str,
    connect_timeout: float = 20.0,
    command_timeout: float = 20.0,
    behavior_hint: dict | None = None,
    session_max_rcpts: int = SMTP_SESSION_MAX_RCPTS,
    total_budget_s: float = SMTP_PROBE_TOTAL_BUDGET_SECONDS,
) -> list[dict]:
    """
    Probe several addresses on ONE mx_host, reusing a single SMTP session for up
    to `session_max_rcpts` RCPTs (RSET between them), then opening a fresh session
    for the rest. Each session gets ONE wall-clock budget covering connect plus all
    its round trips, so SMTP_MX_MAX_ADDRS can no longer multiply into 40s probes.

    Returns exactly len(emails) dicts in input order, matching probe_rcpt()'s shape
    plus "attempted" (False when no RCPT was issued for that address) and
    "defer_seconds" (set only on tarpit_deferred).

    Precondition (documented, not enforced): all `emails` share one domain.
    """
    if not emails:
        return []
    if not (mx_host or "").strip():
        raise ValueError("mx_host_required")

    # HARD GUARDRAIL: refuse TCP/25 probing unless explicitly allowed.
    assert_smtp_probing_allowed()

    started = time.monotonic()
    results: list[dict] = [None] * len(emails)  # type: ignore[list-item]
    pending: list[tuple[int, str, str]] = []  # (index, email_norm, domain)

    for i, raw in enumerate(emails):
        try:
            _local, dom, norm = _normalize_email(raw)
        except Exception as exc:
            ms = int((time.monotonic() - started) * 1000)
            _hook(
                domain="",
                mx_host=mx_host,
                elapsed_ms=ms,
                category="unknown",
                code=None,
                error_kind="invalid_email",
            )
            results[i] = _result(
                mx_host,
                helo_domain,
                category="unknown",
                error=f"invalid_email:{exc}",
                elapsed_ms=ms,
                attempted=False,
            )
            continue
        pending.append((i, norm, dom))

    if not pending:
        return results

    if behavior_hint is None:
        behavior_hint = _get_hint(mx_host)

    # TARPIT GATE - before any socket touches the network. This is the point of the
    # whole design: a known-hostile-yield MX is skipped, and a slow-but-real one is
    # pushed past the greylisting window, instead of merely getting wider timeouts.
    if isinstance(behavior_hint, dict) and behavior_hint.get("tarpit"):
        try:
            srate = float(behavior_hint.get("success_rate") or 0.0)
        except Exception:
            srate = 0.0
        if srate < SMTP_TARPIT_SKIP_SUCCESS_RATE:
            _mark(
                results, pending, mx_host, helo_domain, category="unknown", error="tarpit_skipped"
            )
        else:
            _mark(
                results,
                pending,
                mx_host,
                helo_domain,
                category="temp_fail",
                error="tarpit_deferred",
                defer_seconds=SMTP_TARPIT_DEFER_SECONDS,
            )
        return results

    c_to, cmd_to = _apply_hint_timeouts(connect_timeout, command_timeout, behavior_hint)

    # Fast-fail preflight: if TCP/25 is blocked, skip the expensive SMTP flow.
    ok25, pre_err = _preflight_port25(mx_host)
    if not ok25:
        err = f"port25_unreachable:{pre_err or ''}".rstrip(":")
        for idx, _norm, dom in pending:
            ms = int((time.monotonic() - started) * 1000)
            _hook(
                domain=dom,
                mx_host=mx_host,
                elapsed_ms=ms,
                category="temp_fail",
                code=None,
                error_kind="port25_unreachable",
            )
            results[idx] = _result(
                mx_host,
                helo_domain,
                category="temp_fail",
                error=err,
                elapsed_ms=ms,
                attempted=False,
            )
        return results

    ips = _resolve_mx_ips(
        mx_host,
        prefer_ipv4=bool(SMTP_PREFER_IPV4),
        max_addrs=int(SMTP_MX_MAX_ADDRS),
    ) or [mx_host]

    cap = max(1, int(session_max_rcpts))

    for chunk_start in range(0, len(pending), cap):
        chunk = pending[chunk_start : chunk_start + cap]
        deadline = time.monotonic() + float(total_budget_s)
        mark = time.monotonic()
        smtp: smtplib.SMTP | None = None
        last_exc: Exception | None = None
        # B7: behaviour rows for this session, written after QUIT (see _flush_hooks).
        hooks: list[dict] = []
        read_bytes: list[int] = [0]  # B6: bytes this session made us read

        try:
            for ip in ips:
                budget = _left(deadline)
                if budget <= 0.05:
                    break  # spending a full connect_timeout here cannot help
                # _BoundedSMTP unless a double has replaced smtplib.SMTP, in
                # which case there is no real socket to bound anyway.
                cls, bounds = smtplib.SMTP, {}
                if cls is _SMTP_BASE:
                    cls = _BoundedSMTP
                    bounds = {
                        "deadline": deadline,
                        "chunk_timeout": cmd_to,
                        "counter": read_bytes,
                    }
                try:
                    smtp = cls(
                        host=ip,
                        port=25,
                        local_hostname=helo_domain,
                        timeout=min(c_to, budget),
                        **bounds,
                    )
                    last_exc = None
                    break
                except (TimeoutError, OSError, smtplib.SMTPException) as exc:
                    last_exc = exc
                    try:
                        if smtp is not None:
                            with socket_timeout_guard(0.5):
                                smtp.close()
                    except Exception:
                        pass
                    smtp = None

            if smtp is None:
                raise last_exc if last_exc is not None else TimeoutError("connect_failed")

            _guard_reads(smtp, deadline, cmd_to, read_bytes)
            _set_sock_timeout(smtp, cmd_to, deadline)

            try:
                smtp.ehlo()
            except smtplib.SMTPHeloError:
                smtp.helo()

            try:
                if hasattr(smtp, "has_extn") and smtp.has_extn("starttls"):
                    smtp.starttls()
                    _guard_reads(smtp, deadline, cmd_to, read_bytes)  # starttls drops .file
                    _set_sock_timeout(smtp, cmd_to, deadline)
                    smtp.ehlo()
            except (OSError, smtplib.SMTPException):
                pass  # STARTTLS failures are non-fatal; proceed in plain
        except Exception as exc:
            cat, code, msg, err = _classify_exc(exc)
            ms = int((time.monotonic() - mark) * 1000)
            for idx, _norm, dom in chunk:
                hooks.append(
                    {
                        "domain": dom,
                        "mx_host": mx_host,
                        "elapsed_ms": ms,
                        "category": cat,
                        "code": code,
                        "error_kind": err.split(":", 1)[0] or "error",
                    }
                )
                results[idx] = _result(
                    mx_host,
                    helo_domain,
                    category=cat,
                    code=code,
                    message=msg,
                    error=err,
                    elapsed_ms=ms,
                    attempted=False,
                )
            _quit(smtp)
            _flush_hooks(hooks)
            continue  # a transient failure here must not doom later slices

        consecutive_temp = 0
        for pos, (idx, norm, dom) in enumerate(chunk):
            rest = chunk[pos + 1 :]
            # 0.05 floor: issuing a RCPT that cannot finish just times out, kills
            # the session, and records a bogus temp_fail datapoint.
            if _left(deadline) <= 0.05:
                _mark(
                    results,
                    chunk[pos:],
                    mx_host,
                    helo_domain,
                    category="temp_fail",
                    error="session_budget_exhausted",
                )
                break

            _set_sock_timeout(smtp, cmd_to, deadline)
            dropped = False
            abort_rest: str | None = None
            try:
                m_raw, m_resp = smtp.mail(mail_from)
                m_code = int(m_raw) if isinstance(m_raw, int) else None
                if m_code is None or not (200 <= m_code < 300):
                    # B2: smtp.mail() does NOT raise on 4xx/5xx. Issuing the RCPT
                    # anyway draws "503 need MAIL command", which _classify()
                    # buckets hard_fail and the sequential verifier turns into an
                    # "invalid" verdict: it deletes a good address and refutes the
                    # company's real pattern (failed_count only ever goes up).
                    # A reply here is about our SENDER or the host's rate limit -
                    # a 4xx IS the pushback the consecutive_temp brake infers.
                    now = time.monotonic()
                    ms = int((now - mark) * 1000)
                    mark = now
                    hooks.append(
                        {
                            "domain": dom,
                            "mx_host": mx_host,
                            "elapsed_ms": ms,
                            "category": "temp_fail",
                            "code": m_code,
                            "error_kind": "mail_from_rejected",
                        }
                    )
                    results[idx] = _result(
                        mx_host,
                        helo_domain,
                        category="temp_fail",
                        code=m_code,
                        message=_decode_msg(m_resp),
                        error=f"mail_from_rejected:{m_code}",
                        elapsed_ms=ms,
                        attempted=False,
                    )
                    _mark(
                        results,
                        rest,
                        mx_host,
                        helo_domain,
                        category="temp_fail",
                        error="mail_from_rejected",
                    )
                    break  # end the session; QUIT happens below
                code_raw, resp = smtp.rcpt(norm)
                code = int(code_raw) if isinstance(code_raw, int) else None
                cat, msg, err = _classify(code), _decode_msg(resp), None
                if code == 503:
                    # "Bad sequence of commands" is a protocol fault between us and
                    # the server, never a mailbox verdict. hard_fail here would
                    # delete the address and teach(ok=False); temp_fail cannot.
                    cat, err = "temp_fail", "protocol_sequence_fault:503"
                    abort_rest = "session_protocol_fault"
            except Exception as exc:
                cat, code, msg, err = _classify_exc(exc)
                if isinstance(exc, SmtpSessionLimitError):
                    # A ceiling breach means the peer is hostile or broken: drop
                    # the socket, do not spend another round trip on QUIT.
                    _close(smtp)
                    smtp = None
                    abort_rest = "session_limit"
                elif isinstance(exc, (smtplib.SMTPServerDisconnected, OSError)):
                    dropped = True
                    err = f"disconnected_mid_session:{err}"

            now = time.monotonic()
            ms = int((now - mark) * 1000)
            mark = now
            hooks.append(
                {
                    "domain": dom,
                    "mx_host": mx_host,
                    "elapsed_ms": ms,
                    "category": cat,
                    "code": code,
                    "error_kind": None if err is None else (err.split(":", 1)[0] or "error"),
                }
            )
            results[idx] = _result(
                mx_host, helo_domain, category=cat, code=code, message=msg, error=err, elapsed_ms=ms
            )

            if dropped:
                _close(smtp)
                smtp = None
                _mark(
                    results,
                    rest,
                    mx_host,
                    helo_domain,
                    category="temp_fail",
                    error="session_dropped",
                )
                break

            if abort_rest:
                # Session state is unusable (503 desync, or a B6 ceiling breached).
                _mark(results, rest, mx_host, helo_domain, category="temp_fail", error=abort_rest)
                break

            # Only a *streak* of 4xx is a rate-limit signal: 56% of all probes are
            # legitimately hard_fail, a definitive per-address answer, not pushback.
            consecutive_temp = consecutive_temp + 1 if cat == "temp_fail" else 0
            if rest and consecutive_temp >= SMTP_SESSION_RATELIMIT_STOP_AFTER:
                _mark(
                    results,
                    rest,
                    mx_host,
                    helo_domain,
                    category="temp_fail",
                    error="rate_limited_stop",
                )
                break  # QUIT cleanly below: the host is alive, just asking us to slow down

            if rest:
                try:
                    smtp.rset()
                except Exception:
                    # Session state is now unknowable; don't guess at it.
                    smtp = None
                    _mark(
                        results,
                        rest,
                        mx_host,
                        helo_domain,
                        category="temp_fail",
                        error="session_dropped",
                    )
                    break

        _quit(smtp)
        _flush_hooks(hooks)

    return results


def probe_rcpt(
    email: str,
    mx_host: str,
    *,
    helo_domain: str,
    mail_from: str,
    connect_timeout: float = 20.0,
    command_timeout: float = 20.0,
    behavior_hint: dict | None = None,
) -> dict:
    """
    Probe deliverability by issuing SMTP MAIL FROM / RCPT TO against an MX host.

    Signature and return keys are unchanged. It now also inherits the hard total
    time budget and the tarpit skip/defer short-circuit from the batch engine.
    """
    return probe_rcpt_batch(
        [email],
        mx_host,
        helo_domain=helo_domain,
        mail_from=mail_from,
        connect_timeout=connect_timeout,
        command_timeout=command_timeout,
        behavior_hint=behavior_hint,
        session_max_rcpts=1,
    )[0]


# --- Small context helper -----------------------------------------------------


class socket_timeout_guard:
    """
    Context manager to temporarily set a shorter default socket timeout for
    a narrow operation (e.g., SMTP QUIT), without disturbing global state.
    """

    def __init__(self, timeout_s: float):
        self.timeout_s = timeout_s
        self._prev: float | None = None

    def __enter__(self):
        self._prev = socket.getdefaulttimeout()
        socket.setdefaulttimeout(self.timeout_s)

    def __exit__(self, exc_type, exc, tb):
        socket.setdefaulttimeout(self._prev)
        return False


__all__ = [
    "probe_rcpt",
    "probe_rcpt_batch",
    "record_behavior",
    "is_greylist_deferral",
    "SmtpProbingDisabledError",
    "SmtpSessionLimitError",
]
