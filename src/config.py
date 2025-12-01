from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

# Optional dependency for YAML-based configs (R14 ICP scoring)
try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None  # type: ignore


def _getenv_int(name: str, default: int) -> int:
    v = os.getenv(name, str(default)).strip()
    try:
        return int(v)
    except ValueError as err:
        raise ValueError(f"Environment variable {name} must be an integer; got {v!r}") from err


def _getenv_str(name: str, default: str) -> str:
    return os.getenv(name, default).strip()


def _getenv_list_int(name: str, default_csv: str) -> list[int]:
    raw = os.getenv(name, default_csv).strip()
    out: list[int] = []
    for tok in (t.strip() for t in raw.split(",")):
        if not tok:
            continue
        try:
            out.append(int(tok))
        except ValueError as err:
            raise ValueError(
                f"Environment variable {name} must be a CSV of integers; got {raw!r}"
            ) from err
    return out


def _getenv_list_str(name: str, default_csv: str) -> list[str]:
    raw = os.getenv(name, default_csv).strip()
    out: list[str] = []
    for tok in (t.strip() for t in raw.split(",")):
        if tok:
            out.append(tok)
    return out


def _getenv_bool(name: str, default: bool) -> bool:
    """
    Read a loosely-typed boolean from the environment.

    Treats "1", "true", "yes", "on" (case-insensitive) as True;
    "0", "false", "no", "off", "" as False. If unset, returns default.
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    v = raw.strip().lower()
    if v in {"1", "true", "yes", "on"}:
        return True
    if v in {"0", "false", "no", "off", ""}:
        return False
    # Fallback: any other non-empty value -> True
    return True


# Load .env from project root if present
ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env", override=False)

# ---- Bot identity (used to enforce USER_AGENT naming) ----
BOT_NAME = "EmailVerifierBot"
CONTACT_EMAIL = "banderson@crestwellpartners.com"
CONTACT_URL = "https://verifier.crestwellpartners.com"


def _getenv_user_agent(env_var: str, default: str) -> str:
    """
    Read a user-agent from the environment, but ensure our bot name is present.
    This satisfies tests that check for the presence of 'EmailVerifierBot'.
    """
    ua = os.getenv(env_var, default).strip()
    if BOT_NAME not in ua:
        ua = f"{BOT_NAME} {ua}"
    return ua


# Defaults expected by tests
DEFAULT_DB_URL = f"sqlite:///{(ROOT / 'dev.db').as_posix()}"  # sqlite file in project root
DEFAULT_USER_AGENT = f"{BOT_NAME}/1.0 (+{CONTACT_URL}; contact: {CONTACT_EMAIL})"


def _parse_intervals(v: str | None) -> list[int]:
    if not v:
        return [60, 300, 900]  # sensible defaults: 1m, 5m, 15m
    return [int(x.strip()) for x in v.split(",") if x.strip()]


# -------------------------------
# R09: Fetch/robots config (constants, env-overridable)
# -------------------------------
FETCH_USER_AGENT: str = _getenv_user_agent(
    "FETCH_USER_AGENT",
    f"{BOT_NAME}/0.9 (+{CONTACT_URL}; contact: {CONTACT_EMAIL})",
)
FETCH_DEFAULT_DELAY_SEC: int = _getenv_int("FETCH_DEFAULT_DELAY_SEC", 3)
FETCH_TIMEOUT_SEC: int = _getenv_int("FETCH_TIMEOUT_SEC", 5)
FETCH_CONNECT_TIMEOUT_SEC: int = _getenv_int("FETCH_CONNECT_TIMEOUT_SEC", 5)
FETCH_CACHE_TTL_SEC: int = _getenv_int("FETCH_CACHE_TTL_SEC", 3600)  # 1h default for HTML
ROBOTS_CACHE_TTL_SEC: int = _getenv_int(
    "ROBOTS_CACHE_TTL_SEC",
    86400,
)  # 24h for robots.txt
# jittered backoff handled by caller
FETCH_MAX_RETRIES: int = _getenv_int("FETCH_MAX_RETRIES", 2)
FETCH_MAX_BODY_BYTES: int = _getenv_int(
    "FETCH_MAX_BODY_BYTES",
    2_000_000,
)  # ≈2MB cap
FETCH_ALLOWED_CONTENT_TYPES: list[str] = _getenv_list_str(
    "FETCH_ALLOWED_CONTENT_TYPES",
    "text/html,text/plain",
)

# -------------------------------
# R10: Crawler config (constants, env-overridable)
# -------------------------------
CRAWL_MAX_PAGES_PER_DOMAIN: int = _getenv_int("CRAWL_MAX_PAGES_PER_DOMAIN", 30)
CRAWL_MAX_DEPTH: int = _getenv_int("CRAWL_MAX_DEPTH", 2)
# ~1.5MB HTML cap to avoid giant blobs
CRAWL_HTML_MAX_BYTES: int = _getenv_int("CRAWL_HTML_MAX_BYTES", 1_500_000)
# Network timeouts (seconds) — left as floats; not part of R10 guardrails but used by crawler
CRAWL_CONNECT_TIMEOUT_S: float = float(os.getenv("CRAWL_CONNECT_TIMEOUT_S", "10"))
CRAWL_READ_TIMEOUT_S: float = float(os.getenv("CRAWL_READ_TIMEOUT_S", "15"))
# CSV seed paths and follow keywords (parsing happens in crawler)
CRAWL_SEED_PATHS: str = os.getenv(
    "CRAWL_SEED_PATHS",
    "/team,/about,/contact,/news,/press,/newsroom",
)
CRAWL_FOLLOW_KEYWORDS: str = os.getenv(
    "CRAWL_FOLLOW_KEYWORDS",
    "team,about,contact,leadership,people,staff,news,press,newsroom",
)

# -------------------------------
# R16: SMTP probe config (constants, env-overridable)
# -------------------------------
SMTP_HELO_DOMAIN = os.getenv("SMTP_HELO_DOMAIN", "verifier.crestwellpartners.com")
SMTP_MAIL_FROM = os.getenv("SMTP_MAIL_FROM", f"bounce@{SMTP_HELO_DOMAIN}")
SMTP_CONNECT_TIMEOUT = float(os.getenv("SMTP_CONNECT_TIMEOUT", "10"))
SMTP_COMMAND_TIMEOUT = float(os.getenv("SMTP_COMMAND_TIMEOUT", "10"))

# -------------------------------
# O07: Third-party verifier fallback config
# -------------------------------
THIRD_PARTY_VERIFY_URL: str = os.getenv("THIRD_PARTY_VERIFY_URL", "").strip()
THIRD_PARTY_VERIFY_API_KEY: str = os.getenv(
    "THIRD_PARTY_VERIFY_API_KEY",
    "",
).strip()
# Default disabled; can be enabled via env flag and will typically also
# check for URL/API key presence at call sites.
THIRD_PARTY_VERIFY_ENABLED: bool = _getenv_bool("THIRD_PARTY_VERIFY_ENABLED", False)

# -------------------------------
# O14/R23: Facet materialized view feature flag
# -------------------------------
FACET_USE_MV: bool = _getenv_bool("FACET_USE_MV", True)


@dataclass(frozen=True)
class Settings:
    # NEW: fields required by tests
    DB_URL: str = _getenv_str("DB_URL", DEFAULT_DB_URL)
    USER_AGENT: str = _getenv_user_agent("USER_AGENT", DEFAULT_USER_AGENT)
    # O23: admin auth / hardening
    ADMIN_API_KEY: str = _getenv_str("ADMIN_API_KEY", "")
    ADMIN_ALLOWED_IPS: list[str] = field(
        default_factory=lambda: _getenv_list_str("ADMIN_ALLOWED_IPS", "")
    )

    # existing fields
    RQ_REDIS_URL: str = os.getenv("RQ_REDIS_URL", "redis://127.0.0.1:6379/0")
    QUEUE_NAME: str = os.getenv("QUEUE_NAME", "verify")
    VERIFY_MAX_ATTEMPTS: int = int(os.getenv("VERIFY_MAX_ATTEMPTS", "3"))
    VERIFY_RETRY_INTERVALS: list[int] = field(
        default_factory=lambda: _parse_intervals(os.getenv("VERIFY_RETRY_INTERVALS"))
    )


@dataclass(frozen=True)
class QueueConfig:
    queue_name: str
    dlq_name: str
    rq_redis_url: str


@dataclass(frozen=True)
class RateLimitConfig:
    global_max_concurrency: int
    global_rps: int
    per_mx_max_concurrency_default: int
    per_mx_rps_default: int


@dataclass(frozen=True)
class RetryTimeoutConfig:
    verify_max_attempts: int
    verify_base_backoff_seconds: int
    verify_max_backoff_seconds: int
    smtp_connect_timeout_seconds: int
    smtp_cmd_timeout_seconds: int
    retry_schedule: list[int]  # RQ Retry schedule in seconds


@dataclass(frozen=True)
class SmtpIdentityConfig:
    helo_domain: str


@dataclass(frozen=True)
class FetchConfig:
    user_agent: str
    default_delay_sec: int
    timeout_sec: int
    connect_timeout_sec: int
    cache_ttl_sec: int
    robots_cache_ttl_sec: int
    max_retries: int
    max_body_bytes: int
    allowed_content_types: list[str]


@dataclass(frozen=True)
class AppConfig:
    queue: QueueConfig
    rate: RateLimitConfig
    retry_timeout: RetryTimeoutConfig
    smtp_identity: SmtpIdentityConfig
    fetch: FetchConfig


def load_settings() -> AppConfig:
    queue = QueueConfig(
        queue_name=_getenv_str("QUEUE_NAME", "verify"),
        dlq_name=_getenv_str("DLQ_NAME", "verify_dlq"),
        rq_redis_url=_getenv_str("RQ_REDIS_URL", "redis://127.0.0.1:6379/0"),
    )
    rate = RateLimitConfig(
        global_max_concurrency=_getenv_int("GLOBAL_MAX_CONCURRENCY", 12),
        global_rps=_getenv_int("GLOBAL_RPS", 6),
        per_mx_max_concurrency_default=_getenv_int(
            "PER_MX_MAX_CONCURRENCY_DEFAULT",
            2,
        ),
        per_mx_rps_default=_getenv_int("PER_MX_RPS_DEFAULT", 1),
    )
    retry_timeout = RetryTimeoutConfig(
        verify_max_attempts=_getenv_int("VERIFY_MAX_ATTEMPTS", 5),
        verify_base_backoff_seconds=_getenv_int("VERIFY_BASE_BACKOFF_SECONDS", 2),
        verify_max_backoff_seconds=_getenv_int("VERIFY_MAX_BACKOFF_SECONDS", 90),
        smtp_connect_timeout_seconds=_getenv_int(
            "SMTP_CONNECT_TIMEOUT_SECONDS",
            20,
        ),
        smtp_cmd_timeout_seconds=_getenv_int("SMTP_CMD_TIMEOUT_SECONDS", 30),
        retry_schedule=_getenv_list_int("RETRY_SCHEDULE", "5,15,45,90,180"),
    )
    smtp_identity = SmtpIdentityConfig(
        helo_domain=_getenv_str(
            "SMTP_HELO_DOMAIN",
            "verifier.crestwellpartners.com",
        ),
    )
    fetch = FetchConfig(
        user_agent=FETCH_USER_AGENT,
        default_delay_sec=FETCH_DEFAULT_DELAY_SEC,
        timeout_sec=FETCH_TIMEOUT_SEC,
        connect_timeout_sec=FETCH_CONNECT_TIMEOUT_SEC,
        cache_ttl_sec=FETCH_CACHE_TTL_SEC,
        robots_cache_ttl_sec=ROBOTS_CACHE_TTL_SEC,
        max_retries=FETCH_MAX_RETRIES,
        max_body_bytes=FETCH_MAX_BODY_BYTES,
        allowed_content_types=FETCH_ALLOWED_CONTENT_TYPES,
    )
    return AppConfig(
        queue=queue,
        rate=rate,
        retry_timeout=retry_timeout,
        smtp_identity=smtp_identity,
        fetch=fetch,
    )


def load_icp_config() -> dict[str, Any]:
    """
    Load ICP scoring configuration for R14 from docs/icp-schema.yaml.

    Returns an empty dict if the file does not exist or PyYAML is unavailable.
    The expected shape is:

      min_required: [...]
      weights:
        role_family: ...
        seniority: ...
        company_size: ...
        industry_bonus: ...
        tech_keywords: ...
      thresholds:
        good: ...
        stretch: ...
        reject: ...
      null_penalty: ...
        cap: ...

    Additional keys (e.g., fields, normalization_rules) are preserved but not
    required by the scorer.
    """
    path = ROOT / "docs" / "icp-schema.yaml"
    if yaml is None or not path.exists():
        return {}

    text = path.read_text(encoding="utf-8")
    cfg = yaml.safe_load(text)
    if not isinstance(cfg, dict):
        return {}
    return cfg or {}


settings: Settings = Settings()

# Your new structured config, if you want to import it elsewhere:
app_config: AppConfig = load_settings()

__all__ = [
    "Settings",
    "QueueConfig",
    "RateLimitConfig",
    "RetryTimeoutConfig",
    "SmtpIdentityConfig",
    "FetchConfig",
    "AppConfig",
    "load_settings",
    "settings",  # legacy flat object
    "app_config",  # structured object
    # R09 fetch/robots constants
    "FETCH_USER_AGENT",
    "FETCH_DEFAULT_DELAY_SEC",
    "FETCH_TIMEOUT_SEC",
    "FETCH_CONNECT_TIMEOUT_SEC",
    "FETCH_CACHE_TTL_SEC",
    "ROBOTS_CACHE_TTL_SEC",
    "FETCH_MAX_RETRIES",
    "FETCH_MAX_BODY_BYTES",
    "FETCH_ALLOWED_CONTENT_TYPES",
    # R10 crawler constants
    "CRAWL_MAX_PAGES_PER_DOMAIN",
    "CRAWL_MAX_DEPTH",
    "CRAWL_HTML_MAX_BYTES",
    "CRAWL_CONNECT_TIMEOUT_S",
    "CRAWL_READ_TIMEOUT_S",
    "CRAWL_SEED_PATHS",
    "CRAWL_FOLLOW_KEYWORDS",
    # R14 ICP config
    "load_icp_config",
    # R16 SMTP probe constants
    "SMTP_HELO_DOMAIN",
    "SMTP_MAIL_FROM",
    "SMTP_CONNECT_TIMEOUT",
    "SMTP_COMMAND_TIMEOUT",
    # O07 fallback config
    "THIRD_PARTY_VERIFY_URL",
    "THIRD_PARTY_VERIFY_API_KEY",
    "THIRD_PARTY_VERIFY_ENABLED",
    # O14/R23 facets MV flag
    "FACET_USE_MV",
]
