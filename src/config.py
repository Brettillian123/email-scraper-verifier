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


# ---------------------------------------------------------------------------
# Env helpers
# ---------------------------------------------------------------------------


def _getenv_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        return int(raw)
    except ValueError as err:
        raise ValueError(
            f"Environment variable {name} must be an integer; got {raw!r}"
        ) from err


def _getenv_float(name: str, default: float) -> float:
    raw = os.getenv(name, str(default)).strip()
    try:
        return float(raw)
    except ValueError as err:
        raise ValueError(
            f"Environment variable {name} must be a float; got {raw!r}"
        ) from err


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

    Any other non-empty value is treated as True for permissiveness.
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    v = raw.strip().lower()
    if v in {"1", "true", "yes", "on"}:
        return True
    if v in {"0", "false", "no", "off", ""}:
        return False
    return True


def _parse_intervals(v: str | None) -> list[int]:
    """
    Parse a CSV list of retry intervals in seconds.

    If unset/empty, returns sensible defaults: [60, 300, 900] (1m, 5m, 15m).
    """
    if not v:
        return [60, 300, 900]
    out: list[int] = []
    for tok in (t.strip() for t in v.split(",")):
        if not tok:
            continue
        try:
            out.append(int(tok))
        except ValueError as err:
            raise ValueError(
                f"VERIFY_RETRY_INTERVALS must be a CSV of integers; got {v!r}"
            ) from err
    return out


# ---------------------------------------------------------------------------
# .env loading and identity
# ---------------------------------------------------------------------------

# Load .env from project root if present
ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env", override=False)

# Bot identity (used to enforce USER_AGENT naming)
# NOTE: The primary bot name is "EmailVerifierBot", but we also include
# "Email-Scraper" as an alias for robots.txt compatibility with tests.
BOT_NAME = "EmailVerifierBot"
BOT_NAME_ALIAS = "Email-Scraper"  # For robots.txt test compatibility
CONTACT_EMAIL = "banderson@crestwellpartners.com"
CONTACT_URL = "https://verifier.crestwellpartners.com"


def _getenv_user_agent(env_var: str, default: str) -> str:
    """
    Read a user-agent from the environment, but ensure our bot name is present.

    This satisfies tests that check for the presence of 'EmailVerifierBot'.
    Also includes the Email-Scraper alias for robots.txt compatibility.
    """
    ua = os.getenv(env_var, default).strip()
    if BOT_NAME not in ua:
        ua = f"{BOT_NAME} {ua}"
    # Also ensure the alias is present for robots.txt compatibility
    if BOT_NAME_ALIAS not in ua:
        ua = f"{ua} (compatible; {BOT_NAME_ALIAS})"
    return ua


# Defaults expected by tests
DEFAULT_DB_URL = f"sqlite:///{(ROOT / 'dev.db').as_posix()}"
DEFAULT_USER_AGENT = (
    f"{BOT_NAME}/1.0 (+{CONTACT_URL}; contact: {CONTACT_EMAIL}; "
    f"compatible; {BOT_NAME_ALIAS})"
)

# ---------------------------------------------------------------------------
# R09: Fetch/robots config (constants, env-overridable)
# ---------------------------------------------------------------------------

FETCH_USER_AGENT: str = _getenv_user_agent(
    "FETCH_USER_AGENT",
    f"{BOT_NAME}/0.9 (+{CONTACT_URL}; contact: {CONTACT_EMAIL})",
)
FETCH_DEFAULT_DELAY_SEC: int = _getenv_int("FETCH_DEFAULT_DELAY_SEC", 3)
FETCH_TIMEOUT_SEC: int = _getenv_int("FETCH_TIMEOUT_SEC", 5)
FETCH_CONNECT_TIMEOUT_SEC: int = _getenv_int("FETCH_CONNECT_TIMEOUT_SEC", 5)
FETCH_CACHE_TTL_SEC: int = _getenv_int("FETCH_CACHE_TTL_SEC", 3600)  # 1h default for HTML
ROBOTS_CACHE_TTL_SEC: int = _getenv_int("ROBOTS_CACHE_TTL_SEC", 86400)  # 24h for robots.txt
# jittered backoff handled by caller
FETCH_MAX_RETRIES: int = _getenv_int("FETCH_MAX_RETRIES", 2)
FETCH_MAX_BODY_BYTES: int = _getenv_int("FETCH_MAX_BODY_BYTES", 2_000_000)  # ~2MB cap
FETCH_ALLOWED_CONTENT_TYPES: list[str] = _getenv_list_str(
    "FETCH_ALLOWED_CONTENT_TYPES",
    "text/html,text/plain",
)

# ---------------------------------------------------------------------------
# R10: Crawler config (constants, env-overridable)
# ---------------------------------------------------------------------------

CRAWL_MAX_PAGES_PER_DOMAIN: int = _getenv_int("CRAWL_MAX_PAGES_PER_DOMAIN", 30)
CRAWL_MAX_DEPTH: int = _getenv_int("CRAWL_MAX_DEPTH", 2)
CRAWL_HTML_MAX_BYTES: int = _getenv_int("CRAWL_HTML_MAX_BYTES", 1_500_000)  # ~1.5MB cap

# Network timeouts (seconds) — used by crawler; floats are allowed.
CRAWL_CONNECT_TIMEOUT_S: float = _getenv_float("CRAWL_CONNECT_TIMEOUT_S", 10.0)
CRAWL_READ_TIMEOUT_S: float = _getenv_float("CRAWL_READ_TIMEOUT_S", 15.0)

# If enabled, only enqueue seed paths that are discovered by parsing links from
# one or more discovery pages (defaults to "/" and "/about").
CRAWL_SEEDS_LINKED_ONLY: bool = _getenv_bool("CRAWL_SEEDS_LINKED_ONLY", False)
CRAWL_DISCOVERY_PATHS: list[str] = []  # populated below after helper definitions

# ---------------------------------------------------------------------------
# Tiered seed paths (keep ALL)
# ---------------------------------------------------------------------------

SEED_TIER_1: list[str] = [
    "/",
    "/about",
    "/about-us",
    "/company",
    "/who-we-are",
    "/team",
    "/our-team",
    "/people",
    "/leadership",
    "/executives",
    "/management",
    "/founders",
    "/board",
    "/board-of-directors",
    "/advisors",
    "/investors",
    "/investor-relations",
    "/contact",
]

SEED_TIER_2: list[str] = [
    "/our-company",
    "/our-story",
    "/mission",
    "/values",
    "/culture",
    "/contact-us",
    "/locations",
    "/location",
    "/offices",
    "/office",
    "/teams",
    "/our-teams",
    "/meet-the-team",
    "/meet-our-team",
    "/team-members",
    "/team-member",
    "/our-people",
    "/meet-the-people",
    "/our-crew",
    "/crew",
    "/staff",
    "/our-staff",
    "/staff-directory",
    "/directory",
    "/employee-directory",
    "/employees",
    "/company-directory",
    "/our-leadership",
    "/leadership-team",
    "/executive-team",
    "/exec-team",
    "/executive",
    "/management-team",
    "/senior-leadership",
    "/senior-team",
    "/our-founders",
    "/founder",
    "/co-founders",
    "/cofounders",
    "/founding-team",
    "/founding",
    "/our-board",
    "/directors",
    "/governance",
    "/corporate-governance",
    "/our-advisors",
    "/advisor",
    "/advisory-board",
    "/advisory-council",
    "/ir",
    "/about/team",
    "/about/our-team",
    "/about/people",
    "/about/leadership",
    "/about/our-leadership",
    "/about/leadership-team",
    "/about/executive-team",
    "/about/executives",
    "/about/management",
    "/about/management-team",
    "/about/founders",
    "/about/our-founders",
    "/about/board",
    "/about/our-board",
    "/about/board-of-directors",
    "/about/advisors",
    "/about/advisory-board",
    "/company/team",
    "/company/leadership",
    "/company/management",
    "/company/board",
    "/company/executives",
]

SEED_TIER_3: list[str] = [
    "/en/about",
    "/en/team",
    "/en/leadership",
    "/en/about/team",
    "/en/about/leadership",
    "/us/about",
    "/us/team",
    "/us/leadership",
]

CRAWL_SEED_TIERS: list[list[str]] = [SEED_TIER_1, SEED_TIER_2, SEED_TIER_3]
CRAWL_SEED_PATHS: list[str] = SEED_TIER_1 + SEED_TIER_2 + SEED_TIER_3

# Threshold: stop adding seed tiers if we've already found N pages with people data.
CRAWL_SEED_STOP_MIN_PEOPLE_PAGES: int = _getenv_int("CRAWL_SEED_STOP_MIN_PEOPLE_PAGES", 3)

# Keywords that indicate a link is likely to lead to people/team content.
CRAWL_FOLLOW_KEYWORDS: list[str] = _getenv_list_str(
    "CRAWL_FOLLOW_KEYWORDS",
    "team,people,staff,about,leadership,management,executive,founder,board,advisor,directory,contact,investor",
)

# Discovery paths (default: "/" and "/about")
CRAWL_DISCOVERY_PATHS = _getenv_list_str("CRAWL_DISCOVERY_PATHS", "/,/about")

# ---------------------------------------------------------------------------
# R16: SMTP probe config (env-overridable)
# ---------------------------------------------------------------------------

SMTP_PROBES_ENABLED: bool = _getenv_bool("SMTP_PROBES_ENABLED", True)
SMTP_PROBES_ALLOWED_HOSTS: list[str] = _getenv_list_str("SMTP_PROBES_ALLOWED_HOSTS", "")

SMTP_HELO_DOMAIN: str = _getenv_str("SMTP_HELO_DOMAIN", "verifier.crestwellpartners.com")
SMTP_MAIL_FROM: str = _getenv_str("SMTP_MAIL_FROM", f"bounce@{SMTP_HELO_DOMAIN}")
SMTP_CONNECT_TIMEOUT: int = _getenv_int("SMTP_CONNECT_TIMEOUT", 5)
SMTP_COMMAND_TIMEOUT: int = _getenv_int("SMTP_COMMAND_TIMEOUT", 10)

# TCP port-25 preflight (before full SMTP conversation)
SMTP_PREFLIGHT_ENABLED: bool = _getenv_bool("SMTP_PREFLIGHT_ENABLED", True)
SMTP_PREFLIGHT_TIMEOUT_SECONDS: float = _getenv_float("SMTP_PREFLIGHT_TIMEOUT_SECONDS", 1.5)
SMTP_PREFLIGHT_MAX_ADDRS: int = _getenv_int("SMTP_PREFLIGHT_MAX_ADDRS", 3)
SMTP_PREFLIGHT_CACHE_TTL_SECONDS: int = _getenv_int("SMTP_PREFLIGHT_CACHE_TTL_SECONDS", 300)

# How many MX IPs to try before giving up
SMTP_MX_MAX_ADDRS: int = _getenv_int("SMTP_MX_MAX_ADDRS", 3)

# Prefer IPv4 over IPv6 (many residential ISPs block outbound port 25 on IPv6)
SMTP_PREFER_IPV4: bool = _getenv_bool("SMTP_PREFER_IPV4", True)

# ---------------------------------------------------------------------------
# O07: Third-party fallback verification (env-overridable)
# ---------------------------------------------------------------------------

THIRD_PARTY_VERIFY_URL: str = _getenv_str("THIRD_PARTY_VERIFY_URL", "")
THIRD_PARTY_VERIFY_API_KEY: str = _getenv_str("THIRD_PARTY_VERIFY_API_KEY", "")
THIRD_PARTY_VERIFY_ENABLED: bool = _getenv_bool(
    "THIRD_PARTY_VERIFY_ENABLED",
    bool(THIRD_PARTY_VERIFY_URL and THIRD_PARTY_VERIFY_API_KEY),
)

# ---------------------------------------------------------------------------
# O14/R23: Facets materialized view flag
# ---------------------------------------------------------------------------

FACET_USE_MV: bool = _getenv_bool("FACET_USE_MV", False)


# ---------------------------------------------------------------------------
# Structured config classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Settings:
    """
    Global settings container.

    Attributes are read at import time and frozen for the lifetime of the process.

    Compatibility: a number of legacy/uppercase attribute names are exposed as
    @property aliases (DB_URL, USER_AGENT, ADMIN_API_KEY, ADMIN_ALLOWED_IPS).
    """

    database_url: str = field(
        default_factory=lambda: _getenv_str(
            "DATABASE_URL",
            _getenv_str("DB_URL", DEFAULT_DB_URL),
        )
    )
    user_agent: str = field(
        default_factory=lambda: _getenv_user_agent(
            "USER_AGENT",
            FETCH_USER_AGENT,
        )
    )
    admin_api_key: str = field(default_factory=lambda: _getenv_str("ADMIN_API_KEY", ""))
    admin_allowed_ips: tuple[str, ...] = field(
        default_factory=lambda: tuple(_getenv_list_str("ADMIN_ALLOWED_IPS", ""))
    )
    debug: bool = field(default_factory=lambda: _getenv_bool("DEBUG", False))

    # ---- legacy / uppercase aliases ----

    @property
    def DB_URL(self) -> str:  # noqa: N802
        return self.database_url

    @property
    def USER_AGENT(self) -> str:  # noqa: N802
        return self.user_agent

    @property
    def ADMIN_API_KEY(self) -> str:  # noqa: N802
        return self.admin_api_key

    @property
    def ADMIN_ALLOWED_IPS(self) -> tuple[str, ...]:  # noqa: N802
        return self.admin_allowed_ips

    # O27: AI people extraction config
    openai_api_key: str | None = os.getenv("OPENAI_API_KEY", "").strip() or None
    ai_people_model: str = _getenv_str("AI_PEOPLE_MODEL", "gpt-5-nano")
    ai_people_enabled: bool = _getenv_bool("AI_PEOPLE_ENABLED", False)
    ai_people_max_input_tokens: int = _getenv_int("AI_PEOPLE_MAX_INPUT_TOKENS", 1500)


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
    retry_schedule: list[int]


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


# ---------------------------------------------------------------------------
# O26: AWS SES / test-send email config
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AwsSesConfig:
    """
    Minimal AWS SES config.

    Access key / secret are optional; if unset, boto3 will fall back to the
    default AWS credential chain (env, shared config, EC2/ECS metadata, etc.).
    """

    region: str
    access_key_id: str | None
    secret_access_key: str | None


@dataclass(frozen=True)
class TestSendEmailConfig:
    """
    Email-level config for O26 test-sends.

    This is distinct from src.verify.test_send.TestSendConfig, which controls
    behavioral aspects (delivered_after, update_verify_status).
    """

    from_address: str
    reply_to: str | None
    mail_from_domain: str
    bounce_prefix: str
    subject_prefix: str
    bounces_sqs_url: str | None


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
        rq_redis_url=_getenv_str(
            "RQ_REDIS_URL",
            _getenv_str("REDIS_URL", "redis://127.0.0.1:6379/0"),
        ),
    )

    rate = RateLimitConfig(
        global_max_concurrency=_getenv_int("GLOBAL_MAX_CONCURRENCY", 12),
        global_rps=_getenv_int("GLOBAL_RPS", 6),
        per_mx_max_concurrency_default=_getenv_int("PER_MX_MAX_CONCURRENCY_DEFAULT", 2),
        per_mx_rps_default=_getenv_int("PER_MX_RPS_DEFAULT", 1),
    )

    retry_timeout = RetryTimeoutConfig(
        verify_max_attempts=_getenv_int("VERIFY_MAX_ATTEMPTS", 5),
        verify_base_backoff_seconds=_getenv_int("VERIFY_BASE_BACKOFF_SECONDS", 2),
        verify_max_backoff_seconds=_getenv_int("VERIFY_MAX_BACKOFF_SECONDS", 90),
        smtp_connect_timeout_seconds=_getenv_int("SMTP_CONNECT_TIMEOUT_SECONDS", 5),
        smtp_cmd_timeout_seconds=_getenv_int(
            "SMTP_CMD_TIMEOUT_SECONDS",
            _getenv_int("SMTP_COMMAND_TIMEOUT_SECONDS", 10),
        ),
        retry_schedule=_getenv_list_int("RETRY_SCHEDULE", "5,15,45,90,180"),
    )

    smtp_identity = SmtpIdentityConfig(
        helo_domain=_getenv_str("SMTP_HELO_DOMAIN", "verifier.crestwellpartners.com"),
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


def load_aws_ses_config() -> AwsSesConfig:
    """
    Load AWS SES configuration for O26 test-sends.

    If AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY are not set, callers should
    rely on boto3's default credential resolution chain.
    """
    region = _getenv_str("AWS_REGION", "us-east-1")
    access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    bad_partial = (access_key_id and not secret_access_key) or (
        secret_access_key and not access_key_id
    )
    if bad_partial:
        raise ValueError(
            "Both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set "
            "together, or neither (to use the default AWS credential chain).",
        )

    return AwsSesConfig(
        region=region,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
    )


def load_test_send_email_config() -> TestSendEmailConfig:
    """
    Load SES / email settings for O26 test-sends from environment.

    Expected env vars:
      TEST_SEND_FROM                (required)
      TEST_SEND_REPLY_TO            (optional)
      TEST_SEND_MAIL_FROM_DOMAIN    (required)
      TEST_SEND_BOUNCE_PREFIX       (default: "bounce")
      TEST_SEND_SUBJECT_PREFIX      (default: "[IQVerifier Test]")
      TEST_SEND_BOUNCES_SQS_URL     (optional; used by SQS consumer)
    """
    from_address = _getenv_str("TEST_SEND_FROM", "")
    if not from_address:
        raise ValueError("TEST_SEND_FROM must be set (FROM header for test-sends).")

    reply_to_raw = os.getenv("TEST_SEND_REPLY_TO")
    reply_to = reply_to_raw.strip() if reply_to_raw is not None else None

    mail_from_domain = _getenv_str("TEST_SEND_MAIL_FROM_DOMAIN", "")
    if not mail_from_domain:
        raise ValueError(
            "TEST_SEND_MAIL_FROM_DOMAIN must be set (MAIL FROM / return-path domain).",
        )

    bounce_prefix = _getenv_str("TEST_SEND_BOUNCE_PREFIX", "bounce")
    subject_prefix = _getenv_str("TEST_SEND_SUBJECT_PREFIX", "[IQVerifier Test]")

    bounces_sqs_url_raw = os.getenv("TEST_SEND_BOUNCES_SQS_URL")
    bounces_sqs_url = bounces_sqs_url_raw.strip() if bounces_sqs_url_raw else None

    return TestSendEmailConfig(
        from_address=from_address,
        reply_to=reply_to,
        mail_from_domain=mail_from_domain,
        bounce_prefix=bounce_prefix,
        subject_prefix=subject_prefix,
        bounces_sqs_url=bounces_sqs_url,
    )


def load_icp_config() -> dict[str, Any]:
    """
    Load ICP scoring configuration for R14 from docs/icp-schema.yaml.

    Returns an empty dict if the file does not exist or PyYAML is unavailable.
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
app_config: AppConfig = load_settings()

__all__ = [
    # Settings / structured config
    "Settings",
    "QueueConfig",
    "RateLimitConfig",
    "RetryTimeoutConfig",
    "SmtpIdentityConfig",
    "FetchConfig",
    "AwsSesConfig",
    "TestSendEmailConfig",
    "AppConfig",
    "load_settings",
    "load_aws_ses_config",
    "load_test_send_email_config",
    "settings",
    "app_config",
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
    "CRAWL_SEEDS_LINKED_ONLY",
    "CRAWL_DISCOVERY_PATHS",
    "SEED_TIER_1",
    "SEED_TIER_2",
    "SEED_TIER_3",
    "CRAWL_SEED_TIERS",
    "CRAWL_SEED_PATHS",
    "CRAWL_SEED_STOP_MIN_PEOPLE_PAGES",
    "CRAWL_FOLLOW_KEYWORDS",
    # R14 ICP config
    "load_icp_config",
    # R16 SMTP probe constants
    "SMTP_PROBES_ENABLED",
    "SMTP_PROBES_ALLOWED_HOSTS",
    "SMTP_HELO_DOMAIN",
    "SMTP_MAIL_FROM",
    "SMTP_CONNECT_TIMEOUT",
    "SMTP_COMMAND_TIMEOUT",
    "SMTP_PREFLIGHT_ENABLED",
    "SMTP_PREFLIGHT_TIMEOUT_SECONDS",
    "SMTP_PREFLIGHT_MAX_ADDRS",
    "SMTP_PREFLIGHT_CACHE_TTL_SECONDS",
    "SMTP_MX_MAX_ADDRS",
    "SMTP_PREFER_IPV4",
    # O07 fallback config
    "THIRD_PARTY_VERIFY_URL",
    "THIRD_PARTY_VERIFY_API_KEY",
    "THIRD_PARTY_VERIFY_ENABLED",
    # O14/R23 facets MV flag
    "FACET_USE_MV",
    # Bot identity
    "BOT_NAME",
    "BOT_NAME_ALIAS",
    "CONTACT_EMAIL",
    "CONTACT_URL",
    "DEFAULT_USER_AGENT",
]
