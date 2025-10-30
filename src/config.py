# src/config.py
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv


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


# Load .env from project root if present
ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env", override=False)


def _parse_intervals(v: str | None) -> list[int]:
    if not v:
        return [60, 300, 900]  # sensible defaults: 1m, 5m, 15m
    return [int(x.strip()) for x in v.split(",") if x.strip()]


@dataclass(frozen=True)
class Settings:
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
class AppConfig:
    queue: QueueConfig
    rate: RateLimitConfig
    retry_timeout: RetryTimeoutConfig
    smtp_identity: SmtpIdentityConfig


def load_settings() -> AppConfig:
    """Load environment variables into structured config for R06 queue/rate/retry/SMTP."""
    queue = QueueConfig(
        queue_name=_getenv_str("QUEUE_NAME", "verify"),
        dlq_name=_getenv_str("DLQ_NAME", "verify_dlq"),
        rq_redis_url=_getenv_str("RQ_REDIS_URL", "redis://127.0.0.1:6379/0"),
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
        smtp_connect_timeout_seconds=_getenv_int("SMTP_CONNECT_TIMEOUT_SECONDS", 20),
        smtp_cmd_timeout_seconds=_getenv_int("SMTP_CMD_TIMEOUT_SECONDS", 30),
        retry_schedule=_getenv_list_int("RETRY_SCHEDULE", "5,15,45,90,180"),
    )

    smtp_identity = SmtpIdentityConfig(
        helo_domain=_getenv_str("SMTP_HELO_DOMAIN", "verifier.crestwellpartners.com"),
    )

    return AppConfig(
        queue=queue,
        rate=rate,
        retry_timeout=retry_timeout,
        smtp_identity=smtp_identity,
    )
