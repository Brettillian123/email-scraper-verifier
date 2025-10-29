import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Load .env from project root if present
ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env", override=False)


@dataclass(frozen=True)
class Settings:
    ENV: str = os.getenv("ENV", "dev")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    DB_URL: str = os.getenv("DB_URL", "sqlite:///dev.db")
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    USER_AGENT: str = os.getenv("USER_AGENT", "EmailVerifierBot/0.1")
    HELO_DOMAIN: str = os.getenv("HELO_DOMAIN", "localhost")


settings = Settings()
