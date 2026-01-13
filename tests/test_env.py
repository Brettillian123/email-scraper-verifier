from __future__ import annotations

from src.config import settings


def _get_setting_str(*names: str) -> str:
    for name in names:
        val = getattr(settings, name, None)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return ""


def test_settings_loads_defaults() -> None:
    db_url = _get_setting_str("DB_URL", "DATABASE_URL", "db_url", "database_url")
    assert db_url, "No DB URL found on settings (tried DB_URL/DATABASE_URL/db_url/database_url)"
    assert db_url.startswith(
        ("sqlite://", "sqlite:///", "postgresql://", "postgres://")
    ), f"Unexpected DB URL scheme: {db_url!r}"


def test_user_agent_has_name() -> None:
    user_agent = _get_setting_str("USER_AGENT", "user_agent")
    assert user_agent, "No USER_AGENT found on settings (tried USER_AGENT/user_agent)"
    assert "EmailVerifierBot" in user_agent
