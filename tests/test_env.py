from src.config import settings


def test_settings_loads_defaults():
    assert settings.DB_URL.startswith(("sqlite://", "postgresql://"))


def test_user_agent_has_name():
    assert "EmailVerifierBot" in settings.USER_AGENT
