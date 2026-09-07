from shared.config.settings import Settings


def test_default_settings() -> None:
    settings = Settings(
        postgres_host="testhost",
        postgres_port=5432,
        postgres_db="testdb",
        postgres_user="testuser",
        postgres_password="secretpassword",
    )

    assert settings.app_env == "development"
    assert settings.app_name == "streamforge"
    assert (
        settings.async_database_url
        == "postgresql+psycopg://testuser:secretpassword@testhost:5432/testdb"
    )


def test_settings_environment_override(monkeypatch: object) -> None:
    from pytest import MonkeyPatch

    assert isinstance(monkeypatch, MonkeyPatch)
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("POSTGRES_PORT", "5433")

    settings = Settings()
    assert settings.app_env == "production"
    assert settings.postgres_port == 5433
