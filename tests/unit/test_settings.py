from streamforge.shared.config.settings import Settings


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
    assert settings.database_url is None
    assert settings.db_pool_size == 5
    assert settings.db_max_overflow == 10
    assert settings.db_pool_timeout == 30.0
    assert settings.db_pool_recycle == 1800
    assert settings.db_pool_pre_ping is True


def test_settings_database_url_direct_override() -> None:
    custom_url = "postgresql+psycopg://custom_user:custom_pass@dbhost:5433/custom_db"
    settings = Settings(database_url=custom_url)
    assert settings.async_database_url == custom_url


def test_settings_environment_override(monkeypatch: object) -> None:
    from pytest import MonkeyPatch

    assert isinstance(monkeypatch, MonkeyPatch)
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://envuser:envpass@envhost:5432/envdb")
    monkeypatch.setenv("DB_POOL_SIZE", "20")
    monkeypatch.setenv("DB_MAX_OVERFLOW", "15")
    monkeypatch.setenv("DB_POOL_TIMEOUT", "45.5")
    monkeypatch.setenv("DB_POOL_RECYCLE", "3600")
    monkeypatch.setenv("DB_POOL_PRE_PING", "false")

    settings = Settings()
    assert settings.app_env == "production"
    assert settings.postgres_port == 5433
    assert (
        settings.async_database_url
        == "postgresql+psycopg://envuser:envpass@envhost:5432/envdb"
    )
    assert settings.db_pool_size == 20
    assert settings.db_max_overflow == 15
    assert settings.db_pool_timeout == 45.5
    assert settings.db_pool_recycle == 3600
    assert settings.db_pool_pre_ping is False
