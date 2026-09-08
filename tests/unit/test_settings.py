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

    # Kafka defaults
    assert settings.kafka_bootstrap_servers == "localhost:9092"
    assert settings.kafka_producer_client_id == "order-service"
    assert settings.kafka_producer_acks == "all"
    assert settings.kafka_producer_enable_idempotence is True
    assert settings.kafka_producer_retries == 5
    assert settings.kafka_producer_linger_ms == 5
    assert settings.kafka_producer_delivery_timeout_ms == 120000
    assert settings.kafka_producer_compression_type == "lz4"
    assert settings.kafka_consumer_client_id == "order-consumer"
    assert settings.kafka_consumer_group_id == "order-processing"
    assert settings.kafka_consumer_auto_offset_reset == "earliest"
    assert not hasattr(settings, "enable_auto_commit")
    assert not hasattr(settings, "kafka_consumer_enable_auto_commit")


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

    # Kafka env overrides
    monkeypatch.setenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "broker-1:9092,broker-2:9093,broker-3:9094",
    )
    monkeypatch.setenv("KAFKA_PRODUCER_CLIENT_ID", "order-service-node-1")
    monkeypatch.setenv("KAFKA_PRODUCER_ACKS", "1")
    monkeypatch.setenv("KAFKA_PRODUCER_ENABLE_IDEMPOTENCE", "false")
    monkeypatch.setenv("KAFKA_PRODUCER_RETRIES", "10")
    monkeypatch.setenv("KAFKA_PRODUCER_LINGER_MS", "20")
    monkeypatch.setenv("KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS", "60000")
    monkeypatch.setenv("KAFKA_PRODUCER_COMPRESSION_TYPE", "gzip")
    monkeypatch.setenv("KAFKA_CONSUMER_CLIENT_ID", "order-consumer-node-1")
    monkeypatch.setenv("KAFKA_CONSUMER_GROUP_ID", "order-processing-v2")
    monkeypatch.setenv("KAFKA_CONSUMER_AUTO_OFFSET_RESET", "latest")

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

    assert (
        settings.kafka_bootstrap_servers
        == "broker-1:9092,broker-2:9093,broker-3:9094"
    )
    assert settings.kafka_producer_client_id == "order-service-node-1"
    assert settings.kafka_producer_acks == "1"
    assert settings.kafka_producer_enable_idempotence is False
    assert settings.kafka_producer_retries == 10
    assert settings.kafka_producer_linger_ms == 20
    assert settings.kafka_producer_delivery_timeout_ms == 60000
    assert settings.kafka_producer_compression_type == "gzip"
    assert settings.kafka_consumer_client_id == "order-consumer-node-1"
    assert settings.kafka_consumer_group_id == "order-processing-v2"
    assert settings.kafka_consumer_auto_offset_reset == "latest"
