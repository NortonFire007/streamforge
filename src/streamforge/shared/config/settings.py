from functools import lru_cache
from typing import Annotated

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_env: str = "development"
    app_name: str = "streamforge"
    log_level: str = "INFO"

    postgres_host: str = "localhost"

    postgres_port: Annotated[
        int,
        Field(ge=1, le=65535),
    ] = 5432

    postgres_db: str = "streamforge"
    postgres_user: str = "streamforge"
    postgres_password: str = "streamforge"

    database_url: str | None = None

    db_pool_size: Annotated[
        int,
        Field(ge=1),
    ] = 5

    db_max_overflow: Annotated[
        int,
        Field(ge=0),
    ] = 10

    db_pool_timeout: Annotated[
        float,
        Field(ge=0),
    ] = 30.0

    db_pool_recycle: Annotated[
        int,
        Field(ge=-1),
    ] = 1800

    db_pool_pre_ping: bool = True

    # Kafka shared configuration
    kafka_bootstrap_servers: str = "localhost:9092"

    # Kafka Producer configuration
    kafka_producer_client_id: str = "order-service"
    kafka_producer_acks: str = "all"
    kafka_producer_enable_idempotence: bool = True
    kafka_producer_retries: Annotated[int, Field(ge=0)] = 5
    kafka_producer_linger_ms: Annotated[int, Field(ge=0)] = 5
    kafka_producer_delivery_timeout_ms: Annotated[int, Field(ge=1)] = 120000
    kafka_producer_compression_type: str = "lz4"

    # Kafka Consumer configuration
    # Note: enable.auto.commit is intentionally hardcoded to False in code
    # and MUST NOT be exposed as an environment variable or setting.
    kafka_consumer_client_id: str = "order-consumer"
    kafka_consumer_group_id: str = "order-processing"
    kafka_consumer_auto_offset_reset: str = "earliest"

    @property
    def async_database_url(self) -> str:
        if self.database_url:
            return self.database_url

        return (
            f"postgresql+psycopg://{self.postgres_user}:{self.postgres_password}@"
            f"{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
