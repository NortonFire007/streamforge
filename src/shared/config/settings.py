from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_env: str = Field(default="development", description="Application environment")
    app_name: str = Field(default="streamforge", description="Application name")
    log_level: str = Field(default="INFO", description="Log level")

    postgres_host: str = Field(default="localhost", description="PostgreSQL host")
    postgres_port: int = Field(default=5432, description="PostgreSQL port")
    postgres_db: str = Field(default="streamforge", description="PostgreSQL database name")
    postgres_user: str = Field(default="streamforge", description="PostgreSQL username")
    postgres_password: str = Field(default="streamforge", description="PostgreSQL password")

    kafka_bootstrap_servers: str = Field(
        default="localhost:9092", description="Kafka bootstrap servers"
    )

    @property
    def async_database_url(self) -> str:
        return (
            f"postgresql+psycopg://{self.postgres_user}:{self.postgres_password}@"
            f"{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
