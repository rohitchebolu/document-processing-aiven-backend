"""Runtime configuration for the Gemini + Aiven document processing service."""

from functools import lru_cache
import logging
import os

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class SecretsConfig(BaseSettings):
    """Minimal settings for the active document-processing flow."""

    VERTEX_AI_SERVICE_ACCOUNT_FILE: str | None = Field(
        default=None,
        description="Path to the Vertex AI service account JSON file.",
    )
    VERTEX_AI_LOCATION: str = Field(
        default="us-central1",
        description="Vertex AI region.",
    )
    VERTEX_AI_MODEL: str = Field(
        default="gemini-2.5-flash-lite",
        description="Vertex AI Gemini model to use.",
    )

    SQLALCHEMY_DATABASE_URL: str = Field(
        default="sqlite:///./database.db",
        description="Database connection URL.",
    )

    KAFKA_BOOTSTRAP_SERVERS: str | None = Field(
        default=None,
        description="Aiven Kafka bootstrap servers.",
    )
    KAFKA_SSL_CA_CERT_PATH: str | None = Field(
        default=None,
        description="Path to the Aiven Kafka CA certificate.",
    )
    KAFKA_SSL_ACCESS_CERT_PATH: str | None = Field(
        default=None,
        description="Path to the Aiven Kafka client certificate.",
    )
    KAFKA_SSL_ACCESS_KEY_PATH: str | None = Field(
        default=None,
        description="Path to the Aiven Kafka client key.",
    )

    VALKEY_URL: str | None = Field(
        default=None,
        description="Aiven Valkey connection URL.",
    )

    ENVIRONMENT: str = Field(
        default="development",
        description="Environment name.",
    )
    DEBUG: bool = Field(
        default=False,
        description="Debug mode.",
    )
    LOG_LEVEL: str = Field(
        default="INFO",
        description="Application log level.",
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="allow",
    )

    @field_validator("VERTEX_AI_SERVICE_ACCOUNT_FILE", mode="before")
    @classmethod
    def resolve_credentials_path(cls, value: str | None) -> str | None:
        """Resolve the Vertex credentials path when a relative path is supplied."""
        if value:
            if os.path.isabs(value):
                return value
            return os.path.join(os.getcwd(), value)

        return None

    @field_validator("SQLALCHEMY_DATABASE_URL", mode="before")
    @classmethod
    def normalize_database_url(cls, value: str) -> str:
        """Normalize legacy postgres:// URLs for SQLAlchemy."""
        if value.startswith("postgres://"):
            return value.replace("postgres://", "postgresql://", 1)
        return value

    @field_validator("ENVIRONMENT")
    @classmethod
    def validate_environment(cls, value: str) -> str:
        valid_envs = {"development", "beta", "staging", "production", "test"}
        normalized = value.lower()
        if normalized not in valid_envs:
            raise ValueError(f"ENVIRONMENT must be one of {sorted(valid_envs)}")
        return normalized

    @field_validator("LOG_LEVEL")
    @classmethod
    def validate_log_level(cls, value: str) -> str:
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        normalized = value.upper()
        if normalized not in valid_levels:
            raise ValueError(f"LOG_LEVEL must be one of {sorted(valid_levels)}")
        return normalized

    @field_validator("DEBUG", mode="before")
    @classmethod
    def normalize_debug_flag(cls, value: bool | str) -> bool | str:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes", "on", "debug"}:
                return True
            if normalized in {"0", "false", "no", "off", "release", "prod", "production"}:
                return False
        return value


@lru_cache(maxsize=1)
def get_secrets_config() -> SecretsConfig:
    """Return the cached settings instance."""
    config = SecretsConfig()
    logger.info("Loaded secrets for environment: %s", config.ENVIRONMENT)
    return config
