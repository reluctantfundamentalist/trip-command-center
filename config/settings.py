"""
Pydantic BaseSettings configuration for the Trip.com Flights Sales Intelligence Platform.
All values can be overridden via environment variables.
"""

from pydantic_settings import BaseSettings
from pydantic import Field


class DatabaseSettings(BaseSettings):
    """PostgreSQL connection settings."""
    url: str = Field(
        default="postgresql+asyncpg://flights:flights@localhost:5432/flights_sales",
        alias="DATABASE_URL",
    )
    pool_min_size: int = Field(default=5, alias="DB_POOL_MIN")
    pool_max_size: int = Field(default=20, alias="DB_POOL_MAX")
    echo: bool = Field(default=False, alias="DB_ECHO")

    model_config = {"env_prefix": "", "populate_by_name": True}


class RedisSettings(BaseSettings):
    """Redis connection settings."""
    url: str = Field(default="redis://localhost:6379/0", alias="REDIS_URL")

    model_config = {"env_prefix": "", "populate_by_name": True}


class KafkaSettings(BaseSettings):
    """Kafka broker settings."""
    brokers: str = Field(default="localhost:9092", alias="KAFKA_BROKERS")
    extraction_topic: str = Field(default="extraction.jobs", alias="KAFKA_EXTRACTION_TOPIC")
    consumer_group: str = Field(default="extraction-workers", alias="KAFKA_CONSUMER_GROUP")

    model_config = {"env_prefix": "", "populate_by_name": True}

    @property
    def broker_list(self) -> list[str]:
        return [b.strip() for b in self.brokers.split(",")]


class OutlookSettings(BaseSettings):
    """Microsoft Graph / Outlook OAuth settings."""
    client_id: str = Field(default="", alias="OUTLOOK_CLIENT_ID")
    client_secret: str = Field(default="", alias="OUTLOOK_CLIENT_SECRET")
    tenant_id: str = Field(default="", alias="OUTLOOK_TENANT_ID")

    model_config = {"env_prefix": "", "populate_by_name": True}


class TripClawSettings(BaseSettings):
    """Trip Claw LLM API settings."""
    api_url: str = Field(default="http://localhost:8080/v1", alias="TRIP_CLAW_API_URL")
    api_key: str = Field(default="", alias="TRIP_CLAW_API_KEY")
    model: str = Field(default="trip-claw-v2", alias="TRIP_CLAW_MODEL")
    max_retries: int = Field(default=3, alias="TRIP_CLAW_MAX_RETRIES")
    max_context_tokens: int = Field(default=12000, alias="TRIP_CLAW_MAX_CONTEXT_TOKENS")
    rate_limit_rpm: int = Field(default=200, alias="TRIP_CLAW_RATE_LIMIT_RPM")

    model_config = {"env_prefix": "", "populate_by_name": True}


class NudgeSettings(BaseSettings):
    """Nudge engine configuration (all thresholds)."""
    cold_threshold_days: int = Field(default=30)
    dormant_threshold_days: int = Field(default=90)
    action_item_overdue_threshold_days: int = Field(default=7)
    offer_stall_threshold_days: int = Field(default=14)
    escalation_delay_days: int = Field(default=3)
    nudge_cooldown_hours: int = Field(default=48)
    max_nudges_per_day_per_exec: int = Field(default=5)
    evaluation_interval_minutes: int = Field(default=15)

    model_config = {"env_prefix": "NUDGE_"}


class IngestionSettings(BaseSettings):
    """Email ingestion pipeline settings."""
    poll_interval_seconds: int = Field(default=86400)  # 1 day
    backfill_days: int = Field(default=90)
    max_attachment_size_mb: int = Field(default=25)
    supported_attachment_types: list[str] = Field(
        default=["pdf", "pptx", "xlsx", "docx", "png", "jpg"]
    )
    excluded_senders: list[str] = Field(default=["noreply@*", "calendar@*"])

    model_config = {"env_prefix": "INGESTION_"}


class NotificationSettings(BaseSettings):
    """Notification delivery settings."""
    lark_webhook_url: str = Field(default="", alias="LARK_WEBHOOK_URL")
    smtp_relay_host: str = Field(default="", alias="SMTP_RELAY_HOST")
    smtp_relay_port: int = Field(default=587, alias="SMTP_RELAY_PORT")

    model_config = {"env_prefix": "", "populate_by_name": True}


class Settings(BaseSettings):
    """Root settings aggregating all sub-configs."""
    app_name: str = Field(default="Trip Flights Sales Intelligence")
    environment: str = Field(default="development", alias="ENVIRONMENT")
    debug: bool = Field(default=False, alias="DEBUG")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    outlook: OutlookSettings = Field(default_factory=OutlookSettings)
    trip_claw: TripClawSettings = Field(default_factory=TripClawSettings)
    nudge: NudgeSettings = Field(default_factory=NudgeSettings)
    ingestion: IngestionSettings = Field(default_factory=IngestionSettings)
    notifications: NotificationSettings = Field(default_factory=NotificationSettings)

    model_config = {"env_prefix": "", "populate_by_name": True}


# Singleton
settings = Settings()
