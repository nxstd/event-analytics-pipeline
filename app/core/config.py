from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "event-analytics.events"
    kafka_consumer_group: str = "event-analytics-worker"
    redis_url: str = "redis://localhost:6379/0"
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_database: str = "default"
    clickhouse_username: str = "default"
    clickhouse_password: str = ""
    stats_cache_ttl_seconds: int = 30
    idempotency_ttl_seconds: int = 86400
    idempotency_lock_ttl_seconds: int = 60
    dependency_retry_attempts: int = 12
    dependency_retry_delay_seconds: float = 2.0
    api_port: int = 8000
    worker_metrics_port: int = 8001


@lru_cache
def get_settings() -> Settings:
    return Settings()
