from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="APP_")

    database_url: str = "postgresql://app:app@localhost:5432/app"
    kafka_bootstrap_servers: str = "localhost:9092"
    relay_poll_interval_s: float = 0.5
    relay_batch_size: int = 100
    relay_backoff_initial_s: float = 1.0
    relay_backoff_max_s: float = 30.0
