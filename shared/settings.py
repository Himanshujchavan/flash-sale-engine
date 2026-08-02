"""
Centralized configuration. Each service imports `get_settings()` and reads
what it needs. Values come from environment variables, with sane local
defaults for Windows dev (native Postgres, dockerized Redis/RabbitMQ).

Each service still points at its OWN database (order_db, inventory_db,
payment_db, saga_db) -- this is intentional. In this architecture every
service owns its own data; nothing shares a database or a table with
another service. Only events cross service boundaries.
"""
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # ---- Postgres (native Windows install, default port 5432) ----
    postgres_user: str = "postgres"
    postgres_password: str = "postgres"
    postgres_host: str = "localhost"
    postgres_port: int = 5432

    order_db_name: str = "order_db"
    inventory_db_name: str = "inventory_db"
    payment_db_name: str = "payment_db"
    saga_db_name: str = "saga_db"
    notification_db_name: str = "notification_db"

    # ---- RabbitMQ (docker-compose, default port 5672) ----
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    events_exchange: str = "flash_sale_events"   # topic exchange, all events published here

    # ---- Redis (docker-compose, default port 6379) ----
    redis_url: str = "redis://localhost:6379/0"

    # ---- Seq (docker-compose, default port 5341->80) ----
    seq_url: str = "http://localhost:5341"

    # ---- Rate limiter ----
    rate_limit_capacity: int = 50       # max tokens (burst size) per SKU bucket
    rate_limit_refill_per_sec: float = 20.0  # tokens added per second per SKU

    # ---- Outbox dispatcher ----
    outbox_poll_interval_sec: float = 0.5
    outbox_batch_size: int = 50

    def db_url(self, db_name: str) -> str:
        """Build an asyncpg SQLAlchemy URL for a given service's database."""
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{db_name}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
