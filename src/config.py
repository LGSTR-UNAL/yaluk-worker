"""Worker settings, read from environment variables.

Required variables have no default: the worker refuses to start without them.
"""

import os
from dataclasses import dataclass


class MissingSettings(Exception):
    pass


def _required(name: str, missing: list[str]) -> str:
    value = os.getenv(name, "")
    if not value:
        missing.append(name)
    return value


def _optional(name: str, default: str) -> str:
    return os.getenv(name) or default


@dataclass(frozen=True)
class Settings:
    # Kafka
    kafka_host: str
    consumer_group_id: str
    request_topic: str
    results_topic: str
    kafka_auto_offset_reset: str
    max_poll_interval_ms: int
    producer_flush_timeout: float

    # S3
    s3_endpoint_url: str | None
    s3_access_key_id: str | None
    s3_secret_access_key: str | None
    requests_bucket: str
    resources_bucket: str
    results_bucket: str
    surge_arresters_prefix: str

    # PostgreSQL
    postgres_host: str
    postgres_port: str
    postgres_db: str
    postgres_user: str
    postgres_password: str
    postgres_pool_max_size: int
    postgres_pool_timeout: float
    simulation_table: str

    # ATP / YALUK
    tpbig_path: str
    yaluk_path: str
    work_dir: str

    log_level: str

    @classmethod
    def from_env(cls) -> "Settings":
        missing: list[str] = []
        settings = cls(
            kafka_host=_required("KAFKA_HOST", missing),
            consumer_group_id=_required("CONSUMER_GROUP_ID", missing),
            request_topic=_required("REQUEST_TOPIC", missing),
            results_topic=_required("RESULTS_TOPIC", missing),
            kafka_auto_offset_reset=_optional("KAFKA_AUTO_OFFSET_RESET", "earliest"),
            max_poll_interval_ms=int(_optional("MAX_POLL_INTERVAL_MS", "3600000")),
            producer_flush_timeout=float(_optional("PRODUCER_FLUSH_TIMEOUT", "60")),
            s3_endpoint_url=os.getenv("S3_ENDPOINT_URL") or None,
            s3_access_key_id=os.getenv("S3_ACCESS_KEY_ID") or None,
            s3_secret_access_key=os.getenv("S3_SECRET_ACCESS_KEY") or None,
            requests_bucket=_optional("S3_REQUESTS_BUCKET", "yaluk-requests"),
            resources_bucket=_optional("S3_RESOURCES_BUCKET", "yaluk-resources"),
            results_bucket=_optional("S3_RESULTS_BUCKET", "yaluk-results"),
            surge_arresters_prefix=_optional(
                "S3_SURGE_ARRESTERS_PREFIX", "surge-arresters"
            ).strip("/"),
            postgres_host=_required("POSTGRES_HOST", missing),
            postgres_port=_optional("POSTGRES_PORT", "5432"),
            postgres_db=_required("POSTGRES_DB", missing),
            postgres_user=_required("POSTGRES_USER", missing),
            postgres_password=_required("POSTGRES_PASSWORD", missing),
            postgres_pool_max_size=int(_optional("POSTGRES_POOL_MAX_SIZE", "2")),
            postgres_pool_timeout=float(_optional("POSTGRES_POOL_TIMEOUT", "30")),
            simulation_table=_optional("SIMULATION_TABLE", "streaming.simulation"),
            tpbig_path=_required("TPBIG_PATH", missing),
            yaluk_path=_required("YALUK_PATH", missing),
            work_dir=_optional("WORK_DIR", "/tmp"),
            log_level=_optional("LOG_LEVEL", "INFO").upper(),
        )
        if missing:
            raise MissingSettings(
                "Missing required environment variables: " + ", ".join(missing)
            )
        return settings
