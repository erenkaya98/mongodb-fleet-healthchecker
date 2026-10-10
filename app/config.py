# MongoDB Fleet Healthchecker
# =============================
# Every setting is read from an HC_-prefixed environment variable.
# The .env file is loaded automatically (pydantic-settings).

from __future__ import annotations

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration. All values come from HC_-prefixed env vars."""

    model_config = SettingsConfigDict(
        env_prefix="HC_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Meta MongoDB ──────────────────────────────────────────
    meta_mongo_uri: SecretStr = Field(
        default=SecretStr("mongodb://localhost:27017/healthchecker"),
        description="Metadata MongoDB connection string",
    )
    meta_mongo_db: str = Field(
        default="healthchecker",
        description="Metadata database name",
    )

    # ── Scraper ───────────────────────────────────────────────
    scrape_concurrency: int = Field(
        default=10,
        ge=1,
        le=100,
        description="Number of concurrent scrapes (Semaphore + connector limit)",
    )
    scrape_timeout_total: int = Field(
        default=30,
        description="Total HTTP timeout per target (seconds)",
    )
    scrape_timeout_connect: int = Field(
        default=10,
        description="Connection timeout per target (seconds)",
    )
    scrape_retry_count: int = Field(
        default=1,
        ge=0,
        le=5,
        description="Retry count for connection/timeout errors",
    )
    scrape_retry_backoff: float = Field(
        default=1.0,
        description="Retry backoff multiplier (exponential)",
    )
    scrape_scheme: str = Field(
        default="http",
        description="Scheme used to reach the exporter (http/https)",
    )
    scrape_path: str = Field(
        default="/metrics",
        description="Exporter metrics endpoint path",
    )
    exporter_basic_auth_user: str | None = Field(
        default=None,
        description="Exporter basic auth username",
    )
    exporter_basic_auth_password: SecretStr | None = Field(
        default=None,
        description="Exporter basic auth password",
    )
    exporter_tls_verify: bool = Field(
        default=True,
        description="Verify TLS certificates",
    )

    # ── Direct MongoDB Connection ──────────────────────────────
    direct_connect_timeout: int = Field(
        default=10,
        ge=1,
        le=60,
        description="Direct MongoDB connection timeout (seconds)",
    )
    direct_server_selection_timeout: int = Field(
        default=5,
        ge=1,
        le=30,
        description="MongoDB server selection timeout (saniye)",
    )

    # ── Slow Query Profiler ────────────────────────────────────
    slow_query_threshold_ms: int = Field(
        default=100,
        ge=1,
        description="Minimum duration counted as slow in system.profile (ms)",
    )
    slow_query_window_seconds: int = Field(
        default=300,
        ge=30,
        description="Look-back window inspected in system.profile (seconds)",
    )
    slow_query_snapshot_min_ms: int = Field(
        default=1000,
        ge=1,
        description="Minimum duration for a full query snapshot (ms); shorter ones are only counted",
    )

    # ── Scheduler ─────────────────────────────────────────────
    scheduler_cron: str = Field(
        default="*/5",
        description="Cron expression (minute field only; e.g. */5 = every 5 minutes)",
    )
    scheduler_lock_ttl: int = Field(
        default=120,
        description="Leader lock TTL (seconds); must exceed the cron interval",
    )

    # ── UI / Display ───────────────────────────────────────────
    display_timezone: str = Field(
        default="UTC",
        description="UI timezone (IANA zoneinfo name, e.g. Europe/Istanbul)",
    )

    # ── Rules ─────────────────────────────────────────────────
    rules_file: str = Field(
        default="rules.yml",
        description="Path to the rule definition file",
    )

    # ── Alerting ──────────────────────────────────────────────
    webhook_url: SecretStr | None = Field(
        default=None,
        description="Webhook URL'si (Slack/Teams/genel)",
    )
    webhook_format: str = Field(
        default="generic",
        description="Webhook payload format: slack, teams, generic",
    )
    alert_for_consecutive: int = Field(
        default=2,
        ge=1,
        description="Consecutive bad results required before an alert fires",
    )
    alert_renotify_minutes: int = Field(
        default=60,
        ge=0,
        description="Renotify interval while firing, in minutes (0 = every run)",
    )
    alert_min_severity: str = Field(
        default="warning",
        description="Webhook'a gidecek minimum severity: ok, warning, critical",
    )
    alert_send_resolve: bool = Field(
        default=True,
        description="Send a resolve notification when the condition clears",
    )
    alert_storm_threshold: int = Field(
        default=5,
        description="Group the message when this many targets fail the same rule",
    )
    dashboard_base_url: str = Field(
        default="http://localhost:8000",
        description="Dashboard base URL (used for deep links in alert messages)",
    )

    # ── API / Server ──────────────────────────────────────────
    api_host: str = Field(default="0.0.0.0", description="API bind adresi")
    api_port: int = Field(default=8000, ge=1, le=65535, description="API port")
    api_reload: bool = Field(default=False, description="Uvicorn auto-reload")

    # ── Logging ───────────────────────────────────────────────
    log_level: str = Field(default="INFO", description="Log seviyesi")
    log_json: bool = Field(
        default=False,
        description="Emit logs as JSON (structured logging)",
    )


# Singleton
settings = Settings()
