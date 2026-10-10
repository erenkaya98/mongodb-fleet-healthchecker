# Uygulama sabitleri ve tipleri

from __future__ import annotations

import enum
from datetime import datetime
from typing import Any

import pydantic


class Status(str, enum.Enum):
    """Health check severity levels. Order: OK < WARNING < UNKNOWN < CRITICAL"""

    OK = "ok"
    WARNING = "warning"
    UNKNOWN = "unknown"
    CRITICAL = "critical"

    @property
    def severity(self) -> int:
        """Numeric weight (higher = worse)."""
        order = {Status.OK: 0, Status.WARNING: 1, Status.UNKNOWN: 2, Status.CRITICAL: 3}
        return order[self]

    @classmethod
    def worst(cls, *statuses: Status) -> Status:
        """Return the worst status."""
        return max(statuses, key=lambda s: s.severity)


class AlertState(str, enum.Enum):
    PENDING = "pending"
    FIRING = "firing"
    RESOLVED = "resolved"


# ── Pydantic modeller ───────────────────────────────────────────


class TargetInDB(pydantic.BaseModel):
    """A document in the targets collection."""

    id: str | None = pydantic.Field(default=None, alias="_id")
    ip: str
    port: int = 9216  # exporter port
    mongo_port: int = 27017
    hostname: str | None = None
    cluster: str | None = None
    role: str | None = None  # configsvr, mongos, arbiter, shardsvr, default
    env: str | None = None
    enabled: bool = True
    exporter_url: str | None = None  # override
    mongo_uri: str | None = None  # direct mongo connection (exporter-less mode)
    check_profile: str = "default"
    labels: dict[str, str] = pydantic.Field(default_factory=dict)
    notes: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    model_config = pydantic.ConfigDict(populate_by_name=True)

    @property
    def collection_mode(self) -> str:
        """Either 'exporter' or 'direct': which collector to use."""
        return "direct" if self.mongo_uri else "exporter"


class TargetCreate(pydantic.BaseModel):
    """Payload for creating a target."""

    ip: str
    port: int = 9216
    mongo_port: int = 27017
    hostname: str | None = None
    cluster: str | None = None
    role: str | None = None
    env: str | None = None
    enabled: bool = True
    exporter_url: str | None = None
    mongo_uri: str | None = None  # direct mongo connection (exporter-less mode)
    check_profile: str = "default"
    labels: dict[str, str] = pydantic.Field(default_factory=dict)
    notes: str | None = None


class TargetUpdate(pydantic.BaseModel):
    """Payload for updating a target (all fields optional)."""

    ip: str | None = None
    port: int | None = None
    mongo_port: int | None = None
    hostname: str | None = None
    cluster: str | None = None
    role: str | None = None
    env: str | None = None
    enabled: bool | None = None
    exporter_url: str | None = None
    mongo_uri: str | None = None
    check_profile: str | None = None
    labels: dict[str, str] | None = None
    notes: str | None = None


class Sample(pydantic.BaseModel):
    """A single parsed Prometheus metric sample."""

    name: str
    labels: dict[str, str]
    value: float

    model_config = pydantic.ConfigDict(frozen=True)


class CheckResult(pydantic.BaseModel):
    """Evaluation result of one rule."""

    rule_id: str
    rule_title: str
    status: Status
    measured_value: float | None = None
    warning_threshold: float | None = None
    critical_threshold: float | None = None
    message: str = ""


class ScrapeResult(pydantic.BaseModel):
    """Scrape result for a single target."""

    target_id: str
    url: str
    ok: bool
    http_status: int | None = None
    latency_ms: float = 0.0
    error: str | None = None
    samples: list[Sample] = pydantic.Field(default_factory=list)
    attempts: int = 1
    slow_queries: list[dict[str, Any]] = pydantic.Field(default_factory=list)


class TargetResult(pydantic.BaseModel):
    """Full per-target outcome of a run (scrape + rule evaluation)."""

    run_id: str
    target_id: str
    ts: datetime
    status: Status
    latency_ms: float = 0.0
    http_status: int | None = None
    scrape_ok: bool = False
    error: str | None = None
    checks: list[CheckResult] = pydantic.Field(default_factory=list)
    metrics: dict[str, float] = pydantic.Field(default_factory=dict)


class RunSummary(pydantic.BaseModel):
    """Summary of one run."""

    run_id: str
    started_at: datetime
    finished_at: datetime | None = None
    duration_ms: float = 0.0
    target_count: int = 0
    ok: int = 0
    warning: int = 0
    critical: int = 0
    unknown: int = 0
    triggered_by: str = "schedule"
    instance_id: str = ""


class FleetStatus(pydantic.BaseModel):
    """Fleet status summary for the dashboard."""

    ok: int = 0
    warning: int = 0
    critical: int = 0
    unknown: int = 0
    total: int = 0
    last_run_at: datetime | None = None
    last_run_id: str | None = None
    last_duration_ms: float = 0.0
    clusters: dict[str, dict[str, int]] = pydantic.Field(default_factory=dict)
    envs: dict[str, dict[str, int]] = pydantic.Field(default_factory=dict)
    # target_id (str) -> status; used by the dashboard targets table
    target_statuses: dict[str, str] = pydantic.Field(default_factory=dict)


class AlertRecord(pydantic.BaseModel):
    """A document in the alerts collection."""

    fingerprint: str
    target_id: str
    rule_id: str
    severity: Status
    state: AlertState = AlertState.PENDING
    first_seen_at: datetime | None = None
    last_seen_at: datetime | None = None
    last_sent_at: datetime | None = None
    send_count: int = 0
    resolved_at: datetime | None = None
    payload: dict[str, Any] = pydantic.Field(default_factory=dict)
    last_send_error: str | None = None


class TargetState(pydantic.BaseModel):
    """Current state of a single target."""

    target_id: str
    status: Status = Status.UNKNOWN
    consecutive_failures: int = 0
    consecutive_ok: int = 0
    last_ok_at: datetime | None = None
    last_run_at: datetime | None = None
    last_change_at: datetime | None = None
    last_error: str | None = None
