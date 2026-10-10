# Metadata MongoDB connection management and index creation

from __future__ import annotations

import logging

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from app.config import settings

logger = logging.getLogger(__name__)

_client: AsyncIOMotorClient | None = None
_db: AsyncIOMotorDatabase | None = None


async def connect() -> AsyncIOMotorDatabase:
    """Connect to the metadata MongoDB and ensure every index exists."""
    global _client, _db

    uri = settings.meta_mongo_uri.get_secret_value()
    _client = AsyncIOMotorClient(
        uri,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
        # Secret bilgiyi log'a basma
        appname="mongodb-healthchecker",
    )
    _db = _client[settings.meta_mongo_db]

    await ensure_indexes(_db)
    logger.info("Metadata MongoDB connected: %s", settings.meta_mongo_db)
    return _db


async def disconnect() -> None:
    """Close the MongoDB connection."""
    global _client, _db
    if _client:
        _client.close()
        _client = None
        _db = None
    logger.info("Metadata MongoDB connection closed")


def get_db() -> AsyncIOMotorDatabase:
    """Return the active database handle."""
    if _db is None:
        raise RuntimeError("Not connected to the database. Call connect() first.")
    return _db


async def ensure_indexes(db: AsyncIOMotorDatabase) -> None:
    """Create every collection index and configure TTLs."""

    # ── targets ────────────────────────────────────────────
    # Unique compound index: (ip, port)
    try:
        await db.targets.create_index(
            [("ip", 1), ("port", 1)],
            unique=True,
            name="idx_targets_ip_port",
        )
    except Exception:
        pass  # May fail when the index already exists

    await db.targets.create_index("enabled", name="idx_targets_enabled")
    await db.targets.create_index("cluster", name="idx_targets_cluster")
    await db.targets.create_index("env", name="idx_targets_env")

    # ── runs ───────────────────────────────────────────────
    await db.runs.create_index("run_id", unique=True, name="idx_runs_run_id")
    await db.runs.create_index("started_at", name="idx_runs_started_at")

    # TTL index: auto-delete after 30 days
    try:
        await db.runs.create_index(
            "finished_at",
            expireAfterSeconds=30 * 24 * 3600,
            name="idx_runs_ttl",
        )
    except Exception:
        await db.command(
            {
                "collMod": "runs",
                "index": {
                    "name": "idx_runs_ttl",
                    "expireAfterSeconds": 30 * 24 * 3600,
                },
            }
        )

    # ── results ────────────────────────────────────────────
    await db.results.create_index(
        [("target_id", 1), ("ts", -1)],
        name="idx_results_target_ts",
    )
    await db.results.create_index("run_id", name="idx_results_run_id")

    # TTL index: 14 days
    try:
        await db.results.create_index(
            "ts",
            expireAfterSeconds=14 * 24 * 3600,
            name="idx_results_ttl",
        )
    except Exception:
        await db.command(
            {
                "collMod": "results",
                "index": {
                    "name": "idx_results_ttl",
                    "expireAfterSeconds": 14 * 24 * 3600,
                },
            }
        )

    # ── target_state ───────────────────────────────────────
    await db.target_state.create_index(
        "target_id",
        unique=True,
        name="idx_target_state_target_id",
    )

    # ── alerts ─────────────────────────────────────────────
    await db.alerts.create_index(
        "fingerprint",
        unique=True,
        name="idx_alerts_fingerprint",
    )
    await db.alerts.create_index(
        [("state", 1), ("severity", 1)],
        name="idx_alerts_state_severity",
    )

    # Partial TTL index: resolved alerts are deleted after 90 days
    try:
        await db.alerts.create_index(
            "resolved_at",
            expireAfterSeconds=90 * 24 * 3600,
            name="idx_alerts_resolved_ttl",
            partialFilterExpression={"state": "resolved"},
        )
    except Exception:
        await db.command(
            {
                "collMod": "alerts",
                "index": {
                    "name": "idx_alerts_resolved_ttl",
                    "expireAfterSeconds": 90 * 24 * 3600,
                },
            }
        )

    # ── locks ──────────────────────────────────────────────
    await db.locks.create_index(
        "name",
        unique=True,
        name="idx_locks_name",
    )
    # TTL index: expireAfterSeconds=0 -> deleted as soon as expires_at passes
    try:
        await db.locks.create_index(
            "expires_at",
            expireAfterSeconds=0,
            name="idx_locks_ttl",
        )
    except Exception:
        await db.command(
            {
                "collMod": "locks",
                "index": {
                    "name": "idx_locks_ttl",
                    "expireAfterSeconds": 0,
                },
            }
        )

    # ── slow_queries ─────────────────────────────────────────
    # Snapshotted slow-query details. Compound (target_id, ts) index + TTL.
    await db.slow_queries.create_index(
        [("target_id", 1), ("ts", -1)],
        name="idx_slow_queries_target_ts",
    )
    await db.slow_queries.create_index(
        [("hostname", 1), ("ts", -1)],
        name="idx_slow_queries_host_ts",
    )
    try:
        await db.slow_queries.create_index(
            "ts",
            expireAfterSeconds=7 * 24 * 3600,
            name="idx_slow_queries_ttl",
        )
    except Exception:
        await db.command(
            {
                "collMod": "slow_queries",
                "index": {
                    "name": "idx_slow_queries_ttl",
                    "expireAfterSeconds": 7 * 24 * 3600,
                },
            }
        )

    logger.info("All indexes ensured")
