# Metadata database store layer - target CRUD, run/target_state/results management

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import ReturnDocument

from app.db import get_db
from app.models import (
    TargetCreate,
    TargetInDB,
    TargetResult,
    TargetUpdate,
    RunSummary,
    FleetStatus,
    TargetState,
    Status,
    AlertRecord,
    AlertState,
)

# ──────────────────────────── helpers ────────────────────────────


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _oid_filter(target_id: str) -> dict:
    """Build a filter that matches either an ObjectId or a string id."""
    from bson import ObjectId
    try:
        return {"_id": ObjectId(target_id)}
    except Exception:
        return {"_id": target_id}


# ──────────────────────────── targets ────────────────────────────


async def get_targets(
    enabled_only: bool = False,
    cluster: str | None = None,
    env: str | None = None,
) -> list[TargetInDB]:
    """Target listesini getir."""
    db = get_db()
    filt: dict = {}
    if enabled_only:
        filt["enabled"] = True
    if cluster:
        filt["cluster"] = cluster
    if env:
        filt["env"] = env

    cursor = db.targets.find(filt).sort("ip", 1)
    results = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        results.append(TargetInDB(**doc))
    return results


async def get_target(target_id: str) -> TargetInDB | None:
    """Tek bir target getir."""
    db = get_db()
    doc = await db.targets.find_one(_oid_filter(target_id))
    if not doc:
        return None
    doc["_id"] = str(doc["_id"])
    return TargetInDB(**doc)


async def create_target(data: TargetCreate) -> TargetInDB:
    """Create a target."""
    db = get_db()
    now = _now()
    doc = data.model_dump()
    doc["created_at"] = now
    doc["updated_at"] = now
    result = await db.targets.insert_one(doc)
    doc["_id"] = str(result.inserted_id)
    return TargetInDB(**doc)


async def update_target(target_id: str, data: TargetUpdate) -> TargetInDB | None:
    """Update a target."""
    db = get_db()
    update_data = data.model_dump(exclude_none=True)
    if not update_data:
        return await get_target(target_id)

    update_data["updated_at"] = _now()
    doc = await db.targets.find_one_and_update(
        _oid_filter(target_id),
        {"$set": update_data},
        return_document=ReturnDocument.AFTER,
    )
    if not doc:
        return None
    doc["_id"] = str(doc["_id"])
    return TargetInDB(**doc)


async def delete_target(target_id: str) -> bool:
    """Delete a target and clean up its alerts, results and state."""
    db = get_db()
    result = await db.targets.delete_one(_oid_filter(target_id))
    if result.deleted_count == 0:
        return False

    # Cascade cleanup so no orphan alerts/results/state are left behind.
    # target_id may have been stored as either an ObjectId or a string;
    # iki formu da temizle.
    id_forms = {target_id}
    try:
        from bson import ObjectId

        id_forms.add(str(ObjectId(target_id)))
    except Exception:
        pass

    for coll_name in ("alerts", "results", "target_state"):
        coll = db[coll_name]
        await coll.delete_many({"target_id": {"$in": list(id_forms)}})

    return True


async def toggle_target(target_id: str) -> TargetInDB | None:
    """Target enable/disable toggle."""
    db = get_db()
    target = await get_target(target_id)
    if not target:
        return None
    new_enabled = not target.enabled
    doc = await db.targets.find_one_and_update(
        _oid_filter(target_id),
        {"$set": {"enabled": new_enabled, "updated_at": _now()}},
        return_document=ReturnDocument.AFTER,
    )
    if not doc:
        return None
    doc["_id"] = str(doc["_id"])
    return TargetInDB(**doc)


async def bulk_import_targets(targets: list[TargetCreate]) -> dict:
    """Bulk target import. Try insert_one for each; skip duplicates."""
    db = get_db()
    inserted = 0
    skipped = 0
    errors = 0
    now = _now()

    for t in targets:
        try:
            doc = t.model_dump()
            doc["created_at"] = now
            doc["updated_at"] = now
            await db.targets.insert_one(doc)
            inserted += 1
        except Exception as e:
            if "duplicate" in str(e).lower() or "E11000" in str(e):
                skipped += 1
            else:
                errors += 1

    return {"inserted": inserted, "skipped": skipped, "errors": errors}


# ──────────────────────────── runs ───────────────────────────────


async def create_run(
    triggered_by: str = "schedule",
    instance_id: str = "",
) -> RunSummary:
    """Create a new run."""
    db = get_db()
    run_id = str(uuid.uuid4())
    now = _now()
    run = RunSummary(
        run_id=run_id,
        started_at=now,
        triggered_by=triggered_by,
        instance_id=instance_id,
    )
    await db.runs.insert_one(run.model_dump())
    return run


async def finalize_run(run: RunSummary) -> None:
    """Finish a run and update its summary counters."""
    db = get_db()
    now = _now()
    await db.runs.update_one(
        {"run_id": run.run_id},
        {
            "$set": {
                "finished_at": now,
                "duration_ms": (now - run.started_at).total_seconds() * 1000,
                "target_count": run.target_count,
                "ok": run.ok,
                "warning": run.warning,
                "critical": run.critical,
                "unknown": run.unknown,
            }
        },
    )


async def get_last_run() -> RunSummary | None:
    """Fetch the most recent completed run."""
    db = get_db()
    doc = await db.runs.find_one(
        {"finished_at": {"$exists": True}},
        sort=[("started_at", -1)],
    )
    if not doc:
        return None
    return RunSummary(**doc)


async def get_runs(limit: int = 20) -> list[RunSummary]:
    """Fetch the last N runs."""
    db = get_db()
    cursor = db.runs.find().sort("started_at", -1).limit(limit)
    results = []
    async for doc in cursor:
        results.append(RunSummary(**doc))
    return results


async def get_run(run_id: str) -> RunSummary | None:
    """Run detail."""
    db = get_db()
    doc = await db.runs.find_one({"run_id": run_id})
    if not doc:
        return None
    return RunSummary(**doc)


async def get_run_results(run_id: str) -> list[TargetResult]:
    """Per-target results of a single run."""
    db = get_db()
    cursor = db.results.find({"run_id": run_id}).sort("ts", 1)
    results: list[TargetResult] = []
    async for doc in cursor:
        results.append(TargetResult(**doc))
    return results


# ──────────────────────────── results ────────────────────────────


async def save_results(results: list[TargetResult]) -> None:
    """Bulk-write target results."""
    if not results:
        return
    db = get_db()
    docs = [r.model_dump() for r in results]
    await db.results.insert_many(docs)


async def get_target_history(
    target_id: str,
    hours: int = 24,
    limit: int = 500,
) -> list[TargetResult]:
    """Historical results for a target."""
    db = get_db()
    since = _now().replace(microsecond=0)  # placeholder, filtered below
    cursor = db.results.find(
        {"target_id": target_id}
    ).sort("ts", -1).limit(limit)
    results = []
    cutoff = _now().timestamp() - (hours * 3600)
    async for doc in cursor:
        if doc.get("ts") and doc["ts"].timestamp() < cutoff:
            continue  # Manual filter for the case where no TTL applies
        results.append(TargetResult(**doc))
    return results


# ──────────────────────────── slow_queries ────────────────────────


async def save_slow_queries(
    target_id: str,
    hostname: str,
    ip: str,
    cluster: str | None,
    details: list[dict],
) -> int:
    """Snapshot a target's slow-query details into the metadata database."""
    if not details:
        return 0
    db = get_db()
    now = _now()
    docs = []
    for d in details:
        doc = dict(d)
        # Make datetime values serializable (ts may already be a datetime)
        ts = doc.get("ts")
        if isinstance(ts, datetime) and ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        docs.append({
            "target_id": target_id,
            "hostname": hostname,
            "ip": ip,
            "cluster": cluster,
            "captured_at": now,
            **doc,
        })
    await db.slow_queries.insert_many(docs)
    return len(docs)


async def get_slow_queries(
    target_id: str | None = None,
    hostname: str | None = None,
    limit: int = 100,
    min_ms: int = 0,
) -> list[dict]:
    """Fetch snapshotted slow queries (slowest first)."""
    db = get_db()
    filt: dict = {}
    if target_id:
        filt["target_id"] = target_id
    if hostname:
        filt["hostname"] = hostname
    if min_ms:
        filt["millis"] = {"$gte": min_ms}
    cursor = db.slow_queries.find(filt).sort("millis", -1).limit(limit)
    results = []
    async for doc in cursor:
        doc.pop("_id", None)
        results.append(doc)
    return results


async def get_slow_query_summary(limit: int = 20) -> list[dict]:
    """Slow-query summary per host (total queries, max ms, distinct namespaces)."""
    db = get_db()
    pipeline = [
        {"$sort": {"millis": -1}},
        {"$group": {
            "_id": {"hostname": "$hostname", "ip": "$ip", "cluster": "$cluster"},
            "count": {"$sum": 1},
            "max_ms": {"$max": "$millis"},
            "last_seen": {"$max": "$captured_at"},
            "namespaces": {"$addToSet": "$ns"},
        }},
        {"$sort": {"max_ms": -1}},
        {"$limit": limit},
    ]
    results = []
    async for doc in db.slow_queries.aggregate(pipeline):
        g = doc["_id"]
        results.append({
            "hostname": g.get("hostname", "?"),
            "ip": g.get("ip", "?"),
            "cluster": g.get("cluster", "-"),
            "count": doc["count"],
            "max_ms": doc["max_ms"],
            "last_seen": doc["last_seen"],
            "namespaces": list(doc.get("namespaces", []))[:8],
        })
    return results


async def get_all_slow_query_docs(limit: int = 0) -> list[dict]:
    """Fetch every snapshot (raw dicts) for the index suggestion engine.

    limit=0 -> all records. The suggestion engine must see every namespace; a
    low-millisecond query doing a COLLSCAN is still worth an index suggestion.
    """
    db = get_db()
    cursor = db.slow_queries.find({}, {
        "ns": 1, "coll": 1, "op": 1, "millis": 1, "plan_summary": 1,
        "docs_examined": 1, "keys_examined": 1, "nreturned": 1,
        "filter_keys": 1, "sort": 1, "hostname": 1, "command": 1, "_id": 0,
    }).sort("millis", -1)
    if limit:
        cursor = cursor.limit(limit)
    results = []
    async for doc in cursor:
        results.append(doc)
    return results


# ──────────────────────────── target_state ───────────────────────


async def upsert_target_state(
    target_id: str,
    status: Status,
    error: str | None = None,
) -> TargetState:
    """Upsert target state. On success consecutive_failures is reset."""
    db = get_db()
    now = _now()

    current = await db.target_state.find_one({"target_id": target_id})

    if current:
        prev_status = current.get("status", "unknown")
        is_ok = status == Status.OK

        # Compute the consecutive counters in Python and write them with a single $set.
        # Using the same field in both $inc and $set makes MongoDB raise
        # "would create a conflict at ..." (code 40).
        new_failures = 0 if is_ok else current.get("consecutive_failures", 0) + 1
        new_ok = current.get("consecutive_ok", 0) + 1 if is_ok else 0

        update: dict = {
            "$set": {
                "status": status.value,
                "last_run_at": now,
                "last_error": error,
                "consecutive_failures": new_failures,
                "consecutive_ok": new_ok,
            },
        }
        if is_ok:
            update["$set"]["last_ok_at"] = now
        if prev_status != status.value:
            update["$set"]["last_change_at"] = now

        await db.target_state.update_one({"target_id": target_id}, update)
    else:
        ts = TargetState(
            target_id=target_id,
            status=status,
            consecutive_failures=0 if status == Status.OK else 1,
            consecutive_ok=1 if status == Status.OK else 0,
            last_ok_at=now if status == Status.OK else None,
            last_run_at=now,
            last_change_at=now,
            last_error=error,
        )
        await db.target_state.insert_one(ts.model_dump())

    # Return the current document
    doc = await db.target_state.find_one({"target_id": target_id})
    return TargetState(**doc)


async def get_target_state(target_id: str) -> TargetState | None:
    """Fetch a target's latest state."""
    db = get_db()
    doc = await db.target_state.find_one({"target_id": target_id})
    if not doc:
        return None
    return TargetState(**doc)


async def get_fleet_status() -> FleetStatus:
    """Overall fleet status summary."""
    db = get_db()

    # Target counts
    enabled_count = await db.targets.count_documents({"enabled": True})
    total_count = await db.targets.count_documents({})

    # Status distribution from target states
    pipeline = [
        {"$group": {"_id": "$status", "count": {"$sum": 1}}},
    ]
    status_counts: dict[str, int] = {}
    async for doc in db.target_state.aggregate(pipeline):
        status_counts[doc["_id"]] = doc["count"]

    # Distribution by cluster and environment
    clusters: dict[str, dict[str, int]] = {}
    envs: dict[str, dict[str, int]] = {}
    target_statuses: dict[str, str] = {}

    # target_state.target_id is stored as a string while targets._id is an ObjectId.
    # Cast _id to a string so the $lookup matches (bug fix).
    pipeline2 = [
        {"$match": {"enabled": True}},
        {"$addFields": {"_id_str": {"$toString": "$_id"}}},
        {
            "$lookup": {
                "from": "target_state",
                "localField": "_id_str",
                "foreignField": "target_id",
                "as": "state",
            }
        },
        {"$unwind": {"path": "$state", "preserveNullAndEmptyArrays": True}},
    ]
    async for doc in db.targets.aggregate(pipeline2):
        target_id = str(doc["_id"])
        cluster = doc.get("cluster") or "unknown"
        env = doc.get("env") or "unknown"
        status = doc.get("state", {}).get("status", "unknown")
        target_statuses[target_id] = status

        clusters.setdefault(cluster, {"ok": 0, "warning": 0, "critical": 0, "unknown": 0})
        envs.setdefault(env, {"ok": 0, "warning": 0, "critical": 0, "unknown": 0})
        clusters[cluster][status] = clusters[cluster].get(status, 0) + 1
        envs[env][status] = envs[env].get(status, 0) + 1

    last_run = await get_last_run()

    return FleetStatus(
        ok=status_counts.get("ok", 0),
        warning=status_counts.get("warning", 0),
        critical=status_counts.get("critical", 0),
        unknown=status_counts.get("unknown", 0),
        total=enabled_count,
        last_run_at=last_run.started_at if last_run else None,
        last_run_id=last_run.run_id if last_run else None,
        last_duration_ms=last_run.duration_ms if last_run else 0.0,
        clusters=clusters,
        envs=envs,
        target_statuses=target_statuses,
    )


# ──────────────────────────── alerts ─────────────────────────────


async def get_alert(fingerprint: str) -> AlertRecord | None:
    """Fetch an alert by fingerprint."""
    db = get_db()
    doc = await db.alerts.find_one({"fingerprint": fingerprint})
    if not doc:
        return None
    return AlertRecord(**doc)


async def upsert_alert(alert: AlertRecord) -> AlertRecord:
    """Upsert an alert (keyed by fingerprint)."""
    db = get_db()
    now = _now()

    existing = await get_alert(alert.fingerprint)
    if existing:
        update = alert.model_dump()
        # Only update the current fields; preserve first_seen
        update["first_seen_at"] = existing.first_seen_at or now
        await db.alerts.replace_one(
            {"fingerprint": alert.fingerprint},
            update,
        )
    else:
        alert.first_seen_at = now
        await db.alerts.insert_one(alert.model_dump())

    return await get_alert(alert.fingerprint)  # type: ignore[return-value]


async def get_active_alerts() -> list[AlertRecord]:
    """Aktif (pending + firing) alert'leri getir."""
    db = get_db()
    cursor = db.alerts.find(
        {"state": {"$in": ["pending", "firing"]}}
    ).sort("severity", -1)
    results = []
    async for doc in cursor:
        results.append(AlertRecord(**doc))
    return results


async def get_resolved_alerts(limit: int = 50) -> list[AlertRecord]:
    """Resolved alert'leri getir."""
    db = get_db()
    cursor = db.alerts.find({"state": "resolved"}).sort("resolved_at", -1).limit(limit)
    results = []
    async for doc in cursor:
        results.append(AlertRecord(**doc))
    return results


async def silence_alert(fingerprint: str, minutes: int = 60) -> bool:
    """Temporarily silence an alert (mark it resolved)."""
    db = get_db()
    now = _now()
    result = await db.alerts.update_one(
        {"fingerprint": fingerprint},
        {
            "$set": {
                "state": "resolved",
                "resolved_at": now,
            }
        },
    )
    return result.modified_count > 0


async def silence_alerts_bulk(fingerprints: list[str]) -> int:
    """Silence several alerts at once."""
    db = get_db()
    now = _now()
    result = await db.alerts.update_many(
        {"fingerprint": {"$in": fingerprints}},
        {
            "$set": {
                "state": "resolved",
                "resolved_at": now,
            }
        },
    )
    return result.modified_count
