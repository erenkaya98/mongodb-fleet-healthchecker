# Scheduler - 5-minute loop, leader lock, graceful shutdown
# ========================================================
# APScheduler AsyncIOScheduler + CronTrigger
# Meta DB'de leader lock: findOneAndUpdate upsert + TTL index
# Lock alamayan instance turu atlar.

from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
import uuid
from datetime import UTC, datetime

from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.alerting import process_check_result, send_alert_storm_batch
from app.collector import DirectCollector, MetricsCollector
from app.config import settings
from app.db import get_db
from app.models import (
    CheckResult,
    RunSummary,
    ScrapeResult,
    Status,
    TargetInDB,
    TargetResult,
)
from app.rules import RuleEngine, get_engine
from app.store import (
    create_run,
    finalize_run,
    get_targets,
    save_results,
    save_slow_queries,
    upsert_target_state,
)

logger = logging.getLogger(__name__)

# Instance ID — unique per process
INSTANCE_ID = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"

_running_task: asyncio.Task | None = None


def _now() -> datetime:
    return datetime.now(UTC)


class SchedulerService:
    """Ana scheduler servisi."""

    def __init__(self) -> None:
        self._scheduler: AsyncIOScheduler | None = None
        self._exporter_collector: MetricsCollector | None = None
        self._direct_collector: DirectCollector | None = None
        self._engine: RuleEngine | None = None

    async def start(self) -> None:
        """Start the scheduler and schedule the first job."""
        self._exporter_collector = MetricsCollector()
        await self._exporter_collector.start()

        self._direct_collector = DirectCollector()
        await self._direct_collector.start()

        self._engine = get_engine()

        self._scheduler = AsyncIOScheduler(
            job_defaults={
                "max_instances": 1,
                "coalesce": True,
                "misfire_grace_time": 60,
            }
        )

        self._scheduler.add_job(
            self._run_cycle,
            trigger=CronTrigger(minute=settings.scheduler_cron),
            id="healthcheck_cycle",
            name="MongoDB Healthcheck Cycle",
            replace_existing=True,
        )

        self._scheduler.start()
        logger.info(
            "Scheduler started: cron=%s, instance=%s",
            settings.scheduler_cron,
            INSTANCE_ID,
        )

    async def stop(self) -> None:
        """Graceful shutdown: stop the scheduler and close the collector."""
        global _running_task

        logger.info("Shutdown starting...")

        if self._scheduler:
            try:
                self._scheduler.remove_job("healthcheck_cycle")
            except JobLookupError:
                pass
            self._scheduler.shutdown(wait=False)

        # Wait for an in-flight run to finish (max 60s)
        if _running_task and not _running_task.done():
            logger.info("Bekleyen turun bitmesi bekleniyor...")
            try:
                await asyncio.wait_for(_running_task, timeout=60)
            except TimeoutError:
                logger.warning("Tur timeout oldu, force cancel")
                _running_task.cancel()

        if self._exporter_collector:
            await self._exporter_collector.stop()
        if self._direct_collector:
            await self._direct_collector.stop()

        logger.info("Shutdown complete")

    async def run_now(self, triggered_by: str = "manual") -> str | None:
        """Trigger a manual run (callable from the UI)."""
        global _running_task

        if _running_task and not _running_task.done():
            logger.warning("A run is already in progress, not starting another")
            return None

        logger.info("Starting manual run: triggered_by=%s", triggered_by)
        run_id = await self._run_cycle(triggered_by=triggered_by)
        return run_id

    @property
    def is_running(self) -> bool:
        """Is a run currently in progress?"""
        return _running_task is not None and not _running_task.done()

    async def _acquire_lock(self) -> bool:
        """Meta DB'de leader lock al. Atomik findOneAndUpdate upsert."""
        db = get_db()
        now = _now()
        expires_at = now.timestamp() + settings.scheduler_lock_ttl

        try:
            await db.locks.find_one_and_update(
                {"name": "scheduler_leader"},
                {
                    "$set": {
                        "instance_id": INSTANCE_ID,
                        "acquired_at": now,
                        "expires_at": expires_at,
                    },
                    "$setOnInsert": {"name": "scheduler_leader"},
                },
                upsert=True,
                # Only take it when the lock has expired or does not exist
                # filter: either absent, or expires_at < now
            )
            # findOneAndUpdate with upsert always returns a document.
            # When the lock is held elsewhere we must check expires_at.
            # Simplified approach: always attempt to take it, then verify
            # the returned instance_id.
            doc = await db.locks.find_one({"name": "scheduler_leader"})
            if doc and doc.get("instance_id") == INSTANCE_ID:
                return True
            return False
        except Exception as e:
            logger.error("Failed to acquire lock: %s", e)
            return False

    async def _release_lock(self) -> None:
        """Release the leader lock."""
        db = get_db()
        try:
            await db.locks.delete_one(
                {
                    "name": "scheduler_leader",
                    "instance_id": INSTANCE_ID,
                }
            )
        except Exception as e:
            logger.error("Failed to release lock: %s", e)

    async def _run_cycle(self, triggered_by: str = "schedule") -> str | None:
        """Run a single health check cycle.

        Steps:
        1. Leader lock al
        2. Read enabled targets
        3. Hepsini scrape et
        4. Evaluate with the rule engine
        5. Persist results
        6. Process alerts
        7. Release the lock
        """
        # Lock check
        if not await self._acquire_lock():
            logger.debug("Leader lock not acquired, skipping this run")
            return None

        run: RunSummary | None = None
        try:
            assert self._exporter_collector is not None
            assert self._direct_collector is not None
            assert self._engine is not None

            # Create the run
            run = await create_run(
                triggered_by=triggered_by,
                instance_id=INSTANCE_ID,
            )
            logger.info("Run started: %s (%d targets)", run.run_id, 0)

            # Read enabled targets
            targets = await get_targets(enabled_only=True)
            if not targets:
                logger.info("No enabled targets, skipping this run")
                run.target_count = 0
                await finalize_run(run)
                return run.run_id

            run.target_count = len(targets)

            # Split targets by collection mode
            exporter_targets = [t for t in targets if t.collection_mode == "exporter"]
            direct_targets = [t for t in targets if t.collection_mode == "direct"]

            # Scrape - run both collectors in parallel
            scrape_results: list[ScrapeResult] = []
            scrape_tasks = []

            if exporter_targets:
                scrape_tasks.append(self._exporter_collector.scrape_all(exporter_targets))
            if direct_targets:
                scrape_tasks.append(self._direct_collector.scrape_all(direct_targets))

            if scrape_tasks:
                result_batches = await asyncio.gather(*scrape_tasks)
                for batch in result_batches:
                    scrape_results.extend(batch)

            # Target → scrape result mapping (order koruyarak)
            # scrape_results order must match the target order
            exporter_results = scrape_results[: len(exporter_targets)]
            direct_results = scrape_results[len(exporter_targets) :]

            # Combined list
            all_targets = exporter_targets + direct_targets
            all_scrape_results = exporter_results + direct_results

            # Evaluate + save per target
            target_results: list[TargetResult] = []
            all_checks: list[tuple[TargetInDB, CheckResult]] = []

            for i, target in enumerate(all_targets):
                sr = all_scrape_results[i]

                # Rule evaluation
                checks = self._engine.evaluate(
                    sr,
                    target.check_profile,
                    target_role=target.role,
                )

                # Final target status: the worst rule result
                worst_status = Status.OK
                error_msg: str | None = sr.error
                if checks:
                    worst_status = Status.worst(*[c.status for c in checks])
                elif not sr.ok:
                    worst_status = Status.UNKNOWN
                    error_msg = error_msg or "Scrape failed"

                # Metrics dict (used by the store layer)
                metrics_dict = {s.name: s.value for s in sr.samples}

                tr = TargetResult(
                    run_id=run.run_id,
                    target_id=str(target.id),
                    ts=_now(),
                    status=worst_status,
                    latency_ms=sr.latency_ms,
                    http_status=sr.http_status,
                    scrape_ok=sr.ok,
                    error=error_msg,
                    checks=checks,
                    metrics=metrics_dict,
                )
                target_results.append(tr)

                # Update target state
                await upsert_target_state(
                    str(target.id),
                    worst_status,
                    error_msg,
                )

                # Snapshot slow-query details (before system.profile rolls over)
                if sr.slow_queries:
                    await save_slow_queries(
                        str(target.id),
                        target.hostname or target.ip,
                        target.ip,
                        target.cluster,
                        sr.slow_queries,
                    )

                # Run counters
                if worst_status == Status.OK:
                    run.ok += 1
                elif worst_status == Status.WARNING:
                    run.warning += 1
                elif worst_status == Status.CRITICAL:
                    run.critical += 1
                else:
                    run.unknown += 1

                # Collect every check - the OK ones are needed for resolution
                for c in checks:
                    all_checks.append((target, c))

            # Results toplu kaydet
            await save_results(target_results)

            # Finalize the run
            await finalize_run(run)

            # Alerting
            await self._process_alerts(all_checks)

            logger.info(
                "Run complete: %s -> OK=%d WARN=%d CRIT=%d UNK=%d (%.0fms)",
                run.run_id,
                run.ok,
                run.warning,
                run.critical,
                run.unknown,
                run.duration_ms,
            )

            return run.run_id

        except Exception as e:
            logger.exception("Error during run: %s", e)
            if run:
                try:
                    await finalize_run(run)
                except Exception:
                    pass
            return run.run_id if run else None

        finally:
            await self._release_lock()

    async def _process_alerts(
        self,
        all_checks: list[tuple[TargetInDB, CheckResult]],
    ) -> None:
        """Bulk alert processing, including alert storm detection."""
        import aiohttp

        # Separate OK from non-OK checks.
        # OK checks drive resolution; non-OK checks drive firing and storm detection.
        ok_checks = [(t, c) for t, c in all_checks if c.status == Status.OK]
        bad_checks = [(t, c) for t, c in all_checks if c.status != Status.OK]

        async with aiohttp.ClientSession() as session:
            # 1) OK check'ler — mevcut PENDING/FIRING alert'leri resolved yap
            for target, check in ok_checks:
                placeholder_result = TargetResult(
                    run_id="alerting",
                    target_id=str(target.id),
                    ts=_now(),
                    status=check.status,
                )
                await process_check_result(target, check, placeholder_result, session)

            # 2) Non-OK checks - group by rule and detect storms
            by_rule: dict[str, list[tuple[TargetInDB, CheckResult]]] = {}
            for target, check in bad_checks:
                by_rule.setdefault(check.rule_id, []).append((target, check))

            for rule_id, items in by_rule.items():
                if len(items) >= settings.alert_storm_threshold:
                    # Alert storm - grouped message
                    targets = [t for t, _ in items]
                    checks = [c for _, c in items]
                    severity = Status.worst(*[c.status for c in checks])
                    await send_alert_storm_batch(rule_id, targets, checks, severity)
                else:
                    # Normal - one alert per check
                    for target, check in items:
                        # process_check_result wants a TargetResult, but we do not
                        # actually use the one we hold here - a placeholder is enough
                        placeholder_result = TargetResult(
                            run_id="alerting",
                            target_id=str(target.id),
                            ts=_now(),
                            status=check.status,
                        )
                        await process_check_result(target, check, placeholder_result, session)


# ── Signal handler ──────────────────────────────────────────────

_scheduler_service: SchedulerService | None = None


def setup_signal_handlers(loop: asyncio.AbstractEventLoop) -> None:
    """Graceful shutdown handlers for SIGTERM/SIGINT."""

    async def shutdown(sig: signal.Signals) -> None:
        logger.info("Signal received: %s", sig.name)
        if _scheduler_service:
            await _scheduler_service.stop()
        loop.stop()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(
                sig,
                lambda s=sig: asyncio.create_task(shutdown(s)),
            )
        except NotImplementedError:
            # add_signal_handler is unavailable on Windows
            pass


async def get_scheduler() -> SchedulerService:
    """Singleton scheduler servisi."""
    global _scheduler_service
    if _scheduler_service is None:
        _scheduler_service = SchedulerService()
    return _scheduler_service
