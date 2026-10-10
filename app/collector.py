# aiohttp concurrent scraper — mongodb_exporter /metrics endpoint
# ===============================================================
# Collects metrics from every target concurrently.
# Single ClientSession, asyncio.Semaphore, retry and timeout handling.

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from bson import json_util

import aiohttp
from aiohttp import (
    BasicAuth,
    ClientSession,
    ClientTimeout,
    TCPConnector,
)

from app.config import settings
from app.models import ScrapeResult, Sample, TargetInDB
from app.promparse import parse as parse_prometheus

logger = logging.getLogger(__name__)


def _is_field(name) -> bool:
    """Is this a real field name (not an operator)? Strips $and/$or/$exists prefixes."""
    return isinstance(name, str) and not name.startswith("$")


class MetricsCollector:
    """mongodb_exporter /metrics endpoint'lerinden concurrent metrik toplar."""

    def __init__(self) -> None:
        self._session: Optional[ClientSession] = None
        self._semaphore: Optional[asyncio.Semaphore] = None

    async def start(self) -> None:
        """Create the ClientSession and Semaphore."""
        connector = TCPConnector(
            limit=settings.scrape_concurrency,
            ttl_dns_cache=300,
            force_close=False,  # keep-alive
        )
        timeout = ClientTimeout(
            total=settings.scrape_timeout_total,
            connect=settings.scrape_timeout_connect,
        )
        self._session = ClientSession(
            connector=connector,
            timeout=timeout,
        )
        self._semaphore = asyncio.Semaphore(settings.scrape_concurrency)
        logger.info(
            "Collector started: concurrency=%d, timeout=%ds/%ds",
            settings.scrape_concurrency,
            settings.scrape_timeout_total,
            settings.scrape_timeout_connect,
        )

    async def stop(self) -> None:
        """Close the ClientSession."""
        if self._session:
            await self._session.close()
            self._session = None
            logger.info("Collector durduruldu")

    def _build_url(self, target: TargetInDB) -> str:
        """Build the scrape URL for a target."""
        if target.exporter_url:
            return target.exporter_url
        return f"{settings.scrape_scheme}://{target.ip}:{target.port}{settings.scrape_path}"

    def _build_auth(self) -> Optional[BasicAuth]:
        """Basic auth objesi (varsa)."""
        user = settings.exporter_basic_auth_user
        pw = settings.exporter_basic_auth_password
        if user and pw:
            return BasicAuth(login=user, password=pw.get_secret_value())
        return None

    async def scrape_target(self, target: TargetInDB) -> ScrapeResult:
        """Scrape a single target, with retries."""
        url = self._build_url(target)
        assert self._semaphore is not None
        assert self._session is not None

        auth = self._build_auth()
        max_attempts = 1 + settings.scrape_retry_count
        last_error: str | None = None
        total_latency = 0.0
        attempts = 0

        async with self._semaphore:
            for attempt in range(1, max_attempts + 1):
                attempts = attempt
                start = time.monotonic()
                try:
                    async with self._session.get(
                        url,
                        auth=auth,
                        ssl=settings.exporter_tls_verify,
                    ) as resp:
                        latency = (time.monotonic() - start) * 1000
                        total_latency += latency

                        text = await resp.text()
                        http_status = resp.status

                        if http_status == 200:
                            samples = parse_prometheus(text)
                            logger.info(
                                "Scrape OK: %s (%s) → %d samples, %.0fms",
                                target.hostname or target.ip, url,
                                len(samples), latency,
                            )
                            return ScrapeResult(
                                target_id=str(target.id),
                                url=url,
                                ok=True,
                                http_status=http_status,
                                latency_ms=latency,
                                samples=[Sample(**s._asdict()) for s in samples],
                                attempts=attempts,
                            )
                        else:
                            # 4xx/5xx -> no retry, return immediately
                            return ScrapeResult(
                                target_id=str(target.id),
                                url=url,
                                ok=False,
                                http_status=http_status,
                                latency_ms=latency,
                                error=f"HTTP {http_status}",
                                attempts=attempts,
                            )

                except asyncio.TimeoutError as e:
                    latency = (time.monotonic() - start) * 1000
                    total_latency += latency
                    last_error = f"Timeout: {e}"
                    if attempt < max_attempts:
                        backoff = settings.scrape_retry_backoff * (2 ** (attempt - 1))
                        logger.debug(
                            "Retry %d/%d for %s after %.1fs",
                            attempt + 1, max_attempts, url, backoff,
                        )
                        await asyncio.sleep(backoff)

                except aiohttp.ClientConnectorError as e:
                    latency = (time.monotonic() - start) * 1000
                    total_latency += latency
                    last_error = f"Connection error: {e}"
                    if attempt < max_attempts:
                        backoff = settings.scrape_retry_backoff * (2 ** (attempt - 1))
                        logger.debug(
                            "Retry %d/%d for %s after %.1fs",
                            attempt + 1, max_attempts, url, backoff,
                        )
                        await asyncio.sleep(backoff)

                except aiohttp.ClientError as e:
                    latency = (time.monotonic() - start) * 1000
                    total_latency += latency
                    last_error = f"Client error: {e}"
                    # No retry on generic client errors either
                    break

        # All retries exhausted
        logger.warning(
            "Scrape FAIL: %s (%s) → %s (attempts=%d, %.0fms)",
            target.hostname or target.ip, url,
            last_error or "Unknown error", attempts, total_latency,
        )
        return ScrapeResult(
            target_id=str(target.id),
            url=url,
            ok=False,
            http_status=None,
            latency_ms=total_latency,
            error=last_error or "Unknown error",
            attempts=attempts,
        )

    async def scrape_all(self, targets: list[TargetInDB]) -> list[ScrapeResult]:
        """Scrape every target concurrently.

        One target failing never affects the others.
        """
        if not self._session or not self._semaphore:
            raise RuntimeError("Collector not started. Call start() first.")

        tasks = [self.scrape_target(t) for t in targets]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        output: list[ScrapeResult] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                # Beklenmeyen exception → UNKNOWN sonucu
                target = targets[i]
                output.append(
                    ScrapeResult(
                        target_id=str(target.id),
                        url=self._build_url(target),
                        ok=False,
                        error=f"Unexpected error: {result}",
                        attempts=1,
                    )
                )
            else:
                output.append(result)

        return output


# ── Direct MongoDB Collector (exporter-less mode) ────────────────
# Connects directly to MongoDB, runs diagnostic commands and emits Samples
# under the same names mongodb_exporter uses, so the RuleEngine supports
# both modes without any change.


class DirectCollector:
    """Runs health checks over a direct MongoDB connection."""

    def __init__(self) -> None:
        self._clients: dict[str, object] = {}  # target_id → AsyncIOMotorClient
        self._semaphore: asyncio.Semaphore | None = None

    async def start(self) -> None:
        """Create the Semaphore."""
        self._semaphore = asyncio.Semaphore(settings.scrape_concurrency)
        logger.info(
            "DirectCollector started: concurrency=%d",
            settings.scrape_concurrency,
        )

    async def stop(self) -> None:
        """Close every connection."""
        from motor.motor_asyncio import AsyncIOMotorClient

        for target_id, client in list(self._clients.items()):
            if isinstance(client, AsyncIOMotorClient):
                client.close()
        self._clients.clear()
        logger.info("DirectCollector stopped, %d connections closed", len(self._clients))

    def _get_client(self, target: TargetInDB) -> object:
        """Create or return the motor client for a target."""
        from motor.motor_asyncio import AsyncIOMotorClient

        tid = str(target.id)
        if tid not in self._clients:
            uri = target.mongo_uri or (
                f"mongodb://{target.ip}:{target.mongo_port}"
            )
            self._clients[tid] = AsyncIOMotorClient(
                uri,
                serverSelectionTimeoutMS=(
                    settings.direct_server_selection_timeout * 1000
                ),
                connectTimeoutMS=settings.direct_connect_timeout * 1000,
            )
        return self._clients[tid]

    # -- Metric producers ----------------------------------------

    def _make_sample(self, name: str, value: float, labels: dict[str, str] | None = None) -> Sample:
        return Sample(name=name, labels=labels or {}, value=value)

    async def _collect_server_status(self, client: object) -> list[Sample]:
        """serverStatus → connections, asserts, cursors."""
        try:
            doc = await client.admin.command(
                "serverStatus", check=False
            )
        except Exception as e:
            logger.warning("serverStatus unavailable: %s", e)
            return []

        samples: list[Sample] = []

        # Connections: current + available (for the ratio calculation)
        conn = doc.get("connections", {})
        current = conn.get("current", 0)
        available = conn.get("available", 0)
        samples.append(self._make_sample(
            "mongodb_ss_connections", float(current),
            {"conn_type": "current"},
        ))
        samples.append(self._make_sample(
            "mongodb_ss_connections", float(available),
            {"conn_type": "available"},
        ))

        # Assert warnings
        asserts = doc.get("asserts", {})
        samples.append(self._make_sample(
            "mongodb_ss_asserts_warning",
            float(asserts.get("warning", 0)),
        ))

        # Timed-out cursors
        metrics = doc.get("metrics", {}).get("cursor", {})
        samples.append(self._make_sample(
            "mongodb_ss_timedOutCursors",
            float(metrics.get("timedOut", 0)),
        ))

        # MongoDB process resident memory (RSS, MB) - read-only, clusterMonitor is enough
        mem = doc.get("mem", {})
        resident_mb = mem.get("resident", 0)
        if resident_mb:
            samples.append(self._make_sample(
                "mongodb_mem_resident_mb",
                float(resident_mb),
            ))

        # Operations queued on locks (globalLock.currentQueue) - a bottleneck signal
        gl = doc.get("globalLock", {})
        cur_queue = gl.get("currentQueue", {})
        queued = cur_queue.get("total", 0)
        samples.append(self._make_sample(
            "mongodb_globalLock_currentQueue",
            float(queued),
        ))

        # WiredTiger cache metrikleri (varsa)
        wt = doc.get("wiredTiger", {})
        if wt:
            cache = wt.get("cache", {})
            max_bytes = cache.get("maximum bytes configured", 0)
            used_bytes = cache.get("bytes currently in the cache", 0)
            dirty_bytes = cache.get("tracked dirty bytes in the cache", 0)
            if max_bytes:
                samples.append(self._make_sample(
                    "mongodb_wt_cache_used_percent",
                    round((used_bytes / max_bytes) * 100.0, 2),
                ))
                samples.append(self._make_sample(
                    "mongodb_wt_cache_dirty_percent",
                    round((dirty_bytes / max_bytes) * 100.0, 2),
                ))

            # Read/write ticket availability — MongoDB 8.0 removed concurrentTransactions;
            # emit samples only when the structure exists, else the rule stays UNKNOWN
            # (a missing metric must not look like 0 tickets = saturation).
            ct = wt.get("concurrentTransactions", {})
            read_txn = ct.get("read")
            write_txn = ct.get("write")
            if isinstance(read_txn, dict) and "available" in read_txn:
                samples.append(self._make_sample(
                    "mongodb_wt_read_tickets_available", float(read_txn["available"]),
                ))
            if isinstance(write_txn, dict) and "available" in write_txn:
                samples.append(self._make_sample(
                    "mongodb_wt_write_tickets_available", float(write_txn["available"]),
                ))

            # Checkpoint duration
            txn = wt.get("transaction", {})
            ckpt_min = txn.get("transaction checkpoint min time (msecs)", 0)
            ckpt_max = txn.get("transaction checkpoint max time (msecs)", 0)
            if ckpt_max:
                samples.append(self._make_sample(
                    "mongodb_wt_checkpoint_max_ms", float(ckpt_max),
                ))
                samples.append(self._make_sample(
                    "mongodb_wt_checkpoint_min_ms", float(ckpt_min),
                ))

            # Eviction pressure: read into cache vs evicted
            bytes_read = cache.get("bytes read into cache", 0)
            bytes_evicted = cache.get("bytes written from cache", 0)
            if bytes_read:
                samples.append(self._make_sample(
                    "mongodb_wt_eviction_ratio",
                    round(bytes_evicted / bytes_read, 4),
                ))

        return samples

    async def _collect_replset_status(self, client: object) -> list[Sample]:
        """replSetGetStatus → replset metrikleri."""
        try:
            doc = await client.admin.command(
                "replSetGetStatus", check=False
            )
        except Exception as e:
            logger.debug("replSetGetStatus unavailable (standalone?): %s", e)
            return []

        if doc.get("ok") != 1:
            return []

        samples: list[Sample] = []

        # My state
        my_state = doc.get("myState", 0)
        samples.append(self._make_sample(
            "mongodb_mongod_replset_my_state", float(my_state),
        ))

        # Number of members
        members = doc.get("members", [])
        samples.append(self._make_sample(
            "mongodb_mongod_replset_number_of_members",
            float(len(members)),
        ))

        # Per-member health + lag
        for member in members:
            member_name = member.get("name", "unknown")
            labels = {"member": member_name}
            samples.append(self._make_sample(
                "mongodb_mongod_replset_member_health",
                float(member.get("health", 0)),
                labels,
            ))
            # replication lag: secondary'larda olur
            lag = member.get("secondaryDelaySecs", 0)
            if lag is None:
                lag = 0
            samples.append(self._make_sample(
                "mongodb_mongod_replset_member_replication_lag",
                float(lag),
                labels,
            ))

        return samples

    async def _collect_oplog_window(self, client: object) -> list[Sample]:
        """Oplog head/tail timestamp → oplog penceresi."""
        try:
            local_db = client["local"]
            oplog = local_db["oplog.rs"]

            head_doc = await oplog.find_one(
                sort=[("$natural", -1)],
                projection={"ts": 1},
            )
            tail_doc = await oplog.find_one(
                sort=[("$natural", 1)],
                projection={"ts": 1},
            )
        except Exception as e:
            logger.debug("Could not query the oplog: %s", e)
            return []

        samples: list[Sample] = []
        if head_doc and "ts" in head_doc:
            head_ts = head_doc["ts"]
            if hasattr(head_ts, "time"):
                samples.append(self._make_sample(
                    "mongodb_mongod_replset_oplog_head_timestamp",
                    float(head_ts.time),
                ))
            else:
                samples.append(self._make_sample(
                    "mongodb_mongod_replset_oplog_head_timestamp",
                    float(head_ts.timestamp()),
                ))
        if tail_doc and "ts" in tail_doc:
            tail_ts = tail_doc["ts"]
            if hasattr(tail_ts, "time"):
                samples.append(self._make_sample(
                    "mongodb_mongod_replset_oplog_tail_timestamp",
                    float(tail_ts.time),
                ))
            else:
                samples.append(self._make_sample(
                    "mongodb_mongod_replset_oplog_tail_timestamp",
                    float(tail_ts.timestamp()),
                ))

        return samples

    async def _collect_disk_usage(self, client: object) -> list[Sample]:
        """dbStats -> filesystem utilization ratio (read-only)."""
        try:
            ds = await client.admin.command("dbStats", check=False)
        except Exception as e:
            logger.debug("dbStats unavailable: %s", e)
            return []

        samples: list[Sample] = []
        fs_total = ds.get("fsTotalSize", 0)
        fs_used = ds.get("fsUsedSize", 0)
        if fs_total and fs_used:
            used_percent = (fs_used / fs_total) * 100.0
            samples.append(self._make_sample(
                "mongodb_fs_used_percent", round(used_percent, 2),
            ))
            samples.append(self._make_sample(
                "mongodb_fs_total_gb", round(fs_total / (1024**3), 2),
            ))
            samples.append(self._make_sample(
                "mongodb_fs_used_gb", round(fs_used / (1024**3), 2),
            ))
        return samples

    async def _collect_host_info(self, client: object) -> list[Sample]:
        """hostInfo -> total system RAM + core count (read-only)."""
        try:
            hi = await client.admin.command("hostInfo", check=False)
        except Exception as e:
            logger.debug("hostInfo unavailable: %s", e)
            return []

        samples: list[Sample] = []
        system = hi.get("system", {})
        mem_size_mb = system.get("memSizeMB", 0)
        if mem_size_mb:
            samples.append(self._make_sample(
                "mongodb_mem_total_mb", float(mem_size_mb),
            ))
        num_cores = system.get("numCores", 0)
        if num_cores:
            samples.append(self._make_sample(
                "mongodb_cpu_cores", float(num_cores),
            ))
        return samples

    async def _collect_build_info(self, client: object) -> list[Sample]:
        """buildInfo → mongod versiyonu (read-only)."""
        try:
            bi = await client.admin.command("buildInfo", check=False)
        except Exception as e:
            logger.debug("buildInfo unavailable: %s", e)
            return []

        samples: list[Sample] = []
        version_str = bi.get("version", "")
        if version_str:
            # "7.0.26-14" → 7.0 (major.minor); "4.4.24" → 4.4
            try:
                parts = version_str.split(".")
                major_minor = float(f"{parts[0]}.{parts[1]}")
                samples.append(self._make_sample(
                    "mongodb_mongod_version", major_minor,
                ))
            except (ValueError, IndexError):
                logger.debug("Versiyon parse edilemedi: %s", version_str)
        return samples

    async def _collect_fcv(self, client: object) -> list[Sample]:
        """featureCompatibilityVersion (read-only, clusterMonitor yeterli)."""
        try:
            doc = await client.admin.command(
                {"getParameter": 1, "featureCompatibilityVersion": 1},
                check=False,
            )
        except Exception as e:
            logger.debug("FCV unavailable: %s", e)
            return []

        samples: list[Sample] = []
        fcv = doc.get("featureCompatibilityVersion", {})
        version_str = fcv.get("version", "")
        if version_str:
            try:
                parts = version_str.split(".")
                major_minor = float(f"{parts[0]}.{parts[1]}")
                samples.append(self._make_sample(
                    "mongodb_fcv_version", major_minor,
                ))
            except (ValueError, IndexError):
                logger.debug("FCV parse edilemedi: %s", version_str)
        return samples

    async def _collect_security_opts(self, client: object) -> list[Sample]:
        """getCmdLineOpts -> security / network configuration (read-only)."""
        try:
            doc = await client.admin.command("getCmdLineOpts", check=False)
        except Exception as e:
            logger.debug("getCmdLineOpts unavailable: %s", e)
            return []

        samples: list[Sample] = []
        parsed = doc.get("parsed", {})

        # authorization: enabled/disabled
        security = parsed.get("security", {})
        auth = security.get("authorization", "disabled")
        samples.append(self._make_sample(
            "mongodb_security_authorization_enabled",
            1.0 if auth == "enabled" else 0.0,
        ))

        # bindIp - may be a comma-separated list
        net = parsed.get("net", {})
        bind_ip = str(net.get("bindIp", ""))
        samples.append(self._make_sample(
            "mongodb_net_bindip_count",
            float(len([ip for ip in bind_ip.split(",") if ip.strip()])),
        ))
        # Public bindIp: 0.0.0.0 or :: (all interfaces)
        is_public = any(
            ip.strip() in ("0.0.0.0", "::", "*")
            for ip in bind_ip.split(",")
        )
        samples.append(self._make_sample(
            "mongodb_net_bindip_public",
            1.0 if is_public else 0.0,
        ))

        # TLS durumu
        tls = parsed.get("net", {}).get("tls", {})
        tls_mode = tls.get("mode", "disabled")
        samples.append(self._make_sample(
            "mongodb_net_tls_enabled",
            1.0 if tls_mode != "disabled" else 0.0,
        ))

        return samples

    async def _collect_slow_queries(self, client: object) -> tuple[list[Sample], list[dict]]:
        """Collect slow queries from system.profile (find-only, readAnyDatabase).

        Note: a readAnyDatabase user can only run a plain ``find`` against
        system.profile - aggregate is not permitted on that collection.
        So a plain ``find`` iteration is used (it is capped and small).
        When the profiler is off system.profile is empty -> returns 0 (silent OK).

        Returns: (samples, details)
        - samples: metric samples (total, max_ms, per-ns count)
        - details: raw details of queries exceeding the snapshot threshold
          (slow_query_snapshot_min_ms): ns, millis, planSummary, docsExamined,
          filter - persisted to the `slow_queries` collection in the metadata DB.
        """
        threshold = settings.slow_query_threshold_ms
        snapshot_min = settings.slow_query_snapshot_min_ms
        window = settings.slow_query_window_seconds

        try:
            list_dbs = await client.admin.command("listDatabases", check=False)
        except Exception as e:
            logger.debug("listDatabases unavailable: %s", e)
            return [], []

        if not list_dbs or list_dbs.get("ok") != 1:
            return [], []

        cutoff = datetime.now(timezone.utc) - timedelta(seconds=window)

        total = 0
        max_ms = 0.0
        per_ns: list[tuple[str, str, int]] = []  # (db, ns, count)
        details: list[dict] = []

        for db_info in list_dbs.get("databases", []):
            db_name = db_info.get("name")
            if not db_name or db_name in ("local", "config"):
                # readAnyDatabase local/config kapsamaz
                continue
            try:
                coll = client[db_name]["system.profile"]
                cursor = coll.find(
                    {"ts": {"$gte": cutoff}, "millis": {"$gte": threshold}},
                )
                ns_counts: dict[str, int] = {}
                async for doc in cursor:
                    total += 1
                    ms = doc.get("millis", 0) or 0
                    if ms > max_ms:
                        max_ms = float(ms)
                    ns = doc.get("ns") or db_name
                    ns_counts[ns] = ns_counts.get(ns, 0) + 1

                    # Capture details for queries above the snapshot threshold
                    if ms >= snapshot_min:
                        details.append(self._extract_slow_query_detail(db_name, doc))
                for ns, cnt in ns_counts.items():
                    per_ns.append((db_name, ns, cnt))
            except Exception as e:
                logger.debug(
                    "could not read system.profile (%s): %s", db_name, e
                )
                continue

        samples: list[Sample] = []
        # Scalar metrics are always emitted -> rules produce a clean OK/warning.
        samples.append(self._make_sample(
            "mongodb_slow_query_total", float(total),
        ))
        samples.append(self._make_sample(
            "mongodb_slow_query_max_ms", float(max_ms),
        ))
        for db_name, ns, cnt in per_ns:
            samples.append(self._make_sample(
                "mongodb_slow_query_count", float(cnt),
                {"db": db_name, "ns": ns},
            ))
        return samples, details

    def _extract_slow_query_detail(self, db_name: str, doc: dict) -> dict:
        """Extract the fields needed for analysis from a system.profile document.

        The `command` field (the query itself) is stored with BSON types; it is
        converted into something JSON-serializable (ObjectId/datetime/Timestamp).
        """
        ns = doc.get("ns") or db_name
        cmd = doc.get("command") or doc.get("query") or {}

        # Tam sorguyu sakla (Extended JSON — ObjectId/datetime korunur)
        full_command: dict = {}
        try:
            full_command = json.loads(json_util.dumps(cmd)) if isinstance(cmd, dict) else {}
        except Exception:
            full_command = {}

        coll_name = None
        filter_keys: list = []
        sort_keys: dict = {}
        agg_stages: list = []

        if isinstance(cmd, dict):
            for k in ("find", "aggregate", "count", "distinct", "findAndModify"):
                if k in cmd:
                    coll_name = cmd[k] if isinstance(cmd[k], str) else None
                    break
            # filter field names (not values) - including those inside $or/$and/$nor
            filt = cmd.get("filter", cmd.get("query", cmd.get("$query", {})))
            if isinstance(filt, dict):
                filter_keys = self._extract_filter_keys(filt)
            # distinct: the key field is indexable too
            distinct_key = cmd.get("key")
            if isinstance(distinct_key, str) and distinct_key and distinct_key not in filter_keys:
                filter_keys.append(distinct_key)
            srt = cmd.get("sort", {})
            if isinstance(srt, dict):
                sort_keys = {k: v for k, v in srt.items()}
            # aggregate pipeline: stage names + $match fields
            pipe = cmd.get("pipeline", [])
            if isinstance(pipe, list):
                for stage in pipe[:5]:
                    if isinstance(stage, dict):
                        agg_stages.extend(list(stage.keys()))
                        # also extract filter fields from the $match stage
                        match = stage.get("$match")
                        if isinstance(match, dict):
                            for f in self._extract_filter_keys(match):
                                if f not in filter_keys:
                                    filter_keys.append(f)

        # Re-compress planSummary: for multi-branch queries such as "$in" MongoDB
        # repeats the same IXSCAN description once per branch (e.g. an $in over 31
        # users -> the same line 31 times). Merge the unique parts and add a count.
        plan_raw = doc.get("planSummary", "") or ""
        plan_summary = self._normalize_plan_summary(plan_raw)

        return {
            "db": db_name,
            "ns": ns,
            "coll": coll_name,
            "op": doc.get("op", "query"),
            "millis": doc.get("millis", 0),
            "ts": doc.get("ts"),
            "plan_summary": plan_summary,
            "docs_examined": doc.get("docsExamined"),
            "keys_examined": doc.get("keysExamined"),
            "nreturned": doc.get("nreturned"),
            "filter_keys": filter_keys,
            "sort": sort_keys,
            "agg_stages": agg_stages,
            "command": full_command,
        }

    @staticmethod
    def _extract_filter_keys(filt: dict) -> list[str]:
        """Recursively extract indexable field names from a filter dict.

        - `$or`/`$and`/`$nor`: merge the fields of every sub-filter
        - plain field: add directly (including nested `{field: {$gte: ...}}`)
        - other `$`-prefixed operators ($exists, $in, $regex...) are dropped
        """
        keys: list[str] = []

        def walk(f: dict) -> None:
            if not isinstance(f, dict):
                return
            for k, v in f.items():
                if k in ("$or", "$and", "$nor"):
                    if isinstance(v, list):
                        for sub in v:
                            if isinstance(sub, dict):
                                walk(sub)
                elif isinstance(k, str) and not k.startswith("$"):
                    if k not in keys:
                        keys.append(k)

        walk(filt)
        return keys

    @staticmethod
    def _normalize_plan_summary(raw: str) -> str:
        """Compress repeated stages in planSummary.

        For multi-branch queries such as "$in", MongoDB repeats planSummary once
        per branch (e.g. an $in over 31 users -> "IXSCAN {a:1}, IXSCAN {a:1}, ...").
        A regex splits whole stages such as "IXSCAN { ... }" / "COLLSCAN" and
        counts the repeats: "IXSCAN {a:1} (x31)".
        """
        if not raw:
            return ""
        import re

        # Stages: a single token such as "IXSCAN { ... }" or "COLLSCAN".
        # Capture the whole block, preserving commas inside the braces.
        token_re = re.compile(r"[A-Z]+(?:\s*\{[^{}]*\})?")
        tokens = token_re.findall(raw)

        seen: dict[str, int] = {}
        ordered: list[str] = []
        for t in tokens:
            t = t.strip()
            if not t:
                continue
            if t in seen:
                seen[t] += 1
            else:
                seen[t] = 1
                ordered.append(t)

        out = []
        for t in ordered:
            cnt = seen[t]
            out.append(t if cnt == 1 else f"{t} (x{cnt})")
        return ", ".join(out)

    # ── Ana scrape metodu ──────────────────────────────────────

    async def scrape_target(self, target: TargetInDB) -> ScrapeResult:
        """Connect directly to MongoDB and collect metrics."""
        assert self._semaphore is not None

        uri = target.mongo_uri or f"mongodb://{target.ip}:{target.mongo_port}"
        label = target.hostname or target.ip

        async with self._semaphore:
            start = time.monotonic()
            logger.info("Direct connect: %s → %s", label, uri)
            try:
                client = self._get_client(target)

                # Ping testi
                ping_start = time.monotonic()
                try:
                    await client.admin.command("ping")
                    ping_ok = True
                except Exception as e:
                    ping_ok = False
                    latency = (time.monotonic() - start) * 1000
                    logger.warning(
                        "Direct FAIL (ping): %s → %s (%.0fms)",
                        label, e, latency,
                    )
                    return ScrapeResult(
                        target_id=str(target.id),
                        url=target.mongo_uri or f"mongodb://{target.ip}:{target.mongo_port}",
                        ok=False,
                        error=f"MongoDB ping failed: {e}",
                        latency_ms=latency,
                        attempts=1,
                    )

                # Run the commands in parallel
                server_status_task = self._collect_server_status(client)
                replset_task = self._collect_replset_status(client)
                oplog_task = self._collect_oplog_window(client)
                disk_task = self._collect_disk_usage(client)
                host_info_task = self._collect_host_info(client)
                slow_query_task = self._collect_slow_queries(client)
                build_info_task = self._collect_build_info(client)
                fcv_task = self._collect_fcv(client)
                security_task = self._collect_security_opts(client)

                (
                    ss_samples,
                    rs_samples,
                    oplog_samples,
                    disk_samples,
                    host_info_samples,
                    slow_query_result,
                    build_info_samples,
                    fcv_samples,
                    security_samples,
                ) = await asyncio.gather(
                    server_status_task,
                    replset_task,
                    oplog_task,
                    disk_task,
                    host_info_task,
                    slow_query_task,
                    build_info_task,
                    fcv_task,
                    security_task,
                    return_exceptions=True,
                )

                # slow_query_task now returns a (samples, details) tuple
                slow_query_samples: list[Sample] = []
                slow_query_details: list[dict] = []
                if isinstance(slow_query_result, tuple):
                    slow_query_samples, slow_query_details = slow_query_result
                elif isinstance(slow_query_result, list):
                    # backward compatibility with the old signature (samples only)
                    slow_query_samples = slow_query_result

                # Merge the results
                all_samples: list[Sample] = []

                # scrape_ok: always 1 (direct connection)
                all_samples.append(self._make_sample("scrape_ok", 1.0))

                # mongodb_up
                all_samples.append(self._make_sample("mongodb_up", 1.0 if ping_ok else 0.0))

                if isinstance(ss_samples, list):
                    all_samples.extend(ss_samples)
                if isinstance(rs_samples, list):
                    all_samples.extend(rs_samples)
                if isinstance(oplog_samples, list):
                    all_samples.extend(oplog_samples)
                if isinstance(disk_samples, list):
                    all_samples.extend(disk_samples)
                if isinstance(host_info_samples, list):
                    all_samples.extend(host_info_samples)
                if isinstance(slow_query_samples, list):
                    all_samples.extend(slow_query_samples)
                if isinstance(build_info_samples, list):
                    all_samples.extend(build_info_samples)
                if isinstance(fcv_samples, list):
                    all_samples.extend(fcv_samples)
                if isinstance(security_samples, list):
                    all_samples.extend(security_samples)

                latency = (time.monotonic() - start) * 1000
                logger.info(
                    "Direct OK: %s → %d samples, %.0fms",
                    label, len(all_samples), latency,
                )
                return ScrapeResult(
                    target_id=str(target.id),
                    url=target.mongo_uri or f"mongodb://{target.ip}:{target.mongo_port}",
                    ok=True,
                    http_status=200,  # sentinel: direct mode
                    latency_ms=latency,
                    samples=all_samples,
                    attempts=1,
                    slow_queries=slow_query_details,
                )

            except Exception as e:
                latency = (time.monotonic() - start) * 1000
                logger.exception("DirectCollector scrape error: %s -> %s", target.id, e)
                return ScrapeResult(
                    target_id=str(target.id),
                    url=target.mongo_uri or f"mongodb://{target.ip}:{target.mongo_port}",
                    ok=False,
                    error=f"Direct collection error: {e}",
                    latency_ms=latency,
                    attempts=1,
                )

    async def scrape_all(self, targets: list[TargetInDB]) -> list[ScrapeResult]:
        """Scrape every target concurrently."""
        if not self._semaphore:
            raise RuntimeError("DirectCollector not started. Call start() first.")

        tasks = [self.scrape_target(t) for t in targets]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        output: list[ScrapeResult] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                target = targets[i]
                output.append(
                    ScrapeResult(
                        target_id=str(target.id),
                        url=target.mongo_uri or f"mongodb://{target.ip}:{target.mongo_port}",
                        ok=False,
                        error=f"Unexpected error: {result}",
                        attempts=1,
                    )
                )
            else:
                output.append(result)

        return output
