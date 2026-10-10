# collector.py test suite - fake exporter served by the aiohttp test server

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest
from aiohttp import web

from app.collector import MetricsCollector
from app.config import settings
from app.models import TargetInDB

# ── Fake exporter server ────────────────────────────────────────

FAKE_METRICS = """\
# HELP mongodb_up Whether MongoDB is up
# TYPE mongodb_up gauge
mongodb_up 1
# HELP mongodb_connections Current connections
# TYPE mongodb_connections gauge
mongodb_ss_connections{conn_type="current"} 42
mongodb_ss_connections{conn_type="available"} 51158
# HELP mongodb_uptime Uptime in seconds
# TYPE mongodb_uptime gauge
mongodb_ss_uptime 360000
"""


async def metrics_handler(request: web.Request) -> web.Response:
    """Fake /metrics endpoint."""
    # For the slow-endpoint test
    delay = request.query.get("delay")
    if delay:
        await asyncio.sleep(float(delay))
    return web.Response(text=FAKE_METRICS, content_type="text/plain")


async def error_handler(request: web.Request) -> web.Response:
    """Endpoint returning 500."""
    raise web.HTTPInternalServerError(text="Internal error")


async def not_found_handler(request: web.Request) -> web.Response:
    """Endpoint returning 404."""
    raise web.HTTPNotFound()


@pytest.fixture
async def exporter_server():
    """Fake exporter server."""
    app = web.Application()
    app.router.add_get("/metrics", metrics_handler)
    app.router.add_get("/error", error_handler)
    app.router.add_get("/notfound", not_found_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "localhost", 19216)
    await site.start()

    yield "localhost", 19216

    await runner.cleanup()


@pytest.fixture
async def collector():
    """A started collector."""
    c = MetricsCollector()
    await c.start()
    yield c
    await c.stop()


# ── Tests ───────────────────────────────────────────────────────


class TestCollectorSuccess:
    """Successful scrape scenarios."""

    @pytest.mark.asyncio
    async def test_scrape_single_target(self, exporter_server, collector):
        host, port = exporter_server
        target = TargetInDB(
            ip=host,
            port=port,
            hostname="test-host",
            cluster="test-cluster",
            enabled=True,
        )
        # Override the URL directly instead of going through env settings
        target.exporter_url = f"http://{host}:{port}/metrics"

        result = await collector.scrape_target(target)

        assert result.ok is True
        assert result.http_status == 200
        assert result.latency_ms > 0
        assert len(result.samples) > 0
        assert result.attempts == 1

        # Check parsed metrics
        names = {s.name for s in result.samples}
        assert "mongodb_up" in names
        assert "mongodb_ss_connections" in names
        assert "mongodb_ss_uptime" in names

    @pytest.mark.asyncio
    async def test_scrape_multiple_targets(self, exporter_server, collector):
        host, port = exporter_server
        targets = []
        for i in range(3):
            t = TargetInDB(
                ip=host,
                port=port,
                hostname=f"test-host-{i}",
                cluster="test-cluster",
                enabled=True,
            )
            t.exporter_url = f"http://{host}:{port}/metrics"
            targets.append(t)

        results = await collector.scrape_all(targets)

        assert len(results) == 3
        for r in results:
            assert r.ok is True
            assert r.http_status == 200

    @pytest.mark.asyncio
    async def test_parse_result_has_mongodb_up(self, exporter_server, collector):
        host, port = exporter_server
        target = TargetInDB(
            ip=host,
            port=port,
            enabled=True,
        )
        target.exporter_url = f"http://{host}:{port}/metrics"

        result = await collector.scrape_target(target)
        mongodb_up_samples = [s for s in result.samples if s.name == "mongodb_up"]
        assert len(mongodb_up_samples) == 1
        assert mongodb_up_samples[0].value == 1.0


class TestCollectorErrors:
    """Failure scenarios."""

    @pytest.mark.asyncio
    async def test_http_500(self, exporter_server, collector):
        host, port = exporter_server
        target = TargetInDB(
            ip=host,
            port=port,
            enabled=True,
        )
        target.exporter_url = f"http://{host}:{port}/error"

        result = await collector.scrape_target(target)
        assert result.ok is False
        assert result.http_status == 500

    @pytest.mark.asyncio
    async def test_http_404_no_retry(self, exporter_server, collector):
        """4xx responses must not be retried."""
        host, port = exporter_server
        target = TargetInDB(
            ip=host,
            port=port,
            enabled=True,
        )
        target.exporter_url = f"http://{host}:{port}/notfound"

        result = await collector.scrape_target(target)
        assert result.ok is False
        assert result.http_status == 404
        assert result.attempts == 1  # 4xx'te retry yok

    @pytest.mark.asyncio
    async def test_connection_refused(self, collector):
        """A refused connection must be retried."""
        target = TargetInDB(
            ip="127.0.0.1",
            port=19999,  # unused port
            enabled=True,
        )
        result = await collector.scrape_target(target)
        assert result.ok is False
        assert result.http_status is None
        # Connection errors are retried
        assert result.attempts > 1

    @pytest.mark.asyncio
    async def test_timeout(self, exporter_server, collector):
        """Timeout durumunda retry denenmeli."""
        host, port = exporter_server
        target = TargetInDB(
            ip=host,
            port=port,
            enabled=True,
        )
        # Endpoint delayed by 35 seconds -> exceeds the 30s timeout
        target.exporter_url = f"http://{host}:{port}/metrics?delay=35"
        result = await collector.scrape_target(target)
        assert result.ok is False
        assert result.attempts > 1
        assert "Timeout" in (result.error or "")


class TestCollectorAllResilience:
    """scrape_all: one failing target must not sink the other results."""

    @pytest.mark.asyncio
    async def test_one_fails_others_succeed(self, exporter_server, collector):
        host, port = exporter_server

        # 3 target: 2 good, 1 bad
        good1 = TargetInDB(ip=host, port=port, enabled=True)
        good1.exporter_url = f"http://{host}:{port}/metrics"

        good2 = TargetInDB(ip=host, port=port, enabled=True)
        good2.exporter_url = f"http://{host}:{port}/metrics"

        bad = TargetInDB(ip=host, port=port, enabled=True)
        bad.exporter_url = f"http://{host}:{port}/error"

        results = await collector.scrape_all([good1, good2, bad])

        assert len(results) == 3
        ok_count = sum(1 for r in results if r.ok)
        fail_count = sum(1 for r in results if not r.ok)
        assert ok_count == 2
        assert fail_count == 1


# ── DirectCollector tests ────────────────────────────────────────
# motor AsyncIOMotorClient mock'lanarak test edilir


class MockCollection:
    """Mock MongoDB collection."""

    def __init__(self, docs: list[dict] | None = None) -> None:
        self._docs = docs or []

    def find_one(self, *args, **kwargs):
        fut: asyncio.Future = asyncio.Future()
        if self._docs:
            fut.set_result(self._docs[0])
        else:
            fut.set_result(None)
        return fut


class MockAdmin:
    """Mock admin DB for commands."""

    def __init__(
        self, server_status: dict | None = None, replset_status: dict | None = None
    ) -> None:
        self._server_status = server_status or {}
        self._replset_status = replset_status or {"ok": 0}

    async def command(self, cmd, *args, **kwargs):
        cmd_str = str(cmd).lower() if isinstance(cmd, str) else ""
        if "ping" in cmd_str:
            return {"ok": 1}
        elif "serverstatus" in cmd_str:
            return self._server_status
        elif "replsetgetstatus" in cmd_str:
            return self._replset_status
        return {"ok": 1}


class MockClient:
    """Mock AsyncIOMotorClient."""

    def __init__(self, admin: MockAdmin | None = None, local_db: dict | None = None) -> None:
        self.admin = admin or MockAdmin()
        self._local_db = local_db or {}

    def __getitem__(self, name: str) -> object:
        if name == "local":
            return self._local_db
        return MockCollection()

    def close(self) -> None:
        pass


@pytest.fixture
async def direct_collector():
    """A started DirectCollector."""
    from app.collector import DirectCollector

    dc = DirectCollector()
    await dc.start()
    yield dc
    await dc.stop()


class TestDirectCollector:
    """DirectCollector unit tests (with a mocked MongoDB)."""

    @pytest.mark.asyncio
    async def test_start_stop(self):
        from app.collector import DirectCollector

        dc = DirectCollector()
        assert dc._semaphore is None
        await dc.start()
        assert dc._semaphore is not None
        await dc.stop()
        assert dc._clients == {}

    @pytest.mark.asyncio
    async def test_scrape_ok_and_up_always_present(self, direct_collector, monkeypatch):
        """scrape_ok and mongodb_up must always be present in the samples."""
        admin = MockAdmin(
            server_status={
                "ok": 1,
                "connections": {"current": 10, "available": 100},
            }
        )
        mock_client = MockClient(admin=admin)

        # _get_client override
        monkeypatch.setattr(direct_collector, "_get_client", lambda t: mock_client)

        target = TargetInDB(ip="127.0.0.1", mongo_uri="mongodb://localhost:27017", enabled=True)

        result = await direct_collector.scrape_target(target)

        assert result.ok is True
        assert result.http_status == 200
        names = {s.name for s in result.samples}
        assert "scrape_ok" in names
        assert "mongodb_up" in names
        assert "mongodb_ss_connections" in names

        # scrape_ok = 1.0
        ok_sample = next(s for s in result.samples if s.name == "scrape_ok")
        assert ok_sample.value == 1.0

        # mongodb_up = 1.0
        up_sample = next(s for s in result.samples if s.name == "mongodb_up")
        assert up_sample.value == 1.0

    @pytest.mark.asyncio
    async def test_connections_metric(self, direct_collector, monkeypatch):
        """mongodb_ss_connections must be emitted with current/available labels."""
        admin = MockAdmin(
            server_status={
                "ok": 1,
                "connections": {"current": 42, "available": 51158},
            }
        )
        mock_client = MockClient(admin=admin)
        monkeypatch.setattr(direct_collector, "_get_client", lambda t: mock_client)

        target = TargetInDB(ip="127.0.0.1", mongo_uri="mongodb://localhost:27017", enabled=True)
        result = await direct_collector.scrape_target(target)

        conn_samples = [s for s in result.samples if s.name == "mongodb_ss_connections"]
        assert len(conn_samples) == 2

        by_type = {s.labels.get("conn_type"): s.value for s in conn_samples}
        assert by_type["current"] == 42.0
        assert by_type["available"] == 51158.0

    @pytest.mark.asyncio
    async def test_replset_status_metrics(self, direct_collector, monkeypatch):
        """replSetGetStatus -> replset metrics must be emitted correctly."""
        primary_optime = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
        admin = MockAdmin(
            server_status={"ok": 1, "connections": {}},
            replset_status={
                "ok": 1,
                "myState": 1,
                "members": [
                    {
                        "name": "node1:27017",
                        "health": 1,
                        "stateStr": "PRIMARY",
                        "optimeDate": primary_optime,
                    },
                    {
                        "name": "node2:27017",
                        "health": 1,
                        "stateStr": "SECONDARY",
                        "optimeDate": primary_optime - timedelta(seconds=45),
                    },
                    {
                        "name": "node3:27017",
                        "health": 0,
                        "stateStr": "(not reachable/healthy)",
                        "optimeDate": primary_optime - timedelta(seconds=300),
                    },
                ],
            },
        )
        mock_client = MockClient(admin=admin)
        monkeypatch.setattr(direct_collector, "_get_client", lambda t: mock_client)

        target = TargetInDB(ip="127.0.0.1", mongo_uri="mongodb://localhost:27017", enabled=True)
        result = await direct_collector.scrape_target(target)

        # myState
        state = next(s for s in result.samples if s.name == "mongodb_mongod_replset_my_state")
        assert state.value == 1.0

        # member count
        count = next(
            s for s in result.samples if s.name == "mongodb_mongod_replset_number_of_members"
        )
        assert count.value == 3.0

        # member health (3 samples)
        health_samples = [
            s for s in result.samples if s.name == "mongodb_mongod_replset_member_health"
        ]
        assert len(health_samples) == 3
        health_by_member = {s.labels["member"]: s.value for s in health_samples}
        assert health_by_member["node1:27017"] == 1.0
        assert health_by_member["node2:27017"] == 1.0
        assert health_by_member["node3:27017"] == 0.0

        # Replication lag = primary optime - member optime.
        # Regression guard: the first implementation read secondaryDelaySecs,
        # which is the configured delay of a delayed secondary and is absent on
        # ordinary members, so every node reported a lag of 0 and the
        # replication_lag rule could never fire.
        lag_samples = [
            s for s in result.samples if s.name == "mongodb_mongod_replset_member_replication_lag"
        ]
        assert len(lag_samples) == 3
        lag_by_member = {s.labels["member"]: s.value for s in lag_samples}
        assert lag_by_member["node1:27017"] == 0.0, "the primary cannot lag behind itself"
        assert lag_by_member["node2:27017"] == 45.0
        assert lag_by_member["node3:27017"] == 300.0

    @pytest.mark.asyncio
    async def test_replication_lag_omitted_without_a_primary(self, direct_collector, monkeypatch):
        """During an election there is no primary to measure against.

        Emitting 0 then would read as "perfectly in sync" at exactly the moment
        the replica set is least healthy. No sample at all leaves the rule
        UNKNOWN, which is the honest answer.
        """
        now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
        admin = MockAdmin(
            server_status={"ok": 1, "connections": {}},
            replset_status={
                "ok": 1,
                "myState": 2,
                "members": [
                    {
                        "name": "node1:27017",
                        "health": 1,
                        "stateStr": "SECONDARY",
                        "optimeDate": now,
                    },
                    {
                        "name": "node2:27017",
                        "health": 1,
                        "stateStr": "SECONDARY",
                        "optimeDate": now,
                    },
                ],
            },
        )
        mock_client = MockClient(admin=admin)
        monkeypatch.setattr(direct_collector, "_get_client", lambda t: mock_client)

        target = TargetInDB(ip="127.0.0.1", mongo_uri="mongodb://localhost:27017", enabled=True)
        result = await direct_collector.scrape_target(target)

        lag_samples = [
            s for s in result.samples if s.name == "mongodb_mongod_replset_member_replication_lag"
        ]
        assert lag_samples == []
        # health must still be reported
        health = [s for s in result.samples if s.name == "mongodb_mongod_replset_member_health"]
        assert len(health) == 2

    @pytest.mark.asyncio
    async def test_replication_lag_never_negative(self, direct_collector, monkeypatch):
        """A secondary can report an optime slightly ahead of the primary snapshot."""
        now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
        admin = MockAdmin(
            server_status={"ok": 1, "connections": {}},
            replset_status={
                "ok": 1,
                "myState": 1,
                "members": [
                    {"name": "p:27017", "health": 1, "stateStr": "PRIMARY", "optimeDate": now},
                    {
                        "name": "s:27017",
                        "health": 1,
                        "stateStr": "SECONDARY",
                        "optimeDate": now + timedelta(milliseconds=200),
                    },
                ],
            },
        )
        mock_client = MockClient(admin=admin)
        monkeypatch.setattr(direct_collector, "_get_client", lambda t: mock_client)

        target = TargetInDB(ip="127.0.0.1", mongo_uri="mongodb://localhost:27017", enabled=True)
        result = await direct_collector.scrape_target(target)

        lags = {
            s.labels["member"]: s.value
            for s in result.samples
            if s.name == "mongodb_mongod_replset_member_replication_lag"
        }
        assert lags["s:27017"] == 0.0

    @pytest.mark.asyncio
    async def test_ping_failure_returns_error(self, direct_collector, monkeypatch):
        """A failed ping must return ScrapeResult.ok=False."""

        class FailingAdmin(MockAdmin):
            async def command(self, cmd, *args, **kwargs):
                cmd_str = str(cmd).lower() if isinstance(cmd, str) else ""
                if "ping" in cmd_str:
                    raise Exception("Connection refused")
                return await super().command(cmd, *args, **kwargs)

        mock_client = MockClient(admin=FailingAdmin())
        monkeypatch.setattr(direct_collector, "_get_client", lambda t: mock_client)

        target = TargetInDB(ip="127.0.0.1", mongo_uri="mongodb://localhost:27017", enabled=True)
        result = await direct_collector.scrape_target(target)

        assert result.ok is False
        assert "MongoDB ping failed" in (result.error or "")
        assert result.latency_ms > 0

    @pytest.mark.asyncio
    async def test_scrape_all_mixed(self, direct_collector, monkeypatch):
        """One failing target must not prevent the others from succeeding."""
        admin_ok = MockAdmin(server_status={"ok": 1, "connections": {}})

        class FailingAdmin(MockAdmin):
            async def command(self, cmd, *args, **kwargs):
                cmd_str = str(cmd).lower() if isinstance(cmd, str) else ""
                if "ping" in cmd_str:
                    raise Exception("Connection refused")
                return await super().command(cmd, *args, **kwargs)

        # Mock returning a different client per target
        _call_count = [0]
        _clients = [
            MockClient(admin=admin_ok),
            MockClient(admin=FailingAdmin()),
            MockClient(admin=admin_ok),
        ]

        def get_client(target):
            idx = _call_count[0]
            _call_count[0] += 1
            return _clients[idx]

        monkeypatch.setattr(direct_collector, "_get_client", get_client)

        targets = [
            TargetInDB(ip="127.0.0.1", mongo_uri="mongodb://good1:27017", enabled=True),
            TargetInDB(ip="127.0.0.1", mongo_uri="mongodb://bad:27017", enabled=True),
            TargetInDB(ip="127.0.0.1", mongo_uri="mongodb://good2:27017", enabled=True),
        ]

        results = await direct_collector.scrape_all(targets)

        assert len(results) == 3
        assert results[0].ok is True
        assert results[1].ok is False
        assert results[2].ok is True

    @pytest.mark.asyncio
    async def test_collection_mode_property(self):
        """collection_mode must be computed correctly."""
        exporter_target = TargetInDB(ip="10.0.0.1", enabled=True)
        assert exporter_target.collection_mode == "exporter"

        direct_target = TargetInDB(
            ip="10.0.0.1", mongo_uri="mongodb://localhost:27017", enabled=True
        )
        assert direct_target.collection_mode == "direct"


class TestDirectCollectorWiredTiger:
    """WiredTiger metrik toplama testleri."""

    @pytest.mark.asyncio
    async def test_wt_cache_metrics(self, direct_collector, monkeypatch):
        """serverStatus.wiredTiger -> cache + ticket metrics must be emitted."""
        admin = MockAdmin(
            server_status={
                "ok": 1,
                "connections": {},
                "wiredTiger": {
                    "cache": {
                        "maximum bytes configured": 1_000_000_000,
                        "bytes currently in the cache": 500_000_000,
                        "tracked dirty bytes in the cache": 50_000_000,
                        "bytes read into cache": 10_000_000_000,
                        "bytes written from cache": 5_000_000_000,
                    },
                    "concurrentTransactions": {
                        "read": {"available": 128, "out": 0, "total": 128},
                        "write": {"available": 128, "out": 0, "total": 128},
                    },
                    "transaction": {
                        "transaction checkpoint min time (msecs)": 100,
                        "transaction checkpoint max time (msecs)": 5000,
                    },
                },
            }
        )
        mock_client = MockClient(admin=admin)
        monkeypatch.setattr(direct_collector, "_get_client", lambda t: mock_client)

        target = TargetInDB(ip="127.0.0.1", mongo_uri="mongodb://localhost:27017", enabled=True)
        result = await direct_collector.scrape_target(target)

        assert result.ok is True
        names = {s.name for s in result.samples}
        assert "mongodb_wt_cache_used_percent" in names
        assert "mongodb_wt_cache_dirty_percent" in names
        assert "mongodb_wt_read_tickets_available" in names
        assert "mongodb_wt_write_tickets_available" in names
        assert "mongodb_wt_checkpoint_max_ms" in names
        assert "mongodb_wt_cache_write_read_ratio" in names

        # Value assertions
        used = next(s for s in result.samples if s.name == "mongodb_wt_cache_used_percent")
        assert used.value == 50.0

        dirty = next(s for s in result.samples if s.name == "mongodb_wt_cache_dirty_percent")
        assert dirty.value == 5.0

    @pytest.mark.asyncio
    async def test_wt_metrics_missing_no_crash(self, direct_collector, monkeypatch):
        """Without WiredTiger (MMAPv1/InMemory) it must not crash, just emit no samples."""
        admin = MockAdmin(
            server_status={
                "ok": 1,
                "connections": {},
                # wiredTiger yok
            }
        )
        mock_client = MockClient(admin=admin)
        monkeypatch.setattr(direct_collector, "_get_client", lambda t: mock_client)

        target = TargetInDB(ip="127.0.0.1", mongo_uri="mongodb://localhost:27017", enabled=True)
        result = await direct_collector.scrape_target(target)

        assert result.ok is True
        names = {s.name for s in result.samples}
        assert "mongodb_wt_cache_used_percent" not in names
        assert "mongodb_wt_read_tickets_available" not in names

    @pytest.mark.asyncio
    async def test_wt_tickets_absent_on_mongodb8(self, direct_collector, monkeypatch):
        """MongoDB 8.0 removed concurrentTransactions - no ticket samples at all
        (emitting 0 would trigger a false saturation alarm)."""
        admin = MockAdmin(
            server_status={
                "ok": 1,
                "connections": {},
                "wiredTiger": {
                    "cache": {
                        "maximum bytes configured": 1_000_000_000,
                        "bytes currently in the cache": 800_000_000,
                        "tracked dirty bytes in the cache": 10_000_000,
                        "bytes read into cache": 5_000_000_000,
                        "bytes written from cache": 2_000_000_000,
                    },
                    # 8.0: concurrentTransactions yok
                    "transaction": {
                        "transaction checkpoint min time (msecs)": 50,
                        "transaction checkpoint max time (msecs)": 3000,
                    },
                },
            }
        )
        mock_client = MockClient(admin=admin)
        monkeypatch.setattr(direct_collector, "_get_client", lambda t: mock_client)

        target = TargetInDB(ip="127.0.0.1", mongo_uri="mongodb://localhost:27017", enabled=True)
        result = await direct_collector.scrape_target(target)

        assert result.ok is True
        names = {s.name for s in result.samples}
        # cache metrics must still be emitted
        assert "mongodb_wt_cache_used_percent" in names
        # ticket metrics must not be (stays UNKNOWN, no 0-saturation alarm)
        assert "mongodb_wt_read_tickets_available" not in names
        assert "mongodb_wt_write_tickets_available" not in names


class TestDirectCollectorVersion:
    """Version and FCV collection tests."""

    @pytest.mark.asyncio
    async def test_build_info_version(self, direct_collector, monkeypatch):
        """buildInfo -> mongodb_mongod_version must be emitted."""
        admin = MockAdmin(server_status={"ok": 1, "connections": {}})
        # add a buildInfo command handler
        orig_command = admin.command

        async def command_with_buildinfo(cmd, *args, **kwargs):
            cmd_str = str(cmd).lower() if isinstance(cmd, str) else ""
            if "buildinfo" in cmd_str:
                return {"ok": 1, "version": "7.0.26-14"}
            return await orig_command(cmd, *args, **kwargs)

        admin.command = command_with_buildinfo
        mock_client = MockClient(admin=admin)
        monkeypatch.setattr(direct_collector, "_get_client", lambda t: mock_client)

        target = TargetInDB(ip="127.0.0.1", mongo_uri="mongodb://localhost:27017", enabled=True)
        result = await direct_collector.scrape_target(target)

        assert result.ok is True
        ver = next(s for s in result.samples if s.name == "mongodb_mongod_version")
        assert ver.value == 7.0

    @pytest.mark.asyncio
    async def test_fcv_version(self, direct_collector, monkeypatch):
        """getParameter featureCompatibilityVersion → mongodb_fcv_version."""
        admin = MockAdmin(server_status={"ok": 1, "connections": {}})
        orig_command = admin.command

        async def command_with_fcv(cmd, *args, **kwargs):
            if isinstance(cmd, dict) and "getParameter" in cmd:
                return {"ok": 1, "featureCompatibilityVersion": {"version": "7.0"}}
            return await orig_command(cmd, *args, **kwargs)

        admin.command = command_with_fcv
        mock_client = MockClient(admin=admin)
        monkeypatch.setattr(direct_collector, "_get_client", lambda t: mock_client)

        target = TargetInDB(ip="127.0.0.1", mongo_uri="mongodb://localhost:27017", enabled=True)
        result = await direct_collector.scrape_target(target)

        assert result.ok is True
        fcv = next(s for s in result.samples if s.name == "mongodb_fcv_version")
        assert fcv.value == 7.0


class TestDirectCollectorSecurity:
    """Security configuration collection tests."""

    @pytest.mark.asyncio
    async def test_security_opts(self, direct_collector, monkeypatch):
        """getCmdLineOpts -> authorization, bindIp and TLS metrics."""
        admin = MockAdmin(server_status={"ok": 1, "connections": {}})
        orig_command = admin.command

        async def command_with_opts(cmd, *args, **kwargs):
            cmd_str = str(cmd).lower() if isinstance(cmd, str) else ""
            if "getcmdlineopts" in cmd_str:
                return {
                    "ok": 1,
                    "parsed": {
                        "security": {"authorization": "enabled"},
                        "net": {
                            "bindIp": "127.0.0.1,10.0.0.1",
                            "tls": {"mode": "preferTLS"},
                        },
                    },
                }
            return await orig_command(cmd, *args, **kwargs)

        admin.command = command_with_opts
        mock_client = MockClient(admin=admin)
        monkeypatch.setattr(direct_collector, "_get_client", lambda t: mock_client)

        target = TargetInDB(ip="127.0.0.1", mongo_uri="mongodb://localhost:27017", enabled=True)
        result = await direct_collector.scrape_target(target)

        assert result.ok is True
        auth = next(s for s in result.samples if s.name == "mongodb_security_authorization_enabled")
        assert auth.value == 1.0

        bindip = next(s for s in result.samples if s.name == "mongodb_net_bindip_public")
        assert bindip.value == 0.0  # not public

        tls = next(s for s in result.samples if s.name == "mongodb_net_tls_enabled")
        assert tls.value == 1.0

    @pytest.mark.asyncio
    async def test_security_auth_disabled(self, direct_collector, monkeypatch):
        """authorization: disabled -> 0.0."""
        admin = MockAdmin(server_status={"ok": 1, "connections": {}})
        orig_command = admin.command

        async def command_with_opts(cmd, *args, **kwargs):
            cmd_str = str(cmd).lower() if isinstance(cmd, str) else ""
            if "getcmdlineopts" in cmd_str:
                return {
                    "ok": 1,
                    "parsed": {
                        "security": {"authorization": "disabled"},
                        "net": {"bindIp": "0.0.0.0"},
                    },
                }
            return await orig_command(cmd, *args, **kwargs)

        admin.command = command_with_opts
        mock_client = MockClient(admin=admin)
        monkeypatch.setattr(direct_collector, "_get_client", lambda t: mock_client)

        target = TargetInDB(ip="127.0.0.1", mongo_uri="mongodb://localhost:27017", enabled=True)
        result = await direct_collector.scrape_target(target)

        assert result.ok is True
        auth = next(s for s in result.samples if s.name == "mongodb_security_authorization_enabled")
        assert auth.value == 0.0

        bindip = next(s for s in result.samples if s.name == "mongodb_net_bindip_public")
        assert bindip.value == 1.0  # 0.0.0.0 = public

    @staticmethod
    def _opts_admin(parsed: dict):
        """MockAdmin whose getCmdLineOpts returns the given `parsed` document."""
        admin = MockAdmin(server_status={"ok": 1, "connections": {}})
        orig_command = admin.command

        async def command_with_opts(cmd, *args, **kwargs):
            cmd_str = str(cmd).lower() if isinstance(cmd, str) else ""
            if "getcmdlineopts" in cmd_str:
                return {"ok": 1, "parsed": parsed}
            return await orig_command(cmd, *args, **kwargs)

        admin.command = command_with_opts
        return admin

    @pytest.mark.asyncio
    async def test_authorization_absent_emits_no_sample(self, direct_collector, monkeypatch):
        """Regression: a missing security.authorization is not "disabled".

        The first implementation defaulted the missing key to "disabled", which
        reported auth as off on correctly secured clusters and -- combined with a
        public bindIp -- produced a CRITICAL. Emitting nothing leaves the rule
        UNKNOWN instead of raising a false alarm.
        """
        admin = self._opts_admin({"net": {"bindIp": "0.0.0.0"}})
        monkeypatch.setattr(direct_collector, "_get_client", lambda t: MockClient(admin=admin))

        target = TargetInDB(ip="127.0.0.1", mongo_uri="mongodb://localhost:27017", enabled=True)
        result = await direct_collector.scrape_target(target)

        auth = [s for s in result.samples if s.name == "mongodb_security_authorization_enabled"]
        assert auth == [], "no authorization sample should be emitted when it is not reported"
        # bindIp is still reported
        bindip = next(s for s in result.samples if s.name == "mongodb_net_bindip_public")
        assert bindip.value == 1.0

    @pytest.mark.asyncio
    async def test_keyfile_implies_authorization_enabled(self, direct_collector, monkeypatch):
        """keyFile deployments enable authorization implicitly."""
        admin = self._opts_admin(
            {"security": {"keyFile": "/etc/mongo/keyfile"}, "net": {"bindIp": "0.0.0.0"}}
        )
        monkeypatch.setattr(direct_collector, "_get_client", lambda t: MockClient(admin=admin))

        target = TargetInDB(ip="127.0.0.1", mongo_uri="mongodb://localhost:27017", enabled=True)
        result = await direct_collector.scrape_target(target)

        auth = next(s for s in result.samples if s.name == "mongodb_security_authorization_enabled")
        assert auth.value == 1.0

    @pytest.mark.asyncio
    async def test_cluster_auth_mode_implies_authorization_enabled(
        self, direct_collector, monkeypatch
    ):
        admin = self._opts_admin(
            {"security": {"clusterAuthMode": "x509"}, "net": {"bindIp": "127.0.0.1"}}
        )
        monkeypatch.setattr(direct_collector, "_get_client", lambda t: MockClient(admin=admin))

        target = TargetInDB(ip="127.0.0.1", mongo_uri="mongodb://localhost:27017", enabled=True)
        result = await direct_collector.scrape_target(target)

        auth = next(s for s in result.samples if s.name == "mongodb_security_authorization_enabled")
        assert auth.value == 1.0


class TestSlowQueryRedaction:
    """Slow-query values must not leave the monitored database by default.

    Filter values come straight out of production queries and can carry personal
    data; they end up in the metadata database and on the dashboard. Index
    suggestions are built from field names, so the values are not needed.
    """

    def test_values_are_replaced_with_type_placeholders(self):
        from app.collector import _redact_values

        redacted = _redact_values(
            {
                "find": "subscribers",
                "filter": {
                    "msisdn": "905551234567",
                    "age": {"$gt": 30},
                    "active": True,
                    "$or": [{"email": "a@b.com"}, {"customerId": 12345678901}],
                },
            }
        )

        assert redacted["filter"]["msisdn"] == "<str>"
        assert redacted["filter"]["age"]["$gt"] == "<int>"
        assert redacted["filter"]["active"] == "<bool>"
        assert redacted["filter"]["$or"][0]["email"] == "<str>"
        assert redacted["filter"]["$or"][1]["customerId"] == "<int>"

        # no original value survives anywhere in the output
        blob = json.dumps(redacted)
        for secret in ("905551234567", "a@b.com", "12345678901"):
            assert secret not in blob

    def test_query_shape_survives_redaction(self):
        """Field names, collection, sort direction and limit stay readable."""
        from app.collector import _redact_values

        redacted = _redact_values(
            {
                "find": "subscribers",
                "filter": {"msisdn": "905551234567"},
                "sort": {"createdAt": -1, "name": 1},
                "projection": {"_id": 0, "msisdn": 1},
                "limit": 20,
            }
        )

        assert redacted["find"] == "subscribers"
        assert "msisdn" in redacted["filter"]
        assert redacted["sort"] == {"createdAt": -1, "name": 1}
        assert redacted["projection"] == {"_id": 0, "msisdn": 1}
        assert redacted["limit"] == 20

    def test_detail_extraction_redacts_by_default(self, monkeypatch):
        from app.collector import DirectCollector

        monkeypatch.setattr(settings, "slow_query_store_values", False)
        detail = DirectCollector()._extract_slow_query_detail(
            "shop",
            {
                "ns": "shop.orders",
                "millis": 1500,
                "command": {"find": "orders", "filter": {"email": "user@example.com"}},
            },
        )
        assert "user@example.com" not in json.dumps(detail["command"])
        # field names are still extracted for the index suggester
        assert "email" in detail["filter_keys"]

    def test_detail_extraction_keeps_values_when_opted_in(self, monkeypatch):
        from app.collector import DirectCollector

        monkeypatch.setattr(settings, "slow_query_store_values", True)
        detail = DirectCollector()._extract_slow_query_detail(
            "shop",
            {
                "ns": "shop.orders",
                "millis": 1500,
                "command": {"find": "orders", "filter": {"email": "user@example.com"}},
            },
        )
        assert "user@example.com" in json.dumps(detail["command"])
