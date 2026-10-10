# collector.py test suite — aiohttp test server ile sahte exporter

from __future__ import annotations

import asyncio
import os
import pytest
from aiohttp import web

from app.collector import MetricsCollector
from app.models import TargetInDB, Status


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
    """Sahte /metrics endpoint'i."""
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
    """Sahte exporter sunucusu."""
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
        mongodb_up_samples = [
            s for s in result.samples if s.name == "mongodb_up"
        ]
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

    def __init__(self, server_status: dict | None = None, replset_status: dict | None = None) -> None:
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
    """DirectCollector birim testleri (mock MongoDB ile)."""

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
        admin = MockAdmin(server_status={
            "ok": 1,
            "connections": {"current": 10, "available": 100},
        })
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
        admin = MockAdmin(server_status={
            "ok": 1,
            "connections": {"current": 42, "available": 51158},
        })
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
        admin = MockAdmin(
            server_status={"ok": 1, "connections": {}},
            replset_status={
                "ok": 1,
                "myState": 1,
                "members": [
                    {"name": "node1:27017", "health": 1, "secondaryDelaySecs": 0},
                    {"name": "node2:27017", "health": 1, "secondaryDelaySecs": 2},
                    {"name": "node3:27017", "health": 0, "secondaryDelaySecs": 0},
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
        count = next(s for s in result.samples if s.name == "mongodb_mongod_replset_number_of_members")
        assert count.value == 3.0

        # member health (3 samples)
        health_samples = [s for s in result.samples if s.name == "mongodb_mongod_replset_member_health"]
        assert len(health_samples) == 3
        health_by_member = {s.labels["member"]: s.value for s in health_samples}
        assert health_by_member["node1:27017"] == 1.0
        assert health_by_member["node2:27017"] == 1.0
        assert health_by_member["node3:27017"] == 0.0

        # replication lag
        lag_samples = [s for s in result.samples if s.name == "mongodb_mongod_replset_member_replication_lag"]
        assert len(lag_samples) == 3
        lag_by_member = {s.labels["member"]: s.value for s in lag_samples}
        assert lag_by_member["node2:27017"] == 2.0

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
        _clients = [MockClient(admin=admin_ok), MockClient(admin=FailingAdmin()), MockClient(admin=admin_ok)]

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

        direct_target = TargetInDB(ip="10.0.0.1", mongo_uri="mongodb://localhost:27017", enabled=True)
        assert direct_target.collection_mode == "direct"


class TestDirectCollectorWiredTiger:
    """WiredTiger metrik toplama testleri."""

    @pytest.mark.asyncio
    async def test_wt_cache_metrics(self, direct_collector, monkeypatch):
        """serverStatus.wiredTiger -> cache + ticket metrics must be emitted."""
        admin = MockAdmin(server_status={
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
        })
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
        assert "mongodb_wt_eviction_ratio" in names

        # Value assertions
        used = next(s for s in result.samples if s.name == "mongodb_wt_cache_used_percent")
        assert used.value == 50.0

        dirty = next(s for s in result.samples if s.name == "mongodb_wt_cache_dirty_percent")
        assert dirty.value == 5.0

    @pytest.mark.asyncio
    async def test_wt_metrics_missing_no_crash(self, direct_collector, monkeypatch):
        """Without WiredTiger (MMAPv1/InMemory) it must not crash, just emit no samples."""
        admin = MockAdmin(server_status={
            "ok": 1,
            "connections": {},
            # wiredTiger yok
        })
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
        admin = MockAdmin(server_status={
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
        })
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
    """Versiyon ve FCV toplama testleri."""

    @pytest.mark.asyncio
    async def test_build_info_version(self, direct_collector, monkeypatch):
        """buildInfo -> mongodb_mongod_version must be emitted."""
        admin = MockAdmin(server_status={"ok": 1, "connections": {}})
        # buildInfo command handler ekle
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
        """getCmdLineOpts → authorization + bindIp + TLS metrikleri."""
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
        """authorization: disabled → 0.0."""
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
