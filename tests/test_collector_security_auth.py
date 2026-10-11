# Where the authorization metric comes from
# ========================================
# ``getCmdLineOpts.parsed.security.authorization`` only exists when it was
# written out explicitly, in the config file or on the command line. A node can
# have access control fully enabled and still not report that key at all, so
# getCmdLineOpts cannot answer this question on its own.
#
# Both obvious readings of a missing key are wrong:
#   assume "disabled" -> false CRITICAL on a correctly secured node
#   emit nothing      -> the rule goes UNKNOWN, and because UNKNOWN outranks
#                        WARNING the node's real warnings stop being visible
#
# These tests pin the source precedence that replaced them: any positive
# evidence of auth wins, "disabled" is only claimed when something actually
# said so, and otherwise no sample is emitted.

from __future__ import annotations

import pytest

from app.collector import DirectCollector

METRIC = "mongodb_security_authorization_enabled"


class _FakeAdmin:
    def __init__(self, responses: dict, fail: set[str] | None = None):
        self._responses = responses
        self._fail = fail or set()
        self.calls: list[str] = []

    async def command(self, name, *args, **kwargs):
        self.calls.append(name)
        if name in self._fail:
            raise RuntimeError(f"{name} not permitted")
        return self._responses.get(name, {"ok": 1.0})


class _FakeClient:
    def __init__(self, responses: dict, fail: set[str] | None = None):
        self.admin = _FakeAdmin(responses, fail)


def _opts(security=None, net=None) -> dict:
    """A getCmdLineOpts reply; `security=None` omits the key entirely."""
    parsed: dict = {"net": net if net is not None else {"bindIp": "127.0.0.1"}}
    if security is not None:
        parsed["security"] = security
    return {"ok": 1.0, "parsed": parsed}


def _conn_status(users: list | None) -> dict:
    return {"ok": 1.0, "authInfo": {"authenticatedUsers": users or []}}


_A_USER = [{"user": "monitor", "db": "admin"}]


def _auth_value(samples) -> float | None:
    for s in samples:
        if s.name == METRIC:
            return s.value
    return None


@pytest.fixture
def collector() -> DirectCollector:
    return DirectCollector()


class TestPositiveEvidenceWins:
    """Anything that proves auth is on settles it."""

    @pytest.mark.asyncio
    async def test_authenticated_connection_with_no_security_key(self, collector):
        """The case that motivated the rewrite: no security key in the config,
        yet this very connection is authenticated."""
        client = _FakeClient(
            {
                "getCmdLineOpts": _opts(security=None),
                "connectionStatus": _conn_status(_A_USER),
            }
        )
        assert _auth_value(await collector._collect_security_opts(client)) == 1.0

    @pytest.mark.asyncio
    async def test_authenticated_connection_outranks_a_declared_disabled(self, collector):
        """A stale config file saying "disabled" loses to what the server is
        doing right now."""
        client = _FakeClient(
            {
                "getCmdLineOpts": _opts(security={"authorization": "disabled"}),
                "connectionStatus": _conn_status(_A_USER),
            }
        )
        assert _auth_value(await collector._collect_security_opts(client)) == 1.0

    @pytest.mark.asyncio
    async def test_keyfile_counts_as_enabled(self, collector):
        """keyFile means internal cluster auth, which enables authorization
        implicitly -- so it wins over an anonymous-looking connectionStatus
        rather than being overridden by it."""
        client = _FakeClient(
            {
                "getCmdLineOpts": _opts(security={"keyFile": "/etc/mongo/keyfile"}),
                "connectionStatus": _conn_status([]),
            }
        )
        assert _auth_value(await collector._collect_security_opts(client)) == 1.0

    @pytest.mark.asyncio
    async def test_cluster_auth_mode_counts_as_enabled(self, collector):
        client = _FakeClient(
            {
                "getCmdLineOpts": _opts(security={"clusterAuthMode": "x509"}),
                "connectionStatus": _conn_status([]),
            }
        )
        assert _auth_value(await collector._collect_security_opts(client)) == 1.0


class TestDisabledIsOnlyClaimedOnEvidence:
    @pytest.mark.asyncio
    async def test_anonymous_connection_means_disabled(self, collector):
        """Conclusive: getCmdLineOpts succeeded on an unauthenticated
        connection, so the server hands its startup configuration to anyone."""
        client = _FakeClient(
            {
                "getCmdLineOpts": _opts(security=None),
                "connectionStatus": _conn_status([]),
            }
        )
        assert _auth_value(await collector._collect_security_opts(client)) == 0.0

    @pytest.mark.asyncio
    async def test_declared_disabled_is_honoured(self, collector):
        client = _FakeClient(
            {"getCmdLineOpts": _opts(security={"authorization": "disabled"})},
            fail={"connectionStatus"},
        )
        assert _auth_value(await collector._collect_security_opts(client)) == 0.0

    @pytest.mark.asyncio
    async def test_no_source_speaks_emits_no_sample(self, collector):
        """Nothing declared and connectionStatus unavailable: UNKNOWN is the
        honest answer, so no value is invented."""
        client = _FakeClient(
            {"getCmdLineOpts": _opts(security=None)},
            fail={"connectionStatus"},
        )
        assert _auth_value(await collector._collect_security_opts(client)) is None

    @pytest.mark.asyncio
    async def test_getcmdlineopts_failure_emits_nothing_at_all(self, collector):
        client = _FakeClient({}, fail={"getCmdLineOpts"})
        assert await collector._collect_security_opts(client) == []


class TestMetricIsEmittedWheneverPossible:
    @pytest.mark.asyncio
    async def test_a_reachable_node_always_gets_a_value(self, collector):
        """The root of the UNKNOWN problem was a silently absent metric. As
        long as both commands answer, the rule now always has something to
        evaluate."""
        for security in (None, {"authorization": "enabled"}, {"keyFile": "/k"}):
            for users in ([], _A_USER):
                client = _FakeClient(
                    {
                        "getCmdLineOpts": _opts(security=security),
                        "connectionStatus": _conn_status(users),
                    }
                )
                value = _auth_value(await collector._collect_security_opts(client))
                assert value is not None, f"no metric for security={security} users={users}"


class TestSurroundingMetricsAreUnaffected:
    @pytest.mark.asyncio
    async def test_auth_enabled_on_a_public_bindip_is_not_an_exposure(self, collector):
        """The security_auth_exposed rule fires on auth-off AND public bindIp.
        A node listening on 0.0.0.0 with auth on must not trip it."""
        client = _FakeClient(
            {
                "getCmdLineOpts": _opts(security=None, net={"bindIp": "0.0.0.0"}),
                "connectionStatus": _conn_status(_A_USER),
            }
        )
        samples = {s.name: s.value for s in await collector._collect_security_opts(client)}
        assert samples[METRIC] == 1.0
        assert samples["mongodb_net_bindip_public"] == 1.0
        assert samples["mongodb_net_tls_enabled"] == 0.0
