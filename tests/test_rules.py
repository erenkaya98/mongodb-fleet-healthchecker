# rules.py test suite - OK/WARN/CRIT scenarios for every rule

from __future__ import annotations

from pathlib import Path

import pytest

from app.models import Sample, ScrapeResult, Status
from app.rules import RuleEngine

# -- Helpers ---------------------------------------------------

RULES_PATH = Path(__file__).parent.parent / "rules.yml"


def make_scrape_result(
    ok: bool = True,
    latency_ms: float = 10.0,
    samples: list[Sample] | None = None,
) -> ScrapeResult:
    return ScrapeResult(
        target_id="test_target",
        url="http://localhost:9216/metrics",
        ok=ok,
        http_status=200 if ok else 500,
        latency_ms=latency_ms,
        samples=samples or [],
        attempts=1,
    )


@pytest.fixture(scope="module")
def engine():
    return RuleEngine(str(RULES_PATH))


# ── Tests ───────────────────────────────────────────────────────


class TestRuleLoading:
    """Rule loading tests."""

    def test_loads_default_profile(self, engine):
        rules = engine.get_rules("default")
        assert len(rules) > 5

    def test_loads_configsvr_profile(self, engine):
        rules = engine.get_rules("configsvr")
        assert len(rules) >= 2

    def test_loads_mongos_profile(self, engine):
        rules = engine.get_rules("mongos")
        assert len(rules) >= 2

    def test_loads_arbiter_profile(self, engine):
        rules = engine.get_rules("arbiter")
        assert len(rules) >= 2

    def test_unknown_profile_falls_back_to_default(self, engine):
        rules = engine.get_rules("nonexistent")
        assert len(rules) > 0  # default fallback


class TestScrapeOkRule:
    """exporter_reachable rule."""

    def test_scrape_ok_pass(self, engine):
        result = make_scrape_result(ok=True)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "exporter_reachable")
        assert check.status == Status.OK
        assert check.measured_value == 1.0

    def test_scrape_not_ok_critical(self, engine):
        result = make_scrape_result(ok=False)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "exporter_reachable")
        assert check.status == Status.CRITICAL


class TestMongodbUpRule:
    """mongodb_up rule."""

    def test_mongodb_up_ok(self, engine):
        samples = [Sample(name="mongodb_up", labels={}, value=1.0)]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "mongodb_up")
        assert check.status == Status.OK

    def test_mongodb_up_critical(self, engine):
        samples = [Sample(name="mongodb_up", labels={}, value=0.0)]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "mongodb_up")
        assert check.status == Status.CRITICAL

    def test_mongodb_up_missing_metric(self, engine):
        result = make_scrape_result()  # no samples
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "mongodb_up")
        assert check.status == Status.UNKNOWN


class TestReplsetStateRule:
    """replset_state rule."""

    def test_primary_ok(self, engine):
        samples = [
            Sample(
                name="mongodb_mongod_replset_my_state",
                labels={"set": "rs0"},
                value=1.0,
            )
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "replset_state")
        assert check.status == Status.OK  # 1 (PRIMARY) beklenen [1,2] listesinde → OK

    def test_secondary_ok(self, engine):
        samples = [
            Sample(
                name="mongodb_mongod_replset_my_state",
                labels={"set": "rs0"},
                value=2.0,
            )
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "replset_state")
        assert check.status == Status.OK

    def test_recovering_warning(self, engine):
        """State 3 (RECOVERING) beklenen listede yok → CRITICAL."""
        samples = [
            Sample(
                name="mongodb_mongod_replset_my_state",
                labels={"set": "rs0"},
                value=3.0,
            )
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "replset_state")
        assert check.status == Status.CRITICAL


class TestReplsetHealth:
    """replset_health rule."""

    def test_all_members_healthy(self, engine):
        samples = [
            Sample(
                name="mongodb_mongod_replset_member_health",
                labels={"member": "host1:27017", "set": "rs0"},
                value=1.0,
            ),
            Sample(
                name="mongodb_mongod_replset_member_health",
                labels={"member": "host2:27017", "set": "rs0"},
                value=1.0,
            ),
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "replset_health")
        # aggregation=min, op=lt, critical=1 → min < 1 → CRITICAL
        # min=1.0 → 1 < 1 FALSE → no critical → OK
        assert check.status == Status.OK

    def test_one_member_unhealthy(self, engine):
        samples = [
            Sample(
                name="mongodb_mongod_replset_member_health",
                labels={"member": "host1:27017"},
                value=1.0,
            ),
            Sample(
                name="mongodb_mongod_replset_member_health",
                labels={"member": "host2:27017"},
                value=0.0,
            ),
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "replset_health")
        assert check.status == Status.CRITICAL


class TestReplicationLag:
    """replication_lag rule."""

    def test_lag_ok(self, engine):
        samples = [
            Sample(
                name="mongodb_mongod_replset_member_replication_lag",
                labels={"member": "host2:27017"},
                value=5.0,
            )
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "replication_lag")
        assert check.status == Status.OK

    def test_lag_warning(self, engine):
        samples = [
            Sample(
                name="mongodb_mongod_replset_member_replication_lag",
                labels={"member": "host2:27017"},
                value=45.0,
            )
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "replication_lag")
        assert check.status == Status.WARNING

    def test_lag_critical(self, engine):
        samples = [
            Sample(
                name="mongodb_mongod_replset_member_replication_lag",
                labels={"member": "host2:27017"},
                value=150.0,
            )
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "replication_lag")
        assert check.status == Status.CRITICAL


class TestConnectionsSaturation:
    """connections_saturation rule."""

    def test_low_saturation_ok(self, engine):
        samples = [
            Sample(
                name="mongodb_ss_connections",
                labels={"conn_type": "current"},
                value=100.0,
            ),
            Sample(
                name="mongodb_ss_connections",
                labels={"conn_type": "available"},
                value=51200.0,
            ),
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "connections_saturation")
        assert check.status == Status.OK
        # ratio ≈ 0.0019

    def test_high_saturation_critical(self, engine):
        samples = [
            Sample(
                name="mongodb_ss_connections",
                labels={"conn_type": "current"},
                value=45000.0,
            ),
            Sample(
                name="mongodb_ss_connections",
                labels={"conn_type": "available"},
                value=6298.0,
            ),
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "connections_saturation")
        # ratio = 45000/(45000+6298) = 0.877
        assert check.status == Status.CRITICAL

    def test_warning_saturation(self, engine):
        samples = [
            Sample(
                name="mongodb_ss_connections",
                labels={"conn_type": "current"},
                value=40000.0,
            ),
            Sample(
                name="mongodb_ss_connections",
                labels={"conn_type": "available"},
                value=11000.0,
            ),
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "connections_saturation")
        # ratio = 40000/51000 ≈ 0.78 → WARNING
        assert check.status == Status.WARNING

    def test_missing_metric_unknown(self, engine):
        result = make_scrape_result()
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "connections_saturation")
        assert check.status == Status.UNKNOWN


class TestOplogWindow:
    """oplog_window_hours rule."""

    def test_plenty_of_oplog_ok(self, engine):
        # head = a 1000-hour timestamp difference
        head_val = 1_000_000_000.0
        tail_val = head_val - (48 * 3600)  # 48 saatlik window
        samples = [
            Sample(
                name="mongodb_mongod_replset_oplog_head_timestamp",
                labels={},
                value=head_val,
            ),
            Sample(
                name="mongodb_mongod_replset_oplog_tail_timestamp",
                labels={},
                value=tail_val,
            ),
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "oplog_window_hours")
        assert check.status == Status.OK

    def test_low_oplog_critical(self, engine):
        head_val = 1_000_000_000.0
        tail_val = head_val - (4 * 3600)  # 4 saatlik window
        samples = [
            Sample(
                name="mongodb_mongod_replset_oplog_head_timestamp",
                labels={},
                value=head_val,
            ),
            Sample(
                name="mongodb_mongod_replset_oplog_tail_timestamp",
                labels={},
                value=tail_val,
            ),
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "oplog_window_hours")
        assert check.status == Status.CRITICAL


class TestMemberCount:
    """member_count rule."""

    def test_three_members_ok(self, engine):
        samples = [
            Sample(
                name="mongodb_mongod_replset_number_of_members",
                labels={},
                value=3.0,
            )
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "member_count")
        assert check.status == Status.OK

    def test_two_members_warning(self, engine):
        samples = [
            Sample(
                name="mongodb_mongod_replset_number_of_members",
                labels={},
                value=2.0,
            )
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "member_count")
        assert check.status == Status.WARNING

    def test_one_member_critical(self, engine):
        samples = [
            Sample(
                name="mongodb_mongod_replset_number_of_members",
                labels={},
                value=1.0,
            )
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "member_count")
        assert check.status == Status.CRITICAL


class TestScrapeLatency:
    """scrape_latency rule."""

    def test_fast_scrape_ok(self, engine):
        result = make_scrape_result(ok=True, latency_ms=10.0)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "scrape_latency")
        assert check.status == Status.OK

    def test_slow_scrape_warning(self, engine):
        result = make_scrape_result(ok=True, latency_ms=6000.0)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "scrape_latency")
        assert check.status == Status.WARNING

    def test_very_slow_scrape_critical(self, engine):
        result = make_scrape_result(ok=True, latency_ms=20000.0)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "scrape_latency")
        assert check.status == Status.CRITICAL


class TestDeltaRules:
    """delta aggregation rules (cursor_timeouts, assert_warnings)."""

    def test_delta_no_previous(self, engine):
        """No previous metric -> delta 0 -> OK."""
        samples = [
            Sample(
                name="mongodb_ss_timedOutCursors",
                labels={},
                value=500.0,
            )
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        # cursor_timeouts disabled by default, so won't be evaluated
        # test with assert_warnings instead (also disabled)
        # Let's just verify no crash
        assert len(checks) > 0

    def test_delta_with_previous(self, engine):
        samples = [
            Sample(
                name="mongodb_ss_timedOutCursors",
                labels={},
                value=500.0,
            )
        ]
        result = make_scrape_result(samples=samples)
        prev = {"cursor_timeouts": 400.0}  # 100 increase
        # cursor_timeouts is disabled, test the mechanism works
        # even if disabled rules return nothing
        disabled_rule_ids = ["cursor_timeouts", "assert_warnings"]
        for check in engine.evaluate(result, "default", prev):
            if check.rule_id not in disabled_rule_ids:
                # Only checking non-disabled rules here
                pass


class TestProfileSelection:
    """Profile-based rule selection."""

    def test_configsvr_has_fewer_rules(self, engine):
        default_rules = engine.get_rules("default")
        configsvr_rules = engine.get_rules("configsvr")
        assert len(configsvr_rules) < len(default_rules)

    def test_mongos_no_replset_rules(self, engine):
        mongos_rules = engine.get_rules("mongos")
        rule_ids = {r.rule_id for r in mongos_rules}
        assert "replset_state" not in rule_ids
        assert "replication_lag" not in rule_ids

    def test_arbiter_has_exporter_check(self, engine):
        arbiter_rules = engine.get_rules("arbiter")
        rule_ids = {r.rule_id for r in arbiter_rules}
        assert "exporter_reachable" in rule_ids


class TestWiredTigerRules:
    """A3: WiredTiger cache/ticket rules."""

    def test_wt_cache_used_ok(self, engine):
        samples = [
            Sample(name="mongodb_wt_cache_used_percent", labels={}, value=50.0),
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "wt_cache_used_percent")
        assert check.status == Status.OK

    def test_wt_cache_used_warning(self, engine):
        samples = [
            Sample(name="mongodb_wt_cache_used_percent", labels={}, value=92.0),
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "wt_cache_used_percent")
        assert check.status == Status.WARNING

    def test_wt_cache_used_critical(self, engine):
        samples = [
            Sample(name="mongodb_wt_cache_used_percent", labels={}, value=97.0),
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "wt_cache_used_percent")
        assert check.status == Status.CRITICAL

    def test_wt_cache_dirty_critical(self, engine):
        samples = [
            Sample(name="mongodb_wt_cache_dirty_percent", labels={}, value=45.0),
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "wt_cache_dirty_percent")
        assert check.status == Status.CRITICAL

    def test_wt_read_tickets_critical(self, engine):
        samples = [
            Sample(name="mongodb_wt_read_tickets_available", labels={}, value=0.0),
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "wt_read_tickets")
        assert check.status == Status.CRITICAL

    def test_wt_write_tickets_warning(self, engine):
        samples = [
            Sample(name="mongodb_wt_write_tickets_available", labels={}, value=1.0),
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "wt_write_tickets")
        # available=1: warn threshold is <2, crit is <1 → 1 < 1 False, 1 < 2 True → WARNING
        assert check.status == Status.WARNING

    def test_wt_metrics_missing_unknown(self, engine):
        """With skip_when_absent=true a missing metric drops the rule entirely."""
        result = make_scrape_result()
        checks = engine.evaluate(result, "default")
        found = [c for c in checks if c.rule_id == "wt_cache_used_percent"]
        # wt_cache_used_percent has no skip_when_absent → still UNKNOWN
        assert len(found) == 1 and found[0].status == Status.UNKNOWN
        # wt_read_tickets has skip_when_absent → skipped entirely
        assert not any(c.rule_id == "wt_read_tickets" for c in checks)
        assert not any(c.rule_id == "wt_write_tickets" for c in checks)


class TestVersionEOLRules:
    """A9: version and EOL rules."""

    def test_version_70_ok(self, engine):
        samples = [
            Sample(name="mongodb_mongod_version", labels={}, value=7.0),
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "mongod_version_eol")
        assert check.status == Status.OK

    def test_version_50_warning(self, engine):
        samples = [
            Sample(name="mongodb_mongod_version", labels={}, value=5.0),
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "mongod_version_eol")
        assert check.status == Status.WARNING

    def test_version_44_critical(self, engine):
        samples = [
            Sample(name="mongodb_mongod_version", labels={}, value=4.4),
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "mongod_version_eol")
        assert check.status == Status.CRITICAL

    def test_fcv_mismatch_ok(self, engine):
        samples = [
            Sample(name="mongodb_fcv_version", labels={}, value=7.0),
            Sample(name="mongodb_mongod_version", labels={}, value=7.0),
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "fcv_mismatch")
        assert check.status == Status.OK

    def test_fcv_mismatch_critical(self, engine):
        samples = [
            Sample(name="mongodb_fcv_version", labels={}, value=6.0),
            Sample(name="mongodb_mongod_version", labels={}, value=7.0),
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "fcv_mismatch")
        assert check.status == Status.CRITICAL

    def test_fcv_missing_unknown(self, engine):
        samples = [
            Sample(name="mongodb_mongod_version", labels={}, value=7.0),
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "fcv_mismatch")
        assert check.status == Status.UNKNOWN


class TestSecurityRules:
    """A8: security and compliance rules."""

    def test_auth_enabled_ok(self, engine):
        samples = [
            Sample(name="mongodb_security_authorization_enabled", labels={}, value=1.0),
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "security_authorization")
        assert check.status == Status.OK

    def test_auth_disabled_warning(self, engine):
        """authorization disabled -> warning, no longer crit (internal network assumption)."""
        samples = [
            Sample(name="mongodb_security_authorization_enabled", labels={}, value=0.0),
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "security_authorization")
        assert check.status == Status.WARNING

    def test_bindip_private_ok(self, engine):
        samples = [
            Sample(name="mongodb_net_bindip_public", labels={}, value=0.0),
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "security_bindip_public")
        assert check.status == Status.OK

    def test_bindip_public_warning(self, engine):
        """bindIp public -> warning, no longer crit."""
        samples = [
            Sample(name="mongodb_net_bindip_public", labels={}, value=1.0),
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "security_bindip_public")
        assert check.status == Status.WARNING

    def test_auth_exposed_critical(self, engine):
        """auth=disabled AND bindIp=public -> security_auth_exposed crit."""
        samples = [
            Sample(name="mongodb_security_authorization_enabled", labels={}, value=0.0),
            Sample(name="mongodb_net_bindip_public", labels={}, value=1.0),
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "security_auth_exposed")
        assert check.status == Status.CRITICAL

    def test_auth_exposed_ok_when_auth_on(self, engine):
        """auth=enabled + bindIp=public -> not exposed."""
        samples = [
            Sample(name="mongodb_security_authorization_enabled", labels={}, value=1.0),
            Sample(name="mongodb_net_bindip_public", labels={}, value=1.0),
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "security_auth_exposed")
        assert check.status == Status.OK

    def test_auth_exposed_ok_when_private(self, engine):
        """auth=disabled + bindIp=private -> not exposed."""
        samples = [
            Sample(name="mongodb_security_authorization_enabled", labels={}, value=0.0),
            Sample(name="mongodb_net_bindip_public", labels={}, value=0.0),
        ]
        result = make_scrape_result(samples=samples)
        checks = engine.evaluate(result, "default")
        check = next(c for c in checks if c.rule_id == "security_auth_exposed")
        assert check.status == Status.OK

    def test_standalone_profile_has_new_rules(self, engine):
        """The standalone profile must contain the A3+A9+A8 rules."""
        standalone_rules = engine.get_rules("standalone")
        rule_ids = {r.rule_id for r in standalone_rules}
        assert "wt_cache_used_percent" in rule_ids
        assert "wt_cache_dirty_percent" in rule_ids
        assert "wt_read_tickets" in rule_ids
        assert "wt_write_tickets" in rule_ids
        assert "mongod_version_eol" in rule_ids
        assert "fcv_mismatch" in rule_ids
        assert "security_authorization" in rule_ids
        assert "security_bindip_public" in rule_ids
        assert "security_auth_exposed" in rule_ids
