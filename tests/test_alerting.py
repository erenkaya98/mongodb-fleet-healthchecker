# alerting.py test suite — state machine transitions
# ====================================================
# Tests PENDING→FIRING→RENOTIFY→RESOLVED transitions
# Mocks store functions to isolate state machine logic.

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import aiohttp

from app.models import (
    AlertRecord,
    AlertState,
    Status,
    CheckResult,
    TargetInDB,
    TargetResult,
    TargetState,
)
from app.alerting import (
    make_fingerprint,
    process_check_result,
    _handle_ok,
    _handle_bad,
    _to_slack,
    _to_teams,
    _format_payload,
)


@pytest.fixture
def sample_target():
    return TargetInDB(
        id="507f1f77bcf86cd799439011",
        ip="10.0.1.100",
        mongo_port=27017,
        hostname="db-prod-01",
        cluster="prod-cluster-1",
        env="production",
        role="shardsvr",
        enabled=True,
    )


@pytest.fixture
def sample_check_ok():
    return CheckResult(
        rule_id="mongodb_up",
        rule_title="MongoDB process up",
        status=Status.OK,
        measured_value=1.0,
    )


@pytest.fixture
def sample_check_critical():
    return CheckResult(
        rule_id="mongodb_up",
        rule_title="MongoDB process up",
        status=Status.CRITICAL,
        measured_value=0.0,
        critical_threshold=1.0,
        message="MongoDB process up: 0.0 (crit > 1.0)",
    )


@pytest.fixture
def sample_check_warning():
    return CheckResult(
        rule_id="replication_lag",
        rule_title="Replikasyon gecikmesi",
        status=Status.WARNING,
        measured_value=45.0,
        warning_threshold=30.0,
        critical_threshold=120.0,
        message="Replikasyon gecikmesi: 45.0 s (warn > 30.0 s)",
    )


@pytest.fixture
def sample_target_result():
    return TargetResult(
        run_id="test-run-001",
        target_id="507f1f77bcf86cd799439011",
        ts=__import__("datetime").datetime(2025, 1, 1, 12, 0, 0),
        status=Status.CRITICAL,
        scrape_ok=True,
    )


# ── Tests ───────────────────────────────────────────────────────


class TestFingerprinting:
    """Fingerprint generation."""

    def test_same_inputs_same_fingerprint(self):
        fp1 = make_fingerprint("target1", "rule1")
        fp2 = make_fingerprint("target1", "rule1")
        assert fp1 == fp2

    def test_different_inputs_different_fingerprint(self):
        fp1 = make_fingerprint("target1", "rule1")
        fp2 = make_fingerprint("target2", "rule1")
        assert fp1 != fp2

    def test_length(self):
        fp = make_fingerprint("abc", "def")
        assert len(fp) == 16


class TestFormatting:
    """Payload formatlama testleri."""

    def test_slack_format(self):
        payload = {
            "type": "alert",
            "state": "firing",
            "severity": "critical",
            "title": "🔥 Test Alert",
            "target": {"hostname": "db01", "cluster": "prod", "env": "production", "role": "shardsvr"},
            "result": {"measured_value": 0.0, "message": "test"},
            "dashboard_url": "http://localhost:8000",
        }
        result = _to_slack(payload)
        assert "attachments" in result
        assert len(result["attachments"]) == 1
        assert "blocks" in result["attachments"][0]

    def test_teams_format(self):
        payload = {
            "type": "alert",
            "state": "firing",
            "severity": "warning",
            "title": "⚠️ Warning",
            "target": {"hostname": "db02", "cluster": "staging", "env": "staging", "role": "default"},
            "result": {"message": "test"},
            "dashboard_url": "http://localhost:8000",
        }
        result = _to_teams(payload)
        assert result["@type"] == "MessageCard"
        assert result["themeColor"] == "FFAA00"

    def test_critical_teams_color(self):
        payload = {
            "severity": "critical",
            "title": "test",
            "target": {"hostname": "x", "cluster": "x", "env": "x", "role": "x"},
            "result": {},
        }
        result = _to_teams(payload)
        assert result["themeColor"] == "FF0000"


class TestAlertStateMachine:
    """Alert state machine transitions (mocked store)."""

    @pytest.mark.asyncio
    async def test_new_bad_result_creates_pending(
        self, sample_target, sample_check_critical, sample_target_result
    ):
        """First bad result -> PENDING."""
        with patch("app.alerting.get_alert", new=AsyncMock(return_value=None)):
            with patch("app.alerting.upsert_alert", new=AsyncMock()) as mock_upsert:
                with patch("app.alerting._send_webhook", new=AsyncMock()):
                    await _handle_bad(
                        sample_check_critical,
                        sample_target,
                        sample_target_result,
                        aiohttp.ClientSession(),  # placeholder
                    )
                    assert mock_upsert.called
                    alert = mock_upsert.call_args[0][0]
                    assert isinstance(alert, AlertRecord)
                    assert alert.state == AlertState.PENDING
                    assert alert.severity == Status.CRITICAL

    @pytest.mark.asyncio
    async def test_pending_stays_pending_on_consecutive_one(
        self, sample_target, sample_check_critical, sample_target_result
    ):
        """PENDING durumdayken consecutive=1 → hâlâ PENDING."""
        existing = AlertRecord(
            fingerprint=make_fingerprint(
                str(sample_target.id), sample_check_critical.rule_id
            ),
            target_id=str(sample_target.id),
            rule_id=sample_check_critical.rule_id,
            severity=Status.CRITICAL,
            state=AlertState.PENDING,
        )
        target_state = TargetState(
            target_id=str(sample_target.id),
            status=Status.CRITICAL,
            consecutive_failures=1,
        )

        with patch("app.alerting.get_alert", new=AsyncMock(return_value=existing)):
            with patch("app.alerting.get_target_state", new=AsyncMock(return_value=target_state)):
                with patch("app.alerting.upsert_alert", new=AsyncMock()) as mock_upsert:
                    with patch("app.alerting._send_webhook", new=AsyncMock()) as mock_send:
                        await _handle_bad(
                            sample_check_critical,
                            sample_target,
                            sample_target_result,
                            aiohttp.ClientSession(),
                        )
                        assert mock_upsert.called
                        alert = mock_upsert.call_args[0][0]
                        assert alert.state == AlertState.PENDING
                        assert not mock_send.called

    @pytest.mark.asyncio
    async def test_pending_to_firing_on_consecutive_threshold(
        self, sample_target, sample_check_critical, sample_target_result
    ):
        """PENDING + consecutive=2 -> FIRING, webhook must be sent."""
        existing = AlertRecord(
            fingerprint=make_fingerprint(
                str(sample_target.id), sample_check_critical.rule_id
            ),
            target_id=str(sample_target.id),
            rule_id=sample_check_critical.rule_id,
            severity=Status.CRITICAL,
            state=AlertState.PENDING,
        )
        target_state = TargetState(
            target_id=str(sample_target.id),
            status=Status.CRITICAL,
            consecutive_failures=2,  # threshold
        )

        with patch("app.alerting.get_alert", new=AsyncMock(return_value=existing)):
            with patch("app.alerting.get_target_state", new=AsyncMock(return_value=target_state)):
                with patch("app.alerting.upsert_alert", new=AsyncMock()) as mock_upsert:
                    with patch("app.alerting._send_webhook", new=AsyncMock()) as mock_send:
                        await _handle_bad(
                            sample_check_critical,
                            sample_target,
                            sample_target_result,
                            aiohttp.ClientSession(),
                        )
                        alert = mock_upsert.call_args[0][0]
                        assert alert.state == AlertState.FIRING
                        assert alert.send_count == 1
                        assert mock_send.called

    @pytest.mark.asyncio
    async def test_firing_renotify_after_cooldown(
        self, sample_target, sample_check_critical, sample_target_result
    ):
        """While FIRING, renotify once the cooldown has passed."""
        from datetime import datetime, timezone, timedelta
        existing = AlertRecord(
            fingerprint=make_fingerprint(
                str(sample_target.id), sample_check_critical.rule_id
            ),
            target_id=str(sample_target.id),
            rule_id=sample_check_critical.rule_id,
            severity=Status.CRITICAL,
            state=AlertState.FIRING,
            last_sent_at=datetime.now(timezone.utc) - timedelta(minutes=61),
            send_count=1,
        )

        with patch("app.alerting.get_alert", new=AsyncMock(return_value=existing)):
            with patch("app.alerting.upsert_alert", new=AsyncMock()) as mock_upsert:
                with patch("app.alerting._send_webhook", new=AsyncMock()) as mock_send:
                    await _handle_bad(
                        sample_check_critical,
                        sample_target,
                        sample_target_result,
                        aiohttp.ClientSession(),
                    )
                    assert mock_send.called
                    alert = mock_upsert.call_args[0][0]
                    assert alert.send_count == 2

    @pytest.mark.asyncio
    async def test_firing_no_renotify_within_cooldown(
        self, sample_target, sample_check_critical, sample_target_result
    ):
        """FIRING durumda, cooldown dolmadan bildirim gitmesin."""
        from datetime import datetime, timezone, timedelta
        existing = AlertRecord(
            fingerprint=make_fingerprint(
                str(sample_target.id), sample_check_critical.rule_id
            ),
            target_id=str(sample_target.id),
            rule_id=sample_check_critical.rule_id,
            severity=Status.CRITICAL,
            state=AlertState.FIRING,
            last_sent_at=datetime.now(timezone.utc) - timedelta(minutes=5),
            send_count=1,
        )

        with patch("app.alerting.get_alert", new=AsyncMock(return_value=existing)):
            with patch("app.alerting.upsert_alert", new=AsyncMock()):
                with patch("app.alerting._send_webhook", new=AsyncMock()) as mock_send:
                    await _handle_bad(
                        sample_check_critical,
                        sample_target,
                        sample_target_result,
                        aiohttp.ClientSession(),
                    )
                    assert not mock_send.called

    @pytest.mark.asyncio
    async def test_escalation_bypasses_cooldown(
        self, sample_target, sample_target_result
    ):
        """WARNING -> CRITICAL escalation must bypass the cooldown."""
        from datetime import datetime, timezone, timedelta

        check_crit = CheckResult(
            rule_id="replication_lag",
            rule_title="Replikasyon gecikmesi",
            status=Status.CRITICAL,
            measured_value=150.0,
            critical_threshold=120.0,
        )

        existing = AlertRecord(
            fingerprint=make_fingerprint(str(sample_target.id), check_crit.rule_id),
            target_id=str(sample_target.id),
            rule_id=check_crit.rule_id,
            severity=Status.WARNING,  # previously WARNING
            state=AlertState.FIRING,
            last_sent_at=datetime.now(timezone.utc) - timedelta(minutes=5),  # inside the cooldown
        )

        with patch("app.alerting.get_alert", new=AsyncMock(return_value=existing)):
            with patch("app.alerting.upsert_alert", new=AsyncMock()):
                with patch("app.alerting._send_webhook", new=AsyncMock()) as mock_send:
                    await _handle_bad(
                        check_crit,
                        sample_target,
                        sample_target_result,
                        aiohttp.ClientSession(),
                    )
                    assert mock_send.called  # escalation bypass

    @pytest.mark.asyncio
    async def test_ok_resolves_firing_alert(
        self, sample_target, sample_check_ok, sample_target_result
    ):
        """OK gelince FIRING → RESOLVED + resolve bildirimi."""
        existing = AlertRecord(
            fingerprint=make_fingerprint(
                str(sample_target.id), sample_check_ok.rule_id
            ),
            target_id=str(sample_target.id),
            rule_id=sample_check_ok.rule_id,
            severity=Status.CRITICAL,
            state=AlertState.FIRING,
        )

        with patch("app.alerting.get_alert", new=AsyncMock(return_value=existing)):
            with patch("app.alerting.upsert_alert", new=AsyncMock()) as mock_upsert:
                with patch("app.alerting._send_webhook", new=AsyncMock()) as mock_send:
                    await _handle_ok(
                        sample_check_ok,
                        sample_target,
                        sample_target_result,
                        aiohttp.ClientSession(),
                    )
                    alert = mock_upsert.call_args[0][0]
                    assert alert.state == AlertState.RESOLVED
                    assert alert.resolved_at is not None
                    assert mock_send.called  # resolve bildirimi

    @pytest.mark.asyncio
    async def test_resolved_to_pending_on_new_bad(
        self, sample_target, sample_check_critical, sample_target_result
    ):
        """New bad result while RESOLVED -> PENDING (re-trigger)."""
        existing = AlertRecord(
            fingerprint=make_fingerprint(
                str(sample_target.id), sample_check_critical.rule_id
            ),
            target_id=str(sample_target.id),
            rule_id=sample_check_critical.rule_id,
            severity=Status.CRITICAL,
            state=AlertState.RESOLVED,
        )

        with patch("app.alerting.get_alert", new=AsyncMock(return_value=existing)):
            with patch("app.alerting.upsert_alert", new=AsyncMock()) as mock_upsert:
                with patch("app.alerting._send_webhook", new=AsyncMock()):
                    await _handle_bad(
                        sample_check_critical,
                        sample_target,
                        sample_target_result,
                        aiohttp.ClientSession(),
                    )
                    alert = mock_upsert.call_args[0][0]
                    assert alert.state == AlertState.PENDING
                    assert alert.resolved_at is None
                    assert alert.send_count == 0
