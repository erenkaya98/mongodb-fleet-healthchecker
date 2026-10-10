# Alert engine — webhook notifier + fingerprint dedup/cooldown/resolve
# =====================================================================
# State machine: pending → firing → (renotify) → resolved
# Severity escalation bypasses cooldown.
# Alert storm protection: 5+ targets failing the same rule -> one grouped message.

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

import aiohttp

from app.config import settings
from app.models import (
    AlertRecord,
    AlertState,
    Status,
    CheckResult,
    TargetInDB,
    TargetResult,
)
from app.store import (
    get_alert,
    upsert_alert,
    get_target_state,
)

logger = logging.getLogger(__name__)

_SEVERITY_ORDER = {
    Status.OK: 0,
    Status.WARNING: 1,
    Status.UNKNOWN: 2,
    Status.CRITICAL: 3,
}

_SEVERITY_EMOJI = {
    Status.OK: "✅",
    Status.WARNING: "⚠️",
    Status.CRITICAL: "🔥",
    Status.UNKNOWN: "❓",
}

_MIN_SEVERITY_LEVEL = _SEVERITY_ORDER.get(
    Status(settings.alert_min_severity), 1
)


def make_fingerprint(target_id: str, rule_id: str) -> str:
    """target_id + rule_id → SHA1 fingerprint."""
    raw = f"{target_id}:{rule_id}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


async def process_check_result(
    target: TargetInDB,
    check: CheckResult,
    target_result: TargetResult,
    session: aiohttp.ClientSession,
) -> None:
    """Run the alert state machine for a single check (rule) result."""
    if check.status == Status.OK:
        await _handle_ok(check, target, target_result, session)
        return

    await _handle_bad(check, target, target_result, session)


async def send_alert_storm_batch(
    rule_id: str,
    targets: list[TargetInDB],
    results: list[CheckResult],
    severity: Status,
) -> None:
    """Alert storm: 5+ targets failing the same rule -> a single message."""
    async with aiohttp.ClientSession() as session:
        payload = _build_storm_payload(rule_id, targets, results, severity)
        await _send_webhook(session, payload)


# ── Internal ───────────────────────────────────────────────────


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def _handle_ok(
    check: CheckResult,
    target: TargetInDB,
    target_result: TargetResult,
    session: aiohttp.ClientSession,
) -> None:
    """OK durumu: mevcut alert varsa resolved yap."""
    fp = make_fingerprint(str(target.id), check.rule_id)
    existing = await get_alert(fp)

    if existing and existing.state in (AlertState.PENDING, AlertState.FIRING):
        existing.state = AlertState.RESOLVED
        existing.resolved_at = _now()
        existing.last_seen_at = _now()
        await upsert_alert(existing)

        if settings.alert_send_resolve:
            payload = _build_resolve_payload(existing, target, check)
            await _send_webhook(session, payload)

        logger.info(
            "Alert resolved: %s → %s (rule=%s)",
            target.hostname or target.ip, check.rule_id, check.status.value,
        )


async def _handle_bad(
    check: CheckResult,
    target: TargetInDB,
    target_result: TargetResult,
    session: aiohttp.ClientSession,
) -> None:
    """Bad status (WARNING/CRITICAL/UNKNOWN): state machine."""
    fp = make_fingerprint(str(target.id), check.rule_id)
    existing = await get_alert(fp)
    now = _now()

    severity_level = _SEVERITY_ORDER.get(check.status, 0)

    if existing is None:
        # Yeni alert
        alert = AlertRecord(
            fingerprint=fp,
            target_id=str(target.id),
            rule_id=check.rule_id,
            severity=check.status,
            state=AlertState.PENDING,
            first_seen_at=now,
            last_seen_at=now,
            payload=_build_alert_payload(target, check),
        )
        await upsert_alert(alert)
        logger.debug("New alert PENDING: %s / %s", target.ip, check.rule_id)

    elif existing.state == AlertState.PENDING:
        # Consecutive-failure check
        target_state = await get_target_state(str(target.id))
        consecutive = target_state.consecutive_failures if target_state else 1

        if consecutive >= settings.alert_for_consecutive:
            # PENDING → FIRING
            prev_severity = _SEVERITY_ORDER.get(existing.severity, 0)
            existing.state = AlertState.FIRING
            existing.severity = check.status
            existing.last_seen_at = now
            existing.last_sent_at = now
            existing.send_count = 1
            existing.payload = _build_alert_payload(target, check)
            await upsert_alert(existing)

            await _send_webhook(session, _build_firing_payload(existing, target, check))
            logger.warning(
                "Alert FIRING: %s / %s → %s",
                target.hostname or target.ip, check.rule_id, check.status.value,
            )
        else:
            # Consecutive threshold not reached yet
            existing.last_seen_at = now
            existing.severity = check.status
            await upsert_alert(existing)

    elif existing.state == AlertState.FIRING:
        # Already firing -> check for renotify or escalation
        prev_severity = _SEVERITY_ORDER.get(existing.severity, 0)
        is_escalation = severity_level > prev_severity

        existing.last_seen_at = now
        existing.severity = check.status

        should_send = False

        if is_escalation:
            # Severity escalation → bypass cooldown
            should_send = True
            logger.warning(
                "Alert ESCALATED: %s / %s → %s",
                target.hostname or target.ip, check.rule_id, check.status.value,
            )
        elif settings.alert_renotify_minutes > 0:
            # Cooldown check
            if existing.last_sent_at:
                last_sent = existing.last_sent_at
                if last_sent.tzinfo is None:
                    last_sent = last_sent.replace(tzinfo=timezone.utc)
                minutes_since = (now - last_sent).total_seconds() / 60
                if minutes_since >= settings.alert_renotify_minutes:
                    should_send = True
            else:
                should_send = True
        else:
            should_send = True  # renotify_minutes == 0 → her seferde

        if should_send:
            existing.last_sent_at = now
            existing.send_count += 1
            existing.payload = _build_alert_payload(target, check)
            await _send_webhook(session, _build_firing_payload(existing, target, check))
            logger.info(
                "Alert RENOTIFY: %s / %s (#%d)",
                target.hostname or target.ip, check.rule_id, existing.send_count,
            )

        await upsert_alert(existing)

    elif existing.state == AlertState.RESOLVED:
        # Previously resolved, firing again -> PENDING
        existing.state = AlertState.PENDING
        existing.severity = check.status
        existing.first_seen_at = now
        existing.last_seen_at = now
        existing.resolved_at = None
        existing.send_count = 0
        existing.payload = _build_alert_payload(target, check)
        await upsert_alert(existing)
        logger.info(
            "Alert re-triggered: %s / %s → PENDING",
            target.hostname or target.ip, check.rule_id,
        )


async def _send_webhook(
    session: aiohttp.ClientSession,
    payload: dict[str, Any],
    retries: int = 2,
) -> bool:
    """Send the webhook with retries; the URL is masked in logs."""
    webhook_url = settings.webhook_url
    if not webhook_url:
        logger.debug("Webhook URL not configured, skipping send")
        return False

    url = webhook_url.get_secret_value().strip()
    if not url:
        logger.debug("Webhook URL is empty, skipping send")
        return False
    format_type = settings.webhook_format

    body = _format_payload(payload, format_type)

    masked_url = url[:30] + "..." if len(url) > 35 else url
    logger.debug("Sending webhook: %s", masked_url)

    for attempt in range(retries + 1):
        try:
            async with session.post(
                url,
                json=body,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status < 400:
                    logger.debug("Webhook OK: %d", resp.status)
                    return True
                logger.warning(
                    "Webhook HTTP %d (attempt %d/%d)",
                    resp.status, attempt + 1, retries + 1,
                )
        except Exception as e:
            logger.error(
                "Webhook send failed (attempt %d/%d): %s",
                attempt + 1, retries + 1, e,
            )

    logger.error("Webhook send failed (all retries exhausted)")
    return False


def _format_payload(
    payload: dict[str, Any], format_type: str
) -> dict[str, Any]:
    """Convert the payload to the configured format."""
    if format_type == "slack":
        return _to_slack(payload)
    elif format_type == "teams":
        return _to_teams(payload)
    return payload  # generic — direkt JSON


def _build_alert_payload(
    target: TargetInDB, check: CheckResult
) -> dict[str, Any]:
    """Alert payload (persisted to the database)."""
    return {
        "target_id": str(target.id),
        "ip": target.ip,
        "hostname": target.hostname or target.ip,
        "cluster": target.cluster or "unknown",
        "env": target.env or "unknown",
        "role": target.role or "unknown",
        "mongo_port": target.mongo_port,
        "rule_id": check.rule_id,
        "rule_title": check.rule_title,
        "status": check.status.value,
        "measured_value": check.measured_value,
        "warning_threshold": check.warning_threshold,
        "critical_threshold": check.critical_threshold,
        "message": check.message,
        "dashboard_url": (
            f"{settings.dashboard_base_url}/targets/{target.id}"
        ),
    }


def _build_firing_payload(
    alert: AlertRecord,
    target: TargetInDB,
    check: CheckResult,
) -> dict[str, Any]:
    """Firing notification payload."""
    emoji = _SEVERITY_EMOJI.get(check.status, "❓")
    return {
        "type": "alert",
        "state": "firing",
        "severity": check.status.value,
        "emoji": emoji,
        "title": f"{emoji} {check.rule_title}",
        "target": {
            "id": str(target.id),
            "ip": target.ip,
            "hostname": target.hostname or target.ip,
            "cluster": target.cluster or "unknown",
            "env": target.env or "unknown",
            "role": target.role or "unknown",
        },
        "rule": {
            "id": check.rule_id,
            "title": check.rule_title,
        },
        "result": {
            "status": check.status.value,
            "measured_value": check.measured_value,
            "warning_threshold": check.warning_threshold,
            "critical_threshold": check.critical_threshold,
            "message": check.message,
        },
        "timestamp": _now().isoformat(),
        "dashboard_url": f"{settings.dashboard_base_url}/targets/{target.id}",
        "send_count": alert.send_count,
    }


def _build_resolve_payload(
    alert: AlertRecord,
    target: TargetInDB,
    check: CheckResult,
) -> dict[str, Any]:
    """Resolve notification payload."""
    return {
        "type": "alert",
        "state": "resolved",
        "severity": "ok",
        "emoji": "✅",
        "title": f"✅ RESOLVED: {check.rule_title}",
        "target": {
            "id": str(target.id),
            "ip": target.ip,
            "hostname": target.hostname or target.ip,
            "cluster": target.cluster or "unknown",
            "env": target.env or "unknown",
        },
        "rule": {
            "id": check.rule_id,
            "title": check.rule_title,
        },
        "timestamp": _now().isoformat(),
    }


def _build_storm_payload(
    rule_id: str,
    targets: list[TargetInDB],
    results: list[CheckResult],
    severity: Status,
) -> dict[str, Any]:
    """Alert storm — grouped message."""
    emoji = _SEVERITY_EMOJI.get(severity, "❓")
    target_summaries = [
        {
            "ip": t.ip,
            "hostname": t.hostname or t.ip,
            "cluster": t.cluster or "unknown",
            "env": t.env or "unknown",
            "status": r.status.value,
            "message": r.message,
        }
        for t, r in zip(targets, results)
    ]
    return {
        "type": "alert_storm",
        "severity": severity.value,
        "emoji": emoji,
        "title": f"{emoji} ALERT STORM: {rule_id} — {len(targets)} targets affected",
        "rule_id": rule_id,
        "affected_count": len(targets),
        "targets": target_summaries,
        "timestamp": _now().isoformat(),
        "dashboard_url": settings.dashboard_base_url,
    }


def _to_slack(payload: dict[str, Any]) -> dict[str, Any]:
    """Convert to Slack Block Kit format."""
    state = payload.get("state", "firing")
    title = payload.get("title", "Alert")
    target = payload.get("target", {})
    result = payload.get("result", {})
    url = payload.get("dashboard_url", "")

    color = "#ff0000" if payload.get("severity") == "critical" else "#ffaa00"

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": title},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Target:* {target.get('hostname', '?')}"},
                {"type": "mrkdwn", "text": f"*Cluster:* {target.get('cluster', '?')}"},
                {"type": "mrkdwn", "text": f"*Env:* {target.get('env', '?')}"},
                {"type": "mrkdwn", "text": f"*Role:* {target.get('role', '?')}"},
                {"type": "mrkdwn", "text": f"*Status:* {payload.get('severity', '?')}"},
                {
                    "type": "mrkdwn",
                    "text": f"*Value:* {result.get('measured_value', 'N/A')}",
                },
            ],
        },
    ]

    if result.get("message"):
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": result["message"]},
        })

    if url:
        blocks.append({
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Dashboard"},
                    "url": url,
                }
            ],
        })

    return {
        "attachments": [
            {
                "color": color,
                "blocks": blocks,
            }
        ]
    }


def _to_teams(payload: dict[str, Any]) -> dict[str, Any]:
    """Convert to Microsoft Teams MessageCard format."""
    target = payload.get("target", {})
    result = payload.get("result", {})
    url = payload.get("dashboard_url", "")

    color = "FF0000" if payload.get("severity") == "critical" else "FFAA00"

    sections = [
        {
            "activityTitle": payload.get("title", "Alert"),
            "facts": [
                {"name": "Target", "value": target.get("hostname", "?")},
                {"name": "Cluster", "value": target.get("cluster", "?")},
                {"name": "Environment", "value": target.get("env", "?")},
                {"name": "Severity", "value": payload.get("severity", "?")},
            ],
        }
    ]

    if result.get("message"):
        sections.append({"text": result["message"]})

    card = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": color,
        "summary": payload.get("title", "MongoDB Alert"),
        "sections": sections,
    }

    if url:
        card["potentialAction"] = [
            {
                "@type": "OpenUri",
                "name": "View Dashboard",
                "targets": [{"os": "default", "uri": url}],
            }
        ]

    return card
