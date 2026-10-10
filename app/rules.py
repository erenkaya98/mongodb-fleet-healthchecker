# Rule engine - configuration-driven evaluation loaded from rules.yml
# ======================================================================
# The rule set is selected per target from its check_profile.
# A missing metric yields UNKNOWN and never fails the run.

from __future__ import annotations

import logging
import math
from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml

from app.config import settings
from app.models import CheckResult, ScrapeResult, Status

logger = logging.getLogger(__name__)

# Aggregation options and operators
_OPERATORS: dict[str, Callable[[float, float], bool]] = {
    "gt": lambda val, thresh: val > thresh,
    "gte": lambda val, thresh: val >= thresh,
    "lt": lambda val, thresh: val < thresh,
    "lte": lambda val, thresh: val <= thresh,
    "eq": lambda val, thresh: val == thresh,
    "ne": lambda val, thresh: val != thresh,
}


class RuleDefinition:
    """A single rule definition."""

    def __init__(self, raw: dict) -> None:
        self.rule_id: str = raw["id"]
        self.title: str = raw.get("title", self.rule_id)
        self.metrics: list[str] = (
            raw["metric"] if isinstance(raw["metric"], list) else [raw["metric"]]
        )
        self.labels: dict[str, str] = raw.get("labels", {})
        self.aggregation: str = raw.get("aggregation", "value")
        self.op: str = raw.get("op", "eq")
        self.warning: Any = raw.get("warning")
        self.critical: Any = raw.get("critical")
        self.unit: str = raw.get("unit", "")
        self.enabled: bool = raw.get("enabled", True)
        self.skip_when_absent: bool = raw.get("skip_when_absent", False)
        self.description: str = raw.get("description", "")

    def __repr__(self) -> str:
        return f"Rule({self.rule_id})"


class RuleEngine:
    """Rule engine - loads rule sets from rules.yml."""

    def __init__(self, rules_path: str | None = None) -> None:
        self._profiles: dict[str, list[RuleDefinition]] = {}
        path = rules_path or settings.rules_file
        self._load(Path(path))

    def _load(self, path: Path) -> None:
        """Load the rules.yml file."""
        if not path.exists():
            logger.warning("rules.yml not found: %s, using an empty rule set", path)
            self._profiles = {"default": []}
            return

        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f)

        if not data or "profiles" not in data:
            logger.warning("no profiles found in rules.yml")
            self._profiles = {"default": []}
            return

        for profile_name, rules_raw in data["profiles"].items():
            if not rules_raw:
                continue
            self._profiles[profile_name] = [
                RuleDefinition(r) for r in rules_raw if r.get("enabled", True)
            ]
            logger.debug(
                "Profile loaded: %s -> %d rules",
                profile_name,
                len(self._profiles[profile_name]),
            )

    def get_rules(self, profile: str) -> list[RuleDefinition]:
        """Return the rules for a given profile."""
        return self._profiles.get(profile, self._profiles.get("default", []))

    def evaluate(
        self,
        scrape_result: ScrapeResult,
        profile: str,
        previous_metrics: dict[str, float] | None = None,
        target_role: str | None = None,
    ) -> list[CheckResult]:
        """Evaluate every rule for a target."""
        rules = self.get_rules(profile)
        results: list[CheckResult] = []
        present_metrics = {s.name for s in scrape_result.samples}

        for rule in rules:
            if not rule.enabled:
                continue
            # skip_when_absent: do not evaluate the rule when the metric was never collected
            # (e.g. WiredTiger ticket metrics are gone in MongoDB 8.0 - skip, do not report UNKNOWN)
            if rule.skip_when_absent and not any(m in present_metrics for m in rule.metrics):
                continue
            try:
                result = self._evaluate_rule(rule, scrape_result, previous_metrics, target_role)
            except Exception as exc:
                logger.error("Rule evaluation failed: %s -> %s", rule.rule_id, exc)
                result = CheckResult(
                    rule_id=rule.rule_id,
                    rule_title=rule.title,
                    status=Status.UNKNOWN,
                    message=f"Evaluation error: {exc}",
                )
            results.append(result)

        return results

    def _resolve_metric(
        self,
        rule: RuleDefinition,
        scrape_result: ScrapeResult,
        previous_metrics: dict[str, float] | None = None,
    ) -> float | None:
        """Return the first metric name found among the alternatives."""
        # Synthetic metrics: scrape_ok, scrape_latency_ms
        if "scrape_ok" in rule.metrics:
            return 1.0 if scrape_result.ok else 0.0
        if "scrape_latency_ms" in rule.metrics:
            return scrape_result.latency_ms

        for metric_name in rule.metrics:
            matching = [s for s in scrape_result.samples if s.name == metric_name]
            # Apply the label filter
            for label_key, label_val in rule.labels.items():
                matching = [s for s in matching if s.labels.get(label_key) == label_val]
            if matching:
                match rule.aggregation:
                    case "value":
                        return matching[0].value
                    case "sum":
                        return sum(s.value for s in matching)
                    case "max":
                        return max(s.value for s in matching)
                    case "min":
                        return min(s.value for s in matching)
                    case "count":
                        return float(len(matching))
                    case "absent":
                        return 0.0  # present = problem, missing = absent
                    case "ratio":
                        return self._compute_ratio(scrape_result, rule, metric_name)
                    case "delta":
                        if previous_metrics and rule.rule_id in previous_metrics:
                            prev = previous_metrics[rule.rule_id]
                            return matching[0].value - prev
                        return 0.0
                    case "oplog_window":
                        return self._compute_oplog_window(rule, scrape_result)
                    case "mem_ratio":
                        return self._compute_mem_ratio(rule, scrape_result)
                    case "fcv_mismatch":
                        return self._compute_fcv_mismatch(rule, scrape_result)
                    case "auth_exposed":
                        return self._compute_auth_exposed(rule, scrape_result)
                    case _:
                        return matching[0].value

        # Metric yok → UNKNOWN
        return None

    def _compute_ratio(
        self,
        scrape_result: ScrapeResult,
        rule: RuleDefinition,
        metric_name: str,
    ) -> float | None:
        """current/(current+available) ratio for connections_saturation."""
        current_val: float | None = None
        available_val: float | None = None

        for s in scrape_result.samples:
            if s.name != metric_name:
                continue
            ct = s.labels.get("conn_type", "")
            if ct == "current":
                current_val = s.value
            elif ct == "available":
                available_val = s.value

        if current_val is None or available_val is None:
            return None

        total = current_val + available_val
        if total == 0:
            return 0.0
        return current_val / total

    def _compute_oplog_window(
        self,
        rule: RuleDefinition,
        scrape_result: ScrapeResult,
    ) -> float | None:
        """Oplog head-tail difference, in hours."""
        head: float | None = None
        tail: float | None = None

        for s in scrape_result.samples:
            if s.name == "mongodb_mongod_replset_oplog_head_timestamp":
                head = s.value
            elif s.name == "mongodb_mongod_replset_oplog_tail_timestamp":
                tail = s.value

        if head is None or tail is None:
            return None

        return (head - tail) / 3600.0  # saniye → saat

    def _compute_mem_ratio(
        self,
        rule: RuleDefinition,
        scrape_result: ScrapeResult,
    ) -> float | None:
        """MongoDB resident RAM / total system RAM, as a percentage (0-100)."""
        resident: float | None = None
        total: float | None = None

        for s in scrape_result.samples:
            if s.name == "mongodb_mem_resident_mb":
                resident = s.value
            elif s.name == "mongodb_mem_total_mb":
                total = s.value

        if resident is None or total is None or total == 0:
            return None

        return (resident / total) * 100.0

    def _compute_fcv_mismatch(
        self,
        rule: RuleDefinition,
        scrape_result: ScrapeResult,
    ) -> float | None:
        """FCV < mongod version -> 1 (mismatch), equal -> 0."""
        fcv: float | None = None
        version: float | None = None

        for s in scrape_result.samples:
            if s.name == "mongodb_fcv_version":
                fcv = s.value
            elif s.name == "mongodb_mongod_version":
                version = s.value

        if fcv is None or version is None:
            return None

        return 1.0 if fcv < version else 0.0

    def _compute_auth_exposed(
        self,
        rule: RuleDefinition,
        scrape_result: ScrapeResult,
    ) -> float | None:
        """authorization=disabled AND bindIp=public -> 1 (open access), otherwise 0.

        Returns None (UNKNOWN) when both metrics are missing. With only one metric
        present we stay conservative and do not report exposure (the unknown side is
        assumed to be private).
        """
        auth: float | None = None
        bindip: float | None = None

        for s in scrape_result.samples:
            if s.name == "mongodb_security_authorization_enabled":
                auth = s.value
            elif s.name == "mongodb_net_bindip_public":
                bindip = s.value

        if auth is None and bindip is None:
            return None

        # auth=0 (disabled) AND bindip=1 (public) -> exposed
        if auth == 0.0 and bindip == 1.0:
            return 1.0
        return 0.0

    def _check_threshold(
        self,
        rule: RuleDefinition,
        value: float,
        status: Status,
    ) -> bool:
        """Check a value against one threshold."""
        threshold = rule.warning if status == Status.WARNING else rule.critical

        if threshold is None:
            return False

        # Special handling for the 'ne' operator:
        # - threshold is a list: alarm when the value is NOT in the list
        # - threshold is an empty list: never alarm (leave it to the other level)
        # - threshold is a scalar: alarm when the value is NOT equal
        if rule.op == "ne":
            if isinstance(threshold, list):
                if len(threshold) == 0:
                    return False  # Empty list -> no alarm
                return value not in threshold
            return value != threshold

        # 'absent' operator: a missing metric yields 0, so alarm on absent
        if rule.op == "absent":
            return value == 0.0

        operator = _OPERATORS.get(rule.op)
        if operator:
            return operator(value, threshold)

        return False

    def _evaluate_rule(
        self,
        rule: RuleDefinition,
        scrape_result: ScrapeResult,
        previous_metrics: dict[str, float] | None,
        target_role: str | None = None,
    ) -> CheckResult:
        """Evaluate a single rule."""
        # Role consistency: for replset_state the stored role must match the metric value
        if rule.rule_id == "replset_state" and target_role:
            role_check = self._check_role_match(target_role, scrape_result)
            if role_check is not None:
                return CheckResult(
                    rule_id=rule.rule_id,
                    rule_title=rule.title,
                    status=role_check.status,
                    measured_value=role_check.measured_value,
                    message=role_check.message,
                )

        value = self._resolve_metric(rule, scrape_result, previous_metrics)

        # Metric not found -> UNKNOWN
        if value is None or math.isnan(value):
            return CheckResult(
                rule_id=rule.rule_id,
                rule_title=rule.title,
                status=Status.UNKNOWN,
                message=f"Metric not found: {rule.metrics}",
            )

        # NaN check (belt and braces)
        if math.isnan(value):
            return CheckResult(
                rule_id=rule.rule_id,
                rule_title=rule.title,
                status=Status.UNKNOWN,
                measured_value=value,
                message="Metric value is NaN",
            )

        # Check the CRITICAL threshold first
        is_critical = self._check_threshold(rule, value, Status.CRITICAL)
        if is_critical:
            return CheckResult(
                rule_id=rule.rule_id,
                rule_title=rule.title,
                status=Status.CRITICAL,
                measured_value=value,
                critical_threshold=(
                    rule.critical if isinstance(rule.critical, (int, float)) else None
                ),
                message=self._format_message(rule, value, Status.CRITICAL),
            )

        # Then the WARNING threshold
        is_warning = self._check_threshold(rule, value, Status.WARNING)
        if is_warning:
            return CheckResult(
                rule_id=rule.rule_id,
                rule_title=rule.title,
                status=Status.WARNING,
                measured_value=value,
                warning_threshold=(
                    rule.warning if isinstance(rule.warning, (int, float)) else None
                ),
                message=self._format_message(rule, value, Status.WARNING),
            )

        # OK
        return CheckResult(
            rule_id=rule.rule_id,
            rule_title=rule.title,
            status=Status.OK,
            measured_value=value,
            message=self._format_message(rule, value, Status.OK),
        )

    def _check_role_match(
        self, target_role: str, scrape_result: ScrapeResult
    ) -> CheckResult | None:
        """Compare the stored role against the replset_state metric.

        Returns:
            None: no check possible (metric missing, or the role is not a replset role)
            CheckResult: OK, or CRITICAL on a role mismatch
        """
        # Only meaningful for replset members; skip standalone/arbiter/mongos/configsvr
        if target_role not in ("primary", "secondary"):
            return None

        state: float | None = None
        for s in scrape_result.samples:
            if s.name == "mongodb_mongod_replset_my_state":
                state = s.value
                break

        if state is None:
            return None

        expected_state = 1.0 if target_role == "primary" else 2.0
        state_names = {1.0: "PRIMARY", 2.0: "SECONDARY", 7.0: "ARBITER"}
        actual_name = state_names.get(state, f"UNKNOWN({state})")

        if state == expected_state:
            return CheckResult(
                rule_id="replset_state",
                rule_title="Replica set state",
                status=Status.OK,
                measured_value=state,
                message=f"Replica set state: {state} ({actual_name}) — rol ile uyumlu",
            )

        return CheckResult(
            rule_id="replset_state",
            rule_title="Replica set state",
            status=Status.CRITICAL,
            measured_value=state,
            message=(
                f"ROL UYUMSUZLUĞU: stored role='{target_role}' (beklenen state={expected_state}), "
                f"ama gerçek state={state} ({actual_name})"
            ),
        )

    @staticmethod
    def _format_message(rule: RuleDefinition, value: float, status: Status) -> str:
        """Build the status message."""
        unit_part = f" {rule.unit}" if rule.unit else ""

        if status == Status.OK:
            if rule.op == "ne" and isinstance(rule.critical, list):
                return f"{rule.title}: {value}{unit_part} (beklenen değer: {rule.critical})"
            return f"{rule.title}: {value}{unit_part}"
        elif status == Status.WARNING:
            threshold = rule.warning
            return f"{rule.title}: {value}{unit_part} (warn > {threshold}{unit_part})"
        else:
            threshold = rule.critical
            if isinstance(threshold, list):
                return f"{rule.title}: {value}{unit_part} (beklenen değerler: {threshold})"
            return f"{rule.title}: {value}{unit_part} (crit > {threshold}{unit_part})"


# Singleton
_engine: RuleEngine | None = None


def get_engine(rules_path: str | None = None) -> RuleEngine:
    global _engine
    if _engine is None:
        _engine = RuleEngine(rules_path)
    return _engine
