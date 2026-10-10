# Prometheus text exposition format parser
# ==========================================
# Dependency-free, pure Python implementation.
# Skips '# HELP' / '# TYPE' lines; parses metric name, labels and value.
# Correctly unescapes escaped label values.
# Handles +Inf / -Inf / NaN values.

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import NamedTuple


class Sample(NamedTuple):
    """A single Prometheus metric sample."""

    name: str
    labels: dict[str, str]
    value: float

    @property
    def metric(self) -> str:
        """Metric name without labels (alias)."""
        return self.name


# ── Label value unescape ──────────────────────────────────────
# Prometheus exposition format: backslash escape sequences
_UNESCAPE_RE = re.compile(r'\\([\\"n])')
_UNESCAPE_MAP = {"\\\\": "\\", '\\"': '"', "\\n": "\n"}


def _unescape_label(val: str) -> str:
    """Unescape Prometheus escape sequences inside a label value."""
    # This regex captures a backslash plus the character that follows it
    return _UNESCAPE_RE.sub(lambda m: _UNESCAPE_MAP.get(m.group(0), m.group(0)), val)


# ── Main parse regex ─────────────────────────────────────────
# Format: metric_name{label="value",...} value [timestamp]
# Groups: 1=name, 2=full label block (optional), 3=value, 4=timestamp (optional)
_METRIC_RE = re.compile(
    r"^([a-zA-Z_:][a-zA-Z0-9_:]*)\s*"  # metric name
    r"(?:\{([^}]*)\})?\s*"  # optional {labels}
    r"(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?|NaN|\+?Inf|-Inf)\s*"  # value
    r"(\d+)?\s*$"  # optional timestamp
)

# Label parsing inside braces: key="value" pairs separated by comma
_LABEL_RE = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*"((?:[^"\\]|\\.)*)"')


def parse(text: str) -> list[Sample]:
    """Prometheus text exposition format string'ini parse et.

    '# HELP' / '# TYPE' lines and blank lines are skipped.
    """
    samples: list[Sample] = []

    for line in text.splitlines():
        line = line.strip()

        # Blank lines and comment lines (helper)
        if not line or line.startswith("#"):
            continue

        m = _METRIC_RE.match(line)
        if not m:
            continue

        name = m.group(1)
        labels_str = m.group(2)
        value_str = m.group(3)

        # Value parse
        if value_str in ("NaN", "nan", "NAN"):
            value = float("nan")
        elif value_str in ("+Inf", "Inf"):
            value = float("inf")
        elif value_str == "-Inf":
            value = float("-inf")
        else:
            value = float(value_str)

        # Label parse
        labels: dict[str, str] = {}
        if labels_str:
            for lm in _LABEL_RE.finditer(labels_str):
                k = lm.group(1)
                v = _unescape_label(lm.group(2))
                labels[k] = v

        samples.append(Sample(name=name, labels=labels, value=value))

    return samples


def select(
    samples: Iterable[Sample],
    name: str,
    **label_filters: str,
) -> list[Sample]:
    """Filter samples by metric name and label values.

    Example:
        select(samples, "mongodb_up")
        select(samples, "mongodb_ss_connections", conn_type="current")
    """
    result = []
    for s in samples:
        if s.name != name:
            continue
        match = True
        for k, v in label_filters.items():
            if s.labels.get(k) != v:
                match = False
                break
        if match:
            result.append(s)
    return result


def select_one(
    samples: Iterable[Sample],
    name: str,
    **label_filters: str,
) -> Sample | None:
    """Single-result variant of select()."""
    results = select(samples, name, **label_filters)
    return results[0] if results else None


def sum_by(
    samples: Iterable[Sample],
    name: str,
    by: str = "instance",
) -> dict[str, float]:
    """Label-aware sum over a metric.

    Example:
        sum_by(samples, "node_cpu_seconds_total", by="cpu")
        → {"cpu0": 123.4, "cpu1": 456.7}
    """
    result: dict[str, float] = {}
    for s in samples:
        if s.name != name:
            continue
        key = s.labels.get(by, "")
        result[key] = result.get(key, 0.0) + s.value
    return result


def group_by(
    samples: Iterable[Sample],
    name: str,
) -> dict[str, list[Sample]]:
    """Group every label combination for a given metric name."""
    result: dict[str, list[Sample]] = {}
    for s in samples:
        if s.name != name:
            continue
        # Build a deterministic string key from the labels
        key = ",".join(f"{k}={v}" for k, v in sorted(s.labels.items()))
        result.setdefault(key, []).append(s)
    return result
