# promparse.py test suite - focused on edge cases

from __future__ import annotations

import math
import pytest

from app.promparse import parse, select, select_one, sum_by, group_by, Sample


class TestParseBasics:
    """Basic parsing behaviour."""

    METRICS_TEXT = """\
# HELP mongodb_up Whether MongoDB is up.
# TYPE mongodb_up gauge
mongodb_up 1
mongodb_connections 42
# HELP some_counter A counter
# TYPE some_counter counter
some_counter{instance="host1:9216",job="mongodb"} 123
"""

    def test_skips_help_and_type_lines(self):
        samples = parse(self.METRICS_TEXT)
        names = {s.name for s in samples}
        assert "HELP" not in names
        assert "TYPE" not in names
        assert names == {"mongodb_up", "mongodb_connections", "some_counter"}

    def test_metric_without_labels(self):
        samples = parse("mongodb_up 1")
        assert len(samples) == 1
        assert samples[0].name == "mongodb_up"
        assert samples[0].value == 1.0
        assert samples[0].labels == {}

    def test_metric_with_labels(self):
        samples = parse('some_counter{instance="host1:9216",job="mongodb"} 123')
        assert len(samples) == 1
        assert samples[0].name == "some_counter"
        assert samples[0].labels == {"instance": "host1:9216", "job": "mongodb"}
        assert samples[0].value == 123.0

    def test_float_value(self):
        samples = parse("latency_seconds 0.0042")
        assert samples[0].value == 0.0042

    def test_negative_value(self):
        samples = parse("delta -5")
        assert samples[0].value == -5.0

    def test_exponential_value(self):
        samples = parse("bytes 1.5e10")
        assert samples[0].value == 1.5e10


class TestSpecialValues:
    """NaN, Inf and -Inf values."""

    def test_nan(self):
        samples = parse("metric NaN")
        assert math.isnan(samples[0].value)

    def test_plus_inf(self):
        samples = parse("metric +Inf")
        assert samples[0].value == float("inf")
        assert math.isinf(samples[0].value)

    def test_inf_without_plus(self):
        samples = parse("metric Inf")
        assert samples[0].value == float("inf")

    def test_minus_inf(self):
        samples = parse("metric -Inf")
        assert samples[0].value == float("-inf")


class TestLabelEscaping:
    """Label escape'leri: \\, \", \\n."""

    def test_backslash_escape(self):
        samples = parse(r'metric{path="C:\\Users\\test"} 1')
        assert samples[0].labels["path"] == "C:\\Users\\test"

    def test_quote_escape(self):
        samples = parse(r'metric{name="hello\"world"} 1')
        assert samples[0].labels["name"] == 'hello"world'

    def test_newline_escape(self):
        samples = parse(r'metric{desc="line1\nline2"} 1')
        assert samples[0].labels["desc"] == "line1\nline2"

    def test_complex_escaping(self):
        samples = parse(r'metric{a="b\\c\"d\ne"} 1')
        assert samples[0].labels["a"] == 'b\\c"d\ne'

    def test_empty_label_value(self):
        samples = parse('metric{key=""} 1')
        assert samples[0].labels["key"] == ""


class TestRealWorldMetrics:
    """Tests against real mongodb_exporter metrics."""

    def test_mongodb_up(self):
        text = """\
mongodb_up{cl_role="shardsvr",rs_nm="rs0",rs_state="1"} 1
mongodb_up{cl_role="shardsvr",rs_nm="rs0",rs_state="2"} 1
mongodb_up 0
"""
        samples = parse(text)
        assert len(samples) == 3
        up_samples = select(samples, "mongodb_up")
        assert len(up_samples) == 3
        assert up_samples[0].value == 1.0

    def test_replset_state(self):
        text = 'mongodb_mongod_replset_my_state{cl_role="shardsvr",rs_nm="rs0",set="rs0"} 1'
        samples = parse(text)
        assert len(samples) == 1
        assert samples[0].labels["rs_nm"] == "rs0"
        assert samples[0].labels["set"] == "rs0"

    def test_connections(self):
        text = """\
mongodb_ss_connections{conn_type="current"} 128
mongodb_ss_connections{conn_type="available"} 51200
mongodb_ss_connections{conn_type="totalCreated"} 500000
"""
        samples = parse(text)
        current = select(samples, "mongodb_ss_connections", conn_type="current")
        assert len(current) == 1
        assert current[0].value == 128.0

    def test_replication_lag(self):
        text = 'mongodb_mongod_replset_member_replication_lag{member="host2:27017",rs_nm="rs0",set="rs0"} 1.5'
        samples = parse(text)
        assert samples[0].value == 1.5
        assert samples[0].labels["member"] == "host2:27017"

    def test_oplog_timestamps(self):
        text = """\
mongodb_mongod_replset_oplog_head_timestamp{set="rs0"} 1.72e12
mongodb_mongod_replset_oplog_tail_timestamp{set="rs0"} 1.71e12
"""
        samples = parse(text)
        heads = select(samples, "mongodb_mongod_replset_oplog_head_timestamp")
        tails = select(samples, "mongodb_mongod_replset_oplog_tail_timestamp")
        assert len(heads) == 1
        assert len(tails) == 1
        assert heads[0].value > tails[0].value

    def test_exporter_build_info(self):
        text = 'mongodb_exporter_build_info{branch="master",goversion="go1.21.5",revision="abc123",version="0.40.0"} 1'
        samples = parse(text)
        assert samples[0].labels["version"] == "0.40.0"

    def test_uptime(self):
        text = "mongodb_ss_uptime 360000"
        samples = parse(text)
        assert samples[0].value == 360000.0


class TestSelectFunctions:
    """select, select_one, sum_by and group_by helpers."""

    @pytest.fixture
    def samples(self):
        return [
            Sample("cpu", {"mode": "user"}, 10.0),
            Sample("cpu", {"mode": "system"}, 5.0),
            Sample("cpu", {"mode": "idle"}, 85.0),
            Sample("memory", {"type": "used"}, 4096.0),
            Sample("memory", {"type": "free"}, 1024.0),
        ]

    def test_select_by_name(self, samples):
        cpu = select(samples, "cpu")
        assert len(cpu) == 3

    def test_select_by_label(self, samples):
        result = select(samples, "cpu", mode="user")
        assert len(result) == 1
        assert result[0].value == 10.0

    def test_select_one(self, samples):
        s = select_one(samples, "memory", type="used")
        assert s is not None
        assert s.value == 4096.0

    def test_select_one_none(self, samples):
        s = select_one(samples, "nonexistent")
        assert s is None

    def test_sum_by(self, samples):
        totals = sum_by(samples, "cpu", by="mode")
        assert totals == {"user": 10.0, "system": 5.0, "idle": 85.0}

    def test_group_by(self, samples):
        groups = group_by(samples, "cpu")
        assert len(groups) == 3
        for key, items in groups.items():
            assert len(items) == 1


class TestEdgeCases:
    """Boundary conditions."""

    def test_empty_input(self):
        assert parse("") == []
        assert parse("# just comments\n# nothing else") == []

    def test_trailing_whitespace(self):
        samples = parse("  metric  1  \n")
        assert len(samples) == 1
        assert samples[0].value == 1.0

    def test_multiple_lines_same_metric(self):
        text = "requests_total{method=\"GET\"} 100\nrequests_total{method=\"POST\"} 50\n"
        samples = parse(text)
        assert len(samples) == 2

    def test_names_with_colons(self):
        samples = parse("mongodb_mongod_replset_my_state 1")
        assert samples[0].name == "mongodb_mongod_replset_my_state"

    def test_large_value(self):
        samples = parse("big_number 9999999999999999")
        assert samples[0].value == 9999999999999999.0

    def test_value_zero(self):
        samples = parse("zero_metric 0")
        assert samples[0].value == 0.0

    def test_value_with_decimal_and_exponent(self):
        samples = parse("metric 1.234e-5")
        assert samples[0].value == 1.234e-5

    def test_label_with_number_in_key(self):
        samples = parse('metric{le="0.5"} 10')
        assert samples[0].labels == {"le": "0.5"}

    def test_malformed_line_skipped(self):
        text = "good_metric 1\nthis is not a metric\nanother{key=\"val\"} 2"
        samples = parse(text)
        assert len(samples) == 2
        assert {s.name for s in samples} == {"good_metric", "another"}

    def test_empty_braces(self):
        """Some exporters emit an empty {} label block."""
        samples = parse("metric{} 1")
        assert len(samples) == 1
        assert samples[0].labels == {}
