"""Unit tests for src/data_generator.py - no Kafka or database needed."""
import json
import random
import statistics
from collections import Counter
from datetime import datetime, timedelta

import pytest

from src.data_generator import CUSTOMER_BASELINES, generate_reading, stream_readings
from src.validation import ValidationError, validate_message


@pytest.fixture(autouse=True)
def fixed_random_seed():
    """Make the 'random' numbers the same on every run, so results are repeatable."""
    random.seed(42)


def classify(reading: dict) -> str:
    """Run a generated reading through the real validation rules."""
    try:
        result = validate_message(json.dumps(reading))
    except ValidationError:
        return "rejected"
    return "anomaly" if result.is_anomaly else "normal"


# ---------- Shape of one reading ----------

def test_reading_has_exactly_the_three_required_fields():
    reading = generate_reading("CUST_001")
    assert set(reading) == {"customer_id", "timestamp", "heart_rate"}


def test_reading_field_types():
    reading = generate_reading("CUST_001", anomaly_rate=0)
    assert isinstance(reading["customer_id"], str)
    assert isinstance(reading["timestamp"], str)
    assert isinstance(reading["heart_rate"], int)


def test_timestamp_is_iso_8601_in_utc():
    reading = generate_reading("CUST_001")
    parsed = datetime.fromisoformat(reading["timestamp"])
    assert parsed.utcoffset() == timedelta(0)


def test_unknown_customer_is_refused():
    with pytest.raises(KeyError):
        generate_reading("CUST_999")


# ---------- Realism ----------

@pytest.mark.parametrize("customer_id", list(CUSTOMER_BASELINES))
def test_clean_readings_stay_close_to_the_customer_baseline(customer_id):
    baseline = CUSTOMER_BASELINES[customer_id]
    rates = [generate_reading(customer_id, anomaly_rate=0)["heart_rate"] for _ in range(1000)]
    assert abs(statistics.mean(rates) - baseline) < 1      # centred on the baseline
    assert all(abs(hr - baseline) <= 30 for hr in rates)    # no wild jumps


# ---------- Anomalies (checked with the REAL validation rules) ----------

def test_rate_zero_gives_only_normal_readings():
    outcomes = Counter(classify(generate_reading("CUST_003", anomaly_rate=0)) for _ in range(1000))
    assert outcomes == {"normal": 1000}


def test_rate_one_gives_only_abnormal_readings_of_both_kinds():
    outcomes = Counter(classify(generate_reading("CUST_003", anomaly_rate=1)) for _ in range(300))
    assert outcomes["normal"] == 0
    assert outcomes["anomaly"] > 0 and outcomes["rejected"] > 0


def test_default_rate_is_about_five_percent():
    readings = list(stream_readings(interval_seconds=0, max_rounds=2000))  # 10,000 readings
    abnormal = sum(1 for r in readings if classify(r) != "normal")
    assert 0.035 <= abnormal / len(readings) <= 0.065


# ---------- The stream ----------

def test_each_round_has_one_reading_per_customer():
    readings = list(stream_readings(interval_seconds=0, max_rounds=3))
    assert len(readings) == 3 * len(CUSTOMER_BASELINES)
    assert Counter(r["customer_id"] for r in readings) == {cid: 3 for cid in CUSTOMER_BASELINES}


def test_stream_waits_the_interval_after_each_round(monkeypatch):
    waits = []
    monkeypatch.setattr("src.data_generator.time.sleep", waits.append)  # record instead of sleeping
    list(stream_readings(interval_seconds=0.5, max_rounds=2))
    assert waits == [0.5, 0.5]