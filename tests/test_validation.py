"""Unit tests for src/validation.py - no Kafka or database needed."""
import json
import re
from datetime import timezone

import pytest

from src.validation import ValidationError, validate_message

VALID_TIMESTAMP = "2026-11-25T09:00:00.000+00:00"


def make_message(drop: str | None = None, **changes) -> str:
    """Build a valid JSON message, then change fields (changes) or remove one (drop)."""
    data = {"customer_id": "CUST_001", "timestamp": VALID_TIMESTAMP, "heart_rate": 72}
    data.update(changes)
    if drop:
        del data[drop]
    return json.dumps(data)


# ---------- Valid messages ----------

def test_normal_reading_is_valid():
    reading = validate_message(make_message())
    assert reading.customer_id == "CUST_001"
    assert reading.heart_rate == 72
    assert reading.is_anomaly is False
    assert reading.recorded_at.tzinfo == timezone.utc


def test_bytes_from_kafka_are_accepted():
    reading = validate_message(make_message().encode("utf-8"))
    assert reading.heart_rate == 72


# The numbers are written out on purpose: they are the agreed rules (Phase 3
# design). If someone changes a threshold, these tests fail and force a decision.
@pytest.mark.parametrize(
    ("heart_rate", "expected_anomaly"),
    [
        (20, True),    # lowest possible value: stored, but abnormal
        (39, True),
        (40, False),   # edge of normal is included
        (72, False),
        (180, False),  # edge of normal is included
        (181, True),
        (250, True),   # highest possible value: stored, but abnormal
    ],
)
def test_anomaly_flag_at_boundaries(heart_rate, expected_anomaly):
    reading = validate_message(make_message(heart_rate=heart_rate))
    assert reading.is_anomaly is expected_anomaly


# ---------- Rejected messages: each must fail with a clear reason ----------

@pytest.mark.parametrize(
    ("raw", "expected_reason"),
    [
        pytest.param(None, "empty message", id="no body"),
        pytest.param("hello belt", "not valid JSON", id="plain text"),
        pytest.param(b"\xff\xfe", "not valid JSON", id="broken bytes"),
        pytest.param("[1, 2]", "not a JSON object", id="JSON list"),
        pytest.param(make_message(heart_rate=None), "heart_rate is missing", id="null heart rate"),
        pytest.param(make_message(drop="timestamp"), "timestamp is missing", id="no timestamp"),
        pytest.param(make_message(customer_id="   "), "customer_id must be non-empty text", id="blank id"),
        pytest.param(make_message(customer_id="C" * 21), "longer than 20 characters", id="id too long"),
        pytest.param(make_message(timestamp="yesterday"), "is not ISO 8601", id="bad timestamp"),
        pytest.param(make_message(timestamp="2026-11-25T09:00:00"), "has no time zone", id="no time zone"),
        pytest.param(make_message(heart_rate=19), "out of range", id="19 bpm"),
        pytest.param(make_message(heart_rate=251), "out of range", id="251 bpm"),
        pytest.param(make_message(heart_rate=-1), "out of range", id="-1 bpm"),
        pytest.param(make_message(heart_rate=0), "out of range", id="0 bpm"),
        pytest.param(make_message(heart_rate=72.5), "not a whole number", id="decimal"),
        pytest.param(make_message(heart_rate="72"), "not a whole number", id="text number"),
        pytest.param(make_message(heart_rate=True), "not a whole number", id="boolean"),
    ],
)
def test_invalid_messages_are_rejected_with_reason(raw, expected_reason):
    with pytest.raises(ValidationError, match=re.escape(expected_reason)):
        validate_message(raw)


def test_validated_reading_cannot_be_changed():
    reading = validate_message(make_message())
    with pytest.raises(AttributeError):  # FrozenInstanceError is an AttributeError
        reading.heart_rate = 999