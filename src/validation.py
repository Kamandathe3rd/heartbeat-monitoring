"""
Validation rules for heart-rate messages - the single source of truth.

The consumer and the tests both import these rules, so every threshold
is defined in ONE place.

    valid + normal (40-180 bpm)         -> store, is_anomaly = False
    valid + abnormal (20-39, 181-250)   -> store, is_anomaly = True
    impossible / corrupt                -> raise ValidationError(reason)
"""
import json
from dataclasses import dataclass
from datetime import datetime

# ----- Thresholds (beats per minute) -----
MIN_POSSIBLE_BPM = 20   # below this is impossible -> reject
MAX_POSSIBLE_BPM = 250  # above this is impossible -> reject
MIN_NORMAL_BPM = 40     # 20-39 is possible but abnormal -> store and flag
MAX_NORMAL_BPM = 180    # 181-250 is possible but abnormal -> store and flag

MAX_CUSTOMER_ID_LENGTH = 20  # matches VARCHAR(20) in sql/01_schema.sql
REQUIRED_FIELDS = ("customer_id", "timestamp", "heart_rate")


class ValidationError(ValueError):
    """Raised when a message cannot be stored. str(error) is the reason."""


@dataclass(frozen=True)
class Reading:
    """A validated reading, ready to insert into heartbeat_readings."""

    customer_id: str
    recorded_at: datetime
    heart_rate: int
    is_anomaly: bool


def is_anomaly(heart_rate: int) -> bool:
    """True if the heart rate is possible but outside the normal range."""
    return not MIN_NORMAL_BPM <= heart_rate <= MAX_NORMAL_BPM


def validate_message(raw: bytes | str | None) -> Reading:
    """
    Check one raw Kafka message and return a Reading.

    Raises ValidationError with a human-readable reason if the message
    is impossible or corrupt.
    """
    # 1. Must be a JSON object
    if raw is None:
        raise ValidationError("empty message")
    try:
        data = json.loads(raw)
    except ValueError:  # bad JSON or bad bytes
        raise ValidationError("not valid JSON") from None
    if not isinstance(data, dict):
        raise ValidationError("not a JSON object")

    # 2. All required fields must be present (and not null)
    for field in REQUIRED_FIELDS:
        if data.get(field) is None:
            raise ValidationError(f"{field} is missing")

    # 3. customer_id: non-empty text that fits the database column
    customer_id = data["customer_id"]
    if not isinstance(customer_id, str) or not customer_id.strip():
        raise ValidationError("customer_id must be non-empty text")
    if len(customer_id) > MAX_CUSTOMER_ID_LENGTH:
        raise ValidationError(
            f"customer_id is longer than {MAX_CUSTOMER_ID_LENGTH} characters"
        )

    # 4. timestamp: ISO 8601 text that includes a time zone
    timestamp = data["timestamp"]
    try:
        recorded_at = datetime.fromisoformat(timestamp)
    except (TypeError, ValueError):
        raise ValidationError(f"timestamp {timestamp!r} is not ISO 8601") from None
    if recorded_at.tzinfo is None:
        raise ValidationError(f"timestamp {timestamp!r} has no time zone")

    # 5. heart_rate: a whole number in the possible range
    heart_rate = data["heart_rate"]
    if isinstance(heart_rate, bool) or not isinstance(heart_rate, int):
        raise ValidationError(f"heart_rate {heart_rate!r} is not a whole number")
    if not MIN_POSSIBLE_BPM <= heart_rate <= MAX_POSSIBLE_BPM:
        raise ValidationError(
            f"heart_rate {heart_rate} is out of range "
            f"({MIN_POSSIBLE_BPM}-{MAX_POSSIBLE_BPM})"
        )

    return Reading(customer_id, recorded_at, heart_rate, is_anomaly(heart_rate))


if __name__ == "__main__":
    # Demo: run a few sample messages through the rules
    samples = [
        '{"customer_id": "CUST_001", "timestamp": "2026-11-25T09:00:00.000+00:00", "heart_rate": 72}',
        '{"customer_id": "CUST_002", "timestamp": "2026-11-25T09:00:00.000+00:00", "heart_rate": 204}',
        '{"customer_id": "CUST_003", "timestamp": "2026-11-25T09:00:00.000+00:00", "heart_rate": 27}',
        '{"customer_id": "CUST_004", "timestamp": "2026-11-25T09:00:00.000+00:00", "heart_rate": null}',
        '{"customer_id": "CUST_005", "timestamp": "2026-11-25T09:00:00.000+00:00", "heart_rate": -1}',
        '{"customer_id": "CUST_001", "timestamp": "2026-11-25 09:00", "heart_rate": 70}',
        "hello belt",
    ]
    for raw in samples:
        try:
            reading = validate_message(raw)
            label = "ANOMALY" if reading.is_anomaly else "OK"
            print(f"{label:<8} {reading.customer_id} {reading.heart_rate}")
        except ValidationError as error:
            print(f"REJECT   {error}")