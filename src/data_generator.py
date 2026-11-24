"""
Synthetic heart-rate data generator.

Creates realistic-looking heart-rate readings for a fixed set of fake
customers. Each reading has: customer_id, timestamp (UTC), heart_rate (bpm).
"""
import random
import time
from collections.abc import Iterator
from datetime import datetime, timezone

# ----- Settings -----
# Each fake customer has a personal resting heart rate (baseline) in bpm.
CUSTOMER_BASELINES = {
    "CUST_001": 62,
    "CUST_002": 70,
    "CUST_003": 75,
    "CUST_004": 80,
    "CUST_005": 68,
}

# How much a reading usually varies around the baseline (standard deviation, bpm)
NOISE_BPM = 5

# Chance that a reading is deliberately abnormal (0.05 = 5 %), to test the consumer
ANOMALY_RATE = 0.05

# Seconds to wait between rounds (one round = one reading per customer)
DEFAULT_INTERVAL_SECONDS = 1.0


def make_anomaly(reading: dict) -> dict:
    """Turn a normal reading into one of three kinds of bad reading."""
    kind = random.choice(["too_high", "too_low", "corrupt"])
    if kind == "too_high":
        reading["heart_rate"] = random.randint(185, 220)
    elif kind == "too_low":
        reading["heart_rate"] = random.randint(20, 35)
    else:
        reading["heart_rate"] = random.choice([None, -1, 0])
    return reading


def generate_reading(customer_id: str, anomaly_rate: float = ANOMALY_RATE) -> dict:
    """
    Return one heart-rate reading for `customer_id`, close to their baseline.

    With probability `anomaly_rate` the reading is made abnormal on purpose.
    """
    baseline = CUSTOMER_BASELINES[customer_id]
    heart_rate = round(random.gauss(baseline, NOISE_BPM))
    reading = {
        "customer_id": customer_id,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "heart_rate": heart_rate,
    }
    if random.random() < anomaly_rate:
        reading = make_anomaly(reading)
    return reading


def stream_readings(
    interval_seconds: float = DEFAULT_INTERVAL_SECONDS,
    max_rounds: int | None = None,
    anomaly_rate: float = ANOMALY_RATE,
) -> Iterator[dict]:
    """
    Yield readings non-stop: one per customer each round, then wait.

    max_rounds=None means "run forever"; a number stops after that many rounds
    (handy for tests). anomaly_rate=0 gives only normal readings.
    """
    rounds_done = 0
    while max_rounds is None or rounds_done < max_rounds:
        for customer_id in CUSTOMER_BASELINES:
            yield generate_reading(customer_id, anomaly_rate)
        rounds_done += 1
        time.sleep(interval_seconds)


if __name__ == "__main__":
    # Demo: print a continuous stream until you press Ctrl + C
    print("Streaming heart-rate readings... press Ctrl + C to stop.")
    try:
        for reading in stream_readings():
            print(reading)
    except KeyboardInterrupt:
        print("Stopped by user.")