"""
Alert rules for heart-rate readings - pure logic, no Kafka or database.

The alerter feeds every validated Reading into AlertTracker.check() and
calls check_silence() every few seconds. Times come from the readings
themselves (event time), so the rules can be tested without waiting.

    sustained_anomaly : >= ANOMALY_COUNT abnormal readings for one customer
                        within ANOMALY_WINDOW
    sensor_silence    : no reading from a known customer for SILENCE_LIMIT
    cooldown          : the same customer + rule alerts at most once per COOLDOWN
"""
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta

from src.validation import Reading

# ----- Rule settings (the only place these numbers live) -----
ANOMALY_COUNT = 3                       # this many abnormal readings ...
ANOMALY_WINDOW = timedelta(seconds=60)  # ... within this time -> alert
COOLDOWN = timedelta(minutes=5)         # quiet period after an alert (same customer + rule)
SILENCE_LIMIT = timedelta(seconds=30)   # no reading from a customer for this long -> alert


@dataclass(frozen=True)
class Alert:
    """One alert, ready to log, store or send."""

    customer_id: str
    rule: str          # "sustained_anomaly" or "sensor_silence"
    message: str
    triggered_at: datetime


class AlertTracker:
    """Remembers recent readings per customer and decides when to alert."""

    def __init__(self) -> None:
        self._anomaly_times: dict[str, deque[datetime]] = {}  # recent anomaly times per customer
        self._last_seen: dict[str, datetime] = {}              # latest reading time per customer
        self._last_alert: dict[tuple[str, str], datetime] = {} # (customer, rule) -> last alert time

    def check(self, reading: Reading) -> Alert | None:
        """Record one reading; return an Alert if it completes a sustained anomaly."""
        customer_id = reading.customer_id
        now = reading.recorded_at
        self._last_seen[customer_id] = max(now, self._last_seen.get(customer_id, now))

        if not reading.is_anomaly:
            return None

        times = self._anomaly_times.setdefault(customer_id, deque())
        times.append(now)
        while now - times[0] > ANOMALY_WINDOW:  # forget anomalies older than the window
            times.popleft()

        if len(times) >= ANOMALY_COUNT:
            message = (
                f"{len(times)} abnormal readings within {int(ANOMALY_WINDOW.total_seconds())} s "
                f"(latest {reading.heart_rate} bpm)"
            )
            return self._fire(customer_id, "sustained_anomaly", message, now)
        return None

    def check_silence(self, now: datetime) -> list[Alert]:
        """Return an Alert for every known customer that has gone quiet."""
        alerts = []
        for customer_id, last_seen in self._last_seen.items():
            quiet_for = now - last_seen
            if quiet_for > SILENCE_LIMIT:
                message = f"no reading for {int(quiet_for.total_seconds())} s"
                alert = self._fire(customer_id, "sensor_silence", message, now)
                if alert is not None:
                    alerts.append(alert)
        return alerts

    def _fire(self, customer_id: str, rule: str, message: str, now: datetime) -> Alert | None:
        """Create the Alert unless the same customer + rule alerted within COOLDOWN."""
        last_alert = self._last_alert.get((customer_id, rule))
        if last_alert is not None and now - last_alert < COOLDOWN:
            return None
        self._last_alert[(customer_id, rule)] = now
        return Alert(customer_id, rule, message, now)


if __name__ == "__main__":
    # Demo: one customer's readings over 50 seconds, then two silence checks
    from datetime import timezone

    from src.validation import is_anomaly

    start = datetime(2026, 11, 26, 9, 0, tzinfo=timezone.utc)
    tracker = AlertTracker()
    for second, heart_rate in [(0, 72), (10, 204), (20, 70), (30, 27), (40, 210), (50, 205)]:
        reading = Reading("CUST_001", start + timedelta(seconds=second), heart_rate, is_anomaly(heart_rate))
        alert = tracker.check(reading)
        print(f"t={second:>3}s  {heart_rate:>3} bpm  ->  {alert.message if alert else 'no alert'}")

    for second in (60, 81):
        alerts = tracker.check_silence(start + timedelta(seconds=second))
        print(f"silence check at t={second}s  ->  {[a.message for a in alerts] or 'no alert'}")