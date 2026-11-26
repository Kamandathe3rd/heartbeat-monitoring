"""Unit tests for src/alert_rules.py - no Kafka or database needed, no waiting."""
from datetime import datetime, timedelta, timezone

from src.alert_rules import AlertTracker
from src.validation import Reading, is_anomaly

START = datetime(2026, 11, 26, 9, 0, tzinfo=timezone.utc)


def reading(customer_id: str, second: int, heart_rate: int) -> Reading:
    """A validated reading `second` seconds after START (anomaly flag from the real rules)."""
    return Reading(customer_id, START + timedelta(seconds=second), heart_rate, is_anomaly(heart_rate))


def at(second: int) -> datetime:
    """The moment `second` seconds after START."""
    return START + timedelta(seconds=second)


def feed(tracker: AlertTracker, customer_id: str, events: list[tuple[int, int]]) -> list:
    """Send (second, heart_rate) readings to the tracker; return the alerts that fired."""
    alerts = [tracker.check(reading(customer_id, second, hr)) for second, hr in events]
    return [alert for alert in alerts if alert is not None]


# ---------- Sustained anomaly ----------

def test_normal_readings_never_alert():
    tracker = AlertTracker()
    assert feed(tracker, "CUST_001", [(s, 72) for s in range(0, 600, 5)]) == []


def test_two_anomalies_are_not_enough():
    tracker = AlertTracker()
    assert feed(tracker, "CUST_001", [(0, 204), (10, 27)]) == []


def test_third_anomaly_within_the_window_alerts():
    tracker = AlertTracker()
    alerts = feed(tracker, "CUST_001", [(0, 204), (20, 72), (30, 27), (40, 210)])
    assert len(alerts) == 1
    alert = alerts[0]
    assert alert.customer_id == "CUST_001"
    assert alert.rule == "sustained_anomaly"
    assert alert.triggered_at == at(40)
    assert "3 abnormal readings" in alert.message and "210 bpm" in alert.message


def test_exactly_60_seconds_apart_still_counts():
    tracker = AlertTracker()
    assert len(feed(tracker, "CUST_001", [(0, 204), (30, 204), (60, 204)])) == 1


def test_61_seconds_apart_is_outside_the_window():
    tracker = AlertTracker()
    assert feed(tracker, "CUST_001", [(0, 204), (30, 204), (61, 204)]) == []


def test_anomalies_spread_out_never_alert():
    tracker = AlertTracker()
    assert feed(tracker, "CUST_001", [(0, 204), (40, 204), (80, 204), (120, 204)]) == []


def test_cooldown_blocks_repeats_then_allows_a_new_episode():
    tracker = AlertTracker()
    first = feed(tracker, "CUST_001", [(0, 204), (10, 204), (20, 204)])          # alert at 20 s
    same_episode = feed(tracker, "CUST_001", [(30, 204), (40, 204), (50, 204)])  # within 5 min
    new_episode = feed(tracker, "CUST_001", [(330, 204), (335, 204), (340, 204)])  # 320 s later
    assert [a.triggered_at for a in first] == [at(20)]
    assert same_episode == []
    assert [a.triggered_at for a in new_episode] == [at(340)]


def test_customers_are_tracked_separately():
    tracker = AlertTracker()
    feed(tracker, "CUST_001", [(0, 204), (10, 204)])
    assert feed(tracker, "CUST_002", [(20, 204)]) == []                   # 3 anomalies, but 2 customers
    assert len(feed(tracker, "CUST_001", [(30, 204)])) == 1               # CUST_001's 3rd -> alert
    assert len(feed(tracker, "CUST_002", [(40, 204), (50, 204)])) == 1    # not blocked by CUST_001


# ---------- Sensor silence ----------

def test_no_silence_alert_for_unknown_customers():
    assert AlertTracker().check_silence(at(1000)) == []


def test_silence_alert_only_after_the_limit():
    tracker = AlertTracker()
    tracker.check(reading("CUST_001", 0, 72))
    assert tracker.check_silence(at(30)) == []        # exactly 30 s: not yet
    alerts = tracker.check_silence(at(31))            # 31 s: silent
    assert [(a.customer_id, a.rule) for a in alerts] == [("CUST_001", "sensor_silence")]
    assert alerts[0].message == "no reading for 31 s"


def test_silence_alert_respects_the_cooldown():
    tracker = AlertTracker()
    tracker.check(reading("CUST_001", 0, 72))
    assert len(tracker.check_silence(at(31))) == 1
    assert tracker.check_silence(at(100)) == []       # still silent, but within cooldown
    assert len(tracker.check_silence(at(331))) == 1   # cooldown over: remind again


def test_a_late_message_does_not_make_a_sensor_look_silent():
    tracker = AlertTracker()
    tracker.check(reading("CUST_001", 50, 72))
    tracker.check(reading("CUST_001", 10, 72))        # arrives late, older timestamp
    assert tracker.check_silence(at(75)) == []        # last seen is still 50 s


def test_silence_and_anomaly_cooldowns_are_separate():
    tracker = AlertTracker()
    assert len(feed(tracker, "CUST_001", [(0, 204), (5, 204), (10, 204)])) == 1
    assert len(tracker.check_silence(at(50))) == 1    # a different rule is not blocked