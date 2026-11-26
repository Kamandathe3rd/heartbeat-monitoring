"""
Alerter: a second, independent Kafka consumer group that watches the
heart-rate stream and raises alerts.

    Kafka (group heartbeat-alerter) -> validation -> alert rules
        -> log line (WARNING) + alerts table + email (if configured)

It reads the SAME topic as the consumer, but with its own group, so both
receive every message (fan-out). It stores no readings - that is the
consumer's job. On its very first run it starts at the newest message,
so it never alerts about old history.

Run from the project root:  python -m src.alerter      (Ctrl + C to stop)
"""
import logging
from collections import Counter
from datetime import datetime, timezone

import psycopg2
from confluent_kafka import Consumer, KafkaException

from src.alert_rules import Alert, AlertTracker
from src.config import KAFKA_ALERTER_GROUP_ID, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from src.db import connect_to_database, first_line
from src.notifier import email_enabled, send_email
from src.validation import Reading, ValidationError, validate_message

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("alerter")

# Read up to BATCH_SIZE messages at a time, waiting at most POLL_TIMEOUT_SECONDS
BATCH_SIZE = 100
POLL_TIMEOUT_SECONDS = 1.0

INSERT_ALERT_SQL = """
    INSERT INTO alerts (customer_id, rule, message, triggered_at)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (customer_id, rule, triggered_at) DO NOTHING
    RETURNING id
"""

MARK_EMAILED_SQL = "UPDATE alerts SET email_sent = TRUE WHERE id = %s"


def create_consumer() -> Consumer:
    """A consumer in its OWN group: it gets every message, independently of the storing consumer."""
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_ALERTER_GROUP_ID,
            "auto.offset.reset": "latest",  # first run: only new messages, no alerts about old history
            "enable.auto.commit": False,
        }
    )


def log_assignment(consumer: Consumer, partitions: list) -> None:
    """Called by Kafka when this consumer is given partitions to read."""
    logger.info("Assigned partitions: %s", [p.partition for p in partitions])


def readings_from(messages: list) -> list[Reading]:
    """Validate a batch of Kafka messages; keep only the valid readings."""
    readings = []
    for msg in messages:
        if msg.error():
            logger.error("Kafka error: %s", msg.error())
            continue
        try:
            readings.append(validate_message(msg.value()))
        except ValidationError:
            continue  # rejected messages are the consumer's job, not an alert
    return readings


def save_alert(connection, alert: Alert) -> int | None:
    """Store the alert. Returns its new id, or None if it was already stored."""
    with connection.cursor() as cursor:
        cursor.execute(
            INSERT_ALERT_SQL,
            (alert.customer_id, alert.rule, alert.message, alert.triggered_at),
        )
        row = cursor.fetchone()
    connection.commit()
    return row[0] if row else None


def email_alert(connection, alert_id: int, alert: Alert) -> bool:
    """Email the alert; if the mail server accepted it, mark it as sent."""
    subject = f"[Heartbeat ALERT] {alert.customer_id}: {alert.rule.replace('_', ' ')}"
    body = (
        f"Customer:  {alert.customer_id}\n"
        f"Rule:      {alert.rule}\n"
        f"Details:   {alert.message}\n"
        f"Time:      {alert.triggered_at.isoformat(timespec='seconds')}\n"
        f"Alert id:  {alert_id}\n"
    )
    if not send_email(subject, body):
        return False
    with connection.cursor() as cursor:
        cursor.execute(MARK_EMAILED_SQL, (alert_id,))
    connection.commit()
    return True


def raise_alert(connection, alert: Alert, stats: Counter) -> None:
    """Store, email and log one alert (only once, even if it is raised again)."""
    alert_id = save_alert(connection, alert)
    if alert_id is None:
        logger.info("Alert already stored, skipped: %s %s", alert.rule, alert.customer_id)
        return
    emailed = email_alert(connection, alert_id, alert) if email_enabled() else False
    stats["alerts"] += 1
    stats["emails"] += int(emailed)
    logger.warning(
        "ALERT %s %s: %s (email %s)",
        alert.rule,
        alert.customer_id,
        alert.message,
        "sent" if emailed else "NOT sent",
    )


def raise_all(connection, alerts: list[Alert], stats: Counter):
    """Raise every alert, reconnecting to PostgreSQL if it goes away. Returns the connection."""
    for alert in alerts:
        while True:
            try:
                raise_alert(connection, alert, stats)
                break
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as error:
                logger.error("Database error, alert NOT saved: %s", first_line(error))
                connection.close()
                connection = connect_to_database()
    return connection


def run() -> None:
    """Watch the stream and raise alerts until Ctrl + C is pressed."""
    tracker = AlertTracker()
    stats = Counter()
    connection = None
    consumer = None
    try:
        connection = connect_to_database()
        consumer = create_consumer()
        consumer.subscribe([KAFKA_TOPIC], on_assign=log_assignment)
        logger.info(
            "Watching topic '%s' as group '%s'. Email alerts: %s. Press Ctrl + C to stop.",
            KAFKA_TOPIC,
            KAFKA_ALERTER_GROUP_ID,
            "ON" if email_enabled() else "OFF",
        )

        while True:
            messages = consumer.consume(BATCH_SIZE, POLL_TIMEOUT_SECONDS)

            alerts = []
            for reading in readings_from(messages):
                alert = tracker.check(reading)
                if alert is not None:
                    alerts.append(alert)
            alerts.extend(tracker.check_silence(datetime.now(timezone.utc)))

            connection = raise_all(connection, alerts, stats)

            if messages:
                try:
                    consumer.commit(asynchronous=False)  # after the alerts are stored
                except KafkaException as error:
                    logger.warning("Offset commit failed (%s)", error)

    except KeyboardInterrupt:
        logger.info("Ctrl + C received, stopping...")
    finally:
        if consumer is not None:
            consumer.close()
        if connection is not None:
            connection.close()
        logger.info("Alerter stopped. Alerts raised: %d, emails sent: %d", stats["alerts"], stats["emails"])


if __name__ == "__main__":
    run()