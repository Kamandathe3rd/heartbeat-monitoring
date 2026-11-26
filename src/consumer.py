"""
Kafka consumer: reads heart-rate messages, validates them and stores them
in PostgreSQL.

    valid / anomaly  -> heartbeat_readings  (is_anomaly flags the anomalies)
    impossible       -> rejected_readings   (with the reason)

Kafka offsets are committed only AFTER the database commit, so a crash
never loses a message (it is re-read and the duplicate is skipped).
If PostgreSQL goes down, the current batch is kept and retried until the
database is back.

Run from the project root:  python -m src.consumer      (Ctrl + C to stop)
"""
import logging
import time
from collections import Counter

import psycopg2
from confluent_kafka import Consumer, KafkaException, Message

from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID, KAFKA_TOPIC
from src.db import connect_to_database, first_line
from src.validation import ValidationError, validate_message

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("consumer")

# Read up to BATCH_SIZE messages at a time, waiting at most POLL_TIMEOUT_SECONDS
BATCH_SIZE = 100
POLL_TIMEOUT_SECONDS = 1.0

# How often to log the running totals
SUMMARY_EVERY_SECONDS = 10

INSERT_READING_SQL = """
    INSERT INTO heartbeat_readings (customer_id, recorded_at, heart_rate, is_anomaly)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (customer_id, recorded_at) DO NOTHING
"""

INSERT_REJECT_SQL = """
    INSERT INTO rejected_readings (raw_message, reason, kafka_partition, kafka_offset)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING
"""


def create_consumer() -> Consumer:
    """Create a Kafka consumer that starts at the oldest message and never auto-commits."""
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


def log_assignment(consumer: Consumer, partitions: list) -> None:
    """Called by Kafka when this consumer is given partitions to read."""
    logger.info("Assigned partitions: %s", [p.partition for p in partitions])


def raw_text(msg: Message) -> str:
    """The message body as text, safe to store in a TEXT column (even if broken)."""
    value = msg.value()
    if value is None:
        return ""
    text = value.decode("utf-8", errors="replace")  # bad bytes become '?'-like symbols
    return text.replace("\x00", "\\x00")             # PostgreSQL TEXT cannot hold NUL


def handle_message(msg: Message, cursor) -> str:
    """
    Validate one Kafka message and write it to the right table.

    Returns the outcome: "stored", "anomaly", "rejected" or "duplicate".
    """
    try:
        reading = validate_message(msg.value())
    except ValidationError as error:
        cursor.execute(
            INSERT_REJECT_SQL,
            (raw_text(msg), str(error), msg.partition(), msg.offset()),
        )
        if cursor.rowcount == 0:  # this exact Kafka message was already rejected
            logger.info("DUPLICATE reject p%d/o%d (skipped)", msg.partition(), msg.offset())
            return "duplicate"
        logger.warning("REJECT    p%d/o%d: %s", msg.partition(), msg.offset(), error)
        return "rejected"

    cursor.execute(
        INSERT_READING_SQL,
        (reading.customer_id, reading.recorded_at, reading.heart_rate, reading.is_anomaly),
    )
    if cursor.rowcount == 0:  # ON CONFLICT skipped it: we already have this reading
        logger.info("DUPLICATE %s at %s (skipped)", reading.customer_id, reading.recorded_at.isoformat())
        return "duplicate"

    outcome = "anomaly" if reading.is_anomaly else "stored"
    logger.info(
        "%-9s %s %3d bpm at %s",
        outcome.upper(),
        reading.customer_id,
        reading.heart_rate,
        reading.recorded_at.isoformat(),
    )
    return outcome


def save_batch(connection, messages: list, stats: Counter) -> int:
    """
    Write a batch of messages in ONE database transaction.

    Returns how many messages were handled. `stats` is only updated after a
    successful commit, so a failed and retried batch is never counted twice.
    """
    batch_stats = Counter()
    with connection.cursor() as cursor:
        for msg in messages:
            if msg.error():
                logger.error("Kafka error: %s", msg.error())
                continue
            batch_stats[handle_message(msg, cursor)] += 1
    connection.commit()
    stats.update(batch_stats)
    return batch_stats.total()


def log_summary(stats: Counter, final: bool = False) -> None:
    """Log the running totals."""
    logger.info(
        "%s: %d stored, %d anomaly, %d rejected, %d duplicate (%d messages)",
        "FINAL SUMMARY" if final else "Summary",
        stats["stored"],
        stats["anomaly"],
        stats["rejected"],
        stats["duplicate"],
        stats.total(),
    )


def run() -> None:
    """Consume, validate and store messages until Ctrl + C is pressed."""
    stats = Counter()
    connection = None
    consumer = None
    try:
        connection = connect_to_database()
        consumer = create_consumer()
        consumer.subscribe([KAFKA_TOPIC], on_assign=log_assignment)
        logger.info(
            "Listening on topic '%s' as group '%s'. Press Ctrl + C to stop.",
            KAFKA_TOPIC,
            KAFKA_GROUP_ID,
        )
        last_summary = time.monotonic()

        while True:
            messages = consumer.consume(BATCH_SIZE, POLL_TIMEOUT_SECONDS)
            if messages:
                # Keep retrying THIS batch until the database accepts it
                while True:
                    try:
                        handled = save_batch(connection, messages, stats)  # 1. DB commit
                        break
                    except (psycopg2.OperationalError, psycopg2.InterfaceError) as error:
                        logger.error("Database error, batch NOT saved: %s", first_line(error))
                        connection.close()
                        connection = connect_to_database()
                if handled:
                    try:
                        consumer.commit(asynchronous=False)  # 2. THEN the Kafka bookmark
                    except KafkaException as error:
                        logger.warning(
                            "Offset commit failed (%s); these messages will be re-read "
                            "and skipped as duplicates.", error
                        )

            if time.monotonic() - last_summary >= SUMMARY_EVERY_SECONDS:
                log_summary(stats)
                last_summary = time.monotonic()

    except KeyboardInterrupt:
        logger.info("Ctrl + C received, stopping...")
    finally:
        if consumer is not None:
            consumer.close()     # leave the group cleanly
        if connection is not None:
            connection.close()   # anything not committed is rolled back
        log_summary(stats, final=True)
        logger.info("Consumer closed.")


if __name__ == "__main__":
    run()