"""
Kafka producer: sends synthetic heart-rate readings to the Kafka topic.

Run from the project root:
    python -m src.producer                  # send forever, every 1 s (Ctrl + C to stop)
    python -m src.producer --rounds 3       # send 3 rounds, then stop
    python -m src.producer --help           # show all options
"""
import argparse
import json
import logging

from confluent_kafka import Producer

from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from src.data_generator import ANOMALY_RATE, DEFAULT_INTERVAL_SECONDS, stream_readings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("producer")


def create_producer() -> Producer:
    """Create a Kafka producer that waits for the broker to confirm every message."""
    return Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "heartbeat-producer",
            "acks": "all",
            "enable.idempotence": True,
        }
    )


def delivery_report(err, msg) -> None:
    """Called once per message by Kafka: log whether it arrived."""
    if err is not None:
        logger.error("Delivery FAILED for %s: %s", msg.key().decode(), err)
    else:
        logger.info(
            "Delivered %s -> partition %d, offset %d",
            msg.key().decode(),
            msg.partition(),
            msg.offset(),
        )


def send_reading(producer: Producer, reading: dict) -> None:
    """Turn one reading into JSON and queue it for sending, keyed by customer_id."""
    producer.produce(
        topic=KAFKA_TOPIC,
        key=reading["customer_id"],
        value=json.dumps(reading),
        callback=delivery_report,
    )


def run(interval_seconds: float, max_rounds: int | None, anomaly_rate: float) -> None:
    """Send readings to Kafka until `max_rounds` is reached or Ctrl + C is pressed."""
    producer = create_producer()
    queued = 0
    logger.info(
        "Sending to topic '%s' every %.1f s. Press Ctrl + C to stop.",
        KAFKA_TOPIC,
        interval_seconds,
    )
    try:
        for reading in stream_readings(interval_seconds, max_rounds, anomaly_rate):
            send_reading(producer, reading)
            queued += 1
            producer.poll(0)  # let Kafka run delivery_report for confirmed messages
    except KeyboardInterrupt:
        logger.info("Ctrl + C received, stopping...")
    finally:
        undelivered = producer.flush(10)  # send whatever is still in the outbox
        logger.info("Stopped. Queued: %d, not delivered: %d", queued, undelivered)


def parse_args() -> argparse.Namespace:
    """Read the command-line options."""
    parser = argparse.ArgumentParser(
        description="Stream synthetic heart-rate readings to Kafka."
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_INTERVAL_SECONDS,
        help="seconds between rounds (default: %(default)s)",
    )
    parser.add_argument(
        "--rounds",
        type=int,
        default=None,
        help="stop after this many rounds (default: run forever)",
    )
    parser.add_argument(
        "--anomaly-rate",
        type=float,
        default=ANOMALY_RATE,
        help="share of abnormal readings, 0 to 1 (default: %(default)s)",
    )
    args = parser.parse_args()
    if args.interval < 0:
        parser.error("--interval must be 0 or more")
    if not 0 <= args.anomaly_rate <= 1:
        parser.error("--anomaly-rate must be between 0 and 1")
    return args


if __name__ == "__main__":
    args = parse_args()
    run(args.interval, args.rounds, args.anomaly_rate)