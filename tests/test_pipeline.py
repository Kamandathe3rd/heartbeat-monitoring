"""
End-to-end tests: test messages -> Kafka -> consumer -> PostgreSQL.

Needs Docker (Kafka + PostgreSQL) AND the consumer running in another window:
    python -m src.consumer
Run only these tests:      python -m pytest -m integration -v
Run everything else:       python -m pytest -m "not integration"

Each test uses its own unique customer id (TEST_xxxxxxxx) and deletes its
rows afterwards, so real data is never touched.
"""
import json
import time
import uuid
from datetime import datetime, timedelta, timezone

import psycopg2
import pytest
from confluent_kafka.admin import AdminClient

from src.config import DB_CONFIG, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from src.producer import create_producer

pytestmark = pytest.mark.integration  # every test in this file is an integration test

WAIT_SECONDS = 30  # how long to wait for the consumer to store the test messages


@pytest.fixture(scope="module")
def db():
    """A database connection. Skips the tests if Kafka or PostgreSQL is not running."""
    try:
        AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}).list_topics(timeout=5)
    except Exception as error:
        pytest.skip(f"Kafka is not reachable ({error}). Run: docker compose up -d")
    try:
        connection = psycopg2.connect(**DB_CONFIG, connect_timeout=5)
    except psycopg2.OperationalError as error:
        pytest.skip(f"PostgreSQL is not reachable ({error}). Run: docker compose up -d")
    connection.autocommit = True  # every query sees the latest committed rows
    yield connection
    connection.close()


@pytest.fixture
def test_id(db):
    """A unique customer id for one test; its rows are deleted afterwards."""
    run_id = f"TEST_{uuid.uuid4().hex[:8]}"
    yield run_id
    with db.cursor() as cursor:
        cursor.execute("DELETE FROM heartbeat_readings WHERE customer_id = %s", (run_id,))
        cursor.execute("DELETE FROM rejected_readings WHERE raw_message LIKE %s", (f"%{run_id}%",))


def reading_json(test_id: str, start: datetime, second: int, heart_rate) -> str:
    """A reading for `test_id`, `second` seconds after `start`."""
    timestamp = (start + timedelta(seconds=second)).isoformat(timespec="milliseconds")
    return json.dumps({"customer_id": test_id, "timestamp": timestamp, "heart_rate": heart_rate})


def send_raw(messages: list[str], key: str) -> None:
    """Send raw messages to the real Kafka topic and wait until Kafka confirms them."""
    producer = create_producer()
    for value in messages:
        producer.produce(KAFKA_TOPIC, key=key, value=value)
    assert producer.flush(10) == 0, "Kafka did not confirm all test messages"


def fetch_rows(db, test_id: str) -> tuple[list, list]:
    """Return (stored readings, rejection reasons) that belong to this test."""
    with db.cursor() as cursor:
        cursor.execute(
            "SELECT heart_rate, is_anomaly FROM heartbeat_readings "
            "WHERE customer_id = %s ORDER BY heart_rate",
            (test_id,),
        )
        stored = cursor.fetchall()
        cursor.execute(
            "SELECT reason FROM rejected_readings WHERE raw_message LIKE %s ORDER BY reason",
            (f"%{test_id}%",),
        )
        reasons = [row[0] for row in cursor.fetchall()]
    return stored, reasons


def wait_for_rows(db, test_id: str, readings: int, rejected: int) -> tuple[list, list]:
    """Poll the database until the expected number of rows arrive (or time runs out)."""
    deadline = time.monotonic() + WAIT_SECONDS
    while True:
        stored, reasons = fetch_rows(db, test_id)
        if (len(stored), len(reasons)) == (readings, rejected) or time.monotonic() > deadline:
            return stored, reasons
        time.sleep(0.5)


def test_each_kind_of_reading_lands_in_the_right_table(db, test_id):
    start = datetime.now(timezone.utc)
    send_raw(
        [
            reading_json(test_id, start, 0, 72),    # normal          -> stored
            reading_json(test_id, start, 1, 204),   # too high        -> stored, flagged
            reading_json(test_id, start, 2, 27),    # too low         -> stored, flagged
            reading_json(test_id, start, 3, None),  # missing value   -> rejected
            reading_json(test_id, start, 4, -1),    # impossible      -> rejected
            f"this is not JSON {test_id}",          # corrupt message -> rejected
        ],
        key=test_id,
    )

    stored, reasons = wait_for_rows(db, test_id, readings=3, rejected=3)

    assert stored == [(27, True), (72, False), (204, True)], (
        f"Unexpected stored rows {stored}. Is the consumer running (python -m src.consumer)?"
    )
    assert reasons == [
        "heart_rate -1 is out of range (20-250)",
        "heart_rate is missing",
        "not valid JSON",
    ]


def test_a_duplicate_message_is_stored_only_once(db, test_id):
    start = datetime.now(timezone.utc)
    same_message = reading_json(test_id, start, 0, 80)
    send_raw([same_message, same_message, reading_json(test_id, start, 1, 81)], key=test_id)

    stored, _ = wait_for_rows(db, test_id, readings=2, rejected=0)
    time.sleep(2)  # give a wrong extra row time to appear
    stored_later, _ = fetch_rows(db, test_id)

    assert stored == [(80, False), (81, False)], (
        f"Unexpected stored rows {stored}. Is the consumer running (python -m src.consumer)?"
    )
    assert stored_later == stored  # still only 2 rows: the duplicate was skipped