"""
Shared PostgreSQL helpers, used by both the consumer and the alerter.
"""
import logging
import time

import psycopg2

from src.config import DB_CONFIG

logger = logging.getLogger("db")

# Retry: wait 2 s, then 4, 8, 16, 30, 30 ... seconds between attempts
RETRY_START_SECONDS = 2
RETRY_MAX_SECONDS = 30


def first_line(error: Exception) -> str:
    """The first line of an error message (psycopg2 errors span several lines)."""
    text = str(error).strip()
    return text.splitlines()[0] if text else type(error).__name__


def connect_to_database():
    """Open a PostgreSQL connection, retrying with a growing wait until it works."""
    wait = RETRY_START_SECONDS
    while True:
        try:
            connection = psycopg2.connect(**DB_CONFIG)
            logger.info("Connected to PostgreSQL at %s:%s", DB_CONFIG["host"], DB_CONFIG["port"])
            return connection
        except psycopg2.OperationalError as error:
            logger.error("PostgreSQL unavailable (%s). Retrying in %d s...", first_line(error), wait)
            time.sleep(wait)
            wait = min(wait * 2, RETRY_MAX_SECONDS)