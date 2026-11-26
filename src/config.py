"""
Central settings for the heartbeat pipeline.

Every script imports its settings from here, so values such as the Kafka
address or the database password are written in ONE place (.env) and
read in ONE place (this file).
"""
import os
from pathlib import Path

from dotenv import load_dotenv

# Project root = the folder above src/  ->  ...\heartbeat-monitoring
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Load .env from the project root, whatever folder the script is run from
load_dotenv(PROJECT_ROOT / ".env")


def _require(name: str) -> str:
    """Return the setting called `name`, or stop with a clear message if it is missing."""
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise RuntimeError(
            f"Missing setting '{name}'. Copy .env.example to .env and fill it in."
        )
    return value


# ----- Kafka -----
KAFKA_BOOTSTRAP_SERVERS = _require("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = _require("KAFKA_TOPIC")
KAFKA_GROUP_ID = _require("KAFKA_GROUP_ID")

# ----- PostgreSQL -----
POSTGRES_HOST = _require("POSTGRES_HOST")
POSTGRES_PORT = int(_require("POSTGRES_PORT"))
POSTGRES_DB = _require("POSTGRES_DB")
POSTGRES_USER = _require("POSTGRES_USER")
POSTGRES_PASSWORD = _require("POSTGRES_PASSWORD")

# Ready-made keyword arguments for psycopg2.connect(**DB_CONFIG)
DB_CONFIG = {
    "host": POSTGRES_HOST,
    "port": POSTGRES_PORT,
    "dbname": POSTGRES_DB,
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
}

# ----- Email alerts (optional: leave SMTP_HOST empty to disable email) -----
SMTP_HOST = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT") or 1025)
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "false").strip().lower() == "true"
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "").strip()
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
ALERT_EMAIL_FROM = os.getenv("ALERT_EMAIL_FROM", "heartbeat-monitor@example.com").strip()
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO", "").strip()

# ----- Alerter (optional service) -----
KAFKA_ALERTER_GROUP_ID = os.getenv("KAFKA_ALERTER_GROUP_ID", "heartbeat-alerter").strip()