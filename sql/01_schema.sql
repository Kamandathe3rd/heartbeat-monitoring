-- =====================================================================
-- 01_schema.sql  -  Database schema for the heartbeat monitoring pipeline
--
-- Run (PowerShell, from the project root):
--   docker exec hb-postgres psql -U heartbeat_user -d heartbeat_db -v ON_ERROR_STOP=1 -f /sql/01_schema.sql
--
-- Safe to re-run: each object is only created if it does not exist yet.
-- =====================================================================

-- ---------------------------------------------------------------------
-- Valid heart-rate readings: normal ones + abnormal-but-possible ones
-- (abnormal ones are kept and flagged with is_anomaly = TRUE)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS heartbeat_readings (
    id           BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id  VARCHAR(20)  NOT NULL,
    recorded_at  TIMESTAMPTZ  NOT NULL,              -- when the sensor took the reading
    heart_rate   SMALLINT     NOT NULL,              -- beats per minute
    is_anomaly   BOOLEAN      NOT NULL DEFAULT FALSE,
    ingested_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),-- when the consumer saved it

    -- Safety net: physically possible range (Python validates first)
    CONSTRAINT chk_heart_rate_range CHECK (heart_rate BETWEEN 20 AND 250),

    -- No duplicates if Kafka re-delivers a message (at-least-once delivery)
    CONSTRAINT uq_customer_recorded_at UNIQUE (customer_id, recorded_at)
);

-- Fast time-range queries across all customers ("everything in the last 5 minutes").
-- Per-customer queries use the index that uq_customer_recorded_at creates automatically.
CREATE INDEX IF NOT EXISTS idx_heartbeat_readings_recorded_at
    ON heartbeat_readings (recorded_at);

COMMENT ON TABLE heartbeat_readings IS
    'Valid heart-rate readings from the Kafka consumer. Abnormal but possible values are kept with is_anomaly = TRUE.';


-- ---------------------------------------------------------------------
-- Dead-letter table: impossible or corrupt messages
-- (kept for inspection instead of being silently dropped)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rejected_readings (
    id               BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    raw_message      TEXT         NOT NULL,              -- exactly as received from Kafka
    reason           TEXT         NOT NULL,              -- why it was rejected
    kafka_partition  INTEGER      NOT NULL,              -- where it came from in Kafka
    kafka_offset     BIGINT       NOT NULL,
    rejected_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),

    -- One Kafka message = at most one rejected row, even if it is re-delivered
    CONSTRAINT uq_rejected_kafka_position UNIQUE (kafka_partition, kafka_offset)
);

COMMENT ON TABLE rejected_readings IS
    'Dead-letter table: messages that failed validation, with the reason and their Kafka position.';