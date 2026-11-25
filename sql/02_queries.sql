-- =====================================================================
-- 02_queries.sql  -  Example queries on the heartbeat data
--
-- Run all (PowerShell, from the project root):
--   docker exec -e PAGER=cat hb-postgres psql -U heartbeat_user -d heartbeat_db -f /sql/02_queries.sql
--
-- Read-only: nothing here changes the stored data
-- (the INSERT example is rolled back).
-- =====================================================================

\echo '--- 1. Latest reading per customer ---'
SELECT DISTINCT ON (customer_id)
       customer_id, recorded_at, heart_rate, is_anomaly
FROM heartbeat_readings
ORDER BY customer_id, recorded_at DESC;

\echo '--- 2. Last 5 minutes: readings and heart-rate stats per customer ---'
SELECT customer_id,
       count(*)                  AS readings,
       min(heart_rate)           AS min_bpm,
       round(avg(heart_rate), 1) AS avg_bpm,
       max(heart_rate)           AS max_bpm
FROM heartbeat_readings
WHERE recorded_at >= now() - interval '5 minutes'
GROUP BY customer_id
ORDER BY customer_id;

\echo '--- 3. Anomalies in the last hour (newest first) ---'
SELECT recorded_at, customer_id, heart_rate,
       CASE WHEN heart_rate < 40 THEN 'too low' ELSE 'too high' END AS kind
FROM heartbeat_readings
WHERE is_anomaly
  AND recorded_at >= now() - interval '1 hour'
ORDER BY recorded_at DESC;

\echo '--- 4. Average heart rate per customer per hour (normal readings only) ---'
SELECT date_trunc('hour', recorded_at) AS hour,
       customer_id,
       count(*)                  AS readings,
       round(avg(heart_rate), 1) AS avg_bpm
FROM heartbeat_readings
WHERE NOT is_anomaly
GROUP BY hour, customer_id
ORDER BY hour DESC, customer_id;

\echo '--- 5. Pipeline delay: sensor time -> saved in the database ---'
SELECT count(*) AS readings,
       round(avg(extract(epoch FROM ingested_at - recorded_at)) * 1000) AS avg_delay_ms,
       round(max(extract(epoch FROM ingested_at - recorded_at)) * 1000) AS max_delay_ms
FROM heartbeat_readings;

\echo '--- 6. Rejected messages by reason ---'
SELECT reason, count(*) AS messages
FROM rejected_readings
GROUP BY reason
ORDER BY messages DESC;

\echo '--- 7. Data quality summary ---'
SELECT (SELECT count(*) FROM heartbeat_readings)                  AS stored,
       (SELECT count(*) FROM heartbeat_readings WHERE is_anomaly) AS flagged_anomalies,
       (SELECT count(*) FROM rejected_readings)                   AS rejected;

\echo '--- 8. Insert example (same statement the consumer uses) - rolled back ---'
BEGIN;
INSERT INTO heartbeat_readings (customer_id, recorded_at, heart_rate, is_anomaly)
VALUES ('CUST_001', '2026-11-25T09:00:00.000+00:00', 66, FALSE)
ON CONFLICT (customer_id, recorded_at) DO NOTHING
RETURNING id, customer_id, recorded_at, heart_rate;
ROLLBACK;