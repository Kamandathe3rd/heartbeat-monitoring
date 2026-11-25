-- =====================================================================
-- 03_grafana_reader.sql  -  Read-only database user for the Grafana dashboard
--
-- Run (PowerShell, from the project root), passing the password from .env:
--   docker exec hb-postgres psql -U heartbeat_user -d heartbeat_db -v ON_ERROR_STOP=1 -v reader_password=YOUR_PASSWORD -f /sql/03_grafana_reader.sql
--
-- Safe to re-run: creates the user once, then just refreshes its password and rights.
-- =====================================================================

-- 1. Create the login role only if it does not exist yet (\gexec runs the generated statement)
SELECT 'CREATE ROLE grafana_reader LOGIN'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_reader')\gexec

-- 2. Set (or refresh) its password from the psql variable
ALTER ROLE grafana_reader WITH LOGIN PASSWORD :'reader_password';

-- 3. Extra safety: every session is read-only, and no query may run longer than 10 s
ALTER ROLE grafana_reader SET default_transaction_read_only = on;
ALTER ROLE grafana_reader SET statement_timeout = '10s';

-- 4. Rights: connect, see the schema, and SELECT from tables (now and in future)
GRANT CONNECT ON DATABASE :"DBNAME" TO grafana_reader;
GRANT USAGE ON SCHEMA public TO grafana_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO grafana_reader;