-- Setup pg_cron in postgres database
\c postgres
CREATE EXTENSION IF NOT EXISTS pg_cron;
CREATE EXTENSION IF NOT EXISTS dblink;

-- Create schema and grant permissions
CREATE SCHEMA IF NOT EXISTS cron;
GRANT USAGE ON SCHEMA cron TO current_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA cron TO current_user;

-- Switch to application database and create extension there
\c ${POSTGRES_DB}
CREATE EXTENSION IF NOT EXISTS pg_cron;
