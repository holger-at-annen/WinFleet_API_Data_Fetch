-- Create extension in postgres database first
\c postgres
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Switch to application database and create extension there
\c ${POSTGRES_DB}
CREATE EXTENSION IF NOT EXISTS pg_cron;
