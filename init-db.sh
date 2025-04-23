#!/bin/bash
set -e

# Ensure required environment variables are set
for var in POSTGRES_USER POSTGRES_DB; do
    if [ -z "${!var}" ]; then
        echo "Error: $var environment variable is not set"
        exit 1
    fi
done

# Wait for database to be ready
until pg_isready -U "$POSTGRES_USER" -d "postgres"; do
    echo "Waiting for database to be ready..."
    sleep 2
done

# Initialize pg_cron in postgres database
echo "Setting up extensions in postgres database..."
psql -U "$POSTGRES_USER" -d postgres <<-EOSQL
    -- Create extensions
    CREATE EXTENSION IF NOT EXISTS pg_cron;
    CREATE EXTENSION IF NOT EXISTS dblink;
    
    -- Create schema and grant permissions
    CREATE SCHEMA IF NOT EXISTS cron;
    GRANT USAGE ON SCHEMA cron TO "$POSTGRES_USER";
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA cron TO "$POSTGRES_USER";
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA cron TO "$POSTGRES_USER";
    ALTER DEFAULT PRIVILEGES IN SCHEMA cron GRANT ALL ON TABLES TO "$POSTGRES_USER";
    ALTER DEFAULT PRIVILEGES IN SCHEMA cron GRANT ALL ON SEQUENCES TO "$POSTGRES_USER";
EOSQL

# Create application database if it doesn't exist
echo "Creating application database if it doesn't exist..."
psql -U "$POSTGRES_USER" -d postgres <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_database WHERE datname = '$POSTGRES_DB') THEN
            CREATE DATABASE $POSTGRES_DB;
        END IF;
    END
    \$\$;
EOSQL

# Initialize dblink in application database
echo "Setting up dblink in application database..."
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<-EOSQL
    CREATE EXTENSION IF NOT EXISTS dblink;
EOSQL

# Initialize schema
echo "Initializing database schema..."
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/sql/01-init-tables.sql

# Setup cron job in postgres database
echo "Setting up cron job..."
psql -U "$POSTGRES_USER" -d postgres <<-EOSQL
    SELECT cron.schedule(
        job_name := 'manage_partitions_job',
        schedule := '0 0 * * *',
        command := format(
            'SELECT manage_partitions() FROM dblink(''dbname=%s'', ''SELECT manage_partitions()'') AS t(result void)',
            '$POSTGRES_DB'
        )
    );
EOSQL

echo "Database initialization completed successfully"
