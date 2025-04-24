#!/bin/bash

# Wait for PostgreSQL to be ready using Unix socket
until pg_isready -U "${POSTGRES_USER}" -d postgres -h /var/run/postgresql; do
    echo "Waiting for PostgreSQL to be ready..."
    sleep 2
done

echo "PostgreSQL is ready, initializing database..."

# Run initialization script
python3 /docker-entrypoint-initdb.d/init_db.py

# Check if posts table exists in the target database
check_table() {
    psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -h /var/run/postgresql -tAc "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'posts')"
}

# Wait for table creation
echo "Waiting for tables to be created..."
while [ "$(check_table)" != "t" ]; do
    echo "Table 'posts' not found, waiting..."
    sleep 2
done

echo "Database initialization complete"
